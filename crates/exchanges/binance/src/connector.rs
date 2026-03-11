use std::{error::Error, sync::Arc};

use futures_util::{stream, StreamExt, TryStreamExt};
use md_core::{connector_trait::{ConnectionTasks, ExchangeConnector, WriteCommand}, events::InboundEvent, types::{Instrument, RawMdMsg}};
use reqwest::Client;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info};
use url::Url;
use crate::types::{ApiResponse, BinanceMdMsg, BinanceSnapshotMsg, BinanceSubscriptionMsg, BinanceUrls, DepthQuery, SubscriptionRequest};

pub struct BinanceConnector {
    ws_url: Arc<Url>,
    manager_tx: Sender<ManagerCommand>,
    raw_tx: Sender<BinanceMdMsg>,
    inbound_rx: Receiver<InboundEvent>
}

enum ManagerCommand {
    RecreateWithSnapshots,
    PongAll(Vec<u8>)
}

impl BinanceConnector {
    pub async fn new(urls: BinanceUrls, max_subscription_per_ws: usize, raw_tx: Sender<BinanceMdMsg>) -> Result<Self, Box<dyn Error + Send + Sync>> 
    where 
        Self: Sized
    {
        let (inbound_tx, inbound_rx) = channel::<InboundEvent>(4096);
        let snapshot_url = Arc::from(urls.snapshot);
        let ws_url = Arc::from(urls.ws);
        let subscriptions_payloads = BinanceConnector::build_subscriptions(&urls.exchange_info, max_subscription_per_ws).await?;

        let (manager_tx, manager_rx) = channel::<ManagerCommand>(16);

        tokio::spawn(connection_manager_task(
            ws_url.clone(),
            snapshot_url,
            subscriptions_payloads,
            inbound_tx,
            raw_tx.clone(),
            manager_rx,
        ));

        Ok(Self {
            raw_tx,
            inbound_rx,
            ws_url,
            manager_tx,
        })
    }
}

impl ExchangeConnector for BinanceConnector {
    type SubscriptionPayload = BinanceSubscriptionMsg;

    async fn get_subscriptions_list(rest_url: &Url) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        let client = Client::new();
        let resp = client.get(rest_url.as_str())
                            .send().await?
                            .json::<ApiResponse>().await?;
        let mut list = Vec::new();
        for symbol in resp.symbols {
            if symbol.status == "TRADING" {
                list.push(symbol.symbol);
            }
        }
        Ok(list)
    }

    async fn build_subscriptions(rest_url: &Url, max_subscription_per_ws: usize) -> Result<Vec<BinanceSubscriptionMsg>, Box<dyn Error + Send + Sync>>{
        let mut list = BinanceConnector::get_subscriptions_list(rest_url).await?;
        let subscriptions_payloads_len = (list.len()/max_subscription_per_ws) + 1;
        let mut result = Vec::new();
        let update_settings = "@depth@100ms";
        for i in 0..subscriptions_payloads_len {
            let mut params_len = max_subscription_per_ws;
            if i == subscriptions_payloads_len - 1 { 
                params_len = list.len();
            }
            let symbols: Vec<Instrument> = list
                .drain(..params_len)
                .map(|x| Instrument::from(x))
                .collect();

            let params: Vec<String> = 
                symbols
                .clone()
                .into_iter()
                .map(|x| {
                    let mut s = x.to_string();
                    s.push_str(&update_settings);
                    s.to_lowercase()
                })
                .collect();

            let sub_req = SubscriptionRequest {
                method: String::from("SUBSCRIBE"),
                params: params.clone(),
                id: i as i64
            };
            let json = serde_json::to_string(&sub_req).expect("Failed to serialize SubscribePayload to JSON");
            let message = Message::text(json);

            result.push( BinanceSubscriptionMsg{
                symbols: symbols,
                payload: message
            });
        }
        Ok(result)
    }

    async fn subscribe_streams(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.manager_tx
            .send(ManagerCommand::RecreateWithSnapshots)
            .await
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
    }

    async fn pong_all(&self, payload: Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.manager_tx
            .send(ManagerCommand::PongAll(payload))
            .await
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
    }

    async fn start(&mut self) {
        if let Err(e) = self.subscribe_streams().await {
            error!(url = ?self.ws_url, error = ?e, "error while subscribing to streams");
            return;
        }
        while let Some(msg) = self.inbound_rx.recv().await {
            match msg {
                InboundEvent::WsMessage(payload) => {
                    if let Err(e) = self.raw_tx.send(BinanceMdMsg::Update(payload)).await {
                        error!(url = ?self.ws_url, error = ?e, "error while sending the update message");
                        continue;
                    }
                },
                InboundEvent::Ping(payload) => {
                    if let Err(e) = self.pong_all(payload).await {
                        error!(url = ?self.ws_url, error = ?e, "error while sending the pong command");
                        continue;
                    }
                },
                InboundEvent::ConnectionClosed => {
                    info!(url = ?self.ws_url, "connection close, reconnecting");
                    if let Err(e) = self.subscribe_streams().await {
                        error!(url = ?self.ws_url, error = ?e, "error while subscribing to streams");
                        continue;
                    }
                }
            }
        }
    }

}

async fn connection_manager_task(
    ws_url: Arc<Url>,
    snapshot_url: Arc<Url>,
    subscriptions_payloads: Vec<BinanceSubscriptionMsg>,
    inbound_tx: Sender<InboundEvent>,
    raw_tx: Sender<BinanceMdMsg>,
    mut cmd_rx: Receiver<ManagerCommand>,
) {
    let mut connections: Vec<ConnectionTasks> = Vec::new();

    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            ManagerCommand::PongAll(payload) => {
                pong_all_connections(&connections, &payload).await;
            }
            ManagerCommand::RecreateWithSnapshots => {
                recreate_with_snapshots_backoff(
                    &mut connections,
                    &ws_url,
                    &snapshot_url,
                    &subscriptions_payloads,
                    &inbound_tx,
                    &raw_tx,
                )
                .await;
            }
        }
    }
}

fn abort_all_connections(connections: &mut Vec<ConnectionTasks>) {
    for c in connections.drain(..) {
        c.reader_handle.abort();
        c.writer_handle.abort();
    }
}

async fn pong_all_connections(connections: &Vec<ConnectionTasks>, payload: &Vec<u8>) {
    for c in connections {
        if let Err(e) = c
            .writer_tx
            .send(WriteCommand::Pong(payload.clone()))
            .await {
                error!(error = ?e, "error while sending the pong command");
            }
    }
}

async fn recreate_with_snapshots_backoff(
    connections: &mut Vec<ConnectionTasks>,
    ws_url: &Arc<Url>,
    snapshot_url: &Arc<Url>,
    subscriptions_payloads: &Vec<BinanceSubscriptionMsg>,
    inbound_tx: &Sender<InboundEvent>,
    raw_tx: &Sender<BinanceMdMsg>,
) {
    let backoff_secs = [1, 5, 15, 30, 60];
        let mut attempt: usize = 0;

        loop {
            if let Err(e) = recreate_with_snapshots(
                    connections,
                    ws_url,
                    snapshot_url,
                    subscriptions_payloads,
                    inbound_tx,
                    raw_tx,
                )
                .await
            {
                error!(url = ?ws_url, error = ?e, "error while sending the recreate command");
                let delay = *backoff_secs
                    .get(attempt)
                    .unwrap_or(backoff_secs.last().unwrap());
                attempt = attempt.saturating_add(1);
                sleep(Duration::from_secs(delay)).await;
                // After the last stage we keep retrying every 60s
            }
        }
}

async fn recreate_with_snapshots(
    connections: &mut Vec<ConnectionTasks>,
    ws_url: &Arc<Url>,
    snapshot_url: &Arc<Url>,
    subscriptions_payloads: &Vec<BinanceSubscriptionMsg>,
    inbound_tx: &Sender<InboundEvent>,
    raw_tx: &Sender<BinanceMdMsg>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    abort_all_connections(connections);

    for message in subscriptions_payloads {
        let (writer_tx, writer_rx) = channel::<WriteCommand>(64);
        let (ws_stream, _) = connect_async(ws_url.as_str()).await?;
        let (write, read) = ws_stream.split();

        let reader_url = ws_url.clone();
        let writer_url = ws_url.clone();

        raw_tx
            .send(BinanceMdMsg::Instruments(message.symbols.clone()))
            .await?;

        let reader_tx_clone = inbound_tx.clone();
        let reader_handle = tokio::spawn(async move {
            BinanceConnector::reader_task(writer_url, read, reader_tx_clone).await;
        });

        let writer_handle = tokio::spawn(async move {
            BinanceConnector::writer_task(reader_url, write, writer_rx).await;
        });

        writer_tx
            .send(WriteCommand::Raw(message.payload.clone()))
            .await?;

        let client = reqwest::Client::new();
        let raw_tx_snap = raw_tx.clone();
        let snapshot_url_snap = snapshot_url.clone();
        let symbols = message.symbols.clone();

        stream::iter(symbols)
            .map(Ok::<_, Box<dyn Error + Send + Sync>>)
            .try_for_each_concurrent(10, move |symbol| {
                let client = client.clone();
                let raw_tx = raw_tx_snap.clone();
                let snapshot_url = snapshot_url_snap.clone();

                async move {
                    let params = DepthQuery {
                        symbol: &symbol,
                        limit: 100,
                    };

                    let response = client
                        .get(snapshot_url.as_str())
                        .query(&params)
                        .send()
                        .await?;

                    let bytes = response.bytes().await?;

                    raw_tx
                        .send(BinanceMdMsg::Snapshot(BinanceSnapshotMsg {
                            symbol,
                            payload: RawMdMsg(bytes.to_vec()),
                        }))
                        .await?;

                    Ok(())
                }
            })
            .await?;

        connections.push(ConnectionTasks {
            reader_handle,
            writer_handle,
            writer_tx,
        });
    }
    Ok(())
}