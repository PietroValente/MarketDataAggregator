use std::{collections::HashMap, error::Error, sync::Arc};
use bytes::BytesMut;
use futures_util::{stream, StreamExt, TryStreamExt};
use md_core::{connector_trait::{ConnectionTasks, ExchangeConnector, WriteCommand}, events::{ControlEvent, InboundEvent, PingMsg}, types::{Exchange, Instrument, RawMdMsg}};
use reqwest::Client;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info};
use url::Url;
use crate::types::{ApiResponse, BinanceConnectorError, BinanceMdMsg, BinanceUrls, DepthQuery, ManagerCommand, SnapshotMsg, SubscriptionBatch, SubscriptionRequest, Subscriptions};

pub struct BinanceConnector {
    manager_tx: Sender<ManagerCommand>,
    raw_tx: Sender<BinanceMdMsg>,
    inbound_rx: Receiver<InboundEvent>
}

impl BinanceConnector {
    pub async fn new(client:Client, urls: BinanceUrls, max_subscription_per_ws: usize, raw_tx: Sender<BinanceMdMsg>, control_rx: Receiver<ControlEvent>) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> 
    where 
        Self: Sized
    {
        let (inbound_tx, inbound_rx) = channel::<InboundEvent>(4096);
        let snapshot_url = Arc::from(urls.snapshot);
        let ws_url = Arc::from(urls.ws);
        let subscriptions = BinanceConnector::build_subscriptions(client, &urls.exchange_info, max_subscription_per_ws).await?;
        let batches_payloads = Arc::from(subscriptions.batches);
        raw_tx
            .send(BinanceMdMsg::Instruments(subscriptions.symbols))
            .await?;

        let (manager_tx, manager_rx) = channel::<ManagerCommand>(128);

        tokio::spawn(connection_manager_task(
            ws_url.clone(),
            snapshot_url,
            batches_payloads,
            inbound_tx,
            raw_tx.clone(),
            manager_tx.clone(),
            manager_rx,
        ));

        tokio::spawn(control_manager_task(
            control_rx, 
            manager_tx.clone()
        ));

        Ok(Self {
            raw_tx,
            inbound_rx,
            manager_tx
        })
    }
}

impl ExchangeConnector for BinanceConnector {
    type SubscriptionsInfo = Subscriptions;

    fn exchange() -> Exchange {
        Exchange::Binance
    }

    async fn get_subscriptions_list(client: Client, rest_url: &Url) -> Result<Vec<Instrument>, Box<dyn Error + Send + Sync + 'static>> {     
        let resp = client
            .get(rest_url.as_str())
            .timeout(Duration::from_secs(60))
            .send()
            .await?
            .error_for_status()?;

        let mut stream = resp.bytes_stream();
        let mut body = BytesMut::new();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            body.extend_from_slice(&chunk);
        }

        let resp: ApiResponse = serde_json::from_slice(&body)?;
        let mut list = Vec::new();
        for symbol in resp.symbols {
            if symbol.status == "TRADING" {
                list.push(symbol.symbol);
            }
        }
        Ok(list)
    }

    async fn build_subscriptions(client: Client, rest_url: &Url, max_subscription_per_ws: usize) -> Result<Subscriptions, Box<dyn Error + Send + Sync + 'static>> {
        if max_subscription_per_ws == 0 {
            return Err(BinanceConnectorError::InvalidMaxSubscriptionPerWs.into());
        }
    
        let symbols = BinanceConnector::get_subscriptions_list_backoff(client, rest_url).await;
        let mut batches = Vec::new();
        let update_settings = "@depth@100ms";
    
        for (i, chunk) in symbols.chunks(max_subscription_per_ws).enumerate() {
            let batch_symbols: Vec<Instrument> = chunk.to_vec();
    
            let params: Vec<String> = batch_symbols
                .iter()
                .map(|x| {
                    let mut s = x.to_string().to_lowercase();
                    s.push_str(update_settings);
                    s
                })
                .collect();
    
            let sub_req = SubscriptionRequest {
                method: String::from("SUBSCRIBE"),
                params,
                id: i as i64,
            };
    
            let json = serde_json::to_string(&sub_req)?;
            let message = Message::text(json);
    
            batches.push(SubscriptionBatch {
                symbols: batch_symbols,
                message,
            });
        }
    
        Ok(Subscriptions { symbols, batches })
    }

    async fn subscribe_streams(&mut self) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        self.manager_tx
            .send(ManagerCommand::RecreateWithSnapshots)
            .await
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)
    }

    async fn pong(&self, msg: PingMsg) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        self.manager_tx
            .send(ManagerCommand::Pong(msg))
            .await
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)
    }

    async fn start(&mut self) {
        if let Err(e) = self.subscribe_streams().await {
            error!(exchange = ?BinanceConnector::exchange(), component = ?BinanceConnector::component(), error = ?e, "error while subscribing to streams");
            return;
        }
        while let Some(msg) = self.inbound_rx.recv().await {
            match msg {
                InboundEvent::WsMessage(payload) => {
                    if let Err(e) = self.raw_tx.send(BinanceMdMsg::WsMessage(payload)).await {
                        error!(exchange = ?BinanceConnector::exchange(), component = ?BinanceConnector::component(), error = ?e, "error while sending the update message");
                        continue;
                    }
                },
                InboundEvent::Ping(ping_msg) => {
                    if let Err(e) = self.pong(ping_msg).await {
                        error!(exchange = ?BinanceConnector::exchange(), component = ?BinanceConnector::component(), error = ?e, "error while sending the pong command");
                        continue;
                    }
                },
                InboundEvent::ConnectionClosed => {
                    info!(exchange = ?BinanceConnector::exchange(), component = ?BinanceConnector::component(), "connection close, reconnecting");
                    if let Err(e) = self.subscribe_streams().await {
                        error!(exchange = ?BinanceConnector::exchange(), component = ?BinanceConnector::component(), error = ?e, "error while subscribing to streams");
                        continue;
                    }
                }
            }
        }
    }

}

async fn control_manager_task(mut control_rx: Receiver<ControlEvent>, manager_tx: Sender<ManagerCommand>) {
    while let Some(event) = control_rx.recv().await {
        match event {
            ControlEvent::Resync => {
                if let Err(e) = manager_tx
                    .send(ManagerCommand::RecreateWithSnapshots)
                    .await {
                        error!(exchange = ?BinanceConnector::exchange(), component = ?BinanceConnector::component(), error = ?e, "failed to send RecreateWithSnapshots command to manager");
                    }
            }
        }
    }
}

async fn connection_manager_task( ws_url: Arc<Url>,
    snapshot_url: Arc<Url>,
    batches_payloads: Arc<Vec<SubscriptionBatch>>,
    inbound_tx: Sender<InboundEvent>,
    raw_tx: Sender<BinanceMdMsg>,
    cmd_tx: Sender<ManagerCommand>,
    mut cmd_rx: Receiver<ManagerCommand>,
) {
    let mut connections: HashMap<u8, ConnectionTasks> = HashMap::new();
    let mut recreate_in_progress = false;

    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            ManagerCommand::RecreateFinished => {
                recreate_in_progress = false;
            },
            ManagerCommand::InsertSubscription(ws_id, connection) => {
                connections.insert(ws_id, connection);
            },
            ManagerCommand::Pong(msg) => {
                pong_ws(&connections, msg).await;
            }
            ManagerCommand::RecreateWithSnapshots => {
                if recreate_in_progress {
                    info!(exchange = ?BinanceConnector::exchange(), component = ?BinanceConnector::component(), "recreate already in progress, skipping");
                    continue;
                }

                recreate_in_progress = true;
                abort_all_connections(&mut connections);

                tokio::spawn(recreate_with_snapshots_backoff(
                    ws_url.clone(),
                    snapshot_url.clone(),
                    batches_payloads.clone(),
                    inbound_tx.clone(),
                    raw_tx.clone(),
                    cmd_tx.clone()
                ));
            }
        }
    }
}

fn abort_all_connections(connections: &mut HashMap<u8, ConnectionTasks>) {
    for (_, c) in connections.drain() {
        c.reader_handle.abort();
        c.writer_handle.abort();
    }
}

async fn pong_ws(connections: &HashMap<u8, ConnectionTasks>, msg: PingMsg) {
    if let Some(conn) = connections.get(&msg.ws_id) {
        if let Err(e) = conn
            .writer_tx
            .send(WriteCommand::Pong(msg.payload))
            .await
        {
            error!(exchange = ?BinanceConnector::exchange(), component = ?BinanceConnector::component(), error = ?e, "error while sending the pong command");
        }
    } else {
        error!(exchange = ?BinanceConnector::exchange(), component = ?BinanceConnector::component(), "pong requested for unknown connection");
    }
}

async fn recreate_with_snapshots_backoff(
    ws_url: Arc<Url>,
    snapshot_url: Arc<Url>,
    batches_payloads: Arc<Vec<SubscriptionBatch>>,
    inbound_tx: Sender<InboundEvent>,
    raw_tx: Sender<BinanceMdMsg>,
    cmd_tx: Sender<ManagerCommand>,
) {
    let backoff_secs = [1, 5, 15, 30, 60];
    let mut attempt: usize = 0;

    loop {
        match recreate_with_snapshots(
            ws_url.clone(),
            snapshot_url.clone(),
            batches_payloads.clone(),
            inbound_tx.clone(),
            raw_tx.clone(),
            cmd_tx.clone()
        ).await {
            Ok(()) => {
                if let Err(e) = cmd_tx.send(ManagerCommand::RecreateFinished).await {
                    error!(exchange = ?BinanceConnector::exchange(), component = ?BinanceConnector::component(), error = ?e, "failed to notify recreate finished");
                }
                return;
            }
            Err(e) => {
                error!(exchange = ?BinanceConnector::exchange(), component = ?BinanceConnector::component(), error = ?e, "error while recreating connections");
                let delay = BinanceConnector::retry_delay(&backoff_secs, attempt);
                attempt = attempt.saturating_add(1);
                sleep(Duration::from_secs(delay)).await; // After the last stage we keep retrying every 60s
            }
        }
    }
}

async fn recreate_with_snapshots(
    ws_url: Arc<Url>,
    snapshot_url: Arc<Url>,
    batches_payloads: Arc<Vec<SubscriptionBatch>>,
    inbound_tx: Sender<InboundEvent>,
    raw_tx: Sender<BinanceMdMsg>,
    cmd_tx: Sender<ManagerCommand>
) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    if batches_payloads.len() > BinanceConnector::ws_id_capacity() {
        return Err(BinanceConnectorError::TooManyWsBatchesForU8Id {
            batches: batches_payloads.len(),
            max_supported: BinanceConnector::ws_id_capacity(),
        }
        .into());
    }

    for (i, batch) in batches_payloads.iter().enumerate() {
        let (writer_tx, writer_rx) = channel::<WriteCommand>(64);
        let (ws_stream, _) = connect_async(ws_url.as_str()).await?;
        let (write, read) = ws_stream.split();

        let reader_url = ws_url.clone();
        let writer_url = ws_url.clone();

        let reader_tx_clone = inbound_tx.clone();
        let Some(ws_id) = BinanceConnector::ws_id_from_index(i) else {
            return Err(BinanceConnectorError::TooManyWsBatchesForU8Id {
                batches: batches_payloads.len(),
                max_supported: BinanceConnector::ws_id_capacity(),
            }
            .into());
        };
        let reader_handle = tokio::spawn(async move {
            BinanceConnector::reader_task(ws_id, writer_url, read, reader_tx_clone).await;
        });

        let writer_handle = tokio::spawn(async move {
            BinanceConnector::writer_task(reader_url, write, writer_rx).await;
        });

        cmd_tx.send( ManagerCommand::InsertSubscription(ws_id, ConnectionTasks {
            reader_handle,
            writer_handle,
            writer_tx: writer_tx.clone(),
        })).await?;

        writer_tx
            .send(WriteCommand::Raw(batch.message.clone()))
            .await?;

        let client = reqwest::Client::new();
        let raw_tx_snap = raw_tx.clone();
        let snapshot_url_snap = snapshot_url.clone();
        let symbols = batch.symbols.clone();

        stream::iter(symbols)
            .map(Ok::<_, Box<dyn Error + Send + Sync + 'static>>)
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
                        .await?
                        .error_for_status()?;

                    let bytes = response.bytes().await?;

                    raw_tx
                        .send(BinanceMdMsg::Snapshot(SnapshotMsg {
                            symbol,
                            payload: RawMdMsg(bytes.to_vec()),
                        }))
                        .await?;

                    Ok(())
                }
            })
            .await?;
    }
    Ok(())
}