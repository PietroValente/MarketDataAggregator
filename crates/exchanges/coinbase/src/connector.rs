use std::{collections::HashMap, error::Error, sync::Arc};

use bytes::BytesMut;
use futures_util::StreamExt;
use md_core::{connector_trait::{ConnectionTasks, ExchangeConnector, WriteCommand}, events::{ControlEvent, InboundEvent, PingMsg}, logging::types::Component, types::{Exchange, Instrument}};
use reqwest::Client;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info};
use url::Url;

use crate::types::{ApiResponse, CoinbaseMdMsg, CoinbaseUrls, SubscriptionMsg, Subscriptions, SymbolParam};

pub struct CoinbaseConnector {
    manager_tx: Sender<ManagerCommand>,
    raw_tx: Sender<CoinbaseMdMsg>,
    inbound_rx: Receiver<InboundEvent>
}

enum ManagerCommand {
    InsertSubscription(u8, ConnectionTasks),
    RecreateWithSnapshots,
    RecreateFinished,
    Pong(PingMsg)
}

impl CoinbaseConnector {
    pub async fn new(client:Client, urls: CoinbaseUrls, max_subscription_per_ws: usize, raw_tx: Sender<CoinbaseMdMsg>, control_rx: Receiver<ControlEvent>) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> 
    where 
        Self: Sized
    {
        let (inbound_tx, inbound_rx) = channel::<InboundEvent>(4096);
        let ws_url = Arc::from(urls.ws);
        let subscriptions = CoinbaseConnector::build_subscriptions(client, &urls.exchange_info, max_subscription_per_ws).await?;
        let subscriptions_payloads = Arc::new(subscriptions.messages);
        // raw_tx
        //     .send(BitgetMdMsg::Instruments(subscriptions.symbols))
        //     .await?;

        let (manager_tx, manager_rx) = channel::<ManagerCommand>(128);

        tokio::spawn(connection_manager_task(
            ws_url.clone(),
            subscriptions_payloads,
            inbound_tx,
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
            manager_tx,
        })
    }
}

impl ExchangeConnector for CoinbaseConnector {
    type SubscriptionsInfo = Subscriptions;

    fn exchange() -> Exchange {
        Exchange::Coinbase
    }

    async fn get_subscriptions_list(client:Client, rest_url: &Url) -> Result<Vec<Instrument>, Box<dyn Error + Send + Sync + 'static>> {
        let resp = client
            .get(rest_url.as_str())
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
        for symbol in resp.products {
            if symbol.status == "online" && !symbol.product_id.contains("USDC") {
                list.push(symbol.product_id);
            }
        }
        Ok(list)
    }

    async fn build_subscriptions(
        client: Client,
        rest_url: &Url,
        max_subscription_per_ws: usize,
    ) -> Result<Subscriptions, Box<dyn Error + Send + Sync + 'static>> {
        if max_subscription_per_ws == 0 {
            return Err("max_subscription_per_ws cannot be 0".into());
        }
    
        let symbols = CoinbaseConnector::get_subscriptions_list_backoff(client, rest_url).await;
        let mut messages = Vec::new();
    
        for chunk in symbols.chunks(max_subscription_per_ws) {
            let product_ids: Vec<Instrument> = chunk.to_vec();
    
            let sub_req = SubscriptionMsg {
                op_type: String::from("subscribe"),
                channels: vec![SymbolParam {
                    name: String::from("level2_50"),
                    product_ids,
                }],
            };
    
            let json = serde_json::to_string(&sub_req)?;
            let message = Message::text(json);
    
            messages.push(message);
        }
    
        Ok(Subscriptions { symbols, messages })
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
            error!(exchange = ?Exchange::Coinbase, component = ?Component::Connector, error = ?e, "error while subscribing to streams");
            return;
        }
        while let Some(msg) = self.inbound_rx.recv().await {
            match msg {
                InboundEvent::WsMessage(payload) => {
                    if let Err(e) = self.raw_tx.send(CoinbaseMdMsg(payload)).await {
                        error!(exchange = ?Exchange::Coinbase, component = ?Component::Connector, error = ?e, "error while sending the ws message");
                        continue;
                    }
                },
                InboundEvent::Ping(ping_msg) => {
                    if let Err(e) = self.pong(ping_msg).await {
                        error!(exchange = ?Exchange::Coinbase, component = ?Component::Connector, error = ?e, "error while sending the pong command");
                        continue;
                    }
                },
                InboundEvent::ConnectionClosed => {
                    info!(exchange = ?Exchange::Coinbase, component = ?Component::Connector, "connection close, reconnecting");
                    if let Err(e) = self.subscribe_streams().await {
                        error!(exchange = ?Exchange::Coinbase, component = ?Component::Connector, error = ?e, "error while subscribing to streams");
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
                        error!(exchange = ?Exchange::Coinbase, component = ?Component::Connector, error = ?e, "failed to send RecreateWithSnapshots command to manager");
                    }
            }
        }
    }
}

async fn connection_manager_task(
    ws_url: Arc<Url>,
    subscriptions_payloads: Arc<Vec<Message>>,
    inbound_tx: Sender<InboundEvent>,
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
                    info!(exchange = ?Exchange::Coinbase, component = ?Component::Connector, "recreate already in progress, skipping");
                    continue;
                }

                recreate_in_progress = true;
                abort_all_connections(&mut connections);

                tokio::spawn(recreate_with_snapshots_backoff(
                    ws_url.clone(),
                    subscriptions_payloads.clone(),
                    inbound_tx.clone(),
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
            error!(exchange = ?Exchange::Coinbase, component = ?Component::Connector, error = ?e, "error while sending the pong command");
        }
    } else {
        error!(exchange = ?Exchange::Coinbase, component = ?Component::Connector, "pong requested for unknown connection");
    }
}

async fn recreate_with_snapshots_backoff(
    ws_url: Arc<Url>,
    subscriptions_payloads: Arc<Vec<Message>>,
    inbound_tx: Sender<InboundEvent>,
    cmd_tx: Sender<ManagerCommand>,
) {
    let backoff_secs = [1, 5, 15, 30, 60];
    let mut attempt: usize = 0;

    loop {
        match recreate_with_snapshots(
            ws_url.clone(),
            subscriptions_payloads.clone(),
            inbound_tx.clone(),
            cmd_tx.clone()
        ).await {
            Ok(()) => {
                if let Err(e) = cmd_tx.send(ManagerCommand::RecreateFinished).await {
                    error!(exchange = ?Exchange::Coinbase, component = ?Component::Connector, error = ?e, "failed to notify recreate finished");
                }
                return;
            }
            Err(e) => {
                error!(exchange = ?Exchange::Coinbase, component = ?Component::Connector, error = ?e, "error while recreating connections");
                let delay = *backoff_secs
                    .get(attempt)
                    .unwrap_or(backoff_secs.last().unwrap());
                attempt = attempt.saturating_add(1);
                sleep(Duration::from_secs(delay)).await; // After the last stage we keep retrying every 60s
            }
        }
    }
}

async fn recreate_with_snapshots(
    ws_url: Arc<Url>,
    subscriptions_payloads: Arc<Vec<Message>>,
    inbound_tx: Sender<InboundEvent>,
    cmd_tx: Sender<ManagerCommand>,
) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {    
    for (i, message) in subscriptions_payloads.iter().enumerate() {
        let (writer_tx, writer_rx) = channel::<WriteCommand>(64);
        let (ws_stream, _) = connect_async(ws_url.as_str()).await?;
        let (write, read) = ws_stream.split();

        let reader_url = ws_url.clone();
        let writer_url = ws_url.clone();

        let reader_tx_clone = inbound_tx.clone();
        let ws_id = i as u8;
        let reader_handle = tokio::spawn(async move {
            CoinbaseConnector::reader_task(ws_id, writer_url, read, reader_tx_clone).await;
        });

        let writer_handle = tokio::spawn(async move {
            CoinbaseConnector::writer_task(reader_url, write, writer_rx).await;
        });

        cmd_tx.send( ManagerCommand::InsertSubscription(ws_id, ConnectionTasks {
            reader_handle,
            writer_handle,
            writer_tx: writer_tx.clone(),
        })).await?;
        writer_tx
            .send(WriteCommand::Raw(message.clone()))
            .await?;
    }
    Ok(())
}