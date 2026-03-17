use std::{collections::HashMap, error::Error, sync::Arc};

use futures_util::StreamExt;
use md_core::{connector_trait::{ConnectionTasks, ExchangeConnector, WriteCommand}, events::{ControlEvent, InboundEvent, PingMsg}, logging::types::Component, types::Exchange};
use reqwest::Client;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info};
use url::Url;

use crate::types::{ApiResponse, BitgetMdMsg, BitgetUrls, SubscriptionRequest, SymbolParam};

pub struct BitgetConnector {
    manager_tx: Sender<ManagerCommand>,
    raw_tx: Sender<BitgetMdMsg>,
    inbound_rx: Receiver<InboundEvent>
}

enum ManagerCommand {
    InsertSubscription(u8, ConnectionTasks),
    RecreateWithSnapshots,
    RecreateFinished,
    Pong(PingMsg)
}

impl BitgetConnector {
    pub async fn new(urls: BitgetUrls, max_subscription_per_ws: usize, raw_tx: Sender<BitgetMdMsg>, control_rx: Receiver<ControlEvent>) -> Result<Self, Box<dyn Error + Send + Sync>> 
    where 
        Self: Sized
    {
        let (inbound_tx, inbound_rx) = channel::<InboundEvent>(4096);
        let ws_url = Arc::from(urls.ws);
        let subscriptions_payloads = Arc::new(BitgetConnector::build_subscriptions(&urls.exchange_info, max_subscription_per_ws).await?);

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

impl ExchangeConnector for BitgetConnector {
    type SubscriptionPayload = Message;

    async fn get_subscriptions_list(rest_url: &Url) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        let client = Client::new();
        let resp = client.get(rest_url.as_str())
                            .send().await?
                            .json::<ApiResponse>().await?;
        let mut list = Vec::new();
        for symbol in resp.data {
            if symbol.status == "online" {
                list.push(symbol.symbol);
            }
        }
        Ok(list)
    }

    async fn build_subscriptions(rest_url: &Url, max_subscription_per_ws: usize) -> Result<Vec<Message>, Box<dyn Error + Send + Sync>>{
        let mut list = BitgetConnector::get_subscriptions_list(rest_url).await?;
        let subscriptions_payloads_len = (list.len()/max_subscription_per_ws) + 1;
        let mut result = Vec::new();
        for i in 0..subscriptions_payloads_len {
            let mut params_len = max_subscription_per_ws;
            if i == subscriptions_payloads_len - 1 { 
                params_len = list.len();
            }
            let symbols: Vec<SymbolParam> = list
                .drain(..params_len)
                .map(|x| {
                    SymbolParam {
                        inst_type: String::from("SPOT"),
                        channel: String::from("books"),
                        inst_id: x
                    }
                })
                .collect();

            let sub_req = SubscriptionRequest {
                op: String::from("subscribe"),
                args: symbols
            };
            let json = serde_json::to_string(&sub_req).expect("Failed to serialize SubscribePayload to JSON");
            let message = Message::text(json);

            result.push( message );
        }
        Ok(result)
    }

    async fn subscribe_streams(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.manager_tx
            .send(ManagerCommand::RecreateWithSnapshots)
            .await
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
    }

    async fn pong(&self, msg: PingMsg) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.manager_tx
            .send(ManagerCommand::Pong(msg))
            .await
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
    }

    async fn start(&mut self) {
        if let Err(e) = self.subscribe_streams().await {
            error!(exchange = ?Exchange::Bitget, component = ?Component::Connector, error = ?e, "error while subscribing to streams");
            return;
        }
        while let Some(msg) = self.inbound_rx.recv().await {
            match msg {
                InboundEvent::WsMessage(payload) => {
                    if let Err(e) = self.raw_tx.send(BitgetMdMsg(payload)).await {
                        error!(exchange = ?Exchange::Bitget, component = ?Component::Connector, error = ?e, "error while sending the ws message");
                        continue;
                    }
                },
                InboundEvent::Ping(ping_msg) => {
                    if let Err(e) = self.pong(ping_msg).await {
                        error!(exchange = ?Exchange::Bitget, component = ?Component::Connector, error = ?e, "error while sending the pong command");
                        continue;
                    }
                },
                InboundEvent::ConnectionClosed => {
                    info!(exchange = ?Exchange::Bitget, component = ?Component::Connector, "connection close, reconnecting");
                    if let Err(e) = self.subscribe_streams().await {
                        error!(exchange = ?Exchange::Bitget, component = ?Component::Connector, error = ?e, "error while subscribing to streams");
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
                        error!(exchange = ?Exchange::Bitget, component = ?Component::Connector, error = ?e, "failed to send RecreateWithSnapshots command to manager");
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
                    info!(exchange = ?Exchange::Bitget, component = ?Component::Connector, "recreate already in progress, skipping");
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
            error!(exchange = ?Exchange::Bitget, component = ?Component::Connector, error = ?e, "error while sending the pong command");
        }
    } else {
        error!(exchange = ?Exchange::Bitget, component = ?Component::Connector, "pong requested for unknown connection");
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
                    error!(exchange = ?Exchange::Bitget, component = ?Component::Connector, error = ?e, "failed to notify recreate finished");
                }
                return;
            }
            Err(e) => {
                error!(exchange = ?Exchange::Bitget, component = ?Component::Connector, error = ?e, "error while recreating connections");
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
) -> Result<(), Box<dyn Error + Send + Sync>> {    
    for (i, message) in subscriptions_payloads.iter().enumerate() {
        let (writer_tx, writer_rx) = channel::<WriteCommand>(64);
        let (ws_stream, _) = connect_async(ws_url.as_str()).await?;
        let (write, read) = ws_stream.split();

        let reader_url = ws_url.clone();
        let writer_url = ws_url.clone();

        let reader_tx_clone = inbound_tx.clone();
        let ws_id = i as u8;
        let reader_handle = tokio::spawn(async move {
            BitgetConnector::reader_task(ws_id, writer_url, read, reader_tx_clone).await;
        });

        let writer_handle = tokio::spawn(async move {
            BitgetConnector::writer_task(reader_url, write, writer_rx).await;
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