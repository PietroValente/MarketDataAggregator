use std::{collections::HashMap, error::Error, sync::Arc};

use futures_util::StreamExt;
use md_core::{events::{ControlEvent, InboundEvent, PingMsg}, helpers::connector::fetch_json, traits::connector::{ConnectionTasks, ExchangeConnector, WriteCommand}, types::{Exchange, Instrument}};
use reqwest::Client;
use tokio::{sync::mpsc::{channel, Receiver, Sender}, time::{sleep, Duration}};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info};
use url::Url;

use crate::types::{ApiResponse, ManagerCommand, OkxConnectorError, OkxMdMsg, OkxUrls, SubscriptionRequest, Subscriptions, SymbolParam};

pub struct OkxConnector {
    manager_tx: Sender<ManagerCommand>,
    raw_tx: Sender<OkxMdMsg>,
    inbound_rx: Receiver<InboundEvent>
}

impl OkxConnector {
    pub async fn new(client:Client, urls: OkxUrls, max_subscription_per_ws: usize, raw_tx: Sender<OkxMdMsg>, control_rx: Receiver<ControlEvent>) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> 
    where 
        Self: Sized
    {
        let (inbound_tx, inbound_rx) = channel::<InboundEvent>(4096);
        let ws_url = Arc::from(urls.ws);
        let subscriptions = OkxConnector::build_subscriptions(client, &urls.exchange_info, max_subscription_per_ws).await?;
        let subscriptions_payloads = Arc::new(subscriptions.messages);
        raw_tx
            .send(OkxMdMsg::Instruments(subscriptions.symbols))
            .await?;

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

impl ExchangeConnector for OkxConnector {
    type SubscriptionsInfo = Subscriptions;

    fn exchange() -> Exchange {
        Exchange::Okx
    }

    async fn get_subscriptions_list(client:Client, rest_url: &Url) -> Result<Vec<Instrument>, Box<dyn Error + Send + Sync + 'static>> {
        let resp  = fetch_json::<ApiResponse>( &client, rest_url, None).await?;
        
        let list: Vec<Instrument> = resp.data
            .into_iter()
            .filter(|s| s.state == "live")
            .map(|s| s.inst_id)
            .collect();

        Ok(list)
    }

    async fn build_subscriptions(client: Client, rest_url: &Url, max_subscription_per_ws: usize) -> Result<Subscriptions, Box<dyn Error + Send + Sync + 'static>> {
        if max_subscription_per_ws == 0 {
            return Err(OkxConnectorError::InvalidMaxSubscriptionPerWs.into());
        }
    
        let symbols = OkxConnector::get_subscriptions_list_backoff(client, rest_url).await;
        let mut messages = Vec::new();
    
        for (i, chunk) in symbols.chunks(max_subscription_per_ws).enumerate() {
            let args: Vec<SymbolParam> = chunk
                .iter()
                .cloned()
                .map(|inst_id| SymbolParam {
                    channel: String::from("books"),
                    inst_id,
                })
                .collect();
    
            let sub_req = SubscriptionRequest {
                id: i.to_string(),
                op: String::from("subscribe"),
                args,
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
            error!(exchange = ?OkxConnector::exchange(), component = ?OkxConnector::component(), error = ?e, "error while subscribing to streams");
            return;
        }
        while let Some(msg) = self.inbound_rx.recv().await {
            match msg {
                InboundEvent::WsMessage(payload) => {
                    if let Err(e) = self.raw_tx.send(OkxMdMsg::Raw(payload)).await {
                        error!(exchange = ?OkxConnector::exchange(), component = ?OkxConnector::component(), error = ?e, "error while sending the ws message");
                        continue;
                    }
                },
                InboundEvent::Ping(ping_msg) => {
                    if let Err(e) = self.pong(ping_msg).await {
                        error!(exchange = ?OkxConnector::exchange(), component = ?OkxConnector::component(), error = ?e, "error while sending the pong command");
                        continue;
                    }
                },
                InboundEvent::ConnectionClosed => {
                    info!(exchange = ?OkxConnector::exchange(), component = ?OkxConnector::component(), "connection close, reconnecting");
                    if let Err(e) = self.subscribe_streams().await {
                        error!(exchange = ?OkxConnector::exchange(), component = ?OkxConnector::component(), error = ?e, "error while subscribing to streams");
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
                        error!(exchange = ?OkxConnector::exchange(), component = ?OkxConnector::component(), error = ?e, "failed to send RecreateWithSnapshots command to manager");
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
                    info!(exchange = ?OkxConnector::exchange(), component = ?OkxConnector::component(), "recreate already in progress, skipping");
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
            error!(exchange = ?OkxConnector::exchange(), component = ?OkxConnector::component(), error = ?e, "error while sending the pong command");
        }
    } else {
        error!(exchange = ?OkxConnector::exchange(), component = ?OkxConnector::component(), "pong requested for unknown connection");
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
                    error!(exchange = ?OkxConnector::exchange(), component = ?OkxConnector::component(), error = ?e, "failed to notify recreate finished");
                }
                return;
            }
            Err(e) => {
                error!(exchange = ?OkxConnector::exchange(), component = ?OkxConnector::component(), error = ?e, "error while recreating connections");
                let delay = OkxConnector::retry_delay(&backoff_secs, attempt);
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
    if subscriptions_payloads.len() > OkxConnector::ws_id_capacity() {
        return Err(OkxConnectorError::TooManyWsBatchesForU8Id {
            batches: subscriptions_payloads.len(),
            max_supported: OkxConnector::ws_id_capacity(),
        }
        .into());
    }

    for (i, message) in subscriptions_payloads.iter().enumerate() {
        let (writer_tx, writer_rx) = channel::<WriteCommand>(64);
        let (ws_stream, _) = connect_async(ws_url.as_str()).await?;
        let (write, read) = ws_stream.split();

        let reader_url = ws_url.clone();
        let writer_url = ws_url.clone();

        let reader_tx_clone = inbound_tx.clone();
        let Some(ws_id) = OkxConnector::ws_id_from_index(i) else {
            return Err(OkxConnectorError::TooManyWsBatchesForU8Id {
                batches: subscriptions_payloads.len(),
                max_supported: OkxConnector::ws_id_capacity(),
            }
            .into());
        };
        let reader_handle = tokio::spawn(async move {
            OkxConnector::reader_task(ws_id, writer_url, read, reader_tx_clone).await;
        });

        let writer_handle = tokio::spawn(async move {
            OkxConnector::writer_task(reader_url, write, writer_rx).await;
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