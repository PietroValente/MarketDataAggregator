use std::{collections::HashMap, sync::Arc, time::Duration};

use futures_util::{stream::{SplitSink, SplitStream}, SinkExt, StreamExt};
use tokio::{net::TcpStream, sync::mpsc::{Receiver, Sender}};
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::{error, info, warn};
use url::Url;

use crate::{events::{ControlEvent, InboundEvent, PingMsg}, helpers::connector::{abort_all_connections, pong_ws, retry_with_backoff}, traits::connector::ExchangeConnector};
use crate::types::RawMdMsg;
use crate::connector::types::WriteCommand;

use super::types::{ConnectionTasks, ManagerCommand, BACKOFF_SECS};

/* COMMON CONNECTOR TASKS (shared runtime building blocks) */

/// Default websocket reader.
///
/// It forwards binary/text frames as `RawMdMsg`, pings as `Ping`, and close/errors
/// as `ConnectionClosed` into the unified event channel.
pub async fn reader_task(
    ws_id: u8,
    ws_url: Arc<Url>,
    mut stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    unified_tx: Sender<InboundEvent>,
) {
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(msg) => {
                match msg {
                    Message::Binary(payload) => {
                        if let Err(e) = unified_tx.send(InboundEvent::WsMessage(RawMdMsg(payload))).await {
                            error!(url = ?ws_url, error = ?e, "unified transmitter error");
                            break;
                        }
                    },
                    Message::Text(text) => {
                        let payload = text.into_bytes();
                        if let Err(e) = unified_tx.send(InboundEvent::WsMessage(RawMdMsg(payload))).await {
                            error!(url = ?ws_url, error = ?e, "unified transmitter error");
                            break;
                        }
                    },
                    Message::Ping(payload) => {
                        let ping_msg = PingMsg {
                            ws_id,
                            payload
                        };
                        if let Err(e) = unified_tx.send(InboundEvent::Ping(ping_msg)).await {
                            error!(url = ?ws_url, error = ?e, "unified transmitter error");
                            break;
                        }
                    },
                    Message::Close(_) => {
                        if let Err(e) = unified_tx.send(InboundEvent::ConnectionClosed).await {
                            error!(url = ?ws_url, error = ?e, "unified transmitter error");
                            break;
                        }
                    }
                    other => {
                        warn!(url= ?ws_url, message = ?other, "reader message not process");
                    }
                }
            }
            Err(e) => {
                error!(url = ?ws_url, error = ?e, "stream error");
                if let Err(e) = unified_tx.send(InboundEvent::ConnectionClosed).await {
                    error!(url = ?ws_url, error = ?e, "unified transmitter error");
                    break;
                }
                break;
            }
        }
    }
}

/// Default websocket writer.
///
/// It consumes `WriteCommand`s from an internal channel and forwards them
/// to the websocket sink.
pub async fn writer_task(
    ws_url: Arc<Url>,
    mut sink: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    mut rx: tokio::sync::mpsc::Receiver<WriteCommand>,
) {
    while let Some(cmd) = rx.recv().await {
        let result = match cmd {
            WriteCommand::Pong(payload) => sink.send(Message::Pong(payload)).await,
            WriteCommand::Raw(msg) => sink.send(msg).await
        };

        if let Err(e) = result {
            error!(url = ?ws_url, error = ?e, "writer task failed");
            break;
        }
    }
}

/// Control manager task.
///
/// Listens for high-level control signals (e.g. resync requests) coming from
/// connectors and translates them into `ManagerCommand`s for the manager loop.
/// This acts as a lightweight bridge between runtime events and orchestration logic.
pub async fn control_manager_task<T>(mut control_rx: Receiver<ControlEvent>, manager_tx: Sender<ManagerCommand>)
where
    T: ExchangeConnector
{
    while let Some(event) = control_rx.recv().await {
        match event {
            ControlEvent::Resync => {
                if let Err(e) = manager_tx
                    .send(ManagerCommand::RecreateWithSnapshots)
                    .await {
                        error!(exchange = ?T::exchange(), component = ?T::component(), error = ?e, "failed to send RecreateWithSnapshots command to manager");
                    }
            }
        }
    }
}

/// Connection manager task.
///
/// Owns the lifecycle of all websocket connections for a given exchange.
/// It maintains active connections, handles control commands (insert, pong),
/// and orchestrates full reconnections via snapshot-based recovery.
/// Recreate operations are executed in a background task with retry + backoff,
/// ensuring only one rebuild is in progress at a time.
pub async fn connection_manager_task<T, S, Raw, Recreate, Fut>(
    ws_url: Arc<Url>,
    snapshot_url: Option<Arc<Url>>,
    subscriptions_payloads: Arc<S>,
    inbound_tx: Sender<InboundEvent>,
    raw_tx: Option<Sender<Raw>>,
    cmd_tx: Sender<ManagerCommand>,
    mut cmd_rx: Receiver<ManagerCommand>,
    recreate: Recreate,
) where
    T: ExchangeConnector,
    S: Send + Sync + 'static,
    Raw: Send + 'static,
    Recreate: Fn(
            Arc<Url>,
            Option<Arc<Url>>,
            Arc<S>,
            Sender<InboundEvent>,
            Option<Sender<Raw>>,
            Sender<ManagerCommand>,
        ) -> Fut
        + Send
        + Sync
        + Clone
        + 'static,
    Fut: Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'static,
{
    let mut connections: HashMap<u8, ConnectionTasks> = HashMap::new();
    let mut recreate_in_progress = false;

    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            ManagerCommand::RecreateFinished => {
                recreate_in_progress = false;
            }

            ManagerCommand::InsertSubscription(ws_id, connection) => {
                connections.insert(ws_id, connection);
            }

            ManagerCommand::Pong(msg) => {
                pong_ws::<T>(&connections, msg).await;
            }

            ManagerCommand::RecreateWithSnapshots => {
                if recreate_in_progress {
                    info!(
                        exchange = ?T::exchange(),
                        component = ?T::component(),
                        "recreate already in progress, skipping"
                    );
                    continue;
                }
            
                recreate_in_progress = true;
                abort_all_connections(&mut connections);
            
                let recreate_fn = recreate.clone();
                let cmd_tx_done = cmd_tx.clone();
            
                let recreate_op = {
                    let ws_url = ws_url.clone();
                    let snapshot_url = snapshot_url.clone();
                    let subscriptions_payloads = subscriptions_payloads.clone();
                    let inbound_tx = inbound_tx.clone();
                    let raw_tx = raw_tx.clone();
                    let cmd_tx = cmd_tx.clone();
            
                    move || {
                        recreate_fn(
                            ws_url.clone(),
                            snapshot_url.clone(),
                            subscriptions_payloads.clone(),
                            inbound_tx.clone(),
                            raw_tx.clone(),
                            cmd_tx.clone(),
                        )
                    }
                };
            
                tokio::spawn(async move {
                    retry_with_backoff(
                        &BACKOFF_SECS,
                        recreate_op,
                        |e, attempt: usize, delay: Duration| {
                            error!(
                                exchange = ?T::exchange(),
                                component = ?T::component(),
                                error = ?e,
                                attempt = attempt,
                                delay = ?delay,
                                "error while recreating connections"
                            );
                        },
                    )
                    .await;
            
                    if let Err(e) = cmd_tx_done.send(ManagerCommand::RecreateFinished).await {
                        error!(
                            exchange = ?T::exchange(),
                            component = ?T::component(),
                            error = ?e,
                            "failed to notify recreate finished"
                        );
                    }
                });
            }
        }
    }
}