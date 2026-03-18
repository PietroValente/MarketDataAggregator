use std::error::Error;
use std::sync::Arc;

use tokio::{net::TcpStream, task::JoinHandle};
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};
use futures_util::{stream::SplitSink, stream::SplitStream, SinkExt, StreamExt};
use tracing::{error, warn};
use url::Url;

use crate::events::{InboundEvent, PingMsg};
use crate::types::RawMdMsg;

pub enum WriteCommand {
    Raw(Message),
    Pong(Vec<u8>)
}

pub struct ConnectionTasks {
    pub reader_handle: JoinHandle<()>,
    pub writer_handle: JoinHandle<()>,
    pub writer_tx: Sender<WriteCommand>,
}

#[allow(async_fn_in_trait)]
pub trait ExchangeConnector {
    type SubscriptionPayload;

    async fn get_subscriptions_list(rest_url: &Url) -> Result<Vec<String>, Box<dyn Error + Send + Sync>>;
    async fn build_subscriptions(rest_url: &Url, max_subscription_per_ws: usize) -> Result<Vec<Self::SubscriptionPayload>, Box<dyn Error + Send + Sync>>;
    async fn subscribe_streams(&mut self) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn pong(&self, msg: PingMsg) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn start(&mut self);
    async fn reader_task(
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
    async fn writer_task(
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
}