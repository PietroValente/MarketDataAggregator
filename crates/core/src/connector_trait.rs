use bytes::Bytes;
use tokio::{net::TcpStream, task::JoinHandle};
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};
use futures_util::{stream::SplitSink, stream::SplitStream, SinkExt, StreamExt};
use tracing::{error, warn};
use url::Url;

use crate::events::InboundEvent;
use crate::types::RawMdMsg;

pub enum WriteCommand {
    Raw(Message),
    Ping
}

// specifico avra' Vec<ConnectionTask>
pub struct ConnectionTasks {
    pub reader_handle: JoinHandle<()>,
    pub writer_handle: JoinHandle<()>,
    pub writer_tx: Sender<WriteCommand>,
}

#[allow(async_fn_in_trait)]
pub trait ExchangeConnector {
    fn new(rest_url: Url, ws_url: Url, max_subscription_per_ws: usize) -> Self; //call build_subscription inside
    fn build_subscriptions(rest_url: &Url) -> Vec<Message>; //create Vec<Message> ready to be sent
    fn subscribe_streams(&mut self); //subscribe to all messages, creates one reader_task/writer_task for each ws creato, salva tutti i reader_task/writer_task handler in un vettore
    fn abort_streams(&mut self); //abort all readers and writers through handlers.abort()
    fn start(&mut self); // is the only func the user calls, at the beginning calls subscribe_streams, inside loop receiver on InboundEvents
    fn ping_all(&self);
    async fn reader_task(
        ws_url: &Url,
        mut stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        unified_tx: Sender<InboundEvent>,
    ) {
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(msg) => {
                    match msg {
                        Message::Binary(payload) => {
                            if let Err(e) = unified_tx.send(InboundEvent::WsMessage(RawMdMsg{payload})).await {
                                error!(url = ?ws_url, error = ?e, "unified transmitter error");
                                break;
                            }
                        },
                        Message::Text(text) => {
                            let payload = Bytes::from(text);
                            if let Err(e) = unified_tx.send(InboundEvent::WsMessage(RawMdMsg{payload})).await {
                                error!(url = ?ws_url, error = ?e, "unified transmitter error");
                                break;
                            }
                        },
                        Message::Ping(_) => {
                            if let Err(e) = unified_tx.send(InboundEvent::Ping).await {
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
        ws_url: &Url,
        mut sink: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        mut rx: tokio::sync::mpsc::Receiver<WriteCommand>,
    ) {
        while let Some(cmd) = rx.recv().await {
            let result = match cmd {
                WriteCommand::Ping => sink.send(Message::Ping(Vec::new().into())).await,
                WriteCommand::Raw(msg) => sink.send(msg).await
            };
    
            if let Err(e) = result {
                error!(url = ?ws_url, error = ?e, "writer task failed");
                break;
            }
        }
    }
}