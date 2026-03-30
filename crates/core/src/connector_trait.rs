use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;
use tokio::time::sleep;
use tokio::{net::TcpStream, task::JoinHandle};
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};
use futures_util::{stream::SplitSink, stream::SplitStream, SinkExt, StreamExt};
use tracing::{error, warn};
use url::Url;

use crate::events::{InboundEvent, PingMsg};
use crate::logging::types::Component;
use crate::types::{Exchange, Instrument, RawMdMsg};

/// Commands that can be issued to a connection writer task.
pub enum WriteCommand {
    Raw(Message),
    Pong(Vec<u8>)
}

/// Handles associated with a single websocket connection.
pub struct ConnectionTasks {
    pub reader_handle: JoinHandle<()>,
    pub writer_handle: JoinHandle<()>,
    pub writer_tx: Sender<WriteCommand>,
}

/// Common interface for exchange-specific websocket connectors.
///
/// A connector is responsible for:
/// - building subscription payloads for the exchange,
/// - establishing and maintaining websocket connections,
/// - routing raw messages and ping/pong events into the unified event channel.
#[allow(async_fn_in_trait)]
pub trait ExchangeConnector {
    /// Type used to represent a single subscription payload for this exchange.
    type SubscriptionsInfo;

    /// Exchange identifier for logging/telemetry.
    fn exchange() -> Exchange;

    /// Build websocket subscription payloads, splitting the symbol list across multiple connections if needed.
    async fn build_subscriptions(client: Client, rest_url: &Url, max_subscription_per_ws: usize) -> Result<Self::SubscriptionsInfo, Box<dyn Error + Send + Sync + 'static>>;

    /// (Re)create websocket streams and start listening for market data.
    async fn subscribe_streams(&mut self) -> Result<(), Box<dyn Error + Send + Sync + 'static>>;

    /// Send a pong response for an inbound ping coming from a specific websocket.
    async fn pong(&self, msg: PingMsg) -> Result<(), Box<dyn Error + Send + Sync + 'static>>;

    /// Run the connector event loop (typically until the inbound channel is closed).
    async fn start(&mut self);

    /// Fetch the list of symbols/instruments to subscribe to via REST.
    async fn get_subscriptions_list(client: Client, rest_url: &Url,) -> Result<Vec<Instrument>, Box<dyn Error + Send + Sync + 'static>>;

    /// Maximum number of websocket connections addressable by `u8` IDs.
    fn ws_id_capacity() -> usize {
        usize::from(u8::MAX) + 1
    }

    /// Convert a connection index into a websocket ID.
    fn ws_id_from_index(index: usize) -> Option<u8> {
        u8::try_from(index).ok()
    }

    /// Pick backoff delay for current attempt, capping to last stage.
    fn retry_delay(backoff_secs: &[u64], attempt: usize) -> u64 {
        *backoff_secs
            .get(attempt)
            .unwrap_or(backoff_secs.last().unwrap_or(&1))
    }

    /// Backoff wrapper around `get_subscriptions_list`.
    ///
    /// Useful when the REST call can fail transiently or take too long due to unstable connectivity.
    /// Retries until success with an exponential backoff capped to the last value (60s).
    async fn get_subscriptions_list_backoff(
        client: Client,
        rest_url: &Url
    ) -> Vec<Instrument> {
        let backoff_secs = [1, 5, 15, 30, 60];
        let mut attempt: usize = 0;
    
        loop {
            match Self::get_subscriptions_list(client.clone(), rest_url).await {
                Ok(subscriptions) => {
                    return subscriptions;
                }
                Err(e) => {
                    error!(exchange = ?Self::exchange(), component = ?Component::Connector, error = ?e, "error while building subscriptions");
                    let delay = Self::retry_delay(&backoff_secs, attempt);
                    attempt = attempt.saturating_add(1);
                    sleep(Duration::from_secs(delay)).await; // After the last stage we keep retrying every 60s
                }
            }
        }
    }

    /// Default websocket reader task shared across exchanges.
    ///
    /// It forwards binary/text frames as `RawMdMsg`, pings as `Ping`, and close/errors
    /// as `ConnectionClosed` into the unified event channel.
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

    /// Default websocket writer task shared across exchanges.
    ///
    /// It consumes `WriteCommand`s from an internal channel and forwards them
    /// to the websocket sink.
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