use std::time::Duration;

use thiserror::Error;
use tokio::{sync::mpsc::Sender, task::JoinHandle};
use tokio_tungstenite::tungstenite::Message;

use crate::events::PingMsg;

/* COMMON TYPES TO ALL CONNECTORS */

pub const BACKOFF_SECS: [Duration; 5] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
    Duration::from_secs(16),
];

pub const WS_CONNECT_TIMEOUT_SECS: Duration = Duration::from_secs(10);
pub const INACTIVITY_TIMEOUT_SECS: Duration = Duration::from_secs(4);
pub const POLL_INTERVAL_SECS: Duration = Duration::from_millis(50);

pub enum WriteCommand {
    Raw(Message),
    Pong(Vec<u8>),
}

pub struct ConnectionTasks {
    pub reader_handle: JoinHandle<()>,
    pub writer_handle: JoinHandle<()>,
    pub writer_tx: Sender<WriteCommand>,
}

pub enum ManagerCommand {
    AbortAllConnections,
    InsertSubscription(u8, ConnectionTasks),
    RecreateWithSnapshots,
    RecreateFinished,
    Pong(PingMsg),
}

#[derive(Debug, Error)]
pub enum ConnectorError {
    #[error("max_subscription_per_ws cannot be 0")]
    InvalidMaxSubscriptionPerWs,

    #[error("too many websocket batches for u8 ws_id: got {batches}, max {max_supported}")]
    TooManyWsBatchesForU8Id {
        batches: usize,
        max_supported: usize,
    },
    #[error("websocket connect timeout")]
    WebSocketConnectTimeout,
}
