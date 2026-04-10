use std::time::Duration;

use thiserror::Error;
use tokio::{sync::mpsc::Sender, task::JoinHandle};
use tokio_tungstenite::tungstenite::Message;

use crate::events::PingMsg;

/* COMMON TYPES TO ALL CONNECTORS */

pub const BACKOFF_SECS: [Duration; 5] = [
    Duration::from_secs(1),
    Duration::from_secs(5),
    Duration::from_secs(15),
    Duration::from_secs(30),
    Duration::from_secs(60),
];

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
}
