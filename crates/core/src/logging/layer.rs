use tracing::{Event, Level, Subscriber};
use tracing_subscriber::{Layer, layer::Context};
use tokio::sync::mpsc::Sender;

use super::types::LogEvent;

pub struct DbLoggingLayer {
    sender: Sender<LogEvent>
}

impl DbLoggingLayer {
    pub fn new(sender: Sender<LogEvent>,) -> Self {
        Self {
            sender
        }
    }
}

impl<S> Layer<S> for DbLoggingLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();

        let log = LogEvent {
            level: *metadata.level(),
            target: metadata.target().to_string(),
            message: format!("{:?}", event),
            timestamp: std::time::SystemTime::now(),
        };

        if log.level != Level::TRACE && log.level != Level::DEBUG{
            // TODO: explain why try_send, we need really fast and light layer, we don't want to create heavy computation that may slow the main thread
            let _ = self.sender.try_send(log);
        }
    }
}