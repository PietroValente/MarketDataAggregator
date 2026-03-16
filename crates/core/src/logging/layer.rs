use scylla::value::CqlTimeuuid;
use tokio::sync::mpsc::Sender;
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::{layer::Context, Layer};
use uuid::Uuid;

use super::types::{
    Location, LogComponentExchange, LogEvent, LogGeneric, LogInstrument, LogVisitor
};

pub struct DbLoggingLayer {
    sender: Sender<LogEvent>,
}

impl DbLoggingLayer {
    pub fn new(sender: Sender<LogEvent>) -> Self {
        Self { sender }
    }
}

impl<S> Layer<S> for DbLoggingLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();

        if *metadata.level() == Level::TRACE {
            return; //discard because is not a use case I'm interested
        }

        let mut visitor = LogVisitor::default();
        event.record(&mut visitor);

        let location = match (metadata.file(), metadata.line()) {
            (Some(file), Some(line)) => Some(Location { file, line }),
            _ => None,
        };

        let ts = CqlTimeuuid::from(Uuid::now_v1(&[1,2,3,4,5,6]));

        let generic = LogGeneric {
            level: *metadata.level(),
            timestamp: ts,
            message: visitor.message,
            target: metadata.target(),
            location,
        };

        let log = match (visitor.component, visitor.exchange, visitor.instrument) {
            (Some(component), Some(exchange), Some(instrument)) => {
                LogEvent::Instrument(LogInstrument {
                    instrument,
                    component_exchange: LogComponentExchange {
                        component,
                        exchange,
                        generic,
                    },
                })
            }
            (Some(component), Some(exchange), None) => {
                LogEvent::ComponentExchange(LogComponentExchange {
                    component,
                    exchange,
                    generic,
                })
            }
            _ => LogEvent::Generic(generic),
        };

        // Non-blocking send: if the channel is full we drop the log
        // rather than slowing down the main execution path.
        let _ = self.sender.try_send(log);
    }
}