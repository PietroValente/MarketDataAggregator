use scylla::value::CqlTimeuuid;
use tracing::field::{Field, Visit};

#[derive(Debug)]
pub struct Location {
    pub file: &'static str,
    pub line: u32
}

#[derive(Debug)]
pub struct LogGeneric {
    pub level: tracing::Level,
    pub timestamp: CqlTimeuuid,
    pub message: Option<String>,
    pub target: &'static str,
    pub location: Option<Location>
}

#[derive(Debug)]
pub struct LogComponentExchange {
    pub component: String,
    pub exchange: String,
    pub generic: LogGeneric
}

#[derive(Debug)]
pub struct LogInstrument {
    pub instrument: String,
    pub component_exchange: LogComponentExchange
}

#[derive(Debug)]
pub enum LogEvent {
    Generic(LogGeneric),
    ComponentExchange(LogComponentExchange),
    Instrument(LogInstrument)
}

#[derive(Debug)]
pub enum Component {
    Connector,
    Adapter,
    Engine,
    ExchangeState
}

#[derive(Default)]
pub struct LogVisitor {
    pub message: Option<String>,
    pub exchange: Option<String>,
    pub component: Option<String>,
    pub instrument: Option<String>,
}

impl Visit for LogVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "message" => self.message = Some(value.to_string()),
            "exchange" => self.exchange = Some(value.to_string()),
            "component" => self.component = Some(value.to_string()),
            "instrument" => self.instrument = Some(value.to_string()),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "message" => self.message = Some(format!("{value:?}")),
            "exchange" => self.exchange = Some(format!("{value:?}")),
            "component" => self.component = Some(format!("{value:?}")),
            "instrument" => self.instrument = Some(format!("{value:?}")),
            _ => {}
        }
    }
}