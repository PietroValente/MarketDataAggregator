use std::collections::BTreeMap;

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
    pub location: Option<Location>,
    pub fields: BTreeMap<String, String>,
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

#[derive(Debug, Default)]
pub struct LogVisitor {
    pub message: Option<String>,
    pub exchange: Option<String>,
    pub component: Option<String>,
    pub instrument: Option<String>,
    pub fields: BTreeMap<String, String>,
}

impl LogVisitor {
    fn insert_field(&mut self, field: &Field, value: String) {
        let name = field.name().to_string();
        self.fields.insert(name.clone(), value.clone());
        match field.name() {
            "message" => self.message = Some(value),
            "exchange" => self.exchange = Some(value),
            "component" => self.component = Some(value),
            "instrument" => self.instrument = Some(value),
            _ => {}
        }
    }
}

impl Visit for LogVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.insert_field(field, format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.insert_field(field, value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.insert_field(field, value.to_string());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.insert_field(field, value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.insert_field(field, value.to_string());
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        self.insert_field(field, value.to_string());
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        self.insert_field(field, value.to_string());
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.insert_field(field, value.to_string());
    }

    fn record_error(
        &mut self,
        field: &Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        self.insert_field(field, value.to_string());
    }
}