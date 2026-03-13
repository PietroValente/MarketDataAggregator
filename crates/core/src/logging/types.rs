#[derive(Debug)]
pub struct LogEvent {
    pub level: tracing::Level,
    pub target: String,
    pub message: String,
    pub timestamp: std::time::SystemTime,
}

#[derive(Debug)]
pub enum Component {
    Connector,
    Parser,
    Engine,
    ExchangeState
}