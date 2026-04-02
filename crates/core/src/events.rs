use crate::{book::BookLevels, query::EngineQuery, types::{Exchange, ExchangeStatus, Instrument, RawMdMsg}};

#[derive(Debug)]
pub struct NormalizedBookData {
    pub instrument: Instrument,
    pub levels: BookLevels,
    pub checksum: Option<i32>
}

#[derive(Debug)]
pub enum BookEventType {
    Snapshot,
    Update
}

pub enum EngineMessage {
    Apply(EventEnvelope),
    Query(EngineQuery),
}

#[derive(Debug)]
pub struct EventEnvelope {
    pub exchange: Exchange,
    pub event: NormalizedEvent
}

#[derive(Debug)]
pub enum NormalizedEvent {
    ApplyStatus(ExchangeStatus),
    Book(BookEventType, NormalizedBookData),
}

pub enum ControlEvent {
    Resync
}

#[derive(Debug)]
pub struct PingMsg {
    pub ws_id: u8,
    pub payload: Vec<u8>
}

#[derive(Debug)]
pub enum InboundEvent {
    WsMessage(RawMdMsg),
    Ping(PingMsg),
    ClearBookState,
    ConnectionClosed
}