use tokio::sync::oneshot::Sender;

use crate::{book::{BookLevel, BookLevels}, types::{Exchange, ExchangeStatus, Instrument, RawMdMsg}};

#[derive(Debug)]
pub struct NormalizedBookData {
    pub instrument: Instrument,
    pub levels: BookLevels
}

#[derive(Debug)]
pub enum BookEventType {
    Snapshot,
    Update
}

#[derive(Debug)]
pub struct NormalizedTop {
    pub instrument: Instrument,
    pub n: usize
}

#[derive(Debug)]
pub enum NormalizedQuery {
    TopAsk(Sender<Vec<BookLevel>>, NormalizedTop),
    TopBid(Sender<Vec<BookLevel>>, NormalizedTop),
    GetStatus(Sender<ExchangeStatus>)
}

#[derive(Debug)]
pub enum NormalizedEvent {
    Query(NormalizedQuery),
    ApplyStatus(ExchangeStatus),
    GetStatus,
    Book(BookEventType, NormalizedBookData)
}

#[derive(Debug)]
pub struct EventEnvelope {
    pub exchange: Exchange,
    pub event: NormalizedEvent
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