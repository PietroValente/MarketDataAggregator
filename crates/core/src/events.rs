use std::sync::mpsc::Sender;
use crate::{book::{BookLevel, BookSnapshot, BookUpdate}, types::{Exchange, ExchangeStatus, Instrument, RawMdMsg}};

#[derive(Debug)]
pub struct NormalizedSnapshot {
    pub instrument: Instrument,
    pub data: BookSnapshot
}

#[derive(Debug)]
pub struct NormalizedUpdate {
    pub instrument: Instrument,
    pub data: BookUpdate
}

#[derive(Debug)]
pub struct NormalizedTop {
    pub instrument: Instrument,
    pub n: usize,
    pub reply_to: Sender<Vec<BookLevel>>
}

#[derive(Debug)]
pub enum NormalizedQuery {
    TopAsk(NormalizedTop),
    TopBid(NormalizedTop)
}

#[derive(Debug)]
pub enum NormalizedEvent {
    Query(NormalizedQuery),
    Status(ExchangeStatus),
    Snapshot(NormalizedSnapshot),
    Update(NormalizedUpdate)
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
pub enum InboundEvent {
    WsMessage(RawMdMsg),
    Ping(Vec<u8>),
    ConnectionClosed
}