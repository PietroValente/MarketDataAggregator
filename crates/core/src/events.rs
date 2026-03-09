use std::{sync::mpsc::Sender, time::Instant};

use crate::{book::{BookLevel, BookSnapshot, BookUpdate}, types::{Exchange, ExchangeStatus, Instrument}};

pub struct NormalizedSnapshot {
    pub instrument: Instrument,
    pub timestamp: Instant,
    pub data: BookSnapshot
}

pub struct NormalizedUpdate {
    pub instrument: Instrument,
    pub timestamp: Instant,
    pub data: BookUpdate
}

pub struct NormalizedTop {
    pub instrument: Instrument,
    pub n: usize,
    pub reply_to: Sender<Vec<BookLevel>>
}

pub enum NormalizedQuery {
    TopAsk(NormalizedTop),
    TopBid(NormalizedTop)
}

pub enum NormalizedEvent {
    Query(NormalizedQuery),
    Status(ExchangeStatus),
    Snapshot(NormalizedSnapshot),
    Update(NormalizedUpdate)
}

pub struct EventEnvelope {
    pub exchange: Exchange,
    pub event: NormalizedEvent
}

pub enum ControlEvent {
    Resync
}