use std::time::Instant;

use crate::{book::{BookSnapshot, BookUpdate}, types::{Exchange, ExchangeStatus, Instrument}};

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
    pub n: usize
}

pub enum NormalizedEvent {
    TopAsk(Exchange, NormalizedTop),
    TopBid(Exchange, NormalizedTop),
    Status(Exchange, ExchangeStatus),
    Snapshot(Exchange, NormalizedSnapshot),
    Update(Exchange, NormalizedUpdate)
}