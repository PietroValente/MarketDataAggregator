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

pub enum NormalizedEvent {
    Status(Exchange, ExchangeStatus),
    Snapshot(Exchange, NormalizedSnapshot),
    Update(Exchange, NormalizedUpdate)
}