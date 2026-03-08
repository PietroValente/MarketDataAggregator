use md_core::{book::{BookLevel, LocalBook, LocalBookError}, events::{NormalizedSnapshot, NormalizedTop, NormalizedUpdate}, types::{Exchange, ExchangeStatus, Instrument}};
use tracing::{debug, info, warn};
use std::{collections::HashMap, time::Instant};
use thiserror::Error;

pub struct ExchangeState {
    exchange: Exchange, //keep for log
    last_snapshot_at: Instant,
    last_update_at: Instant,
    status: ExchangeStatus,
    markets: HashMap<Instrument, LocalBook>,
}

impl ExchangeState {
    pub fn new(e: Exchange) -> Self {
        Self {
            exchange: e,
            last_snapshot_at: Instant::now(),
            last_update_at: Instant::now(),
            status: ExchangeStatus::Initializing,
            markets: HashMap::new()
        }
    }

    pub fn apply_snapshot(&mut self, snapshot: NormalizedSnapshot) {
        debug!(exchange = ?self.exchange, instrument = ?snapshot.instrument, last_snapshot_at = ?self.last_snapshot_at, new_snapshot_at = ?snapshot.timestamp, "applying snapshot");
        self.last_snapshot_at = snapshot.timestamp;
        self.markets.entry(snapshot.instrument).or_insert(LocalBook::new()).apply_snapshot(snapshot.data);
    }

    pub fn apply_update(&mut self, update: NormalizedUpdate) -> Result<(), ExchangeStateError> {
        let delta_time = update.timestamp-self.last_update_at;
        debug!(exchange = ?self.exchange, instrument = ?update.instrument, last_update_at = ?self.last_update_at, new_update_at = ?update.timestamp, delta_time = ?delta_time, "applying update");
        let now = Instant::now();
        let staleness = update.timestamp - now;
        if staleness.as_millis() > 500 {
            warn!(exchange = ?self.exchange, instrument = ?update.instrument, new_update_at = ?update.timestamp, now = ?now,  staleness = ?staleness, "critical staleness")
        }

        self.last_update_at = update.timestamp;
        self.markets.get_mut(&update.instrument)
            .ok_or_else(|| ExchangeStateError::InstrumentNotFound(update.instrument))?
            .apply_update(update.data)?;
        Ok(())    
    }

    pub fn apply_status(&mut self, status: ExchangeStatus) {
        info!(exchange = ?self.exchange, status = ?status, "applying status");
        self.status = status;
    }

    pub fn get_status(&self) -> &ExchangeStatus {
        &self.status
    }

    pub fn top_n_ask(&self, top: NormalizedTop) -> Result<Vec<BookLevel>, ExchangeStateError> {
        let local_book = self.markets.get(&top.instrument).ok_or_else(|| ExchangeStateError::InstrumentNotFound(top.instrument.clone()))?;
        debug!(exchange = ?self.exchange, instrument = ?top.instrument, n = top.n, ask_len = local_book.ask_len(),  "top_n_ask");
        Ok(local_book.top_n_ask(top.n))
    }

    pub fn top_n_bid(&self, top: NormalizedTop) -> Result<Vec<BookLevel>, ExchangeStateError> {
        let local_book = self.markets.get(&top.instrument).ok_or_else(|| ExchangeStateError::InstrumentNotFound(top.instrument.clone()))?;
        debug!(exchange = ?self.exchange, instrument = ?top.instrument, n = top.n, bid_len = local_book.bid_len(),  "top_n_bid");
        Ok(local_book.top_n_bid(top.n))
    }
}

#[derive(Error, Debug)]
pub enum ExchangeStateError {
    #[error("The exchange does not track this instrument: {0}")]
    InstrumentNotFound(Instrument),
    #[error("LocalBookError")]
    LocalBookError(#[from] LocalBookError)
}