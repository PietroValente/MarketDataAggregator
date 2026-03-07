use md_core::{book::{BookLevel, LocalBook, LocalBookError}, events::{NormalizedSnapshot, NormalizedTop, NormalizedUpdate}, types::{ExchangeStatus, Instrument}};
use std::{collections::HashMap, time::Instant};
use thiserror::Error;

pub struct ExchangeState {
    last_snapshot_at: Instant,
    last_update_at: Instant,
    status: ExchangeStatus,
    markets: HashMap<Instrument, LocalBook>,
}

impl ExchangeState {
    pub fn new() -> Self {
        Self {
            last_snapshot_at: Instant::now(),
            last_update_at: Instant::now(),
            status: ExchangeStatus::Initializing,
            markets: HashMap::new()
        }
    }

    pub fn apply_snapshot(&mut self, snapshot: NormalizedSnapshot) {
        // TODO: log the last snapshot and new one
        self.last_snapshot_at = snapshot.timestamp;
        self.markets.entry(snapshot.instrument).or_insert(LocalBook::new()).apply_snapshot(snapshot.data);
    }

    pub fn apply_update(&mut self, update: NormalizedUpdate) -> Result<(), ExchangeStateError> {
        // TODO: log if from last update passed more than X milliseconds
        self.last_update_at = update.timestamp;
        self.markets.get_mut(&update.instrument)
            .ok_or_else(|| ExchangeStateError::InstrumentNotFound(update.instrument))?
            .apply_update(update.data)?;
        Ok(())    
    }

    pub fn apply_status(&mut self, status: ExchangeStatus) {
        // TODO: log change of state
        self.status = status;
    }

    pub fn top_n_ask(&self, top: NormalizedTop) -> Result<Vec<BookLevel>, ExchangeStateError> {       
        Ok(self.markets.get(&top.instrument)
            .ok_or_else(|| ExchangeStateError::InstrumentNotFound(top.instrument))?
            .top_n_ask(top.n))
    }

    pub fn top_n_bid(&self, top: NormalizedTop) -> Result<Vec<BookLevel>, ExchangeStateError> {       
        Ok(self.markets.get(&top.instrument)
            .ok_or_else(|| ExchangeStateError::InstrumentNotFound(top.instrument))?
            .top_n_bid(top.n))
    }
}

#[derive(Error, Debug)]
pub enum ExchangeStateError {
    #[error("The exchange does not track this instrument: {0}")]
    InstrumentNotFound(Instrument),
    #[error("LocalBookError")]
    LocalBookError(#[from] LocalBookError)
}