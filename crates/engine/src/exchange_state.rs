use md_core::{book::{BookLevel, LocalBook}, events::{NormalizedBookData, NormalizedTop}, logging::types::Component, types::{Exchange, ExchangeStatus, Instrument}};
use tracing::{debug, info};
use std::collections::HashMap;
use thiserror::Error;

pub struct ExchangeState {
    exchange: Exchange, //kept for log
    status: ExchangeStatus,
    markets: HashMap<Instrument, LocalBook>,
}

impl ExchangeState {
    pub fn new(e: Exchange) -> Self {
        Self {
            exchange: e,
            status: ExchangeStatus::Initializing,
            markets: HashMap::new()
        }
    }

    pub fn apply_snapshot(&mut self, snapshot: NormalizedBookData) {
        debug!(exchange = ?self.exchange, component = ?Component::ExchangeState, instrument = ?snapshot.instrument, "applying snapshot");
        self.markets.entry(snapshot.instrument).or_insert(LocalBook::new()).apply_snapshot(snapshot.levels);
    }

    pub fn apply_update(&mut self, update: NormalizedBookData) -> Result<(), ExchangeStateError> {
        debug!(exchange = ?self.exchange, component = ?Component::ExchangeState, instrument = ?update.instrument, "applying update");

        self.markets.get_mut(&update.instrument)
            .ok_or_else(|| ExchangeStateError::InstrumentNotFound(update.instrument))?
            .apply_update(update.levels);
        Ok(())    
    }

    pub fn apply_status(&mut self, status: ExchangeStatus) {
        info!(exchange = ?self.exchange, component = ?Component::ExchangeState, status = ?status, "applying status");
        self.status = status;
    }

    pub fn get_status(&self) -> &ExchangeStatus {
        &self.status
    }

    pub fn top_n_ask(&self, top: &NormalizedTop) -> Result<Vec<BookLevel>, ExchangeStateError> {
        let local_book = self.markets.get(&top.instrument).ok_or_else(|| {ExchangeStateError::InstrumentNotFound(top.instrument.clone())})?;
        debug!(exchange = ?self.exchange, component = ?Component::ExchangeState, instrument = ?top.instrument, n = top.n, ask_len = local_book.ask_len(),  "top_n_ask");
        Ok(local_book.top_n_ask(top.n))
    }

    pub fn top_n_bid(&self, top: &NormalizedTop) -> Result<Vec<BookLevel>, ExchangeStateError> {
        let local_book = self.markets.get(&top.instrument).ok_or_else(|| ExchangeStateError::InstrumentNotFound(top.instrument.clone()))?;
        debug!(exchange = ?self.exchange, component = ?Component::ExchangeState, instrument = ?top.instrument, n = top.n, bid_len = local_book.bid_len(),  "top_n_bid");
        Ok(local_book.top_n_bid(top.n))
    }
}

#[derive(Error, Debug)]
pub enum ExchangeStateError {
    #[error("The exchange does not track this instrument: {0}")]
    InstrumentNotFound(Instrument)
}

#[cfg(test)]
mod tests {

}