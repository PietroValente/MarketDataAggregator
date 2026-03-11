use md_core::{book::{BookLevel, LocalBook, LocalBookError}, events::{ControlEvent, NormalizedSnapshot, NormalizedTop, NormalizedUpdate}, types::{Exchange, ExchangeStatus, Instrument}};
use tokio::sync::mpsc::{error::SendError, Sender};
use tracing::{debug, info};
use std::collections::HashMap;
use thiserror::Error;

pub struct ExchangeState {
    exchange: Exchange, //kept for log
    control_tx: Sender<ControlEvent>,
    status: ExchangeStatus,
    markets: HashMap<Instrument, LocalBook>,
}

impl ExchangeState {
    pub fn new(e: Exchange, control_tx: Sender<ControlEvent>) -> Self {
        Self {
            exchange: e,
            control_tx,
            status: ExchangeStatus::Initializing,
            markets: HashMap::new()
        }
    }

    pub fn apply_snapshot(&mut self, snapshot: NormalizedSnapshot) {
        debug!(exchange = ?self.exchange, instrument = ?snapshot.instrument, "applying snapshot");
        self.markets.entry(snapshot.instrument).or_insert(LocalBook::new()).apply_snapshot(snapshot.data);
    }

    pub fn apply_update(&mut self, update: NormalizedUpdate) -> Result<(), ExchangeStateError> {
        debug!(exchange = ?self.exchange, instrument = ?update.instrument, "applying update");

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

    pub fn send_control_event(&mut self, event: ControlEvent) -> Result<(), SendError<ControlEvent>> {
        self.control_tx.blocking_send(event)
    }

    pub fn top_n_ask(&self, top: &NormalizedTop) -> Result<Vec<BookLevel>, ExchangeStateError> {
        let local_book = self.markets.get(&top.instrument).ok_or_else(|| ExchangeStateError::InstrumentNotFound(top.instrument.clone()))?;
        debug!(exchange = ?self.exchange, instrument = ?top.instrument, n = top.n, ask_len = local_book.ask_len(),  "top_n_ask");
        Ok(local_book.top_n_ask(top.n))
    }

    pub fn top_n_bid(&self, top: &NormalizedTop) -> Result<Vec<BookLevel>, ExchangeStateError> {
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