use md_core::{book::{BookLevel, LocalBook}, events::{ControlEvent, NormalizedBookData, NormalizedTop}, helpers::book::ChecksumError, logging::types::Component, types::{Exchange, ExchangeStatus, Instrument}};
use tokio::sync::mpsc::Sender;
use tracing::{debug, error, info};
use std::collections::HashMap;
use thiserror::Error;

pub struct ExchangeState {
    exchange: Exchange, //kept for log
    status: ExchangeStatus,
    control_tx: Sender<ControlEvent>,
    markets: HashMap<Instrument, LocalBook>,
}

impl ExchangeState {
    pub fn new(exchange: Exchange, control_tx: Sender<ControlEvent>) -> Self {
        Self {
            exchange,
            status: ExchangeStatus::Initializing(0.0),
            markets: HashMap::new(),
            control_tx
        }
    }

    pub fn resync(&mut self) {
        if let Err(e) = self.control_tx.blocking_send(ControlEvent::Resync) {
            error!(exchange = ?self.exchange, component = ?Component::ExchangeState, error = ?e, "error while sending resync");
        }
    }

    pub fn apply_snapshot(&mut self, snapshot: NormalizedBookData) -> Result<(), ExchangeStateError> {
        let NormalizedBookData {
            instrument,
            levels,
            checksum,
        } = snapshot;
    
        debug!(
            exchange = ?self.exchange,
            component = ?Component::ExchangeState,
            instrument = ?instrument,
            "applying snapshot"
        );
    
        let book = self
            .markets
            .entry(instrument.clone())
            .or_insert_with(LocalBook::new);
    
        book.apply_snapshot(levels);
    
        if let Some(expected) = checksum {
            book.verify_okx_bitget_checksum(expected).map_err(|source| {
                ExchangeStateError::ChecksumError {
                    instrument,
                    source,
                }
            })?;
        }
    
        Ok(())
    }
    
    pub fn apply_update(&mut self, update: NormalizedBookData) -> Result<(), ExchangeStateError> {
        let NormalizedBookData {
            instrument,
            levels,
            checksum,
        } = update;
    
        debug!(
            exchange = ?self.exchange,
            component = ?Component::ExchangeState,
            instrument = ?instrument,
            "applying update"
        );
    
        let book = self
            .markets
            .get_mut(&instrument)
            .ok_or_else(|| ExchangeStateError::InstrumentNotFound(instrument.clone()))?;
    
        book.apply_update(levels);
    
        if let Some(expected) = checksum {
            book.verify_okx_bitget_checksum(expected).map_err(|source| {
                ExchangeStateError::ChecksumError {
                    instrument,
                    source,
                }
            })?;
        }
    
        Ok(())
    }

    pub fn apply_status(&mut self, status: ExchangeStatus) {
        info!(exchange = ?self.exchange, component = ?Component::ExchangeState, status = ?status, "applying status");
        self.status = status;
    }

    pub fn get_status(&self) -> ExchangeStatus {
        self.status.clone()
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
    InstrumentNotFound(Instrument),

    #[error("Checksum error for {instrument}: {source}")]
    ChecksumError {
        instrument: Instrument,
        #[source]
        source: ChecksumError
    },
}