use md_core::{book::{BookLevel, LocalBook}, events::{ControlEvent, NormalizedBookData}, helpers::book::ChecksumError, logging::types::Component, query::BookView, types::{Exchange, ExchangeStatus, Instrument}};
use tokio::sync::mpsc::Sender;
use tracing::{debug, error, info};
use std::collections::{BTreeSet, HashMap};
use thiserror::Error;

pub struct ExchangeState {
    exchange: Exchange,
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

    pub fn instruments(&self) -> BTreeSet<Instrument> {
        self.markets.keys().cloned().collect()
    }

    pub fn instruments_iter(&self) -> impl Iterator<Item = &Instrument> {
        self.markets.keys()
    }

    pub fn instruments_len(&self) -> usize {
        self.markets.len()
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
        if status == ExchangeStatus::Initializing(0.0) {
            self.markets.clear();
        }
        self.status = status;
    }

    pub fn get_status(&self) -> ExchangeStatus {
        self.status.clone()
    }

    pub fn top_n_asks(&self, instrument: &Instrument, depth: usize) -> Result<Vec<BookLevel>, ExchangeStateError> {
        let local_book = self.markets.get(&instrument).ok_or_else(|| {ExchangeStateError::InstrumentNotFound(instrument.clone())})?;
        debug!(exchange = ?self.exchange, component = ?Component::ExchangeState, instrument = ?instrument, n = depth,  "top_n_ask");
        Ok(local_book.top_n_asks(depth))
    }

    pub fn top_n_bids(&self, instrument: &Instrument, depth: usize) -> Result<Vec<BookLevel>, ExchangeStateError> {
        let local_book = self.markets.get(&instrument).ok_or_else(|| ExchangeStateError::InstrumentNotFound(instrument.clone()))?;
        debug!(exchange = ?self.exchange, component = ?Component::ExchangeState, instrument = ?instrument, n = depth, "top_n_bid");
        Ok(local_book.top_n_bids(depth))
    }

    pub fn book_view(&self, instrument: Instrument, depth: usize) ->  Result<BookView, ExchangeStateError> {
        let local_book = self.markets.get(&instrument).ok_or_else(|| {ExchangeStateError::InstrumentNotFound(instrument.clone())})?;
        let asks = local_book.top_n_asks(depth);
        let bids = local_book.top_n_bids(depth);
        let spread = local_book.spread();
        let mid = local_book.mid();
        Ok(BookView {
            exchange: self.exchange,
            instrument: instrument,
            asks,
            bids,
            spread,
            mid,
            status: self.status.clone()
        })
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