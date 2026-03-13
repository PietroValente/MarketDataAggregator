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

#[cfg(test)]
mod tests {
    use super::*;
    use md_core::{
        book::{BookSnapshot, BookUpdate},
        events::{ControlEvent, NormalizedSnapshot, NormalizedTop, NormalizedUpdate},
        types::{Exchange, ExchangeStatus, Instrument, Price, Qty},
    };
    use rust_decimal::Decimal;
    use tokio::sync::{mpsc, oneshot};

    fn instrument(sym: &str) -> Instrument {
        Instrument(sym.to_string())
    }

    fn px(v: i64) -> Price {
        Price(Decimal::from(v))
    }

    fn qty(v: i64) -> Qty {
        Qty(Decimal::from(v))
    }

    fn snapshot(sym: &str, last_update_id: u64, bids: Vec<(Price, Qty)>, asks: Vec<(Price, Qty)>) -> NormalizedSnapshot {
        NormalizedSnapshot {
            instrument: instrument(sym),
            data: BookSnapshot {
                last_update_id,
                bids,
                asks,
            },
        }
    }

    fn update(
        sym: &str,
        first_update_id: u64,
        last_update_id: u64,
        bids: Vec<(Price, Qty)>,
        asks: Vec<(Price, Qty)>,
    ) -> NormalizedUpdate {
        NormalizedUpdate {
            instrument: instrument(sym),
            data: BookUpdate {
                first_update_id,
                last_update_id,
                bids,
                asks,
            },
        }
    }

    fn top_req(sym: &str, n: usize) -> NormalizedTop {
        let (tx, _rx) = oneshot::channel();
        NormalizedTop {
            instrument: instrument(sym),
            n,
            reply_to: tx,
        }
    }

    #[test]
    fn new_starts_initializing() {
        let (control_tx, _control_rx) = mpsc::channel(8);
        let state = ExchangeState::new(Exchange::Binance, control_tx);

        assert!(matches!(state.get_status(), ExchangeStatus::Initializing));
    }

    #[test]
    fn apply_status_updates_status() {
        let (control_tx, _control_rx) = mpsc::channel(8);
        let mut state = ExchangeState::new(Exchange::Binance, control_tx);

        state.apply_status(ExchangeStatus::Running);

        assert!(matches!(state.get_status(), ExchangeStatus::Running));
    }

    #[test]
    fn apply_snapshot_creates_market_and_exposes_top_of_book() {
        let (control_tx, _control_rx) = mpsc::channel(8);
        let mut state = ExchangeState::new(Exchange::Binance, control_tx);

        state.apply_snapshot(snapshot(
            "BTCUSDT",
            42,
            vec![(px(100), qty(2)), (px(99), qty(3))],
            vec![(px(101), qty(1)), (px(102), qty(4))],
        ));

        let bids = state.top_n_bid(&top_req("BTCUSDT", 2)).unwrap();
        let asks = state.top_n_ask(&top_req("BTCUSDT", 2)).unwrap();

        assert_eq!(bids.len(), 2);
        assert_eq!(asks.len(), 2);

        assert_eq!(bids[0].px(), &px(100));
        assert_eq!(bids[0].qty(), &qty(2));
        assert_eq!(bids[1].px(), &px(99));
        assert_eq!(bids[1].qty(), &qty(3));

        assert_eq!(asks[0].px(), &px(101));
        assert_eq!(asks[0].qty(), &qty(1));
        assert_eq!(asks[1].px(), &px(102));
        assert_eq!(asks[1].qty(), &qty(4));
    }

    #[test]
    fn apply_update_on_missing_instrument_returns_instrument_not_found() {
        let (control_tx, _control_rx) = mpsc::channel(8);
        let mut state = ExchangeState::new(Exchange::Binance, control_tx);

        let err = state
            .apply_update(update(
                "BTCUSDT",
                1,
                2,
                vec![(px(100), qty(1))],
                vec![],
            ))
            .unwrap_err();

        assert!(matches!(
            err,
            ExchangeStateError::InstrumentNotFound(ref i) if i == &instrument("BTCUSDT")
        ));
    }

    #[test]
    fn apply_update_mutates_existing_book_observably() {
        let (control_tx, _control_rx) = mpsc::channel(8);
        let mut state = ExchangeState::new(Exchange::Binance, control_tx);

        state.apply_snapshot(snapshot(
            "BTCUSDT",
            100,
            vec![(px(100), qty(2)), (px(99), qty(3))],
            vec![(px(101), qty(1)), (px(102), qty(4))],
        ));

        state.apply_update(update(
            "BTCUSDT",
            101,
            102,
            vec![(px(100), qty(7)), (px(98), qty(1))],
            vec![(px(101), qty(5))],
        )).unwrap();

        let bids = state.top_n_bid(&top_req("BTCUSDT", 3)).unwrap();
        let asks = state.top_n_ask(&top_req("BTCUSDT", 2)).unwrap();

        assert_eq!(bids.len(), 3);
        assert_eq!(bids[0].px(), &px(100));
        assert_eq!(bids[0].qty(), &qty(7));
        assert_eq!(bids[1].px(), &px(99));
        assert_eq!(bids[2].px(), &px(98));

        assert_eq!(asks.len(), 2);
        assert_eq!(asks[0].px(), &px(101));
        assert_eq!(asks[0].qty(), &qty(5));
        assert_eq!(asks[1].px(), &px(102));
    }

    #[test]
    fn top_queries_on_missing_instrument_return_instrument_not_found() {
        let (control_tx, _control_rx) = mpsc::channel(8);
        let state = ExchangeState::new(Exchange::Binance, control_tx);

        let ask_err = state.top_n_ask(&top_req("ETHUSDT", 5)).unwrap_err();
        let bid_err = state.top_n_bid(&top_req("ETHUSDT", 5)).unwrap_err();

        assert!(matches!(
            ask_err,
            ExchangeStateError::InstrumentNotFound(ref i) if i == &instrument("ETHUSDT")
        ));
        assert!(matches!(
            bid_err,
            ExchangeStateError::InstrumentNotFound(ref i) if i == &instrument("ETHUSDT")
        ));
    }

    #[test]
    fn send_control_event_forwards_event_to_channel() {
        let (control_tx, mut control_rx) = mpsc::channel(8);
        let mut state = ExchangeState::new(Exchange::Binance, control_tx);

        state.send_control_event(ControlEvent::Resync).unwrap();

        let evt = control_rx.blocking_recv().expect("expected control event");
        assert!(matches!(evt, ControlEvent::Resync));
    }

    #[test]
    fn apply_update_propagates_local_book_out_of_sync() {
        let (control_tx, _control_rx) = mpsc::channel(8);
        let mut state = ExchangeState::new(Exchange::Binance, control_tx);

        state.apply_snapshot(snapshot(
            "BTCUSDT",
            10,
            vec![(px(99), qty(1))],
            vec![(px(101), qty(1))],
        ));

        let err = state
            .apply_update(update(
                "BTCUSDT",
                12, // gap: expected <= 11
                13,
                vec![(px(100), qty(2))],
                vec![(px(102), qty(2))],
            ))
            .unwrap_err();

        assert!(matches!(err, ExchangeStateError::LocalBookError(LocalBookError::OutOfSync)));
    }
}