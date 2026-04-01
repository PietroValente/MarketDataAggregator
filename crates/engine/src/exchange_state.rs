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

#[cfg(test)]
mod tests {
    use super::*;
    use md_core::{book::BookLevels, types::{Price, Qty}};
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use rust_decimal::Decimal;
    use std::collections::BTreeMap;
    use tokio::sync::mpsc;

    fn instrument(v: &str) -> Instrument {
        Instrument(v.to_owned())
    }

    fn price(v: i64) -> Price {
        Price(Decimal::new(v, 2))
    }

    fn qty(v: i64) -> Qty {
        Qty(Decimal::new(v, 3))
    }

    #[test]
    fn apply_update_on_unknown_instrument_returns_not_found() {
        let (tx, _rx) = mpsc::channel(4);
        let mut state = ExchangeState::new(Exchange::Binance, tx);
        let missing = instrument("ETHUSDT");

        let err = state
            .apply_update(NormalizedBookData {
                instrument: missing.clone(),
                levels: BookLevels {
                    asks: vec![(price(10000), qty(1000))],
                    bids: vec![(price(9900), qty(1000))],
                },
                checksum: None,
            })
            .expect_err("update must fail before snapshot initialization");

        match err {
            ExchangeStateError::InstrumentNotFound(found) => assert_eq!(found, missing),
            _ => panic!("unexpected error variant"),
        }
    }

    #[test]
    fn state_updates_are_isolated_per_instrument() {
        let (tx, _rx) = mpsc::channel(4);
        let mut state = ExchangeState::new(Exchange::Bitget, tx);
        let btc = instrument("BTCUSDT");
        let eth = instrument("ETHUSDT");

        state.apply_snapshot(NormalizedBookData {
            instrument: btc.clone(),
            levels: BookLevels {
                asks: vec![(price(10100), qty(1000))],
                bids: vec![(price(10000), qty(2000))],
            },
            checksum: None,
        }).unwrap();
        state.apply_snapshot(NormalizedBookData {
            instrument: eth.clone(),
            levels: BookLevels {
                asks: vec![(price(20100), qty(3000))],
                bids: vec![(price(20000), qty(4000))],
            },
            checksum: None,
        }).unwrap();

        // Update only BTC and ensure ETH remains untouched.
        state.apply_update(NormalizedBookData {
            instrument: btc.clone(),
            levels: BookLevels {
                asks: vec![(price(10100), qty(0)), (price(10200), qty(5000))],
                bids: vec![(price(10000), qty(2500))],
            },
            checksum: None,
        }).unwrap();

        assert_eq!(
            state.top_n_ask(&NormalizedTop { instrument: btc.clone(), n: 5 }).unwrap(),
            vec![BookLevel::new(qty(5000), price(10200))]
        );
        assert_eq!(
            state.top_n_bid(&NormalizedTop { instrument: btc.clone(), n: 5 }).unwrap(),
            vec![BookLevel::new(qty(2500), price(10000))]
        );
        assert_eq!(
            state.top_n_ask(&NormalizedTop { instrument: eth.clone(), n: 5 }).unwrap(),
            vec![BookLevel::new(qty(3000), price(20100))]
        );
        assert_eq!(
            state.top_n_bid(&NormalizedTop { instrument: eth.clone(), n: 5 }).unwrap(),
            vec![BookLevel::new(qty(4000), price(20000))]
        );
    }

    #[test]
    fn status_transitions_are_applied() {
        let (tx, _rx) = mpsc::channel(4);
        let mut state = ExchangeState::new(Exchange::Bybit, tx);

        match state.get_status() {
            ExchangeStatus::Initializing(p) => assert_eq!(p, 0.0),
            ExchangeStatus::Running => panic!("unexpected initial running status"),
        }

        state.apply_status(ExchangeStatus::Initializing(0.42));
        match state.get_status() {
            ExchangeStatus::Initializing(p) => assert_eq!(p, 0.42),
            ExchangeStatus::Running => panic!("unexpected running status"),
        }

        state.apply_status(ExchangeStatus::Running);
        assert!(matches!(state.get_status(), ExchangeStatus::Running));
    }

    #[test]
    fn random_multi_market_updates_match_reference_model() {
        let (tx, _rx) = mpsc::channel(8);
        let mut state = ExchangeState::new(Exchange::Okx, tx);
        let mut rng = StdRng::seed_from_u64(0xE11E_55AA);

        let markets = vec![
            instrument("BTCUSDT"),
            instrument("ETHUSDT"),
            instrument("SOLUSDT"),
        ];

        let mut ask_ref: HashMap<Instrument, BTreeMap<Price, Qty>> = HashMap::new();
        let mut bid_ref: HashMap<Instrument, BTreeMap<Price, Qty>> = HashMap::new();

        for m in &markets {
            let mut asks = Vec::new();
            let mut bids = Vec::new();
            let mut ask_map = BTreeMap::new();
            let mut bid_map = BTreeMap::new();

            for _ in 0..120 {
                let p = price(rng.gen_range(10000..11000));
                let q = qty(rng.gen_range(1..500_000));
                asks.push((p, q));
                ask_map.insert(p, q);
            }
            for _ in 0..120 {
                let p = price(rng.gen_range(9000..10000));
                let q = qty(rng.gen_range(1..500_000));
                bids.push((p, q));
                bid_map.insert(p, q);
            }

            ask_ref.insert(m.clone(), ask_map);
            bid_ref.insert(m.clone(), bid_map);
            state.apply_snapshot(NormalizedBookData {
                instrument: m.clone(),
                levels: BookLevels { asks, bids },
                checksum: None,
            }).unwrap();
        }

        for _ in 0..2500 {
            let idx = rng.gen_range(0..markets.len());
            let instrument = markets[idx].clone();
            let ask_batch = rng.gen_range(0..15);
            let bid_batch = rng.gen_range(0..15);

            let mut asks = Vec::with_capacity(ask_batch);
            let mut bids = Vec::with_capacity(bid_batch);

            for _ in 0..ask_batch {
                let p = price(rng.gen_range(9500..11500));
                let q = if rng.gen_bool(0.35) {
                    qty(0)
                } else {
                    qty(rng.gen_range(1..500_000))
                };
                asks.push((p, q));
                let map = ask_ref.get_mut(&instrument).expect("known market");
                if q.is_zero() {
                    map.remove(&p);
                } else {
                    map.insert(p, q);
                }
            }

            for _ in 0..bid_batch {
                let p = price(rng.gen_range(8500..10500));
                let q = if rng.gen_bool(0.35) {
                    qty(0)
                } else {
                    qty(rng.gen_range(1..500_000))
                };
                bids.push((p, q));
                let map = bid_ref.get_mut(&instrument).expect("known market");
                if q.is_zero() {
                    map.remove(&p);
                } else {
                    map.insert(p, q);
                }
            }

            state.apply_update(NormalizedBookData {
                instrument: instrument.clone(),
                levels: BookLevels { asks, bids },
                checksum: None,
            }).unwrap();
        }

        for m in &markets {
            let expected_asks: Vec<BookLevel> = ask_ref[m]
                .iter()
                .take(25)
                .map(|(p, q)| BookLevel::new(*q, *p))
                .collect();
            let expected_bids: Vec<BookLevel> = bid_ref[m]
                .iter()
                .rev()
                .take(25)
                .map(|(p, q)| BookLevel::new(*q, *p))
                .collect();

            assert_eq!(
                state.top_n_ask(&NormalizedTop { instrument: m.clone(), n: 25 }).unwrap(),
                expected_asks
            );
            assert_eq!(
                state.top_n_bid(&NormalizedTop { instrument: m.clone(), n: 25 }).unwrap(),
                expected_bids
            );
        }
    }
}