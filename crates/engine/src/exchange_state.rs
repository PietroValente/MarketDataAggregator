use md_core::{book::{BookLevel, LocalBook}, events::{ControlEvent, NormalizedBookData}, helpers::book::ChecksumError, logging::types::Component, query::BookView, types::{Exchange, ExchangeStatus, Instrument}};
use tokio::sync::mpsc::Sender;
use tracing::{error, info};
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
        if self.status != status {
            info!(
                exchange = ?self.exchange,
                component = ?Component::ExchangeState,
                previous_status = ?self.status,
                new_status = ?status,
                "exchange status changed"
            );
        }
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
        Ok(local_book.top_n_asks(depth))
    }

    pub fn top_n_bids(&self, instrument: &Instrument, depth: usize) -> Result<Vec<BookLevel>, ExchangeStateError> {
        let local_book = self.markets.get(&instrument).ok_or_else(|| ExchangeStateError::InstrumentNotFound(instrument.clone()))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use md_core::{
        book::{BookLevel, BookLevels, LocalBook},
        events::NormalizedBookData,
        helpers::book::compute_checksum,
        types::{Exchange, ExchangeStatus, Price, Qty},
    };
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use tokio::sync::mpsc;

    fn price(v: i64) -> Price {
        Price(Decimal::new(v, 2))
    }

    fn qty(v: i64) -> Qty {
        Qty(Decimal::new(v, 3))
    }

    fn state_with_capacity(cap: usize) -> (ExchangeState, mpsc::Receiver<ControlEvent>) {
        let (tx, rx) = mpsc::channel(cap);
        (ExchangeState::new(Exchange::Okx, tx), rx)
    }

    #[test]
    fn update_before_snapshot_returns_instrument_not_found() {
        let (mut state, _ctrl) = state_with_capacity(4);
        let inst = Instrument("BTC-PERP".to_string());

        let err = state
            .apply_update(NormalizedBookData {
                instrument: inst.clone(),
                levels: BookLevels {
                    asks: vec![(price(10100), qty(1000))],
                    bids: vec![(price(10000), qty(1000))],
                },
                checksum: None,
            })
            .unwrap_err();

        match err {
            ExchangeStateError::InstrumentNotFound(i) => assert_eq!(i, inst),
            _ => panic!("expected InstrumentNotFound"),
        }
    }

    #[test]
    fn snapshot_checksum_mismatch_is_error() {
        let (mut state, _ctrl) = state_with_capacity(4);
        let inst = Instrument("ETH-PERP".to_string());
        let levels = BookLevels {
            asks: vec![(price(10100), qty(1000))],
            bids: vec![(price(10000), qty(1000))],
        };

        let mut book = LocalBook::new();
        book.apply_snapshot(levels.clone());
        let good = compute_checksum(book.top_n_bids(25), book.top_n_asks(25));

        let err = state
            .apply_snapshot(NormalizedBookData {
                instrument: inst.clone(),
                levels,
                checksum: Some(good.wrapping_add(1)),
            })
            .unwrap_err();

        assert!(matches!(err, ExchangeStateError::ChecksumError { .. }));
    }

    #[test]
    fn initializing_zero_status_clears_markets() {
        let (mut state, _ctrl) = state_with_capacity(4);
        let inst = Instrument("SOL-PERP".to_string());

        state
            .apply_snapshot(NormalizedBookData {
                instrument: inst.clone(),
                levels: BookLevels {
                    asks: vec![(price(10100), qty(1000))],
                    bids: vec![(price(10000), qty(1000))],
                },
                checksum: None,
            })
            .unwrap();

        assert_eq!(state.instruments_len(), 1);

        state.apply_status(ExchangeStatus::Initializing(0.0));

        assert_eq!(state.instruments_len(), 0);
        assert!(state.book_view(inst.clone(), 5).is_err());
    }

    #[test]
    fn book_view_matches_top_n_and_spread() {
        let (mut state, _ctrl) = state_with_capacity(4);
        let inst = Instrument("ARB-PERP".to_string());

        state
            .apply_snapshot(NormalizedBookData {
                instrument: inst.clone(),
                levels: BookLevels {
                    asks: vec![(price(10150), qty(1000)), (price(10200), qty(2000))],
                    bids: vec![(price(10100), qty(3000)), (price(10050), qty(4000))],
                },
                checksum: None,
            })
            .unwrap();

        let view = state.book_view(inst.clone(), 2).unwrap();
        assert_eq!(view.asks, state.top_n_asks(&inst, 2).unwrap());
        assert_eq!(view.bids, state.top_n_bids(&inst, 2).unwrap());
        assert_eq!(view.spread, Some(price(50)));
        assert_eq!(view.mid, Some((price(10100) + price(10150)) / Price(dec!(2))));
    }

    #[test]
    fn random_updates_match_reference_with_checksum_each_step() {
        let mut rng = StdRng::seed_from_u64(0xE5_5E_5E_5E);
        let (mut state, _ctrl) = state_with_capacity(4);
        let inst = Instrument("STRESS-PERP".to_string());

        let mut asks_ref: std::collections::BTreeMap<Price, Qty> =
            std::collections::BTreeMap::new();
        let mut bids_ref: std::collections::BTreeMap<Price, Qty> =
            std::collections::BTreeMap::new();

        let mut asks = Vec::new();
        let mut bids = Vec::new();
        for _ in 0..400 {
            let p = price(rng.gen_range(10000..11000));
            let q = qty(rng.gen_range(1..1_000_000));
            asks.push((p, q));
            asks_ref.insert(p, q);
        }
        for _ in 0..400 {
            let p = price(rng.gen_range(9000..10000));
            let q = qty(rng.gen_range(1..1_000_000));
            bids.push((p, q));
            bids_ref.insert(p, q);
        }

        let snapshot = BookLevels { asks, bids };
        let mut ref_book = LocalBook::new();
        ref_book.apply_snapshot(snapshot.clone());

        state
            .apply_snapshot(NormalizedBookData {
                instrument: inst.clone(),
                levels: snapshot,
                checksum: None,
            })
            .unwrap();

        for _ in 0..5000 {
            let mut asks_updates = Vec::new();
            let mut bids_updates = Vec::new();

            let asks_batch = rng.gen_range(0..15);
            let bids_batch = rng.gen_range(0..15);

            for _ in 0..asks_batch {
                let p = price(rng.gen_range(9500..11500));
                let remove = rng.gen_bool(0.35);
                let q = if remove {
                    qty(0)
                } else {
                    qty(rng.gen_range(1..1_000_000))
                };
                asks_updates.push((p, q));
                if q.is_zero() {
                    asks_ref.remove(&p);
                } else {
                    asks_ref.insert(p, q);
                }
            }

            for _ in 0..bids_batch {
                let p = price(rng.gen_range(8500..10500));
                let remove = rng.gen_bool(0.35);
                let q = if remove {
                    qty(0)
                } else {
                    qty(rng.gen_range(1..1_000_000))
                };
                bids_updates.push((p, q));
                if q.is_zero() {
                    bids_ref.remove(&p);
                } else {
                    bids_ref.insert(p, q);
                }
            }

            let levels = BookLevels {
                asks: asks_updates,
                bids: bids_updates,
            };
            ref_book.apply_update(levels.clone());
            let expected_cs = compute_checksum(ref_book.top_n_bids(25), ref_book.top_n_asks(25));

            state
                .apply_update(NormalizedBookData {
                    instrument: inst.clone(),
                    levels,
                    checksum: Some(expected_cs),
                })
                .unwrap();

            assert_eq!(state.top_n_asks(&inst, 25).unwrap().len(), asks_ref.len().min(25));
            assert_eq!(state.top_n_bids(&inst, 25).unwrap().len(), bids_ref.len().min(25));

            let expected_asks: Vec<BookLevel> = asks_ref
                .iter()
                .take(25)
                .map(|(p, q)| BookLevel::new(*q, *p))
                .collect();
            let expected_bids: Vec<BookLevel> = bids_ref
                .iter()
                .rev()
                .take(25)
                .map(|(p, q)| BookLevel::new(*q, *p))
                .collect();

            assert_eq!(state.top_n_asks(&inst, 25).unwrap(), expected_asks);
            assert_eq!(state.top_n_bids(&inst, 25).unwrap(), expected_bids);
        }
    }
}