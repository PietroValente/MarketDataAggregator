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
    InstrumentNotFound(Instrument)
}

#[cfg(test)]
mod tests {
    use super::*;
    use md_core::{
        book::BookLevels,
        types::{Price, Qty},
    };
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use rust_decimal::Decimal;
    use std::collections::BTreeMap;
    use tokio::sync::oneshot::{self, Sender};

    fn instrument(name: &str) -> Instrument {
        Instrument(name.to_string())
    }

    fn price(v: i64) -> Price {
        Price(Decimal::new(v, 2))
    }

    fn qty(v: i64) -> Qty {
        Qty(Decimal::new(v, 3))
    }

    fn mk_top(instrument: Instrument, n: usize) -> (Sender<Vec<BookLevel>>, NormalizedTop) {
        let (tx, _rx) = oneshot::channel();
        (tx, NormalizedTop {
            instrument,
            n
        })
    }

    #[test]
    fn update_before_snapshot_returns_not_found() {
        let mut state = ExchangeState::new(Exchange::Binance);
        let err = state
            .apply_update(NormalizedBookData {
                instrument: instrument("BTCUSDT"),
                levels: BookLevels {
                    asks: vec![(price(10100), qty(1000))],
                    bids: vec![(price(10000), qty(2000))],
                },
            })
            .expect_err("update without snapshot must fail");

        match err {
            ExchangeStateError::InstrumentNotFound(inst) => {
                assert_eq!(inst, instrument("BTCUSDT"));
            }
        }
    }

    #[test]
    fn status_defaults_to_initializing_and_transitions_are_persisted() {
        let mut state = ExchangeState::new(Exchange::Coinbase);
        assert!(matches!(state.get_status(), ExchangeStatus::Initializing));

        state.apply_status(ExchangeStatus::Running);
        assert!(matches!(state.get_status(), ExchangeStatus::Running));

        state.apply_status(ExchangeStatus::Error("ws disconnected".to_string()));
        match state.get_status() {
            ExchangeStatus::Error(msg) => assert_eq!(msg, "ws disconnected"),
            other => panic!("expected Error status, got {other:?}"),
        }
    }

    #[test]
    fn snapshot_and_update_flow_for_single_instrument() {
        let mut state = ExchangeState::new(Exchange::Bitget);
        let inst = instrument("ETHUSDT");

        state.apply_snapshot(NormalizedBookData {
            instrument: inst.clone(),
            levels: BookLevels {
                asks: vec![(price(20000), qty(1000)), (price(20100), qty(1200))],
                bids: vec![(price(19900), qty(1300)), (price(19800), qty(1500))],
            },
        });

        state
            .apply_update(NormalizedBookData {
                instrument: inst.clone(),
                levels: BookLevels {
                    asks: vec![(price(20000), qty(900)), (price(20100), qty(0))],
                    bids: vec![(price(19900), qty(1700)), (price(19700), qty(1400))],
                },
            })
            .expect("update after snapshot must work");

        let (_, data) = mk_top(inst.clone(), 10);

        let ask_top = state.top_n_ask(&data).unwrap();
        let bid_top = state.top_n_bid(&data).unwrap();

        assert_eq!(
            ask_top,
            vec![BookLevel::new(qty(900), price(20000))]
        );
        assert_eq!(
            bid_top,
            vec![
                BookLevel::new(qty(1700), price(19900)),
                BookLevel::new(qty(1500), price(19800)),
                BookLevel::new(qty(1400), price(19700)),
            ]
        );
    }

    #[test]
    fn snapshot_overwrites_existing_instrument_state() {
        let mut state = ExchangeState::new(Exchange::Kraken);
        let inst = instrument("SOLUSDT");

        state.apply_snapshot(NormalizedBookData {
            instrument: inst.clone(),
            levels: BookLevels {
                asks: vec![(price(1000), qty(1000)), (price(1100), qty(2000))],
                bids: vec![(price(900), qty(3000)), (price(800), qty(4000))],
            },
        });

        state.apply_snapshot(NormalizedBookData {
            instrument: inst.clone(),
            levels: BookLevels {
                asks: vec![(price(1200), qty(5000))],
                bids: vec![(price(700), qty(6000))],
            },
        });

        let (_, data) = mk_top(inst.clone(), 10);

        assert_eq!(
            state.top_n_ask(&data).unwrap(),
            vec![BookLevel::new(qty(5000), price(1200))]
        );
        assert_eq!(
            state.top_n_bid(&data).unwrap(),
            vec![BookLevel::new(qty(6000), price(700))]
        );
    }

    #[test]
    fn random_stress_matches_reference_model_multi_instrument() {
        let mut rng = StdRng::seed_from_u64(0xBADC0FFE_u64);
        let mut state = ExchangeState::new(Exchange::Bybit);

        let instruments = [
            instrument("BTCUSDT"),
            instrument("ETHUSDT"),
            instrument("SOLUSDT"),
            instrument("XRPUSDT"),
            instrument("BNBUSDT"),
            instrument("DOGEUSDT"),
            instrument("ADAUSDT"),
            instrument("DOTUSDT"),
        ];

        let mut ask_ref: HashMap<Instrument, BTreeMap<Price, Qty>> = HashMap::new();
        let mut bid_ref: HashMap<Instrument, BTreeMap<Price, Qty>> = HashMap::new();

        // bootstrap snapshots for all instruments
        for inst in &instruments {
            let mut asks = Vec::new();
            let mut bids = Vec::new();
            let mut ask_map = BTreeMap::new();
            let mut bid_map = BTreeMap::new();

            for _ in 0..200 {
                let p = price(rng.gen_range(10000..13000));
                let q = qty(rng.gen_range(1..1_000_000));
                asks.push((p, q));
                ask_map.insert(p, q);
            }
            for _ in 0..200 {
                let p = price(rng.gen_range(7000..10000));
                let q = qty(rng.gen_range(1..1_000_000));
                bids.push((p, q));
                bid_map.insert(p, q);
            }

            state.apply_snapshot(NormalizedBookData {
                instrument: inst.clone(),
                levels: BookLevels { asks, bids },
            });
            ask_ref.insert(inst.clone(), ask_map);
            bid_ref.insert(inst.clone(), bid_map);
        }

        for _step in 0..5000 {
            let idx = rng.gen_range(0..instruments.len());
            let inst = instruments[idx].clone();
            let force_snapshot = rng.gen_bool(0.03);

            if force_snapshot {
                let mut asks = Vec::new();
                let mut bids = Vec::new();
                let mut ask_map = BTreeMap::new();
                let mut bid_map = BTreeMap::new();

                let ask_count = rng.gen_range(0..80);
                let bid_count = rng.gen_range(0..80);
                for _ in 0..ask_count {
                    let p = price(rng.gen_range(10000..14000));
                    let q = qty(rng.gen_range(1..1_000_000));
                    asks.push((p, q));
                    ask_map.insert(p, q);
                }
                for _ in 0..bid_count {
                    let p = price(rng.gen_range(6000..10000));
                    let q = qty(rng.gen_range(1..1_000_000));
                    bids.push((p, q));
                    bid_map.insert(p, q);
                }

                state.apply_snapshot(NormalizedBookData {
                    instrument: inst.clone(),
                    levels: BookLevels { asks, bids },
                });
                ask_ref.insert(inst.clone(), ask_map);
                bid_ref.insert(inst.clone(), bid_map);
            } else {
                let ask_batch = rng.gen_range(0..25);
                let bid_batch = rng.gen_range(0..25);
                let mut ask_updates = Vec::new();
                let mut bid_updates = Vec::new();

                for _ in 0..ask_batch {
                    let p = price(rng.gen_range(9000..14500));
                    let remove = rng.gen_bool(0.35);
                    let q = if remove {
                        qty(0)
                    } else {
                        qty(rng.gen_range(1..1_000_000))
                    };
                    ask_updates.push((p, q));
                    let map = ask_ref
                        .get_mut(&inst)
                        .expect("instrument should be initialized");
                    if q.is_zero() {
                        map.remove(&p);
                    } else {
                        map.insert(p, q);
                    }
                }

                for _ in 0..bid_batch {
                    let p = price(rng.gen_range(5500..10500));
                    let remove = rng.gen_bool(0.35);
                    let q = if remove {
                        qty(0)
                    } else {
                        qty(rng.gen_range(1..1_000_000))
                    };
                    bid_updates.push((p, q));
                    let map = bid_ref
                        .get_mut(&inst)
                        .expect("instrument should be initialized");
                    if q.is_zero() {
                        map.remove(&p);
                    } else {
                        map.insert(p, q);
                    }
                }

                state
                    .apply_update(NormalizedBookData {
                        instrument: inst.clone(),
                        levels: BookLevels {
                            asks: ask_updates,
                            bids: bid_updates,
                        },
                    })
                    .expect("updates should not fail for initialized instrument");
            }

            let n = rng.gen_range(0..40);
            let (_, data) = mk_top(inst.clone(), n);
            let got_asks = state.top_n_ask(&data).unwrap();
            let got_bids = state.top_n_bid(&data).unwrap();

            let exp_asks: Vec<BookLevel> = ask_ref
                .get(&inst)
                .expect("instrument should be initialized")
                .iter()
                .take(n)
                .map(|(p, q)| BookLevel::new(*q, *p))
                .collect();
            let exp_bids: Vec<BookLevel> = bid_ref
                .get(&inst)
                .expect("instrument should be initialized")
                .iter()
                .rev()
                .take(n)
                .map(|(p, q)| BookLevel::new(*q, *p))
                .collect();

            assert_eq!(got_asks, exp_asks);
            assert_eq!(got_bids, exp_bids);
        }
    }
}