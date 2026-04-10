use md_core::{
    events::{BookEventType, ControlEvent, EngineMessage, EventEnvelope, NormalizedEvent},
    logging::types::Component,
    types::Exchange,
};
use std::collections::HashMap;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, warn};

use crate::exchange_state::{ExchangeState, ExchangeStateError};

pub struct Engine {
    pub(crate) exchanges: HashMap<Exchange, ExchangeState>,
    rx: Receiver<EngineMessage>,
}

impl Engine {
    pub fn new(
        rx: Receiver<EngineMessage>,
        exchanges_controller: HashMap<Exchange, Sender<ControlEvent>>,
    ) -> Self {
        let mut map = HashMap::new();
        for (exchange, control_tx) in exchanges_controller {
            map.insert(exchange, ExchangeState::new(exchange, control_tx));
        }
        Self { exchanges: map, rx }
    }
    pub fn run(&mut self) {
        while let Some(msg) = self.rx.blocking_recv() {
            match msg {
                EngineMessage::Apply(event_enveloped) => {
                    let EventEnvelope { exchange, event } = event_enveloped;
                    let Some(exchange_state) = self.exchanges.get_mut(&exchange) else {
                        error!(exchange = ?exchange, component = ?Component::Engine, "exchange not found");
                        continue;
                    };

                    match event {
                        NormalizedEvent::ApplyStatus(data) => {
                            exchange_state.apply_status(data);
                        }
                        NormalizedEvent::Book(event_type, book_data) => match event_type {
                            BookEventType::Snapshot => {
                                if let Err(e) = exchange_state.apply_snapshot(book_data) {
                                    match e {
                                        ExchangeStateError::InstrumentNotFound(instrument) => {
                                            warn!(
                                                exchange = ?exchange,
                                                component = ?Component::Engine,
                                                instrument = ?instrument,
                                                "snapshot ignored: instrument not tracked yet"
                                            );
                                            continue;
                                        }
                                        ExchangeStateError::ChecksumError {
                                            instrument: _,
                                            source: _,
                                        } => {
                                            error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "apply_snapshot checksum error, resync requested");
                                            exchange_state.resync();
                                        }
                                    }
                                }
                            }
                            BookEventType::Update => {
                                if let Err(e) = exchange_state.apply_update(book_data) {
                                    match e {
                                        ExchangeStateError::InstrumentNotFound(instrument) => {
                                            warn!(
                                                exchange = ?exchange,
                                                component = ?Component::Engine,
                                                instrument = ?instrument,
                                                "update ignored: instrument not tracked yet"
                                            );
                                            continue;
                                        }
                                        ExchangeStateError::ChecksumError {
                                            instrument: _,
                                            source: _,
                                        } => {
                                            error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "apply_update checksum error, resync requested");
                                            exchange_state.resync();
                                        }
                                    }
                                }
                            }
                        },
                    }
                }
                EngineMessage::Query(engine_query) => {
                    self.handle_query(engine_query);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use md_core::{
        book::{BookLevels, LocalBook},
        events::{
            BookEventType, ControlEvent, EngineMessage, EventEnvelope, NormalizedBookData,
            NormalizedEvent,
        },
        helpers::book::compute_checksum,
        query::EngineQuery,
        types::{Instrument, Price, Qty},
    };
    use rust_decimal::Decimal;
    use tokio::sync::{mpsc, oneshot};

    fn price(v: i64) -> Price {
        Price(Decimal::new(v, 2))
    }

    fn qty(v: i64) -> Qty {
        Qty(Decimal::new(v, 3))
    }

    fn run_engine(
        exchanges: std::collections::HashMap<Exchange, mpsc::Sender<ControlEvent>>,
    ) -> (mpsc::Sender<EngineMessage>, std::thread::JoinHandle<()>) {
        let (tx, rx) = mpsc::channel::<EngineMessage>(64);
        let mut engine = Engine::new(rx, exchanges);
        let h = std::thread::spawn(move || engine.run());
        (tx, h)
    }

    #[test]
    fn snapshot_checksum_mismatch_triggers_resync() {
        let (control_tx, mut control_rx) = mpsc::channel::<ControlEvent>(4);
        let (engine_tx, h) = run_engine(HashMap::from([(Exchange::Binance, control_tx)]));

        let levels = BookLevels {
            asks: vec![(price(10100), qty(1000))],
            bids: vec![(price(10000), qty(1000))],
        };
        let mut ref_book = LocalBook::new();
        ref_book.apply_snapshot(levels.clone());
        let good = compute_checksum(ref_book.top_n_bids(25), ref_book.top_n_asks(25));

        engine_tx
            .blocking_send(EngineMessage::Apply(EventEnvelope {
                exchange: Exchange::Binance,
                event: NormalizedEvent::Book(
                    BookEventType::Snapshot,
                    NormalizedBookData {
                        instrument: Instrument("BTC".to_string()),
                        levels,
                        checksum: Some(good.wrapping_add(7)),
                    },
                ),
            }))
            .unwrap();
        drop(engine_tx);

        assert!(matches!(
            control_rx.blocking_recv(),
            Some(ControlEvent::Resync)
        ));
        h.join().unwrap();
    }

    #[test]
    fn update_without_snapshot_does_not_resync() {
        let (control_tx, mut control_rx) = mpsc::channel::<ControlEvent>(4);
        let (engine_tx, h) = run_engine(HashMap::from([(Exchange::Binance, control_tx)]));

        engine_tx
            .blocking_send(EngineMessage::Apply(EventEnvelope {
                exchange: Exchange::Binance,
                event: NormalizedEvent::Book(
                    BookEventType::Update,
                    NormalizedBookData {
                        instrument: Instrument("BTC".to_string()),
                        levels: BookLevels {
                            asks: vec![(price(10100), qty(1000))],
                            bids: vec![(price(10000), qty(1000))],
                        },
                        checksum: None,
                    },
                ),
            }))
            .unwrap();
        drop(engine_tx);
        h.join().unwrap();

        assert!(control_rx.try_recv().is_err());
    }

    #[test]
    fn unknown_exchange_apply_is_skipped_without_panic() {
        let (control_tx, _control_rx) = mpsc::channel::<ControlEvent>(4);
        let (engine_tx, h) = run_engine(HashMap::from([(Exchange::Binance, control_tx)]));

        engine_tx
            .blocking_send(EngineMessage::Apply(EventEnvelope {
                exchange: Exchange::Okx,
                event: NormalizedEvent::Book(
                    BookEventType::Snapshot,
                    NormalizedBookData {
                        instrument: Instrument("BTC".to_string()),
                        levels: BookLevels {
                            asks: vec![(price(10100), qty(1000))],
                            bids: vec![(price(10000), qty(1000))],
                        },
                        checksum: None,
                    },
                ),
            }))
            .unwrap();
        drop(engine_tx);
        h.join().unwrap();
    }

    #[test]
    fn snapshot_then_query_book_returns_applied_levels() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (control_tx, _control_rx) = mpsc::channel::<ControlEvent>(4);
        let (engine_tx, h) = run_engine(HashMap::from([(Exchange::Binance, control_tx)]));

        let inst = Instrument("ETH-PERP".to_string());
        let levels = BookLevels {
            asks: vec![(price(10150), qty(1000)), (price(10200), qty(2000))],
            bids: vec![(price(10100), qty(3000))],
        };

        engine_tx
            .blocking_send(EngineMessage::Apply(EventEnvelope {
                exchange: Exchange::Binance,
                event: NormalizedEvent::Book(
                    BookEventType::Snapshot,
                    NormalizedBookData {
                        instrument: inst.clone(),
                        levels: levels.clone(),
                        checksum: None,
                    },
                ),
            }))
            .unwrap();

        let (reply_tx, reply_rx) = oneshot::channel();
        engine_tx
            .blocking_send(EngineMessage::Query(EngineQuery::Book {
                exchange: Exchange::Binance,
                instrument: inst.clone(),
                depth: 10,
                reply_to: reply_tx,
            }))
            .unwrap();

        let view = rt.block_on(reply_rx).unwrap().expect("book query ok");

        assert_eq!(view.exchange, Exchange::Binance);
        assert_eq!(view.instrument, inst);
        assert_eq!(view.asks.len(), 2);
        assert_eq!(view.bids.len(), 1);

        drop(engine_tx);
        h.join().unwrap();
    }

    #[test]
    fn random_apply_sequence_checksum_and_query_stay_consistent() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        use rand::{Rng, SeedableRng, rngs::StdRng};

        let mut rng = StdRng::seed_from_u64(0xE_71_71_E);
        let (control_tx, mut control_rx) = mpsc::channel::<ControlEvent>(64);
        let (engine_tx, h) = run_engine(HashMap::from([(Exchange::Bitget, control_tx)]));

        let inst = Instrument("RNG-PERP".to_string());
        let mut ref_book = LocalBook::new();

        let mut asks = Vec::new();
        let mut bids = Vec::new();
        for _ in 0..80 {
            asks.push((
                price(rng.gen_range(10000..10500)),
                qty(rng.gen_range(1..500_000)),
            ));
        }
        for _ in 0..80 {
            bids.push((
                price(rng.gen_range(9900..10000)),
                qty(rng.gen_range(1..500_000)),
            ));
        }
        let snap = BookLevels { asks, bids };
        ref_book.apply_snapshot(snap.clone());

        engine_tx
            .blocking_send(EngineMessage::Apply(EventEnvelope {
                exchange: Exchange::Bitget,
                event: NormalizedEvent::Book(
                    BookEventType::Snapshot,
                    NormalizedBookData {
                        instrument: inst.clone(),
                        levels: snap,
                        checksum: None,
                    },
                ),
            }))
            .unwrap();

        for _ in 0..200 {
            let mut au = Vec::new();
            let mut bu = Vec::new();
            for _ in 0..rng.gen_range(0..8) {
                let p = price(rng.gen_range(9800..10600));
                let q = if rng.gen_bool(0.2) {
                    qty(0)
                } else {
                    qty(rng.gen_range(1..500_000))
                };
                au.push((p, q));
            }
            for _ in 0..rng.gen_range(0..8) {
                let p = price(rng.gen_range(9800..10100));
                let q = if rng.gen_bool(0.2) {
                    qty(0)
                } else {
                    qty(rng.gen_range(1..500_000))
                };
                bu.push((p, q));
            }
            let upd = BookLevels { asks: au, bids: bu };
            ref_book.apply_update(upd.clone());
            let cs = compute_checksum(ref_book.top_n_bids(25), ref_book.top_n_asks(25));

            engine_tx
                .blocking_send(EngineMessage::Apply(EventEnvelope {
                    exchange: Exchange::Bitget,
                    event: NormalizedEvent::Book(
                        BookEventType::Update,
                        NormalizedBookData {
                            instrument: inst.clone(),
                            levels: upd,
                            checksum: Some(cs),
                        },
                    ),
                }))
                .unwrap();

            let (reply_tx, reply_rx) = oneshot::channel();
            engine_tx
                .blocking_send(EngineMessage::Query(EngineQuery::Book {
                    exchange: Exchange::Bitget,
                    instrument: inst.clone(),
                    depth: 25,
                    reply_to: reply_tx,
                }))
                .unwrap();

            let view = rt.block_on(reply_rx).unwrap().expect("book query ok");

            assert_eq!(view.asks, ref_book.top_n_asks(25));
            assert_eq!(view.bids, ref_book.top_n_bids(25));
        }

        assert!(control_rx.try_recv().is_err());

        drop(engine_tx);
        h.join().unwrap();
    }
}
