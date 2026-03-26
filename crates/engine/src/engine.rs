use std::collections::HashMap;
use md_core::{events::{BookEventType, EventEnvelope, NormalizedEvent, NormalizedQuery}, logging::types::Component, types::Exchange};
use tokio::sync::mpsc::Receiver;
use tracing::error;

use crate::exchange_state::ExchangeState;

pub struct Engine {
    exchanges: HashMap<Exchange, ExchangeState>,
    rx: Receiver<EventEnvelope>
}

impl Engine {
    pub fn new(rx: Receiver<EventEnvelope>, exchanges: Vec<Exchange>) -> Self  {
        let mut map = HashMap::new();
        for exchange in exchanges {
            map.insert(exchange, ExchangeState::new(exchange));
        }
        Self {
            exchanges: map,
            rx
        }
    }
    pub fn run(&mut self) {
        while let Some(event_enveloped) = self.rx.blocking_recv() {
            let EventEnvelope { exchange, event} = event_enveloped;
            let Some(exchange_state) = self.exchanges.get_mut(&exchange) else {
                error!(exchange = ?exchange, component = ?Component::Engine, "exchange not found");
                continue;
            };

            match event {
                NormalizedEvent::Status(data) => {
                    exchange_state.apply_status(data);
                },
                NormalizedEvent::Book(event_type, book_data) => {
                    match event_type {
                        BookEventType::Snapshot => {
                            exchange_state.apply_snapshot(book_data);       
                        },
                        BookEventType::Update => {
                            if let Err(e) = exchange_state.apply_update(book_data) {
                                error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "Instrument not found while updating");
                            }
                        }
                    }
                },
                NormalizedEvent::Query(query) => {
                    match query {
                        NormalizedQuery::TopAsk(data) => {
                            match exchange_state.top_n_ask(&data) {
                                Ok(vec) => {
                                    if let Err(e) = data.reply_to.send(vec) {
                                        error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "top_n_ask error")
                                    }
                                },
                                Err(e) => {
                                    error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "top_n_ask error");
                                    if let Err(e) = data.reply_to.send(Vec::new()) {
                                        error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "top_n_ask error")
                                    }
                                }
                            }
                        },
                        NormalizedQuery::TopBid(data) => {
                            match exchange_state.top_n_bid(&data) {
                                Ok(vec) => {
                                    if let Err(e) = data.reply_to.send(vec) {
                                        error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "top_n_bid error")
                                    }
                                },
                                Err(e) => {
                                    error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "top_n_bid error");
                                    if let Err(e) = data.reply_to.send(Vec::new()) {
                                        error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "top_n_bid error")
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use md_core::{
        book::{BookLevel, BookLevels},
        events::{BookEventType, EventEnvelope, NormalizedBookData, NormalizedEvent, NormalizedQuery, NormalizedTop},
        types::{Exchange, ExchangeStatus, Instrument, Price, Qty},
    };
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use rust_decimal::Decimal;
    use std::collections::{BTreeMap, HashMap};
    use tokio::sync::{mpsc, oneshot};

    fn instrument(name: &str) -> Instrument {
        Instrument(name.to_string())
    }

    fn price(v: i64) -> Price {
        Price(Decimal::new(v, 2))
    }

    fn qty(v: i64) -> Qty {
        Qty(Decimal::new(v, 3))
    }

    fn envelope(exchange: Exchange, event: NormalizedEvent) -> EventEnvelope {
        EventEnvelope { exchange, event }
    }

    #[test]
    fn query_on_missing_instrument_returns_empty_vec() {
        let (tx, rx) = mpsc::channel(32);
        let mut engine = Engine::new(rx, vec![Exchange::Binance]);

        let inst = instrument("BTCUSDT");
        let (reply_tx, reply_rx) = oneshot::channel::<Vec<BookLevel>>();

        tx.blocking_send(envelope(
            Exchange::Binance,
            NormalizedEvent::Query(NormalizedQuery::TopAsk(NormalizedTop {
                instrument: inst,
                n: 10,
                reply_to: reply_tx,
            })),
        ))
        .expect("send query");
        drop(tx);

        engine.run();
        let result = reply_rx.blocking_recv().expect("reply received");
        assert!(result.is_empty());
    }

    #[test]
    fn engine_handles_unknown_exchange_without_affecting_known_exchange() {
        let (tx, rx) = mpsc::channel(64);
        let mut engine = Engine::new(rx, vec![Exchange::Binance]);
        let inst = instrument("ETHUSDT");

        // Unknown exchange event should be ignored.
        tx.blocking_send(envelope(
            Exchange::Bitget,
            NormalizedEvent::Book(
                BookEventType::Snapshot,
                NormalizedBookData {
                    instrument: inst.clone(),
                    levels: BookLevels {
                        asks: vec![(price(21000), qty(1000))],
                        bids: vec![(price(20900), qty(1000))],
                    },
                },
            ),
        ))
        .expect("send unknown exchange snapshot");

        // Known exchange snapshot + query must still work.
        tx.blocking_send(envelope(
            Exchange::Binance,
            NormalizedEvent::Book(
                BookEventType::Snapshot,
                NormalizedBookData {
                    instrument: inst.clone(),
                    levels: BookLevels {
                        asks: vec![(price(20000), qty(1200))],
                        bids: vec![(price(19900), qty(1300))],
                    },
                },
            ),
        ))
        .expect("send known exchange snapshot");

        let (reply_tx, reply_rx) = oneshot::channel::<Vec<BookLevel>>();
        tx.blocking_send(envelope(
            Exchange::Binance,
            NormalizedEvent::Query(NormalizedQuery::TopAsk(NormalizedTop {
                instrument: inst,
                n: 5,
                reply_to: reply_tx,
            })),
        ))
        .expect("send top ask query");
        drop(tx);

        engine.run();
        let asks = reply_rx.blocking_recv().expect("reply received");
        assert_eq!(asks, vec![BookLevel::new(qty(1200), price(20000))]);
    }

    #[test]
    fn update_before_snapshot_followed_by_snapshot_recovers_correctly() {
        let (tx, rx) = mpsc::channel(64);
        let mut engine = Engine::new(rx, vec![Exchange::Binance]);
        let inst = instrument("SOLUSDT");

        // First update is invalid (instrument not initialized); engine should handle and continue.
        tx.blocking_send(envelope(
            Exchange::Binance,
            NormalizedEvent::Book(
                BookEventType::Update,
                NormalizedBookData {
                    instrument: inst.clone(),
                    levels: BookLevels {
                        asks: vec![(price(1000), qty(1000))],
                        bids: vec![(price(900), qty(1000))],
                    },
                },
            ),
        ))
        .expect("send update before snapshot");

        tx.blocking_send(envelope(
            Exchange::Binance,
            NormalizedEvent::Book(
                BookEventType::Snapshot,
                NormalizedBookData {
                    instrument: inst.clone(),
                    levels: BookLevels {
                        asks: vec![(price(1100), qty(2000))],
                        bids: vec![(price(800), qty(3000))],
                    },
                },
            ),
        ))
        .expect("send snapshot");

        let (reply_tx, reply_rx) = oneshot::channel::<Vec<BookLevel>>();
        tx.blocking_send(envelope(
            Exchange::Binance,
            NormalizedEvent::Query(NormalizedQuery::TopBid(NormalizedTop {
                instrument: inst,
                n: 3,
                reply_to: reply_tx,
            })),
        ))
        .expect("send top bid query");
        drop(tx);

        engine.run();
        let bids = reply_rx.blocking_recv().expect("reply received");
        assert_eq!(bids, vec![BookLevel::new(qty(3000), price(800))]);
    }

    #[test]
    fn random_event_stream_matches_reference_for_queries() {
        let (tx, rx) = mpsc::channel(20000);
        let mut engine = Engine::new(rx, vec![Exchange::Binance, Exchange::Bybit]);
        let mut rng = StdRng::seed_from_u64(0xDEADBEEF_u64);

        let instruments = [
            instrument("BTCUSDT"),
            instrument("ETHUSDT"),
            instrument("SOLUSDT"),
            instrument("XRPUSDT"),
            instrument("ADAUSDT"),
        ];
        let exchanges = [Exchange::Binance, Exchange::Bybit];

        let mut ask_ref: HashMap<(Exchange, Instrument), BTreeMap<Price, Qty>> = HashMap::new();
        let mut bid_ref: HashMap<(Exchange, Instrument), BTreeMap<Price, Qty>> = HashMap::new();

        // Bootstrap snapshots for every exchange/instrument pair so updates are valid.
        for ex in exchanges {
            for inst in &instruments {
                let mut asks = Vec::new();
                let mut bids = Vec::new();
                let mut ask_map = BTreeMap::new();
                let mut bid_map = BTreeMap::new();

                for _ in 0..80 {
                    let p = price(rng.gen_range(10000..13000));
                    let q = qty(rng.gen_range(1..1_000_000));
                    asks.push((p, q));
                    ask_map.insert(p, q);
                }
                for _ in 0..80 {
                    let p = price(rng.gen_range(7000..10000));
                    let q = qty(rng.gen_range(1..1_000_000));
                    bids.push((p, q));
                    bid_map.insert(p, q);
                }

                tx.blocking_send(envelope(
                    ex,
                    NormalizedEvent::Book(
                        BookEventType::Snapshot,
                        NormalizedBookData {
                            instrument: inst.clone(),
                            levels: BookLevels { asks, bids },
                        },
                    ),
                ))
                .expect("send bootstrap snapshot");

                ask_ref.insert((ex, inst.clone()), ask_map);
                bid_ref.insert((ex, inst.clone()), bid_map);
            }
        }

        let mut ask_queries = Vec::new();
        let mut bid_queries = Vec::new();
        let mut expected_asks = Vec::new();
        let mut expected_bids = Vec::new();

        for _step in 0..3000 {
            let ex = exchanges[rng.gen_range(0..exchanges.len())];
            let inst = instruments[rng.gen_range(0..instruments.len())].clone();

            // occasional status events to cover non-book path
            if rng.gen_bool(0.02) {
                let status = if rng.gen_bool(0.5) {
                    ExchangeStatus::Running
                } else {
                    ExchangeStatus::Initializing
                };
                tx.blocking_send(envelope(ex, NormalizedEvent::Status(status)))
                    .expect("send status");
            }

            let snapshot_event = rng.gen_bool(0.03);
            if snapshot_event {
                let ask_count = rng.gen_range(0..40);
                let bid_count = rng.gen_range(0..40);
                let mut asks = Vec::new();
                let mut bids = Vec::new();
                let mut ask_map = BTreeMap::new();
                let mut bid_map = BTreeMap::new();

                for _ in 0..ask_count {
                    let p = price(rng.gen_range(10000..14000));
                    let q = qty(rng.gen_range(1..1_000_000));
                    asks.push((p, q));
                    ask_map.insert(p, q);
                }
                for _ in 0..bid_count {
                    let p = price(rng.gen_range(5500..10000));
                    let q = qty(rng.gen_range(1..1_000_000));
                    bids.push((p, q));
                    bid_map.insert(p, q);
                }

                tx.blocking_send(envelope(
                    ex,
                    NormalizedEvent::Book(
                        BookEventType::Snapshot,
                        NormalizedBookData {
                            instrument: inst.clone(),
                            levels: BookLevels {
                                asks: asks.clone(),
                                bids: bids.clone(),
                            },
                        },
                    ),
                ))
                .expect("send random snapshot");

                ask_ref.insert((ex, inst.clone()), ask_map);
                bid_ref.insert((ex, inst.clone()), bid_map);
            } else {
                let ask_batch = rng.gen_range(0..20);
                let bid_batch = rng.gen_range(0..20);
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
                        .get_mut(&(ex, inst.clone()))
                        .expect("reference book exists");
                    if q.is_zero() {
                        map.remove(&p);
                    } else {
                        map.insert(p, q);
                    }
                }

                for _ in 0..bid_batch {
                    let p = price(rng.gen_range(5000..10500));
                    let remove = rng.gen_bool(0.35);
                    let q = if remove {
                        qty(0)
                    } else {
                        qty(rng.gen_range(1..1_000_000))
                    };
                    bid_updates.push((p, q));
                    let map = bid_ref
                        .get_mut(&(ex, inst.clone()))
                        .expect("reference book exists");
                    if q.is_zero() {
                        map.remove(&p);
                    } else {
                        map.insert(p, q);
                    }
                }

                tx.blocking_send(envelope(
                    ex,
                    NormalizedEvent::Book(
                        BookEventType::Update,
                        NormalizedBookData {
                            instrument: inst.clone(),
                            levels: BookLevels {
                                asks: ask_updates,
                                bids: bid_updates,
                            },
                        },
                    ),
                ))
                .expect("send random update");
            }

            // Periodically ask queries and compare with reference oracle after engine run.
            if rng.gen_bool(0.25) {
                let n = rng.gen_range(0..30);
                let (ask_tx, ask_rx) = oneshot::channel::<Vec<BookLevel>>();
                let (bid_tx, bid_rx) = oneshot::channel::<Vec<BookLevel>>();

                tx.blocking_send(envelope(
                    ex,
                    NormalizedEvent::Query(NormalizedQuery::TopAsk(NormalizedTop {
                        instrument: inst.clone(),
                        n,
                        reply_to: ask_tx,
                    })),
                ))
                .expect("send ask query");
                tx.blocking_send(envelope(
                    ex,
                    NormalizedEvent::Query(NormalizedQuery::TopBid(NormalizedTop {
                        instrument: inst.clone(),
                        n,
                        reply_to: bid_tx,
                    })),
                ))
                .expect("send bid query");

                let exp_asks: Vec<BookLevel> = ask_ref
                    .get(&(ex, inst.clone()))
                    .expect("reference book exists")
                    .iter()
                    .take(n)
                    .map(|(p, q)| BookLevel::new(*q, *p))
                    .collect();
                let exp_bids: Vec<BookLevel> = bid_ref
                    .get(&(ex, inst.clone()))
                    .expect("reference book exists")
                    .iter()
                    .rev()
                    .take(n)
                    .map(|(p, q)| BookLevel::new(*q, *p))
                    .collect();

                ask_queries.push(ask_rx);
                bid_queries.push(bid_rx);
                expected_asks.push(exp_asks);
                expected_bids.push(exp_bids);
            }
        }

        // Explicit missing-instrument queries should return empty vec.
        let missing = instrument("NOT_INITIALIZED");
        let (m_ask_tx, m_ask_rx) = oneshot::channel::<Vec<BookLevel>>();
        let (m_bid_tx, m_bid_rx) = oneshot::channel::<Vec<BookLevel>>();
        tx.blocking_send(envelope(
            Exchange::Binance,
            NormalizedEvent::Query(NormalizedQuery::TopAsk(NormalizedTop {
                instrument: missing.clone(),
                n: 10,
                reply_to: m_ask_tx,
            })),
        ))
        .expect("send missing ask query");
        tx.blocking_send(envelope(
            Exchange::Binance,
            NormalizedEvent::Query(NormalizedQuery::TopBid(NormalizedTop {
                instrument: missing,
                n: 10,
                reply_to: m_bid_tx,
            })),
        ))
        .expect("send missing bid query");

        ask_queries.push(m_ask_rx);
        bid_queries.push(m_bid_rx);
        expected_asks.push(Vec::new());
        expected_bids.push(Vec::new());

        drop(tx);
        engine.run();

        for (rx, expected) in ask_queries.into_iter().zip(expected_asks.into_iter()) {
            let got = rx.blocking_recv().expect("ask query reply");
            assert_eq!(got, expected);
        }
        for (rx, expected) in bid_queries.into_iter().zip(expected_bids.into_iter()) {
            let got = rx.blocking_recv().expect("bid query reply");
            assert_eq!(got, expected);
        }
    }
}