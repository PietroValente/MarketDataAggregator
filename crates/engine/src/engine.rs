use std::collections::HashMap;
use md_core::{events::{BookEventType, ControlEvent, EventEnvelope, NormalizedEvent, NormalizedQuery}, logging::types::Component, types::Exchange};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;

use crate::exchange_state::{ExchangeState, ExchangeStateError};

pub struct Engine {
    exchanges: HashMap<Exchange, ExchangeState>,
    rx: Receiver<EventEnvelope>
}

impl Engine {
    pub fn new(rx: Receiver<EventEnvelope>, exchanges_controller: HashMap<Exchange, Sender<ControlEvent>>) -> Self  {
        let mut map = HashMap::new();
        for (exchange, control_tx) in exchanges_controller {
            map.insert(exchange, ExchangeState::new(exchange, control_tx));
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
                NormalizedEvent::ApplyStatus(data) => {
                    exchange_state.apply_status(data);
                },
                NormalizedEvent::GetStatus => {
                    exchange_state.get_status();
                },
                NormalizedEvent::Book(event_type, book_data) => {
                    match event_type {
                        BookEventType::Snapshot => {
                            if let Err(e) = exchange_state.apply_snapshot(book_data) {
                                error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "apply_snapshot error");
                                match e {
                                    ExchangeStateError::InstrumentNotFound(_) => {
                                        continue;
                                    },
                                    ExchangeStateError::ChecksumError { instrument: _, source: _ } => {
                                        exchange_state.resync();
                                    }
                                }
                            }
                        },
                        BookEventType::Update => {
                            if let Err(e) = exchange_state.apply_update(book_data) {
                                error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "apply_update error");
                                match e {
                                    ExchangeStateError::InstrumentNotFound(_) => {
                                        continue;
                                    },
                                    ExchangeStateError::ChecksumError { instrument: _, source: _ } => {
                                        exchange_state.resync();
                                    }
                                }
                            }
                        }
                    }
                },
                NormalizedEvent::Query(query) => {
                    match query {
                        NormalizedQuery::TopAsk(reply_to, data) => {
                            match exchange_state.top_n_ask(&data) {
                                Ok(vec) => {
                                    if let Err(e) = reply_to.send(vec) {
                                        error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "top_n_ask error")
                                    }
                                },
                                Err(e) => {
                                    error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "top_n_ask error");
                                    if let Err(e) = reply_to.send(Vec::new()) {
                                        error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "top_n_ask error")
                                    }
                                }
                            }
                        },
                        NormalizedQuery::TopBid(reply_to, data) => {
                            match exchange_state.top_n_bid(&data) {
                                Ok(vec) => {
                                    if let Err(e) = reply_to.send(vec) {
                                        error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "top_n_bid error")
                                    }
                                },
                                Err(e) => {
                                    error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "top_n_bid error");
                                    if let Err(e) = reply_to.send(Vec::new()) {
                                        error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "top_n_bid error")
                                    }
                                }
                            }
                        },
                        NormalizedQuery::GetStatus(reply_to) => {
                            let status = exchange_state.get_status();
                            if let Err(e) = reply_to.send(status) {
                                error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "get_status error")
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
        events::{NormalizedBookData, NormalizedTop},
        types::{ExchangeStatus, Instrument, Price, Qty},
    };
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use rust_decimal::Decimal;
    use std::{collections::BTreeMap, thread};
    use tokio::sync::{mpsc, oneshot};

    fn instrument(v: &str) -> Instrument {
        Instrument(v.to_owned())
    }

    fn price(v: i64) -> Price {
        Price(Decimal::new(v, 2))
    }

    fn qty(v: i64) -> Qty {
        Qty(Decimal::new(v, 3))
    }

    fn top_ask_query(exchange: Exchange, instrument: Instrument, n: usize) -> (oneshot::Receiver<Vec<BookLevel>>, EventEnvelope) {
        let (tx, rx) = oneshot::channel();
        let event = EventEnvelope {
            exchange,
            event: NormalizedEvent::Query(NormalizedQuery::TopAsk(
                tx,
                NormalizedTop { instrument, n },
            )),
        };
        (rx, event)
    }

    fn top_bid_query(exchange: Exchange, instrument: Instrument, n: usize) -> (oneshot::Receiver<Vec<BookLevel>>, EventEnvelope) {
        let (tx, rx) = oneshot::channel();
        let event = EventEnvelope {
            exchange,
            event: NormalizedEvent::Query(NormalizedQuery::TopBid(
                tx,
                NormalizedTop { instrument, n },
            )),
        };
        (rx, event)
    }

    #[test]
    fn top_queries_return_empty_when_instrument_is_missing() {
        let (event_tx, event_rx) = mpsc::channel(64);
        let (control_tx, _control_rx) = mpsc::channel(8);
        let exchanges = HashMap::from([(Exchange::Binance, control_tx)]);

        let mut engine = Engine::new(event_rx, exchanges);
        let handle = thread::spawn(move || engine.run());

        let (ask_rx, ask_event) = top_ask_query(Exchange::Binance, instrument("BTCUSDT"), 10);
        event_tx.blocking_send(ask_event).unwrap();
        assert_eq!(ask_rx.blocking_recv().unwrap(), Vec::<BookLevel>::new());

        let (bid_rx, bid_event) = top_bid_query(Exchange::Binance, instrument("BTCUSDT"), 10);
        event_tx.blocking_send(bid_event).unwrap();
        assert_eq!(bid_rx.blocking_recv().unwrap(), Vec::<BookLevel>::new());

        drop(event_tx);
        handle.join().unwrap();
    }

    #[test]
    fn routes_book_and_query_events_per_exchange() {
        let (event_tx, event_rx) = mpsc::channel(64);
        let (binance_ctrl_tx, _binance_ctrl_rx) = mpsc::channel(8);
        let (okx_ctrl_tx, _okx_ctrl_rx) = mpsc::channel(8);
        let exchanges = HashMap::from([
            (Exchange::Binance, binance_ctrl_tx),
            (Exchange::Okx, okx_ctrl_tx),
        ]);

        let mut engine = Engine::new(event_rx, exchanges);
        let handle = thread::spawn(move || engine.run());

        let btc = instrument("BTCUSDT");
        let eth = instrument("ETHUSDT");

        event_tx.blocking_send(EventEnvelope {
            exchange: Exchange::Binance,
            event: NormalizedEvent::Book(
                BookEventType::Snapshot,
                NormalizedBookData {
                    instrument: btc.clone(),
                    levels: BookLevels {
                        asks: vec![(price(10100), qty(1000))],
                        bids: vec![(price(10000), qty(2000))],
                    },
                    checksum: None,
                },
            ),
        }).unwrap();

        event_tx.blocking_send(EventEnvelope {
            exchange: Exchange::Okx,
            event: NormalizedEvent::Book(
                BookEventType::Snapshot,
                NormalizedBookData {
                    instrument: eth.clone(),
                    levels: BookLevels {
                        asks: vec![(price(20100), qty(3000))],
                        bids: vec![(price(20000), qty(4000))],
                    },
                    checksum: None,
                },
            ),
        }).unwrap();

        let (ask_btc_rx, ask_btc_event) = top_ask_query(Exchange::Binance, btc.clone(), 5);
        event_tx.blocking_send(ask_btc_event).unwrap();
        assert_eq!(
            ask_btc_rx.blocking_recv().unwrap(),
            vec![BookLevel::new(qty(1000), price(10100))]
        );

        let (bid_eth_rx, bid_eth_event) = top_bid_query(Exchange::Okx, eth.clone(), 5);
        event_tx.blocking_send(bid_eth_event).unwrap();
        assert_eq!(
            bid_eth_rx.blocking_recv().unwrap(),
            vec![BookLevel::new(qty(4000), price(20000))]
        );

        drop(event_tx);
        handle.join().unwrap();
    }

    #[test]
    fn checksum_error_triggers_resync_control_event() {
        let (event_tx, event_rx) = mpsc::channel(64);
        let (control_tx, mut control_rx) = mpsc::channel(8);
        let exchanges = HashMap::from([(Exchange::Okx, control_tx)]);

        let mut engine = Engine::new(event_rx, exchanges);
        let handle = thread::spawn(move || engine.run());

        event_tx.blocking_send(EventEnvelope {
            exchange: Exchange::Okx,
            event: NormalizedEvent::Book(
                BookEventType::Snapshot,
                NormalizedBookData {
                    instrument: instrument("BTCUSDT"),
                    levels: BookLevels {
                        asks: vec![(price(10100), qty(1000))],
                        bids: vec![(price(10000), qty(1000))],
                    },
                    checksum: Some(i32::MIN),
                },
            ),
        }).unwrap();

        assert!(matches!(control_rx.blocking_recv(), Some(ControlEvent::Resync)));

        drop(event_tx);
        handle.join().unwrap();
    }

    #[test]
    fn random_event_stream_keeps_top_of_book_consistent() {
        let (event_tx, event_rx) = mpsc::channel(256);
        let (control_tx, _control_rx) = mpsc::channel(8);
        let exchanges = HashMap::from([(Exchange::Binance, control_tx)]);

        let mut engine = Engine::new(event_rx, exchanges);
        let handle = thread::spawn(move || engine.run());

        let mut rng = StdRng::seed_from_u64(0xC0DE_FEED);
        let market = instrument("BTCUSDT");
        let mut asks_ref: BTreeMap<Price, Qty> = BTreeMap::new();
        let mut bids_ref: BTreeMap<Price, Qty> = BTreeMap::new();

        // Initialize market in engine before updates.
        event_tx.blocking_send(EventEnvelope {
            exchange: Exchange::Binance,
            event: NormalizedEvent::Book(
                BookEventType::Snapshot,
                NormalizedBookData {
                    instrument: market.clone(),
                    levels: BookLevels {
                        asks: vec![],
                        bids: vec![],
                    },
                    checksum: None,
                },
            ),
        }).unwrap();

        for _ in 0..1200 {
            let mut asks = Vec::new();
            let mut bids = Vec::new();
            let ask_batch = rng.gen_range(0..8);
            let bid_batch = rng.gen_range(0..8);

            for _ in 0..ask_batch {
                let p = price(rng.gen_range(10000..11000));
                let q = if rng.gen_bool(0.35) { qty(0) } else { qty(rng.gen_range(1..250_000)) };
                asks.push((p, q));
                if q.is_zero() {
                    asks_ref.remove(&p);
                } else {
                    asks_ref.insert(p, q);
                }
            }
            for _ in 0..bid_batch {
                let p = price(rng.gen_range(9000..10000));
                let q = if rng.gen_bool(0.35) { qty(0) } else { qty(rng.gen_range(1..250_000)) };
                bids.push((p, q));
                if q.is_zero() {
                    bids_ref.remove(&p);
                } else {
                    bids_ref.insert(p, q);
                }
            }

            event_tx.blocking_send(EventEnvelope {
                exchange: Exchange::Binance,
                event: NormalizedEvent::Book(
                    BookEventType::Update,
                    NormalizedBookData {
                        instrument: market.clone(),
                        levels: BookLevels { asks, bids },
                        checksum: None,
                    },
                ),
            }).unwrap();

            // Periodically assert observable output from engine.
            if rng.gen_bool(0.2) {
                let (ask_rx, ask_event) = top_ask_query(Exchange::Binance, market.clone(), 1);
                event_tx.blocking_send(ask_event).unwrap();
                let got_ask = ask_rx.blocking_recv().unwrap();
                let expected_ask: Vec<BookLevel> = asks_ref
                    .iter()
                    .take(1)
                    .map(|(p, q)| BookLevel::new(*q, *p))
                    .collect();
                assert_eq!(got_ask, expected_ask);

                let (bid_rx, bid_event) = top_bid_query(Exchange::Binance, market.clone(), 1);
                event_tx.blocking_send(bid_event).unwrap();
                let got_bid = bid_rx.blocking_recv().unwrap();
                let expected_bid: Vec<BookLevel> = bids_ref
                    .iter()
                    .rev()
                    .take(1)
                    .map(|(p, q)| BookLevel::new(*q, *p))
                    .collect();
                assert_eq!(got_bid, expected_bid);
            }
        }

        let (status_tx, status_rx) = oneshot::channel();
        event_tx.blocking_send(EventEnvelope {
            exchange: Exchange::Binance,
            event: NormalizedEvent::Query(NormalizedQuery::GetStatus(status_tx)),
        }).unwrap();
        assert!(matches!(
            status_rx.blocking_recv().unwrap(),
            ExchangeStatus::Initializing(_)
        ));

        drop(event_tx);
        handle.join().unwrap();
    }
}