use std::collections::HashMap;
use md_core::{events::{ControlEvent, EventEnvelope, NormalizedEvent, NormalizedQuery, NormalizedTop}, types::{Exchange, Instrument}};
use tokio::sync::{mpsc::{Receiver, Sender}, oneshot};
use tracing::error;

use crate::exchange_state::{ExchangeState, ExchangeStateError};

pub struct Engine {
    exchanges: HashMap<Exchange, ExchangeState>,
    rx: Receiver<EventEnvelope>
}

impl Engine {
    pub fn new(rx: Receiver<EventEnvelope>, control_senders: HashMap<Exchange, Sender<ControlEvent>>) -> Self  {
        let mut map = HashMap::new();
        for (exchange, sender) in control_senders {
            map.insert(exchange, ExchangeState::new(exchange, sender));
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
                error!(exchange = ?exchange, "exchange not found");
                continue;
            };

            match event {
                NormalizedEvent::Status(data) => {
                    exchange_state.apply_status(data);
                },
                NormalizedEvent::Snapshot(data) => { 
                    exchange_state.apply_snapshot(data)
                },
                NormalizedEvent::Update(data) => {
                    let instrument = data.instrument.clone();
                    if let Err(e) = exchange_state.apply_update(data) {
                        match e {
                            ExchangeStateError::InstrumentNotFound(i) => {
                                error!(exchange = ?exchange, instrument = ?i, "Instrument not found while updating");
                            },
                            ExchangeStateError::LocalBookError(e) => {
                                error!(exchange = ?exchange, error = ?e, "Local book update failed, triggering reinitialization");
                                if let Err(_e) = exchange_state.send_control_event(ControlEvent::Resync){
                                    error!(exchange = ?exchange, error = ?_e, "resync error");
                                }
                            }
                        }
                    }

                    // DEBUG section
                    if instrument == Instrument("BTCUSDT".to_string()) {
                        let (tx, _) = oneshot::channel();
                        let (tx2, _) = oneshot::channel();
                        let mut asks = exchange_state.top_n_ask(&NormalizedTop {
                            instrument: instrument.clone(),
                            n: 5,
                            reply_to: tx
                        }).unwrap();
                        let mut bids = exchange_state.top_n_bid(&NormalizedTop {
                            instrument: instrument.clone(),
                            n: 5,
                            reply_to: tx2
                        }).unwrap();
                        asks.sort_by(|a, b| b.px().partial_cmp(&a.px()).unwrap());
                        bids.sort_by(|a, b| b.px().partial_cmp(&a.px()).unwrap());
                        // print!("\x1B[2J\x1B[1;1H");
                        // println!("{}", instrument);
                        // for a in asks {
                        //     println!("{}    {}", a.px(), a.qty());
                        // }
                        // println!("--------");
                        // for b in bids {
                        //     println!("{}    {}", b.px(), b.qty());
                        // }
                    }
                },
                NormalizedEvent::Query(query) => {
                    match query {
                        NormalizedQuery::TopAsk(data) => {
                            match exchange_state.top_n_ask(&data) {
                                Ok(vec) => {
                                    if let Err(e) = data.reply_to.send(vec) {
                                        error!(exchange = ?exchange, error = ?e, "top_n_ask error")
                                    }
                                },
                                Err(e) => {
                                    error!(exchange = ?exchange, error = ?e, "top_n_ask error");
                                }
                            }
                        },
                        NormalizedQuery::TopBid(data) => {
                            match exchange_state.top_n_bid(&data) {
                                Ok(vec) => {
                                    if let Err(e) = data.reply_to.send(vec) {
                                        error!(exchange = ?exchange, error = ?e, "top_n_bid error")
                                    }
                                },
                                Err(e) => {
                                    error!(exchange = ?exchange, error = ?e, "top_n_bid error");
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
        book::{BookLevel, BookSnapshot, BookUpdate},
        events::{
            ControlEvent, EventEnvelope, NormalizedEvent, NormalizedQuery, NormalizedSnapshot,
            NormalizedTop, NormalizedUpdate,
        },
        types::{Exchange, ExchangeStatus, Instrument, Price, Qty},
    };
    use rust_decimal::Decimal;
    use std::{collections::HashMap, thread};
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

    fn assert_level(level: &BookLevel, expected_px: i64, expected_qty: i64) {
        assert_eq!(level.px(), &px(expected_px));
        assert_eq!(level.qty(), &qty(expected_qty));
    }

    fn snapshot(
        sym: &str,
        last_update_id: u64,
        bids: Vec<(Price, Qty)>,
        asks: Vec<(Price, Qty)>,
    ) -> NormalizedSnapshot {
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

    fn envelope(exchange: Exchange, event: NormalizedEvent) -> EventEnvelope {
        EventEnvelope { exchange, event }
    }

    fn spawn_engine(
        exchanges: &[Exchange],
    ) -> (
        mpsc::Sender<EventEnvelope>,
        HashMap<Exchange, mpsc::Receiver<ControlEvent>>,
        thread::JoinHandle<()>,
    ) {
        let (tx, rx) = mpsc::channel(64);

        let mut control_senders = HashMap::new();
        let mut control_receivers = HashMap::new();

        for exchange in exchanges.iter().copied() {
            let (ctrl_tx, ctrl_rx) = mpsc::channel(8);
            control_senders.insert(exchange, ctrl_tx);
            control_receivers.insert(exchange, ctrl_rx);
        }

        let mut engine = Engine::new(rx, control_senders);
        let handle = thread::spawn(move || {
            engine.run();
        });

        (tx, control_receivers, handle)
    }

    #[test]
    fn processes_snapshot_update_and_answers_top_queries() {
        let exchange = Exchange::Binance;
        let (tx, _control_rxs, handle) = spawn_engine(&[exchange]);

        tx.blocking_send(envelope(
            exchange,
            NormalizedEvent::Status(ExchangeStatus::Running),
        ))
        .unwrap();

        tx.blocking_send(envelope(
            exchange,
            NormalizedEvent::Snapshot(snapshot(
                "BTCUSDT",
                100,
                vec![(px(100), qty(2)), (px(99), qty(3))],
                vec![(px(101), qty(1)), (px(102), qty(4))],
            )),
        ))
        .unwrap();

        tx.blocking_send(envelope(
            exchange,
            NormalizedEvent::Update(update(
                "BTCUSDT",
                101,
                102,
                vec![(px(100), qty(7)), (px(98), qty(1))],
                vec![(px(101), qty(5))],
            )),
        ))
        .unwrap();

        let (bid_tx, bid_rx) = oneshot::channel();
        tx.blocking_send(envelope(
            exchange,
            NormalizedEvent::Query(NormalizedQuery::TopBid(NormalizedTop {
                instrument: instrument("BTCUSDT"),
                n: 3,
                reply_to: bid_tx,
            })),
        ))
        .unwrap();

        let (ask_tx, ask_rx) = oneshot::channel();
        tx.blocking_send(envelope(
            exchange,
            NormalizedEvent::Query(NormalizedQuery::TopAsk(NormalizedTop {
                instrument: instrument("BTCUSDT"),
                n: 2,
                reply_to: ask_tx,
            })),
        ))
        .unwrap();

        drop(tx);

        let bids = bid_rx.blocking_recv().expect("expected top bid reply");
        let asks = ask_rx.blocking_recv().expect("expected top ask reply");

        assert_eq!(bids.len(), 3);
        assert_level(&bids[0], 100, 7);
        assert_level(&bids[1], 99, 3);
        assert_level(&bids[2], 98, 1);

        assert_eq!(asks.len(), 2);
        assert_level(&asks[0], 101, 5);
        assert_level(&asks[1], 102, 4);

        handle.join().unwrap();
    }

    #[test]
    fn ignores_unknown_exchange_and_keeps_processing_valid_events() {
        let (tx, _control_rxs, handle) = spawn_engine(&[Exchange::Binance]);

        tx.blocking_send(envelope(
            Exchange::Okx,
            NormalizedEvent::Snapshot(snapshot(
                "BTCUSDT",
                10,
                vec![(px(50), qty(1))],
                vec![(px(51), qty(1))],
            )),
        ))
        .unwrap();

        tx.blocking_send(envelope(
            Exchange::Binance,
            NormalizedEvent::Snapshot(snapshot(
                "BTCUSDT",
                20,
                vec![(px(100), qty(2))],
                vec![(px(101), qty(3))],
            )),
        ))
        .unwrap();

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.blocking_send(envelope(
            Exchange::Binance,
            NormalizedEvent::Query(NormalizedQuery::TopAsk(NormalizedTop {
                instrument: instrument("BTCUSDT"),
                n: 1,
                reply_to: reply_tx,
            })),
        ))
        .unwrap();

        drop(tx);

        let asks = reply_rx.blocking_recv().expect("expected top ask reply");
        assert_eq!(asks.len(), 1);
        assert_level(&asks[0], 101, 3);

        handle.join().unwrap();
    }

    #[test]
    fn missing_instrument_update_does_not_trigger_resync() {
        let exchange = Exchange::Binance;
        let (tx, mut control_rxs, handle) = spawn_engine(&[exchange]);

        tx.blocking_send(envelope(
            exchange,
            NormalizedEvent::Update(update(
                "ETHUSDT",
                1,
                2,
                vec![(px(100), qty(1))],
                vec![],
            )),
        ))
        .unwrap();

        drop(tx);

        let control_rx = control_rxs.get_mut(&exchange).unwrap();
        let control_event = control_rx.blocking_recv();

        assert!(
            control_event.is_none(),
            "missing instrument must not trigger resync"
        );

        handle.join().unwrap();
    }

    #[test]
    fn local_book_out_of_sync_triggers_resync() {
        let exchange = Exchange::Binance;
        let (tx, mut control_rxs, handle) = spawn_engine(&[exchange]);

        tx.blocking_send(envelope(
            exchange,
            NormalizedEvent::Snapshot(snapshot(
                "BTCUSDT",
                10,
                vec![(px(99), qty(1))],
                vec![(px(101), qty(1))],
            )),
        ))
        .unwrap();

        tx.blocking_send(envelope(
            exchange,
            NormalizedEvent::Update(update(
                "BTCUSDT",
                12, // gap: expected <= 11
                13,
                vec![(px(100), qty(2))],
                vec![(px(102), qty(2))],
            )),
        ))
        .unwrap();

        drop(tx);

        let control_rx = control_rxs.get_mut(&exchange).unwrap();
        let evt = control_rx
            .blocking_recv()
            .expect("expected resync control event");

        assert!(matches!(evt, ControlEvent::Resync));

        handle.join().unwrap();
    }

    #[test]
    fn query_for_missing_instrument_drops_reply_sender_without_panicking() {
        let exchange = Exchange::Binance;
        let (tx, _control_rxs, handle) = spawn_engine(&[exchange]);

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.blocking_send(envelope(
            exchange,
            NormalizedEvent::Query(NormalizedQuery::TopBid(NormalizedTop {
                instrument: instrument("ETHUSDT"),
                n: 5,
                reply_to: reply_tx,
            })),
        ))
        .unwrap();

        drop(tx);

        let res = reply_rx.blocking_recv();
        assert!(
            res.is_err(),
            "engine should not send a reply when the instrument is missing"
        );

        handle.join().unwrap();
    }
}