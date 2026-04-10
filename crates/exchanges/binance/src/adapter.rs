use std::{
    collections::HashMap,
    error::Error,
    mem,
    time::{SystemTime, UNIX_EPOCH},
};

use md_core::{
    book::BookLevels,
    events::{BookEventType, ControlEvent, EngineMessage, NormalizedBookData, NormalizedEvent},
    helpers::adapter::{clear_book_state, compute_status, send_normalized_event, send_status},
    traits::adapter::ExchangeAdapter,
    types::{Exchange, Instrument},
};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, warn};

use crate::types::{
    BinanceMdMsg, BookState, BookSyncStatus, ParsedBookSnapshot, ParsedBookUpdate,
    ValidateBookError, ValidateSnapshot, WsMessage,
};
pub struct BinanceAdapter {
    raw_tx: Sender<BinanceMdMsg>,
    raw_rx: Receiver<BinanceMdMsg>,
    normalized_tx: Sender<EngineMessage>,
    control_tx: Sender<ControlEvent>,
    book_states: HashMap<Instrument, BookState>,
    live_books: usize,
}

impl BinanceAdapter {
    pub fn new(
        raw_tx: Sender<BinanceMdMsg>,
        raw_rx: Receiver<BinanceMdMsg>,
        normalized_tx: Sender<EngineMessage>,
        control_tx: Sender<ControlEvent>,
    ) -> Self {
        Self {
            raw_tx,
            raw_rx,
            normalized_tx,
            control_tx,
            book_states: HashMap::new(),
            live_books: 0,
        }
    }

    fn drain_buffered_updates(&mut self, instrument: Instrument) {
        let Some(book) = self.book_states.get_mut(&instrument) else {
            error!(exchange = ?BinanceAdapter::exchange(), component = ?BinanceAdapter::component(), symbol = ?instrument, "symbol not found");
            return;
        };
        if book.status != BookSyncStatus::WaitingSnapshot {
            error!(exchange = ?BinanceAdapter::exchange(), component = ?BinanceAdapter::component(), symbol = ?instrument, status = ?book.status, "book status different from WaitingSnapshot");
            return;
        }

        for u in mem::take(&mut book.symbols_pending_snapshot) {
            if let Err(e) = self.raw_tx.blocking_send(BinanceMdMsg::WsMessage(u)) {
                error!(
                    exchange = ?BinanceAdapter::exchange(),
                    component = ?BinanceAdapter::component(),
                    error = ?e,
                    "error while sending the update event"
                );
            }
        }
        book.status = BookSyncStatus::Live;
        self.live_books += 1;
    }

    fn validate_snapshot(&mut self, payload: &ValidateSnapshot) -> Result<(), ValidateBookError> {
        let Some(book) = self.book_states.get_mut(&payload.symbol) else {
            return Err(ValidateBookError::InstrumentNotFound(
                payload.symbol.clone(),
            ));
        };
        book.last_applied_update_id = Some(payload.last_update_id);
        Ok(())
    }

    fn validate_update(&mut self, payload: &ParsedBookUpdate) -> Result<(), ValidateBookError> {
        if payload.event_type != "depthUpdate" {
            return Err(ValidateBookError::UnknownType(payload.event_type.clone()));
        }
        let Some(book) = self.book_states.get_mut(&payload.symbol) else {
            return Err(ValidateBookError::InstrumentNotFound(
                payload.symbol.clone(),
            ));
        };
        if let Ok(elapsed) = SystemTime::now().duration_since(UNIX_EPOCH) {
            let now = elapsed.as_millis() as u64;
            let latency = now.saturating_sub(payload.event_time);

            if latency > 1000 {
                warn!(
                    exchange = ?BinanceAdapter::exchange(),
                    component = ?BinanceAdapter::component(),
                    symbol = ?payload.symbol,
                    latency_ms = latency,
                    "high latency for update"
                );
            }
        }

        let Some(last_applied_update_id) = book.last_applied_update_id else {
            return Err(ValidateBookError::MissingSnapshot);
        };

        if payload.final_update_id <= last_applied_update_id {
            return Err(ValidateBookError::StaleUpdate {
                event_last_update_id: payload.final_update_id,
                book_last_update_id: last_applied_update_id,
            });
        }
        if payload.first_update_id > last_applied_update_id + 1 {
            return Err(ValidateBookError::UpdateGap {
                event_first_update_id: payload.first_update_id,
                expected_next_update_id: last_applied_update_id + 1,
            });
        }
        book.last_applied_update_id = Some(payload.final_update_id);

        Ok(())
    }

    pub fn run(&mut self) {
        while let Some(msg) = self.raw_rx.blocking_recv() {
            match msg {
                BinanceMdMsg::ResetBookState => {
                    self.live_books = 0;
                    clear_book_state(&mut self.book_states);
                    send_status::<BinanceAdapter>(
                        &self.normalized_tx,
                        compute_status(self.live_books, self.book_states.len()),
                    );
                }
                BinanceMdMsg::Instruments(list) => {
                    for i in list.iter() {
                        self.book_states.insert(i.clone(), BookState::new());
                    }
                    send_status::<BinanceAdapter>(
                        &self.normalized_tx,
                        compute_status(self.live_books, self.book_states.len()),
                    );
                }
                BinanceMdMsg::Snapshot(payload) => {
                    let Ok(parsed_snapshot) =
                        serde_json::from_slice::<ParsedBookSnapshot>(&payload.payload)
                    else {
                        let text = String::from_utf8_lossy(&payload.payload);
                        error!(exchange = ?BinanceAdapter::exchange(), component = ?BinanceAdapter::component(), symbol = ?payload.symbol, text = ?text, "error while parsing snapshot");
                        continue;
                    };
                    if let Err(e) = self.validate_snapshot(&ValidateSnapshot {
                        symbol: payload.symbol.clone(),
                        last_update_id: parsed_snapshot.last_update_id,
                    }) {
                        error!(exchange = ?BinanceAdapter::exchange(), component = ?BinanceAdapter::component(), symbol = ?payload.symbol, error = ?e, "error while validating snapshot");
                        if let Err(e) = self.control_tx.blocking_send(ControlEvent::Resync) {
                            error!(exchange = ?BinanceAdapter::exchange(), component = ?BinanceAdapter::component(), error = ?e, "error while sending resync");
                        }
                    }
                    let book_snapshot = BookLevels {
                        asks: parsed_snapshot.asks,
                        bids: parsed_snapshot.bids,
                    };
                    let snapshot_event = NormalizedEvent::Book(
                        BookEventType::Snapshot,
                        NormalizedBookData {
                            instrument: payload.symbol.clone(),
                            levels: book_snapshot,
                            checksum: None,
                        },
                    );
                    send_normalized_event::<BinanceAdapter>(&self.normalized_tx, snapshot_event);
                    self.drain_buffered_updates(payload.symbol);
                    send_status::<BinanceAdapter>(
                        &self.normalized_tx,
                        compute_status(self.live_books, self.book_states.len()),
                    );
                }
                BinanceMdMsg::WsMessage(payload) => {
                    match serde_json::from_slice::<WsMessage>(&payload) {
                        Ok(WsMessage::Confirmation(confirmation)) => {
                            if let Some(result) = confirmation.result {
                                error!(exchange = ?BinanceAdapter::exchange(), component = ?BinanceAdapter::component(), result = ?result,"subscription result different from null");
                            }
                        }
                        Err(_) => {
                            let text = String::from_utf8_lossy(&payload);
                            error!(exchange = ?BinanceAdapter::exchange(), component = ?BinanceAdapter::component(), text = ?text, "error while parsing update");
                        }
                        Ok(WsMessage::Update(update)) => {
                            let Some(book) = self.book_states.get_mut(&update.symbol) else {
                                error!(exchange = ?BinanceAdapter::exchange(), component = ?BinanceAdapter::component(), symbol = ?update.symbol, "symbol not found");
                                continue;
                            };
                            match book.status {
                                BookSyncStatus::Live => {}
                                BookSyncStatus::WaitingSnapshot => {
                                    book.symbols_pending_snapshot.push(payload);
                                    continue;
                                }
                            };

                            //process the update
                            match self.validate_update(&update) {
                                Err(
                                    e @ ValidateBookError::UpdateGap {
                                        event_first_update_id,
                                        expected_next_update_id,
                                    },
                                ) => {
                                    error!(
                                        exchange = ?BinanceAdapter::exchange(),
                                        component = ?BinanceAdapter::component(),
                                        symbol = ?update.symbol,
                                        error = ?e,
                                        event_first_update_id = event_first_update_id,
                                        expected_next_update_id = expected_next_update_id,
                                        "error while validating update"
                                    );
                                    if let Err(e) =
                                        self.control_tx.blocking_send(ControlEvent::Resync)
                                    {
                                        error!(
                                            exchange = ?BinanceAdapter::exchange(),
                                            component = ?BinanceAdapter::component(),
                                            error = ?e,
                                            "error while sending resync"
                                        );
                                    }
                                    continue;
                                }
                                Err(e @ ValidateBookError::StaleUpdate { .. }) => {
                                    warn!(
                                        exchange = ?BinanceAdapter::exchange(),
                                        component = ?BinanceAdapter::component(),
                                        symbol = ?update.symbol,
                                        error = ?e,
                                        "stale update dropped"
                                    );
                                    continue;
                                }
                                Err(e @ ValidateBookError::UnknownType(_)) => {
                                    warn!(
                                        exchange = ?BinanceAdapter::exchange(),
                                        component = ?BinanceAdapter::component(),
                                        symbol = ?update.symbol,
                                        error = ?e,
                                        "unexpected update type dropped"
                                    );
                                    continue;
                                }
                                Err(e) => {
                                    error!(
                                        exchange = ?BinanceAdapter::exchange(),
                                        component = ?BinanceAdapter::component(),
                                        symbol = ?update.symbol,
                                        error = ?e,
                                        "update validation failed"
                                    );
                                    continue;
                                }
                                Ok(()) => {}
                            }

                            let book_update = BookLevels {
                                bids: update.bids,
                                asks: update.asks,
                            };

                            let update_event = NormalizedEvent::Book(
                                BookEventType::Update,
                                NormalizedBookData {
                                    instrument: update.symbol.clone(),
                                    levels: book_update,
                                    checksum: None,
                                },
                            );
                            send_normalized_event::<BinanceAdapter>(
                                &self.normalized_tx,
                                update_event,
                            );
                        }
                    }
                }
            }
        }
    }
}

impl ExchangeAdapter for BinanceAdapter {
    type SnapshotPayload = ValidateSnapshot;
    type UpdatePayload = ParsedBookUpdate;

    fn exchange() -> Exchange {
        Exchange::Binance
    }

    fn validate_snapshot(
        &mut self,
        payload: &Self::SnapshotPayload,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        BinanceAdapter::validate_snapshot(self, payload)
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)
    }

    fn validate_update(
        &mut self,
        payload: &Self::UpdatePayload,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        BinanceAdapter::validate_update(self, payload)
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)
    }

    fn run(&mut self) {
        BinanceAdapter::run(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use md_core::types::RawMdMsg;
    use tokio::sync::mpsc;

    fn instrument(v: &str) -> Instrument {
        Instrument(v.to_owned())
    }

    fn make_update_msg(symbol: &str, first_id: u64, final_id: u64, event_type: &str) -> RawMdMsg {
        let payload = format!(
            r#"{{"e":"{event_type}","E":1,"s":"{symbol}","U":{first_id},"u":{final_id},"b":[["100.00","1.0"]],"a":[["101.00","2.0"]]}}"#
        );
        RawMdMsg(payload.into_bytes())
    }

    #[test]
    fn buffered_updates_are_drained_after_snapshot() {
        // Use a dedicated output channel as adapter.raw_tx so we can assert drained messages.
        let (drain_tx, mut drain_rx) = mpsc::channel(128);
        let (_dummy_in_tx, dummy_in_rx) = mpsc::channel(1);
        let (normalized_tx, _normalized_rx) = mpsc::channel(8);
        let (control_tx, _control_rx) = mpsc::channel(16);
        let mut adapter = BinanceAdapter::new(drain_tx, dummy_in_rx, normalized_tx, control_tx);
        let symbol = instrument("BTCUSDT");

        let book = adapter
            .book_states
            .entry(symbol.clone())
            .or_insert_with(BookState::new);
        book.symbols_pending_snapshot
            .push(make_update_msg("BTCUSDT", 101, 101, "depthUpdate"));
        book.symbols_pending_snapshot
            .push(make_update_msg("BTCUSDT", 102, 102, "depthUpdate"));

        adapter.drain_buffered_updates(symbol.clone());

        let msg1 = drain_rx.blocking_recv();
        let msg2 = drain_rx.blocking_recv();
        assert!(matches!(msg1, Some(BinanceMdMsg::WsMessage(_))));
        assert!(matches!(msg2, Some(BinanceMdMsg::WsMessage(_))));

        let book = adapter.book_states.get(&symbol).expect("symbol must exist");
        assert_eq!(book.status, BookSyncStatus::Live);
        assert!(book.symbols_pending_snapshot.is_empty());
        assert_eq!(adapter.live_books, 1);
    }

    #[test]
    fn validate_update_gap_is_detected_with_expected_values() {
        let (raw_tx, raw_rx) = mpsc::channel(8);
        let (normalized_tx, _normalized_rx) = mpsc::channel(8);
        let (control_tx, _control_rx) = mpsc::channel(8);
        let mut adapter = BinanceAdapter::new(raw_tx, raw_rx, normalized_tx, control_tx);
        let symbol = instrument("BTCUSDT");

        adapter.book_states.insert(symbol.clone(), BookState::new());
        adapter
            .validate_snapshot(&ValidateSnapshot {
                symbol: symbol.clone(),
                last_update_id: 100,
            })
            .unwrap();

        let gap_update = ParsedBookUpdate {
            event_type: "depthUpdate".to_owned(),
            event_time: 0,
            symbol,
            first_update_id: 105,
            final_update_id: 105,
            bids: vec![],
            asks: vec![],
        };
        let err = adapter
            .validate_update(&gap_update)
            .expect_err("gap must fail");
        match err {
            ValidateBookError::UpdateGap {
                event_first_update_id,
                expected_next_update_id,
            } => {
                assert_eq!(event_first_update_id, 105);
                assert_eq!(expected_next_update_id, 101);
            }
            _ => panic!("unexpected error variant"),
        }
    }

    #[test]
    fn validate_update_random_sequence_matches_reference_state_machine() {
        // Small deterministic RNG to avoid adding dev-dependencies.
        fn next_u64(state: &mut u64) -> u64 {
            *state ^= *state << 13;
            *state ^= *state >> 7;
            *state ^= *state << 17;
            *state
        }

        let (raw_tx, raw_rx) = mpsc::channel(8);
        let (normalized_tx, _normalized_rx) = mpsc::channel(8);
        let (control_tx, _control_rx) = mpsc::channel(8);
        let mut adapter = BinanceAdapter::new(raw_tx, raw_rx, normalized_tx, control_tx);
        let symbol = instrument("BTCUSDT");
        adapter.book_states.insert(symbol.clone(), BookState::new());

        // Apply initial snapshot once for update validation.
        adapter
            .validate_snapshot(&ValidateSnapshot {
                symbol: symbol.clone(),
                last_update_id: 1000,
            })
            .unwrap();
        let mut ref_last = 1000_u64;
        let mut seed = 0xB1AC_E555_u64;

        for _ in 0..5000 {
            let roll = (next_u64(&mut seed) % 100) as u8;
            let (event_type, first_id, final_id, expected_kind) = if roll < 10 {
                ("other", ref_last + 1, ref_last + 1, "unknown")
            } else if roll < 35 {
                // stale update
                let old = ref_last.saturating_sub((next_u64(&mut seed) % 3) + 1);
                ("depthUpdate", old, old, "stale")
            } else if roll < 60 {
                // gap update
                let first = ref_last + 2 + (next_u64(&mut seed) % 3);
                let final_id = first + (next_u64(&mut seed) % 2);
                ("depthUpdate", first, final_id, "gap")
            } else {
                // valid update where first <= ref_last+1 and final > ref_last
                let first = ref_last + (next_u64(&mut seed) % 2);
                let final_id = ref_last + 1 + (next_u64(&mut seed) % 3);
                ("depthUpdate", first, final_id, "ok")
            };

            let update = ParsedBookUpdate {
                event_type: event_type.to_string(),
                event_time: 0,
                symbol: symbol.clone(),
                first_update_id: first_id,
                final_update_id: final_id,
                bids: vec![],
                asks: vec![],
            };

            let got = adapter.validate_update(&update);
            match expected_kind {
                "unknown" => assert!(matches!(got, Err(ValidateBookError::UnknownType(_)))),
                "stale" => assert!(matches!(got, Err(ValidateBookError::StaleUpdate { .. }))),
                "gap" => assert!(matches!(got, Err(ValidateBookError::UpdateGap { .. }))),
                "ok" => {
                    assert!(got.is_ok(), "valid update should be accepted");
                    ref_last = final_id;
                    let state_last = adapter
                        .book_states
                        .get(&symbol)
                        .unwrap()
                        .last_applied_update_id;
                    assert_eq!(state_last, Some(ref_last));
                }
                _ => unreachable!(),
            }
        }
    }
}
