use std::{collections::HashMap, error::Error, time::{SystemTime, UNIX_EPOCH}};

use md_core::{book::BookLevels, events::{BookEventType, ControlEvent, EventEnvelope, NormalizedBookData, NormalizedEvent}, helpers::adapter::{clear_book_state, compute_status, send_normalized_event, send_status}, traits::adapter::ExchangeAdapter, types::{Exchange, Instrument}};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, warn};

use crate::types::{BookState, DepthBookAction, OkxMdMsg, ParsedBookMessage, ValidateBookError, WsMessage};

pub struct OkxAdapter {
    raw_rx: Receiver<OkxMdMsg>,
    normalized_tx: Sender<EventEnvelope>,
    control_tx: Sender<ControlEvent>,
    book_states: HashMap<Instrument, BookState>,
    live_books: usize
}

impl OkxAdapter {
    pub fn new(raw_rx: Receiver<OkxMdMsg>, normalized_tx: Sender<EventEnvelope>, control_tx: Sender<ControlEvent>) -> Self {
        Self {
            raw_rx,
            normalized_tx,
            control_tx,
            book_states: HashMap::new(),
            live_books: 0
        }
    }

    fn validate_snapshot(&mut self, payload: &ParsedBookMessage) -> Result<(), ValidateBookError> {
        let Some(book) = self.book_states.get_mut(&payload.arg.inst_id) else {
            return Err(ValidateBookError::InstrumentNotFound(payload.arg.inst_id.clone()));
        };
        let Some(book_data) = payload.data.last() else {
            return Err(ValidateBookError::MissingBookData);
        };
        if !book.initialized {
            book.initialized = true;
            self.live_books += 1;
        }
        book.prev_seq_id = Some(book_data.seq_id);
        Ok(())
    }

    fn validate_update(&mut self, payload: &ParsedBookMessage) -> Result<(), ValidateBookError> {
        let Some(book) = self.book_states.get_mut(&payload.arg.inst_id) else {
            return Err(ValidateBookError::InstrumentNotFound(payload.arg.inst_id.clone()));
        };
        let Some(book_data) = payload.data.last() else {
            return Err(ValidateBookError::MissingBookData);
        };

        if let Ok(elapsed) = SystemTime::now().duration_since(UNIX_EPOCH) {
            let now = elapsed.as_millis() as u64;
            let latency = now.saturating_sub(book_data.ts.parse::<u64>().unwrap_or(0));

            if latency > 1000 {
                warn!(exchange = ?OkxAdapter::exchange(), component = ?OkxAdapter::component(), symbol = ?payload.arg.inst_id, "high latency for update: {} ms", latency);
            }
        }

        let event_prev_seq_id = book_data.prev_seq_id as u64;
        match book.prev_seq_id {
            Some(prev_seq_id) => {
                if event_prev_seq_id < prev_seq_id {
                    return Err(ValidateBookError::StaleUpdate { event_prev_seq_id, book_prev_seq_id: prev_seq_id });
                }
                if event_prev_seq_id > prev_seq_id {
                    return Err(ValidateBookError::UpdateGap { event_prev_seq_id, expected_prev_seq_id: prev_seq_id});
                }
            },
            None => {
                return Err(ValidateBookError::MissingSnapshot)
            }
        }
        book.prev_seq_id = Some(book_data.seq_id); 
        Ok(())
    }

    pub fn run(&mut self) {
        while let Some(msg) = self.raw_rx.blocking_recv() {
            match msg {
                OkxMdMsg::ResetBookState => {
                    self.live_books = 0;
                    clear_book_state(&mut self.book_states);
                    send_status::<OkxAdapter>(&self.normalized_tx, compute_status(self.live_books, self.book_states.len()));
                },
                OkxMdMsg::Instruments(symbols) => {
                    for i in symbols.iter() {
                        self.book_states.insert(i.clone(), BookState::new());
                    }
                    send_status::<OkxAdapter>(&self.normalized_tx, compute_status(self.live_books, self.book_states.len()));
                },
                OkxMdMsg::Raw(msg) => {
                    match serde_json::from_slice::<WsMessage>(&msg) {
                        Ok(WsMessage::Confirmation(_)) => {
                            continue;
                        },
                        Err(_) => {
                            let text = String::from_utf8_lossy(&msg);
                            error!(exchange = ?OkxAdapter::exchange(), component = ?OkxAdapter::component(), text = ?text, "error while parsing update");
                        },
                        Ok(WsMessage::Depth(depth)) => {
                            let inst_id = depth.arg.inst_id.replace("-", "");
                            let action = depth.action.clone();
        
                            match action {
                                DepthBookAction::Snapshot => {
                                    if let Err(e) = self.validate_snapshot(&depth) {
                                        error!(exchange = ?OkxAdapter::exchange(), component = ?OkxAdapter::component(), error = ?e, "error while validating snapshot");
                                        if let Err(e) = self.control_tx.blocking_send(ControlEvent::Resync) {
                                            error!(exchange = ?OkxAdapter::exchange(), component = ?OkxAdapter::component(), error = ?e, "error while sending resync");
                                        }
                                    }
                                    for data in depth.data {
                                        let mut checksum = None;
                                        if data.checksum != 0 {
                                            checksum = Some(data.checksum);
                                        }
                                        let snapshot_event = NormalizedEvent::Book(BookEventType::Snapshot, NormalizedBookData {
                                            instrument: Instrument::from(inst_id.clone()),
                                            levels: BookLevels {
                                                asks: data.asks,
                                                bids: data.bids
                                            },
                                            checksum
                                        });
                                        send_normalized_event::<OkxAdapter>(&self.normalized_tx, snapshot_event);
                                    }
                                    send_status::<OkxAdapter>(&self.normalized_tx, compute_status(self.live_books, self.book_states.len()));
                                },
                                DepthBookAction::Update => {
                                    match self.validate_update(&depth) {
                                        Err(e @ ValidateBookError::StaleUpdate { event_prev_seq_id: _, book_prev_seq_id: _ }) => {
                                            error!(
                                                exchange = ?OkxAdapter::exchange(),
                                                component = ?OkxAdapter::component(),
                                                symbol = ?depth.arg.inst_id,
                                                error = ?e,
                                                "error while validating update"
                                            );
                                            continue;
                                        },
                                        Err(e) => {
                                            error!(exchange = ?OkxAdapter::exchange(), component = ?OkxAdapter::component(), symbol = ?inst_id, error = ?e, "error while validating update");
                                            if let Err(e) = self.control_tx.blocking_send(ControlEvent::Resync) {
                                                error!(exchange = ?OkxAdapter::exchange(), component = ?OkxAdapter::component(), error = ?e, "error while sending resync");
                                            }
                                        },
                                        Ok(()) => {}
                                    }
                                    for data in depth.data {
                                        let mut checksum = None;
                                        if data.checksum != 0 {
                                            checksum = Some(data.checksum);
                                        }
                                        let update_event = NormalizedEvent::Book(BookEventType::Update, NormalizedBookData {
                                            instrument: Instrument::from(inst_id.clone()),
                                            levels: BookLevels {
                                                asks: data.asks,
                                                bids: data.bids
                                            },
                                            checksum
                                        });
                                        send_normalized_event::<OkxAdapter>(&self.normalized_tx, update_event);
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

impl ExchangeAdapter for OkxAdapter {
    type SnapshotPayload = ParsedBookMessage;
    type UpdatePayload = ParsedBookMessage;

    fn exchange() -> Exchange {
        Exchange::Okx
    }

    fn validate_snapshot(&mut self, payload: &Self::SnapshotPayload) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        OkxAdapter::validate_snapshot(self, payload).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)
    }

    fn validate_update(&mut self, payload: &Self::UpdatePayload) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        OkxAdapter::validate_update(self, payload).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)
    }

    fn run(&mut self) {
        OkxAdapter::run(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use md_core::types::{Price, Qty};
    use rust_decimal::Decimal;
    use tokio::sync::mpsc;

    fn instrument(v: &str) -> Instrument {
        Instrument(v.to_owned())
    }

    fn level(px: i64, q: i64) -> (Price, Qty) {
        (Price(Decimal::new(px, 2)), Qty(Decimal::new(q, 3)))
    }

    fn msg(symbol: &str, seq_id: u64, prev_seq_id: i64) -> ParsedBookMessage {
        ParsedBookMessage {
            arg: crate::types::SymbolParam {
                channel: "books".to_owned(),
                inst_id: instrument(symbol),
            },
            action: DepthBookAction::Update,
            data: vec![crate::types::ParsedBookData {
                asks: vec![level(10100, 1000)],
                bids: vec![level(10000, 1000)],
                ts: "0".to_owned(),
                checksum: 1,
                seq_id,
                prev_seq_id,
            }],
        }
    }

    fn snapshot_msg(symbol: &str, seq_id: u64) -> ParsedBookMessage {
        ParsedBookMessage {
            arg: crate::types::SymbolParam {
                channel: "books".to_owned(),
                inst_id: instrument(symbol),
            },
            action: DepthBookAction::Snapshot,
            data: vec![crate::types::ParsedBookData {
                asks: vec![level(10100, 1000)],
                bids: vec![level(10000, 1000)],
                ts: "0".to_owned(),
                checksum: 1,
                seq_id,
                prev_seq_id: seq_id as i64,
            }],
        }
    }

    fn raw_depth_msg(action: &str, seq_id: u64, prev_seq_id: i64) -> OkxMdMsg {
        let payload = format!(
            r#"{{"arg":{{"channel":"books","instId":"BTCUSDT"}},"action":"{action}","data":[{{"asks":[["101.00","1.0","0","1"]],"bids":[["100.00","1.0","0","1"]],"ts":"1","checksum":1,"seqId":{seq_id},"prevSeqId":{prev_seq_id}}}]}}"#
        );
        OkxMdMsg::Raw(md_core::types::RawMdMsg(payload.into_bytes()))
    }

    #[test]
    fn snapshot_initialization_is_idempotent_and_sets_prev_seq() {
        let (_raw_tx, raw_rx) = mpsc::channel(8);
        let (normalized_tx, _normalized_rx) = mpsc::channel(8);
        let (control_tx, _control_rx) = mpsc::channel(8);
        let mut adapter = OkxAdapter::new(raw_rx, normalized_tx, control_tx);
        let symbol = instrument("BTCUSDT");
        adapter.book_states.insert(symbol.clone(), BookState::new());

        adapter.validate_snapshot(&snapshot_msg("BTCUSDT", 100)).unwrap();
        assert_eq!(adapter.live_books, 1);
        assert_eq!(adapter.book_states.get(&symbol).unwrap().prev_seq_id, Some(100));

        adapter.validate_snapshot(&snapshot_msg("BTCUSDT", 120)).unwrap();
        assert_eq!(adapter.live_books, 1);
        assert_eq!(adapter.book_states.get(&symbol).unwrap().prev_seq_id, Some(120));
    }

    #[test]
    fn update_detects_missing_snapshot_stale_and_gap() {
        let (_raw_tx, raw_rx) = mpsc::channel(8);
        let (normalized_tx, _normalized_rx) = mpsc::channel(8);
        let (control_tx, _control_rx) = mpsc::channel(8);
        let mut adapter = OkxAdapter::new(raw_rx, normalized_tx, control_tx);
        adapter.book_states.insert(instrument("BTCUSDT"), BookState::new());

        let missing = adapter
            .validate_update(&msg("BTCUSDT", 101, 100))
            .expect_err("update before snapshot must fail");
        assert!(matches!(missing, ValidateBookError::MissingSnapshot));

        adapter.validate_snapshot(&snapshot_msg("BTCUSDT", 100)).unwrap();

        let stale = adapter
            .validate_update(&msg("BTCUSDT", 101, 99))
            .expect_err("event prev lower than expected must fail");
        assert!(matches!(stale, ValidateBookError::StaleUpdate { .. }));

        let gap = adapter
            .validate_update(&msg("BTCUSDT", 101, 101))
            .expect_err("event prev higher than expected must fail");
        assert!(matches!(gap, ValidateBookError::UpdateGap { .. }));
    }

    #[test]
    fn random_prev_seq_transitions_match_reference_model() {
        fn next_u64(state: &mut u64) -> u64 {
            *state ^= *state << 13;
            *state ^= *state >> 7;
            *state ^= *state << 17;
            *state
        }

        let (_raw_tx, raw_rx) = mpsc::channel(8);
        let (normalized_tx, _normalized_rx) = mpsc::channel(8);
        let (control_tx, _control_rx) = mpsc::channel(8);
        let mut adapter = OkxAdapter::new(raw_rx, normalized_tx, control_tx);
        let symbol = instrument("BTCUSDT");
        adapter.book_states.insert(symbol.clone(), BookState::new());
        adapter.validate_snapshot(&snapshot_msg("BTCUSDT", 1000)).unwrap();

        let mut ref_prev = 1000_u64;
        let mut seed = 0x0A55_A11C_u64;

        for _ in 0..5000 {
            let roll = (next_u64(&mut seed) % 100) as u8;
            let (event_prev, next_seq, expected) = if roll < 30 {
                (ref_prev.saturating_sub(1 + (next_u64(&mut seed) % 2)), ref_prev + 1, "stale")
            } else if roll < 60 {
                (ref_prev + 1 + (next_u64(&mut seed) % 2), ref_prev + 2, "gap")
            } else {
                (ref_prev, ref_prev + 1 + (next_u64(&mut seed) % 3), "ok")
            };

            let got = adapter.validate_update(&msg("BTCUSDT", next_seq, event_prev as i64));
            match expected {
                "stale" => {
                    assert!(matches!(got, Err(ValidateBookError::StaleUpdate { .. })));
                    assert_eq!(adapter.book_states.get(&symbol).unwrap().prev_seq_id, Some(ref_prev));
                }
                "gap" => {
                    assert!(matches!(got, Err(ValidateBookError::UpdateGap { .. })));
                    assert_eq!(adapter.book_states.get(&symbol).unwrap().prev_seq_id, Some(ref_prev));
                }
                "ok" => {
                    assert!(got.is_ok(), "matching prev_seq should pass");
                    ref_prev = next_seq;
                    assert_eq!(adapter.book_states.get(&symbol).unwrap().prev_seq_id, Some(ref_prev));
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn run_stale_update_is_dropped_while_gap_triggers_resync_and_emits_update() {
        let (raw_tx, raw_rx) = mpsc::channel(128);
        let (normalized_tx, mut normalized_rx) = mpsc::channel(128);
        let (control_tx, mut control_rx) = mpsc::channel(128);
        let mut adapter = OkxAdapter::new(raw_rx, normalized_tx, control_tx);

        let handle = thread::spawn(move || adapter.run());
        raw_tx
            .blocking_send(OkxMdMsg::Instruments(vec![instrument("BTCUSDT")]))
            .unwrap();
        raw_tx
            .blocking_send(raw_depth_msg("snapshot", 100, 100))
            .unwrap();
        // stale (event prev < book prev) -> dropped, no resync
        raw_tx
            .blocking_send(raw_depth_msg("update", 101, 99))
            .unwrap();
        // gap (event prev > book prev) -> resync + update emitted
        raw_tx
            .blocking_send(raw_depth_msg("update", 102, 101))
            .unwrap();
        drop(raw_tx);
        handle.join().unwrap();

        let mut resync_count = 0usize;
        while matches!(control_rx.try_recv(), Ok(ControlEvent::Resync)) {
            resync_count += 1;
        }
        assert_eq!(resync_count, 1, "only gap should trigger resync");

        let mut update_count = 0usize;
        while let Ok(event) = normalized_rx.try_recv() {
            if let NormalizedEvent::Book(BookEventType::Update, book) = event.event {
                if book.instrument == instrument("BTCUSDT") {
                    update_count += 1;
                }
            }
        }
        assert_eq!(update_count, 1, "stale update must be dropped, gap update emitted");
    }
}