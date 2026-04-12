use std::{
    collections::HashMap,
    error::Error,
    thread,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use md_core::{
    book::BookLevels,
    connector::types::{INACTIVITY_TIMEOUT_SECS, POLL_INTERVAL_SECS},
    events::{BookEventType, ControlEvent, EngineMessage, NormalizedBookData, NormalizedEvent},
    helpers::adapter::{clear_book_state, compute_status, send_normalized_event, send_status},
    traits::adapter::ExchangeAdapter,
    types::{Exchange, Instrument},
};
use tokio::sync::mpsc::{Receiver, Sender, error::TryRecvError};
use tracing::{error, warn};

use crate::types::{
    BitgetMdMsg, BookState, DepthBookAction, ParsedBookMessage, ValidateBookError, WsMessage,
};

pub struct BitgetAdapter {
    raw_rx: Receiver<BitgetMdMsg>,
    normalized_tx: Sender<EngineMessage>,
    control_tx: Sender<ControlEvent>,
    book_states: HashMap<Instrument, BookState>,
    live_books: usize,
    resync_in_progress: bool,
}

impl BitgetAdapter {
    pub fn new(
        raw_rx: Receiver<BitgetMdMsg>,
        normalized_tx: Sender<EngineMessage>,
        control_tx: Sender<ControlEvent>,
    ) -> Self {
        Self {
            raw_rx,
            normalized_tx,
            control_tx,
            book_states: HashMap::new(),
            live_books: 0,
            resync_in_progress: false,
        }
    }

    fn validate_snapshot(&mut self, payload: &ParsedBookMessage) -> Result<(), ValidateBookError> {
        if payload.action != DepthBookAction::Snapshot {
            return Err(ValidateBookError::InvalidSnapshotAction);
        }
        let Some(book) = self.book_states.get_mut(&payload.arg.inst_id) else {
            return Err(ValidateBookError::InstrumentNotFound(
                payload.arg.inst_id.clone(),
            ));
        };
        let Some(book_data) = payload.data.last() else {
            return Err(ValidateBookError::MissingBookData);
        };
        if !book.initialized {
            book.initialized = true;
            self.live_books += 1;
        }
        book.last_seq = Some(book_data.seq);
        Ok(())
    }

    fn validate_update(&mut self, payload: &ParsedBookMessage) -> Result<(), ValidateBookError> {
        if payload.action != DepthBookAction::Update {
            return Err(ValidateBookError::InvalidUpdateAction);
        }
        let Some(book) = self.book_states.get_mut(&payload.arg.inst_id) else {
            return Err(ValidateBookError::InstrumentNotFound(
                payload.arg.inst_id.clone(),
            ));
        };
        let Some(book_data) = payload.data.last() else {
            return Err(ValidateBookError::MissingBookData);
        };

        if let Ok(elapsed) = SystemTime::now().duration_since(UNIX_EPOCH) {
            let now = elapsed.as_millis() as u64;
            let latency = now.saturating_sub(book_data.ts.parse::<u64>().unwrap_or(0));

            if latency > 1000 {
                warn!(
                    exchange = ?BitgetAdapter::exchange(),
                    component = ?BitgetAdapter::component(),
                    symbol = ?payload.arg.inst_id,
                    latency_ms = latency,
                    "high latency for update"
                );
            }
        }

        match book.last_seq {
            Some(last_seq) => {
                if book_data.seq <= last_seq {
                    return Err(ValidateBookError::StaleUpdate {
                        new_seq: book_data.seq,
                        last_seq,
                    });
                }
            }
            None => return Err(ValidateBookError::MissingSnapshot),
        }
        book.last_seq = Some(book_data.seq);
        Ok(())
    }

    pub fn run(&mut self) {
        let mut last_msg_at = Instant::now();

        loop {
            match self.raw_rx.try_recv() {
                Ok(msg) => {
                    last_msg_at = Instant::now();
                    match msg {
                        BitgetMdMsg::ResetBookState => {
                            self.resync_in_progress = false;
                            self.live_books = 0;
                            clear_book_state(&mut self.book_states);
                            send_status::<BitgetAdapter>(
                                &self.normalized_tx,
                                compute_status(self.live_books, self.book_states.len()),
                            );
                        }
                        BitgetMdMsg::Instruments(symbols) => {
                            for i in symbols.iter() {
                                self.book_states.insert(i.clone(), BookState::new());
                            }
                            send_status::<BitgetAdapter>(
                                &self.normalized_tx,
                                compute_status(self.live_books, self.book_states.len()),
                            );
                        }
                        BitgetMdMsg::Raw(msg) => match serde_json::from_slice::<WsMessage>(&msg) {
                            Ok(WsMessage::Confirmation(_)) => {
                                continue;
                            }
                            Err(_) => {
                                let text = String::from_utf8_lossy(&msg);
                                error!(exchange = ?BitgetAdapter::exchange(), component = ?BitgetAdapter::component(), text = ?text, "error while parsing update");
                            }
                            Ok(WsMessage::Depth(depth)) => {
                                let inst_id = depth.arg.inst_id.clone();
                                let action = depth.action.clone();

                                match action {
                                    DepthBookAction::Snapshot => {
                                        if let Err(e) = self.validate_snapshot(&depth) {
                                            error!(exchange = ?BitgetAdapter::exchange(), component = ?BitgetAdapter::component(), symbol = ?inst_id, error = ?e, "error while validating snapshot");
                                            if !self.resync_in_progress
                                                && let Err(e) = self
                                                    .control_tx
                                                    .blocking_send(ControlEvent::Resync)
                                            {
                                                error!(exchange = ?BitgetAdapter::exchange(), component = ?BitgetAdapter::component(), error = ?e, "error while sending resync");
                                            } else {
                                                self.resync_in_progress = true;
                                            }
                                            continue;
                                        }
                                        for data in depth.data {
                                            let mut checksum = None;
                                            if data.checksum != 0 {
                                                checksum = Some(data.checksum);
                                            }
                                            let snapshot_event = NormalizedEvent::Book(
                                                BookEventType::Snapshot,
                                                NormalizedBookData {
                                                    instrument: inst_id.clone(),
                                                    levels: BookLevels {
                                                        asks: data.asks,
                                                        bids: data.bids,
                                                    },
                                                    checksum,
                                                },
                                            );
                                            send_normalized_event::<BitgetAdapter>(
                                                &self.normalized_tx,
                                                snapshot_event,
                                            );
                                        }
                                        send_status::<BitgetAdapter>(
                                            &self.normalized_tx,
                                            compute_status(self.live_books, self.book_states.len()),
                                        );
                                    }
                                    DepthBookAction::Update => {
                                        match self.validate_update(&depth) {
                                            Err(
                                                e @ ValidateBookError::StaleUpdate {
                                                    new_seq: _,
                                                    last_seq: _,
                                                },
                                            ) => {
                                                warn!(
                                                    exchange = ?BitgetAdapter::exchange(),
                                                    component = ?BitgetAdapter::component(),
                                                    symbol = ?depth.arg.inst_id,
                                                    error = ?e,
                                                    "stale update dropped"
                                                );
                                                continue;
                                            }
                                            Err(e @ ValidateBookError::MissingSnapshot) => {
                                                warn!(exchange = ?BitgetAdapter::exchange(), component = ?BitgetAdapter::component(), symbol = ?depth.arg.inst_id, error = ?e, "missing snapshot, skipping update");
                                                continue;
                                            }
                                            Err(e) => {
                                                error!(exchange = ?BitgetAdapter::exchange(), component = ?BitgetAdapter::component(), symbol = ?inst_id, error = ?e, "error while validating update");
                                                if !self.resync_in_progress
                                                    && let Err(e) = self
                                                        .control_tx
                                                        .blocking_send(ControlEvent::Resync)
                                                {
                                                    error!(exchange = ?BitgetAdapter::exchange(), component = ?BitgetAdapter::component(), error = ?e, "error while sending resync");
                                                } else {
                                                    self.resync_in_progress = true;
                                                }
                                                continue;
                                            }
                                            Ok(()) => {}
                                        }
                                        for data in depth.data {
                                            let mut checksum = None;
                                            if data.checksum != 0 {
                                                checksum = Some(data.checksum);
                                            }
                                            let update_event = NormalizedEvent::Book(
                                                BookEventType::Update,
                                                NormalizedBookData {
                                                    instrument: inst_id.clone(),
                                                    levels: BookLevels {
                                                        asks: data.asks,
                                                        bids: data.bids,
                                                    },
                                                    checksum,
                                                },
                                            );
                                            send_normalized_event::<BitgetAdapter>(
                                                &self.normalized_tx,
                                                update_event,
                                            );
                                        }
                                    }
                                }
                            }
                        },
                    }
                }
                Err(TryRecvError::Empty) => {
                    if last_msg_at.elapsed() >= INACTIVITY_TIMEOUT_SECS {
                        warn!(
                            exchange = ?BitgetAdapter::exchange(),
                            component = ?BitgetAdapter::component(),
                            timeout_secs = ?INACTIVITY_TIMEOUT_SECS.as_secs(),
                            "no messages received for too long, triggering resync"
                        );

                        if !self.resync_in_progress
                            && let Err(e) = self.control_tx.blocking_send(ControlEvent::Resync)
                        {
                            error!(exchange = ?BitgetAdapter::exchange(), component = ?BitgetAdapter::component(), error = ?e, "error while sending resync after inactivity timeout");
                        } else {
                            self.resync_in_progress = true;
                        }

                        last_msg_at = Instant::now();
                    }

                    thread::sleep(POLL_INTERVAL_SECS);
                }
                Err(TryRecvError::Disconnected) => break,
            }
        }
    }
}

impl ExchangeAdapter for BitgetAdapter {
    type SnapshotPayload = ParsedBookMessage;
    type UpdatePayload = ParsedBookMessage;

    fn exchange() -> Exchange {
        Exchange::Bitget
    }

    fn validate_snapshot(
        &mut self,
        payload: &Self::SnapshotPayload,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        BitgetAdapter::validate_snapshot(self, payload)
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)
    }

    fn validate_update(
        &mut self,
        payload: &Self::UpdatePayload,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        BitgetAdapter::validate_update(self, payload)
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)
    }

    fn run(&mut self) {
        BitgetAdapter::run(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use md_core::types::{Price, Qty};
    use rust_decimal::Decimal;
    use tokio::sync::mpsc;

    fn instrument(v: &str) -> Instrument {
        Instrument(v.to_owned())
    }

    fn level(px: i64, q: i64) -> (Price, Qty) {
        (Price(Decimal::new(px, 2)), Qty(Decimal::new(q, 3)))
    }

    fn message(action: DepthBookAction, inst: &str, seq: u64, data_ts: &str) -> ParsedBookMessage {
        ParsedBookMessage {
            action,
            arg: crate::types::SymbolParam {
                inst_type: "SPOT".to_owned(),
                channel: "books".to_owned(),
                inst_id: instrument(inst),
            },
            data: vec![crate::types::ParsedBookData {
                asks: vec![level(10100, 1000)],
                bids: vec![level(10000, 1000)],
                checksum: 1,
                seq,
                ts: data_ts.to_owned(),
            }],
            ts: 0,
        }
    }

    #[test]
    fn snapshot_initializes_once_and_updates_last_seq() {
        let (_raw_tx, raw_rx) = mpsc::channel(8);
        let (normalized_tx, _normalized_rx) = mpsc::channel(8);
        let (control_tx, _control_rx) = mpsc::channel(8);
        let mut adapter = BitgetAdapter::new(raw_rx, normalized_tx, control_tx);
        let symbol = instrument("BTCUSDT");
        adapter.book_states.insert(symbol.clone(), BookState::new());

        adapter
            .validate_snapshot(&message(DepthBookAction::Snapshot, "BTCUSDT", 10, "0"))
            .unwrap();
        assert_eq!(adapter.live_books, 1);
        assert_eq!(adapter.book_states.get(&symbol).unwrap().last_seq, Some(10));

        // Second snapshot for same symbol must not double-count live book.
        adapter
            .validate_snapshot(&message(DepthBookAction::Snapshot, "BTCUSDT", 15, "0"))
            .unwrap();
        assert_eq!(adapter.live_books, 1);
        assert_eq!(adapter.book_states.get(&symbol).unwrap().last_seq, Some(15));
    }

    #[test]
    fn update_requires_snapshot_before_accepting_data() {
        let (_raw_tx, raw_rx) = mpsc::channel(8);
        let (normalized_tx, _normalized_rx) = mpsc::channel(8);
        let (control_tx, _control_rx) = mpsc::channel(8);
        let mut adapter = BitgetAdapter::new(raw_rx, normalized_tx, control_tx);
        adapter
            .book_states
            .insert(instrument("BTCUSDT"), BookState::new());

        let err = adapter
            .validate_update(&message(DepthBookAction::Update, "BTCUSDT", 1, "0"))
            .expect_err("update before snapshot must fail");
        assert!(matches!(err, ValidateBookError::MissingSnapshot));
    }

    #[test]
    fn random_update_sequence_matches_reference_last_seq_model() {
        fn next_u64(state: &mut u64) -> u64 {
            *state ^= *state << 13;
            *state ^= *state >> 7;
            *state ^= *state << 17;
            *state
        }

        let (_raw_tx, raw_rx) = mpsc::channel(8);
        let (normalized_tx, _normalized_rx) = mpsc::channel(8);
        let (control_tx, _control_rx) = mpsc::channel(8);
        let mut adapter = BitgetAdapter::new(raw_rx, normalized_tx, control_tx);
        let symbol = instrument("BTCUSDT");
        adapter.book_states.insert(symbol.clone(), BookState::new());

        adapter
            .validate_snapshot(&message(DepthBookAction::Snapshot, "BTCUSDT", 1000, "0"))
            .unwrap();
        let mut ref_last = 1000_u64;
        let mut seed = 0xB175_E7A7_u64;

        for _ in 0..5000 {
            let roll = (next_u64(&mut seed) % 100) as u8;
            let (seq, expected_ok) = if roll < 35 {
                // stale
                let old = ref_last.saturating_sub((next_u64(&mut seed) % 3) + 1);
                (old, false)
            } else {
                // valid strictly increasing sequence
                (ref_last + 1 + (next_u64(&mut seed) % 3), true)
            };

            let got =
                adapter.validate_update(&message(DepthBookAction::Update, "BTCUSDT", seq, "0"));
            if expected_ok {
                assert!(got.is_ok(), "strictly increasing sequence must be accepted");
                ref_last = seq;
                assert_eq!(
                    adapter.book_states.get(&symbol).unwrap().last_seq,
                    Some(ref_last)
                );
            } else {
                assert!(matches!(got, Err(ValidateBookError::StaleUpdate { .. })));
                assert_eq!(
                    adapter.book_states.get(&symbol).unwrap().last_seq,
                    Some(ref_last)
                );
            }
        }
    }
}
