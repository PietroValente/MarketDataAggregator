use std::{
    collections::HashMap,
    error::Error,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use md_core::{
    book::BookLevels,
    events::{BookEventType, ControlEvent, EngineMessage, NormalizedBookData, NormalizedEvent},
    helpers::adapter::{
        clear_book_state, compute_status, handle_inactivity_timeout, send_normalized_event,
        send_status,
    },
    traits::adapter::ExchangeAdapter,
    types::{Exchange, Instrument},
};
use tokio::sync::mpsc::{Receiver, Sender, error::TryRecvError};
use tracing::{error, warn};

use crate::types::{
    BookState, BybitMdMsg, DepthBookAction, ParsedBookMessage, ValidateBookError, WsMessage,
};

pub struct BybitAdapter {
    raw_rx: Receiver<BybitMdMsg>,
    normalized_tx: Sender<EngineMessage>,
    control_tx: Sender<ControlEvent>,
    book_states: HashMap<Instrument, BookState>,
    live_books: usize,
    resync_in_progress: bool,
}

impl BybitAdapter {
    pub fn new(
        raw_rx: Receiver<BybitMdMsg>,
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
        let Some(book) = self.book_states.get_mut(&payload.data.symbol) else {
            return Err(ValidateBookError::InstrumentNotFound(
                payload.data.symbol.clone(),
            ));
        };
        if !book.initialized {
            book.initialized = true;
            self.live_books += 1;
        }
        book.last_update_id = Some(payload.data.update_id);
        Ok(())
    }

    fn validate_update(&mut self, payload: &ParsedBookMessage) -> Result<(), ValidateBookError> {
        if payload.action != DepthBookAction::Delta {
            return Err(ValidateBookError::InvalidDeltaAction);
        }
        let Some(book) = self.book_states.get_mut(&payload.data.symbol) else {
            return Err(ValidateBookError::InstrumentNotFound(
                payload.data.symbol.clone(),
            ));
        };

        if let Ok(elapsed) = SystemTime::now().duration_since(UNIX_EPOCH) {
            let now = elapsed.as_millis() as u64;
            let latency = now.saturating_sub(payload.cts);

            if latency > 1000 {
                warn!(
                    exchange = ?BybitAdapter::exchange(),
                    component = ?BybitAdapter::component(),
                    symbol = ?payload.data.symbol,
                    latency_ms = latency,
                    "high latency for update"
                );
            }
        }

        match book.last_update_id {
            Some(last_update_id) => {
                if payload.data.update_id <= last_update_id {
                    return Err(ValidateBookError::StaleUpdate {
                        new_update_id: payload.data.update_id,
                        last_update_id,
                    });
                }
            }
            None => return Err(ValidateBookError::MissingSnapshot),
        }
        book.last_update_id = Some(payload.data.update_id);
        Ok(())
    }

    pub fn run(&mut self) {
        let mut last_msg_at = Instant::now();

        loop {
            match self.raw_rx.try_recv() {
                Ok(msg) => match msg {
                    BybitMdMsg::ResetBookState => {
                        self.resync_in_progress = false;
                        self.live_books = 0;
                        clear_book_state(&mut self.book_states);
                        send_status::<BybitAdapter>(
                            &self.normalized_tx,
                            compute_status(self.live_books, self.book_states.len()),
                        );
                    }
                    BybitMdMsg::Instruments(symbols) => {
                        for i in symbols.iter() {
                            self.book_states.insert(i.clone(), BookState::new());
                        }
                        send_status::<BybitAdapter>(
                            &self.normalized_tx,
                            compute_status(self.live_books, self.book_states.len()),
                        );
                    }
                    BybitMdMsg::Raw(msg) => match serde_json::from_slice::<WsMessage>(&msg) {
                        Ok(WsMessage::Confirmation(_)) => {
                            continue;
                        }
                        Err(_) => {
                            let text = String::from_utf8_lossy(&msg);
                            error!(exchange = ?BybitAdapter::exchange(), component = ?BybitAdapter::component(), text = ?text, "error while parsing update");
                        }
                        Ok(WsMessage::Depth(depth)) => {
                            last_msg_at = Instant::now();
                            let action = depth.action.clone();

                            match action {
                                DepthBookAction::Snapshot => {
                                    if let Err(e) = self.validate_snapshot(&depth) {
                                        error!(exchange = ?BybitAdapter::exchange(), component = ?BybitAdapter::component(), symbol = ?depth.data.symbol, error = ?e, "error while validating snapshot");
                                        if !self.resync_in_progress
                                            && let Err(e) =
                                                self.control_tx.blocking_send(ControlEvent::Resync)
                                        {
                                            error!(exchange = ?BybitAdapter::exchange(), component = ?BybitAdapter::component(), error = ?e, "error while sending resync");
                                        } else {
                                            self.resync_in_progress = true;
                                        }
                                        continue;
                                    }
                                    let snapshot_event = NormalizedEvent::Book(
                                        BookEventType::Snapshot,
                                        NormalizedBookData {
                                            instrument: depth.data.symbol,
                                            levels: BookLevels {
                                                asks: depth.data.asks,
                                                bids: depth.data.bids,
                                            },
                                            checksum: None,
                                        },
                                    );
                                    send_normalized_event::<BybitAdapter>(
                                        &self.normalized_tx,
                                        snapshot_event,
                                    );
                                    send_status::<BybitAdapter>(
                                        &self.normalized_tx,
                                        compute_status(self.live_books, self.book_states.len()),
                                    );
                                }
                                DepthBookAction::Delta => {
                                    match self.validate_update(&depth) {
                                        Err(
                                            e @ ValidateBookError::StaleUpdate {
                                                new_update_id: _,
                                                last_update_id: _,
                                            },
                                        ) => {
                                            warn!(
                                                exchange = ?BybitAdapter::exchange(),
                                                component = ?BybitAdapter::component(),
                                                symbol = ?depth.data.symbol,
                                                error = ?e,
                                                "stale update dropped"
                                            );
                                            continue;
                                        }
                                        Err(e @ ValidateBookError::MissingSnapshot) => {
                                            warn!(exchange = ?BybitAdapter::exchange(), component = ?BybitAdapter::component(), symbol = ?depth.data.symbol, error = ?e, "missing snapshot, skipping update");
                                            continue;
                                        }
                                        Err(e) => {
                                            error!(exchange = ?BybitAdapter::exchange(), component = ?BybitAdapter::component(), symbol = ?depth.data.symbol, error = ?e, "error while validating update");
                                            if !self.resync_in_progress
                                                && let Err(e) = self
                                                    .control_tx
                                                    .blocking_send(ControlEvent::Resync)
                                            {
                                                error!(exchange = ?BybitAdapter::exchange(), component = ?BybitAdapter::component(), error = ?e, "error while sending resync");
                                            } else {
                                                self.resync_in_progress = true;
                                            }
                                            continue;
                                        }
                                        Ok(()) => {}
                                    }
                                    let update_event = NormalizedEvent::Book(
                                        BookEventType::Update,
                                        NormalizedBookData {
                                            instrument: depth.data.symbol,
                                            levels: BookLevels {
                                                asks: depth.data.asks,
                                                bids: depth.data.bids,
                                            },
                                            checksum: None,
                                        },
                                    );
                                    send_normalized_event::<BybitAdapter>(
                                        &self.normalized_tx,
                                        update_event,
                                    );
                                }
                            }
                        }
                    },
                },
                Err(TryRecvError::Empty) => {
                    handle_inactivity_timeout::<BybitAdapter>(
                        &mut last_msg_at,
                        &mut self.resync_in_progress,
                        &self.control_tx,
                    );
                }
                Err(TryRecvError::Disconnected) => break,
            }
        }
    }
}

impl ExchangeAdapter for BybitAdapter {
    type SnapshotPayload = ParsedBookMessage;
    type UpdatePayload = ParsedBookMessage;

    fn exchange() -> Exchange {
        Exchange::Bybit
    }

    fn validate_snapshot(
        &mut self,
        payload: &Self::SnapshotPayload,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        BybitAdapter::validate_snapshot(self, payload)
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)
    }

    fn validate_update(
        &mut self,
        payload: &Self::UpdatePayload,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        BybitAdapter::validate_update(self, payload)
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)
    }

    fn run(&mut self) {
        BybitAdapter::run(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use md_core::types::ExchangeStatus;
    use md_core::types::{Price, Qty};
    use rust_decimal::Decimal;
    use std::thread;
    use tokio::sync::mpsc;

    fn instrument(v: &str) -> Instrument {
        Instrument(v.to_owned())
    }

    fn level(px: i64, q: i64) -> (Price, Qty) {
        (Price(Decimal::new(px, 2)), Qty(Decimal::new(q, 3)))
    }

    fn message(action: DepthBookAction, symbol: &str, update_id: u64) -> ParsedBookMessage {
        ParsedBookMessage {
            topic: "orderbook.50.BTCUSDT".to_owned(),
            action,
            ts: 0,
            cts: 0,
            data: crate::types::ParsedBookData {
                symbol: instrument(symbol),
                bids: vec![level(10000, 1000)],
                asks: vec![level(10100, 1000)],
                update_id,
                sequence: update_id,
            },
        }
    }

    fn raw_depth_msg(action: &str, symbol: &str, update_id: u64) -> BybitMdMsg {
        let payload = format!(
            r#"{{"topic":"orderbook.50.{symbol}","type":"{action}","ts":1,"cts":1,"data":{{"s":"{symbol}","b":[["100.00","1.0"]],"a":[["101.00","2.0"]],"u":{update_id},"seq":{update_id}}}}}"#
        );
        BybitMdMsg::Raw(md_core::types::RawMdMsg(payload.into_bytes()))
    }

    #[test]
    fn snapshot_initializes_once_and_updates_last_update_id() {
        let (_raw_tx, raw_rx) = mpsc::channel(8);
        let (normalized_tx, _normalized_rx) = mpsc::channel(8);
        let (control_tx, _control_rx) = mpsc::channel(8);
        let mut adapter = BybitAdapter::new(raw_rx, normalized_tx, control_tx);
        let symbol = instrument("BTCUSDT");
        adapter.book_states.insert(symbol.clone(), BookState::new());

        adapter
            .validate_snapshot(&message(DepthBookAction::Snapshot, "BTCUSDT", 10))
            .unwrap();
        assert_eq!(adapter.live_books, 1);
        assert_eq!(
            adapter.book_states.get(&symbol).unwrap().last_update_id,
            Some(10)
        );

        // Repeated snapshots should not increase live_books multiple times.
        adapter
            .validate_snapshot(&message(DepthBookAction::Snapshot, "BTCUSDT", 15))
            .unwrap();
        assert_eq!(adapter.live_books, 1);
        assert_eq!(
            adapter.book_states.get(&symbol).unwrap().last_update_id,
            Some(15)
        );
    }

    #[test]
    fn delta_requires_snapshot_before_accepting_updates() {
        let (_raw_tx, raw_rx) = mpsc::channel(8);
        let (normalized_tx, _normalized_rx) = mpsc::channel(8);
        let (control_tx, _control_rx) = mpsc::channel(8);
        let mut adapter = BybitAdapter::new(raw_rx, normalized_tx, control_tx);
        adapter
            .book_states
            .insert(instrument("BTCUSDT"), BookState::new());

        let err = adapter
            .validate_update(&message(DepthBookAction::Delta, "BTCUSDT", 1))
            .expect_err("delta before snapshot must fail");
        assert!(matches!(err, ValidateBookError::MissingSnapshot));
    }

    #[test]
    fn random_delta_sequence_matches_reference_update_id_model() {
        fn next_u64(state: &mut u64) -> u64 {
            *state ^= *state << 13;
            *state ^= *state >> 7;
            *state ^= *state << 17;
            *state
        }

        let (_raw_tx, raw_rx) = mpsc::channel(8);
        let (normalized_tx, _normalized_rx) = mpsc::channel(8);
        let (control_tx, _control_rx) = mpsc::channel(8);
        let mut adapter = BybitAdapter::new(raw_rx, normalized_tx, control_tx);
        let symbol = instrument("BTCUSDT");
        adapter.book_states.insert(symbol.clone(), BookState::new());

        adapter
            .validate_snapshot(&message(DepthBookAction::Snapshot, "BTCUSDT", 1000))
            .unwrap();
        let mut ref_last = 1000_u64;
        let mut seed = 0xB1B1_7001_u64;

        for _ in 0..5000 {
            let roll = (next_u64(&mut seed) % 100) as u8;
            let (update_id, expected_ok) = if roll < 35 {
                let stale = ref_last.saturating_sub((next_u64(&mut seed) % 3) + 1);
                (stale, false)
            } else {
                (ref_last + 1 + (next_u64(&mut seed) % 3), true)
            };

            let got =
                adapter.validate_update(&message(DepthBookAction::Delta, "BTCUSDT", update_id));
            if expected_ok {
                assert!(got.is_ok(), "increasing update_id must be accepted");
                ref_last = update_id;
                assert_eq!(
                    adapter.book_states.get(&symbol).unwrap().last_update_id,
                    Some(ref_last)
                );
            } else {
                assert!(matches!(got, Err(ValidateBookError::StaleUpdate { .. })));
                assert_eq!(
                    adapter.book_states.get(&symbol).unwrap().last_update_id,
                    Some(ref_last)
                );
            }
        }
    }

    #[test]
    fn run_reset_restores_initializing_status_progression() {
        let (raw_tx, raw_rx) = mpsc::channel(64);
        let (normalized_tx, mut normalized_rx) = mpsc::channel(64);
        let (control_tx, _control_rx) = mpsc::channel(64);
        let mut adapter = BybitAdapter::new(raw_rx, normalized_tx, control_tx);

        let handle = thread::spawn(move || adapter.run());
        raw_tx
            .blocking_send(BybitMdMsg::Instruments(vec![
                instrument("BTCUSDT"),
                instrument("ETHUSDT"),
            ]))
            .unwrap();
        raw_tx
            .blocking_send(raw_depth_msg("snapshot", "BTCUSDT", 10))
            .unwrap();
        raw_tx.blocking_send(BybitMdMsg::ResetBookState).unwrap();
        raw_tx
            .blocking_send(raw_depth_msg("snapshot", "BTCUSDT", 11))
            .unwrap();
        drop(raw_tx);
        handle.join().unwrap();

        let mut init_zero = 0usize;
        let mut init_half = 0usize;
        while let Ok(msg) = normalized_rx.try_recv() {
            if let EngineMessage::Apply(envelope) = msg
                && let NormalizedEvent::ApplyStatus(status) = envelope.event
            {
                match status {
                    ExchangeStatus::Initializing(p) if (p - 0.0).abs() < f32::EPSILON => {
                        init_zero += 1
                    }
                    ExchangeStatus::Initializing(p) if (p - 0.5).abs() < f32::EPSILON => {
                        init_half += 1
                    }
                    _ => {}
                }
            }
        }

        assert!(
            init_zero >= 2,
            "expected initializing 0.0 before and after reset"
        );
        assert!(
            init_half >= 2,
            "expected initializing 0.5 after snapshots on 1/2 books"
        );
    }
}
