use std::{collections::HashMap, error::Error, time::{SystemTime, UNIX_EPOCH}};

use chrono::DateTime;
use md_core::{book::BookLevels, events::{BookEventType, ControlEvent, EventEnvelope, NormalizedBookData, NormalizedEvent}, helpers::adapter::{clear_book_state, compute_status, send_normalized_event, send_status}, traits::adapter::ExchangeAdapter, types::{Exchange, Instrument}};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, warn};

use crate::types::{BookState, CoinbaseMdMsg, ParsedBookSnapshot, ParsedBookUpdate, Side, ValidateBookError, WsMessage};

pub struct CoinbaseAdapter {
    raw_rx: Receiver<CoinbaseMdMsg>,
    normalized_tx: Sender<EventEnvelope>,
    control_tx: Sender<ControlEvent>,
    book_states: HashMap<Instrument, BookState>,
    live_books: usize
}

impl CoinbaseAdapter {
    pub fn new(raw_rx: Receiver<CoinbaseMdMsg>, normalized_tx: Sender<EventEnvelope>, control_tx: Sender<ControlEvent>) -> Self {
        Self {
            raw_rx,
            normalized_tx,
            control_tx,
            book_states: HashMap::new(),
            live_books: 0
        }
    }

    fn validate_snapshot(&mut self, payload: &ParsedBookSnapshot) -> Result<(), ValidateBookError> {
        let Some(book) = self.book_states.get_mut(&payload.product_id) else {
            return Err(ValidateBookError::InstrumentNotFound(payload.product_id.clone()));
        };
        if !book.initialized {
            book.initialized = true;
            self.live_books += 1;
        }
        Ok(())
    }

    fn validate_update(&mut self, payload: &ParsedBookUpdate) -> Result<(), ValidateBookError> {
        let Some(book) = self.book_states.get_mut(&payload.product_id) else {
            return Err(ValidateBookError::InstrumentNotFound(payload.product_id.clone()));
        };

        if let Ok(elapsed) = SystemTime::now().duration_since(UNIX_EPOCH) {
            let now = elapsed.as_millis() as u64;
            let payload_ms = DateTime::parse_from_rfc3339(&payload.time)
                .map_err(|_| ValidateBookError::InvalidTimestamp(payload.time.clone()))?
                .timestamp_millis() as u64;
            let latency = now.saturating_sub(payload_ms);

            if latency > 1000 {
                warn!(exchange = ?CoinbaseAdapter::exchange(), component = ?CoinbaseAdapter::component(), symbol = ?payload.product_id, "high latency for update: {} ms", latency);
            }
        }

        if !book.initialized {
            return Err(ValidateBookError::MissingSnapshot)
        }
        Ok(())
    }

    pub fn run(&mut self) {
        while let Some(msg) = self.raw_rx.blocking_recv() {
            match msg {
                CoinbaseMdMsg::ResetBookState => {
                    self.live_books = 0;
                    clear_book_state(&mut self.book_states);
                    send_status::<CoinbaseAdapter>(&self.normalized_tx, compute_status(self.live_books, self.book_states.len()));
                },
                CoinbaseMdMsg::Instruments(symbols) => {
                    for i in symbols.iter() {
                        self.book_states.insert(i.clone(), BookState::new());
                    }
                    send_status::<CoinbaseAdapter>(&self.normalized_tx, compute_status(self.live_books, self.book_states.len()));
                },
                CoinbaseMdMsg::Raw(msg) => {
                    match serde_json::from_slice::<WsMessage>(&msg) {
                        Ok(WsMessage::Confirmation(_)) => {
                            continue;
                        },
                        Err(_) => {
                            let text = String::from_utf8_lossy(&msg);
                            error!(exchange = ?CoinbaseAdapter::exchange(), component = ?CoinbaseAdapter::component(), text = ?text, "error while parsing update");
                        },
                        Ok(WsMessage::Snapshot(depth)) => {
                            let inst_id = depth.product_id.replace("-", "");
                            if let Err(e) = self.validate_snapshot(&depth) {
                                error!(exchange = ?CoinbaseAdapter::exchange(), component = ?CoinbaseAdapter::component(), error = ?e, "error while validating snapshot");
                                if let Err(e) = self.control_tx.blocking_send(ControlEvent::Resync) {
                                    error!(exchange = ?CoinbaseAdapter::exchange(), component = ?CoinbaseAdapter::component(), error = ?e, "error while sending resync");
                                }
                            }
                            let snapshot_event = NormalizedEvent::Book(BookEventType::Snapshot, NormalizedBookData {
                                instrument: Instrument::from(inst_id),
                                levels: BookLevels {
                                    asks: depth.asks,
                                    bids: depth.bids
                                },
                                checksum: None
                            });
                            send_normalized_event::<CoinbaseAdapter>(&self.normalized_tx, snapshot_event);
                            send_status::<CoinbaseAdapter>(&self.normalized_tx, compute_status(self.live_books, self.book_states.len()));
                        },
                        Ok(WsMessage::Update(depth)) => {
                            let inst_id = depth.product_id.replace("-", "");
                            if let Err(e) = self.validate_update(&depth) {
                                error!(exchange = ?CoinbaseAdapter::exchange(), component = ?CoinbaseAdapter::component(), error = ?e, "error while validating update");
                                if let Err(e) = self.control_tx.blocking_send(ControlEvent::Resync) {
                                    error!(exchange = ?CoinbaseAdapter::exchange(), component = ?CoinbaseAdapter::component(), error = ?e, "error while sending resync");
                                }
                            }
                            let mut bids = Vec::new();
                            let mut asks = Vec::new();
        
                            for (side, price, qty) in depth.changes {
                                match side {
                                    Side::Bid => bids.push((price, qty)),
                                    Side::Ask => asks.push((price, qty)),
                                }
                            }
        
                            let update_event = NormalizedEvent::Book(BookEventType::Update, NormalizedBookData {
                                instrument: Instrument::from(inst_id),
                                levels: BookLevels {
                                    asks,
                                    bids
                                },
                                checksum: None
                            });
                            send_normalized_event::<CoinbaseAdapter>(&self.normalized_tx, update_event);
                        }
                    }        
                }
            }
        }
    }
}

impl ExchangeAdapter for CoinbaseAdapter {
    type SnapshotPayload = ParsedBookSnapshot;
    type UpdatePayload = ParsedBookUpdate;

    fn exchange() -> Exchange {
        Exchange::Coinbase
    }

    fn validate_snapshot(&mut self, payload: &Self::SnapshotPayload) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        CoinbaseAdapter::validate_snapshot(self, payload).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)
    }

    fn validate_update(&mut self, payload: &Self::UpdatePayload) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        CoinbaseAdapter::validate_update(self, payload).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)
    }

    fn run(&mut self) {
        CoinbaseAdapter::run(self)
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

    fn update(symbol: &str, ts: &str) -> ParsedBookUpdate {
        ParsedBookUpdate {
            op_type: "l2update".to_owned(),
            product_id: instrument(symbol),
            changes: vec![
                (Side::Bid, Price(Decimal::new(10000, 2)), Qty(Decimal::new(1000, 3))),
                (Side::Ask, Price(Decimal::new(10100, 2)), Qty(Decimal::new(2000, 3))),
            ],
            time: ts.to_owned(),
        }
    }

    fn snapshot(symbol: &str) -> ParsedBookSnapshot {
        ParsedBookSnapshot {
            op_type: "snapshot".to_owned(),
            product_id: instrument(symbol),
            asks: vec![(Price(Decimal::new(10100, 2)), Qty(Decimal::new(1000, 3)))],
            bids: vec![(Price(Decimal::new(10000, 2)), Qty(Decimal::new(1000, 3)))],
            time: "2024-01-01T00:00:00Z".to_owned(),
        }
    }

    #[test]
    fn snapshot_initialization_is_idempotent() {
        let (_raw_tx, raw_rx) = mpsc::channel(8);
        let (normalized_tx, _normalized_rx) = mpsc::channel(8);
        let (control_tx, _control_rx) = mpsc::channel(8);
        let mut adapter = CoinbaseAdapter::new(raw_rx, normalized_tx, control_tx);
        let symbol = instrument("BTCUSD");
        adapter.book_states.insert(symbol.clone(), BookState::new());

        adapter.validate_snapshot(&snapshot("BTCUSD")).unwrap();
        assert_eq!(adapter.live_books, 1);
        assert!(adapter.book_states.get(&symbol).unwrap().initialized);

        adapter.validate_snapshot(&snapshot("BTCUSD")).unwrap();
        assert_eq!(adapter.live_books, 1);
        assert!(adapter.book_states.get(&symbol).unwrap().initialized);
    }

    #[test]
    fn update_rejects_missing_snapshot_and_invalid_timestamp() {
        let (_raw_tx, raw_rx) = mpsc::channel(8);
        let (normalized_tx, _normalized_rx) = mpsc::channel(8);
        let (control_tx, _control_rx) = mpsc::channel(8);
        let mut adapter = CoinbaseAdapter::new(raw_rx, normalized_tx, control_tx);
        adapter.book_states.insert(instrument("BTCUSD"), BookState::new());

        let err = adapter
            .validate_update(&update("BTCUSD", "2024-01-01T00:00:00Z"))
            .expect_err("update before snapshot must fail");
        assert!(matches!(err, ValidateBookError::MissingSnapshot));

        adapter.validate_snapshot(&snapshot("BTCUSD")).unwrap();
        let ts_err = adapter
            .validate_update(&update("BTCUSD", "not-a-timestamp"))
            .expect_err("invalid timestamp must fail");
        assert!(matches!(ts_err, ValidateBookError::InvalidTimestamp(_)));
    }

    #[test]
    fn random_timestamp_inputs_do_not_break_validation_contract() {
        fn next_u64(state: &mut u64) -> u64 {
            *state ^= *state << 13;
            *state ^= *state >> 7;
            *state ^= *state << 17;
            *state
        }

        let (_raw_tx, raw_rx) = mpsc::channel(8);
        let (normalized_tx, _normalized_rx) = mpsc::channel(8);
        let (control_tx, _control_rx) = mpsc::channel(8);
        let mut adapter = CoinbaseAdapter::new(raw_rx, normalized_tx, control_tx);
        adapter.book_states.insert(instrument("BTCUSD"), BookState::new());
        adapter.validate_snapshot(&snapshot("BTCUSD")).unwrap();

        let mut seed = 0xC01B_A553_u64;
        for _ in 0..3000 {
            let roll = (next_u64(&mut seed) % 100) as u8;
            let ts = if roll < 80 {
                "2024-01-01T00:00:00Z".to_owned()
            } else {
                format!("bad-ts-{}", next_u64(&mut seed))
            };
            let result = adapter.validate_update(&update("BTCUSD", &ts));
            if ts.starts_with("bad-ts-") {
                assert!(matches!(result, Err(ValidateBookError::InvalidTimestamp(_))));
            } else {
                assert!(result.is_ok(), "valid RFC3339 timestamp should pass");
            }
        }
    }
}