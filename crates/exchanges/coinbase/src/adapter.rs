use std::collections::HashMap;
use std::error::Error;
use std::time::{SystemTime, UNIX_EPOCH};
use chrono::DateTime;
use md_core::traits::adapter::ExchangeAdapter;
use md_core::events::ControlEvent;
use md_core::types::ExchangeStatus;
use md_core::{book::BookLevels, events::{BookEventType, EventEnvelope, NormalizedBookData, NormalizedEvent}, types::{Exchange, Instrument}};
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

    fn clear_book_state(&mut self) {
        for (_, book) in &mut self.book_states {
            book.initialized = false;
        }
        self.live_books = 0;
        
        let status = ExchangeStatus::Initializing(0.0);
        let event_envelope = EventEnvelope {
            exchange: CoinbaseAdapter::exchange(),
            event: NormalizedEvent::ApplyStatus(status)
        };
        if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
            error!(exchange = ?CoinbaseAdapter::exchange(), component = ?CoinbaseAdapter::component(), error = ?e, "error while sending running status event");
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
                CoinbaseMdMsg::Instruments(symbols) => {
                    for i in symbols.iter() {
                        self.book_states.insert(i.clone(), BookState::new());
                    }
                    let event_envelope = EventEnvelope {
                        exchange: CoinbaseAdapter::exchange(),
                        event: NormalizedEvent::ApplyStatus(ExchangeStatus::Initializing(0.0))
                    };
                    if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                        error!(exchange = ?CoinbaseAdapter::exchange(), component = ?CoinbaseAdapter::component(), error = ?e, "error while sending running status event");
                    }
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
                                self.clear_book_state();
                                if let Err(e) = self.control_tx.blocking_send(ControlEvent::Resync) {
                                    error!(exchange = ?CoinbaseAdapter::exchange(), component = ?CoinbaseAdapter::component(), error = ?e, "error while sending resync");
                                }
                            }
                            let snapshot_event = NormalizedEvent::Book(BookEventType::Snapshot, NormalizedBookData {
                                instrument: Instrument::from(inst_id),
                                levels: BookLevels {
                                    asks: depth.asks,
                                    bids: depth.bids
                                }
                            });
                            let event_envelope = EventEnvelope {
                                exchange: CoinbaseAdapter::exchange(),
                                event: snapshot_event
                            };
        
                            if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                                error!(exchange = ?CoinbaseAdapter::exchange(), component = ?CoinbaseAdapter::component(), error = ?e, "error while sending snapshot event");
                            }

                            let mut status = ExchangeStatus::Initializing(0.0);
                            if !self.book_states.is_empty() {
                                status = ExchangeStatus::Initializing(self.live_books as f32 / self.book_states.len() as f32);
                            }
                            if self.live_books == self.book_states.len() && !self.book_states.is_empty() {
                                status = ExchangeStatus::Running;
                            }
                            let event_envelope = EventEnvelope {
                                exchange: CoinbaseAdapter::exchange(),
                                event: NormalizedEvent::ApplyStatus(status)
                            };
                            if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                                error!(exchange = ?CoinbaseAdapter::exchange(), component = ?CoinbaseAdapter::component(), error = ?e, "error while sending running status event");
                            }
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
        
                            let snapshot_event = NormalizedEvent::Book(BookEventType::Update, NormalizedBookData {
                                instrument: Instrument::from(inst_id),
                                levels: BookLevels {
                                    asks,
                                    bids
                                }
                            });
                            let event_envelope = EventEnvelope {
                                exchange: CoinbaseAdapter::exchange(),
                                event: snapshot_event
                            };
        
                            if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                                error!(exchange = ?CoinbaseAdapter::exchange(), component = ?CoinbaseAdapter::component(), error = ?e, "error while sending snapshot event");
                            }
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