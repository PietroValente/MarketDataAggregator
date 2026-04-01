use std::{collections::HashMap, error::Error, time::{SystemTime, UNIX_EPOCH}};

use md_core::{book::BookLevels, events::{BookEventType, ControlEvent, EventEnvelope, NormalizedBookData, NormalizedEvent}, helpers::adapter::{clear_book_state, compute_status, send_normalized_event, send_status}, traits::adapter::ExchangeAdapter, types::{Exchange, Instrument}};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, warn};

use crate::types::{BookState, BybitMdMsg, DepthBookAction, ParsedBookMessage, ValidateBookError, WsMessage};

pub struct BybitAdapter {
    raw_rx: Receiver<BybitMdMsg>,
    normalized_tx: Sender<EventEnvelope>,
    control_tx: Sender<ControlEvent>,
    book_states: HashMap<Instrument, BookState>,
    live_books: usize
}

impl BybitAdapter {
    pub fn new(raw_rx: Receiver<BybitMdMsg>, normalized_tx: Sender<EventEnvelope>, control_tx: Sender<ControlEvent>) -> Self {
        Self {
            raw_rx,
            normalized_tx,
            control_tx,
            book_states: HashMap::new(),
            live_books: 0
        }
    }

    fn validate_snapshot(&mut self, payload: &ParsedBookMessage) -> Result<(), ValidateBookError> {
        if payload.action != DepthBookAction::Snapshot {
            return Err(ValidateBookError::InvalidSnapshotAction);
        }
        let Some(book) = self.book_states.get_mut(&payload.data.symbol) else {
            return Err(ValidateBookError::InstrumentNotFound(payload.data.symbol.clone()));
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
            return Err(ValidateBookError::InstrumentNotFound(payload.data.symbol.clone()));
        };

        if let Ok(elapsed) = SystemTime::now().duration_since(UNIX_EPOCH) {
            let now = elapsed.as_millis() as u64;
            let latency = now.saturating_sub(payload.cts);

            if latency > 1000 {
                warn!(exchange = ?BybitAdapter::exchange(), component = ?BybitAdapter::component(), symbol = ?payload.data.symbol, "high latency for update: {} ms", latency);
            }
        }

        match book.last_update_id {
            Some(last_update_id) => {
                if payload.data.update_id <= last_update_id {
                    return Err(ValidateBookError::StaleUpdate { new_update_id: payload.data.update_id, last_update_id });
                }
            },
            None => {
                return Err(ValidateBookError::MissingSnapshot)
            }
        }
        book.last_update_id = Some(payload.data.update_id);
        Ok(())
    }

    pub fn run(&mut self) {
        while let Some(msg) = self.raw_rx.blocking_recv() {
            match msg {
                BybitMdMsg::ResetBookState => {
                    self.live_books = 0;
                    clear_book_state(&mut self.book_states);
                    send_status::<BybitAdapter>(&self.normalized_tx, compute_status(self.live_books, self.book_states.len()));
                },
                BybitMdMsg::Instruments(symbols) => {
                    for i in symbols.iter() {
                        self.book_states.insert(i.clone(), BookState::new());
                    }
                    send_status::<BybitAdapter>(&self.normalized_tx, compute_status(self.live_books, self.book_states.len()));
                },
                BybitMdMsg::Raw(msg) => {
                    match serde_json::from_slice::<WsMessage>(&msg) {
                        Ok(WsMessage::Confirmation(_)) => {
                            continue;
                        },
                        Err(_) => {
                            let text = String::from_utf8_lossy(&msg);
                            error!(exchange = ?BybitAdapter::exchange(), component = ?BybitAdapter::component(), text = ?text, "error while parsing update");
                        },
                        Ok(WsMessage::Depth(depth)) => {
                            let action = depth.action.clone();
        
                            match action {
                                DepthBookAction::Snapshot => {
                                    if let Err(e) = self.validate_snapshot(&depth) {
                                        error!(exchange = ?BybitAdapter::exchange(), component = ?BybitAdapter::component(), symbol = ?depth.data.symbol, error = ?e, "error while validating snapshot");
                                        if let Err(e) = self.control_tx.blocking_send(ControlEvent::Resync) {
                                            error!(exchange = ?BybitAdapter::exchange(), component = ?BybitAdapter::component(), error = ?e, "error while sending resync");
                                        }
                                    }
                                    let snapshot_event = NormalizedEvent::Book(BookEventType::Snapshot, NormalizedBookData {
                                        instrument: Instrument::from(depth.data.symbol),
                                        levels: BookLevels {
                                            asks: depth.data.asks,
                                            bids: depth.data.bids
                                        },
                                        checksum: None
                                    });
                                    send_normalized_event::<BybitAdapter>(&self.normalized_tx, snapshot_event);
                                    send_status::<BybitAdapter>(&self.normalized_tx, compute_status(self.live_books, self.book_states.len()));
                                },
                                DepthBookAction::Delta => {
                                    match self.validate_update(&depth) {
                                        Err(e @ ValidateBookError::StaleUpdate { new_update_id: _, last_update_id: _ }) => {
                                            error!(
                                                exchange = ?BybitAdapter::exchange(),
                                                component = ?BybitAdapter::component(),
                                                symbol = ?depth.data.symbol,
                                                error = ?e,
                                                "error while validating update"
                                            );
                                            continue;
                                        },
                                        Err(e) => {
                                            error!(exchange = ?BybitAdapter::exchange(), component = ?BybitAdapter::component(), symbol = ?depth.data.symbol, error = ?e, "error while validating update");
                                            if let Err(e) = self.control_tx.blocking_send(ControlEvent::Resync) {
                                                error!(exchange = ?BybitAdapter::exchange(), component = ?BybitAdapter::component(), error = ?e, "error while sending resync");
                                            }
                                        },
                                        Ok(()) => {}
                                    }
                                    let update_event = NormalizedEvent::Book(BookEventType::Update, NormalizedBookData {
                                        instrument: Instrument::from(depth.data.symbol),
                                        levels: BookLevels {
                                            asks: depth.data.asks,
                                            bids: depth.data.bids
                                        },
                                        checksum: None
                                    });
                                    send_normalized_event::<BybitAdapter>(&self.normalized_tx, update_event);
                                }
                            }
                        }
                    }        
                }
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

    fn validate_snapshot(&mut self, payload: &Self::SnapshotPayload) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        BybitAdapter::validate_snapshot(self, payload).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)
    }

    fn validate_update(&mut self, payload: &Self::UpdatePayload) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        BybitAdapter::validate_update(self, payload).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)
    }

    fn run(&mut self) {
        BybitAdapter::run(self)
    }
}