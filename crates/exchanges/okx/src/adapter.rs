use std::{collections::HashMap, error::Error, time::{SystemTime, UNIX_EPOCH}};

use md_core::{book::BookLevels, events::{BookEventType, ControlEvent, EventEnvelope, NormalizedBookData, NormalizedEvent}, traits::adapter::ExchangeAdapter, types::{Exchange, ExchangeStatus, Instrument}};
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

    fn clear_book_state(&mut self) {
        for (_, book) in &mut self.book_states {
            book.initialized = false;
            book.prev_seq_id = None;
        }
        self.live_books = 0;

        let status = ExchangeStatus::Initializing(0.0);
        let event_envelope = EventEnvelope {
            exchange: OkxAdapter::exchange(),
            event: NormalizedEvent::ApplyStatus(status)
        };
        if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
            error!(exchange = ?OkxAdapter::exchange(), component = ?OkxAdapter::component(), error = ?e, "error while sending running status event");
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
                OkxMdMsg::ClearBookState => {
                    self.clear_book_state();
                },
                OkxMdMsg::Instruments(symbols) => {
                    for i in symbols.iter() {
                        self.book_states.insert(i.clone(), BookState::new());
                    }
                    let event_envelope = EventEnvelope {
                        exchange: OkxAdapter::exchange(),
                        event: NormalizedEvent::ApplyStatus(ExchangeStatus::Initializing(0.0))
                    };
                    if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                        error!(exchange = ?OkxAdapter::exchange(), component = ?OkxAdapter::component(), error = ?e, "error while sending running status event");
                    }
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
                                        let snapshot_event = NormalizedEvent::Book(BookEventType::Snapshot, NormalizedBookData {
                                            instrument: Instrument::from(inst_id.clone()),
                                            levels: BookLevels {
                                                asks: data.asks,
                                                bids: data.bids
                                            }
                                        });
                                        let event_envelope = EventEnvelope {
                                            exchange: OkxAdapter::exchange(),
                                            event: snapshot_event
                                        };
                    
                                        if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                                            error!(exchange = ?OkxAdapter::exchange(), component = ?OkxAdapter::component(), error = ?e, "error while sending snapshot event");
                                        }
                                    }
                                    let mut status = ExchangeStatus::Initializing(0.0);
                                    if !self.book_states.is_empty() {
                                        status = ExchangeStatus::Initializing(self.live_books as f32 / self.book_states.len() as f32);
                                    }
                                    if self.live_books == self.book_states.len() && !self.book_states.is_empty() {
                                        status = ExchangeStatus::Running;
                                    }
                                    let event_envelope = EventEnvelope {
                                        exchange: OkxAdapter::exchange(),
                                        event: NormalizedEvent::ApplyStatus(status)
                                    };
                                    if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                                        error!(exchange = ?OkxAdapter::exchange(), component = ?OkxAdapter::component(), error = ?e, "error while sending running status event");
                                    }
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
                                        let snapshot_event = NormalizedEvent::Book(BookEventType::Update, NormalizedBookData {
                                            instrument: Instrument::from(inst_id.clone()),
                                            levels: BookLevels {
                                                asks: data.asks,
                                                bids: data.bids
                                            }
                                        });
                                        let event_envelope = EventEnvelope {
                                            exchange: OkxAdapter::exchange(),
                                            event: snapshot_event
                                        };
                    
                                        if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                                            error!(exchange = ?OkxAdapter::exchange(), component = ?OkxAdapter::component(), error = ?e, "error while sending snapshot event");
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