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