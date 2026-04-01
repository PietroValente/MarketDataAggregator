use std::{collections::HashMap, error::Error, time::{SystemTime, UNIX_EPOCH}};

use md_core::{book::BookLevels, events::{BookEventType, ControlEvent, EventEnvelope, NormalizedBookData, NormalizedEvent}, helpers::adapter::{clear_book_state, compute_status, send_normalized_event, send_status}, traits::adapter::ExchangeAdapter, types::{Exchange, Instrument}};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, warn};

use crate::types::{BitgetMdMsg, BookState, DepthBookAction, ParsedBookMessage, ValidateBookError, WsMessage};

pub struct BitgetAdapter {
    raw_rx: Receiver<BitgetMdMsg>,
    normalized_tx: Sender<EventEnvelope>,
    control_tx: Sender<ControlEvent>,
    book_states: HashMap<Instrument, BookState>,
    live_books: usize
}

impl BitgetAdapter {
    pub fn new(raw_rx: Receiver<BitgetMdMsg>, normalized_tx: Sender<EventEnvelope>, control_tx: Sender<ControlEvent>) -> Self {
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
        book.last_seq = Some(book_data.seq);
        Ok(())
    }

    fn validate_update(&mut self, payload: &ParsedBookMessage) -> Result<(), ValidateBookError> {
        if payload.action != DepthBookAction::Update {
            return Err(ValidateBookError::InvalidUpdateAction);
        }
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
                warn!(exchange = ?BitgetAdapter::exchange(), component = ?BitgetAdapter::component(), symbol = ?payload.arg.inst_id, "high latency for update: {} ms", latency);
            }
        }

        match book.last_seq {
            Some(last_seq) => {
                if book_data.seq <= last_seq {
                    return Err(ValidateBookError::StaleUpdate { new_seq: book_data.seq, last_seq });
                }
            },
            None => {
                return Err(ValidateBookError::MissingSnapshot)
            }
        }
        book.last_seq = Some(book_data.seq);
        Ok(())
    }

    pub fn run(&mut self) {
        while let Some(msg) = self.raw_rx.blocking_recv() {
            match msg {
                BitgetMdMsg::ResetBookState => {
                    self.live_books = 0;
                    clear_book_state(&mut self.book_states);
                    send_status::<BitgetAdapter>(&self.normalized_tx, compute_status(self.live_books, self.book_states.len()));
                },
                BitgetMdMsg::Instruments(symbols) => {
                    for i in symbols.iter() {
                        self.book_states.insert(i.clone(), BookState::new());
                    }
                    send_status::<BitgetAdapter>(&self.normalized_tx, compute_status(self.live_books, self.book_states.len()));
                },
                BitgetMdMsg::Raw(msg) => {
                    match serde_json::from_slice::<WsMessage>(&msg) {
                        Ok(WsMessage::Confirmation(_)) => {
                            continue;
                        },
                        Err(_) => {
                            let text = String::from_utf8_lossy(&msg);
                            error!(exchange = ?BitgetAdapter::exchange(), component = ?BitgetAdapter::component(), text = ?text, "error while parsing update");
                        },
                        Ok(WsMessage::Depth(depth)) => {
                            let inst_id = depth.arg.inst_id.clone();
                            let action = depth.action.clone();
        
                            match action {
                                DepthBookAction::Snapshot => {
                                    if let Err(e) = self.validate_snapshot(&depth) {
                                        error!(exchange = ?BitgetAdapter::exchange(), component = ?BitgetAdapter::component(), symbol = ?inst_id, error = ?e, "error while validating snapshot");                                        if let Err(e) = self.control_tx.blocking_send(ControlEvent::Resync) {
                                            error!(exchange = ?BitgetAdapter::exchange(), component = ?BitgetAdapter::component(), error = ?e, "error while sending resync");
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
                                        send_normalized_event::<BitgetAdapter>(&self.normalized_tx, snapshot_event);
                                    }
                                    send_status::<BitgetAdapter>(&self.normalized_tx, compute_status(self.live_books, self.book_states.len()));
                                },
                                DepthBookAction::Update => {
                                    match self.validate_update(&depth) {
                                        Err(e @ ValidateBookError::StaleUpdate { new_seq: _, last_seq: _ }) => {
                                            error!(
                                                exchange = ?BitgetAdapter::exchange(),
                                                component = ?BitgetAdapter::component(),
                                                symbol = ?depth.arg.inst_id,
                                                error = ?e,
                                                "error while validating update"
                                            );
                                            continue;
                                        },
                                        Err(e) => {
                                            error!(exchange = ?BitgetAdapter::exchange(), component = ?BitgetAdapter::component(), symbol = ?inst_id, error = ?e, "error while validating update");
                                            if let Err(e) = self.control_tx.blocking_send(ControlEvent::Resync) {
                                                error!(exchange = ?BitgetAdapter::exchange(), component = ?BitgetAdapter::component(), error = ?e, "error while sending resync");
                                            }
                                        },
                                        Ok(()) => {}
                                    }
                                    for data in depth.data {
                                        let update_event = NormalizedEvent::Book(BookEventType::Update, NormalizedBookData {
                                            instrument: Instrument::from(inst_id.clone()),
                                            levels: BookLevels {
                                                asks: data.asks,
                                                bids: data.bids
                                            }
                                        });
                                        send_normalized_event::<BitgetAdapter>(&self.normalized_tx, update_event);
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

impl ExchangeAdapter for BitgetAdapter {
    type SnapshotPayload = ParsedBookMessage;
    type UpdatePayload = ParsedBookMessage;

    fn exchange() -> Exchange {
        Exchange::Bitget
    }

    fn validate_snapshot(&mut self, payload: &Self::SnapshotPayload) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        BitgetAdapter::validate_snapshot(self, payload).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)
    }

    fn validate_update(&mut self, payload: &Self::UpdatePayload) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        BitgetAdapter::validate_update(self, payload).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)
    }

    fn run(&mut self) {
        BitgetAdapter::run(self)
    }
}