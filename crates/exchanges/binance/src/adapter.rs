use std::{collections::HashMap, error::Error, mem, time::{SystemTime, UNIX_EPOCH}};

use md_core::{book::BookLevels, events::{BookEventType, ControlEvent, EventEnvelope, NormalizedBookData, NormalizedEvent}, helpers::adapter::{clear_book_state, compute_status, send_normalized_event, send_status}, traits::adapter::ExchangeAdapter, types::{Exchange, Instrument}};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, warn};

use crate::types::{BinanceMdMsg, BookState, BookSyncStatus, ParsedBookSnapshot, ParsedBookUpdate, ValidateBookError, ValidateSnapshot, WsMessage};
pub struct BinanceAdapter {
    raw_tx: Sender<BinanceMdMsg>,
    raw_rx: Receiver<BinanceMdMsg>,
    normalized_tx: Sender<EventEnvelope>,
    control_tx: Sender<ControlEvent>,
    book_states: HashMap<Instrument, BookState>,
    live_books: usize
}

impl BinanceAdapter {
    pub fn new(raw_tx: Sender<BinanceMdMsg>, raw_rx: Receiver<BinanceMdMsg>, normalized_tx: Sender<EventEnvelope>, control_tx: Sender<ControlEvent>) -> Self {
        Self {
            raw_tx,
            raw_rx,
            normalized_tx,
            control_tx,
            book_states: HashMap::new(),
            live_books: 0
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
            return Err(ValidateBookError::InstrumentNotFound(payload.symbol.clone()));
        };
        book.last_applied_update_id = Some(payload.last_update_id);
        Ok(())
    }

    fn validate_update(&mut self, payload: &ParsedBookUpdate) -> Result<(), ValidateBookError> {
        if payload.event_type != "depthUpdate" {
            return Err(ValidateBookError::UnknownType(payload.event_type.clone()));
        }
        let Some(book) = self.book_states.get_mut(&payload.symbol) else {
            return Err(ValidateBookError::InstrumentNotFound(payload.symbol.clone()));
        };
        if let Ok(elapsed) = SystemTime::now().duration_since(UNIX_EPOCH) {
            let now = elapsed.as_millis() as u64;
            let latency = now.saturating_sub(payload.event_time);

            if latency > 1000 {
                warn!(exchange = ?BinanceAdapter::exchange(), component = ?BinanceAdapter::component(), symbol = ?payload.symbol, "high latency for update: {} ms", latency);
            }
        }

        let Some(last_applied_update_id) = book.last_applied_update_id else {
            return Err(ValidateBookError::MissingSnapshot);
        };

        if payload.final_update_id <= last_applied_update_id {
            return Err(ValidateBookError::StaleUpdate { event_last_update_id: payload.final_update_id, book_last_update_id: last_applied_update_id });
        }
        if payload.first_update_id > last_applied_update_id + 1 {
            return Err(ValidateBookError::UpdateGap { event_first_update_id: payload.first_update_id, expected_next_update_id: last_applied_update_id + 1 });
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
                    send_status::<BinanceAdapter>(&self.normalized_tx, compute_status(self.live_books, self.book_states.len()));
                },
                BinanceMdMsg::Instruments(list) => {
                    for i in list.iter() {
                        self.book_states.insert(i.clone(), BookState::new());
                    }
                    send_status::<BinanceAdapter>(&self.normalized_tx, compute_status(self.live_books, self.book_states.len()));
                },
                BinanceMdMsg::Snapshot(payload) => {
                    let Ok(parsed_snapshot) = serde_json::from_slice::<ParsedBookSnapshot>(&payload.payload) else {
                        let text = String::from_utf8_lossy(&payload.payload);
                        error!(exchange = ?BinanceAdapter::exchange(), component = ?BinanceAdapter::component(), symbol = ?payload.symbol, text = ?text, "error while parsing snapshot");
                        continue;
                    };
                    if let Err(e) = self.validate_snapshot(&ValidateSnapshot{
                        symbol: payload.symbol.clone(),
                        last_update_id: parsed_snapshot.last_update_id
                    }) {
                        error!(exchange = ?BinanceAdapter::exchange(), component = ?BinanceAdapter::component(), symbol = ?payload.symbol, error = ?e, "error while validating snapshot");                     
                        if let Err(e) = self.control_tx.blocking_send(ControlEvent::Resync) {
                            error!(exchange = ?BinanceAdapter::exchange(), component = ?BinanceAdapter::component(), error = ?e, "error while sending resync");
                        }
                    }
                    let book_snapshot = BookLevels {
                        asks: parsed_snapshot.asks,
                        bids: parsed_snapshot.bids
                    };
                    let snapshot_event = NormalizedEvent::Book(BookEventType::Snapshot, NormalizedBookData {
                        instrument: payload.symbol.clone(),
                        levels: book_snapshot,
                        checksum: None
                    });
                    send_normalized_event::<BinanceAdapter>(&self.normalized_tx, snapshot_event);
                    self.drain_buffered_updates(payload.symbol);
                    send_status::<BinanceAdapter>(&self.normalized_tx, compute_status(self.live_books, self.book_states.len()));
                },
                BinanceMdMsg::WsMessage(payload) => {
                    match serde_json::from_slice::<WsMessage>(&payload) {
                        Ok(WsMessage::Confirmation(confirmation)) => {
                            if let Some(result) = confirmation.result {
                                error!(exchange = ?BinanceAdapter::exchange(), component = ?BinanceAdapter::component(), result = ?result,"subscription result different from null");
                            }
                        },
                        Err(_) => {
                            let text = String::from_utf8_lossy(&payload);
                            error!(exchange = ?BinanceAdapter::exchange(), component = ?BinanceAdapter::component(), text = ?text, "error while parsing update");
                        },
                        Ok(WsMessage::Update(update)) => {
                            let Some(book) = self.book_states.get_mut(&update.symbol) else {
                                error!(exchange = ?BinanceAdapter::exchange(), component = ?BinanceAdapter::component(), symbol = ?update.symbol, "symbol not found");
                                continue;
                            };
                            match book.status {
                                BookSyncStatus::Live => {},
                                BookSyncStatus::WaitingSnapshot => {
                                    book.symbols_pending_snapshot.push(payload);
                                    continue;
                                }
                            };

                            //process the update
                            match self.validate_update(&update) {
                                Err(e @ ValidateBookError::UpdateGap {
                                    event_first_update_id,
                                    expected_next_update_id,
                                }) => {
                                    error!(
                                        exchange = ?BinanceAdapter::exchange(),
                                        component = ?BinanceAdapter::component(),
                                        symbol = ?update.symbol,
                                        error = ?e,
                                        event_first_update_id = event_first_update_id,
                                        expected_next_update_id = expected_next_update_id,
                                        "error while validating update"
                                    );
                                    if let Err(e) = self.control_tx.blocking_send(ControlEvent::Resync) {
                                        error!(
                                            exchange = ?BinanceAdapter::exchange(),
                                            component = ?BinanceAdapter::component(),
                                            error = ?e,
                                            "error while sending resync"
                                        );
                                    }
                                    continue;
                                },
                                Err(e) => {
                                    error!(
                                        exchange = ?BinanceAdapter::exchange(),
                                        component = ?BinanceAdapter::component(),
                                        symbol = ?update.symbol,
                                        error = ?e,
                                        "error while validating update"
                                    );
                                    continue;
                                },
                                Ok(()) => {}
                            }
        
                            let book_update = BookLevels {
                                bids: update.bids,
                                asks: update.asks
                            };
        
                            let update_event = NormalizedEvent::Book(BookEventType::Update, NormalizedBookData {
                                instrument: update.symbol.clone(),
                                levels: book_update,
                                checksum: None
                            });
                            send_normalized_event::<BinanceAdapter>(&self.normalized_tx, update_event);
                        },
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

    fn validate_snapshot(&mut self, payload: &Self::SnapshotPayload) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        BinanceAdapter::validate_snapshot(self, payload).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)
    }

    fn validate_update(&mut self, payload: &Self::UpdatePayload) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        BinanceAdapter::validate_update(self, payload).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)
    }

    fn run(&mut self) {
        BinanceAdapter::run(self)
    }
}