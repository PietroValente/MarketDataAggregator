use std::{collections::{hash_map::Entry, HashMap}, time::{SystemTime, UNIX_EPOCH}};

use md_core::adapter_trait::ExchangeAdapter;
use md_core::{book::BookLevels, events::{BookEventType, EventEnvelope, NormalizedBookData, NormalizedEvent}, logging::types::Component, types::{Exchange, Instrument}};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, info, warn};

use crate::types::{BinanceMdMsg, ParsedBookSnapshot, ParsedBookUpdate, WsMessage};

pub struct BinanceAdapter {
    raw_rx: Receiver<BinanceMdMsg>,
    normalized_tx: Sender<EventEnvelope>,
    symbols_pending_snapshot: HashMap<Instrument, Vec<EventEnvelope>>
}

impl BinanceAdapter {
    pub fn new(raw_rx: Receiver<BinanceMdMsg>, normalized_tx: Sender<EventEnvelope>) -> Self {
        Self {
            raw_rx,
            normalized_tx,
            symbols_pending_snapshot: HashMap::new()
        }
    }

    fn drain_buffered_updates(&mut self, i: Instrument) {
        let Some(buffer_updates) = self.symbols_pending_snapshot.remove(&i) else {
            error!(exchange = ?Exchange::Binance, component = ?Component::Adapter, symbol = ?i, "symbol live not found");
            return;
        };
        for u in buffer_updates {
            if let Err(e) = self.normalized_tx.blocking_send(u) {
                error!(exchange = ?Exchange::Binance, component = ?Component::Adapter, error = ?e, "error while sending the update event");
            }
        }
    }

    pub fn run(&mut self) {
        while let Some(msg) = self.raw_rx.blocking_recv() {
            match msg {
                BinanceMdMsg::Instruments(list) => {
                    for i in list {
                        self.symbols_pending_snapshot.insert(i, Vec::new());
                    }
                },
                BinanceMdMsg::Snapshot(payload) => {
                    let Ok(parsed_snapshot) = serde_json::from_slice::<ParsedBookSnapshot>(&payload.payload) else {
                        error!(exchange = ?Exchange::Binance, component = ?Component::Adapter, symbol = ?payload.symbol, text = ?String::from_utf8(payload.payload.clone()).unwrap(), "error while parsing snapshot");
                        continue;
                    };
                    let book_snapshot = BookLevels {
                        asks: parsed_snapshot.asks,
                        bids: parsed_snapshot.bids
                    };
                    let snapshot_event = NormalizedEvent::Book(BookEventType::Snapshot, NormalizedBookData {
                        instrument: payload.symbol.clone(),
                        levels: book_snapshot
                    });
                    let event_envelope = EventEnvelope {
                        exchange: Exchange::Binance,
                        event: snapshot_event
                    };

                    if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                        error!(exchange = ?Exchange::Binance, component = ?Component::Adapter, error = ?e, "error while sending snapshot event");
                    }

                    self.drain_buffered_updates(payload.symbol);
                },
                BinanceMdMsg::WsMessage(payload) => {
                    match serde_json::from_slice::<WsMessage>(&payload) {
                        Ok(WsMessage::Confirmation(confirmation)) => {
                            if let Some(result) = confirmation.result {
                                error!(exchange = ?Exchange::Binance, component = ?Component::Adapter, result = ?result,"subscription result different from null");
                            }
                        },
                        Err(_) => {
                            let text = String::from_utf8_lossy(&payload);
                            error!(exchange = ?Exchange::Binance, component = ?Component::Adapter, text = ?text, "error while parsing update");
                        },
                        Ok(WsMessage::Update(update)) => {
                            if update.event_type != "depthUpdate" {
                                error!(exchange = ?Exchange::Binance, component = ?Component::Adapter, symbol = ?update.symbol, "unknown type of event: {}", update.event_type);
                                continue;
                            }
        
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64;
        
                            let latency = now.saturating_sub(update.event_time);
        
                            if latency > 1000 {
                                warn!(exchange = ?Exchange::Binance, component = ?Component::Adapter, symbol = ?update.symbol, "high latency for update: {} ms", latency);
                            }
        
                            let book_update = BookLevels {
                                bids: update.bids,
                                asks: update.asks
                            };
        
                            let update_event = NormalizedEvent::Book(BookEventType::Update, NormalizedBookData {
                                instrument: update.symbol.clone(),
                                levels: book_update
                            });
                            let event_envelope = EventEnvelope {
                                exchange: Exchange::Binance,
                                event: update_event
                            };
                            
                            match self.symbols_pending_snapshot.entry(update.symbol.clone()) {
                                Entry::Occupied(mut e) => {
                                    info!(exchange = ?Exchange::Binance, component = ?Component::Adapter, symbol = ?update.symbol, "buffering symbol not live");
                                    e.get_mut().push(event_envelope);
                                }
                                Entry::Vacant(_) => {
                                    if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                                        error!(exchange = ?Exchange::Binance, component = ?Component::Adapter, error = ?e, "error while sending update event");
                                    }
                                }
                            }
                        },
                    }
                }
            }
        }
    }
}

impl ExchangeAdapter for BinanceAdapter {
    type SnapshotPayload = ParsedBookSnapshot;
    type UpdatePayload = ParsedBookUpdate;

    fn exchange(&self) -> Exchange {
        Exchange::Binance
    }

    fn validate_snapshot(&mut self, _payload: &Self::SnapshotPayload) {
        todo!()
    }

    fn validate_update(&mut self, _payload: &Self::UpdatePayload) {
        todo!()
    }

    fn run(&mut self) {
        BinanceAdapter::run(self)
    }
}