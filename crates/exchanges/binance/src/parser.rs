use std::{collections::{hash_map::Entry, HashMap}, time::{SystemTime, UNIX_EPOCH}};

use md_core::{book::{BookSnapshot, BookUpdate}, events::{EventEnvelope, NormalizedEvent, NormalizedSnapshot, NormalizedUpdate}, logging::types::Component, types::{Exchange, Instrument}};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, info, warn};

use crate::types::{BinanceMdMsg, DepthSnapshot, DepthUpdate};

pub struct BinanceParser {
    raw_rx: Receiver<BinanceMdMsg>,
    normalized_tx: Sender<EventEnvelope>,
    symbols_pending_snapshot: HashMap<Instrument, Vec<EventEnvelope>>
}

impl BinanceParser {
    pub fn new(raw_rx: Receiver<BinanceMdMsg>, normalized_tx: Sender<EventEnvelope>) -> Self {
        Self {
            raw_rx,
            normalized_tx,
            symbols_pending_snapshot: HashMap::new()
        }
    }

    fn drain_buffered_updates(&mut self, i: Instrument) {
        let Some(buffer_updates) = self.symbols_pending_snapshot.remove(&i) else {
            error!(exchange = ?Exchange::Binance, component = ?Component::Parser, symbol = ?i, "symbol live not found");
            return;
        };
        for u in buffer_updates {
            if let Err(e) = self.normalized_tx.blocking_send(u) {
                error!(exchange = ?Exchange::Binance, component = ?Component::Parser, error = ?e, "error while sending the update event");
            }
        }
    }

    pub fn start(&mut self) {
        while let Some(msg) = self.raw_rx.blocking_recv() {
            match msg {
                BinanceMdMsg::Instruments(list) => {
                    for i in list {
                        self.symbols_pending_snapshot.insert(i, Vec::new());
                    }
                },
                BinanceMdMsg::Snapshot(payload) => {
                    let Ok(parsed_snapshot) = serde_json::from_slice::<DepthSnapshot>(&payload.payload) else {
                        error!(exchange = ?Exchange::Binance, component = ?Component::Parser, symbol = ?payload.symbol, text = ?String::from_utf8(payload.payload.clone()).unwrap(), "error while parsing snapshot");
                        continue;
                    };
                    let book_snapshot = BookSnapshot {
                        last_update_id: parsed_snapshot.last_update_id,
                        asks: parsed_snapshot.asks,
                        bids: parsed_snapshot.bids
                    };
                    let snapshot_event = NormalizedEvent::Snapshot(NormalizedSnapshot {
                        instrument: payload.symbol.clone(),
                        data: book_snapshot
                    });
                    let event_envelope = EventEnvelope {
                        exchange: Exchange::Binance,
                        event: snapshot_event
                    };

                    if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                        error!(exchange = ?Exchange::Binance, component = ?Component::Parser, error = ?e, "error while sending snapshot event");
                    }

                    self.drain_buffered_updates(payload.symbol);
                },
                BinanceMdMsg::Update(payload) => {
                    let Ok(parsed_update) = serde_json::from_slice::<DepthUpdate>(&payload) else {
                        let text = String::from_utf8(payload.clone()).unwrap();
                        if !text.contains(r#""result": null,"#) { //subscription confirmation message
                            error!(exchange = ?Exchange::Binance, component = ?Component::Parser, text = ?text, "error while parsing update");
                        }
                        continue;
                    };

                    if parsed_update.event_type != "depthUpdate" {
                        error!(exchange = ?Exchange::Binance, component = ?Component::Parser, symbol = ?parsed_update.symbol, "unknown type of event: {}", parsed_update.event_type);
                        continue;
                    }

                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;

                    let latency = now.saturating_sub(parsed_update.event_time);

                    if latency > 1000 {
                        warn!(exchange = ?Exchange::Binance, component = ?Component::Parser, symbol = ?parsed_update.symbol, "high latency for update: {} ms", latency);
                    }

                    let book_update = BookUpdate {
                        first_update_id: parsed_update.first_update_id,
                        last_update_id: parsed_update.final_update_id,
                        bids: parsed_update.bids,
                        asks: parsed_update.asks
                    };

                    let update_event = NormalizedEvent::Update( NormalizedUpdate {
                        instrument: parsed_update.symbol.clone(),
                        data: book_update
                    });
                    let event_envelope = EventEnvelope {
                        exchange: Exchange::Binance,
                        event: update_event
                    };
                    
                    match self.symbols_pending_snapshot.entry(parsed_update.symbol.clone()) {
                        Entry::Occupied(mut e) => {
                            info!(exchange = ?Exchange::Binance, component = ?Component::Parser, symbol = ?parsed_update.symbol, "buffering symbol not live");
                            e.get_mut().push(event_envelope);
                        }
                        Entry::Vacant(_) => {
                            if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                                error!(exchange = ?Exchange::Binance, component = ?Component::Parser, error = ?e, "error while sending update event");
                            }
                        }
                    }
                }
            }
        }
    }
}