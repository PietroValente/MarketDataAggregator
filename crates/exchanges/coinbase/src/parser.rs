use md_core::{book::{BookSnapshot, BookUpdate}, events::{EventEnvelope, NormalizedEvent, NormalizedSnapshot, NormalizedUpdate}, logging::types::Component, types::{Exchange, Instrument}};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;

use crate::types::{CoinbaseMdMsg, Side, WsMessage};

pub struct CoinbaseParser {
    raw_rx: Receiver<CoinbaseMdMsg>,
    normalized_tx: Sender<EventEnvelope>
}

impl CoinbaseParser {
    pub fn new(raw_rx: Receiver<CoinbaseMdMsg>, normalized_tx: Sender<EventEnvelope>) -> Self {
        Self {
            raw_rx,
            normalized_tx
        }
    }

    pub fn run(&mut self) {
        while let Some(msg) = self.raw_rx.blocking_recv() {
            match serde_json::from_slice::<WsMessage>(&msg) {
                Ok(WsMessage::Confirmation(_)) => {
                    continue;
                },
                Err(_) => {
                    let text = String::from_utf8_lossy(&msg);
                    error!(exchange = ?Exchange::Coinbase, component = ?Component::Parser, text = ?text, "error while parsing update");
                },
                Ok(WsMessage::Snapshot(depth)) => {
                    let inst_id = depth.product_id.replace("-", "");
                    let snapshot_event = NormalizedEvent::Snapshot(NormalizedSnapshot {
                        instrument: Instrument::from(inst_id),
                        data: BookSnapshot {
                            last_update_id: 0,
                            asks: depth.asks,
                            bids: depth.bids
                        }
                    });
                    let event_envelope = EventEnvelope {
                        exchange: Exchange::Coinbase,
                        event: snapshot_event
                    };

                    if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                        error!(exchange = ?Exchange::Coinbase, component = ?Component::Parser, error = ?e, "error while sending snapshot event");
                    }
                },
                Ok(WsMessage::Update(depth)) => {
                    let inst_id = depth.product_id.replace("-", "");
                    let mut bids = Vec::new();
                    let mut asks = Vec::new();

                    for (side, price, qty) in depth.changes {
                        match side {
                            Side::Bid => bids.push((price, qty)),
                            Side::Ask => asks.push((price, qty)),
                        }
                    }

                    let snapshot_event = NormalizedEvent::Update(NormalizedUpdate {
                        instrument: Instrument::from(inst_id),
                        data: BookUpdate {
                            first_update_id: None,
                            last_update_id: 0,
                            asks,
                            bids
                        }
                    });
                    let event_envelope = EventEnvelope {
                        exchange: Exchange::Coinbase,
                        event: snapshot_event
                    };

                    if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                        error!(exchange = ?Exchange::Coinbase, component = ?Component::Parser, error = ?e, "error while sending snapshot event");
                    }
                }
            }
        }
    }
}