use md_core::{book::{BookSnapshot, BookUpdate}, events::{EventEnvelope, NormalizedEvent, NormalizedSnapshot, NormalizedUpdate}, logging::types::Component, types::{Exchange, Instrument}};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;

use crate::types::{BybitMdMsg, DepthBookAction, WsMessage};

pub struct BybitParser {
    raw_rx: Receiver<BybitMdMsg>,
    normalized_tx: Sender<EventEnvelope>
}

impl BybitParser {
    pub fn new(raw_rx: Receiver<BybitMdMsg>, normalized_tx: Sender<EventEnvelope>) -> Self {
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
                    error!(exchange = ?Exchange::Bybit, component = ?Component::Parser, text = ?text, "error while parsing update");
                },
                Ok(WsMessage::Depth(depth)) => {
                    match depth.action {
                        DepthBookAction::Snapshot => {
                            let snapshot_event = NormalizedEvent::Snapshot(NormalizedSnapshot {
                                instrument: Instrument::from(depth.data.symbol),
                                data: BookSnapshot {
                                    last_update_id: depth.data.update_id,
                                    asks: depth.data.asks,
                                    bids: depth.data.bids
                                }
                            });
                            let event_envelope = EventEnvelope {
                                exchange: Exchange::Bybit,
                                event: snapshot_event
                            };
        
                            if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                                error!(exchange = ?Exchange::Bybit, component = ?Component::Parser, error = ?e, "error while sending snapshot event");
                            }
                        },
                        DepthBookAction::Delta => {
                            let snapshot_event = NormalizedEvent::Update(NormalizedUpdate {
                                instrument: Instrument::from(depth.data.symbol),
                                data: BookUpdate {
                                    first_update_id: None,
                                    last_update_id: depth.data.update_id,
                                    asks: depth.data.asks,
                                    bids: depth.data.bids
                                }
                            });
                            let event_envelope = EventEnvelope {
                                exchange: Exchange::Bybit,
                                event: snapshot_event
                            };
        
                            if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                                error!(exchange = ?Exchange::Bybit, component = ?Component::Parser, error = ?e, "error while sending snapshot event");
                            }
                        }
                    }
                }
            }
        }
    }
}