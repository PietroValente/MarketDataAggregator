use md_core::{book::{BookSnapshot, BookUpdate}, events::{EventEnvelope, NormalizedEvent, NormalizedSnapshot, NormalizedUpdate}, logging::types::Component, types::{Exchange, Instrument}};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;

use crate::types::{BitgetMdMsg, DepthBookAction, WsMessage};

pub struct BitgetParser {
    raw_rx: Receiver<BitgetMdMsg>,
    normalized_tx: Sender<EventEnvelope>
}

impl BitgetParser {
    pub fn new(raw_rx: Receiver<BitgetMdMsg>, normalized_tx: Sender<EventEnvelope>) -> Self {
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
                    error!(exchange = ?Exchange::Bitget, component = ?Component::Parser, text = ?text, "error while parsing update");
                },
                Ok(WsMessage::Depth(depth)) => {
                    let inst_id = depth.arg.inst_id;

                    match depth.action {
                        DepthBookAction::Snapshot => {
                            for data in depth.data {
                                let snapshot_event = NormalizedEvent::Snapshot(NormalizedSnapshot {
                                    instrument: Instrument::from(inst_id.clone()),
                                    data: BookSnapshot {
                                        last_update_id: data.seq,
                                        asks: data.asks,
                                        bids: data.bids
                                    }
                                });
                                let event_envelope = EventEnvelope {
                                    exchange: Exchange::Bitget,
                                    event: snapshot_event
                                };
            
                                if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                                    error!(exchange = ?Exchange::Bitget, component = ?Component::Parser, error = ?e, "error while sending snapshot event");
                                }
                            }
                        },
                        DepthBookAction::Update => {
                            for data in depth.data {
                                let snapshot_event = NormalizedEvent::Update(NormalizedUpdate {
                                    instrument: Instrument::from(inst_id.clone()),
                                    data: BookUpdate {
                                        first_update_id: None,
                                        last_update_id: data.seq,
                                        asks: data.asks,
                                        bids: data.bids
                                    }
                                });
                                let event_envelope = EventEnvelope {
                                    exchange: Exchange::Bitget,
                                    event: snapshot_event
                                };
            
                                if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                                    error!(exchange = ?Exchange::Bitget, component = ?Component::Parser, error = ?e, "error while sending snapshot event");
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}