use md_core::{book::BookLevels, events::{BookEventType, EventEnvelope, NormalizedBookData, NormalizedEvent}, logging::types::Component, types::{Exchange, Instrument}};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;

use crate::types::{OkxMdMsg, DepthBookAction, WsMessage};

pub struct OkxAdapter {
    raw_rx: Receiver<OkxMdMsg>,
    normalized_tx: Sender<EventEnvelope>
}

impl OkxAdapter {
    pub fn new(raw_rx: Receiver<OkxMdMsg>, normalized_tx: Sender<EventEnvelope>) -> Self {
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
                    error!(exchange = ?Exchange::Okx, component = ?Component::Adapter, text = ?text, "error while parsing update");
                },
                Ok(WsMessage::Depth(depth)) => {
                    let inst_id = depth.arg.inst_id.replace("-", "");

                    match depth.action {
                        DepthBookAction::Snapshot => {
                            for data in depth.data {
                                let snapshot_event = NormalizedEvent::Book(BookEventType::Snapshot, NormalizedBookData {
                                    instrument: Instrument::from(inst_id.clone()),
                                    levels: BookLevels {
                                        asks: data.asks,
                                        bids: data.bids
                                    }
                                });
                                let event_envelope = EventEnvelope {
                                    exchange: Exchange::Okx,
                                    event: snapshot_event
                                };
            
                                if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                                    error!(exchange = ?Exchange::Okx, component = ?Component::Adapter, error = ?e, "error while sending snapshot event");
                                }
                            }
                        },
                        DepthBookAction::Update => {
                            for data in depth.data {
                                let snapshot_event = NormalizedEvent::Book(BookEventType::Update, NormalizedBookData {
                                    instrument: Instrument::from(inst_id.clone()),
                                    levels: BookLevels {
                                        asks: data.asks,
                                        bids: data.bids
                                    }
                                });
                                let event_envelope = EventEnvelope {
                                    exchange: Exchange::Okx,
                                    event: snapshot_event
                                };
            
                                if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                                    error!(exchange = ?Exchange::Okx, component = ?Component::Adapter, error = ?e, "error while sending snapshot event");
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}