use std::error::Error;

use md_core::adapter_trait::ExchangeAdapter;
use md_core::events::ControlEvent;
use md_core::{book::BookLevels, events::{BookEventType, EventEnvelope, NormalizedBookData, NormalizedEvent}, logging::types::Component, types::{Exchange, Instrument}};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;

use crate::types::{DepthBookAction, OkxMdMsg, ParsedBookMessage, WsMessage};

pub struct OkxAdapter {
    raw_rx: Receiver<OkxMdMsg>,
    normalized_tx: Sender<EventEnvelope>,
    control_tx: Sender<ControlEvent>
}

impl OkxAdapter {
    pub fn new(raw_rx: Receiver<OkxMdMsg>, normalized_tx: Sender<EventEnvelope>, control_tx: Sender<ControlEvent>) -> Self {
        Self {
            raw_rx,
            normalized_tx,
            control_tx
        }
    }

    fn validate_snapshot(&mut self, _payload: &ParsedBookMessage) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        Ok(())
    }

    fn validate_update(&mut self, _payload: &ParsedBookMessage) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        Ok(())
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
                    let action = depth.action.clone();

                    match action {
                        DepthBookAction::Snapshot => {
                            if let Err(e) = self.validate_snapshot(&depth) {
                                error!(exchange = ?Exchange::Okx, component = ?Component::Adapter, error = ?e, "error while validating snapshot");
                                if let Err(e) = self.control_tx.blocking_send(ControlEvent::Resync) {
                                    error!(exchange = ?Exchange::Okx, component = ?Component::Adapter, error = ?e, "error while sending resync");
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
                                    exchange: Exchange::Okx,
                                    event: snapshot_event
                                };
            
                                if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                                    error!(exchange = ?Exchange::Okx, component = ?Component::Adapter, error = ?e, "error while sending snapshot event");
                                }
                            }
                        },
                        DepthBookAction::Update => {
                            if let Err(e) = self.validate_update(&depth) {
                                error!(exchange = ?Exchange::Okx, component = ?Component::Adapter, error = ?e, "error while validating snapshot");
                                if let Err(e) = self.control_tx.blocking_send(ControlEvent::Resync) {
                                    error!(exchange = ?Exchange::Okx, component = ?Component::Adapter, error = ?e, "error while sending resync");
                                }
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

impl ExchangeAdapter for OkxAdapter {
    type SnapshotPayload = ParsedBookMessage;
    type UpdatePayload = ParsedBookMessage;

    fn exchange(&self) -> Exchange {
        Exchange::Okx
    }

    fn validate_snapshot(&mut self, payload: &Self::SnapshotPayload) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        OkxAdapter::validate_snapshot(self, payload)
    }

    fn validate_update(&mut self, payload: &Self::UpdatePayload) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        OkxAdapter::validate_update(self, payload)
    }

    fn run(&mut self) {
        OkxAdapter::run(self)
    }
}