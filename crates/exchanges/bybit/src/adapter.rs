use std::error::Error;

use md_core::adapter_trait::ExchangeAdapter;
use md_core::events::ControlEvent;
use md_core::{book::BookLevels, events::{BookEventType, EventEnvelope, NormalizedBookData, NormalizedEvent}, logging::types::Component, types::{Exchange, Instrument}};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;

use crate::types::{BybitMdMsg, DepthBookAction, ParsedBookMessage, WsMessage};

pub struct BybitAdapter {
    raw_rx: Receiver<BybitMdMsg>,
    normalized_tx: Sender<EventEnvelope>,
    control_tx: Sender<ControlEvent>
}

impl BybitAdapter {
    pub fn new(raw_rx: Receiver<BybitMdMsg>, normalized_tx: Sender<EventEnvelope>, control_tx: Sender<ControlEvent>) -> Self {
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
                    error!(exchange = ?Exchange::Bybit, component = ?Component::Adapter, text = ?text, "error while parsing update");
                },
                Ok(WsMessage::Depth(depth)) => {
                    let action = depth.action.clone();

                    match action {
                        DepthBookAction::Snapshot => {
                            if let Err(e) = self.validate_snapshot(&depth) {
                                error!(exchange = ?Exchange::Bybit, component = ?Component::Adapter, error = ?e, "error while validating snapshot");
                                if let Err(e) = self.control_tx.blocking_send(ControlEvent::Resync) {
                                    error!(exchange = ?Exchange::Bybit, component = ?Component::Adapter, error = ?e, "error while sending resync");
                                }
                            }
                            let snapshot_event = NormalizedEvent::Book(BookEventType::Snapshot, NormalizedBookData {
                                instrument: Instrument::from(depth.data.symbol),
                                levels: BookLevels {
                                    asks: depth.data.asks,
                                    bids: depth.data.bids
                                }
                            });
                            let event_envelope = EventEnvelope {
                                exchange: Exchange::Bybit,
                                event: snapshot_event
                            };
        
                            if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                                error!(exchange = ?Exchange::Bybit, component = ?Component::Adapter, error = ?e, "error while sending snapshot event");
                            }
                        },
                        DepthBookAction::Delta => {
                            if let Err(e) = self.validate_update(&depth) {
                                error!(exchange = ?Exchange::Bybit, component = ?Component::Adapter, error = ?e, "error while validating snapshot");
                                if let Err(e) = self.control_tx.blocking_send(ControlEvent::Resync) {
                                    error!(exchange = ?Exchange::Bybit, component = ?Component::Adapter, error = ?e, "error while sending resync");
                                }
                            }
                            let snapshot_event = NormalizedEvent::Book(BookEventType::Update, NormalizedBookData {
                                instrument: Instrument::from(depth.data.symbol),
                                levels: BookLevels {
                                    asks: depth.data.asks,
                                    bids: depth.data.bids
                                }
                            });
                            let event_envelope = EventEnvelope {
                                exchange: Exchange::Bybit,
                                event: snapshot_event
                            };
        
                            if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                                error!(exchange = ?Exchange::Bybit, component = ?Component::Adapter, error = ?e, "error while sending snapshot event");
                            }
                        }
                    }
                }
            }
        }
    }
}

impl ExchangeAdapter for BybitAdapter {
    type SnapshotPayload = ParsedBookMessage;
    type UpdatePayload = ParsedBookMessage;

    fn exchange(&self) -> Exchange {
        Exchange::Bybit
    }

    fn validate_snapshot(&mut self, payload: &Self::SnapshotPayload) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        BybitAdapter::validate_snapshot(self, payload)
    }

    fn validate_update(&mut self, payload: &Self::UpdatePayload) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        BybitAdapter::validate_update(self, payload)
    }

    fn run(&mut self) {
        BybitAdapter::run(self)
    }
}