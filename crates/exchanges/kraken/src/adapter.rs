use std::error::Error;

use md_core::adapter_trait::ExchangeAdapter;
use md_core::events::ControlEvent;
use md_core::{book::BookLevels, events::{BookEventType, EventEnvelope, NormalizedBookData, NormalizedEvent}, logging::types::Component, types::{Exchange, Instrument}};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;

use crate::types::{DepthBookAction, KrakenMdMsg, ParsedBookMessage, WsMessage};

pub struct KrakenAdapter {
    raw_rx: Receiver<KrakenMdMsg>,
    normalized_tx: Sender<EventEnvelope>,
    control_tx: Sender<ControlEvent>
}

impl KrakenAdapter {
    pub fn new(raw_rx: Receiver<KrakenMdMsg>, normalized_tx: Sender<EventEnvelope>, control_tx: Sender<ControlEvent>) -> Self {
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
                Ok(WsMessage::Confirmation(msg)) => {
                    if !msg.success {
                        error!(exchange = ?Exchange::Kraken, component = ?Component::Adapter, msg = ?msg, "subscription not confirmed");
                    }
                    continue;
                },
                Err(_) => {
                    let text = String::from_utf8_lossy(&msg);
                    error!(exchange = ?Exchange::Kraken, component = ?Component::Adapter, text = ?text, "error while parsing update");
                },
                Ok(WsMessage::Depth(depth)) => {
                    let op_type = depth.op_type.clone();

                    match op_type {
                        DepthBookAction::Snapshot => {
                            if let Err(e) = self.validate_snapshot(&depth) {
                                error!(exchange = ?Exchange::Kraken, component = ?Component::Adapter, error = ?e, "error while validating snapshot");
                                if let Err(e) = self.control_tx.blocking_send(ControlEvent::Resync) {
                                    error!(exchange = ?Exchange::Kraken, component = ?Component::Adapter, error = ?e, "error while sending resync");
                                }
                            }
                            for data in depth.data {
                                let symbol = data.symbol.replace("/", "");
                                let snapshot_event = NormalizedEvent::Book(BookEventType::Snapshot, NormalizedBookData {
                                    instrument: Instrument::from(symbol),
                                    levels: BookLevels {
                                        asks: data.asks,
                                        bids: data.bids
                                    }
                                });
                                let event_envelope = EventEnvelope {
                                    exchange: Exchange::Kraken,
                                    event: snapshot_event
                                };
            
                                if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                                    error!(exchange = ?Exchange::Kraken, component = ?Component::Adapter, error = ?e, "error while sending snapshot event");
                                }
                            }
                        },
                        DepthBookAction::Update => {
                            if let Err(e) = self.validate_update(&depth) {
                                error!(exchange = ?Exchange::Kraken, component = ?Component::Adapter, error = ?e, "error while validating snapshot");
                                if let Err(e) = self.control_tx.blocking_send(ControlEvent::Resync) {
                                    error!(exchange = ?Exchange::Kraken, component = ?Component::Adapter, error = ?e, "error while sending resync");
                                }
                            }
                            for data in depth.data {
                                let symbol = data.symbol.replace("/", "");
                                let snapshot_event = NormalizedEvent::Book(BookEventType::Update, NormalizedBookData {
                                    instrument: Instrument::from(symbol),
                                    levels: BookLevels {
                                        asks: data.asks,
                                        bids: data.bids
                                    }
                                });
                                let event_envelope = EventEnvelope {
                                    exchange: Exchange::Kraken,
                                    event: snapshot_event
                                };
            
                                if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                                    error!(exchange = ?Exchange::Kraken, component = ?Component::Adapter, error = ?e, "error while sending snapshot event");
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

impl ExchangeAdapter for KrakenAdapter {
    type SnapshotPayload = ParsedBookMessage;
    type UpdatePayload = ParsedBookMessage;

    fn exchange(&self) -> Exchange {
        Exchange::Kraken
    }

    fn validate_snapshot(&mut self, payload: &Self::SnapshotPayload) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        KrakenAdapter::validate_snapshot(self, payload)
    }

    fn validate_update(&mut self, payload: &Self::UpdatePayload) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        KrakenAdapter::validate_update(self, payload)
    }

    fn run(&mut self) {
        KrakenAdapter::run(self)
    }
}