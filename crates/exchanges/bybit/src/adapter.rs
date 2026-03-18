use md_core::adapter_trait::ExchangeAdapter;
use md_core::{book::BookLevels, events::{BookEventType, EventEnvelope, NormalizedBookData, NormalizedEvent}, logging::types::Component, types::{Exchange, Instrument}};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;

use crate::types::{BybitMdMsg, DepthBookAction, ParsedBookMessage, WsMessage};

pub struct BybitAdapter {
    raw_rx: Receiver<BybitMdMsg>,
    normalized_tx: Sender<EventEnvelope>
}

impl BybitAdapter {
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
                    error!(exchange = ?Exchange::Bybit, component = ?Component::Adapter, text = ?text, "error while parsing update");
                },
                Ok(WsMessage::Depth(depth)) => {
                    match depth.action {
                        DepthBookAction::Snapshot => {
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

    fn validate_snapshot(&mut self, _payload: &Self::SnapshotPayload) {
        todo!()
    }

    fn validate_update(&mut self, _payload: &Self::UpdatePayload) {
        todo!()
    }

    fn run(&mut self) {
        BybitAdapter::run(self)
    }
}