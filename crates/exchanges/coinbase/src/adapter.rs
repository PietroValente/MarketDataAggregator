use md_core::adapter_trait::ExchangeAdapter;
use md_core::{book::BookLevels, events::{BookEventType, EventEnvelope, NormalizedBookData, NormalizedEvent}, logging::types::Component, types::{Exchange, Instrument}};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;

use crate::types::{CoinbaseMdMsg, ParsedBookSnapshot, ParsedBookUpdate, Side, WsMessage};

pub struct CoinbaseAdapter {
    raw_rx: Receiver<CoinbaseMdMsg>,
    normalized_tx: Sender<EventEnvelope>
}

impl CoinbaseAdapter {
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
                    error!(exchange = ?Exchange::Coinbase, component = ?Component::Adapter, text = ?text, "error while parsing update");
                },
                Ok(WsMessage::Snapshot(depth)) => {
                    let inst_id = depth.product_id.replace("-", "");
                    let snapshot_event = NormalizedEvent::Book(BookEventType::Snapshot, NormalizedBookData {
                        instrument: Instrument::from(inst_id),
                        levels: BookLevels {
                            asks: depth.asks,
                            bids: depth.bids
                        }
                    });
                    let event_envelope = EventEnvelope {
                        exchange: Exchange::Coinbase,
                        event: snapshot_event
                    };

                    if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                        error!(exchange = ?Exchange::Coinbase, component = ?Component::Adapter, error = ?e, "error while sending snapshot event");
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

                    let snapshot_event = NormalizedEvent::Book(BookEventType::Update, NormalizedBookData {
                        instrument: Instrument::from(inst_id),
                        levels: BookLevels {
                            asks,
                            bids
                        }
                    });
                    let event_envelope = EventEnvelope {
                        exchange: Exchange::Coinbase,
                        event: snapshot_event
                    };

                    if let Err(e) = self.normalized_tx.blocking_send(event_envelope) {
                        error!(exchange = ?Exchange::Coinbase, component = ?Component::Adapter, error = ?e, "error while sending snapshot event");
                    }
                }
            }
        }
    }
}

impl ExchangeAdapter for CoinbaseAdapter {
    type SnapshotPayload = ParsedBookSnapshot;
    type UpdatePayload = ParsedBookUpdate;

    fn exchange(&self) -> Exchange {
        Exchange::Coinbase
    }

    fn validate_snapshot(&mut self, _payload: &Self::SnapshotPayload) {
        todo!()
    }

    fn validate_update(&mut self, _payload: &Self::UpdatePayload) {
        todo!()
    }

    fn run(&mut self) {
        CoinbaseAdapter::run(self)
    }
}