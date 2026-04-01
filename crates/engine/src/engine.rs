use std::collections::HashMap;
use md_core::{events::{BookEventType, ControlEvent, EventEnvelope, NormalizedEvent, NormalizedQuery}, logging::types::Component, types::Exchange};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;

use crate::exchange_state::{ExchangeState, ExchangeStateError};

pub struct Engine {
    exchanges: HashMap<Exchange, ExchangeState>,
    rx: Receiver<EventEnvelope>
}

impl Engine {
    pub fn new(rx: Receiver<EventEnvelope>, exchanges_controller: HashMap<Exchange, Sender<ControlEvent>>) -> Self  {
        let mut map = HashMap::new();
        for (exchange, control_tx) in exchanges_controller {
            map.insert(exchange, ExchangeState::new(exchange, control_tx));
        }
        Self {
            exchanges: map,
            rx
        }
    }
    pub fn run(&mut self) {
        while let Some(event_enveloped) = self.rx.blocking_recv() {
            let EventEnvelope { exchange, event} = event_enveloped;
            let Some(exchange_state) = self.exchanges.get_mut(&exchange) else {
                error!(exchange = ?exchange, component = ?Component::Engine, "exchange not found");
                continue;
            };

            match event {
                NormalizedEvent::ApplyStatus(data) => {
                    exchange_state.apply_status(data);
                },
                NormalizedEvent::GetStatus => {
                    exchange_state.get_status();
                },
                NormalizedEvent::Book(event_type, book_data) => {
                    match event_type {
                        BookEventType::Snapshot => {
                            if let Err(e) = exchange_state.apply_snapshot(book_data) {
                                error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "apply_snapshot error");
                                match e {
                                    ExchangeStateError::InstrumentNotFound(_) => {
                                        continue;
                                    },
                                    ExchangeStateError::ChecksumError { instrument: _, source: _ } => {
                                        exchange_state.resync();
                                    }
                                }
                            }
                        },
                        BookEventType::Update => {
                            if let Err(e) = exchange_state.apply_update(book_data) {
                                error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "apply_update error");
                                match e {
                                    ExchangeStateError::InstrumentNotFound(_) => {
                                        continue;
                                    },
                                    ExchangeStateError::ChecksumError { instrument: _, source: _ } => {
                                        exchange_state.resync();
                                    }
                                }
                            }
                        }
                    }
                },
                NormalizedEvent::Query(query) => {
                    match query {
                        NormalizedQuery::TopAsk(reply_to, data) => {
                            match exchange_state.top_n_ask(&data) {
                                Ok(vec) => {
                                    if let Err(e) = reply_to.send(vec) {
                                        error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "top_n_ask error")
                                    }
                                },
                                Err(e) => {
                                    error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "top_n_ask error");
                                    if let Err(e) = reply_to.send(Vec::new()) {
                                        error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "top_n_ask error")
                                    }
                                }
                            }
                        },
                        NormalizedQuery::TopBid(reply_to, data) => {
                            match exchange_state.top_n_bid(&data) {
                                Ok(vec) => {
                                    if let Err(e) = reply_to.send(vec) {
                                        error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "top_n_bid error")
                                    }
                                },
                                Err(e) => {
                                    error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "top_n_bid error");
                                    if let Err(e) = reply_to.send(Vec::new()) {
                                        error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "top_n_bid error")
                                    }
                                }
                            }
                        },
                        NormalizedQuery::GetStatus(reply_to) => {
                            let status = exchange_state.get_status();
                            if let Err(e) = reply_to.send(status) {
                                error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "get_status error")
                            }
                        }
                    }
                }
            }
        }
    }
}