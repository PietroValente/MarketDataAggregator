use std::collections::HashMap;
use md_core::{events::{BookEventType, ControlEvent, EngineMessage, EventEnvelope, NormalizedEvent}, logging::types::Component, query::EngineQuery, types::Exchange};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;

use crate::exchange_state::{ExchangeState, ExchangeStateError};

pub struct Engine {
    exchanges: HashMap<Exchange, ExchangeState>,
    rx: Receiver<EngineMessage>
}

impl Engine {
    pub fn new(rx: Receiver<EngineMessage>, exchanges_controller: HashMap<Exchange, Sender<ControlEvent>>) -> Self  {
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
        while let Some(msg) = self.rx.blocking_recv() {
            match msg {
                EngineMessage::Apply(event_enveloped)  => {
                    let EventEnvelope { exchange, event} = event_enveloped;
                    let Some(exchange_state) = self.exchanges.get_mut(&exchange) else {
                        error!(exchange = ?exchange, component = ?Component::Engine, "exchange not found");
                        continue;
                    };
        
                    match event {
                        NormalizedEvent::ApplyStatus(data) => {
                            exchange_state.apply_status(data);
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
                        }
                    }
                },
                EngineMessage::Query(engine_query) => {
                    match engine_query {
                        EngineQuery::ExchangeStatus { exchange, reply_to } => {
                            let exchange_state = self.exchanges.get_mut(&exchange);

                            let result = match exchange_state {
                                Some(exchange_state) => {
                                    Ok(exchange_state.get_status())
                                },
                                None => {
                                    Err("exchange not found".into())
                                }
                            };

                            if reply_to.send(result).is_err() {
                                error!(exchange = ?exchange, component = ?Component::Engine, "error sending ExchangeStatus response")
                            }
                        },
                        EngineQuery::Book { exchange, instrument, depth, reply_to } => {
                            let exchange_state = self.exchanges.get_mut(&exchange);

                            let result = match exchange_state {
                                Some(exchange_state) => {
                                    exchange_state.book_view(instrument, depth).map_err(|e| Box::from(e))
                                },
                                None => {
                                    Err("exchange not found".into())
                                }
                            };

                            if reply_to.send(result).is_err() {
                                error!(exchange = ?exchange, component = ?Component::Engine, "error sending Book response")
                            }
                        },
                        EngineQuery::Best { instrument: _, reply_to: _ } => {

                        },
                        EngineQuery::Spread { instrument: _, reply_to: _ } => {

                        },
                        EngineQuery::Depth { instrument: _, depth: _, reply_to: _ } => {

                        },
                        EngineQuery::List { exchange: _, reply_to: _ } => {

                        },
                        EngineQuery::Search { query: _, reply_to: _ } => {

                        },
                        EngineQuery::AllStatuses { reply_to: _ } => {

                        }
                    }
                }
            }
        }
    }
}