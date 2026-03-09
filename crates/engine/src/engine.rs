use std::collections::HashMap;
use md_core::{events::{ControlEvent, EventEnvelope, NormalizedEvent, NormalizedQuery}, types::Exchange};
use tokio::sync::mpsc::{Sender, Receiver};
use tracing::error;

use crate::exchange_state::{ExchangeState, ExchangeStateError};

pub struct Engine {
    exchanges: HashMap<Exchange, ExchangeState>,
    rx: Receiver<EventEnvelope>
}

impl Engine {
    pub fn new(rx: Receiver<EventEnvelope>, control_senders: HashMap<Exchange, Sender<ControlEvent>>) -> Self  {
        let mut map = HashMap::new();
        for (exchange, sender) in control_senders {
            map.insert(exchange, ExchangeState::new(exchange, sender));
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
                error!(exchange = ?exchange, "exchange not found");
                continue;
            };

            match event {
                NormalizedEvent::Status(data) => {
                    exchange_state.apply_status(data);
                },
                NormalizedEvent::Snapshot(data) => { 
                    exchange_state.apply_snapshot(data)
                },
                NormalizedEvent::Update(data) => {
                    if let Err(e) = exchange_state.apply_update(data) {
                        match e {
                            ExchangeStateError::InstrumentNotFound(i) => {
                                error!(exchange = ?exchange, instrument = ?i, "Instrument not found while updating");
                            },
                            ExchangeStateError::LocalBookError(e) => {
                                error!(exchange = ?exchange, error = ?e, "Local book update failed, triggering reinitialization");
                                if let Err(_e) = exchange_state.send_control_event(ControlEvent::Resync){
                                    // TODO: manage channel error while sending resync
                                }
                            }
                        }
                    }
                },
                NormalizedEvent::Query(query) => {
                    match query {
                        NormalizedQuery::TopAsk(data) => {
                            match exchange_state.top_n_ask(&data) {
                                Ok(vec) => {
                                    if let Err(e) = data.reply_to.send(vec) {
                                        error!(exchange = ?exchange, error = ?e, "top_n_ask error")
                                    }
                                },
                                Err(e) => {
                                    error!(exchange = ?exchange, error = ?e, "top_n_ask error");
                                }
                            }
                        },
                        NormalizedQuery::TopBid(data) => {
                            match exchange_state.top_n_bid(&data) {
                                Ok(vec) => {
                                    if let Err(e) = data.reply_to.send(vec) {
                                        error!(exchange = ?exchange, error = ?e, "top_n_bid error")
                                    }
                                },
                                Err(e) => {
                                    error!(exchange = ?exchange, error = ?e, "top_n_bid error");
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}