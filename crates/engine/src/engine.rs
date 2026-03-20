use std::collections::HashMap;
use md_core::{events::{BookEventType, EventEnvelope, NormalizedEvent, NormalizedQuery}, logging::types::Component, types::Exchange};
use tokio::sync::mpsc::Receiver;
use tracing::error;

use crate::exchange_state::ExchangeState;

pub struct Engine {
    exchanges: HashMap<Exchange, ExchangeState>,
    rx: Receiver<EventEnvelope>
}

impl Engine {
    pub fn new(rx: Receiver<EventEnvelope>, exchanges: Vec<Exchange>) -> Self  {
        let mut map = HashMap::new();
        for exchange in exchanges {
            map.insert(exchange, ExchangeState::new(exchange));
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
                NormalizedEvent::Status(data) => {
                    exchange_state.apply_status(data);
                },
                NormalizedEvent::Book(event_type, book_data) => {
                    match event_type {
                        BookEventType::Snapshot => {
                            exchange_state.apply_snapshot(book_data);       
                        },
                        BookEventType::Update => {
                            if let Err(e) = exchange_state.apply_update(book_data) {
                                error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "Instrument not found while updating");
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
                                        error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "top_n_ask error")
                                    }
                                },
                                Err(e) => {
                                    error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "top_n_ask error");
                                    if let Err(e) = data.reply_to.send(Vec::new()) {
                                        error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "top_n_ask error")
                                    }
                                }
                            }
                        },
                        NormalizedQuery::TopBid(data) => {
                            match exchange_state.top_n_bid(&data) {
                                Ok(vec) => {
                                    if let Err(e) = data.reply_to.send(vec) {
                                        error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "top_n_bid error")
                                    }
                                },
                                Err(e) => {
                                    error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "top_n_bid error");
                                    if let Err(e) = data.reply_to.send(Vec::new()) {
                                        error!(exchange = ?exchange, component = ?Component::Engine, error = ?e, "top_n_bid error")
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

}