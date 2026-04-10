use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet},
    error::Error,
};

use md_core::{
    book::BookLevel,
    logging::types::Component,
    query::{
        AggregatedDepthView, BestLevelPerExchange, BookView, EngineQuery, ExchangeStatusView,
        SpreadView,
    },
    types::{Exchange, ExchangeStatus, Instrument, Price, Qty},
};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use tracing::warn;

use crate::engine::Engine;

type EngineError = Box<dyn Error + Send + Sync + 'static>;

impl Engine {
    pub fn handle_query(&mut self, engine_query: EngineQuery) {
        match engine_query {
            EngineQuery::ExchangeStatus { exchange, reply_to } => {
                let result = self.query_exchange_status(exchange);

                if reply_to.send(result).is_err() {
                    warn!(
                        exchange = ?exchange,
                        component = ?Component::Engine,
                        query_type = "exchange_status",
                        "query requester dropped before response send"
                    );
                }
            }

            EngineQuery::Book {
                exchange,
                instrument,
                depth,
                reply_to,
            } => {
                let result = self.query_book(exchange, instrument, depth);

                if reply_to.send(result).is_err() {
                    warn!(
                        exchange = ?exchange,
                        component = ?Component::Engine,
                        query_type = "book",
                        "query requester dropped before response send"
                    );
                }
            }

            EngineQuery::Best {
                instrument,
                reply_to,
            } => {
                let result = self.query_best(&instrument);

                if reply_to.send(result).is_err() {
                    warn!(component = ?Component::Engine, query_type = "best", "query requester dropped before response send");
                }
            }

            EngineQuery::Spread {
                instrument,
                reply_to,
            } => {
                let result = self.query_spread(&instrument);

                if reply_to.send(result).is_err() {
                    warn!(component = ?Component::Engine, query_type = "spread", "query requester dropped before response send");
                }
            }

            EngineQuery::Depth {
                instrument,
                depth,
                reply_to,
            } => {
                let result = self.query_depth(&instrument, depth);

                if reply_to.send(result).is_err() {
                    warn!(component = ?Component::Engine, query_type = "depth", "query requester dropped before response send");
                }
            }

            EngineQuery::List { exchange, reply_to } => {
                let result = self.query_list(exchange);

                if reply_to.send(result).is_err() {
                    warn!(
                        exchange = ?exchange,
                        component = ?Component::Engine,
                        query_type = "list",
                        "query requester dropped before response send"
                    );
                }
            }

            EngineQuery::Search { query, reply_to } => {
                let result = self.query_search(&query);

                if reply_to.send(result).is_err() {
                    warn!(
                        component = ?Component::Engine,
                        query_type = "search",
                        "query requester dropped before response send"
                    );
                }
            }
            EngineQuery::SearchContains {
                query,
                limit,
                reply_to,
            } => {
                let result = self.query_search_contains(&query, limit);

                if reply_to.send(result).is_err() {
                    warn!(
                        component = ?Component::Engine,
                        query_type = "search_contains",
                        "query requester dropped before response send"
                    );
                }
            }
            EngineQuery::SearchSuffix {
                query,
                limit,
                reply_to,
            } => {
                let result = self.query_search_suffix(&query, limit);

                if reply_to.send(result).is_err() {
                    warn!(
                        component = ?Component::Engine,
                        query_type = "search_suffix",
                        "query requester dropped before response send"
                    );
                }
            }
            EngineQuery::SearchGlob {
                query,
                limit,
                reply_to,
            } => {
                let result = self.query_search_glob(&query, limit);

                if reply_to.send(result).is_err() {
                    warn!(
                        component = ?Component::Engine,
                        query_type = "search_glob",
                        "query requester dropped before response send"
                    );
                }
            }

            EngineQuery::AllStatuses { reply_to } => {
                let result = self.query_all_statuses();

                if reply_to.send(result).is_err() {
                    warn!(
                        component = ?Component::Engine,
                        query_type = "all_statuses",
                        "query requester dropped before response send"
                    );
                }
            }
        }
    }

    fn query_exchange_status(&self, exchange: Exchange) -> Result<ExchangeStatus, EngineError> {
        match self.exchanges.get(&exchange) {
            Some(exchange_state) => Ok(exchange_state.get_status()),
            None => Err("exchange not found".into()),
        }
    }

    fn query_book(
        &self,
        exchange: Exchange,
        instrument: Instrument,
        depth: usize,
    ) -> Result<BookView, EngineError> {
        match self.exchanges.get(&exchange) {
            Some(exchange_state) => exchange_state
                .book_view(instrument, depth)
                .map_err(Box::from),
            None => Err("exchange not found".into()),
        }
    }

    fn query_best(&self, instrument: &Instrument) -> Vec<BestLevelPerExchange> {
        let mut result = Vec::new();

        for (&exchange, exchange_state) in &self.exchanges {
            let best_bid = exchange_state
                .top_n_bids(instrument, 1)
                .ok()
                .and_then(|bids| bids.into_iter().next());

            let best_ask = exchange_state
                .top_n_asks(instrument, 1)
                .ok()
                .and_then(|asks| asks.into_iter().next());

            result.push(BestLevelPerExchange {
                exchange,
                instrument: instrument.clone(),
                best_bid,
                best_ask,
                status: exchange_state.get_status(),
            });
        }

        result
    }

    fn query_spread(&self, instrument: &Instrument) -> Option<SpreadView> {
        let mut best_bid: Option<(Exchange, BookLevel)> = None;
        let mut best_ask: Option<(Exchange, BookLevel)> = None;

        for (&exchange, exchange_state) in &self.exchanges {
            if exchange_state.get_status() != ExchangeStatus::Live {
                continue;
            }

            let exchange_best_bid = exchange_state
                .top_n_bids(instrument, 1)
                .ok()
                .and_then(|bids| bids.into_iter().next());

            let exchange_best_ask = exchange_state
                .top_n_asks(instrument, 1)
                .ok()
                .and_then(|asks| asks.into_iter().next());

            if let Some(bid) = exchange_best_bid {
                match best_bid {
                    Some((_, current)) if current >= bid => {}
                    _ => best_bid = Some((exchange, bid)),
                }
            }

            if let Some(ask) = exchange_best_ask {
                match best_ask {
                    Some((_, current)) if current <= ask => {}
                    _ => best_ask = Some((exchange, ask)),
                }
            }
        }

        let result = match (best_bid, best_ask) {
            (Some((best_bid_exchange, best_bid)), Some((best_ask_exchange, best_ask))) => {
                let absolute_spread = *best_ask.px() - *best_bid.px();
                let mid = (*best_ask.px() + *best_bid.px()) / Price(dec!(2));

                let relative_bps = *(absolute_spread / mid) * dec!(10_000);
                let relative_spread_bps = relative_bps.to_f64()?;

                Some(SpreadView {
                    instrument: instrument.clone(),
                    best_bid_exchange,
                    best_bid,
                    best_ask_exchange,
                    best_ask,
                    absolute_spread,
                    relative_spread_bps,
                })
            }
            _ => None,
        };

        result
    }

    fn query_depth(&self, instrument: &Instrument, depth: usize) -> AggregatedDepthView {
        let per_exchange_depth = depth;

        let asks: BTreeSet<BookLevel> = self
            .exchanges
            .values()
            .filter(|exchange_state| exchange_state.get_status() == ExchangeStatus::Live)
            .filter_map(|exchange_state| {
                exchange_state
                    .top_n_asks(instrument, per_exchange_depth)
                    .ok()
            })
            .flatten()
            .fold(BTreeMap::<Price, Qty>::new(), |mut acc, level| {
                acc.entry(*level.px())
                    .and_modify(|qty| *qty = *qty + *level.qty())
                    .or_insert(*level.qty());
                acc
            })
            .into_iter()
            .take(depth)
            .map(|(price, total_qty)| BookLevel::new(total_qty, price))
            .collect();

        let bids: BTreeSet<BookLevel> = self
            .exchanges
            .values()
            .filter(|exchange_state| exchange_state.get_status() == ExchangeStatus::Live)
            .filter_map(|exchange_state| {
                exchange_state
                    .top_n_bids(instrument, per_exchange_depth)
                    .ok()
            })
            .flatten()
            .fold(BTreeMap::<Reverse<Price>, Qty>::new(), |mut acc, level| {
                acc.entry(Reverse(*level.px()))
                    .and_modify(|qty| *qty = *qty + *level.qty())
                    .or_insert(*level.qty());
                acc
            })
            .into_iter()
            .take(depth)
            .map(|(reverse_px, total_qty)| BookLevel::new(total_qty, reverse_px.0))
            .collect();

        let best_ask = asks.iter().next();
        let best_bid = bids.iter().next_back();
        let spread = match (best_ask, best_bid) {
            (Some(ask), Some(bid)) => Some(*ask.px() - *bid.px()),
            _ => None,
        };
        let mid = match (best_ask, best_bid) {
            (Some(ask), Some(bid)) => Some((*ask.px() + *bid.px()) / Price(dec!(2))),
            _ => None,
        };

        AggregatedDepthView {
            instrument: instrument.clone(),
            asks,
            bids,
            spread,
            mid,
        }
    }

    fn query_list(&self, exchange: Option<Exchange>) -> Result<BTreeSet<Instrument>, EngineError> {
        match exchange {
            Some(exchange) => match self.exchanges.get(&exchange) {
                Some(exchange_state) => Ok(exchange_state.instruments()),
                None => Err("exchange not found".into()),
            },
            None => Ok(self
                .exchanges
                .values()
                .flat_map(|ex| ex.instruments_iter())
                .cloned()
                .collect()),
        }
    }

    fn query_search(&self, query: &String) -> BTreeMap<Instrument, Vec<Exchange>> {
        let mut result: BTreeMap<Instrument, Vec<Exchange>> = BTreeMap::new();

        for (&exchange, exchange_state) in &self.exchanges {
            for instrument in exchange_state.instruments_iter() {
                if instrument.starts_with(query) {
                    result.entry(instrument.clone()).or_default().push(exchange);
                }
            }
        }

        result
    }

    fn query_search_contains(
        &self,
        query: &str,
        limit: usize,
    ) -> BTreeMap<Instrument, Vec<Exchange>> {
        self.query_search_with_limit(limit, |instrument| instrument.contains(query))
    }

    fn query_search_suffix(
        &self,
        query: &str,
        limit: usize,
    ) -> BTreeMap<Instrument, Vec<Exchange>> {
        self.query_search_with_limit(limit, |instrument| instrument.ends_with(query))
    }

    fn query_search_glob(&self, query: &str, limit: usize) -> BTreeMap<Instrument, Vec<Exchange>> {
        self.query_search_with_limit(limit, |instrument| Self::glob_match_star(instrument, query))
    }

    fn query_search_with_limit<F>(
        &self,
        limit: usize,
        mut predicate: F,
    ) -> BTreeMap<Instrument, Vec<Exchange>>
    where
        F: FnMut(&str) -> bool,
    {
        let mut result: BTreeMap<Instrument, Vec<Exchange>> = BTreeMap::new();
        if limit == 0 {
            return result;
        }

        for (&exchange, exchange_state) in &self.exchanges {
            for instrument in exchange_state.instruments_iter() {
                if !predicate(instrument) {
                    continue;
                }

                let is_new = !result.contains_key(instrument);
                if is_new && result.len() >= limit {
                    continue;
                }

                result.entry(instrument.clone()).or_default().push(exchange);
            }
        }

        result
    }

    fn glob_match_star(text: &str, pattern: &str) -> bool {
        if pattern == "*" {
            return true;
        }

        let starts_star = pattern.starts_with('*');
        let ends_star = pattern.ends_with('*');
        let parts: Vec<&str> = pattern.split('*').filter(|p| !p.is_empty()).collect();
        if parts.is_empty() {
            return true;
        }

        let mut idx = 0usize;
        if !starts_star {
            if !text.starts_with(parts[0]) {
                return false;
            }
            idx = parts[0].len();
        }

        let last = parts.len() - 1;
        for (pi, part) in parts.iter().enumerate() {
            if !starts_star && pi == 0 {
                continue;
            }
            if !ends_star && pi == last {
                break;
            }
            if let Some(pos) = text[idx..].find(part) {
                idx += pos + part.len();
            } else {
                return false;
            }
        }

        if !ends_star {
            text.ends_with(parts[last])
        } else {
            true
        }
    }

    fn query_all_statuses(&self) -> Vec<ExchangeStatusView> {
        let mut result = Vec::new();

        for (&exchange, exchange_state) in &self.exchanges {
            result.push(ExchangeStatusView {
                exchange,
                status: exchange_state.get_status(),
                instruments: exchange_state.instruments_len(),
            });
        }

        result
    }
}
