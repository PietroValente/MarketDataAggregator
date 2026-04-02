use std::error::Error;

use tokio::sync::oneshot;

use crate::{book::BookLevel, types::{Exchange, ExchangeStatus, Instrument, Price, Qty}};

pub enum EngineQuery {
    ExchangeStatus {
        exchange: Exchange,
        reply_to: oneshot::Sender<Result<ExchangeStatus, Box<dyn Error + Send + Sync + 'static>>>,
    },
    Book {
        exchange: Exchange,
        instrument: Instrument,
        depth: usize,
        reply_to: oneshot::Sender<Result<BookView, Box<dyn Error + Send + Sync + 'static>>>
    },
    Best {
        instrument: Instrument,
        reply_to: oneshot::Sender<Vec<BestLevelPerExchange>>,
    },
    Spread {
        instrument: Instrument,
        reply_to: oneshot::Sender<Option<SpreadView>>,
    },
    Depth {
        instrument: Instrument,
        depth: usize,
        reply_to: oneshot::Sender<AggregatedDepthView>,
    },
    List {
        exchange: Option<Exchange>,
        reply_to: oneshot::Sender<Vec<Instrument>>,
    },
    Search {
        query: String,
        reply_to: oneshot::Sender<Vec<SearchResult>>,
    },
    AllStatuses {
        reply_to: oneshot::Sender<Vec<ExchangeStatusView>>,
    },
}

pub struct ExchangeStatusView {
    pub exchange: Exchange,
    pub status: ExchangeStatus,
    pub instruments: usize,
    pub live_instruments: usize,
}

pub struct BookView {
    pub exchange: Exchange,
    pub instrument: Instrument,
    pub asks: Vec<BookLevel>,
    pub bids: Vec<BookLevel>,
    pub spread: Option<Price>,
    pub mid: Option<Price>,
    pub status: ExchangeStatus,
}

pub struct BestLevelPerExchange {
    pub exchange: Exchange,
    pub instrument: Instrument,
    pub best_bid: Option<BookLevel>,
    pub best_ask: Option<BookLevel>,
    pub status: ExchangeStatus,
}

pub struct SpreadView {
    pub instrument: Instrument,
    pub best_bid_exchange: Exchange,
    pub best_bid: BookLevel,
    pub best_ask_exchange: Exchange,
    pub best_ask: BookLevel,
    pub absolute_spread: Price,
    pub relative_spread_bps: f64,
}

pub struct AggregatedDepthView {
    pub instrument: Instrument,
    pub asks: Vec<AggregatedDepthLevel>,
    pub bids: Vec<AggregatedDepthLevel>,
}

pub struct AggregatedDepthLevel {
    pub price: Price,
    pub total_qty: Qty,
}

pub struct SearchResult {
    pub instrument: Instrument,
    pub exchanges: Vec<Exchange>,
}
