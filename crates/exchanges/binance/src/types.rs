use std::str::FromStr;

use md_core::types::{Instrument, Price, Qty, RawMdMsg};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

/* Connector commands and transport messages */

pub enum BinanceMdMsg {
    Instruments(Vec<Instrument>),
    Snapshot(SnapshotMsg),
    WsMessage(RawMdMsg),
    ResetBookState,
}

pub struct SnapshotMsg {
    pub symbol: Instrument,
    pub payload: RawMdMsg,
}

/* Connector/API configuration and subscription payloads */

pub struct BinanceUrls {
    pub exchange_info: Url,
    pub snapshot: Url,
    pub ws: Url,
}

#[derive(Deserialize)]
pub struct ApiResponse {
    pub symbols: Vec<SymbolInfo>,
}

#[derive(Deserialize)]
pub struct SymbolInfo {
    pub symbol: Instrument,
    pub status: String,
}

#[derive(Clone)]
pub struct Subscriptions {
    pub symbols: Vec<Instrument>,
    pub batches: Vec<SubscriptionBatch>,
}

#[derive(Clone)]
pub struct SubscriptionBatch {
    pub symbols: Vec<Instrument>,
    pub message: Message,
}

#[derive(Debug, Serialize)]
pub struct SubscriptionRequest {
    pub method: String,
    pub params: Vec<String>,
    pub id: i64,
}

#[derive(Debug, Serialize)]
pub struct DepthQuery<'a> {
    pub symbol: &'a str,
    pub limit: u32,
}

/* Book state and sync flow */

#[derive(Debug, PartialEq, Default)]
pub enum BookSyncStatus {
    #[default]
    WaitingSnapshot,
    Live,
}

#[derive(Default)]
pub struct BookState {
    pub status: BookSyncStatus,
    pub last_applied_update_id: Option<u64>,
    pub symbols_pending_snapshot: Vec<RawMdMsg>,
}

impl BookState {
    pub fn new() -> Self {
        Self {
            status: BookSyncStatus::WaitingSnapshot,
            last_applied_update_id: None,
            symbols_pending_snapshot: Vec::new(),
        }
    }
}

pub struct ValidateSnapshot {
    pub symbol: Instrument,
    pub last_update_id: u64,
}

/* Incoming websocket payloads */

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum WsMessage {
    Confirmation(SubscriptionConfirmation),
    Update(ParsedBookUpdate),
}

#[derive(Debug, Deserialize)]
pub struct SubscriptionConfirmation {
    pub result: Option<serde_json::Value>,
    pub id: u64,
}

#[derive(Debug, Deserialize)]
pub struct ParsedBookUpdate {
    #[serde(rename = "e")]
    pub event_type: String,

    #[serde(rename = "E")]
    pub event_time: u64,

    #[serde(rename = "s")]
    pub symbol: Instrument,

    #[serde(rename = "U")]
    pub first_update_id: u64,

    #[serde(rename = "u")]
    pub final_update_id: u64,

    #[serde(rename = "b", deserialize_with = "deserialize_levels")]
    pub bids: Vec<(Price, Qty)>,

    #[serde(rename = "a", deserialize_with = "deserialize_levels")]
    pub asks: Vec<(Price, Qty)>,
}

#[derive(Debug, Deserialize)]
pub struct ParsedBookSnapshot {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,

    #[serde(deserialize_with = "deserialize_levels")]
    pub bids: Vec<(Price, Qty)>,

    #[serde(deserialize_with = "deserialize_levels")]
    pub asks: Vec<(Price, Qty)>,
}

/* Error types */

#[derive(Error, Debug)]
pub enum ValidateBookError {
    #[error("Instrument not found: {0}")]
    InstrumentNotFound(Instrument),

    #[error("Unknown type of event: {0}")]
    UnknownType(String),

    #[error(
        "Stale update: event last_update_id={event_last_update_id} <= book last_applied_update_id={book_last_update_id}"
    )]
    StaleUpdate {
        event_last_update_id: u64,
        book_last_update_id: u64,
    },

    #[error(
        "Update gap detected: event first_update_id={event_first_update_id} > expected={expected_next_update_id}"
    )]
    UpdateGap {
        event_first_update_id: u64,
        expected_next_update_id: u64,
    },

    #[error(
        "Cannot apply update: last_applied_update_id is None (book not initialized with snapshot)"
    )]
    MissingSnapshot,
}

/* Shared deserializer helpers */

fn deserialize_levels<'de, D>(deserializer: D) -> Result<Vec<(Price, Qty)>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw: Vec<(String, String)> = Vec::deserialize(deserializer)?;

    raw.into_iter()
        .map(|(p, q)| {
            let price = Decimal::from_str(&p).map_err(serde::de::Error::custom)?;
            let qty = Decimal::from_str(&q).map_err(serde::de::Error::custom)?;

            Ok((Price(price), Qty(qty)))
        })
        .collect()
}
