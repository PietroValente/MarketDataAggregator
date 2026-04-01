use std::str::FromStr;

use md_core::types::{Instrument, Price, Qty, RawMdMsg};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

/* Connector commands and transport messages */

pub enum OkxMdMsg {
    Instruments(Vec<Instrument>),
    Raw(RawMdMsg),
    ClearBookState
}

/* Connector/API configuration and subscription payloads */

pub struct OkxUrls {
    pub exchange_info: Url,
    pub ws: Url,
}

#[derive(Clone)]
pub struct Subscriptions {
    pub symbols: Vec<Instrument>,
    pub messages: Vec<Message>,
}

#[derive(Deserialize)]
pub struct ApiResponse {
    pub data: Vec<SymbolInfo>,
}

#[derive(Deserialize)]
pub struct SymbolInfo {
    #[serde(rename = "instId")]
    pub inst_id: Instrument,
    pub state: String,
}

#[derive(Debug, Serialize)]
pub struct SubscriptionRequest {
    pub id: String,
    pub op: String,
    pub args: Vec<SymbolParam>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SymbolParam {
    pub channel: String,
    #[serde(rename = "instId")]
    pub inst_id: Instrument,
}

/* Book state and sync flow */

pub struct BookState {
    pub initialized: bool,
    pub prev_seq_id: Option<u64>,
}

impl BookState {
    pub fn new() -> Self {
        Self {
            initialized: false,
            prev_seq_id: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DepthBookAction {
    Snapshot,
    Update,
}

/* Incoming websocket payloads */

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum WsMessage {
    Confirmation(SubscriptionConfirmation),
    Depth(ParsedBookMessage),
}

#[derive(Debug, Deserialize)]
pub struct SubscriptionConfirmation {
    pub id: String,
    pub event: String,
    pub arg: SymbolParam,
    #[serde(rename = "connId")]
    pub inst_id: String,
}

#[derive(Debug, Deserialize)]
pub struct ParsedBookMessage {
    pub arg: SymbolParam,
    pub action: DepthBookAction,
    pub data: Vec<ParsedBookData>,
}

#[derive(Debug, Deserialize)]
pub struct ParsedBookData {
    #[serde(deserialize_with = "deserialize_levels")]
    pub asks: Vec<(Price, Qty)>,
    #[serde(deserialize_with = "deserialize_levels")]
    pub bids: Vec<(Price, Qty)>,
    pub ts: String,
    pub checksum: i32,
    #[serde(rename = "seqId")]
    pub seq_id: u64,
    #[serde(rename = "prevSeqId")]
    pub prev_seq_id: i64,
}

/* Error types */

#[derive(Debug, Error)]
pub enum ValidateBookError {
    #[error("Instrument not found: {0}")]
    InstrumentNotFound(Instrument),

    #[error("Missing book data in payload")]
    MissingBookData,

    #[error("Missing snapshot for this instrument")]
    MissingSnapshot,

    #[error("Stale update: event prev_seq_id={event_prev_seq_id} < book prev_seq_id={book_prev_seq_id}")]
    StaleUpdate {
        event_prev_seq_id: u64,
        book_prev_seq_id: u64,
    },

    #[error("Update gap detected: event prev_seq_id={event_prev_seq_id} > expected prev_seq_id={expected_prev_seq_id}")]
    UpdateGap {
        event_prev_seq_id: u64,
        expected_prev_seq_id: u64,
    },
}

/* Shared deserializer helpers */

fn deserialize_levels<'de, D>(deserializer: D) -> Result<Vec<(Price, Qty)>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw: Vec<[String; 4]> = Vec::deserialize(deserializer)?;

    raw.into_iter()
        .map(|[p, q, _, _]| {
            let price = Decimal::from_str(&p).map_err(serde::de::Error::custom)?;
            let qty = Decimal::from_str(&q).map_err(serde::de::Error::custom)?;
            Ok((Price(price), Qty(qty)))
        })
        .collect()
}
