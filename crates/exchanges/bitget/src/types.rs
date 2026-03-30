use md_core::types::{Instrument, Price, Qty, RawMdMsg};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;
use tokio_tungstenite::tungstenite::Message;
use url::Url;
use std::str::FromStr;

pub struct BookState {
    pub last_seq: Option<u64>
}

impl BookState {
    pub fn new() -> Self {
        Self {
            last_seq: None
        }
    }
}

pub struct BitgetUrls {
    pub exchange_info: Url,
    pub ws: Url,
}

pub enum BitgetMdMsg {
    Instruments(Vec<Instrument>),
    Raw(RawMdMsg)
}

#[derive(Clone)]
pub struct Subscriptions {
    pub symbols: Vec<Instrument>,
    pub messages: Vec<Message>
}

#[derive(Deserialize)]
pub struct ApiResponse {
    pub data: Vec<SymbolInfo>,
}

#[derive(Deserialize)]
pub struct SymbolInfo {
    pub symbol: Instrument,
    pub status: String
}

#[derive(Debug, Serialize)]
pub struct SubscriptionRequest {
    pub op: String,
    pub args: Vec<SymbolParam>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SymbolParam {
    #[serde(rename = "instType")]
    pub inst_type: String,

    pub channel: String,
    
    #[serde(rename = "instId")]
    pub inst_id: Instrument
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum WsMessage {
    Confirmation(SubscriptionConfirmation),
    Depth(ParsedBookMessage)
}

#[derive(Debug, Deserialize)]
pub struct SubscriptionConfirmation {
    pub event: String,
    pub arg: SymbolParam
}

#[derive(Debug, Deserialize)]
pub struct ParsedBookMessage {
    pub action: DepthBookAction,
    pub arg: SymbolParam,
    pub data: Vec<ParsedBookData>,
    pub ts: u64
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DepthBookAction {
    Snapshot,
    Update
}

#[derive(Debug, Deserialize)]
pub struct ParsedBookData {
    #[serde(deserialize_with = "deserialize_levels")]
    pub asks: Vec<(Price, Qty)>,

    #[serde(deserialize_with = "deserialize_levels")]
    pub bids: Vec<(Price, Qty)>,

    pub checksum: i32,

    pub seq: u64,

    pub ts: String
}

fn deserialize_levels<'de, D>(deserializer: D) -> Result<Vec<(Price, Qty)>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw: Vec<[String; 2]> = Vec::deserialize(deserializer)?;

    raw.into_iter()
        .map(|[p, q]| {
            let price = Decimal::from_str(&p).map_err(serde::de::Error::custom)?;
            let qty = Decimal::from_str(&q).map_err(serde::de::Error::custom)?;

            Ok((Price(price), Qty(qty)))
        })
        .collect()
}

#[derive(Debug, Error)]
pub enum ValidateBookError {
    #[error("Invalid action for snapshot")]
    InvalidSnapshotAction,

    #[error("Invalid action for update")]
    InvalidUpdateAction,

    #[error("Instrument not found: {0}")]
    InstrumentNotFound(Instrument),

    #[error("Missing book data in payload")]
    MissingBookData,

    #[error("Missing snapshot for this instrument")]
    MissingSnapshot,

    #[error("Stale update: event seq={new_seq} <= book seq={last_seq}")]
    StaleUpdate {
        new_seq: u64,
        last_seq: u64,
    }
}