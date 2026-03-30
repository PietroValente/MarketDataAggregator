use md_core::types::{Instrument, Price, Qty, RawMdMsg};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;
use tokio_tungstenite::tungstenite::Message;
use url::Url;
use std::str::FromStr;

pub struct BookState {
    pub initialized: bool,
    pub last_update_id: Option<u64>
}

impl BookState {
    pub fn new() -> Self {
        Self {
            initialized: false,
            last_update_id: None
        }
    }
}

pub struct BybitUrls {
    pub exchange_info: Url,
    pub ws: Url,
}

pub enum BybitMdMsg {
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
    pub result: ApiList
}

#[derive(Deserialize)]
pub struct ApiList {
    pub list: Vec<SymbolInfo>,
}

#[derive(Deserialize)]
pub struct SymbolInfo {
    pub symbol: Instrument,
    pub status: String
}

#[derive(Debug, Serialize)]
pub struct SubscriptionRequest {
    pub op: String,
    pub args: Vec<String>
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum WsMessage {
    Confirmation(SubscriptionConfirmation),
    Depth(ParsedBookMessage)
}

#[derive(Debug, Deserialize)]
pub struct SubscriptionConfirmation {
    pub success: bool,
    pub ret_msg: String,
    pub conn_id: String,
    pub op: String
}

#[derive(Debug, Deserialize)]
pub struct ParsedBookMessage {
    pub topic: String,

    #[serde(rename = "type")]
    pub action: DepthBookAction,

    pub ts: u64,
    pub cts: u64, 
    pub data: ParsedBookData
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DepthBookAction {
    Snapshot,
    Delta
}

#[derive(Debug, Deserialize)]
pub struct ParsedBookData {
    #[serde(rename = "s")]
    pub symbol: Instrument,

    #[serde(rename = "b", deserialize_with = "deserialize_levels")]
    pub bids: Vec<(Price, Qty)>,

    #[serde(rename = "a", deserialize_with = "deserialize_levels")]
    pub asks: Vec<(Price, Qty)>,

    #[serde(rename = "u")]
    pub update_id: u64,

    #[serde(rename = "seq")]
    pub sequence: u64
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

    #[error("Invalid action for delta")]
    InvalidDeltaAction,

    #[error("Instrument not found: {0}")]
    InstrumentNotFound(Instrument),

    #[error("Missing snapshot for this instrument")]
    MissingSnapshot,

    #[error("Stale update: event update_id={new_update_id} <= book update_id={last_update_id}")]
    StaleUpdate {
        new_update_id: u64,
        last_update_id: u64,
    }
}