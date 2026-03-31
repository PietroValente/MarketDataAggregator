use std::str::FromStr;

use md_core::{
    events::PingMsg, traits::connector::ConnectionTasks, types::{Instrument, Price, Qty, RawMdMsg}
};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

/* Connector commands and transport messages */

pub enum ManagerCommand {
    InsertSubscription(u8, ConnectionTasks),
    RecreateWithSnapshots,
    RecreateFinished,
    Pong(PingMsg),
}

pub enum CoinbaseMdMsg {
    Instruments(Vec<Instrument>),
    Raw(RawMdMsg),
}

/* Connector/API configuration and subscription payloads */

pub struct CoinbaseUrls {
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
    pub products: Vec<SymbolInfo>,
}

#[derive(Deserialize)]
pub struct SymbolInfo {
    pub product_id: Instrument,
    pub status: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubscriptionMsg {
    #[serde(rename = "type")]
    pub op_type: String,
    pub channels: Vec<SymbolParam>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SymbolParam {
    pub name: String,
    pub product_ids: Vec<Instrument>,
}

/* Book state and sync flow */

pub struct BookState {
    pub initialized: bool,
}

impl BookState {
    pub fn new() -> Self {
        Self { initialized: false }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Bid,
    Ask,
}

/* Incoming websocket payloads */

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum WsMessage {
    Confirmation(SubscriptionMsg),
    Snapshot(ParsedBookSnapshot),
    Update(ParsedBookUpdate),
}

#[derive(Debug, Deserialize)]
pub struct ParsedBookSnapshot {
    #[serde(rename = "type")]
    pub op_type: String,
    pub product_id: Instrument,
    #[serde(deserialize_with = "deserialize_levels")]
    pub asks: Vec<(Price, Qty)>,
    #[serde(deserialize_with = "deserialize_levels")]
    pub bids: Vec<(Price, Qty)>,
    pub time: String,
}

#[derive(Debug, Deserialize)]
pub struct ParsedBookUpdate {
    #[serde(rename = "type")]
    pub op_type: String,
    pub product_id: Instrument,
    #[serde(deserialize_with = "deserialize_changes")]
    pub changes: Vec<(Side, Price, Qty)>,
    pub time: String,
}

/* Error types */

#[derive(Debug, Error)]
pub enum ValidateBookError {
    #[error("Instrument not found: {0}")]
    InstrumentNotFound(Instrument),

    #[error("Missing snapshot for this instrument")]
    MissingSnapshot,

    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(String),
}

#[derive(Debug, Error)]
pub enum CoinbaseConnectorError {
    #[error("max_subscription_per_ws cannot be 0")]
    InvalidMaxSubscriptionPerWs,

    #[error("too many websocket batches for u8 ws_id: got {batches}, max {max_supported}")]
    TooManyWsBatchesForU8Id {
        batches: usize,
        max_supported: usize,
    },
}

/* Shared deserializer helpers */

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

fn deserialize_changes<'de, D>(deserializer: D) -> Result<Vec<(Side, Price, Qty)>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw: Vec<[String; 3]> = Vec::deserialize(deserializer)?;

    raw.into_iter()
        .map(|[side, p, q]| {
            let side = match side.as_str() {
                "buy" => Side::Bid,
                "sell" => Side::Ask,
                other => return Err(serde::de::Error::custom(format!("invalid side: {other}"))),
            };

            let price = Decimal::from_str(&p).map_err(serde::de::Error::custom)?;
            let qty = Decimal::from_str(&q).map_err(serde::de::Error::custom)?;
            Ok((side, Price(price), Qty(qty)))
        })
        .collect()
}
