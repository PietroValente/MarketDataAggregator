use md_core::types::{Price, Qty, RawMdMsg};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use url::Url;
use std::{collections::HashMap, ops::Deref};

pub struct KrakenUrls {
    pub exchange_info: Url,
    pub ws: Url,
}

pub struct KrakenMdMsg(pub RawMdMsg);

impl Deref for KrakenMdMsg {
    type Target = RawMdMsg;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Deserialize)]
pub struct ApiResponse {
    pub result: HashMap<String, SymbolInfo>,
}

#[derive(Deserialize)]
pub struct SymbolInfo {
    pub wsname: String,
    pub status: String
}

#[derive(Debug, Serialize)]
pub struct SubscriptionRequest {
    pub method: String,
    pub params: SymbolParam
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SymbolParam {
    pub channel: String,
    pub symbol: Vec<String>
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum WsMessage {
    Confirmation(SubscriptionConfirmation),
    Depth(ParsedBookMessage)
}

#[derive(Debug, Deserialize)]
pub struct SubscriptionConfirmation {
    pub method: String,
    pub result: SubscriptionResult,
    pub success: bool,
    pub time_in: String,
    pub time_out: String,
}

#[derive(Debug, Deserialize)]
pub struct SubscriptionResult {
    pub channel: String,
    pub depth: u32,
    pub snapshot: bool,
    pub symbol: String,
}

#[derive(Debug, Deserialize)]
pub struct ParsedBookMessage {
    pub channel: String,

    #[serde(rename = "type")]
    pub op_type: DepthBookAction,

    pub data: Vec<ParsedBookData>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DepthBookAction {
    Snapshot,
    Update
}

#[derive(Debug, Deserialize)]
pub struct ParsedBookData {
    pub symbol: String,

    #[serde(deserialize_with = "deserialize_levels")]
    pub bids: Vec<(Price, Qty)>,

    #[serde(deserialize_with = "deserialize_levels")]
    pub asks: Vec<(Price, Qty)>,

    pub checksum: u32,

    pub timestamp: String
}

fn deserialize_levels<'de, D>(deserializer: D) -> Result<Vec<(Price, Qty)>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    struct RawLevel {
        price: Decimal,
        qty: Decimal,
    }

    let raw: Vec<RawLevel> = Vec::deserialize(deserializer)?;

    raw.into_iter()
        .map(|level| Ok((Price(level.price), Qty(level.qty))))
        .collect()
}