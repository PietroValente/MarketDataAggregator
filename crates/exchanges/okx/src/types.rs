use md_core::types::{Price, Qty, RawMdMsg};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use url::Url;
use std::{ops::Deref, str::FromStr};

pub struct OkxUrls {
    pub exchange_info: Url,
    pub ws: Url,
}

pub struct OkxMdMsg(pub RawMdMsg);

impl Deref for OkxMdMsg {
    type Target = RawMdMsg;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Deserialize)]
pub struct ApiResponse {
    pub data: Vec<SymbolInfo>,
}

#[derive(Deserialize)]
pub struct SymbolInfo {
    #[serde(rename = "instId")]
    pub inst_id: String,

    pub state: String
}

#[derive(Debug, Serialize)]
pub struct SubscriptionRequest {
    pub id: String,
    pub op: String,
    pub args: Vec<SymbolParam>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SymbolParam {
    pub channel: String,
    
    #[serde(rename = "instId")]
    pub inst_id: String
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum WsMessage {
    Confirmation(SubscriptionConfirmation),
    Depth(ParsedBookMessage)
}

#[derive(Debug, Deserialize)]
pub struct SubscriptionConfirmation {
    pub id: String,
    pub event: String,
    pub arg: SymbolParam,

    #[serde(rename = "connId")]
    pub inst_id: String
}

#[derive(Debug, Deserialize)]
pub struct ParsedBookMessage {
    pub arg: SymbolParam,
    pub action: DepthBookAction,
    pub data: Vec<ParsedBookData>
}

#[derive(Debug, Clone, Deserialize)]
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

    pub ts: String,

    pub checksum: i32,

    #[serde(rename = "seqId")]
    pub seq_id: u64,

    #[serde(rename = "prevSeqId")]
    pub prev_seq_id: i64,
}

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