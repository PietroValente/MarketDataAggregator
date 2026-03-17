use md_core::types::{Instrument, Price, Qty, RawMdMsg};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use tokio_tungstenite::tungstenite::Message;
use url::Url;
use std::{ops::Deref, str::FromStr};

pub struct BitgetUrls {
    pub exchange_info: Url,
    pub ws: Url,
}

pub struct BitgetMdMsg(pub RawMdMsg);

impl Deref for BitgetMdMsg {
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
    pub symbol: String,
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
    pub inst_id: String
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum WsMessage {
    Confirmation(SubscriptionConfirmation),
    Depth(DepthBook)
}

#[derive(Debug, Deserialize)]
pub struct SubscriptionConfirmation {
    pub event: String,
    pub arg: SymbolParam
}

#[derive(Debug, Deserialize)]
pub struct DepthBook {
    pub action: DepthBookAction,
    pub arg: SymbolParam,
    pub data: Vec<BookData>,
    pub ts: u64
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DepthBookAction {
    Snapshot,
    Update
}

#[derive(Debug, Deserialize)]
pub struct BookData {
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

#[derive(Debug, Deserialize)]
pub struct DepthUpdate {
    #[serde(rename = "action")]
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





#[derive(Clone)]
pub struct BinanceSubscriptionMsg {
    pub symbols: Vec<Instrument>,
    pub payload: Message
}

#[derive(Debug, Serialize)]
pub struct DepthQuery<'a> {
    pub symbol: &'a str,
    pub limit: u32,
}



