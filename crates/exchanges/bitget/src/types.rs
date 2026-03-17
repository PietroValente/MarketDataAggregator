use md_core::types::{Instrument, Price, Qty, RawMdMsg};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use tokio_tungstenite::tungstenite::Message;
use url::Url;
use std::str::FromStr;

pub struct BitgetUrls {
    pub exchange_info: Url,
    pub ws: Url,
}

pub struct BitgetMdMsg(pub RawMdMsg);

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

#[derive(Debug, Serialize)]
pub struct SymbolParam {
    #[serde(rename = "instType")]
    pub inst_type: String,
    pub channel: String,
    #[serde(rename = "instId")]
    pub inst_id: String
}





pub struct BinanceSnapshotMsg {
    pub symbol: Instrument,
    pub payload: RawMdMsg
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

#[derive(Debug, Deserialize)]
pub struct DepthSnapshot {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,

    #[serde(deserialize_with = "deserialize_levels")]
    pub bids: Vec<(Price, Qty)>,

    #[serde(deserialize_with = "deserialize_levels")]
    pub asks: Vec<(Price, Qty)>,
}

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