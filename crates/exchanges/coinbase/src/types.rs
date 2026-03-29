use md_core::types::{Instrument, Price, Qty, RawMdMsg};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use url::Url;
use std::{ops::Deref, str::FromStr};

pub struct CoinbaseUrls {
    pub exchange_info: Url,
    pub ws: Url,
}

pub struct CoinbaseMdMsg(pub RawMdMsg);

impl Deref for CoinbaseMdMsg {
    type Target = RawMdMsg;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Deserialize)]
pub struct ApiResponse {
    pub products: Vec<SymbolInfo>,
}

#[derive(Deserialize)]
pub struct SymbolInfo {
    pub product_id: Instrument,
    pub status: String
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubscriptionMsg {
    #[serde(rename = "type")]
    pub op_type: String,

    pub channels: Vec<SymbolParam>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SymbolParam {
    pub name: String,
    pub product_ids: Vec<Instrument>
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum WsMessage {
    Confirmation(SubscriptionMsg),
    Snapshot(ParsedBookSnapshot),
    Update(ParsedBookUpdate)
}

#[derive(Debug, Deserialize)]
pub struct ParsedBookSnapshot {
    #[serde(rename = "type")]
    pub op_type: String,

    pub product_id: String,

    #[serde(deserialize_with = "deserialize_levels")]
    pub asks: Vec<(Price, Qty)>,

    #[serde(deserialize_with = "deserialize_levels")]
    pub bids: Vec<(Price, Qty)>,

    pub time: String
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
pub struct ParsedBookUpdate {
    #[serde(rename = "type")]
    pub op_type: String,

    pub product_id: String,

    #[serde(deserialize_with = "deserialize_changes")]
    pub changes: Vec<(Side, Price, Qty)>,

    pub time: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Bid,
    Ask,
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
                other => {
                    return Err(serde::de::Error::custom(format!(
                        "invalid side: {other}"
                    )))
                }
            };

            let price = Decimal::from_str(&p).map_err(serde::de::Error::custom)?;
            let qty = Decimal::from_str(&q).map_err(serde::de::Error::custom)?;

            Ok((side, Price(price), Qty(qty)))
        })
        .collect()
}