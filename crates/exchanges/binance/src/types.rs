use md_core::types::RawMdMsg;
use serde::{Deserialize, Serialize};
use tokio_tungstenite::tungstenite::Message;
use url::Url;

#[derive(Deserialize)]
pub struct ApiResponse {
    pub symbols: Vec<SymbolInfo>,
}

#[derive(Deserialize)]
pub struct SymbolInfo {
    pub symbol: String,
    pub status: String
}

#[derive(Debug, Serialize)]
pub struct SubscriptionRequest {
    pub method: String,
    pub params: Vec<String>,
    pub id: i64
}

pub struct BinanceUrls {
    pub exchange_info: Url,
    pub snapshot: Url,
    pub ws: Url,
}

pub struct BinanceSnapshotMsg {
    pub symbol: String,
    pub payload: RawMdMsg
}

pub enum BinanceMdMsg {
    Snapshot(BinanceSnapshotMsg),
    Update(RawMdMsg)
}

pub struct BinanceSubscriptionMsg {
    pub symbols: Vec<String>,
    pub payload: Message
}

#[derive(Debug, Serialize)]
pub struct DepthQuery<'a> {
    pub symbol: &'a str,
    pub limit: u32,
}