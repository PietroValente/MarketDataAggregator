use serde::Deserialize;
use std::fs;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub binance: BinanceConfig,
    pub channels: ChannelConfig,
}

#[derive(Debug, Deserialize)]
pub struct BinanceConfig {
    pub exchange_info: String,
    pub snapshot: String,
    pub ws: String,
    pub max_subscription_per_ws: usize,
}

#[derive(Debug, Deserialize)]
pub struct ChannelConfig {
    pub raw_buffer: usize,
    pub normalized_buffer: usize,
    pub control_buffer: usize,
    pub log_buffer: usize,
}

pub fn load_config(path: &str) -> Result<AppConfig, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(path)?;
    let config = toml::from_str(&content)?;
    Ok(config)
}