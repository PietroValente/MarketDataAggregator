use std::{fmt, ops::{Deref, DerefMut}};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Price(pub Decimal);

impl fmt::Display for Price {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Deref for Price {
    type Target = Decimal;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Price {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Qty(pub Decimal);

impl fmt::Display for Qty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Deref for Qty {
    type Target = Decimal;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Qty {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Qty {
    pub fn is_zero(&self) -> bool {
        self.0.is_zero()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct Instrument(pub String);

impl fmt::Display for Instrument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Deref for Instrument {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Instrument {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<String> for Instrument {
    fn from(value: String) -> Self {
        Instrument(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Exchange {
    Unknown,
    Binance,
    Bitget,
    Bybit,
    Coinbase,
    Okx
}

impl fmt::Display for Exchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Exchange::Unknown => write!(f, "Unknown"),
            Exchange::Binance => write!(f, "Binance"),
            Exchange::Bitget => write!(f, "Bitget"),
            Exchange::Bybit => write!(f, "Bybit"),
            Exchange::Coinbase => write!(f, "Coinbase"),
            Exchange::Okx => write!(f, "OKX")
        }
    }
}

impl From<&str> for Exchange {
    fn from(value: &str) -> Self {
        match value.to_lowercase().as_str() {
            "binance" => Exchange::Binance,
            "bitget" => Exchange::Bitget,
            "bybit" => Exchange::Bybit,
            "coinbase" => Exchange::Coinbase,
            "okx" => Exchange::Okx,
            _ => Exchange::Unknown
        }
    }
}

#[derive(Debug, Clone)]
pub enum ExchangeStatus {
    Initializing(f32),
    Running
}

impl fmt::Display for ExchangeStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExchangeStatus::Initializing(percentage) => write!(f, "Initializing: {:.2}%", percentage*100.0),
            ExchangeStatus::Running => write!(f, "Running")
        }
    }
}

#[derive(Debug)]
pub struct RawMdMsg(pub Vec<u8>);

impl Deref for RawMdMsg {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RawMdMsg {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}