use std::{error::Error, fmt, ops::{Deref, DerefMut}};
use rust_decimal::Decimal;
use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Price(pub Decimal);

impl fmt::Display for Price {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Qty(pub Decimal);

impl fmt::Display for Qty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Qty {
    pub fn is_zero(&self) -> bool {
        self.0.is_zero()
    }
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
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
    Binance,
    Bybit,
    Coinbase,
    Kraken,
    Okx
}

impl fmt::Display for Exchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Exchange::Binance => write!(f, "Binance"),
            Exchange::Bybit => write!(f, "Bybit"),
            Exchange::Coinbase => write!(f, "Coinbase"),
            Exchange::Kraken => write!(f, "Kraken"),
            Exchange::Okx => write!(f, "OKX")
        }
    }
}

#[derive(Debug)]
pub enum ExchangeStatus {
    Initializing,
    Running,
    Error(Box<dyn Error + Send + Sync>)
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