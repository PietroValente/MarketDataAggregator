use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    ops::{Add, Deref, DerefMut, Div, Mul, Sub},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Price(pub Decimal);

impl Add for Price {
    type Output = Price;

    fn add(self, rhs: Self) -> Self::Output {
        Price(self.0 + rhs.0)
    }
}

impl Sub for Price {
    type Output = Price;

    fn sub(self, rhs: Self) -> Self::Output {
        Price(self.0 - rhs.0)
    }
}

impl Mul for Price {
    type Output = Price;

    fn mul(self, rhs: Self) -> Self::Output {
        Price(self.0 * rhs.0)
    }
}

impl Div for Price {
    type Output = Price;

    fn div(self, rhs: Self) -> Self::Output {
        Price(self.0 / rhs.0)
    }
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Qty(pub Decimal);

impl Add for Qty {
    type Output = Qty;

    fn add(self, rhs: Self) -> Self::Output {
        Qty(self.0 + rhs.0)
    }
}

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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
    Okx,
}

impl fmt::Display for Exchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Exchange::Unknown => write!(f, "Unknown"),
            Exchange::Binance => write!(f, "Binance"),
            Exchange::Bitget => write!(f, "Bitget"),
            Exchange::Bybit => write!(f, "Bybit"),
            Exchange::Coinbase => write!(f, "Coinbase"),
            Exchange::Okx => write!(f, "OKX"),
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
            _ => Exchange::Unknown,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExchangeStatus {
    Initializing(f32),
    Live,
}

impl fmt::Display for ExchangeStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExchangeStatus::Initializing(percentage) => {
                write!(f, "Initializing: {:.2}%", percentage * 100.0)
            }
            ExchangeStatus::Live => write!(f, "Live"),
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
