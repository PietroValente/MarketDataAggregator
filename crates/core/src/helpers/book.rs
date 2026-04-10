use crc32fast::Hasher;
use std::fmt::Write;

use crate::book::BookLevel;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ChecksumError {
    #[error("checksum mismatch: expected={expected}, computed={computed}")]
    Mismatch { expected: i32, computed: i32 },
}

pub fn compute_checksum(bids: Vec<BookLevel>, asks: Vec<BookLevel>) -> i32 {
    let mut bids = bids.into_iter().take(25);
    let mut asks = asks.into_iter().take(25);

    let mut buf = String::with_capacity(512);

    loop {
        let next_bid = bids.next();
        let next_ask = asks.next();

        if next_bid.is_none() && next_ask.is_none() {
            break;
        }

        if let Some(level) = next_bid {
            push_level(&mut buf, &level);
        }

        if let Some(level) = next_ask {
            push_level(&mut buf, &level);
        }
    }

    let mut hasher = Hasher::new();
    hasher.update(buf.as_bytes());
    hasher.finalize() as i32
}

pub fn verify_checksum(
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
    expected: i32,
) -> Result<(), ChecksumError> {
    let computed = compute_checksum(bids, asks);

    if computed != expected {
        return Err(ChecksumError::Mismatch { expected, computed });
    }

    Ok(())
}

fn push_level(buf: &mut String, level: &BookLevel) {
    if !buf.is_empty() {
        buf.push(':');
    }

    let _ = write!(buf, "{}:{}", level.px(), level.qty());
}
