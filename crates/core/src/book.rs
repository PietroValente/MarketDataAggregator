use std::{cmp::Reverse, collections::BTreeMap, fmt::Display};
use crate::types::{Price, Qty};

#[derive(Debug, Clone, Copy)]
pub struct BookLevel {
    qty: Qty,
    px: Price
}

impl BookLevel {
    pub fn new(qty: Qty, px: Price) -> Self {
        Self {
            qty,
            px
        }
    }

    pub fn qty(&self) -> &Qty {
        &self.qty
    }

    pub fn px(&self) -> &Price {
        &self.px
    }
}

impl Display for BookLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}   {}", self.px, self.qty)
    }
}

#[derive(Debug)]
pub struct BookLevels {
    pub bids: Vec<(Price, Qty)>,
    pub asks: Vec<(Price, Qty)>,
}

pub struct LocalBook {
    update_id: u64,
    ask: BTreeMap<Price, BookLevel>,
    bid: BTreeMap<Reverse<Price>, BookLevel>
}

impl LocalBook {
    pub fn new() -> Self {
        Self {
            update_id: 0,
            ask: BTreeMap::new(),
            bid: BTreeMap::new()
        }
    }

    pub fn apply_snapshot(&mut self, snapshot: BookLevels) {
        self.clear();
        for (price, qty) in snapshot.asks {
            self.upsert_ask(price, qty);
        }
        for (price, qty) in snapshot.bids {
            self.upsert_bid(price, qty);
        }
    }

    pub fn apply_update(&mut self, update: BookLevels) {
        for (price, qty) in update.asks {
            if qty.is_zero() {
                self.remove_ask(price);
            } else {
                self.upsert_ask(price, qty);
            }
        }
        for (price, qty) in update.bids {
            if qty.is_zero() {
                self.remove_bid(price);
            } else {
                self.upsert_bid(price, qty);
            }
        }
    }

    fn upsert_ask(&mut self, price: Price, qty: Qty) {
        let lvl = BookLevel::new(qty, price);
        *self.ask.entry(price).or_insert(lvl) = lvl;
    }

    fn upsert_bid(&mut self, price: Price, qty: Qty) {
        let lvl = BookLevel::new(qty, price);
        *self.bid.entry(Reverse(price)).or_insert(lvl) = lvl;
    }

    fn remove_ask(&mut self, price: Price) -> Option<BookLevel> {
        self.ask.remove(&price)
    }

    fn remove_bid(&mut self, price: Price) -> Option<BookLevel> {
        self.bid.remove(&Reverse(price))
    }

    fn clear(&mut self) {
        self.ask.clear();
        self.bid.clear();
        self.update_id = 0;
    }

    pub fn top_n_ask(&self, n: usize) -> Vec<BookLevel> {
        self.ask.values().take(n).copied().collect()
    }

    pub fn top_n_bid(&self, n: usize) -> Vec<BookLevel> {
        self.bid.values().take(n).copied().collect()
    }

    pub fn ask_len(&self) -> usize {
        self.ask.len()
    }

    pub fn bid_len(&self) -> usize {
        self.bid.len()
    }
}

#[cfg(test)]
mod tests {
}