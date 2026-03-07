use std::{cmp::Reverse, collections::BTreeMap};
use thiserror::Error;
use crate::types::{Price, Qty};

#[derive(Clone, Copy)]
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

pub struct BookSnapshot {
    last_update_id: u64,
    bids: Vec<(Price, Qty)>,
    asks: Vec<(Price, Qty)>,
}

pub struct BookUpdate {
    first_update_id: u64,
    last_update_id: u64,
    bids: Vec<(Price, Qty)>,
    asks: Vec<(Price, Qty)>,
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

    pub fn apply_snapshot(&mut self, snapshot: BookSnapshot) {
        self.clear();
        self.update_id = snapshot.last_update_id;
        for (price, qty) in snapshot.asks {
            self.upsert_ask(price, qty);
        }
        for (price, qty) in snapshot.bids {
            self.upsert_bid(price, qty);
        }
    }

    pub fn apply_update(&mut self, update: BookUpdate) -> Result<(), LocalBookError> {
        if update.last_update_id <= self.update_id {
            return Ok(());
        }
        if update.first_update_id > self.update_id + 1 {
            return Err(LocalBookError::OutOfSync);
        }
        self.update_id = update.last_update_id;
        for (price, qty) in update.asks {
            self.upsert_ask(price, qty);
        }
        for (price, qty) in update.bids {
            self.upsert_bid(price, qty);
        }
        Ok(())
    }

    pub fn upsert_ask(&mut self, price: Price, qty: Qty) {
        let lvl = BookLevel::new(qty, price);
        *self.ask.entry(price).or_insert(lvl) = lvl;
    }

    pub fn upsert_bid(&mut self, price: Price, qty: Qty) {
        let lvl = BookLevel::new(qty, price);
        *self.bid.entry(Reverse(price)).or_insert(lvl) = lvl;
    }

    pub fn clear(&mut self) {
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
}

#[derive(Error, Debug)]
pub enum LocalBookError {
    #[error("Local orderbook is no longer synchronized with the exchange stream")]
    OutOfSync
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn px(v: i64) -> Price {
        Price(dec!(0) + rust_decimal::Decimal::from(v))
    }

    fn qty(v: i64) -> Qty {
        Qty(dec!(0) + rust_decimal::Decimal::from(v))
    }

    #[test]
    fn new_book_is_empty() {
        let book = LocalBook::new();

        assert_eq!(book.update_id, 0);
        assert!(book.top_n_ask(10).is_empty());
        assert!(book.top_n_bid(10).is_empty());
    }

    #[test]
    fn apply_snapshot_loads_book_and_orders_sides_correctly() {
        let mut book = LocalBook::new();

        book.apply_snapshot(BookSnapshot {
            last_update_id: 42,
            asks: vec![(px(105), qty(5)), (px(101), qty(1)), (px(103), qty(3))],
            bids: vec![(px(99), qty(9)), (px(100), qty(10)), (px(98), qty(8))],
        });

        assert_eq!(book.update_id, 42);

        let asks = book.top_n_ask(3);
        assert_eq!(asks.len(), 3);
        assert_eq!(asks[0].px, px(101));
        assert_eq!(asks[1].px, px(103));
        assert_eq!(asks[2].px, px(105));

        let bids = book.top_n_bid(3);
        assert_eq!(bids.len(), 3);
        assert_eq!(bids[0].px, px(100));
        assert_eq!(bids[1].px, px(99));
        assert_eq!(bids[2].px, px(98));
    }

    #[test]
    fn apply_snapshot_replaces_previous_state() {
        let mut book = LocalBook::new();

        book.apply_snapshot(BookSnapshot {
            last_update_id: 10,
            asks: vec![(px(101), qty(1))],
            bids: vec![(px(99), qty(1))],
        });

        book.apply_snapshot(BookSnapshot {
            last_update_id: 20,
            asks: vec![(px(110), qty(10))],
            bids: vec![(px(90), qty(10))],
        });

        assert_eq!(book.update_id, 20);

        let asks = book.top_n_ask(10);
        let bids = book.top_n_bid(10);

        assert_eq!(asks.len(), 1);
        assert_eq!(asks[0].px, px(110));
        assert_eq!(asks[0].qty, qty(10));

        assert_eq!(bids.len(), 1);
        assert_eq!(bids[0].px, px(90));
        assert_eq!(bids[0].qty, qty(10));
    }

    #[test]
    fn upsert_replaces_existing_ask_and_bid_level() {
        let mut book = LocalBook::new();

        book.upsert_ask(px(101), qty(1));
        book.upsert_ask(px(101), qty(7));

        let asks = book.top_n_ask(10);
        assert_eq!(asks.len(), 1);
        assert_eq!(asks[0].px, px(101));
        assert_eq!(asks[0].qty, qty(7));

        book.upsert_bid(px(99), qty(2));
        book.upsert_bid(px(99), qty(8));

        let bids = book.top_n_bid(10);
        assert_eq!(bids.len(), 1);
        assert_eq!(bids[0].px, px(99));
        assert_eq!(bids[0].qty, qty(8));
    }

    #[test]
    fn apply_update_accepts_next_contiguous_update() {
        let mut book = LocalBook::new();

        book.apply_snapshot(BookSnapshot {
            last_update_id: 100,
            asks: vec![(px(101), qty(1))],
            bids: vec![(px(99), qty(1))],
        });

        let res = book.apply_update(BookUpdate {
            first_update_id: 101,
            last_update_id: 102,
            asks: vec![(px(102), qty(2))],
            bids: vec![(px(100), qty(3))],
        });

        assert!(res.is_ok());
        assert_eq!(book.update_id, 102);

        let asks = book.top_n_ask(10);
        let bids = book.top_n_bid(10);

        assert_eq!(asks.len(), 2);
        assert_eq!(asks[0].px, px(101));
        assert_eq!(asks[1].px, px(102));

        assert_eq!(bids.len(), 2);
        assert_eq!(bids[0].px, px(100));
        assert_eq!(bids[1].px, px(99));
    }

    #[test]
    fn apply_update_ignores_stale_update() {
        let mut book = LocalBook::new();

        book.apply_snapshot(BookSnapshot {
            last_update_id: 50,
            asks: vec![(px(101), qty(1))],
            bids: vec![(px(99), qty(1))],
        });

        let res = book.apply_update(BookUpdate {
            first_update_id: 45,
            last_update_id: 50,
            asks: vec![(px(200), qty(99))],
            bids: vec![(px(1), qty(99))],
        });

        assert!(res.is_ok());
        assert_eq!(book.update_id, 50);

        let asks = book.top_n_ask(10);
        let bids = book.top_n_bid(10);

        assert_eq!(asks.len(), 1);
        assert_eq!(asks[0].px, px(101));

        assert_eq!(bids.len(), 1);
        assert_eq!(bids[0].px, px(99));
    }

    #[test]
    fn apply_update_returns_out_of_sync_when_there_is_a_gap() {
        let mut book = LocalBook::new();

        book.apply_snapshot(BookSnapshot {
            last_update_id: 10,
            asks: vec![(px(101), qty(1))],
            bids: vec![(px(99), qty(1))],
        });

        let res = book.apply_update(BookUpdate {
            first_update_id: 12,
            last_update_id: 13,
            asks: vec![(px(102), qty(2))],
            bids: vec![(px(100), qty(2))],
        });

        assert!(matches!(res, Err(LocalBookError::OutOfSync)));
        assert_eq!(book.update_id, 10);

        let asks = book.top_n_ask(10);
        let bids = book.top_n_bid(10);

        assert_eq!(asks.len(), 1);
        assert_eq!(asks[0].px, px(101));

        assert_eq!(bids.len(), 1);
        assert_eq!(bids[0].px, px(99));
    }

    #[test]
    fn top_n_returns_only_available_levels() {
        let mut book = LocalBook::new();

        book.apply_snapshot(BookSnapshot {
            last_update_id: 1,
            asks: vec![(px(101), qty(1)), (px(102), qty(2))],
            bids: vec![(px(99), qty(1))],
        });

        assert_eq!(book.top_n_ask(10).len(), 2);
        assert_eq!(book.top_n_bid(10).len(), 1);
    }

    #[test]
    fn clear_removes_everything_and_resets_update_id() {
        let mut book = LocalBook::new();

        book.apply_snapshot(BookSnapshot {
            last_update_id: 99,
            asks: vec![(px(101), qty(1))],
            bids: vec![(px(99), qty(1))],
        });

        book.clear();

        assert_eq!(book.update_id, 0);
        assert!(book.top_n_ask(10).is_empty());
        assert!(book.top_n_bid(10).is_empty());
    }
}