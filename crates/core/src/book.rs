use std::{cmp::Reverse, collections::BTreeMap, fmt::Display};
use rust_decimal::Decimal;

use crate::{helpers::book::{verify_checksum, ChecksumError}, types::{Price, Qty}};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
        fn fmt_decimal(mut x: Decimal, scale: u32) -> String {
            x.rescale(scale);
            x.to_string()
        }

        let px = fmt_decimal(self.px.0, 2);
        let qty = fmt_decimal(self.qty.0, 5);

        write!(f, "{:>12}   {:>12}", px, qty)
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

    pub fn verify_okx_bitget_checksum(&self, expected: i32) -> Result<(), ChecksumError> {
        verify_checksum(
            self.top_n_bid(25),
            self.top_n_ask(25),
            expected,
        )
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
    use super::*;
    use rust_decimal::Decimal;
    use std::collections::BTreeMap;

    fn price(v: i64) -> Price {
        Price(Decimal::new(v, 2))
    }

    fn qty(v: i64) -> Qty {
        Qty(Decimal::new(v, 3))
    }

    #[test]
    fn snapshot_replaces_previous_state() {
        let mut book = LocalBook::new();
        book.apply_snapshot(BookLevels {
            asks: vec![(price(10100), qty(1000)), (price(10200), qty(2000))],
            bids: vec![(price(10000), qty(3000)), (price(9900), qty(4000))],
        });

        assert_eq!(book.ask_len(), 2);
        assert_eq!(book.bid_len(), 2);

        book.apply_snapshot(BookLevels {
            asks: vec![(price(10300), qty(5000))],
            bids: vec![(price(9800), qty(6000))],
        });

        assert_eq!(book.ask_len(), 1);
        assert_eq!(book.bid_len(), 1);
        assert_eq!(book.top_n_ask(10), vec![BookLevel::new(qty(5000), price(10300))]);
        assert_eq!(book.top_n_bid(10), vec![BookLevel::new(qty(6000), price(9800))]);
    }

    #[test]
    fn updates_handle_insert_replace_and_remove_for_both_sides() {
        let mut book = LocalBook::new();
        book.apply_snapshot(BookLevels {
            asks: vec![(price(10000), qty(1000))],
            bids: vec![(price(9900), qty(1100))],
        });

        book.apply_update(BookLevels {
            asks: vec![
                (price(10000), qty(1200)), // replace existing ask
                (price(10100), qty(1300)), // insert new ask
            ],
            bids: vec![
                (price(9900), qty(0)),     // remove existing bid
                (price(9800), qty(1400)),  // insert new bid
            ],
        });

        assert_eq!(book.ask_len(), 2);
        assert_eq!(book.bid_len(), 1);
        assert_eq!(
            book.top_n_ask(10),
            vec![
                BookLevel::new(qty(1200), price(10000)),
                BookLevel::new(qty(1300), price(10100)),
            ]
        );
        assert_eq!(book.top_n_bid(10), vec![BookLevel::new(qty(1400), price(9800))]);
    }

    #[test]
    fn top_n_respects_order_and_bounds() {
        let mut book = LocalBook::new();
        book.apply_snapshot(BookLevels {
            asks: vec![
                (price(10400), qty(1000)),
                (price(10200), qty(2000)),
                (price(10300), qty(3000)),
            ],
            bids: vec![
                (price(9900), qty(4000)),
                (price(10100), qty(5000)),
                (price(10000), qty(6000)),
            ],
        });

        // asks should be ascending by price
        assert_eq!(
            book.top_n_ask(2),
            vec![
                BookLevel::new(qty(2000), price(10200)),
                BookLevel::new(qty(3000), price(10300)),
            ]
        );

        // bids should be descending by price
        assert_eq!(
            book.top_n_bid(2),
            vec![
                BookLevel::new(qty(5000), price(10100)),
                BookLevel::new(qty(6000), price(10000)),
            ]
        );

        // requesting more than available must return all levels
        assert_eq!(book.top_n_ask(99).len(), 3);
        assert_eq!(book.top_n_bid(99).len(), 3);
    }

    #[test]
    fn duplicate_prices_in_single_update_follow_last_write_wins() {
        let mut book = LocalBook::new();
        book.apply_snapshot(BookLevels {
            asks: vec![(price(10000), qty(1000))],
            bids: vec![(price(9900), qty(1000))],
        });

        // Same price repeated in one batch should be processed in order.
        book.apply_update(BookLevels {
            asks: vec![
                (price(10000), qty(0)),     // remove existing
                (price(10000), qty(1200)),  // reinsert
                (price(10000), qty(1300)),  // replace again (final value)
            ],
            bids: vec![
                (price(9900), qty(1400)),   // replace existing
                (price(9900), qty(0)),      // remove
                (price(9900), qty(1500)),   // reinsert (final value)
            ],
        });

        assert_eq!(
            book.top_n_ask(1),
            vec![BookLevel::new(qty(1300), price(10000))]
        );
        assert_eq!(
            book.top_n_bid(1),
            vec![BookLevel::new(qty(1500), price(9900))]
        );
    }

    #[test]
    fn removing_nonexistent_levels_is_noop() {
        let mut book = LocalBook::new();
        book.apply_snapshot(BookLevels {
            asks: vec![(price(10100), qty(1000))],
            bids: vec![(price(9900), qty(2000))],
        });

        // Removing prices that are not present should not mutate existing state.
        book.apply_update(BookLevels {
            asks: vec![(price(10500), qty(0))],
            bids: vec![(price(9500), qty(0))],
        });

        assert_eq!(
            book.top_n_ask(10),
            vec![BookLevel::new(qty(1000), price(10100))]
        );
        assert_eq!(
            book.top_n_bid(10),
            vec![BookLevel::new(qty(2000), price(9900))]
        );
    }

    #[test]
    fn random_updates_match_reference_model_under_stress() {
        use rand::{Rng, SeedableRng, rngs::StdRng};

        let mut rng = StdRng::seed_from_u64(0xA11CE_B00C);
        let mut book = LocalBook::new();
        let mut ask_ref: BTreeMap<Price, Qty> = BTreeMap::new();
        let mut bid_ref: BTreeMap<Price, Qty> = BTreeMap::new();

        // Start from a noisy snapshot with duplicate prices to stress last-write behavior.
        let mut asks = Vec::new();
        let mut bids = Vec::new();
        for _ in 0..500 {
            let p = price(rng.gen_range(10000..11000));
            let q = qty(rng.gen_range(1..1_000_000));
            asks.push((p, q));
            ask_ref.insert(p, q);
        }
        for _ in 0..500 {
            let p = price(rng.gen_range(9000..10000));
            let q = qty(rng.gen_range(1..1_000_000));
            bids.push((p, q));
            bid_ref.insert(p, q);
        }

        book.apply_snapshot(BookLevels { asks, bids });

        for _step in 0..10_000 {
            let mut ask_updates = Vec::new();
            let mut bid_updates = Vec::new();

            let ask_batch = rng.gen_range(0..20);
            let bid_batch = rng.gen_range(0..20);

            for _ in 0..ask_batch {
                let p = price(rng.gen_range(9500..11500));
                let remove = rng.gen_bool(0.35);
                let q = if remove {
                    qty(0)
                } else {
                    qty(rng.gen_range(1..1_000_000))
                };
                ask_updates.push((p, q));
                if q.is_zero() {
                    ask_ref.remove(&p);
                } else {
                    ask_ref.insert(p, q);
                }
            }

            for _ in 0..bid_batch {
                let p = price(rng.gen_range(8500..10500));
                let remove = rng.gen_bool(0.35);
                let q = if remove {
                    qty(0)
                } else {
                    qty(rng.gen_range(1..1_000_000))
                };
                bid_updates.push((p, q));
                if q.is_zero() {
                    bid_ref.remove(&p);
                } else {
                    bid_ref.insert(p, q);
                }
            }

            book.apply_update(BookLevels {
                asks: ask_updates,
                bids: bid_updates,
            });

            assert_eq!(book.ask_len(), ask_ref.len());
            assert_eq!(book.bid_len(), bid_ref.len());

            let expected_asks: Vec<BookLevel> = ask_ref
                .iter()
                .take(25)
                .map(|(p, q)| BookLevel::new(*q, *p))
                .collect();
            let expected_bids: Vec<BookLevel> = bid_ref
                .iter()
                .rev()
                .take(25)
                .map(|(p, q)| BookLevel::new(*q, *p))
                .collect();

            assert_eq!(book.top_n_ask(25), expected_asks);
            assert_eq!(book.top_n_bid(25), expected_bids);
        }
    }
}