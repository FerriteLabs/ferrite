//! PN-Counter: increment + decrement, built as two G-Counters.

use crate::{Crdt, GCounter};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PnCounter {
    pos: GCounter,
    neg: GCounter,
}

impl PnCounter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn increment(&mut self, replica: impl Into<String>, delta: u64) {
        self.pos.increment(replica, delta);
    }

    pub fn decrement(&mut self, replica: impl Into<String>, delta: u64) {
        self.neg.increment(replica, delta);
    }

    /// Net value (pos − neg).  Saturates at i128 to bound the API.
    pub fn value(&self) -> i128 {
        i128::from(self.pos.value()) - i128::from(self.neg.value())
    }
}

impl Crdt for PnCounter {
    fn merge(&mut self, other: &Self) {
        self.pos.merge(&other.pos);
        self.neg.merge(&other.neg);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inc_and_dec() {
        let mut c = PnCounter::new();
        c.increment("a", 10);
        c.decrement("a", 3);
        assert_eq!(c.value(), 7);
    }

    #[test]
    fn merge_converges() {
        let mut a = PnCounter::new();
        a.increment("a", 5);
        a.decrement("a", 1);
        let mut b = PnCounter::new();
        b.increment("b", 3);
        b.decrement("b", 2);
        a.merge(&b);
        b.merge(&a);
        assert_eq!(a, b);
        assert_eq!(a.value(), 5);
    }

    #[test]
    fn merge_is_idempotent() {
        let mut a = PnCounter::new();
        a.increment("a", 5);
        a.decrement("a", 2);
        let snap = a.clone();
        a.merge(&snap);
        assert_eq!(a, snap);
    }
}
