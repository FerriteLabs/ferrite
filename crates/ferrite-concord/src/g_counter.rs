//! G-Counter: monotonic, increment-only, per-replica counter.

use crate::Crdt;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Grow-only counter.  Each replica increments its own slot; total value
/// is the sum across all slots.  Merge is per-slot max.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GCounter {
    slots: BTreeMap<String, u64>,
}

impl GCounter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment this replica's slot by `delta`.
    pub fn increment(&mut self, replica: impl Into<String>, delta: u64) {
        *self.slots.entry(replica.into()).or_insert(0) += delta;
    }

    /// Total value across all replicas.
    pub fn value(&self) -> u64 {
        self.slots.values().copied().sum()
    }

    pub fn slot(&self, replica: &str) -> u64 {
        self.slots.get(replica).copied().unwrap_or(0)
    }
}

impl Crdt for GCounter {
    fn merge(&mut self, other: &Self) {
        for (replica, &v) in &other.slots {
            let entry = self.slots.entry(replica.clone()).or_insert(0);
            if v > *entry {
                *entry = v;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_replica() {
        let mut c = GCounter::new();
        c.increment("a", 3);
        c.increment("a", 2);
        assert_eq!(c.value(), 5);
    }

    #[test]
    fn merge_is_per_slot_max() {
        let mut a = GCounter::new();
        let mut b = GCounter::new();
        a.increment("a", 5);
        b.increment("a", 3); // older view
        b.increment("b", 7);
        a.merge(&b);
        // a's slot a stays 5, b's slot becomes 7
        assert_eq!(a.slot("a"), 5);
        assert_eq!(a.slot("b"), 7);
        assert_eq!(a.value(), 12);
    }

    #[test]
    fn merge_is_commutative() {
        let mut a1 = GCounter::new();
        a1.increment("a", 5);
        a1.increment("b", 3);
        let mut b1 = GCounter::new();
        b1.increment("a", 7);
        b1.increment("c", 2);

        let mut left = a1.clone();
        left.merge(&b1);
        let mut right = b1.clone();
        right.merge(&a1);
        assert_eq!(left, right);
    }

    #[test]
    fn merge_is_associative() {
        let mut a = GCounter::new();
        a.increment("a", 1);
        let mut b = GCounter::new();
        b.increment("b", 2);
        let mut c = GCounter::new();
        c.increment("c", 4);

        let mut ab = a.clone();
        ab.merge(&b);
        ab.merge(&c);
        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);
        assert_eq!(ab, a_bc);
    }

    #[test]
    fn merge_is_idempotent() {
        let mut a = GCounter::new();
        a.increment("a", 5);
        let snap = a.clone();
        a.merge(&snap);
        assert_eq!(a, snap);
    }
}
