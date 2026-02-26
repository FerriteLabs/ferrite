#![forbid(unsafe_code)]
//! Vector clock implementation for causal ordering across regions.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Ordering result when comparing two vector clocks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClockOrdering {
    /// This clock happened before the other.
    Before,
    /// This clock happened after the other.
    After,
    /// The clocks are concurrent (conflicting).
    Concurrent,
    /// The clocks are identical.
    Equal,
}

/// A vector clock tracking causal ordering across regions.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VectorClock {
    clocks: BTreeMap<String, u64>,
}

impl VectorClock {
    /// Create an empty vector clock.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment the counter for the given region.
    pub fn increment(&mut self, region_id: &str) {
        let counter = self.clocks.entry(region_id.to_string()).or_insert(0);
        *counter += 1;
    }

    /// Merge with another vector clock (point-wise max).
    pub fn merge(&mut self, other: &VectorClock) {
        for (region, &counter) in &other.clocks {
            let entry = self.clocks.entry(region.clone()).or_insert(0);
            *entry = (*entry).max(counter);
        }
    }

    /// Compare this clock with another.
    pub fn compare(&self, other: &VectorClock) -> ClockOrdering {
        let all_keys: std::collections::BTreeSet<&String> =
            self.clocks.keys().chain(other.clocks.keys()).collect();

        let mut self_lte = true; // all self[k] <= other[k]
        let mut other_lte = true; // all other[k] <= self[k]

        for key in all_keys {
            let s = self.clocks.get(key).copied().unwrap_or(0);
            let o = other.clocks.get(key).copied().unwrap_or(0);

            if s > o {
                self_lte = false;
            }
            if o > s {
                other_lte = false;
            }
        }

        match (self_lte, other_lte) {
            (true, true) => ClockOrdering::Equal,
            (true, false) => ClockOrdering::Before,
            (false, true) => ClockOrdering::After,
            (false, false) => ClockOrdering::Concurrent,
        }
    }

    /// Check if two clocks are concurrent (neither dominates).
    pub fn is_concurrent(&self, other: &VectorClock) -> bool {
        self.compare(other) == ClockOrdering::Concurrent
    }

    /// Get the counter value for a region.
    pub fn get(&self, region_id: &str) -> u64 {
        self.clocks.get(region_id).copied().unwrap_or(0)
    }

    /// Return the internal clock map.
    pub fn clocks(&self) -> &BTreeMap<String, u64> {
        &self.clocks
    }

    /// Alias for [`clocks`] — returns a reference to the internal map.
    pub fn to_map(&self) -> &BTreeMap<String, u64> {
        &self.clocks
    }

    /// Check if this clock strictly dominates another (happened after).
    pub fn dominates(&self, other: &VectorClock) -> bool {
        matches!(self.compare(other), ClockOrdering::After)
    }
}

/// Record of a detected conflict for audit / diagnostics.
#[derive(Debug, Clone)]
pub struct ConflictRecord {
    /// Key involved in the conflict.
    pub key: String,
    /// Region A identifier.
    pub region_a: String,
    /// Region B identifier.
    pub region_b: String,
    /// Vector clock from region A.
    pub clock_a: VectorClock,
    /// Vector clock from region B.
    pub clock_b: VectorClock,
    /// How the conflict was resolved.
    pub resolution: ConflictResolutionKind,
    /// When the conflict was recorded.
    pub timestamp: std::time::SystemTime,
}

/// The kind of resolution applied to a conflict.
#[derive(Debug, Clone)]
pub enum ConflictResolutionKind {
    /// Last writer wins based on timestamp.
    LastWriterWins,
    /// CRDT-aware merge.
    CrdtMerge,
    /// A specific region was given priority.
    RegionPriority(String),
    /// Deferred to manual resolution.
    Manual,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_clock_is_empty() {
        let vc = VectorClock::new();
        assert!(vc.clocks().is_empty());
    }

    #[test]
    fn test_increment() {
        let mut vc = VectorClock::new();
        vc.increment("us-east");
        assert_eq!(vc.get("us-east"), 1);
        vc.increment("us-east");
        assert_eq!(vc.get("us-east"), 2);
        vc.increment("eu-west");
        assert_eq!(vc.get("eu-west"), 1);
    }

    #[test]
    fn test_merge() {
        let mut a = VectorClock::new();
        a.increment("us-east");
        a.increment("us-east");

        let mut b = VectorClock::new();
        b.increment("eu-west");
        b.increment("eu-west");
        b.increment("eu-west");

        a.merge(&b);
        assert_eq!(a.get("us-east"), 2);
        assert_eq!(a.get("eu-west"), 3);
    }

    #[test]
    fn test_compare_equal() {
        let mut a = VectorClock::new();
        a.increment("r1");
        let b = a.clone();
        assert_eq!(a.compare(&b), ClockOrdering::Equal);
    }

    #[test]
    fn test_compare_before() {
        let mut a = VectorClock::new();
        a.increment("r1");

        let mut b = a.clone();
        b.increment("r1");

        assert_eq!(a.compare(&b), ClockOrdering::Before);
    }

    #[test]
    fn test_compare_after() {
        let mut a = VectorClock::new();
        a.increment("r1");

        let mut b = a.clone();
        b.increment("r1");

        assert_eq!(b.compare(&a), ClockOrdering::After);
    }

    #[test]
    fn test_compare_concurrent() {
        let mut a = VectorClock::new();
        a.increment("r1");

        let mut b = VectorClock::new();
        b.increment("r2");

        assert_eq!(a.compare(&b), ClockOrdering::Concurrent);
        assert!(a.is_concurrent(&b));
    }

    #[test]
    fn test_merge_preserves_max() {
        let mut a = VectorClock::new();
        a.increment("r1");
        a.increment("r1");
        a.increment("r1"); // r1=3

        let mut b = VectorClock::new();
        b.increment("r1"); // r1=1
        b.increment("r2");
        b.increment("r2"); // r2=2

        a.merge(&b);
        assert_eq!(a.get("r1"), 3); // kept higher
        assert_eq!(a.get("r2"), 2); // merged in
    }

    #[test]
    fn test_dominates() {
        let mut a = VectorClock::new();
        a.increment("r1");
        a.increment("r1");

        let mut b = VectorClock::new();
        b.increment("r1");

        assert!(a.dominates(&b));
        assert!(!b.dominates(&a));
    }

    #[test]
    fn test_to_map() {
        let mut vc = VectorClock::new();
        vc.increment("r1");
        vc.increment("r2");
        let map = vc.to_map();
        assert_eq!(map.len(), 2);
        assert_eq!(map["r1"], 1);
        assert_eq!(map["r2"], 1);
    }
}
