//! CRDT Counter implementations

use super::types::SiteId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Grow-only counter (G-Counter)
///
/// A counter that can only be incremented. Each site maintains its own
/// count, and the total value is the sum of all site counts.
///
/// # Example
///
/// ```
/// use ferrite::crdt::{GCounter, SiteId};
///
/// let site1 = SiteId::new(1);
/// let site2 = SiteId::new(2);
///
/// let mut counter1 = GCounter::new();
/// counter1.increment(site1, 5);
///
/// let mut counter2 = GCounter::new();
/// counter2.increment(site2, 3);
///
/// counter1.merge(&counter2);
/// assert_eq!(counter1.value(), 8);
/// ```
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GCounter {
    /// Per-site increment counts
    counts: HashMap<SiteId, u64>,
}

impl GCounter {
    /// Create a new grow-only counter
    pub fn new() -> Self {
        Self {
            counts: HashMap::new(),
        }
    }

    /// Increment the counter for a specific site
    pub fn increment(&mut self, site: SiteId, delta: u64) {
        *self.counts.entry(site).or_insert(0) += delta;
    }

    /// Get the total counter value
    pub fn value(&self) -> u64 {
        self.counts.values().sum()
    }

    /// Get the count for a specific site
    pub fn site_count(&self, site: SiteId) -> u64 {
        self.counts.get(&site).copied().unwrap_or(0)
    }

    /// Merge another counter into this one
    pub fn merge(&mut self, other: &GCounter) {
        for (&site, &count) in &other.counts {
            let entry = self.counts.entry(site).or_insert(0);
            *entry = (*entry).max(count);
        }
    }

    /// Get all sites in this counter
    pub fn sites(&self) -> impl Iterator<Item = SiteId> + '_ {
        self.counts.keys().copied()
    }

    /// Get the number of sites
    pub fn num_sites(&self) -> usize {
        self.counts.len()
    }
}

impl PartialEq for GCounter {
    fn eq(&self, other: &Self) -> bool {
        self.counts == other.counts
    }
}

impl Eq for GCounter {}

/// Positive-Negative counter (PN-Counter)
///
/// A counter that supports both increment and decrement operations.
/// Implemented as two G-Counters: one for positive and one for negative values.
///
/// # Example
///
/// ```
/// use ferrite::crdt::{PNCounter, SiteId};
///
/// let site = SiteId::new(1);
///
/// let mut counter = PNCounter::new();
/// counter.increment(site, 10);
/// counter.decrement(site, 3);
///
/// assert_eq!(counter.value(), 7);
/// ```
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PNCounter {
    /// Counter for positive values
    positive: GCounter,
    /// Counter for negative values
    negative: GCounter,
}

impl PNCounter {
    /// Create a new PN-Counter
    pub fn new() -> Self {
        Self {
            positive: GCounter::new(),
            negative: GCounter::new(),
        }
    }

    /// Increment the counter
    pub fn increment(&mut self, site: SiteId, delta: u64) {
        self.positive.increment(site, delta);
    }

    /// Decrement the counter
    pub fn decrement(&mut self, site: SiteId, delta: u64) {
        self.negative.increment(site, delta);
    }

    /// Apply a signed delta (positive increments, negative decrements)
    pub fn apply(&mut self, site: SiteId, delta: i64) {
        if delta >= 0 {
            self.positive.increment(site, delta as u64);
        } else {
            self.negative.increment(site, (-delta) as u64);
        }
    }

    /// Get the counter value
    pub fn value(&self) -> i64 {
        self.positive.value() as i64 - self.negative.value() as i64
    }

    /// Get the positive count
    pub fn positive_value(&self) -> u64 {
        self.positive.value()
    }

    /// Get the negative count
    pub fn negative_value(&self) -> u64 {
        self.negative.value()
    }

    /// Merge another counter into this one
    pub fn merge(&mut self, other: &PNCounter) {
        self.positive.merge(&other.positive);
        self.negative.merge(&other.negative);
    }

    /// Get all sites in this counter
    pub fn sites(&self) -> impl Iterator<Item = SiteId> + '_ {
        self.positive.sites().chain(self.negative.sites())
    }
}

impl PartialEq for PNCounter {
    fn eq(&self, other: &Self) -> bool {
        self.positive == other.positive && self.negative == other.negative
    }
}

impl Eq for PNCounter {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gcounter_basic() {
        let site = SiteId::new(1);
        let mut counter = GCounter::new();

        assert_eq!(counter.value(), 0);
        counter.increment(site, 5);
        assert_eq!(counter.value(), 5);
        counter.increment(site, 3);
        assert_eq!(counter.value(), 8);
    }

    #[test]
    fn test_gcounter_multi_site() {
        let site1 = SiteId::new(1);
        let site2 = SiteId::new(2);

        let mut counter = GCounter::new();
        counter.increment(site1, 5);
        counter.increment(site2, 3);

        assert_eq!(counter.value(), 8);
        assert_eq!(counter.site_count(site1), 5);
        assert_eq!(counter.site_count(site2), 3);
    }

    #[test]
    fn test_gcounter_merge() {
        let site1 = SiteId::new(1);
        let site2 = SiteId::new(2);

        let mut counter1 = GCounter::new();
        counter1.increment(site1, 5);

        let mut counter2 = GCounter::new();
        counter2.increment(site2, 3);

        counter1.merge(&counter2);
        assert_eq!(counter1.value(), 8);

        // Merging again should not change anything (idempotent)
        counter1.merge(&counter2);
        assert_eq!(counter1.value(), 8);
    }

    #[test]
    fn test_gcounter_merge_same_site() {
        let site = SiteId::new(1);

        let mut counter1 = GCounter::new();
        counter1.increment(site, 5);

        let mut counter2 = GCounter::new();
        counter2.increment(site, 3);

        counter1.merge(&counter2);
        // Should take max of the two
        assert_eq!(counter1.value(), 5);
    }

    #[test]
    fn test_pncounter_basic() {
        let site = SiteId::new(1);
        let mut counter = PNCounter::new();

        counter.increment(site, 10);
        assert_eq!(counter.value(), 10);

        counter.decrement(site, 3);
        assert_eq!(counter.value(), 7);
    }

    #[test]
    fn test_pncounter_negative() {
        let site = SiteId::new(1);
        let mut counter = PNCounter::new();

        counter.decrement(site, 5);
        assert_eq!(counter.value(), -5);
    }

    #[test]
    fn test_pncounter_apply() {
        let site = SiteId::new(1);
        let mut counter = PNCounter::new();

        counter.apply(site, 10);
        assert_eq!(counter.value(), 10);

        counter.apply(site, -3);
        assert_eq!(counter.value(), 7);
    }

    #[test]
    fn test_pncounter_merge() {
        let site1 = SiteId::new(1);
        let site2 = SiteId::new(2);

        let mut counter1 = PNCounter::new();
        counter1.increment(site1, 5);

        let mut counter2 = PNCounter::new();
        counter2.increment(site2, 3);
        counter2.decrement(site2, 1);

        counter1.merge(&counter2);
        assert_eq!(counter1.value(), 7); // 5 + 3 - 1

        // Verify convergence
        counter2.merge(&counter1);
        assert_eq!(counter2.value(), counter1.value());
    }

    #[test]
    fn test_pncounter_concurrent_increments() {
        let site1 = SiteId::new(1);
        let site2 = SiteId::new(2);

        // Both sites increment concurrently
        let mut counter1 = PNCounter::new();
        counter1.increment(site1, 5);

        let mut counter2 = PNCounter::new();
        counter2.increment(site2, 3);

        // Merge in both directions
        counter1.merge(&counter2);
        counter2.merge(&counter1);

        // Both should converge to the same value
        assert_eq!(counter1.value(), 8);
        assert_eq!(counter2.value(), 8);
    }
}
