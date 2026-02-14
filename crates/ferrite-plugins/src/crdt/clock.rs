//! Clock types for CRDT ordering and causality

use super::types::SiteId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Vector clock for causality tracking
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorClock {
    /// Per-site logical clocks
    clocks: HashMap<SiteId, u64>,
}

impl VectorClock {
    /// Create a new empty vector clock
    pub fn new() -> Self {
        Self {
            clocks: HashMap::new(),
        }
    }

    /// Increment the clock for a specific site
    pub fn increment(&mut self, site: SiteId) {
        *self.clocks.entry(site).or_insert(0) += 1;
    }

    /// Get the clock value for a specific site
    pub fn get(&self, site: SiteId) -> u64 {
        self.clocks.get(&site).copied().unwrap_or(0)
    }

    /// Set the clock value for a specific site
    pub fn set(&mut self, site: SiteId, value: u64) {
        self.clocks.insert(site, value);
    }

    /// Merge another vector clock into this one
    pub fn merge(&mut self, other: &VectorClock) {
        for (&site, &clock) in &other.clocks {
            let entry = self.clocks.entry(site).or_insert(0);
            *entry = (*entry).max(clock);
        }
    }

    /// Check if this clock happened-before another
    pub fn happened_before(&self, other: &VectorClock) -> bool {
        // All our clocks must be <= other's clocks
        let all_le = self
            .clocks
            .iter()
            .all(|(site, &clock)| other.clocks.get(site).copied().unwrap_or(0) >= clock);

        // And at least one must be strictly less (or we're missing sites they have)
        let at_least_one_lt = self
            .clocks
            .iter()
            .any(|(site, &clock)| other.clocks.get(site).copied().unwrap_or(0) > clock)
            || other
                .clocks
                .keys()
                .any(|site| !self.clocks.contains_key(site));

        all_le && at_least_one_lt
    }

    /// Check if two clocks are concurrent (neither happened-before the other)
    pub fn concurrent(&self, other: &VectorClock) -> bool {
        !self.happened_before(other) && !other.happened_before(self) && self != other
    }

    /// Get all sites in this clock
    pub fn sites(&self) -> impl Iterator<Item = SiteId> + '_ {
        self.clocks.keys().copied()
    }

    /// Get the number of sites
    pub fn len(&self) -> usize {
        self.clocks.len()
    }

    /// Check if the clock is empty
    pub fn is_empty(&self) -> bool {
        self.clocks.is_empty()
    }
}

/// Hybrid Logical Clock timestamp
///
/// Combines physical time with a logical counter for total ordering.
/// Uses site ID for tie-breaking.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HybridTimestamp {
    /// Physical time (wall clock) in microseconds
    pub physical: u64,
    /// Logical counter for same physical time
    pub logical: u32,
    /// Site ID for tie-breaking
    pub site: SiteId,
}

impl HybridTimestamp {
    /// Create a new timestamp
    pub fn new(physical: u64, logical: u32, site: SiteId) -> Self {
        Self {
            physical,
            logical,
            site,
        }
    }

    /// Create a timestamp from current system time
    pub fn now(site: SiteId) -> Self {
        let physical = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        Self {
            physical,
            logical: 0,
            site,
        }
    }

    /// Create a zero timestamp
    pub fn zero(site: SiteId) -> Self {
        Self {
            physical: 0,
            logical: 0,
            site,
        }
    }
}

impl PartialOrd for HybridTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HybridTimestamp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.physical.cmp(&other.physical) {
            std::cmp::Ordering::Equal => match self.logical.cmp(&other.logical) {
                std::cmp::Ordering::Equal => self.site.cmp(&other.site),
                ord => ord,
            },
            ord => ord,
        }
    }
}

impl std::fmt::Debug for HybridTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HT({}.{}.{})", self.physical, self.logical, self.site.0)
    }
}

impl Default for HybridTimestamp {
    fn default() -> Self {
        Self {
            physical: 0,
            logical: 0,
            site: SiteId(0),
        }
    }
}

/// Hybrid Logical Clock
///
/// Tracks both physical and logical time, ensuring timestamps
/// are monotonically increasing and can be merged across sites.
#[derive(Clone, Debug)]
pub struct HybridClock {
    /// Local site ID
    site: SiteId,
    /// Last timestamp issued
    last: HybridTimestamp,
}

impl HybridClock {
    /// Create a new hybrid clock for a site
    pub fn new(site: SiteId) -> Self {
        Self {
            site,
            last: HybridTimestamp::zero(site),
        }
    }

    /// Get the current timestamp (advancing the clock)
    pub fn now(&mut self) -> HybridTimestamp {
        let physical = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        if physical > self.last.physical {
            // Physical time advanced
            self.last = HybridTimestamp::new(physical, 0, self.site);
        } else {
            // Same or earlier physical time, advance logical
            self.last = HybridTimestamp::new(
                self.last.physical,
                self.last.logical.saturating_add(1),
                self.site,
            );
        }

        self.last
    }

    /// Update clock based on a received timestamp
    pub fn receive(&mut self, received: HybridTimestamp) -> HybridTimestamp {
        let physical = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        if physical > self.last.physical && physical > received.physical {
            // Our physical time is ahead
            self.last = HybridTimestamp::new(physical, 0, self.site);
        } else if self.last.physical > received.physical {
            // Our logical time is ahead
            self.last = HybridTimestamp::new(
                self.last.physical,
                self.last.logical.saturating_add(1),
                self.site,
            );
        } else if received.physical > self.last.physical {
            // Received time is ahead
            self.last = HybridTimestamp::new(received.physical, received.logical + 1, self.site);
        } else {
            // Same physical time
            let logical = self.last.logical.max(received.logical) + 1;
            self.last = HybridTimestamp::new(self.last.physical, logical, self.site);
        }

        self.last
    }

    /// Get the site ID
    pub fn site(&self) -> SiteId {
        self.site
    }

    /// Get the last timestamp (without advancing)
    pub fn last(&self) -> HybridTimestamp {
        self.last
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_clock_increment() {
        let mut vc = VectorClock::new();
        let site = SiteId(1);

        assert_eq!(vc.get(site), 0);
        vc.increment(site);
        assert_eq!(vc.get(site), 1);
        vc.increment(site);
        assert_eq!(vc.get(site), 2);
    }

    #[test]
    fn test_vector_clock_merge() {
        let mut vc1 = VectorClock::new();
        let mut vc2 = VectorClock::new();
        let site1 = SiteId(1);
        let site2 = SiteId(2);

        vc1.increment(site1);
        vc1.increment(site1);
        vc2.increment(site2);

        vc1.merge(&vc2);
        assert_eq!(vc1.get(site1), 2);
        assert_eq!(vc1.get(site2), 1);
    }

    #[test]
    fn test_vector_clock_happened_before() {
        let mut vc1 = VectorClock::new();
        let mut vc2 = VectorClock::new();
        let site = SiteId(1);

        vc1.increment(site);
        vc2.increment(site);
        vc2.increment(site);

        assert!(vc1.happened_before(&vc2));
        assert!(!vc2.happened_before(&vc1));
    }

    #[test]
    fn test_vector_clock_concurrent() {
        let mut vc1 = VectorClock::new();
        let mut vc2 = VectorClock::new();
        let site1 = SiteId(1);
        let site2 = SiteId(2);

        vc1.increment(site1);
        vc2.increment(site2);

        assert!(vc1.concurrent(&vc2));
        assert!(vc2.concurrent(&vc1));
    }

    #[test]
    fn test_hybrid_timestamp_ordering() {
        let site1 = SiteId(1);
        let site2 = SiteId(2);

        let ts1 = HybridTimestamp::new(100, 0, site1);
        let ts2 = HybridTimestamp::new(100, 1, site1);
        let ts3 = HybridTimestamp::new(100, 1, site2);
        let ts4 = HybridTimestamp::new(101, 0, site1);

        assert!(ts1 < ts2);
        assert!(ts2 < ts3); // Same physical and logical, site2 > site1
        assert!(ts3 < ts4);
    }

    #[test]
    fn test_hybrid_clock() {
        let site = SiteId(1);
        let mut clock = HybridClock::new(site);

        let ts1 = clock.now();
        let ts2 = clock.now();

        assert!(ts1 < ts2);
    }

    #[test]
    fn test_hybrid_clock_receive() {
        let site1 = SiteId(1);
        let site2 = SiteId(2);
        let mut clock1 = HybridClock::new(site1);

        // Receive a timestamp from the future
        let future_ts = HybridTimestamp::new(u64::MAX / 2, 5, site2);
        let received = clock1.receive(future_ts);

        assert!(received > future_ts);
    }
}
