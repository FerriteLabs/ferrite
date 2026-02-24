//! Hybrid Logical Clock (HLC) for causal ordering
//!
//! Implements the HLC algorithm from Kulkarni et al. for providing
//! causally consistent timestamps across distributed nodes without
//! tight clock synchronization.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// A hybrid logical clock timestamp combining physical wall clock
/// time with a logical counter and node identifier for total ordering.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct HlcTimestamp {
    /// Physical time in milliseconds since epoch
    pub physical: u64,
    /// Logical counter for events at the same physical time
    pub logical: u32,
    /// Node identifier for tie-breaking
    pub node_id: u16,
}

impl HlcTimestamp {
    /// Create a new HLC timestamp
    pub fn new(physical: u64, logical: u32, node_id: u16) -> Self {
        Self {
            physical,
            logical,
            node_id,
        }
    }

    /// Returns true if this timestamp is causally before `other`
    pub fn is_before(&self, other: &HlcTimestamp) -> bool {
        self < other
    }

    /// Returns milliseconds elapsed since this timestamp was created
    pub fn elapsed_ms(&self) -> u64 {
        wall_clock_ms().saturating_sub(self.physical)
    }
}

impl Ord for HlcTimestamp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.physical
            .cmp(&other.physical)
            .then(self.logical.cmp(&other.logical))
            .then(self.node_id.cmp(&other.node_id))
    }
}

impl PartialOrd for HlcTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Returns the current wall clock time in milliseconds since epoch
fn wall_clock_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis() as u64
}

/// Hybrid Logical Clock for generating causally ordered timestamps.
///
/// Each node maintains its own `HybridClock` instance. Timestamps
/// generated are guaranteed to be monotonically increasing and
/// respect causal ordering when `update` is called on message receipt.
pub struct HybridClock {
    /// Current physical component
    physical: AtomicU64,
    /// Current logical component (stored as u64 for atomics, used as u32)
    logical: AtomicU64,
    /// Node identifier for tie-breaking
    node_id: u16,
}

impl HybridClock {
    /// Create a new hybrid clock for the given node
    pub fn new(node_id: u16) -> Self {
        Self {
            physical: AtomicU64::new(wall_clock_ms()),
            logical: AtomicU64::new(0),
            node_id,
        }
    }

    /// Generate a new timestamp, advancing the clock.
    ///
    /// Algorithm:
    /// - If wall clock > current physical: adopt wall clock, reset logical
    /// - Otherwise: increment logical counter
    pub fn now(&self) -> HlcTimestamp {
        let wall = wall_clock_ms();
        let old_physical = self.physical.load(Ordering::SeqCst);

        if wall > old_physical {
            self.physical.store(wall, Ordering::SeqCst);
            self.logical.store(0, Ordering::SeqCst);
            HlcTimestamp::new(wall, 0, self.node_id)
        } else {
            let new_logical = self.logical.fetch_add(1, Ordering::SeqCst) + 1;
            HlcTimestamp::new(old_physical, new_logical as u32, self.node_id)
        }
    }

    /// Update the clock upon receiving a remote timestamp.
    ///
    /// Merges the received timestamp with the local clock to maintain
    /// causal ordering: the result is always greater than both the
    /// local state and the received timestamp.
    pub fn update(&self, received: &HlcTimestamp) -> HlcTimestamp {
        let wall = wall_clock_ms();
        let old_physical = self.physical.load(Ordering::SeqCst);

        let new_physical = wall.max(old_physical).max(received.physical);
        self.physical.store(new_physical, Ordering::SeqCst);

        let new_logical = if new_physical == old_physical && new_physical == received.physical {
            // All three are equal — take max logical + 1
            let old_logical = self.logical.load(Ordering::SeqCst) as u32;
            old_logical.max(received.logical) + 1
        } else if new_physical == old_physical {
            // Local physical won — increment local logical
            self.logical.load(Ordering::SeqCst) as u32 + 1
        } else if new_physical == received.physical {
            // Received physical won — increment received logical
            received.logical + 1
        } else {
            // Wall clock won — reset
            0
        };

        self.logical.store(new_logical as u64, Ordering::SeqCst);
        HlcTimestamp::new(new_physical, new_logical, self.node_id)
    }

    /// Read the current timestamp without advancing the clock
    pub fn current(&self) -> HlcTimestamp {
        HlcTimestamp::new(
            self.physical.load(Ordering::SeqCst),
            self.logical.load(Ordering::SeqCst) as u32,
            self.node_id,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_ordering() {
        let a = HlcTimestamp::new(100, 0, 1);
        let b = HlcTimestamp::new(100, 1, 1);
        let c = HlcTimestamp::new(101, 0, 1);

        assert!(a < b);
        assert!(b < c);
        assert!(a.is_before(&b));
        assert!(b.is_before(&c));
    }

    #[test]
    fn test_timestamp_node_id_tiebreak() {
        let a = HlcTimestamp::new(100, 0, 1);
        let b = HlcTimestamp::new(100, 0, 2);
        assert!(a < b);
    }

    #[test]
    fn test_clock_now_monotonic() {
        let clock = HybridClock::new(1);
        let t1 = clock.now();
        let t2 = clock.now();
        let t3 = clock.now();

        assert!(t1 < t2);
        assert!(t2 < t3);
    }

    #[test]
    fn test_clock_update_advances() {
        let clock = HybridClock::new(1);
        let local = clock.now();

        // Simulate a remote timestamp far in the future
        let remote = HlcTimestamp::new(local.physical + 10_000, 5, 2);
        let updated = clock.update(&remote);

        assert!(updated.physical >= remote.physical);
        assert!(updated > remote);
        assert!(updated > local);
    }

    #[test]
    fn test_clock_update_same_physical() {
        let clock = HybridClock::new(1);
        let local = clock.now();

        let remote = HlcTimestamp::new(local.physical, local.logical + 10, 2);
        let updated = clock.update(&remote);

        assert!(updated > local);
        assert!(updated > remote);
    }

    #[test]
    fn test_current_does_not_advance() {
        let clock = HybridClock::new(1);
        let t1 = clock.current();
        let t2 = clock.current();

        assert_eq!(t1, t2);
    }

    #[test]
    fn test_elapsed_ms() {
        let ts = HlcTimestamp::new(wall_clock_ms().saturating_sub(100), 0, 1);
        assert!(ts.elapsed_ms() >= 100);
    }

    #[test]
    fn test_timestamp_default() {
        let ts = HlcTimestamp::default();
        assert_eq!(ts.physical, 0);
        assert_eq!(ts.logical, 0);
        assert_eq!(ts.node_id, 0);
    }
}
