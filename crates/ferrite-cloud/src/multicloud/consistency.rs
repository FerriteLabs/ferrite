//! Consistency levels for geo-distributed reads
//!
//! Provides bounded-staleness and session consistency checks
//! to allow applications to choose their consistency/latency tradeoff.

use std::collections::HashMap;
use std::time::Duration;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use super::hlc::HlcTimestamp;

/// Consistency level for read operations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConsistencyLevel {
    /// Wait for all replicas to acknowledge
    Strong,
    /// Allow reads up to `max_staleness` behind the latest write
    BoundedStaleness {
        /// Maximum acceptable staleness
        max_staleness: Duration,
    },
    /// Read-your-writes within a session
    SessionConsistency,
    /// Reads never see out-of-order writes
    ConsistentPrefix,
    /// No guarantees on recency
    Eventual,
}

/// Result of a consistency check
#[derive(Debug, Clone, PartialEq)]
pub enum ConsistencyResult {
    /// Read is allowed at the requested consistency level
    Allowed,
    /// Read is stale by the given duration
    StaleBy(Duration),
    /// Consistency requirement cannot be satisfied
    NotSatisfied(String),
}

/// Tracks per-region and per-session timestamps to evaluate
/// whether a read satisfies the requested consistency level.
pub struct ConsistencyChecker {
    /// Latest known timestamp per region
    region_timestamps: DashMap<String, HlcTimestamp>,
    /// Per-session vector of last write timestamps by region
    session_vectors: DashMap<String, HashMap<String, HlcTimestamp>>,
}

impl ConsistencyChecker {
    /// Create a new consistency checker
    pub fn new() -> Self {
        Self {
            region_timestamps: DashMap::new(),
            session_vectors: DashMap::new(),
        }
    }

    /// Update the latest known timestamp for a region
    pub fn update_region_timestamp(&self, region: &str, ts: HlcTimestamp) {
        self.region_timestamps
            .entry(region.to_string())
            .and_modify(|existing| {
                if ts > *existing {
                    *existing = ts;
                }
            })
            .or_insert(ts);
    }

    /// Record a write in a session for read-your-writes tracking
    pub fn record_session_write(&self, session_id: &str, region: &str, ts: HlcTimestamp) {
        self.session_vectors
            .entry(session_id.to_string())
            .and_modify(|map| {
                map.entry(region.to_string())
                    .and_modify(|existing| {
                        if ts > *existing {
                            *existing = ts;
                        }
                    })
                    .or_insert(ts);
            })
            .or_insert_with(|| {
                let mut map = HashMap::new();
                map.insert(region.to_string(), ts);
                map
            });
    }

    /// Check whether a read at the given consistency level can proceed
    pub fn can_read(
        &self,
        level: &ConsistencyLevel,
        read_region: &str,
        session_id: Option<&str>,
    ) -> ConsistencyResult {
        match level {
            ConsistencyLevel::Eventual => ConsistencyResult::Allowed,

            ConsistencyLevel::Strong => {
                // Strong: the read region must have the globally latest timestamp
                let region_ts = self.region_timestamps.get(read_region);
                let max_ts = self.max_global_timestamp();

                match (region_ts, max_ts) {
                    (Some(local), Some(global)) if *local >= global => ConsistencyResult::Allowed,
                    (Some(local), Some(global)) => {
                        let staleness_ms = global.physical.saturating_sub(local.physical);
                        ConsistencyResult::StaleBy(Duration::from_millis(staleness_ms))
                    }
                    (None, Some(_)) => ConsistencyResult::NotSatisfied(format!(
                        "no timestamp for region '{read_region}'"
                    )),
                    _ => ConsistencyResult::Allowed,
                }
            }

            ConsistencyLevel::BoundedStaleness { max_staleness } => {
                let region_ts = self.region_timestamps.get(read_region);
                let max_ts = self.max_global_timestamp();

                match (region_ts, max_ts) {
                    (Some(local), Some(global)) => {
                        let staleness_ms = global.physical.saturating_sub(local.physical);
                        let staleness = Duration::from_millis(staleness_ms);
                        if staleness <= *max_staleness {
                            ConsistencyResult::Allowed
                        } else {
                            ConsistencyResult::StaleBy(staleness)
                        }
                    }
                    (None, Some(_)) => ConsistencyResult::NotSatisfied(format!(
                        "no timestamp for region '{read_region}'"
                    )),
                    _ => ConsistencyResult::Allowed,
                }
            }

            ConsistencyLevel::SessionConsistency => {
                let session = match session_id {
                    Some(id) => id,
                    None => {
                        return ConsistencyResult::NotSatisfied(
                            "session id required for session consistency".to_string(),
                        );
                    }
                };

                let session_vec = self.session_vectors.get(session);
                let region_ts = self.region_timestamps.get(read_region);

                match (session_vec, region_ts) {
                    (Some(writes), Some(local_ts)) => {
                        // Check that the read region has caught up to all writes
                        // the session has done in any region
                        for (_region, write_ts) in writes.iter() {
                            if *write_ts > *local_ts {
                                let staleness = write_ts.physical.saturating_sub(local_ts.physical);
                                return ConsistencyResult::StaleBy(Duration::from_millis(
                                    staleness,
                                ));
                            }
                        }
                        ConsistencyResult::Allowed
                    }
                    (None, _) => {
                        // No writes in this session — any read is fine
                        ConsistencyResult::Allowed
                    }
                    (Some(_), None) => ConsistencyResult::NotSatisfied(format!(
                        "no timestamp for region '{read_region}'"
                    )),
                }
            }

            ConsistencyLevel::ConsistentPrefix => {
                // Consistent prefix is satisfied as long as the region has
                // some timestamp (i.e., it has received at least one update).
                if self.region_timestamps.contains_key(read_region) {
                    ConsistencyResult::Allowed
                } else {
                    ConsistencyResult::NotSatisfied(format!(
                        "no timestamp for region '{read_region}'"
                    ))
                }
            }
        }
    }

    /// Find the maximum timestamp across all regions
    fn max_global_timestamp(&self) -> Option<HlcTimestamp> {
        self.region_timestamps
            .iter()
            .map(|entry| *entry.value())
            .max()
    }
}

impl Default for ConsistencyChecker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eventual_always_allowed() {
        let checker = ConsistencyChecker::new();
        let result = checker.can_read(&ConsistencyLevel::Eventual, "us-east-1", None);
        assert_eq!(result, ConsistencyResult::Allowed);
    }

    #[test]
    fn test_strong_up_to_date() {
        let checker = ConsistencyChecker::new();
        let ts = HlcTimestamp::new(1000, 0, 1);
        checker.update_region_timestamp("us-east-1", ts);

        let result = checker.can_read(&ConsistencyLevel::Strong, "us-east-1", None);
        assert_eq!(result, ConsistencyResult::Allowed);
    }

    #[test]
    fn test_strong_stale() {
        let checker = ConsistencyChecker::new();
        checker.update_region_timestamp("us-east-1", HlcTimestamp::new(1000, 0, 1));
        checker.update_region_timestamp("eu-west-1", HlcTimestamp::new(2000, 0, 2));

        let result = checker.can_read(&ConsistencyLevel::Strong, "us-east-1", None);
        assert!(matches!(result, ConsistencyResult::StaleBy(_)));
    }

    #[test]
    fn test_bounded_staleness_within_bound() {
        let checker = ConsistencyChecker::new();
        checker.update_region_timestamp("us-east-1", HlcTimestamp::new(900, 0, 1));
        checker.update_region_timestamp("eu-west-1", HlcTimestamp::new(1000, 0, 2));

        let level = ConsistencyLevel::BoundedStaleness {
            max_staleness: Duration::from_millis(200),
        };
        let result = checker.can_read(&level, "us-east-1", None);
        assert_eq!(result, ConsistencyResult::Allowed);
    }

    #[test]
    fn test_bounded_staleness_exceeded() {
        let checker = ConsistencyChecker::new();
        checker.update_region_timestamp("us-east-1", HlcTimestamp::new(500, 0, 1));
        checker.update_region_timestamp("eu-west-1", HlcTimestamp::new(1000, 0, 2));

        let level = ConsistencyLevel::BoundedStaleness {
            max_staleness: Duration::from_millis(200),
        };
        let result = checker.can_read(&level, "us-east-1", None);
        assert!(matches!(result, ConsistencyResult::StaleBy(_)));
    }

    #[test]
    fn test_session_consistency_satisfied() {
        let checker = ConsistencyChecker::new();
        let ts = HlcTimestamp::new(1000, 0, 1);

        checker.record_session_write("sess-1", "us-east-1", ts);
        checker.update_region_timestamp("eu-west-1", HlcTimestamp::new(1000, 0, 2));

        let result = checker.can_read(
            &ConsistencyLevel::SessionConsistency,
            "eu-west-1",
            Some("sess-1"),
        );
        assert_eq!(result, ConsistencyResult::Allowed);
    }

    #[test]
    fn test_session_consistency_not_caught_up() {
        let checker = ConsistencyChecker::new();
        checker.record_session_write("sess-1", "us-east-1", HlcTimestamp::new(2000, 0, 1));
        checker.update_region_timestamp("eu-west-1", HlcTimestamp::new(1000, 0, 2));

        let result = checker.can_read(
            &ConsistencyLevel::SessionConsistency,
            "eu-west-1",
            Some("sess-1"),
        );
        assert!(matches!(result, ConsistencyResult::StaleBy(_)));
    }

    #[test]
    fn test_session_consistency_requires_session_id() {
        let checker = ConsistencyChecker::new();
        let result = checker.can_read(&ConsistencyLevel::SessionConsistency, "us-east-1", None);
        assert!(matches!(result, ConsistencyResult::NotSatisfied(_)));
    }

    #[test]
    fn test_consistent_prefix() {
        let checker = ConsistencyChecker::new();

        // No timestamp yet — not satisfied
        let result = checker.can_read(&ConsistencyLevel::ConsistentPrefix, "us-east-1", None);
        assert!(matches!(result, ConsistencyResult::NotSatisfied(_)));

        // After update — satisfied
        checker.update_region_timestamp("us-east-1", HlcTimestamp::new(100, 0, 1));
        let result = checker.can_read(&ConsistencyLevel::ConsistentPrefix, "us-east-1", None);
        assert_eq!(result, ConsistencyResult::Allowed);
    }
}
