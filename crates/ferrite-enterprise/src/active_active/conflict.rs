#![forbid(unsafe_code)]
//! Conflict detection and resolution for multi-region active-active replication.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::vector_clock::VectorClock;

/// Strategy for resolving conflicting writes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConflictStrategy {
    /// Use timestamp to pick the most recent write.
    LastWriterWins,
    /// Deterministic tiebreaker using region ID ordering.
    HighestRegionId,
    /// Attempt automatic merge (for compatible types).
    Merge,
    /// Name of a custom resolver (future: WASM).
    Custom(String),
}

impl std::fmt::Display for ConflictStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LastWriterWins => write!(f, "lww"),
            Self::HighestRegionId => write!(f, "highest-region-id"),
            Self::Merge => write!(f, "merge"),
            Self::Custom(name) => write!(f, "custom:{name}"),
        }
    }
}

impl ConflictStrategy {
    /// Parse a strategy from a string.
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "lww" | "lastwriterwins" | "last-writer-wins" => Some(Self::LastWriterWins),
            "highest-region-id" | "highestregionid" => Some(Self::HighestRegionId),
            "merge" => Some(Self::Merge),
            other if other.starts_with("custom:") => Some(Self::Custom(
                other.trim_start_matches("custom:").to_string(),
            )),
            _ => None,
        }
    }
}

/// Which side won a conflict.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConflictWinner {
    Local,
    Remote,
    Merged,
}

/// A detected conflict between local and remote writes.
#[derive(Debug, Clone)]
pub struct Conflict {
    pub key: String,
    pub local_value: Vec<u8>,
    pub remote_value: Vec<u8>,
    pub local_clock: VectorClock,
    pub remote_clock: VectorClock,
    pub local_timestamp: DateTime<Utc>,
    pub remote_timestamp: DateTime<Utc>,
    pub resolution: Option<ConflictResolution>,
}

/// The result of resolving a conflict.
#[derive(Debug, Clone)]
pub struct ConflictResolution {
    pub strategy_used: ConflictStrategy,
    pub winner: ConflictWinner,
    pub resolved_value: Vec<u8>,
    pub resolved_at: DateTime<Utc>,
}

/// Resolves conflicts using the configured strategy.
#[derive(Debug)]
pub struct ConflictResolver;

impl ConflictResolver {
    /// Resolve a conflict using the given strategy.
    pub fn resolve(conflict: &Conflict, strategy: &ConflictStrategy) -> ConflictResolution {
        match strategy {
            ConflictStrategy::LastWriterWins => Self::resolve_lww(conflict),
            ConflictStrategy::HighestRegionId => Self::resolve_highest_region(conflict),
            ConflictStrategy::Merge => Self::resolve_merge(conflict),
            ConflictStrategy::Custom(_name) => {
                // Fall back to LWW for custom resolvers (WASM not yet implemented)
                Self::resolve_lww(conflict)
            }
        }
    }

    fn resolve_lww(conflict: &Conflict) -> ConflictResolution {
        if conflict.local_timestamp >= conflict.remote_timestamp {
            ConflictResolution {
                strategy_used: ConflictStrategy::LastWriterWins,
                winner: ConflictWinner::Local,
                resolved_value: conflict.local_value.clone(),
                resolved_at: Utc::now(),
            }
        } else {
            ConflictResolution {
                strategy_used: ConflictStrategy::LastWriterWins,
                winner: ConflictWinner::Remote,
                resolved_value: conflict.remote_value.clone(),
                resolved_at: Utc::now(),
            }
        }
    }

    fn resolve_highest_region(conflict: &Conflict) -> ConflictResolution {
        // Compare the highest region IDs present in each clock
        let local_max = conflict
            .local_clock
            .clocks()
            .keys()
            .max()
            .cloned()
            .unwrap_or_default();
        let remote_max = conflict
            .remote_clock
            .clocks()
            .keys()
            .max()
            .cloned()
            .unwrap_or_default();

        if local_max >= remote_max {
            ConflictResolution {
                strategy_used: ConflictStrategy::HighestRegionId,
                winner: ConflictWinner::Local,
                resolved_value: conflict.local_value.clone(),
                resolved_at: Utc::now(),
            }
        } else {
            ConflictResolution {
                strategy_used: ConflictStrategy::HighestRegionId,
                winner: ConflictWinner::Remote,
                resolved_value: conflict.remote_value.clone(),
                resolved_at: Utc::now(),
            }
        }
    }

    fn resolve_merge(conflict: &Conflict) -> ConflictResolution {
        // Simple merge: concatenate values with a separator.
        // In a full implementation, this would inspect value types for
        // CRDT-aware merging.
        let mut merged = conflict.local_value.clone();
        merged.push(b'|');
        merged.extend_from_slice(&conflict.remote_value);

        ConflictResolution {
            strategy_used: ConflictStrategy::Merge,
            winner: ConflictWinner::Merged,
            resolved_value: merged,
            resolved_at: Utc::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_conflict(local_newer: bool) -> Conflict {
        let mut local_clock = VectorClock::new();
        local_clock.increment("us-east");
        let mut remote_clock = VectorClock::new();
        remote_clock.increment("eu-west");

        let (lt, rt) = if local_newer {
            (Utc::now(), Utc::now() - chrono::Duration::seconds(10))
        } else {
            (Utc::now() - chrono::Duration::seconds(10), Utc::now())
        };

        Conflict {
            key: "test-key".to_string(),
            local_value: b"local-val".to_vec(),
            remote_value: b"remote-val".to_vec(),
            local_clock,
            remote_clock,
            local_timestamp: lt,
            remote_timestamp: rt,
            resolution: None,
        }
    }

    #[test]
    fn test_lww_local_wins() {
        let conflict = make_conflict(true);
        let res = ConflictResolver::resolve(&conflict, &ConflictStrategy::LastWriterWins);
        assert_eq!(res.winner, ConflictWinner::Local);
        assert_eq!(res.resolved_value, b"local-val");
    }

    #[test]
    fn test_lww_remote_wins() {
        let conflict = make_conflict(false);
        let res = ConflictResolver::resolve(&conflict, &ConflictStrategy::LastWriterWins);
        assert_eq!(res.winner, ConflictWinner::Remote);
        assert_eq!(res.resolved_value, b"remote-val");
    }

    #[test]
    fn test_highest_region_id() {
        let conflict = make_conflict(true);
        let res = ConflictResolver::resolve(&conflict, &ConflictStrategy::HighestRegionId);
        // "us-east" > "eu-west" lexicographically → local wins
        assert_eq!(res.winner, ConflictWinner::Local);
    }

    #[test]
    fn test_merge_strategy() {
        let conflict = make_conflict(true);
        let res = ConflictResolver::resolve(&conflict, &ConflictStrategy::Merge);
        assert_eq!(res.winner, ConflictWinner::Merged);
        assert!(res.resolved_value.contains(&b'|'));
    }

    #[test]
    fn test_strategy_display() {
        assert_eq!(ConflictStrategy::LastWriterWins.to_string(), "lww");
        assert_eq!(ConflictStrategy::Merge.to_string(), "merge");
    }

    #[test]
    fn test_strategy_parse() {
        assert_eq!(
            ConflictStrategy::from_str_loose("lww"),
            Some(ConflictStrategy::LastWriterWins)
        );
        assert_eq!(
            ConflictStrategy::from_str_loose("merge"),
            Some(ConflictStrategy::Merge)
        );
        assert_eq!(ConflictStrategy::from_str_loose("unknown"), None);
    }
}
