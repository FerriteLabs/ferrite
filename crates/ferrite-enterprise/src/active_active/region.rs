#![forbid(unsafe_code)]
//! Region management for multi-region active-active replication.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Status of a region in the active-active cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegionStatus {
    /// Region is active and accepting reads/writes.
    Active,
    /// Region is syncing initial data.
    Syncing,
    /// Region is degraded (lagging behind).
    Degraded,
    /// Region is offline / unreachable.
    Offline,
    /// Region is joining the cluster.
    Joining,
}

impl std::fmt::Display for RegionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Syncing => write!(f, "syncing"),
            Self::Degraded => write!(f, "degraded"),
            Self::Offline => write!(f, "offline"),
            Self::Joining => write!(f, "joining"),
        }
    }
}

/// A region participating in active-active replication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Region {
    /// Unique identifier for this region.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Network endpoint (host:port).
    pub endpoint: String,
    /// Current status.
    pub status: RegionStatus,
    /// Current replication lag in milliseconds.
    pub replication_lag_ms: u64,
    /// Last time this region synced.
    pub last_sync: Option<DateTime<Utc>>,
    /// Number of keys synced to this region.
    pub keys_synced: u64,
    /// Number of conflicts resolved for this region.
    pub conflicts_resolved: u64,
}

impl Region {
    /// Create a new region in Joining status.
    pub fn new(id: String, name: String, endpoint: String) -> Self {
        Self {
            id,
            name,
            endpoint,
            status: RegionStatus::Joining,
            replication_lag_ms: 0,
            last_sync: None,
            keys_synced: 0,
            conflicts_resolved: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_region() {
        let r = Region::new(
            "us-east-1".to_string(),
            "US East".to_string(),
            "10.0.1.1:6379".to_string(),
        );
        assert_eq!(r.id, "us-east-1");
        assert_eq!(r.status, RegionStatus::Joining);
        assert_eq!(r.replication_lag_ms, 0);
        assert!(r.last_sync.is_none());
    }

    #[test]
    fn test_region_status_display() {
        assert_eq!(RegionStatus::Active.to_string(), "active");
        assert_eq!(RegionStatus::Offline.to_string(), "offline");
        assert_eq!(RegionStatus::Joining.to_string(), "joining");
    }
}
