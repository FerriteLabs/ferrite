//! Resource monitoring for edge deployments.
//!
//! Tracks memory, CPU, disk, and network usage to enforce resource limits
//! and trigger adaptive behavior on constrained devices.

use serde::{Deserialize, Serialize};

/// Resource usage snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceSnapshot {
    /// Memory usage in bytes.
    pub memory_used: u64,
    /// Memory limit in bytes.
    pub memory_limit: u64,
    /// Disk usage in bytes.
    pub disk_used: u64,
    /// Disk limit in bytes.
    pub disk_limit: u64,
    /// Number of keys stored.
    pub key_count: u64,
    /// Key limit.
    pub key_limit: u64,
    /// CPU usage estimate (0.0 - 1.0).
    pub cpu_usage: f64,
}

impl ResourceSnapshot {
    /// Memory usage as a percentage.
    pub fn memory_percent(&self) -> f64 {
        if self.memory_limit == 0 {
            return 0.0;
        }
        (self.memory_used as f64 / self.memory_limit as f64) * 100.0
    }

    /// Disk usage as a percentage.
    pub fn disk_percent(&self) -> f64 {
        if self.disk_limit == 0 {
            return 0.0;
        }
        (self.disk_used as f64 / self.disk_limit as f64) * 100.0
    }

    /// Key usage as a percentage.
    pub fn key_percent(&self) -> f64 {
        if self.key_limit == 0 {
            return 0.0;
        }
        (self.key_count as f64 / self.key_limit as f64) * 100.0
    }

    /// Returns true if any resource is above the warning threshold (80%).
    pub fn is_warning(&self) -> bool {
        self.memory_percent() > 80.0 || self.disk_percent() > 80.0 || self.key_percent() > 80.0
    }

    /// Returns true if any resource is above the critical threshold (95%).
    pub fn is_critical(&self) -> bool {
        self.memory_percent() > 95.0 || self.disk_percent() > 95.0 || self.key_percent() > 95.0
    }
}

/// Recommended action based on resource state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceAction {
    /// No action needed.
    None,
    /// Start evicting least-used data.
    Evict,
    /// Trigger compaction to reclaim space.
    Compact,
    /// Compress data to save space.
    Compress,
    /// Sync and offload data to cloud.
    SyncAndOffload,
    /// Reject new writes (emergency).
    RejectWrites,
}

/// Determines the recommended action based on resource usage.
pub fn recommend_action(snapshot: &ResourceSnapshot) -> ResourceAction {
    if snapshot.memory_percent() > 95.0 || snapshot.disk_percent() > 95.0 {
        ResourceAction::RejectWrites
    } else if snapshot.memory_percent() > 90.0 {
        ResourceAction::Evict
    } else if snapshot.disk_percent() > 85.0 {
        ResourceAction::Compress
    } else if snapshot.memory_percent() > 80.0 {
        ResourceAction::SyncAndOffload
    } else if snapshot.disk_percent() > 70.0 {
        ResourceAction::Compact
    } else {
        ResourceAction::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_snapshot_percentages() {
        let snapshot = ResourceSnapshot {
            memory_used: 40 * 1024 * 1024,
            memory_limit: 50 * 1024 * 1024,
            disk_used: 250 * 1024 * 1024,
            disk_limit: 500 * 1024 * 1024,
            key_count: 50_000,
            key_limit: 100_000,
            cpu_usage: 0.3,
        };

        assert!((snapshot.memory_percent() - 80.0).abs() < f64::EPSILON);
        assert!((snapshot.disk_percent() - 50.0).abs() < f64::EPSILON);
        assert!((snapshot.key_percent() - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_warning_threshold() {
        let snapshot = ResourceSnapshot {
            memory_used: 42 * 1024 * 1024,
            memory_limit: 50 * 1024 * 1024, // 84%
            disk_used: 0,
            disk_limit: 100,
            key_count: 0,
            key_limit: 100,
            cpu_usage: 0.0,
        };
        assert!(snapshot.is_warning());
        assert!(!snapshot.is_critical());
    }

    #[test]
    fn test_critical_threshold() {
        let snapshot = ResourceSnapshot {
            memory_used: 48 * 1024 * 1024,
            memory_limit: 50 * 1024 * 1024, // 96%
            disk_used: 0,
            disk_limit: 100,
            key_count: 0,
            key_limit: 100,
            cpu_usage: 0.0,
        };
        assert!(snapshot.is_critical());
    }

    #[test]
    fn test_recommend_action() {
        // Normal
        let normal = ResourceSnapshot {
            memory_used: 25,
            memory_limit: 100,
            disk_used: 25,
            disk_limit: 100,
            key_count: 0,
            key_limit: 100,
            cpu_usage: 0.0,
        };
        assert_eq!(recommend_action(&normal), ResourceAction::None);

        // High memory
        let high_mem = ResourceSnapshot {
            memory_used: 92,
            memory_limit: 100,
            ..normal.clone()
        };
        assert_eq!(recommend_action(&high_mem), ResourceAction::Evict);

        // Critical
        let critical = ResourceSnapshot {
            memory_used: 96,
            memory_limit: 100,
            ..normal.clone()
        };
        assert_eq!(recommend_action(&critical), ResourceAction::RejectWrites);
    }
}
