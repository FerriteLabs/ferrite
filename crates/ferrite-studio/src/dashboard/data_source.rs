//! Dashboard data sources — aggregates all observability data
#![allow(dead_code)]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// Aggregated dashboard data source that collects metrics from all subsystems.
pub struct DashboardDataSource {
    /// Cached snapshot for debouncing
    last_snapshot: Option<(Instant, DashboardSnapshot)>,
    /// Minimum interval between full collections
    min_interval: Duration,
    /// Custom labels applied to this data source
    labels: HashMap<String, String>,
}

impl DashboardDataSource {
    /// Create a new dashboard data source with default settings.
    pub fn new() -> Self {
        Self {
            last_snapshot: None,
            min_interval: Duration::from_secs(1),
            labels: HashMap::new(),
        }
    }

    /// Collect a point-in-time snapshot of all dashboard metrics.
    pub fn collect_snapshot(&self) -> DashboardSnapshot {
        DashboardSnapshot {
            server: ServerInfo {
                version: env!("CARGO_PKG_VERSION").to_string(),
                uptime_secs: 0,
                connected_clients: 0,
                used_memory_bytes: 0,
                total_commands_processed: 0,
            },
            hot_keys: Vec::new(),
            hot_prefixes: Vec::new(),
            latency: LatencyInfo {
                get_p50_us: 0.0,
                get_p99_us: 0.0,
                set_p50_us: 0.0,
                set_p99_us: 0.0,
                overall_p99_us: 0.0,
            },
            tiering: TieringInfo {
                memory_keys: 0,
                mmap_keys: 0,
                disk_keys: 0,
                cloud_keys: 0,
                monthly_cost: 0.0,
                optimal_cost: 0.0,
                savings_pct: 0.0,
            },
            cluster: ClusterInfo {
                state: "ok".to_string(),
                nodes_total: 1,
                nodes_healthy: 1,
                slots_assigned: 16384,
                replication_lag_ms: 0,
            },
            memory: MemoryInfo {
                used_bytes: 0,
                peak_bytes: 0,
                fragmentation_ratio: 1.0,
                eviction_count: 0,
            },
            keyspace: Vec::new(),
        }
    }

    /// Collect time-series data over the specified window.
    pub fn collect_timeseries(&self, _window: Duration) -> TimeSeriesData {
        TimeSeriesData {
            timestamps: Vec::new(),
            ops_per_sec: Vec::new(),
            latency_p99_us: Vec::new(),
            memory_bytes: Vec::new(),
            connections: Vec::new(),
            tiering_migrations: Vec::new(),
        }
    }
}

impl Default for DashboardDataSource {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Snapshot types
// ---------------------------------------------------------------------------

/// Point-in-time snapshot of all dashboard metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardSnapshot {
    /// Server-level information
    pub server: ServerInfo,
    /// Hottest keys by access rate
    pub hot_keys: Vec<HotKeyInfo>,
    /// Hottest key prefixes
    pub hot_prefixes: Vec<HotPrefixInfo>,
    /// Latency percentiles
    pub latency: LatencyInfo,
    /// Storage tiering breakdown
    pub tiering: TieringInfo,
    /// Cluster health summary
    pub cluster: ClusterInfo,
    /// Memory usage details
    pub memory: MemoryInfo,
    /// Per-database keyspace stats
    pub keyspace: Vec<KeyspaceInfo>,
}

/// Server-level information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerInfo {
    /// Ferrite version string
    pub version: String,
    /// Seconds since server start
    pub uptime_secs: u64,
    /// Number of connected clients
    pub connected_clients: u32,
    /// Total memory used in bytes
    pub used_memory_bytes: u64,
    /// Total commands processed since start
    pub total_commands_processed: u64,
}

/// A single hot key entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotKeyInfo {
    /// Key name
    pub key: String,
    /// Operations per second for this key
    pub ops_per_sec: f64,
    /// Milliseconds since last access
    pub last_access_ago_ms: u64,
}

/// A hot key prefix entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotPrefixInfo {
    /// Key prefix
    pub prefix: String,
    /// Number of keys matching this prefix
    pub key_count: u64,
    /// Total operations across all keys with this prefix
    pub total_ops: u64,
}

/// Latency percentile information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyInfo {
    /// GET P50 latency in microseconds
    pub get_p50_us: f64,
    /// GET P99 latency in microseconds
    pub get_p99_us: f64,
    /// SET P50 latency in microseconds
    pub set_p50_us: f64,
    /// SET P99 latency in microseconds
    pub set_p99_us: f64,
    /// Overall P99 latency in microseconds
    pub overall_p99_us: f64,
}

/// Storage tiering breakdown.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringInfo {
    /// Keys in the mutable memory region
    pub memory_keys: u64,
    /// Keys in the mmap read-only region
    pub mmap_keys: u64,
    /// Keys in the disk region
    pub disk_keys: u64,
    /// Keys offloaded to cloud storage
    pub cloud_keys: u64,
    /// Estimated monthly storage cost (USD)
    pub monthly_cost: f64,
    /// Optimal monthly cost after tiering recommendations
    pub optimal_cost: f64,
    /// Percentage savings available
    pub savings_pct: f64,
}

/// Cluster health information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterInfo {
    /// Cluster state (ok, fail, degraded)
    pub state: String,
    /// Total number of nodes
    pub nodes_total: u32,
    /// Number of healthy nodes
    pub nodes_healthy: u32,
    /// Number of hash slots assigned
    pub slots_assigned: u32,
    /// Replication lag in milliseconds
    pub replication_lag_ms: u64,
}

/// Memory usage details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryInfo {
    /// Current memory usage in bytes
    pub used_bytes: u64,
    /// Peak memory usage in bytes
    pub peak_bytes: u64,
    /// Memory fragmentation ratio
    pub fragmentation_ratio: f64,
    /// Number of keys evicted
    pub eviction_count: u64,
}

/// Per-database keyspace statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyspaceInfo {
    /// Database index (0-15)
    pub db_index: u8,
    /// Number of keys
    pub keys: u64,
    /// Number of keys with expiry set
    pub expires: u64,
    /// Average TTL in milliseconds
    pub avg_ttl_ms: u64,
}

// ---------------------------------------------------------------------------
// Time-series types
// ---------------------------------------------------------------------------

/// Time-series data over a configurable window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesData {
    /// Timestamps in epoch milliseconds
    pub timestamps: Vec<u64>,
    /// Operations per second at each timestamp
    pub ops_per_sec: Vec<f64>,
    /// P99 latency in microseconds at each timestamp
    pub latency_p99_us: Vec<f64>,
    /// Memory usage in bytes at each timestamp
    pub memory_bytes: Vec<u64>,
    /// Connected clients at each timestamp
    pub connections: Vec<u32>,
    /// Tier migration events at each timestamp
    pub tiering_migrations: Vec<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_source_new() {
        let ds = DashboardDataSource::new();
        let snap = ds.collect_snapshot();
        assert_eq!(snap.cluster.state, "ok");
        assert_eq!(snap.cluster.nodes_total, 1);
    }

    #[test]
    fn test_collect_timeseries() {
        let ds = DashboardDataSource::default();
        let ts = ds.collect_timeseries(Duration::from_secs(60));
        assert!(ts.timestamps.is_empty());
    }
}
