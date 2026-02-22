//! Multi-region synchronization

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

/// Sync configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
    /// Sync interval in milliseconds
    pub interval_ms: u64,
    /// Batch size for sync operations
    pub batch_size: usize,
    /// Enable compression
    pub compression: bool,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            interval_ms: 100,
            batch_size: 1000,
            compression: true,
        }
    }
}

/// Sync status
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct SyncStatus {
    /// Total items synced
    pub items_synced: u64,
    /// Items pending sync
    pub items_pending: u64,
    /// Last sync timestamp
    pub last_sync: u64,
    /// Sync errors
    pub errors: u64,
    /// Bytes transferred
    pub bytes_transferred: u64,
}


/// A sync item in the queue
#[derive(Debug, Clone)]
struct SyncItem {
    region: String,
    key: String,
    value: Vec<u8>,
    timestamp: u64,
}

/// Sync manager for multi-region replication
pub struct SyncManager {
    config: SyncConfig,
    queue: RwLock<VecDeque<SyncItem>>,
    status: RwLock<SyncStatus>,
    region_status: RwLock<HashMap<String, RegionSyncStatus>>,
}

/// Per-region sync status
#[derive(Debug, Clone, Default)]
struct RegionSyncStatus {
    last_sync: u64,
    items_synced: u64,
    errors: u64,
    #[allow(dead_code)] // Planned for v0.2 — stored for sync lag monitoring
    lag_ms: u64,
}

impl SyncManager {
    /// Create a new sync manager
    pub fn new(config: SyncConfig) -> Self {
        Self {
            config,
            queue: RwLock::new(VecDeque::new()),
            status: RwLock::new(SyncStatus::default()),
            region_status: RwLock::new(HashMap::new()),
        }
    }

    /// Queue an item for sync
    pub fn queue_sync(&self, region: &str, key: &str, value: &[u8], timestamp: u64) {
        let item = SyncItem {
            region: region.to_string(),
            key: key.to_string(),
            value: value.to_vec(),
            timestamp,
        };

        self.queue.write().push_back(item);
        self.status.write().items_pending += 1;
    }

    /// Get pending items for a region
    pub fn get_pending(&self, region: &str, limit: usize) -> Vec<(String, Vec<u8>, u64)> {
        let queue = self.queue.read();
        queue
            .iter()
            .filter(|item| item.region == region)
            .take(limit)
            .map(|item| (item.key.clone(), item.value.clone(), item.timestamp))
            .collect()
    }

    /// Mark items as synced
    pub fn mark_synced(&self, region: &str, count: u64, bytes: u64) {
        let now = current_timestamp();

        // Update global status
        {
            let mut status = self.status.write();
            status.items_synced += count;
            status.items_pending = status.items_pending.saturating_sub(count);
            status.last_sync = now;
            status.bytes_transferred += bytes;
        }

        // Update region status
        {
            let mut region_status = self.region_status.write();
            let entry = region_status.entry(region.to_string()).or_default();
            entry.last_sync = now;
            entry.items_synced += count;
        }

        // Remove synced items from queue
        let mut queue = self.queue.write();
        let mut removed = 0u64;
        queue.retain(|item| {
            if item.region == region && removed < count {
                removed += 1;
                false
            } else {
                true
            }
        });
    }

    /// Record a sync error
    pub fn record_error(&self, region: &str) {
        self.status.write().errors += 1;

        let mut region_status = self.region_status.write();
        let entry = region_status.entry(region.to_string()).or_default();
        entry.errors += 1;
    }

    /// Get current status
    pub fn status(&self) -> SyncStatus {
        self.status.read().clone()
    }

    /// Get queue size
    pub fn queue_size(&self) -> usize {
        self.queue.read().len()
    }

    /// Get config
    pub fn config(&self) -> &SyncConfig {
        &self.config
    }

    /// Get region lag in milliseconds
    pub fn get_region_lag(&self, region: &str) -> u64 {
        let region_status = self.region_status.read();
        region_status
            .get(region)
            .map(|s| {
                let now = current_timestamp();
                if s.last_sync > 0 {
                    now.saturating_sub(s.last_sync) * 1000
                } else {
                    0
                }
            })
            .unwrap_or(0)
    }

    /// Clear the sync queue
    pub fn clear(&self) {
        self.queue.write().clear();
        self.status.write().items_pending = 0;
    }
}

impl Default for SyncManager {
    fn default() -> Self {
        Self::new(SyncConfig::default())
    }
}

/// Get current timestamp in seconds
fn current_timestamp() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_queue() {
        let manager = SyncManager::default();

        manager.queue_sync("us-west-2", "key1", b"value1", 1000);
        manager.queue_sync("us-west-2", "key2", b"value2", 1001);
        manager.queue_sync("eu-west-1", "key3", b"value3", 1002);

        assert_eq!(manager.queue_size(), 3);

        let pending = manager.get_pending("us-west-2", 10);
        assert_eq!(pending.len(), 2);

        manager.mark_synced("us-west-2", 2, 12);
        assert_eq!(manager.queue_size(), 1);

        let status = manager.status();
        assert_eq!(status.items_synced, 2);
    }

    #[test]
    fn test_sync_status() {
        let manager = SyncManager::default();

        manager.queue_sync("region1", "key1", b"value", 1000);
        manager.mark_synced("region1", 1, 5);

        let status = manager.status();
        assert_eq!(status.items_synced, 1);
        assert_eq!(status.bytes_transferred, 5);
    }
}
