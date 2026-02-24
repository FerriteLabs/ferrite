//! Async Replication Slots
//!
//! PostgreSQL-style logical replication slots for external consumers.
//! Tracks consumer progress and allows resumable streaming.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                   Replication Slots                          │
//! ├─────────────────────────────────────────────────────────────┤
//! │  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐ │
//! │  │  Slot    │   │   WAL    │   │ Consumer │   │ Progress │ │
//! │  │ Manager  │──▶│  Reader  │──▶│ Stream   │──▶│ Tracker  │ │
//! │  └──────────┘   └──────────┘   └──────────┘   └──────────┘ │
//! │       │              │              │              │        │
//! │       ▼              ▼              ▼              ▼        │
//! │  Slot Registry   Log Replay    Async Stream   LSN Commit   │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

/// Log Sequence Number for tracking position
pub type LSN = u64;

/// Slot identifier
pub type SlotId = String;

/// Replication slot state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SlotState {
    /// Slot is created but not active
    #[default]
    Inactive,
    /// Slot is actively streaming
    Active,
    /// Slot is paused
    Paused,
    /// Slot is being cleaned up
    Dropping,
}

/// Type of replication
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ReplicationType {
    /// Physical replication (byte-level)
    Physical,
    /// Logical replication (decoded changes)
    #[default]
    Logical,
}

/// Slot configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotConfig {
    /// Slot name
    pub name: String,
    /// Replication type
    pub replication_type: ReplicationType,
    /// Plugin name for logical decoding
    pub plugin: String,
    /// Key patterns to replicate
    pub key_patterns: Vec<String>,
    /// Database number (for Redis compatibility)
    pub database: u8,
    /// Retain WAL even when inactive
    pub retain_wal: bool,
    /// Maximum lag before dropping slot (0 = no limit)
    pub max_lag_bytes: u64,
}

impl Default for SlotConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            replication_type: ReplicationType::Logical,
            plugin: "default".to_string(),
            key_patterns: vec!["*".to_string()],
            database: 0,
            retain_wal: true,
            max_lag_bytes: 0,
        }
    }
}

impl SlotConfig {
    /// Create a new slot config
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }

    /// Set key patterns
    pub fn with_patterns(mut self, patterns: Vec<String>) -> Self {
        self.key_patterns = patterns;
        self
    }

    /// Set plugin
    pub fn with_plugin(mut self, plugin: impl Into<String>) -> Self {
        self.plugin = plugin.into();
        self
    }

    /// Set database
    pub fn with_database(mut self, db: u8) -> Self {
        self.database = db;
        self
    }
}

/// A change event from the replication stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    /// Log sequence number
    pub lsn: LSN,
    /// Timestamp
    pub timestamp: u64,
    /// Operation type
    pub operation: ChangeOperation,
    /// Key affected
    pub key: String,
    /// New value (for SET/UPDATE)
    pub value: Option<Vec<u8>>,
    /// Old value (for UPDATE/DELETE)
    pub old_value: Option<Vec<u8>>,
    /// Database number
    pub database: u8,
    /// Transaction ID
    pub xid: u64,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl ChangeEvent {
    /// Create a new change event
    pub fn new(lsn: LSN, operation: ChangeOperation, key: impl Into<String>) -> Self {
        Self {
            lsn,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            operation,
            key: key.into(),
            value: None,
            old_value: None,
            database: 0,
            xid: 0,
            metadata: HashMap::new(),
        }
    }

    /// Set value
    pub fn with_value(mut self, value: Vec<u8>) -> Self {
        self.value = Some(value);
        self
    }

    /// Set old value
    pub fn with_old_value(mut self, old_value: Vec<u8>) -> Self {
        self.old_value = Some(old_value);
        self
    }

    /// Set transaction ID
    pub fn with_xid(mut self, xid: u64) -> Self {
        self.xid = xid;
        self
    }

    /// Set database
    pub fn with_database(mut self, db: u8) -> Self {
        self.database = db;
        self
    }
}

/// Type of change operation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeOperation {
    /// Key was inserted
    Insert,
    /// Key was updated
    Update,
    /// Key was deleted
    Delete,
    /// Key expired
    Expire,
    /// Transaction begin
    Begin,
    /// Transaction commit
    Commit,
    /// Transaction rollback
    Rollback,
}

impl ChangeOperation {
    /// Get string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            ChangeOperation::Insert => "INSERT",
            ChangeOperation::Update => "UPDATE",
            ChangeOperation::Delete => "DELETE",
            ChangeOperation::Expire => "EXPIRE",
            ChangeOperation::Begin => "BEGIN",
            ChangeOperation::Commit => "COMMIT",
            ChangeOperation::Rollback => "ROLLBACK",
        }
    }
}

/// A replication slot
#[derive(Debug)]
pub struct ReplicationSlot {
    /// Slot configuration
    pub config: SlotConfig,
    /// Current state
    pub state: SlotState,
    /// Creation timestamp
    pub created_at: u64,
    /// Last activity timestamp
    pub last_active: u64,
    /// Confirmed flush LSN (consumer progress)
    pub confirmed_flush_lsn: AtomicU64,
    /// Current streaming LSN
    pub current_lsn: AtomicU64,
    /// WAL retained start LSN
    pub retained_lsn: AtomicU64,
    /// Active consumer count
    pub active_consumers: AtomicU64,
    /// Total bytes sent
    pub bytes_sent: AtomicU64,
    /// Total changes sent
    pub changes_sent: AtomicU64,
}

impl ReplicationSlot {
    /// Create a new replication slot
    pub fn new(config: SlotConfig) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            config,
            state: SlotState::Inactive,
            created_at: now,
            last_active: now,
            confirmed_flush_lsn: AtomicU64::new(0),
            current_lsn: AtomicU64::new(0),
            retained_lsn: AtomicU64::new(0),
            active_consumers: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            changes_sent: AtomicU64::new(0),
        }
    }

    /// Get slot name
    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// Get current lag in bytes
    pub fn lag_bytes(&self) -> u64 {
        let current = self.current_lsn.load(Ordering::Relaxed);
        let confirmed = self.confirmed_flush_lsn.load(Ordering::Relaxed);
        current.saturating_sub(confirmed)
    }

    /// Confirm consumer progress
    pub fn confirm(&self, lsn: LSN) {
        self.confirmed_flush_lsn.fetch_max(lsn, Ordering::Relaxed);
    }

    /// Get slot statistics
    pub fn stats(&self) -> SlotStats {
        SlotStats {
            name: self.config.name.clone(),
            state: self.state,
            created_at: self.created_at,
            last_active: self.last_active,
            current_lsn: self.current_lsn.load(Ordering::Relaxed),
            confirmed_lsn: self.confirmed_flush_lsn.load(Ordering::Relaxed),
            retained_lsn: self.retained_lsn.load(Ordering::Relaxed),
            lag_bytes: self.lag_bytes(),
            active_consumers: self.active_consumers.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            changes_sent: self.changes_sent.load(Ordering::Relaxed),
        }
    }
}

/// Slot statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotStats {
    /// Slot name
    pub name: String,
    /// Current state
    pub state: SlotState,
    /// Creation timestamp
    pub created_at: u64,
    /// Last activity
    pub last_active: u64,
    /// Current LSN
    pub current_lsn: LSN,
    /// Confirmed flush LSN
    pub confirmed_lsn: LSN,
    /// Retained WAL start
    pub retained_lsn: LSN,
    /// Current lag in bytes
    pub lag_bytes: u64,
    /// Active consumers
    pub active_consumers: u64,
    /// Total bytes sent
    pub bytes_sent: u64,
    /// Total changes sent
    pub changes_sent: u64,
}

/// Error types for replication operations
#[derive(Debug, Clone, thiserror::Error)]
pub enum ReplicationError {
    /// Slot not found
    #[error("slot not found: {0}")]
    SlotNotFound(String),

    /// Slot already exists
    #[error("slot already exists: {0}")]
    SlotExists(String),

    /// Slot is not active
    #[error("slot is not active: {0}")]
    SlotNotActive(String),

    /// LSN is invalid
    #[error("invalid LSN: {0}")]
    InvalidLSN(String),

    /// Consumer error
    #[error("consumer error: {0}")]
    ConsumerError(String),
}

/// Result type for replication operations
pub type Result<T> = std::result::Result<T, ReplicationError>;

/// Manages replication slots
pub struct SlotManager {
    slots: RwLock<HashMap<SlotId, ReplicationSlot>>,
    current_lsn: AtomicU64,
    change_sender: broadcast::Sender<ChangeEvent>,
}

impl SlotManager {
    /// Create a new slot manager
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(10000);

        Self {
            slots: RwLock::new(HashMap::new()),
            current_lsn: AtomicU64::new(1),
            change_sender: sender,
        }
    }

    /// Create a new replication slot
    pub fn create_slot(&self, config: SlotConfig) -> Result<SlotId> {
        let name = config.name.clone();

        let mut slots = self.slots.write();
        if slots.contains_key(&name) {
            return Err(ReplicationError::SlotExists(name));
        }

        let slot = ReplicationSlot::new(config);
        slots.insert(name.clone(), slot);

        Ok(name)
    }

    /// Drop a replication slot
    pub fn drop_slot(&self, name: &str) -> Result<()> {
        let mut slots = self.slots.write();

        if let Some(mut slot) = slots.remove(name) {
            slot.state = SlotState::Dropping;
            Ok(())
        } else {
            Err(ReplicationError::SlotNotFound(name.to_string()))
        }
    }

    /// Get a slot by name
    pub fn get_slot(&self, name: &str) -> Option<SlotStats> {
        self.slots.read().get(name).map(|s| s.stats())
    }

    /// List all slots
    pub fn list_slots(&self) -> Vec<SlotStats> {
        self.slots.read().values().map(|s| s.stats()).collect()
    }

    /// Start replication from a slot
    pub fn start_replication(
        &self,
        name: &str,
        start_lsn: Option<LSN>,
    ) -> Result<broadcast::Receiver<ChangeEvent>> {
        let mut slots = self.slots.write();
        let slot = slots
            .get_mut(name)
            .ok_or_else(|| ReplicationError::SlotNotFound(name.to_string()))?;

        slot.state = SlotState::Active;
        slot.active_consumers.fetch_add(1, Ordering::Relaxed);

        if let Some(lsn) = start_lsn {
            slot.confirmed_flush_lsn.store(lsn, Ordering::Relaxed);
        }

        slot.last_active = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Ok(self.change_sender.subscribe())
    }

    /// Stop replication for a slot
    pub fn stop_replication(&self, name: &str) -> Result<()> {
        let mut slots = self.slots.write();
        let slot = slots
            .get_mut(name)
            .ok_or_else(|| ReplicationError::SlotNotFound(name.to_string()))?;

        let consumers = slot.active_consumers.fetch_sub(1, Ordering::Relaxed);
        if consumers <= 1 {
            slot.state = SlotState::Inactive;
        }

        Ok(())
    }

    /// Confirm consumer progress
    pub fn confirm_flush(&self, name: &str, lsn: LSN) -> Result<()> {
        let slots = self.slots.read();
        let slot = slots
            .get(name)
            .ok_or_else(|| ReplicationError::SlotNotFound(name.to_string()))?;

        slot.confirm(lsn);
        Ok(())
    }

    /// Publish a change event
    pub fn publish_change(&self, event: ChangeEvent) {
        let lsn = self.current_lsn.fetch_add(1, Ordering::Relaxed);
        let mut event = event;
        event.lsn = lsn;

        // Update slot stats
        let slots = self.slots.read();
        for slot in slots.values() {
            if slot.state == SlotState::Active {
                // Check if event matches slot's key patterns
                if self.matches_patterns(&event.key, &slot.config.key_patterns) {
                    slot.current_lsn.store(lsn, Ordering::Relaxed);
                    slot.changes_sent.fetch_add(1, Ordering::Relaxed);
                    if let Some(ref value) = event.value {
                        slot.bytes_sent
                            .fetch_add(value.len() as u64, Ordering::Relaxed);
                    }
                }
            }
        }
        drop(slots);

        // Broadcast to all subscribers
        let _ = self.change_sender.send(event);
    }

    /// Check if key matches any pattern
    fn matches_patterns(&self, key: &str, patterns: &[String]) -> bool {
        for pattern in patterns {
            if pattern == "*" {
                return true;
            }
            if pattern.ends_with('*') {
                let prefix = &pattern[..pattern.len() - 1];
                if key.starts_with(prefix) {
                    return true;
                }
            } else if let Some(suffix) = pattern.strip_prefix('*') {
                if key.ends_with(suffix) {
                    return true;
                }
            } else if pattern == key {
                return true;
            }
        }
        false
    }

    /// Get current LSN
    pub fn current_lsn(&self) -> LSN {
        self.current_lsn.load(Ordering::Relaxed)
    }

    /// Get global statistics
    pub fn stats(&self) -> GlobalSlotStats {
        let slots = self.slots.read();

        GlobalSlotStats {
            slot_count: slots.len() as u64,
            active_slots: slots
                .values()
                .filter(|s| s.state == SlotState::Active)
                .count() as u64,
            total_consumers: slots
                .values()
                .map(|s| s.active_consumers.load(Ordering::Relaxed))
                .sum(),
            current_lsn: self.current_lsn.load(Ordering::Relaxed),
        }
    }
}

impl Default for SlotManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Global slot statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalSlotStats {
    /// Total slot count
    pub slot_count: u64,
    /// Active slots
    pub active_slots: u64,
    /// Total consumers
    pub total_consumers: u64,
    /// Current LSN
    pub current_lsn: LSN,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn test_create_slot() {
        let manager = SlotManager::new();

        let config = SlotConfig::new("test_slot").with_patterns(vec!["users:*".to_string()]);

        let id = manager.create_slot(config).unwrap();
        assert_eq!(id, "test_slot");

        let slot = manager.get_slot("test_slot").unwrap();
        assert_eq!(slot.name, "test_slot");
        assert_eq!(slot.state, SlotState::Inactive);
    }

    #[test]
    fn test_drop_slot() {
        let manager = SlotManager::new();

        manager.create_slot(SlotConfig::new("test")).unwrap();
        assert!(manager.get_slot("test").is_some());

        manager.drop_slot("test").unwrap();
        assert!(manager.get_slot("test").is_none());
    }

    #[test]
    fn test_start_stop_replication() {
        let manager = SlotManager::new();

        manager.create_slot(SlotConfig::new("test")).unwrap();

        let _receiver = manager.start_replication("test", None).unwrap();

        let slot = manager.get_slot("test").unwrap();
        assert_eq!(slot.state, SlotState::Active);
        assert_eq!(slot.active_consumers, 1);

        manager.stop_replication("test").unwrap();

        let slot = manager.get_slot("test").unwrap();
        assert_eq!(slot.state, SlotState::Inactive);
    }

    #[test]
    fn test_confirm_flush() {
        let manager = SlotManager::new();

        manager.create_slot(SlotConfig::new("test")).unwrap();
        manager.start_replication("test", None).unwrap();

        manager.confirm_flush("test", 100).unwrap();

        let slot = manager.get_slot("test").unwrap();
        assert_eq!(slot.confirmed_lsn, 100);
    }

    #[test]
    fn test_publish_change() {
        let manager = SlotManager::new();

        manager.create_slot(SlotConfig::new("test")).unwrap();
        let mut receiver = manager.start_replication("test", None).unwrap();

        let event =
            ChangeEvent::new(0, ChangeOperation::Insert, "key1").with_value(b"value1".to_vec());

        manager.publish_change(event);

        // Check if event was received
        let received = receiver.try_recv();
        assert!(received.is_ok());
    }

    #[test]
    fn test_pattern_matching() {
        let manager = SlotManager::new();

        assert!(manager.matches_patterns("users:123", &["users:*".to_string()]));
        assert!(manager.matches_patterns("anything", &["*".to_string()]));
        assert!(!manager.matches_patterns("orders:123", &["users:*".to_string()]));
        assert!(manager.matches_patterns("file.json", &["*.json".to_string()]));
    }

    #[test]
    fn test_slot_stats() {
        let manager = SlotManager::new();

        manager.create_slot(SlotConfig::new("slot1")).unwrap();
        manager.create_slot(SlotConfig::new("slot2")).unwrap();
        manager.start_replication("slot1", None).unwrap();

        let stats = manager.stats();
        assert_eq!(stats.slot_count, 2);
        assert_eq!(stats.active_slots, 1);
    }
}
