//! Cluster Slot Migration
//!
//! This module implements slot migration for Redis Cluster:
//! - MIGRATE command for moving individual keys
//! - Slot rebalancing algorithms
//! - Migration state management

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use parking_lot::RwLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::{info, warn};

use super::{ClusterManager, HashSlot, NodeId, CLUSTER_SLOTS};
use crate::protocol::{encode_frame, Frame};
use crate::storage::{Store, Value};

/// Migration options for the MIGRATE command
#[derive(Debug, Clone)]
pub struct MigrateOptions {
    /// Target host
    pub host: String,
    /// Target port
    pub port: u16,
    /// Destination database
    pub db: u8,
    /// Timeout in milliseconds
    pub timeout_ms: u64,
    /// Copy the key instead of moving (COPY option)
    pub copy: bool,
    /// Replace existing key on destination (REPLACE option)
    pub replace: bool,
    /// Authentication for target node
    pub auth: Option<String>,
}

impl Default for MigrateOptions {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: 6379,
            db: 0,
            timeout_ms: 5000,
            copy: false,
            replace: false,
            auth: None,
        }
    }
}

/// Result of a key migration
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrateResult {
    /// Migration succeeded
    Ok,
    /// Key doesn't exist (NOKEY)
    NoKey,
    /// Migration failed
    Error(String),
}

/// Slot migration status
#[derive(Debug, Clone)]
pub struct SlotMigrationStatus {
    /// Slot being migrated
    pub slot: u16,
    /// Source node
    pub source_node: NodeId,
    /// Target node
    pub target_node: NodeId,
    /// Keys remaining to migrate
    pub keys_remaining: usize,
    /// Keys migrated so far
    pub keys_migrated: usize,
    /// Migration start time
    pub started_at: Instant,
    /// Current state
    pub state: MigrationState,
}

/// Migration state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationState {
    /// Migration not started
    Pending,
    /// Migration in progress
    InProgress,
    /// Migration completed
    Completed,
    /// Migration failed
    Failed,
}

/// Slot migration manager
pub struct SlotMigrationManager {
    /// Reference to cluster manager
    cluster: Arc<ClusterManager>,
    /// Reference to local store
    store: Arc<Store>,
    /// Active migrations (slot -> status)
    migrations: RwLock<HashMap<u16, SlotMigrationStatus>>,
    /// Migration configuration
    config: MigrationConfig,
}

/// Migration configuration
#[derive(Debug, Clone)]
pub struct MigrationConfig {
    /// Timeout for individual key migrations
    pub key_timeout_ms: u64,
    /// Number of keys to migrate in a batch
    pub batch_size: usize,
    /// Delay between batches (to reduce load)
    pub batch_delay_ms: u64,
    /// Maximum concurrent migrations
    pub max_concurrent: usize,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            key_timeout_ms: 5000,
            batch_size: 100,
            batch_delay_ms: 10,
            max_concurrent: 1,
        }
    }
}

impl SlotMigrationManager {
    /// Create a new migration manager
    pub fn new(cluster: Arc<ClusterManager>, store: Arc<Store>, config: MigrationConfig) -> Self {
        Self {
            cluster,
            store,
            migrations: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Migrate a single key to another node
    ///
    /// This implements the MIGRATE command functionality
    pub async fn migrate_key(
        &self,
        key: &Bytes,
        options: &MigrateOptions,
        db: u8,
    ) -> MigrateResult {
        // Get the key's value and TTL using Store's API
        let value = match self.store.get(db, key) {
            Some(v) => v,
            None => return MigrateResult::NoKey,
        };

        let ttl_secs = self.store.ttl(db, key);
        let ttl = ttl_secs.and_then(|s| {
            if s > 0 {
                Some(Duration::from_secs(s as u64))
            } else {
                None
            }
        });

        // Serialize the value for RESTORE command
        let serialized = serialize_value_for_restore(&value, ttl);

        // Connect to target node
        let target_addr = format!("{}:{}", options.host, options.port);
        let stream = match timeout(
            Duration::from_millis(options.timeout_ms),
            TcpStream::connect(&target_addr),
        )
        .await
        {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => return MigrateResult::Error(format!("ERR connection failed: {}", e)),
            Err(_) => return MigrateResult::Error("ERR timeout connecting".to_string()),
        };

        // Perform the migration
        match self
            .send_restore_command(stream, key, &serialized, ttl, options)
            .await
        {
            Ok(_) => {
                // If not COPY mode, delete local key
                if !options.copy {
                    self.store.del(db, &[key.clone()]);
                }
                MigrateResult::Ok
            }
            Err(e) => MigrateResult::Error(format!("ERR {}", e)),
        }
    }

    /// Send RESTORE command to target node
    async fn send_restore_command(
        &self,
        mut stream: TcpStream,
        key: &Bytes,
        serialized: &[u8],
        ttl: Option<Duration>,
        options: &MigrateOptions,
    ) -> io::Result<()> {
        // Build RESTORE command
        // RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]
        let ttl_ms = ttl.map(|d| d.as_millis() as i64).unwrap_or(0);

        let mut cmd_parts = vec![
            Frame::bulk("RESTORE"),
            Frame::Bulk(Some(key.clone())),
            Frame::Integer(ttl_ms),
            Frame::Bulk(Some(Bytes::copy_from_slice(serialized))),
        ];

        if options.replace {
            cmd_parts.push(Frame::bulk("REPLACE"));
        }

        let cmd = Frame::Array(Some(cmd_parts));
        let mut buf = BytesMut::new();
        encode_frame(&cmd, &mut buf);

        // Send command
        stream.write_all(&buf).await?;

        // Read response
        let mut buf = vec![0u8; 1024];
        let n = stream.read(&mut buf).await?;

        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Connection closed",
            ));
        }

        // Check response
        let response = String::from_utf8_lossy(&buf[..n]);
        if response.starts_with("+OK") {
            Ok(())
        } else if response.starts_with("-") {
            Err(io::Error::other(response.trim().to_string()))
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unexpected response",
            ))
        }
    }

    /// Start migrating a slot to another node
    pub fn start_slot_migration(
        &self,
        slot: u16,
        source_node: NodeId,
        target_node: NodeId,
    ) -> io::Result<()> {
        if slot >= CLUSTER_SLOTS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid slot number",
            ));
        }

        let mut migrations = self.migrations.write();

        // Check if already migrating
        if migrations.contains_key(&slot) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "Slot already being migrated",
            ));
        }

        // Check concurrent migration limit
        let active_count = migrations
            .values()
            .filter(|m| m.state == MigrationState::InProgress)
            .count();

        if active_count >= self.config.max_concurrent {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "Too many concurrent migrations",
            ));
        }

        // Count keys in this slot
        let keys_count = self.count_keys_in_slot(slot);

        let status = SlotMigrationStatus {
            slot,
            source_node,
            target_node,
            keys_remaining: keys_count,
            keys_migrated: 0,
            started_at: Instant::now(),
            state: MigrationState::Pending,
        };

        migrations.insert(slot, status);
        info!("Started migration for slot {} ({} keys)", slot, keys_count);

        Ok(())
    }

    /// Get migration status for a slot
    pub fn get_migration_status(&self, slot: u16) -> Option<SlotMigrationStatus> {
        self.migrations.read().get(&slot).cloned()
    }

    /// Get all active migrations
    pub fn get_active_migrations(&self) -> Vec<SlotMigrationStatus> {
        self.migrations
            .read()
            .values()
            .filter(|m| m.state == MigrationState::InProgress)
            .cloned()
            .collect()
    }

    /// Complete a slot migration
    pub fn complete_slot_migration(&self, slot: u16) {
        let mut migrations = self.migrations.write();
        if let Some(status) = migrations.get_mut(&slot) {
            status.state = MigrationState::Completed;
            info!(
                "Completed migration for slot {} ({} keys migrated)",
                slot, status.keys_migrated
            );
        }
    }

    /// Cancel a slot migration
    pub fn cancel_slot_migration(&self, slot: u16) {
        let mut migrations = self.migrations.write();
        if let Some(status) = migrations.get_mut(&slot) {
            status.state = MigrationState::Failed;
            warn!("Cancelled migration for slot {}", slot);
        }
    }

    /// Count keys belonging to a specific slot
    fn count_keys_in_slot(&self, slot: u16) -> usize {
        let mut count = 0;
        for key in self.store.keys(0) {
            if HashSlot::for_key(&key) == slot {
                count += 1;
            }
        }
        count
    }

    /// Get keys in a specific slot
    pub fn get_keys_in_slot(&self, slot: u16, count: usize) -> Vec<Bytes> {
        let mut keys = Vec::new();
        for key in self.store.keys(0) {
            if HashSlot::for_key(&key) == slot {
                keys.push(key);
                if keys.len() >= count {
                    break;
                }
            }
        }
        keys
    }
}

/// Serialize a value for the RESTORE command
fn serialize_value_for_restore(value: &Value, _ttl: Option<Duration>) -> Vec<u8> {
    // Simplified serialization - in production this would be RDB format
    // For now, we use a simple format that can be parsed by RESTORE
    let mut buf = Vec::new();

    // Version byte
    buf.push(0);

    // Type byte
    match value {
        Value::String(s) => {
            buf.push(0); // String type
            let len = s.len() as u32;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(s);
        }
        Value::List(items) => {
            buf.push(1); // List type
            let len = items.len() as u32;
            buf.extend_from_slice(&len.to_le_bytes());
            for item in items {
                let item_len = item.len() as u32;
                buf.extend_from_slice(&item_len.to_le_bytes());
                buf.extend_from_slice(item);
            }
        }
        Value::Set(members) => {
            buf.push(2); // Set type
            let len = members.len() as u32;
            buf.extend_from_slice(&len.to_le_bytes());
            for member in members {
                let member_len = member.len() as u32;
                buf.extend_from_slice(&member_len.to_le_bytes());
                buf.extend_from_slice(member);
            }
        }
        Value::Hash(fields) => {
            buf.push(3); // Hash type
            let len = fields.len() as u32;
            buf.extend_from_slice(&len.to_le_bytes());
            for (k, v) in fields {
                let key_len = k.len() as u32;
                buf.extend_from_slice(&key_len.to_le_bytes());
                buf.extend_from_slice(k);
                let val_len = v.len() as u32;
                buf.extend_from_slice(&val_len.to_le_bytes());
                buf.extend_from_slice(v);
            }
        }
        Value::SortedSet {
            by_score,
            by_member: _,
        } => {
            buf.push(4); // Sorted set type
            let len = by_score.len() as u32;
            buf.extend_from_slice(&len.to_le_bytes());
            for (score, member) in by_score.keys() {
                buf.extend_from_slice(&score.into_inner().to_le_bytes());
                let member_len = member.len() as u32;
                buf.extend_from_slice(&member_len.to_le_bytes());
                buf.extend_from_slice(member);
            }
        }
        Value::Stream(stream) => {
            buf.push(5); // Stream type
                         // Simplified stream serialization
            let entries = &stream.entries;
            let len = entries.len() as u32;
            buf.extend_from_slice(&len.to_le_bytes());
            // Full stream serialization would include all entry data
        }
        Value::HyperLogLog(data) => {
            buf.push(6); // HyperLogLog type
            let len = data.len() as u32;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(data);
        }
    }

    // Add checksum (CRC64 in real Redis, simplified here)
    let checksum: u64 = buf.iter().map(|&b| b as u64).sum();
    buf.extend_from_slice(&checksum.to_le_bytes());

    buf
}

/// Slot rebalancing algorithm
pub struct SlotRebalancer {
    /// Target slots per node (roughly equal distribution)
    target_slots_per_node: usize,
}

impl SlotRebalancer {
    /// Create a new rebalancer
    pub fn new() -> Self {
        Self {
            target_slots_per_node: 0,
        }
    }

    /// Calculate rebalancing plan
    ///
    /// Returns a list of (slot, source_node, target_node) tuples
    pub fn calculate_rebalance_plan(
        &mut self,
        nodes: &[NodeId],
        current_assignments: &HashMap<NodeId, Vec<u16>>,
    ) -> Vec<(u16, NodeId, NodeId)> {
        if nodes.is_empty() {
            return Vec::new();
        }

        // Calculate target slots per node
        self.target_slots_per_node = CLUSTER_SLOTS as usize / nodes.len();
        let remainder = CLUSTER_SLOTS as usize % nodes.len();

        // Calculate how many slots each node should have
        let mut target_counts: HashMap<&NodeId, usize> = HashMap::new();
        for (i, node) in nodes.iter().enumerate() {
            let count = self.target_slots_per_node + if i < remainder { 1 } else { 0 };
            target_counts.insert(node, count);
        }

        // Calculate current counts
        let mut current_counts: HashMap<&NodeId, usize> = HashMap::new();
        for node in nodes {
            let count = current_assignments.get(node).map(|s| s.len()).unwrap_or(0);
            current_counts.insert(node, count);
        }

        // Find nodes that have too many slots (donors) and too few (recipients)
        let mut donors: Vec<(&NodeId, usize)> = Vec::new();
        let mut recipients: Vec<(&NodeId, usize)> = Vec::new();

        for node in nodes {
            let current = *current_counts.get(node).unwrap_or(&0);
            let target = *target_counts.get(node).unwrap_or(&0);

            if current > target {
                donors.push((node, current - target));
            } else if current < target {
                recipients.push((node, target - current));
            }
        }

        // Generate migration plan
        let mut plan = Vec::new();

        for (donor, excess) in &mut donors {
            if *excess == 0 {
                continue;
            }

            // Get donor's slots
            let donor_slots = match current_assignments.get(*donor) {
                Some(slots) => slots.clone(),
                None => continue,
            };

            let mut slots_to_move = donor_slots.into_iter().take(*excess).collect::<Vec<_>>();

            for (recipient, needed) in recipients.iter_mut() {
                if *needed == 0 || slots_to_move.is_empty() {
                    continue;
                }

                let move_count = (*needed).min(slots_to_move.len());
                for _ in 0..move_count {
                    if let Some(slot) = slots_to_move.pop() {
                        plan.push((slot, (*donor).clone(), (*recipient).clone()));
                        *needed -= 1;
                        *excess -= 1;
                    }
                }
            }
        }

        plan
    }

    /// Estimate migration time based on key counts
    pub fn estimate_migration_time(
        &self,
        plan: &[(u16, NodeId, NodeId)],
        keys_per_slot: &HashMap<u16, usize>,
        keys_per_second: usize,
    ) -> Duration {
        let total_keys: usize = plan
            .iter()
            .map(|(slot, _, _)| keys_per_slot.get(slot).copied().unwrap_or(0))
            .sum();

        if keys_per_second == 0 {
            return Duration::from_secs(0);
        }

        Duration::from_secs((total_keys / keys_per_second) as u64)
    }
}

impl Default for SlotRebalancer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migrate_options_default() {
        let opts = MigrateOptions::default();
        assert_eq!(opts.port, 6379);
        assert_eq!(opts.db, 0);
        assert_eq!(opts.timeout_ms, 5000);
        assert!(!opts.copy);
        assert!(!opts.replace);
    }

    #[test]
    fn test_migration_config_default() {
        let config = MigrationConfig::default();
        assert_eq!(config.key_timeout_ms, 5000);
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.batch_delay_ms, 10);
        assert_eq!(config.max_concurrent, 1);
    }

    #[test]
    fn test_slot_rebalancer_empty_nodes() {
        let mut rebalancer = SlotRebalancer::new();
        let plan = rebalancer.calculate_rebalance_plan(&[], &HashMap::new());
        assert!(plan.is_empty());
    }

    #[test]
    fn test_slot_rebalancer_balanced() {
        let mut rebalancer = SlotRebalancer::new();

        let nodes = vec!["node1".to_string(), "node2".to_string()];
        let mut assignments = HashMap::new();

        // Each node has half the slots - already balanced
        let node1_slots: Vec<u16> = (0..8192).collect();
        let node2_slots: Vec<u16> = (8192..16384).collect();

        assignments.insert("node1".to_string(), node1_slots);
        assignments.insert("node2".to_string(), node2_slots);

        let plan = rebalancer.calculate_rebalance_plan(&nodes, &assignments);
        assert!(plan.is_empty(), "Already balanced - no migrations needed");
    }

    #[test]
    fn test_slot_rebalancer_imbalanced() {
        let mut rebalancer = SlotRebalancer::new();

        let nodes = vec!["node1".to_string(), "node2".to_string()];
        let mut assignments = HashMap::new();

        // Node1 has all slots, node2 has none
        let node1_slots: Vec<u16> = (0..16384).collect();
        assignments.insert("node1".to_string(), node1_slots);
        assignments.insert("node2".to_string(), Vec::new());

        let plan = rebalancer.calculate_rebalance_plan(&nodes, &assignments);

        // Should plan to move 8192 slots from node1 to node2
        assert_eq!(plan.len(), 8192);

        // All moves should be from node1 to node2
        for (_, source, target) in &plan {
            assert_eq!(source, "node1");
            assert_eq!(target, "node2");
        }
    }

    #[test]
    fn test_slot_rebalancer_three_nodes() {
        let mut rebalancer = SlotRebalancer::new();

        let nodes = vec![
            "node1".to_string(),
            "node2".to_string(),
            "node3".to_string(),
        ];
        let mut assignments = HashMap::new();

        // Node1 has 8000 slots, node2 has 8384, node3 has 0
        let node1_slots: Vec<u16> = (0..8000).collect();
        let node2_slots: Vec<u16> = (8000..16384).collect();
        assignments.insert("node1".to_string(), node1_slots);
        assignments.insert("node2".to_string(), node2_slots);
        assignments.insert("node3".to_string(), Vec::new());

        let plan = rebalancer.calculate_rebalance_plan(&nodes, &assignments);

        // Target is ~5461 slots per node (16384/3)
        // node1 has 8000 (excess: 2539)
        // node2 has 8384 (excess: 2923)
        // node3 has 0 (deficit: 5461)

        // Plan should move slots to balance
        assert!(!plan.is_empty());

        // Verify all targets are node3
        for (_, _, target) in &plan {
            assert_eq!(target, "node3");
        }
    }

    #[test]
    fn test_estimate_migration_time() {
        let rebalancer = SlotRebalancer::new();

        let plan = vec![
            (0, "node1".to_string(), "node2".to_string()),
            (1, "node1".to_string(), "node2".to_string()),
        ];

        let mut keys_per_slot = HashMap::new();
        keys_per_slot.insert(0u16, 1000);
        keys_per_slot.insert(1u16, 2000);

        let estimate = rebalancer.estimate_migration_time(&plan, &keys_per_slot, 1000);
        assert_eq!(estimate, Duration::from_secs(3)); // 3000 keys / 1000 per second
    }

    #[test]
    fn test_serialize_value_string() {
        let value = Value::String(Bytes::from("hello"));
        let serialized = serialize_value_for_restore(&value, None);

        // Should have version, type, length, data, checksum
        assert!(serialized.len() > 5 + 8); // minimum: version + type + len + "hello" + checksum
    }

    #[test]
    fn test_serialize_value_list() {
        use std::collections::VecDeque;
        let value = Value::List(VecDeque::from([Bytes::from("a"), Bytes::from("b")]));
        let serialized = serialize_value_for_restore(&value, None);
        assert!(!serialized.is_empty());
    }

    #[test]
    fn test_migration_state_transitions() {
        assert_eq!(MigrationState::Pending as u8, 0);
        assert_ne!(MigrationState::InProgress, MigrationState::Completed);
    }
}
