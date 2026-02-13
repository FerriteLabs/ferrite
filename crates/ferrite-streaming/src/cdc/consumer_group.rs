//! Consumer Groups for CDC
//!
//! Kafka-style consumer groups with automatic partition assignment,
//! rebalancing, heartbeat tracking, and pending entry lists.

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::RwLock;

/// Unique identifier for a consumer group.
pub type ConsumerGroupId = String;

/// Unique identifier for a consumer within a group.
pub type ConsumerId = String;

// ---------------------------------------------------------------------------
// PartitionAssignmentStrategy
// ---------------------------------------------------------------------------

/// Strategy used to assign partitions to consumers during a rebalance.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum PartitionAssignmentStrategy {
    /// Each consumer gets roughly equal partitions in order.
    #[default]
    RoundRobin,
    /// Contiguous ranges of partitions are assigned to each consumer.
    Range,
}

impl PartitionAssignmentStrategy {
    /// Compute partition→consumer mapping.
    pub fn assign(
        &self,
        partitions: &[u32],
        consumers: &[ConsumerId],
    ) -> HashMap<ConsumerId, Vec<u32>> {
        let mut result: HashMap<ConsumerId, Vec<u32>> = HashMap::new();
        if consumers.is_empty() || partitions.is_empty() {
            return result;
        }

        for c in consumers {
            result.insert(c.clone(), Vec::new());
        }

        match self {
            Self::RoundRobin => {
                for (i, &p) in partitions.iter().enumerate() {
                    let consumer = &consumers[i % consumers.len()];
                    result.entry(consumer.clone()).or_default().push(p);
                }
            }
            Self::Range => {
                let n = consumers.len();
                let chunk = partitions.len() / n;
                let remainder = partitions.len() % n;
                let mut offset = 0;
                for (i, c) in consumers.iter().enumerate() {
                    let extra = if i < remainder { 1 } else { 0 };
                    let end = offset + chunk + extra;
                    result
                        .entry(c.clone())
                        .or_default()
                        .extend_from_slice(&partitions[offset..end]);
                    offset = end;
                }
            }
        }

        result
    }
}

// ---------------------------------------------------------------------------
// StartingOffset
// ---------------------------------------------------------------------------

/// Where a newly created consumer group should begin reading.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum StartingOffset {
    /// Start from the earliest available event.
    Earliest,
    /// Start from the latest (tail) of the stream.
    #[default]
    Latest,
    /// Start from a specific event ID.
    Specific(u64),
}

// ---------------------------------------------------------------------------
// PendingEntry
// ---------------------------------------------------------------------------

/// A single unacknowledged message tracked in the PEL.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PendingEntry {
    /// Event/message ID.
    pub event_id: u64,
    /// Consumer that owns this entry.
    pub consumer_id: ConsumerId,
    /// When the message was delivered.
    pub delivered_at: SystemTime,
    /// How many times it has been delivered (for retry tracking).
    pub delivery_count: u32,
}

// ---------------------------------------------------------------------------
// PendingEntryList (PEL)
// ---------------------------------------------------------------------------

/// Pending Entry List – tracks unacknowledged messages per consumer.
#[derive(Debug, Default)]
pub struct PendingEntryList {
    /// event_id → PendingEntry
    entries: BTreeMap<u64, PendingEntry>,
}

impl PendingEntryList {
    /// Record delivery of a message to a consumer.
    pub fn add(&mut self, event_id: u64, consumer_id: ConsumerId) {
        let entry = self
            .entries
            .entry(event_id)
            .or_insert_with(|| PendingEntry {
                event_id,
                consumer_id: consumer_id.clone(),
                delivered_at: SystemTime::now(),
                delivery_count: 0,
            });
        entry.delivery_count += 1;
        entry.consumer_id = consumer_id;
    }

    /// Acknowledge (remove) a message.
    pub fn ack(&mut self, event_id: u64) -> bool {
        self.entries.remove(&event_id).is_some()
    }

    /// Acknowledge all messages up to and including `event_id`.
    pub fn ack_up_to(&mut self, event_id: u64) -> usize {
        let to_remove: Vec<u64> = self
            .entries
            .range(..=event_id)
            .map(|(&id, _)| id)
            .collect();
        let count = to_remove.len();
        for id in to_remove {
            self.entries.remove(&id);
        }
        count
    }

    /// Return all pending entries for a given consumer.
    pub fn pending_for(&self, consumer_id: &str) -> Vec<&PendingEntry> {
        self.entries
            .values()
            .filter(|e| e.consumer_id == consumer_id)
            .collect()
    }

    /// Total number of pending entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the PEL is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Return entries that have been pending longer than `timeout`.
    pub fn timed_out(&self, timeout: Duration) -> Vec<&PendingEntry> {
        let cutoff = SystemTime::now() - timeout;
        self.entries
            .values()
            .filter(|e| e.delivered_at < cutoff)
            .collect()
    }
}

// ---------------------------------------------------------------------------
// ConsumerMember
// ---------------------------------------------------------------------------

/// Tracks a single consumer's membership in a group.
#[derive(Debug, Clone)]
pub struct ConsumerMember {
    /// Consumer identifier.
    pub id: ConsumerId,
    /// Assigned partitions.
    pub partitions: Vec<u32>,
    /// When the consumer joined.
    pub joined_at: Instant,
    /// Last heartbeat time.
    pub last_heartbeat: Instant,
    /// Consumer metadata (e.g. hostname).
    pub metadata: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// ConsumerGroupConfig
// ---------------------------------------------------------------------------

/// Configuration for a consumer group.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConsumerGroupConfig {
    /// How often consumers must send heartbeats.
    pub heartbeat_interval: Duration,
    /// If a consumer misses heartbeats for this long it is evicted.
    pub session_timeout: Duration,
    /// Partition assignment strategy.
    pub assignment_strategy: PartitionAssignmentStrategy,
    /// Starting offset for new groups.
    pub starting_offset: StartingOffset,
    /// Number of partitions for this group's stream.
    pub num_partitions: u32,
    /// Maximum number of unacknowledged messages before pausing delivery.
    pub max_pending: usize,
}

impl Default for ConsumerGroupConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(3),
            session_timeout: Duration::from_secs(30),
            assignment_strategy: PartitionAssignmentStrategy::default(),
            starting_offset: StartingOffset::default(),
            num_partitions: 4,
            max_pending: 10_000,
        }
    }
}

// ---------------------------------------------------------------------------
// ConsumerGroup
// ---------------------------------------------------------------------------

/// A consumer group with membership tracking, partition assignment, and PEL.
pub struct ConsumerGroup {
    /// Group identifier.
    pub name: ConsumerGroupId,
    /// Configuration.
    pub config: ConsumerGroupConfig,
    /// Current members.
    members: RwLock<Vec<ConsumerMember>>,
    /// Partition assignment: consumer → partitions.
    assignments: RwLock<HashMap<ConsumerId, Vec<u32>>>,
    /// Committed offset per partition.
    committed_offsets: RwLock<HashMap<u32, u64>>,
    /// Pending entry list.
    pel: RwLock<PendingEntryList>,
    /// Generation ID (incremented on each rebalance).
    generation: AtomicU64,
    /// When the group was created.
    pub created_at: SystemTime,
}

impl ConsumerGroup {
    /// Create a new consumer group.
    pub fn new(name: impl Into<String>, config: ConsumerGroupConfig) -> Self {
        let starting = match &config.starting_offset {
            StartingOffset::Earliest => 0,
            StartingOffset::Latest => u64::MAX,
            StartingOffset::Specific(id) => *id,
        };

        let mut offsets = HashMap::new();
        for p in 0..config.num_partitions {
            offsets.insert(p, starting);
        }

        Self {
            name: name.into(),
            config,
            members: RwLock::new(Vec::new()),
            assignments: RwLock::new(HashMap::new()),
            committed_offsets: RwLock::new(offsets),
            pel: RwLock::new(PendingEntryList::default()),
            generation: AtomicU64::new(0),
            created_at: SystemTime::now(),
        }
    }

    /// A consumer joins the group, triggering a rebalance.
    pub async fn join(
        &self,
        consumer_id: ConsumerId,
        metadata: HashMap<String, String>,
    ) -> ConsumerGroupJoinResult {
        let now = Instant::now();
        let mut members = self.members.write().await;

        // Remove if already present (rejoin)
        members.retain(|m| m.id != consumer_id);

        members.push(ConsumerMember {
            id: consumer_id.clone(),
            partitions: Vec::new(),
            joined_at: now,
            last_heartbeat: now,
            metadata,
        });

        drop(members);

        let generation = self.rebalance().await;

        let assignments = self.assignments.read().await;
        let partitions = assignments
            .get(&consumer_id)
            .cloned()
            .unwrap_or_default();

        tracing::info!(
            group = %self.name,
            consumer = %consumer_id,
            generation,
            partitions = ?partitions,
            "consumer joined group"
        );

        ConsumerGroupJoinResult {
            generation,
            partitions,
        }
    }

    /// A consumer leaves the group, triggering a rebalance.
    pub async fn leave(&self, consumer_id: &str) -> bool {
        let mut members = self.members.write().await;
        let before = members.len();
        members.retain(|m| m.id != consumer_id);
        let removed = members.len() < before;
        drop(members);

        if removed {
            self.rebalance().await;
            tracing::info!(group = %self.name, consumer = %consumer_id, "consumer left group");
        }
        removed
    }

    /// Record a heartbeat from a consumer.
    pub async fn heartbeat(&self, consumer_id: &str) -> bool {
        let mut members = self.members.write().await;
        if let Some(m) = members.iter_mut().find(|m| m.id == consumer_id) {
            m.last_heartbeat = Instant::now();
            true
        } else {
            false
        }
    }

    /// Evict consumers whose session has timed out and rebalance if needed.
    pub async fn check_sessions(&self) -> Vec<ConsumerId> {
        let timeout = self.config.session_timeout;
        let mut members = self.members.write().await;

        let mut evicted = Vec::new();
        members.retain(|m| {
            if m.last_heartbeat.elapsed() > timeout {
                evicted.push(m.id.clone());
                false
            } else {
                true
            }
        });

        drop(members);

        if !evicted.is_empty() {
            self.rebalance().await;
            for id in &evicted {
                tracing::warn!(group = %self.name, consumer = %id, "consumer evicted (session timeout)");
            }
        }

        evicted
    }

    /// Perform a rebalance: reassign partitions across current members.
    async fn rebalance(&self) -> u64 {
        let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;

        let members = self.members.read().await;
        let consumer_ids: Vec<ConsumerId> = members.iter().map(|m| m.id.clone()).collect();
        drop(members);

        let partitions: Vec<u32> = (0..self.config.num_partitions).collect();
        let new_assignments = self.config.assignment_strategy.assign(&partitions, &consumer_ids);

        // Update member partition lists
        let mut members = self.members.write().await;
        for m in members.iter_mut() {
            if let Some(p) = new_assignments.get(&m.id) {
                m.partitions = p.clone();
            }
        }
        drop(members);

        *self.assignments.write().await = new_assignments;

        tracing::debug!(group = %self.name, generation, "rebalance complete");
        generation
    }

    /// Commit an offset for a partition.
    pub async fn commit_offset(&self, partition: u32, offset: u64) {
        self.committed_offsets
            .write()
            .await
            .insert(partition, offset);
    }

    /// Get the committed offset for a partition.
    pub async fn committed_offset(&self, partition: u32) -> Option<u64> {
        self.committed_offsets.read().await.get(&partition).copied()
    }

    /// Record that a message was delivered (adds to PEL).
    pub async fn record_delivery(&self, event_id: u64, consumer_id: ConsumerId) {
        self.pel.write().await.add(event_id, consumer_id);
    }

    /// Acknowledge a message (removes from PEL).
    pub async fn ack(&self, event_id: u64) -> bool {
        self.pel.write().await.ack(event_id)
    }

    /// Acknowledge all messages up to and including `event_id`.
    pub async fn ack_up_to(&self, event_id: u64) -> usize {
        self.pel.write().await.ack_up_to(event_id)
    }

    /// Number of pending (unacknowledged) entries.
    pub async fn pending_count(&self) -> usize {
        self.pel.read().await.len()
    }

    /// Current generation.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    /// Current number of members.
    pub async fn member_count(&self) -> usize {
        self.members.read().await.len()
    }

    /// Get current assignments snapshot.
    pub async fn assignments(&self) -> HashMap<ConsumerId, Vec<u32>> {
        self.assignments.read().await.clone()
    }

    /// Serializable info snapshot.
    pub async fn info(&self) -> ConsumerGroupInfo {
        let members = self.members.read().await;
        let pel = self.pel.read().await;
        ConsumerGroupInfo {
            name: self.name.clone(),
            members: members.len(),
            generation: self.generation(),
            pending: pel.len(),
            created_at: self.created_at,
        }
    }
}

/// Result of a consumer join operation.
#[derive(Debug, Clone)]
pub struct ConsumerGroupJoinResult {
    /// New generation ID.
    pub generation: u64,
    /// Partitions assigned to the joining consumer.
    pub partitions: Vec<u32>,
}

/// Serializable consumer group info.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConsumerGroupInfo {
    pub name: String,
    pub members: usize,
    pub generation: u64,
    pub pending: usize,
    #[serde(with = "system_time_serde")]
    pub created_at: SystemTime,
}

// ---------------------------------------------------------------------------
// ConsumerGroupManager
// ---------------------------------------------------------------------------

/// Manages all consumer groups.
pub struct ConsumerGroupManager {
    groups: DashMap<ConsumerGroupId, Arc<ConsumerGroup>>,
}

impl ConsumerGroupManager {
    pub fn new() -> Self {
        Self {
            groups: DashMap::new(),
        }
    }

    /// Create a new consumer group.
    pub fn create(
        &self,
        name: impl Into<String>,
        config: ConsumerGroupConfig,
    ) -> Result<Arc<ConsumerGroup>, String> {
        let name = name.into();
        if self.groups.contains_key(&name) {
            return Err(format!("Consumer group '{}' already exists", name));
        }
        let group = Arc::new(ConsumerGroup::new(name.clone(), config));
        self.groups.insert(name, Arc::clone(&group));
        Ok(group)
    }

    /// Delete a consumer group.
    pub fn delete(&self, name: &str) -> bool {
        self.groups.remove(name).is_some()
    }

    /// Get a group by name.
    pub fn get(&self, name: &str) -> Option<Arc<ConsumerGroup>> {
        self.groups.get(name).map(|g| Arc::clone(&g))
    }

    /// List all groups.
    pub async fn list(&self) -> Vec<ConsumerGroupInfo> {
        let mut infos = Vec::new();
        for entry in self.groups.iter() {
            infos.push(entry.value().info().await);
        }
        infos
    }

    /// Number of groups.
    pub fn len(&self) -> usize {
        self.groups.len()
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }
}

impl Default for ConsumerGroupManager {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Serde helper
// ---------------------------------------------------------------------------

mod system_time_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    pub fn serialize<S>(time: &SystemTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let millis = time
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;
        millis.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<SystemTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = u64::deserialize(deserializer)?;
        Ok(UNIX_EPOCH + Duration::from_millis(millis))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_robin_assignment() {
        let strategy = PartitionAssignmentStrategy::RoundRobin;
        let partitions = vec![0, 1, 2, 3, 4, 5];
        let consumers = vec!["c1".into(), "c2".into(), "c3".into()];
        let result = strategy.assign(&partitions, &consumers);

        assert_eq!(result["c1"], vec![0, 3]);
        assert_eq!(result["c2"], vec![1, 4]);
        assert_eq!(result["c3"], vec![2, 5]);
    }

    #[test]
    fn test_range_assignment() {
        let strategy = PartitionAssignmentStrategy::Range;
        let partitions = vec![0, 1, 2, 3, 4];
        let consumers = vec!["c1".into(), "c2".into()];
        let result = strategy.assign(&partitions, &consumers);

        assert_eq!(result["c1"], vec![0, 1, 2]);
        assert_eq!(result["c2"], vec![3, 4]);
    }

    #[test]
    fn test_assignment_empty_consumers() {
        let strategy = PartitionAssignmentStrategy::RoundRobin;
        let result = strategy.assign(&[0, 1], &[]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_pel_add_ack() {
        let mut pel = PendingEntryList::default();
        pel.add(1, "c1".into());
        pel.add(2, "c1".into());
        pel.add(3, "c2".into());

        assert_eq!(pel.len(), 3);
        assert_eq!(pel.pending_for("c1").len(), 2);

        assert!(pel.ack(2));
        assert_eq!(pel.len(), 2);
        assert!(!pel.ack(99));
    }

    #[test]
    fn test_pel_ack_up_to() {
        let mut pel = PendingEntryList::default();
        for i in 1..=5 {
            pel.add(i, "c1".into());
        }
        let count = pel.ack_up_to(3);
        assert_eq!(count, 3);
        assert_eq!(pel.len(), 2);
    }

    #[tokio::test]
    async fn test_consumer_group_join_leave() {
        let group = ConsumerGroup::new("test-group", ConsumerGroupConfig::default());

        let r1 = group.join("c1".into(), HashMap::new()).await;
        assert_eq!(r1.generation, 1);
        assert!(!r1.partitions.is_empty());

        let r2 = group.join("c2".into(), HashMap::new()).await;
        assert_eq!(r2.generation, 2);

        assert_eq!(group.member_count().await, 2);

        assert!(group.leave("c1").await);
        assert_eq!(group.member_count().await, 1);
        assert_eq!(group.generation(), 3);
    }

    #[tokio::test]
    async fn test_consumer_group_heartbeat() {
        let group = ConsumerGroup::new("test-group", ConsumerGroupConfig::default());
        group.join("c1".into(), HashMap::new()).await;

        assert!(group.heartbeat("c1").await);
        assert!(!group.heartbeat("unknown").await);
    }

    #[tokio::test]
    async fn test_consumer_group_commit_offset() {
        let group = ConsumerGroup::new("test-group", ConsumerGroupConfig::default());

        group.commit_offset(0, 100).await;
        assert_eq!(group.committed_offset(0).await, Some(100));
    }

    #[tokio::test]
    async fn test_consumer_group_pel() {
        let group = ConsumerGroup::new("test-group", ConsumerGroupConfig::default());

        group.record_delivery(1, "c1".into()).await;
        group.record_delivery(2, "c1".into()).await;
        assert_eq!(group.pending_count().await, 2);

        assert!(group.ack(1).await);
        assert_eq!(group.pending_count().await, 1);
    }

    #[tokio::test]
    async fn test_consumer_group_manager() {
        let mgr = ConsumerGroupManager::new();

        let group = mgr
            .create("group1", ConsumerGroupConfig::default())
            .unwrap();
        assert_eq!(group.name, "group1");

        assert!(mgr.create("group1", ConsumerGroupConfig::default()).is_err());
        assert!(mgr.get("group1").is_some());
        assert_eq!(mgr.len(), 1);

        assert!(mgr.delete("group1"));
        assert_eq!(mgr.len(), 0);
    }

    #[tokio::test]
    async fn test_rebalance_on_membership_change() {
        let config = ConsumerGroupConfig {
            num_partitions: 6,
            ..Default::default()
        };
        let group = ConsumerGroup::new("test", config);

        // One consumer gets all partitions
        let r1 = group.join("c1".into(), HashMap::new()).await;
        assert_eq!(r1.partitions.len(), 6);

        // Two consumers split
        let _r2 = group.join("c2".into(), HashMap::new()).await;
        let assignments = group.assignments().await;
        let total: usize = assignments.values().map(|p| p.len()).sum();
        assert_eq!(total, 6);

        // Three consumers
        let _r3 = group.join("c3".into(), HashMap::new()).await;
        let assignments = group.assignments().await;
        for (_, parts) in &assignments {
            assert_eq!(parts.len(), 2);
        }
    }
}
