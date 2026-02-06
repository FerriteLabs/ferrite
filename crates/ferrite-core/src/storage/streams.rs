//! Stream data structures

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

/// Stream entry ID (timestamp-sequence)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StreamEntryId {
    /// Milliseconds timestamp
    pub ms: u64,
    /// Sequence number within the millisecond
    pub seq: u64,
}

impl StreamEntryId {
    /// Create a new stream entry ID
    pub fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }

    /// Parse a stream ID from string (format: "ms-seq" or "*")
    pub fn parse(s: &str) -> Option<Self> {
        if s == "*" {
            return None; // Auto-generate
        }
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() == 2 {
            let ms = parts[0].parse().ok()?;
            let seq = parts[1].parse().ok()?;
            Some(Self { ms, seq })
        } else if parts.len() == 1 {
            let ms = parts[0].parse().ok()?;
            Some(Self { ms, seq: 0 })
        } else {
            None
        }
    }

    /// Format the ID as a string
    pub fn as_string(&self) -> String {
        format!("{}-{}", self.ms, self.seq)
    }
}

impl std::fmt::Display for StreamEntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

/// A stream entry containing field-value pairs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamEntry {
    /// The entry ID
    pub id: StreamEntryId,
    /// Field-value pairs
    pub fields: Vec<(Bytes, Bytes)>,
}

/// A pending entry in a consumer group (PEL - Pending Entries List)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamPendingEntry {
    /// The message ID
    pub id: StreamEntryId,
    /// Consumer that owns this pending entry
    pub consumer: Bytes,
    /// Time when the message was delivered (milliseconds since Unix epoch)
    pub delivery_time: u64,
    /// Number of times this message was delivered
    pub delivery_count: u64,
}

/// A consumer in a consumer group
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamConsumer {
    /// Consumer name
    pub name: Bytes,
    /// Pending entries for this consumer (message IDs)
    pub pending: HashSet<StreamEntryId>,
    /// Last time this consumer was seen (milliseconds since Unix epoch)
    pub seen_time: u64,
}

impl StreamConsumer {
    /// Create a new consumer
    pub fn new(name: Bytes) -> Self {
        Self {
            name,
            pending: HashSet::new(),
            seen_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }
}

/// A consumer group for a stream
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamConsumerGroup {
    /// Group name
    pub name: Bytes,
    /// Last delivered ID (entries after this are available to consumers)
    pub last_delivered_id: StreamEntryId,
    /// Pending entries list (PEL) - maps message ID to pending entry info
    pub pending: HashMap<StreamEntryId, StreamPendingEntry>,
    /// Consumers in this group
    pub consumers: HashMap<Bytes, StreamConsumer>,
    /// Number of entries read from the stream
    pub entries_read: Option<u64>,
}

impl StreamConsumerGroup {
    /// Create a new consumer group
    pub fn new(name: Bytes, last_delivered_id: StreamEntryId) -> Self {
        Self {
            name,
            last_delivered_id,
            pending: HashMap::new(),
            consumers: HashMap::new(),
            entries_read: None,
        }
    }

    /// Get or create a consumer
    pub fn get_or_create_consumer(&mut self, consumer_name: &Bytes) -> &mut StreamConsumer {
        self.consumers
            .entry(consumer_name.clone())
            .or_insert_with(|| StreamConsumer::new(consumer_name.clone()))
    }

    /// Add a pending entry
    pub fn add_pending(&mut self, id: StreamEntryId, consumer_name: &Bytes) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let pending_entry = StreamPendingEntry {
            id: id.clone(),
            consumer: consumer_name.clone(),
            delivery_time: now,
            delivery_count: 1,
        };

        self.pending.insert(id.clone(), pending_entry);

        let consumer = self.get_or_create_consumer(consumer_name);
        consumer.pending.insert(id);
        consumer.seen_time = now;
    }

    /// Acknowledge messages
    pub fn ack(&mut self, ids: &[StreamEntryId]) -> usize {
        let mut count = 0;
        for id in ids {
            if let Some(pending) = self.pending.remove(id) {
                if let Some(consumer) = self.consumers.get_mut(&pending.consumer) {
                    consumer.pending.remove(id);
                }
                count += 1;
            }
        }
        count
    }

    /// Get the number of pending entries
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Claim pending entries from one consumer to another
    /// Returns the IDs that were successfully claimed
    pub fn claim(
        &mut self,
        ids: &[StreamEntryId],
        new_consumer: &Bytes,
        min_idle_time: u64,
        new_delivery_time: Option<u64>,
        new_retry_count: Option<u64>,
        force: bool,
    ) -> Vec<StreamEntryId> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut claimed = Vec::new();

        for id in ids {
            let should_claim = if let Some(pe) = self.pending.get(id) {
                let idle_time = now.saturating_sub(pe.delivery_time);
                idle_time >= min_idle_time
            } else {
                force // Only claim non-existing entries if FORCE is set
            };

            if !should_claim {
                continue;
            }

            // Remove from old consumer's pending set
            if let Some(pe) = self.pending.get(id) {
                if let Some(old_consumer) = self.consumers.get_mut(&pe.consumer) {
                    old_consumer.pending.remove(id);
                }
            }

            // Determine delivery time and count
            let delivery_time = new_delivery_time.unwrap_or(now);
            let delivery_count = new_retry_count.unwrap_or_else(|| {
                self.pending
                    .get(id)
                    .map(|pe| pe.delivery_count + 1)
                    .unwrap_or(1)
            });

            // Create or update pending entry
            let new_pe = StreamPendingEntry {
                id: id.clone(),
                consumer: new_consumer.clone(),
                delivery_time,
                delivery_count,
            };

            self.pending.insert(id.clone(), new_pe);

            // Add to new consumer's pending set
            let consumer = self.get_or_create_consumer(new_consumer);
            consumer.pending.insert(id.clone());
            consumer.seen_time = now;

            claimed.push(id.clone());
        }

        claimed
    }

    /// Get pending entries for a specific consumer
    pub fn get_pending_for_consumer(&self, consumer_name: &Bytes) -> Vec<&StreamPendingEntry> {
        self.pending
            .values()
            .filter(|pe| pe.consumer == *consumer_name)
            .collect()
    }

    /// Get pending entries that have been idle for at least the specified time
    pub fn get_idle_pending(&self, min_idle_ms: u64) -> Vec<&StreamPendingEntry> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        self.pending
            .values()
            .filter(|pe| now.saturating_sub(pe.delivery_time) >= min_idle_ms)
            .collect()
    }

    /// Get the minimum and maximum pending entry IDs
    pub fn get_pending_id_range(&self) -> Option<(StreamEntryId, StreamEntryId)> {
        if self.pending.is_empty() {
            return None;
        }

        let mut min_id: Option<&StreamEntryId> = None;
        let mut max_id: Option<&StreamEntryId> = None;

        for id in self.pending.keys() {
            min_id = Some(match min_id {
                Some(current) if id < current => id,
                Some(current) => current,
                None => id,
            });
            max_id = Some(match max_id {
                Some(current) if id > current => id,
                Some(current) => current,
                None => id,
            });
        }

        Some((min_id?.clone(), max_id?.clone()))
    }

    /// Get count of pending entries per consumer
    pub fn get_pending_counts_by_consumer(&self) -> HashMap<Bytes, usize> {
        let mut counts = HashMap::new();
        for pe in self.pending.values() {
            *counts.entry(pe.consumer.clone()).or_insert(0) += 1;
        }
        counts
    }

    /// Delete a consumer and optionally transfer their pending entries
    /// Returns the number of pending entries that were deleted/transferred
    pub fn delete_consumer(&mut self, consumer_name: &Bytes, transfer_to: Option<&Bytes>) -> usize {
        // Get the consumer's pending entries
        let pending_ids: Vec<StreamEntryId> = self
            .consumers
            .get(consumer_name)
            .map(|c| c.pending.iter().cloned().collect())
            .unwrap_or_default();

        let count = pending_ids.len();

        if let Some(target_consumer) = transfer_to {
            // Transfer pending entries to another consumer
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            for id in &pending_ids {
                if let Some(pe) = self.pending.get_mut(id) {
                    pe.consumer = target_consumer.clone();
                }
            }

            // Add to target consumer's pending set
            let target = self.get_or_create_consumer(target_consumer);
            for id in pending_ids {
                target.pending.insert(id);
            }
            target.seen_time = now;
        } else {
            // Remove pending entries entirely
            for id in &pending_ids {
                self.pending.remove(id);
            }
        }

        // Remove the consumer
        self.consumers.remove(consumer_name);

        count
    }

    /// Increment the entries_read counter
    pub fn increment_entries_read(&mut self, count: u64) {
        self.entries_read = Some(self.entries_read.unwrap_or(0) + count);
    }

    /// Update the last delivered ID
    pub fn set_last_delivered_id(&mut self, id: StreamEntryId) {
        self.last_delivered_id = id;
    }
}

/// Redis Stream data structure
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Stream {
    /// Entries ordered by ID
    pub entries: BTreeMap<StreamEntryId, Vec<(Bytes, Bytes)>>,
    /// Last generated ID
    pub last_id: StreamEntryId,
    /// Number of entries
    pub length: usize,
    /// Consumer groups
    pub consumer_groups: HashMap<Bytes, StreamConsumerGroup>,
}

impl Default for Stream {
    fn default() -> Self {
        Self::new()
    }
}

impl Stream {
    /// Create a new empty stream
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            last_id: StreamEntryId::new(0, 0),
            length: 0,
            consumer_groups: HashMap::new(),
        }
    }

    /// Create a consumer group
    pub fn create_group(&mut self, name: Bytes, start_id: StreamEntryId) -> bool {
        if self.consumer_groups.contains_key(&name) {
            return false;
        }
        self.consumer_groups
            .insert(name.clone(), StreamConsumerGroup::new(name, start_id));
        true
    }

    /// Get a consumer group
    pub fn get_group(&self, name: &Bytes) -> Option<&StreamConsumerGroup> {
        self.consumer_groups.get(name)
    }

    /// Get a mutable consumer group
    pub fn get_group_mut(&mut self, name: &Bytes) -> Option<&mut StreamConsumerGroup> {
        self.consumer_groups.get_mut(name)
    }

    /// Delete a consumer group
    pub fn delete_group(&mut self, name: &Bytes) -> bool {
        self.consumer_groups.remove(name).is_some()
    }

    /// Create a consumer in a group
    pub fn create_consumer(&mut self, group_name: &Bytes, consumer_name: &Bytes) -> bool {
        if let Some(group) = self.consumer_groups.get_mut(group_name) {
            if group.consumers.contains_key(consumer_name) {
                return false;
            }
            group.consumers.insert(
                consumer_name.clone(),
                StreamConsumer::new(consumer_name.clone()),
            );
            true
        } else {
            false
        }
    }

    /// Delete a consumer from a group
    pub fn delete_consumer(&mut self, group_name: &Bytes, consumer_name: &Bytes) -> Option<usize> {
        if let Some(group) = self.consumer_groups.get_mut(group_name) {
            if let Some(consumer) = group.consumers.remove(consumer_name) {
                // Return the number of pending entries that were orphaned
                let pending_count = consumer.pending.len();
                // Remove the consumer's pending entries from the group PEL
                for id in &consumer.pending {
                    group.pending.remove(id);
                }
                return Some(pending_count);
            }
        }
        None
    }

    /// Set the last delivered ID for a group
    pub fn set_group_id(&mut self, group_name: &Bytes, id: StreamEntryId) -> bool {
        if let Some(group) = self.consumer_groups.get_mut(group_name) {
            group.last_delivered_id = id;
            true
        } else {
            false
        }
    }

    /// Add an entry to the stream
    pub fn add(
        &mut self,
        id: Option<StreamEntryId>,
        fields: Vec<(Bytes, Bytes)>,
    ) -> Option<StreamEntryId> {
        let new_id = match id {
            Some(id) => {
                // Ensure ID is greater than last ID
                if id <= self.last_id {
                    return None;
                }
                id
            }
            None => {
                // Auto-generate ID
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                if now_ms == self.last_id.ms {
                    StreamEntryId::new(now_ms, self.last_id.seq + 1)
                } else if now_ms > self.last_id.ms {
                    StreamEntryId::new(now_ms, 0)
                } else {
                    StreamEntryId::new(self.last_id.ms, self.last_id.seq + 1)
                }
            }
        };

        self.entries.insert(new_id.clone(), fields);
        self.last_id = new_id.clone();
        self.length += 1;
        Some(new_id)
    }

    /// Get entries in a range
    pub fn range(
        &self,
        start: &StreamEntryId,
        end: &StreamEntryId,
        count: Option<usize>,
    ) -> Vec<StreamEntry> {
        let mut result = Vec::new();
        for (id, fields) in self.entries.range(start..=end) {
            result.push(StreamEntry {
                id: id.clone(),
                fields: fields.clone(),
            });
            if let Some(c) = count {
                if result.len() >= c {
                    break;
                }
            }
        }
        result
    }

    /// Delete entries by ID
    pub fn delete(&mut self, ids: &[StreamEntryId]) -> usize {
        let mut count = 0;
        for id in ids {
            if self.entries.remove(id).is_some() {
                count += 1;
                self.length -= 1;
            }
        }
        count
    }

    /// Trim the stream to a maximum length
    pub fn trim(&mut self, maxlen: usize) -> usize {
        let mut removed = 0;
        while self.length > maxlen {
            if let Some((id, _)) = self
                .entries
                .iter()
                .next()
                .map(|(k, v)| (k.clone(), v.clone()))
            {
                self.entries.remove(&id);
                self.length -= 1;
                removed += 1;
            } else {
                break;
            }
        }
        removed
    }

    /// Trim the stream by removing entries with IDs less than the given minimum ID.
    /// Returns the number of removed entries.
    pub fn trim_by_minid(&mut self, minid: &StreamEntryId) -> usize {
        let mut removed = 0;
        let ids_to_remove: Vec<StreamEntryId> = self
            .entries
            .range(..minid)
            .map(|(id, _)| id.clone())
            .collect();

        for id in ids_to_remove {
            self.entries.remove(&id);
            self.length -= 1;
            removed += 1;
        }
        removed
    }
}
