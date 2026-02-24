#![forbid(unsafe_code)]
//! Consumer group management for Kafka-compatible streaming.

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

/// State of a consumer group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupState {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead,
}

/// A member of a consumer group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMember {
    /// Unique member identifier.
    pub member_id: String,
    /// Client-supplied identifier.
    pub client_id: String,
    /// Partitions assigned to this member: `(topic, partition)`.
    pub assigned_partitions: Vec<(String, u32)>,
    /// Timestamp when the member joined.
    pub joined_at: DateTime<Utc>,
}

/// A consumer group with offset tracking and membership.
pub struct ConsumerGroup {
    /// Group identifier.
    pub group_id: String,
    /// Current members.
    pub members: Vec<GroupMember>,
    /// Generation counter (incremented on each rebalance).
    pub generation: u32,
    /// Protocol type (e.g. "consumer").
    pub protocol_type: String,
    /// Current group state.
    pub state: GroupState,
    /// Committed offsets keyed by `(topic, partition)`.
    pub offsets: DashMap<(String, u32), i64>,
}

impl ConsumerGroup {
    /// Create a new empty consumer group.
    pub fn new(group_id: String) -> Self {
        Self {
            group_id,
            members: Vec::new(),
            generation: 0,
            protocol_type: "consumer".into(),
            state: GroupState::Empty,
            offsets: DashMap::new(),
        }
    }

    /// Commit an offset for a (topic, partition) pair.
    pub fn commit_offset(&self, topic: String, partition: u32, offset: i64) {
        self.offsets.insert((topic, partition), offset);
    }

    /// Get the committed offset for a (topic, partition) pair.
    pub fn get_committed_offset(&self, topic: &str, partition: u32) -> Option<i64> {
        self.offsets
            .get(&(topic.to_string(), partition))
            .map(|r| *r)
    }

    /// Add a member and trigger a rebalance. Returns the new generation and
    /// the member's assigned partitions.
    pub fn join(&mut self, member: GroupMember) -> (u32, Vec<(String, u32)>) {
        let assignments = member.assigned_partitions.clone();
        self.members.push(member);
        self.generation += 1;
        self.state = GroupState::Stable;
        (self.generation, assignments)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_group() {
        let group = ConsumerGroup::new("test-group".into());
        assert_eq!(group.group_id, "test-group");
        assert_eq!(group.state, GroupState::Empty);
        assert_eq!(group.generation, 0);
        assert!(group.members.is_empty());
    }

    #[test]
    fn test_commit_and_get_offset() {
        let group = ConsumerGroup::new("g1".into());
        group.commit_offset("topic-a".into(), 0, 100);
        assert_eq!(group.get_committed_offset("topic-a", 0), Some(100));
        assert_eq!(group.get_committed_offset("topic-a", 1), None);
    }

    #[test]
    fn test_join_increments_generation() {
        let mut group = ConsumerGroup::new("g1".into());
        let member = GroupMember {
            member_id: "m1".into(),
            client_id: "c1".into(),
            assigned_partitions: vec![("t".into(), 0)],
            joined_at: Utc::now(),
        };
        let (gen, _) = group.join(member);
        assert_eq!(gen, 1);
        assert_eq!(group.state, GroupState::Stable);
        assert_eq!(group.members.len(), 1);
    }
}
