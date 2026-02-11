//! Routing decision types

use super::IndexType;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// A routing decision determining where and how to execute a query
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RouteDecision {
    /// Target for the query
    pub target: RouteTarget,
    /// Index to use
    pub index: IndexType,
    /// Whether to prefer local execution
    pub prefer_local: bool,
    /// Stale read tolerance
    #[serde(skip)]
    pub stale_tolerance: Option<Duration>,
    /// Shard hint
    pub shard_hint: Option<u32>,
    /// Priority (higher = more important)
    pub priority: u8,
    /// Whether this is cacheable
    pub cacheable: bool,
}

impl Default for RouteDecision {
    fn default() -> Self {
        Self {
            target: RouteTarget::Primary,
            index: IndexType::Hash,
            prefer_local: false,
            stale_tolerance: None,
            shard_hint: None,
            priority: 5,
            cacheable: true,
        }
    }
}

impl RouteDecision {
    /// Create a decision routing to primary
    pub fn primary() -> Self {
        Self {
            target: RouteTarget::Primary,
            ..Default::default()
        }
    }

    /// Create a decision routing to any replica
    pub fn replica() -> Self {
        Self {
            target: RouteTarget::AnyReplica,
            ..Default::default()
        }
    }

    /// Create a decision for a specific node
    pub fn node(node_id: String) -> Self {
        Self {
            target: RouteTarget::SpecificNode(node_id),
            ..Default::default()
        }
    }

    /// Set the index type
    pub fn with_index(mut self, index: IndexType) -> Self {
        self.index = index;
        self
    }

    /// Set local preference
    pub fn prefer_local(mut self) -> Self {
        self.prefer_local = true;
        self
    }

    /// Set stale tolerance
    pub fn allow_stale(mut self, duration: Duration) -> Self {
        self.stale_tolerance = Some(duration);
        self
    }

    /// Set shard hint
    pub fn with_shard(mut self, shard: u32) -> Self {
        self.shard_hint = Some(shard);
        self
    }

    /// Set priority
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }

    /// Set cacheable
    pub fn cacheable(mut self, cacheable: bool) -> Self {
        self.cacheable = cacheable;
        self
    }
}

/// Target for query routing
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RouteTarget {
    /// Route to primary/leader
    #[default]
    Primary,
    /// Route to any available replica
    AnyReplica,
    /// Route to a specific replica
    SpecificReplica(String),
    /// Route to a specific node
    SpecificNode(String),
    /// Prefer local node, fallback to others
    LocalPreferred,
    /// Use consistent hashing
    ConsistentHash,
    /// Broadcast to all nodes
    Broadcast,
}

impl RouteTarget {
    /// Check if this target requires the primary
    pub fn requires_primary(&self) -> bool {
        matches!(self, RouteTarget::Primary)
    }

    /// Check if this can use replicas
    pub fn allows_replica(&self) -> bool {
        matches!(
            self,
            RouteTarget::AnyReplica
                | RouteTarget::SpecificReplica(_)
                | RouteTarget::LocalPreferred
                | RouteTarget::ConsistentHash
        )
    }

    /// Get specific node ID if any
    pub fn node_id(&self) -> Option<&str> {
        match self {
            RouteTarget::SpecificReplica(id) | RouteTarget::SpecificNode(id) => Some(id),
            _ => None,
        }
    }
}

/// Context for making routing decisions
#[derive(Clone, Debug, Default)]
pub struct RoutingContext {
    /// Client ID for affinity
    pub client_id: Option<String>,
    /// Current transaction ID
    pub transaction_id: Option<u64>,
    /// Preferred shard
    pub preferred_shard: Option<u32>,
    /// Current node ID
    pub current_node: Option<String>,
    /// Request deadline
    pub deadline: Option<std::time::Instant>,
}

impl RoutingContext {
    /// Create a new routing context
    pub fn new() -> Self {
        Self::default()
    }

    /// Set client ID
    pub fn with_client(mut self, client_id: String) -> Self {
        self.client_id = Some(client_id);
        self
    }

    /// Set transaction ID
    pub fn with_transaction(mut self, txn_id: u64) -> Self {
        self.transaction_id = Some(txn_id);
        self
    }

    /// Set preferred shard
    pub fn with_shard(mut self, shard: u32) -> Self {
        self.preferred_shard = Some(shard);
        self
    }

    /// Set current node
    pub fn with_node(mut self, node_id: String) -> Self {
        self.current_node = Some(node_id);
        self
    }

    /// Set deadline
    pub fn with_deadline(mut self, deadline: std::time::Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_route_decision_default() {
        let decision = RouteDecision::default();
        assert_eq!(decision.target, RouteTarget::Primary);
        assert_eq!(decision.index, IndexType::Hash);
    }

    #[test]
    fn test_route_decision_builder() {
        let decision = RouteDecision::replica()
            .with_index(IndexType::BTree)
            .prefer_local()
            .with_priority(10);

        assert_eq!(decision.target, RouteTarget::AnyReplica);
        assert_eq!(decision.index, IndexType::BTree);
        assert!(decision.prefer_local);
        assert_eq!(decision.priority, 10);
    }

    #[test]
    fn test_route_target_properties() {
        assert!(RouteTarget::Primary.requires_primary());
        assert!(!RouteTarget::AnyReplica.requires_primary());

        assert!(RouteTarget::AnyReplica.allows_replica());
        assert!(!RouteTarget::Primary.allows_replica());
    }
}
