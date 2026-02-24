//! Federated Queries across Ferrite instances
//!
//! Query data across multiple Ferrite instances as if they were one logical
//! database. Supports cross-region scatter-gather execution, partial
//! aggregation, and global materialized views.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────┐
//! │                Federation Manager                     │
//! │  ┌────────────┐  ┌────────────┐  ┌────────────────┐ │
//! │  │  Query     │  │  Scatter   │  │  Global View   │ │
//! │  │  Router    │  │  Gather    │  │  Manager       │ │
//! │  └─────┬──────┘  └─────┬──────┘  └────────────────┘ │
//! └────────┼───────────────┼─────────────────────────────┘
//!          │               │
//!    ┌─────▼───┐    ┌──────▼────┐    ┌────────────┐
//!    │ Node A  │    │  Node B   │    │  Node C    │
//!    │ us-east │    │  eu-west  │    │  ap-south  │
//!    └─────────┘    └───────────┘    └────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use ferrite::federation::{FederationManager, FederationConfig, FederatedNode};
//!
//! let mut manager = FederationManager::new(FederationConfig::default());
//! manager.add_node(FederatedNode::new("us-east", "10.0.1.1:6379"))?;
//! manager.add_node(FederatedNode::new("eu-west", "10.0.2.1:6379"))?;
//!
//! // Query across all nodes
//! let plan = manager.plan_query("SELECT * FROM users:* WHERE $.active = true")?;
//! ```

#![allow(dead_code)]
pub mod executor;
pub mod planner;
pub mod scatter_gather;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the federation manager.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederationConfig {
    /// Timeout for cross-node queries.
    pub query_timeout: Duration,
    /// Maximum number of nodes in the federation.
    pub max_nodes: usize,
    /// Enable partial results when some nodes are unavailable.
    pub allow_partial_results: bool,
    /// Maximum concurrent cross-node requests.
    pub max_concurrent_requests: usize,
    /// Enable query result caching.
    pub cache_results: bool,
    /// Cache TTL for federated query results.
    pub cache_ttl: Duration,
    /// Retry count for failed node requests.
    pub retry_count: u32,
}

impl Default for FederationConfig {
    fn default() -> Self {
        Self {
            query_timeout: Duration::from_secs(30),
            max_nodes: 100,
            allow_partial_results: true,
            max_concurrent_requests: 10,
            cache_results: true,
            cache_ttl: Duration::from_secs(60),
            retry_count: 2,
        }
    }
}

// ---------------------------------------------------------------------------
// Node representation
// ---------------------------------------------------------------------------

/// A node in the federation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederatedNode {
    /// Unique node identifier.
    pub id: String,
    /// Node address (host:port).
    pub address: String,
    /// Node region (for latency-aware routing).
    pub region: Option<String>,
    /// Node health status.
    pub status: NodeStatus,
    /// Estimated latency to this node (milliseconds).
    pub latency_ms: Option<u64>,
    /// Tags for filtering.
    pub tags: HashMap<String, String>,
}

impl FederatedNode {
    pub fn new(id: &str, address: &str) -> Self {
        Self {
            id: id.to_string(),
            address: address.to_string(),
            region: None,
            status: NodeStatus::Unknown,
            latency_ms: None,
            tags: HashMap::new(),
        }
    }

    pub fn with_region(mut self, region: &str) -> Self {
        self.region = Some(region.to_string());
        self
    }
}

/// Health status of a federated node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Node is healthy and responding.
    Healthy,
    /// Node is responding but degraded.
    Degraded,
    /// Node is unreachable.
    Unreachable,
    /// Status is unknown (not yet checked).
    Unknown,
}

impl Default for NodeStatus {
    fn default() -> Self {
        Self::Unknown
    }
}

// ---------------------------------------------------------------------------
// Query planning
// ---------------------------------------------------------------------------

/// A federated query execution plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPlan {
    /// The original query.
    pub original_query: String,
    /// Per-node sub-queries to execute.
    pub node_queries: Vec<NodeQuery>,
    /// How to merge results from nodes.
    pub merge_strategy: MergeStrategy,
    /// Estimated cost.
    pub estimated_cost: QueryCost,
}

/// A query to send to a specific node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeQuery {
    pub node_id: String,
    pub query: String,
    pub priority: u32,
}

/// Strategy for merging results from multiple nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergeStrategy {
    /// Concatenate all results.
    Concatenate,
    /// Union and deduplicate.
    Union,
    /// Merge and re-sort.
    Sort,
    /// Merge partial aggregations.
    Aggregate,
    /// Take first successful response.
    FirstSuccess,
}

impl Default for MergeStrategy {
    fn default() -> Self {
        Self::Concatenate
    }
}

/// Estimated cost of a federated query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryCost {
    /// Number of nodes involved.
    pub nodes_involved: usize,
    /// Estimated total network bytes.
    pub estimated_bytes: u64,
    /// Estimated latency (ms).
    pub estimated_latency_ms: u64,
}

// ---------------------------------------------------------------------------
// Federation Manager
// ---------------------------------------------------------------------------

/// Manages a federation of Ferrite instances.
pub struct FederationManager {
    config: FederationConfig,
    nodes: RwLock<HashMap<String, FederatedNode>>,
    stats: FederationStats,
}

#[derive(Debug, Default)]
struct FederationStats {
    queries_executed: AtomicU64,
    queries_failed: AtomicU64,
    total_nodes_queried: AtomicU64,
    partial_results_returned: AtomicU64,
}

impl FederationManager {
    /// Creates a new federation manager.
    pub fn new(config: FederationConfig) -> Self {
        Self {
            config,
            nodes: RwLock::new(HashMap::new()),
            stats: FederationStats::default(),
        }
    }

    /// Adds a node to the federation.
    pub fn add_node(&self, node: FederatedNode) -> Result<(), FederationError> {
        let mut nodes = self.nodes.write();
        if nodes.len() >= self.config.max_nodes {
            return Err(FederationError::MaxNodesReached(self.config.max_nodes));
        }
        if nodes.contains_key(&node.id) {
            return Err(FederationError::NodeAlreadyExists(node.id.clone()));
        }
        nodes.insert(node.id.clone(), node);
        Ok(())
    }

    /// Removes a node from the federation.
    pub fn remove_node(&self, node_id: &str) -> Result<(), FederationError> {
        if self.nodes.write().remove(node_id).is_none() {
            return Err(FederationError::NodeNotFound(node_id.to_string()));
        }
        Ok(())
    }

    /// Updates a node's status.
    pub fn update_node_status(
        &self,
        node_id: &str,
        status: NodeStatus,
    ) -> Result<(), FederationError> {
        let mut nodes = self.nodes.write();
        let node = nodes
            .get_mut(node_id)
            .ok_or_else(|| FederationError::NodeNotFound(node_id.to_string()))?;
        node.status = status;
        Ok(())
    }

    /// Returns all healthy nodes.
    pub fn healthy_nodes(&self) -> Vec<FederatedNode> {
        self.nodes
            .read()
            .values()
            .filter(|n| n.status == NodeStatus::Healthy || n.status == NodeStatus::Unknown)
            .cloned()
            .collect()
    }

    /// Plans a federated query across available nodes.
    pub fn plan_query(&self, query: &str) -> Result<QueryPlan, FederationError> {
        let nodes = self.healthy_nodes();
        if nodes.is_empty() {
            return Err(FederationError::NoAvailableNodes);
        }

        let node_queries: Vec<NodeQuery> = nodes
            .iter()
            .enumerate()
            .map(|(i, node)| NodeQuery {
                node_id: node.id.clone(),
                query: query.to_string(),
                priority: i as u32,
            })
            .collect();

        let estimated_cost = QueryCost {
            nodes_involved: node_queries.len(),
            estimated_bytes: 0,
            estimated_latency_ms: nodes
                .iter()
                .filter_map(|n| n.latency_ms)
                .max()
                .unwrap_or(100),
        };

        Ok(QueryPlan {
            original_query: query.to_string(),
            node_queries,
            merge_strategy: MergeStrategy::Concatenate,
            estimated_cost,
        })
    }

    /// Returns the number of nodes in the federation.
    pub fn node_count(&self) -> usize {
        self.nodes.read().len()
    }

    /// Returns all nodes.
    pub fn list_nodes(&self) -> Vec<FederatedNode> {
        self.nodes.read().values().cloned().collect()
    }

    /// Returns federation statistics.
    pub fn stats_snapshot(&self) -> FederationStatsSnapshot {
        FederationStatsSnapshot {
            queries_executed: self.stats.queries_executed.load(Ordering::Relaxed),
            queries_failed: self.stats.queries_failed.load(Ordering::Relaxed),
            total_nodes_queried: self.stats.total_nodes_queried.load(Ordering::Relaxed),
            partial_results_returned: self.stats.partial_results_returned.load(Ordering::Relaxed),
            active_nodes: self.healthy_nodes().len() as u64,
            total_nodes: self.node_count() as u64,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederationStatsSnapshot {
    pub queries_executed: u64,
    pub queries_failed: u64,
    pub total_nodes_queried: u64,
    pub partial_results_returned: u64,
    pub active_nodes: u64,
    pub total_nodes: u64,
}

// ---------------------------------------------------------------------------
// Global materialized view
// ---------------------------------------------------------------------------

/// A global materialized view across federation nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalView {
    /// View name.
    pub name: String,
    /// Query that defines the view.
    pub query: String,
    /// Refresh strategy.
    pub refresh: ViewRefreshStrategy,
    /// Nodes that contribute to this view.
    pub source_nodes: Vec<String>,
}

/// How often to refresh a global view.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ViewRefreshStrategy {
    /// Refresh on every read.
    OnRead,
    /// Refresh at fixed intervals.
    Periodic,
    /// Refresh when underlying data changes.
    OnChange,
    /// Manual refresh only.
    Manual,
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, thiserror::Error)]
pub enum FederationError {
    #[error("no available nodes in federation")]
    NoAvailableNodes,

    #[error("node not found: {0}")]
    NodeNotFound(String),

    #[error("node already exists: {0}")]
    NodeAlreadyExists(String),

    #[error("maximum nodes reached: {0}")]
    MaxNodesReached(usize),

    #[error("query timeout after {0:?}")]
    QueryTimeout(Duration),

    #[error("partial results: {available}/{total} nodes responded")]
    PartialResults { available: usize, total: usize },

    #[error("federation query failed: {0}")]
    QueryFailed(String),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = FederationConfig::default();
        assert_eq!(config.max_nodes, 100);
        assert!(config.allow_partial_results);
        assert!(config.cache_results);
    }

    #[test]
    fn test_add_and_remove_nodes() {
        let manager = FederationManager::new(FederationConfig::default());

        manager
            .add_node(FederatedNode::new("node-1", "10.0.1.1:6379"))
            .unwrap();
        manager
            .add_node(FederatedNode::new("node-2", "10.0.2.1:6379"))
            .unwrap();
        assert_eq!(manager.node_count(), 2);

        manager.remove_node("node-1").unwrap();
        assert_eq!(manager.node_count(), 1);
    }

    #[test]
    fn test_duplicate_node_error() {
        let manager = FederationManager::new(FederationConfig::default());
        manager
            .add_node(FederatedNode::new("node-1", "addr"))
            .unwrap();

        let result = manager.add_node(FederatedNode::new("node-1", "addr2"));
        assert!(matches!(result, Err(FederationError::NodeAlreadyExists(_))));
    }

    #[test]
    fn test_max_nodes() {
        let manager = FederationManager::new(FederationConfig {
            max_nodes: 2,
            ..Default::default()
        });

        manager.add_node(FederatedNode::new("n1", "a1")).unwrap();
        manager.add_node(FederatedNode::new("n2", "a2")).unwrap();

        let result = manager.add_node(FederatedNode::new("n3", "a3"));
        assert!(matches!(result, Err(FederationError::MaxNodesReached(2))));
    }

    #[test]
    fn test_plan_query() {
        let manager = FederationManager::new(FederationConfig::default());
        manager
            .add_node(FederatedNode::new("n1", "10.0.1.1:6379"))
            .unwrap();
        manager
            .add_node(FederatedNode::new("n2", "10.0.2.1:6379"))
            .unwrap();

        let plan = manager.plan_query("SELECT * FROM users:*").unwrap();

        assert_eq!(plan.node_queries.len(), 2);
        assert_eq!(plan.estimated_cost.nodes_involved, 2);
    }

    #[test]
    fn test_plan_query_no_nodes() {
        let manager = FederationManager::new(FederationConfig::default());
        let result = manager.plan_query("SELECT * FROM users:*");
        assert!(matches!(result, Err(FederationError::NoAvailableNodes)));
    }

    #[test]
    fn test_healthy_nodes_filter() {
        let manager = FederationManager::new(FederationConfig::default());
        manager.add_node(FederatedNode::new("n1", "a1")).unwrap();
        manager.add_node(FederatedNode::new("n2", "a2")).unwrap();

        manager
            .update_node_status("n1", NodeStatus::Unreachable)
            .unwrap();

        let healthy = manager.healthy_nodes();
        assert_eq!(healthy.len(), 1);
        assert_eq!(healthy[0].id, "n2");
    }

    #[test]
    fn test_node_with_region() {
        let node = FederatedNode::new("n1", "addr").with_region("us-east-1");
        assert_eq!(node.region.unwrap(), "us-east-1");
    }

    #[test]
    fn test_stats_snapshot() {
        let manager = FederationManager::new(FederationConfig::default());
        manager.add_node(FederatedNode::new("n1", "a1")).unwrap();

        let stats = manager.stats_snapshot();
        assert_eq!(stats.total_nodes, 1);
        assert_eq!(stats.queries_executed, 0);
    }
}
