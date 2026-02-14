//! Federated Query Executor
//!
//! Dispatches sub-queries to remote Ferrite nodes and external data sources
//! in parallel, collects results, and merges them according to the query plan.

use super::scatter_gather::{MergedResult, NodeResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::error;

// ---------------------------------------------------------------------------
// Foreign data source abstraction
// ---------------------------------------------------------------------------

/// Trait for pluggable external data sources (PostgreSQL, S3, HTTP, etc.).
#[async_trait::async_trait]
pub trait ForeignSource: Send + Sync {
    /// Source type name (e.g. "postgres", "s3", "http").
    fn source_type(&self) -> &str;

    /// Execute a query against this source, returning JSON rows.
    async fn execute(&self, query: &str) -> Result<Vec<serde_json::Value>, FederatedError>;

    /// Check whether the source is reachable.
    async fn health_check(&self) -> Result<(), FederatedError>;
}

/// Errors that can occur during federated query execution.
#[derive(Debug, Clone, thiserror::Error)]
pub enum FederatedError {
    #[error("node {node_id} unreachable: {reason}")]
    NodeUnreachable { node_id: String, reason: String },

    #[error("query timeout after {0:?}")]
    Timeout(Duration),

    #[error("source error: {0}")]
    SourceError(String),

    #[error("merge error: {0}")]
    MergeError(String),

    #[error("no nodes available")]
    NoNodes,
}

// ---------------------------------------------------------------------------
// Executor
// ---------------------------------------------------------------------------

/// Configuration for a federated node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederatedNodeConfig {
    /// Unique node identifier.
    pub node_id: String,
    /// Connection address.
    pub address: String,
    /// Whether this is a Ferrite node (vs foreign source).
    pub is_ferrite: bool,
    /// Query timeout.
    pub timeout: Duration,
    /// Weight for load balancing (higher = more traffic).
    pub weight: u32,
}

/// The federated query executor.
pub struct FederatedExecutor {
    /// Registered Ferrite nodes.
    nodes: RwLock<Vec<FederatedNodeConfig>>,
    /// Registered foreign data sources.
    foreign_sources: RwLock<HashMap<String, Arc<dyn ForeignSource>>>,
    /// Default query timeout.
    default_timeout: Duration,
    /// Statistics.
    stats: ExecutorStats,
}

/// Executor statistics.
#[derive(Debug, Default)]
pub struct ExecutorStats {
    pub queries_executed: std::sync::atomic::AtomicU64,
    pub nodes_queried: std::sync::atomic::AtomicU64,
    pub cache_hits: std::sync::atomic::AtomicU64,
    pub partial_results: std::sync::atomic::AtomicU64,
    pub errors: std::sync::atomic::AtomicU64,
}

/// A federated query execution plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederatedQueryPlan {
    /// Sub-queries to dispatch.
    pub sub_queries: Vec<SubQuery>,
    /// How to merge results.
    pub merge_strategy: MergeStrategy,
    /// Optional result cache TTL.
    pub cache_ttl: Option<Duration>,
}

/// A sub-query targeting a specific node or source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubQuery {
    /// Target node or source ID.
    pub target_id: String,
    /// The query string to execute.
    pub query: String,
    /// Optional predicate pushdown hints.
    pub pushdown_predicates: Vec<String>,
}

/// Strategy for merging sub-query results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergeStrategy {
    /// Simple concatenation.
    Concatenate,
    /// Union with deduplication.
    Union,
    /// Sort-merge on a specified column.
    SortMerge,
    /// Take first successful result.
    FirstSuccess,
}

impl FederatedExecutor {
    /// Create a new executor.
    pub fn new(default_timeout: Duration) -> Self {
        Self {
            nodes: RwLock::new(Vec::new()),
            foreign_sources: RwLock::new(HashMap::new()),
            default_timeout,
            stats: ExecutorStats::default(),
        }
    }

    /// Register a Ferrite node.
    pub async fn add_node(&self, config: FederatedNodeConfig) {
        self.nodes.write().await.push(config);
    }

    /// Remove a node by ID.
    pub async fn remove_node(&self, node_id: &str) {
        self.nodes.write().await.retain(|n| n.node_id != node_id);
    }

    /// Register a foreign data source.
    pub async fn add_foreign_source(&self, name: String, source: Arc<dyn ForeignSource>) {
        self.foreign_sources.write().await.insert(name, source);
    }

    /// Remove a foreign data source.
    pub async fn remove_foreign_source(&self, name: &str) {
        self.foreign_sources.write().await.remove(name);
    }

    /// Execute a federated query plan.
    pub async fn execute(&self, plan: &FederatedQueryPlan) -> Result<MergedResult, FederatedError> {
        use std::sync::atomic::Ordering;

        self.stats.queries_executed.fetch_add(1, Ordering::Relaxed);

        if plan.sub_queries.is_empty() {
            return Err(FederatedError::NoNodes);
        }

        // Dispatch all sub-queries in parallel.
        let mut handles = Vec::new();

        for sub_query in &plan.sub_queries {
            let target_id = sub_query.target_id.clone();
            let query = sub_query.query.clone();
            let timeout = self.default_timeout;

            // Check if it's a foreign source.
            let foreign = self.foreign_sources.read().await.get(&target_id).cloned();

            if let Some(source) = foreign {
                handles.push(tokio::spawn(async move {
                    let start = Instant::now();
                    match tokio::time::timeout(timeout, source.execute(&query)).await {
                        Ok(Ok(data)) => NodeResult {
                            node_id: target_id,
                            success: true,
                            row_count: data.len(),
                            data,
                            latency_ms: start.elapsed().as_millis() as u64,
                            error: None,
                        },
                        Ok(Err(e)) => NodeResult {
                            node_id: target_id,
                            success: false,
                            row_count: 0,
                            data: Vec::new(),
                            latency_ms: start.elapsed().as_millis() as u64,
                            error: Some(e.to_string()),
                        },
                        Err(_) => NodeResult {
                            node_id: target_id,
                            success: false,
                            row_count: 0,
                            data: Vec::new(),
                            latency_ms: timeout.as_millis() as u64,
                            error: Some("timeout".to_string()),
                        },
                    }
                }));
            } else {
                // Ferrite node - placeholder for actual RESP client.
                let tid = target_id.clone();
                handles.push(tokio::spawn(async move {
                    NodeResult {
                        node_id: tid,
                        success: false,
                        row_count: 0,
                        data: Vec::new(),
                        latency_ms: 0,
                        error: Some("ferrite node query not yet implemented".to_string()),
                    }
                }));
            }

            self.stats.nodes_queried.fetch_add(1, Ordering::Relaxed);
        }

        // Collect all results.
        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(e) => {
                    self.stats.errors.fetch_add(1, Ordering::Relaxed);
                    results.push(NodeResult {
                        node_id: "unknown".to_string(),
                        success: false,
                        row_count: 0,
                        data: Vec::new(),
                        latency_ms: 0,
                        error: Some(format!("task join error: {e}")),
                    });
                }
            }
        }

        // Merge.
        let merged = match plan.merge_strategy {
            MergeStrategy::Concatenate => super::scatter_gather::merge_concatenate(&results),
            MergeStrategy::Union => super::scatter_gather::merge_union(&results, "id"),
            MergeStrategy::FirstSuccess => {
                if let Some(first_ok) = results.iter().find(|r| r.success) {
                    super::scatter_gather::merge_concatenate(&[first_ok.clone()])
                } else {
                    super::scatter_gather::merge_concatenate(&results)
                }
            }
            MergeStrategy::SortMerge => {
                // Fallback to concatenate for now; full sort-merge needs column info.
                super::scatter_gather::merge_concatenate(&results)
            }
        };

        if merged.partial {
            self.stats.partial_results.fetch_add(1, Ordering::Relaxed);
        }

        Ok(merged)
    }

    /// List registered nodes.
    pub async fn list_nodes(&self) -> Vec<FederatedNodeConfig> {
        self.nodes.read().await.clone()
    }

    /// List registered foreign source names.
    pub async fn list_foreign_sources(&self) -> Vec<String> {
        self.foreign_sources.read().await.keys().cloned().collect()
    }
}

impl Default for FederatedExecutor {
    fn default() -> Self {
        Self::new(Duration::from_secs(10))
    }
}

// ---------------------------------------------------------------------------
// In-memory foreign source (for testing)
// ---------------------------------------------------------------------------

/// A simple in-memory foreign source for testing.
pub struct InMemorySource {
    data: Vec<serde_json::Value>,
}

impl InMemorySource {
    pub fn new(data: Vec<serde_json::Value>) -> Self {
        Self { data }
    }
}

#[async_trait::async_trait]
impl ForeignSource for InMemorySource {
    fn source_type(&self) -> &str {
        "memory"
    }

    async fn execute(&self, _query: &str) -> Result<Vec<serde_json::Value>, FederatedError> {
        Ok(self.data.clone())
    }

    async fn health_check(&self) -> Result<(), FederatedError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn execute_with_in_memory_source() {
        let executor = FederatedExecutor::new(Duration::from_secs(5));

        let source = Arc::new(InMemorySource::new(vec![
            serde_json::json!({"id": "1", "name": "Alice"}),
            serde_json::json!({"id": "2", "name": "Bob"}),
        ]));

        executor
            .add_foreign_source("mem1".to_string(), source)
            .await;

        let plan = FederatedQueryPlan {
            sub_queries: vec![SubQuery {
                target_id: "mem1".to_string(),
                query: "SELECT * FROM users".to_string(),
                pushdown_predicates: Vec::new(),
            }],
            merge_strategy: MergeStrategy::Concatenate,
            cache_ttl: None,
        };

        let result = executor.execute(&plan).await.unwrap();
        assert_eq!(result.total_rows, 2);
        assert!(!result.partial);
    }

    #[tokio::test]
    async fn execute_multiple_sources() {
        let executor = FederatedExecutor::new(Duration::from_secs(5));

        executor
            .add_foreign_source(
                "s1".to_string(),
                Arc::new(InMemorySource::new(vec![serde_json::json!({"id": "a"})])),
            )
            .await;
        executor
            .add_foreign_source(
                "s2".to_string(),
                Arc::new(InMemorySource::new(vec![serde_json::json!({"id": "b"})])),
            )
            .await;

        let plan = FederatedQueryPlan {
            sub_queries: vec![
                SubQuery {
                    target_id: "s1".to_string(),
                    query: "q".to_string(),
                    pushdown_predicates: Vec::new(),
                },
                SubQuery {
                    target_id: "s2".to_string(),
                    query: "q".to_string(),
                    pushdown_predicates: Vec::new(),
                },
            ],
            merge_strategy: MergeStrategy::Concatenate,
            cache_ttl: None,
        };

        let result = executor.execute(&plan).await.unwrap();
        assert_eq!(result.total_rows, 2);
        assert_eq!(result.nodes_queried, 2);
        assert_eq!(result.nodes_responded, 2);
    }

    #[tokio::test]
    async fn first_success_strategy() {
        let executor = FederatedExecutor::new(Duration::from_secs(5));

        executor
            .add_foreign_source(
                "s1".to_string(),
                Arc::new(InMemorySource::new(vec![
                    serde_json::json!({"id": "winner"}),
                ])),
            )
            .await;

        let plan = FederatedQueryPlan {
            sub_queries: vec![SubQuery {
                target_id: "s1".to_string(),
                query: "q".to_string(),
                pushdown_predicates: Vec::new(),
            }],
            merge_strategy: MergeStrategy::FirstSuccess,
            cache_ttl: None,
        };

        let result = executor.execute(&plan).await.unwrap();
        assert_eq!(result.total_rows, 1);
    }

    #[tokio::test]
    async fn node_management() {
        let executor = FederatedExecutor::new(Duration::from_secs(5));

        executor
            .add_node(FederatedNodeConfig {
                node_id: "n1".to_string(),
                address: "127.0.0.1:6379".to_string(),
                is_ferrite: true,
                timeout: Duration::from_secs(5),
                weight: 1,
            })
            .await;

        assert_eq!(executor.list_nodes().await.len(), 1);

        executor.remove_node("n1").await;
        assert!(executor.list_nodes().await.is_empty());
    }
}
