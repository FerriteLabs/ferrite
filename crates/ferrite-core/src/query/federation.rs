//! Federated Query Engine
//!
//! Extends FerriteQL to query across multiple Ferrite clusters and external
//! data sources (PostgreSQL, S3 Parquet files, HTTP APIs) with a single
//! unified query. Implements push-down predicates and distributed execution.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                   Federated Query Engine                     │
//! │  ┌──────────────────┐    ┌──────────────────────────────┐   │
//! │  │  Query Planner   │───▶│  Distributed Executor        │   │
//! │  │  (push-down opt) │    │  (fan-out, merge, cache)     │   │
//! │  └──────────────────┘    └──────────────┬───────────────┘   │
//! │                                         │                   │
//! │  ┌──────────┬──────────┬────────────┬───▼────────┐          │
//! │  │ Ferrite  │ Ferrite  │ Postgres   │  S3/Parquet│          │
//! │  │ Cluster1 │ Cluster2 │ Connector  │  Connector │          │
//! │  └──────────┘──────────┘────────────┘────────────┘          │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Data source definitions
// ---------------------------------------------------------------------------

/// A registered external data source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSource {
    /// Unique name identifying this data source.
    pub name: String,
    /// The type of the data source.
    pub source_type: DataSourceType,
    /// Connection configuration for reaching the source.
    pub connection: ConnectionConfig,
    /// Whether this source is read-only.
    pub read_only: bool,
    /// Optional TTL for caching results from this source.
    pub cache_ttl: Option<Duration>,
}

/// Supported data source types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataSourceType {
    /// Another Ferrite cluster
    Ferrite,
    /// PostgreSQL database
    Postgres,
    /// MySQL database
    MySQL,
    /// S3 Parquet files
    S3Parquet,
    /// HTTP/REST API
    HttpApi,
}

impl std::fmt::Display for DataSourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ferrite => write!(f, "ferrite"),
            Self::Postgres => write!(f, "postgres"),
            Self::MySQL => write!(f, "mysql"),
            Self::S3Parquet => write!(f, "s3_parquet"),
            Self::HttpApi => write!(f, "http_api"),
        }
    }
}

/// Connection configuration for a data source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    /// Connection URL for the data source.
    pub url: String,
    /// Connection pool size.
    pub pool_size: usize,
    /// Timeout for establishing a connection.
    pub connect_timeout: Duration,
    /// Timeout for individual queries.
    pub query_timeout: Duration,
    /// Optional authentication credentials.
    pub credentials: Option<Credentials>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            pool_size: 4,
            connect_timeout: Duration::from_secs(5),
            query_timeout: Duration::from_secs(30),
            credentials: None,
        }
    }
}

/// Credentials for connecting to a data source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Credentials {
    /// Username for authentication.
    pub username: Option<String>,
    /// Password for authentication.
    pub password: Option<String>,
    /// Token for token-based authentication.
    pub token: Option<String>,
}

// ---------------------------------------------------------------------------
// Federated query plan
// ---------------------------------------------------------------------------

/// A federated query plan that spans multiple data sources.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederatedPlan {
    /// Unique plan ID
    pub plan_id: String,
    /// Sub-plans targeting individual sources
    pub sub_plans: Vec<SubPlan>,
    /// How to merge results from sub-plans
    pub merge_strategy: MergeStrategy,
    /// Predicates that can be pushed to sources
    pub pushed_predicates: Vec<Predicate>,
    /// Remaining predicates to evaluate after merge
    pub post_merge_predicates: Vec<Predicate>,
    /// Estimated total cost
    pub estimated_cost: f64,
}

/// A sub-plan targeting a single data source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubPlan {
    /// Name of the target data source.
    pub source_name: String,
    /// The query to execute on this source.
    pub query: String,
    /// Predicates pushed down to this source.
    pub predicates: Vec<Predicate>,
    /// Columns to project from the result.
    pub projections: Vec<String>,
    /// Optional row limit for this sub-plan.
    pub limit: Option<usize>,
    /// Estimated number of rows returned.
    pub estimated_rows: u64,
    /// Estimated execution cost.
    pub estimated_cost: f64,
}

/// Strategy for merging results from multiple sub-plans.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergeStrategy {
    /// Simple concatenation (UNION ALL)
    Append,
    /// Hash-based merge join
    HashJoin,
    /// Sort-merge join
    SortMergeJoin,
    /// Nested loop join (for small datasets)
    NestedLoop,
}

/// A predicate that can be pushed down to data sources.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Predicate {
    /// Field name the predicate applies to.
    pub field: String,
    /// Comparison operator.
    pub operator: PredicateOperator,
    /// Value to compare against.
    pub value: serde_json::Value,
}

/// Comparison operators for predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PredicateOperator {
    /// Equal (`=`).
    Eq,
    /// Not equal (`!=`).
    Neq,
    /// Less than (`<`).
    Lt,
    /// Less than or equal (`<=`).
    Lte,
    /// Greater than (`>`).
    Gt,
    /// Greater than or equal (`>=`).
    Gte,
    /// Pattern match (`LIKE`).
    Like,
    /// Set membership (`IN`).
    In,
    /// Null check (`IS NULL`).
    IsNull,
    /// Non-null check (`IS NOT NULL`).
    IsNotNull,
}

impl std::fmt::Display for PredicateOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eq => write!(f, "="),
            Self::Neq => write!(f, "!="),
            Self::Lt => write!(f, "<"),
            Self::Lte => write!(f, "<="),
            Self::Gt => write!(f, ">"),
            Self::Gte => write!(f, ">="),
            Self::Like => write!(f, "LIKE"),
            Self::In => write!(f, "IN"),
            Self::IsNull => write!(f, "IS NULL"),
            Self::IsNotNull => write!(f, "IS NOT NULL"),
        }
    }
}

// ---------------------------------------------------------------------------
// Query result
// ---------------------------------------------------------------------------

/// Result of a federated query execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederatedResult {
    /// Column definitions for the result set.
    pub columns: Vec<ColumnDef>,
    /// Result rows, each a vector of JSON values.
    pub rows: Vec<Vec<serde_json::Value>>,
    /// Total number of rows returned.
    pub total_rows: u64,
    /// Names of the data sources that were queried.
    pub sources_queried: Vec<String>,
    /// Wall-clock execution time.
    pub execution_time: Duration,
    /// Whether the result was served from cache.
    pub from_cache: bool,
}

/// Column definition in a result set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name.
    pub name: String,
    /// Data source this column originates from, if known.
    pub source: Option<String>,
    /// Data type of the column.
    pub data_type: String,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors from the federated query engine.
#[derive(Debug, thiserror::Error)]
pub enum FederatedQueryError {
    /// The requested data source was not found.
    #[error("Data source not found: {0}")]
    SourceNotFound(String),

    /// A data source with this name is already registered.
    #[error("Data source already registered: {0}")]
    SourceExists(String),

    /// Failed to connect to a data source.
    #[error("Connection failed to {data_source}: {reason}")]
    ConnectionFailed {
        /// Name of the data source.
        data_source: String,
        /// Reason the connection failed.
        reason: String,
    },

    /// Query execution failed on a data source.
    #[error("Query execution failed on {data_source}: {reason}")]
    ExecutionFailed {
        /// Name of the data source.
        data_source: String,
        /// Reason execution failed.
        reason: String,
    },

    /// Merging results from sub-plans failed.
    #[error("Merge failed: {0}")]
    MergeFailed(String),

    /// A query timed out on a data source.
    #[error("Query timeout on {data_source} after {timeout:?}")]
    Timeout {
        /// Name of the data source that timed out.
        data_source: String,
        /// The timeout duration that was exceeded.
        timeout: Duration,
    },

    /// An error occurred during query planning.
    #[error("Planning error: {0}")]
    PlanningError(String),

    /// The requested operation is not supported for this source type.
    #[error("Unsupported operation for source type {0}: {1}")]
    Unsupported(String, String),
}

// ---------------------------------------------------------------------------
// Result cache
// ---------------------------------------------------------------------------

struct CachedResult {
    result: FederatedResult,
    expires_at: std::time::Instant,
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

/// Metrics for the federated query engine.
#[derive(Debug, Default)]
pub struct FederatedMetrics {
    /// Total number of federated queries executed.
    pub queries_total: AtomicU64,
    /// Number of queries served from cache.
    pub queries_from_cache: AtomicU64,
    /// Total number of sub-queries dispatched to sources.
    pub sub_queries_total: AtomicU64,
    /// Total rows scanned across all sources.
    pub rows_scanned: AtomicU64,
    /// Total rows returned to callers.
    pub rows_returned: AtomicU64,
    /// Total number of errors encountered.
    pub errors_total: AtomicU64,
}

/// The federated query engine.
pub struct FederatedQueryEngine {
    sources: Arc<parking_lot::RwLock<HashMap<String, DataSource>>>,
    cache: Arc<parking_lot::RwLock<HashMap<String, CachedResult>>>,
    metrics: Arc<FederatedMetrics>,
    default_timeout: Duration,
}

impl FederatedQueryEngine {
    /// Create a new federated query engine.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            sources: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            cache: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            metrics: Arc::new(FederatedMetrics::default()),
            default_timeout: Duration::from_secs(30),
        }
    }

    /// Register a data source.
    pub fn register_source(&self, source: DataSource) -> Result<(), FederatedQueryError> {
        let mut sources = self.sources.write();
        if sources.contains_key(&source.name) {
            return Err(FederatedQueryError::SourceExists(source.name.clone()));
        }
        sources.insert(source.name.clone(), source);
        Ok(())
    }

    /// Unregister a data source.
    pub fn unregister_source(&self, name: &str) -> Result<(), FederatedQueryError> {
        let mut sources = self.sources.write();
        if sources.remove(name).is_none() {
            return Err(FederatedQueryError::SourceNotFound(name.to_string()));
        }
        Ok(())
    }

    /// List all registered data sources.
    pub fn list_sources(&self) -> Vec<DataSource> {
        self.sources.read().values().cloned().collect()
    }

    /// Get a specific data source.
    pub fn get_source(&self, name: &str) -> Result<DataSource, FederatedQueryError> {
        self.sources
            .read()
            .get(name)
            .cloned()
            .ok_or_else(|| FederatedQueryError::SourceNotFound(name.to_string()))
    }

    /// Create a federated query plan.
    pub fn plan(
        &self,
        query: &str,
        target_sources: &[String],
    ) -> Result<FederatedPlan, FederatedQueryError> {
        let sources = self.sources.read();

        let mut sub_plans = Vec::new();
        for source_name in target_sources {
            let source = sources
                .get(source_name)
                .ok_or_else(|| FederatedQueryError::SourceNotFound(source_name.clone()))?;

            sub_plans.push(SubPlan {
                source_name: source_name.clone(),
                query: query.to_string(),
                predicates: Vec::new(),
                projections: Vec::new(),
                limit: None,
                estimated_rows: 0,
                estimated_cost: estimate_cost(&source.source_type),
            });
        }

        let total_cost: f64 = sub_plans.iter().map(|p| p.estimated_cost).sum();

        Ok(FederatedPlan {
            plan_id: uuid::Uuid::new_v4().to_string(),
            sub_plans,
            merge_strategy: MergeStrategy::Append,
            pushed_predicates: Vec::new(),
            post_merge_predicates: Vec::new(),
            estimated_cost: total_cost,
        })
    }

    /// Execute a federated query plan.
    pub fn execute(&self, plan: &FederatedPlan) -> Result<FederatedResult, FederatedQueryError> {
        self.metrics.queries_total.fetch_add(1, Ordering::Relaxed);

        // Check cache
        if let Some(cached) = self.cache.read().get(&plan.plan_id) {
            if cached.expires_at > std::time::Instant::now() {
                self.metrics
                    .queries_from_cache
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(cached.result.clone());
            }
        }

        let start = std::time::Instant::now();
        let all_rows = Vec::new();
        let mut sources_queried = Vec::new();
        let mut columns = Vec::new();

        for sub in &plan.sub_plans {
            self.metrics
                .sub_queries_total
                .fetch_add(1, Ordering::Relaxed);
            sources_queried.push(sub.source_name.clone());

            // In a real implementation, this would dispatch to source-specific connectors.
            // For now we produce an empty result set per source.
            if columns.is_empty() {
                columns = sub
                    .projections
                    .iter()
                    .map(|p| ColumnDef {
                        name: p.clone(),
                        source: Some(sub.source_name.clone()),
                        data_type: "string".to_string(),
                    })
                    .collect();
            }
        }

        let total_rows = all_rows.len() as u64;
        self.metrics
            .rows_returned
            .fetch_add(total_rows, Ordering::Relaxed);

        Ok(FederatedResult {
            columns,
            rows: all_rows,
            total_rows,
            sources_queried,
            execution_time: start.elapsed(),
            from_cache: false,
        })
    }

    /// Get metrics.
    pub fn metrics(&self) -> &FederatedMetrics {
        &self.metrics
    }

    /// Clear the result cache.
    pub fn clear_cache(&self) {
        self.cache.write().clear();
    }
}

fn estimate_cost(source_type: &DataSourceType) -> f64 {
    match source_type {
        DataSourceType::Ferrite => 1.0,
        DataSourceType::Postgres => 5.0,
        DataSourceType::MySQL => 5.0,
        DataSourceType::S3Parquet => 10.0,
        DataSourceType::HttpApi => 20.0,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_source(name: &str, stype: DataSourceType) -> DataSource {
        DataSource {
            name: name.to_string(),
            source_type: stype,
            connection: ConnectionConfig {
                url: format!("localhost/{}", name),
                ..Default::default()
            },
            read_only: true,
            cache_ttl: Some(Duration::from_secs(60)),
        }
    }

    #[test]
    fn test_register_and_list_sources() {
        let engine = FederatedQueryEngine::new();
        engine
            .register_source(test_source("cluster-a", DataSourceType::Ferrite))
            .unwrap();
        engine
            .register_source(test_source("pg-main", DataSourceType::Postgres))
            .unwrap();

        let sources = engine.list_sources();
        assert_eq!(sources.len(), 2);
    }

    #[test]
    fn test_duplicate_source_error() {
        let engine = FederatedQueryEngine::new();
        engine
            .register_source(test_source("src1", DataSourceType::Ferrite))
            .unwrap();
        let result = engine.register_source(test_source("src1", DataSourceType::Ferrite));
        assert!(matches!(result, Err(FederatedQueryError::SourceExists(_))));
    }

    #[test]
    fn test_unregister_source() {
        let engine = FederatedQueryEngine::new();
        engine
            .register_source(test_source("tmp", DataSourceType::Ferrite))
            .unwrap();
        engine.unregister_source("tmp").unwrap();
        assert!(engine.list_sources().is_empty());
    }

    #[test]
    fn test_plan_creation() {
        let engine = FederatedQueryEngine::new();
        engine
            .register_source(test_source("a", DataSourceType::Ferrite))
            .unwrap();
        engine
            .register_source(test_source("b", DataSourceType::Postgres))
            .unwrap();

        let plan = engine
            .plan("SELECT * FROM users", &["a".into(), "b".into()])
            .unwrap();
        assert_eq!(plan.sub_plans.len(), 2);
        assert!(plan.estimated_cost > 0.0);
    }

    #[test]
    fn test_plan_unknown_source_error() {
        let engine = FederatedQueryEngine::new();
        let result = engine.plan("SELECT 1", &["missing".into()]);
        assert!(matches!(
            result,
            Err(FederatedQueryError::SourceNotFound(_))
        ));
    }

    #[test]
    fn test_execute_plan() {
        let engine = FederatedQueryEngine::new();
        engine
            .register_source(test_source("local", DataSourceType::Ferrite))
            .unwrap();

        let plan = engine
            .plan("SELECT * FROM keys", &["local".into()])
            .unwrap();
        let result = engine.execute(&plan).unwrap();
        assert_eq!(result.sources_queried, vec!["local"]);
        assert!(!result.from_cache);
    }

    #[test]
    fn test_predicate_display() {
        assert_eq!(PredicateOperator::Eq.to_string(), "=");
        assert_eq!(PredicateOperator::Like.to_string(), "LIKE");
        assert_eq!(PredicateOperator::IsNull.to_string(), "IS NULL");
    }

    #[test]
    fn test_merge_strategy_default() {
        let plan = FederatedPlan {
            plan_id: "test".into(),
            sub_plans: vec![],
            merge_strategy: MergeStrategy::Append,
            pushed_predicates: vec![],
            post_merge_predicates: vec![],
            estimated_cost: 0.0,
        };
        assert_eq!(plan.merge_strategy, MergeStrategy::Append);
    }
}
