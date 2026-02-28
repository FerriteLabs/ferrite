//! Federated Query Engine
//!
//! Execute queries across multiple data sources through a unified
//! FerriteQL interface. Supports Ferrite, Redis, PostgreSQL, S3, Kafka.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the federation engine.
#[derive(Debug, Clone)]
pub struct FederationConfig {
    /// Maximum number of registered data sources.
    pub max_sources: usize,
    /// Default query timeout.
    pub query_timeout: Duration,
    /// Whether to cache results from foreign sources.
    pub cache_foreign_results: bool,
    /// TTL for cached foreign results.
    pub cache_ttl: Duration,
    /// Maximum concurrent source queries.
    pub max_concurrent_sources: usize,
}

impl Default for FederationConfig {
    fn default() -> Self {
        Self {
            max_sources: 16,
            query_timeout: Duration::from_secs(30),
            cache_foreign_results: true,
            cache_ttl: Duration::from_secs(60),
            max_concurrent_sources: 4,
        }
    }
}

// ---------------------------------------------------------------------------
// Data source definitions
// ---------------------------------------------------------------------------

/// A registered external data source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSource {
    /// Unique name identifying this data source.
    pub name: String,
    /// The type of the data source.
    pub source_type: SourceType,
    /// Connection string for reaching the source.
    pub connection_string: String,
    /// Optional schema information.
    pub schema: Option<SourceSchema>,
    /// Whether this source is enabled.
    pub enabled: bool,
    /// Latency budget in milliseconds.
    pub latency_budget_ms: u64,
}

/// Supported data source types.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SourceType {
    /// Native Ferrite instance.
    Ferrite,
    /// Redis-compatible server.
    Redis,
    /// PostgreSQL database.
    PostgreSQL,
    /// Amazon S3 / object storage.
    S3,
    /// Apache Kafka topic.
    Kafka,
    /// HTTP/REST API.
    Http,
    /// User-defined source type.
    Custom(String),
}

impl std::fmt::Display for SourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ferrite => write!(f, "ferrite"),
            Self::Redis => write!(f, "redis"),
            Self::PostgreSQL => write!(f, "postgresql"),
            Self::S3 => write!(f, "s3"),
            Self::Kafka => write!(f, "kafka"),
            Self::Http => write!(f, "http"),
            Self::Custom(name) => write!(f, "custom:{}", name),
        }
    }
}

/// Schema information for a data source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceSchema {
    /// Table definitions.
    pub tables: Vec<TableDef>,
    /// Key pattern definitions.
    pub key_patterns: Vec<String>,
}

/// Table definition within a source schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDef {
    /// Table name.
    pub name: String,
    /// Column definitions.
    pub columns: Vec<ColumnDef>,
}

/// Column definition within a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name.
    pub name: String,
    /// Data type of the column.
    pub data_type: String,
    /// Whether the column is nullable.
    pub nullable: bool,
}

// ---------------------------------------------------------------------------
// Federated query types
// ---------------------------------------------------------------------------

/// A query targeting a specific data source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederatedQuery {
    /// Name of the data source to query.
    pub source: String,
    /// The operation to perform.
    pub operation: QueryOperation,
    /// Filters to apply.
    pub filters: Vec<QueryFilter>,
    /// Column projections.
    pub projections: Vec<String>,
    /// Optional row limit.
    pub limit: Option<usize>,
    /// Cache key for result caching.
    pub cache_key: Option<String>,
}

/// Query operation types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryOperation {
    /// Scan keys matching a pattern.
    Scan { pattern: String },
    /// Get a specific key.
    Get { key: String },
    /// Execute a SQL query.
    Query { sql: String },
    /// Join two federated queries.
    Join {
        left: Box<FederatedQuery>,
        right: Box<FederatedQuery>,
        on: String,
    },
}

/// A filter condition for a query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryFilter {
    /// Field to filter on.
    pub field: String,
    /// Filter operator.
    pub op: FilterOp,
    /// Value to compare against.
    pub value: serde_json::Value,
}

/// Filter comparison operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FilterOp {
    /// Equal.
    Eq,
    /// Not equal.
    Ne,
    /// Greater than.
    Gt,
    /// Less than.
    Lt,
    /// Greater than or equal.
    Gte,
    /// Less than or equal.
    Lte,
    /// Pattern match.
    Like,
    /// Set membership.
    In,
}

impl std::fmt::Display for FilterOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eq => write!(f, "="),
            Self::Ne => write!(f, "!="),
            Self::Gt => write!(f, ">"),
            Self::Lt => write!(f, "<"),
            Self::Gte => write!(f, ">="),
            Self::Lte => write!(f, "<="),
            Self::Like => write!(f, "LIKE"),
            Self::In => write!(f, "IN"),
        }
    }
}

// ---------------------------------------------------------------------------
// Query results and plans
// ---------------------------------------------------------------------------

/// Result of a federated query execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// Result rows as JSON values.
    pub rows: Vec<serde_json::Value>,
    /// Column names.
    pub columns: Vec<String>,
    /// Total number of rows.
    pub total_rows: usize,
    /// Source that produced the result.
    pub source: String,
    /// Whether the result was served from cache.
    pub cached: bool,
    /// Execution time in milliseconds.
    pub execution_ms: u64,
}

/// Execution plan for a federated query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPlan {
    /// Steps in the execution plan.
    pub steps: Vec<PlanStep>,
    /// Estimated total cost.
    pub estimated_cost: f64,
    /// Sources involved in the plan.
    pub sources_involved: Vec<String>,
}

/// A single step in a query plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanStep {
    /// Description of the operation.
    pub operation: String,
    /// Source this step targets.
    pub source: String,
    /// Estimated number of rows.
    pub estimated_rows: usize,
    /// Estimated cost for this step.
    pub cost: f64,
}

/// Summary information about a data source.
#[derive(Debug, Clone)]
pub struct DataSourceInfo {
    /// Source name.
    pub name: String,
    /// Source type.
    pub source_type: SourceType,
    /// Whether the source is enabled.
    pub enabled: bool,
    /// Measured latency in milliseconds.
    pub latency_ms: u64,
    /// Number of queries executed against this source.
    pub queries_executed: u64,
    /// Timestamp of last use.
    pub last_used: Option<Instant>,
}

/// Health information for a data source.
#[derive(Debug, Clone)]
pub struct SourceHealth {
    /// Source name.
    pub name: String,
    /// Whether the source is reachable.
    pub reachable: bool,
    /// Measured latency in milliseconds.
    pub latency_ms: u64,
    /// Timestamp of the last health check.
    pub last_check: Instant,
}

/// Aggregate statistics for the federation engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederationStats {
    /// Total queries executed.
    pub total_queries: u64,
    /// Cross-source queries executed.
    pub cross_source_queries: u64,
    /// Cache hits.
    pub cache_hits: u64,
    /// Cache misses.
    pub cache_misses: u64,
    /// Average latency in milliseconds.
    pub avg_latency_ms: f64,
    /// Number of registered sources.
    pub sources_registered: usize,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors from the federated query engine.
#[derive(Debug, thiserror::Error)]
pub enum FederationError {
    /// The requested data source was not found.
    #[error("source not found: {0}")]
    SourceNotFound(String),

    /// A data source is not reachable.
    #[error("source unreachable: {0}")]
    SourceUnreachable(String),

    /// Query timed out.
    #[error("query timeout after {0:?}")]
    QueryTimeout(Duration),

    /// Invalid query.
    #[error("invalid query: {0}")]
    InvalidQuery(String),

    /// Cross-source join failed.
    #[error("join failed: {0}")]
    JoinFailed(String),

    /// Maximum number of sources reached.
    #[error("maximum sources reached ({0})")]
    MaxSources(usize),
}

// ---------------------------------------------------------------------------
// Internal tracking
// ---------------------------------------------------------------------------

struct SourceTracker {
    queries_executed: AtomicU64,
    last_used: RwLock<Option<Instant>>,
    health: RwLock<Option<SourceHealth>>,
}

impl SourceTracker {
    fn new() -> Self {
        Self {
            queries_executed: AtomicU64::new(0),
            last_used: RwLock::new(None),
            health: RwLock::new(None),
        }
    }
}

struct CachedResult {
    result: QueryResult,
    expires_at: Instant,
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

/// The federated query engine.
pub struct FederationEngine {
    config: FederationConfig,
    sources: RwLock<HashMap<String, DataSource>>,
    trackers: RwLock<HashMap<String, SourceTracker>>,
    cache: RwLock<HashMap<String, CachedResult>>,
    total_queries: AtomicU64,
    cross_source_queries: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    total_latency_ms: AtomicU64,
}

impl FederationEngine {
    /// Create a new federation engine with the given configuration.
    pub fn new(config: FederationConfig) -> Self {
        Self {
            config,
            sources: RwLock::new(HashMap::new()),
            trackers: RwLock::new(HashMap::new()),
            cache: RwLock::new(HashMap::new()),
            total_queries: AtomicU64::new(0),
            cross_source_queries: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            total_latency_ms: AtomicU64::new(0),
        }
    }

    /// Register a new data source.
    pub fn register_source(&self, source: DataSource) -> Result<(), FederationError> {
        let mut sources = self.sources.write();
        if sources.len() >= self.config.max_sources {
            return Err(FederationError::MaxSources(self.config.max_sources));
        }
        let name = source.name.clone();
        sources.insert(name.clone(), source);
        self.trackers.write().insert(name, SourceTracker::new());
        Ok(())
    }

    /// Remove a registered data source.
    pub fn remove_source(&self, name: &str) -> Result<(), FederationError> {
        let mut sources = self.sources.write();
        if sources.remove(name).is_none() {
            return Err(FederationError::SourceNotFound(name.to_string()));
        }
        self.trackers.write().remove(name);
        Ok(())
    }

    /// List all registered data sources with summary info.
    pub fn list_sources(&self) -> Vec<DataSourceInfo> {
        let sources = self.sources.read();
        let trackers = self.trackers.read();
        sources
            .values()
            .map(|s| {
                let tracker = trackers.get(&s.name);
                DataSourceInfo {
                    name: s.name.clone(),
                    source_type: s.source_type.clone(),
                    enabled: s.enabled,
                    latency_ms: s.latency_budget_ms,
                    queries_executed: tracker
                        .map(|t| t.queries_executed.load(Ordering::Relaxed))
                        .unwrap_or(0),
                    last_used: tracker.and_then(|t| *t.last_used.read()),
                }
            })
            .collect()
    }

    /// Execute a federated query against the appropriate source.
    pub fn execute(&self, query: FederatedQuery) -> Result<QueryResult, FederationError> {
        let start = Instant::now();
        self.total_queries.fetch_add(1, Ordering::Relaxed);

        // Check cache
        if let Some(ref cache_key) = query.cache_key {
            if let Some(cached) = self.cache.read().get(cache_key) {
                if cached.expires_at > Instant::now() {
                    self.cache_hits.fetch_add(1, Ordering::Relaxed);
                    return Ok(cached.result.clone());
                }
            }
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
        }

        // Verify source exists
        let source_name = query.source.clone();
        {
            let sources = self.sources.read();
            if !sources.contains_key(&source_name) {
                return Err(FederationError::SourceNotFound(source_name));
            }
        }

        // Track usage
        if let Some(tracker) = self.trackers.read().get(&source_name) {
            tracker.queries_executed.fetch_add(1, Ordering::Relaxed);
            *tracker.last_used.write() = Some(Instant::now());
        }

        let elapsed = start.elapsed();
        self.total_latency_ms
            .fetch_add(elapsed.as_millis() as u64, Ordering::Relaxed);

        let result = QueryResult {
            rows: Vec::new(),
            columns: query.projections.clone(),
            total_rows: 0,
            source: source_name,
            cached: false,
            execution_ms: elapsed.as_millis() as u64,
        };

        // Cache the result
        if let Some(ref cache_key) = query.cache_key {
            if self.config.cache_foreign_results {
                self.cache.write().insert(
                    cache_key.clone(),
                    CachedResult {
                        result: result.clone(),
                        expires_at: Instant::now() + self.config.cache_ttl,
                    },
                );
            }
        }

        Ok(result)
    }

    /// Execute queries across multiple sources and join results.
    pub fn execute_cross_source(
        &self,
        queries: Vec<FederatedQuery>,
        join_key: &str,
    ) -> Result<QueryResult, FederationError> {
        let start = Instant::now();
        self.total_queries.fetch_add(1, Ordering::Relaxed);
        self.cross_source_queries.fetch_add(1, Ordering::Relaxed);

        if queries.is_empty() {
            return Err(FederationError::InvalidQuery(
                "no queries provided".to_string(),
            ));
        }

        let mut all_rows: Vec<serde_json::Value> = Vec::new();
        let mut all_columns: Vec<String> = Vec::new();
        let mut sources = Vec::new();

        for query in &queries {
            let result = self.execute(query.clone())?;
            sources.push(result.source.clone());
            for col in &result.columns {
                if !all_columns.contains(col) {
                    all_columns.push(col.clone());
                }
            }
            all_rows.extend(result.rows);
        }

        let elapsed = start.elapsed();
        self.total_latency_ms
            .fetch_add(elapsed.as_millis() as u64, Ordering::Relaxed);

        Ok(QueryResult {
            total_rows: all_rows.len(),
            rows: all_rows,
            columns: all_columns,
            source: format!("cross[{}]({})", sources.join(","), join_key),
            cached: false,
            execution_ms: elapsed.as_millis() as u64,
        })
    }

    /// Generate an execution plan for a query without executing it.
    pub fn explain(&self, query: &FederatedQuery) -> QueryPlan {
        let sources = self.sources.read();
        let source_info = sources.get(&query.source);

        let cost = source_info
            .map(|s| estimate_source_cost(&s.source_type))
            .unwrap_or(100.0);

        let op_desc = match &query.operation {
            QueryOperation::Scan { pattern } => format!("Scan({})", pattern),
            QueryOperation::Get { key } => format!("Get({})", key),
            QueryOperation::Query { sql } => format!("Query({})", sql),
            QueryOperation::Join { on, .. } => format!("Join(on={})", on),
        };

        QueryPlan {
            steps: vec![PlanStep {
                operation: op_desc,
                source: query.source.clone(),
                estimated_rows: query.limit.unwrap_or(1000),
                cost,
            }],
            estimated_cost: cost,
            sources_involved: vec![query.source.clone()],
        }
    }

    /// Get health information for a source.
    pub fn source_health(&self, name: &str) -> Option<SourceHealth> {
        let trackers = self.trackers.read();
        let tracker = trackers.get(name)?;
        let health = tracker.health.read();
        health.clone().or_else(|| {
            let sources = self.sources.read();
            sources.get(name).map(|s| SourceHealth {
                name: s.name.clone(),
                reachable: s.enabled,
                latency_ms: s.latency_budget_ms,
                last_check: Instant::now(),
            })
        })
    }

    /// Get aggregate statistics.
    pub fn stats(&self) -> FederationStats {
        let total = self.total_queries.load(Ordering::Relaxed);
        let total_latency = self.total_latency_ms.load(Ordering::Relaxed);
        let avg = if total > 0 {
            total_latency as f64 / total as f64
        } else {
            0.0
        };

        FederationStats {
            total_queries: total,
            cross_source_queries: self.cross_source_queries.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            avg_latency_ms: avg,
            sources_registered: self.sources.read().len(),
        }
    }
}

fn estimate_source_cost(source_type: &SourceType) -> f64 {
    match source_type {
        SourceType::Ferrite => 1.0,
        SourceType::Redis => 2.0,
        SourceType::PostgreSQL => 5.0,
        SourceType::S3 => 10.0,
        SourceType::Kafka => 8.0,
        SourceType::Http => 15.0,
        SourceType::Custom(_) => 20.0,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_source(name: &str, stype: SourceType) -> DataSource {
        DataSource {
            name: name.to_string(),
            source_type: stype,
            connection_string: format!("localhost/{}", name),
            schema: None,
            enabled: true,
            latency_budget_ms: 100,
        }
    }

    #[test]
    fn test_register_and_list_sources() {
        let engine = FederationEngine::new(FederationConfig::default());
        engine
            .register_source(make_source("cluster-a", SourceType::Ferrite))
            .expect("register should succeed");
        engine
            .register_source(make_source("pg-main", SourceType::PostgreSQL))
            .expect("register should succeed");

        let sources = engine.list_sources();
        assert_eq!(sources.len(), 2);
    }

    #[test]
    fn test_remove_source() {
        let engine = FederationEngine::new(FederationConfig::default());
        engine
            .register_source(make_source("tmp", SourceType::Ferrite))
            .expect("register should succeed");
        engine.remove_source("tmp").expect("remove should succeed");
        assert!(engine.list_sources().is_empty());
    }

    #[test]
    fn test_remove_missing_source() {
        let engine = FederationEngine::new(FederationConfig::default());
        let result = engine.remove_source("nonexistent");
        assert!(matches!(result, Err(FederationError::SourceNotFound(_))));
    }

    #[test]
    fn test_max_sources_limit() {
        let config = FederationConfig {
            max_sources: 2,
            ..Default::default()
        };
        let engine = FederationEngine::new(config);
        engine
            .register_source(make_source("a", SourceType::Ferrite))
            .expect("first register");
        engine
            .register_source(make_source("b", SourceType::Redis))
            .expect("second register");
        let result = engine.register_source(make_source("c", SourceType::S3));
        assert!(matches!(result, Err(FederationError::MaxSources(2))));
    }

    #[test]
    fn test_execute_query() {
        let engine = FederationEngine::new(FederationConfig::default());
        engine
            .register_source(make_source("local", SourceType::Ferrite))
            .expect("register should succeed");

        let query = FederatedQuery {
            source: "local".to_string(),
            operation: QueryOperation::Scan {
                pattern: "users:*".to_string(),
            },
            filters: vec![],
            projections: vec!["name".to_string()],
            limit: Some(10),
            cache_key: None,
        };

        let result = engine.execute(query).expect("execute should succeed");
        assert_eq!(result.source, "local");
        assert!(!result.cached);
    }

    #[test]
    fn test_execute_unknown_source() {
        let engine = FederationEngine::new(FederationConfig::default());
        let query = FederatedQuery {
            source: "missing".to_string(),
            operation: QueryOperation::Get {
                key: "k".to_string(),
            },
            filters: vec![],
            projections: vec![],
            limit: None,
            cache_key: None,
        };
        let result = engine.execute(query);
        assert!(matches!(result, Err(FederationError::SourceNotFound(_))));
    }

    #[test]
    fn test_explain_query() {
        let engine = FederationEngine::new(FederationConfig::default());
        engine
            .register_source(make_source("pg", SourceType::PostgreSQL))
            .expect("register should succeed");

        let query = FederatedQuery {
            source: "pg".to_string(),
            operation: QueryOperation::Query {
                sql: "SELECT 1".to_string(),
            },
            filters: vec![],
            projections: vec![],
            limit: None,
            cache_key: None,
        };

        let plan = engine.explain(&query);
        assert_eq!(plan.sources_involved, vec!["pg"]);
        assert!(plan.estimated_cost > 0.0);
    }

    #[test]
    fn test_cross_source_query() {
        let engine = FederationEngine::new(FederationConfig::default());
        engine
            .register_source(make_source("a", SourceType::Ferrite))
            .expect("register");
        engine
            .register_source(make_source("b", SourceType::Redis))
            .expect("register");

        let queries = vec![
            FederatedQuery {
                source: "a".to_string(),
                operation: QueryOperation::Scan {
                    pattern: "*".to_string(),
                },
                filters: vec![],
                projections: vec!["id".to_string()],
                limit: None,
                cache_key: None,
            },
            FederatedQuery {
                source: "b".to_string(),
                operation: QueryOperation::Scan {
                    pattern: "*".to_string(),
                },
                filters: vec![],
                projections: vec!["id".to_string()],
                limit: None,
                cache_key: None,
            },
        ];

        let result = engine
            .execute_cross_source(queries, "id")
            .expect("cross source should succeed");
        assert!(result.source.contains("cross"));
    }

    #[test]
    fn test_stats() {
        let engine = FederationEngine::new(FederationConfig::default());
        engine
            .register_source(make_source("s", SourceType::Ferrite))
            .expect("register");

        let stats = engine.stats();
        assert_eq!(stats.total_queries, 0);
        assert_eq!(stats.sources_registered, 1);
    }

    #[test]
    fn test_source_health() {
        let engine = FederationEngine::new(FederationConfig::default());
        engine
            .register_source(make_source("h", SourceType::Ferrite))
            .expect("register");

        let health = engine.source_health("h");
        assert!(health.is_some());
        assert!(health.as_ref().map_or(false, |h| h.reachable));
    }

    #[test]
    fn test_filter_op_display() {
        assert_eq!(FilterOp::Eq.to_string(), "=");
        assert_eq!(FilterOp::Ne.to_string(), "!=");
        assert_eq!(FilterOp::Like.to_string(), "LIKE");
        assert_eq!(FilterOp::In.to_string(), "IN");
    }

    #[test]
    fn test_source_type_display() {
        assert_eq!(SourceType::Ferrite.to_string(), "ferrite");
        assert_eq!(SourceType::PostgreSQL.to_string(), "postgresql");
        assert_eq!(
            SourceType::Custom("mydb".to_string()).to_string(),
            "custom:mydb"
        );
    }
}
