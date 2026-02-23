//! Cross-Instance Query Federation Executor
//!
//! Implements distributed query execution across multiple Ferrite instances
//! with scatter-gather, push-down optimization, and latency-aware routing.
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────┐
//! │                   FederatedExecutor                            │
//! │  ┌───────────────────┐   ┌─────────────────────────────────┐  │
//! │  │  Query Planner    │──▶│  Scatter-Gather Executor         │  │
//! │  │  (push-down opt)  │   │  (fan-out, merge, retry)        │  │
//! │  └───────────────────┘   └──────────────┬──────────────────┘  │
//! │                                         │                     │
//! │  ┌──────────┬──────────┬────────────┬───▼────────┐            │
//! │  │ Ferrite  │ Ferrite  │ Ferrite    │  Ferrite   │            │
//! │  │ Region-A │ Region-B │ Region-C   │  Local     │            │
//! │  └──────────┘──────────┘────────────┘────────────┘            │
//! └────────────────────────────────────────────────────────────────┘
//! ```

use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the federated query executor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederationConfig {
    /// Timeout for federated queries in milliseconds.
    pub timeout_ms: u64,
    /// Maximum number of remote instances that can be registered.
    pub max_instances: usize,
    /// Maximum number of concurrent federated queries.
    pub max_concurrent_queries: usize,
    /// Whether to push down predicates to remote instances.
    pub enable_pushdown: bool,
    /// Whether to prefer the local instance when possible.
    pub prefer_local: bool,
    /// Whether to retry on individual instance failure.
    pub retry_on_failure: bool,
    /// Whether to return partial results when some instances time out.
    pub partial_results_on_timeout: bool,
}

impl Default for FederationConfig {
    fn default() -> Self {
        Self {
            timeout_ms: 30_000,
            max_instances: 64,
            max_concurrent_queries: 100,
            enable_pushdown: true,
            prefer_local: true,
            retry_on_failure: true,
            partial_results_on_timeout: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Instance types
// ---------------------------------------------------------------------------

/// Unique identifier for a remote instance (UUID v4 stored as string).
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct InstanceId(pub String);

impl InstanceId {
    /// Creates a new random instance ID.
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

impl Default for InstanceId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for InstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A remote Ferrite instance to federate queries to.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteInstance {
    /// Human-readable name for this instance.
    pub name: String,
    /// Network endpoint (e.g. `"ferrite://host:6380"`).
    pub endpoint: String,
    /// Optional region label for latency-aware routing.
    pub region: Option<String>,
    /// Routing priority (lower is preferred).
    pub priority: u32,
    /// Capabilities this instance supports.
    pub capabilities: Vec<InstanceCapability>,
    /// Authentication configuration.
    pub auth: Option<InstanceAuth>,
}

/// Capabilities supported by a remote instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum InstanceCapability {
    /// Basic key-value operations.
    KeyValue,
    /// Full-text search.
    Search,
    /// Vector similarity search.
    Vector,
    /// Graph queries.
    Graph,
    /// Time series operations.
    TimeSeries,
    /// Document store operations.
    Document,
    /// Streaming operations.
    Streaming,
    /// FerriteQL query language support.
    FerriteQL,
}

impl std::fmt::Display for InstanceCapability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KeyValue => write!(f, "key_value"),
            Self::Search => write!(f, "search"),
            Self::Vector => write!(f, "vector"),
            Self::Graph => write!(f, "graph"),
            Self::TimeSeries => write!(f, "time_series"),
            Self::Document => write!(f, "document"),
            Self::Streaming => write!(f, "streaming"),
            Self::FerriteQL => write!(f, "ferriteql"),
        }
    }
}

/// Authentication for a remote instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InstanceAuth {
    /// No authentication required.
    None,
    /// Password-based authentication.
    Password(String),
    /// Token-based authentication.
    Token(String),
    /// TLS client certificate authentication.
    Tls {
        /// Path to the client certificate.
        cert_path: String,
    },
}

// ---------------------------------------------------------------------------
// Health tracking
// ---------------------------------------------------------------------------

/// Health status of a remote instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthStatus {
    /// Instance is healthy and responsive.
    Healthy,
    /// Instance is responsive but experiencing issues.
    Degraded {
        /// Description of why the instance is degraded.
        reason: String,
    },
    /// Instance is not reachable.
    Unreachable,
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Healthy => write!(f, "healthy"),
            Self::Degraded { reason } => write!(f, "degraded: {}", reason),
            Self::Unreachable => write!(f, "unreachable"),
        }
    }
}

/// Health information for a remote instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceHealth {
    /// Instance identifier.
    pub id: InstanceId,
    /// Current health status.
    pub status: HealthStatus,
    /// Measured latency in milliseconds.
    pub latency_ms: f64,
    /// Timestamp of the last health check.
    pub last_check: DateTime<Utc>,
    /// Total queries served by this instance.
    pub queries_served: u64,
    /// Error rate as a fraction (0.0–1.0).
    pub error_rate: f64,
}

// ---------------------------------------------------------------------------
// Query planning
// ---------------------------------------------------------------------------

/// Execution plan for a federated query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederatedPlan {
    /// Ordered steps to execute.
    pub steps: Vec<PlanStep>,
    /// Estimated total latency in milliseconds.
    pub estimated_latency_ms: f64,
    /// Instances involved in the plan.
    pub instances_involved: Vec<InstanceId>,
    /// Predicates pushed down to remote instances.
    pub pushdown_predicates: Vec<String>,
    /// Estimated bytes transferred over the network.
    pub data_transfer_bytes: u64,
}

/// A single step in a federated execution plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanStep {
    /// Target instance for this step.
    pub instance_id: InstanceId,
    /// Operation to perform.
    pub operation: PlanOperation,
    /// Estimated number of rows this step produces.
    pub estimated_rows: u64,
    /// Estimated latency for this step in milliseconds.
    pub estimated_latency_ms: f64,
    /// Indices of steps this step depends on.
    pub depends_on: Vec<usize>,
}

/// Operations in a federated execution plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PlanOperation {
    /// Scan keys matching a pattern on a remote instance.
    RemoteScan {
        /// Key pattern to scan.
        pattern: String,
        /// Optional filter predicate.
        filter: Option<String>,
    },
    /// Execute a SQL/FerriteQL query on a remote instance.
    RemoteQuery {
        /// The query string.
        sql: String,
    },
    /// Perform a join locally after gathering remote data.
    LocalJoin {
        /// Join type (e.g. "inner", "left", "right").
        join_type: String,
        /// Join condition expression.
        condition: String,
    },
    /// Perform aggregation locally.
    LocalAggregate {
        /// Aggregate function expressions.
        functions: Vec<String>,
    },
    /// Sort results locally.
    LocalSort {
        /// Columns to sort by.
        columns: Vec<String>,
    },
    /// Merge results from multiple steps.
    Merge {
        /// Strategy for merging.
        strategy: MergeStrategy,
    },
}

/// Strategy for merging results from multiple instances.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergeStrategy {
    /// Simple concatenation (UNION ALL).
    Append,
    /// Merge pre-sorted streams on a sort key.
    SortMerge {
        /// Column to sort-merge on.
        sort_key: String,
    },
    /// Hash-based join on a key column.
    HashJoin {
        /// Column to join on.
        join_key: String,
    },
    /// Deduplicated union (UNION).
    Union,
}

impl std::fmt::Display for MergeStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Append => write!(f, "append"),
            Self::SortMerge { sort_key } => write!(f, "sort_merge({})", sort_key),
            Self::HashJoin { join_key } => write!(f, "hash_join({})", join_key),
            Self::Union => write!(f, "union"),
        }
    }
}

// ---------------------------------------------------------------------------
// Query results
// ---------------------------------------------------------------------------

/// Result of a federated query execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederatedResult {
    /// Column names in the result set.
    pub columns: Vec<String>,
    /// Rows of values.
    pub rows: Vec<Vec<FedValue>>,
    /// Execution statistics.
    pub stats: FederatedQueryStats,
    /// Whether this result is partial (some instances timed out or failed).
    pub partial: bool,
}

/// A value in a federated query result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FedValue {
    /// SQL NULL.
    Null,
    /// Boolean value.
    Bool(bool),
    /// 64-bit signed integer.
    Int(i64),
    /// 64-bit floating point number.
    Float(f64),
    /// UTF-8 string.
    String(String),
    /// Raw bytes.
    Bytes(Vec<u8>),
}

impl std::fmt::Display for FedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Bool(b) => write!(f, "{}", b),
            Self::Int(n) => write!(f, "{}", n),
            Self::Float(n) => write!(f, "{}", n),
            Self::String(s) => write!(f, "{}", s),
            Self::Bytes(b) => write!(f, "<{} bytes>", b.len()),
        }
    }
}

/// Statistics from a federated query execution.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FederatedQueryStats {
    /// Number of instances that were queried.
    pub instances_queried: u64,
    /// Total rows scanned across all instances.
    pub total_rows_scanned: u64,
    /// Rows transferred over the network.
    pub rows_transferred: u64,
    /// Total wall-clock latency in milliseconds.
    pub total_latency_ms: u64,
    /// Time spent waiting for network I/O in milliseconds.
    pub network_latency_ms: u64,
    /// Time spent on local processing in milliseconds.
    pub processing_latency_ms: u64,
}

// ---------------------------------------------------------------------------
// Instance info / stats
// ---------------------------------------------------------------------------

/// Summary information about a registered instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceInfo {
    /// Instance identifier.
    pub id: InstanceId,
    /// Human-readable name.
    pub name: String,
    /// Network endpoint.
    pub endpoint: String,
    /// Optional region label.
    pub region: Option<String>,
    /// Current health status.
    pub status: HealthStatus,
    /// Last measured latency in milliseconds.
    pub latency_ms: f64,
    /// Capabilities of this instance.
    pub capabilities: Vec<InstanceCapability>,
}

/// Aggregate statistics for the federation layer.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FederationStats {
    /// Total number of registered instances.
    pub total_instances: u64,
    /// Number of instances currently healthy.
    pub healthy_instances: u64,
    /// Total federated queries executed.
    pub total_federated_queries: u64,
    /// Total rows transferred across all queries.
    pub total_rows_transferred: u64,
    /// Average query latency in milliseconds.
    pub avg_latency_ms: f64,
    /// Total number of errors.
    pub errors: u64,
    /// Number of times push-down optimization was applied.
    pub pushdown_optimizations: u64,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors from the federated query executor.
#[derive(Debug, thiserror::Error)]
pub enum FederationError {
    /// The specified instance was not found.
    #[error("instance not found: {0}")]
    InstanceNotFound(String),

    /// A remote instance is unreachable.
    #[error("instance unreachable: {0}")]
    InstanceUnreachable(String),

    /// The federated query timed out.
    #[error("query timeout")]
    QueryTimeout,

    /// Query planning failed.
    #[error("planning failed: {0}")]
    PlanningFailed(String),

    /// Query execution failed.
    #[error("execution failed: {0}")]
    ExecutionFailed(String),

    /// Maximum number of instances exceeded.
    #[error("max instances exceeded")]
    MaxInstancesExceeded,

    /// Partial results were returned.
    #[error("partial results: {0}")]
    PartialResults(String),

    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Internal state
// ---------------------------------------------------------------------------

/// Internal record for a registered instance.
#[derive(Debug, Clone)]
struct InstanceRecord {
    id: InstanceId,
    instance: RemoteInstance,
    health: InstanceHealth,
}

// ---------------------------------------------------------------------------
// Executor
// ---------------------------------------------------------------------------

/// Federated query executor for cross-instance query federation.
///
/// Executes queries across multiple Ferrite instances using scatter-gather,
/// push-down optimization, and latency-aware routing.
pub struct FederatedExecutor {
    config: FederationConfig,
    instances: DashMap<InstanceId, InstanceRecord>,
    // Aggregate counters
    total_queries: AtomicU64,
    total_rows_transferred: AtomicU64,
    total_errors: AtomicU64,
    total_pushdowns: AtomicU64,
    total_latency_sum_ms: AtomicU64,
}

impl FederatedExecutor {
    /// Create a new federated executor with the given configuration.
    pub fn new(config: FederationConfig) -> Self {
        Self {
            config,
            instances: DashMap::new(),
            total_queries: AtomicU64::new(0),
            total_rows_transferred: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            total_pushdowns: AtomicU64::new(0),
            total_latency_sum_ms: AtomicU64::new(0),
        }
    }

    /// Register a remote instance for federated query execution.
    pub fn register_instance(
        &self,
        instance: RemoteInstance,
    ) -> Result<InstanceId, FederationError> {
        if self.instances.len() >= self.config.max_instances {
            return Err(FederationError::MaxInstancesExceeded);
        }

        let id = InstanceId::new();
        let health = InstanceHealth {
            id: id.clone(),
            status: HealthStatus::Healthy,
            latency_ms: 0.0,
            last_check: Utc::now(),
            queries_served: 0,
            error_rate: 0.0,
        };

        let record = InstanceRecord {
            id: id.clone(),
            instance,
            health,
        };
        self.instances.insert(id.clone(), record);
        Ok(id)
    }

    /// Unregister a remote instance.
    pub fn unregister_instance(&self, id: &InstanceId) -> Result<(), FederationError> {
        self.instances
            .remove(id)
            .map(|_| ())
            .ok_or_else(|| FederationError::InstanceNotFound(id.to_string()))
    }

    /// Execute a federated query across registered instances.
    ///
    /// Builds an execution plan, dispatches sub-queries to relevant instances
    /// (scatter), collects and merges results (gather).
    pub async fn execute_federated(&self, query: &str) -> Result<FederatedResult, FederationError> {
        if query.trim().is_empty() {
            return Err(FederationError::PlanningFailed("empty query".to_string()));
        }

        let plan = self.plan_query(query)?;
        self.total_queries.fetch_add(1, Ordering::Relaxed);

        let start = std::time::Instant::now();

        // Scatter: collect results from each step that targets a remote instance
        let mut all_rows: Vec<Vec<FedValue>> = Vec::new();
        let mut columns: Vec<String> = Vec::new();
        let mut instances_queried: u64 = 0;
        let mut total_rows_scanned: u64 = 0;
        let mut partial = false;

        for step in &plan.steps {
            match &step.operation {
                PlanOperation::RemoteScan { pattern, filter } => {
                    let instance_result =
                        self.execute_remote_scan(&step.instance_id, pattern, filter.as_deref());
                    match instance_result {
                        Ok((cols, rows)) => {
                            if columns.is_empty() {
                                columns = cols;
                            }
                            total_rows_scanned += rows.len() as u64;
                            all_rows.extend(rows);
                            instances_queried += 1;

                            // Update instance health
                            if let Some(mut record) = self.instances.get_mut(&step.instance_id) {
                                record.health.queries_served += 1;
                            }
                        }
                        Err(_) if self.config.partial_results_on_timeout => {
                            partial = true;
                            self.total_errors.fetch_add(1, Ordering::Relaxed);
                            if let Some(mut record) = self.instances.get_mut(&step.instance_id) {
                                record.health.status = HealthStatus::Degraded {
                                    reason: "query failed".to_string(),
                                };
                                let served = record.health.queries_served;
                                record.health.error_rate = if served > 0 {
                                    (record.health.error_rate * served as f64 + 1.0)
                                        / (served as f64 + 1.0)
                                } else {
                                    1.0
                                };
                            }
                        }
                        Err(e) => {
                            self.total_errors.fetch_add(1, Ordering::Relaxed);
                            return Err(e);
                        }
                    }
                }
                PlanOperation::RemoteQuery { sql } => {
                    let instance_result = self.execute_remote_query(&step.instance_id, sql);
                    match instance_result {
                        Ok((cols, rows)) => {
                            if columns.is_empty() {
                                columns = cols;
                            }
                            total_rows_scanned += rows.len() as u64;
                            all_rows.extend(rows);
                            instances_queried += 1;

                            if let Some(mut record) = self.instances.get_mut(&step.instance_id) {
                                record.health.queries_served += 1;
                            }
                        }
                        Err(_) if self.config.partial_results_on_timeout => {
                            partial = true;
                            self.total_errors.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            self.total_errors.fetch_add(1, Ordering::Relaxed);
                            return Err(e);
                        }
                    }
                }
                PlanOperation::Merge { strategy } => {
                    all_rows = self.apply_merge_strategy(strategy, all_rows);
                }
                PlanOperation::LocalSort { columns: sort_cols } => {
                    all_rows = self.apply_local_sort(sort_cols, &columns, all_rows);
                }
                PlanOperation::LocalAggregate { .. } | PlanOperation::LocalJoin { .. } => {
                    // Aggregation and join are handled as pass-through in this
                    // initial implementation; real execution would compute them.
                }
            }
        }

        let elapsed_ms = start.elapsed().as_millis() as u64;
        let rows_transferred = all_rows.len() as u64;

        self.total_rows_transferred
            .fetch_add(rows_transferred, Ordering::Relaxed);
        self.total_latency_sum_ms
            .fetch_add(elapsed_ms, Ordering::Relaxed);

        if columns.is_empty() {
            columns = vec!["key".to_string(), "value".to_string()];
        }

        let stats = FederatedQueryStats {
            instances_queried,
            total_rows_scanned,
            rows_transferred,
            total_latency_ms: elapsed_ms,
            network_latency_ms: elapsed_ms / 2, // approximate
            processing_latency_ms: elapsed_ms / 2,
        };

        Ok(FederatedResult {
            columns,
            rows: all_rows,
            stats,
            partial,
        })
    }

    /// Build an execution plan for a federated query.
    ///
    /// Analyses the query, determines which instances to target, applies
    /// push-down optimisation where enabled, and orders steps by estimated
    /// latency.
    pub fn plan_query(&self, query: &str) -> Result<FederatedPlan, FederationError> {
        if query.trim().is_empty() {
            return Err(FederationError::PlanningFailed("empty query".to_string()));
        }

        let mut steps = Vec::new();
        let mut instances_involved = Vec::new();
        let mut pushdown_predicates = Vec::new();
        let mut total_latency = 0.0_f64;
        let mut total_transfer: u64 = 0;

        // Extract WHERE-clause predicates for push-down
        let extracted = Self::extract_pushdown_predicates(query);
        if self.config.enable_pushdown && !extracted.is_empty() {
            pushdown_predicates = extracted.clone();
            self.total_pushdowns.fetch_add(1, Ordering::Relaxed);
        }

        // Determine operation type
        let is_select = query.trim().to_uppercase().starts_with("SELECT");

        // Collect instances sorted by priority then latency
        let mut sorted_instances: Vec<InstanceRecord> =
            self.instances.iter().map(|r| r.value().clone()).collect();
        sorted_instances.sort_by(|a, b| {
            a.instance.priority.cmp(&b.instance.priority).then_with(|| {
                a.health
                    .latency_ms
                    .partial_cmp(&b.health.latency_ms)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
        });

        // Create a step per instance
        for record in &sorted_instances {
            let step_idx = steps.len();
            let est_latency = record.health.latency_ms.max(1.0);
            let est_rows: u64 = 100; // heuristic
            let est_bytes: u64 = est_rows * 256; // ~256 bytes per row

            let operation = if is_select {
                let filter = if !pushdown_predicates.is_empty() {
                    Some(pushdown_predicates.join(" AND "))
                } else {
                    None
                };
                let pattern = Self::extract_scan_pattern(query);
                PlanOperation::RemoteScan { pattern, filter }
            } else {
                PlanOperation::RemoteQuery {
                    sql: query.to_string(),
                }
            };

            steps.push(PlanStep {
                instance_id: record.id.clone(),
                operation,
                estimated_rows: est_rows,
                estimated_latency_ms: est_latency,
                depends_on: Vec::new(),
            });

            instances_involved.push(record.id.clone());
            total_latency = total_latency.max(est_latency);
            total_transfer += est_bytes;

            // If prefer_local and this is the first (highest priority), use only it
            // for single-instance queries
            if self.config.prefer_local && step_idx == 0 && sorted_instances.len() > 1 {
                // Continue to add remaining instances for scatter
            }
        }

        // Add a merge step if multiple instances are involved
        if steps.len() > 1 {
            let merge_strategy = Self::select_merge_strategy(query);
            steps.push(PlanStep {
                instance_id: instances_involved
                    .first()
                    .cloned()
                    .unwrap_or_else(InstanceId::new),
                operation: PlanOperation::Merge {
                    strategy: merge_strategy,
                },
                estimated_rows: steps.iter().map(|s| s.estimated_rows).sum(),
                estimated_latency_ms: 1.0,
                depends_on: (0..steps.len() - 1).collect(),
            });
        }

        Ok(FederatedPlan {
            steps,
            estimated_latency_ms: total_latency,
            instances_involved,
            pushdown_predicates,
            data_transfer_bytes: total_transfer,
        })
    }

    /// Get the health of a specific instance.
    pub fn get_instance_health(&self, id: &InstanceId) -> Option<InstanceHealth> {
        self.instances.get(id).map(|r| r.health.clone())
    }

    /// List all registered instances with summary info.
    pub fn list_instances(&self) -> Vec<InstanceInfo> {
        self.instances
            .iter()
            .map(|r| {
                let rec = r.value();
                InstanceInfo {
                    id: rec.id.clone(),
                    name: rec.instance.name.clone(),
                    endpoint: rec.instance.endpoint.clone(),
                    region: rec.instance.region.clone(),
                    status: rec.health.status.clone(),
                    latency_ms: rec.health.latency_ms,
                    capabilities: rec.instance.capabilities.clone(),
                }
            })
            .collect()
    }

    /// Get aggregate statistics for the federation layer.
    pub fn get_stats(&self) -> FederationStats {
        let total_instances = self.instances.len() as u64;
        let healthy_instances = self
            .instances
            .iter()
            .filter(|r| matches!(r.value().health.status, HealthStatus::Healthy))
            .count() as u64;

        let total_queries = self.total_queries.load(Ordering::Relaxed);
        let total_rows = self.total_rows_transferred.load(Ordering::Relaxed);
        let total_latency = self.total_latency_sum_ms.load(Ordering::Relaxed);
        let avg_latency = if total_queries > 0 {
            total_latency as f64 / total_queries as f64
        } else {
            0.0
        };

        FederationStats {
            total_instances,
            healthy_instances,
            total_federated_queries: total_queries,
            total_rows_transferred: total_rows,
            avg_latency_ms: avg_latency,
            errors: self.total_errors.load(Ordering::Relaxed),
            pushdown_optimizations: self.total_pushdowns.load(Ordering::Relaxed),
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Execute a remote scan on a specific instance (stub).
    fn execute_remote_scan(
        &self,
        id: &InstanceId,
        pattern: &str,
        _filter: Option<&str>,
    ) -> Result<(Vec<String>, Vec<Vec<FedValue>>), FederationError> {
        if !self.instances.contains_key(id) {
            return Err(FederationError::InstanceNotFound(id.to_string()));
        }

        // In a real implementation this would open a connection to the remote
        // instance and execute the scan. For now return an empty result set.
        let columns = vec!["key".to_string(), "value".to_string()];
        let _pattern = pattern; // acknowledged
        Ok((columns, Vec::new()))
    }

    /// Execute a remote query on a specific instance (stub).
    fn execute_remote_query(
        &self,
        id: &InstanceId,
        _sql: &str,
    ) -> Result<(Vec<String>, Vec<Vec<FedValue>>), FederationError> {
        if !self.instances.contains_key(id) {
            return Err(FederationError::InstanceNotFound(id.to_string()));
        }

        let columns = vec!["key".to_string(), "value".to_string()];
        Ok((columns, Vec::new()))
    }

    /// Apply a merge strategy to collected rows.
    fn apply_merge_strategy(
        &self,
        strategy: &MergeStrategy,
        rows: Vec<Vec<FedValue>>,
    ) -> Vec<Vec<FedValue>> {
        match strategy {
            MergeStrategy::Append => rows,
            MergeStrategy::Union => {
                // Deduplicate by serialised form
                let mut seen = std::collections::HashSet::new();
                rows.into_iter()
                    .filter(|row| {
                        let key = format!("{:?}", row);
                        seen.insert(key)
                    })
                    .collect()
            }
            MergeStrategy::SortMerge { sort_key: _ } | MergeStrategy::HashJoin { join_key: _ } => {
                // Full merge/join would require column resolution; pass-through
                // for now.
                rows
            }
        }
    }

    /// Apply local sort to rows.
    fn apply_local_sort(
        &self,
        _sort_cols: &[String],
        _columns: &[String],
        rows: Vec<Vec<FedValue>>,
    ) -> Vec<Vec<FedValue>> {
        // A full implementation would sort by the named columns. Pass-through
        // for now.
        rows
    }

    /// Extract WHERE-clause predicates suitable for push-down.
    fn extract_pushdown_predicates(query: &str) -> Vec<String> {
        let upper = query.to_uppercase();
        let Some(where_pos) = upper.find("WHERE") else {
            return Vec::new();
        };

        let after_where = &query[where_pos + 5..];
        // Trim at ORDER BY / GROUP BY / LIMIT / HAVING / end of string
        let end_pos = ["ORDER BY", "GROUP BY", "LIMIT", "HAVING"]
            .iter()
            .filter_map(|kw| after_where.to_uppercase().find(kw))
            .min()
            .unwrap_or(after_where.len());

        let clause = after_where[..end_pos].trim();
        if clause.is_empty() {
            return Vec::new();
        }

        // Split on AND (simple heuristic; real parsing uses the AST)
        clause
            .split(" AND ")
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect()
    }

    /// Extract the key pattern from a SELECT … FROM <pattern> query.
    fn extract_scan_pattern(query: &str) -> String {
        let upper = query.to_uppercase();
        if let Some(from_pos) = upper.find("FROM") {
            let after_from = query[from_pos + 4..].trim();
            let end = after_from
                .find(|c: char| c.is_whitespace())
                .unwrap_or(after_from.len());
            return after_from[..end].to_string();
        }
        "*".to_string()
    }

    /// Select a merge strategy based on the query text.
    fn select_merge_strategy(query: &str) -> MergeStrategy {
        let upper = query.to_uppercase();
        if upper.contains("ORDER BY") {
            if let Some(pos) = upper.find("ORDER BY") {
                let after = query[pos + 8..].trim();
                let col_end = after
                    .find(|c: char| c == ',' || c.is_whitespace())
                    .unwrap_or(after.len());
                let sort_key = after[..col_end].to_string();
                return MergeStrategy::SortMerge { sort_key };
            }
        }
        if upper.contains("JOIN") {
            if let Some(pos) = upper.find(" ON ") {
                let after = query[pos + 4..].trim();
                let key_end = after
                    .find(|c: char| c.is_whitespace())
                    .unwrap_or(after.len());
                let join_key = after[..key_end].to_string();
                return MergeStrategy::HashJoin { join_key };
            }
        }
        if upper.contains("DISTINCT") || upper.contains("UNION") {
            return MergeStrategy::Union;
        }
        MergeStrategy::Append
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> FederationConfig {
        FederationConfig::default()
    }

    fn test_instance(name: &str) -> RemoteInstance {
        RemoteInstance {
            name: name.to_string(),
            endpoint: format!("ferrite://{}:6380", name),
            region: Some("us-east-1".to_string()),
            priority: 1,
            capabilities: vec![InstanceCapability::KeyValue, InstanceCapability::FerriteQL],
            auth: None,
        }
    }

    // -- Registration / removal -------------------------------------------

    #[test]
    fn test_register_instance() {
        let executor = FederatedExecutor::new(default_config());
        let id = executor
            .register_instance(test_instance("node-1"))
            .expect("register");
        assert!(!id.0.is_empty());
        assert_eq!(executor.list_instances().len(), 1);
    }

    #[test]
    fn test_register_multiple_instances() {
        let executor = FederatedExecutor::new(default_config());
        let id1 = executor
            .register_instance(test_instance("node-1"))
            .expect("register");
        let id2 = executor
            .register_instance(test_instance("node-2"))
            .expect("register");
        assert_ne!(id1, id2);
        assert_eq!(executor.list_instances().len(), 2);
    }

    #[test]
    fn test_unregister_instance() {
        let executor = FederatedExecutor::new(default_config());
        let id = executor
            .register_instance(test_instance("node-1"))
            .expect("register");
        executor.unregister_instance(&id).expect("unregister");
        assert!(executor.list_instances().is_empty());
    }

    #[test]
    fn test_unregister_missing_instance() {
        let executor = FederatedExecutor::new(default_config());
        let bogus = InstanceId::new();
        let result = executor.unregister_instance(&bogus);
        assert!(matches!(result, Err(FederationError::InstanceNotFound(_))));
    }

    #[test]
    fn test_max_instances_exceeded() {
        let mut cfg = default_config();
        cfg.max_instances = 2;
        let executor = FederatedExecutor::new(cfg);
        executor
            .register_instance(test_instance("a"))
            .expect("register a");
        executor
            .register_instance(test_instance("b"))
            .expect("register b");
        let result = executor.register_instance(test_instance("c"));
        assert!(matches!(result, Err(FederationError::MaxInstancesExceeded)));
    }

    // -- Query plan generation -------------------------------------------

    #[test]
    fn test_plan_simple_select() {
        let executor = FederatedExecutor::new(default_config());
        executor
            .register_instance(test_instance("node-1"))
            .expect("register");

        let plan = executor.plan_query("SELECT * FROM users:*").expect("plan");
        assert!(!plan.steps.is_empty());
        assert!(!plan.instances_involved.is_empty());
        assert!(plan.estimated_latency_ms >= 0.0);
    }

    #[test]
    fn test_plan_multiple_instances_has_merge_step() {
        let executor = FederatedExecutor::new(default_config());
        executor
            .register_instance(test_instance("a"))
            .expect("register");
        executor
            .register_instance(test_instance("b"))
            .expect("register");

        let plan = executor.plan_query("SELECT * FROM keys:*").expect("plan");
        // Should have 2 remote steps + 1 merge step
        assert!(plan.steps.len() >= 3);
        let last = plan.steps.last().expect("last step");
        assert!(matches!(last.operation, PlanOperation::Merge { .. }));
    }

    #[test]
    fn test_plan_empty_query_fails() {
        let executor = FederatedExecutor::new(default_config());
        let result = executor.plan_query("");
        assert!(matches!(result, Err(FederationError::PlanningFailed(_))));
    }

    #[test]
    fn test_plan_no_instances() {
        let executor = FederatedExecutor::new(default_config());
        let plan = executor.plan_query("SELECT * FROM users:*").expect("plan");
        assert!(plan.steps.is_empty());
        assert!(plan.instances_involved.is_empty());
    }

    // -- Push-down predicates -------------------------------------------

    #[test]
    fn test_pushdown_predicate_extraction() {
        let predicates = FederatedExecutor::extract_pushdown_predicates(
            "SELECT * FROM users:* WHERE status = 'active' AND age > 21",
        );
        assert_eq!(predicates.len(), 2);
        assert!(predicates[0].contains("status"));
        assert!(predicates[1].contains("age"));
    }

    #[test]
    fn test_pushdown_no_where_clause() {
        let predicates = FederatedExecutor::extract_pushdown_predicates("SELECT * FROM users:*");
        assert!(predicates.is_empty());
    }

    #[test]
    fn test_pushdown_with_order_by() {
        let predicates = FederatedExecutor::extract_pushdown_predicates(
            "SELECT * FROM users:* WHERE status = 'active' ORDER BY name",
        );
        assert_eq!(predicates.len(), 1);
        assert!(predicates[0].contains("status"));
    }

    #[test]
    fn test_pushdown_included_in_plan() {
        let executor = FederatedExecutor::new(default_config());
        executor
            .register_instance(test_instance("node-1"))
            .expect("register");

        let plan = executor
            .plan_query("SELECT * FROM users:* WHERE status = 'active'")
            .expect("plan");
        assert!(!plan.pushdown_predicates.is_empty());
        assert!(plan.pushdown_predicates[0].contains("status"));
    }

    #[test]
    fn test_pushdown_disabled() {
        let mut cfg = default_config();
        cfg.enable_pushdown = false;
        let executor = FederatedExecutor::new(cfg);
        executor
            .register_instance(test_instance("node-1"))
            .expect("register");

        let plan = executor
            .plan_query("SELECT * FROM users:* WHERE status = 'active'")
            .expect("plan");
        assert!(plan.pushdown_predicates.is_empty());
    }

    // -- Merge strategy selection ----------------------------------------

    #[test]
    fn test_merge_strategy_append_default() {
        let strategy = FederatedExecutor::select_merge_strategy("SELECT * FROM users:*");
        assert_eq!(strategy, MergeStrategy::Append);
    }

    #[test]
    fn test_merge_strategy_sort_merge_on_order_by() {
        let strategy =
            FederatedExecutor::select_merge_strategy("SELECT * FROM users:* ORDER BY name");
        assert!(matches!(strategy, MergeStrategy::SortMerge { .. }));
    }

    #[test]
    fn test_merge_strategy_hash_join() {
        let strategy = FederatedExecutor::select_merge_strategy(
            "SELECT * FROM users:* JOIN orders:* ON u.id = o.user_id",
        );
        assert!(matches!(strategy, MergeStrategy::HashJoin { .. }));
    }

    #[test]
    fn test_merge_strategy_union_on_distinct() {
        let strategy =
            FederatedExecutor::select_merge_strategy("SELECT DISTINCT name FROM users:*");
        assert_eq!(strategy, MergeStrategy::Union);
    }

    // -- Health tracking -------------------------------------------------

    #[test]
    fn test_get_instance_health() {
        let executor = FederatedExecutor::new(default_config());
        let id = executor
            .register_instance(test_instance("node-1"))
            .expect("register");

        let health = executor.get_instance_health(&id).expect("health exists");
        assert!(matches!(health.status, HealthStatus::Healthy));
        assert_eq!(health.queries_served, 0);
    }

    #[test]
    fn test_health_missing_instance() {
        let executor = FederatedExecutor::new(default_config());
        let bogus = InstanceId::new();
        assert!(executor.get_instance_health(&bogus).is_none());
    }

    // -- Stats -----------------------------------------------------------

    #[test]
    fn test_initial_stats() {
        let executor = FederatedExecutor::new(default_config());
        let stats = executor.get_stats();
        assert_eq!(stats.total_instances, 0);
        assert_eq!(stats.total_federated_queries, 0);
        assert_eq!(stats.errors, 0);
    }

    #[test]
    fn test_stats_after_registration() {
        let executor = FederatedExecutor::new(default_config());
        executor
            .register_instance(test_instance("a"))
            .expect("register");
        executor
            .register_instance(test_instance("b"))
            .expect("register");

        let stats = executor.get_stats();
        assert_eq!(stats.total_instances, 2);
        assert_eq!(stats.healthy_instances, 2);
    }

    #[tokio::test]
    async fn test_stats_after_query() {
        let executor = FederatedExecutor::new(default_config());
        executor
            .register_instance(test_instance("node-1"))
            .expect("register");

        let _ = executor.execute_federated("SELECT * FROM users:*").await;

        let stats = executor.get_stats();
        assert_eq!(stats.total_federated_queries, 1);
    }

    // -- Execution -------------------------------------------------------

    #[tokio::test]
    async fn test_execute_federated_empty_query() {
        let executor = FederatedExecutor::new(default_config());
        let result = executor.execute_federated("").await;
        assert!(matches!(result, Err(FederationError::PlanningFailed(_))));
    }

    #[tokio::test]
    async fn test_execute_federated_no_instances() {
        let executor = FederatedExecutor::new(default_config());
        let result = executor.execute_federated("SELECT * FROM users:*").await;
        assert!(result.is_ok());
        let res = result.expect("result");
        assert_eq!(res.stats.instances_queried, 0);
    }

    #[tokio::test]
    async fn test_execute_federated_single_instance() {
        let executor = FederatedExecutor::new(default_config());
        executor
            .register_instance(test_instance("node-1"))
            .expect("register");

        let result = executor
            .execute_federated("SELECT * FROM users:*")
            .await
            .expect("execute");
        assert!(!result.partial);
        assert!(!result.columns.is_empty());
    }

    #[tokio::test]
    async fn test_execute_federated_multiple_instances() {
        let executor = FederatedExecutor::new(default_config());
        executor
            .register_instance(test_instance("a"))
            .expect("register");
        executor
            .register_instance(test_instance("b"))
            .expect("register");

        let result = executor
            .execute_federated("SELECT * FROM users:*")
            .await
            .expect("execute");
        assert!(!result.partial);
        assert_eq!(result.stats.instances_queried, 2);
    }

    // -- Partial results --------------------------------------------------

    #[tokio::test]
    async fn test_partial_results_flag() {
        let executor = FederatedExecutor::new(default_config());
        // Execute with no instances — result should not be partial
        let result = executor
            .execute_federated("SELECT * FROM users:*")
            .await
            .expect("execute");
        assert!(!result.partial);
    }

    // -- Error handling ---------------------------------------------------

    #[test]
    fn test_error_display() {
        let err = FederationError::InstanceNotFound("abc".to_string());
        assert!(err.to_string().contains("abc"));

        let err = FederationError::QueryTimeout;
        assert_eq!(err.to_string(), "query timeout");

        let err = FederationError::MaxInstancesExceeded;
        assert_eq!(err.to_string(), "max instances exceeded");
    }

    // -- InstanceId -------------------------------------------------------

    #[test]
    fn test_instance_id_display() {
        let id = InstanceId::new();
        let s = id.to_string();
        assert!(!s.is_empty());
        // UUID v4 string format: 36 chars
        assert_eq!(s.len(), 36);
    }

    #[test]
    fn test_instance_id_default() {
        let id = InstanceId::default();
        assert!(!id.0.is_empty());
    }

    // -- Scan pattern extraction ------------------------------------------

    #[test]
    fn test_extract_scan_pattern() {
        let pattern = FederatedExecutor::extract_scan_pattern("SELECT * FROM users:*");
        assert_eq!(pattern, "users:*");
    }

    #[test]
    fn test_extract_scan_pattern_with_where() {
        let pattern = FederatedExecutor::extract_scan_pattern(
            "SELECT * FROM orders:* WHERE status = 'active'",
        );
        assert_eq!(pattern, "orders:*");
    }

    #[test]
    fn test_extract_scan_pattern_no_from() {
        let pattern = FederatedExecutor::extract_scan_pattern("INSERT INTO foo VALUES (1)");
        assert_eq!(pattern, "*");
    }

    // -- FedValue / Display -----------------------------------------------

    #[test]
    fn test_fed_value_display() {
        assert_eq!(FedValue::Null.to_string(), "NULL");
        assert_eq!(FedValue::Bool(true).to_string(), "true");
        assert_eq!(FedValue::Int(42).to_string(), "42");
        assert_eq!(FedValue::Float(3.14).to_string(), "3.14");
        assert_eq!(FedValue::String("hello".into()).to_string(), "hello");
        assert_eq!(FedValue::Bytes(vec![1, 2, 3]).to_string(), "<3 bytes>");
    }

    // -- MergeStrategy / Display ------------------------------------------

    #[test]
    fn test_merge_strategy_display() {
        assert_eq!(MergeStrategy::Append.to_string(), "append");
        assert_eq!(MergeStrategy::Union.to_string(), "union");
        assert_eq!(
            MergeStrategy::SortMerge {
                sort_key: "name".into()
            }
            .to_string(),
            "sort_merge(name)"
        );
        assert_eq!(
            MergeStrategy::HashJoin {
                join_key: "id".into()
            }
            .to_string(),
            "hash_join(id)"
        );
    }

    // -- HealthStatus / Display -------------------------------------------

    #[test]
    fn test_health_status_display() {
        assert_eq!(HealthStatus::Healthy.to_string(), "healthy");
        assert_eq!(HealthStatus::Unreachable.to_string(), "unreachable");
        assert_eq!(
            HealthStatus::Degraded {
                reason: "high latency".into()
            }
            .to_string(),
            "degraded: high latency"
        );
    }

    // -- InstanceCapability / Display -------------------------------------

    #[test]
    fn test_instance_capability_display() {
        assert_eq!(InstanceCapability::KeyValue.to_string(), "key_value");
        assert_eq!(InstanceCapability::Search.to_string(), "search");
        assert_eq!(InstanceCapability::FerriteQL.to_string(), "ferriteql");
    }

    // -- Instance info listing --------------------------------------------

    #[test]
    fn test_list_instances_info() {
        let executor = FederatedExecutor::new(default_config());
        let id = executor
            .register_instance(RemoteInstance {
                name: "prod-1".to_string(),
                endpoint: "ferrite://prod:6380".to_string(),
                region: Some("eu-west-1".to_string()),
                priority: 2,
                capabilities: vec![InstanceCapability::KeyValue],
                auth: Some(InstanceAuth::Token("secret".to_string())),
            })
            .expect("register");

        let infos = executor.list_instances();
        assert_eq!(infos.len(), 1);
        let info = &infos[0];
        assert_eq!(info.id, id);
        assert_eq!(info.name, "prod-1");
        assert_eq!(info.endpoint, "ferrite://prod:6380");
        assert_eq!(info.region, Some("eu-west-1".to_string()));
        assert!(matches!(info.status, HealthStatus::Healthy));
        assert_eq!(info.capabilities, vec![InstanceCapability::KeyValue]);
    }

    // -- Latency-aware ordering -------------------------------------------

    #[test]
    fn test_plan_respects_priority_ordering() {
        let executor = FederatedExecutor::new(default_config());

        let mut high_prio = test_instance("primary");
        high_prio.priority = 0;
        let mut low_prio = test_instance("secondary");
        low_prio.priority = 10;

        // Register low-priority first to ensure ordering is by priority, not
        // insertion order.
        executor.register_instance(low_prio).expect("register");
        executor.register_instance(high_prio).expect("register");

        let plan = executor.plan_query("SELECT * FROM keys:*").expect("plan");
        // The first remote step should target the high-priority instance
        let first_step = &plan.steps[0];
        let first_instance = executor
            .instances
            .get(&first_step.instance_id)
            .expect("instance");
        assert_eq!(first_instance.instance.priority, 0);
    }

    // -- Config defaults --------------------------------------------------

    #[test]
    fn test_config_defaults() {
        let cfg = FederationConfig::default();
        assert_eq!(cfg.timeout_ms, 30_000);
        assert_eq!(cfg.max_instances, 64);
        assert_eq!(cfg.max_concurrent_queries, 100);
        assert!(cfg.enable_pushdown);
        assert!(cfg.prefer_local);
        assert!(cfg.retry_on_failure);
        assert!(cfg.partial_results_on_timeout);
    }
}
