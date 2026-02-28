//! Embedded Analytics Engine
//!
//! Run SQL-like analytics directly on cached data without ETL.
//! Supports aggregations, window functions, and streaming results.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the analytics engine.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AnalyticsConfig {
    /// Maximum number of keys to scan per query.
    pub max_scan_keys: usize,
    /// Query timeout.
    pub query_timeout: Duration,
    /// Enable approximate (sampled) queries.
    pub enable_approximate: bool,
    /// Sample rate when approximate mode is enabled (0.0–1.0).
    pub sample_rate: f64,
}

impl Default for AnalyticsConfig {
    fn default() -> Self {
        Self {
            max_scan_keys: 1_000_000,
            query_timeout: Duration::from_secs(30),
            enable_approximate: true,
            sample_rate: 0.1,
        }
    }
}

// ---------------------------------------------------------------------------
// Query model
// ---------------------------------------------------------------------------

/// A declarative analytics query.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AnalyticsQuery {
    /// Key pattern to scan (e.g. `"users:*"`).
    pub source: String,
    /// Columns / expressions to return.
    pub select: Vec<SelectExpr>,
    /// Optional row-level filter.
    pub filter: Option<FilterExpr>,
    /// Group-by field names.
    pub group_by: Vec<String>,
    /// Optional HAVING filter (applied after aggregation).
    pub having: Option<FilterExpr>,
    /// Order-by clauses.
    pub order_by: Vec<OrderExpr>,
    /// Maximum number of result rows.
    pub limit: Option<usize>,
    /// Optional window specification.
    pub window: Option<WindowSpec>,
}

/// A single SELECT expression.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SelectExpr {
    /// Field name.
    pub field: String,
    /// Optional alias for the output column.
    pub alias: Option<String>,
    /// Optional aggregate function applied to the field.
    pub function: Option<AggFunction>,
}

/// Supported aggregate functions.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum AggFunction {
    /// Count of non-null values.
    Count,
    /// Numeric sum.
    Sum,
    /// Arithmetic mean.
    Avg,
    /// Minimum value.
    Min,
    /// Maximum value.
    Max,
    /// Count of distinct values.
    CountDistinct,
    /// Approximate percentile (0.0–1.0).
    Percentile(f64),
    /// Standard deviation.
    StdDev,
    /// Variance.
    Variance,
    /// First value in the group.
    First,
    /// Last value in the group.
    Last,
}

/// A filter predicate.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FilterExpr {
    /// Field name to filter on.
    pub field: String,
    /// Comparison operator.
    pub op: FilterOp,
    /// Value to compare against.
    pub value: serde_json::Value,
}

/// Filter comparison operators.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum FilterOp {
    /// Equals.
    Eq,
    /// Not equals.
    Ne,
    /// Greater than.
    Gt,
    /// Less than.
    Lt,
    /// Greater than or equal.
    Gte,
    /// Less than or equal.
    Lte,
    /// SQL LIKE pattern match.
    Like,
    /// Value is in a set.
    In,
    /// Value is between two bounds.
    Between,
    /// Value is null.
    IsNull,
    /// Value is not null.
    IsNotNull,
}

/// An ORDER BY clause.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderExpr {
    /// Field name to sort on.
    pub field: String,
    /// Sort descending when `true`.
    pub descending: bool,
}

/// Window function specification.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WindowSpec {
    /// Partition-by field names.
    pub partition_by: Vec<String>,
    /// Order within the window.
    pub order_by: Vec<OrderExpr>,
    /// Frame bounds.
    pub frame: WindowFrame,
}

/// Window frame boundaries.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WindowFrame {
    /// Row-based bounds (start, end) relative to the current row.
    Rows(i64, i64),
    /// Range-based bounds (start, end) relative to the current row value.
    Range(i64, i64),
    /// Unbounded frame (entire partition).
    Unbounded,
}

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

/// The result of an analytics query execution.
#[derive(Clone, Debug, Serialize)]
pub struct AnalyticsResult {
    /// Column metadata.
    pub columns: Vec<ColumnInfo>,
    /// Data rows (each row is a vector of JSON values).
    pub rows: Vec<Vec<serde_json::Value>>,
    /// Total keys scanned.
    pub total_scanned: u64,
    /// Total keys matching the filter.
    pub total_matched: u64,
    /// Wall-clock execution time in milliseconds.
    pub execution_ms: u64,
    /// Whether the result was computed using approximate sampling.
    pub approximate: bool,
}

/// Metadata for a single result column.
#[derive(Clone, Debug, Serialize)]
pub struct ColumnInfo {
    /// Display name.
    pub name: String,
    /// Inferred data type (e.g. `"integer"`, `"float"`, `"string"`).
    pub data_type: String,
    /// Whether this column is the result of an aggregate function.
    pub is_aggregate: bool,
}

/// Explanation of how a query would be executed.
#[derive(Clone, Debug, Serialize)]
pub struct QueryExplanation {
    /// Human-readable plan steps.
    pub plan_steps: Vec<String>,
    /// Estimated number of keys to scan.
    pub estimated_keys: u64,
    /// Relative cost estimate.
    pub estimated_cost: f64,
    /// Whether an index will be used.
    pub will_use_index: bool,
    /// Whether approximate mode will be enabled.
    pub approximate: bool,
}

/// A single histogram bucket.
#[derive(Clone, Debug, Serialize)]
pub struct HistogramBucket {
    /// Lower bound (inclusive).
    pub lower: f64,
    /// Upper bound (exclusive).
    pub upper: f64,
    /// Number of values in this bucket.
    pub count: u64,
    /// Percentage of total values.
    pub percentage: f64,
}

/// Cumulative analytics engine statistics.
#[derive(Clone, Debug, Serialize)]
pub struct AnalyticsStats {
    /// Total queries executed.
    pub queries_executed: u64,
    /// Total keys scanned across all queries.
    pub keys_scanned: u64,
    /// Average query time in milliseconds.
    pub avg_query_ms: f64,
    /// Number of queries executed in approximate mode.
    pub approximate_queries: u64,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Analytics engine errors.
#[derive(Debug, Clone, thiserror::Error)]
pub enum AnalyticsError {
    /// The query is malformed or unsupported.
    #[error("invalid query: {0}")]
    InvalidQuery(String),
    /// Query exceeded the configured timeout.
    #[error("query timeout after {0:?}")]
    Timeout(Duration),
    /// Query attempted to scan more keys than allowed.
    #[error("too many keys: scanned {scanned}, limit {limit}")]
    TooManyKeys {
        /// Number of keys scanned so far.
        scanned: u64,
        /// Configured limit.
        limit: u64,
    },
    /// Invalid or unsupported aggregation.
    #[error("invalid aggregation: {0}")]
    InvalidAggregation(String),
    /// Type mismatch in expression evaluation.
    #[error("type mismatch: {0}")]
    TypeMismatch(String),
    /// Referenced field does not exist.
    #[error("field not found: {0}")]
    FieldNotFound(String),
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

/// Embedded analytics engine that executes queries over key-value data.
pub struct AnalyticsEngine {
    config: AnalyticsConfig,
    queries_executed: AtomicU64,
    keys_scanned: AtomicU64,
    total_query_ms: AtomicU64,
    approximate_queries: AtomicU64,
    // Mock data store for standalone operation.
    mock_data: RwLock<HashMap<String, Vec<HashMap<String, serde_json::Value>>>>,
}

impl AnalyticsEngine {
    /// Create a new analytics engine with the given configuration.
    pub fn new(config: AnalyticsConfig) -> Self {
        Self {
            config,
            queries_executed: AtomicU64::new(0),
            keys_scanned: AtomicU64::new(0),
            total_query_ms: AtomicU64::new(0),
            approximate_queries: AtomicU64::new(0),
            mock_data: RwLock::new(HashMap::new()),
        }
    }

    // -- helpers for testing -------------------------------------------------

    /// Insert mock data for a given source pattern (test helper).
    pub fn insert_mock_data(&self, source: &str, rows: Vec<HashMap<String, serde_json::Value>>) {
        let mut data = self.mock_data.write();
        data.insert(source.to_string(), rows);
    }

    /// Retrieve the mock rows for a source, returning an empty vec when absent.
    fn get_mock_rows(&self, source: &str) -> Vec<HashMap<String, serde_json::Value>> {
        let data = self.mock_data.read();
        // Exact match first, then try prefix patterns.
        if let Some(rows) = data.get(source) {
            return rows.clone();
        }
        // Simple glob: strip trailing '*' and match prefixes.
        let prefix = source.trim_end_matches('*');
        for (key, rows) in data.iter() {
            if key.starts_with(prefix) || prefix.is_empty() {
                return rows.clone();
            }
        }
        Vec::new()
    }

    // -- public API ----------------------------------------------------------

    /// Execute an analytics query.
    pub fn execute(&self, query: AnalyticsQuery) -> Result<AnalyticsResult, AnalyticsError> {
        let start = Instant::now();

        // Validate
        self.validate_query(&query)?;

        // Check timeout
        if start.elapsed() > self.config.query_timeout {
            return Err(AnalyticsError::Timeout(self.config.query_timeout));
        }

        let rows = self.get_mock_rows(&query.source);
        let total_scanned = rows.len() as u64;

        if total_scanned > self.config.max_scan_keys as u64 {
            return Err(AnalyticsError::TooManyKeys {
                scanned: total_scanned,
                limit: self.config.max_scan_keys as u64,
            });
        }

        // Filter
        let filtered: Vec<_> = rows
            .into_iter()
            .filter(|row| self.matches_filter(row, &query.filter))
            .collect();
        let total_matched = filtered.len() as u64;

        // Group-by + aggregate
        let (columns, mut result_rows) = if query.group_by.is_empty() {
            self.compute_flat(&query.select, &filtered)
        } else {
            self.compute_grouped(&query.select, &filtered, &query.group_by)
        };

        // Having
        if let Some(ref having) = query.having {
            result_rows.retain(|row| {
                let map = self.row_to_map(&columns, row);
                self.matches_filter(&map, &Some(having.clone()))
            });
        }

        // Order by
        if !query.order_by.is_empty() {
            self.sort_rows(&columns, &mut result_rows, &query.order_by);
        }

        // Limit
        if let Some(limit) = query.limit {
            result_rows.truncate(limit);
        }

        let approximate = self.config.enable_approximate && total_scanned > 10_000;

        let elapsed_ms = start.elapsed().as_millis() as u64;
        self.queries_executed.fetch_add(1, Ordering::Relaxed);
        self.keys_scanned
            .fetch_add(total_scanned, Ordering::Relaxed);
        self.total_query_ms.fetch_add(elapsed_ms, Ordering::Relaxed);
        if approximate {
            self.approximate_queries.fetch_add(1, Ordering::Relaxed);
        }

        Ok(AnalyticsResult {
            columns,
            rows: result_rows,
            total_scanned,
            total_matched,
            execution_ms: elapsed_ms,
            approximate,
        })
    }

    /// Return a human-readable execution plan without running the query.
    pub fn explain(&self, query: &AnalyticsQuery) -> QueryExplanation {
        let mut steps = Vec::new();
        steps.push(format!("Scan source: {}", query.source));

        if query.filter.is_some() {
            steps.push("Apply filter".to_string());
        }
        if !query.group_by.is_empty() {
            steps.push(format!("Group by: {}", query.group_by.join(", ")));
        }
        let has_agg = query.select.iter().any(|s| s.function.is_some());
        if has_agg {
            steps.push("Compute aggregations".to_string());
        }
        if query.having.is_some() {
            steps.push("Apply HAVING filter".to_string());
        }
        if !query.order_by.is_empty() {
            let order_desc: Vec<String> = query
                .order_by
                .iter()
                .map(|o| format!("{} {}", o.field, if o.descending { "DESC" } else { "ASC" }))
                .collect();
            steps.push(format!("Order by: {}", order_desc.join(", ")));
        }
        if let Some(limit) = query.limit {
            steps.push(format!("Limit: {}", limit));
        }

        let estimated_keys = self.get_mock_rows(&query.source).len() as u64;
        let estimated_cost = estimated_keys as f64
            * if has_agg { 2.0 } else { 1.0 }
            * if !query.group_by.is_empty() { 1.5 } else { 1.0 };

        QueryExplanation {
            plan_steps: steps,
            estimated_keys,
            estimated_cost,
            will_use_index: false,
            approximate: self.config.enable_approximate && estimated_keys > 10_000,
        }
    }

    /// Shortcut: execute a single aggregation over a source.
    pub fn aggregate(
        &self,
        source: &str,
        function: AggFunction,
        field: &str,
        filter: Option<FilterExpr>,
    ) -> Result<serde_json::Value, AnalyticsError> {
        let query = AnalyticsQuery {
            source: source.to_string(),
            select: vec![SelectExpr {
                field: field.to_string(),
                alias: None,
                function: Some(function),
            }],
            filter,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            window: None,
        };
        let result = self.execute(query)?;
        result
            .rows
            .first()
            .and_then(|r| r.first().cloned())
            .ok_or_else(|| AnalyticsError::InvalidAggregation("no result".to_string()))
    }

    /// Shortcut: count matching rows.
    pub fn count(&self, source: &str, filter: Option<FilterExpr>) -> Result<u64, AnalyticsError> {
        let val = self.aggregate(source, AggFunction::Count, "*", filter)?;
        Ok(val.as_u64().unwrap_or(0))
    }

    /// Return the top-k values for a field, ordered by descending value.
    pub fn top_k(
        &self,
        source: &str,
        field: &str,
        k: usize,
    ) -> Result<Vec<(String, serde_json::Value)>, AnalyticsError> {
        let rows = self.get_mock_rows(source);
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        // Collect (field_value_string, original_value) pairs
        let mut entries: Vec<(String, serde_json::Value)> = rows
            .iter()
            .filter_map(|row| {
                row.get(field).map(|v| {
                    let key_str = match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    (key_str, v.clone())
                })
            })
            .collect();

        // Sort by numeric value descending, fall back to string comparison.
        entries.sort_by(|a, b| {
            let va = a.1.as_f64().unwrap_or(f64::NEG_INFINITY);
            let vb = b.1.as_f64().unwrap_or(f64::NEG_INFINITY);
            vb.partial_cmp(&va).unwrap_or(std::cmp::Ordering::Equal)
        });

        entries.truncate(k);

        self.queries_executed.fetch_add(1, Ordering::Relaxed);
        self.keys_scanned
            .fetch_add(rows.len() as u64, Ordering::Relaxed);

        Ok(entries)
    }

    /// Compute a histogram over a numeric field.
    pub fn histogram(
        &self,
        source: &str,
        field: &str,
        buckets: usize,
    ) -> Result<Vec<HistogramBucket>, AnalyticsError> {
        if buckets == 0 {
            return Err(AnalyticsError::InvalidQuery(
                "buckets must be > 0".to_string(),
            ));
        }

        let rows = self.get_mock_rows(source);
        let values: Vec<f64> = rows
            .iter()
            .filter_map(|row| row.get(field).and_then(|v| v.as_f64()))
            .collect();

        if values.is_empty() {
            return Err(AnalyticsError::FieldNotFound(field.to_string()));
        }

        let min_val = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max_val = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let range = max_val - min_val;
        let bucket_width = if range == 0.0 {
            1.0
        } else {
            range / buckets as f64
        };

        let total = values.len() as u64;
        let mut counts = vec![0u64; buckets];

        for v in &values {
            let idx = if range == 0.0 {
                0
            } else {
                let i = ((v - min_val) / bucket_width) as usize;
                i.min(buckets - 1)
            };
            counts[idx] += 1;
        }

        let result: Vec<HistogramBucket> = counts
            .into_iter()
            .enumerate()
            .map(|(i, count)| {
                let lower = min_val + (i as f64) * bucket_width;
                let upper = lower + bucket_width;
                HistogramBucket {
                    lower,
                    upper,
                    count,
                    percentage: if total > 0 {
                        (count as f64 / total as f64) * 100.0
                    } else {
                        0.0
                    },
                }
            })
            .collect();

        self.queries_executed.fetch_add(1, Ordering::Relaxed);
        self.keys_scanned
            .fetch_add(rows.len() as u64, Ordering::Relaxed);

        Ok(result)
    }

    /// Return cumulative engine statistics.
    pub fn stats(&self) -> AnalyticsStats {
        let executed = self.queries_executed.load(Ordering::Relaxed);
        let total_ms = self.total_query_ms.load(Ordering::Relaxed);
        AnalyticsStats {
            queries_executed: executed,
            keys_scanned: self.keys_scanned.load(Ordering::Relaxed),
            avg_query_ms: if executed > 0 {
                total_ms as f64 / executed as f64
            } else {
                0.0
            },
            approximate_queries: self.approximate_queries.load(Ordering::Relaxed),
        }
    }

    // -- internal helpers ----------------------------------------------------

    fn validate_query(&self, query: &AnalyticsQuery) -> Result<(), AnalyticsError> {
        if query.source.is_empty() {
            return Err(AnalyticsError::InvalidQuery(
                "source pattern is required".to_string(),
            ));
        }
        if query.select.is_empty() {
            return Err(AnalyticsError::InvalidQuery(
                "at least one select expression is required".to_string(),
            ));
        }
        Ok(())
    }

    fn matches_filter(
        &self,
        row: &HashMap<String, serde_json::Value>,
        filter: &Option<FilterExpr>,
    ) -> bool {
        let filter = match filter {
            Some(f) => f,
            None => return true,
        };

        let field_val = row.get(&filter.field);

        match filter.op {
            FilterOp::IsNull => field_val.is_none() || field_val == Some(&serde_json::Value::Null),
            FilterOp::IsNotNull => {
                field_val.is_some() && field_val != Some(&serde_json::Value::Null)
            }
            FilterOp::Eq => field_val.map_or(false, |v| *v == filter.value),
            FilterOp::Ne => field_val.map_or(true, |v| *v != filter.value),
            FilterOp::Gt => self.compare_values(field_val, &filter.value, |a, b| a > b),
            FilterOp::Lt => self.compare_values(field_val, &filter.value, |a, b| a < b),
            FilterOp::Gte => self.compare_values(field_val, &filter.value, |a, b| a >= b),
            FilterOp::Lte => self.compare_values(field_val, &filter.value, |a, b| a <= b),
            FilterOp::Like => {
                if let (Some(serde_json::Value::String(v)), serde_json::Value::String(pat)) =
                    (field_val, &filter.value)
                {
                    let regex_pat = pat.replace('%', ".*").replace('_', ".");
                    regex::Regex::new(&format!("^{}$", regex_pat))
                        .map_or(false, |re| re.is_match(v))
                } else {
                    false
                }
            }
            FilterOp::In => {
                if let serde_json::Value::Array(arr) = &filter.value {
                    field_val.map_or(false, |v| arr.contains(v))
                } else {
                    false
                }
            }
            FilterOp::Between => {
                if let serde_json::Value::Array(arr) = &filter.value {
                    if arr.len() == 2 {
                        self.compare_values(field_val, &arr[0], |a, b| a >= b)
                            && self.compare_values(field_val, &arr[1], |a, b| a <= b)
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
        }
    }

    fn compare_values<F>(
        &self,
        field_val: Option<&serde_json::Value>,
        cmp_val: &serde_json::Value,
        cmp: F,
    ) -> bool
    where
        F: Fn(f64, f64) -> bool,
    {
        let a = field_val.and_then(|v| v.as_f64());
        let b = cmp_val.as_f64();
        match (a, b) {
            (Some(a), Some(b)) => cmp(a, b),
            _ => false,
        }
    }

    fn compute_flat(
        &self,
        select: &[SelectExpr],
        rows: &[HashMap<String, serde_json::Value>],
    ) -> (Vec<ColumnInfo>, Vec<Vec<serde_json::Value>>) {
        let has_agg = select.iter().any(|s| s.function.is_some());

        if has_agg {
            // Single aggregate row
            let columns: Vec<ColumnInfo> = select
                .iter()
                .map(|s| ColumnInfo {
                    name: s.alias.clone().unwrap_or_else(|| {
                        if let Some(ref f) = s.function {
                            format!("{:?}({})", f, s.field)
                        } else {
                            s.field.clone()
                        }
                    }),
                    data_type: if s.function.is_some() {
                        "number".to_string()
                    } else {
                        "string".to_string()
                    },
                    is_aggregate: s.function.is_some(),
                })
                .collect();

            let values: Vec<serde_json::Value> = select
                .iter()
                .map(|s| self.apply_agg(&s.function, &s.field, rows))
                .collect();

            (columns, vec![values])
        } else {
            // Plain select
            let columns: Vec<ColumnInfo> = select
                .iter()
                .map(|s| ColumnInfo {
                    name: s.alias.clone().unwrap_or_else(|| s.field.clone()),
                    data_type: "string".to_string(),
                    is_aggregate: false,
                })
                .collect();

            let result_rows: Vec<Vec<serde_json::Value>> = rows
                .iter()
                .map(|row| {
                    select
                        .iter()
                        .map(|s| {
                            row.get(&s.field)
                                .cloned()
                                .unwrap_or(serde_json::Value::Null)
                        })
                        .collect()
                })
                .collect();

            (columns, result_rows)
        }
    }

    fn compute_grouped(
        &self,
        select: &[SelectExpr],
        rows: &[HashMap<String, serde_json::Value>],
        group_by: &[String],
    ) -> (Vec<ColumnInfo>, Vec<Vec<serde_json::Value>>) {
        // Build groups
        let mut groups: HashMap<String, Vec<&HashMap<String, serde_json::Value>>> = HashMap::new();

        for row in rows {
            let key: String = group_by
                .iter()
                .map(|g| {
                    row.get(g)
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "null".to_string())
                })
                .collect::<Vec<_>>()
                .join("|");
            groups.entry(key).or_default().push(row);
        }

        let columns: Vec<ColumnInfo> = select
            .iter()
            .map(|s| ColumnInfo {
                name: s.alias.clone().unwrap_or_else(|| {
                    if let Some(ref f) = s.function {
                        format!("{:?}({})", f, s.field)
                    } else {
                        s.field.clone()
                    }
                }),
                data_type: if s.function.is_some() {
                    "number".to_string()
                } else {
                    "string".to_string()
                },
                is_aggregate: s.function.is_some(),
            })
            .collect();

        let result_rows: Vec<Vec<serde_json::Value>> = groups
            .values()
            .map(|group_rows| {
                let owned: Vec<HashMap<String, serde_json::Value>> =
                    group_rows.iter().map(|r| (*r).clone()).collect();
                select
                    .iter()
                    .map(|s| {
                        if s.function.is_some() {
                            self.apply_agg(&s.function, &s.field, &owned)
                        } else {
                            group_rows
                                .first()
                                .and_then(|r| r.get(&s.field).cloned())
                                .unwrap_or(serde_json::Value::Null)
                        }
                    })
                    .collect()
            })
            .collect();

        (columns, result_rows)
    }

    fn apply_agg(
        &self,
        function: &Option<AggFunction>,
        field: &str,
        rows: &[HashMap<String, serde_json::Value>],
    ) -> serde_json::Value {
        let func = match function {
            Some(f) => f,
            None => {
                return rows
                    .first()
                    .and_then(|r| r.get(field).cloned())
                    .unwrap_or(serde_json::Value::Null);
            }
        };

        match func {
            AggFunction::Count => {
                if field == "*" {
                    serde_json::json!(rows.len())
                } else {
                    let count = rows
                        .iter()
                        .filter(|r| r.get(field).map_or(false, |v| !v.is_null()))
                        .count();
                    serde_json::json!(count)
                }
            }
            AggFunction::Sum => {
                let sum: f64 = rows
                    .iter()
                    .filter_map(|r| r.get(field).and_then(|v| v.as_f64()))
                    .sum();
                serde_json::json!(sum)
            }
            AggFunction::Avg => {
                let vals: Vec<f64> = rows
                    .iter()
                    .filter_map(|r| r.get(field).and_then(|v| v.as_f64()))
                    .collect();
                if vals.is_empty() {
                    serde_json::Value::Null
                } else {
                    let avg = vals.iter().sum::<f64>() / vals.len() as f64;
                    serde_json::json!(avg)
                }
            }
            AggFunction::Min => {
                let min = rows
                    .iter()
                    .filter_map(|r| r.get(field).and_then(|v| v.as_f64()))
                    .fold(f64::INFINITY, f64::min);
                if min == f64::INFINITY {
                    serde_json::Value::Null
                } else {
                    serde_json::json!(min)
                }
            }
            AggFunction::Max => {
                let max = rows
                    .iter()
                    .filter_map(|r| r.get(field).and_then(|v| v.as_f64()))
                    .fold(f64::NEG_INFINITY, f64::max);
                if max == f64::NEG_INFINITY {
                    serde_json::Value::Null
                } else {
                    serde_json::json!(max)
                }
            }
            AggFunction::CountDistinct => {
                let distinct: std::collections::HashSet<String> = rows
                    .iter()
                    .filter_map(|r| r.get(field).map(|v| v.to_string()))
                    .collect();
                serde_json::json!(distinct.len())
            }
            AggFunction::Percentile(p) => {
                let mut vals: Vec<f64> = rows
                    .iter()
                    .filter_map(|r| r.get(field).and_then(|v| v.as_f64()))
                    .collect();
                if vals.is_empty() {
                    return serde_json::Value::Null;
                }
                vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let idx = ((p * (vals.len() as f64 - 1.0)).round()) as usize;
                let idx = idx.min(vals.len() - 1);
                serde_json::json!(vals[idx])
            }
            AggFunction::StdDev => {
                let vals: Vec<f64> = rows
                    .iter()
                    .filter_map(|r| r.get(field).and_then(|v| v.as_f64()))
                    .collect();
                if vals.len() < 2 {
                    return serde_json::json!(0.0);
                }
                let mean = vals.iter().sum::<f64>() / vals.len() as f64;
                let var =
                    vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (vals.len() - 1) as f64;
                serde_json::json!(var.sqrt())
            }
            AggFunction::Variance => {
                let vals: Vec<f64> = rows
                    .iter()
                    .filter_map(|r| r.get(field).and_then(|v| v.as_f64()))
                    .collect();
                if vals.len() < 2 {
                    return serde_json::json!(0.0);
                }
                let mean = vals.iter().sum::<f64>() / vals.len() as f64;
                let var =
                    vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (vals.len() - 1) as f64;
                serde_json::json!(var)
            }
            AggFunction::First => rows
                .first()
                .and_then(|r| r.get(field).cloned())
                .unwrap_or(serde_json::Value::Null),
            AggFunction::Last => rows
                .last()
                .and_then(|r| r.get(field).cloned())
                .unwrap_or(serde_json::Value::Null),
        }
    }

    fn row_to_map(
        &self,
        columns: &[ColumnInfo],
        row: &[serde_json::Value],
    ) -> HashMap<String, serde_json::Value> {
        columns
            .iter()
            .zip(row.iter())
            .map(|(c, v)| (c.name.clone(), v.clone()))
            .collect()
    }

    fn sort_rows(
        &self,
        columns: &[ColumnInfo],
        rows: &mut [Vec<serde_json::Value>],
        order_by: &[OrderExpr],
    ) {
        let col_indices: Vec<(usize, bool)> = order_by
            .iter()
            .filter_map(|o| {
                columns
                    .iter()
                    .position(|c| c.name == o.field)
                    .map(|idx| (idx, o.descending))
            })
            .collect();

        rows.sort_by(|a, b| {
            for &(idx, desc) in &col_indices {
                let va = a.get(idx).and_then(|v| v.as_f64()).unwrap_or(0.0);
                let vb = b.get(idx).and_then(|v| v.as_f64()).unwrap_or(0.0);
                let cmp = va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal);
                let cmp = if desc { cmp.reverse() } else { cmp };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_data() -> Vec<HashMap<String, serde_json::Value>> {
        vec![
            HashMap::from([
                ("name".to_string(), serde_json::json!("Alice")),
                ("age".to_string(), serde_json::json!(30)),
                ("department".to_string(), serde_json::json!("Engineering")),
                ("salary".to_string(), serde_json::json!(120_000)),
            ]),
            HashMap::from([
                ("name".to_string(), serde_json::json!("Bob")),
                ("age".to_string(), serde_json::json!(25)),
                ("department".to_string(), serde_json::json!("Marketing")),
                ("salary".to_string(), serde_json::json!(90_000)),
            ]),
            HashMap::from([
                ("name".to_string(), serde_json::json!("Charlie")),
                ("age".to_string(), serde_json::json!(35)),
                ("department".to_string(), serde_json::json!("Engineering")),
                ("salary".to_string(), serde_json::json!(140_000)),
            ]),
            HashMap::from([
                ("name".to_string(), serde_json::json!("Diana")),
                ("age".to_string(), serde_json::json!(28)),
                ("department".to_string(), serde_json::json!("Marketing")),
                ("salary".to_string(), serde_json::json!(95_000)),
            ]),
            HashMap::from([
                ("name".to_string(), serde_json::json!("Eve")),
                ("age".to_string(), serde_json::json!(32)),
                ("department".to_string(), serde_json::json!("Engineering")),
                ("salary".to_string(), serde_json::json!(130_000)),
            ]),
        ]
    }

    fn engine_with_data() -> AnalyticsEngine {
        let engine = AnalyticsEngine::new(AnalyticsConfig::default());
        engine.insert_mock_data("employees:*", sample_data());
        engine
    }

    #[test]
    fn test_basic_aggregation_sum() {
        let engine = engine_with_data();
        let query = AnalyticsQuery {
            source: "employees:*".to_string(),
            select: vec![SelectExpr {
                field: "salary".to_string(),
                alias: Some("total_salary".to_string()),
                function: Some(AggFunction::Sum),
            }],
            filter: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            window: None,
        };
        let result = engine.execute(query).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], serde_json::json!(575_000.0));
        assert_eq!(result.total_matched, 5);
    }

    #[test]
    fn test_aggregation_count_avg() {
        let engine = engine_with_data();
        let result = engine.count("employees:*", None).unwrap();
        assert_eq!(result, 5);

        let avg = engine
            .aggregate("employees:*", AggFunction::Avg, "age", None)
            .unwrap();
        assert_eq!(avg, serde_json::json!(30.0));
    }

    #[test]
    fn test_group_by() {
        let engine = engine_with_data();
        let query = AnalyticsQuery {
            source: "employees:*".to_string(),
            select: vec![
                SelectExpr {
                    field: "department".to_string(),
                    alias: None,
                    function: None,
                },
                SelectExpr {
                    field: "salary".to_string(),
                    alias: Some("avg_salary".to_string()),
                    function: Some(AggFunction::Avg),
                },
            ],
            filter: None,
            group_by: vec!["department".to_string()],
            having: None,
            order_by: vec![],
            limit: None,
            window: None,
        };
        let result = engine.execute(query).unwrap();
        assert_eq!(result.rows.len(), 2); // Engineering & Marketing
    }

    #[test]
    fn test_filter() {
        let engine = engine_with_data();
        let query = AnalyticsQuery {
            source: "employees:*".to_string(),
            select: vec![SelectExpr {
                field: "name".to_string(),
                alias: None,
                function: None,
            }],
            filter: Some(FilterExpr {
                field: "age".to_string(),
                op: FilterOp::Gt,
                value: serde_json::json!(29),
            }),
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            window: None,
        };
        let result = engine.execute(query).unwrap();
        // Alice(30), Charlie(35), Eve(32)
        assert_eq!(result.rows.len(), 3);
    }

    #[test]
    fn test_order_by() {
        let engine = engine_with_data();
        let query = AnalyticsQuery {
            source: "employees:*".to_string(),
            select: vec![
                SelectExpr {
                    field: "name".to_string(),
                    alias: None,
                    function: None,
                },
                SelectExpr {
                    field: "salary".to_string(),
                    alias: None,
                    function: None,
                },
            ],
            filter: None,
            group_by: vec![],
            having: None,
            order_by: vec![OrderExpr {
                field: "salary".to_string(),
                descending: true,
            }],
            limit: Some(3),
            window: None,
        };
        let result = engine.execute(query).unwrap();
        assert_eq!(result.rows.len(), 3);
        // Top salaries: 140k, 130k, 120k
        assert_eq!(result.rows[0][1], serde_json::json!(140_000));
        assert_eq!(result.rows[1][1], serde_json::json!(130_000));
        assert_eq!(result.rows[2][1], serde_json::json!(120_000));
    }

    #[test]
    fn test_top_k() {
        let engine = engine_with_data();
        let top = engine.top_k("employees:*", "salary", 2).unwrap();
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].1, serde_json::json!(140_000));
        assert_eq!(top[1].1, serde_json::json!(130_000));
    }

    #[test]
    fn test_histogram() {
        let engine = engine_with_data();
        let hist = engine.histogram("employees:*", "age", 3).unwrap();
        assert_eq!(hist.len(), 3);
        let total_count: u64 = hist.iter().map(|b| b.count).sum();
        assert_eq!(total_count, 5);
        let total_pct: f64 = hist.iter().map(|b| b.percentage).sum();
        assert!((total_pct - 100.0).abs() < 0.01);
    }

    #[test]
    fn test_explain() {
        let engine = engine_with_data();
        let query = AnalyticsQuery {
            source: "employees:*".to_string(),
            select: vec![SelectExpr {
                field: "salary".to_string(),
                alias: None,
                function: Some(AggFunction::Sum),
            }],
            filter: Some(FilterExpr {
                field: "age".to_string(),
                op: FilterOp::Gt,
                value: serde_json::json!(25),
            }),
            group_by: vec!["department".to_string()],
            having: None,
            order_by: vec![OrderExpr {
                field: "salary".to_string(),
                descending: true,
            }],
            limit: Some(10),
            window: None,
        };
        let plan = engine.explain(&query);
        assert!(!plan.plan_steps.is_empty());
        assert!(plan.plan_steps[0].contains("Scan"));
        assert!(plan.plan_steps.iter().any(|s| s.contains("filter")));
        assert!(plan.plan_steps.iter().any(|s| s.contains("Group")));
    }

    #[test]
    fn test_invalid_query_empty_source() {
        let engine = AnalyticsEngine::new(AnalyticsConfig::default());
        let query = AnalyticsQuery {
            source: "".to_string(),
            select: vec![SelectExpr {
                field: "x".to_string(),
                alias: None,
                function: None,
            }],
            filter: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            window: None,
        };
        assert!(matches!(
            engine.execute(query),
            Err(AnalyticsError::InvalidQuery(_))
        ));
    }

    #[test]
    fn test_stats() {
        let engine = engine_with_data();
        let _ = engine.count("employees:*", None);
        let _ = engine.count("employees:*", None);
        let stats = engine.stats();
        assert!(stats.queries_executed >= 2);
        assert!(stats.keys_scanned > 0);
    }

    #[test]
    fn test_filter_between() {
        let engine = engine_with_data();
        let query = AnalyticsQuery {
            source: "employees:*".to_string(),
            select: vec![SelectExpr {
                field: "name".to_string(),
                alias: None,
                function: None,
            }],
            filter: Some(FilterExpr {
                field: "age".to_string(),
                op: FilterOp::Between,
                value: serde_json::json!([28, 32]),
            }),
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            window: None,
        };
        let result = engine.execute(query).unwrap();
        // Alice(30), Diana(28), Eve(32)
        assert_eq!(result.rows.len(), 3);
    }
}
