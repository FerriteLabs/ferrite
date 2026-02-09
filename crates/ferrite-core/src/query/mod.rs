//! # FerriteQL - SQL-like Query Language
//!
//! Query key-value data with familiar SQL syntax. FerriteQL brings the power
//! of relational queries to Redis-style data structures.
//!
//! ## Why FerriteQL?
//!
//! Redis requires application-level joins and aggregations. FerriteQL enables:
//!
//! - **Complex queries**: JOIN, GROUP BY, ORDER BY, LIMIT
//! - **Aggregations**: COUNT, SUM, AVG, MIN, MAX, DISTINCT
//! - **Pattern matching**: Query across key patterns like `users:*`
//! - **Materialized views**: Pre-computed query results with auto-refresh
//! - **Prepared statements**: Parse once, execute many times
//!
//! ## Quick Start
//!
//! ### Basic SELECT
//!
//! ```no_run
//! use ferrite::query::{QueryEngine, QueryConfig, ResultSet};
//! use ferrite::storage::Store;
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let store = Arc::new(Store::new());
//! let engine = QueryEngine::new(store, QueryConfig::default());
//!
//! // Query all active users
//! let result: ResultSet = engine.execute(
//!     "SELECT * FROM users:* WHERE status = 'active' LIMIT 100",
//!     0  // database number
//! ).await?;
//!
//! for row in &result.rows {
//!     println!("{:?}", row);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ### JOIN Queries
//!
//! ```no_run
//! use ferrite::query::{QueryEngine, QueryConfig};
//! use ferrite::storage::Store;
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let store = Arc::new(Store::new());
//! let engine = QueryEngine::new(store, QueryConfig::default());
//!
//! // Join users with their orders
//! let result = engine.execute(r#"
//!     SELECT
//!         u.name,
//!         u.email,
//!         o.total,
//!         o.created_at
//!     FROM users:* AS u
//!     JOIN orders:* AS o ON o.user_id = u.id
//!     WHERE o.status = 'completed'
//!     ORDER BY o.created_at DESC
//!     LIMIT 50
//! "#, 0).await?;
//!
//! println!("Found {} orders", result.rows.len());
//! # Ok(())
//! # }
//! ```
//!
//! ### Aggregations
//!
//! ```no_run
//! use ferrite::query::{QueryEngine, QueryConfig};
//! use ferrite::storage::Store;
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let store = Arc::new(Store::new());
//! let engine = QueryEngine::new(store, QueryConfig::default());
//!
//! // Revenue by product category
//! let result = engine.execute(r#"
//!     SELECT
//!         p.category,
//!         COUNT(*) as num_orders,
//!         SUM(o.total) as revenue,
//!         AVG(o.total) as avg_order
//!     FROM products:* AS p
//!     JOIN order_items:* AS oi ON oi.product_id = p.id
//!     JOIN orders:* AS o ON o.id = oi.order_id
//!     WHERE o.status = 'completed'
//!     GROUP BY p.category
//!     ORDER BY revenue DESC
//! "#, 0).await?;
//!
//! for row in &result.rows {
//!     let category = row.get(0).ok_or("missing column")?;
//!     let revenue = row.get(2).ok_or("missing column")?;
//!     println!("{}: ${}", category, revenue);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ### Prepared Statements
//!
//! ```no_run
//! use ferrite::query::{QueryEngine, QueryConfig, Value};
//! use ferrite::storage::Store;
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let store = Arc::new(Store::new());
//! let engine = QueryEngine::new(store, QueryConfig::default());
//!
//! // Prepare statement once
//! engine.prepare("user_orders", r#"
//!     SELECT o.*
//!     FROM orders:* AS o
//!     WHERE o.user_id = $1 AND o.status = $2
//!     ORDER BY o.created_at DESC
//!     LIMIT $3
//! "#).await?;
//!
//! // Execute many times with different parameters
//! let result = engine.execute_prepared(
//!     "user_orders",
//!     vec![
//!         Value::String("user:123".to_string()),
//!         Value::String("completed".to_string()),
//!         Value::Int(10),
//!     ],
//!     0
//! ).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Materialized Views
//!
//! ```no_run
//! use ferrite::query::{QueryEngine, QueryConfig};
//! use ferrite::storage::Store;
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let store = Arc::new(Store::new());
//! let engine = QueryEngine::new(store, QueryConfig::default());
//!
//! // Create a materialized view for dashboard
//! engine.create_view(
//!     "daily_stats",
//!     r#"
//!         SELECT
//!             DATE(o.created_at) as day,
//!             COUNT(*) as orders,
//!             SUM(o.total) as revenue
//!         FROM orders:* AS o
//!         WHERE o.status = 'completed'
//!         GROUP BY DATE(o.created_at)
//!         ORDER BY day DESC
//!         LIMIT 30
//!     "#,
//!     Some(60_000)  // Refresh every 60 seconds
//! ).await?;
//!
//! // Query the view (fast - uses cached results)
//! let result = engine.execute(
//!     "SELECT * FROM VIEW:daily_stats",
//!     0
//! ).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ### EXPLAIN Plans
//!
//! ```no_run
//! use ferrite::query::{QueryEngine, QueryConfig};
//! use ferrite::storage::Store;
//! use std::sync::Arc;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let store = Arc::new(Store::new());
//! let engine = QueryEngine::new(store, QueryConfig::default());
//!
//! // Get query execution plan
//! let plan = engine.explain(r#"
//!     SELECT * FROM users:*
//!     WHERE status = 'active'
//!     ORDER BY created_at DESC
//! "#)?;
//!
//! println!("{}", plan);
//! // Output:
//! // Scan: users:* (estimated: 10000 keys)
//! //   Filter: status = 'active'
//! //   Sort: created_at DESC
//! //   Memory: ~2.5MB estimated
//! # Ok(())
//! # }
//! ```
//!
//! ## SQL Syntax Reference
//!
//! ### SELECT
//!
//! ```sql
//! SELECT [DISTINCT] column [, column ...]
//! FROM pattern [AS alias]
//! [JOIN pattern [AS alias] ON condition]
//! [WHERE condition]
//! [GROUP BY column [, column ...]]
//! [HAVING condition]
//! [ORDER BY column [ASC|DESC] [, column [ASC|DESC] ...]]
//! [LIMIT n]
//! [OFFSET n]
//! ```
//!
//! ### Supported Functions
//!
//! | Category | Functions |
//! |----------|-----------|
//! | Aggregate | COUNT, SUM, AVG, MIN, MAX, COUNT(DISTINCT) |
//! | String | UPPER, LOWER, LENGTH, SUBSTRING, CONCAT, TRIM |
//! | Math | ABS, ROUND, CEIL, FLOOR, MOD |
//! | Date | DATE, YEAR, MONTH, DAY, HOUR, NOW |
//! | JSON | JSON_EXTRACT, JSON_TYPE, JSON_ARRAY_LENGTH |
//! | Null | COALESCE, NULLIF, IFNULL |
//!
//! ### Operators
//!
//! | Type | Operators |
//! |------|-----------|
//! | Comparison | =, !=, <>, <, >, <=, >= |
//! | Logical | AND, OR, NOT |
//! | Pattern | LIKE, NOT LIKE, GLOB |
//! | Null | IS NULL, IS NOT NULL |
//! | Range | BETWEEN, IN |
//!
//! ## Redis CLI Commands
//!
//! ```text
//! # Execute a query
//! QUERY SELECT * FROM users:* WHERE status = 'active'
//!
//! # Query with JSON output
//! QUERY.JSON SELECT u.name, u.email FROM users:* AS u
//!
//! # Explain query plan
//! QUERY.EXPLAIN SELECT * FROM orders:* WHERE total > 100
//!
//! # Prepare statement
//! QUERY.PREPARE get_user SELECT * FROM users:* WHERE id = $1
//!
//! # Execute prepared statement
//! QUERY.EXEC get_user "user:123"
//!
//! # Create materialized view
//! QUERY.VIEW.CREATE daily_stats REFRESH 60000 AS
//!   SELECT DATE(created_at) as day, COUNT(*) FROM orders:* GROUP BY day
//!
//! # Drop view
//! QUERY.VIEW.DROP daily_stats
//!
//! # List views
//! QUERY.VIEW.LIST
//! ```
//!
//! ## Configuration
//!
//! ```no_run
//! use ferrite::query::QueryConfig;
//!
//! let config = QueryConfig {
//!     max_scan_rows: 1_000_000,      // Maximum keys to scan
//!     max_memory_bytes: 256 * 1024 * 1024,  // 256MB memory limit
//!     timeout_ms: 30_000,            // 30 second timeout
//!     cache_enabled: true,           // Cache query results
//!     cache_ttl_secs: 60,            // Cache TTL
//!     max_concurrent_queries: 100,   // Concurrent query limit
//!     explain_enabled: true,         // Enable EXPLAIN
//! };
//! ```
//!
//! ## Performance Tips
//!
//! | Technique | Impact | Notes |
//! |-----------|--------|-------|
//! | Use specific patterns | High | `users:active:*` vs `users:*` |
//! | Add LIMIT | High | Prevents scanning everything |
//! | Prepared statements | Medium | Avoid repeated parsing |
//! | Materialized views | High | Pre-compute expensive queries |
//! | Index on sort columns | High | Fast ORDER BY |
//!
//! ## Query Statistics
//!
//! ```no_run
//! use ferrite::query::{QueryEngine, QueryConfig, QueryStats};
//! use ferrite::storage::Store;
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let store = Arc::new(Store::new());
//! let engine = QueryEngine::new(store, QueryConfig::default());
//!
//! let result = engine.execute("SELECT * FROM users:*", 0).await?;
//!
//! let stats = &result.stats;
//! println!("Keys scanned: {}", stats.keys_scanned);
//! println!("Rows examined: {}", stats.rows_examined);
//! println!("Rows returned: {}", stats.rows_returned);
//! println!("Execution time: {}μs", stats.execution_time_us);
//! println!("Memory used: {} bytes", stats.memory_used_bytes);
//! println!("Used index: {}", stats.used_index);
//! # Ok(())
//! # }
//! ```
//!
//! ## Best Practices
//!
//! 1. **Use specific key patterns**: Reduces scan scope significantly
//! 2. **Always use LIMIT**: Prevent runaway queries
//! 3. **Leverage prepared statements**: For repeated queries
//! 4. **Create views for dashboards**: Pre-compute expensive aggregations
//! 5. **Monitor query stats**: Track slow queries and optimize

/// AI-driven query optimizer with workload analysis.
pub mod ai_optimizer;
/// Abstract syntax tree definitions for FerriteQL.
pub mod ast;
/// Query plan executor against the storage engine.
pub mod executor;
/// Foreign data wrapper support.
pub mod fdw;
/// Federated query execution across multiple instances.
pub mod federation;
/// Built-in scalar and aggregate functions.
pub mod functions;
/// Incremental view refresh engine.
pub mod incremental;
/// JIT compilation for query expressions.
pub mod jit;
/// Lexer/tokenizer for FerriteQL.
pub mod lexer;
/// Query optimizer for plan transformation.
pub mod optimizer;
/// FerriteQL query parser.
pub mod parser;
/// Query planner that converts AST to execution plans.
pub mod planner;
/// Type system and schema definitions.
pub mod types;
/// Materialized view management and subscriptions.
pub mod views;

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

pub use ast::*;
pub use executor::QueryExecutor;
pub use parser::QueryParser;
pub use planner::QueryPlanner;

use crate::protocol::Frame;
use crate::storage::Store;

/// Query engine configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryConfig {
    /// Maximum number of rows to scan
    pub max_scan_rows: usize,
    /// Maximum memory for query execution (bytes)
    pub max_memory_bytes: usize,
    /// Query timeout in milliseconds
    pub timeout_ms: u64,
    /// Enable query caching
    pub cache_enabled: bool,
    /// Cache TTL in seconds
    pub cache_ttl_secs: u64,
    /// Maximum concurrent queries
    pub max_concurrent_queries: usize,
    /// Enable explain plans
    pub explain_enabled: bool,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            max_scan_rows: 1_000_000,
            max_memory_bytes: 256 * 1024 * 1024, // 256MB
            timeout_ms: 30_000,
            cache_enabled: true,
            cache_ttl_secs: 60,
            max_concurrent_queries: 100,
            explain_enabled: true,
        }
    }
}

/// Query result row
#[derive(Clone, Debug)]
pub struct Row {
    /// Column values
    pub values: Vec<Value>,
}

impl Row {
    /// Creates a new row with the given column values.
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    /// Returns a reference to the value at the given column index.
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }
}

/// Query result value
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum Value {
    /// SQL NULL value.
    Null,
    /// Boolean value.
    Bool(bool),
    /// 64-bit signed integer.
    Int(i64),
    /// 64-bit floating point number.
    Float(f64),
    /// UTF-8 string value.
    String(String),
    /// Raw byte sequence.
    Bytes(Bytes),
    /// Ordered list of values.
    Array(Vec<Value>),
    /// Key-value map of string keys to values.
    Map(Vec<(String, Value)>),
}

impl Value {
    /// Returns the value as a string slice, if it is a `String` variant.
    pub fn as_string(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Returns the value as an `i64`, coercing from float or string if possible.
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(n) => Some(*n),
            Value::Float(f) => Some(*f as i64),
            Value::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Returns the value as an `f64`, coercing from int or string if possible.
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            Value::Int(n) => Some(*n as f64),
            Value::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Returns `true` if this value is `Null`.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Returns `true` if the value is considered truthy (non-null, non-zero, non-empty).
    pub fn is_truthy(&self) -> bool {
        match self {
            Value::Null => false,
            Value::Bool(b) => *b,
            Value::Int(n) => *n != 0,
            Value::Float(f) => *f != 0.0,
            Value::String(s) => !s.is_empty(),
            Value::Bytes(b) => !b.is_empty(),
            Value::Array(a) => !a.is_empty(),
            Value::Map(m) => !m.is_empty(),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Int(n) => write!(f, "{}", n),
            Value::Float(n) => write!(f, "{}", n),
            Value::String(s) => write!(f, "{}", s),
            Value::Bytes(b) => write!(f, "{}", String::from_utf8_lossy(b)),
            Value::Array(a) => {
                write!(f, "[")?;
                for (i, v) in a.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, "]")
            }
            Value::Map(m) => {
                write!(f, "{{")?;
                for (i, (k, v)) in m.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", k, v)?;
                }
                write!(f, "}}")
            }
        }
    }
}

/// Query result set
#[derive(Clone, Debug)]
pub struct ResultSet {
    /// Column names
    pub columns: Vec<String>,
    /// Result rows
    pub rows: Vec<Row>,
    /// Execution statistics
    pub stats: QueryStats,
}

impl ResultSet {
    /// Creates a new result set with the given columns and rows.
    pub fn new(columns: Vec<String>, rows: Vec<Row>) -> Self {
        Self {
            columns,
            rows,
            stats: QueryStats::default(),
        }
    }

    /// Creates an empty result set with no columns or rows.
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            stats: QueryStats::default(),
        }
    }

    /// Attaches execution statistics to this result set.
    pub fn with_stats(mut self, stats: QueryStats) -> Self {
        self.stats = stats;
        self
    }

    /// Convert to RESP Frame
    pub fn to_frame(&self) -> Frame {
        let mut result = Vec::new();

        // Add column names as first row
        let header: Vec<Frame> = self
            .columns
            .iter()
            .map(|c| Frame::Bulk(Some(Bytes::from(c.clone()))))
            .collect();
        result.push(Frame::Array(Some(header)));

        // Add data rows
        for row in &self.rows {
            let row_frames: Vec<Frame> = row
                .values
                .iter()
                .map(|v| match v {
                    Value::Null => Frame::Null,
                    Value::Bool(b) => Frame::Boolean(*b),
                    Value::Int(n) => Frame::Integer(*n),
                    Value::Float(f) => Frame::Double(*f),
                    Value::String(s) => Frame::Bulk(Some(Bytes::from(s.clone()))),
                    Value::Bytes(b) => Frame::Bulk(Some(b.clone())),
                    Value::Array(a) => {
                        let frames: Vec<Frame> = a.iter().map(value_to_frame).collect();
                        Frame::Array(Some(frames))
                    }
                    Value::Map(m) => {
                        let mut map = HashMap::new();
                        for (k, v) in m {
                            map.insert(Bytes::from(k.clone()), value_to_frame(v));
                        }
                        Frame::Map(map)
                    }
                })
                .collect();
            result.push(Frame::Array(Some(row_frames)));
        }

        Frame::Array(Some(result))
    }
}

fn value_to_frame(v: &Value) -> Frame {
    match v {
        Value::Null => Frame::Null,
        Value::Bool(b) => Frame::Boolean(*b),
        Value::Int(n) => Frame::Integer(*n),
        Value::Float(f) => Frame::Double(*f),
        Value::String(s) => Frame::Bulk(Some(Bytes::from(s.clone()))),
        Value::Bytes(b) => Frame::Bulk(Some(b.clone())),
        Value::Array(a) => {
            let frames: Vec<Frame> = a.iter().map(value_to_frame).collect();
            Frame::Array(Some(frames))
        }
        Value::Map(m) => {
            let mut map = HashMap::new();
            for (k, v) in m {
                map.insert(Bytes::from(k.clone()), value_to_frame(v));
            }
            Frame::Map(map)
        }
    }
}

/// Query execution statistics
#[derive(Clone, Debug, Default)]
pub struct QueryStats {
    /// Number of keys scanned
    pub keys_scanned: u64,
    /// Number of rows examined
    pub rows_examined: u64,
    /// Number of rows returned
    pub rows_returned: u64,
    /// Execution time in microseconds
    pub execution_time_us: u64,
    /// Memory used in bytes
    pub memory_used_bytes: u64,
    /// Whether the query used an index
    pub used_index: bool,
    /// Index name if used
    pub index_name: Option<String>,
}

/// Query engine for executing FerriteQL queries
pub struct QueryEngine {
    store: Arc<Store>,
    config: QueryConfig,
    parser: QueryParser,
    planner: QueryPlanner,
    executor: QueryExecutor,
    views: Arc<RwLock<views::ViewManager>>,
    prepared: Arc<RwLock<HashMap<String, PreparedStatement>>>,
}

/// Prepared statement
pub struct PreparedStatement {
    /// Name used to reference this prepared statement.
    pub name: String,
    /// The parsed query AST.
    pub query: Statement,
    /// Expected parameter types for bind variables.
    pub param_types: Vec<types::DataType>,
}

impl QueryEngine {
    /// Create a new query engine
    pub fn new(store: Arc<Store>, config: QueryConfig) -> Self {
        Self {
            store: store.clone(),
            config: config.clone(),
            parser: QueryParser::new(),
            planner: QueryPlanner::new(config.clone()),
            executor: QueryExecutor::new(store.clone(), config.clone()),
            views: Arc::new(RwLock::new(views::ViewManager::new(store))),
            prepared: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Execute a query string
    pub async fn execute(&self, query: &str, db: u8) -> Result<ResultSet, QueryError> {
        let start = std::time::Instant::now();

        // Parse the query
        let statement = self.parser.parse(query)?;

        // Plan the query
        let plan = self.planner.plan(&statement)?;

        // Execute the plan
        let mut result = self.executor.execute(&plan, db).await?;

        // Update stats
        result.stats.execution_time_us = start.elapsed().as_micros() as u64;

        Ok(result)
    }

    /// Execute a query and return as Frame
    pub async fn execute_to_frame(&self, query: &str, db: u8) -> Frame {
        match self.execute(query, db).await {
            Ok(result) => result.to_frame(),
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    /// Prepare a statement
    pub async fn prepare(&self, name: &str, query: &str) -> Result<(), QueryError> {
        let statement = self.parser.parse(query)?;
        let param_types = self.parser.extract_param_types(&statement);

        let prepared = PreparedStatement {
            name: name.to_string(),
            query: statement,
            param_types,
        };

        let mut stmts = self.prepared.write().await;
        stmts.insert(name.to_string(), prepared);

        Ok(())
    }

    /// Execute a prepared statement
    pub async fn execute_prepared(
        &self,
        name: &str,
        params: Vec<Value>,
        db: u8,
    ) -> Result<ResultSet, QueryError> {
        let stmts = self.prepared.read().await;
        let prepared = stmts
            .get(name)
            .ok_or_else(|| QueryError::PreparedNotFound(name.to_string()))?;

        if params.len() != prepared.param_types.len() {
            return Err(QueryError::InvalidParams(format!(
                "expected {} params, got {}",
                prepared.param_types.len(),
                params.len()
            )));
        }

        // Plan with parameters substituted
        let plan = self.planner.plan_with_params(&prepared.query, &params)?;

        // Execute
        self.executor.execute(&plan, db).await
    }

    /// Create a materialized view
    pub async fn create_view(
        &self,
        name: &str,
        query: &str,
        refresh_interval_ms: Option<u64>,
    ) -> Result<(), QueryError> {
        let statement = self.parser.parse(query)?;
        let mut views = self.views.write().await;
        views.create(name, statement, refresh_interval_ms).await
    }

    /// Drop a materialized view
    pub async fn drop_view(&self, name: &str) -> Result<(), QueryError> {
        let mut views = self.views.write().await;
        views.drop_view(name).await
    }

    /// Get explain plan for a query
    pub fn explain(&self, query: &str) -> Result<String, QueryError> {
        let statement = self.parser.parse(query)?;
        let plan = self.planner.plan(&statement)?;
        Ok(self.planner.explain(&plan))
    }
}

/// Query errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum QueryError {
    /// Query could not be parsed.
    #[error("parse error: {0}")]
    Parse(String),

    /// Invalid SQL syntax.
    #[error("invalid syntax: {0}")]
    Syntax(String),

    /// Referenced column does not exist.
    #[error("unknown column: {0}")]
    UnknownColumn(String),

    /// Referenced table or key pattern does not exist.
    #[error("unknown table/pattern: {0}")]
    UnknownTable(String),

    /// Type mismatch in expression or comparison.
    #[error("type error: {0}")]
    TypeError(String),

    /// Runtime execution failure.
    #[error("execution error: {0}")]
    Execution(String),

    /// Query exceeded the configured timeout.
    #[error("timeout")]
    Timeout,

    /// Query exceeded the configured memory limit.
    #[error("memory limit exceeded")]
    MemoryLimitExceeded,

    /// Query exceeded the configured row limit.
    #[error("row limit exceeded")]
    RowLimitExceeded,

    /// Referenced prepared statement was not found.
    #[error("prepared statement not found: {0}")]
    PreparedNotFound(String),

    /// Invalid parameters for a prepared statement.
    #[error("invalid parameters: {0}")]
    InvalidParams(String),

    /// Error operating on a materialized view.
    #[error("view error: {0}")]
    View(String),

    /// Feature or syntax not yet supported.
    #[error("unsupported: {0}")]
    Unsupported(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_display() {
        assert_eq!(Value::Null.to_string(), "NULL");
        assert_eq!(Value::Int(42).to_string(), "42");
        assert_eq!(Value::String("hello".to_string()).to_string(), "hello");
        assert_eq!(Value::Bool(true).to_string(), "true");
    }

    #[test]
    fn test_value_is_truthy() {
        assert!(!Value::Null.is_truthy());
        assert!(Value::Bool(true).is_truthy());
        assert!(!Value::Bool(false).is_truthy());
        assert!(Value::Int(1).is_truthy());
        assert!(!Value::Int(0).is_truthy());
        assert!(Value::String("hello".to_string()).is_truthy());
        assert!(!Value::String("".to_string()).is_truthy());
    }

    #[test]
    fn test_result_set() {
        let rs = ResultSet::new(
            vec!["name".to_string(), "age".to_string()],
            vec![
                Row::new(vec![Value::String("Alice".to_string()), Value::Int(30)]),
                Row::new(vec![Value::String("Bob".to_string()), Value::Int(25)]),
            ],
        );

        assert_eq!(rs.columns.len(), 2);
        assert_eq!(rs.rows.len(), 2);
    }
}
