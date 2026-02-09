//! Foreign Data Wrappers for Federated Queries
//!
//! Provides the trait interface and concrete implementations for accessing
//! external data sources from FerriteQL. Each wrapper translates queries
//! into source-native operations and pushes predicates where possible.
//!
//! # Supported Sources
//!
//! - **Ferrite**: Other Ferrite clusters (native RESP protocol)
//! - **PostgreSQL**: Relational databases (SQL push-down)
//! - **S3/Parquet**: Columnar analytics files
//! - **REST API**: HTTP JSON endpoints
//! - **CSV**: Local or remote CSV files

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::info;

/// A row of data returned from a foreign data source
pub type FdwRow = HashMap<String, FdwValue>;

/// Values that flow through the foreign data wrapper layer
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FdwValue {
    /// SQL NULL value.
    Null,
    /// Boolean value.
    Bool(bool),
    /// 64-bit signed integer value.
    Int(i64),
    /// 64-bit floating-point value.
    Float(f64),
    /// UTF-8 string value.
    String(String),
    /// Raw byte array value.
    Bytes(Vec<u8>),
    /// Ordered array of FDW values.
    Array(Vec<FdwValue>),
    /// String-keyed map of FDW values.
    Map(HashMap<String, FdwValue>),
}

impl fmt::Display for FdwValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FdwValue::Null => write!(f, "NULL"),
            FdwValue::Bool(b) => write!(f, "{}", b),
            FdwValue::Int(i) => write!(f, "{}", i),
            FdwValue::Float(v) => write!(f, "{}", v),
            FdwValue::String(s) => write!(f, "{}", s),
            FdwValue::Bytes(b) => write!(f, "<{} bytes>", b.len()),
            FdwValue::Array(a) => write!(f, "[{} items]", a.len()),
            FdwValue::Map(m) => write!(f, "{{{} fields}}", m.len()),
        }
    }
}

/// A predicate that can be pushed down to the data source
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FdwPredicate {
    /// field = value
    Eq {
        /// Column name.
        field: String,
        /// Value to compare against.
        value: FdwValue,
    },
    /// field != value
    Ne {
        /// Column name.
        field: String,
        /// Value to compare against.
        value: FdwValue,
    },
    /// field > value
    Gt {
        /// Column name.
        field: String,
        /// Value to compare against.
        value: FdwValue,
    },
    /// field >= value
    Ge {
        /// Column name.
        field: String,
        /// Value to compare against.
        value: FdwValue,
    },
    /// field < value
    Lt {
        /// Column name.
        field: String,
        /// Value to compare against.
        value: FdwValue,
    },
    /// field <= value
    Le {
        /// Column name.
        field: String,
        /// Value to compare against.
        value: FdwValue,
    },
    /// field LIKE pattern
    Like {
        /// Column name.
        field: String,
        /// SQL LIKE pattern string.
        pattern: String,
    },
    /// field IN (values)
    In {
        /// Column name.
        field: String,
        /// Set of values to match against.
        values: Vec<FdwValue>,
    },
    /// field IS NULL
    IsNull {
        /// Column name.
        field: String,
    },
    /// field IS NOT NULL
    IsNotNull {
        /// Column name.
        field: String,
    },
    /// predicate AND predicate
    And(Box<FdwPredicate>, Box<FdwPredicate>),
    /// predicate OR predicate
    Or(Box<FdwPredicate>, Box<FdwPredicate>),
    /// NOT predicate
    Not(Box<FdwPredicate>),
}

/// Schema information for a foreign table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FdwSchema {
    /// Table/collection name
    pub name: String,
    /// Column definitions
    pub columns: Vec<FdwColumn>,
    /// Estimated row count (for query planning)
    pub estimated_rows: u64,
    /// Average row size in bytes
    pub avg_row_bytes: u64,
}

/// A column in a foreign table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FdwColumn {
    /// Column name.
    pub name: String,
    /// Column data type.
    pub data_type: FdwDataType,
    /// Whether the column allows NULL values.
    pub nullable: bool,
    /// Whether this column is part of the primary key.
    pub is_primary_key: bool,
}

/// Data types for schema definition
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FdwDataType {
    /// Boolean type.
    Boolean,
    /// 64-bit signed integer type.
    Integer,
    /// 64-bit floating-point type.
    Float,
    /// UTF-8 string type.
    String,
    /// Raw byte array type.
    Bytes,
    /// Timestamp type.
    Timestamp,
    /// JSON document type.
    Json,
}

/// Capabilities a foreign data wrapper can advertise
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FdwCapabilities {
    /// Can push WHERE predicates to the source
    pub supports_predicate_pushdown: bool,
    /// Can push column selection (SELECT specific columns)
    pub supports_projection_pushdown: bool,
    /// Can push LIMIT to the source
    pub supports_limit_pushdown: bool,
    /// Can push ORDER BY to the source
    pub supports_sort_pushdown: bool,
    /// Can push aggregations to the source
    pub supports_aggregate_pushdown: bool,
    /// Supports write operations (INSERT/UPDATE/DELETE)
    pub supports_writes: bool,
    /// Supports transactions
    pub supports_transactions: bool,
    /// Maximum concurrent connections
    pub max_concurrency: usize,
}

impl Default for FdwCapabilities {
    fn default() -> Self {
        Self {
            supports_predicate_pushdown: true,
            supports_projection_pushdown: true,
            supports_limit_pushdown: false,
            supports_sort_pushdown: false,
            supports_aggregate_pushdown: false,
            supports_writes: false,
            supports_transactions: false,
            max_concurrency: 4,
        }
    }
}

/// A scan request sent to a foreign data wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FdwScanRequest {
    /// Table to scan
    pub table: String,
    /// Columns to return (None = all)
    pub columns: Option<Vec<String>>,
    /// Predicates to apply (if pushdown supported)
    pub predicates: Vec<FdwPredicate>,
    /// Maximum rows to return (if limit pushdown supported)
    pub limit: Option<u64>,
    /// Offset for pagination
    pub offset: Option<u64>,
    /// Sort specification
    pub order_by: Option<Vec<(String, SortDirection)>>,
}

/// Sort direction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortDirection {
    /// Sort in ascending order.
    Ascending,
    /// Sort in descending order.
    Descending,
}

/// Result of a foreign data scan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FdwScanResult {
    /// Rows returned
    pub rows: Vec<FdwRow>,
    /// Total row count (if known, for pagination)
    pub total_count: Option<u64>,
    /// Whether there are more rows available
    pub has_more: bool,
    /// Predicates that were actually pushed down (vs evaluated locally)
    pub pushed_predicates: Vec<usize>,
    /// Execution statistics
    pub stats: FdwScanStats,
}

/// Statistics from a foreign data scan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FdwScanStats {
    /// Time spent in the data source
    pub source_time: Duration,
    /// Time spent in network transfer
    pub transfer_time: Duration,
    /// Bytes transferred from source
    pub bytes_transferred: u64,
    /// Rows scanned at source (may be > returned if filtered)
    pub rows_scanned: u64,
    /// Rows returned
    pub rows_returned: u64,
}

impl Default for FdwScanStats {
    fn default() -> Self {
        Self {
            source_time: Duration::ZERO,
            transfer_time: Duration::ZERO,
            bytes_transferred: 0,
            rows_scanned: 0,
            rows_returned: 0,
        }
    }
}

/// Configuration for a foreign data wrapper connection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FdwConnectionConfig {
    /// Source type name
    pub source_type: String,
    /// Connection parameters (driver-specific)
    pub params: HashMap<String, String>,
    /// Connection pool size
    pub pool_size: usize,
    /// Query timeout
    pub timeout: Duration,
    /// Cache results for this duration
    pub cache_ttl: Option<Duration>,
}

/// Registry of foreign data wrappers
pub struct FdwRegistry {
    /// Registered wrappers by name
    wrappers: HashMap<String, FdwEntry>,
}

struct FdwEntry {
    config: FdwConnectionConfig,
    capabilities: FdwCapabilities,
    schemas: Vec<FdwSchema>,
}

impl FdwRegistry {
    /// Creates an empty registry.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            wrappers: HashMap::new(),
        }
    }

    /// Register a new foreign data wrapper
    pub fn register(
        &mut self,
        name: &str,
        config: FdwConnectionConfig,
        capabilities: FdwCapabilities,
        schemas: Vec<FdwSchema>,
    ) -> Result<(), FdwError> {
        if self.wrappers.contains_key(name) {
            return Err(FdwError::AlreadyRegistered(name.to_string()));
        }

        info!(
            name = name,
            source_type = config.source_type,
            tables = schemas.len(),
            "Registered foreign data wrapper"
        );

        self.wrappers.insert(
            name.to_string(),
            FdwEntry {
                config,
                capabilities,
                schemas,
            },
        );
        Ok(())
    }

    /// Unregister a wrapper
    pub fn unregister(&mut self, name: &str) -> Result<(), FdwError> {
        self.wrappers
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| FdwError::NotFound(name.to_string()))
    }

    /// Get capabilities of a registered wrapper
    pub fn capabilities(&self, name: &str) -> Result<&FdwCapabilities, FdwError> {
        self.wrappers
            .get(name)
            .map(|e| &e.capabilities)
            .ok_or_else(|| FdwError::NotFound(name.to_string()))
    }

    /// Get schemas from a registered wrapper
    pub fn schemas(&self, name: &str) -> Result<&[FdwSchema], FdwError> {
        self.wrappers
            .get(name)
            .map(|e| e.schemas.as_slice())
            .ok_or_else(|| FdwError::NotFound(name.to_string()))
    }

    /// Get config for a registered wrapper
    pub fn config(&self, name: &str) -> Result<&FdwConnectionConfig, FdwError> {
        self.wrappers
            .get(name)
            .map(|e| &e.config)
            .ok_or_else(|| FdwError::NotFound(name.to_string()))
    }

    /// List all registered wrapper names
    pub fn list(&self) -> Vec<String> {
        self.wrappers.keys().cloned().collect()
    }

    /// Check if a wrapper is registered
    pub fn contains(&self, name: &str) -> bool {
        self.wrappers.contains_key(name)
    }

    /// Plan which predicates can be pushed down for a given source
    pub fn plan_pushdown(
        &self,
        source: &str,
        predicates: &[FdwPredicate],
    ) -> Result<PushdownPlan, FdwError> {
        let caps = self.capabilities(source)?;

        let mut pushed = Vec::new();
        let mut local = Vec::new();

        for (i, pred) in predicates.iter().enumerate() {
            if caps.supports_predicate_pushdown && Self::is_pushable(pred) {
                pushed.push(i);
            } else {
                local.push(i);
            }
        }

        Ok(PushdownPlan {
            source: source.to_string(),
            pushed_predicate_indices: pushed,
            local_predicate_indices: local,
        })
    }

    fn is_pushable(pred: &FdwPredicate) -> bool {
        match pred {
            FdwPredicate::Eq { .. }
            | FdwPredicate::Ne { .. }
            | FdwPredicate::Gt { .. }
            | FdwPredicate::Ge { .. }
            | FdwPredicate::Lt { .. }
            | FdwPredicate::Le { .. }
            | FdwPredicate::IsNull { .. }
            | FdwPredicate::IsNotNull { .. } => true,
            FdwPredicate::Like { .. } | FdwPredicate::In { .. } => true,
            FdwPredicate::And(l, r) => Self::is_pushable(l) && Self::is_pushable(r),
            FdwPredicate::Or(l, r) => Self::is_pushable(l) && Self::is_pushable(r),
            FdwPredicate::Not(inner) => Self::is_pushable(inner),
        }
    }
}

/// Plan for predicate pushdown
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushdownPlan {
    /// Foreign data source name.
    pub source: String,
    /// Indices of predicates that will be pushed to the source
    pub pushed_predicate_indices: Vec<usize>,
    /// Indices of predicates that must be evaluated locally
    pub local_predicate_indices: Vec<usize>,
}

/// Errors from foreign data wrapper operations
#[derive(Debug, Clone, thiserror::Error)]
pub enum FdwError {
    /// Wrapper not found in registry.
    #[error("Foreign data wrapper not found: {0}")]
    NotFound(String),

    /// Wrapper name already registered.
    #[error("Foreign data wrapper already registered: {0}")]
    AlreadyRegistered(String),

    /// Failed to connect to the foreign data source.
    #[error("Connection failed to {target}: {reason}")]
    ConnectionFailed {
        /// Host or endpoint that was unreachable.
        target: String,
        /// Human-readable failure reason.
        reason: String,
    },

    /// Query execution failed at the source.
    #[error("Query execution failed: {0}")]
    QueryFailed(String),

    /// Returned schema does not match the expected definition.
    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),

    /// Requested operation is not supported by this wrapper.
    #[error("Unsupported operation: {0}")]
    Unsupported(String),

    /// Operation exceeded the configured timeout.
    #[error("Timeout after {0:?}")]
    Timeout(Duration),
}

/// Helper to generate SQL WHERE clause from FdwPredicates (for SQL sources)
pub fn predicates_to_sql(predicates: &[FdwPredicate]) -> String {
    if predicates.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = predicates.iter().map(predicate_to_sql).collect();
    format!("WHERE {}", parts.join(" AND "))
}

fn predicate_to_sql(pred: &FdwPredicate) -> String {
    match pred {
        FdwPredicate::Eq { field, value } => format!("{} = {}", field, value_to_sql(value)),
        FdwPredicate::Ne { field, value } => format!("{} != {}", field, value_to_sql(value)),
        FdwPredicate::Gt { field, value } => format!("{} > {}", field, value_to_sql(value)),
        FdwPredicate::Ge { field, value } => format!("{} >= {}", field, value_to_sql(value)),
        FdwPredicate::Lt { field, value } => format!("{} < {}", field, value_to_sql(value)),
        FdwPredicate::Le { field, value } => format!("{} <= {}", field, value_to_sql(value)),
        FdwPredicate::Like { field, pattern } => format!("{} LIKE '{}'", field, pattern),
        FdwPredicate::In { field, values } => {
            let vals: Vec<String> = values.iter().map(value_to_sql).collect();
            format!("{} IN ({})", field, vals.join(", "))
        }
        FdwPredicate::IsNull { field } => format!("{} IS NULL", field),
        FdwPredicate::IsNotNull { field } => format!("{} IS NOT NULL", field),
        FdwPredicate::And(l, r) => {
            format!("({} AND {})", predicate_to_sql(l), predicate_to_sql(r))
        }
        FdwPredicate::Or(l, r) => {
            format!("({} OR {})", predicate_to_sql(l), predicate_to_sql(r))
        }
        FdwPredicate::Not(inner) => format!("NOT ({})", predicate_to_sql(inner)),
    }
}

fn value_to_sql(value: &FdwValue) -> String {
    match value {
        FdwValue::Null => "NULL".to_string(),
        FdwValue::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        FdwValue::Int(i) => i.to_string(),
        FdwValue::Float(f) => f.to_string(),
        FdwValue::String(s) => format!("'{}'", s.replace('\'', "''")),
        _ => "NULL".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn postgres_config() -> FdwConnectionConfig {
        let mut params = HashMap::new();
        params.insert("host".to_string(), "localhost".to_string());
        params.insert("port".to_string(), "5432".to_string());
        params.insert("database".to_string(), "mydb".to_string());
        FdwConnectionConfig {
            source_type: "postgres".to_string(),
            params,
            pool_size: 5,
            timeout: Duration::from_secs(30),
            cache_ttl: Some(Duration::from_secs(60)),
        }
    }

    fn user_schema() -> FdwSchema {
        FdwSchema {
            name: "users".to_string(),
            columns: vec![
                FdwColumn {
                    name: "id".to_string(),
                    data_type: FdwDataType::Integer,
                    nullable: false,
                    is_primary_key: true,
                },
                FdwColumn {
                    name: "name".to_string(),
                    data_type: FdwDataType::String,
                    nullable: false,
                    is_primary_key: false,
                },
                FdwColumn {
                    name: "email".to_string(),
                    data_type: FdwDataType::String,
                    nullable: true,
                    is_primary_key: false,
                },
            ],
            estimated_rows: 100_000,
            avg_row_bytes: 256,
        }
    }

    #[test]
    fn test_registry_register_and_list() {
        let mut registry = FdwRegistry::new();
        registry
            .register(
                "pg_users",
                postgres_config(),
                FdwCapabilities::default(),
                vec![user_schema()],
            )
            .unwrap();

        assert!(registry.contains("pg_users"));
        assert_eq!(registry.list().len(), 1);
    }

    #[test]
    fn test_registry_duplicate() {
        let mut registry = FdwRegistry::new();
        registry
            .register(
                "test",
                postgres_config(),
                FdwCapabilities::default(),
                vec![],
            )
            .unwrap();
        let result = registry.register(
            "test",
            postgres_config(),
            FdwCapabilities::default(),
            vec![],
        );
        assert!(matches!(result, Err(FdwError::AlreadyRegistered(_))));
    }

    #[test]
    fn test_registry_unregister() {
        let mut registry = FdwRegistry::new();
        registry
            .register(
                "test",
                postgres_config(),
                FdwCapabilities::default(),
                vec![],
            )
            .unwrap();
        registry.unregister("test").unwrap();
        assert!(!registry.contains("test"));

        let result = registry.unregister("nonexistent");
        assert!(matches!(result, Err(FdwError::NotFound(_))));
    }

    #[test]
    fn test_capabilities() {
        let mut registry = FdwRegistry::new();
        let caps = FdwCapabilities {
            supports_predicate_pushdown: true,
            supports_aggregate_pushdown: true,
            ..Default::default()
        };
        registry
            .register("pg", postgres_config(), caps, vec![])
            .unwrap();

        let caps = registry.capabilities("pg").unwrap();
        assert!(caps.supports_predicate_pushdown);
        assert!(caps.supports_aggregate_pushdown);
    }

    #[test]
    fn test_schemas() {
        let mut registry = FdwRegistry::new();
        registry
            .register(
                "pg",
                postgres_config(),
                FdwCapabilities::default(),
                vec![user_schema()],
            )
            .unwrap();

        let schemas = registry.schemas("pg").unwrap();
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].name, "users");
        assert_eq!(schemas[0].columns.len(), 3);
    }

    #[test]
    fn test_plan_pushdown() {
        let mut registry = FdwRegistry::new();
        registry
            .register("pg", postgres_config(), FdwCapabilities::default(), vec![])
            .unwrap();

        let predicates = vec![
            FdwPredicate::Eq {
                field: "id".to_string(),
                value: FdwValue::Int(42),
            },
            FdwPredicate::Like {
                field: "name".to_string(),
                pattern: "John%".to_string(),
            },
        ];

        let plan = registry.plan_pushdown("pg", &predicates).unwrap();
        assert_eq!(plan.pushed_predicate_indices.len(), 2);
        assert_eq!(plan.local_predicate_indices.len(), 0);
    }

    #[test]
    fn test_plan_pushdown_no_support() {
        let mut registry = FdwRegistry::new();
        let caps = FdwCapabilities {
            supports_predicate_pushdown: false,
            ..Default::default()
        };
        registry
            .register("rest", postgres_config(), caps, vec![])
            .unwrap();

        let predicates = vec![FdwPredicate::Eq {
            field: "id".to_string(),
            value: FdwValue::Int(1),
        }];

        let plan = registry.plan_pushdown("rest", &predicates).unwrap();
        assert_eq!(plan.pushed_predicate_indices.len(), 0);
        assert_eq!(plan.local_predicate_indices.len(), 1);
    }

    #[test]
    fn test_predicates_to_sql() {
        let preds = vec![
            FdwPredicate::Eq {
                field: "age".to_string(),
                value: FdwValue::Int(25),
            },
            FdwPredicate::Like {
                field: "name".to_string(),
                pattern: "John%".to_string(),
            },
        ];
        let sql = predicates_to_sql(&preds);
        assert!(sql.contains("age = 25"));
        assert!(sql.contains("name LIKE 'John%'"));
        assert!(sql.starts_with("WHERE"));
    }

    #[test]
    fn test_empty_predicates_to_sql() {
        assert_eq!(predicates_to_sql(&[]), "");
    }

    #[test]
    fn test_complex_predicate_to_sql() {
        let pred = FdwPredicate::And(
            Box::new(FdwPredicate::Gt {
                field: "age".to_string(),
                value: FdwValue::Int(18),
            }),
            Box::new(FdwPredicate::Or(
                Box::new(FdwPredicate::Eq {
                    field: "status".to_string(),
                    value: FdwValue::String("active".to_string()),
                }),
                Box::new(FdwPredicate::IsNull {
                    field: "deleted_at".to_string(),
                }),
            )),
        );
        let sql = predicate_to_sql(&pred);
        assert!(sql.contains("age > 18"));
        assert!(sql.contains("status = 'active'"));
        assert!(sql.contains("deleted_at IS NULL"));
    }

    #[test]
    fn test_in_predicate_sql() {
        let pred = FdwPredicate::In {
            field: "id".to_string(),
            values: vec![FdwValue::Int(1), FdwValue::Int(2), FdwValue::Int(3)],
        };
        let sql = predicate_to_sql(&pred);
        assert_eq!(sql, "id IN (1, 2, 3)");
    }

    #[test]
    fn test_fdw_value_display() {
        assert_eq!(FdwValue::Null.to_string(), "NULL");
        assert_eq!(FdwValue::Int(42).to_string(), "42");
        assert_eq!(FdwValue::String("hello".to_string()).to_string(), "hello");
        assert_eq!(FdwValue::Bool(true).to_string(), "true");
    }

    #[test]
    fn test_scan_request() {
        let req = FdwScanRequest {
            table: "users".to_string(),
            columns: Some(vec!["id".to_string(), "name".to_string()]),
            predicates: vec![FdwPredicate::Gt {
                field: "id".to_string(),
                value: FdwValue::Int(0),
            }],
            limit: Some(100),
            offset: None,
            order_by: Some(vec![("id".to_string(), SortDirection::Ascending)]),
        };

        assert_eq!(req.table, "users");
        assert_eq!(req.columns.as_ref().unwrap().len(), 2);
        assert_eq!(req.limit, Some(100));
    }

    #[test]
    fn test_sql_string_escaping() {
        let pred = FdwPredicate::Eq {
            field: "name".to_string(),
            value: FdwValue::String("O'Brien".to_string()),
        };
        let sql = predicate_to_sql(&pred);
        assert!(sql.contains("O''Brien")); // Properly escaped
    }

    #[test]
    fn test_not_predicate() {
        let pred = FdwPredicate::Not(Box::new(FdwPredicate::IsNull {
            field: "email".to_string(),
        }));
        let sql = predicate_to_sql(&pred);
        assert_eq!(sql, "NOT (email IS NULL)");
    }
}
