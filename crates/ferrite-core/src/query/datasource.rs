//! Cross-Model Data Source Registry for FerriteQL
//!
//! Provides a unified abstraction layer that allows FerriteQL to query across
//! different data models (key-value, vector search, graph, timeseries, document).
//! Each data source registers itself with the [`DataSourceRegistry`] and exposes
//! a common [`DataSource`] trait for schema introspection, scanning, and lookup.
//!
//! # Architecture
//!
//! ```text
//! FerriteQL Engine
//!     │
//!     ▼
//! DataSourceRegistry ─┬─ KeyValueSource  (main KV store)
//!                     ├─ VectorSource    (vector search)
//!                     ├─ GraphSource     (graph traversal)
//!                     ├─ TimeSeriesSource(time-series data)
//!                     └─ DocumentSource  (JSON documents)
//! ```
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use ferrite_core::query::datasource::{
//!     DataSourceRegistry, DataSourceConfig, KeyValueSource,
//! };
//!
//! let registry = DataSourceRegistry::new(DataSourceConfig::default());
//! let kv = Arc::new(KeyValueSource::new());
//! registry.register("kv", kv).unwrap();
//!
//! let sources = registry.list();
//! assert_eq!(sources.len(), 1);
//! ```

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::query::Value;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors produced by data source operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum DataSourceError {
    /// The named data source was not found in the registry.
    #[error("data source not found: {0}")]
    NotFound(String),

    /// A data source with that name is already registered.
    #[error("data source already registered: {0}")]
    AlreadyRegistered(String),

    /// The registry has reached its configured maximum number of sources.
    #[error("registry full: max {max} sources")]
    RegistryFull {
        /// Configured maximum number of data sources.
        max: usize,
    },

    /// A scan or lookup operation failed at the source.
    #[error("source operation failed: {0}")]
    OperationFailed(String),

    /// The requested operation is not supported by this data source.
    #[error("unsupported operation {operation} on source {source_name}")]
    UnsupportedOperation {
        /// Name of the data source.
        source_name: String,
        /// Operation that was attempted.
        operation: String,
    },

    /// A schema-related error (e.g. column mismatch).
    #[error("schema error: {0}")]
    SchemaError(String),
}

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// The backing data model of a data source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataSourceType {
    /// Standard key-value store.
    KeyValue,
    /// Vector similarity search.
    Vector,
    /// Graph nodes and edges.
    Graph,
    /// Time-series metrics / events.
    TimeSeries,
    /// JSON / BSON documents.
    Document,
    /// Full-text search index.
    Search,
    /// Append-only event stream.
    Stream,
}

impl fmt::Display for DataSourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataSourceType::KeyValue => write!(f, "KeyValue"),
            DataSourceType::Vector => write!(f, "Vector"),
            DataSourceType::Graph => write!(f, "Graph"),
            DataSourceType::TimeSeries => write!(f, "TimeSeries"),
            DataSourceType::Document => write!(f, "Document"),
            DataSourceType::Search => write!(f, "Search"),
            DataSourceType::Stream => write!(f, "Stream"),
        }
    }
}

/// Operations a data source may support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataSourceOperation {
    /// Scan keys/rows matching a pattern.
    Scan,
    /// Point-lookup by key.
    Lookup,
    /// Predicate-based filtering.
    Filter,
    /// Aggregation (COUNT, SUM, AVG …).
    Aggregate,
    /// Join with another source.
    Join,
    /// Server-side sorting.
    Sort,
}

impl fmt::Display for DataSourceOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataSourceOperation::Scan => write!(f, "Scan"),
            DataSourceOperation::Lookup => write!(f, "Lookup"),
            DataSourceOperation::Filter => write!(f, "Filter"),
            DataSourceOperation::Aggregate => write!(f, "Aggregate"),
            DataSourceOperation::Join => write!(f, "Join"),
            DataSourceOperation::Sort => write!(f, "Sort"),
        }
    }
}

// ---------------------------------------------------------------------------
// Schema types
// ---------------------------------------------------------------------------

/// Column data type within a data source schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnType {
    /// UTF-8 string.
    String,
    /// 64-bit signed integer.
    Integer,
    /// 64-bit floating point.
    Float,
    /// Boolean.
    Boolean,
    /// Raw byte array.
    Bytes,
    /// Timestamp (milliseconds since epoch).
    Timestamp,
    /// JSON value.
    Json,
    /// Fixed-length floating-point vector.
    Vector,
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnType::String => write!(f, "STRING"),
            ColumnType::Integer => write!(f, "INTEGER"),
            ColumnType::Float => write!(f, "FLOAT"),
            ColumnType::Boolean => write!(f, "BOOLEAN"),
            ColumnType::Bytes => write!(f, "BYTES"),
            ColumnType::Timestamp => write!(f, "TIMESTAMP"),
            ColumnType::Json => write!(f, "JSON"),
            ColumnType::Vector => write!(f, "VECTOR"),
        }
    }
}

/// Definition of a single column in a data source schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name.
    pub name: String,
    /// Column data type.
    pub column_type: ColumnType,
    /// Whether NULL values are allowed.
    pub nullable: bool,
}

/// Schema describing the columns a data source produces.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSourceSchema {
    /// Ordered list of column definitions.
    pub columns: Vec<ColumnDef>,
}

impl DataSourceSchema {
    /// Creates a new schema from column definitions.
    pub fn new(columns: Vec<ColumnDef>) -> Self {
        Self { columns }
    }

    /// Returns the column names in order.
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Looks up a column definition by name.
    pub fn get_column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }
}

// ---------------------------------------------------------------------------
// Row type
// ---------------------------------------------------------------------------

/// A single row of data returned from a data source.
///
/// Values are ordered to match the source's [`DataSourceSchema`].
#[derive(Debug, Clone)]
pub struct DataSourceRow {
    /// The key or identifier for this row.
    pub key: String,
    /// Column values in schema order.
    pub values: Vec<Value>,
}

impl DataSourceRow {
    /// Creates a new row.
    pub fn new(key: String, values: Vec<Value>) -> Self {
        Self { key, values }
    }

    /// Returns the value at the given column index.
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }
}

// ---------------------------------------------------------------------------
// DataSource trait
// ---------------------------------------------------------------------------

/// Trait implemented by each data model adapter.
///
/// Every data source plugged into FerriteQL must expose scanning, point-lookup,
/// and schema introspection through this interface.
#[async_trait]
pub trait DataSource: Send + Sync {
    /// Human-readable name of the data source.
    fn name(&self) -> &str;

    /// The backing data-model type.
    fn source_type(&self) -> DataSourceType;

    /// Operations this source supports.
    fn supported_operations(&self) -> Vec<DataSourceOperation>;

    /// Scan rows matching `pattern`, returning at most `limit` results.
    async fn scan(
        &self,
        pattern: &str,
        limit: usize,
    ) -> Result<Vec<DataSourceRow>, DataSourceError>;

    /// Point-lookup a single row by `key`.
    async fn lookup(&self, key: &str) -> Result<Option<DataSourceRow>, DataSourceError>;

    /// Returns the schema of the rows this source produces.
    fn schema(&self) -> DataSourceSchema;
}

// ---------------------------------------------------------------------------
// Registry types
// ---------------------------------------------------------------------------

/// Configuration for the [`DataSourceRegistry`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSourceConfig {
    /// Maximum number of data sources that can be registered.
    pub max_sources: usize,
    /// Default row limit applied when a scan omits an explicit limit.
    pub default_scan_limit: usize,
}

impl Default for DataSourceConfig {
    fn default() -> Self {
        Self {
            max_sources: 64,
            default_scan_limit: 1000,
        }
    }
}

/// Metadata about a registered data source (returned by [`DataSourceRegistry::list`]).
#[derive(Debug, Clone)]
pub struct DataSourceInfo {
    /// Registered name.
    pub name: String,
    /// Backing data-model type.
    pub source_type: DataSourceType,
    /// Operations the source supports.
    pub supported_operations: Vec<DataSourceOperation>,
    /// Column names exposed by the source.
    pub schema_columns: Vec<String>,
}

/// Thread-safe registry of data sources for FerriteQL.
///
/// Uses [`DashMap`] for lock-free concurrent reads and writes so that
/// sources can be registered or looked up from any async task.
pub struct DataSourceRegistry {
    sources: DashMap<String, Arc<dyn DataSource>>,
    config: DataSourceConfig,
}

impl DataSourceRegistry {
    /// Creates a new, empty registry with the given configuration.
    pub fn new(config: DataSourceConfig) -> Self {
        Self {
            sources: DashMap::new(),
            config,
        }
    }

    /// Registers a data source under the given `name`.
    ///
    /// Returns an error if a source with the same name already exists or if the
    /// registry is at capacity.
    pub fn register(&self, name: &str, source: Arc<dyn DataSource>) -> Result<(), DataSourceError> {
        if self.sources.len() >= self.config.max_sources {
            return Err(DataSourceError::RegistryFull {
                max: self.config.max_sources,
            });
        }
        if self.sources.contains_key(name) {
            return Err(DataSourceError::AlreadyRegistered(name.to_string()));
        }
        self.sources.insert(name.to_string(), source);
        Ok(())
    }

    /// Retrieves a data source by name.
    pub fn get(&self, name: &str) -> Option<Arc<dyn DataSource>> {
        self.sources.get(name).map(|r| Arc::clone(r.value()))
    }

    /// Removes and returns a data source by name.
    pub fn unregister(&self, name: &str) -> Result<Arc<dyn DataSource>, DataSourceError> {
        self.sources
            .remove(name)
            .map(|(_, v)| v)
            .ok_or_else(|| DataSourceError::NotFound(name.to_string()))
    }

    /// Lists metadata for every registered data source.
    pub fn list(&self) -> Vec<DataSourceInfo> {
        self.sources
            .iter()
            .map(|entry| {
                let source = entry.value();
                DataSourceInfo {
                    name: entry.key().clone(),
                    source_type: source.source_type(),
                    supported_operations: source.supported_operations(),
                    schema_columns: source
                        .schema()
                        .column_names()
                        .into_iter()
                        .map(String::from)
                        .collect(),
                }
            })
            .collect()
    }

    /// Returns all sources whose [`DataSourceType`] matches `source_type`.
    pub fn get_by_type(&self, source_type: DataSourceType) -> Vec<Arc<dyn DataSource>> {
        self.sources
            .iter()
            .filter(|entry| entry.value().source_type() == source_type)
            .map(|entry| Arc::clone(entry.value()))
            .collect()
    }

    /// Returns the number of registered data sources.
    pub fn len(&self) -> usize {
        self.sources.len()
    }

    /// Returns `true` if no data sources are registered.
    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    /// Returns a reference to the registry configuration.
    pub fn config(&self) -> &DataSourceConfig {
        &self.config
    }
}

// ---------------------------------------------------------------------------
// Built-in adapters
// ---------------------------------------------------------------------------

/// Adapter that exposes the main Ferrite key-value store as a data source.
pub struct KeyValueSource {
    name: String,
}

impl KeyValueSource {
    /// Creates a new key-value data source with the default name `"kv"`.
    pub fn new() -> Self {
        Self {
            name: "kv".to_string(),
        }
    }
}

impl Default for KeyValueSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DataSource for KeyValueSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn source_type(&self) -> DataSourceType {
        DataSourceType::KeyValue
    }

    fn supported_operations(&self) -> Vec<DataSourceOperation> {
        vec![
            DataSourceOperation::Scan,
            DataSourceOperation::Lookup,
            DataSourceOperation::Filter,
            DataSourceOperation::Sort,
        ]
    }

    async fn scan(
        &self,
        _pattern: &str,
        _limit: usize,
    ) -> Result<Vec<DataSourceRow>, DataSourceError> {
        // Placeholder — will be wired to the storage engine
        Ok(Vec::new())
    }

    async fn lookup(&self, _key: &str) -> Result<Option<DataSourceRow>, DataSourceError> {
        Ok(None)
    }

    fn schema(&self) -> DataSourceSchema {
        DataSourceSchema::new(vec![
            ColumnDef {
                name: "key".to_string(),
                column_type: ColumnType::String,
                nullable: false,
            },
            ColumnDef {
                name: "value".to_string(),
                column_type: ColumnType::String,
                nullable: true,
            },
            ColumnDef {
                name: "ttl".to_string(),
                column_type: ColumnType::Integer,
                nullable: true,
            },
            ColumnDef {
                name: "type".to_string(),
                column_type: ColumnType::String,
                nullable: false,
            },
        ])
    }
}

/// Stub adapter for vector similarity search results.
pub struct VectorSource {
    name: String,
}

impl VectorSource {
    /// Creates a new vector data source with the default name `"vectors"`.
    pub fn new() -> Self {
        Self {
            name: "vectors".to_string(),
        }
    }
}

impl Default for VectorSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DataSource for VectorSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn source_type(&self) -> DataSourceType {
        DataSourceType::Vector
    }

    fn supported_operations(&self) -> Vec<DataSourceOperation> {
        vec![
            DataSourceOperation::Scan,
            DataSourceOperation::Lookup,
            DataSourceOperation::Filter,
        ]
    }

    async fn scan(
        &self,
        _pattern: &str,
        _limit: usize,
    ) -> Result<Vec<DataSourceRow>, DataSourceError> {
        Ok(Vec::new())
    }

    async fn lookup(&self, _key: &str) -> Result<Option<DataSourceRow>, DataSourceError> {
        Ok(None)
    }

    fn schema(&self) -> DataSourceSchema {
        DataSourceSchema::new(vec![
            ColumnDef {
                name: "id".to_string(),
                column_type: ColumnType::String,
                nullable: false,
            },
            ColumnDef {
                name: "embedding".to_string(),
                column_type: ColumnType::Vector,
                nullable: false,
            },
            ColumnDef {
                name: "score".to_string(),
                column_type: ColumnType::Float,
                nullable: true,
            },
            ColumnDef {
                name: "metadata".to_string(),
                column_type: ColumnType::Json,
                nullable: true,
            },
        ])
    }
}

/// Stub adapter for time-series data.
pub struct TimeSeriesSource {
    name: String,
}

impl TimeSeriesSource {
    /// Creates a new time-series data source with the default name `"timeseries"`.
    pub fn new() -> Self {
        Self {
            name: "timeseries".to_string(),
        }
    }
}

impl Default for TimeSeriesSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DataSource for TimeSeriesSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn source_type(&self) -> DataSourceType {
        DataSourceType::TimeSeries
    }

    fn supported_operations(&self) -> Vec<DataSourceOperation> {
        vec![
            DataSourceOperation::Scan,
            DataSourceOperation::Lookup,
            DataSourceOperation::Filter,
            DataSourceOperation::Aggregate,
            DataSourceOperation::Sort,
        ]
    }

    async fn scan(
        &self,
        _pattern: &str,
        _limit: usize,
    ) -> Result<Vec<DataSourceRow>, DataSourceError> {
        Ok(Vec::new())
    }

    async fn lookup(&self, _key: &str) -> Result<Option<DataSourceRow>, DataSourceError> {
        Ok(None)
    }

    fn schema(&self) -> DataSourceSchema {
        DataSourceSchema::new(vec![
            ColumnDef {
                name: "metric".to_string(),
                column_type: ColumnType::String,
                nullable: false,
            },
            ColumnDef {
                name: "timestamp".to_string(),
                column_type: ColumnType::Timestamp,
                nullable: false,
            },
            ColumnDef {
                name: "value".to_string(),
                column_type: ColumnType::Float,
                nullable: false,
            },
            ColumnDef {
                name: "labels".to_string(),
                column_type: ColumnType::Json,
                nullable: true,
            },
        ])
    }
}

/// Stub adapter for JSON document collections.
pub struct DocumentSource {
    name: String,
}

impl DocumentSource {
    /// Creates a new document data source with the default name `"documents"`.
    pub fn new() -> Self {
        Self {
            name: "documents".to_string(),
        }
    }
}

impl Default for DocumentSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DataSource for DocumentSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn source_type(&self) -> DataSourceType {
        DataSourceType::Document
    }

    fn supported_operations(&self) -> Vec<DataSourceOperation> {
        vec![
            DataSourceOperation::Scan,
            DataSourceOperation::Lookup,
            DataSourceOperation::Filter,
            DataSourceOperation::Aggregate,
            DataSourceOperation::Sort,
        ]
    }

    async fn scan(
        &self,
        _pattern: &str,
        _limit: usize,
    ) -> Result<Vec<DataSourceRow>, DataSourceError> {
        Ok(Vec::new())
    }

    async fn lookup(&self, _key: &str) -> Result<Option<DataSourceRow>, DataSourceError> {
        Ok(None)
    }

    fn schema(&self) -> DataSourceSchema {
        DataSourceSchema::new(vec![
            ColumnDef {
                name: "id".to_string(),
                column_type: ColumnType::String,
                nullable: false,
            },
            ColumnDef {
                name: "body".to_string(),
                column_type: ColumnType::Json,
                nullable: false,
            },
            ColumnDef {
                name: "created_at".to_string(),
                column_type: ColumnType::Timestamp,
                nullable: true,
            },
            ColumnDef {
                name: "updated_at".to_string(),
                column_type: ColumnType::Timestamp,
                nullable: true,
            },
        ])
    }
}

/// Stub adapter for graph traversal results.
pub struct GraphSource {
    name: String,
}

impl GraphSource {
    /// Creates a new graph data source with the default name `"graph"`.
    pub fn new() -> Self {
        Self {
            name: "graph".to_string(),
        }
    }
}

impl Default for GraphSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DataSource for GraphSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn source_type(&self) -> DataSourceType {
        DataSourceType::Graph
    }

    fn supported_operations(&self) -> Vec<DataSourceOperation> {
        vec![
            DataSourceOperation::Scan,
            DataSourceOperation::Lookup,
            DataSourceOperation::Filter,
            DataSourceOperation::Join,
        ]
    }

    async fn scan(
        &self,
        _pattern: &str,
        _limit: usize,
    ) -> Result<Vec<DataSourceRow>, DataSourceError> {
        Ok(Vec::new())
    }

    async fn lookup(&self, _key: &str) -> Result<Option<DataSourceRow>, DataSourceError> {
        Ok(None)
    }

    fn schema(&self) -> DataSourceSchema {
        DataSourceSchema::new(vec![
            ColumnDef {
                name: "node_id".to_string(),
                column_type: ColumnType::String,
                nullable: false,
            },
            ColumnDef {
                name: "label".to_string(),
                column_type: ColumnType::String,
                nullable: false,
            },
            ColumnDef {
                name: "properties".to_string(),
                column_type: ColumnType::Json,
                nullable: true,
            },
            ColumnDef {
                name: "edges".to_string(),
                column_type: ColumnType::Json,
                nullable: true,
            },
        ])
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Registry CRUD -------------------------------------------------------

    #[test]
    fn test_register_and_get() {
        let registry = DataSourceRegistry::new(DataSourceConfig::default());
        let kv = Arc::new(KeyValueSource::new());
        registry.register("kv", kv).unwrap();

        let source = registry.get("kv");
        assert!(source.is_some());
        assert_eq!(source.unwrap().name(), "kv");
    }

    #[test]
    fn test_register_duplicate() {
        let registry = DataSourceRegistry::new(DataSourceConfig::default());
        let kv1 = Arc::new(KeyValueSource::new());
        let kv2 = Arc::new(KeyValueSource::new());
        registry.register("kv", kv1).unwrap();

        let err = registry.register("kv", kv2).unwrap_err();
        assert!(matches!(err, DataSourceError::AlreadyRegistered(_)));
    }

    #[test]
    fn test_registry_full() {
        let config = DataSourceConfig {
            max_sources: 1,
            ..Default::default()
        };
        let registry = DataSourceRegistry::new(config);
        registry
            .register("kv", Arc::new(KeyValueSource::new()))
            .unwrap();

        let err = registry
            .register("vec", Arc::new(VectorSource::new()))
            .unwrap_err();
        assert!(matches!(err, DataSourceError::RegistryFull { max: 1 }));
    }

    #[test]
    fn test_unregister() {
        let registry = DataSourceRegistry::new(DataSourceConfig::default());
        registry
            .register("kv", Arc::new(KeyValueSource::new()))
            .unwrap();

        let removed = registry.unregister("kv").unwrap();
        assert_eq!(removed.name(), "kv");
        assert!(registry.is_empty());
    }

    #[test]
    fn test_unregister_not_found() {
        let registry = DataSourceRegistry::new(DataSourceConfig::default());
        let result = registry.unregister("nope");
        assert!(matches!(result, Err(DataSourceError::NotFound(_))));
    }

    #[test]
    fn test_get_missing() {
        let registry = DataSourceRegistry::new(DataSourceConfig::default());
        assert!(registry.get("nope").is_none());
    }

    #[test]
    fn test_len_and_is_empty() {
        let registry = DataSourceRegistry::new(DataSourceConfig::default());
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);

        registry
            .register("kv", Arc::new(KeyValueSource::new()))
            .unwrap();
        assert!(!registry.is_empty());
        assert_eq!(registry.len(), 1);
    }

    // -- Listing & type filtering -------------------------------------------

    #[test]
    fn test_list() {
        let registry = DataSourceRegistry::new(DataSourceConfig::default());
        registry
            .register("kv", Arc::new(KeyValueSource::new()))
            .unwrap();
        registry
            .register("vec", Arc::new(VectorSource::new()))
            .unwrap();

        let list = registry.list();
        assert_eq!(list.len(), 2);

        let names: Vec<&str> = list.iter().map(|i| i.name.as_str()).collect();
        assert!(names.contains(&"kv"));
        assert!(names.contains(&"vec"));
    }

    #[test]
    fn test_get_by_type() {
        let registry = DataSourceRegistry::new(DataSourceConfig::default());
        registry
            .register("kv", Arc::new(KeyValueSource::new()))
            .unwrap();
        registry
            .register("vec", Arc::new(VectorSource::new()))
            .unwrap();
        registry
            .register("ts", Arc::new(TimeSeriesSource::new()))
            .unwrap();

        let kv_sources = registry.get_by_type(DataSourceType::KeyValue);
        assert_eq!(kv_sources.len(), 1);
        assert_eq!(kv_sources[0].name(), "kv");

        let vector_sources = registry.get_by_type(DataSourceType::Vector);
        assert_eq!(vector_sources.len(), 1);

        let graph_sources = registry.get_by_type(DataSourceType::Graph);
        assert!(graph_sources.is_empty());
    }

    // -- Schema introspection -----------------------------------------------

    #[test]
    fn test_kv_schema() {
        let source = KeyValueSource::new();
        let schema = source.schema();
        let names = schema.column_names();
        assert_eq!(names, vec!["key", "value", "ttl", "type"]);

        let key_col = schema.get_column("key").unwrap();
        assert_eq!(key_col.column_type, ColumnType::String);
        assert!(!key_col.nullable);
    }

    #[test]
    fn test_vector_schema() {
        let source = VectorSource::new();
        let schema = source.schema();
        assert_eq!(schema.columns.len(), 4);
        assert!(schema.get_column("embedding").is_some());
        assert_eq!(
            schema.get_column("embedding").unwrap().column_type,
            ColumnType::Vector,
        );
    }

    #[test]
    fn test_timeseries_schema() {
        let source = TimeSeriesSource::new();
        let schema = source.schema();
        let names = schema.column_names();
        assert_eq!(names, vec!["metric", "timestamp", "value", "labels"]);
    }

    #[test]
    fn test_document_schema() {
        let source = DocumentSource::new();
        let schema = source.schema();
        assert!(schema.get_column("body").is_some());
        assert_eq!(
            schema.get_column("body").unwrap().column_type,
            ColumnType::Json,
        );
    }

    #[test]
    fn test_graph_schema() {
        let source = GraphSource::new();
        let schema = source.schema();
        let names = schema.column_names();
        assert!(names.contains(&"node_id"));
        assert!(names.contains(&"edges"));
    }

    // -- Adapter construction & trait compliance ----------------------------

    #[test]
    fn test_adapter_source_types() {
        assert_eq!(
            KeyValueSource::new().source_type(),
            DataSourceType::KeyValue
        );
        assert_eq!(VectorSource::new().source_type(), DataSourceType::Vector);
        assert_eq!(
            TimeSeriesSource::new().source_type(),
            DataSourceType::TimeSeries,
        );
        assert_eq!(
            DocumentSource::new().source_type(),
            DataSourceType::Document,
        );
        assert_eq!(GraphSource::new().source_type(), DataSourceType::Graph);
    }

    #[test]
    fn test_adapter_supported_operations() {
        let kv = KeyValueSource::new();
        let ops = kv.supported_operations();
        assert!(ops.contains(&DataSourceOperation::Scan));
        assert!(ops.contains(&DataSourceOperation::Lookup));

        let graph = GraphSource::new();
        let ops = graph.supported_operations();
        assert!(ops.contains(&DataSourceOperation::Join));
    }

    #[test]
    fn test_adapter_names() {
        assert_eq!(KeyValueSource::new().name(), "kv");
        assert_eq!(VectorSource::new().name(), "vectors");
        assert_eq!(TimeSeriesSource::new().name(), "timeseries");
        assert_eq!(DocumentSource::new().name(), "documents");
        assert_eq!(GraphSource::new().name(), "graph");
    }

    #[tokio::test]
    async fn test_kv_scan_returns_empty() {
        let source = KeyValueSource::new();
        let rows = source.scan("*", 100).await.unwrap();
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn test_kv_lookup_returns_none() {
        let source = KeyValueSource::new();
        let row = source.lookup("nonexistent").await.unwrap();
        assert!(row.is_none());
    }

    #[tokio::test]
    async fn test_vector_scan_returns_empty() {
        let source = VectorSource::new();
        let rows = source.scan("*", 10).await.unwrap();
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn test_graph_lookup_returns_none() {
        let source = GraphSource::new();
        let row = source.lookup("node:1").await.unwrap();
        assert!(row.is_none());
    }

    // -- DataSourceRow ------------------------------------------------------

    #[test]
    fn test_datasource_row() {
        let row = DataSourceRow::new(
            "user:1".to_string(),
            vec![
                Value::String("user:1".to_string()),
                Value::String("Alice".to_string()),
            ],
        );
        assert_eq!(row.key, "user:1");
        assert_eq!(row.get(0), Some(&Value::String("user:1".to_string())));
        assert_eq!(row.get(1), Some(&Value::String("Alice".to_string())));
        assert!(row.get(2).is_none());
    }

    // -- Display impls ------------------------------------------------------

    #[test]
    fn test_display_impls() {
        assert_eq!(DataSourceType::KeyValue.to_string(), "KeyValue");
        assert_eq!(DataSourceType::TimeSeries.to_string(), "TimeSeries");
        assert_eq!(DataSourceOperation::Scan.to_string(), "Scan");
        assert_eq!(DataSourceOperation::Aggregate.to_string(), "Aggregate");
        assert_eq!(ColumnType::Timestamp.to_string(), "TIMESTAMP");
        assert_eq!(ColumnType::Vector.to_string(), "VECTOR");
    }

    // -- Config defaults ----------------------------------------------------

    #[test]
    fn test_config_defaults() {
        let config = DataSourceConfig::default();
        assert_eq!(config.max_sources, 64);
        assert_eq!(config.default_scan_limit, 1000);
    }

    // -- DataSourceInfo from list -------------------------------------------

    #[test]
    fn test_list_info_contains_schema_columns() {
        let registry = DataSourceRegistry::new(DataSourceConfig::default());
        registry
            .register("ts", Arc::new(TimeSeriesSource::new()))
            .unwrap();

        let list = registry.list();
        assert_eq!(list.len(), 1);

        let info = &list[0];
        assert_eq!(info.name, "ts");
        assert_eq!(info.source_type, DataSourceType::TimeSeries);
        assert!(info.schema_columns.contains(&"metric".to_string()));
        assert!(info.schema_columns.contains(&"timestamp".to_string()));
    }
}
