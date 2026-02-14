//! Visual Query Builder
//!
//! Provides a visual interface for building queries through:
//! - Drag-and-drop query construction
//! - Query templates and snippets
//! - Query history and favorites
//! - Query explanation and optimization hints
//! - Query execution plans
//! - Result visualization

use std::collections::HashMap;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::StudioError;

/// Query types supported by the builder
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryType {
    /// Key-value operations
    KeyValue,
    /// List operations
    List,
    /// Set operations
    Set,
    /// Hash operations
    Hash,
    /// Sorted set operations
    SortedSet,
    /// Stream operations
    Stream,
    /// Pub/Sub operations
    PubSub,
    /// Script operations
    Script,
    /// Transaction
    Transaction,
    /// Aggregation pipeline
    Aggregation,
    /// Vector search
    VectorSearch,
    /// Full-text search
    FullTextSearch,
    /// Graph traversal
    GraphTraversal,
    /// Time series
    TimeSeries,
}

/// Query block in the visual builder
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryBlock {
    /// Block identifier
    pub id: String,
    /// Block type
    pub block_type: BlockType,
    /// Block position (for UI)
    pub position: Position,
    /// Block parameters
    pub params: HashMap<String, serde_json::Value>,
    /// Connected inputs
    pub inputs: Vec<Connection>,
    /// Connected outputs
    pub outputs: Vec<Connection>,
    /// Validation state
    pub valid: bool,
    /// Validation errors
    pub errors: Vec<String>,
}

/// Block types for visual query building
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockType {
    // Data Sources
    /// Key pattern source
    KeyPattern,
    /// Specific keys
    KeyList,
    /// Query result source (for chaining)
    QueryResult,
    /// Stream source
    StreamSource,
    /// Database scan
    Scan,

    // Operations
    /// GET operation
    Get,
    /// SET operation
    Set,
    /// DELETE operation
    Delete,
    /// MGET operation
    MultiGet,
    /// MSET operation
    MultiSet,

    // List operations
    /// LPUSH/RPUSH
    ListPush,
    /// LPOP/RPOP
    ListPop,
    /// LRANGE
    ListRange,

    // Hash operations
    /// HGET/HSET/HMGET/HMSET
    HashOp,

    // Set operations
    /// SADD/SREM/SMEMBERS
    SetOp,
    /// Set operations (UNION/INTER/DIFF)
    SetCombine,

    // Sorted set operations
    /// ZADD/ZREM
    ZSetOp,
    /// ZRANGE/ZRANGEBYSCORE
    ZSetRange,

    // Transformations
    /// Filter results
    Filter,
    /// Map/transform results
    Map,
    /// Sort results
    Sort,
    /// Limit results
    Limit,
    /// Group results
    GroupBy,

    // Aggregations
    /// Count
    Count,
    /// Sum
    Sum,
    /// Average
    Average,
    /// Min/Max
    MinMax,

    // Joins
    /// Join two result sets
    Join,
    /// Left join
    LeftJoin,

    // Advanced
    /// Lua script
    Script,
    /// Transaction wrapper
    Transaction,
    /// Pipeline wrapper
    Pipeline,

    // Vector search
    /// KNN search
    VectorKnn,
    /// Vector similarity
    VectorSimilarity,

    // Output
    /// Return results
    Return,
    /// Store to key
    Store,
    /// Publish to channel
    Publish,
}

/// Position for UI layout
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Position {
    pub x: f64,
    pub y: f64,
}

/// Connection between blocks
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Connection {
    /// Source block ID
    pub from_block: String,
    /// Source port name
    pub from_port: String,
    /// Target block ID
    pub to_block: String,
    /// Target port name
    pub to_port: String,
}

/// Visual query definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VisualQuery {
    /// Query identifier
    pub id: String,
    /// Query name
    pub name: String,
    /// Query description
    pub description: Option<String>,
    /// Query blocks
    pub blocks: Vec<QueryBlock>,
    /// Connections between blocks
    pub connections: Vec<Connection>,
    /// Query parameters (for parameterized queries)
    pub parameters: HashMap<String, QueryParameter>,
    /// Query metadata
    pub metadata: QueryMetadata,
}

/// Query parameter definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryParameter {
    /// Parameter name
    pub name: String,
    /// Parameter type
    pub param_type: ParameterType,
    /// Default value
    pub default: Option<serde_json::Value>,
    /// Required
    pub required: bool,
    /// Description
    pub description: Option<String>,
    /// Validation regex (for strings)
    pub validation: Option<String>,
}

/// Parameter types
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ParameterType {
    String,
    Integer,
    Float,
    Boolean,
    Array,
    Object,
    Key,
    Pattern,
}

/// Query metadata
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct QueryMetadata {
    /// Created timestamp
    pub created_at: u64,
    /// Last modified timestamp
    pub modified_at: u64,
    /// Created by user
    pub created_by: Option<String>,
    /// Tags
    pub tags: Vec<String>,
    /// Is template
    pub is_template: bool,
    /// Template category
    pub template_category: Option<String>,
    /// Execution count
    pub execution_count: u64,
    /// Average execution time (ms)
    pub avg_execution_time_ms: f64,
}

/// Compiled query ready for execution
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompiledQuery {
    /// Original visual query ID
    pub source_id: String,
    /// Compiled command(s)
    pub commands: Vec<CompiledCommand>,
    /// Execution plan
    pub plan: ExecutionPlan,
    /// Estimated cost
    pub estimated_cost: QueryCost,
    /// Optimization hints applied
    pub optimizations: Vec<String>,
    /// Warnings
    pub warnings: Vec<String>,
}

/// Compiled command
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompiledCommand {
    /// Command name
    pub command: String,
    /// Command arguments
    pub args: Vec<String>,
    /// Parameter placeholders
    pub placeholders: Vec<String>,
}

/// Query execution plan
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionPlan {
    /// Plan steps
    pub steps: Vec<PlanStep>,
    /// Is pipeline possible
    pub can_pipeline: bool,
    /// Requires transaction
    pub requires_transaction: bool,
    /// Estimated rows processed
    pub estimated_rows: Option<u64>,
}

/// Single step in execution plan
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlanStep {
    /// Step index
    pub index: usize,
    /// Operation type
    pub operation: String,
    /// Operation details
    pub details: String,
    /// Estimated cost
    pub cost: f64,
    /// Rows estimate
    pub rows: Option<u64>,
    /// Dependencies (step indices)
    pub depends_on: Vec<usize>,
}

/// Query cost estimate
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct QueryCost {
    /// Estimated CPU cost
    pub cpu_cost: f64,
    /// Estimated memory cost (bytes)
    pub memory_cost: u64,
    /// Estimated network cost (if distributed)
    pub network_cost: f64,
    /// Estimated disk I/O
    pub disk_io: f64,
    /// Total cost score
    pub total_cost: f64,
    /// Cost level (low, medium, high)
    pub level: CostLevel,
}

/// Cost level classification
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum CostLevel {
    #[default]
    Low,
    Medium,
    High,
    Critical,
}

/// Query execution result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryResult {
    /// Query ID
    pub query_id: String,
    /// Execution success
    pub success: bool,
    /// Result data
    pub data: Option<serde_json::Value>,
    /// Error message
    pub error: Option<String>,
    /// Execution time (ms)
    pub execution_time_ms: f64,
    /// Rows affected/returned
    pub rows: usize,
    /// Result metadata
    pub metadata: ResultMetadata,
}

/// Result metadata
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ResultMetadata {
    /// Commands executed
    pub commands_executed: usize,
    /// Keys scanned
    pub keys_scanned: Option<u64>,
    /// Memory used (bytes)
    pub memory_used: Option<u64>,
    /// Cache hits
    pub cache_hits: u64,
    /// Cache misses
    pub cache_misses: u64,
}

/// Query history entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryHistoryEntry {
    /// History entry ID
    pub id: String,
    /// Query (visual or raw)
    pub query: QueryHistoryItem,
    /// Execution timestamp
    pub executed_at: u64,
    /// Execution time (ms)
    pub execution_time_ms: f64,
    /// Success
    pub success: bool,
    /// Error message
    pub error: Option<String>,
    /// Rows returned/affected
    pub rows: usize,
    /// User who executed
    pub user: Option<String>,
}

/// Query in history
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum QueryHistoryItem {
    /// Raw command
    Raw { command: String },
    /// Visual query
    Visual { query_id: String, name: String },
}

/// Query template
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryTemplate {
    /// Template ID
    pub id: String,
    /// Template name
    pub name: String,
    /// Template description
    pub description: String,
    /// Category
    pub category: String,
    /// Visual query definition
    pub query: VisualQuery,
    /// Usage examples
    pub examples: Vec<TemplateExample>,
    /// Tags
    pub tags: Vec<String>,
    /// Is built-in
    pub builtin: bool,
}

/// Template usage example
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TemplateExample {
    /// Example name
    pub name: String,
    /// Example parameters
    pub params: HashMap<String, serde_json::Value>,
    /// Expected result description
    pub expected_result: String,
}

/// Visual Query Builder
pub struct QueryBuilder {
    /// Query history
    history: RwLock<Vec<QueryHistoryEntry>>,
    /// Saved queries
    saved_queries: RwLock<HashMap<String, VisualQuery>>,
    /// Query templates
    templates: RwLock<Vec<QueryTemplate>>,
    /// Favorites
    favorites: RwLock<Vec<String>>,
    /// Max history size
    max_history_size: usize,
    /// Next ID counter
    next_id: RwLock<u64>,
}

impl QueryBuilder {
    /// Create a new query builder
    pub fn new() -> Self {
        let builder = Self {
            history: RwLock::new(Vec::new()),
            saved_queries: RwLock::new(HashMap::new()),
            templates: RwLock::new(Vec::new()),
            favorites: RwLock::new(Vec::new()),
            max_history_size: 1000,
            next_id: RwLock::new(1),
        };

        // Initialize with built-in templates
        builder.load_builtin_templates();

        builder
    }

    fn generate_id(&self) -> String {
        let mut id = self.next_id.write();
        let new_id = format!("qb_{}", *id);
        *id += 1;
        new_id
    }

    /// Load built-in query templates
    fn load_builtin_templates(&self) {
        let templates = vec![
            self.create_key_pattern_template(),
            self.create_bulk_delete_template(),
            self.create_aggregation_template(),
            self.create_vector_search_template(),
            self.create_time_series_template(),
        ];

        *self.templates.write() = templates;
    }

    fn create_key_pattern_template(&self) -> QueryTemplate {
        QueryTemplate {
            id: "builtin_key_pattern".to_string(),
            name: "Key Pattern Search".to_string(),
            description: "Find keys matching a pattern and retrieve their values".to_string(),
            category: "Basic".to_string(),
            query: VisualQuery {
                id: "builtin_key_pattern_query".to_string(),
                name: "Key Pattern Search".to_string(),
                description: Some("Search for keys by pattern".to_string()),
                blocks: vec![
                    QueryBlock {
                        id: "b1".to_string(),
                        block_type: BlockType::KeyPattern,
                        position: Position { x: 100.0, y: 100.0 },
                        params: [("pattern".to_string(), serde_json::json!("*"))]
                            .into_iter()
                            .collect(),
                        inputs: vec![],
                        outputs: vec![],
                        valid: true,
                        errors: vec![],
                    },
                    QueryBlock {
                        id: "b2".to_string(),
                        block_type: BlockType::MultiGet,
                        position: Position { x: 300.0, y: 100.0 },
                        params: HashMap::new(),
                        inputs: vec![],
                        outputs: vec![],
                        valid: true,
                        errors: vec![],
                    },
                    QueryBlock {
                        id: "b3".to_string(),
                        block_type: BlockType::Return,
                        position: Position { x: 500.0, y: 100.0 },
                        params: HashMap::new(),
                        inputs: vec![],
                        outputs: vec![],
                        valid: true,
                        errors: vec![],
                    },
                ],
                connections: vec![
                    Connection {
                        from_block: "b1".to_string(),
                        from_port: "keys".to_string(),
                        to_block: "b2".to_string(),
                        to_port: "keys".to_string(),
                    },
                    Connection {
                        from_block: "b2".to_string(),
                        from_port: "values".to_string(),
                        to_block: "b3".to_string(),
                        to_port: "data".to_string(),
                    },
                ],
                parameters: [(
                    "pattern".to_string(),
                    QueryParameter {
                        name: "pattern".to_string(),
                        param_type: ParameterType::Pattern,
                        default: Some(serde_json::json!("*")),
                        required: true,
                        description: Some("Key pattern to search".to_string()),
                        validation: None,
                    },
                )]
                .into_iter()
                .collect(),
                metadata: QueryMetadata {
                    is_template: true,
                    template_category: Some("Basic".to_string()),
                    ..Default::default()
                },
            },
            examples: vec![TemplateExample {
                name: "Find user keys".to_string(),
                params: [("pattern".to_string(), serde_json::json!("user:*"))]
                    .into_iter()
                    .collect(),
                expected_result: "Returns all keys matching user:* with their values".to_string(),
            }],
            tags: vec![
                "keys".to_string(),
                "search".to_string(),
                "basic".to_string(),
            ],
            builtin: true,
        }
    }

    fn create_bulk_delete_template(&self) -> QueryTemplate {
        QueryTemplate {
            id: "builtin_bulk_delete".to_string(),
            name: "Bulk Delete".to_string(),
            description: "Delete multiple keys matching a pattern".to_string(),
            category: "Maintenance".to_string(),
            query: VisualQuery {
                id: "builtin_bulk_delete_query".to_string(),
                name: "Bulk Delete".to_string(),
                description: Some("Delete keys by pattern".to_string()),
                blocks: vec![
                    QueryBlock {
                        id: "b1".to_string(),
                        block_type: BlockType::Scan,
                        position: Position { x: 100.0, y: 100.0 },
                        params: [("pattern".to_string(), serde_json::json!("temp:*"))]
                            .into_iter()
                            .collect(),
                        inputs: vec![],
                        outputs: vec![],
                        valid: true,
                        errors: vec![],
                    },
                    QueryBlock {
                        id: "b2".to_string(),
                        block_type: BlockType::Delete,
                        position: Position { x: 300.0, y: 100.0 },
                        params: HashMap::new(),
                        inputs: vec![],
                        outputs: vec![],
                        valid: true,
                        errors: vec![],
                    },
                ],
                connections: vec![Connection {
                    from_block: "b1".to_string(),
                    from_port: "keys".to_string(),
                    to_block: "b2".to_string(),
                    to_port: "keys".to_string(),
                }],
                parameters: [(
                    "pattern".to_string(),
                    QueryParameter {
                        name: "pattern".to_string(),
                        param_type: ParameterType::Pattern,
                        default: Some(serde_json::json!("temp:*")),
                        required: true,
                        description: Some("Key pattern to delete".to_string()),
                        validation: None,
                    },
                )]
                .into_iter()
                .collect(),
                metadata: QueryMetadata {
                    is_template: true,
                    template_category: Some("Maintenance".to_string()),
                    ..Default::default()
                },
            },
            examples: vec![],
            tags: vec!["delete".to_string(), "maintenance".to_string()],
            builtin: true,
        }
    }

    fn create_aggregation_template(&self) -> QueryTemplate {
        QueryTemplate {
            id: "builtin_aggregation".to_string(),
            name: "Aggregation Pipeline".to_string(),
            description: "Aggregate data from multiple keys".to_string(),
            category: "Analytics".to_string(),
            query: VisualQuery {
                id: "builtin_aggregation_query".to_string(),
                name: "Aggregation".to_string(),
                description: Some("Sum values across keys".to_string()),
                blocks: vec![
                    QueryBlock {
                        id: "b1".to_string(),
                        block_type: BlockType::KeyPattern,
                        position: Position { x: 100.0, y: 100.0 },
                        params: [("pattern".to_string(), serde_json::json!("counter:*"))]
                            .into_iter()
                            .collect(),
                        inputs: vec![],
                        outputs: vec![],
                        valid: true,
                        errors: vec![],
                    },
                    QueryBlock {
                        id: "b2".to_string(),
                        block_type: BlockType::MultiGet,
                        position: Position { x: 300.0, y: 100.0 },
                        params: HashMap::new(),
                        inputs: vec![],
                        outputs: vec![],
                        valid: true,
                        errors: vec![],
                    },
                    QueryBlock {
                        id: "b3".to_string(),
                        block_type: BlockType::Sum,
                        position: Position { x: 500.0, y: 100.0 },
                        params: HashMap::new(),
                        inputs: vec![],
                        outputs: vec![],
                        valid: true,
                        errors: vec![],
                    },
                    QueryBlock {
                        id: "b4".to_string(),
                        block_type: BlockType::Return,
                        position: Position { x: 700.0, y: 100.0 },
                        params: HashMap::new(),
                        inputs: vec![],
                        outputs: vec![],
                        valid: true,
                        errors: vec![],
                    },
                ],
                connections: vec![],
                parameters: HashMap::new(),
                metadata: QueryMetadata {
                    is_template: true,
                    template_category: Some("Analytics".to_string()),
                    ..Default::default()
                },
            },
            examples: vec![],
            tags: vec!["aggregation".to_string(), "analytics".to_string()],
            builtin: true,
        }
    }

    fn create_vector_search_template(&self) -> QueryTemplate {
        QueryTemplate {
            id: "builtin_vector_search".to_string(),
            name: "Vector Similarity Search".to_string(),
            description: "Find similar items using vector embeddings".to_string(),
            category: "AI/ML".to_string(),
            query: VisualQuery {
                id: "builtin_vector_search_query".to_string(),
                name: "Vector Search".to_string(),
                description: Some("K-NN vector similarity search".to_string()),
                blocks: vec![
                    QueryBlock {
                        id: "b1".to_string(),
                        block_type: BlockType::VectorKnn,
                        position: Position { x: 100.0, y: 100.0 },
                        params: [
                            ("index".to_string(), serde_json::json!("embeddings")),
                            ("k".to_string(), serde_json::json!(10)),
                        ]
                        .into_iter()
                        .collect(),
                        inputs: vec![],
                        outputs: vec![],
                        valid: true,
                        errors: vec![],
                    },
                    QueryBlock {
                        id: "b2".to_string(),
                        block_type: BlockType::Return,
                        position: Position { x: 300.0, y: 100.0 },
                        params: HashMap::new(),
                        inputs: vec![],
                        outputs: vec![],
                        valid: true,
                        errors: vec![],
                    },
                ],
                connections: vec![],
                parameters: [(
                    "vector".to_string(),
                    QueryParameter {
                        name: "vector".to_string(),
                        param_type: ParameterType::Array,
                        default: None,
                        required: true,
                        description: Some("Query vector".to_string()),
                        validation: None,
                    },
                )]
                .into_iter()
                .collect(),
                metadata: QueryMetadata {
                    is_template: true,
                    template_category: Some("AI/ML".to_string()),
                    ..Default::default()
                },
            },
            examples: vec![],
            tags: vec!["vector".to_string(), "search".to_string(), "ai".to_string()],
            builtin: true,
        }
    }

    fn create_time_series_template(&self) -> QueryTemplate {
        QueryTemplate {
            id: "builtin_time_series".to_string(),
            name: "Time Series Query".to_string(),
            description: "Query time series data with aggregation".to_string(),
            category: "Time Series".to_string(),
            query: VisualQuery {
                id: "builtin_time_series_query".to_string(),
                name: "Time Series".to_string(),
                description: Some("Query and aggregate time series".to_string()),
                blocks: vec![],
                connections: vec![],
                parameters: HashMap::new(),
                metadata: QueryMetadata {
                    is_template: true,
                    template_category: Some("Time Series".to_string()),
                    ..Default::default()
                },
            },
            examples: vec![],
            tags: vec!["timeseries".to_string(), "analytics".to_string()],
            builtin: true,
        }
    }

    // ========== Query Operations ==========

    /// Create a new visual query
    pub fn create_query(&self, name: &str) -> VisualQuery {
        let id = self.generate_id();

        VisualQuery {
            id,
            name: name.to_string(),
            description: None,
            blocks: vec![],
            connections: vec![],
            parameters: HashMap::new(),
            metadata: QueryMetadata {
                created_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                modified_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                ..Default::default()
            },
        }
    }

    /// Save a visual query
    pub fn save_query(&self, query: VisualQuery) -> Result<(), StudioError> {
        let id = query.id.clone();
        self.saved_queries.write().insert(id.clone(), query);
        Ok(())
    }

    /// Get a saved query
    pub fn get_query(&self, id: &str) -> Option<VisualQuery> {
        self.saved_queries.read().get(id).cloned()
    }

    /// Delete a saved query
    pub fn delete_query(&self, id: &str) -> Result<(), StudioError> {
        self.saved_queries.write().remove(id);
        self.favorites.write().retain(|f| f != id);
        Ok(())
    }

    /// List all saved queries
    pub fn list_queries(&self) -> Vec<QuerySummary> {
        self.saved_queries
            .read()
            .values()
            .map(|q| QuerySummary {
                id: q.id.clone(),
                name: q.name.clone(),
                description: q.description.clone(),
                block_count: q.blocks.len(),
                is_favorite: self.favorites.read().contains(&q.id),
                created_at: q.metadata.created_at,
                modified_at: q.metadata.modified_at,
                execution_count: q.metadata.execution_count,
                tags: q.metadata.tags.clone(),
            })
            .collect()
    }

    /// Compile a visual query to executable commands
    pub fn compile(&self, query: &VisualQuery) -> Result<CompiledQuery, StudioError> {
        // Validate query first
        let validation = self.validate(query)?;
        if !validation.valid {
            return Err(StudioError::BadRequest(format!(
                "Query validation failed: {:?}",
                validation.errors
            )));
        }

        // Build execution plan
        let plan = self.build_execution_plan(query);

        // Compile to commands
        let commands = self.compile_blocks(query)?;

        // Estimate cost
        let estimated_cost = self.estimate_cost(query);

        Ok(CompiledQuery {
            source_id: query.id.clone(),
            commands,
            plan,
            estimated_cost,
            optimizations: vec![],
            warnings: vec![],
        })
    }

    fn compile_blocks(&self, query: &VisualQuery) -> Result<Vec<CompiledCommand>, StudioError> {
        let mut commands = Vec::new();

        for block in &query.blocks {
            if let Some(cmd) = self.compile_block(block)? {
                commands.push(cmd);
            }
        }

        Ok(commands)
    }

    fn compile_block(&self, block: &QueryBlock) -> Result<Option<CompiledCommand>, StudioError> {
        let cmd = match &block.block_type {
            BlockType::Get => {
                let key = block
                    .params
                    .get("key")
                    .and_then(|v| v.as_str())
                    .unwrap_or("*");
                Some(CompiledCommand {
                    command: "GET".to_string(),
                    args: vec![key.to_string()],
                    placeholders: vec![],
                })
            }
            BlockType::Set => {
                let key = block
                    .params
                    .get("key")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let value = block
                    .params
                    .get("value")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                Some(CompiledCommand {
                    command: "SET".to_string(),
                    args: vec![key.to_string(), value.to_string()],
                    placeholders: vec![],
                })
            }
            BlockType::Delete => {
                let key = block
                    .params
                    .get("key")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                Some(CompiledCommand {
                    command: "DEL".to_string(),
                    args: vec![key.to_string()],
                    placeholders: vec![],
                })
            }
            BlockType::KeyPattern | BlockType::Scan => {
                let pattern = block
                    .params
                    .get("pattern")
                    .and_then(|v| v.as_str())
                    .unwrap_or("*");
                Some(CompiledCommand {
                    command: "SCAN".to_string(),
                    args: vec!["0".to_string(), "MATCH".to_string(), pattern.to_string()],
                    placeholders: vec![],
                })
            }
            BlockType::MultiGet => Some(CompiledCommand {
                command: "MGET".to_string(),
                args: vec![],
                placeholders: vec!["keys".to_string()],
            }),
            _ => None,
        };

        Ok(cmd)
    }

    fn build_execution_plan(&self, query: &VisualQuery) -> ExecutionPlan {
        let steps: Vec<PlanStep> = query
            .blocks
            .iter()
            .enumerate()
            .map(|(i, block)| PlanStep {
                index: i,
                operation: format!("{:?}", block.block_type),
                details: format!("Execute {:?}", block.block_type),
                cost: 1.0,
                rows: Some(100),
                depends_on: vec![],
            })
            .collect();

        ExecutionPlan {
            steps,
            can_pipeline: true,
            requires_transaction: false,
            estimated_rows: Some(100),
        }
    }

    fn estimate_cost(&self, query: &VisualQuery) -> QueryCost {
        let block_count = query.blocks.len() as f64;

        QueryCost {
            cpu_cost: block_count * 0.1,
            memory_cost: (block_count * 1024.0) as u64,
            network_cost: 0.0,
            disk_io: block_count * 0.5,
            total_cost: block_count,
            level: if block_count < 5.0 {
                CostLevel::Low
            } else if block_count < 10.0 {
                CostLevel::Medium
            } else {
                CostLevel::High
            },
        }
    }

    /// Validate a visual query
    pub fn validate(&self, query: &VisualQuery) -> Result<ValidationResult, StudioError> {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        // Check for empty query
        if query.blocks.is_empty() {
            errors.push("Query has no blocks".to_string());
        }

        // Check for orphaned blocks (no connections)
        for block in &query.blocks {
            let has_input = query.connections.iter().any(|c| c.to_block == block.id);
            let has_output = query.connections.iter().any(|c| c.from_block == block.id);

            // Source blocks don't need inputs
            let is_source = matches!(
                block.block_type,
                BlockType::KeyPattern
                    | BlockType::KeyList
                    | BlockType::Scan
                    | BlockType::StreamSource
            );

            // Output blocks don't need outputs
            let is_sink = matches!(
                block.block_type,
                BlockType::Return | BlockType::Store | BlockType::Publish
            );

            if !is_source && !has_input && query.blocks.len() > 1 {
                warnings.push(format!("Block {} has no input connections", block.id));
            }

            if !is_sink && !has_output && query.blocks.len() > 1 {
                warnings.push(format!("Block {} has no output connections", block.id));
            }
        }

        // Check required parameters
        for (name, param) in &query.parameters {
            if param.required && param.default.is_none() {
                warnings.push(format!(
                    "Required parameter '{}' has no default value",
                    name
                ));
            }
        }

        Ok(ValidationResult {
            valid: errors.is_empty(),
            errors,
            warnings,
        })
    }

    // ========== Template Operations ==========

    /// Get all templates
    pub fn get_templates(&self) -> Vec<QueryTemplate> {
        self.templates.read().clone()
    }

    /// Get templates by category
    pub fn get_templates_by_category(&self, category: &str) -> Vec<QueryTemplate> {
        self.templates
            .read()
            .iter()
            .filter(|t| t.category == category)
            .cloned()
            .collect()
    }

    /// Create query from template
    pub fn from_template(&self, template_id: &str) -> Result<VisualQuery, StudioError> {
        let templates = self.templates.read();
        let template = templates
            .iter()
            .find(|t| t.id == template_id)
            .ok_or_else(|| StudioError::NotFound(format!("Template {} not found", template_id)))?;

        let mut query = template.query.clone();
        query.id = self.generate_id();
        query.metadata.is_template = false;
        query.metadata.created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Ok(query)
    }

    // ========== History Operations ==========

    /// Add to history
    pub fn add_to_history(&self, entry: QueryHistoryEntry) {
        let mut history = self.history.write();
        history.insert(0, entry);

        // Trim history
        if history.len() > self.max_history_size {
            history.truncate(self.max_history_size);
        }
    }

    /// Get history
    pub fn get_history(&self, limit: usize) -> Vec<QueryHistoryEntry> {
        self.history.read().iter().take(limit).cloned().collect()
    }

    /// Clear history
    pub fn clear_history(&self) {
        self.history.write().clear();
    }

    // ========== Favorites ==========

    /// Add to favorites
    pub fn add_favorite(&self, query_id: &str) -> Result<(), StudioError> {
        if !self.saved_queries.read().contains_key(query_id) {
            return Err(StudioError::NotFound(query_id.to_string()));
        }

        let mut favorites = self.favorites.write();
        if !favorites.contains(&query_id.to_string()) {
            favorites.push(query_id.to_string());
        }

        Ok(())
    }

    /// Remove from favorites
    pub fn remove_favorite(&self, query_id: &str) {
        self.favorites.write().retain(|f| f != query_id);
    }

    /// Get favorites
    pub fn get_favorites(&self) -> Vec<VisualQuery> {
        let favorites = self.favorites.read();
        let queries = self.saved_queries.read();

        favorites
            .iter()
            .filter_map(|id| queries.get(id).cloned())
            .collect()
    }
}

impl Default for QueryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Query summary for listing
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuerySummary {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub block_count: usize,
    pub is_favorite: bool,
    pub created_at: u64,
    pub modified_at: u64,
    pub execution_count: u64,
    pub tags: Vec<String>,
}

/// Validation result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidationResult {
    pub valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_query() {
        let builder = QueryBuilder::new();
        let query = builder.create_query("Test Query");

        assert_eq!(query.name, "Test Query");
        assert!(query.blocks.is_empty());
    }

    #[test]
    fn test_save_and_get_query() {
        let builder = QueryBuilder::new();
        let query = builder.create_query("Test");

        builder.save_query(query.clone()).unwrap();

        let loaded = builder.get_query(&query.id);
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().name, "Test");
    }

    #[test]
    fn test_builtin_templates() {
        let builder = QueryBuilder::new();
        let templates = builder.get_templates();

        assert!(!templates.is_empty());
        assert!(templates.iter().any(|t| t.id == "builtin_key_pattern"));
    }

    #[test]
    fn test_from_template() {
        let builder = QueryBuilder::new();
        let query = builder.from_template("builtin_key_pattern").unwrap();

        assert!(!query.id.is_empty());
        assert_eq!(query.name, "Key Pattern Search");
        assert!(!query.metadata.is_template);
    }

    #[test]
    fn test_validation_empty_query() {
        let builder = QueryBuilder::new();
        let query = builder.create_query("Empty");

        let result = builder.validate(&query).unwrap();
        assert!(!result.valid);
        assert!(result.errors.contains(&"Query has no blocks".to_string()));
    }

    #[test]
    fn test_compile_simple_query() {
        let builder = QueryBuilder::new();
        let mut query = builder.create_query("Simple");

        query.blocks.push(QueryBlock {
            id: "b1".to_string(),
            block_type: BlockType::Get,
            position: Position::default(),
            params: [("key".to_string(), serde_json::json!("test:key"))]
                .into_iter()
                .collect(),
            inputs: vec![],
            outputs: vec![],
            valid: true,
            errors: vec![],
        });

        let compiled = builder.compile(&query).unwrap();
        assert!(!compiled.commands.is_empty());
        assert_eq!(compiled.commands[0].command, "GET");
    }

    #[test]
    fn test_history() {
        let builder = QueryBuilder::new();

        let entry = QueryHistoryEntry {
            id: "h1".to_string(),
            query: QueryHistoryItem::Raw {
                command: "GET test".to_string(),
            },
            executed_at: 0,
            execution_time_ms: 1.0,
            success: true,
            error: None,
            rows: 1,
            user: None,
        };

        builder.add_to_history(entry);

        let history = builder.get_history(10);
        assert_eq!(history.len(), 1);
    }

    #[test]
    fn test_favorites() {
        let builder = QueryBuilder::new();
        let query = builder.create_query("Test");
        let id = query.id.clone();

        builder.save_query(query).unwrap();
        builder.add_favorite(&id).unwrap();

        let favorites = builder.get_favorites();
        assert_eq!(favorites.len(), 1);

        builder.remove_favorite(&id);
        let favorites = builder.get_favorites();
        assert!(favorites.is_empty());
    }
}
