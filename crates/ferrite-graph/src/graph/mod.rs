//! # Graph Database Engine
//!
//! Native graph database capabilities for connected data, social networks,
//! knowledge graphs, and recommendation systems.
//!
//! ## Why Graph?
//!
//! Traditional Redis stores data in flat structures. For connected data:
//!
//! - **Relationships are first-class**: Edges have types, properties, direction
//! - **Efficient traversals**: Follow connections without expensive JOINs
//! - **Pattern matching**: Find complex structures in your data
//! - **Graph algorithms**: PageRank, shortest path, community detection
//!
//! ## Use Cases
//!
//! | Use Case | Description |
//! |----------|-------------|
//! | Social Networks | Friends, followers, recommendations |
//! | Knowledge Graphs | Entities, relationships, reasoning |
//! | Fraud Detection | Transaction patterns, account links |
//! | Recommendations | Similar items, collaborative filtering |
//! | Network Topology | Infrastructure, dependencies |
//! | Access Control | Permission hierarchies, role graphs |
//!
//! ## Quick Start
//!
//! ### Creating a Graph
//!
//! ```no_run
//! use ferrite::graph::{Graph, PropertyValue};
//!
//! let graph = Graph::new();
//!
//! // Add vertices with properties
//! let alice = graph.add_vertex("Person")
//!     .property("name", "Alice")
//!     .property("age", 30i64)
//!     .property("email", "alice@example.com")
//!     .build()?;
//!
//! let bob = graph.add_vertex("Person")
//!     .property("name", "Bob")
//!     .property("age", 25i64)
//!     .build()?;
//!
//! let acme = graph.add_vertex("Company")
//!     .property("name", "Acme Corp")
//!     .property("industry", "Technology")
//!     .build()?;
//!
//! // Create relationships
//! graph.add_edge(alice, bob, "KNOWS")
//!     .property("since", 2020i64)
//!     .property("relationship", "friends")
//!     .build()?;
//!
//! graph.add_edge(alice, acme, "WORKS_AT")
//!     .property("role", "Engineer")
//!     .property("start_year", 2019i64)
//!     .build()?;
//!
//! graph.add_edge(bob, acme, "WORKS_AT")
//!     .property("role", "Designer")
//!     .build()?;
//!
//! println!("Graph has {} vertices and {} edges",
//!     graph.vertex_count(),
//!     graph.edge_count()
//! );
//! # Ok::<(), ferrite::graph::GraphError>(())
//! ```
//!
//! ### Graph Traversals
//!
//! ```no_run
//! use ferrite::graph::Graph;
//!
//! let graph = Graph::new();
//! // ... add vertices and edges ...
//!
//! # let alice = graph.add_vertex("Person").property("name", "Alice").build().expect("failed to add vertex");
//! // Find Alice's friends (outgoing KNOWS edges)
//! let friends = graph.traverse(alice)
//!     .out("KNOWS")
//!     .collect::<Vec<_>>();
//!
//! println!("Alice knows {} people", friends.len());
//!
//! // Find friends of friends
//! let friends_of_friends = graph.traverse(alice)
//!     .out("KNOWS")     // First hop: Alice's friends
//!     .out("KNOWS")     // Second hop: Their friends
//!     .collect::<Vec<_>>();
//!
//! // Find who works at the same company
//! let coworkers = graph.traverse(alice)
//!     .out("WORKS_AT")  // Companies Alice works at
//!     .in_("WORKS_AT")  // People who work there
//!     .collect::<Vec<_>>();
//! ```
//!
//! ### Pattern Matching
//!
//! ```no_run
//! use ferrite::graph::{Graph, Pattern, PatternMatcher};
//!
//! let graph = Graph::new();
//! // ... add vertices and edges ...
//!
//! // Find triangles: (a)-[KNOWS]->(b)-[KNOWS]->(c)-[KNOWS]->(a)
//! let pattern = Pattern::new()
//!     .vertex("a", "Person")
//!     .vertex("b", "Person")
//!     .vertex("c", "Person")
//!     .edge("a", "KNOWS", "b")
//!     .edge("b", "KNOWS", "c")
//!     .edge("c", "KNOWS", "a");
//!
//! let matches = graph.match_pattern(&pattern);
//! println!("Found {} triangles", matches.len());
//!
//! for m in &matches {
//!     let a = &m.vertices["a"];
//!     let b = &m.vertices["b"];
//!     let c = &m.vertices["c"];
//!     println!("{} - {} - {}",
//!         a.properties.get("name").expect("name property missing"),
//!         b.properties.get("name").expect("name property missing"),
//!         c.properties.get("name").expect("name property missing")
//!     );
//! }
//! ```
//!
//! ### Graph Algorithms
//!
//! ```no_run
//! use ferrite::graph::Graph;
//!
//! let graph = Graph::new();
//! // ... add vertices and edges ...
//!
//! # let alice = graph.add_vertex("Person").build().expect("failed to add vertex");
//! # let bob = graph.add_vertex("Person").build().expect("failed to add vertex");
//! // PageRank - find most influential nodes
//! let pagerank = graph.pagerank(20, 0.85);  // 20 iterations, damping=0.85
//! for (vertex_id, score) in &pagerank {
//!     if let Some(v) = graph.get_vertex(*vertex_id) {
//!         println!("{}: {:.4}",
//!             v.properties.get("name").unwrap_or(&ferrite::graph::PropertyValue::Null),
//!             score
//!         );
//!     }
//! }
//!
//! // Shortest path - find path between vertices
//! if let Some(path) = graph.shortest_path(alice, bob) {
//!     println!("Path from Alice to Bob:");
//!     for vertex_id in &path {
//!         if let Some(v) = graph.get_vertex(*vertex_id) {
//!             println!("  -> {}", v.properties.get("name").expect("name property missing"));
//!         }
//!     }
//! }
//!
//! // Connected components - find clusters
//! let components = graph.connected_components();
//! println!("Found {} connected components", components.len());
//! ```
//!
//! ## Redis CLI Commands
//!
//! ```text
//! # Create a vertex
//! GRAPH.ADDVERTEX mygraph Person name "Alice" age 30
//!
//! # Create an edge
//! GRAPH.ADDEDGE mygraph vertex:1 vertex:2 KNOWS since 2020
//!
//! # Get a vertex
//! GRAPH.GETVERTEX mygraph vertex:1
//!
//! # Traverse outgoing edges
//! GRAPH.TRAVERSE mygraph vertex:1 OUT KNOWS
//!
//! # Pattern matching (Cypher-like)
//! GRAPH.MATCH mygraph "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b"
//!
//! # Run PageRank
//! GRAPH.PAGERANK mygraph ITERATIONS 20 DAMPING 0.85
//!
//! # Find shortest path
//! GRAPH.PATH mygraph vertex:1 vertex:10
//!
//! # Create property index
//! GRAPH.INDEX mygraph Person name
//!
//! # Find by property
//! GRAPH.FIND mygraph Person name "Alice"
//!
//! # Graph statistics
//! GRAPH.INFO mygraph
//! ```
//!
//! ## Property Indexes
//!
//! ```no_run
//! use ferrite::graph::{Graph, PropertyValue};
//!
//! let graph = Graph::new();
//! // ... add vertices ...
//!
//! // Create an index on Person.name
//! graph.create_index("Person", "name")?;
//!
//! // Fast lookup by property (uses index)
//! let alices = graph.find_by_property(
//!     "Person",
//!     "name",
//!     &PropertyValue::String("Alice".to_string())
//! );
//!
//! println!("Found {} people named Alice", alices.len());
//! # Ok::<(), ferrite::graph::GraphError>(())
//! ```
//!
//! ## Query Language
//!
//! FerriteGraph supports a Cypher-like query language:
//!
//! ```no_run
//! use ferrite::graph::Graph;
//!
//! let graph = Graph::new();
//! // ... add data ...
//!
//! // Simple pattern match
//! let result = graph.query(r#"
//!     MATCH (p:Person)-[:WORKS_AT]->(c:Company)
//!     WHERE c.name = 'Acme Corp'
//!     RETURN p.name, p.role
//! "#)?;
//!
//! for row in &result.rows {
//!     println!("{:?}", row);
//! }
//!
//! // With aggregation
//! let result = graph.query(r#"
//!     MATCH (p:Person)-[:KNOWS]->(friend:Person)
//!     RETURN p.name, COUNT(friend) as friend_count
//!     ORDER BY friend_count DESC
//!     LIMIT 10
//! "#)?;
//! # Ok::<(), ferrite::graph::GraphError>(())
//! ```
//!
//! ## Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────┐
//! │                    Graph Database Engine                      │
//! ├──────────────────────────────────────────────────────────────┤
//! │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │
//! │  │   Vertices   │  │    Edges     │  │   Properties     │   │
//! │  │   (Nodes)    │──│  (Relations) │──│   (Key-Value)    │   │
//! │  └──────────────┘  └──────────────┘  └──────────────────┘   │
//! │         │                 │                   │              │
//! │  ┌──────▼──────┐  ┌──────▼──────┐  ┌─────────▼─────────┐   │
//! │  │   Labels    │  │  Traversal  │  │     Indexing      │   │
//! │  │   (Types)   │  │ (BFS/DFS)   │  │   (Properties)    │   │
//! │  └─────────────┘  └─────────────┘  └───────────────────┘   │
//! │         │                 │                   │              │
//! │  ┌──────▼──────────────────────────────────────────────┐    │
//! │  │              Graph Query Engine                      │    │
//! │  │        (Pattern Matching, Pathfinding)               │    │
//! │  └──────────────────────────────────────────────────────┘    │
//! └──────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Performance
//!
//! | Operation | Complexity | Typical Latency |
//! |-----------|------------|-----------------|
//! | Add vertex | O(1) | <1μs |
//! | Add edge | O(1) | <1μs |
//! | Get vertex | O(1) | <1μs |
//! | Traverse (per hop) | O(degree) | 1-10μs |
//! | Pattern match | O(n * m) | 1-100ms |
//! | PageRank (per iteration) | O(V + E) | 10-100ms |
//! | Shortest path (BFS) | O(V + E) | 1-50ms |
//!
//! ## Best Practices
//!
//! 1. **Use labels**: Filter traversals by vertex/edge type
//! 2. **Create indexes**: For frequently queried properties
//! 3. **Limit traversal depth**: Unbounded traversals can explode
//! 4. **Batch operations**: Add multiple vertices/edges in batches
//! 5. **Use pattern matching**: More efficient than manual traversal

pub mod aggregation;
pub mod algorithm;
pub mod edge;
pub mod index;
pub mod pattern;
pub mod query;
pub mod serialization;
pub mod storage;
pub mod traversal;
pub mod vertex;

pub use aggregation::{Degree, DegreeInfo, GraphStatistics, GraphStats, NeighborhoodAggregation};
pub use algorithm::{
    AllPaths, ConnectedComponents, CycleDetectingDfs, DfsCycleResult, GraphAlgorithm, PageRank,
    ShortestPath,
};
pub use edge::{Direction, Edge, EdgeBuilder, EdgeFilter, EdgeId};
pub use index::{GraphIndex, LabelIndex, PropertyIndex};
pub use pattern::{PathPattern, Pattern, PatternMatcher};
pub use query::{GraphQuery, MatchClause, QueryBuilder, WhereClause};
pub use serialization::{AdjList, GraphMl, ImportStats};
pub use storage::{AdjacencyList, GraphStorage};
pub use traversal::{TraversalResult, TraversalStep, Traverser};
pub use vertex::{Vertex, VertexBuilder, VertexFilter, VertexId};

/// Type alias: `Node` is the same as `Vertex` (property graph terminology).
pub type Node = Vertex;
/// Type alias: `NodeId` is the same as `VertexId`.
pub type NodeId = VertexId;

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Graph database error types
#[derive(Debug, thiserror::Error)]
pub enum GraphError {
    /// Vertex not found
    #[error("vertex not found: {0}")]
    VertexNotFound(VertexId),

    /// Edge not found
    #[error("edge not found: {0}")]
    EdgeNotFound(EdgeId),

    /// Invalid query
    #[error("invalid query: {0}")]
    InvalidQuery(String),

    /// Property not found
    #[error("property not found: {0}")]
    PropertyNotFound(String),

    /// Label not found
    #[error("label not found: {0}")]
    LabelNotFound(String),

    /// Constraint violation
    #[error("constraint violation: {0}")]
    ConstraintViolation(String),

    /// Index error
    #[error("index error: {0}")]
    IndexError(String),

    /// Algorithm error
    #[error("algorithm error: {0}")]
    AlgorithmError(String),
}

/// Result type for graph operations
pub type Result<T> = std::result::Result<T, GraphError>;

/// Property value type
#[derive(Debug, Clone, PartialEq)]
pub enum PropertyValue {
    /// String value
    String(String),
    /// Integer value
    Integer(i64),
    /// Float value
    Float(f64),
    /// Boolean value
    Boolean(bool),
    /// List of values
    List(Vec<PropertyValue>),
    /// Map of values
    Map(HashMap<String, PropertyValue>),
    /// Null value
    Null,
}

impl PropertyValue {
    /// Get as string
    pub fn as_string(&self) -> Option<&str> {
        match self {
            PropertyValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Get as integer
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            PropertyValue::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Get as float
    pub fn as_float(&self) -> Option<f64> {
        match self {
            PropertyValue::Float(f) => Some(*f),
            PropertyValue::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Get as boolean
    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            PropertyValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// Check if null
    pub fn is_null(&self) -> bool {
        matches!(self, PropertyValue::Null)
    }
}

impl From<&str> for PropertyValue {
    fn from(s: &str) -> Self {
        PropertyValue::String(s.to_string())
    }
}

impl From<String> for PropertyValue {
    fn from(s: String) -> Self {
        PropertyValue::String(s)
    }
}

impl From<i64> for PropertyValue {
    fn from(i: i64) -> Self {
        PropertyValue::Integer(i)
    }
}

impl From<i32> for PropertyValue {
    fn from(i: i32) -> Self {
        PropertyValue::Integer(i as i64)
    }
}

impl From<f64> for PropertyValue {
    fn from(f: f64) -> Self {
        PropertyValue::Float(f)
    }
}

impl From<bool> for PropertyValue {
    fn from(b: bool) -> Self {
        PropertyValue::Boolean(b)
    }
}

impl std::fmt::Display for PropertyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PropertyValue::String(s) => write!(f, "\"{}\"", s),
            PropertyValue::Integer(i) => write!(f, "{}", i),
            PropertyValue::Float(fl) => write!(f, "{}", fl),
            PropertyValue::Boolean(b) => write!(f, "{}", b),
            PropertyValue::List(l) => write!(f, "[{} items]", l.len()),
            PropertyValue::Map(m) => write!(f, "{{{} entries}}", m.len()),
            PropertyValue::Null => write!(f, "null"),
        }
    }
}

/// Main graph database
pub struct Graph {
    /// Graph storage
    storage: Arc<RwLock<GraphStorage>>,
    /// Configuration
    #[allow(dead_code)] // Planned for v0.2 — stored for future graph config support
    config: GraphConfig,
    /// Metrics
    metrics: GraphMetrics,
    /// Label index
    label_index: Arc<RwLock<LabelIndex>>,
    /// Property indexes
    property_indexes: Arc<RwLock<HashMap<String, PropertyIndex>>>,
}

/// Graph configuration
#[derive(Debug, Clone)]
pub struct GraphConfig {
    /// Maximum vertices
    pub max_vertices: usize,
    /// Maximum edges
    pub max_edges: usize,
    /// Enable auto-indexing
    pub auto_index: bool,
    /// Default relationship type
    pub default_relationship: String,
    /// Enable property caching
    pub property_cache: bool,
    /// Cache size
    pub cache_size: usize,
}

impl Default for GraphConfig {
    fn default() -> Self {
        Self {
            max_vertices: 10_000_000,
            max_edges: 100_000_000,
            auto_index: true,
            default_relationship: "RELATED_TO".to_string(),
            property_cache: true,
            cache_size: 10_000,
        }
    }
}

/// Graph metrics
#[derive(Debug, Default)]
pub struct GraphMetrics {
    /// Total vertices
    pub vertices: std::sync::atomic::AtomicU64,
    /// Total edges
    pub edges: std::sync::atomic::AtomicU64,
    /// Traversals performed
    pub traversals: std::sync::atomic::AtomicU64,
    /// Queries executed
    pub queries: std::sync::atomic::AtomicU64,
}

impl Graph {
    /// Create a new graph
    pub fn new() -> Self {
        Self::with_config(GraphConfig::default())
    }

    /// Create with configuration
    pub fn with_config(config: GraphConfig) -> Self {
        Self {
            storage: Arc::new(RwLock::new(GraphStorage::new())),
            config,
            metrics: GraphMetrics::default(),
            label_index: Arc::new(RwLock::new(LabelIndex::new())),
            property_indexes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a builder
    pub fn builder() -> GraphBuilder {
        GraphBuilder::new()
    }

    /// Add a vertex
    pub fn add_vertex(&self, label: &str) -> VertexBuilder {
        VertexBuilder::new(
            label,
            Arc::clone(&self.storage),
            Arc::clone(&self.label_index),
        )
    }

    /// Get a vertex by ID
    pub fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        self.storage.read().get_vertex(id)
    }

    /// Remove a vertex
    pub fn remove_vertex(&self, id: VertexId) -> Result<()> {
        let mut storage = self.storage.write();

        // Remove all edges connected to this vertex
        let edges_to_remove: Vec<EdgeId> = storage.get_edges(id).to_vec();
        for edge_id in edges_to_remove {
            storage.remove_edge(edge_id)?;
        }

        storage.remove_vertex(id)?;
        self.label_index.write().remove_vertex(id);

        self.metrics
            .vertices
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Add an edge
    pub fn add_edge(&self, from: VertexId, to: VertexId, label: &str) -> EdgeBuilder {
        EdgeBuilder::new(from, to, label, Arc::clone(&self.storage))
    }

    /// Get an edge by ID
    pub fn get_edge(&self, id: EdgeId) -> Option<Edge> {
        self.storage.read().get_edge(id)
    }

    /// Remove an edge
    pub fn remove_edge(&self, id: EdgeId) -> Result<()> {
        self.storage.write().remove_edge(id)?;
        self.metrics
            .edges
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Get vertices by label
    pub fn vertices_by_label(&self, label: &str) -> Vec<Vertex> {
        let ids = self.label_index.read().get_vertices(label);
        let storage = self.storage.read();
        ids.into_iter()
            .filter_map(|id| storage.get_vertex(id))
            .collect()
    }

    /// Get vertices that have all the specified labels
    pub fn vertices_by_labels(&self, labels: &[&str]) -> Vec<Vertex> {
        if labels.is_empty() {
            return self.vertices();
        }
        // Start with first label's vertices, then intersect
        let ids = self.label_index.read().get_vertices(labels[0]);
        let storage = self.storage.read();
        ids.into_iter()
            .filter_map(|id| storage.get_vertex(id))
            .filter(|v| labels.iter().all(|l| v.labels.contains(&l.to_string())))
            .collect()
    }

    /// Get all vertices
    pub fn vertices(&self) -> Vec<Vertex> {
        self.storage.read().all_vertices()
    }

    /// Get all edges
    pub fn edges(&self) -> Vec<Edge> {
        self.storage.read().all_edges()
    }

    /// Start a traversal
    pub fn traverse(&self, start: VertexId) -> Traverser {
        Traverser::new(start, Arc::clone(&self.storage))
    }

    /// Execute a query
    pub fn query(&self, query: &str) -> Result<QueryResult> {
        let parsed = GraphQuery::parse(query)?;
        self.execute_query(parsed)
    }

    /// Execute a parsed query
    pub fn execute_query(&self, query: GraphQuery) -> Result<QueryResult> {
        self.metrics
            .queries
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        query.execute(&self.storage.read())
    }

    /// Execute a Cypher query string (read-only).
    ///
    /// Parses and executes a Cypher query against the graph.
    /// Supports MATCH, WHERE, RETURN, ORDER BY, LIMIT, SKIP, and aggregations.
    pub fn cypher_query(&self, cypher: &str) -> Result<QueryResult> {
        use crate::cypher::{self, CypherParser};
        let stmt = CypherParser::parse(cypher).map_err(GraphError::InvalidQuery)?;
        match stmt {
            cypher::CypherStatement::Query(ref q) => {
                self.metrics
                    .queries
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                cypher::execute_read_only(q, &self.storage.read())
                    .map_err(GraphError::InvalidQuery)
            }
            _ => Err(GraphError::InvalidQuery(
                "cypher_query only supports read queries; use cypher_execute for mutations"
                    .to_string(),
            )),
        }
    }

    /// Execute a Cypher statement that may mutate the graph.
    ///
    /// Supports CREATE statements and MATCH+CREATE combinations in addition
    /// to read queries.
    pub fn cypher_execute(&self, cypher: &str) -> Result<QueryResult> {
        use crate::cypher::{self, CypherParser};
        let stmt = CypherParser::parse(cypher).map_err(GraphError::InvalidQuery)?;
        self.metrics
            .queries
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        cypher::execute(&stmt, &mut self.storage.write()).map_err(GraphError::InvalidQuery)
    }

    /// Plan a Cypher query and return the execution plan.
    pub fn cypher_plan(&self, cypher: &str) -> Result<crate::cypher::QueryPlan> {
        use crate::cypher::{self, CypherParser};
        let stmt = CypherParser::parse(cypher).map_err(GraphError::InvalidQuery)?;
        match stmt {
            cypher::CypherStatement::Query(ref q) => Ok(cypher::plan(q)),
            _ => Err(GraphError::InvalidQuery(
                "can only plan read queries".to_string(),
            )),
        }
    }

    /// Create a property index
    pub fn create_index(&self, label: &str, property: &str) -> Result<()> {
        let key = format!("{}:{}", label, property);
        let mut indexes = self.property_indexes.write();

        if indexes.contains_key(&key) {
            return Err(GraphError::IndexError(format!(
                "Index {} already exists",
                key
            )));
        }

        let mut index = PropertyIndex::new(label, property);

        // Populate index with existing vertices
        let storage = self.storage.read();
        for vertex in storage.all_vertices() {
            if vertex.labels.contains(&label.to_string()) {
                if let Some(value) = vertex.properties.get(property) {
                    index.add(vertex.id, value.clone());
                }
            }
        }

        indexes.insert(key, index);
        Ok(())
    }

    /// Find vertices by property value
    pub fn find_by_property(
        &self,
        label: &str,
        property: &str,
        value: &PropertyValue,
    ) -> Vec<Vertex> {
        let key = format!("{}:{}", label, property);
        let indexes = self.property_indexes.read();

        if let Some(index) = indexes.get(&key) {
            let ids = index.find(value);
            let storage = self.storage.read();
            ids.into_iter()
                .filter_map(|id| storage.get_vertex(id))
                .collect()
        } else {
            // Fall back to full scan
            self.vertices_by_label(label)
                .into_iter()
                .filter(|v| v.properties.get(property) == Some(value))
                .collect()
        }
    }

    /// Get vertex count
    pub fn vertex_count(&self) -> usize {
        self.storage.read().vertex_count()
    }

    /// Get edge count
    pub fn edge_count(&self) -> usize {
        self.storage.read().edge_count()
    }

    /// Get metrics
    pub fn metrics(&self) -> &GraphMetrics {
        &self.metrics
    }

    /// Run PageRank algorithm
    pub fn pagerank(&self, iterations: usize, damping: f64) -> HashMap<VertexId, f64> {
        PageRank::new(iterations, damping).run(&self.storage.read())
    }

    /// Find shortest path
    pub fn shortest_path(&self, from: VertexId, to: VertexId) -> Option<Vec<VertexId>> {
        ShortestPath::bfs(&self.storage.read(), from, to)
    }

    /// Find connected components
    pub fn connected_components(&self) -> Vec<Vec<VertexId>> {
        ConnectedComponents::find(&self.storage.read())
    }

    /// Find shortest path using Dijkstra (weighted)
    pub fn dijkstra_path(&self, from: VertexId, to: VertexId) -> Option<(Vec<VertexId>, f64)> {
        ShortestPath::dijkstra(&self.storage.read(), from, to)
    }

    /// Find all paths between two vertices with a maximum depth limit
    pub fn all_paths(&self, from: VertexId, to: VertexId, max_depth: usize) -> Vec<Vec<VertexId>> {
        AllPaths::find(&self.storage.read(), from, to, max_depth)
    }

    /// Run DFS with cycle detection from a starting vertex
    pub fn detect_cycles(&self, start: VertexId) -> DfsCycleResult {
        CycleDetectingDfs::run(&self.storage.read(), start, None)
    }

    /// Check if the graph (from a starting vertex) contains a cycle
    pub fn has_cycle(&self, start: VertexId) -> bool {
        CycleDetectingDfs::has_cycle(&self.storage.read(), start)
    }

    /// Get degree information for a vertex
    pub fn degree(&self, vertex: VertexId) -> DegreeInfo {
        Degree::info(&self.storage.read(), vertex)
    }

    /// Get graph-level statistics
    pub fn statistics(&self) -> GraphStats {
        GraphStatistics::compute(&self.storage.read())
    }

    /// Average a numeric property across a vertex's outgoing neighbors
    pub fn average_neighbor_property(&self, vertex: VertexId, property: &str) -> Option<f64> {
        NeighborhoodAggregation::average_neighbor_property(&self.storage.read(), vertex, property)
    }

    /// Export graph to GraphML string
    pub fn export_graphml(&self) -> String {
        GraphMl::export_string(&self.storage.read())
    }

    /// Import graph from GraphML string
    pub fn import_graphml(&self, graphml: &str) -> Result<ImportStats> {
        GraphMl::import(graphml.as_bytes(), &mut self.storage.write())
    }

    /// Export graph to adjacency list string
    pub fn export_adjlist(&self) -> String {
        AdjList::export_string(&self.storage.read())
    }

    /// Import graph from adjacency list string
    pub fn import_adjlist(&self, adjlist: &str) -> Result<ImportStats> {
        AdjList::import(adjlist.as_bytes(), &mut self.storage.write())
    }

    /// Match a pattern
    pub fn match_pattern(&self, pattern: &Pattern) -> Vec<PatternMatch> {
        PatternMatcher::new(pattern.clone()).find(&self.storage.read())
    }

    /// Clear the graph
    pub fn clear(&self) {
        self.storage.write().clear();
        self.label_index.write().clear();
        self.property_indexes.write().clear();
    }
}

impl Default for Graph {
    fn default() -> Self {
        Self::new()
    }
}

/// Graph builder
pub struct GraphBuilder {
    config: GraphConfig,
}

impl GraphBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: GraphConfig::default(),
        }
    }

    /// Set max vertices
    pub fn max_vertices(mut self, max: usize) -> Self {
        self.config.max_vertices = max;
        self
    }

    /// Set max edges
    pub fn max_edges(mut self, max: usize) -> Self {
        self.config.max_edges = max;
        self
    }

    /// Enable/disable auto indexing
    pub fn auto_index(mut self, enabled: bool) -> Self {
        self.config.auto_index = enabled;
        self
    }

    /// Build the graph
    pub fn build(self) -> Graph {
        Graph::with_config(self.config)
    }
}

impl Default for GraphBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Query result
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Result rows
    pub rows: Vec<QueryRow>,
    /// Column names
    pub columns: Vec<String>,
    /// Execution time in milliseconds
    pub took_ms: u64,
}

impl QueryResult {
    /// Create empty result
    pub fn empty() -> Self {
        Self {
            rows: Vec::new(),
            columns: Vec::new(),
            took_ms: 0,
        }
    }

    /// Get row count
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// A row in query results
#[derive(Debug, Clone)]
pub struct QueryRow {
    /// Values by column name
    pub values: HashMap<String, QueryValue>,
}

impl QueryRow {
    /// Get value by column name
    pub fn get(&self, column: &str) -> Option<&QueryValue> {
        self.values.get(column)
    }
}

/// Query result value
#[derive(Debug, Clone)]
pub enum QueryValue {
    /// Vertex
    Vertex(Vertex),
    /// Edge
    Edge(Edge),
    /// Property value
    Property(PropertyValue),
    /// Path (list of vertices/edges)
    Path(Vec<QueryValue>),
    /// Null
    Null,
}

/// Pattern match result
#[derive(Debug, Clone)]
pub struct PatternMatch {
    /// Matched vertices by variable name
    pub vertices: HashMap<String, Vertex>,
    /// Matched edges by variable name
    pub edges: HashMap<String, Edge>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_graph_creation() {
        let graph = Graph::new();
        assert_eq!(graph.vertex_count(), 0);
        assert_eq!(graph.edge_count(), 0);
    }

    #[test]
    fn test_add_vertex() {
        let graph = Graph::new();
        let v = graph
            .add_vertex("Person")
            .property("name", "Alice")
            .build()
            .unwrap();

        assert_eq!(graph.vertex_count(), 1);

        let vertex = graph.get_vertex(v).unwrap();
        assert_eq!(vertex.label, "Person");
    }

    #[test]
    fn test_add_edge() {
        let graph = Graph::new();

        let alice = graph
            .add_vertex("Person")
            .property("name", "Alice")
            .build()
            .unwrap();
        let bob = graph
            .add_vertex("Person")
            .property("name", "Bob")
            .build()
            .unwrap();

        let e = graph.add_edge(alice, bob, "KNOWS").build().unwrap();

        assert_eq!(graph.edge_count(), 1);

        let edge = graph.get_edge(e).unwrap();
        assert_eq!(edge.label, "KNOWS");
        assert_eq!(edge.from, alice);
        assert_eq!(edge.to, bob);
    }

    #[test]
    fn test_vertices_by_label() {
        let graph = Graph::new();

        graph
            .add_vertex("Person")
            .property("name", "Alice")
            .build()
            .unwrap();
        graph
            .add_vertex("Person")
            .property("name", "Bob")
            .build()
            .unwrap();
        graph
            .add_vertex("Company")
            .property("name", "Acme")
            .build()
            .unwrap();

        let people = graph.vertices_by_label("Person");
        assert_eq!(people.len(), 2);
    }

    #[test]
    fn test_property_value_conversions() {
        let s: PropertyValue = "hello".into();
        assert_eq!(s.as_string(), Some("hello"));

        let i: PropertyValue = 42i64.into();
        assert_eq!(i.as_integer(), Some(42));

        let f: PropertyValue = 3.14f64.into();
        assert_eq!(f.as_float(), Some(3.14));

        let b: PropertyValue = true.into();
        assert_eq!(b.as_boolean(), Some(true));
    }

    #[test]
    fn test_graph_builder() {
        let graph = Graph::builder()
            .max_vertices(1000)
            .max_edges(5000)
            .auto_index(false)
            .build();

        assert_eq!(graph.config.max_vertices, 1000);
    }

    #[test]
    fn test_remove_vertex() {
        let graph = Graph::new();

        let v = graph.add_vertex("Person").build().unwrap();
        assert_eq!(graph.vertex_count(), 1);

        graph.remove_vertex(v).unwrap();
        assert_eq!(graph.vertex_count(), 0);
    }

    #[test]
    fn test_graph_clear() {
        let graph = Graph::new();

        graph.add_vertex("Person").build().unwrap();
        graph.add_vertex("Person").build().unwrap();

        assert_eq!(graph.vertex_count(), 2);

        graph.clear();
        assert_eq!(graph.vertex_count(), 0);
    }

    // --- Multi-label tests ---

    #[test]
    fn test_vertex_multi_label() {
        let graph = Graph::new();

        let v = graph
            .add_vertex("Person")
            .label("Employee")
            .property("name", "Alice")
            .build()
            .unwrap();

        let vertex = graph.get_vertex(v).unwrap();
        assert!(vertex.has_label("Person"));
        assert!(vertex.has_label("Employee"));
        assert!(!vertex.has_label("Company"));
        assert_eq!(vertex.labels.len(), 2);
    }

    #[test]
    fn test_vertex_multi_label_index() {
        let graph = Graph::new();

        graph
            .add_vertex("Person")
            .label("Employee")
            .property("name", "Alice")
            .build()
            .unwrap();
        graph
            .add_vertex("Person")
            .property("name", "Bob")
            .build()
            .unwrap();

        // Both should show up as Person
        assert_eq!(graph.vertices_by_label("Person").len(), 2);
        // Only Alice is Employee
        assert_eq!(graph.vertices_by_label("Employee").len(), 1);
    }

    #[test]
    fn test_vertices_by_labels_intersection() {
        let graph = Graph::new();

        graph
            .add_vertex("Person")
            .label("Employee")
            .build()
            .unwrap();
        graph.add_vertex("Person").build().unwrap();
        graph
            .add_vertex("Employee")
            .label("Manager")
            .build()
            .unwrap();

        let result = graph.vertices_by_labels(&["Person", "Employee"]);
        assert_eq!(result.len(), 1);
    }

    // --- CRUD tests ---

    #[test]
    fn test_edge_with_properties() {
        let graph = Graph::new();

        let a = graph
            .add_vertex("Person")
            .property("name", "A")
            .build()
            .unwrap();
        let b = graph
            .add_vertex("Person")
            .property("name", "B")
            .build()
            .unwrap();

        let e = graph
            .add_edge(a, b, "KNOWS")
            .property("since", 2020i64)
            .property("strength", 0.9f64)
            .build()
            .unwrap();

        let edge = graph.get_edge(e).unwrap();
        assert_eq!(edge.get_integer("since"), Some(2020));
        assert_eq!(edge.get_float("strength"), Some(0.9));
    }

    #[test]
    fn test_remove_vertex_cascades_edges() {
        let graph = Graph::new();

        let a = graph.add_vertex("Person").build().unwrap();
        let b = graph.add_vertex("Person").build().unwrap();
        graph.add_edge(a, b, "KNOWS").build().unwrap();

        assert_eq!(graph.edge_count(), 1);
        graph.remove_vertex(a).unwrap();
        assert_eq!(graph.edge_count(), 0);
    }

    #[test]
    fn test_remove_edge_preserves_vertices() {
        let graph = Graph::new();

        let a = graph.add_vertex("Person").build().unwrap();
        let b = graph.add_vertex("Person").build().unwrap();
        let e = graph.add_edge(a, b, "KNOWS").build().unwrap();

        graph.remove_edge(e).unwrap();
        assert_eq!(graph.edge_count(), 0);
        assert_eq!(graph.vertex_count(), 2);
    }

    #[test]
    fn test_find_by_property() {
        let graph = Graph::new();

        graph
            .add_vertex("Person")
            .property("name", "Alice")
            .build()
            .unwrap();
        graph
            .add_vertex("Person")
            .property("name", "Bob")
            .build()
            .unwrap();

        let found = graph.find_by_property(
            "Person",
            "name",
            &PropertyValue::String("Alice".to_string()),
        );
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].get_string("name"), Some("Alice"));
    }

    #[test]
    fn test_find_by_property_with_index() {
        let graph = Graph::new();

        graph
            .add_vertex("Person")
            .property("name", "Alice")
            .build()
            .unwrap();
        graph
            .add_vertex("Person")
            .property("name", "Bob")
            .build()
            .unwrap();
        graph.create_index("Person", "name").unwrap();

        let found =
            graph.find_by_property("Person", "name", &PropertyValue::String("Bob".to_string()));
        assert_eq!(found.len(), 1);
    }

    // --- Cypher integration tests ---

    fn build_social_graph() -> Graph {
        let graph = Graph::new();

        let alice = graph
            .add_vertex("Person")
            .property("name", "Alice")
            .property("age", 30i64)
            .build()
            .unwrap();
        let bob = graph
            .add_vertex("Person")
            .property("name", "Bob")
            .property("age", 25i64)
            .build()
            .unwrap();
        let charlie = graph
            .add_vertex("Person")
            .property("name", "Charlie")
            .property("age", 35i64)
            .build()
            .unwrap();
        let acme = graph
            .add_vertex("Company")
            .property("name", "Acme Corp")
            .build()
            .unwrap();

        graph.add_edge(alice, bob, "KNOWS").build().unwrap();
        graph.add_edge(bob, charlie, "KNOWS").build().unwrap();
        graph.add_edge(alice, acme, "WORKS_AT").build().unwrap();
        graph.add_edge(bob, acme, "WORKS_AT").build().unwrap();

        graph
    }

    #[test]
    fn test_cypher_match_by_label() {
        let graph = build_social_graph();
        let result = graph.cypher_query("MATCH (n:Person) RETURN n").unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_cypher_match_property_filter() {
        let graph = build_social_graph();
        let result = graph
            .cypher_query("MATCH (n:Person {name: 'Alice'}) RETURN n")
            .unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_cypher_traverse_relationship() {
        let graph = build_social_graph();
        let result = graph
            .cypher_query("MATCH (a)-[:KNOWS]->(b) RETURN a, b")
            .unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_cypher_variable_length_path() {
        let graph = build_social_graph();
        let result = graph
            .cypher_query("MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..3]->(b) RETURN b")
            .unwrap();
        assert!(result.len() >= 2);
    }

    #[test]
    fn test_cypher_where_gt() {
        let graph = build_social_graph();
        let result = graph
            .cypher_query("MATCH (n:Person) WHERE n.age > 28 RETURN n")
            .unwrap();
        assert_eq!(result.len(), 2); // Alice=30, Charlie=35
    }

    #[test]
    fn test_cypher_count() {
        let graph = build_social_graph();
        let result = graph
            .cypher_query("MATCH (n:Person) RETURN count(n)")
            .unwrap();
        assert_eq!(result.len(), 1);
        match result.rows[0].values.values().next().unwrap() {
            QueryValue::Property(PropertyValue::Integer(3)) => {}
            other => panic!("expected count=3, got {:?}", other),
        }
    }

    #[test]
    fn test_cypher_sum() {
        let graph = build_social_graph();
        let result = graph
            .cypher_query("MATCH (n:Person) RETURN sum(n.age) AS total")
            .unwrap();
        match result.rows[0].get("total").unwrap() {
            QueryValue::Property(PropertyValue::Float(f)) => {
                assert!((f - 90.0).abs() < 0.01);
            }
            other => panic!("expected 90.0, got {:?}", other),
        }
    }

    #[test]
    fn test_cypher_avg() {
        let graph = build_social_graph();
        let result = graph
            .cypher_query("MATCH (n:Person) RETURN avg(n.age) AS average")
            .unwrap();
        match result.rows[0].get("average").unwrap() {
            QueryValue::Property(PropertyValue::Float(f)) => {
                assert!((f - 30.0).abs() < 0.01);
            }
            other => panic!("expected 30.0, got {:?}", other),
        }
    }

    #[test]
    fn test_cypher_order_by_limit() {
        let graph = build_social_graph();
        let result = graph
            .cypher_query("MATCH (n:Person) RETURN n.name ORDER BY n.age LIMIT 2")
            .unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_cypher_create_node() {
        let graph = build_social_graph();
        let prev = graph.vertex_count();
        graph
            .cypher_execute("CREATE (n:Person {name: 'Diana', age: 28})")
            .unwrap();
        assert_eq!(graph.vertex_count(), prev + 1);
    }

    #[test]
    fn test_cypher_match_create_edge() {
        let graph = build_social_graph();
        let prev_edges = graph.edge_count();
        graph
            .cypher_execute(
                "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Charlie'}) CREATE (a)-[:FRIENDS]->(b)",
            )
            .unwrap();
        assert_eq!(graph.edge_count(), prev_edges + 1);
    }

    #[test]
    fn test_cypher_where_and() {
        let graph = build_social_graph();
        let result = graph
            .cypher_query("MATCH (n:Person) WHERE n.age > 24 AND n.age < 31 RETURN n")
            .unwrap();
        assert_eq!(result.len(), 2); // Bob=25, Alice=30
    }

    #[test]
    fn test_cypher_plan() {
        let graph = build_social_graph();
        let plan = graph
            .cypher_plan("MATCH (n:Person) WHERE n.age > 25 RETURN n LIMIT 5")
            .unwrap();
        assert!(plan.estimated_cost > 0.0);
        assert!(!plan.steps.is_empty());
    }

    // --- Algorithm tests via Graph API ---

    #[test]
    fn test_shortest_path_integration() {
        let graph = build_social_graph();
        let verts = graph.vertices();
        let alice_id = verts
            .iter()
            .find(|v| v.get_string("name") == Some("Alice"))
            .unwrap()
            .id;
        let charlie_id = verts
            .iter()
            .find(|v| v.get_string("name") == Some("Charlie"))
            .unwrap()
            .id;

        let path = graph.shortest_path(alice_id, charlie_id);
        assert!(path.is_some());
        let path = path.unwrap();
        assert!(path.len() >= 2);
        assert_eq!(path[0], alice_id);
        assert_eq!(*path.last().unwrap(), charlie_id);
    }

    #[test]
    fn test_pagerank_integration() {
        let graph = build_social_graph();
        let ranks = graph.pagerank(20, 0.85);
        assert_eq!(ranks.len(), 4);
        let sum: f64 = ranks.values().sum();
        assert!((sum - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_connected_components_integration() {
        let graph = build_social_graph();
        let components = graph.connected_components();
        // All 4 vertices are connected
        assert_eq!(components.len(), 1);
        assert_eq!(components[0].len(), 4);
    }

    #[test]
    fn test_pattern_matching_integration() {
        let graph = build_social_graph();
        let pattern = Pattern::new()
            .node("a", vec!["Person"])
            .node("b", vec!["Person"])
            .edge("a", "b", vec!["KNOWS"], Direction::Out);

        let matches = graph.match_pattern(&pattern);
        assert_eq!(matches.len(), 2);
    }

    #[test]
    fn test_traversal_integration() {
        let graph = build_social_graph();
        let alice_id = graph
            .vertices()
            .iter()
            .find(|v| v.get_string("name") == Some("Alice"))
            .unwrap()
            .id;

        let friends = graph.traverse(alice_id).out(Some("KNOWS")).collect();
        assert_eq!(friends.len(), 1);
        assert_eq!(friends[0].get_string("name"), Some("Bob"));

        let coworkers = graph
            .traverse(alice_id)
            .out(Some("WORKS_AT"))
            .in_(Some("WORKS_AT"))
            .collect();
        // Alice and Bob both work at Acme, so traversal yields Alice and Bob
        assert!(coworkers.len() >= 1);
    }

    // --- New beta-quality feature tests ---

    #[test]
    fn test_dijkstra_integration() {
        let graph = build_social_graph();
        let verts = graph.vertices();
        let alice_id = verts
            .iter()
            .find(|v| v.get_string("name") == Some("Alice"))
            .unwrap()
            .id;
        let charlie_id = verts
            .iter()
            .find(|v| v.get_string("name") == Some("Charlie"))
            .unwrap()
            .id;

        let result = graph.dijkstra_path(alice_id, charlie_id);
        assert!(result.is_some());
        let (path, cost) = result.unwrap();
        assert_eq!(path[0], alice_id);
        assert_eq!(*path.last().unwrap(), charlie_id);
        assert!(cost > 0.0);
    }

    #[test]
    fn test_all_paths_integration() {
        let graph = build_social_graph();
        let verts = graph.vertices();
        let alice_id = verts
            .iter()
            .find(|v| v.get_string("name") == Some("Alice"))
            .unwrap()
            .id;
        let charlie_id = verts
            .iter()
            .find(|v| v.get_string("name") == Some("Charlie"))
            .unwrap()
            .id;

        let paths = graph.all_paths(alice_id, charlie_id, 5);
        assert!(!paths.is_empty());
        for path in &paths {
            assert_eq!(path[0], alice_id);
            assert_eq!(*path.last().unwrap(), charlie_id);
        }
    }

    #[test]
    fn test_cycle_detection_integration() {
        let graph = build_social_graph();
        let alice_id = graph
            .vertices()
            .iter()
            .find(|v| v.get_string("name") == Some("Alice"))
            .unwrap()
            .id;

        // DAG, no cycles
        assert!(!graph.has_cycle(alice_id));
    }

    #[test]
    fn test_degree_integration() {
        let graph = build_social_graph();
        let alice_id = graph
            .vertices()
            .iter()
            .find(|v| v.get_string("name") == Some("Alice"))
            .unwrap()
            .id;

        let info = graph.degree(alice_id);
        // Alice out-edges: Alice->Bob(KNOWS), Alice->Acme(WORKS_AT) = 2
        assert_eq!(info.out_degree, 2);
        assert_eq!(info.in_degree, 0);
        assert_eq!(info.total_degree, 2);
    }

    #[test]
    fn test_statistics_integration() {
        let graph = build_social_graph();
        let stats = graph.statistics();

        assert_eq!(stats.vertex_count, 4);
        assert_eq!(stats.edge_count, 4);
        assert!(stats.density > 0.0);
        assert_eq!(stats.self_loops, 0);
    }

    #[test]
    fn test_neighbor_aggregation_integration() {
        let graph = build_social_graph();
        let alice_id = graph
            .vertices()
            .iter()
            .find(|v| v.get_string("name") == Some("Alice"))
            .unwrap()
            .id;

        let avg = graph.average_neighbor_property(alice_id, "age");
        // Alice's outgoing neighbors include Bob(25) and possibly Acme(no age)
        assert!(avg.is_some());
    }

    #[test]
    fn test_graphml_roundtrip_integration() {
        let graph = build_social_graph();
        let xml = graph.export_graphml();

        assert!(xml.contains("<graphml"));
        assert!(xml.contains("Alice"));

        let graph2 = Graph::new();
        let stats = graph2.import_graphml(&xml).unwrap();

        assert_eq!(stats.vertices_imported, 4);
        assert_eq!(stats.edges_imported, 4);
        assert_eq!(graph2.vertex_count(), 4);
    }

    #[test]
    fn test_adjlist_roundtrip_integration() {
        let graph = build_social_graph();
        let output = graph.export_adjlist();

        assert!(output.contains("->"));

        let graph2 = Graph::new();
        let stats = graph2.import_adjlist(&output).unwrap();

        assert_eq!(stats.vertices_imported, 4);
        assert_eq!(stats.edges_imported, 4);
    }

    #[test]
    fn test_optional_pattern_match_integration() {
        let graph = build_social_graph();

        // All persons, optionally with a KNOWS relationship
        let pattern = Pattern::new().node("p", vec!["Person"]).optional_edge(
            "p",
            "friend",
            vec!["KNOWS"],
            Direction::Out,
        );

        let matches = graph.match_pattern(&pattern);
        // All 3 persons should appear; some with friends bound, some without
        assert!(matches.len() >= 3);
    }
}
