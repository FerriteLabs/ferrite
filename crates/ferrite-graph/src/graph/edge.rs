//! Graph edge types
//!
//! Provides edge (relationship) abstractions for the graph database.

use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

/// Unique edge identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EdgeId(pub u64);

impl EdgeId {
    /// Create a new edge ID
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Get the raw ID value
    pub fn value(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for EdgeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "e{}", self.0)
    }
}

impl From<u64> for EdgeId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

/// Global edge ID counter
static EDGE_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Generate a new unique edge ID
pub fn next_edge_id() -> EdgeId {
    EdgeId(EDGE_ID_COUNTER.fetch_add(1, Ordering::SeqCst))
}

/// Edge direction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Direction {
    /// Outgoing edges (from this vertex)
    Out,
    /// Incoming edges (to this vertex)
    In,
    /// Both directions
    #[default]
    Both,
}

/// An edge (relationship) in the graph
#[derive(Debug, Clone)]
pub struct Edge {
    /// Unique identifier
    pub id: EdgeId,
    /// Source vertex
    pub from: VertexId,
    /// Target vertex
    pub to: VertexId,
    /// Edge label (type)
    pub label: String,
    /// Properties
    pub properties: HashMap<String, PropertyValue>,
    /// Creation timestamp
    pub created_at: i64,
    /// Last modified timestamp
    pub updated_at: i64,
    /// Weight (for weighted graphs)
    pub weight: f64,
}

impl Edge {
    /// Create a new edge
    pub fn new(id: EdgeId, from: VertexId, to: VertexId, label: impl Into<String>) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            id,
            from,
            to,
            label: label.into(),
            properties: HashMap::new(),
            created_at: now,
            updated_at: now,
            weight: 1.0,
        }
    }

    /// Get a property
    pub fn get(&self, key: &str) -> Option<&PropertyValue> {
        self.properties.get(key)
    }

    /// Get a string property
    pub fn get_string(&self, key: &str) -> Option<&str> {
        self.properties.get(key).and_then(|v| v.as_string())
    }

    /// Get an integer property
    pub fn get_integer(&self, key: &str) -> Option<i64> {
        self.properties.get(key).and_then(|v| v.as_integer())
    }

    /// Get a float property
    pub fn get_float(&self, key: &str) -> Option<f64> {
        self.properties.get(key).and_then(|v| v.as_float())
    }

    /// Get a boolean property
    pub fn get_boolean(&self, key: &str) -> Option<bool> {
        self.properties.get(key).and_then(|v| v.as_boolean())
    }

    /// Set a property
    pub fn set(&mut self, key: impl Into<String>, value: impl Into<PropertyValue>) {
        self.properties.insert(key.into(), value.into());
        self.updated_at = chrono::Utc::now().timestamp_millis();
    }

    /// Remove a property
    pub fn remove(&mut self, key: &str) -> Option<PropertyValue> {
        let result = self.properties.remove(key);
        if result.is_some() {
            self.updated_at = chrono::Utc::now().timestamp_millis();
        }
        result
    }

    /// Check if property exists
    pub fn has(&self, key: &str) -> bool {
        self.properties.contains_key(key)
    }

    /// Get all property keys
    pub fn keys(&self) -> Vec<&str> {
        self.properties.keys().map(|k| k.as_str()).collect()
    }

    /// Get property count
    pub fn property_count(&self) -> usize {
        self.properties.len()
    }

    /// Check if edge has label
    pub fn has_label(&self, label: &str) -> bool {
        self.label == label
    }

    /// Set weight
    pub fn set_weight(&mut self, weight: f64) {
        self.weight = weight;
    }

    /// Get the other vertex (given one end)
    pub fn other(&self, vertex: VertexId) -> Option<VertexId> {
        if self.from == vertex {
            Some(self.to)
        } else if self.to == vertex {
            Some(self.from)
        } else {
            None
        }
    }

    /// Check if edge connects to a vertex
    pub fn connects(&self, vertex: VertexId) -> bool {
        self.from == vertex || self.to == vertex
    }

    /// Check if edge is a self-loop
    pub fn is_self_loop(&self) -> bool {
        self.from == self.to
    }

    /// Convert to map
    pub fn to_map(&self) -> HashMap<String, PropertyValue> {
        let mut map = self.properties.clone();
        map.insert("_id".to_string(), PropertyValue::Integer(self.id.0 as i64));
        map.insert(
            "_label".to_string(),
            PropertyValue::String(self.label.clone()),
        );
        map.insert(
            "_from".to_string(),
            PropertyValue::Integer(self.from.0 as i64),
        );
        map.insert("_to".to_string(), PropertyValue::Integer(self.to.0 as i64));
        map.insert("_weight".to_string(), PropertyValue::Float(self.weight));
        map
    }
}

impl PartialEq for Edge {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Edge {}

impl std::hash::Hash for Edge {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

/// Edge builder
pub struct EdgeBuilder {
    from: VertexId,
    to: VertexId,
    label: String,
    properties: HashMap<String, PropertyValue>,
    weight: f64,
    storage: Arc<RwLock<GraphStorage>>,
}

impl EdgeBuilder {
    /// Create a new edge builder
    pub fn new(
        from: VertexId,
        to: VertexId,
        label: &str,
        storage: Arc<RwLock<GraphStorage>>,
    ) -> Self {
        Self {
            from,
            to,
            label: label.to_string(),
            properties: HashMap::new(),
            weight: 1.0,
            storage,
        }
    }

    /// Add a property
    pub fn property<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<PropertyValue>,
    {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Add multiple properties
    pub fn properties(mut self, props: HashMap<String, PropertyValue>) -> Self {
        self.properties.extend(props);
        self
    }

    /// Set weight
    pub fn weight(mut self, weight: f64) -> Self {
        self.weight = weight;
        self
    }

    /// Build and add the edge
    pub fn build(self) -> Result<EdgeId> {
        let id = next_edge_id();
        let mut edge = Edge::new(id, self.from, self.to, &self.label);
        edge.properties = self.properties;
        edge.weight = self.weight;

        self.storage.write().add_edge(edge)?;

        Ok(id)
    }
}

/// Edge reference (for traversal results)
#[derive(Debug, Clone)]
pub struct EdgeRef {
    /// Edge ID
    pub id: EdgeId,
    /// Source vertex
    pub from: VertexId,
    /// Target vertex
    pub to: VertexId,
    /// Edge label
    pub label: String,
}

impl EdgeRef {
    /// Create from edge
    pub fn from_edge(edge: &Edge) -> Self {
        Self {
            id: edge.id,
            from: edge.from,
            to: edge.to,
            label: edge.label.clone(),
        }
    }
}

impl From<&Edge> for EdgeRef {
    fn from(edge: &Edge) -> Self {
        Self::from_edge(edge)
    }
}

/// Edge filter for queries
#[derive(Debug, Clone)]
pub enum EdgeFilter {
    /// Match by label
    Label(String),
    /// Match by labels (any of)
    Labels(Vec<String>),
    /// Match by property
    Property(String, PropertyValue),
    /// Match by property comparison
    PropertyCompare(String, super::vertex::Comparison, PropertyValue),
    /// Match by weight range
    WeightRange(f64, f64),
    /// Match by direction
    Direction(Direction),
    /// Match all
    All,
    /// Combine filters with AND
    And(Vec<EdgeFilter>),
    /// Combine filters with OR
    Or(Vec<EdgeFilter>),
    /// Negate filter
    Not(Box<EdgeFilter>),
}

impl EdgeFilter {
    /// Create label filter
    pub fn label(label: &str) -> Self {
        EdgeFilter::Label(label.to_string())
    }

    /// Create labels filter (any of)
    pub fn labels(labels: Vec<&str>) -> Self {
        EdgeFilter::Labels(labels.into_iter().map(|s| s.to_string()).collect())
    }

    /// Create property equality filter
    pub fn property(key: &str, value: impl Into<PropertyValue>) -> Self {
        EdgeFilter::Property(key.to_string(), value.into())
    }

    /// Create weight range filter
    pub fn weight_range(min: f64, max: f64) -> Self {
        EdgeFilter::WeightRange(min, max)
    }

    /// Create direction filter
    pub fn direction(direction: Direction) -> Self {
        EdgeFilter::Direction(direction)
    }

    /// Combine with AND
    pub fn and(self, other: EdgeFilter) -> Self {
        match self {
            EdgeFilter::And(mut filters) => {
                filters.push(other);
                EdgeFilter::And(filters)
            }
            _ => EdgeFilter::And(vec![self, other]),
        }
    }

    /// Combine with OR
    pub fn or(self, other: EdgeFilter) -> Self {
        match self {
            EdgeFilter::Or(mut filters) => {
                filters.push(other);
                EdgeFilter::Or(filters)
            }
            _ => EdgeFilter::Or(vec![self, other]),
        }
    }

    /// Negate
    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> Self {
        EdgeFilter::Not(Box::new(self))
    }

    /// Check if edge matches filter
    pub fn matches(&self, edge: &Edge, direction: Option<Direction>) -> bool {
        match self {
            EdgeFilter::Label(label) => edge.label == *label,
            EdgeFilter::Labels(labels) => labels.contains(&edge.label),
            EdgeFilter::Property(key, value) => edge.properties.get(key) == Some(value),
            EdgeFilter::PropertyCompare(key, op, value) => {
                if let Some(prop) = edge.properties.get(key) {
                    super::vertex::compare_values(prop, *op, value)
                } else {
                    false
                }
            }
            EdgeFilter::WeightRange(min, max) => edge.weight >= *min && edge.weight <= *max,
            EdgeFilter::Direction(dir) => direction.map(|d| d == *dir).unwrap_or(true),
            EdgeFilter::All => true,
            EdgeFilter::And(filters) => filters.iter().all(|f| f.matches(edge, direction)),
            EdgeFilter::Or(filters) => filters.iter().any(|f| f.matches(edge, direction)),
            EdgeFilter::Not(filter) => !filter.matches(edge, direction),
        }
    }
}

/// Multi-edge (for multigraphs)
#[derive(Debug, Clone)]
pub struct MultiEdge {
    /// Edges between the same pair of vertices
    pub edges: Vec<Edge>,
}

impl MultiEdge {
    /// Create new multi-edge
    pub fn new() -> Self {
        Self { edges: Vec::new() }
    }

    /// Add an edge
    pub fn add(&mut self, edge: Edge) {
        self.edges.push(edge);
    }

    /// Get edge count
    pub fn len(&self) -> usize {
        self.edges.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }

    /// Get by label
    pub fn by_label(&self, label: &str) -> Vec<&Edge> {
        self.edges.iter().filter(|e| e.label == label).collect()
    }
}

impl Default for MultiEdge {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_edge_id() {
        let id1 = next_edge_id();
        let id2 = next_edge_id();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_edge_creation() {
        let id = EdgeId::new(1);
        let from = VertexId::new(1);
        let to = VertexId::new(2);
        let edge = Edge::new(id, from, to, "KNOWS");

        assert_eq!(edge.id, id);
        assert_eq!(edge.from, from);
        assert_eq!(edge.to, to);
        assert_eq!(edge.label, "KNOWS");
    }

    #[test]
    fn test_edge_properties() {
        let id = EdgeId::new(1);
        let from = VertexId::new(1);
        let to = VertexId::new(2);
        let mut edge = Edge::new(id, from, to, "KNOWS");

        edge.set("since", 2020i64);
        edge.set("strength", 0.8f64);

        assert_eq!(edge.get_integer("since"), Some(2020));
        assert_eq!(edge.get_float("strength"), Some(0.8));
    }

    #[test]
    fn test_edge_other() {
        let id = EdgeId::new(1);
        let from = VertexId::new(1);
        let to = VertexId::new(2);
        let edge = Edge::new(id, from, to, "KNOWS");

        assert_eq!(edge.other(from), Some(to));
        assert_eq!(edge.other(to), Some(from));
        assert_eq!(edge.other(VertexId::new(3)), None);
    }

    #[test]
    fn test_edge_connects() {
        let edge = Edge::new(EdgeId::new(1), VertexId::new(1), VertexId::new(2), "KNOWS");

        assert!(edge.connects(VertexId::new(1)));
        assert!(edge.connects(VertexId::new(2)));
        assert!(!edge.connects(VertexId::new(3)));
    }

    #[test]
    fn test_edge_self_loop() {
        let self_loop = Edge::new(EdgeId::new(1), VertexId::new(1), VertexId::new(1), "SELF");
        assert!(self_loop.is_self_loop());

        let normal = Edge::new(EdgeId::new(2), VertexId::new(1), VertexId::new(2), "KNOWS");
        assert!(!normal.is_self_loop());
    }

    #[test]
    fn test_edge_filter_label() {
        let edge = Edge::new(EdgeId::new(1), VertexId::new(1), VertexId::new(2), "KNOWS");

        let filter = EdgeFilter::label("KNOWS");
        assert!(filter.matches(&edge, None));

        let filter2 = EdgeFilter::label("FOLLOWS");
        assert!(!filter2.matches(&edge, None));
    }

    #[test]
    fn test_edge_filter_labels() {
        let edge = Edge::new(EdgeId::new(1), VertexId::new(1), VertexId::new(2), "KNOWS");

        let filter = EdgeFilter::labels(vec!["KNOWS", "FOLLOWS"]);
        assert!(filter.matches(&edge, None));

        let filter2 = EdgeFilter::labels(vec!["WORKS_AT", "LIVES_IN"]);
        assert!(!filter2.matches(&edge, None));
    }

    #[test]
    fn test_edge_filter_weight() {
        let mut edge = Edge::new(EdgeId::new(1), VertexId::new(1), VertexId::new(2), "KNOWS");
        edge.weight = 0.5;

        let filter = EdgeFilter::weight_range(0.0, 1.0);
        assert!(filter.matches(&edge, None));

        let filter2 = EdgeFilter::weight_range(0.6, 1.0);
        assert!(!filter2.matches(&edge, None));
    }

    #[test]
    fn test_edge_filter_and() {
        let mut edge = Edge::new(EdgeId::new(1), VertexId::new(1), VertexId::new(2), "KNOWS");
        edge.set("since", 2020i64);

        let filter = EdgeFilter::label("KNOWS").and(EdgeFilter::property("since", 2020i64));

        assert!(filter.matches(&edge, None));
    }

    #[test]
    fn test_direction() {
        assert_eq!(Direction::default(), Direction::Both);
    }

    #[test]
    fn test_edge_to_map() {
        let mut edge = Edge::new(EdgeId::new(1), VertexId::new(1), VertexId::new(2), "KNOWS");
        edge.set("since", 2020i64);

        let map = edge.to_map();
        assert!(map.contains_key("_id"));
        assert!(map.contains_key("_label"));
        assert!(map.contains_key("_from"));
        assert!(map.contains_key("_to"));
        assert!(map.contains_key("since"));
    }

    #[test]
    fn test_multi_edge() {
        let mut me = MultiEdge::new();
        assert!(me.is_empty());

        me.add(Edge::new(
            EdgeId::new(1),
            VertexId::new(1),
            VertexId::new(2),
            "KNOWS",
        ));
        me.add(Edge::new(
            EdgeId::new(2),
            VertexId::new(1),
            VertexId::new(2),
            "FOLLOWS",
        ));

        assert_eq!(me.len(), 2);
        assert_eq!(me.by_label("KNOWS").len(), 1);
    }

    #[test]
    fn test_edge_ref() {
        let edge = Edge::new(EdgeId::new(1), VertexId::new(1), VertexId::new(2), "KNOWS");

        let edge_ref = EdgeRef::from_edge(&edge);
        assert_eq!(edge_ref.id, edge.id);
        assert_eq!(edge_ref.from, edge.from);
        assert_eq!(edge_ref.to, edge.to);
    }
}
