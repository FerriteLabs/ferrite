//! Graph vertex types
//!
//! Provides vertex (node) abstractions for the graph database.

use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

/// Unique vertex identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VertexId(pub u64);

impl VertexId {
    /// Create a new vertex ID
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Get the raw ID value
    pub fn value(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for VertexId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v{}", self.0)
    }
}

impl From<u64> for VertexId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

/// Global vertex ID counter
static VERTEX_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Generate a new unique vertex ID
pub fn next_vertex_id() -> VertexId {
    VertexId(VERTEX_ID_COUNTER.fetch_add(1, Ordering::SeqCst))
}

/// A vertex (node) in the graph
#[derive(Debug, Clone)]
pub struct Vertex {
    /// Unique identifier
    pub id: VertexId,
    /// Primary vertex label (type)
    pub label: String,
    /// All vertex labels (includes primary label)
    pub labels: Vec<String>,
    /// Properties
    pub properties: HashMap<String, PropertyValue>,
    /// Creation timestamp
    pub created_at: i64,
    /// Last modified timestamp
    pub updated_at: i64,
}

impl Vertex {
    /// Create a new vertex with a single label
    pub fn new(id: VertexId, label: impl Into<String>) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        let label_str = label.into();
        Self {
            id,
            label: label_str.clone(),
            labels: vec![label_str],
            properties: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Create a new vertex with multiple labels
    pub fn with_labels(id: VertexId, labels: Vec<String>) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        let primary = labels.first().cloned().unwrap_or_default();
        Self {
            id,
            label: primary,
            labels,
            properties: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Add a label to this vertex
    pub fn add_label(&mut self, label: impl Into<String>) {
        let label = label.into();
        if !self.labels.contains(&label) {
            self.labels.push(label);
            self.updated_at = chrono::Utc::now().timestamp_millis();
        }
    }

    /// Remove a label from this vertex
    pub fn remove_label(&mut self, label: &str) -> bool {
        if let Some(pos) = self.labels.iter().position(|l| l == label) {
            self.labels.remove(pos);
            if self.label == label {
                self.label = self.labels.first().cloned().unwrap_or_default();
            }
            self.updated_at = chrono::Utc::now().timestamp_millis();
            true
        } else {
            false
        }
    }

    /// Check if vertex has any of the given labels
    pub fn has_any_label(&self, labels: &[String]) -> bool {
        labels.iter().any(|l| self.labels.contains(l))
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

    /// Check if vertex has label
    pub fn has_label(&self, label: &str) -> bool {
        self.labels.contains(&label.to_string())
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
            "_labels".to_string(),
            PropertyValue::List(
                self.labels
                    .iter()
                    .map(|l| PropertyValue::String(l.clone()))
                    .collect(),
            ),
        );
        map
    }
}

impl PartialEq for Vertex {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Vertex {}

impl std::hash::Hash for Vertex {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

/// Vertex builder
pub struct VertexBuilder {
    labels: Vec<String>,
    properties: HashMap<String, PropertyValue>,
    storage: Arc<RwLock<GraphStorage>>,
    label_index: Arc<RwLock<LabelIndex>>,
}

impl VertexBuilder {
    /// Create a new vertex builder
    pub fn new(
        label: &str,
        storage: Arc<RwLock<GraphStorage>>,
        label_index: Arc<RwLock<LabelIndex>>,
    ) -> Self {
        Self {
            labels: vec![label.to_string()],
            properties: HashMap::new(),
            storage,
            label_index,
        }
    }

    /// Add an additional label
    pub fn label(mut self, label: &str) -> Self {
        let label = label.to_string();
        if !self.labels.contains(&label) {
            self.labels.push(label);
        }
        self
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

    /// Build and add the vertex
    pub fn build(self) -> Result<VertexId> {
        let id = next_vertex_id();
        let mut vertex = Vertex::with_labels(id, self.labels.clone());
        vertex.properties = self.properties;

        self.storage.write().add_vertex(vertex)?;
        let mut label_index = self.label_index.write();
        for label in &self.labels {
            label_index.add_vertex(id, label);
        }

        Ok(id)
    }
}

/// Vertex reference (for traversal results)
#[derive(Debug, Clone)]
pub struct VertexRef {
    /// Vertex ID
    pub id: VertexId,
    /// Vertex label
    pub label: String,
}

impl VertexRef {
    /// Create from vertex
    pub fn from_vertex(vertex: &Vertex) -> Self {
        Self {
            id: vertex.id,
            label: vertex.label.clone(),
        }
    }
}

impl From<&Vertex> for VertexRef {
    fn from(vertex: &Vertex) -> Self {
        Self::from_vertex(vertex)
    }
}

/// Vertex filter for queries
#[derive(Debug, Clone)]
pub enum VertexFilter {
    /// Match by label
    Label(String),
    /// Match by property
    Property(String, PropertyValue),
    /// Match by property comparison
    PropertyCompare(String, Comparison, PropertyValue),
    /// Match all
    All,
    /// Combine filters with AND
    And(Vec<VertexFilter>),
    /// Combine filters with OR
    Or(Vec<VertexFilter>),
    /// Negate filter
    Not(Box<VertexFilter>),
}

/// Comparison operators
#[derive(Debug, Clone, Copy)]
pub enum Comparison {
    /// Equal
    Eq,
    /// Not equal
    Ne,
    /// Less than
    Lt,
    /// Less than or equal
    Le,
    /// Greater than
    Gt,
    /// Greater than or equal
    Ge,
    /// Contains (for strings)
    Contains,
    /// Starts with
    StartsWith,
    /// Ends with
    EndsWith,
}

impl VertexFilter {
    /// Create label filter
    pub fn label(label: &str) -> Self {
        VertexFilter::Label(label.to_string())
    }

    /// Create property equality filter
    pub fn property(key: &str, value: impl Into<PropertyValue>) -> Self {
        VertexFilter::Property(key.to_string(), value.into())
    }

    /// Create property comparison filter
    pub fn compare(key: &str, op: Comparison, value: impl Into<PropertyValue>) -> Self {
        VertexFilter::PropertyCompare(key.to_string(), op, value.into())
    }

    /// Combine with AND
    pub fn and(self, other: VertexFilter) -> Self {
        match self {
            VertexFilter::And(mut filters) => {
                filters.push(other);
                VertexFilter::And(filters)
            }
            _ => VertexFilter::And(vec![self, other]),
        }
    }

    /// Combine with OR
    pub fn or(self, other: VertexFilter) -> Self {
        match self {
            VertexFilter::Or(mut filters) => {
                filters.push(other);
                VertexFilter::Or(filters)
            }
            _ => VertexFilter::Or(vec![self, other]),
        }
    }

    /// Negate
    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> Self {
        VertexFilter::Not(Box::new(self))
    }

    /// Check if vertex matches filter
    pub fn matches(&self, vertex: &Vertex) -> bool {
        match self {
            VertexFilter::Label(label) => vertex.labels.contains(label),
            VertexFilter::Property(key, value) => vertex.properties.get(key) == Some(value),
            VertexFilter::PropertyCompare(key, op, value) => {
                if let Some(prop) = vertex.properties.get(key) {
                    compare_values(prop, *op, value)
                } else {
                    false
                }
            }
            VertexFilter::All => true,
            VertexFilter::And(filters) => filters.iter().all(|f| f.matches(vertex)),
            VertexFilter::Or(filters) => filters.iter().any(|f| f.matches(vertex)),
            VertexFilter::Not(filter) => !filter.matches(vertex),
        }
    }
}

/// Compare two property values
pub fn compare_values(a: &PropertyValue, op: Comparison, b: &PropertyValue) -> bool {
    match (a, b) {
        (PropertyValue::Integer(a), PropertyValue::Integer(b)) => match op {
            Comparison::Eq => a == b,
            Comparison::Ne => a != b,
            Comparison::Lt => a < b,
            Comparison::Le => a <= b,
            Comparison::Gt => a > b,
            Comparison::Ge => a >= b,
            _ => false,
        },
        (PropertyValue::Float(a), PropertyValue::Float(b)) => match op {
            Comparison::Eq => (a - b).abs() < f64::EPSILON,
            Comparison::Ne => (a - b).abs() >= f64::EPSILON,
            Comparison::Lt => a < b,
            Comparison::Le => a <= b,
            Comparison::Gt => a > b,
            Comparison::Ge => a >= b,
            _ => false,
        },
        (PropertyValue::String(a), PropertyValue::String(b)) => match op {
            Comparison::Eq => a == b,
            Comparison::Ne => a != b,
            Comparison::Lt => a < b,
            Comparison::Le => a <= b,
            Comparison::Gt => a > b,
            Comparison::Ge => a >= b,
            Comparison::Contains => a.contains(b.as_str()),
            Comparison::StartsWith => a.starts_with(b.as_str()),
            Comparison::EndsWith => a.ends_with(b.as_str()),
        },
        (PropertyValue::Boolean(a), PropertyValue::Boolean(b)) => match op {
            Comparison::Eq => a == b,
            Comparison::Ne => a != b,
            _ => false,
        },
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vertex_id() {
        let id1 = next_vertex_id();
        let id2 = next_vertex_id();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_vertex_creation() {
        let id = VertexId::new(1);
        let vertex = Vertex::new(id, "Person");

        assert_eq!(vertex.id, id);
        assert_eq!(vertex.label, "Person");
    }

    #[test]
    fn test_vertex_properties() {
        let id = VertexId::new(1);
        let mut vertex = Vertex::new(id, "Person");

        vertex.set("name", "Alice");
        vertex.set("age", 30i64);

        assert_eq!(vertex.get_string("name"), Some("Alice"));
        assert_eq!(vertex.get_integer("age"), Some(30));
    }

    #[test]
    fn test_vertex_property_operations() {
        let id = VertexId::new(1);
        let mut vertex = Vertex::new(id, "Person");

        vertex.set("name", "Alice");
        assert!(vertex.has("name"));
        assert!(!vertex.has("age"));

        vertex.remove("name");
        assert!(!vertex.has("name"));
    }

    #[test]
    fn test_vertex_filter_label() {
        let vertex = Vertex::new(VertexId::new(1), "Person");
        let filter = VertexFilter::label("Person");

        assert!(filter.matches(&vertex));

        let filter2 = VertexFilter::label("Company");
        assert!(!filter2.matches(&vertex));
    }

    #[test]
    fn test_vertex_filter_property() {
        let mut vertex = Vertex::new(VertexId::new(1), "Person");
        vertex.set("name", "Alice");

        let filter = VertexFilter::property("name", "Alice");
        assert!(filter.matches(&vertex));

        let filter2 = VertexFilter::property("name", "Bob");
        assert!(!filter2.matches(&vertex));
    }

    #[test]
    fn test_vertex_filter_comparison() {
        let mut vertex = Vertex::new(VertexId::new(1), "Person");
        vertex.set("age", 30i64);

        let filter = VertexFilter::compare("age", Comparison::Gt, 25i64);
        assert!(filter.matches(&vertex));

        let filter2 = VertexFilter::compare("age", Comparison::Lt, 25i64);
        assert!(!filter2.matches(&vertex));
    }

    #[test]
    fn test_vertex_filter_and() {
        let mut vertex = Vertex::new(VertexId::new(1), "Person");
        vertex.set("name", "Alice");
        vertex.set("age", 30i64);

        let filter = VertexFilter::label("Person").and(VertexFilter::property("name", "Alice"));

        assert!(filter.matches(&vertex));
    }

    #[test]
    fn test_vertex_filter_or() {
        let mut vertex = Vertex::new(VertexId::new(1), "Person");
        vertex.set("name", "Alice");

        let filter =
            VertexFilter::property("name", "Alice").or(VertexFilter::property("name", "Bob"));

        assert!(filter.matches(&vertex));
    }

    #[test]
    fn test_vertex_filter_not() {
        let vertex = Vertex::new(VertexId::new(1), "Person");

        let filter = VertexFilter::label("Company").not();
        assert!(filter.matches(&vertex));
    }

    #[test]
    fn test_vertex_to_map() {
        let mut vertex = Vertex::new(VertexId::new(1), "Person");
        vertex.set("name", "Alice");

        let map = vertex.to_map();
        assert!(map.contains_key("_id"));
        assert!(map.contains_key("_label"));
        assert!(map.contains_key("name"));
    }

    #[test]
    fn test_string_comparison() {
        let a = PropertyValue::String("hello".to_string());
        let b = PropertyValue::String("ell".to_string());

        assert!(compare_values(&a, Comparison::Contains, &b));
        assert!(compare_values(
            &a,
            Comparison::StartsWith,
            &PropertyValue::String("hel".to_string())
        ));
        assert!(compare_values(
            &a,
            Comparison::EndsWith,
            &PropertyValue::String("llo".to_string())
        ));
    }
}
