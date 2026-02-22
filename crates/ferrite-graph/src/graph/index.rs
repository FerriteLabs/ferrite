//! Graph indexing
//!
//! Provides indexes for efficient vertex and edge lookups.

use super::*;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Graph index trait
pub trait GraphIndex: Send + Sync {
    /// Index type name
    fn index_type(&self) -> &str;

    /// Get indexed field
    fn field(&self) -> &str;

    /// Get index size
    fn size(&self) -> usize;

    /// Clear the index
    fn clear(&mut self);
}

/// Label index for vertices
pub struct LabelIndex {
    /// Label to vertex IDs
    label_to_vertices: HashMap<String, HashSet<VertexId>>,
    /// Vertex to labels (supports multiple labels per vertex)
    vertex_to_labels: HashMap<VertexId, Vec<String>>,
}

impl LabelIndex {
    /// Create a new label index
    pub fn new() -> Self {
        Self {
            label_to_vertices: HashMap::new(),
            vertex_to_labels: HashMap::new(),
        }
    }

    /// Add a vertex with label
    pub fn add_vertex(&mut self, id: VertexId, label: &str) {
        self.label_to_vertices
            .entry(label.to_string())
            .or_default()
            .insert(id);
        let labels = self.vertex_to_labels.entry(id).or_default();
        let label_str = label.to_string();
        if !labels.contains(&label_str) {
            labels.push(label_str);
        }
    }

    /// Remove a vertex
    pub fn remove_vertex(&mut self, id: VertexId) {
        if let Some(labels) = self.vertex_to_labels.remove(&id) {
            for label in labels {
                if let Some(vertices) = self.label_to_vertices.get_mut(&label) {
                    vertices.remove(&id);
                    if vertices.is_empty() {
                        self.label_to_vertices.remove(&label);
                    }
                }
            }
        }
    }

    /// Get vertices by label
    pub fn get_vertices(&self, label: &str) -> Vec<VertexId> {
        self.label_to_vertices
            .get(label)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get primary label for vertex
    pub fn get_label(&self, id: VertexId) -> Option<&str> {
        self.vertex_to_labels
            .get(&id)
            .and_then(|labels| labels.first())
            .map(|s| s.as_str())
    }

    /// Get all labels for vertex
    pub fn get_labels(&self, id: VertexId) -> Vec<&str> {
        self.vertex_to_labels
            .get(&id)
            .map(|labels| labels.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    /// Get all labels
    pub fn labels(&self) -> Vec<&str> {
        self.label_to_vertices.keys().map(|s| s.as_str()).collect()
    }

    /// Get vertex count for label
    pub fn count(&self, label: &str) -> usize {
        self.label_to_vertices
            .get(label)
            .map(|s| s.len())
            .unwrap_or(0)
    }

    /// Clear the index
    pub fn clear(&mut self) {
        self.label_to_vertices.clear();
        self.vertex_to_labels.clear();
    }
}

impl Default for LabelIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphIndex for LabelIndex {
    fn index_type(&self) -> &str {
        "label"
    }

    fn field(&self) -> &str {
        "_label"
    }

    fn size(&self) -> usize {
        self.vertex_to_labels.len()
    }

    fn clear(&mut self) {
        self.label_to_vertices.clear();
        self.vertex_to_labels.clear();
    }
}

/// Property index for vertices
pub struct PropertyIndex {
    /// Label this index applies to
    label: String,
    /// Property being indexed
    property: String,
    /// Value to vertex IDs (for exact match)
    exact_index: HashMap<PropertyValueKey, HashSet<VertexId>>,
    /// Sorted values for range queries (numeric properties)
    range_index: BTreeMap<OrderedValue, HashSet<VertexId>>,
    /// Full-text index (for string properties)
    text_index: HashMap<String, HashSet<VertexId>>,
    /// Index type
    #[allow(dead_code)] // Planned for v0.2 — stored for index type selection logic
    index_type: PropertyIndexType,
}

/// Property index type
#[derive(Debug, Clone, Copy)]
pub enum PropertyIndexType {
    /// Exact match only
    Exact,
    /// Range queries (numeric)
    Range,
    /// Full-text search
    FullText,
    /// Composite (all types)
    Composite,
}

impl PropertyIndex {
    /// Create a new property index
    pub fn new(label: &str, property: &str) -> Self {
        Self {
            label: label.to_string(),
            property: property.to_string(),
            exact_index: HashMap::new(),
            range_index: BTreeMap::new(),
            text_index: HashMap::new(),
            index_type: PropertyIndexType::Composite,
        }
    }

    /// Create with specific type
    pub fn with_type(label: &str, property: &str, index_type: PropertyIndexType) -> Self {
        Self {
            index_type,
            ..Self::new(label, property)
        }
    }

    /// Add a vertex to the index
    pub fn add(&mut self, id: VertexId, value: PropertyValue) {
        // Exact index
        let key = PropertyValueKey::from(&value);
        self.exact_index
            .entry(key)
            .or_default()
            .insert(id);

        // Range index (for numeric values)
        if let Some(ordered) = OrderedValue::from_property(&value) {
            self.range_index
                .entry(ordered)
                .or_default()
                .insert(id);
        }

        // Text index (for string values)
        if let PropertyValue::String(s) = &value {
            // Simple word tokenization
            for word in s.to_lowercase().split_whitespace() {
                self.text_index
                    .entry(word.to_string())
                    .or_default()
                    .insert(id);
            }
        }
    }

    /// Remove a vertex from the index
    pub fn remove(&mut self, id: VertexId, value: &PropertyValue) {
        let key = PropertyValueKey::from(value);
        if let Some(vertices) = self.exact_index.get_mut(&key) {
            vertices.remove(&id);
        }

        if let Some(ordered) = OrderedValue::from_property(value) {
            if let Some(vertices) = self.range_index.get_mut(&ordered) {
                vertices.remove(&id);
            }
        }

        if let PropertyValue::String(s) = value {
            for word in s.to_lowercase().split_whitespace() {
                if let Some(vertices) = self.text_index.get_mut(word) {
                    vertices.remove(&id);
                }
            }
        }
    }

    /// Find by exact value
    pub fn find(&self, value: &PropertyValue) -> Vec<VertexId> {
        let key = PropertyValueKey::from(value);
        self.exact_index
            .get(&key)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Find by range (inclusive)
    pub fn find_range(&self, min: &PropertyValue, max: &PropertyValue) -> Vec<VertexId> {
        let min_ord = OrderedValue::from_property(min);
        let max_ord = OrderedValue::from_property(max);

        match (min_ord, max_ord) {
            (Some(min_ord), Some(max_ord)) => {
                let mut result = HashSet::new();
                for (_, vertices) in self.range_index.range(min_ord..=max_ord) {
                    result.extend(vertices.iter().cloned());
                }
                result.into_iter().collect()
            }
            _ => Vec::new(),
        }
    }

    /// Find by text search
    pub fn find_text(&self, query: &str) -> Vec<VertexId> {
        let mut result = HashSet::new();
        let query_lower = query.to_lowercase();

        for word in query_lower.split_whitespace() {
            if let Some(vertices) = self.text_index.get(word) {
                if result.is_empty() {
                    result = vertices.clone();
                } else {
                    result = result.intersection(vertices).cloned().collect();
                }
            } else {
                return Vec::new(); // Word not found, no matches
            }
        }

        result.into_iter().collect()
    }

    /// Get the label
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Get the property
    pub fn property(&self) -> &str {
        &self.property
    }
}

impl GraphIndex for PropertyIndex {
    fn index_type(&self) -> &str {
        "property"
    }

    fn field(&self) -> &str {
        &self.property
    }

    fn size(&self) -> usize {
        self.exact_index.values().map(|s| s.len()).sum()
    }

    fn clear(&mut self) {
        self.exact_index.clear();
        self.range_index.clear();
        self.text_index.clear();
    }
}

/// Property value key for hashing
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum PropertyValueKey {
    String(String),
    Integer(i64),
    Boolean(bool),
    Float(u64), // Bit representation
    Null,
}

impl From<&PropertyValue> for PropertyValueKey {
    fn from(value: &PropertyValue) -> Self {
        match value {
            PropertyValue::String(s) => PropertyValueKey::String(s.clone()),
            PropertyValue::Integer(i) => PropertyValueKey::Integer(*i),
            PropertyValue::Boolean(b) => PropertyValueKey::Boolean(*b),
            PropertyValue::Float(f) => PropertyValueKey::Float(f.to_bits()),
            PropertyValue::Null => PropertyValueKey::Null,
            _ => PropertyValueKey::Null,
        }
    }
}

/// Ordered value for range queries
#[derive(Debug, Clone, PartialEq)]
struct OrderedValue(f64);

impl OrderedValue {
    fn from_property(value: &PropertyValue) -> Option<Self> {
        match value {
            PropertyValue::Integer(i) => Some(OrderedValue(*i as f64)),
            PropertyValue::Float(f) => Some(OrderedValue(*f)),
            _ => None,
        }
    }
}

impl Eq for OrderedValue {}

impl PartialOrd for OrderedValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Composite index for multiple properties
pub struct CompositeIndex {
    /// Label
    #[allow(dead_code)] // Planned for v0.2 — stored for composite index label filtering
    label: String,
    /// Properties (in order)
    properties: Vec<String>,
    /// Index data
    data: HashMap<Vec<PropertyValueKey>, HashSet<VertexId>>,
}

impl CompositeIndex {
    /// Create a new composite index
    pub fn new(label: &str, properties: Vec<&str>) -> Self {
        Self {
            label: label.to_string(),
            properties: properties.into_iter().map(|s| s.to_string()).collect(),
            data: HashMap::new(),
        }
    }

    /// Add a vertex
    pub fn add(&mut self, id: VertexId, values: Vec<PropertyValue>) {
        if values.len() != self.properties.len() {
            return;
        }

        let key: Vec<PropertyValueKey> = values.iter().map(PropertyValueKey::from).collect();
        self.data.entry(key).or_default().insert(id);
    }

    /// Find by values
    pub fn find(&self, values: Vec<PropertyValue>) -> Vec<VertexId> {
        let key: Vec<PropertyValueKey> = values.iter().map(PropertyValueKey::from).collect();
        self.data
            .get(&key)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Find by prefix
    pub fn find_prefix(&self, values: Vec<PropertyValue>) -> Vec<VertexId> {
        let prefix: Vec<PropertyValueKey> =
            values.iter().map(PropertyValueKey::from).collect();
        let prefix_len = prefix.len();

        let mut result = HashSet::new();
        for (key, vertices) in &self.data {
            if key.len() >= prefix_len && key[..prefix_len] == prefix[..] {
                result.extend(vertices.iter().cloned());
            }
        }

        result.into_iter().collect()
    }
}

impl GraphIndex for CompositeIndex {
    fn index_type(&self) -> &str {
        "composite"
    }

    fn field(&self) -> &str {
        // Return first property
        self.properties.first().map(|s| s.as_str()).unwrap_or("")
    }

    fn size(&self) -> usize {
        self.data.values().map(|s| s.len()).sum()
    }

    fn clear(&mut self) {
        self.data.clear();
    }
}

/// Unique constraint index
pub struct UniqueIndex {
    /// Underlying property index
    inner: PropertyIndex,
}

impl UniqueIndex {
    /// Create a new unique index
    pub fn new(label: &str, property: &str) -> Self {
        Self {
            inner: PropertyIndex::new(label, property),
        }
    }

    /// Try to add a vertex (fails if value already exists)
    pub fn try_add(&mut self, id: VertexId, value: PropertyValue) -> Result<()> {
        let existing = self.inner.find(&value);
        if !existing.is_empty() {
            return Err(GraphError::ConstraintViolation(format!(
                "Unique constraint violation: {} already exists",
                value
            )));
        }

        self.inner.add(id, value);
        Ok(())
    }

    /// Find by value
    pub fn find(&self, value: &PropertyValue) -> Option<VertexId> {
        self.inner.find(value).into_iter().next()
    }

    /// Remove a vertex
    pub fn remove(&mut self, id: VertexId, value: &PropertyValue) {
        self.inner.remove(id, value);
    }
}

impl GraphIndex for UniqueIndex {
    fn index_type(&self) -> &str {
        "unique"
    }

    fn field(&self) -> &str {
        self.inner.field()
    }

    fn size(&self) -> usize {
        self.inner.size()
    }

    fn clear(&mut self) {
        self.inner.clear();
    }
}

/// Edge label index
pub struct EdgeLabelIndex {
    /// Label to edge IDs
    label_to_edges: HashMap<String, HashSet<EdgeId>>,
    /// Edge to label
    edge_to_label: HashMap<EdgeId, String>,
}

impl EdgeLabelIndex {
    /// Create a new edge label index
    pub fn new() -> Self {
        Self {
            label_to_edges: HashMap::new(),
            edge_to_label: HashMap::new(),
        }
    }

    /// Add an edge
    pub fn add(&mut self, id: EdgeId, label: &str) {
        self.label_to_edges
            .entry(label.to_string())
            .or_default()
            .insert(id);
        self.edge_to_label.insert(id, label.to_string());
    }

    /// Remove an edge
    pub fn remove(&mut self, id: EdgeId) {
        if let Some(label) = self.edge_to_label.remove(&id) {
            if let Some(edges) = self.label_to_edges.get_mut(&label) {
                edges.remove(&id);
            }
        }
    }

    /// Get edges by label
    pub fn get_edges(&self, label: &str) -> Vec<EdgeId> {
        self.label_to_edges
            .get(label)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get all labels
    pub fn labels(&self) -> Vec<&str> {
        self.label_to_edges.keys().map(|s| s.as_str()).collect()
    }
}

impl Default for EdgeLabelIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphIndex for EdgeLabelIndex {
    fn index_type(&self) -> &str {
        "edge_label"
    }

    fn field(&self) -> &str {
        "_label"
    }

    fn size(&self) -> usize {
        self.edge_to_label.len()
    }

    fn clear(&mut self) {
        self.label_to_edges.clear();
        self.edge_to_label.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_label_index() {
        let mut index = LabelIndex::new();

        index.add_vertex(VertexId::new(1), "Person");
        index.add_vertex(VertexId::new(2), "Person");
        index.add_vertex(VertexId::new(3), "Company");

        let people = index.get_vertices("Person");
        assert_eq!(people.len(), 2);

        let companies = index.get_vertices("Company");
        assert_eq!(companies.len(), 1);

        assert_eq!(index.get_label(VertexId::new(1)), Some("Person"));
    }

    #[test]
    fn test_label_index_remove() {
        let mut index = LabelIndex::new();

        index.add_vertex(VertexId::new(1), "Person");
        index.add_vertex(VertexId::new(2), "Person");

        index.remove_vertex(VertexId::new(1));

        let people = index.get_vertices("Person");
        assert_eq!(people.len(), 1);
    }

    #[test]
    fn test_property_index_exact() {
        let mut index = PropertyIndex::new("Person", "name");

        index.add(VertexId::new(1), PropertyValue::String("Alice".to_string()));
        index.add(VertexId::new(2), PropertyValue::String("Bob".to_string()));

        let result = index.find(&PropertyValue::String("Alice".to_string()));
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], VertexId::new(1));
    }

    #[test]
    fn test_property_index_range() {
        let mut index = PropertyIndex::new("Person", "age");

        index.add(VertexId::new(1), PropertyValue::Integer(25));
        index.add(VertexId::new(2), PropertyValue::Integer(30));
        index.add(VertexId::new(3), PropertyValue::Integer(35));
        index.add(VertexId::new(4), PropertyValue::Integer(40));

        let result = index.find_range(&PropertyValue::Integer(28), &PropertyValue::Integer(38));

        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_property_index_text() {
        let mut index = PropertyIndex::new("Article", "content");

        index.add(
            VertexId::new(1),
            PropertyValue::String("hello world rust".to_string()),
        );
        index.add(
            VertexId::new(2),
            PropertyValue::String("hello rust programming".to_string()),
        );
        index.add(
            VertexId::new(3),
            PropertyValue::String("python programming".to_string()),
        );

        let result = index.find_text("hello rust");
        assert_eq!(result.len(), 2);

        let result2 = index.find_text("world");
        assert_eq!(result2.len(), 1);
    }

    #[test]
    fn test_composite_index() {
        let mut index = CompositeIndex::new("Person", vec!["first_name", "last_name"]);

        index.add(
            VertexId::new(1),
            vec![
                PropertyValue::String("John".to_string()),
                PropertyValue::String("Doe".to_string()),
            ],
        );
        index.add(
            VertexId::new(2),
            vec![
                PropertyValue::String("John".to_string()),
                PropertyValue::String("Smith".to_string()),
            ],
        );
        index.add(
            VertexId::new(3),
            vec![
                PropertyValue::String("Jane".to_string()),
                PropertyValue::String("Doe".to_string()),
            ],
        );

        // Exact match
        let result = index.find(vec![
            PropertyValue::String("John".to_string()),
            PropertyValue::String("Doe".to_string()),
        ]);
        assert_eq!(result.len(), 1);

        // Prefix match
        let result2 = index.find_prefix(vec![PropertyValue::String("John".to_string())]);
        assert_eq!(result2.len(), 2);
    }

    #[test]
    fn test_unique_index() {
        let mut index = UniqueIndex::new("User", "email");

        index
            .try_add(
                VertexId::new(1),
                PropertyValue::String("alice@example.com".to_string()),
            )
            .unwrap();

        // Should fail - duplicate
        let result = index.try_add(
            VertexId::new(2),
            PropertyValue::String("alice@example.com".to_string()),
        );
        assert!(result.is_err());

        // Should succeed - different value
        index
            .try_add(
                VertexId::new(2),
                PropertyValue::String("bob@example.com".to_string()),
            )
            .unwrap();
    }

    #[test]
    fn test_edge_label_index() {
        let mut index = EdgeLabelIndex::new();

        index.add(EdgeId::new(1), "KNOWS");
        index.add(EdgeId::new(2), "KNOWS");
        index.add(EdgeId::new(3), "FOLLOWS");

        let knows = index.get_edges("KNOWS");
        assert_eq!(knows.len(), 2);

        let follows = index.get_edges("FOLLOWS");
        assert_eq!(follows.len(), 1);
    }

    #[test]
    fn test_graph_index_trait() {
        let index = LabelIndex::new();

        assert_eq!(index.index_type(), "label");
        assert_eq!(index.field(), "_label");
        assert_eq!(index.size(), 0);
    }

    #[test]
    fn test_index_clear() {
        let mut index = LabelIndex::new();

        index.add_vertex(VertexId::new(1), "Person");
        index.add_vertex(VertexId::new(2), "Person");

        index.clear();

        assert_eq!(index.size(), 0);
        assert!(index.get_vertices("Person").is_empty());
    }
}
