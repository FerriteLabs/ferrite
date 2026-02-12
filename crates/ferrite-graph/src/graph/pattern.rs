//! Graph pattern matching
//!
//! Provides pattern matching for subgraph queries.

use super::*;
use std::collections::{HashMap, HashSet};

/// A graph pattern to match
#[derive(Debug, Clone)]
pub struct Pattern {
    /// Node patterns
    pub nodes: Vec<NodePattern>,
    /// Edge patterns
    pub edges: Vec<EdgePattern>,
    /// Constraints
    pub constraints: Vec<PatternConstraint>,
    /// Optional edge patterns (may or may not match)
    pub optional_edges: Vec<EdgePattern>,
}

impl Pattern {
    /// Create a new empty pattern
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            constraints: Vec::new(),
            optional_edges: Vec::new(),
        }
    }

    /// Add a node pattern
    pub fn node(mut self, variable: &str, labels: Vec<&str>) -> Self {
        self.nodes.push(NodePattern {
            variable: variable.to_string(),
            labels: labels.into_iter().map(|s| s.to_string()).collect(),
            properties: HashMap::new(),
        });
        self
    }

    /// Add a node pattern with properties
    pub fn node_with_props(
        mut self,
        variable: &str,
        labels: Vec<&str>,
        properties: HashMap<String, PropertyValue>,
    ) -> Self {
        self.nodes.push(NodePattern {
            variable: variable.to_string(),
            labels: labels.into_iter().map(|s| s.to_string()).collect(),
            properties,
        });
        self
    }

    /// Add an edge pattern
    pub fn edge(mut self, from: &str, to: &str, labels: Vec<&str>, direction: Direction) -> Self {
        self.edges.push(EdgePattern {
            variable: None,
            from: from.to_string(),
            to: to.to_string(),
            labels: labels.into_iter().map(|s| s.to_string()).collect(),
            direction,
            properties: HashMap::new(),
            min_hops: 1,
            max_hops: 1,
        });
        self
    }

    /// Add a named edge pattern
    pub fn named_edge(
        mut self,
        variable: &str,
        from: &str,
        to: &str,
        labels: Vec<&str>,
        direction: Direction,
    ) -> Self {
        self.edges.push(EdgePattern {
            variable: Some(variable.to_string()),
            from: from.to_string(),
            to: to.to_string(),
            labels: labels.into_iter().map(|s| s.to_string()).collect(),
            direction,
            properties: HashMap::new(),
            min_hops: 1,
            max_hops: 1,
        });
        self
    }

    /// Add a variable-length path pattern
    pub fn path(
        mut self,
        from: &str,
        to: &str,
        labels: Vec<&str>,
        min_hops: usize,
        max_hops: usize,
    ) -> Self {
        self.edges.push(EdgePattern {
            variable: None,
            from: from.to_string(),
            to: to.to_string(),
            labels: labels.into_iter().map(|s| s.to_string()).collect(),
            direction: Direction::Out,
            properties: HashMap::new(),
            min_hops,
            max_hops,
        });
        self
    }

    /// Add a constraint
    pub fn constraint(mut self, constraint: PatternConstraint) -> Self {
        self.constraints.push(constraint);
        self
    }

    /// Add an optional edge pattern (OPTIONAL MATCH semantics).
    ///
    /// If the pattern does not match for a given binding, the target variable is left unbound
    /// rather than filtering out the binding.
    pub fn optional_edge(
        mut self,
        from: &str,
        to: &str,
        labels: Vec<&str>,
        direction: Direction,
    ) -> Self {
        self.optional_edges.push(EdgePattern {
            variable: None,
            from: from.to_string(),
            to: to.to_string(),
            labels: labels.into_iter().map(|s| s.to_string()).collect(),
            direction,
            properties: HashMap::new(),
            min_hops: 1,
            max_hops: 1,
        });
        self
    }

    /// Add a named optional edge pattern
    pub fn named_optional_edge(
        mut self,
        variable: &str,
        from: &str,
        to: &str,
        labels: Vec<&str>,
        direction: Direction,
    ) -> Self {
        self.optional_edges.push(EdgePattern {
            variable: Some(variable.to_string()),
            from: from.to_string(),
            to: to.to_string(),
            labels: labels.into_iter().map(|s| s.to_string()).collect(),
            direction,
            properties: HashMap::new(),
            min_hops: 1,
            max_hops: 1,
        });
        self
    }

    /// Add property equality constraint
    pub fn where_eq(mut self, variable: &str, property: &str, value: PropertyValue) -> Self {
        self.constraints.push(PatternConstraint::PropertyEqual {
            variable: variable.to_string(),
            property: property.to_string(),
            value,
        });
        self
    }

    /// Get all variables in the pattern
    pub fn variables(&self) -> HashSet<String> {
        let mut vars = HashSet::new();

        for node in &self.nodes {
            vars.insert(node.variable.clone());
        }

        for edge in &self.edges {
            if let Some(ref var) = edge.variable {
                vars.insert(var.clone());
            }
        }

        vars
    }
}

impl Default for Pattern {
    fn default() -> Self {
        Self::new()
    }
}

/// Node pattern
#[derive(Debug, Clone)]
pub struct NodePattern {
    /// Variable name
    pub variable: String,
    /// Required labels
    pub labels: Vec<String>,
    /// Required properties
    pub properties: HashMap<String, PropertyValue>,
}

impl NodePattern {
    /// Check if a vertex matches this pattern
    pub fn matches(&self, vertex: &Vertex) -> bool {
        // Check labels (vertex must have all required labels)
        for label in &self.labels {
            if !vertex.labels.contains(label) {
                return false;
            }
        }

        // Check properties
        for (key, value) in &self.properties {
            if vertex.properties.get(key) != Some(value) {
                return false;
            }
        }

        true
    }
}

/// Edge pattern
#[derive(Debug, Clone)]
pub struct EdgePattern {
    /// Variable name
    pub variable: Option<String>,
    /// From node variable
    pub from: String,
    /// To node variable
    pub to: String,
    /// Required labels
    pub labels: Vec<String>,
    /// Direction
    pub direction: Direction,
    /// Required properties
    pub properties: HashMap<String, PropertyValue>,
    /// Minimum hops (for variable length paths)
    pub min_hops: usize,
    /// Maximum hops (for variable length paths)
    pub max_hops: usize,
}

impl EdgePattern {
    /// Check if an edge matches this pattern
    pub fn matches(&self, edge: &Edge) -> bool {
        // Check labels
        if !self.labels.is_empty() && !self.labels.contains(&edge.label) {
            return false;
        }

        // Check properties
        for (key, value) in &self.properties {
            if edge.properties.get(key) != Some(value) {
                return false;
            }
        }

        true
    }
}

/// Pattern constraint
#[derive(Debug, Clone)]
pub enum PatternConstraint {
    /// Property equals value
    PropertyEqual {
        variable: String,
        property: String,
        value: PropertyValue,
    },
    /// Property comparison
    PropertyCompare {
        variable: String,
        property: String,
        operator: ComparisonOp,
        value: PropertyValue,
    },
    /// Variables must be different
    Different(String, String),
    /// Variables must be the same
    Same(String, String),
    /// Property exists
    HasProperty { variable: String, property: String },
    /// Custom predicate
    Custom(String),
}

/// Comparison operator
#[derive(Debug, Clone, Copy)]
pub enum ComparisonOp {
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
}

/// Path pattern for variable-length paths
#[derive(Debug, Clone)]
pub struct PathPattern {
    /// From node variable
    pub from: String,
    /// To node variable
    pub to: String,
    /// Edge labels to follow
    pub labels: Vec<String>,
    /// Minimum path length
    pub min_length: usize,
    /// Maximum path length
    pub max_length: usize,
    /// Direction
    pub direction: Direction,
}

impl PathPattern {
    /// Create a new path pattern
    pub fn new(from: &str, to: &str) -> Self {
        Self {
            from: from.to_string(),
            to: to.to_string(),
            labels: Vec::new(),
            min_length: 1,
            max_length: 10,
            direction: Direction::Out,
        }
    }

    /// Set labels
    pub fn labels(mut self, labels: Vec<&str>) -> Self {
        self.labels = labels.into_iter().map(|s| s.to_string()).collect();
        self
    }

    /// Set length bounds
    pub fn length(mut self, min: usize, max: usize) -> Self {
        self.min_length = min;
        self.max_length = max;
        self
    }

    /// Set direction
    pub fn direction(mut self, direction: Direction) -> Self {
        self.direction = direction;
        self
    }
}

/// Pattern matcher
pub struct PatternMatcher {
    pattern: Pattern,
}

impl PatternMatcher {
    /// Create a new pattern matcher
    pub fn new(pattern: Pattern) -> Self {
        Self { pattern }
    }

    /// Find all matches
    pub fn find(&self, storage: &GraphStorage) -> Vec<PatternMatch> {
        let mut matches = Vec::new();

        // Start with empty bindings
        let mut bindings: Vec<HashMap<String, MatchedElement>> = vec![HashMap::new()];

        // Match node patterns first
        for node_pattern in &self.pattern.nodes {
            bindings = self.match_node_pattern(storage, &bindings, node_pattern);
        }

        // Match edge patterns
        for edge_pattern in &self.pattern.edges {
            bindings = self.match_edge_pattern(storage, &bindings, edge_pattern);
        }

        // Apply constraints
        bindings.retain(|binding| self.check_constraints(storage, binding));

        // Match optional edge patterns (OPTIONAL MATCH semantics)
        for opt_edge_pattern in &self.pattern.optional_edges {
            bindings = self.match_optional_edge_pattern(storage, &bindings, opt_edge_pattern);
        }

        // Convert to PatternMatch
        for binding in bindings {
            let mut vertices = HashMap::new();
            let mut edges = HashMap::new();

            for (var, element) in binding {
                match element {
                    MatchedElement::Vertex(v) => {
                        vertices.insert(var, v);
                    }
                    MatchedElement::Edge(e) => {
                        edges.insert(var, e);
                    }
                }
            }

            matches.push(PatternMatch { vertices, edges });
        }

        matches
    }

    fn match_node_pattern(
        &self,
        storage: &GraphStorage,
        bindings: &[HashMap<String, MatchedElement>],
        pattern: &NodePattern,
    ) -> Vec<HashMap<String, MatchedElement>> {
        let mut new_bindings = Vec::new();

        for binding in bindings {
            // Check if variable is already bound
            if let Some(MatchedElement::Vertex(existing)) = binding.get(&pattern.variable) {
                if pattern.matches(existing) {
                    new_bindings.push(binding.clone());
                }
                continue;
            }

            // Find matching vertices
            for vertex in storage.all_vertices() {
                if pattern.matches(&vertex) {
                    let mut new_binding = binding.clone();
                    new_binding.insert(pattern.variable.clone(), MatchedElement::Vertex(vertex));
                    new_bindings.push(new_binding);
                }
            }
        }

        new_bindings
    }

    fn match_edge_pattern(
        &self,
        storage: &GraphStorage,
        bindings: &[HashMap<String, MatchedElement>],
        pattern: &EdgePattern,
    ) -> Vec<HashMap<String, MatchedElement>> {
        let mut new_bindings = Vec::new();

        for binding in bindings {
            // Get from vertex
            let from_vertex = match binding.get(&pattern.from) {
                Some(MatchedElement::Vertex(v)) => v,
                _ => continue,
            };

            // Handle variable-length paths
            if pattern.min_hops != 1 || pattern.max_hops != 1 {
                new_bindings.extend(self.match_variable_length_path(
                    storage,
                    binding,
                    pattern,
                    from_vertex,
                ));
                continue;
            }

            // Get edges
            let edge_ids = match pattern.direction {
                Direction::Out => storage.get_out_edges(from_vertex.id),
                Direction::In => storage.get_in_edges(from_vertex.id),
                Direction::Both => storage.get_edges(from_vertex.id),
            };

            for eid in edge_ids {
                if let Some(edge) = storage.get_edge(eid) {
                    if !pattern.matches(&edge) {
                        continue;
                    }

                    // Get target vertex
                    let target_id = match pattern.direction {
                        Direction::Out => edge.to,
                        Direction::In => edge.from,
                        Direction::Both => edge.other(from_vertex.id).unwrap_or(edge.to),
                    };

                    // Check if 'to' is already bound
                    if let Some(MatchedElement::Vertex(existing)) = binding.get(&pattern.to) {
                        if existing.id != target_id {
                            continue;
                        }
                    }

                    if let Some(target) = storage.get_vertex(target_id) {
                        let mut new_binding = binding.clone();

                        // Bind edge if named
                        if let Some(ref var) = pattern.variable {
                            new_binding.insert(var.clone(), MatchedElement::Edge(edge));
                        }

                        // Bind target
                        new_binding.insert(pattern.to.clone(), MatchedElement::Vertex(target));
                        new_bindings.push(new_binding);
                    }
                }
            }
        }

        new_bindings
    }

    fn match_optional_edge_pattern(
        &self,
        storage: &GraphStorage,
        bindings: &[HashMap<String, MatchedElement>],
        pattern: &EdgePattern,
    ) -> Vec<HashMap<String, MatchedElement>> {
        let mut new_bindings = Vec::new();

        for binding in bindings {
            let from_vertex = match binding.get(&pattern.from) {
                Some(MatchedElement::Vertex(v)) => v,
                _ => {
                    // Keep the binding as-is if 'from' isn't bound
                    new_bindings.push(binding.clone());
                    continue;
                }
            };

            let edge_ids = match pattern.direction {
                Direction::Out => storage.get_out_edges(from_vertex.id),
                Direction::In => storage.get_in_edges(from_vertex.id),
                Direction::Both => storage.get_edges(from_vertex.id),
            };

            let mut found = false;
            for eid in edge_ids {
                if let Some(edge) = storage.get_edge(eid) {
                    if !pattern.matches(&edge) {
                        continue;
                    }

                    let target_id = match pattern.direction {
                        Direction::Out => edge.to,
                        Direction::In => edge.from,
                        Direction::Both => edge.other(from_vertex.id).unwrap_or(edge.to),
                    };

                    if let Some(target) = storage.get_vertex(target_id) {
                        let mut new_binding = binding.clone();
                        if let Some(ref var) = pattern.variable {
                            new_binding.insert(var.clone(), MatchedElement::Edge(edge));
                        }
                        new_binding.insert(pattern.to.clone(), MatchedElement::Vertex(target));
                        new_bindings.push(new_binding);
                        found = true;
                    }
                }
            }

            // If no match found, keep original binding (OPTIONAL semantics)
            if !found {
                new_bindings.push(binding.clone());
            }
        }

        new_bindings
    }

    fn match_variable_length_path(
        &self,
        storage: &GraphStorage,
        binding: &HashMap<String, MatchedElement>,
        pattern: &EdgePattern,
        start: &Vertex,
    ) -> Vec<HashMap<String, MatchedElement>> {
        let mut results = Vec::new();
        let mut visited = HashSet::new();
        visited.insert(start.id);

        // BFS with depth tracking
        let mut queue = vec![(start.id, 0)];
        let mut reachable: HashMap<VertexId, usize> = HashMap::new();

        while let Some((current_id, depth)) = queue.pop() {
            if depth >= pattern.max_hops {
                continue;
            }

            let edge_ids = match pattern.direction {
                Direction::Out => storage.get_out_edges(current_id),
                Direction::In => storage.get_in_edges(current_id),
                Direction::Both => storage.get_edges(current_id),
            };

            for eid in edge_ids {
                if let Some(edge) = storage.get_edge_ref(eid) {
                    // Check labels
                    if !pattern.labels.is_empty() && !pattern.labels.contains(&edge.label) {
                        continue;
                    }

                    let next_id = match pattern.direction {
                        Direction::Out => edge.to,
                        Direction::In => edge.from,
                        Direction::Both => edge.other(current_id).unwrap_or(edge.to),
                    };

                    let new_depth = depth + 1;

                    if new_depth >= pattern.min_hops {
                        reachable.insert(next_id, new_depth);
                    }

                    if !visited.contains(&next_id) && new_depth < pattern.max_hops {
                        visited.insert(next_id);
                        queue.push((next_id, new_depth));
                    }
                }
            }
        }

        // Check if 'to' is already bound
        if let Some(MatchedElement::Vertex(existing)) = binding.get(&pattern.to) {
            if reachable.contains_key(&existing.id) {
                results.push(binding.clone());
            }
        } else {
            // Bind all reachable vertices
            for (vid, _) in reachable {
                if let Some(vertex) = storage.get_vertex(vid) {
                    let mut new_binding = binding.clone();
                    new_binding.insert(pattern.to.clone(), MatchedElement::Vertex(vertex));
                    results.push(new_binding);
                }
            }
        }

        results
    }

    fn check_constraints(
        &self,
        _storage: &GraphStorage,
        binding: &HashMap<String, MatchedElement>,
    ) -> bool {
        for constraint in &self.pattern.constraints {
            match constraint {
                PatternConstraint::PropertyEqual {
                    variable,
                    property,
                    value,
                } => {
                    let actual = match binding.get(variable) {
                        Some(MatchedElement::Vertex(v)) => v.properties.get(property),
                        Some(MatchedElement::Edge(e)) => e.properties.get(property),
                        None => return false,
                    };

                    if actual != Some(value) {
                        return false;
                    }
                }
                PatternConstraint::Different(a, b) => {
                    let id_a = match binding.get(a) {
                        Some(MatchedElement::Vertex(v)) => Some(v.id.0),
                        Some(MatchedElement::Edge(e)) => Some(e.id.0),
                        None => None,
                    };
                    let id_b = match binding.get(b) {
                        Some(MatchedElement::Vertex(v)) => Some(v.id.0),
                        Some(MatchedElement::Edge(e)) => Some(e.id.0),
                        None => None,
                    };

                    if id_a.is_some() && id_a == id_b {
                        return false;
                    }
                }
                PatternConstraint::Same(a, b) => {
                    let id_a = match binding.get(a) {
                        Some(MatchedElement::Vertex(v)) => Some(v.id.0),
                        _ => None,
                    };
                    let id_b = match binding.get(b) {
                        Some(MatchedElement::Vertex(v)) => Some(v.id.0),
                        _ => None,
                    };

                    if id_a != id_b {
                        return false;
                    }
                }
                PatternConstraint::HasProperty { variable, property } => {
                    let has = match binding.get(variable) {
                        Some(MatchedElement::Vertex(v)) => v.properties.contains_key(property),
                        Some(MatchedElement::Edge(e)) => e.properties.contains_key(property),
                        None => false,
                    };

                    if !has {
                        return false;
                    }
                }
                PatternConstraint::PropertyCompare {
                    variable,
                    property,
                    operator,
                    value,
                } => {
                    let actual = match binding.get(variable) {
                        Some(MatchedElement::Vertex(v)) => v.properties.get(property),
                        Some(MatchedElement::Edge(e)) => e.properties.get(property),
                        None => return false,
                    };

                    if let Some(actual) = actual {
                        let result = match operator {
                            ComparisonOp::Eq => {
                                compare_values(actual, value) == std::cmp::Ordering::Equal
                            }
                            ComparisonOp::Ne => {
                                compare_values(actual, value) != std::cmp::Ordering::Equal
                            }
                            ComparisonOp::Lt => {
                                compare_values(actual, value) == std::cmp::Ordering::Less
                            }
                            ComparisonOp::Le => {
                                compare_values(actual, value) != std::cmp::Ordering::Greater
                            }
                            ComparisonOp::Gt => {
                                compare_values(actual, value) == std::cmp::Ordering::Greater
                            }
                            ComparisonOp::Ge => {
                                compare_values(actual, value) != std::cmp::Ordering::Less
                            }
                        };
                        if !result {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                PatternConstraint::Custom(_) => {
                    // Custom predicates would be evaluated here
                }
            }
        }

        true
    }
}

/// Matched element
#[derive(Debug, Clone)]
enum MatchedElement {
    Vertex(Vertex),
    Edge(Edge),
}

fn compare_values(a: &PropertyValue, b: &PropertyValue) -> std::cmp::Ordering {
    match (a, b) {
        (PropertyValue::Integer(a), PropertyValue::Integer(b)) => a.cmp(b),
        (PropertyValue::Float(a), PropertyValue::Float(b)) => {
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        }
        (PropertyValue::String(a), PropertyValue::String(b)) => a.cmp(b),
        _ => std::cmp::Ordering::Equal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_storage() -> GraphStorage {
        let mut storage = GraphStorage::new();

        let mut v1 = Vertex::new(VertexId::new(1), "Person");
        v1.set("name", "Alice");
        v1.set("age", 30i64);
        storage.add_vertex(v1).unwrap();

        let mut v2 = Vertex::new(VertexId::new(2), "Person");
        v2.set("name", "Bob");
        v2.set("age", 25i64);
        storage.add_vertex(v2).unwrap();

        let mut v3 = Vertex::new(VertexId::new(3), "Person");
        v3.set("name", "Charlie");
        v3.set("age", 35i64);
        storage.add_vertex(v3).unwrap();

        storage
            .add_edge(Edge::new(
                EdgeId::new(1),
                VertexId::new(1),
                VertexId::new(2),
                "KNOWS",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(2),
                VertexId::new(2),
                VertexId::new(3),
                "KNOWS",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(3),
                VertexId::new(1),
                VertexId::new(3),
                "FOLLOWS",
            ))
            .unwrap();

        storage
    }

    #[test]
    fn test_pattern_node_only() {
        let storage = create_test_storage();

        let pattern = Pattern::new().node("n", vec!["Person"]);

        let matcher = PatternMatcher::new(pattern);
        let matches = matcher.find(&storage);

        assert_eq!(matches.len(), 3);
    }

    #[test]
    fn test_pattern_with_property() {
        let storage = create_test_storage();

        let pattern = Pattern::new().node("n", vec!["Person"]).where_eq(
            "n",
            "name",
            PropertyValue::String("Alice".to_string()),
        );

        let matcher = PatternMatcher::new(pattern);
        let matches = matcher.find(&storage);

        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn test_pattern_with_edge() {
        let storage = create_test_storage();

        let pattern = Pattern::new()
            .node("a", vec!["Person"])
            .node("b", vec!["Person"])
            .edge("a", "b", vec!["KNOWS"], Direction::Out);

        let matcher = PatternMatcher::new(pattern);
        let matches = matcher.find(&storage);

        assert_eq!(matches.len(), 2);
    }

    #[test]
    fn test_pattern_variable_length_path() {
        let storage = create_test_storage();

        let pattern = Pattern::new()
            .node("a", vec!["Person"])
            .node("b", vec!["Person"])
            .path("a", "b", vec!["KNOWS"], 1, 3);

        let matcher = PatternMatcher::new(pattern);
        let matches = matcher.find(&storage);

        // Should find paths of length 1-3
        assert!(!matches.is_empty());
    }

    #[test]
    fn test_pattern_constraint_different() {
        let storage = create_test_storage();

        let pattern = Pattern::new()
            .node("a", vec!["Person"])
            .node("b", vec!["Person"])
            .constraint(PatternConstraint::Different(
                "a".to_string(),
                "b".to_string(),
            ));

        let matcher = PatternMatcher::new(pattern);
        let matches = matcher.find(&storage);

        // All pairs where a != b
        assert_eq!(matches.len(), 6); // 3 * 2 combinations
    }

    #[test]
    fn test_node_pattern_matches() {
        let vertex = Vertex::new(VertexId::new(1), "Person");

        let pattern = NodePattern {
            variable: "n".to_string(),
            labels: vec!["Person".to_string()],
            properties: HashMap::new(),
        };

        assert!(pattern.matches(&vertex));

        let pattern2 = NodePattern {
            variable: "n".to_string(),
            labels: vec!["Company".to_string()],
            properties: HashMap::new(),
        };

        assert!(!pattern2.matches(&vertex));
    }

    #[test]
    fn test_path_pattern_builder() {
        let path = PathPattern::new("a", "b")
            .labels(vec!["KNOWS"])
            .length(1, 5)
            .direction(Direction::Both);

        assert_eq!(path.from, "a");
        assert_eq!(path.to, "b");
        assert_eq!(path.min_length, 1);
        assert_eq!(path.max_length, 5);
    }

    #[test]
    fn test_pattern_variables() {
        let pattern = Pattern::new()
            .node("a", vec![])
            .node("b", vec![])
            .named_edge("r", "a", "b", vec![], Direction::Out);

        let vars = pattern.variables();
        assert!(vars.contains("a"));
        assert!(vars.contains("b"));
        assert!(vars.contains("r"));
    }

    #[test]
    fn test_optional_match_found() {
        let storage = create_test_storage();

        // Alice KNOWS Bob, so optional should bind
        let pattern = Pattern::new()
            .node("a", vec!["Person"])
            .optional_edge("a", "friend", vec!["KNOWS"], Direction::Out)
            .where_eq("a", "name", PropertyValue::String("Alice".to_string()));

        let matcher = PatternMatcher::new(pattern);
        let matches = matcher.find(&storage);

        assert_eq!(matches.len(), 1);
        // The optional edge found a target, so "friend" should be bound
        assert!(matches[0].vertices.contains_key("friend"));
    }

    #[test]
    fn test_optional_match_not_found() {
        let storage = create_test_storage();

        // Charlie has no outgoing KNOWS edges
        let pattern = Pattern::new()
            .node("a", vec!["Person"])
            .optional_edge("a", "friend", vec!["KNOWS"], Direction::Out)
            .where_eq("a", "name", PropertyValue::String("Charlie".to_string()));

        let matcher = PatternMatcher::new(pattern);
        let matches = matcher.find(&storage);

        // Charlie should still appear even though the optional edge doesn't match
        assert_eq!(matches.len(), 1);
        assert!(matches[0].vertices.contains_key("a"));
        // "friend" won't be in vertices since there's no KNOWS edge from Charlie
        assert!(!matches[0].vertices.contains_key("friend"));
    }

    #[test]
    fn test_optional_match_mixed() {
        let storage = create_test_storage();

        // All persons, optionally with a FOLLOWS edge
        let pattern = Pattern::new().node("a", vec!["Person"]).optional_edge(
            "a",
            "target",
            vec!["FOLLOWS"],
            Direction::Out,
        );

        let matcher = PatternMatcher::new(pattern);
        let matches = matcher.find(&storage);

        // Alice->Charlie via FOLLOWS, Bob and Charlie have no outgoing FOLLOWS
        // So we expect: Alice+target, Bob (no target), Charlie (no target) = 3 results
        assert_eq!(matches.len(), 3);
    }
}
