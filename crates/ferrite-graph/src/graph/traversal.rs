//! Graph traversal operations
//!
//! Provides BFS, DFS, and custom traversal operations.

use super::*;
use std::collections::{HashSet, VecDeque};

/// A step in a traversal
#[derive(Debug, Clone)]
pub enum TraversalStep {
    /// Move to outgoing neighbors
    Out(Option<String>),
    /// Move to incoming neighbors
    In(Option<String>),
    /// Move in both directions
    Both(Option<String>),
    /// Filter by vertex predicate
    FilterVertex(VertexFilter),
    /// Filter by edge predicate
    FilterEdge(EdgeFilter),
    /// Limit results
    Limit(usize),
    /// Skip results
    Skip(usize),
    /// Deduplicate by vertex
    Dedup,
    /// Get properties
    Values(Vec<String>),
    /// Count
    Count,
    /// Aggregate
    Aggregate(String),
    /// Order by property
    OrderBy(String, bool),
    /// Range (like pagination)
    Range(usize, usize),
}

/// Traversal result
#[derive(Debug, Clone)]
pub struct TraversalResult {
    /// Vertices in result
    pub vertices: Vec<Vertex>,
    /// Edges in result
    pub edges: Vec<Edge>,
    /// Paths found
    pub paths: Vec<Vec<VertexId>>,
    /// Property values
    pub values: Vec<PropertyValue>,
    /// Count (if count step was used)
    pub count: Option<usize>,
}

impl TraversalResult {
    /// Create empty result
    pub fn new() -> Self {
        Self {
            vertices: Vec::new(),
            edges: Vec::new(),
            paths: Vec::new(),
            values: Vec::new(),
            count: None,
        }
    }

    /// Get vertex count
    pub fn len(&self) -> usize {
        self.vertices.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.vertices.is_empty()
    }
}

impl Default for TraversalResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Graph traverser for fluent traversal API
pub struct Traverser {
    /// Starting vertices
    starts: Vec<VertexId>,
    /// Traversal steps
    steps: Vec<TraversalStep>,
    /// Storage reference
    storage: Arc<RwLock<GraphStorage>>,
}

impl Traverser {
    /// Create a new traverser
    pub fn new(start: VertexId, storage: Arc<RwLock<GraphStorage>>) -> Self {
        Self {
            starts: vec![start],
            steps: Vec::new(),
            storage,
        }
    }

    /// Create with multiple starting vertices
    pub fn from_vertices(starts: Vec<VertexId>, storage: Arc<RwLock<GraphStorage>>) -> Self {
        Self {
            starts,
            steps: Vec::new(),
            storage,
        }
    }

    /// Move to outgoing vertices
    pub fn out(mut self, label: Option<&str>) -> Self {
        self.steps
            .push(TraversalStep::Out(label.map(|s| s.to_string())));
        self
    }

    /// Move to outgoing vertices with specific label
    pub fn out_e(self, label: &str) -> Self {
        self.out(Some(label))
    }

    /// Move to incoming vertices
    pub fn in_(mut self, label: Option<&str>) -> Self {
        self.steps
            .push(TraversalStep::In(label.map(|s| s.to_string())));
        self
    }

    /// Move to incoming vertices with specific label
    pub fn in_e(self, label: &str) -> Self {
        self.in_(Some(label))
    }

    /// Move in both directions
    pub fn both(mut self, label: Option<&str>) -> Self {
        self.steps
            .push(TraversalStep::Both(label.map(|s| s.to_string())));
        self
    }

    /// Move in both directions with specific label
    pub fn both_e(self, label: &str) -> Self {
        self.both(Some(label))
    }

    /// Filter by vertex predicate
    pub fn filter_vertex(mut self, filter: VertexFilter) -> Self {
        self.steps.push(TraversalStep::FilterVertex(filter));
        self
    }

    /// Filter by label
    pub fn has_label(self, label: &str) -> Self {
        self.filter_vertex(VertexFilter::label(label))
    }

    /// Filter by property
    pub fn has(self, key: &str, value: impl Into<PropertyValue>) -> Self {
        self.filter_vertex(VertexFilter::property(key, value))
    }

    /// Filter by edge predicate
    pub fn filter_edge(mut self, filter: EdgeFilter) -> Self {
        self.steps.push(TraversalStep::FilterEdge(filter));
        self
    }

    /// Limit results
    pub fn limit(mut self, n: usize) -> Self {
        self.steps.push(TraversalStep::Limit(n));
        self
    }

    /// Skip results
    pub fn skip(mut self, n: usize) -> Self {
        self.steps.push(TraversalStep::Skip(n));
        self
    }

    /// Deduplicate
    pub fn dedup(mut self) -> Self {
        self.steps.push(TraversalStep::Dedup);
        self
    }

    /// Get property values
    pub fn values(mut self, keys: Vec<&str>) -> Self {
        self.steps.push(TraversalStep::Values(
            keys.into_iter().map(|s| s.to_string()).collect(),
        ));
        self
    }

    /// Get a single property value
    pub fn value(self, key: &str) -> Self {
        self.values(vec![key])
    }

    /// Count results
    pub fn count(mut self) -> Self {
        self.steps.push(TraversalStep::Count);
        self
    }

    /// Order by property
    pub fn order_by(mut self, key: &str, ascending: bool) -> Self {
        self.steps
            .push(TraversalStep::OrderBy(key.to_string(), ascending));
        self
    }

    /// Range (pagination)
    pub fn range(mut self, start: usize, end: usize) -> Self {
        self.steps.push(TraversalStep::Range(start, end));
        self
    }

    /// Execute the traversal
    pub fn execute(self) -> TraversalResult {
        let storage = self.storage.read();
        let mut current_vertices: Vec<VertexId> = self.starts.clone();
        let mut seen: HashSet<VertexId> = HashSet::new();
        let mut result = TraversalResult::new();
        let mut edge_filter: Option<EdgeFilter> = None;

        for step in &self.steps {
            match step {
                TraversalStep::Out(label) => {
                    current_vertices = self.expand_out(
                        &storage,
                        &current_vertices,
                        label.as_deref(),
                        &edge_filter,
                    );
                    edge_filter = None;
                }
                TraversalStep::In(label) => {
                    current_vertices =
                        self.expand_in(&storage, &current_vertices, label.as_deref(), &edge_filter);
                    edge_filter = None;
                }
                TraversalStep::Both(label) => {
                    current_vertices = self.expand_both(
                        &storage,
                        &current_vertices,
                        label.as_deref(),
                        &edge_filter,
                    );
                    edge_filter = None;
                }
                TraversalStep::FilterVertex(filter) => {
                    current_vertices.retain(|&vid| {
                        storage
                            .get_vertex(vid)
                            .map(|v| filter.matches(&v))
                            .unwrap_or(false)
                    });
                }
                TraversalStep::FilterEdge(filter) => {
                    edge_filter = Some(filter.clone());
                }
                TraversalStep::Limit(n) => {
                    current_vertices.truncate(*n);
                }
                TraversalStep::Skip(n) => {
                    if *n < current_vertices.len() {
                        current_vertices = current_vertices[*n..].to_vec();
                    } else {
                        current_vertices.clear();
                    }
                }
                TraversalStep::Dedup => {
                    let mut deduped = Vec::new();
                    for vid in current_vertices {
                        if seen.insert(vid) {
                            deduped.push(vid);
                        }
                    }
                    current_vertices = deduped;
                }
                TraversalStep::Values(keys) => {
                    for vid in &current_vertices {
                        if let Some(vertex) = storage.get_vertex(*vid) {
                            for key in keys {
                                if let Some(value) = vertex.properties.get(key) {
                                    result.values.push(value.clone());
                                }
                            }
                        }
                    }
                }
                TraversalStep::Count => {
                    result.count = Some(current_vertices.len());
                }
                TraversalStep::OrderBy(key, ascending) => {
                    current_vertices.sort_by(|a, b| {
                        let va = storage
                            .get_vertex(*a)
                            .and_then(|v| v.properties.get(key).cloned());
                        let vb = storage
                            .get_vertex(*b)
                            .and_then(|v| v.properties.get(key).cloned());

                        let cmp = compare_property_values(&va, &vb);
                        if *ascending {
                            cmp
                        } else {
                            cmp.reverse()
                        }
                    });
                }
                TraversalStep::Range(start, end) => {
                    let s = (*start).min(current_vertices.len());
                    let e = (*end).min(current_vertices.len());
                    current_vertices = current_vertices[s..e].to_vec();
                }
                TraversalStep::Aggregate(_) => {
                    // Aggregation would be implemented here
                }
            }
        }

        // Collect final vertices
        for vid in current_vertices {
            if let Some(vertex) = storage.get_vertex(vid) {
                result.vertices.push(vertex);
            }
        }

        result
    }

    /// Collect results as vertices
    pub fn collect(self) -> Vec<Vertex> {
        self.execute().vertices
    }

    /// Collect as vertex IDs
    pub fn collect_ids(self) -> Vec<VertexId> {
        self.execute().vertices.into_iter().map(|v| v.id).collect()
    }

    /// Get first result
    pub fn first(self) -> Option<Vertex> {
        self.limit(1).execute().vertices.into_iter().next()
    }

    /// Get count
    pub fn to_count(self) -> usize {
        self.count().execute().count.unwrap_or(0)
    }

    // Helper methods

    fn expand_out(
        &self,
        storage: &GraphStorage,
        vertices: &[VertexId],
        label: Option<&str>,
        edge_filter: &Option<EdgeFilter>,
    ) -> Vec<VertexId> {
        let mut result = Vec::new();

        for &vid in vertices {
            for eid in storage.get_out_edges(vid) {
                if let Some(edge) = storage.get_edge_ref(eid) {
                    // Check label
                    if let Some(l) = label {
                        if edge.label != l {
                            continue;
                        }
                    }

                    // Check edge filter
                    if let Some(filter) = edge_filter {
                        if !filter.matches(edge, Some(Direction::Out)) {
                            continue;
                        }
                    }

                    result.push(edge.to);
                }
            }
        }

        result
    }

    fn expand_in(
        &self,
        storage: &GraphStorage,
        vertices: &[VertexId],
        label: Option<&str>,
        edge_filter: &Option<EdgeFilter>,
    ) -> Vec<VertexId> {
        let mut result = Vec::new();

        for &vid in vertices {
            for eid in storage.get_in_edges(vid) {
                if let Some(edge) = storage.get_edge_ref(eid) {
                    // Check label
                    if let Some(l) = label {
                        if edge.label != l {
                            continue;
                        }
                    }

                    // Check edge filter
                    if let Some(filter) = edge_filter {
                        if !filter.matches(edge, Some(Direction::In)) {
                            continue;
                        }
                    }

                    result.push(edge.from);
                }
            }
        }

        result
    }

    fn expand_both(
        &self,
        storage: &GraphStorage,
        vertices: &[VertexId],
        label: Option<&str>,
        edge_filter: &Option<EdgeFilter>,
    ) -> Vec<VertexId> {
        let mut result = self.expand_out(storage, vertices, label, edge_filter);
        result.extend(self.expand_in(storage, vertices, label, edge_filter));
        result
    }
}

/// Compare property values for ordering
fn compare_property_values(
    a: &Option<PropertyValue>,
    b: &Option<PropertyValue>,
) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(a), Some(b)) => match (a, b) {
            (PropertyValue::Integer(a), PropertyValue::Integer(b)) => a.cmp(b),
            (PropertyValue::Float(a), PropertyValue::Float(b)) => {
                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
            }
            (PropertyValue::String(a), PropertyValue::String(b)) => a.cmp(b),
            (PropertyValue::Boolean(a), PropertyValue::Boolean(b)) => a.cmp(b),
            _ => std::cmp::Ordering::Equal,
        },
    }
}

/// BFS traversal
pub struct BfsTraversal<'a> {
    storage: &'a GraphStorage,
    queue: VecDeque<(VertexId, usize)>,
    visited: HashSet<VertexId>,
    max_depth: Option<usize>,
}

impl<'a> BfsTraversal<'a> {
    /// Create new BFS traversal
    pub fn new(storage: &'a GraphStorage, start: VertexId) -> Self {
        let mut queue = VecDeque::new();
        queue.push_back((start, 0));

        let mut visited = HashSet::new();
        visited.insert(start);

        Self {
            storage,
            queue,
            visited,
            max_depth: None,
        }
    }

    /// Set maximum depth
    pub fn max_depth(mut self, depth: usize) -> Self {
        self.max_depth = Some(depth);
        self
    }
}

impl Iterator for BfsTraversal<'_> {
    type Item = (VertexId, usize);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((vid, depth)) = self.queue.pop_front() {
            // Check max depth
            if self.max_depth.map_or(true, |max| depth < max) {
                // Add neighbors
                for neighbor in self.storage.out_neighbors(vid) {
                    if self.visited.insert(neighbor) {
                        self.queue.push_back((neighbor, depth + 1));
                    }
                }
            }

            return Some((vid, depth));
        }

        None
    }
}

/// DFS traversal
pub struct DfsTraversal<'a> {
    storage: &'a GraphStorage,
    stack: Vec<(VertexId, usize)>,
    visited: HashSet<VertexId>,
    max_depth: Option<usize>,
}

impl<'a> DfsTraversal<'a> {
    /// Create new DFS traversal
    pub fn new(storage: &'a GraphStorage, start: VertexId) -> Self {
        let stack = vec![(start, 0)];

        Self {
            storage,
            stack,
            visited: HashSet::new(),
            max_depth: None,
        }
    }

    /// Set maximum depth
    pub fn max_depth(mut self, depth: usize) -> Self {
        self.max_depth = Some(depth);
        self
    }
}

impl Iterator for DfsTraversal<'_> {
    type Item = (VertexId, usize);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((vid, depth)) = self.stack.pop() {
            if !self.visited.insert(vid) {
                continue;
            }

            // Check max depth
            if let Some(max) = self.max_depth {
                if depth >= max {
                    return Some((vid, depth));
                }
            }

            // Add neighbors
            for neighbor in self.storage.out_neighbors(vid) {
                if !self.visited.contains(&neighbor) {
                    self.stack.push((neighbor, depth + 1));
                }
            }

            return Some((vid, depth));
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_graph() -> Arc<RwLock<GraphStorage>> {
        let mut storage = GraphStorage::new();

        // Create a simple graph: 1 -> 2 -> 3, 1 -> 3
        for i in 1..=4 {
            let mut v = Vertex::new(VertexId::new(i), "Person");
            v.set("name", format!("Person{}", i));
            v.set("age", (i * 10) as i64);
            storage.add_vertex(v).unwrap();
        }

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
            .add_edge(Edge::new(
                EdgeId::new(4),
                VertexId::new(3),
                VertexId::new(4),
                "KNOWS",
            ))
            .unwrap();

        Arc::new(RwLock::new(storage))
    }

    #[test]
    fn test_traverser_out() {
        let storage = create_test_graph();
        let traverser = Traverser::new(VertexId::new(1), storage);

        let result = traverser.out(None).collect();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_traverser_out_with_label() {
        let storage = create_test_graph();
        let traverser = Traverser::new(VertexId::new(1), storage);

        let result = traverser.out(Some("KNOWS")).collect();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_traverser_in() {
        let storage = create_test_graph();
        let traverser = Traverser::new(VertexId::new(3), storage);

        let result = traverser.in_(None).collect();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_traverser_chain() {
        let storage = create_test_graph();
        let traverser = Traverser::new(VertexId::new(1), storage);

        let result = traverser.out(Some("KNOWS")).out(Some("KNOWS")).collect();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, VertexId::new(3));
    }

    #[test]
    fn test_traverser_filter() {
        let storage = create_test_graph();
        let traverser = Traverser::new(VertexId::new(1), storage);

        let result = traverser.out(None).has("age", 30i64).collect();

        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_traverser_limit() {
        let storage = create_test_graph();
        let traverser = Traverser::new(VertexId::new(1), storage);

        let result = traverser.out(None).limit(1).collect();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_traverser_dedup() {
        let storage = create_test_graph();
        let traverser = Traverser::new(VertexId::new(1), storage);

        let result = traverser.out(None).both(None).dedup().collect();
        // Should have unique vertices only
        let ids: HashSet<_> = result.iter().map(|v| v.id).collect();
        assert_eq!(result.len(), ids.len());
    }

    #[test]
    fn test_traverser_count() {
        let storage = create_test_graph();
        let traverser = Traverser::new(VertexId::new(1), storage);

        let count = traverser.out(None).to_count();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_traverser_values() {
        let storage = create_test_graph();
        let traverser = Traverser::new(VertexId::new(1), storage);

        let result = traverser.out(None).values(vec!["name"]).execute();
        assert_eq!(result.values.len(), 2);
    }

    #[test]
    fn test_traverser_first() {
        let storage = create_test_graph();
        let traverser = Traverser::new(VertexId::new(1), storage);

        let first = traverser.out(None).first();
        assert!(first.is_some());
    }

    #[test]
    fn test_bfs_traversal() {
        let storage = create_test_graph();
        let storage_guard = storage.read();

        let bfs = BfsTraversal::new(&storage_guard, VertexId::new(1));
        let visited: Vec<_> = bfs.collect();

        assert_eq!(visited.len(), 4);
        assert_eq!(visited[0].0, VertexId::new(1));
        assert_eq!(visited[0].1, 0); // depth 0
    }

    #[test]
    fn test_bfs_max_depth() {
        let storage = create_test_graph();
        let storage_guard = storage.read();

        let bfs = BfsTraversal::new(&storage_guard, VertexId::new(1)).max_depth(1);
        let visited: Vec<_> = bfs.collect();

        // Should visit only 1 and its immediate neighbors
        assert!(visited.len() <= 3);
    }

    #[test]
    fn test_dfs_traversal() {
        let storage = create_test_graph();
        let storage_guard = storage.read();

        let dfs = DfsTraversal::new(&storage_guard, VertexId::new(1));
        let visited: Vec<_> = dfs.collect();

        assert_eq!(visited.len(), 4);
        assert_eq!(visited[0].0, VertexId::new(1));
    }

    #[test]
    fn test_traversal_result() {
        let result = TraversalResult::new();
        assert!(result.is_empty());
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_order_by() {
        let storage = create_test_graph();
        let traverser = Traverser::new(VertexId::new(1), storage);

        let result = traverser
            .out(None)
            .out(None)
            .order_by("age", false)
            .collect();

        if result.len() >= 2 {
            let ages: Vec<i64> = result.iter().filter_map(|v| v.get_integer("age")).collect();
            // Should be descending
            for i in 0..ages.len() - 1 {
                assert!(ages[i] >= ages[i + 1]);
            }
        }
    }
}
