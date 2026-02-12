//! Graph aggregation operations
//!
//! Provides degree calculations, neighborhood aggregation, and graph-level statistics.

use super::*;

/// Node degree information
#[derive(Debug, Clone)]
pub struct DegreeInfo {
    /// Vertex ID
    pub vertex: VertexId,
    /// Number of outgoing edges
    pub out_degree: usize,
    /// Number of incoming edges
    pub in_degree: usize,
    /// Total degree (in + out)
    pub total_degree: usize,
}

/// Degree calculations for graph vertices
pub struct Degree;

impl Degree {
    /// Get degree info for a single vertex
    pub fn info(storage: &GraphStorage, vertex: VertexId) -> DegreeInfo {
        let out_degree = storage.out_degree(vertex);
        let in_degree = storage.in_degree(vertex);
        DegreeInfo {
            vertex,
            out_degree,
            in_degree,
            total_degree: out_degree + in_degree,
        }
    }

    /// Get degree info for all vertices
    pub fn all(storage: &GraphStorage) -> Vec<DegreeInfo> {
        storage
            .vertex_ids()
            .into_iter()
            .map(|v| Self::info(storage, v))
            .collect()
    }

    /// Get vertices with highest total degree
    pub fn top_k(storage: &GraphStorage, k: usize) -> Vec<DegreeInfo> {
        let mut degrees = Self::all(storage);
        degrees.sort_by(|a, b| b.total_degree.cmp(&a.total_degree));
        degrees.truncate(k);
        degrees
    }

    /// Average degree across all vertices
    pub fn average(storage: &GraphStorage) -> f64 {
        let n = storage.vertex_count();
        if n == 0 {
            return 0.0;
        }
        // Each edge contributes 1 to out-degree and 1 to in-degree, so total_degree sum = 2 * edges
        (2.0 * storage.edge_count() as f64) / n as f64
    }

    /// Degree distribution: maps degree -> count of vertices with that degree
    pub fn distribution(storage: &GraphStorage) -> HashMap<usize, usize> {
        let mut dist: HashMap<usize, usize> = HashMap::new();
        for v in storage.vertex_ids() {
            let d = storage.out_degree(v) + storage.in_degree(v);
            *dist.entry(d).or_insert(0) += 1;
        }
        dist
    }
}

/// Neighborhood aggregation operations
pub struct NeighborhoodAggregation;

impl NeighborhoodAggregation {
    /// Compute the average of a numeric property across a vertex's outgoing neighbors.
    ///
    /// Returns `None` if the vertex has no outgoing neighbors or none have the property.
    pub fn average_neighbor_property(
        storage: &GraphStorage,
        vertex: VertexId,
        property: &str,
    ) -> Option<f64> {
        let neighbors = storage.out_neighbors(vertex);
        if neighbors.is_empty() {
            return None;
        }

        let mut sum = 0.0;
        let mut count = 0usize;

        for nid in neighbors {
            if let Some(v) = storage.get_vertex_ref(nid) {
                if let Some(val) = v.properties.get(property) {
                    if let Some(f) = val.as_float() {
                        sum += f;
                        count += 1;
                    }
                }
            }
        }

        if count == 0 {
            None
        } else {
            Some(sum / count as f64)
        }
    }

    /// Compute the sum of a numeric property across a vertex's outgoing neighbors.
    pub fn sum_neighbor_property(storage: &GraphStorage, vertex: VertexId, property: &str) -> f64 {
        let mut sum = 0.0;
        for nid in storage.out_neighbors(vertex) {
            if let Some(v) = storage.get_vertex_ref(nid) {
                if let Some(val) = v.properties.get(property) {
                    if let Some(f) = val.as_float() {
                        sum += f;
                    }
                }
            }
        }
        sum
    }

    /// Compute the min of a numeric property across a vertex's outgoing neighbors.
    pub fn min_neighbor_property(
        storage: &GraphStorage,
        vertex: VertexId,
        property: &str,
    ) -> Option<f64> {
        let mut result: Option<f64> = None;
        for nid in storage.out_neighbors(vertex) {
            if let Some(v) = storage.get_vertex_ref(nid) {
                if let Some(val) = v.properties.get(property) {
                    if let Some(f) = val.as_float() {
                        result = Some(result.map_or(f, |r: f64| r.min(f)));
                    }
                }
            }
        }
        result
    }

    /// Compute the max of a numeric property across a vertex's outgoing neighbors.
    pub fn max_neighbor_property(
        storage: &GraphStorage,
        vertex: VertexId,
        property: &str,
    ) -> Option<f64> {
        let mut result: Option<f64> = None;
        for nid in storage.out_neighbors(vertex) {
            if let Some(v) = storage.get_vertex_ref(nid) {
                if let Some(val) = v.properties.get(property) {
                    if let Some(f) = val.as_float() {
                        result = Some(result.map_or(f, |r: f64| r.max(f)));
                    }
                }
            }
        }
        result
    }

    /// Collect distinct values of a property across outgoing neighbors.
    pub fn collect_neighbor_property(
        storage: &GraphStorage,
        vertex: VertexId,
        property: &str,
    ) -> Vec<PropertyValue> {
        let mut values = Vec::new();
        for nid in storage.out_neighbors(vertex) {
            if let Some(v) = storage.get_vertex_ref(nid) {
                if let Some(val) = v.properties.get(property) {
                    if !values.contains(val) {
                        values.push(val.clone());
                    }
                }
            }
        }
        values
    }
}

/// Graph-level statistics
#[derive(Debug, Clone)]
pub struct GraphStats {
    /// Number of vertices
    pub vertex_count: usize,
    /// Number of edges
    pub edge_count: usize,
    /// Graph density (edges / max_possible_edges)
    pub density: f64,
    /// Average degree
    pub average_degree: f64,
    /// Maximum degree
    pub max_degree: usize,
    /// Minimum degree
    pub min_degree: usize,
    /// Number of self-loops
    pub self_loops: usize,
    /// Number of distinct labels
    pub label_count: usize,
    /// Number of distinct edge labels
    pub edge_label_count: usize,
}

/// Compute graph-level statistics
pub struct GraphStatistics;

impl GraphStatistics {
    /// Compute comprehensive statistics for the graph
    pub fn compute(storage: &GraphStorage) -> GraphStats {
        let vertex_count = storage.vertex_count();
        let edge_count = storage.edge_count();

        let max_possible = if vertex_count > 1 {
            vertex_count * (vertex_count - 1)
        } else {
            1
        };
        let density = edge_count as f64 / max_possible as f64;

        let average_degree = if vertex_count > 0 {
            (2.0 * edge_count as f64) / vertex_count as f64
        } else {
            0.0
        };

        let mut max_degree = 0usize;
        let mut min_degree = usize::MAX;

        for v in storage.vertex_ids() {
            let d = storage.out_degree(v) + storage.in_degree(v);
            max_degree = max_degree.max(d);
            min_degree = min_degree.min(d);
        }

        if vertex_count == 0 {
            min_degree = 0;
        }

        let self_loops = storage
            .all_edges()
            .iter()
            .filter(|e| e.is_self_loop())
            .count();

        let mut labels = std::collections::HashSet::new();
        let mut edge_labels = std::collections::HashSet::new();

        for v in storage.all_vertices() {
            for l in &v.labels {
                labels.insert(l.clone());
            }
        }
        for e in storage.all_edges() {
            edge_labels.insert(e.label.clone());
        }

        GraphStats {
            vertex_count,
            edge_count,
            density,
            average_degree,
            max_degree,
            min_degree,
            self_loops,
            label_count: labels.len(),
            edge_label_count: edge_labels.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_graph() -> GraphStorage {
        let mut storage = GraphStorage::new();

        let mut v1 = Vertex::new(VertexId::new(100), "Person");
        v1.set("name", "Alice");
        v1.set("age", 30i64);
        storage.add_vertex(v1).unwrap();

        let mut v2 = Vertex::new(VertexId::new(101), "Person");
        v2.set("name", "Bob");
        v2.set("age", 25i64);
        storage.add_vertex(v2).unwrap();

        let mut v3 = Vertex::new(VertexId::new(102), "Person");
        v3.set("name", "Charlie");
        v3.set("age", 35i64);
        storage.add_vertex(v3).unwrap();

        let mut v4 = Vertex::new(VertexId::new(103), "Company");
        v4.set("name", "Acme");
        storage.add_vertex(v4).unwrap();

        // Alice -> Bob, Alice -> Charlie, Bob -> Charlie
        storage
            .add_edge(Edge::new(
                EdgeId::new(100),
                VertexId::new(100),
                VertexId::new(101),
                "KNOWS",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(101),
                VertexId::new(100),
                VertexId::new(102),
                "KNOWS",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(102),
                VertexId::new(101),
                VertexId::new(102),
                "KNOWS",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(103),
                VertexId::new(100),
                VertexId::new(103),
                "WORKS_AT",
            ))
            .unwrap();

        storage
    }

    #[test]
    fn test_degree_info() {
        let storage = create_test_graph();

        let info = Degree::info(&storage, VertexId::new(100));
        assert_eq!(info.out_degree, 3); // Alice -> Bob, Charlie, Acme
        assert_eq!(info.in_degree, 0);
        assert_eq!(info.total_degree, 3);
    }

    #[test]
    fn test_degree_all() {
        let storage = create_test_graph();
        let all = Degree::all(&storage);
        assert_eq!(all.len(), 4);
    }

    #[test]
    fn test_degree_top_k() {
        let storage = create_test_graph();
        let top = Degree::top_k(&storage, 2);
        assert_eq!(top.len(), 2);
        // Highest degree first
        assert!(top[0].total_degree >= top[1].total_degree);
    }

    #[test]
    fn test_degree_average() {
        let storage = create_test_graph();
        let avg = Degree::average(&storage);
        // 4 edges, 4 vertices => avg = 2*4/4 = 2.0
        assert!((avg - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_degree_distribution() {
        let storage = create_test_graph();
        let dist = Degree::distribution(&storage);
        assert!(!dist.is_empty());
    }

    #[test]
    fn test_neighborhood_avg() {
        let storage = create_test_graph();

        // Alice's outgoing neighbors: Bob(25), Charlie(35), Acme(no age)
        let avg =
            NeighborhoodAggregation::average_neighbor_property(&storage, VertexId::new(100), "age");
        assert!(avg.is_some());
        assert!((avg.unwrap() - 30.0).abs() < f64::EPSILON); // (25+35)/2
    }

    #[test]
    fn test_neighborhood_sum() {
        let storage = create_test_graph();
        let sum =
            NeighborhoodAggregation::sum_neighbor_property(&storage, VertexId::new(100), "age");
        assert!((sum - 60.0).abs() < f64::EPSILON); // 25 + 35
    }

    #[test]
    fn test_neighborhood_min_max() {
        let storage = create_test_graph();

        let min =
            NeighborhoodAggregation::min_neighbor_property(&storage, VertexId::new(100), "age");
        assert_eq!(min, Some(25.0));

        let max =
            NeighborhoodAggregation::max_neighbor_property(&storage, VertexId::new(100), "age");
        assert_eq!(max, Some(35.0));
    }

    #[test]
    fn test_neighborhood_no_neighbors() {
        let storage = create_test_graph();

        // Acme has no outgoing edges
        let avg =
            NeighborhoodAggregation::average_neighbor_property(&storage, VertexId::new(103), "age");
        assert!(avg.is_none());
    }

    #[test]
    fn test_collect_neighbor_property() {
        let storage = create_test_graph();
        let values = NeighborhoodAggregation::collect_neighbor_property(
            &storage,
            VertexId::new(100),
            "name",
        );
        assert_eq!(values.len(), 3); // Bob, Charlie, Acme
    }

    #[test]
    fn test_graph_statistics() {
        let storage = create_test_graph();
        let stats = GraphStatistics::compute(&storage);

        assert_eq!(stats.vertex_count, 4);
        assert_eq!(stats.edge_count, 4);
        assert!(stats.density > 0.0);
        assert!((stats.average_degree - 2.0).abs() < f64::EPSILON);
        assert_eq!(stats.self_loops, 0);
        assert_eq!(stats.label_count, 2); // Person, Company
        assert_eq!(stats.edge_label_count, 2); // KNOWS, WORKS_AT
    }

    #[test]
    fn test_graph_statistics_empty() {
        let storage = GraphStorage::new();
        let stats = GraphStatistics::compute(&storage);

        assert_eq!(stats.vertex_count, 0);
        assert_eq!(stats.edge_count, 0);
        assert_eq!(stats.min_degree, 0);
    }
}
