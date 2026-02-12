//! Graph storage implementation
//!
//! Provides efficient storage for vertices and edges using adjacency lists.

use super::*;
use std::collections::{HashMap, HashSet};

/// Graph storage using adjacency lists
pub struct GraphStorage {
    /// Vertices by ID
    vertices: HashMap<VertexId, Vertex>,
    /// Edges by ID
    edges: HashMap<EdgeId, Edge>,
    /// Outgoing edges by vertex
    out_edges: HashMap<VertexId, HashSet<EdgeId>>,
    /// Incoming edges by vertex
    in_edges: HashMap<VertexId, HashSet<EdgeId>>,
}

impl GraphStorage {
    /// Create new storage
    pub fn new() -> Self {
        Self {
            vertices: HashMap::new(),
            edges: HashMap::new(),
            out_edges: HashMap::new(),
            in_edges: HashMap::new(),
        }
    }

    /// Add a vertex
    pub fn add_vertex(&mut self, vertex: Vertex) -> Result<()> {
        let id = vertex.id;
        self.vertices.insert(id, vertex);
        self.out_edges.entry(id).or_default();
        self.in_edges.entry(id).or_default();
        Ok(())
    }

    /// Get a vertex
    pub fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        self.vertices.get(&id).cloned()
    }

    /// Get a vertex reference
    pub fn get_vertex_ref(&self, id: VertexId) -> Option<&Vertex> {
        self.vertices.get(&id)
    }

    /// Get a mutable vertex reference
    pub fn get_vertex_mut(&mut self, id: VertexId) -> Option<&mut Vertex> {
        self.vertices.get_mut(&id)
    }

    /// Remove a vertex
    pub fn remove_vertex(&mut self, id: VertexId) -> Result<()> {
        self.vertices
            .remove(&id)
            .ok_or(GraphError::VertexNotFound(id))?;
        self.out_edges.remove(&id);
        self.in_edges.remove(&id);
        Ok(())
    }

    /// Add an edge
    pub fn add_edge(&mut self, edge: Edge) -> Result<()> {
        let from = edge.from;
        let to = edge.to;
        let id = edge.id;

        // Verify vertices exist
        if !self.vertices.contains_key(&from) {
            return Err(GraphError::VertexNotFound(from));
        }
        if !self.vertices.contains_key(&to) {
            return Err(GraphError::VertexNotFound(to));
        }

        self.edges.insert(id, edge);
        self.out_edges
            .entry(from)
            .or_default()
            .insert(id);
        self.in_edges
            .entry(to)
            .or_default()
            .insert(id);

        Ok(())
    }

    /// Get an edge
    pub fn get_edge(&self, id: EdgeId) -> Option<Edge> {
        self.edges.get(&id).cloned()
    }

    /// Get an edge reference
    pub fn get_edge_ref(&self, id: EdgeId) -> Option<&Edge> {
        self.edges.get(&id)
    }

    /// Get a mutable edge reference
    pub fn get_edge_mut(&mut self, id: EdgeId) -> Option<&mut Edge> {
        self.edges.get_mut(&id)
    }

    /// Remove an edge
    pub fn remove_edge(&mut self, id: EdgeId) -> Result<()> {
        let edge = self
            .edges
            .remove(&id)
            .ok_or(GraphError::EdgeNotFound(id))?;

        if let Some(out) = self.out_edges.get_mut(&edge.from) {
            out.remove(&id);
        }
        if let Some(in_) = self.in_edges.get_mut(&edge.to) {
            in_.remove(&id);
        }

        Ok(())
    }

    /// Get all edges connected to a vertex
    pub fn get_edges(&self, vertex: VertexId) -> Vec<EdgeId> {
        let mut edges = Vec::new();

        if let Some(out) = self.out_edges.get(&vertex) {
            edges.extend(out.iter().cloned());
        }
        if let Some(in_) = self.in_edges.get(&vertex) {
            edges.extend(in_.iter().cloned());
        }

        edges.sort();
        edges.dedup();
        edges
    }

    /// Get outgoing edges from a vertex
    pub fn get_out_edges(&self, vertex: VertexId) -> Vec<EdgeId> {
        self.out_edges
            .get(&vertex)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get incoming edges to a vertex
    pub fn get_in_edges(&self, vertex: VertexId) -> Vec<EdgeId> {
        self.in_edges
            .get(&vertex)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get outgoing neighbors
    pub fn out_neighbors(&self, vertex: VertexId) -> Vec<VertexId> {
        self.get_out_edges(vertex)
            .into_iter()
            .filter_map(|eid| self.edges.get(&eid).map(|e| e.to))
            .collect()
    }

    /// Get incoming neighbors
    pub fn in_neighbors(&self, vertex: VertexId) -> Vec<VertexId> {
        self.get_in_edges(vertex)
            .into_iter()
            .filter_map(|eid| self.edges.get(&eid).map(|e| e.from))
            .collect()
    }

    /// Get all neighbors
    pub fn neighbors(&self, vertex: VertexId) -> Vec<VertexId> {
        let mut neighbors = HashSet::new();

        for eid in self.get_out_edges(vertex) {
            if let Some(edge) = self.edges.get(&eid) {
                neighbors.insert(edge.to);
            }
        }
        for eid in self.get_in_edges(vertex) {
            if let Some(edge) = self.edges.get(&eid) {
                neighbors.insert(edge.from);
            }
        }

        neighbors.into_iter().collect()
    }

    /// Get edges between two vertices
    pub fn edges_between(&self, from: VertexId, to: VertexId) -> Vec<EdgeId> {
        self.get_out_edges(from)
            .into_iter()
            .filter(|eid| self.edges.get(eid).map(|e| e.to == to).unwrap_or(false))
            .collect()
    }

    /// Get all vertices
    pub fn all_vertices(&self) -> Vec<Vertex> {
        self.vertices.values().cloned().collect()
    }

    /// Get all vertex IDs
    pub fn vertex_ids(&self) -> Vec<VertexId> {
        self.vertices.keys().cloned().collect()
    }

    /// Get all edges
    pub fn all_edges(&self) -> Vec<Edge> {
        self.edges.values().cloned().collect()
    }

    /// Get all edge IDs
    pub fn edge_ids(&self) -> Vec<EdgeId> {
        self.edges.keys().cloned().collect()
    }

    /// Get vertex count
    pub fn vertex_count(&self) -> usize {
        self.vertices.len()
    }

    /// Get edge count
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// Check if vertex exists
    pub fn has_vertex(&self, id: VertexId) -> bool {
        self.vertices.contains_key(&id)
    }

    /// Check if edge exists
    pub fn has_edge(&self, id: EdgeId) -> bool {
        self.edges.contains_key(&id)
    }

    /// Get degree (total edges) of a vertex
    pub fn degree(&self, vertex: VertexId) -> usize {
        self.out_degree(vertex) + self.in_degree(vertex)
    }

    /// Get out-degree of a vertex
    pub fn out_degree(&self, vertex: VertexId) -> usize {
        self.out_edges.get(&vertex).map(|s| s.len()).unwrap_or(0)
    }

    /// Get in-degree of a vertex
    pub fn in_degree(&self, vertex: VertexId) -> usize {
        self.in_edges.get(&vertex).map(|s| s.len()).unwrap_or(0)
    }

    /// Clear the storage
    pub fn clear(&mut self) {
        self.vertices.clear();
        self.edges.clear();
        self.out_edges.clear();
        self.in_edges.clear();
    }

    /// Get vertices by filter
    pub fn filter_vertices<F>(&self, predicate: F) -> Vec<Vertex>
    where
        F: Fn(&Vertex) -> bool,
    {
        self.vertices
            .values()
            .filter(|v| predicate(v))
            .cloned()
            .collect()
    }

    /// Get edges by filter
    pub fn filter_edges<F>(&self, predicate: F) -> Vec<Edge>
    where
        F: Fn(&Edge) -> bool,
    {
        self.edges
            .values()
            .filter(|e| predicate(e))
            .cloned()
            .collect()
    }
}

impl Default for GraphStorage {
    fn default() -> Self {
        Self::new()
    }
}

/// Adjacency list representation
#[derive(Debug, Clone)]
pub struct AdjacencyList {
    /// Adjacency list (vertex -> list of (neighbor, edge_id, weight))
    list: HashMap<VertexId, Vec<(VertexId, EdgeId, f64)>>,
}

impl AdjacencyList {
    /// Create from storage
    pub fn from_storage(storage: &GraphStorage, directed: bool) -> Self {
        let mut list: HashMap<VertexId, Vec<(VertexId, EdgeId, f64)>> = HashMap::new();

        for vertex_id in storage.vertex_ids() {
            list.entry(vertex_id).or_default();
        }

        for edge in storage.all_edges() {
            list.entry(edge.from)
                .or_default()
                .push((edge.to, edge.id, edge.weight));

            if !directed {
                list.entry(edge.to).or_default().push((
                    edge.from,
                    edge.id,
                    edge.weight,
                ));
            }
        }

        Self { list }
    }

    /// Get neighbors of a vertex
    pub fn neighbors(&self, vertex: VertexId) -> &[(VertexId, EdgeId, f64)] {
        self.list.get(&vertex).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Get vertex count
    pub fn vertex_count(&self) -> usize {
        self.list.len()
    }

    /// Get all vertices
    pub fn vertices(&self) -> Vec<VertexId> {
        self.list.keys().cloned().collect()
    }
}

/// Compressed sparse row (CSR) format for efficient traversal
#[derive(Debug, Clone)]
pub struct CompressedGraph {
    /// Vertex IDs in order
    vertices: Vec<VertexId>,
    /// Vertex ID to index
    vertex_index: HashMap<VertexId, usize>,
    /// Row pointers (offsets into edges)
    row_ptr: Vec<usize>,
    /// Column indices (neighbor indices)
    col_idx: Vec<usize>,
    /// Edge weights
    weights: Vec<f64>,
    /// Edge IDs
    edge_ids: Vec<EdgeId>,
}

impl CompressedGraph {
    /// Create from storage
    pub fn from_storage(storage: &GraphStorage) -> Self {
        let vertices: Vec<VertexId> = storage.vertex_ids();
        let vertex_index: HashMap<VertexId, usize> =
            vertices.iter().enumerate().map(|(i, &v)| (v, i)).collect();

        let mut row_ptr = vec![0];
        let mut col_idx = Vec::new();
        let mut weights = Vec::new();
        let mut edge_ids = Vec::new();

        for &vertex in &vertices {
            let out_edges = storage.get_out_edges(vertex);
            for eid in out_edges {
                if let Some(edge) = storage.get_edge_ref(eid) {
                    if let Some(&idx) = vertex_index.get(&edge.to) {
                        col_idx.push(idx);
                        weights.push(edge.weight);
                        edge_ids.push(eid);
                    }
                }
            }
            row_ptr.push(col_idx.len());
        }

        Self {
            vertices,
            vertex_index,
            row_ptr,
            col_idx,
            weights,
            edge_ids,
        }
    }

    /// Get vertex index
    pub fn vertex_to_index(&self, vertex: VertexId) -> Option<usize> {
        self.vertex_index.get(&vertex).copied()
    }

    /// Get vertex from index
    pub fn index_to_vertex(&self, index: usize) -> Option<VertexId> {
        self.vertices.get(index).copied()
    }

    /// Get neighbors by index
    pub fn neighbors(&self, index: usize) -> &[usize] {
        if index >= self.row_ptr.len() - 1 {
            return &[];
        }
        let start = self.row_ptr[index];
        let end = self.row_ptr[index + 1];
        &self.col_idx[start..end]
    }

    /// Get neighbor weights by index
    pub fn neighbor_weights(&self, index: usize) -> &[f64] {
        if index >= self.row_ptr.len() - 1 {
            return &[];
        }
        let start = self.row_ptr[index];
        let end = self.row_ptr[index + 1];
        &self.weights[start..end]
    }

    /// Get vertex count
    pub fn vertex_count(&self) -> usize {
        self.vertices.len()
    }

    /// Get edge count
    pub fn edge_count(&self) -> usize {
        self.col_idx.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_storage() -> GraphStorage {
        let mut storage = GraphStorage::new();

        let v1 = Vertex::new(VertexId::new(1), "Person");
        let v2 = Vertex::new(VertexId::new(2), "Person");
        let v3 = Vertex::new(VertexId::new(3), "Person");

        storage.add_vertex(v1).unwrap();
        storage.add_vertex(v2).unwrap();
        storage.add_vertex(v3).unwrap();

        let e1 = Edge::new(EdgeId::new(1), VertexId::new(1), VertexId::new(2), "KNOWS");
        let e2 = Edge::new(EdgeId::new(2), VertexId::new(2), VertexId::new(3), "KNOWS");
        let e3 = Edge::new(
            EdgeId::new(3),
            VertexId::new(1),
            VertexId::new(3),
            "FOLLOWS",
        );

        storage.add_edge(e1).unwrap();
        storage.add_edge(e2).unwrap();
        storage.add_edge(e3).unwrap();

        storage
    }

    #[test]
    fn test_add_vertex() {
        let mut storage = GraphStorage::new();
        let v = Vertex::new(VertexId::new(1), "Person");
        storage.add_vertex(v).unwrap();

        assert_eq!(storage.vertex_count(), 1);
        assert!(storage.has_vertex(VertexId::new(1)));
    }

    #[test]
    fn test_add_edge() {
        let mut storage = GraphStorage::new();

        storage
            .add_vertex(Vertex::new(VertexId::new(1), "Person"))
            .unwrap();
        storage
            .add_vertex(Vertex::new(VertexId::new(2), "Person"))
            .unwrap();

        let e = Edge::new(EdgeId::new(1), VertexId::new(1), VertexId::new(2), "KNOWS");
        storage.add_edge(e).unwrap();

        assert_eq!(storage.edge_count(), 1);
    }

    #[test]
    fn test_edge_requires_vertices() {
        let mut storage = GraphStorage::new();

        let e = Edge::new(EdgeId::new(1), VertexId::new(1), VertexId::new(2), "KNOWS");
        let result = storage.add_edge(e);

        assert!(result.is_err());
    }

    #[test]
    fn test_neighbors() {
        let storage = create_test_storage();

        let out = storage.out_neighbors(VertexId::new(1));
        assert_eq!(out.len(), 2);

        let in_ = storage.in_neighbors(VertexId::new(3));
        assert_eq!(in_.len(), 2);
    }

    #[test]
    fn test_edges_between() {
        let storage = create_test_storage();

        let edges = storage.edges_between(VertexId::new(1), VertexId::new(2));
        assert_eq!(edges.len(), 1);
    }

    #[test]
    fn test_degree() {
        let storage = create_test_storage();

        assert_eq!(storage.out_degree(VertexId::new(1)), 2);
        assert_eq!(storage.in_degree(VertexId::new(1)), 0);
        assert_eq!(storage.in_degree(VertexId::new(3)), 2);
    }

    #[test]
    fn test_remove_vertex() {
        let mut storage = create_test_storage();

        storage.remove_vertex(VertexId::new(1)).unwrap();
        assert_eq!(storage.vertex_count(), 2);
        assert!(!storage.has_vertex(VertexId::new(1)));
    }

    #[test]
    fn test_remove_edge() {
        let mut storage = create_test_storage();

        storage.remove_edge(EdgeId::new(1)).unwrap();
        assert_eq!(storage.edge_count(), 2);
    }

    #[test]
    fn test_filter_vertices() {
        let storage = create_test_storage();

        let filtered = storage.filter_vertices(|v| v.id.0 > 1);
        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn test_adjacency_list() {
        let storage = create_test_storage();
        let adj = AdjacencyList::from_storage(&storage, true);

        assert_eq!(adj.vertex_count(), 3);
        assert_eq!(adj.neighbors(VertexId::new(1)).len(), 2);
    }

    #[test]
    fn test_compressed_graph() {
        let storage = create_test_storage();
        let csr = CompressedGraph::from_storage(&storage);

        assert_eq!(csr.vertex_count(), 3);
        assert_eq!(csr.edge_count(), 3);
    }

    #[test]
    fn test_clear() {
        let mut storage = create_test_storage();
        storage.clear();

        assert_eq!(storage.vertex_count(), 0);
        assert_eq!(storage.edge_count(), 0);
    }
}
