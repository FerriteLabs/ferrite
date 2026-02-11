//! HNSW (Hierarchical Navigable Small World) index
//!
//! A graph-based approximate nearest neighbor index with logarithmic
//! search complexity. Best for high recall with moderate dataset sizes.

use super::distance::distance;
use super::types::{DistanceMetric, SearchResult, VectorId};
use super::{VectorError, VectorIndex};
use parking_lot::RwLock;
use rand::Rng;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

/// HNSW index for approximate nearest neighbor search
///
/// This index provides high recall with logarithmic search time by
/// building a hierarchical graph of vectors.
///
/// # Parameters
///
/// - `m`: Maximum connections per node (default: 16)
/// - `ef_construction`: Size of candidate list during construction (default: 200)
/// - `ef_search`: Size of candidate list during search (default: 50)
pub struct HnswIndex {
    /// Vector dimension
    dim: usize,
    /// Distance metric
    metric: DistanceMetric,
    /// Maximum connections per node at layer 0
    m0: usize,
    /// Maximum connections per node at higher layers
    m: usize,
    /// Candidate list size during construction
    ef_construction: usize,
    /// Candidate list size during search
    ef_search: RwLock<usize>,
    /// Level generation factor
    ml: f64,
    /// Vectors stored by internal ID
    vectors: RwLock<Vec<Option<Vec<f32>>>>,
    /// Mapping from external ID to internal ID
    id_to_internal: RwLock<HashMap<VectorId, usize>>,
    /// Mapping from internal ID to external ID
    internal_to_id: RwLock<HashMap<usize, VectorId>>,
    /// Graph layers (layer -> node -> neighbors)
    layers: RwLock<Vec<HashMap<usize, Vec<usize>>>>,
    /// Entry point node
    entry_point: RwLock<Option<usize>>,
    /// Maximum level of entry point
    max_level: RwLock<usize>,
    /// Node levels
    node_levels: RwLock<HashMap<usize, usize>>,
    /// Count of vectors
    count: RwLock<usize>,
}

/// Candidate for search (stores distance and node ID)
#[derive(Clone, Copy)]
struct Candidate {
    distance: f32,
    node: usize,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance && self.node == other.node
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse order for min-heap behavior in max-heap
        other
            .distance
            .partial_cmp(&self.distance)
            .unwrap_or(Ordering::Equal)
    }
}

/// Max-heap candidate (for pruning)
struct MaxCandidate {
    distance: f32,
    node: usize,
}

impl PartialEq for MaxCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}

impl Eq for MaxCandidate {}

impl PartialOrd for MaxCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MaxCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .partial_cmp(&other.distance)
            .unwrap_or(Ordering::Equal)
    }
}

impl HnswIndex {
    /// Create a new HNSW index with default parameters
    pub fn new(dimension: usize, metric: DistanceMetric) -> Self {
        Self::with_params(dimension, metric, 16, 200, 50)
    }

    /// Create a new HNSW index with custom parameters
    pub fn with_params(
        dimension: usize,
        metric: DistanceMetric,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
    ) -> Self {
        let m0 = m * 2; // Layer 0 has 2x connections
        let ml = 1.0 / (m as f64).ln();

        Self {
            dim: dimension,
            metric,
            m0,
            m,
            ef_construction,
            ef_search: RwLock::new(ef_search),
            ml,
            vectors: RwLock::new(Vec::new()),
            id_to_internal: RwLock::new(HashMap::new()),
            internal_to_id: RwLock::new(HashMap::new()),
            layers: RwLock::new(vec![HashMap::new()]),
            entry_point: RwLock::new(None),
            max_level: RwLock::new(0),
            node_levels: RwLock::new(HashMap::new()),
            count: RwLock::new(0),
        }
    }

    /// Reconstruct an HNSW index from raw parts (used by deserialization)
    #[allow(clippy::too_many_arguments)]
    pub fn from_raw_parts(
        dimension: usize,
        metric: DistanceMetric,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
        vectors_with_ids: Vec<(usize, Vec<f32>)>,
        id_to_internal: HashMap<VectorId, usize>,
        internal_to_id: HashMap<usize, VectorId>,
        layers: Vec<HashMap<usize, Vec<usize>>>,
        entry_point: Option<usize>,
        max_level: usize,
        node_levels: HashMap<usize, usize>,
        count: usize,
    ) -> Self {
        let m0 = m * 2;
        let ml = 1.0 / (m as f64).ln();

        // Reconstruct vectors array
        let max_internal = vectors_with_ids
            .iter()
            .map(|(id, _)| *id)
            .max()
            .map(|m| m + 1)
            .unwrap_or(0);
        let mut vectors = vec![None; max_internal];
        for (id, vec) in vectors_with_ids {
            if id < vectors.len() {
                vectors[id] = Some(vec);
            }
        }

        Self {
            dim: dimension,
            metric,
            m0,
            m,
            ef_construction,
            ef_search: RwLock::new(ef_search),
            ml,
            vectors: RwLock::new(vectors),
            id_to_internal: RwLock::new(id_to_internal),
            internal_to_id: RwLock::new(internal_to_id),
            layers: RwLock::new(layers),
            entry_point: RwLock::new(entry_point),
            max_level: RwLock::new(max_level),
            node_levels: RwLock::new(node_levels),
            count: RwLock::new(count),
        }
    }

    /// Get the M parameter
    pub fn m_param(&self) -> u32 {
        self.m as u32
    }

    /// Get the ef_construction parameter
    pub fn ef_construction_param(&self) -> u32 {
        self.ef_construction as u32
    }

    /// Get the number of layers
    pub fn num_layers(&self) -> usize {
        self.layers.read().len()
    }

    /// Get the max level value
    pub fn max_level_val(&self) -> usize {
        *self.max_level.read()
    }

    /// Get raw vectors (for serialization)
    pub fn raw_vectors(&self) -> Vec<Option<Vec<f32>>> {
        self.vectors.read().clone()
    }

    /// Get raw layers (for serialization)
    pub fn raw_layers(&self) -> Vec<HashMap<usize, Vec<usize>>> {
        self.layers.read().clone()
    }

    /// Get raw ID mappings (for serialization)
    pub fn raw_id_mappings(&self) -> Vec<(usize, VectorId)> {
        self.internal_to_id
            .read()
            .iter()
            .map(|(&k, v)| (k, v.clone()))
            .collect()
    }

    /// Get the entry point internal ID
    pub fn entry_point_id(&self) -> Option<usize> {
        *self.entry_point.read()
    }

    /// Get raw node levels (for serialization)
    pub fn raw_node_levels(&self) -> HashMap<usize, usize> {
        self.node_levels.read().clone()
    }

    /// Builder method to set M parameter
    pub fn with_m(mut self, m: usize) -> Self {
        self.m = m;
        self.m0 = m * 2;
        self.ml = 1.0 / (m as f64).ln();
        self
    }

    /// Builder method to set ef_construction parameter
    pub fn with_ef_construction(mut self, ef_construction: usize) -> Self {
        self.ef_construction = ef_construction;
        self
    }

    /// Builder method to set ef_search parameter
    pub fn with_ef_search(self, ef_search: usize) -> Self {
        *self.ef_search.write() = ef_search;
        self
    }

    /// Set ef_search parameter
    pub fn set_ef_search(&self, ef: usize) {
        *self.ef_search.write() = ef;
    }

    /// Get ef_search parameter
    pub fn ef_search(&self) -> usize {
        *self.ef_search.read()
    }

    /// Generate random level for a new node
    fn random_level(&self) -> usize {
        let mut rng = rand::thread_rng();
        let r: f64 = rng.gen();
        (-r.ln() * self.ml).floor() as usize
    }

    /// Calculate distance between query and node
    fn get_distance(&self, query: &[f32], node: usize) -> f32 {
        let vectors = self.vectors.read();
        if let Some(Some(vec)) = vectors.get(node) {
            distance(query, vec, self.metric)
        } else {
            f32::MAX
        }
    }

    /// Search layer for nearest neighbors
    fn search_layer(
        &self,
        query: &[f32],
        entry_points: Vec<usize>,
        ef: usize,
        layer: usize,
    ) -> Vec<Candidate> {
        let layers = self.layers.read();
        let layer_graph = match layers.get(layer) {
            Some(g) => g,
            None => return Vec::new(),
        };

        let mut visited: HashSet<usize> = HashSet::new();
        let mut candidates: BinaryHeap<Candidate> = BinaryHeap::new();
        let mut result: BinaryHeap<MaxCandidate> = BinaryHeap::new();

        for &ep in &entry_points {
            let dist = self.get_distance(query, ep);
            visited.insert(ep);
            candidates.push(Candidate {
                distance: dist,
                node: ep,
            });
            result.push(MaxCandidate {
                distance: dist,
                node: ep,
            });
        }

        while let Some(current) = candidates.pop() {
            // Get furthest in result
            let furthest_dist = result.peek().map(|c| c.distance).unwrap_or(f32::MAX);

            if current.distance > furthest_dist {
                break;
            }

            // Get neighbors of current node
            if let Some(neighbors) = layer_graph.get(&current.node) {
                for &neighbor in neighbors {
                    if visited.contains(&neighbor) {
                        continue;
                    }
                    visited.insert(neighbor);

                    let dist = self.get_distance(query, neighbor);

                    if dist < furthest_dist || result.len() < ef {
                        candidates.push(Candidate {
                            distance: dist,
                            node: neighbor,
                        });
                        result.push(MaxCandidate {
                            distance: dist,
                            node: neighbor,
                        });

                        if result.len() > ef {
                            result.pop();
                        }
                    }
                }
            }
        }

        // Convert result to vector of candidates
        result
            .into_iter()
            .map(|c| Candidate {
                distance: c.distance,
                node: c.node,
            })
            .collect()
    }

    /// Select neighbors using simple heuristic
    fn select_neighbors(&self, candidates: Vec<Candidate>, m: usize) -> Vec<usize> {
        let mut sorted: Vec<_> = candidates;
        sorted.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Equal)
        });
        sorted.into_iter().take(m).map(|c| c.node).collect()
    }

    /// Add connections for a node at a specific layer
    fn add_connections(&self, node: usize, neighbors: Vec<usize>, layer: usize) {
        let mut layers = self.layers.write();

        // Ensure layer exists
        while layers.len() <= layer {
            layers.push(HashMap::new());
        }

        // Add connections from node to neighbors
        layers[layer].insert(node, neighbors.clone());

        // Add reverse connections
        let m = if layer == 0 { self.m0 } else { self.m };
        for &neighbor in &neighbors {
            let neighbor_neighbors = layers[layer].entry(neighbor).or_default();
            if !neighbor_neighbors.contains(&node) {
                neighbor_neighbors.push(node);

                // Prune if too many connections
                if neighbor_neighbors.len() > m {
                    // Simple pruning: keep closest
                    let vectors = self.vectors.read();
                    if let Some(Some(neighbor_vec)) = vectors.get(neighbor) {
                        let neighbor_vec = neighbor_vec.clone();
                        drop(vectors);

                        let mut with_dist: Vec<(usize, f32)> = neighbor_neighbors
                            .iter()
                            .map(|&n| {
                                let dist = {
                                    let vectors = self.vectors.read();
                                    if let Some(Some(v)) = vectors.get(n) {
                                        distance(&neighbor_vec, v, self.metric)
                                    } else {
                                        f32::MAX
                                    }
                                };
                                (n, dist)
                            })
                            .collect();

                        with_dist.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));

                        *neighbor_neighbors =
                            with_dist.into_iter().take(m).map(|(n, _)| n).collect();
                    }
                }
            }
        }
    }
}

impl VectorIndex for HnswIndex {
    fn add(&self, id: VectorId, vector: &[f32]) -> Result<(), VectorError> {
        if vector.len() != self.dim {
            return Err(VectorError::DimensionMismatch {
                expected: self.dim,
                got: vector.len(),
            });
        }

        // Check if already exists
        if let Some(&internal_id) = self.id_to_internal.read().get(&id) {
            // Update existing vector
            self.vectors.write()[internal_id] = Some(vector.to_vec());
            return Ok(());
        }

        // Allocate internal ID
        let internal_id = {
            let mut vectors = self.vectors.write();
            let id = vectors.len();
            vectors.push(Some(vector.to_vec()));
            id
        };

        // Store mappings
        self.id_to_internal.write().insert(id.clone(), internal_id);
        self.internal_to_id.write().insert(internal_id, id);

        // Generate random level
        let level = self.random_level();
        self.node_levels.write().insert(internal_id, level);

        let max_level = *self.max_level.read();
        let entry_point = *self.entry_point.read();

        // Insert into graph
        if let Some(ep) = entry_point {
            let mut current_ep = vec![ep];

            // Search from top to level+1
            for l in (level + 1..=max_level).rev() {
                let nearest = self.search_layer(vector, current_ep.clone(), 1, l);
                if !nearest.is_empty() {
                    current_ep = vec![nearest[0].node];
                }
            }

            // Insert at each level from level down to 0
            for l in (0..=level.min(max_level)).rev() {
                let candidates =
                    self.search_layer(vector, current_ep.clone(), self.ef_construction, l);
                let m = if l == 0 { self.m0 } else { self.m };
                let neighbors = self.select_neighbors(candidates.clone(), m);
                self.add_connections(internal_id, neighbors, l);

                // Update entry points for next level
                current_ep = candidates.iter().map(|c| c.node).collect();
            }

            // Update entry point if new node has higher level
            if level > max_level {
                *self.max_level.write() = level;
                *self.entry_point.write() = Some(internal_id);

                // Ensure layers exist
                let mut layers = self.layers.write();
                while layers.len() <= level {
                    layers.push(HashMap::new());
                }
            }
        } else {
            // First node
            *self.entry_point.write() = Some(internal_id);
            *self.max_level.write() = level;

            // Initialize layers
            let mut layers = self.layers.write();
            while layers.len() <= level {
                layers.push(HashMap::new());
            }
        }

        *self.count.write() += 1;
        Ok(())
    }

    fn remove(&self, id: &VectorId) -> Result<bool, VectorError> {
        let internal_id = match self.id_to_internal.read().get(id).copied() {
            Some(id) => id,
            None => return Ok(false),
        };

        // Remove from mappings
        self.id_to_internal.write().remove(id);
        self.internal_to_id.write().remove(&internal_id);

        // Mark vector as deleted
        if let Some(slot) = self.vectors.write().get_mut(internal_id) {
            *slot = None;
        }

        // Remove from node levels
        self.node_levels.write().remove(&internal_id);

        // Remove from graph layers
        {
            let mut layers = self.layers.write();
            for layer in layers.iter_mut() {
                // Remove node's entry
                layer.remove(&internal_id);

                // Remove from neighbors' lists
                for neighbors in layer.values_mut() {
                    neighbors.retain(|&n| n != internal_id);
                }
            }
        }

        // Update entry point if needed
        if self
            .entry_point
            .read()
            .map(|ep| ep == internal_id)
            .unwrap_or(false)
        {
            // Find new entry point
            let new_ep = self
                .node_levels
                .read()
                .iter()
                .max_by_key(|(_, &level)| level)
                .map(|(&id, _)| id);
            *self.entry_point.write() = new_ep;

            if let Some(ep) = new_ep {
                *self.max_level.write() = self.node_levels.read().get(&ep).copied().unwrap_or(0);
            } else {
                *self.max_level.write() = 0;
            }
        }

        *self.count.write() -= 1;
        Ok(true)
    }

    fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, VectorError> {
        if query.len() != self.dim {
            return Err(VectorError::DimensionMismatch {
                expected: self.dim,
                got: query.len(),
            });
        }

        let entry_point = match *self.entry_point.read() {
            Some(ep) => ep,
            None => return Ok(Vec::new()),
        };

        let max_level = *self.max_level.read();
        let ef_search = *self.ef_search.read();

        let mut current_ep = vec![entry_point];

        // Search from top to layer 1
        for l in (1..=max_level).rev() {
            let nearest = self.search_layer(query, current_ep.clone(), 1, l);
            if !nearest.is_empty() {
                current_ep = vec![nearest[0].node];
            }
        }

        // Search layer 0 with ef_search candidates
        let candidates = self.search_layer(query, current_ep, ef_search.max(k), 0);

        // Convert to search results
        let internal_to_id = self.internal_to_id.read();
        let mut results: Vec<SearchResult> = candidates
            .into_iter()
            .filter_map(|c| {
                internal_to_id
                    .get(&c.node)
                    .map(|id| SearchResult::new(id.clone(), c.distance))
            })
            .collect();

        // Sort by distance and take k
        results.sort_by(|a, b| a.score.partial_cmp(&b.score).unwrap_or(Ordering::Equal));
        results.truncate(k);

        Ok(results)
    }

    fn dimension(&self) -> usize {
        self.dim
    }

    fn len(&self) -> usize {
        *self.count.read()
    }

    fn metric(&self) -> DistanceMetric {
        self.metric
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hnsw_basic() {
        let index = HnswIndex::new(3, DistanceMetric::Cosine);

        index.add(VectorId::new("v1"), &[1.0, 0.0, 0.0]).unwrap();
        index.add(VectorId::new("v2"), &[0.0, 1.0, 0.0]).unwrap();
        index.add(VectorId::new("v3"), &[0.0, 0.0, 1.0]).unwrap();

        assert_eq!(index.len(), 3);
    }

    #[test]
    fn test_hnsw_search() {
        let index = HnswIndex::with_params(3, DistanceMetric::Cosine, 4, 100, 50);

        // Add some vectors
        index.add(VectorId::new("v1"), &[1.0, 0.0, 0.0]).unwrap();
        index.add(VectorId::new("v2"), &[0.9, 0.1, 0.0]).unwrap();
        index.add(VectorId::new("v3"), &[0.0, 1.0, 0.0]).unwrap();
        index.add(VectorId::new("v4"), &[0.1, 0.9, 0.0]).unwrap();

        // Search for vector similar to v1
        let results = index.search(&[1.0, 0.0, 0.0], 2).unwrap();
        assert!(!results.is_empty());

        // First result should be v1 (exact match)
        assert_eq!(results[0].id.as_str(), "v1");
    }

    #[test]
    fn test_hnsw_remove() {
        let index = HnswIndex::new(3, DistanceMetric::Cosine);

        index.add(VectorId::new("v1"), &[1.0, 0.0, 0.0]).unwrap();
        index.add(VectorId::new("v2"), &[0.0, 1.0, 0.0]).unwrap();
        assert_eq!(index.len(), 2);

        let removed = index.remove(&VectorId::new("v1")).unwrap();
        assert!(removed);
        assert_eq!(index.len(), 1);

        // Search should not find removed vector
        let results = index.search(&[1.0, 0.0, 0.0], 10).unwrap();
        assert!(!results.iter().any(|r| r.id.as_str() == "v1"));
    }

    #[test]
    fn test_hnsw_dimension_mismatch() {
        let index = HnswIndex::new(3, DistanceMetric::Cosine);

        let result = index.add(VectorId::new("v1"), &[1.0, 0.0]);
        assert!(matches!(result, Err(VectorError::DimensionMismatch { .. })));
    }

    #[test]
    fn test_hnsw_empty() {
        let index = HnswIndex::new(3, DistanceMetric::Cosine);
        assert!(index.is_empty());

        let results = index.search(&[1.0, 0.0, 0.0], 10).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_hnsw_larger() {
        let index = HnswIndex::with_params(64, DistanceMetric::Euclidean, 8, 50, 20);

        // Add 100 random vectors
        for i in 0..100 {
            let vec: Vec<f32> = (0..64).map(|j| ((i + j) as f32) / 100.0).collect();
            index.add(VectorId::new(format!("v{}", i)), &vec).unwrap();
        }

        assert_eq!(index.len(), 100);

        // Search should return results
        let query: Vec<f32> = (0..64).map(|j| (j as f32) / 100.0).collect();
        let results = index.search(&query, 5).unwrap();
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn test_hnsw_ef_search() {
        let index = HnswIndex::new(3, DistanceMetric::Cosine);

        assert_eq!(index.ef_search(), 50);
        index.set_ef_search(100);
        assert_eq!(index.ef_search(), 100);
    }
}
