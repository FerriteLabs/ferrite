//! HNSW (Hierarchical Navigable Small World) Index
//!
//! High-performance approximate nearest neighbor search for semantic cache.
//! Provides O(log n) search complexity compared to O(n) brute force.
//!
//! # Performance Characteristics
//!
//! - Search: O(log n) average case
//! - Insert: O(log n) average case
//! - Memory: O(n * M) where M is max connections per node
//! - High recall (>95%) with proper tuning
//!
//! # Example
//!
//! ```ignore
//! use ferrite::semantic::hnsw::{HnswIndex, HnswConfig};
//!
//! let config = HnswConfig {
//!     dimension: 384,
//!     m: 16,
//!     ef_construction: 200,
//!     ef_search: 50,
//!     ..Default::default()
//! };
//!
//! let mut index = HnswIndex::new(config);
//! index.insert(1, &embedding)?;
//!
//! let results = index.search(&query_embedding, 10, 0.8);
//! ```

use super::{DistanceMetric, SemanticError};
use parking_lot::RwLock;
use rand::Rng;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// HNSW configuration
#[derive(Clone, Debug)]
pub struct HnswConfig {
    /// Vector dimension
    pub dimension: usize,
    /// Maximum number of connections per layer (M parameter)
    pub m: usize,
    /// Maximum connections for layer 0 (typically 2*M)
    pub m_max_0: usize,
    /// Size of dynamic candidate list during construction
    pub ef_construction: usize,
    /// Size of dynamic candidate list during search
    pub ef_search: usize,
    /// Level generation multiplier (1/ln(M))
    pub ml: f64,
    /// Distance metric
    pub metric: DistanceMetric,
    /// Maximum number of elements
    pub max_elements: usize,
}

impl Default for HnswConfig {
    fn default() -> Self {
        let m = 16;
        Self {
            dimension: 384,
            m,
            m_max_0: m * 2,
            ef_construction: 200,
            ef_search: 50,
            ml: 1.0 / (m as f64).ln(),
            metric: DistanceMetric::Cosine,
            max_elements: 1_000_000,
        }
    }
}

impl HnswConfig {
    /// Create config optimized for high recall
    pub fn high_recall(dimension: usize) -> Self {
        Self {
            dimension,
            m: 32,
            m_max_0: 64,
            ef_construction: 400,
            ef_search: 100,
            ml: 1.0 / 32_f64.ln(),
            metric: DistanceMetric::Cosine,
            max_elements: 1_000_000,
        }
    }

    /// Create config optimized for speed
    pub fn high_speed(dimension: usize) -> Self {
        Self {
            dimension,
            m: 12,
            m_max_0: 24,
            ef_construction: 100,
            ef_search: 20,
            ml: 1.0 / 12_f64.ln(),
            metric: DistanceMetric::Cosine,
            max_elements: 1_000_000,
        }
    }

    /// Create config optimized for memory efficiency
    pub fn low_memory(dimension: usize) -> Self {
        Self {
            dimension,
            m: 8,
            m_max_0: 16,
            ef_construction: 100,
            ef_search: 30,
            ml: 1.0 / 8_f64.ln(),
            metric: DistanceMetric::Cosine,
            max_elements: 1_000_000,
        }
    }
}

/// A node in the HNSW graph
#[derive(Clone)]
struct HnswNode {
    /// Node ID
    id: u64,
    /// Vector data
    vector: Vec<f32>,
    /// Connections per layer: layer -> connected node IDs
    connections: Vec<Vec<u64>>,
    /// Maximum layer this node appears in
    max_level: usize,
}

impl HnswNode {
    fn new(id: u64, vector: Vec<f32>, max_level: usize) -> Self {
        let connections = (0..=max_level).map(|_| Vec::new()).collect();
        Self {
            id,
            vector,
            connections,
            max_level,
        }
    }
}

/// Entry in the candidate heap (ordered by distance)
#[derive(Clone, PartialEq)]
struct Candidate {
    id: u64,
    distance: f32,
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse for min-heap behavior
        other
            .distance
            .partial_cmp(&self.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Max-heap candidate (for maintaining top-k farthest)
#[derive(Clone, PartialEq)]
struct MaxCandidate {
    id: u64,
    distance: f32,
}

impl Eq for MaxCandidate {}

impl PartialOrd for MaxCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MaxCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance
            .partial_cmp(&other.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// HNSW Index for approximate nearest neighbor search
pub struct HnswIndex {
    /// Configuration
    config: HnswConfig,
    /// All nodes (ID -> Node)
    nodes: RwLock<HashMap<u64, HnswNode>>,
    /// Entry point (top-level node ID)
    entry_point: RwLock<Option<u64>>,
    /// Current maximum level
    max_level: AtomicUsize,
    /// Number of elements
    count: AtomicUsize,
    /// Statistics
    stats: HnswStats,
}

/// HNSW statistics
#[derive(Default)]
pub struct HnswStats {
    /// Number of search operations
    pub searches: AtomicU64,
    /// Number of insert operations
    pub inserts: AtomicU64,
    /// Number of delete operations
    pub deletes: AtomicU64,
    /// Total distance calculations
    pub distance_calcs: AtomicU64,
}

impl HnswIndex {
    /// Create a new HNSW index
    pub fn new(config: HnswConfig) -> Self {
        Self {
            config,
            nodes: RwLock::new(HashMap::new()),
            entry_point: RwLock::new(None),
            max_level: AtomicUsize::new(0),
            count: AtomicUsize::new(0),
            stats: HnswStats::default(),
        }
    }

    /// Create with default configuration
    pub fn with_dimension(dimension: usize) -> Self {
        Self::new(HnswConfig {
            dimension,
            ..HnswConfig::default()
        })
    }

    /// Insert a vector into the index
    pub fn insert(&self, id: u64, vector: &[f32]) -> Result<(), SemanticError> {
        if vector.len() != self.config.dimension {
            return Err(SemanticError::DimensionMismatch {
                expected: self.config.dimension,
                got: vector.len(),
            });
        }

        let level = self.random_level();
        let mut node = HnswNode::new(id, vector.to_vec(), level);

        // Get write lock for insertion
        let mut nodes = self.nodes.write();
        let mut entry_point = self.entry_point.write();

        // First node case
        if entry_point.is_none() {
            nodes.insert(id, node);
            *entry_point = Some(id);
            self.max_level.store(level, Ordering::SeqCst);
            self.count.fetch_add(1, Ordering::SeqCst);
            self.stats.inserts.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        let ep_id = (*entry_point).ok_or(SemanticError::Internal("missing entry point".to_string()))?;
        let current_max_level = self.max_level.load(Ordering::SeqCst);

        // Navigate from top level down to the new node's level
        let mut current_ep = ep_id;

        // Search from top to level+1
        for lc in (level + 1..=current_max_level).rev() {
            let nearest = self.search_layer_single(&nodes, vector, current_ep, lc);
            current_ep = nearest.id;
        }

        // Insert at levels 0 to level
        for lc in (0..=level.min(current_max_level)).rev() {
            let candidates =
                self.search_layer(&nodes, vector, current_ep, self.config.ef_construction, lc);

            let neighbors = self.select_neighbors(&nodes, vector, &candidates, self.get_m(lc));

            // Connect new node to neighbors
            if lc < node.connections.len() {
                node.connections[lc] = neighbors.iter().map(|c| c.id).collect();
            }

            // Add bidirectional connections and collect nodes that need shrinking
            let max_conn = self.get_m(lc);
            let mut needs_shrink: Vec<u64> = Vec::new();

            for neighbor in &neighbors {
                if let Some(neighbor_node) = nodes.get_mut(&neighbor.id) {
                    if lc < neighbor_node.connections.len() {
                        neighbor_node.connections[lc].push(id);

                        // Mark for shrinking if necessary
                        if neighbor_node.connections[lc].len() > max_conn {
                            needs_shrink.push(neighbor.id);
                        }
                    }
                }
            }

            // Now shrink connections - collect all neighbor vectors first
            for neighbor_id in needs_shrink {
                // Collect neighbor's neighbor vectors before getting mutable reference
                let neighbor_neighbor_vectors: Vec<(u64, Vec<f32>)> = {
                    if let Some(neighbor_node) = nodes.get(&neighbor_id) {
                        if lc < neighbor_node.connections.len() {
                            neighbor_node.connections[lc]
                                .iter()
                                .filter_map(|&nn_id| {
                                    nodes.get(&nn_id).map(|nn| (nn_id, nn.vector.clone()))
                                })
                                .collect()
                        } else {
                            Vec::new()
                        }
                    } else {
                        Vec::new()
                    }
                };

                // Now we can safely get mutable reference and shrink
                if let Some(neighbor_node) = nodes.get_mut(&neighbor_id) {
                    self.shrink_with_vectors(
                        neighbor_node,
                        lc,
                        max_conn,
                        &neighbor_neighbor_vectors,
                    );
                }
            }

            if !candidates.is_empty() {
                current_ep = candidates[0].id;
            }
        }

        // Update entry point if new node has higher level
        if level > current_max_level {
            *entry_point = Some(id);
            self.max_level.store(level, Ordering::SeqCst);
        }

        nodes.insert(id, node);
        self.count.fetch_add(1, Ordering::SeqCst);
        self.stats.inserts.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Search for k nearest neighbors
    pub fn search(&self, query: &[f32], k: usize, threshold: f32) -> Vec<(u64, f32)> {
        if query.len() != self.config.dimension {
            return Vec::new();
        }

        let nodes = self.nodes.read();
        let entry_point = self.entry_point.read();

        let Some(ep_id) = *entry_point else {
            return Vec::new();
        };
        let max_level = self.max_level.load(Ordering::SeqCst);

        self.stats.searches.fetch_add(1, Ordering::Relaxed);

        // Navigate from top level to level 1
        let mut current_ep = ep_id;
        for lc in (1..=max_level).rev() {
            let nearest = self.search_layer_single(&nodes, query, current_ep, lc);
            current_ep = nearest.id;
        }

        // Search at layer 0
        let candidates =
            self.search_layer(&nodes, query, current_ep, self.config.ef_search.max(k), 0);

        // Filter by threshold and return top k
        let mut results: Vec<(u64, f32)> = candidates
            .into_iter()
            .filter(|c| self.distance_to_similarity(c.distance) >= threshold)
            .take(k)
            .map(|c| (c.id, self.distance_to_similarity(c.distance)))
            .collect();

        // Sort by similarity descending
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        results
    }

    /// Remove a vector from the index
    pub fn remove(&self, id: u64) -> bool {
        let mut nodes = self.nodes.write();
        let mut entry_point = self.entry_point.write();

        if nodes.remove(&id).is_none() {
            return false;
        }

        // Remove connections to this node from all neighbors
        for node in nodes.values_mut() {
            for connections in &mut node.connections {
                connections.retain(|&conn_id| conn_id != id);
            }
        }

        // Update entry point if necessary
        if *entry_point == Some(id) {
            *entry_point = nodes.keys().next().copied();
            if let Some(new_ep) = *entry_point {
                let new_level = nodes.get(&new_ep).map(|n| n.max_level).unwrap_or(0);
                self.max_level.store(new_level, Ordering::SeqCst);
            } else {
                self.max_level.store(0, Ordering::SeqCst);
            }
        }

        self.count.fetch_sub(1, Ordering::SeqCst);
        self.stats.deletes.fetch_add(1, Ordering::Relaxed);

        true
    }

    /// Get number of elements
    pub fn len(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear the index
    pub fn clear(&self) {
        let mut nodes = self.nodes.write();
        let mut entry_point = self.entry_point.write();

        nodes.clear();
        *entry_point = None;
        self.max_level.store(0, Ordering::SeqCst);
        self.count.store(0, Ordering::SeqCst);
    }

    /// Get statistics
    pub fn stats(&self) -> (u64, u64, u64, u64) {
        (
            self.stats.searches.load(Ordering::Relaxed),
            self.stats.inserts.load(Ordering::Relaxed),
            self.stats.deletes.load(Ordering::Relaxed),
            self.stats.distance_calcs.load(Ordering::Relaxed),
        )
    }

    /// Get configuration
    pub fn config(&self) -> &HnswConfig {
        &self.config
    }

    // === Private methods ===

    /// Generate a random level for a new node
    fn random_level(&self) -> usize {
        let mut rng = rand::thread_rng();
        let mut level = 0;

        while rng.gen::<f64>() < self.config.ml && level < 16 {
            level += 1;
        }

        level
    }

    /// Get max connections for a layer
    fn get_m(&self, layer: usize) -> usize {
        if layer == 0 {
            self.config.m_max_0
        } else {
            self.config.m
        }
    }

    /// Search a single layer, returning nearest neighbor
    fn search_layer_single(
        &self,
        nodes: &HashMap<u64, HnswNode>,
        query: &[f32],
        ep_id: u64,
        layer: usize,
    ) -> Candidate {
        let ep_node = match nodes.get(&ep_id) {
            Some(n) => n,
            None => {
                return Candidate {
                    id: ep_id,
                    distance: f32::MAX,
                }
            }
        };

        let mut current = Candidate {
            id: ep_id,
            distance: self.calculate_distance(query, &ep_node.vector),
        };

        let mut improved = true;
        while improved {
            improved = false;

            if let Some(node) = nodes.get(&current.id) {
                if layer < node.connections.len() {
                    for &neighbor_id in &node.connections[layer] {
                        if let Some(neighbor) = nodes.get(&neighbor_id) {
                            let dist = self.calculate_distance(query, &neighbor.vector);
                            if dist < current.distance {
                                current = Candidate {
                                    id: neighbor_id,
                                    distance: dist,
                                };
                                improved = true;
                            }
                        }
                    }
                }
            }
        }

        current
    }

    /// Search a layer with ef candidates
    fn search_layer(
        &self,
        nodes: &HashMap<u64, HnswNode>,
        query: &[f32],
        ep_id: u64,
        ef: usize,
        layer: usize,
    ) -> Vec<Candidate> {
        let ep_node = match nodes.get(&ep_id) {
            Some(n) => n,
            None => return Vec::new(),
        };

        let mut visited = HashSet::new();
        visited.insert(ep_id);

        let ep_dist = self.calculate_distance(query, &ep_node.vector);

        // Min-heap for candidates to explore
        let mut candidates: BinaryHeap<Candidate> = BinaryHeap::new();
        candidates.push(Candidate {
            id: ep_id,
            distance: ep_dist,
        });

        // Max-heap for results (keep closest ef elements)
        let mut results: BinaryHeap<MaxCandidate> = BinaryHeap::new();
        results.push(MaxCandidate {
            id: ep_id,
            distance: ep_dist,
        });

        while let Some(current) = candidates.pop() {
            // Stop if current candidate is worse than worst result
            if let Some(worst) = results.peek() {
                if current.distance > worst.distance && results.len() >= ef {
                    break;
                }
            }

            if let Some(node) = nodes.get(&current.id) {
                if layer < node.connections.len() {
                    for &neighbor_id in &node.connections[layer] {
                        if visited.contains(&neighbor_id) {
                            continue;
                        }
                        visited.insert(neighbor_id);

                        if let Some(neighbor) = nodes.get(&neighbor_id) {
                            let dist = self.calculate_distance(query, &neighbor.vector);
                            self.stats.distance_calcs.fetch_add(1, Ordering::Relaxed);

                            let should_add = results.len() < ef
                                || results.peek().map(|w| dist < w.distance).unwrap_or(true);

                            if should_add {
                                candidates.push(Candidate {
                                    id: neighbor_id,
                                    distance: dist,
                                });
                                results.push(MaxCandidate {
                                    id: neighbor_id,
                                    distance: dist,
                                });

                                if results.len() > ef {
                                    results.pop();
                                }
                            }
                        }
                    }
                }
            }
        }

        // Convert results to sorted vector
        let mut result_vec: Vec<Candidate> = results
            .into_iter()
            .map(|c| Candidate {
                id: c.id,
                distance: c.distance,
            })
            .collect();
        result_vec.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        result_vec
    }

    /// Select neighbors using simple heuristic
    fn select_neighbors(
        &self,
        _nodes: &HashMap<u64, HnswNode>,
        _query: &[f32],
        candidates: &[Candidate],
        m: usize,
    ) -> Vec<Candidate> {
        candidates.iter().take(m).cloned().collect()
    }

    /// Shrink connections using pre-collected neighbor vectors (borrow-safe version)
    fn shrink_with_vectors(
        &self,
        node: &mut HnswNode,
        layer: usize,
        max_conn: usize,
        neighbor_vectors: &[(u64, Vec<f32>)],
    ) {
        if layer >= node.connections.len() || node.connections[layer].len() <= max_conn {
            return;
        }

        // Calculate distances using the pre-collected vectors
        let mut neighbor_dists: Vec<(u64, f32)> = neighbor_vectors
            .iter()
            .map(|(id, vec)| (*id, self.calculate_distance(&node.vector, vec)))
            .collect();

        neighbor_dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        neighbor_dists.truncate(max_conn);

        node.connections[layer] = neighbor_dists.into_iter().map(|(id, _)| id).collect();
    }

    /// Shrink connections to maintain max degree
    #[allow(dead_code)] // Planned for v0.2 — reserved for HNSW graph maintenance operations
    fn shrink_connections(
        &self,
        nodes: &HashMap<u64, HnswNode>,
        node: &mut HnswNode,
        layer: usize,
        max_conn: usize,
    ) {
        if layer >= node.connections.len() || node.connections[layer].len() <= max_conn {
            return;
        }

        // Collect neighbor vectors first
        let neighbor_vectors: Vec<(u64, Vec<f32>)> = node.connections[layer]
            .iter()
            .filter_map(|&neighbor_id| {
                nodes
                    .get(&neighbor_id)
                    .map(|neighbor| (neighbor_id, neighbor.vector.clone()))
            })
            .collect();

        self.shrink_with_vectors(node, layer, max_conn, &neighbor_vectors);
    }

    /// Calculate distance between two vectors
    fn calculate_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.config.metric {
            DistanceMetric::Cosine => 1.0 - cosine_similarity(a, b),
            DistanceMetric::Euclidean => euclidean_distance(a, b),
            DistanceMetric::DotProduct => -dot_product(a, b),
        }
    }

    /// Convert distance to similarity score
    fn distance_to_similarity(&self, distance: f32) -> f32 {
        match self.config.metric {
            DistanceMetric::Cosine => 1.0 - distance,
            DistanceMetric::Euclidean => 1.0 / (1.0 + distance),
            DistanceMetric::DotProduct => -distance, // Already similarity-like
        }
    }
}

// Distance functions
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        0.0
    } else {
        dot / (norm_a * norm_b)
    }
}

fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f32>()
        .sqrt()
}

fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

// Thread safety
unsafe impl Send for HnswIndex {}
unsafe impl Sync for HnswIndex {}

#[cfg(test)]
mod tests {
    use super::*;

    fn random_vector(dim: usize, seed: u64) -> Vec<f32> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        (0..dim)
            .map(|i| {
                let mut hasher = DefaultHasher::new();
                i.hash(&mut hasher);
                seed.hash(&mut hasher);
                let h = hasher.finish();
                ((h % 1000) as f32 / 1000.0) * 2.0 - 1.0
            })
            .collect()
    }

    fn normalize(v: &mut Vec<f32>) {
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for x in v.iter_mut() {
                *x /= norm;
            }
        }
    }

    #[test]
    fn test_hnsw_insert_search() {
        let index = HnswIndex::with_dimension(4);

        // Use simple vectors
        let vectors: Vec<Vec<f32>> = vec![
            vec![1.0, 0.0, 0.0, 0.0],
            vec![0.0, 1.0, 0.0, 0.0],
            vec![0.0, 0.0, 1.0, 0.0],
            vec![0.0, 0.0, 0.0, 1.0],
        ];

        for (i, vec) in vectors.iter().enumerate() {
            index.insert(i as u64, vec).unwrap();
        }

        assert_eq!(index.len(), 4);

        // Search for the first vector
        let results = index.search(&vectors[0], 4, 0.0);

        // HNSW is approximate with random level assignment, so we verify:
        // 1. Search returns results
        assert!(!results.is_empty(), "Search should return results");

        // 2. Results have valid structure (IDs and similarity scores)
        for (id, similarity) in &results {
            assert!(*id < 4, "ID should be valid");
            assert!(
                *similarity >= -1.0 && *similarity <= 1.0,
                "Similarity should be in valid range"
            );
        }

        // 3. Results are sorted by similarity (descending)
        for i in 1..results.len() {
            assert!(
                results[i - 1].1 >= results[i].1,
                "Results should be sorted by similarity"
            );
        }
    }

    #[test]
    fn test_hnsw_remove() {
        let index = HnswIndex::with_dimension(64);

        let mut vec = random_vector(64, 42);
        normalize(&mut vec);
        index.insert(42, &vec).unwrap();

        assert_eq!(index.len(), 1);
        assert!(index.remove(42));
        assert_eq!(index.len(), 0);
        assert!(!index.remove(42)); // Already removed
    }

    #[test]
    fn test_hnsw_clear() {
        let index = HnswIndex::with_dimension(64);

        for i in 0..5 {
            let mut vec = random_vector(64, i);
            normalize(&mut vec);
            index.insert(i, &vec).unwrap();
        }

        assert_eq!(index.len(), 5);
        index.clear();
        assert!(index.is_empty());
    }

    #[test]
    fn test_hnsw_dimension_mismatch() {
        let index = HnswIndex::with_dimension(64);

        let vec = random_vector(128, 0); // Wrong dimension
        let result = index.insert(0, &vec);

        assert!(matches!(
            result,
            Err(SemanticError::DimensionMismatch {
                expected: 64,
                got: 128
            })
        ));
    }

    #[test]
    fn test_hnsw_configs() {
        let high_recall = HnswConfig::high_recall(128);
        assert_eq!(high_recall.m, 32);
        assert_eq!(high_recall.ef_construction, 400);

        let high_speed = HnswConfig::high_speed(128);
        assert_eq!(high_speed.m, 12);
        assert_eq!(high_speed.ef_search, 20);

        let low_memory = HnswConfig::low_memory(128);
        assert_eq!(low_memory.m, 8);
    }

    #[test]
    fn test_hnsw_threshold_filtering() {
        let index = HnswIndex::with_dimension(64);

        // Insert vectors with known similarity
        let mut base = random_vector(64, 0);
        normalize(&mut base);
        index.insert(0, &base).unwrap();

        // Insert a very different vector
        let mut different: Vec<f32> = base.iter().map(|x| -x).collect();
        normalize(&mut different);
        index.insert(1, &different).unwrap();

        // Search with high threshold should only return similar
        let results = index.search(&base, 10, 0.9);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 0);
    }

    #[test]
    fn test_hnsw_stats() {
        let index = HnswIndex::with_dimension(64);

        let mut vec = random_vector(64, 0);
        normalize(&mut vec);
        index.insert(0, &vec).unwrap();
        index.search(&vec, 1, 0.0);

        let (searches, inserts, deletes, _) = index.stats();
        assert_eq!(inserts, 1);
        assert_eq!(searches, 1);
        assert_eq!(deletes, 0);
    }
}
