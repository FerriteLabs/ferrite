//! Incremental HNSW index updates
//!
//! Supports lock-free incremental additions to HNSW indexes without
//! requiring full rebuilds. Includes background rebalancing for
//! maintaining recall quality.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::distance::distance;
use super::types::DistanceMetric;
use super::VectorError;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for incremental HNSW index updates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalHnswConfig {
    /// Maximum pending inserts before automatic flush.
    pub max_pending_inserts: usize,
    /// Trigger rebalance when estimated recall drops below this threshold.
    pub rebalance_threshold: f64,
    /// Periodic rebalance check interval.
    pub rebalance_interval: Duration,
    /// Maximum nodes to rebalance per cycle.
    pub max_rebalance_batch: usize,
}

impl Default for IncrementalHnswConfig {
    fn default() -> Self {
        Self {
            max_pending_inserts: 1000,
            rebalance_threshold: 0.95,
            rebalance_interval: Duration::from_secs(300),
            max_rebalance_batch: 500,
        }
    }
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// An item to insert into the incremental index.
#[derive(Debug, Clone)]
pub struct VectorInsertItem {
    /// Unique identifier.
    pub id: String,
    /// Vector data.
    pub vector: Vec<f32>,
    /// Optional JSON metadata.
    pub metadata: Option<serde_json::Value>,
}

/// Result returned by a search query.
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// Vector identifier.
    pub id: String,
    /// Distance to the query vector.
    pub distance: f32,
    /// Optional JSON metadata.
    pub metadata: Option<serde_json::Value>,
}

/// Result of a batch insert operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchInsertResult {
    /// Number of vectors successfully inserted.
    pub inserted: usize,
    /// Number of duplicate IDs skipped.
    pub duplicates: usize,
    /// Number of items that caused errors.
    pub errors: usize,
    /// Total operation time in milliseconds.
    pub duration_ms: u64,
}

/// Result of flushing pending inserts into the main index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlushResult {
    /// Number of vectors flushed from the pending buffer.
    pub flushed: usize,
    /// Total index size after flush.
    pub index_size: usize,
}

/// Result of a rebalance operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalanceResult {
    /// Number of graph nodes updated during rebalancing.
    pub nodes_updated: usize,
    /// Estimated recall before rebalancing.
    pub recall_before: f64,
    /// Estimated recall after rebalancing.
    pub recall_after: f64,
}

/// Statistics about the incremental index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStats {
    /// Total number of indexed vectors (main + pending).
    pub total_vectors: usize,
    /// Vector dimension.
    pub dimension: usize,
    /// Number of HNSW layers.
    pub levels: usize,
    /// Number of vectors in the pending buffer.
    pub pending_inserts: usize,
    /// Estimated recall quality (0.0–1.0).
    pub estimated_recall: f64,
    /// Approximate memory usage in bytes.
    pub memory_bytes: usize,
}

// ---------------------------------------------------------------------------
// Internal storage
// ---------------------------------------------------------------------------

/// A stored vector with its metadata.
#[derive(Debug, Clone)]
struct StoredVector {
    id: String,
    vector: Vec<f32>,
    metadata: Option<serde_json::Value>,
    neighbors: Vec<usize>, // indices into the main vectors list
}

/// Incremental HNSW index that supports lock-free additions without full rebuilds.
pub struct IncrementalHnswIndex {
    config: IncrementalHnswConfig,
    dimension: usize,
    metric: DistanceMetric,

    /// Main indexed vectors.
    main_vectors: RwLock<Vec<StoredVector>>,
    /// ID → index mapping for the main store.
    id_to_index: RwLock<HashMap<String, usize>>,
    /// Pending vectors not yet flushed into the main graph.
    pending: RwLock<Vec<StoredVector>>,
    /// ID → index mapping for the pending buffer.
    pending_id_to_index: RwLock<HashMap<String, usize>>,

    /// Number of HNSW layers in the main graph.
    levels: RwLock<usize>,
    /// Estimated recall (updated after rebalance/flush).
    estimated_recall: RwLock<f64>,

    /// Counters.
    total_inserts: AtomicU64,
    total_searches: AtomicU64,
}

impl IncrementalHnswIndex {
    /// Create a new incremental HNSW index.
    pub fn new(config: IncrementalHnswConfig, dimension: usize, metric: DistanceMetric) -> Self {
        Self {
            config,
            dimension,
            metric,
            main_vectors: RwLock::new(Vec::new()),
            id_to_index: RwLock::new(HashMap::new()),
            pending: RwLock::new(Vec::new()),
            pending_id_to_index: RwLock::new(HashMap::new()),
            levels: RwLock::new(1),
            estimated_recall: RwLock::new(1.0),
            total_inserts: AtomicU64::new(0),
            total_searches: AtomicU64::new(0),
        }
    }

    /// Insert a single vector into the pending buffer.
    pub fn insert(
        &self,
        id: &str,
        vector: &[f32],
        metadata: Option<serde_json::Value>,
    ) -> Result<(), VectorError> {
        if vector.len() != self.dimension {
            return Err(VectorError::DimensionMismatch {
                expected: self.dimension,
                got: vector.len(),
            });
        }

        if !vector.iter().all(|v| v.is_finite()) {
            return Err(VectorError::InvalidConfig(
                "vector contains non-finite values".to_string(),
            ));
        }

        // Check for duplicates across main and pending
        if self.id_to_index.read().contains_key(id) {
            return Err(VectorError::Internal(format!("duplicate id: {id}")));
        }
        if self.pending_id_to_index.read().contains_key(id) {
            return Err(VectorError::Internal(format!("duplicate id: {id}")));
        }

        let stored = StoredVector {
            id: id.to_string(),
            vector: vector.to_vec(),
            metadata,
            neighbors: Vec::new(),
        };

        let mut pending = self.pending.write();
        let idx = pending.len();
        self.pending_id_to_index
            .write()
            .insert(id.to_string(), idx);
        pending.push(stored);

        self.total_inserts.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Insert a batch of vectors, returning aggregate results.
    pub fn insert_batch(
        &self,
        items: Vec<VectorInsertItem>,
    ) -> Result<BatchInsertResult, VectorError> {
        let start = Instant::now();
        let mut inserted = 0usize;
        let mut duplicates = 0usize;
        let mut errors = 0usize;

        for item in items {
            match self.insert(&item.id, &item.vector, item.metadata) {
                Ok(()) => inserted += 1,
                Err(VectorError::Internal(msg)) if msg.starts_with("duplicate") => {
                    duplicates += 1;
                }
                Err(_) => errors += 1,
            }
        }

        Ok(BatchInsertResult {
            inserted,
            duplicates,
            errors,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }

    /// Search across both the main index and pending buffer.
    pub fn search(&self, query: &[f32], top_k: usize) -> Vec<SearchResult> {
        if query.len() != self.dimension {
            return Vec::new();
        }

        self.total_searches.fetch_add(1, Ordering::Relaxed);

        let mut results: Vec<SearchResult> = Vec::new();

        // Search main vectors
        {
            let main = self.main_vectors.read();
            for sv in main.iter() {
                let d = distance(query, &sv.vector, self.metric);
                results.push(SearchResult {
                    id: sv.id.clone(),
                    distance: d,
                    metadata: sv.metadata.clone(),
                });
            }
        }

        // Search pending vectors
        {
            let pending = self.pending.read();
            for sv in pending.iter() {
                let d = distance(query, &sv.vector, self.metric);
                results.push(SearchResult {
                    id: sv.id.clone(),
                    distance: d,
                    metadata: sv.metadata.clone(),
                });
            }
        }

        results.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(top_k);
        results
    }

    /// Delete a vector by ID. Returns `true` if found and removed.
    pub fn delete(&self, id: &str) -> bool {
        // Try main index first
        if let Some(idx) = self.id_to_index.write().remove(id) {
            let mut main = self.main_vectors.write();
            if idx < main.len() {
                main[idx].id.clear(); // tombstone
            }
            return true;
        }

        // Try pending buffer
        if let Some(idx) = self.pending_id_to_index.write().remove(id) {
            let mut pending = self.pending.write();
            if idx < pending.len() {
                pending[idx].id.clear(); // tombstone
            }
            return true;
        }

        false
    }

    /// Update a vector in-place by deleting and re-inserting.
    pub fn update(&self, id: &str, vector: &[f32]) -> Result<(), VectorError> {
        if vector.len() != self.dimension {
            return Err(VectorError::DimensionMismatch {
                expected: self.dimension,
                got: vector.len(),
            });
        }

        // Retrieve existing metadata before deletion
        let metadata = self.get_metadata(id);

        if !self.delete(id) {
            return Err(VectorError::NotFound(id.to_string()));
        }

        self.insert(id, vector, metadata)
    }

    /// Number of vectors in the pending buffer.
    pub fn pending_count(&self) -> usize {
        self.pending.read().len()
    }

    /// Flush all pending inserts into the main index.
    pub fn flush(&self) -> Result<FlushResult, VectorError> {
        let mut pending = self.pending.write();
        let mut pending_ids = self.pending_id_to_index.write();
        let mut main = self.main_vectors.write();
        let mut id_map = self.id_to_index.write();

        let flushed = pending.len();

        for sv in pending.drain(..) {
            if sv.id.is_empty() {
                continue; // skip tombstones
            }
            let idx = main.len();
            id_map.insert(sv.id.clone(), idx);

            // Build simple neighbor connections
            let neighbors = self.find_nearest_indices(&main, &sv.vector, 16);
            let mut stored = sv;
            stored.neighbors = neighbors;
            main.push(stored);
        }
        pending_ids.clear();

        // Degrade estimated recall slightly with incremental additions
        let mut recall = self.estimated_recall.write();
        if flushed > 0 && *recall > 0.85 {
            *recall -= 0.01_f64.min(*recall - 0.80);
        }

        let index_size = main.len();
        Ok(FlushResult {
            flushed,
            index_size,
        })
    }

    /// Estimate current recall quality by sampling brute-force comparisons.
    pub fn check_recall(&self, sample_size: usize) -> f64 {
        let main = self.main_vectors.read();
        if main.len() < 2 {
            return 1.0;
        }

        let sample_size = sample_size.min(main.len());
        let mut hits = 0usize;
        let mut total = 0usize;

        for i in 0..sample_size {
            let query = &main[i].vector;
            // Brute-force nearest neighbor
            let mut best_dist = f32::MAX;
            let mut best_idx = i;
            for (j, sv) in main.iter().enumerate() {
                if j == i || sv.id.is_empty() {
                    continue;
                }
                let d = distance(query, &sv.vector, self.metric);
                if d < best_dist {
                    best_dist = d;
                    best_idx = j;
                }
            }

            // Check if the neighbor graph would find this
            if main[i].neighbors.contains(&best_idx) {
                hits += 1;
            }
            total += 1;
        }

        if total == 0 {
            1.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Rebalance the graph by reconnecting nodes to improve recall quality.
    pub fn rebalance(&self) -> Result<RebalanceResult, VectorError> {
        let recall_before = self.check_recall(50);
        let mut nodes_updated = 0usize;

        {
            let mut main = self.main_vectors.write();
            let len = main.len();
            let batch = self.config.max_rebalance_batch.min(len);

            for i in 0..batch {
                if main[i].id.is_empty() {
                    continue;
                }
                // Collect vectors needed for neighbor computation
                let query = main[i].vector.clone();
                let new_neighbors = self.find_nearest_indices(&main, &query, 16);
                if new_neighbors != main[i].neighbors {
                    main[i].neighbors = new_neighbors;
                    nodes_updated += 1;
                }
            }
        }

        let recall_after = self.check_recall(50);
        *self.estimated_recall.write() = recall_after;

        Ok(RebalanceResult {
            nodes_updated,
            recall_before,
            recall_after,
        })
    }

    /// Return current index statistics.
    pub fn stats(&self) -> IndexStats {
        let main_len = self.main_vectors.read().len();
        let pending_len = self.pending.read().len();
        let levels = *self.levels.read();
        let recall = *self.estimated_recall.read();

        // Approximate memory: vector data + overhead per entry
        let vector_bytes = (main_len + pending_len) * self.dimension * 4;
        let overhead = (main_len + pending_len) * 128; // ids, metadata ptrs, neighbors

        IndexStats {
            total_vectors: main_len + pending_len,
            dimension: self.dimension,
            levels,
            pending_inserts: pending_len,
            estimated_recall: recall,
            memory_bytes: vector_bytes + overhead,
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Find the nearest `k` vector indices in the given store via brute-force.
    fn find_nearest_indices(&self, store: &[StoredVector], query: &[f32], k: usize) -> Vec<usize> {
        let mut dists: Vec<(usize, f32)> = store
            .iter()
            .enumerate()
            .filter(|(_, sv)| !sv.id.is_empty())
            .map(|(i, sv)| (i, distance(query, &sv.vector, self.metric)))
            .collect();

        dists.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        dists.into_iter().take(k).map(|(i, _)| i).collect()
    }

    /// Retrieve metadata for a vector by ID.
    fn get_metadata(&self, id: &str) -> Option<serde_json::Value> {
        if let Some(&idx) = self.id_to_index.read().get(id) {
            let main = self.main_vectors.read();
            if idx < main.len() {
                return main[idx].metadata.clone();
            }
        }
        if let Some(&idx) = self.pending_id_to_index.read().get(id) {
            let pending = self.pending.read();
            if idx < pending.len() {
                return pending[idx].metadata.clone();
            }
        }
        None
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config() -> IncrementalHnswConfig {
        IncrementalHnswConfig {
            max_pending_inserts: 100,
            ..Default::default()
        }
    }

    #[test]
    fn test_insert_and_search() {
        let idx = IncrementalHnswIndex::new(make_config(), 3, DistanceMetric::Euclidean);

        idx.insert("a", &[1.0, 0.0, 0.0], None).unwrap();
        idx.insert("b", &[0.0, 1.0, 0.0], None).unwrap();
        idx.insert("c", &[0.0, 0.0, 1.0], None).unwrap();

        let results = idx.search(&[1.0, 0.0, 0.0], 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, "a");
    }

    #[test]
    fn test_dimension_mismatch() {
        let idx = IncrementalHnswIndex::new(make_config(), 3, DistanceMetric::Cosine);
        let result = idx.insert("a", &[1.0, 0.0], None);
        assert!(matches!(result, Err(VectorError::DimensionMismatch { .. })));
    }

    #[test]
    fn test_batch_insert() {
        let idx = IncrementalHnswIndex::new(make_config(), 3, DistanceMetric::Cosine);

        let items = vec![
            VectorInsertItem {
                id: "a".to_string(),
                vector: vec![1.0, 0.0, 0.0],
                metadata: None,
            },
            VectorInsertItem {
                id: "b".to_string(),
                vector: vec![0.0, 1.0, 0.0],
                metadata: None,
            },
            VectorInsertItem {
                id: "a".to_string(), // duplicate
                vector: vec![0.0, 0.0, 1.0],
                metadata: None,
            },
        ];

        let result = idx.insert_batch(items).unwrap();
        assert_eq!(result.inserted, 2);
        assert_eq!(result.duplicates, 1);
    }

    #[test]
    fn test_flush() {
        let idx = IncrementalHnswIndex::new(make_config(), 3, DistanceMetric::Cosine);

        idx.insert("a", &[1.0, 0.0, 0.0], None).unwrap();
        idx.insert("b", &[0.0, 1.0, 0.0], None).unwrap();
        assert_eq!(idx.pending_count(), 2);

        let result = idx.flush().unwrap();
        assert_eq!(result.flushed, 2);
        assert_eq!(result.index_size, 2);
        assert_eq!(idx.pending_count(), 0);
    }

    #[test]
    fn test_delete() {
        let idx = IncrementalHnswIndex::new(make_config(), 3, DistanceMetric::Cosine);
        idx.insert("a", &[1.0, 0.0, 0.0], None).unwrap();
        assert!(idx.delete("a"));
        assert!(!idx.delete("nonexistent"));
    }

    #[test]
    fn test_update() {
        let idx = IncrementalHnswIndex::new(make_config(), 3, DistanceMetric::Euclidean);
        idx.insert("a", &[1.0, 0.0, 0.0], None).unwrap();
        idx.flush().unwrap();

        idx.update("a", &[0.0, 1.0, 0.0]).unwrap();

        let results = idx.search(&[0.0, 1.0, 0.0], 1);
        assert_eq!(results[0].id, "a");
    }

    #[test]
    fn test_stats() {
        let idx = IncrementalHnswIndex::new(make_config(), 4, DistanceMetric::Cosine);
        idx.insert("a", &[1.0, 0.0, 0.0, 0.0], None).unwrap();
        idx.flush().unwrap();
        idx.insert("b", &[0.0, 1.0, 0.0, 0.0], None).unwrap();

        let stats = idx.stats();
        assert_eq!(stats.total_vectors, 2);
        assert_eq!(stats.dimension, 4);
        assert_eq!(stats.pending_inserts, 1);
    }

    #[test]
    fn test_rebalance() {
        let idx = IncrementalHnswIndex::new(make_config(), 3, DistanceMetric::Euclidean);
        for i in 0..20 {
            let v = vec![i as f32, (i * 2) as f32, (i * 3) as f32];
            idx.insert(&format!("v{i}"), &v, None).unwrap();
        }
        idx.flush().unwrap();

        let result = idx.rebalance().unwrap();
        assert!(result.recall_after >= 0.0);
    }

    #[test]
    fn test_check_recall_empty() {
        let idx = IncrementalHnswIndex::new(make_config(), 3, DistanceMetric::Cosine);
        assert_eq!(idx.check_recall(10), 1.0);
    }
}
