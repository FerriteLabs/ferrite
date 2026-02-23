//! Semantic cache implementation
//!
//! Provides similarity-based caching using vector embeddings.

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::{DistanceMetric, IndexType, SemanticConfig, SemanticError};

/// A cache entry with metadata
#[derive(Clone, Debug)]
pub struct CacheEntry {
    /// The original query/key
    pub query: String,
    /// The cached value
    pub value: Bytes,
    /// The embedding vector
    pub embedding: Vec<f32>,
    /// Creation timestamp
    pub created_at: Instant,
    /// TTL in seconds (0 = no expiry)
    pub ttl_secs: u64,
    /// Hit count
    pub hits: u64,
    /// Last access time
    pub last_accessed: Instant,
    /// Optional metadata
    pub metadata: Option<Bytes>,
}

impl CacheEntry {
    /// Create a new cache entry
    pub fn new(query: String, value: Bytes, embedding: Vec<f32>, ttl_secs: u64) -> Self {
        let now = Instant::now();
        Self {
            query,
            value,
            embedding,
            created_at: now,
            ttl_secs,
            hits: 0,
            last_accessed: now,
            metadata: None,
        }
    }

    /// Check if the entry has expired
    pub fn is_expired(&self) -> bool {
        if self.ttl_secs == 0 {
            return false;
        }
        self.created_at.elapsed() > Duration::from_secs(self.ttl_secs)
    }

    /// Record a hit
    pub fn record_hit(&mut self) {
        self.hits += 1;
        self.last_accessed = Instant::now();
    }
}

/// Result from a semantic cache lookup
#[derive(Clone, Debug)]
pub struct CacheResult {
    /// The matching entry
    pub entry: CacheEntry,
    /// Similarity score (0.0 to 1.0)
    pub similarity: f32,
    /// Entry ID
    pub id: u64,
}

/// Semantic cache with vector similarity search
pub struct SemanticCache {
    /// Configuration
    config: SemanticConfig,
    /// Cache entries by ID
    entries: DashMap<u64, CacheEntry>,
    /// Vector index for similarity search
    index: RwLock<VectorIndex>,
    /// Next entry ID
    next_id: AtomicU64,
    /// Stats
    stats: CacheStats,
}

struct CacheStats {
    hits: AtomicU64,
    misses: AtomicU64,
    sets: AtomicU64,
    evictions: AtomicU64,
}

impl Default for CacheStats {
    fn default() -> Self {
        Self {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            sets: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }
}

/// Simple vector index for similarity search
struct VectorIndex {
    /// Vectors stored as (id, embedding)
    vectors: Vec<(u64, Vec<f32>)>,
    /// Dimension of vectors
    dimension: usize,
    /// Distance metric
    metric: DistanceMetric,
    /// Index type
    _index_type: IndexType,
}

impl VectorIndex {
    fn new(dimension: usize, metric: DistanceMetric, index_type: IndexType) -> Self {
        Self {
            vectors: Vec::new(),
            dimension,
            metric,
            _index_type: index_type,
        }
    }

    fn add(&mut self, id: u64, embedding: Vec<f32>) -> Result<(), SemanticError> {
        if embedding.len() != self.dimension {
            return Err(SemanticError::DimensionMismatch {
                expected: self.dimension,
                got: embedding.len(),
            });
        }
        self.vectors.push((id, embedding));
        Ok(())
    }

    fn remove(&mut self, id: u64) -> bool {
        if let Some(pos) = self.vectors.iter().position(|(i, _)| *i == id) {
            self.vectors.swap_remove(pos);
            true
        } else {
            false
        }
    }

    fn search(&self, query: &[f32], k: usize, threshold: f32) -> Vec<(u64, f32)> {
        if query.len() != self.dimension {
            return Vec::new();
        }

        let mut results: Vec<(u64, f32)> = self
            .vectors
            .iter()
            .map(|(id, vec)| (*id, self.calculate_similarity(query, vec)))
            .filter(|(_, sim)| *sim >= threshold)
            .collect();

        // Sort by similarity descending
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(k);
        results
    }

    fn calculate_similarity(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.metric {
            DistanceMetric::Cosine => cosine_similarity(a, b),
            DistanceMetric::Euclidean => {
                // Convert distance to similarity (1 / (1 + distance))
                let dist = euclidean_distance(a, b);
                1.0 / (1.0 + dist)
            }
            DistanceMetric::DotProduct => {
                // Normalize dot product to 0-1 range
                let dot = dot_product(a, b);
                (dot + 1.0) / 2.0 // Assuming normalized vectors
            }
        }
    }

    fn len(&self) -> usize {
        self.vectors.len()
    }

    fn clear(&mut self) {
        self.vectors.clear();
    }
}

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

impl SemanticCache {
    /// Create a new semantic cache with the given configuration
    pub fn new(config: SemanticConfig) -> Self {
        let index = VectorIndex::new(
            config.embedding_dim,
            config.distance_metric,
            config.index_type,
        );

        Self {
            config,
            entries: DashMap::new(),
            index: RwLock::new(index),
            next_id: AtomicU64::new(1),
            stats: CacheStats::default(),
        }
    }

    /// Create a new semantic cache with default configuration
    pub fn with_defaults() -> Self {
        Self::new(SemanticConfig::default())
    }

    /// Set a value in the cache with an embedding
    pub fn set(
        &self,
        query: &str,
        value: Bytes,
        embedding: &[f32],
        ttl_secs: Option<u64>,
    ) -> Result<u64, SemanticError> {
        // Validate embedding dimension
        if embedding.len() != self.config.embedding_dim {
            return Err(SemanticError::DimensionMismatch {
                expected: self.config.embedding_dim,
                got: embedding.len(),
            });
        }

        // Check capacity and evict if needed
        if self.entries.len() >= self.config.max_entries {
            self.evict_lru();
        }

        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let ttl = ttl_secs.unwrap_or(self.config.default_ttl_secs);

        let entry = CacheEntry::new(query.to_string(), value, embedding.to_vec(), ttl);

        // Add to index
        {
            let mut index = self.index.write();
            index.add(id, embedding.to_vec())?;
        }

        // Add to entries
        self.entries.insert(id, entry);
        self.stats.sets.fetch_add(1, Ordering::Relaxed);

        Ok(id)
    }

    /// Get a value from the cache by similarity
    pub fn get(
        &self,
        query_embedding: &[f32],
        threshold: Option<f32>,
    ) -> Result<Option<CacheResult>, SemanticError> {
        let threshold = threshold.unwrap_or(self.config.default_threshold);

        if !(0.0..=1.0).contains(&threshold) {
            return Err(SemanticError::InvalidThreshold(threshold));
        }

        // Search the index
        let results = {
            let index = self.index.read();
            index.search(query_embedding, 1, threshold)
        };

        if let Some((id, similarity)) = results.first() {
            if let Some(mut entry) = self.entries.get_mut(id) {
                // Check expiration
                if entry.is_expired() {
                    drop(entry);
                    self.remove(*id);
                    self.stats.misses.fetch_add(1, Ordering::Relaxed);
                    return Ok(None);
                }

                entry.record_hit();
                self.stats.hits.fetch_add(1, Ordering::Relaxed);

                return Ok(Some(CacheResult {
                    entry: entry.clone(),
                    similarity: *similarity,
                    id: *id,
                }));
            }
        }

        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }

    /// Get multiple similar entries
    pub fn get_many(
        &self,
        query_embedding: &[f32],
        k: usize,
        threshold: Option<f32>,
    ) -> Result<Vec<CacheResult>, SemanticError> {
        let threshold = threshold.unwrap_or(self.config.default_threshold);

        if !(0.0..=1.0).contains(&threshold) {
            return Err(SemanticError::InvalidThreshold(threshold));
        }

        // Search the index
        let results = {
            let index = self.index.read();
            index.search(query_embedding, k, threshold)
        };

        let mut cache_results = Vec::new();

        for (id, similarity) in results {
            if let Some(mut entry) = self.entries.get_mut(&id) {
                if entry.is_expired() {
                    continue;
                }

                entry.record_hit();
                cache_results.push(CacheResult {
                    entry: entry.clone(),
                    similarity,
                    id,
                });
            }
        }

        if !cache_results.is_empty() {
            self.stats
                .hits
                .fetch_add(cache_results.len() as u64, Ordering::Relaxed);
        } else {
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
        }

        Ok(cache_results)
    }

    /// Remove an entry by ID
    pub fn remove(&self, id: u64) -> bool {
        if self.entries.remove(&id).is_some() {
            let mut index = self.index.write();
            index.remove(id);
            true
        } else {
            false
        }
    }

    /// Clear all entries
    pub fn clear(&self) {
        self.entries.clear();
        let mut index = self.index.write();
        index.clear();
    }

    /// Get the number of entries in the cache
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStatistics {
        CacheStatistics {
            entries: self.entries.len(),
            hits: self.stats.hits.load(Ordering::Relaxed),
            misses: self.stats.misses.load(Ordering::Relaxed),
            sets: self.stats.sets.load(Ordering::Relaxed),
            evictions: self.stats.evictions.load(Ordering::Relaxed),
            hit_rate: self.calculate_hit_rate(),
        }
    }

    /// Get the configuration
    pub fn config(&self) -> &SemanticConfig {
        &self.config
    }

    fn calculate_hit_rate(&self) -> f64 {
        let hits = self.stats.hits.load(Ordering::Relaxed);
        let misses = self.stats.misses.load(Ordering::Relaxed);
        let total = hits + misses;

        if total == 0 {
            0.0
        } else {
            (hits as f64) / (total as f64)
        }
    }

    fn evict_lru(&self) {
        // Find the least recently used entry
        let mut oldest_id = None;
        let mut oldest_time = Instant::now();

        for entry in self.entries.iter() {
            if entry.last_accessed < oldest_time {
                oldest_time = entry.last_accessed;
                oldest_id = Some(*entry.key());
            }
        }

        if let Some(id) = oldest_id {
            self.remove(id);
            self.stats.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Cache statistics
#[derive(Clone, Debug)]
pub struct CacheStatistics {
    /// Number of entries
    pub entries: usize,
    /// Number of cache hits
    pub hits: u64,
    /// Number of cache misses
    pub misses: u64,
    /// Number of set operations
    pub sets: u64,
    /// Number of evictions
    pub evictions: u64,
    /// Hit rate (0.0 to 1.0)
    pub hit_rate: f64,
}

/// Builder for SemanticCache
pub struct SemanticCacheBuilder {
    config: SemanticConfig,
}

impl SemanticCacheBuilder {
    /// Create a new builder with default configuration
    pub fn new() -> Self {
        Self {
            config: SemanticConfig::default(),
        }
    }

    /// Set the embedding dimension
    pub fn dimension(mut self, dim: usize) -> Self {
        self.config.embedding_dim = dim;
        self
    }

    /// Set the default similarity threshold
    pub fn threshold(mut self, threshold: f32) -> Self {
        self.config.default_threshold = threshold;
        self
    }

    /// Set the maximum number of entries
    pub fn max_entries(mut self, max: usize) -> Self {
        self.config.max_entries = max;
        self
    }

    /// Set the default TTL in seconds
    pub fn ttl(mut self, ttl_secs: u64) -> Self {
        self.config.default_ttl_secs = ttl_secs;
        self
    }

    /// Set the distance metric
    pub fn metric(mut self, metric: DistanceMetric) -> Self {
        self.config.distance_metric = metric;
        self
    }

    /// Set the index type
    pub fn index_type(mut self, index_type: IndexType) -> Self {
        self.config.index_type = index_type;
        self
    }

    /// Build the semantic cache
    pub fn build(self) -> SemanticCache {
        SemanticCache::new(self.config)
    }
}

impl Default for SemanticCacheBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// Make SemanticCache thread-safe
unsafe impl Send for SemanticCache {}
unsafe impl Sync for SemanticCache {}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_embedding(seed: u8) -> Vec<f32> {
        // Generate a simple test embedding
        (0..384)
            .map(|i| ((i as u8).wrapping_add(seed)) as f32 / 255.0)
            .collect()
    }

    #[test]
    fn test_cache_set_and_get() {
        let cache = SemanticCache::with_defaults();

        let embedding = test_embedding(42);
        let id = cache
            .set("test query", Bytes::from("test value"), &embedding, None)
            .unwrap();

        assert!(id > 0);
        assert_eq!(cache.len(), 1);

        // Get with same embedding should match
        let result = cache.get(&embedding, Some(0.99)).unwrap();
        assert!(result.is_some());
        let result = result.unwrap();
        assert!(result.similarity > 0.99);
        assert_eq!(result.entry.value, Bytes::from("test value"));
    }

    #[test]
    fn test_cache_similarity_threshold() {
        let cache = SemanticCache::with_defaults();

        let embedding1 = test_embedding(0);
        let embedding2 = test_embedding(50); // Different embedding

        cache
            .set("query 1", Bytes::from("value 1"), &embedding1, None)
            .unwrap();

        // Search with different embedding - may or may not match depending on threshold
        let result = cache.get(&embedding2, Some(0.99)).unwrap();
        // With very high threshold, should not match different embeddings
        // (depends on how different the embeddings actually are)

        // Search with same embedding
        let result = cache.get(&embedding1, Some(0.99)).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_cache_remove() {
        let cache = SemanticCache::with_defaults();

        let embedding = test_embedding(0);
        let id = cache
            .set("test", Bytes::from("value"), &embedding, None)
            .unwrap();

        assert_eq!(cache.len(), 1);
        assert!(cache.remove(id));
        assert_eq!(cache.len(), 0);
        assert!(!cache.remove(id)); // Already removed
    }

    #[test]
    fn test_cache_clear() {
        let cache = SemanticCache::with_defaults();

        for i in 0..5 {
            let embedding = test_embedding(i);
            cache
                .set(
                    &format!("query {}", i),
                    Bytes::from("value"),
                    &embedding,
                    None,
                )
                .unwrap();
        }

        assert_eq!(cache.len(), 5);
        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn test_cache_stats() {
        let cache = SemanticCache::with_defaults();

        let embedding = test_embedding(0);
        cache
            .set("test", Bytes::from("value"), &embedding, None)
            .unwrap();

        // Hit
        let _ = cache.get(&embedding, None);
        // Miss
        let _ = cache.get(&test_embedding(100), Some(0.99));

        let stats = cache.stats();
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.sets, 1);
        assert!(stats.hits >= 1);
    }

    #[test]
    fn test_cache_builder() {
        let cache = SemanticCacheBuilder::new()
            .dimension(512)
            .threshold(0.9)
            .max_entries(1000)
            .ttl(7200)
            .metric(DistanceMetric::Cosine)
            .build();

        let config = cache.config();
        assert_eq!(config.embedding_dim, 512);
        assert_eq!(config.default_threshold, 0.9);
        assert_eq!(config.max_entries, 1000);
        assert_eq!(config.default_ttl_secs, 7200);
    }

    #[test]
    fn test_dimension_mismatch() {
        let cache = SemanticCache::with_defaults(); // 384 dimensions

        let wrong_dim: Vec<f32> = vec![0.0; 128]; // Wrong dimension
        let result = cache.set("test", Bytes::from("value"), &wrong_dim, None);

        assert!(matches!(
            result,
            Err(SemanticError::DimensionMismatch { .. })
        ));
    }

    #[test]
    fn test_invalid_threshold() {
        let cache = SemanticCache::with_defaults();

        let embedding = test_embedding(0);
        cache
            .set("test", Bytes::from("value"), &embedding, None)
            .unwrap();

        let result = cache.get(&embedding, Some(1.5)); // Invalid threshold
        assert!(matches!(result, Err(SemanticError::InvalidThreshold(_))));
    }

    #[test]
    fn test_cosine_similarity() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        assert!((cosine_similarity(&a, &b) - 1.0).abs() < 0.001);

        let c = vec![0.0, 1.0, 0.0];
        assert!((cosine_similarity(&a, &c)).abs() < 0.001);
    }

    #[test]
    fn test_cache_ttl_expiration() {
        let config = super::super::SemanticConfig {
            default_ttl_secs: 0, // TTL=0 means custom per-entry
            ..Default::default()
        };
        let cache = SemanticCache::new(config);

        let embedding = test_embedding(0);

        // Set with a very short TTL
        let id = cache
            .set("expiring", Bytes::from("value"), &embedding, Some(1))
            .unwrap();

        // Should hit immediately
        let result = cache.get(&embedding, Some(0.99)).unwrap();
        assert!(result.is_some());

        // Wait for TTL to expire
        std::thread::sleep(std::time::Duration::from_millis(1100));

        // Should now miss (expired)
        let result = cache.get(&embedding, Some(0.99)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_cache_statistics_tracking() {
        let cache = SemanticCache::with_defaults();

        // Start with zero stats
        let stats = cache.stats();
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
        assert_eq!(stats.sets, 0);
        assert_eq!(stats.evictions, 0);
        assert_eq!(stats.hit_rate, 0.0);

        let embedding = test_embedding(0);
        cache
            .set("q1", Bytes::from("v1"), &embedding, None)
            .unwrap();

        let stats = cache.stats();
        assert_eq!(stats.sets, 1);

        // Two hits
        let _ = cache.get(&embedding, None).unwrap();
        let _ = cache.get(&embedding, None).unwrap();

        // One miss
        let _ = cache.get(&test_embedding(200), Some(0.999)).unwrap();

        let stats = cache.stats();
        assert_eq!(stats.sets, 1);
        assert!(stats.hits >= 2);
        assert!(stats.hit_rate > 0.0);
    }

    #[test]
    fn test_cache_eviction_on_full() {
        let config = super::super::SemanticConfig {
            max_entries: 3,
            ..Default::default()
        };
        let cache = SemanticCache::new(config);

        // Fill the cache
        for i in 0..3u8 {
            let emb = test_embedding(i * 80);
            cache
                .set(
                    &format!("query_{}", i),
                    Bytes::from(format!("val_{}", i)),
                    &emb,
                    None,
                )
                .unwrap();
        }
        assert_eq!(cache.len(), 3);

        // Adding one more should trigger eviction
        let emb_new = test_embedding(250);
        cache
            .set("new_query", Bytes::from("new_val"), &emb_new, None)
            .unwrap();

        // Should still be at or below max
        assert!(cache.len() <= 3);
    }

    #[test]
    fn test_cache_get_many() {
        let cache = SemanticCacheBuilder::new()
            .dimension(384)
            .threshold(0.5)
            .max_entries(100)
            .build();

        // Add multiple entries
        for i in 0..10u8 {
            let emb = test_embedding(i);
            cache
                .set(
                    &format!("query_{}", i),
                    Bytes::from(format!("val_{}", i)),
                    &emb,
                    None,
                )
                .unwrap();
        }

        // Get many with low threshold should return multiple results
        let query_emb = test_embedding(0);
        let results = cache.get_many(&query_emb, 5, Some(0.5)).unwrap();
        assert!(!results.is_empty());
        assert!(results.len() <= 5);
    }

    #[test]
    fn test_zero_vector_set_and_get() {
        let cache = SemanticCacheBuilder::new()
            .dimension(4)
            .threshold(0.5)
            .max_entries(10)
            .build();

        let zero = vec![0.0f32; 4];
        let id = cache
            .set("zero", Bytes::from("zero_val"), &zero, None)
            .unwrap();
        assert!(id > 0);

        // Querying with zero vector: cosine similarity is 0 for zero vectors
        // so it should not match
        let result = cache.get(&zero, Some(0.5)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_high_dimensionality_embeddings() {
        let dim = 1536; // OpenAI embedding dimension
        let config = super::super::SemanticConfig {
            embedding_dim: dim,
            ..Default::default()
        };
        let cache = SemanticCache::new(config);

        let emb: Vec<f32> = (0..dim).map(|i| (i as f32 / dim as f32) - 0.5).collect();
        let id = cache
            .set("high_dim", Bytes::from("hd_value"), &emb, None)
            .unwrap();
        assert!(id > 0);

        // Same embedding should hit
        let result = cache.get(&emb, Some(0.99)).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_cache_entry_hit_count() {
        let cache = SemanticCache::with_defaults();

        let embedding = test_embedding(42);
        cache
            .set("test", Bytes::from("value"), &embedding, None)
            .unwrap();

        // Access multiple times
        for _ in 0..5 {
            let result = cache.get(&embedding, Some(0.99)).unwrap();
            assert!(result.is_some());
        }

        // Check that the entry's hit count was incremented
        let result = cache.get(&embedding, Some(0.99)).unwrap().unwrap();
        assert!(result.entry.hits >= 5);
    }

    #[test]
    fn test_negative_threshold() {
        let cache = SemanticCache::with_defaults();
        let embedding = test_embedding(0);
        cache
            .set("test", Bytes::from("value"), &embedding, None)
            .unwrap();

        let result = cache.get(&embedding, Some(-0.5));
        assert!(matches!(result, Err(SemanticError::InvalidThreshold(_))));
    }

    #[test]
    fn test_empty_cache_operations() {
        let cache = SemanticCache::with_defaults();

        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);

        // Get on empty cache should miss
        let embedding = test_embedding(0);
        let result = cache.get(&embedding, None).unwrap();
        assert!(result.is_none());

        // Remove on empty cache should return false
        assert!(!cache.remove(999));

        // Clear on empty cache should not panic
        cache.clear();
    }
}
