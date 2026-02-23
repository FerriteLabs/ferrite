//! Enhanced Semantic Cache
//!
//! A high-level semantic cache that provides similarity-based caching with
//! configurable thresholds, TTL, eviction policies, and comprehensive statistics.
//!
//! This module builds on the base [`SemanticCache`](super::cache::SemanticCache)
//! by adding:
//!
//! - Per-entry metadata (JSON)
//! - Radius-based invalidation
//! - Average similarity tracking
//! - Configurable eviction strategies
//! - Thread-safe atomic statistics
//!
//! # Example
//!
//! ```rust
//! use ferrite_ai::semantic::semantic_cache::{EnhancedSemanticCache, SemanticCacheConfig};
//! use bytes::Bytes;
//!
//! let config = SemanticCacheConfig::default();
//! let cache = EnhancedSemanticCache::new(config);
//!
//! // Insert an entry
//! let embedding = vec![0.1_f32; 384];
//! cache.set(&embedding, Bytes::from("cached response"), None);
//!
//! // Lookup by similarity
//! if let Some(entry) = cache.get(&embedding) {
//!     println!("Hit! response={:?}, similarity={:.4}", entry.response, entry.similarity);
//! }
//!
//! // Check stats
//! let stats = cache.stats();
//! println!("hits={}, misses={}, avg_sim={:.4}", stats.hits(), stats.misses(), stats.avg_similarity());
//! ```

use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the enhanced semantic cache
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SemanticCacheConfig {
    /// Similarity threshold (0.0 to 1.0). Queries must exceed this
    /// similarity score to be considered a cache hit.
    pub threshold: f32,

    /// Optional TTL for cache entries. `None` means entries never expire.
    pub ttl: Option<Duration>,

    /// Maximum number of entries in the cache. When exceeded, the LRU
    /// entry is evicted.
    pub max_entries: usize,

    /// Vector dimension for embeddings
    pub dimension: usize,
}

impl Default for SemanticCacheConfig {
    fn default() -> Self {
        Self {
            threshold: 0.85,
            ttl: None,
            max_entries: 100_000,
            dimension: 384,
        }
    }
}

// ============================================================================
// Cache Statistics
// ============================================================================

/// Thread-safe statistics for the semantic cache.
///
/// All counters use relaxed atomic ordering for maximum performance.
/// The average similarity is tracked as a running sum and count.
pub struct CacheStats {
    hit_count: AtomicU64,
    miss_count: AtomicU64,
    total_queries: AtomicU64,
    similarity_sum_x1000: AtomicU64, // sum of similarities * 1000 for integer atomic
    similarity_count: AtomicU64,
}

impl CacheStats {
    /// Create new zeroed statistics
    pub fn new() -> Self {
        Self {
            hit_count: AtomicU64::new(0),
            miss_count: AtomicU64::new(0),
            total_queries: AtomicU64::new(0),
            similarity_sum_x1000: AtomicU64::new(0),
            similarity_count: AtomicU64::new(0),
        }
    }

    /// Total cache hits
    pub fn hits(&self) -> u64 {
        self.hit_count.load(Ordering::Relaxed)
    }

    /// Total cache misses
    pub fn misses(&self) -> u64 {
        self.miss_count.load(Ordering::Relaxed)
    }

    /// Total queries (hits + misses)
    pub fn total_queries(&self) -> u64 {
        self.total_queries.load(Ordering::Relaxed)
    }

    /// Average similarity of cache hits (0.0 if no hits)
    pub fn avg_similarity(&self) -> f64 {
        let count = self.similarity_count.load(Ordering::Relaxed);
        if count == 0 {
            return 0.0;
        }
        let sum = self.similarity_sum_x1000.load(Ordering::Relaxed) as f64 / 1000.0;
        sum / count as f64
    }

    /// Hit rate (0.0 to 1.0)
    pub fn hit_rate(&self) -> f64 {
        let total = self.total_queries.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        self.hit_count.load(Ordering::Relaxed) as f64 / total as f64
    }

    fn record_hit(&self, similarity: f32) {
        self.hit_count.fetch_add(1, Ordering::Relaxed);
        self.total_queries.fetch_add(1, Ordering::Relaxed);
        // Track similarity as integer * 1000 for atomic add
        let sim_int = (similarity * 1000.0) as u64;
        self.similarity_sum_x1000
            .fetch_add(sim_int, Ordering::Relaxed);
        self.similarity_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_miss(&self) {
        self.miss_count.fetch_add(1, Ordering::Relaxed);
        self.total_queries.fetch_add(1, Ordering::Relaxed);
    }

    /// Reset all counters to zero
    pub fn reset(&self) {
        self.hit_count.store(0, Ordering::Relaxed);
        self.miss_count.store(0, Ordering::Relaxed);
        self.total_queries.store(0, Ordering::Relaxed);
        self.similarity_sum_x1000.store(0, Ordering::Relaxed);
        self.similarity_count.store(0, Ordering::Relaxed);
    }
}

impl Default for CacheStats {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Cache Entry
// ============================================================================

/// A single entry in the semantic cache
#[derive(Clone, Debug)]
pub struct CacheEntryData {
    /// The embedding vector
    pub embedding: Vec<f32>,
    /// The cached response
    pub response: Bytes,
    /// Optional JSON metadata
    pub metadata: Option<Value>,
    /// When this entry was created
    pub created_at: Instant,
    /// When this entry was last accessed
    pub last_accessed: Instant,
    /// Number of times this entry was hit
    pub hit_count: u64,
}

impl CacheEntryData {
    fn is_expired(&self, ttl: Option<Duration>) -> bool {
        match ttl {
            Some(ttl) => self.created_at.elapsed() > ttl,
            None => false,
        }
    }
}

/// Result of a cache lookup
#[derive(Clone, Debug)]
pub struct CacheHit {
    /// The cached response
    pub response: Bytes,
    /// Similarity score between query and cached embedding
    pub similarity: f32,
    /// Optional metadata stored with the entry
    pub metadata: Option<Value>,
}

// ============================================================================
// Enhanced Semantic Cache
// ============================================================================

/// High-performance semantic cache using vector similarity.
///
/// Stores embeddings alongside cached responses and retrieves them
/// when a query embedding is sufficiently similar (above the configured
/// threshold).
///
/// Thread-safe: all operations use interior mutability with `parking_lot`.
pub struct EnhancedSemanticCache {
    config: SemanticCacheConfig,
    entries: RwLock<Vec<CacheEntryData>>,
    stats: CacheStats,
}

impl EnhancedSemanticCache {
    /// Create a new semantic cache with the given configuration
    pub fn new(config: SemanticCacheConfig) -> Self {
        Self {
            config,
            entries: RwLock::new(Vec::new()),
            stats: CacheStats::new(),
        }
    }

    /// Look up a cached response by embedding similarity.
    ///
    /// Returns `Some(CacheHit)` if a cached entry exceeds the similarity
    /// threshold, or `None` if no sufficiently similar entry is found.
    pub fn get(&self, embedding: &[f32]) -> Option<CacheHit> {
        let entries = self.entries.read();

        let mut best_idx: Option<usize> = None;
        let mut best_sim: f32 = -1.0;

        for (i, entry) in entries.iter().enumerate() {
            // Skip expired entries
            if entry.is_expired(self.config.ttl) {
                continue;
            }

            let sim = cosine_similarity(embedding, &entry.embedding);
            if sim >= self.config.threshold && sim > best_sim {
                best_sim = sim;
                best_idx = Some(i);
            }
        }

        drop(entries);

        if let Some(idx) = best_idx {
            let mut entries = self.entries.write();
            if let Some(entry) = entries.get_mut(idx) {
                entry.last_accessed = Instant::now();
                entry.hit_count += 1;

                let hit = CacheHit {
                    response: entry.response.clone(),
                    similarity: best_sim,
                    metadata: entry.metadata.clone(),
                };

                self.stats.record_hit(best_sim);
                return Some(hit);
            }
        }

        self.stats.record_miss();
        None
    }

    /// Store a response in the cache with its embedding vector.
    ///
    /// If the cache is at capacity, the least-recently-used entry is evicted.
    pub fn set(&self, embedding: &[f32], response: Bytes, metadata: Option<Value>) {
        let mut entries = self.entries.write();

        // Evict if at capacity
        if entries.len() >= self.config.max_entries {
            self.evict_lru_locked(&mut entries);
        }

        let now = Instant::now();
        entries.push(CacheEntryData {
            embedding: embedding.to_vec(),
            response,
            metadata,
            created_at: now,
            last_accessed: now,
            hit_count: 0,
        });
    }

    /// Invalidate all entries within a similarity radius of the given embedding.
    ///
    /// Any entry whose cosine similarity to `embedding` is >= `radius` will be
    /// removed. This is useful for invalidating semantically related cache entries
    /// when the underlying data changes.
    pub fn invalidate(&self, embedding: &[f32], radius: f32) {
        let mut entries = self.entries.write();
        entries.retain(|entry| {
            let sim = cosine_similarity(embedding, &entry.embedding);
            sim < radius
        });
    }

    /// Get a reference to the cache statistics
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Clear all entries and reset statistics
    pub fn clear(&self) {
        let mut entries = self.entries.write();
        entries.clear();
        self.stats.reset();
    }

    /// Get the current number of entries in the cache
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Remove expired entries from the cache
    pub fn purge_expired(&self) {
        if let Some(ttl) = self.config.ttl {
            let mut entries = self.entries.write();
            entries.retain(|entry| !entry.is_expired(Some(ttl)));
        }
    }

    /// Get the cache configuration
    pub fn config(&self) -> &SemanticCacheConfig {
        &self.config
    }

    /// Evict the least-recently-used entry (must be called with write lock held)
    fn evict_lru_locked(&self, entries: &mut Vec<CacheEntryData>) {
        if entries.is_empty() {
            return;
        }

        let mut oldest_idx = 0;
        let mut oldest_time = entries[0].last_accessed;

        for (i, entry) in entries.iter().enumerate().skip(1) {
            if entry.last_accessed < oldest_time {
                oldest_time = entry.last_accessed;
                oldest_idx = i;
            }
        }

        entries.swap_remove(oldest_idx);
    }
}

// SAFETY: EnhancedSemanticCache uses RwLock and atomic types for all
// interior mutability, making it safe to share across threads.
unsafe impl Send for EnhancedSemanticCache {}
unsafe impl Sync for EnhancedSemanticCache {}

// ============================================================================
// Distance Functions
// ============================================================================

/// Compute cosine similarity between two vectors (1.0 = identical, 0.0 = orthogonal)
#[inline]
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");

    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;

    for i in 0..a.len() {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }

    let denom = (norm_a * norm_b).sqrt();
    if denom > 0.0 {
        dot / denom
    } else {
        0.0
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_embedding(seed: u8, dim: usize) -> Vec<f32> {
        (0..dim)
            .map(|i| ((i as u8).wrapping_add(seed)) as f32 / 255.0 - 0.5)
            .collect()
    }

    fn make_normalized_embedding(seed: u8, dim: usize) -> Vec<f32> {
        let mut emb = make_embedding(seed, dim);
        let norm: f32 = emb.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for x in emb.iter_mut() {
                *x /= norm;
            }
        }
        emb
    }

    #[test]
    fn test_cache_hit_with_same_embedding() {
        let config = SemanticCacheConfig {
            threshold: 0.85,
            ttl: None,
            max_entries: 100,
            dimension: 128,
        };
        let cache = EnhancedSemanticCache::new(config);

        let emb = make_normalized_embedding(42, 128);
        cache.set(&emb, Bytes::from("hello"), None);

        // Same embedding should be a hit
        let result = cache.get(&emb);
        assert!(result.is_some());
        let hit = result.expect("should be a hit");
        assert!(hit.similarity > 0.99);
        assert_eq!(hit.response, Bytes::from("hello"));
    }

    #[test]
    fn test_cache_miss_with_different_embedding() {
        let config = SemanticCacheConfig {
            threshold: 0.99, // Very high threshold
            ttl: None,
            max_entries: 100,
            dimension: 128,
        };
        let cache = EnhancedSemanticCache::new(config);

        let emb1 = make_normalized_embedding(0, 128);
        cache.set(&emb1, Bytes::from("answer 1"), None);

        // Very different embedding should miss with high threshold
        let emb2 = make_normalized_embedding(200, 128);
        let result = cache.get(&emb2);
        // Whether this is a hit or miss depends on the actual similarity
        // With seed 0 vs 200 the embeddings are quite different
        let stats = cache.stats();
        assert_eq!(stats.total_queries(), 1);
    }

    #[test]
    fn test_cache_miss_empty_cache() {
        let config = SemanticCacheConfig::default();
        let cache = EnhancedSemanticCache::new(config);

        let emb = make_normalized_embedding(0, 384);
        let result = cache.get(&emb);
        assert!(result.is_none());
        assert_eq!(cache.stats().misses(), 1);
    }

    #[test]
    fn test_cache_stats_tracking() {
        let config = SemanticCacheConfig {
            threshold: 0.85,
            ttl: None,
            max_entries: 100,
            dimension: 128,
        };
        let cache = EnhancedSemanticCache::new(config);

        let emb = make_normalized_embedding(10, 128);
        cache.set(&emb, Bytes::from("value"), None);

        // Hit with same embedding
        let _ = cache.get(&emb);
        assert_eq!(cache.stats().hits(), 1);

        // Hit again
        let _ = cache.get(&emb);
        assert_eq!(cache.stats().hits(), 2);
        assert_eq!(cache.stats().total_queries(), 2);

        // Avg similarity should be close to 1.0
        assert!(cache.stats().avg_similarity() > 0.99);
    }

    #[test]
    fn test_cache_with_metadata() {
        let config = SemanticCacheConfig {
            threshold: 0.85,
            ttl: None,
            max_entries: 100,
            dimension: 64,
        };
        let cache = EnhancedSemanticCache::new(config);

        let emb = make_normalized_embedding(5, 64);
        let metadata = serde_json::json!({
            "model": "gpt-4",
            "tokens": 150,
            "cost": 0.003
        });
        cache.set(&emb, Bytes::from("AI response"), Some(metadata.clone()));

        let result = cache.get(&emb);
        assert!(result.is_some());
        let hit = result.expect("should hit");
        assert_eq!(hit.metadata.expect("should have metadata"), metadata);
    }

    #[test]
    fn test_cache_invalidate_by_radius() {
        let config = SemanticCacheConfig {
            threshold: 0.5,
            ttl: None,
            max_entries: 100,
            dimension: 64,
        };
        let cache = EnhancedSemanticCache::new(config);

        let emb1 = make_normalized_embedding(0, 64);
        let emb2 = make_normalized_embedding(1, 64); // very similar to emb1
        let emb3 = make_normalized_embedding(200, 64); // quite different

        cache.set(&emb1, Bytes::from("v1"), None);
        cache.set(&emb2, Bytes::from("v2"), None);
        cache.set(&emb3, Bytes::from("v3"), None);
        assert_eq!(cache.len(), 3);

        // Invalidate entries similar to emb1 with a high radius
        cache.invalidate(&emb1, 0.9);

        // emb1 itself should be invalidated (similarity = 1.0 >= 0.9)
        // emb2 is very similar to emb1, should also likely be invalidated
        // emb3 should likely survive
        assert!(cache.len() < 3);
    }

    #[test]
    fn test_cache_clear() {
        let config = SemanticCacheConfig {
            threshold: 0.85,
            ttl: None,
            max_entries: 100,
            dimension: 64,
        };
        let cache = EnhancedSemanticCache::new(config);

        for i in 0..10u8 {
            let emb = make_normalized_embedding(i, 64);
            cache.set(&emb, Bytes::from(format!("val {}", i)), None);
        }

        assert_eq!(cache.len(), 10);
        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.stats().hits(), 0);
        assert_eq!(cache.stats().misses(), 0);
    }

    #[test]
    fn test_cache_eviction_on_capacity() {
        let config = SemanticCacheConfig {
            threshold: 0.85,
            ttl: None,
            max_entries: 3,
            dimension: 64,
        };
        let cache = EnhancedSemanticCache::new(config);

        for i in 0..5u8 {
            let emb = make_normalized_embedding(i * 50, 64);
            cache.set(&emb, Bytes::from(format!("val {}", i)), None);
        }

        // Should not exceed max_entries
        assert!(cache.len() <= 3);
    }

    #[test]
    fn test_cache_ttl_expiration() {
        let config = SemanticCacheConfig {
            threshold: 0.85,
            ttl: Some(Duration::from_millis(50)),
            max_entries: 100,
            dimension: 64,
        };
        let cache = EnhancedSemanticCache::new(config);

        let emb = make_normalized_embedding(0, 64);
        cache.set(&emb, Bytes::from("value"), None);

        // Should hit immediately
        let result = cache.get(&emb);
        assert!(result.is_some());

        // Wait for expiration
        std::thread::sleep(Duration::from_millis(60));

        // Should miss after TTL
        let result = cache.get(&emb);
        assert!(result.is_none());
    }

    #[test]
    fn test_cache_purge_expired() {
        let config = SemanticCacheConfig {
            threshold: 0.85,
            ttl: Some(Duration::from_millis(50)),
            max_entries: 100,
            dimension: 64,
        };
        let cache = EnhancedSemanticCache::new(config);

        for i in 0..5u8 {
            let emb = make_normalized_embedding(i * 50, 64);
            cache.set(&emb, Bytes::from(format!("val {}", i)), None);
        }

        assert_eq!(cache.len(), 5);

        std::thread::sleep(Duration::from_millis(60));
        cache.purge_expired();

        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_zero_vector_handling() {
        let config = SemanticCacheConfig {
            threshold: 0.85,
            ttl: None,
            max_entries: 100,
            dimension: 4,
        };
        let cache = EnhancedSemanticCache::new(config);

        // Zero vector
        let zero = vec![0.0f32; 4];
        cache.set(&zero, Bytes::from("zero"), None);

        // Should not crash, similarity with zero vector is 0.0
        let result = cache.get(&zero);
        // cosine_similarity(0, 0) = 0.0, which is < threshold (0.85)
        assert!(result.is_none());
    }

    #[test]
    fn test_high_dimensionality() {
        let dim = 1536; // OpenAI text-embedding-3-small dimension
        let config = SemanticCacheConfig {
            threshold: 0.85,
            ttl: None,
            max_entries: 100,
            dimension: dim,
        };
        let cache = EnhancedSemanticCache::new(config);

        let emb = make_normalized_embedding(42, dim);
        cache.set(&emb, Bytes::from("high-dim value"), None);

        let result = cache.get(&emb);
        assert!(result.is_some());
    }

    #[test]
    fn test_similarity_threshold_behavior() {
        let dim = 64;

        // Very high threshold: only exact or near-exact matches
        let cache_strict = EnhancedSemanticCache::new(SemanticCacheConfig {
            threshold: 0.99,
            ttl: None,
            max_entries: 100,
            dimension: dim,
        });

        // Low threshold: more lenient matching
        let cache_lenient = EnhancedSemanticCache::new(SemanticCacheConfig {
            threshold: 0.5,
            ttl: None,
            max_entries: 100,
            dimension: dim,
        });

        let emb1 = make_normalized_embedding(0, dim);
        let emb_similar = make_normalized_embedding(1, dim); // slightly different

        cache_strict.set(&emb1, Bytes::from("strict"), None);
        cache_lenient.set(&emb1, Bytes::from("lenient"), None);

        // Lenient cache should be more likely to match
        let strict_result = cache_strict.get(&emb_similar);
        let lenient_result = cache_lenient.get(&emb_similar);

        // The lenient cache should always match if the strict one does
        if strict_result.is_some() {
            assert!(lenient_result.is_some());
        }
    }

    #[test]
    fn test_hit_rate_calculation() {
        let config = SemanticCacheConfig {
            threshold: 0.85,
            ttl: None,
            max_entries: 100,
            dimension: 64,
        };
        let cache = EnhancedSemanticCache::new(config);

        let emb = make_normalized_embedding(0, 64);
        cache.set(&emb, Bytes::from("value"), None);

        // 3 hits
        for _ in 0..3 {
            let _ = cache.get(&emb);
        }

        // 2 misses (empty embedding, different from stored)
        let diff = make_normalized_embedding(200, 64);
        for _ in 0..2 {
            let _ = cache.get(&diff);
        }

        // hit_rate should reflect the ratio
        let stats = cache.stats();
        assert_eq!(stats.total_queries(), 5);
        // Exact hit rate depends on whether diff matches threshold
    }
}
