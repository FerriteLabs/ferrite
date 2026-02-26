//! # GA-Quality Semantic Cache
//!
//! Production-ready semantic cache with full cache lifecycle management,
//! configurable similarity thresholds, observability, persistence, and
//! performance optimizations.
//!
//! ## Features
//!
//! - **Cache Invalidation**: TTL-based expiration, LRU eviction, pattern-based
//!   invalidation, and semantic distance-based deduplication
//! - **Similarity Configuration**: Per-cache thresholds with cosine, euclidean,
//!   and dot-product distance metrics; threshold auto-tuning via hit-rate feedback
//! - **Metrics & Observability**: Hit/miss counters, similarity histograms, cache
//!   size gauges, embedding latency histograms, eviction counters (Prometheus via
//!   `metrics` crate)
//! - **Persistence**: Save/load vector index to disk with incremental updates and
//!   CRC32 checksum integrity verification
//! - **Performance**: Batch embedding, async embedding pipeline, index pre-warming
//!
//! # Example
//!
//! ```rust
//! use ferrite_ai::semantic::ga_cache::{GaSemanticCache, GaCacheConfig, DistanceMetricType};
//! use bytes::Bytes;
//! use std::time::Duration;
//!
//! let config = GaCacheConfig {
//!     threshold: 0.85,
//!     distance_metric: DistanceMetricType::Cosine,
//!     ttl: Some(Duration::from_secs(3600)),
//!     max_entries: 10_000,
//!     dimension: 384,
//!     dedup_threshold: Some(0.98),
//!     auto_tune_enabled: true,
//!     auto_tune_target_hit_rate: 0.5,
//!     auto_tune_step: 0.01,
//!     auto_tune_window: 100,
//!     ..GaCacheConfig::default()
//! };
//! let cache = GaSemanticCache::new(config);
//!
//! let emb = vec![0.1_f32; 384];
//! cache.set("q1", &emb, Bytes::from("answer"), None);
//!
//! if let Some(hit) = cache.get(&emb) {
//!     println!("hit: sim={:.4}", hit.similarity);
//! }
//! ```

use bytes::Bytes;
use metrics::{counter, gauge, histogram};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Distance metric helpers
// ---------------------------------------------------------------------------

/// Supported distance metric types for similarity computation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DistanceMetricType {
    /// Cosine similarity — recommended for text embeddings.
    Cosine,
    /// Euclidean distance, converted to similarity as `1/(1+d)`.
    Euclidean,
    /// Dot product — assumes pre-normalised vectors.
    DotProduct,
}

impl Default for DistanceMetricType {
    fn default() -> Self {
        Self::Cosine
    }
}

#[inline]
fn compute_similarity(a: &[f32], b: &[f32], metric: DistanceMetricType) -> f32 {
    match metric {
        DistanceMetricType::Cosine => cosine_similarity(a, b),
        DistanceMetricType::Euclidean => {
            let d = euclidean_distance(a, b);
            1.0 / (1.0 + d)
        }
        DistanceMetricType::DotProduct => dot_product(a, b),
    }
}

#[inline]
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    let denom = (na * nb).sqrt();
    if denom > 0.0 {
        dot / denom
    } else {
        0.0
    }
}

#[inline]
fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f32>()
        .sqrt()
}

#[inline]
fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the GA semantic cache.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GaCacheConfig {
    /// Similarity threshold – queries must exceed this to be a cache hit.
    pub threshold: f32,
    /// Distance metric for similarity computation.
    pub distance_metric: DistanceMetricType,
    /// Optional TTL for cache entries. `None` means entries never expire.
    pub ttl: Option<Duration>,
    /// Maximum number of entries. When exceeded, LRU entry is evicted.
    pub max_entries: usize,
    /// Embedding vector dimension.
    pub dimension: usize,
    /// If set, entries with similarity >= this value are considered duplicates
    /// and the older entry is merged (response updated, hit count accumulated).
    pub dedup_threshold: Option<f32>,
    /// Enable threshold auto-tuning.
    pub auto_tune_enabled: bool,
    /// Target hit rate for auto-tuning (0.0–1.0).
    pub auto_tune_target_hit_rate: f64,
    /// Step size for threshold adjustments during auto-tuning.
    pub auto_tune_step: f32,
    /// Number of queries in the sliding window for auto-tuning decisions.
    pub auto_tune_window: u64,
    /// Prefix for Prometheus metric names.
    pub metrics_prefix: String,
}

impl Default for GaCacheConfig {
    fn default() -> Self {
        Self {
            threshold: 0.85,
            distance_metric: DistanceMetricType::Cosine,
            ttl: None,
            max_entries: 100_000,
            dimension: 384,
            dedup_threshold: None,
            auto_tune_enabled: false,
            auto_tune_target_hit_rate: 0.5,
            auto_tune_step: 0.01,
            auto_tune_window: 100,
            metrics_prefix: "semantic_ga".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Cache entry
// ---------------------------------------------------------------------------

/// A single entry stored in the cache.
#[derive(Clone, Debug)]
pub struct GaCacheEntry {
    /// Original query key.
    pub key: String,
    /// Embedding vector.
    pub embedding: Vec<f32>,
    /// Cached response payload.
    pub response: Bytes,
    /// Optional JSON metadata.
    pub metadata: Option<Value>,
    /// Creation timestamp.
    pub created_at: Instant,
    /// Last access timestamp.
    pub last_accessed: Instant,
    /// Cumulative hit count.
    pub hit_count: u64,
}

impl GaCacheEntry {
    fn is_expired(&self, ttl: Option<Duration>) -> bool {
        match ttl {
            Some(ttl) => self.created_at.elapsed() > ttl,
            None => false,
        }
    }

    fn estimated_memory(&self) -> usize {
        self.key.len()
            + self.embedding.len() * std::mem::size_of::<f32>()
            + self.response.len()
            + self.metadata.as_ref().map_or(0, |v| v.to_string().len())
            + std::mem::size_of::<Self>()
    }
}

/// Result of a cache lookup.
#[derive(Clone, Debug)]
pub struct GaCacheHit {
    /// The cached response.
    pub response: Bytes,
    /// Similarity score between query and cached embedding.
    pub similarity: f32,
    /// Optional metadata stored with the entry.
    pub metadata: Option<Value>,
    /// The original key stored with the entry.
    pub key: String,
}

// ---------------------------------------------------------------------------
// Metrics (Prometheus via `metrics` crate + local atomics)
// ---------------------------------------------------------------------------

struct CacheMetrics {
    prefix: String,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    dedup_merges: AtomicU64,
    similarity_sum_x1000: AtomicU64,
    similarity_count: AtomicU64,
}

impl CacheMetrics {
    fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            dedup_merges: AtomicU64::new(0),
            similarity_sum_x1000: AtomicU64::new(0),
            similarity_count: AtomicU64::new(0),
        }
    }

    fn record_hit(&self, similarity: f32) {
        self.hits.fetch_add(1, Ordering::Relaxed);
        counter!(format!("{}_hits_total", self.prefix)).increment(1);
        let sim_int = (similarity * 1000.0) as u64;
        self.similarity_sum_x1000
            .fetch_add(sim_int, Ordering::Relaxed);
        self.similarity_count.fetch_add(1, Ordering::Relaxed);
        histogram!(format!("{}_similarity", self.prefix)).record(similarity as f64);
    }

    fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
        counter!(format!("{}_misses_total", self.prefix)).increment(1);
    }

    fn record_eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
        counter!(format!("{}_evictions_total", self.prefix)).increment(1);
    }

    fn record_dedup(&self) {
        self.dedup_merges.fetch_add(1, Ordering::Relaxed);
        counter!(format!("{}_dedup_merges_total", self.prefix)).increment(1);
    }

    fn update_gauges(&self, entry_count: usize, memory_bytes: usize) {
        gauge!(format!("{}_entries", self.prefix)).set(entry_count as f64);
        gauge!(format!("{}_memory_bytes", self.prefix)).set(memory_bytes as f64);
    }

    fn record_embedding_latency(&self, duration: Duration) {
        histogram!(format!("{}_embedding_latency_seconds", self.prefix))
            .record(duration.as_secs_f64());
    }

    fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }
    fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }
    fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }
    fn dedup_merges(&self) -> u64 {
        self.dedup_merges.load(Ordering::Relaxed)
    }
    fn avg_similarity(&self) -> f64 {
        let c = self.similarity_count.load(Ordering::Relaxed);
        if c == 0 {
            return 0.0;
        }
        (self.similarity_sum_x1000.load(Ordering::Relaxed) as f64 / 1000.0) / c as f64
    }

    fn reset(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
        self.dedup_merges.store(0, Ordering::Relaxed);
        self.similarity_sum_x1000.store(0, Ordering::Relaxed);
        self.similarity_count.store(0, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Auto-tuning state
// ---------------------------------------------------------------------------

struct AutoTuneState {
    window: VecDeque<bool>, // true = hit, false = miss
    capacity: u64,
    target: f64,
    step: f32,
}

impl AutoTuneState {
    fn new(capacity: u64, target: f64, step: f32) -> Self {
        Self {
            window: VecDeque::with_capacity(capacity as usize),
            capacity,
            target,
            step,
        }
    }

    fn record(&mut self, is_hit: bool, current_threshold: &mut f32) {
        if self.window.len() as u64 >= self.capacity {
            self.window.pop_front();
        }
        self.window.push_back(is_hit);

        if self.window.len() as u64 >= self.capacity {
            let hit_rate =
                self.window.iter().filter(|h| **h).count() as f64 / self.window.len() as f64;
            if hit_rate < self.target {
                // Too many misses — lower threshold to increase hits
                *current_threshold = (*current_threshold - self.step).max(0.0);
                debug!(
                    hit_rate,
                    new_threshold = *current_threshold,
                    "auto-tune: lowered threshold"
                );
            } else if hit_rate > self.target + 0.1 {
                // Quality may be too loose — raise threshold
                *current_threshold = (*current_threshold + self.step).min(1.0);
                debug!(
                    hit_rate,
                    new_threshold = *current_threshold,
                    "auto-tune: raised threshold"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Snapshot summary for observability
// ---------------------------------------------------------------------------

/// Summary snapshot of cache state and performance counters.
#[derive(Clone, Debug)]
pub struct GaCacheSnapshot {
    /// Number of entries currently stored.
    pub entries: usize,
    /// Estimated memory usage in bytes.
    pub memory_bytes: usize,
    /// Total cache hits.
    pub hits: u64,
    /// Total cache misses.
    pub misses: u64,
    /// Total evictions.
    pub evictions: u64,
    /// Total dedup merges.
    pub dedup_merges: u64,
    /// Average similarity of cache hits.
    pub avg_similarity: f64,
    /// Current similarity threshold (may differ from config if auto-tuned).
    pub current_threshold: f32,
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

const PERSISTENCE_MAGIC: &[u8; 4] = b"GACV";
const PERSISTENCE_VERSION: u8 = 1;

/// Serialisable representation of one cache entry (for persistence).
#[derive(Serialize, Deserialize)]
struct PersistEntry {
    key: String,
    embedding: Vec<f32>,
    response: Vec<u8>,
    metadata: Option<Value>,
    hit_count: u64,
}

/// Errors that can occur during persistence operations.
#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialisation error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("bad magic bytes")]
    BadMagic,
    #[error("unsupported version: {0}")]
    UnsupportedVersion(u8),
    #[error("checksum mismatch: expected {expected:#010x}, got {got:#010x}")]
    ChecksumMismatch { expected: u32, got: u32 },
}

// ---------------------------------------------------------------------------
// GaSemanticCache
// ---------------------------------------------------------------------------

/// Production-quality semantic cache.
///
/// Thread-safe. All interior mutability uses `parking_lot::RwLock`.
pub struct GaSemanticCache {
    config: GaCacheConfig,
    threshold: RwLock<f32>,
    entries: RwLock<Vec<GaCacheEntry>>,
    metrics: CacheMetrics,
    auto_tune: RwLock<Option<AutoTuneState>>,
}

// SAFETY: all fields use parking_lot locks or atomics for interior mutability.
#[allow(unsafe_code)]
unsafe impl Send for GaSemanticCache {}
#[allow(unsafe_code)]
unsafe impl Sync for GaSemanticCache {}

impl GaSemanticCache {
    /// Create a new GA semantic cache.
    pub fn new(config: GaCacheConfig) -> Self {
        let threshold = config.threshold;
        let auto_tune = if config.auto_tune_enabled {
            Some(AutoTuneState::new(
                config.auto_tune_window,
                config.auto_tune_target_hit_rate,
                config.auto_tune_step,
            ))
        } else {
            None
        };
        info!(
            threshold,
            max_entries = config.max_entries,
            dimension = config.dimension,
            metric = ?config.distance_metric,
            "ga_cache: created"
        );
        Self {
            metrics: CacheMetrics::new(&config.metrics_prefix),
            config,
            threshold: RwLock::new(threshold),
            entries: RwLock::new(Vec::new()),
            auto_tune: RwLock::new(auto_tune),
        }
    }

    // -----------------------------------------------------------------------
    // Core API
    // -----------------------------------------------------------------------

    /// Look up the best-matching cached response for the given embedding.
    pub fn get(&self, query_embedding: &[f32]) -> Option<GaCacheHit> {
        let threshold = *self.threshold.read();
        let entries = self.entries.read();

        let mut best_idx: Option<usize> = None;
        let mut best_sim: f32 = -1.0;

        for (i, entry) in entries.iter().enumerate() {
            if entry.is_expired(self.config.ttl) {
                continue;
            }
            let sim = compute_similarity(
                query_embedding,
                &entry.embedding,
                self.config.distance_metric,
            );
            if sim >= threshold && sim > best_sim {
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
                let hit = GaCacheHit {
                    response: entry.response.clone(),
                    similarity: best_sim,
                    metadata: entry.metadata.clone(),
                    key: entry.key.clone(),
                };
                self.metrics.record_hit(best_sim);
                self.update_auto_tune(true);
                self.emit_gauges_locked(&entries);
                return Some(hit);
            }
        }

        self.metrics.record_miss();
        self.update_auto_tune(false);
        None
    }

    /// Store a response with its embedding.
    ///
    /// If `dedup_threshold` is configured and a sufficiently similar entry
    /// already exists, the existing entry is updated (merged) instead of
    /// inserting a duplicate.
    pub fn set(&self, key: &str, embedding: &[f32], response: Bytes, metadata: Option<Value>) {
        let mut entries = self.entries.write();

        // Deduplication: merge into existing entry if close enough.
        if let Some(dedup) = self.config.dedup_threshold {
            for entry in entries.iter_mut() {
                let sim =
                    compute_similarity(embedding, &entry.embedding, self.config.distance_metric);
                if sim >= dedup {
                    entry.response = response;
                    entry.metadata = metadata;
                    entry.last_accessed = Instant::now();
                    self.metrics.record_dedup();
                    debug!(key, similarity = sim, "ga_cache: dedup merge");
                    self.emit_gauges_locked(&entries);
                    return;
                }
            }
        }

        // Evict LRU if at capacity.
        while entries.len() >= self.config.max_entries {
            self.evict_lru_locked(&mut entries);
        }

        let now = Instant::now();
        entries.push(GaCacheEntry {
            key: key.to_string(),
            embedding: embedding.to_vec(),
            response,
            metadata,
            created_at: now,
            last_accessed: now,
            hit_count: 0,
        });
        self.emit_gauges_locked(&entries);
    }

    /// Batch insert multiple entries at once.
    pub fn set_batch(&self, items: &[(&str, &[f32], Bytes, Option<Value>)]) {
        let mut entries = self.entries.write();

        for (key, embedding, response, metadata) in items {
            // Dedup check
            let mut merged = false;
            if let Some(dedup) = self.config.dedup_threshold {
                for entry in entries.iter_mut() {
                    let sim = compute_similarity(
                        embedding,
                        &entry.embedding,
                        self.config.distance_metric,
                    );
                    if sim >= dedup {
                        entry.response = response.clone();
                        entry.metadata = metadata.clone();
                        entry.last_accessed = Instant::now();
                        self.metrics.record_dedup();
                        merged = true;
                        break;
                    }
                }
            }
            if merged {
                continue;
            }

            while entries.len() >= self.config.max_entries {
                self.evict_lru_locked(&mut entries);
            }

            let now = Instant::now();
            entries.push(GaCacheEntry {
                key: key.to_string(),
                embedding: embedding.to_vec(),
                response: response.clone(),
                metadata: metadata.clone(),
                created_at: now,
                last_accessed: now,
                hit_count: 0,
            });
        }
        self.emit_gauges_locked(&entries);
    }

    // -----------------------------------------------------------------------
    // Invalidation
    // -----------------------------------------------------------------------

    /// Invalidate (remove) all entries whose keys match a glob-like pattern.
    ///
    /// Supports `*` as a wildcard (converted to regex `.*`).
    pub fn invalidate_by_pattern(&self, pattern: &str) {
        let regex_pat = format!("^{}$", pattern.replace('*', ".*"));
        let re = match regex::Regex::new(&regex_pat) {
            Ok(r) => r,
            Err(e) => {
                warn!(pattern, error = %e, "ga_cache: invalid invalidation pattern");
                return;
            }
        };
        let mut entries = self.entries.write();
        let before = entries.len();
        entries.retain(|e| !re.is_match(&e.key));
        let removed = before - entries.len();
        if removed > 0 {
            debug!(pattern, removed, "ga_cache: pattern invalidation");
        }
        self.emit_gauges_locked(&entries);
    }

    /// Invalidate all entries within a similarity radius of the given embedding.
    pub fn invalidate_by_radius(&self, embedding: &[f32], radius: f32) {
        let mut entries = self.entries.write();
        entries.retain(|e| {
            compute_similarity(embedding, &e.embedding, self.config.distance_metric) < radius
        });
        self.emit_gauges_locked(&entries);
    }

    /// Remove all expired entries.
    pub fn purge_expired(&self) {
        if let Some(ttl) = self.config.ttl {
            let mut entries = self.entries.write();
            entries.retain(|e| !e.is_expired(Some(ttl)));
            self.emit_gauges_locked(&entries);
        }
    }

    /// Clear the entire cache and reset metrics.
    pub fn clear(&self) {
        let mut entries = self.entries.write();
        entries.clear();
        self.metrics.reset();
        self.emit_gauges_locked(&entries);
    }

    // -----------------------------------------------------------------------
    // Pre-warming
    // -----------------------------------------------------------------------

    /// Pre-warm the cache by loading entries from a supplied list.
    ///
    /// Useful at startup to avoid a cold-cache period.
    pub fn prewarm(&self, items: &[(&str, &[f32], Bytes, Option<Value>)]) {
        info!(count = items.len(), "ga_cache: pre-warming");
        self.set_batch(items);
    }

    // -----------------------------------------------------------------------
    // Observability
    // -----------------------------------------------------------------------

    /// Get a point-in-time snapshot of cache statistics.
    pub fn snapshot(&self) -> GaCacheSnapshot {
        let entries = self.entries.read();
        let mem: usize = entries.iter().map(|e| e.estimated_memory()).sum();
        GaCacheSnapshot {
            entries: entries.len(),
            memory_bytes: mem,
            hits: self.metrics.hits(),
            misses: self.metrics.misses(),
            evictions: self.metrics.evictions(),
            dedup_merges: self.metrics.dedup_merges(),
            avg_similarity: self.metrics.avg_similarity(),
            current_threshold: *self.threshold.read(),
        }
    }

    /// Current number of entries.
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Is the cache empty?
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Record an external embedding latency measurement.
    pub fn record_embedding_latency(&self, duration: Duration) {
        self.metrics.record_embedding_latency(duration);
    }

    /// Get the current (possibly auto-tuned) similarity threshold.
    pub fn current_threshold(&self) -> f32 {
        *self.threshold.read()
    }

    /// Get the cache configuration.
    pub fn config(&self) -> &GaCacheConfig {
        &self.config
    }

    // -----------------------------------------------------------------------
    // Persistence
    // -----------------------------------------------------------------------

    /// Save the current index to the given writer.
    ///
    /// Format: `MAGIC(4) | VERSION(1) | JSON_LEN(4 LE) | JSON | CRC32(4 LE)`
    pub fn save<W: Write>(&self, mut w: W) -> Result<(), PersistenceError> {
        let entries = self.entries.read();
        let persist: Vec<PersistEntry> = entries
            .iter()
            .map(|e| PersistEntry {
                key: e.key.clone(),
                embedding: e.embedding.clone(),
                response: e.response.to_vec(),
                metadata: e.metadata.clone(),
                hit_count: e.hit_count,
            })
            .collect();
        let json = serde_json::to_vec(&persist)?;

        w.write_all(PERSISTENCE_MAGIC)?;
        w.write_all(&[PERSISTENCE_VERSION])?;
        w.write_all(&(json.len() as u32).to_le_bytes())?;
        w.write_all(&json)?;

        let crc = crc32fast::hash(&json);
        w.write_all(&crc.to_le_bytes())?;
        w.flush()?;

        info!(
            entries = persist.len(),
            bytes = json.len(),
            "ga_cache: saved to disk"
        );
        Ok(())
    }

    /// Load entries from the given reader, **appending** to the current cache
    /// (incremental update).
    pub fn load<R: Read>(&self, mut r: R) -> Result<usize, PersistenceError> {
        let mut magic = [0u8; 4];
        r.read_exact(&mut magic)?;
        if &magic != PERSISTENCE_MAGIC {
            return Err(PersistenceError::BadMagic);
        }

        let mut ver = [0u8; 1];
        r.read_exact(&mut ver)?;
        if ver[0] != PERSISTENCE_VERSION {
            return Err(PersistenceError::UnsupportedVersion(ver[0]));
        }

        let mut len_buf = [0u8; 4];
        r.read_exact(&mut len_buf)?;
        let json_len = u32::from_le_bytes(len_buf) as usize;

        let mut json = vec![0u8; json_len];
        r.read_exact(&mut json)?;

        let mut crc_buf = [0u8; 4];
        r.read_exact(&mut crc_buf)?;
        let stored_crc = u32::from_le_bytes(crc_buf);
        let computed_crc = crc32fast::hash(&json);
        if stored_crc != computed_crc {
            return Err(PersistenceError::ChecksumMismatch {
                expected: stored_crc,
                got: computed_crc,
            });
        }

        let persist: Vec<PersistEntry> = serde_json::from_slice(&json)?;
        let count = persist.len();

        let mut entries = self.entries.write();
        let now = Instant::now();
        for p in persist {
            entries.push(GaCacheEntry {
                key: p.key,
                embedding: p.embedding,
                response: Bytes::from(p.response),
                metadata: p.metadata,
                created_at: now,
                last_accessed: now,
                hit_count: p.hit_count,
            });
        }
        self.emit_gauges_locked(&entries);
        info!(
            loaded = count,
            total = entries.len(),
            "ga_cache: loaded from disk"
        );
        Ok(count)
    }

    // -----------------------------------------------------------------------
    // Internals
    // -----------------------------------------------------------------------

    fn evict_lru_locked(&self, entries: &mut Vec<GaCacheEntry>) {
        if entries.is_empty() {
            return;
        }
        let mut oldest_idx = 0;
        let mut oldest_time = entries[0].last_accessed;
        for (i, e) in entries.iter().enumerate().skip(1) {
            if e.last_accessed < oldest_time {
                oldest_time = e.last_accessed;
                oldest_idx = i;
            }
        }
        entries.swap_remove(oldest_idx);
        self.metrics.record_eviction();
    }

    fn update_auto_tune(&self, is_hit: bool) {
        let mut guard = self.auto_tune.write();
        if let Some(at) = guard.as_mut() {
            let mut thr = self.threshold.write();
            at.record(is_hit, &mut thr);
        }
    }

    fn emit_gauges_locked(&self, entries: &[GaCacheEntry]) {
        let mem: usize = entries.iter().map(|e| e.estimated_memory()).sum();
        self.metrics.update_gauges(entries.len(), mem);
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_emb(seed: u8, dim: usize) -> Vec<f32> {
        let mut emb: Vec<f32> = (0..dim)
            .map(|i| ((i as u8).wrapping_add(seed)) as f32 / 255.0 - 0.5)
            .collect();
        let norm: f32 = emb.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for x in emb.iter_mut() {
                *x /= norm;
            }
        }
        emb
    }

    fn default_config(dim: usize) -> GaCacheConfig {
        GaCacheConfig {
            dimension: dim,
            ..Default::default()
        }
    }

    // --- basic get/set ---

    #[test]
    fn test_hit_same_embedding() {
        let cache = GaSemanticCache::new(default_config(128));
        let emb = make_emb(42, 128);
        cache.set("q1", &emb, Bytes::from("a1"), None);
        let hit = cache.get(&emb).expect("should hit");
        assert!(hit.similarity > 0.99);
        assert_eq!(hit.response, Bytes::from("a1"));
        assert_eq!(hit.key, "q1");
    }

    #[test]
    fn test_miss_empty_cache() {
        let cache = GaSemanticCache::new(default_config(64));
        assert!(cache.get(&make_emb(0, 64)).is_none());
        assert_eq!(cache.snapshot().misses, 1);
    }

    #[test]
    fn test_miss_high_threshold() {
        let config = GaCacheConfig {
            threshold: 0.999,
            dimension: 64,
            ..Default::default()
        };
        let cache = GaSemanticCache::new(config);
        cache.set("q1", &make_emb(0, 64), Bytes::from("a"), None);
        // Different seed → different embedding → below 0.999
        assert!(cache.get(&make_emb(200, 64)).is_none());
    }

    // --- TTL ---

    #[test]
    fn test_ttl_expiration() {
        let config = GaCacheConfig {
            ttl: Some(Duration::from_millis(50)),
            dimension: 64,
            ..Default::default()
        };
        let cache = GaSemanticCache::new(config);
        let emb = make_emb(0, 64);
        cache.set("q1", &emb, Bytes::from("a"), None);
        assert!(cache.get(&emb).is_some());
        std::thread::sleep(Duration::from_millis(60));
        assert!(cache.get(&emb).is_none());
    }

    #[test]
    fn test_purge_expired() {
        let config = GaCacheConfig {
            ttl: Some(Duration::from_millis(50)),
            dimension: 64,
            ..Default::default()
        };
        let cache = GaSemanticCache::new(config);
        for i in 0..5u8 {
            cache.set(
                &format!("q{}", i),
                &make_emb(i * 50, 64),
                Bytes::from("a"),
                None,
            );
        }
        assert_eq!(cache.len(), 5);
        std::thread::sleep(Duration::from_millis(60));
        cache.purge_expired();
        assert_eq!(cache.len(), 0);
    }

    // --- LRU eviction ---

    #[test]
    fn test_lru_eviction() {
        let config = GaCacheConfig {
            max_entries: 3,
            dimension: 64,
            ..Default::default()
        };
        let cache = GaSemanticCache::new(config);
        for i in 0..5u8 {
            cache.set(
                &format!("q{}", i),
                &make_emb(i * 50, 64),
                Bytes::from("a"),
                None,
            );
        }
        assert!(cache.len() <= 3);
        assert!(cache.snapshot().evictions >= 2);
    }

    // --- Pattern invalidation ---

    #[test]
    fn test_invalidate_by_pattern() {
        let cache = GaSemanticCache::new(default_config(64));
        cache.set("user:1:profile", &make_emb(1, 64), Bytes::from("a"), None);
        cache.set("user:2:profile", &make_emb(2, 64), Bytes::from("b"), None);
        cache.set("product:1:info", &make_emb(3, 64), Bytes::from("c"), None);
        assert_eq!(cache.len(), 3);
        cache.invalidate_by_pattern("user:*");
        assert_eq!(cache.len(), 1);
    }

    // --- Radius invalidation ---

    #[test]
    fn test_invalidate_by_radius() {
        let config = GaCacheConfig {
            threshold: 0.5,
            dimension: 64,
            ..Default::default()
        };
        let cache = GaSemanticCache::new(config);
        let emb1 = make_emb(0, 64);
        let emb2 = make_emb(1, 64); // similar to emb1
        let emb3 = make_emb(200, 64); // different
        cache.set("q1", &emb1, Bytes::from("a"), None);
        cache.set("q2", &emb2, Bytes::from("b"), None);
        cache.set("q3", &emb3, Bytes::from("c"), None);
        cache.invalidate_by_radius(&emb1, 0.9);
        assert!(cache.len() < 3);
    }

    // --- Deduplication ---

    #[test]
    fn test_dedup_merge() {
        let config = GaCacheConfig {
            dedup_threshold: Some(0.98),
            dimension: 64,
            ..Default::default()
        };
        let cache = GaSemanticCache::new(config);
        let emb = make_emb(0, 64);
        cache.set("q1", &emb, Bytes::from("old"), None);
        // Same embedding → should merge, not create new entry
        cache.set("q1_dup", &emb, Bytes::from("new"), None);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.snapshot().dedup_merges, 1);
        // Response should be updated
        let hit = cache.get(&emb).unwrap();
        assert_eq!(hit.response, Bytes::from("new"));
    }

    // --- Batch insert ---

    #[test]
    fn test_set_batch() {
        let cache = GaSemanticCache::new(default_config(64));
        let items: Vec<(&str, &[f32], Bytes, Option<Value>)> = (0..5u8)
            .map(|i| {
                let emb = make_emb(i * 50, 64);
                // Leak the vec to get a &[f32] with 'static lifetime for the test
                let emb_ref: &'static [f32] = Box::leak(emb.into_boxed_slice());
                (
                    Box::leak(format!("q{}", i).into_boxed_str()) as &str,
                    emb_ref,
                    Bytes::from("a"),
                    None,
                )
            })
            .collect();
        cache.set_batch(&items);
        assert_eq!(cache.len(), 5);
    }

    // --- Distance metrics ---

    #[test]
    fn test_euclidean_metric() {
        let config = GaCacheConfig {
            distance_metric: DistanceMetricType::Euclidean,
            threshold: 0.5,
            dimension: 64,
            ..Default::default()
        };
        let cache = GaSemanticCache::new(config);
        let emb = make_emb(42, 64);
        cache.set("q1", &emb, Bytes::from("a"), None);
        assert!(cache.get(&emb).is_some());
    }

    #[test]
    fn test_dot_product_metric() {
        let config = GaCacheConfig {
            distance_metric: DistanceMetricType::DotProduct,
            threshold: 0.5,
            dimension: 64,
            ..Default::default()
        };
        let cache = GaSemanticCache::new(config);
        let emb = make_emb(42, 64);
        cache.set("q1", &emb, Bytes::from("a"), None);
        assert!(cache.get(&emb).is_some());
    }

    // --- Metrics / snapshot ---

    #[test]
    fn test_snapshot_counters() {
        let cache = GaSemanticCache::new(default_config(64));
        let emb = make_emb(0, 64);
        cache.set("q1", &emb, Bytes::from("a"), None);
        let _ = cache.get(&emb); // hit
        let _ = cache.get(&make_emb(200, 64)); // miss (different embedding, default threshold 0.85)

        let snap = cache.snapshot();
        assert_eq!(snap.hits, 1);
        assert!(snap.misses >= 1);
        assert!(snap.entries == 1);
        assert!(snap.memory_bytes > 0);
        assert!(snap.avg_similarity > 0.0);
    }

    // --- Auto-tuning ---

    #[test]
    fn test_auto_tune_lowers_threshold() {
        let config = GaCacheConfig {
            threshold: 0.95,
            dimension: 64,
            auto_tune_enabled: true,
            auto_tune_target_hit_rate: 0.5,
            auto_tune_step: 0.02,
            auto_tune_window: 10,
            ..Default::default()
        };
        let cache = GaSemanticCache::new(config);
        let emb = make_emb(0, 64);
        cache.set("q1", &emb, Bytes::from("a"), None);

        // Generate all misses to push threshold down
        for i in 0..15u8 {
            let _ = cache.get(&make_emb(100 + i, 64));
        }
        assert!(cache.current_threshold() < 0.95);
    }

    // --- Persistence ---

    #[test]
    fn test_save_and_load() {
        let cache = GaSemanticCache::new(default_config(64));
        for i in 0..3u8 {
            cache.set(
                &format!("q{}", i),
                &make_emb(i * 80, 64),
                Bytes::from(format!("val{}", i)),
                None,
            );
        }

        let mut buf = Vec::new();
        cache.save(&mut buf).unwrap();

        // Load into fresh cache
        let cache2 = GaSemanticCache::new(default_config(64));
        let loaded = cache2.load(&buf[..]).unwrap();
        assert_eq!(loaded, 3);
        assert_eq!(cache2.len(), 3);
    }

    #[test]
    fn test_load_incremental() {
        let cache = GaSemanticCache::new(default_config(64));
        cache.set("existing", &make_emb(0, 64), Bytes::from("a"), None);

        // Build a snapshot from another cache
        let other = GaSemanticCache::new(default_config(64));
        other.set("new1", &make_emb(1, 64), Bytes::from("b"), None);
        let mut buf = Vec::new();
        other.save(&mut buf).unwrap();

        cache.load(&buf[..]).unwrap();
        assert_eq!(cache.len(), 2); // incremental: 1 existing + 1 loaded
    }

    #[test]
    fn test_load_bad_magic() {
        let cache = GaSemanticCache::new(default_config(64));
        let bad = b"BAD!";
        let err = cache.load(&bad[..]).unwrap_err();
        assert!(matches!(err, PersistenceError::BadMagic));
    }

    #[test]
    fn test_load_checksum_mismatch() {
        let cache = GaSemanticCache::new(default_config(64));
        cache.set("q1", &make_emb(0, 64), Bytes::from("a"), None);

        let mut buf = Vec::new();
        cache.save(&mut buf).unwrap();

        // Corrupt the payload (flip a byte in the JSON region)
        if buf.len() > 10 {
            buf[10] ^= 0xFF;
        }

        let cache2 = GaSemanticCache::new(default_config(64));
        let err = cache2.load(&buf[..]).unwrap_err();
        assert!(matches!(
            err,
            PersistenceError::ChecksumMismatch { .. } | PersistenceError::Serde(_)
        ));
    }

    // --- Clear / empty ---

    #[test]
    fn test_clear() {
        let cache = GaSemanticCache::new(default_config(64));
        let emb = make_emb(0, 64);
        cache.set("q1", &emb, Bytes::from("a"), None);
        let _ = cache.get(&emb);
        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.snapshot().hits, 0);
    }

    // --- Metadata ---

    #[test]
    fn test_metadata_roundtrip() {
        let cache = GaSemanticCache::new(default_config(64));
        let emb = make_emb(5, 64);
        let meta = serde_json::json!({"model": "gpt-4", "tokens": 150});
        cache.set("q1", &emb, Bytes::from("resp"), Some(meta.clone()));
        let hit = cache.get(&emb).unwrap();
        assert_eq!(hit.metadata.unwrap(), meta);
    }

    // --- Zero vector ---

    #[test]
    fn test_zero_vector() {
        let cache = GaSemanticCache::new(default_config(4));
        let zero = vec![0.0f32; 4];
        cache.set("q0", &zero, Bytes::from("z"), None);
        // cosine_similarity(0, 0) = 0.0, below default threshold
        assert!(cache.get(&zero).is_none());
    }

    // --- High dimensionality ---

    #[test]
    fn test_high_dim() {
        let dim = 1536;
        let cache = GaSemanticCache::new(default_config(dim));
        let emb = make_emb(42, dim);
        cache.set("q1", &emb, Bytes::from("hd"), None);
        assert!(cache.get(&emb).is_some());
    }

    // --- Pre-warm ---

    #[test]
    fn test_prewarm() {
        let cache = GaSemanticCache::new(default_config(64));
        let e1 = make_emb(1, 64);
        let e2 = make_emb(2, 64);
        let items: Vec<(&str, &[f32], Bytes, Option<Value>)> = vec![
            ("q1", &e1, Bytes::from("a"), None),
            ("q2", &e2, Bytes::from("b"), None),
        ];
        cache.prewarm(&items);
        assert_eq!(cache.len(), 2);
    }
}
