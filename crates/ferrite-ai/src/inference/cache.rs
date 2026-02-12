//! Inference Result Cache
//!
//! Caches inference results to avoid duplicate computation.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::{InferenceInput, InferenceOutput};

/// Cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceCacheConfig {
    /// Maximum cache entries
    pub max_entries: usize,
    /// TTL for cache entries in seconds
    pub ttl_secs: u64,
    /// Enable cache
    pub enabled: bool,
    /// Hash algorithm to use
    pub hash_algorithm: HashAlgorithm,
}

impl Default for InferenceCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 10000,
            ttl_secs: 3600,
            enabled: true,
            hash_algorithm: HashAlgorithm::Xxhash,
        }
    }
}

impl InferenceCacheConfig {
    /// Disable cache
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }

    /// Set max entries
    pub fn with_max_entries(mut self, max: usize) -> Self {
        self.max_entries = max;
        self
    }

    /// Set TTL
    pub fn with_ttl(mut self, secs: u64) -> Self {
        self.ttl_secs = secs;
        self
    }
}

/// Hash algorithm for cache keys
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum HashAlgorithm {
    /// Default Rust hasher
    Default,
    /// xxHash (faster)
    Xxhash,
    /// SHA256 (deterministic)
    Sha256,
}

/// A cached prediction
#[derive(Debug, Clone)]
pub struct CachedPrediction {
    /// Model name
    pub model: String,
    /// Input hash
    pub input_hash: u64,
    /// Cached output
    pub output: InferenceOutput,
    /// Cache time
    pub cached_at: Instant,
    /// Hit count
    pub hits: u64,
}

impl CachedPrediction {
    /// Check if expired
    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.cached_at.elapsed() > ttl
    }
}

/// Cache statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CacheStats {
    /// Total cache hits
    pub hits: u64,
    /// Total cache misses
    pub misses: u64,
    /// Current entry count
    pub entries: usize,
    /// Hit rate
    pub hit_rate: f64,
    /// Total bytes cached (approximate)
    pub bytes_cached: u64,
    /// Evictions
    pub evictions: u64,
}

/// Inference result cache
pub struct InferenceCache {
    config: InferenceCacheConfig,
    cache: RwLock<HashMap<(String, u64), CachedPrediction>>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

impl InferenceCache {
    /// Create a new cache
    pub fn new(config: InferenceCacheConfig) -> Self {
        Self {
            config,
            cache: RwLock::new(HashMap::new()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    /// Create with default config
    pub fn with_defaults() -> Self {
        Self::new(InferenceCacheConfig::default())
    }

    /// Get from cache
    pub fn get(&self, model: &str, input: &InferenceInput) -> Option<InferenceOutput> {
        if !self.config.enabled {
            return None;
        }

        let hash = self.hash_input(input);
        let key = (model.to_string(), hash);
        let ttl = Duration::from_secs(self.config.ttl_secs);

        let mut cache = self.cache.write();
        if let Some(entry) = cache.get_mut(&key) {
            if !entry.is_expired(ttl) {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(entry.output.clone());
            } else {
                cache.remove(&key);
            }
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Put in cache
    pub fn put(&self, model: &str, input: &InferenceInput, output: InferenceOutput) {
        if !self.config.enabled {
            return;
        }

        let hash = self.hash_input(input);
        let key = (model.to_string(), hash);

        let entry = CachedPrediction {
            model: model.to_string(),
            input_hash: hash,
            output,
            cached_at: Instant::now(),
            hits: 0,
        };

        let mut cache = self.cache.write();

        // Evict if at capacity
        if cache.len() >= self.config.max_entries {
            self.evict_oldest(&mut cache);
        }

        cache.insert(key, entry);
    }

    /// Hash an input
    fn hash_input(&self, input: &InferenceInput) -> u64 {
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();

        match input {
            InferenceInput::Scalar(v) => {
                "scalar".hash(&mut hasher);
                v.to_bits().hash(&mut hasher);
            }
            InferenceInput::Vector(v) => {
                "vector".hash(&mut hasher);
                for x in v {
                    x.to_bits().hash(&mut hasher);
                }
            }
            InferenceInput::Matrix(m) => {
                "matrix".hash(&mut hasher);
                for row in m {
                    for x in row {
                        x.to_bits().hash(&mut hasher);
                    }
                }
            }
            InferenceInput::Text(t) => {
                "text".hash(&mut hasher);
                t.hash(&mut hasher);
            }
            InferenceInput::Image(b) | InferenceInput::Raw(b) => {
                "bytes".hash(&mut hasher);
                b.hash(&mut hasher);
            }
            InferenceInput::Named(tensors) => {
                "named".hash(&mut hasher);
                for (name, values) in tensors {
                    name.hash(&mut hasher);
                    for x in values {
                        x.to_bits().hash(&mut hasher);
                    }
                }
            }
        }

        hasher.finish()
    }

    /// Evict oldest entry
    fn evict_oldest(&self, cache: &mut HashMap<(String, u64), CachedPrediction>) {
        if let Some(oldest_key) = cache
            .iter()
            .min_by_key(|(_, v)| v.cached_at)
            .map(|(k, _)| k.clone())
        {
            cache.remove(&oldest_key);
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Clear cache
    pub fn clear(&self) {
        self.cache.write().clear();
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let entries = self.cache.read().len();
        let total = hits + misses;

        CacheStats {
            hits,
            misses,
            entries,
            hit_rate: if total > 0 {
                hits as f64 / total as f64
            } else {
                0.0
            },
            bytes_cached: 0, // Would need sizeof tracking
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }

    /// Get entry count
    pub fn len(&self) -> usize {
        self.cache.read().len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.cache.read().is_empty()
    }

    /// Get config
    pub fn config(&self) -> &InferenceCacheConfig {
        &self.config
    }
}

impl Default for InferenceCache {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_config() {
        let config = InferenceCacheConfig::default()
            .with_max_entries(1000)
            .with_ttl(60);

        assert_eq!(config.max_entries, 1000);
        assert_eq!(config.ttl_secs, 60);
    }

    #[test]
    fn test_cache_get_put() {
        let cache = InferenceCache::with_defaults();

        let input = InferenceInput::Text("test".to_string());
        let output = InferenceOutput::Scalar(1.0);

        // Miss
        assert!(cache.get("model", &input).is_none());

        // Put
        cache.put("model", &input, output.clone());

        // Hit
        let result = cache.get("model", &input);
        assert!(result.is_some());
    }

    #[test]
    fn test_cache_stats() {
        let cache = InferenceCache::with_defaults();

        let input = InferenceInput::Scalar(1.0);
        let output = InferenceOutput::Scalar(2.0);

        // Miss
        cache.get("model", &input);
        cache.get("model", &input);

        // Put and hit
        cache.put("model", &input, output);
        cache.get("model", &input);

        let stats = cache.stats();
        assert_eq!(stats.misses, 2);
        assert_eq!(stats.hits, 1);
    }

    #[test]
    fn test_cache_eviction() {
        let config = InferenceCacheConfig::default().with_max_entries(2);
        let cache = InferenceCache::new(config);

        cache.put(
            "model",
            &InferenceInput::Scalar(1.0),
            InferenceOutput::Scalar(1.0),
        );
        cache.put(
            "model",
            &InferenceInput::Scalar(2.0),
            InferenceOutput::Scalar(2.0),
        );
        cache.put(
            "model",
            &InferenceInput::Scalar(3.0),
            InferenceOutput::Scalar(3.0),
        );

        assert!(cache.len() <= 2);
    }

    #[test]
    fn test_disabled_cache() {
        let cache = InferenceCache::new(InferenceCacheConfig::disabled());

        let input = InferenceInput::Scalar(1.0);
        cache.put("model", &input, InferenceOutput::Scalar(1.0));

        // Should not cache when disabled
        assert!(cache.get("model", &input).is_none());
    }
}
