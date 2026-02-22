//! Prefetch engine for predictive data warming.
//!
//! Uses the adaptive predictor's output to proactively move data from
//! cold/warm tiers to hot tier before predicted access spikes.

use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Configuration for the prefetch engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrefetchConfig {
    /// Enable prefetching.
    pub enabled: bool,
    /// Maximum number of keys to prefetch per cycle.
    pub max_prefetch_per_cycle: usize,
    /// How often to run prefetch evaluation (seconds).
    pub evaluation_interval_secs: u64,
    /// Minimum confidence threshold for prefetching (0.0-1.0).
    pub confidence_threshold: f64,
    /// Maximum memory budget for prefetched data (bytes).
    pub max_prefetch_memory: u64,
    /// Look-ahead window for prediction (how far ahead to predict).
    pub lookahead: Duration,
    /// Cool-down period after a failed prefetch before retrying.
    pub retry_cooldown: Duration,
    /// Maximum concurrent prefetch I/O operations.
    pub max_concurrent_io: usize,
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_prefetch_per_cycle: 100,
            evaluation_interval_secs: 30,
            confidence_threshold: 0.75,
            max_prefetch_memory: 256 * 1024 * 1024, // 256MB
            lookahead: Duration::from_secs(300),    // 5 minutes ahead
            retry_cooldown: Duration::from_secs(60),
            max_concurrent_io: 8,
        }
    }
}

/// A candidate for prefetching, ordered by expected value.
#[derive(Debug, Clone)]
pub struct PrefetchCandidate {
    /// Key to prefetch.
    pub key: String,
    /// Predicted access probability in the lookahead window.
    pub probability: f64,
    /// Expected data size in bytes.
    pub estimated_size: u64,
    /// Current storage tier.
    pub current_tier: String,
    /// Expected latency reduction from prefetching (microseconds).
    pub latency_savings_us: u64,
    /// Priority score combining probability, size, and latency savings.
    pub priority_score: f64,
}

impl PartialEq for PrefetchCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for PrefetchCandidate {}

impl PartialOrd for PrefetchCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PrefetchCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority_score
            .partial_cmp(&other.priority_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Statistics for the prefetch engine.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PrefetchStats {
    /// Total prefetch operations attempted.
    pub total_prefetches: u64,
    /// Prefetch operations that completed successfully.
    pub successful_prefetches: u64,
    /// Prefetch operations that failed.
    pub failed_prefetches: u64,
    /// Prefetched keys that were subsequently accessed.
    pub prefetch_hits: u64,
    /// Prefetched keys evicted without being accessed.
    pub prefetch_misses: u64,
    /// Total bytes prefetched.
    pub bytes_prefetched: u64,
    /// Total latency saved by prefetching in microseconds.
    pub total_latency_saved_us: u64,
    /// Number of prefetch evaluation cycles executed.
    pub cycles_run: u64,
}

impl PrefetchStats {
    /// Hit rate of prefetched keys (how often prefetched data was actually accessed).
    pub fn hit_rate(&self) -> f64 {
        let total = self.prefetch_hits + self.prefetch_misses;
        if total == 0 {
            return 0.0;
        }
        self.prefetch_hits as f64 / total as f64
    }

    /// Success rate of prefetch operations.
    pub fn success_rate(&self) -> f64 {
        if self.total_prefetches == 0 {
            return 0.0;
        }
        self.successful_prefetches as f64 / self.total_prefetches as f64
    }
}

/// The prefetch engine that coordinates predictive data warming.
pub struct PrefetchEngine {
    config: PrefetchConfig,
    stats: RwLock<PrefetchStats>,
    #[allow(dead_code)] // Planned for v0.2 — stored for prefetch engine enable/disable
    active: AtomicBool,
    current_memory_used: AtomicU64,
    prefetch_queue: RwLock<BinaryHeap<PrefetchCandidate>>,
    cooldown_keys: RwLock<HashMap<String, Instant>>,
}

impl PrefetchEngine {
    /// Create a new prefetch engine with the given configuration.
    pub fn new(config: PrefetchConfig) -> Self {
        Self {
            config,
            stats: RwLock::new(PrefetchStats::default()),
            active: AtomicBool::new(false),
            current_memory_used: AtomicU64::new(0),
            prefetch_queue: RwLock::new(BinaryHeap::new()),
            cooldown_keys: RwLock::new(HashMap::new()),
        }
    }

    /// Evaluates candidates and queues them for prefetching.
    pub fn evaluate_candidates(&self, candidates: Vec<PrefetchCandidate>) -> usize {
        let mut queue = self.prefetch_queue.write();
        let cooldowns = self.cooldown_keys.read();
        let now = Instant::now();
        let mut queued = 0;

        for candidate in candidates {
            // Skip if below confidence threshold
            if candidate.probability < self.config.confidence_threshold {
                continue;
            }

            // Skip if in cooldown
            if let Some(cooldown_until) = cooldowns.get(&candidate.key) {
                if now < *cooldown_until {
                    continue;
                }
            }

            // Skip if would exceed memory budget
            let current = self.current_memory_used.load(Ordering::Relaxed);
            if current + candidate.estimated_size > self.config.max_prefetch_memory {
                continue;
            }

            if queued < self.config.max_prefetch_per_cycle {
                queue.push(candidate);
                queued += 1;
            }
        }

        queued
    }

    /// Processes the next batch of prefetch candidates.
    pub fn process_batch(&self) -> Vec<PrefetchCandidate> {
        let mut queue = self.prefetch_queue.write();
        let batch_size = self.config.max_concurrent_io.min(queue.len());
        let mut batch = Vec::with_capacity(batch_size);

        for _ in 0..batch_size {
            if let Some(candidate) = queue.pop() {
                batch.push(candidate);
            }
        }

        if !batch.is_empty() {
            self.stats.write().cycles_run += 1;
        }

        batch
    }

    /// Records a successful prefetch.
    pub fn record_success(&self, _key: &str, size: u64) {
        let mut stats = self.stats.write();
        stats.total_prefetches += 1;
        stats.successful_prefetches += 1;
        stats.bytes_prefetched += size;
        self.current_memory_used.fetch_add(size, Ordering::Relaxed);
    }

    /// Records a failed prefetch and puts the key in cooldown.
    pub fn record_failure(&self, key: &str) {
        let mut stats = self.stats.write();
        stats.total_prefetches += 1;
        stats.failed_prefetches += 1;

        let cooldown_until = Instant::now() + self.config.retry_cooldown;
        self.cooldown_keys
            .write()
            .insert(key.to_string(), cooldown_until);
    }

    /// Records that a prefetched key was actually accessed (hit).
    pub fn record_hit(&self, latency_saved_us: u64) {
        let mut stats = self.stats.write();
        stats.prefetch_hits += 1;
        stats.total_latency_saved_us += latency_saved_us;
    }

    /// Records that a prefetched key was evicted without being accessed (miss).
    pub fn record_miss(&self) {
        self.stats.write().prefetch_misses += 1;
    }

    /// Returns current prefetch statistics.
    pub fn stats(&self) -> PrefetchStats {
        self.stats.read().clone()
    }

    /// Returns the current prefetch memory usage.
    pub fn memory_used(&self) -> u64 {
        self.current_memory_used.load(Ordering::Relaxed)
    }

    /// Returns the number of pending prefetch candidates.
    pub fn pending_count(&self) -> usize {
        self.prefetch_queue.read().len()
    }

    /// Clears expired cooldowns.
    pub fn clear_expired_cooldowns(&self) {
        let now = Instant::now();
        self.cooldown_keys.write().retain(|_, v| *v > now);
    }

    /// Resets all state.
    pub fn reset(&self) {
        *self.stats.write() = PrefetchStats::default();
        self.prefetch_queue.write().clear();
        self.cooldown_keys.write().clear();
        self.current_memory_used.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_candidate(key: &str, probability: f64, size: u64) -> PrefetchCandidate {
        PrefetchCandidate {
            key: key.to_string(),
            probability,
            estimated_size: size,
            current_tier: "cold".to_string(),
            latency_savings_us: 1000,
            priority_score: probability * 1000.0,
        }
    }

    #[test]
    fn test_evaluate_candidates() {
        let engine = PrefetchEngine::new(PrefetchConfig {
            confidence_threshold: 0.5,
            ..Default::default()
        });

        let candidates = vec![
            make_candidate("k1", 0.9, 1024),
            make_candidate("k2", 0.3, 1024), // Below threshold
            make_candidate("k3", 0.7, 1024),
        ];

        let queued = engine.evaluate_candidates(candidates);
        assert_eq!(queued, 2); // k2 filtered out
    }

    #[test]
    fn test_process_batch() {
        let engine = PrefetchEngine::new(PrefetchConfig {
            max_concurrent_io: 2,
            confidence_threshold: 0.5,
            ..Default::default()
        });

        let candidates = vec![
            make_candidate("k1", 0.9, 1024),
            make_candidate("k2", 0.8, 1024),
            make_candidate("k3", 0.7, 1024),
        ];

        engine.evaluate_candidates(candidates);
        let batch = engine.process_batch();
        assert_eq!(batch.len(), 2); // Limited by max_concurrent_io
    }

    #[test]
    fn test_record_success_and_failure() {
        let engine = PrefetchEngine::new(PrefetchConfig::default());

        engine.record_success("k1", 1024);
        engine.record_failure("k2");

        let stats = engine.stats();
        assert_eq!(stats.total_prefetches, 2);
        assert_eq!(stats.successful_prefetches, 1);
        assert_eq!(stats.failed_prefetches, 1);
        assert_eq!(stats.bytes_prefetched, 1024);
    }

    #[test]
    fn test_hit_rate() {
        let engine = PrefetchEngine::new(PrefetchConfig::default());
        engine.record_hit(500);
        engine.record_hit(300);
        engine.record_miss();

        let stats = engine.stats();
        assert!((stats.hit_rate() - 0.6666).abs() < 0.01);
        assert_eq!(stats.total_latency_saved_us, 800);
    }

    #[test]
    fn test_memory_budget() {
        let engine = PrefetchEngine::new(PrefetchConfig {
            max_prefetch_memory: 2048,
            confidence_threshold: 0.5,
            ..Default::default()
        });

        engine.record_success("k1", 1024);
        engine.record_success("k2", 1024);

        let candidates = vec![make_candidate("k3", 0.9, 1024)];
        let queued = engine.evaluate_candidates(candidates);
        assert_eq!(queued, 0); // Exceeds memory budget
    }

    #[test]
    fn test_stats_rates() {
        let stats = PrefetchStats::default();
        assert_eq!(stats.hit_rate(), 0.0);
        assert_eq!(stats.success_rate(), 0.0);
    }
}
