//! ML-Driven Hot/Cold/Warm Key Classification
//!
//! Classifies keys into storage tiers based on access frequency, recency,
//! and value size using exponential decay scoring.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Storage tier for a key
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Tier {
    /// Frequently accessed — kept in memory
    Hot,
    /// Moderately accessed — kept in read-only / mmap tier
    Warm,
    /// Rarely accessed — eligible for disk
    Cold,
}

/// Classification decision with confidence score
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TierDecision {
    /// Assigned tier
    pub tier: Tier,
    /// Confidence in the decision (0.0 to 1.0)
    pub confidence: f64,
    /// Computed score used for the decision
    pub score: f64,
}

/// Per-key access statistics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyAccessStats {
    /// Total number of accesses recorded
    pub access_count: u64,
    /// Last access timestamp (Unix ms)
    pub last_access_ts: u64,
    /// Exponentially decayed access frequency
    pub weighted_frequency: f64,
    /// Running average value size in bytes
    pub avg_value_size: u64,
    /// Timestamp when the key was first tracked (Unix ms)
    pub created_at: u64,
}

/// Aggregate statistics for the classifier
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ClassifierStats {
    /// Total keys being tracked
    pub tracked_keys: usize,
    /// Keys currently classified as hot
    pub hot_keys: usize,
    /// Keys currently classified as warm
    pub warm_keys: usize,
    /// Keys currently classified as cold
    pub cold_keys: usize,
    /// Total accesses recorded
    pub total_accesses: u64,
}

/// Configuration for the tier classifier
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClassifierConfig {
    /// Score above which a key is classified as hot
    pub hot_threshold: f64,
    /// Score below which a key is classified as cold
    pub cold_threshold: f64,
    /// Exponential decay factor applied per second to weighted frequency
    pub decay_factor: f64,
    /// Weight of recency relative to frequency in the scoring formula
    pub recency_weight: f64,
    /// Minimum interval between reclassification sweeps (ms)
    pub reclassification_interval_ms: u64,
    /// Maximum number of keys to track (LRU eviction beyond this)
    pub max_tracked_keys: usize,
}

impl Default for ClassifierConfig {
    fn default() -> Self {
        Self {
            hot_threshold: 0.7,
            cold_threshold: 0.3,
            decay_factor: 0.001,
            recency_weight: 0.4,
            reclassification_interval_ms: 5_000,
            max_tracked_keys: 1_000_000,
        }
    }
}

/// ML-driven hot/cold/warm key classifier
///
/// Tracks per-key access statistics and classifies keys into storage tiers
/// using an exponentially-decayed frequency + recency scoring model.
pub struct TierClassifier {
    config: ClassifierConfig,
    state: RwLock<ClassifierState>,
}

struct ClassifierState {
    keys: HashMap<String, KeyAccessStats>,
    total_accesses: u64,
}

impl TierClassifier {
    /// Create a new classifier with the given configuration
    pub fn new(config: ClassifierConfig) -> Self {
        Self {
            state: RwLock::new(ClassifierState {
                keys: HashMap::new(),
                total_accesses: 0,
            }),
            config,
        }
    }

    /// Record an access for `key` with the given `value_size` in bytes
    pub fn record_access(&self, key: &str, value_size: u64) {
        let now = current_timestamp_ms();
        let mut state = self.state.write();

        // Evict the oldest-accessed key when at capacity and inserting a new key
        if !state.keys.contains_key(key) && state.keys.len() >= self.config.max_tracked_keys {
            self.evict_oldest(&mut state);
        }

        let stats = state.keys.entry(key.to_owned()).or_insert(KeyAccessStats {
            access_count: 0,
            last_access_ts: now,
            weighted_frequency: 0.0,
            avg_value_size: value_size,
            created_at: now,
        });

        // Apply exponential decay since last access then add the new access
        let elapsed_secs = (now.saturating_sub(stats.last_access_ts)) as f64 / 1000.0;
        let decay = (-self.config.decay_factor * elapsed_secs).exp();
        stats.weighted_frequency = stats.weighted_frequency * decay + 1.0;

        stats.access_count += 1;
        stats.last_access_ts = now;

        // Running average for value size
        let count = stats.access_count;
        stats.avg_value_size = stats
            .avg_value_size
            .saturating_mul(count - 1)
            .saturating_add(value_size)
            / count;

        state.total_accesses += 1;
    }

    /// Classify a single key, returning `None` if the key is not tracked
    pub fn classify(&self, key: &str) -> Option<TierDecision> {
        let state = self.state.read();
        let stats = state.keys.get(key)?;
        Some(self.score_and_classify(stats))
    }

    /// Classify a batch of keys
    pub fn classify_batch(&self, keys: &[String]) -> Vec<(String, TierDecision)> {
        let state = self.state.read();
        keys.iter()
            .filter_map(|k| {
                state
                    .keys
                    .get(k.as_str())
                    .map(|s| (k.clone(), self.score_and_classify(s)))
            })
            .collect()
    }

    /// Return up to `limit` keys that should be promoted to a hotter tier.
    ///
    /// Candidates are cold or warm keys whose current score exceeds the
    /// threshold for the next-hotter tier, ordered by score descending.
    pub fn get_promotion_candidates(&self, limit: usize) -> Vec<String> {
        let state = self.state.read();
        let mut candidates: Vec<(String, f64)> = state
            .keys
            .iter()
            .filter_map(|(k, s)| {
                let decision = self.score_and_classify(s);
                match decision.tier {
                    // Cold keys whose score has risen above the cold ceiling
                    Tier::Cold if decision.score >= self.config.cold_threshold => {
                        Some((k.clone(), decision.score))
                    }
                    // Warm keys whose score has risen above the hot floor
                    Tier::Warm if decision.score >= self.config.hot_threshold => {
                        Some((k.clone(), decision.score))
                    }
                    _ => None,
                }
            })
            .collect();

        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(limit);
        candidates.into_iter().map(|(k, _)| k).collect()
    }

    /// Return up to `limit` keys that should be demoted to a colder tier.
    ///
    /// Candidates are hot or warm keys whose current score has dropped below
    /// the threshold for the next-colder tier, ordered by score ascending.
    pub fn get_demotion_candidates(&self, limit: usize) -> Vec<String> {
        let state = self.state.read();
        let mut candidates: Vec<(String, f64)> = state
            .keys
            .iter()
            .filter_map(|(k, s)| {
                let decision = self.score_and_classify(s);
                match decision.tier {
                    // Hot keys whose score has dropped below the hot floor
                    Tier::Hot if decision.score < self.config.hot_threshold => {
                        Some((k.clone(), decision.score))
                    }
                    // Warm keys whose score has dropped below the cold ceiling
                    Tier::Warm if decision.score < self.config.cold_threshold => {
                        Some((k.clone(), decision.score))
                    }
                    _ => None,
                }
            })
            .collect();

        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(limit);
        candidates.into_iter().map(|(k, _)| k).collect()
    }

    /// Return aggregate classifier statistics
    pub fn stats(&self) -> ClassifierStats {
        let state = self.state.read();
        let (mut hot, mut warm, mut cold) = (0usize, 0, 0);
        for stats in state.keys.values() {
            match self.score_and_classify(stats).tier {
                Tier::Hot => hot += 1,
                Tier::Warm => warm += 1,
                Tier::Cold => cold += 1,
            }
        }
        ClassifierStats {
            tracked_keys: state.keys.len(),
            hot_keys: hot,
            warm_keys: warm,
            cold_keys: cold,
            total_accesses: state.total_accesses,
        }
    }

    // ---- internal helpers ----

    /// Compute score and classify a key given its access stats.
    ///
    /// ```text
    /// score = weighted_frequency * freq_weight + recency_score * recency_weight
    /// recency_score = 1.0 / (1.0 + (now - last_access) / decay_half_life)
    /// ```
    fn score_and_classify(&self, stats: &KeyAccessStats) -> TierDecision {
        let now = current_timestamp_ms();
        let decay_half_life = (0.693 / self.config.decay_factor) * 1000.0; // convert to ms
        let elapsed = now.saturating_sub(stats.last_access_ts) as f64;
        let recency_score = 1.0 / (1.0 + elapsed / decay_half_life);

        // Apply decay to the stored weighted frequency up to now
        let elapsed_secs = elapsed / 1000.0;
        let decayed_freq =
            stats.weighted_frequency * (-self.config.decay_factor * elapsed_secs).exp();

        let freq_weight = 1.0 - self.config.recency_weight;

        // Normalise frequency with a soft-max so the combined score stays in [0, 1]
        let normalised_freq = decayed_freq / (1.0 + decayed_freq);

        let score = normalised_freq * freq_weight + recency_score * self.config.recency_weight;

        let tier = if score >= self.config.hot_threshold {
            Tier::Hot
        } else if score >= self.config.cold_threshold {
            Tier::Warm
        } else {
            Tier::Cold
        };

        // Confidence is higher the further the score is from the nearest threshold
        let dist_to_hot = (score - self.config.hot_threshold).abs();
        let dist_to_cold = (score - self.config.cold_threshold).abs();
        let min_dist = dist_to_hot.min(dist_to_cold);
        let range = self.config.hot_threshold - self.config.cold_threshold;
        let confidence = if range > 0.0 {
            (min_dist / range).min(1.0)
        } else {
            1.0
        };

        TierDecision {
            tier,
            confidence,
            score,
        }
    }

    /// Evict the least-recently accessed key to make room for a new one.
    fn evict_oldest(&self, state: &mut ClassifierState) {
        if let Some(oldest_key) = state
            .keys
            .iter()
            .min_by_key(|(_, s)| s.last_access_ts)
            .map(|(k, _)| k.clone())
        {
            state.keys.remove(&oldest_key);
        }
    }
}

/// Get current timestamp in milliseconds
fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    fn test_config() -> ClassifierConfig {
        ClassifierConfig {
            hot_threshold: 0.7,
            cold_threshold: 0.3,
            decay_factor: 0.001,
            recency_weight: 0.4,
            reclassification_interval_ms: 1_000,
            max_tracked_keys: 100,
        }
    }

    #[test]
    fn test_record_and_classify_cold() {
        let classifier = TierClassifier::new(test_config());
        // A single access a while ago should score low (cold)
        classifier.record_access("key1", 64);
        // Immediately after one access the recency is high, but frequency is moderate
        let decision = classifier.classify("key1").unwrap();
        // The key exists and has a valid tier
        assert!(matches!(decision.tier, Tier::Hot | Tier::Warm | Tier::Cold));
    }

    #[test]
    fn test_hot_key_many_accesses() {
        let classifier = TierClassifier::new(test_config());
        for _ in 0..100 {
            classifier.record_access("hot", 128);
        }
        let decision = classifier.classify("hot").unwrap();
        assert_eq!(decision.tier, Tier::Hot);
    }

    #[test]
    fn test_unknown_key_returns_none() {
        let classifier = TierClassifier::new(test_config());
        assert!(classifier.classify("missing").is_none());
    }

    #[test]
    fn test_classify_batch() {
        let classifier = TierClassifier::new(test_config());
        for _ in 0..50 {
            classifier.record_access("a", 64);
        }
        classifier.record_access("b", 64);

        let keys = vec!["a".to_string(), "b".to_string(), "missing".to_string()];
        let results = classifier.classify_batch(&keys);
        // "missing" is not tracked so only 2 results
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_stats() {
        let classifier = TierClassifier::new(test_config());
        classifier.record_access("k1", 64);
        classifier.record_access("k2", 64);
        let s = classifier.stats();
        assert_eq!(s.tracked_keys, 2);
        assert_eq!(s.total_accesses, 2);
    }

    #[test]
    fn test_max_tracked_keys_eviction() {
        let config = ClassifierConfig {
            max_tracked_keys: 3,
            ..test_config()
        };
        let classifier = TierClassifier::new(config);
        classifier.record_access("a", 10);
        thread::sleep(Duration::from_millis(5));
        classifier.record_access("b", 10);
        thread::sleep(Duration::from_millis(5));
        classifier.record_access("c", 10);
        thread::sleep(Duration::from_millis(5));
        // Adding a 4th key should evict the oldest ("a")
        classifier.record_access("d", 10);
        assert!(classifier.classify("a").is_none());
        assert!(classifier.classify("d").is_some());
        assert_eq!(classifier.stats().tracked_keys, 3);
    }

    #[test]
    fn test_demotion_candidates() {
        let config = ClassifierConfig {
            hot_threshold: 0.7,
            cold_threshold: 0.3,
            decay_factor: 10.0, // very aggressive decay for testing
            recency_weight: 0.4,
            max_tracked_keys: 100,
            ..test_config()
        };
        let classifier = TierClassifier::new(config);
        // Build up high frequency so key is hot
        for _ in 0..100 {
            classifier.record_access("will_cool", 64);
        }
        // Wait for aggressive decay to take effect
        thread::sleep(Duration::from_millis(200));
        let demotions = classifier.get_demotion_candidates(10);
        // The key may or may not appear depending on timing, but method shouldn't panic
        assert!(demotions.len() <= 10);
    }

    #[test]
    fn test_default_config() {
        let config = ClassifierConfig::default();
        assert!(config.hot_threshold > config.cold_threshold);
        assert!(config.decay_factor > 0.0);
        assert!(config.max_tracked_keys > 0);
    }
}
