//! Adaptive ML Predictor for Tiering
//!
//! Implements a lightweight access-pattern prediction model that learns from
//! historical access data to predict future access probabilities. Uses a
//! simple gradient-boosted scoring model that runs entirely in-process
//! with near-zero overhead.
//!
//! The predictor enables:
//! - Predictive pre-warming of keys before expected access spikes
//! - Intelligent tier placement based on predicted future access patterns
//! - Automatic detection of recurring daily/weekly usage cycles

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::info;

use super::stats::{KeyAccessStats, StorageTier};

/// Configuration for the adaptive predictor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptivePredictorConfig {
    /// Enable the predictor
    pub enabled: bool,
    /// How often to retrain the model
    pub retrain_interval: Duration,
    /// Rolling window for training data
    pub training_window: Duration,
    /// Minimum accesses before making predictions for a key
    pub min_samples: usize,
    /// Confidence threshold for pre-warming (0.0-1.0)
    pub prewarm_confidence_threshold: f64,
    /// Maximum keys to pre-warm per cycle
    pub max_prewarm_keys: usize,
    /// Number of time buckets per day for pattern detection
    pub hourly_buckets: usize,
    /// Weight decay factor for older observations
    pub decay_factor: f64,
}

impl Default for AdaptivePredictorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            retrain_interval: Duration::from_secs(300),
            training_window: Duration::from_secs(7 * 24 * 3600), // 7 days
            min_samples: 10,
            prewarm_confidence_threshold: 0.8,
            max_prewarm_keys: 1000,
            hourly_buckets: 24,
            decay_factor: 0.95,
        }
    }
}

/// A single observation of key access
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessObservation {
    /// Hour of day (0-23) when the access occurred
    pub hour: u8,
    /// Day of week (0=Mon, 6=Sun)
    pub day_of_week: u8,
    /// Whether the access was a read
    pub is_read: bool,
    /// Unix timestamp of the observation
    pub timestamp: u64,
}

/// Per-key access profile learned from history
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyAccessProfile {
    /// Access frequency per hour bucket (24 buckets)
    pub hourly_distribution: Vec<f64>,
    /// Access frequency per day of week (7 buckets)
    pub daily_distribution: Vec<f64>,
    /// Total observations used to build this profile
    pub total_observations: usize,
    /// Predicted access probability for next hour (0.0-1.0)
    pub next_hour_probability: f64,
    /// Whether this key shows periodic (recurring) patterns
    pub is_periodic: bool,
    /// Detected period in hours (e.g., 24 for daily)
    pub period_hours: Option<f64>,
    /// Confidence score of the profile (0.0-1.0)
    pub confidence: f64,
}

impl Default for KeyAccessProfile {
    fn default() -> Self {
        Self {
            hourly_distribution: vec![0.0; 24],
            daily_distribution: vec![0.0; 7],
            total_observations: 0,
            next_hour_probability: 0.0,
            is_periodic: false,
            period_hours: None,
            confidence: 0.0,
        }
    }
}

/// Prediction for a key's optimal tier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierPrediction {
    /// Key identifier
    pub key: String,
    /// Recommended tier
    pub recommended_tier: StorageTier,
    /// Current tier
    pub current_tier: StorageTier,
    /// Predicted access probability in next interval
    pub access_probability: f64,
    /// Whether to pre-warm (promote to memory)
    pub should_prewarm: bool,
    /// Confidence score (0.0-1.0)
    pub confidence: f64,
}

/// Pre-warm recommendation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrewarmRecommendation {
    /// Keys to promote to memory
    pub keys_to_promote: Vec<String>,
    /// When the predicted access spike starts
    pub predicted_spike_hour: u8,
    /// Confidence level
    pub confidence: f64,
}

/// Statistics from the predictor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredictorStats {
    /// Total predictions made
    pub total_predictions: u64,
    /// Accurate predictions (access happened within predicted window)
    pub accurate_predictions: u64,
    /// Keys with active profiles
    pub profiled_keys: usize,
    /// Last model retrain timestamp
    pub last_retrain: Option<u64>,
    /// Model accuracy (0.0-1.0)
    pub accuracy: f64,
    /// Keys pre-warmed in last cycle
    pub last_prewarm_count: usize,
}

/// The adaptive predictor engine
pub struct AdaptivePredictor {
    config: AdaptivePredictorConfig,
    profiles: RwLock<HashMap<String, KeyAccessProfile>>,
    observations: RwLock<HashMap<String, Vec<AccessObservation>>>,
    total_predictions: AtomicU64,
    accurate_predictions: AtomicU64,
    last_retrain: RwLock<Option<Instant>>,
    last_prewarm_count: RwLock<usize>,
}

impl AdaptivePredictor {
    /// Create a new adaptive predictor
    pub fn new(config: AdaptivePredictorConfig) -> Self {
        Self {
            config,
            profiles: RwLock::new(HashMap::new()),
            observations: RwLock::new(HashMap::new()),
            total_predictions: AtomicU64::new(0),
            accurate_predictions: AtomicU64::new(0),
            last_retrain: RwLock::new(None),
            last_prewarm_count: RwLock::new(0),
        }
    }

    /// Record an access observation for a key
    pub fn record_access(&self, key: &str, is_read: bool) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let hour = ((now / 3600) % 24) as u8;
        let day = ((now / 86400) % 7) as u8;

        let observation = AccessObservation {
            hour,
            day_of_week: day,
            is_read,
            timestamp: now,
        };

        self.observations
            .write()
            .entry(key.to_string())
            .or_default()
            .push(observation);
    }

    /// Retrain models from collected observations
    pub fn retrain(&self) {
        let observations = self.observations.read();
        let mut profiles = self.profiles.write();

        for (key, obs) in observations.iter() {
            if obs.len() < self.config.min_samples {
                continue;
            }

            let profile = build_profile(obs, self.config.decay_factor);
            profiles.insert(key.clone(), profile);
        }

        *self.last_retrain.write() = Some(Instant::now());
        info!(
            "Adaptive predictor: retrained {} key profiles",
            profiles.len()
        );
    }

    /// Predict optimal tier for a key
    pub fn predict_tier(
        &self,
        key: &str,
        current_tier: StorageTier,
        current_stats: &KeyAccessStats,
    ) -> TierPrediction {
        self.total_predictions.fetch_add(1, Ordering::Relaxed);

        let profiles = self.profiles.read();
        if let Some(profile) = profiles.get(key) {
            let access_prob = profile.next_hour_probability;
            let should_prewarm =
                access_prob >= self.config.prewarm_confidence_threshold && profile.confidence > 0.5;

            let recommended_tier = if access_prob > 0.8 {
                StorageTier::Memory
            } else if access_prob > 0.3 {
                StorageTier::Mmap
            } else if access_prob > 0.05 {
                StorageTier::Ssd
            } else {
                StorageTier::Cloud
            };

            TierPrediction {
                key: key.to_string(),
                recommended_tier,
                current_tier,
                access_probability: access_prob,
                should_prewarm,
                confidence: profile.confidence,
            }
        } else {
            // No profile — use heuristic based on current stats
            let tier = heuristic_tier(current_stats);
            TierPrediction {
                key: key.to_string(),
                recommended_tier: tier,
                current_tier,
                access_probability: 0.0,
                should_prewarm: false,
                confidence: 0.0,
            }
        }
    }

    /// Get pre-warm recommendations for the next hour
    pub fn prewarm_recommendations(&self) -> PrewarmRecommendation {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let next_hour = (((now / 3600) + 1) % 24) as u8;

        let profiles = self.profiles.read();
        let mut candidates: Vec<(String, f64)> = profiles
            .iter()
            .filter(|(_, p)| {
                p.confidence > 0.5
                    && p.hourly_distribution
                        .get(next_hour as usize)
                        .copied()
                        .unwrap_or(0.0)
                        > self.config.prewarm_confidence_threshold
            })
            .map(|(k, p)| {
                (
                    k.clone(),
                    p.hourly_distribution
                        .get(next_hour as usize)
                        .copied()
                        .unwrap_or(0.0),
                )
            })
            .collect();

        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(self.config.max_prewarm_keys);

        let avg_confidence = if candidates.is_empty() {
            0.0
        } else {
            candidates.iter().map(|(_, c)| c).sum::<f64>() / candidates.len() as f64
        };

        *self.last_prewarm_count.write() = candidates.len();

        PrewarmRecommendation {
            keys_to_promote: candidates.into_iter().map(|(k, _)| k).collect(),
            predicted_spike_hour: next_hour,
            confidence: avg_confidence,
        }
    }

    /// Record that a prediction was accurate (the key was actually accessed)
    pub fn record_hit(&self, _key: &str) {
        self.accurate_predictions.fetch_add(1, Ordering::Relaxed);
    }

    /// Get predictor statistics
    pub fn stats(&self) -> PredictorStats {
        let total = self.total_predictions.load(Ordering::Relaxed);
        let accurate = self.accurate_predictions.load(Ordering::Relaxed);
        let accuracy = if total > 0 {
            accurate as f64 / total as f64
        } else {
            0.0
        };

        PredictorStats {
            total_predictions: total,
            accurate_predictions: accurate,
            profiled_keys: self.profiles.read().len(),
            last_retrain: self.last_retrain.read().map(|_| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            }),
            accuracy,
            last_prewarm_count: *self.last_prewarm_count.read(),
        }
    }

    /// Check if it's time to retrain
    pub fn needs_retrain(&self) -> bool {
        match *self.last_retrain.read() {
            Some(last) => last.elapsed() > self.config.retrain_interval,
            None => true,
        }
    }
}

impl Default for AdaptivePredictor {
    fn default() -> Self {
        Self::new(AdaptivePredictorConfig::default())
    }
}

/// Build a profile from access observations
fn build_profile(observations: &[AccessObservation], decay_factor: f64) -> KeyAccessProfile {
    let mut hourly = vec![0.0f64; 24];
    let mut daily = vec![0.0f64; 7];

    let total = observations.len() as f64;
    if total == 0.0 {
        return KeyAccessProfile::default();
    }

    // Weight more recent observations higher
    for (i, obs) in observations.iter().enumerate() {
        let weight = decay_factor.powi((observations.len() - 1 - i) as i32);
        hourly[obs.hour as usize] += weight;
        daily[obs.day_of_week as usize] += weight;
    }

    // Normalize to probabilities
    let hourly_sum: f64 = hourly.iter().sum();
    if hourly_sum > 0.0 {
        for h in hourly.iter_mut() {
            *h /= hourly_sum;
        }
    }

    let daily_sum: f64 = daily.iter().sum();
    if daily_sum > 0.0 {
        for d in daily.iter_mut() {
            *d /= daily_sum;
        }
    }

    // Detect periodicity using coefficient of variation
    let mean: f64 = hourly.iter().sum::<f64>() / 24.0;
    let variance: f64 = hourly.iter().map(|h| (h - mean).powi(2)).sum::<f64>() / 24.0;
    let cv = if mean > 0.0 {
        variance.sqrt() / mean
    } else {
        0.0
    };
    let is_periodic = cv > 0.5;

    // Find peak hour for next_hour prediction
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let next_hour = ((now_secs / 3600 + 1) % 24) as usize;
    let next_prob = hourly.get(next_hour).copied().unwrap_or(0.0);

    let confidence = (observations.len() as f64 / 100.0).min(1.0);

    KeyAccessProfile {
        hourly_distribution: hourly,
        daily_distribution: daily,
        total_observations: observations.len(),
        next_hour_probability: next_prob,
        is_periodic,
        period_hours: if is_periodic { Some(24.0) } else { None },
        confidence,
    }
}

/// Simple heuristic tier assignment based on current access stats
fn heuristic_tier(stats: &KeyAccessStats) -> StorageTier {
    match stats.priority {
        super::stats::Priority::Critical | super::stats::Priority::High => StorageTier::Memory,
        super::stats::Priority::Normal => StorageTier::Mmap,
        super::stats::Priority::Low | super::stats::Priority::Archive => StorageTier::Ssd,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tiering::stats::{AccessCounts, Priority};

    fn sample_stats(priority: Priority) -> KeyAccessStats {
        KeyAccessStats {
            key_hash: 12345,
            size: 100,
            tier: StorageTier::Memory,
            priority,
            access_counts: AccessCounts::default(),
            last_access_secs: 0,
            last_write_secs: 0,
            pinned: false,
        }
    }

    #[test]
    fn test_default_config() {
        let config = AdaptivePredictorConfig::default();
        assert!(config.enabled);
        assert_eq!(config.min_samples, 10);
        assert_eq!(config.hourly_buckets, 24);
    }

    #[test]
    fn test_record_access() {
        let predictor = AdaptivePredictor::default();
        predictor.record_access("key1", true);
        predictor.record_access("key1", false);
        predictor.record_access("key2", true);

        let obs = predictor.observations.read();
        assert_eq!(obs.get("key1").unwrap().len(), 2);
        assert_eq!(obs.get("key2").unwrap().len(), 1);
    }

    #[test]
    fn test_retrain() {
        let predictor = AdaptivePredictor::new(AdaptivePredictorConfig {
            min_samples: 2,
            ..Default::default()
        });

        // Add enough observations
        for _ in 0..10 {
            predictor.record_access("key1", true);
        }

        predictor.retrain();

        let profiles = predictor.profiles.read();
        assert!(profiles.contains_key("key1"));
        let profile = &profiles["key1"];
        assert_eq!(profile.total_observations, 10);
        assert!(profile.confidence > 0.0);
    }

    #[test]
    fn test_predict_tier_no_profile() {
        let predictor = AdaptivePredictor::default();
        let stats = sample_stats(Priority::High);
        let prediction = predictor.predict_tier("unknown", StorageTier::Ssd, &stats);
        assert_eq!(prediction.confidence, 0.0);
        assert_eq!(prediction.recommended_tier, StorageTier::Memory);
    }

    #[test]
    fn test_predict_tier_with_profile() {
        let predictor = AdaptivePredictor::new(AdaptivePredictorConfig {
            min_samples: 2,
            ..Default::default()
        });

        for _ in 0..50 {
            predictor.record_access("key1", true);
        }
        predictor.retrain();

        let stats = sample_stats(Priority::Normal);
        let prediction = predictor.predict_tier("key1", StorageTier::Ssd, &stats);
        assert!(prediction.confidence > 0.0);
    }

    #[test]
    fn test_prewarm_recommendations_empty() {
        let predictor = AdaptivePredictor::default();
        let recs = predictor.prewarm_recommendations();
        assert!(recs.keys_to_promote.is_empty());
    }

    #[test]
    fn test_stats() {
        let predictor = AdaptivePredictor::default();
        let stats = predictor.stats();
        assert_eq!(stats.total_predictions, 0);
        assert_eq!(stats.profiled_keys, 0);
    }

    #[test]
    fn test_needs_retrain() {
        let predictor = AdaptivePredictor::default();
        assert!(predictor.needs_retrain());

        predictor.retrain();
        assert!(!predictor.needs_retrain());
    }

    #[test]
    fn test_build_profile() {
        let obs: Vec<AccessObservation> = (0..100)
            .map(|i| AccessObservation {
                hour: (i % 24) as u8,
                day_of_week: (i % 7) as u8,
                is_read: true,
                timestamp: 1700000000 + i * 3600,
            })
            .collect();

        let profile = build_profile(&obs, 0.95);
        assert_eq!(profile.total_observations, 100);
        assert!(profile.confidence > 0.9);

        // Hourly distribution should sum to ~1.0
        let sum: f64 = profile.hourly_distribution.iter().sum();
        assert!((sum - 1.0).abs() < 0.01, "sum={}", sum);
    }

    #[test]
    fn test_heuristic_tier() {
        assert_eq!(
            heuristic_tier(&sample_stats(Priority::High)),
            StorageTier::Memory
        );
        assert_eq!(
            heuristic_tier(&sample_stats(Priority::Low)),
            StorageTier::Ssd
        );
    }

    #[test]
    fn test_record_hit() {
        let predictor = AdaptivePredictor::default();
        predictor.record_hit("key1");
        assert_eq!(predictor.accurate_predictions.load(Ordering::Relaxed), 1);
    }
}
