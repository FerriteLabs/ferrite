//! Adaptive ML Tiering Engine
//!
//! Implements an online learning model with feedback loops for intelligent
//! tier placement decisions. The engine uses stochastic gradient descent
//! on feature vectors extracted from key access patterns, combined with
//! an adaptive migration controller that prevents thrashing and rate-limits
//! tier transitions.
//!
//! Unlike the batch-oriented [`super::predictor::AdaptivePredictor`], this
//! module trains incrementally on every access, providing real-time
//! adaptation without periodic retraining.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::debug;

use super::predictor::KeyAccessProfile;
use super::stats::{KeyAccessStats, Priority, StorageTier};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the ML tiering engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MlTieringConfig {
    /// SGD learning rate.
    pub learning_rate: f64,
    /// L2 regularization coefficient.
    pub regularization: f64,
    /// SGD momentum factor.
    pub momentum: f64,
    /// Minimum time a key must stay in a tier before being eligible for
    /// migration. Keyed by [`StorageTier`].
    pub min_residency: HashMap<StorageTier, Duration>,
    /// Maximum number of migrations allowed per second.
    pub max_migrations_per_second: u32,
    /// Number of input features the model expects.
    pub feature_count: usize,
    /// Exponential moving average alpha for accuracy tracking.
    pub accuracy_ema_alpha: f64,
}

impl Default for MlTieringConfig {
    fn default() -> Self {
        let mut min_residency = HashMap::new();
        min_residency.insert(StorageTier::Memory, Duration::from_secs(60));
        min_residency.insert(StorageTier::Mmap, Duration::from_secs(300));
        min_residency.insert(StorageTier::Ssd, Duration::from_secs(1800));
        min_residency.insert(StorageTier::Cloud, Duration::from_secs(3600));

        Self {
            learning_rate: 0.01,
            regularization: 0.001,
            momentum: 0.9,
            feature_count: 6,
            min_residency,
            max_migrations_per_second: 100,
            accuracy_ema_alpha: 0.05,
        }
    }
}

// ---------------------------------------------------------------------------
// Feature extraction
// ---------------------------------------------------------------------------

/// Feature vector extracted from [`KeyAccessStats`].
///
/// Each feature is a normalized f64 value designed to be fed into the
/// [`OnlineLearningModel`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureVector {
    /// 1.0 / (1.0 + seconds_since_last_access / 3600.0)
    pub recency_score: f64,
    /// log2(1.0 + accesses_per_hour)
    pub frequency_score: f64,
    /// log2(1.0 + size_bytes) / 30.0  (normalized)
    pub size_score: f64,
    /// reads / (reads + writes + 1.0)
    pub read_write_ratio: f64,
    /// Periodicity score from predictor profile (0.0 if unavailable).
    pub periodicity_score: f64,
    /// 1.0 / (1.0 + total_age_hours / 24.0)
    pub age_score: f64,
}

impl FeatureVector {
    /// Return the features as a fixed-size slice.
    pub fn as_slice(&self) -> [f64; 6] {
        [
            self.recency_score,
            self.frequency_score,
            self.size_score,
            self.read_write_ratio,
            self.periodicity_score,
            self.age_score,
        ]
    }
}

/// Extracts [`FeatureVector`]s from [`KeyAccessStats`].
pub struct FeatureExtractor;

impl FeatureExtractor {
    /// Extract a feature vector from access statistics.
    ///
    /// `current_secs` is the current timestamp in seconds since the engine
    /// started (same epoch as [`KeyAccessStats::last_access_secs`]).
    ///
    /// `profile` is an optional predictor profile that supplies the
    /// periodicity score.
    pub fn extract(
        stats: &KeyAccessStats,
        current_secs: u64,
        profile: Option<&KeyAccessProfile>,
    ) -> FeatureVector {
        let secs_since_access = current_secs.saturating_sub(stats.last_access_secs) as f64;
        let recency_score = 1.0 / (1.0 + secs_since_access / 3600.0);

        let accesses_per_hour = stats.access_counts.reads_per_hour()
            + stats.access_counts.writes_per_hour();
        let frequency_score = (1.0 + accesses_per_hour).log2();

        let size_score = (1.0 + stats.size as f64).log2() / 30.0;

        let reads = stats.access_counts.total_reads() as f64;
        let writes = stats.access_counts.total_writes() as f64;
        let read_write_ratio = reads / (reads + writes + 1.0);

        let periodicity_score = profile
            .map(|p| {
                if p.is_periodic {
                    p.confidence
                } else {
                    0.0
                }
            })
            .unwrap_or(0.0);

        let total_age_hours = current_secs as f64 / 3600.0;
        let age_score = 1.0 / (1.0 + total_age_hours / 24.0);

        FeatureVector {
            recency_score,
            frequency_score,
            size_score,
            read_write_ratio,
            periodicity_score,
            age_score,
        }
    }
}

// ---------------------------------------------------------------------------
// Online learning model
// ---------------------------------------------------------------------------

/// Lightweight online learning model using SGD with momentum.
///
/// The model predicts the probability that a key will be accessed in the
/// near future. It is trained incrementally on every access event so there
/// is no need for batch retraining.
pub struct OnlineLearningModel {
    /// Per-feature weights.
    weights: RwLock<Vec<f64>>,
    /// Bias term.
    bias: RwLock<f64>,
    /// Momentum velocity per weight.
    velocity: RwLock<Vec<f64>>,
    /// Momentum velocity for the bias.
    bias_velocity: RwLock<f64>,
    /// Configuration snapshot (learning rate, regularization, momentum).
    config: MlTieringConfig,
    /// Exponential moving average of prediction accuracy (0.0–1.0).
    accuracy_ema: RwLock<f64>,
    /// Total training samples processed.
    samples_trained: AtomicU64,
}

impl OnlineLearningModel {
    /// Create a new model with the given configuration.
    pub fn new(config: MlTieringConfig) -> Self {
        let n = config.feature_count;
        Self {
            weights: RwLock::new(vec![0.0; n]),
            bias: RwLock::new(0.0),
            velocity: RwLock::new(vec![0.0; n]),
            bias_velocity: RwLock::new(0.0),
            accuracy_ema: RwLock::new(0.5),
            samples_trained: AtomicU64::new(0),
            config,
        }
    }

    /// Predict access probability for the given feature vector.
    ///
    /// Returns a value in `[0.0, 1.0]` via sigmoid activation.
    pub fn predict(&self, features: &[f64; 6]) -> f64 {
        let weights = self.weights.read();
        let bias = *self.bias.read();
        let z: f64 = weights
            .iter()
            .zip(features.iter())
            .map(|(w, f)| w * f)
            .sum::<f64>()
            + bias;
        sigmoid(z)
    }

    /// Train the model on a single sample.
    ///
    /// `features` is the input vector and `label` is the ground truth
    /// (1.0 = accessed, 0.0 = not accessed).
    pub fn train(&self, features: &[f64; 6], label: f64) {
        let prediction = self.predict(features);
        let error = prediction - label;

        let lr = self.config.learning_rate;
        let reg = self.config.regularization;
        let mom = self.config.momentum;

        {
            let mut weights = self.weights.write();
            let mut velocity = self.velocity.write();
            for i in 0..weights.len() {
                let grad = error * features[i] + reg * weights[i];
                velocity[i] = mom * velocity[i] + lr * grad;
                weights[i] -= velocity[i];
            }
        }

        {
            let mut bias = self.bias.write();
            let mut bv = self.bias_velocity.write();
            *bv = mom * *bv + lr * error;
            *bias -= *bv;
        }

        // Update accuracy EMA
        let correct = if (label > 0.5) == (prediction > 0.5) {
            1.0
        } else {
            0.0
        };
        {
            let alpha = self.config.accuracy_ema_alpha;
            let mut acc = self.accuracy_ema.write();
            *acc = alpha * correct + (1.0 - alpha) * *acc;
        }

        self.samples_trained.fetch_add(1, Ordering::Relaxed);
    }

    /// Current exponential moving average of prediction accuracy.
    pub fn accuracy(&self) -> f64 {
        *self.accuracy_ema.read()
    }

    /// Total number of training samples processed.
    pub fn samples_trained(&self) -> u64 {
        self.samples_trained.load(Ordering::Relaxed)
    }

    /// Return a snapshot of the current weights (for diagnostics).
    pub fn weights(&self) -> Vec<f64> {
        self.weights.read().clone()
    }
}

/// Sigmoid activation function.
fn sigmoid(x: f64) -> f64 {
    1.0 / (1.0 + (-x).exp())
}

// ---------------------------------------------------------------------------
// Adaptive migration controller
// ---------------------------------------------------------------------------

/// Per-key migration state tracked by the controller.
#[derive(Debug, Clone)]
struct KeyMigrationState {
    /// When the key arrived in its current tier.
    arrived_at: Instant,
    /// Current tier.
    tier: StorageTier,
}

/// Controls tier migration decisions using the [`OnlineLearningModel`].
///
/// The controller adds safety rails on top of the raw model predictions:
/// - **Minimum residency timers** prevent thrashing by requiring keys to
///   stay in a tier for a configurable duration before moving.
/// - **Rate limiting** caps the number of migrations per second.
/// - **Feedback tracking** records migration outcomes so the model can
///   learn from its own decisions.
pub struct AdaptiveMigrationController {
    /// The underlying ML model.
    model: OnlineLearningModel,
    /// Per-key migration state.
    key_states: RwLock<HashMap<u64, KeyMigrationState>>,
    /// Migrations executed in the current rate-limit window.
    migrations_this_second: AtomicU64,
    /// Start of the current rate-limit window.
    window_start: RwLock<Instant>,
    /// Successful migrations (for feedback tracking).
    successful_migrations: AtomicU64,
    /// Failed migrations (for feedback tracking).
    failed_migrations: AtomicU64,
    /// Configuration.
    config: MlTieringConfig,
}

impl AdaptiveMigrationController {
    /// Create a new migration controller.
    pub fn new(config: MlTieringConfig) -> Self {
        let model = OnlineLearningModel::new(config.clone());
        Self {
            model,
            key_states: RwLock::new(HashMap::new()),
            migrations_this_second: AtomicU64::new(0),
            window_start: RwLock::new(Instant::now()),
            successful_migrations: AtomicU64::new(0),
            failed_migrations: AtomicU64::new(0),
            config,
        }
    }

    /// Decide whether a key should migrate, and to which tier.
    ///
    /// Returns `Some(target_tier)` if the key should be migrated, or `None`
    /// if it should stay put. The decision respects residency timers and
    /// rate limits.
    pub fn should_migrate(
        &self,
        key_hash: u64,
        current_tier: StorageTier,
        stats: &KeyAccessStats,
        current_secs: u64,
        profile: Option<&KeyAccessProfile>,
    ) -> Option<StorageTier> {
        // Pinned keys never migrate
        if stats.pinned {
            return None;
        }

        // Critical priority keys stay in memory
        if stats.priority == Priority::Critical {
            return if current_tier != StorageTier::Memory {
                Some(StorageTier::Memory)
            } else {
                None
            };
        }

        // Check residency timer
        if !self.residency_elapsed(key_hash, current_tier) {
            return None;
        }

        // Check rate limit
        if !self.try_acquire_rate_limit() {
            return None;
        }

        // Score using the ML model
        let features = FeatureExtractor::extract(stats, current_secs, profile);
        let score = self.model.predict(&features.as_slice());

        let target = score_to_tier(score, stats.priority);

        if target == current_tier {
            return None;
        }

        // Record new residency state
        {
            let mut states = self.key_states.write();
            states.insert(
                key_hash,
                KeyMigrationState {
                    arrived_at: Instant::now(),
                    tier: target,
                },
            );
        }

        debug!(
            key_hash,
            score,
            from = %current_tier,
            to = %target,
            "ML engine recommends migration"
        );

        Some(target)
    }

    /// Train the model on an observed access event.
    pub fn record_access(
        &self,
        stats: &KeyAccessStats,
        current_secs: u64,
        profile: Option<&KeyAccessProfile>,
    ) {
        let features = FeatureExtractor::extract(stats, current_secs, profile);
        self.model.train(&features.as_slice(), 1.0);
    }

    /// Record a non-access (the key was not accessed in the prediction
    /// window). Used for negative-sample training.
    pub fn record_non_access(
        &self,
        stats: &KeyAccessStats,
        current_secs: u64,
        profile: Option<&KeyAccessProfile>,
    ) {
        let features = FeatureExtractor::extract(stats, current_secs, profile);
        self.model.train(&features.as_slice(), 0.0);
    }

    /// Record a successful migration (for feedback tracking).
    pub fn record_migration_success(&self) {
        self.successful_migrations.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed migration (for feedback tracking).
    pub fn record_migration_failure(&self) {
        self.failed_migrations.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the underlying model's prediction accuracy (EMA).
    pub fn model_accuracy(&self) -> f64 {
        self.model.accuracy()
    }

    /// Total training samples processed by the model.
    pub fn samples_trained(&self) -> u64 {
        self.model.samples_trained()
    }

    /// Number of successful migrations recorded.
    pub fn successful_migrations(&self) -> u64 {
        self.successful_migrations.load(Ordering::Relaxed)
    }

    /// Number of failed migrations recorded.
    pub fn failed_migrations(&self) -> u64 {
        self.failed_migrations.load(Ordering::Relaxed)
    }

    /// Reference to the underlying model (for diagnostics).
    pub fn model(&self) -> &OnlineLearningModel {
        &self.model
    }

    // -- internal helpers ---------------------------------------------------

    /// Check whether the key has satisfied its minimum residency in the
    /// current tier.
    fn residency_elapsed(&self, key_hash: u64, current_tier: StorageTier) -> bool {
        let states = self.key_states.read();
        match states.get(&key_hash) {
            Some(state) if state.tier == current_tier => {
                let min = self
                    .config
                    .min_residency
                    .get(&current_tier)
                    .copied()
                    .unwrap_or(Duration::from_secs(60));
                state.arrived_at.elapsed() >= min
            }
            // No prior state — allow migration.
            _ => true,
        }
    }

    /// Try to acquire a slot in the per-second rate limiter.
    fn try_acquire_rate_limit(&self) -> bool {
        let now = Instant::now();
        {
            let mut ws = self.window_start.write();
            if now.duration_since(*ws) >= Duration::from_secs(1) {
                *ws = now;
                self.migrations_this_second.store(0, Ordering::Relaxed);
            }
        }
        let prev = self.migrations_this_second.fetch_add(1, Ordering::Relaxed);
        if prev >= self.config.max_migrations_per_second as u64 {
            self.migrations_this_second.fetch_sub(1, Ordering::Relaxed);
            return false;
        }
        true
    }
}

/// Map a model score + priority to a target [`StorageTier`].
fn score_to_tier(score: f64, priority: Priority) -> StorageTier {
    match priority {
        Priority::Critical => StorageTier::Memory,
        Priority::High => {
            if score > 0.5 {
                StorageTier::Memory
            } else {
                StorageTier::Mmap
            }
        }
        Priority::Normal => {
            if score > 0.7 {
                StorageTier::Memory
            } else if score > 0.4 {
                StorageTier::Mmap
            } else if score > 0.15 {
                StorageTier::Ssd
            } else {
                StorageTier::Cloud
            }
        }
        Priority::Low => {
            if score > 0.8 {
                StorageTier::Mmap
            } else if score > 0.3 {
                StorageTier::Ssd
            } else {
                StorageTier::Cloud
            }
        }
        Priority::Archive => StorageTier::Cloud,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tiering::stats::AccessCounts;

    fn make_stats(
        size: u32,
        tier: StorageTier,
        last_access: u64,
        reads: u32,
        writes: u32,
    ) -> KeyAccessStats {
        KeyAccessStats {
            key_hash: 0,
            size,
            tier,
            access_counts: AccessCounts {
                reads_1m: reads,
                reads_1h: reads,
                reads_1d: reads,
                writes_1m: writes,
                writes_1h: writes,
                writes_1d: writes,
            },
            last_access_secs: last_access,
            last_write_secs: last_access,
            priority: Priority::Normal,
            pinned: false,
        }
    }

    // -- Feature extraction -------------------------------------------------

    #[test]
    fn test_feature_extraction_recency() {
        let stats = make_stats(1024, StorageTier::Memory, 3600, 10, 2);
        let features = FeatureExtractor::extract(&stats, 7200, None);
        // 1 hour since last access → 1/(1+1) = 0.5
        assert!((features.recency_score - 0.5).abs() < 1e-6);
    }

    #[test]
    fn test_feature_extraction_frequency() {
        let stats = make_stats(1024, StorageTier::Memory, 100, 48, 0);
        let features = FeatureExtractor::extract(&stats, 100, None);
        // reads_per_hour = 48/24 = 2.0, writes_per_hour = 0
        // frequency = log2(1+2) = log2(3) ≈ 1.585
        let expected = 3.0_f64.log2();
        assert!(
            (features.frequency_score - expected).abs() < 1e-6,
            "got {}",
            features.frequency_score
        );
    }

    #[test]
    fn test_feature_extraction_size_normalized() {
        let stats = make_stats(1024, StorageTier::Memory, 0, 0, 0);
        let features = FeatureExtractor::extract(&stats, 0, None);
        // log2(1025) / 30 ≈ 0.333
        let expected = (1025.0_f64).log2() / 30.0;
        assert!(
            (features.size_score - expected).abs() < 1e-6,
            "got {}",
            features.size_score
        );
    }

    #[test]
    fn test_feature_extraction_read_write_ratio() {
        let stats = make_stats(100, StorageTier::Memory, 0, 10, 5);
        let features = FeatureExtractor::extract(&stats, 0, None);
        // 10 / (10 + 5 + 1) = 0.625
        let expected = 10.0 / 16.0;
        assert!(
            (features.read_write_ratio - expected).abs() < 1e-6,
            "got {}",
            features.read_write_ratio
        );
    }

    #[test]
    fn test_feature_extraction_with_periodic_profile() {
        let stats = make_stats(100, StorageTier::Memory, 0, 0, 0);
        let profile = KeyAccessProfile {
            is_periodic: true,
            confidence: 0.85,
            ..Default::default()
        };
        let features = FeatureExtractor::extract(&stats, 0, Some(&profile));
        assert!((features.periodicity_score - 0.85).abs() < 1e-6);
    }

    // -- Online learning model ----------------------------------------------

    #[test]
    fn test_model_initial_prediction() {
        let model = OnlineLearningModel::new(MlTieringConfig::default());
        let features = [0.5, 0.5, 0.5, 0.5, 0.0, 0.5];
        // With zero weights, sigmoid(0) = 0.5
        let pred = model.predict(&features);
        assert!(
            (pred - 0.5).abs() < 1e-6,
            "initial prediction should be 0.5, got {}",
            pred
        );
    }

    #[test]
    fn test_model_convergence_hot_key() {
        let config = MlTieringConfig {
            learning_rate: 0.1,
            ..Default::default()
        };
        let model = OnlineLearningModel::new(config);

        let hot_features = [1.0, 1.0, 0.3, 0.8, 0.5, 0.9];
        let cold_features = [0.1, 0.0, 0.3, 0.2, 0.0, 0.1];

        // Train: hot key accessed (label=1), cold key not (label=0)
        for _ in 0..200 {
            model.train(&hot_features, 1.0);
            model.train(&cold_features, 0.0);
        }

        let hot_score = model.predict(&hot_features);
        let cold_score = model.predict(&cold_features);

        assert!(
            hot_score > 0.7,
            "hot key should score high, got {}",
            hot_score
        );
        assert!(
            cold_score < 0.3,
            "cold key should score low, got {}",
            cold_score
        );
        assert!(
            hot_score > cold_score,
            "hot key ({}) should score higher than cold key ({})",
            hot_score,
            cold_score
        );
    }

    #[test]
    fn test_model_accuracy_tracking() {
        let config = MlTieringConfig {
            learning_rate: 0.1,
            accuracy_ema_alpha: 0.1,
            ..Default::default()
        };
        let model = OnlineLearningModel::new(config);

        // Train consistently correct patterns
        let features = [1.0, 1.0, 0.5, 0.5, 0.5, 0.5];
        for _ in 0..100 {
            model.train(&features, 1.0);
        }

        // After many correct updates the EMA accuracy should be high
        assert!(
            model.accuracy() > 0.7,
            "accuracy should improve, got {}",
            model.accuracy()
        );
        assert_eq!(model.samples_trained(), 100);
    }

    // -- Migration controller -----------------------------------------------

    #[test]
    fn test_migration_residency_enforcement() {
        let mut config = MlTieringConfig::default();
        // Set a long residency so the test can verify blocking
        config.min_residency.insert(StorageTier::Memory, Duration::from_secs(9999));

        let controller = AdaptiveMigrationController::new(config);
        let stats = make_stats(1024, StorageTier::Memory, 100, 0, 0);

        // Insert a residency record for this key
        {
            let mut states = controller.key_states.write();
            states.insert(
                0,
                KeyMigrationState {
                    arrived_at: Instant::now(),
                    tier: StorageTier::Memory,
                },
            );
        }

        // should_migrate must return None due to residency timer
        let result = controller.should_migrate(0, StorageTier::Memory, &stats, 200, None);
        assert!(
            result.is_none(),
            "migration should be blocked by residency timer"
        );
    }

    #[test]
    fn test_migration_rate_limiting() {
        let config = MlTieringConfig {
            max_migrations_per_second: 2,
            ..Default::default()
        };
        let controller = AdaptiveMigrationController::new(config);

        // Exhaust the rate limit
        assert!(controller.try_acquire_rate_limit());
        assert!(controller.try_acquire_rate_limit());
        assert!(
            !controller.try_acquire_rate_limit(),
            "third acquisition should be denied"
        );
    }

    #[test]
    fn test_migration_pinned_key() {
        let controller = AdaptiveMigrationController::new(MlTieringConfig::default());
        let mut stats = make_stats(1024, StorageTier::Memory, 100, 0, 0);
        stats.pinned = true;

        let result = controller.should_migrate(42, StorageTier::Memory, &stats, 200, None);
        assert!(result.is_none(), "pinned keys must not migrate");
    }

    #[test]
    fn test_migration_feedback_tracking() {
        let controller = AdaptiveMigrationController::new(MlTieringConfig::default());

        controller.record_migration_success();
        controller.record_migration_success();
        controller.record_migration_failure();

        assert_eq!(controller.successful_migrations(), 2);
        assert_eq!(controller.failed_migrations(), 1);
    }

    #[test]
    fn test_score_to_tier_mapping() {
        assert_eq!(score_to_tier(0.9, Priority::Normal), StorageTier::Memory);
        assert_eq!(score_to_tier(0.5, Priority::Normal), StorageTier::Mmap);
        assert_eq!(score_to_tier(0.2, Priority::Normal), StorageTier::Ssd);
        assert_eq!(score_to_tier(0.05, Priority::Normal), StorageTier::Cloud);

        // Critical always Memory
        assert_eq!(score_to_tier(0.0, Priority::Critical), StorageTier::Memory);
    }
}
