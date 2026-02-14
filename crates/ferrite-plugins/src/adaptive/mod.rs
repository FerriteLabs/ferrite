//! Adaptive Performance Engine
//!
//! ML-based auto-tuning that adapts to workload patterns for self-optimizing performance.
//!
//! # Features
//!
//! - Automatic memory allocation tuning between tiers
//! - Workload pattern detection and prediction
//! - Dynamic eviction policy adjustment
//! - Thread pool sizing optimization
//! - Data structure optimization based on usage
//! - Predictive cache pre-warming
//!
//! # Example
//!
//! ```ignore
//! // Enable adaptive mode
//! CONFIG SET adaptive-mode on
//!
//! // The system automatically:
//! // - Adjusts memory allocation between tiers
//! // - Tunes eviction policies based on access patterns
//! // - Pre-warms cache for predictable traffic patterns
//! // - Adjusts thread pool sizes
//! // - Optimizes data structures based on usage
//!
//! // View current adaptations
//! ADAPTIVE STATUS
//! ```

pub mod detector;
pub mod engine;
pub mod optimizer;
pub mod predictor;
pub mod rl_agent;
pub mod tier_classifier;
pub mod tuner;

pub use detector::{PatternDetector, WorkloadPattern, WorkloadType};
pub use engine::{Adaptation, AdaptiveEngine, AdaptiveStatus};
pub use optimizer::{ConfigOptimizer, OptimizationTarget, ResourceOptimizer};
pub use predictor::{LoadPredictor, PredictionResult, TrafficPattern};
pub use rl_agent::{RLTieringAgent, RLTieringConfig, TieringAction, TieringState};
pub use tier_classifier::{
    ClassifierConfig, ClassifierStats, KeyAccessStats, Tier, TierClassifier, TierDecision,
};
pub use tuner::{AutoTuner, TuningDecision, TuningParameter};

use serde::{Deserialize, Serialize};

/// Configuration for adaptive performance
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdaptiveConfig {
    /// Enable adaptive mode
    pub enabled: bool,
    /// Learning rate for ML models (0.0 to 1.0)
    pub learning_rate: f64,
    /// Adaptation interval in seconds
    pub adaptation_interval_secs: u64,
    /// Minimum observations before adapting
    pub min_observations: usize,
    /// Maximum memory tier adjustment per cycle (percentage)
    pub max_tier_adjustment_percent: f64,
    /// Enable predictive pre-warming
    pub predictive_warming_enabled: bool,
    /// Look-ahead time for predictions (seconds)
    pub prediction_horizon_secs: u64,
    /// Confidence threshold for adaptations (0.0 to 1.0)
    pub confidence_threshold: f64,
    /// Enable conservative mode (slower but safer adaptations)
    pub conservative_mode: bool,
    /// History retention in hours
    pub history_retention_hours: u64,
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            learning_rate: 0.1,
            adaptation_interval_secs: 60,
            min_observations: 100,
            max_tier_adjustment_percent: 10.0,
            predictive_warming_enabled: true,
            prediction_horizon_secs: 300, // 5 minutes
            confidence_threshold: 0.7,
            conservative_mode: true,
            history_retention_hours: 24,
        }
    }
}

impl AdaptiveConfig {
    /// Create a configuration for aggressive optimization
    pub fn aggressive() -> Self {
        Self {
            learning_rate: 0.3,
            max_tier_adjustment_percent: 25.0,
            confidence_threshold: 0.5,
            conservative_mode: false,
            ..Default::default()
        }
    }

    /// Create a configuration for production (stable)
    pub fn production() -> Self {
        Self {
            learning_rate: 0.05,
            max_tier_adjustment_percent: 5.0,
            confidence_threshold: 0.8,
            conservative_mode: true,
            ..Default::default()
        }
    }
}

/// Metrics sample for analysis
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricsSample {
    /// Timestamp (Unix ms)
    pub timestamp: u64,
    /// Operations per second
    pub ops_per_second: f64,
    /// Read ratio (0.0 to 1.0)
    pub read_ratio: f64,
    /// Average latency in microseconds
    pub avg_latency_us: f64,
    /// P99 latency in microseconds
    pub p99_latency_us: f64,
    /// Memory usage (bytes)
    pub memory_used: u64,
    /// Memory limit (bytes)
    pub memory_limit: u64,
    /// Cache hit rate (0.0 to 1.0)
    pub cache_hit_rate: f64,
    /// Active connections
    pub connections: u64,
    /// CPU utilization (0.0 to 1.0)
    pub cpu_utilization: f64,
}

impl MetricsSample {
    /// Create a new sample with current timestamp
    pub fn new() -> Self {
        Self {
            timestamp: current_timestamp_ms(),
            ops_per_second: 0.0,
            read_ratio: 0.0,
            avg_latency_us: 0.0,
            p99_latency_us: 0.0,
            memory_used: 0,
            memory_limit: 0,
            cache_hit_rate: 0.0,
            connections: 0,
            cpu_utilization: 0.0,
        }
    }

    /// Calculate memory pressure (0.0 to 1.0)
    pub fn memory_pressure(&self) -> f64 {
        if self.memory_limit == 0 {
            0.0
        } else {
            self.memory_used as f64 / self.memory_limit as f64
        }
    }
}

impl Default for MetricsSample {
    fn default() -> Self {
        Self::new()
    }
}

/// Adaptive system statistics
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AdaptiveStats {
    /// Total adaptations made
    pub adaptations_made: u64,
    /// Successful adaptations
    pub successful_adaptations: u64,
    /// Rolled back adaptations
    pub rollbacks: u64,
    /// Current confidence score
    pub current_confidence: f64,
    /// Detected workload type
    pub workload_type: Option<String>,
    /// Time since last adaptation (seconds)
    pub time_since_adaptation_secs: u64,
    /// Performance improvement percentage
    pub performance_improvement_percent: f64,
}

/// Errors that can occur in adaptive operations
#[derive(Debug, Clone, thiserror::Error)]
pub enum AdaptiveError {
    /// Not enough data for adaptation
    #[error("insufficient data: need {needed} observations, have {have}")]
    InsufficientData {
        /// Required observations
        needed: usize,
        /// Available observations
        have: usize,
    },

    /// Low confidence in prediction
    #[error("low confidence: {0:.2} < threshold {1:.2}")]
    LowConfidence(f64, f64),

    /// Adaptation in progress
    #[error("adaptation already in progress")]
    AdaptationInProgress,

    /// Invalid configuration
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// System constraint violation
    #[error("constraint violation: {0}")]
    ConstraintViolation(String),

    /// Internal error
    #[error("internal error: {0}")]
    Internal(String),
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

    #[test]
    fn test_adaptive_config_default() {
        let config = AdaptiveConfig::default();
        assert!(config.enabled);
        assert!(config.conservative_mode);
        assert!(config.learning_rate > 0.0 && config.learning_rate <= 1.0);
    }

    #[test]
    fn test_adaptive_config_variants() {
        let aggressive = AdaptiveConfig::aggressive();
        let production = AdaptiveConfig::production();

        assert!(aggressive.learning_rate > production.learning_rate);
        assert!(!aggressive.conservative_mode);
        assert!(production.conservative_mode);
    }

    #[test]
    fn test_metrics_sample() {
        let mut sample = MetricsSample::new();
        sample.memory_used = 500;
        sample.memory_limit = 1000;

        assert_eq!(sample.memory_pressure(), 0.5);
    }
}
