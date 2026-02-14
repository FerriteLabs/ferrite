//! Adaptive Engine
//!
//! Core engine that coordinates adaptive performance optimizations.

use super::{
    detector::{PatternDetector, WorkloadPattern},
    optimizer::{ConfigOptimizer, ResourceOptimizer},
    predictor::LoadPredictor,
    tuner::{AutoTuner, TuningDecision},
    AdaptiveConfig, AdaptiveError, AdaptiveStats, MetricsSample,
};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Adaptive performance engine
pub struct AdaptiveEngine {
    /// Configuration
    config: RwLock<AdaptiveConfig>,
    /// Pattern detector
    detector: RwLock<PatternDetector>,
    /// Load predictor
    predictor: RwLock<LoadPredictor>,
    /// Auto tuner
    tuner: RwLock<AutoTuner>,
    /// Config optimizer
    config_optimizer: RwLock<ConfigOptimizer>,
    /// Resource optimizer
    resource_optimizer: RwLock<ResourceOptimizer>,
    /// Active adaptations
    active_adaptations: RwLock<Vec<Adaptation>>,
    /// Adaptation history
    history: RwLock<VecDeque<AdaptationRecord>>,
    /// Whether adaptation is in progress
    adapting: AtomicBool,
    /// Last adaptation time
    last_adaptation: RwLock<Option<Instant>>,
    /// Statistics
    stats: AdaptiveEngineStats,
    /// Running flag
    running: AtomicBool,
}

impl AdaptiveEngine {
    /// Create a new adaptive engine
    pub fn new(config: AdaptiveConfig) -> Self {
        Self {
            config: RwLock::new(config),
            detector: RwLock::new(PatternDetector::default()),
            predictor: RwLock::new(LoadPredictor::default()),
            tuner: RwLock::new(AutoTuner::default()),
            config_optimizer: RwLock::new(ConfigOptimizer::default()),
            resource_optimizer: RwLock::new(ResourceOptimizer::default()),
            active_adaptations: RwLock::new(Vec::new()),
            history: RwLock::new(VecDeque::with_capacity(1000)),
            adapting: AtomicBool::new(false),
            last_adaptation: RwLock::new(None),
            stats: AdaptiveEngineStats::default(),
            running: AtomicBool::new(false),
        }
    }

    /// Start the adaptive engine
    pub async fn start(&self) {
        self.running.store(true, Ordering::SeqCst);
    }

    /// Stop the adaptive engine
    pub async fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Check if the engine is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Record a metrics sample
    pub async fn record_sample(&self, sample: MetricsSample) {
        let mut detector = self.detector.write().await;
        detector.add_sample(sample.clone());

        let mut predictor = self.predictor.write().await;
        predictor.add_sample(sample);

        self.stats.samples_recorded.fetch_add(1, Ordering::Relaxed);
    }

    /// Run an adaptation cycle
    pub async fn adapt(&self) -> Result<Vec<Adaptation>, AdaptiveError> {
        let config = self.config.read().await;
        if !config.enabled {
            return Ok(Vec::new());
        }

        // Check if adaptation is already in progress
        if self.adapting.swap(true, Ordering::SeqCst) {
            return Err(AdaptiveError::AdaptationInProgress);
        }

        // Ensure we reset the flag when done
        let _guard = scopeguard::guard((), |_| {
            self.adapting.store(false, Ordering::SeqCst);
        });

        // Detect current workload pattern
        let mut detector = self.detector.write().await;
        let pattern = match detector.detect() {
            Some(p) => p,
            None => {
                return Err(AdaptiveError::InsufficientData {
                    needed: 10,
                    have: 0,
                });
            }
        };
        drop(detector);

        // Check confidence threshold
        if pattern.confidence < config.confidence_threshold {
            return Err(AdaptiveError::LowConfidence(
                pattern.confidence,
                config.confidence_threshold,
            ));
        }

        // Get predictions
        let mut predictor = self.predictor.write().await;
        let prediction = predictor.predict(Duration::from_secs(config.prediction_horizon_secs));
        drop(predictor);

        // Generate tuning decisions
        let mut tuner = self.tuner.write().await;
        let decisions = tuner.analyze(&pattern, prediction.as_ref());
        drop(tuner);

        // Convert decisions to adaptations
        let mut adaptations = Vec::new();
        for decision in decisions {
            let adaptation = self.decision_to_adaptation(decision, &pattern).await;
            adaptations.push(adaptation);
        }

        // Apply adaptations if not in conservative mode or if confidence is high
        if !config.conservative_mode || pattern.confidence > 0.9 {
            for adaptation in &adaptations {
                self.apply_adaptation(adaptation).await?;
            }
        }

        // Record adaptations
        let mut active = self.active_adaptations.write().await;
        active.extend(adaptations.clone());

        // Update last adaptation time
        let mut last = self.last_adaptation.write().await;
        *last = Some(Instant::now());

        // Update stats
        self.stats
            .adaptations_made
            .fetch_add(adaptations.len() as u64, Ordering::Relaxed);

        Ok(adaptations)
    }

    /// Convert a tuning decision to an adaptation
    async fn decision_to_adaptation(
        &self,
        decision: TuningDecision,
        pattern: &WorkloadPattern,
    ) -> Adaptation {
        Adaptation {
            id: format!("adapt-{}", current_timestamp_ms()),
            adaptation_type: decision.adaptation_type(),
            description: decision.description(),
            parameters: decision.parameters(),
            confidence: pattern.confidence,
            expected_improvement: decision.expected_improvement(),
            applied_at: None,
            rollback_at: None,
            status: AdaptationStatus::Pending,
        }
    }

    /// Apply an adaptation
    async fn apply_adaptation(&self, adaptation: &Adaptation) -> Result<(), AdaptiveError> {
        match &adaptation.adaptation_type {
            AdaptationType::MemoryTierAdjustment { .. } => {
                let mut optimizer = self.resource_optimizer.write().await;
                optimizer.apply_memory_adjustment(adaptation)?;
            }
            AdaptationType::EvictionPolicyChange { .. } => {
                let mut optimizer = self.config_optimizer.write().await;
                optimizer.apply_eviction_change(adaptation)?;
            }
            AdaptationType::ThreadPoolResize { .. } => {
                let mut optimizer = self.resource_optimizer.write().await;
                optimizer.apply_thread_resize(adaptation)?;
            }
            AdaptationType::CachePrewarm { .. } => {
                // Pre-warming is handled asynchronously
                self.stats
                    .prewarm_operations
                    .fetch_add(1, Ordering::Relaxed);
            }
            AdaptationType::ConfigurationChange { .. } => {
                let mut optimizer = self.config_optimizer.write().await;
                optimizer.apply_config_change(adaptation)?;
            }
        }

        self.stats
            .successful_adaptations
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Rollback an adaptation
    pub async fn rollback(&self, adaptation_id: &str) -> Result<(), AdaptiveError> {
        let mut active = self.active_adaptations.write().await;
        let pos = active
            .iter()
            .position(|a| a.id == adaptation_id)
            .ok_or_else(|| AdaptiveError::Internal("Adaptation not found".to_string()))?;

        let mut adaptation = active.remove(pos);
        adaptation.status = AdaptationStatus::RolledBack;
        adaptation.rollback_at = Some(current_timestamp_ms());

        // Record in history
        let mut history = self.history.write().await;
        history.push_back(AdaptationRecord {
            adaptation,
            outcome: AdaptationOutcome::RolledBack,
            performance_delta: None,
        });

        self.stats.rollbacks.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Get current status
    pub async fn status(&self) -> AdaptiveStatus {
        let config = self.config.read().await;
        let detector = self.detector.read().await;
        let active = self.active_adaptations.read().await;
        let last = self.last_adaptation.read().await;

        let time_since_adaptation = last.map(|t| t.elapsed().as_secs()).unwrap_or(0);

        AdaptiveStatus {
            enabled: config.enabled,
            running: self.running.load(Ordering::SeqCst),
            current_pattern: detector.current_pattern().cloned(),
            active_adaptations: active.len(),
            adaptations_made: self.stats.adaptations_made.load(Ordering::Relaxed),
            successful_adaptations: self.stats.successful_adaptations.load(Ordering::Relaxed),
            rollbacks: self.stats.rollbacks.load(Ordering::Relaxed),
            time_since_adaptation_secs: time_since_adaptation,
            samples_recorded: self.stats.samples_recorded.load(Ordering::Relaxed),
        }
    }

    /// Get statistics
    pub fn stats(&self) -> AdaptiveStats {
        AdaptiveStats {
            adaptations_made: self.stats.adaptations_made.load(Ordering::Relaxed),
            successful_adaptations: self.stats.successful_adaptations.load(Ordering::Relaxed),
            rollbacks: self.stats.rollbacks.load(Ordering::Relaxed),
            current_confidence: 0.0, // Would need pattern access
            workload_type: None,
            time_since_adaptation_secs: 0,
            performance_improvement_percent: 0.0,
        }
    }

    /// Get active adaptations
    pub async fn get_active_adaptations(&self) -> Vec<Adaptation> {
        let active = self.active_adaptations.read().await;
        active.clone()
    }

    /// Get adaptation history
    pub async fn get_history(&self, limit: usize) -> Vec<AdaptationRecord> {
        let history = self.history.read().await;
        history.iter().rev().take(limit).cloned().collect()
    }

    /// Update configuration
    pub async fn update_config(&self, config: AdaptiveConfig) {
        let mut current = self.config.write().await;
        *current = config;
    }

    /// Clear all data and reset
    pub async fn reset(&self) {
        let mut detector = self.detector.write().await;
        detector.clear();

        let mut predictor = self.predictor.write().await;
        predictor.clear();

        let mut active = self.active_adaptations.write().await;
        active.clear();

        let mut history = self.history.write().await;
        history.clear();

        let mut last = self.last_adaptation.write().await;
        *last = None;
    }
}

impl Default for AdaptiveEngine {
    fn default() -> Self {
        Self::new(AdaptiveConfig::default())
    }
}

/// Adaptive engine statistics
#[derive(Default)]
struct AdaptiveEngineStats {
    samples_recorded: AtomicU64,
    adaptations_made: AtomicU64,
    successful_adaptations: AtomicU64,
    rollbacks: AtomicU64,
    prewarm_operations: AtomicU64,
}

/// Adaptive engine status
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdaptiveStatus {
    /// Whether adaptive mode is enabled
    pub enabled: bool,
    /// Whether the engine is running
    pub running: bool,
    /// Current detected pattern
    pub current_pattern: Option<WorkloadPattern>,
    /// Number of active adaptations
    pub active_adaptations: usize,
    /// Total adaptations made
    pub adaptations_made: u64,
    /// Successful adaptations
    pub successful_adaptations: u64,
    /// Rollbacks performed
    pub rollbacks: u64,
    /// Time since last adaptation (seconds)
    pub time_since_adaptation_secs: u64,
    /// Samples recorded
    pub samples_recorded: u64,
}

/// An adaptation applied by the engine
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Adaptation {
    /// Unique identifier
    pub id: String,
    /// Type of adaptation
    pub adaptation_type: AdaptationType,
    /// Human-readable description
    pub description: String,
    /// Parameters for the adaptation
    pub parameters: std::collections::HashMap<String, String>,
    /// Confidence level
    pub confidence: f64,
    /// Expected improvement percentage
    pub expected_improvement: f64,
    /// When applied
    pub applied_at: Option<u64>,
    /// When rolled back (if applicable)
    pub rollback_at: Option<u64>,
    /// Current status
    pub status: AdaptationStatus,
}

/// Type of adaptation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AdaptationType {
    /// Memory tier allocation adjustment
    MemoryTierAdjustment {
        /// Tier name
        tier: String,
        /// New allocation percentage
        allocation_percent: f64,
    },
    /// Eviction policy change
    EvictionPolicyChange {
        /// New policy name
        policy: String,
    },
    /// Thread pool resize
    ThreadPoolResize {
        /// Pool name
        pool: String,
        /// New size
        new_size: usize,
    },
    /// Cache pre-warming
    CachePrewarm {
        /// Keys to prewarm
        key_patterns: Vec<String>,
    },
    /// Configuration parameter change
    ConfigurationChange {
        /// Parameter name
        parameter: String,
        /// New value
        value: String,
    },
}

/// Status of an adaptation
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdaptationStatus {
    /// Pending application
    Pending,
    /// Applied and active
    Active,
    /// Completed successfully
    Completed,
    /// Rolled back
    RolledBack,
    /// Failed to apply
    Failed,
}

/// Record of a past adaptation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdaptationRecord {
    /// The adaptation
    pub adaptation: Adaptation,
    /// Outcome
    pub outcome: AdaptationOutcome,
    /// Performance delta (positive = improvement)
    pub performance_delta: Option<f64>,
}

/// Outcome of an adaptation
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdaptationOutcome {
    /// Successful
    Success,
    /// Partially successful
    Partial,
    /// No significant change
    Neutral,
    /// Negative impact, rolled back
    RolledBack,
    /// Failed to apply
    Failed,
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

    #[tokio::test]
    async fn test_engine_creation() {
        let engine = AdaptiveEngine::default();
        let status = engine.status().await;
        assert!(status.enabled);
        assert!(!status.running);
    }

    #[tokio::test]
    async fn test_engine_start_stop() {
        let engine = AdaptiveEngine::default();

        engine.start().await;
        assert!(engine.is_running());

        engine.stop().await;
        assert!(!engine.is_running());
    }

    #[tokio::test]
    async fn test_sample_recording() {
        let engine = AdaptiveEngine::default();

        let sample = MetricsSample::new();
        engine.record_sample(sample).await;

        let status = engine.status().await;
        assert_eq!(status.samples_recorded, 1);
    }

    #[tokio::test]
    async fn test_adapt_insufficient_data() {
        let engine = AdaptiveEngine::default();

        // Should fail with insufficient data
        let result = engine.adapt().await;
        assert!(matches!(
            result,
            Err(AdaptiveError::InsufficientData { .. })
        ));
    }
}
