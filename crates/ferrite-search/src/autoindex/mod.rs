//! AI-Powered Auto-Indexing Engine
//!
//! Automatically detects query patterns and creates optimal indexes without
//! manual intervention. Uses ML-based analysis to predict access patterns and
//! recommend index configurations.
//!
//! # Features
//!
//! - Query pattern collector for tracking access patterns
//! - Index recommender with cost model
//! - ML model for optimal index configuration prediction
//! - Auto-apply mode with safety limits
//! - A/B testing for validation
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    Query Workload Stream                         │
//! └─────────────────────────────────┬───────────────────────────────┘
//!                                   │
//!                                   ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                  Query Pattern Collector                         │
//! │  ┌───────────────┐  ┌───────────────┐  ┌───────────────────┐   │
//! │  │ Key Patterns  │  │ Access Stats  │  │ Field Tracking    │   │
//! │  │ users:*       │  │ freq, latency │  │ json paths        │   │
//! │  └───────────────┘  └───────────────┘  └───────────────────┘   │
//! └─────────────────────────────────┬───────────────────────────────┘
//!                                   │
//!                                   ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                  Index Recommender Engine                        │
//! │  ┌───────────────┐  ┌───────────────┐  ┌───────────────────┐   │
//! │  │ Cost Model    │  │ ML Predictor  │  │ Confidence Score  │   │
//! │  │ storage/write │  │ access trends │  │ threshold check   │   │
//! │  └───────────────┘  └───────────────┘  └───────────────────┘   │
//! └─────────────────────────────────┬───────────────────────────────┘
//!                                   │
//!                                   ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                  Auto-Apply Controller                           │
//! │  ┌───────────────┐  ┌───────────────┐  ┌───────────────────┐   │
//! │  │ Safety Limits │  │ A/B Testing   │  │ Rollback Handler  │   │
//! │  │ max indexes   │  │ impact check  │  │ undo bad indexes  │   │
//! │  └───────────────┘  └───────────────┘  └───────────────────┘   │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```ignore
//! use ferrite::autoindex::{AutoIndexEngine, AutoIndexConfig};
//!
//! let engine = AutoIndexEngine::new(AutoIndexConfig::default());
//!
//! // Record query patterns (usually done automatically)
//! engine.record_access("users:*", AccessType::Read, Duration::from_micros(50));
//! engine.record_access("users:*:profile", AccessType::Read, Duration::from_micros(80));
//!
//! // Get recommendations
//! let recommendations = engine.recommend()?;
//!
//! // Auto-apply (if enabled)
//! engine.apply_recommendations(&recommendations).await?;
//! ```

#![allow(dead_code, unused_imports, unused_variables)]
pub mod collector;
pub mod cost;
pub mod predictor;
pub mod recommender;

pub use collector::{AccessPattern, AccessType, PatternCollector, QueryPattern};
pub use cost::{CostModel, IndexCost, StorageCost};
pub use predictor::{AccessPredictor, PredictionResult, TrendAnalysis};
pub use recommender::{
    IndexRecommendation, IndexType, RecommendationConfidence, Recommender, RecommenderConfig,
};

use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Configuration for auto-indexing
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AutoIndexConfig {
    /// Enable auto-indexing
    pub enabled: bool,
    /// Minimum access count before considering index
    pub min_access_count: u64,
    /// Analysis interval in seconds
    pub analysis_interval_secs: u64,
    /// Confidence threshold for recommendations (0.0-1.0)
    pub confidence_threshold: f64,
    /// Maximum indexes per key pattern
    pub max_indexes_per_pattern: usize,
    /// Maximum total auto-created indexes
    pub max_total_indexes: usize,
    /// Enable auto-apply mode
    pub auto_apply: bool,
    /// Storage budget for indexes (bytes)
    pub storage_budget_bytes: u64,
    /// Maximum write amplification allowed
    pub max_write_amplification: f64,
    /// Enable A/B testing for new indexes
    pub ab_testing_enabled: bool,
    /// A/B test duration in seconds
    pub ab_test_duration_secs: u64,
    /// History retention in hours
    pub history_retention_hours: u64,
    /// Enable ML predictor
    pub ml_predictor_enabled: bool,
}

impl Default for AutoIndexConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_access_count: 100,
            analysis_interval_secs: 300, // 5 minutes
            confidence_threshold: 0.7,
            max_indexes_per_pattern: 3,
            max_total_indexes: 50,
            auto_apply: false, // Safe default: require manual approval
            storage_budget_bytes: 1024 * 1024 * 1024, // 1GB
            max_write_amplification: 2.0,
            ab_testing_enabled: true,
            ab_test_duration_secs: 3600, // 1 hour
            history_retention_hours: 24,
            ml_predictor_enabled: true,
        }
    }
}

impl AutoIndexConfig {
    /// Create an aggressive configuration
    pub fn aggressive() -> Self {
        Self {
            min_access_count: 50,
            confidence_threshold: 0.5,
            max_total_indexes: 100,
            auto_apply: true,
            ab_testing_enabled: false,
            ..Default::default()
        }
    }

    /// Create a conservative configuration
    pub fn conservative() -> Self {
        Self {
            min_access_count: 500,
            confidence_threshold: 0.9,
            max_total_indexes: 20,
            auto_apply: false,
            ab_testing_enabled: true,
            ..Default::default()
        }
    }
}

/// Auto-Index Engine - main coordinator
pub struct AutoIndexEngine {
    /// Configuration
    config: AutoIndexConfig,
    /// Pattern collector
    collector: Arc<PatternCollector>,
    /// Recommender engine
    recommender: Arc<Recommender>,
    /// Cost model
    cost_model: Arc<CostModel>,
    /// Access predictor
    predictor: Arc<AccessPredictor>,
    /// Active indexes (pattern -> index info)
    active_indexes: DashMap<String, ActiveIndex>,
    /// A/B tests in progress
    ab_tests: DashMap<String, ABTest>,
    /// Statistics
    stats: RwLock<AutoIndexStats>,
    /// Running flag
    running: AtomicBool,
    /// Last analysis time
    last_analysis: RwLock<Instant>,
}

/// Information about an active auto-created index
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActiveIndex {
    /// Pattern this index covers
    pub pattern: String,
    /// Index type
    pub index_type: IndexType,
    /// Fields included in index
    pub fields: Vec<String>,
    /// Creation timestamp
    pub created_at: u64,
    /// Access count since creation
    pub access_count: u64,
    /// Estimated storage size
    pub storage_bytes: u64,
    /// Performance improvement observed
    pub latency_improvement_percent: f64,
    /// Is this from A/B test
    pub from_ab_test: bool,
}

/// A/B test for index effectiveness
#[derive(Debug)]
pub struct ABTest {
    /// Test ID
    pub id: String,
    /// Pattern being tested
    pub pattern: String,
    /// Proposed index
    pub proposed_index: IndexRecommendation,
    /// Start time
    pub started_at: Instant,
    /// Duration
    pub duration: Duration,
    /// Metrics with index
    pub metrics_with_index: RwLock<TestMetrics>,
    /// Metrics without index
    pub metrics_without_index: RwLock<TestMetrics>,
    /// Current state
    pub state: RwLock<ABTestState>,
}

/// Metrics collected during A/B test
#[derive(Clone, Debug, Default)]
pub struct TestMetrics {
    /// Total queries
    pub queries: u64,
    /// Total latency (us)
    pub total_latency_us: u64,
    /// P50 latency (us)
    pub p50_latency_us: u64,
    /// P99 latency (us)
    pub p99_latency_us: u64,
    /// Throughput (ops/sec)
    pub throughput: f64,
}

/// A/B test state
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ABTestState {
    /// Test running
    Running,
    /// Test completed, analyzing
    Analyzing,
    /// Index accepted
    Accepted,
    /// Index rejected
    Rejected,
}

/// Statistics for auto-indexing
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AutoIndexStats {
    /// Total patterns analyzed
    pub patterns_analyzed: u64,
    /// Total recommendations generated
    pub recommendations_generated: u64,
    /// Indexes created automatically
    pub indexes_created: u64,
    /// Indexes removed (unused or harmful)
    pub indexes_removed: u64,
    /// A/B tests conducted
    pub ab_tests_conducted: u64,
    /// A/B tests passed
    pub ab_tests_passed: u64,
    /// Estimated storage used by auto-indexes
    pub storage_used_bytes: u64,
    /// Estimated latency improvement
    pub total_latency_improvement_percent: f64,
}

impl AutoIndexEngine {
    /// Create a new auto-index engine
    pub fn new(config: AutoIndexConfig) -> Self {
        let collector = Arc::new(PatternCollector::new(config.history_retention_hours));
        let cost_model = Arc::new(CostModel::new(
            config.storage_budget_bytes,
            config.max_write_amplification,
        ));
        let recommender = Arc::new(Recommender::new(RecommenderConfig {
            min_access_count: config.min_access_count,
            confidence_threshold: config.confidence_threshold,
            max_indexes_per_pattern: config.max_indexes_per_pattern,
        }));
        let predictor = Arc::new(AccessPredictor::new(config.ml_predictor_enabled));

        Self {
            config,
            collector,
            recommender,
            cost_model,
            predictor,
            active_indexes: DashMap::new(),
            ab_tests: DashMap::new(),
            stats: RwLock::new(AutoIndexStats::default()),
            running: AtomicBool::new(false),
            last_analysis: RwLock::new(Instant::now()),
        }
    }

    /// Record a query access
    pub fn record_access(
        &self,
        pattern: &str,
        access_type: AccessType,
        latency: Duration,
        fields: Option<&[String]>,
    ) {
        if !self.config.enabled {
            return;
        }

        self.collector.record(pattern, access_type, latency, fields);
    }

    /// Analyze patterns and generate recommendations
    pub fn analyze(&self) -> Vec<IndexRecommendation> {
        if !self.config.enabled {
            return Vec::new();
        }

        let patterns = self.collector.get_hot_patterns(100);
        let patterns_count = patterns.len();
        let mut recommendations = Vec::new();

        for pattern in patterns {
            if pattern.access_count < self.config.min_access_count {
                continue;
            }

            // Skip if we already have max indexes for this pattern
            let existing_count = self
                .active_indexes
                .iter()
                .filter(|i| i.pattern == pattern.pattern)
                .count();
            if existing_count >= self.config.max_indexes_per_pattern {
                continue;
            }

            // Get prediction for future access (used for future enhancements)
            let _prediction = if self.config.ml_predictor_enabled {
                Some(self.predictor.predict(&pattern))
            } else {
                None
            };

            // Generate recommendations
            let recs = self.recommender.recommend(&pattern, &self.cost_model);

            for rec in recs {
                // Check confidence threshold
                if rec.confidence.score >= self.config.confidence_threshold {
                    // Check storage budget
                    let current_storage = self.stats.read().storage_used_bytes;
                    if current_storage + rec.estimated_storage_bytes
                        <= self.config.storage_budget_bytes
                    {
                        recommendations.push(rec);
                    }
                }
            }
        }

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.patterns_analyzed += patterns_count as u64;
            stats.recommendations_generated += recommendations.len() as u64;
        }

        *self.last_analysis.write() = Instant::now();

        recommendations
    }

    /// Get current recommendations
    pub fn get_recommendations(&self) -> Vec<IndexRecommendation> {
        // Check if analysis is needed
        let should_analyze = {
            let last = self.last_analysis.read();
            last.elapsed() > Duration::from_secs(self.config.analysis_interval_secs)
        };

        if should_analyze {
            self.analyze()
        } else {
            // Return cached recommendations from last analysis
            self.analyze() // For simplicity, just re-analyze
        }
    }

    /// Apply a single recommendation
    pub fn apply_recommendation(&self, rec: &IndexRecommendation) -> Result<(), AutoIndexError> {
        // Check limits
        if self.active_indexes.len() >= self.config.max_total_indexes {
            return Err(AutoIndexError::LimitExceeded(format!(
                "max indexes ({}) reached",
                self.config.max_total_indexes
            )));
        }

        // Check storage budget
        let current_storage = self.stats.read().storage_used_bytes;
        if current_storage + rec.estimated_storage_bytes > self.config.storage_budget_bytes {
            return Err(AutoIndexError::StorageBudgetExceeded);
        }

        // Create the index
        let index_key = format!("{}:{:?}", rec.pattern, rec.index_type);

        let active_index = ActiveIndex {
            pattern: rec.pattern.clone(),
            index_type: rec.index_type.clone(),
            fields: rec.fields.clone(),
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            access_count: 0,
            storage_bytes: rec.estimated_storage_bytes,
            latency_improvement_percent: 0.0,
            from_ab_test: false,
        };

        self.active_indexes.insert(index_key, active_index);

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.indexes_created += 1;
            stats.storage_used_bytes += rec.estimated_storage_bytes;
        }

        Ok(())
    }

    /// Start A/B test for a recommendation
    pub fn start_ab_test(&self, rec: IndexRecommendation) -> Result<String, AutoIndexError> {
        if !self.config.ab_testing_enabled {
            return Err(AutoIndexError::ABTestingDisabled);
        }

        let test_id = format!("ab_{}_{}", rec.pattern, uuid::Uuid::new_v4());

        let test = ABTest {
            id: test_id.clone(),
            pattern: rec.pattern.clone(),
            proposed_index: rec,
            started_at: Instant::now(),
            duration: Duration::from_secs(self.config.ab_test_duration_secs),
            metrics_with_index: RwLock::new(TestMetrics::default()),
            metrics_without_index: RwLock::new(TestMetrics::default()),
            state: RwLock::new(ABTestState::Running),
        };

        self.ab_tests.insert(test_id.clone(), test);

        {
            let mut stats = self.stats.write();
            stats.ab_tests_conducted += 1;
        }

        Ok(test_id)
    }

    /// Complete an A/B test and decide whether to keep the index
    pub fn complete_ab_test(&self, test_id: &str) -> Result<bool, AutoIndexError> {
        let test = self
            .ab_tests
            .get(test_id)
            .ok_or_else(|| AutoIndexError::TestNotFound(test_id.to_string()))?;

        // Calculate improvement
        let with_metrics = test.metrics_with_index.read().clone();
        let without_metrics = test.metrics_without_index.read().clone();

        // Compare latencies
        let improvement = if without_metrics.p50_latency_us > 0 {
            let diff = without_metrics.p50_latency_us as f64 - with_metrics.p50_latency_us as f64;
            diff / without_metrics.p50_latency_us as f64
        } else {
            0.0
        };

        // Accept if improvement > 10%
        let accepted = improvement > 0.1;

        *test.state.write() = if accepted {
            ABTestState::Accepted
        } else {
            ABTestState::Rejected
        };

        if accepted {
            // Apply the index permanently
            self.apply_recommendation(&test.proposed_index)?;

            let mut stats = self.stats.write();
            stats.ab_tests_passed += 1;
        }

        drop(test);
        self.ab_tests.remove(test_id);

        Ok(accepted)
    }

    /// Remove an index
    pub fn remove_index(&self, pattern: &str, index_type: &IndexType) -> bool {
        let key = format!("{}:{:?}", pattern, index_type);

        if let Some((_, index)) = self.active_indexes.remove(&key) {
            let mut stats = self.stats.write();
            stats.indexes_removed += 1;
            stats.storage_used_bytes = stats.storage_used_bytes.saturating_sub(index.storage_bytes);
            true
        } else {
            false
        }
    }

    /// Get all active indexes
    pub fn get_active_indexes(&self) -> Vec<ActiveIndex> {
        self.active_indexes.iter().map(|i| i.clone()).collect()
    }

    /// Get statistics
    pub fn stats(&self) -> AutoIndexStats {
        self.stats.read().clone()
    }

    /// Get configuration
    pub fn config(&self) -> &AutoIndexConfig {
        &self.config
    }

    /// Check if an index exists for a pattern
    pub fn has_index(&self, pattern: &str, index_type: &IndexType) -> bool {
        let key = format!("{}:{:?}", pattern, index_type);
        self.active_indexes.contains_key(&key)
    }

    /// Update index access count
    pub fn record_index_usage(&self, pattern: &str, index_type: &IndexType) {
        let key = format!("{}:{:?}", pattern, index_type);
        if let Some(mut index) = self.active_indexes.get_mut(&key) {
            index.access_count += 1;
        }
    }

    /// Clean up unused indexes
    pub fn cleanup_unused(&self, min_access_per_day: u64) -> u64 {
        let mut removed = 0u64;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let one_day = 24 * 60 * 60;

        let to_remove: Vec<String> = self
            .active_indexes
            .iter()
            .filter(|entry| {
                let age_days = (now - entry.created_at) / one_day;
                if age_days == 0 {
                    return false;
                }
                let access_per_day = entry.access_count / age_days;
                access_per_day < min_access_per_day
            })
            .map(|entry| entry.key().clone())
            .collect();

        for key in to_remove {
            if let Some((_, index)) = self.active_indexes.remove(&key) {
                let mut stats = self.stats.write();
                stats.indexes_removed += 1;
                stats.storage_used_bytes =
                    stats.storage_used_bytes.saturating_sub(index.storage_bytes);
                removed += 1;
            }
        }

        removed
    }
}

/// Errors that can occur in auto-indexing
#[derive(Debug, Clone, thiserror::Error)]
pub enum AutoIndexError {
    /// Limit exceeded
    #[error("limit exceeded: {0}")]
    LimitExceeded(String),

    /// Storage budget exceeded
    #[error("storage budget exceeded")]
    StorageBudgetExceeded,

    /// A/B testing disabled
    #[error("A/B testing is disabled")]
    ABTestingDisabled,

    /// Test not found
    #[error("A/B test not found: {0}")]
    TestNotFound(String),

    /// Index already exists
    #[error("index already exists: {0}")]
    IndexExists(String),

    /// Invalid configuration
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// Internal error
    #[error("internal error: {0}")]
    Internal(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = AutoIndexConfig::default();
        assert!(config.enabled);
        assert!(!config.auto_apply);
        assert!(config.confidence_threshold > 0.0);
    }

    #[test]
    fn test_config_variants() {
        let aggressive = AutoIndexConfig::aggressive();
        let conservative = AutoIndexConfig::conservative();

        assert!(aggressive.auto_apply);
        assert!(!conservative.auto_apply);
        assert!(aggressive.confidence_threshold < conservative.confidence_threshold);
    }

    #[test]
    fn test_engine_creation() {
        let engine = AutoIndexEngine::new(AutoIndexConfig::default());
        let stats = engine.stats();
        assert_eq!(stats.indexes_created, 0);
    }

    #[test]
    fn test_record_access() {
        let engine = AutoIndexEngine::new(AutoIndexConfig::default());

        engine.record_access("users:*", AccessType::Read, Duration::from_micros(50), None);
        engine.record_access("users:*", AccessType::Read, Duration::from_micros(60), None);

        // Patterns should be collected
        let patterns = engine.collector.get_hot_patterns(10);
        assert!(!patterns.is_empty());
    }

    #[test]
    fn test_apply_recommendation() {
        let engine = AutoIndexEngine::new(AutoIndexConfig::default());

        let rec = IndexRecommendation {
            pattern: "users:*".to_string(),
            index_type: IndexType::BTree,
            fields: vec!["name".to_string()],
            confidence: RecommendationConfidence {
                score: 0.8,
                reasons: vec!["high access frequency".to_string()],
            },
            estimated_storage_bytes: 1024 * 1024,
            estimated_latency_improvement: 0.3,
        };

        engine.apply_recommendation(&rec).unwrap();

        let indexes = engine.get_active_indexes();
        assert_eq!(indexes.len(), 1);
    }

    #[test]
    fn test_storage_budget() {
        let mut config = AutoIndexConfig::default();
        config.storage_budget_bytes = 1024; // Very small budget

        let engine = AutoIndexEngine::new(config);

        let rec = IndexRecommendation {
            pattern: "users:*".to_string(),
            index_type: IndexType::BTree,
            fields: vec![],
            confidence: RecommendationConfidence {
                score: 0.9,
                reasons: vec![],
            },
            estimated_storage_bytes: 2048, // Exceeds budget
            estimated_latency_improvement: 0.5,
        };

        let result = engine.apply_recommendation(&rec);
        assert!(matches!(result, Err(AutoIndexError::StorageBudgetExceeded)));
    }
}
