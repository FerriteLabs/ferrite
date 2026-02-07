//! Cost-Aware Tiering Module
//!
//! This module implements intelligent data tiering that optimizes for cost,
//! not just recency. It automatically balances storage costs, access patterns,
//! and latency requirements to minimize total cost of ownership while meeting
//! SLA targets.
//!
//! ## Architecture
//!
//! The tiering engine consists of:
//! - **Access Pattern Analyzer**: Tracks frequency, recency, and size per key
//! - **Cost Calculator**: Computes monthly costs per tier
//! - **Placement Optimizer**: Selects optimal tier per key
//! - **Migration Executor**: Handles background data movement
//!
//! ## Usage
//!
//! ```rust,ignore
//! use ferrite::tiering::{TieringEngine, TieringConfig};
//!
//! let config = TieringConfig::default();
//! let engine = TieringEngine::new(config);
//!
//! // Record an access
//! engine.record_access(&key, AccessType::Read, key_size);
//!
//! // Get optimal tier for a key
//! let optimal = engine.optimal_tier(&key_stats);
//! ```

mod access;
pub mod backend;
pub mod compression;
mod config;
mod cost;
mod engine;
mod migration;
mod optimizer;
pub mod pipeline;
pub mod predictor;
pub mod prefetch;
mod stats;

pub use access::{AccessPattern, AccessPatternAnalyzer, AccessType};
pub use backend::{
    BackendRegistry, ColdStorageBackend, ColdStorageError, InMemoryBackend, LocalFilesystemBackend,
    LocalFilesystemConfig,
};
#[cfg(feature = "cloud")]
pub use backend::{S3Backend, S3BackendConfig};
pub use config::{
    OptimizationTarget, PatternPolicy, PlacementConstraints, TierCost, TierCostConfig,
    TieringConfig,
};
pub use cost::{TierCostBreakdown, TierCostCalculator};
pub use engine::{TierDistribution, TieringEngine, TieringInfo};
pub use migration::{Migration, MigrationDirection, MigrationExecutor, MigrationState};
pub use optimizer::PlacementOptimizer;
pub use predictor::{
    AdaptivePredictor, AdaptivePredictorConfig, KeyAccessProfile, PredictorStats,
    PrewarmRecommendation, TierPrediction,
};
pub use prefetch::{PrefetchCandidate, PrefetchConfig, PrefetchEngine, PrefetchStats};
pub use stats::{AccessCounts, KeyAccessStats, Priority, StorageTier};

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_tiering_engine_basic() {
        let config = TieringConfig::default();
        let engine = TieringEngine::new(config);

        let key = Bytes::from("test:key");
        engine.record_access(&key, AccessType::Read, 1024);

        let stats = engine.get_stats(&key);
        assert!(stats.is_some());
    }

    #[test]
    fn test_cost_calculation() {
        let config = TierCostConfig::default();
        let calculator = TierCostCalculator::new(config);

        let stats = KeyAccessStats::new(1024, StorageTier::Memory);
        let cost = calculator.calculate_monthly_cost(&stats, StorageTier::Memory);

        assert!(cost.total > 0.0);
    }

    #[test]
    fn test_optimal_tier_critical_priority() {
        let config = TieringConfig::default();
        let engine = TieringEngine::new(config);

        let mut stats = KeyAccessStats::new(1024, StorageTier::Cloud);
        stats.priority = Priority::Critical;

        let constraints = PlacementConstraints::default();
        let optimal = engine.optimal_tier(&stats, &constraints);

        assert_eq!(optimal, StorageTier::Memory);
    }

    #[test]
    fn test_storage_tier_ordering() {
        assert!(StorageTier::Memory < StorageTier::Mmap);
        assert!(StorageTier::Mmap < StorageTier::Ssd);
        assert!(StorageTier::Ssd < StorageTier::Cloud);
        assert!(StorageTier::Cloud < StorageTier::Archive);
    }
}
