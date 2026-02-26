#![forbid(unsafe_code)]
//! Tier classification and tuning reports for adaptive storage tiering.
//!
//! Uses access frequency and recency data from the [`WorkloadProfiler`] to
//! classify key ranges as hot/warm/cold and produce actionable tier-move
//! recommendations.

use serde::{Deserialize, Serialize};

/// Recommendation for which storage tier a key range should use.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TierRecommendation {
    /// Keep in memory (mutable region).
    Hot,
    /// Move to mmap (read-only region).
    Warm,
    /// Move to disk/cloud.
    Cold,
}

impl std::fmt::Display for TierRecommendation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TierRecommendation::Hot => write!(f, "HOT"),
            TierRecommendation::Warm => write!(f, "WARM"),
            TierRecommendation::Cold => write!(f, "COLD"),
        }
    }
}

/// A recommended tier move for a key pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierMove {
    /// Key pattern (prefix) this recommendation applies to.
    pub key_pattern: String,
    /// Current tier classification.
    pub current_tier: TierRecommendation,
    /// Recommended tier to move to.
    pub recommended_tier: TierRecommendation,
    /// Access frequency (accesses per second).
    pub access_frequency: f64,
    /// Seconds since last access.
    pub last_access_secs_ago: u64,
}

/// Full workload report from the profiler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadReport {
    /// Total keys analyzed.
    pub total_keys_analyzed: u64,
    /// Number of keys classified as hot.
    pub hot_keys: u64,
    /// Number of keys classified as warm.
    pub warm_keys: u64,
    /// Number of keys classified as cold.
    pub cold_keys: u64,
    /// Read/write ratio (reads / total).
    pub read_write_ratio: f64,
    /// Current throughput in ops/sec.
    pub throughput_ops_per_sec: f64,
    /// Average value size in bytes.
    pub avg_value_size: f64,
    /// Memory usage fraction (0.0–1.0).
    pub memory_usage_fraction: f64,
}

/// Optimization report from the auto-tuner combining workload analysis with
/// tier-move recommendations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TuningReport {
    /// Total keys analyzed.
    pub total_keys_analyzed: u64,
    /// Number of keys classified as hot.
    pub hot_keys: u64,
    /// Number of keys classified as warm.
    pub warm_keys: u64,
    /// Number of keys classified as cold.
    pub cold_keys: u64,
    /// Estimated memory savings if cold keys are demoted (percentage).
    pub estimated_memory_savings_pct: f64,
    /// Concrete tier-move recommendations.
    pub recommendations: Vec<TierMove>,
    /// Read/write ratio.
    pub read_write_ratio: f64,
    /// Current throughput.
    pub throughput_ops_per_sec: f64,
}

/// Thresholds for tier classification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierThresholds {
    /// Accesses per second above which a key is considered hot.
    pub hot_threshold: f64,
    /// Seconds since last access above which a key is considered cold.
    pub cold_threshold_secs: u64,
}

impl Default for TierThresholds {
    fn default() -> Self {
        Self {
            hot_threshold: 1.0,
            cold_threshold_secs: 300,
        }
    }
}

impl TierThresholds {
    /// Classify a key based on its access frequency and recency.
    pub fn classify(&self, access_frequency: f64, last_access_secs_ago: u64) -> TierRecommendation {
        if access_frequency >= self.hot_threshold {
            TierRecommendation::Hot
        } else if last_access_secs_ago >= self.cold_threshold_secs {
            TierRecommendation::Cold
        } else {
            TierRecommendation::Warm
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tier_recommendation_display() {
        assert_eq!(TierRecommendation::Hot.to_string(), "HOT");
        assert_eq!(TierRecommendation::Warm.to_string(), "WARM");
        assert_eq!(TierRecommendation::Cold.to_string(), "COLD");
    }

    #[test]
    fn test_tier_recommendation_eq() {
        assert_eq!(TierRecommendation::Hot, TierRecommendation::Hot);
        assert_ne!(TierRecommendation::Hot, TierRecommendation::Cold);
    }

    #[test]
    fn test_default_thresholds() {
        let t = TierThresholds::default();
        assert!((t.hot_threshold - 1.0).abs() < f64::EPSILON);
        assert_eq!(t.cold_threshold_secs, 300);
    }

    #[test]
    fn test_classify_hot() {
        let t = TierThresholds::default();
        assert_eq!(t.classify(5.0, 10), TierRecommendation::Hot);
    }

    #[test]
    fn test_classify_cold() {
        let t = TierThresholds::default();
        assert_eq!(t.classify(0.1, 600), TierRecommendation::Cold);
    }

    #[test]
    fn test_classify_warm() {
        let t = TierThresholds::default();
        assert_eq!(t.classify(0.5, 100), TierRecommendation::Warm);
    }

    #[test]
    fn test_classify_boundary_hot() {
        let t = TierThresholds::default();
        // Exactly at hot threshold → hot
        assert_eq!(t.classify(1.0, 0), TierRecommendation::Hot);
    }

    #[test]
    fn test_classify_boundary_cold() {
        let t = TierThresholds::default();
        // Exactly at cold threshold, below hot → cold
        assert_eq!(t.classify(0.5, 300), TierRecommendation::Cold);
    }

    #[test]
    fn test_custom_thresholds() {
        let t = TierThresholds {
            hot_threshold: 10.0,
            cold_threshold_secs: 60,
        };
        assert_eq!(t.classify(10.0, 0), TierRecommendation::Hot);
        assert_eq!(t.classify(5.0, 30), TierRecommendation::Warm);
        assert_eq!(t.classify(5.0, 60), TierRecommendation::Cold);
    }

    #[test]
    fn test_tier_move_creation() {
        let tm = TierMove {
            key_pattern: "user:*".to_string(),
            current_tier: TierRecommendation::Hot,
            recommended_tier: TierRecommendation::Cold,
            access_frequency: 0.01,
            last_access_secs_ago: 3600,
        };
        assert_eq!(tm.key_pattern, "user:*");
        assert_eq!(tm.current_tier, TierRecommendation::Hot);
        assert_eq!(tm.recommended_tier, TierRecommendation::Cold);
    }

    #[test]
    fn test_tuning_report_creation() {
        let report = TuningReport {
            total_keys_analyzed: 1000,
            hot_keys: 100,
            warm_keys: 600,
            cold_keys: 300,
            estimated_memory_savings_pct: 25.0,
            recommendations: vec![],
            read_write_ratio: 0.7,
            throughput_ops_per_sec: 5000.0,
        };
        assert_eq!(report.total_keys_analyzed, 1000);
        assert_eq!(report.hot_keys + report.warm_keys + report.cold_keys, 1000);
    }

    #[test]
    fn test_workload_report_creation() {
        let report = WorkloadReport {
            total_keys_analyzed: 500,
            hot_keys: 50,
            warm_keys: 300,
            cold_keys: 150,
            read_write_ratio: 0.8,
            throughput_ops_per_sec: 2000.0,
            avg_value_size: 512.0,
            memory_usage_fraction: 0.65,
        };
        assert_eq!(report.total_keys_analyzed, 500);
        assert!(report.read_write_ratio > 0.5);
    }
}
