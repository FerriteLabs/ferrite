#![forbid(unsafe_code)]
//! Recommendation types for the adaptive query optimizer.

use serde::{Deserialize, Serialize};
use std::fmt;

/// An action the optimizer can recommend.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Action {
    /// Adjust tier boundary thresholds for hot/warm/cold classification.
    AdjustTierBoundary {
        /// Target tier name.
        tier: String,
        /// New threshold value.
        threshold: f64,
    },
    /// Modify the eviction policy or batch size.
    ModifyEvictionPolicy {
        /// New eviction batch size.
        batch_size: usize,
    },
    /// Enable compression for values above a size threshold.
    EnableCompression {
        /// Minimum value size in bytes to trigger compression.
        min_value_size: usize,
    },
    /// Disable compression.
    DisableCompression,
    /// Rebalance data across shards.
    RebalanceShards {
        /// Target shard count.
        target_shards: usize,
    },
    /// Adjust the memory budget for in-memory tier.
    AdjustMemoryBudget {
        /// New budget in bytes.
        budget_bytes: u64,
    },
    /// Create an index on frequently queried keys.
    CreateIndex {
        /// Index name.
        name: String,
        /// Key pattern to index.
        pattern: String,
    },
    /// Drop an underused index.
    DropIndex {
        /// Index name.
        name: String,
    },
    /// Promote hot keys to a faster tier.
    PromoteHotKeys {
        /// Number of keys to promote.
        key_count: usize,
    },
    /// Demote cold keys to a slower tier.
    DemoteColdKeys {
        /// Number of keys to demote.
        key_count: usize,
    },
    /// Adjust connection pool size.
    AdjustConnectionPool {
        /// New pool size.
        size: usize,
    },
    /// Enable read-ahead prefetching.
    EnablePrefetch {
        /// Prefetch depth.
        depth: usize,
    },
    /// Adjust write buffer size.
    AdjustWriteBuffer {
        /// New buffer size in bytes.
        size_bytes: usize,
    },
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Action::AdjustTierBoundary { tier, threshold } => {
                write!(f, "Adjust tier boundary for '{}' to {:.2}", tier, threshold)
            }
            Action::ModifyEvictionPolicy { batch_size } => {
                write!(f, "Set eviction batch size to {}", batch_size)
            }
            Action::EnableCompression { min_value_size } => {
                write!(
                    f,
                    "Enable compression for values > {} bytes",
                    min_value_size
                )
            }
            Action::DisableCompression => write!(f, "Disable compression"),
            Action::RebalanceShards { target_shards } => {
                write!(f, "Rebalance to {} shards", target_shards)
            }
            Action::AdjustMemoryBudget { budget_bytes } => {
                write!(
                    f,
                    "Adjust memory budget to {} MB",
                    budget_bytes / (1024 * 1024)
                )
            }
            Action::CreateIndex { name, pattern } => {
                write!(f, "Create index '{}' on pattern '{}'", name, pattern)
            }
            Action::DropIndex { name } => write!(f, "Drop index '{}'", name),
            Action::PromoteHotKeys { key_count } => {
                write!(f, "Promote {} hot keys to faster tier", key_count)
            }
            Action::DemoteColdKeys { key_count } => {
                write!(f, "Demote {} cold keys to slower tier", key_count)
            }
            Action::AdjustConnectionPool { size } => {
                write!(f, "Adjust connection pool to {}", size)
            }
            Action::EnablePrefetch { depth } => write!(f, "Enable prefetch depth {}", depth),
            Action::AdjustWriteBuffer { size_bytes } => {
                write!(f, "Adjust write buffer to {} KB", size_bytes / 1024)
            }
        }
    }
}

/// Priority level for a recommendation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum RecommendationPriority {
    /// Low priority — minor optimization.
    Low = 1,
    /// Medium priority — noticeable improvement.
    Medium = 2,
    /// High priority — significant performance impact.
    High = 3,
    /// Critical — immediate action recommended.
    Critical = 4,
}

impl fmt::Display for RecommendationPriority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecommendationPriority::Low => write!(f, "LOW"),
            RecommendationPriority::Medium => write!(f, "MEDIUM"),
            RecommendationPriority::High => write!(f, "HIGH"),
            RecommendationPriority::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// A single optimization recommendation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Recommendation {
    /// Unique identifier.
    pub id: String,
    /// Name of the rule that generated this recommendation.
    pub rule_name: String,
    /// The recommended action.
    pub action: Action,
    /// Priority level.
    pub priority: RecommendationPriority,
    /// Confidence score (0.0 to 1.0).
    pub confidence: f64,
    /// Estimated performance impact as a percentage improvement.
    pub estimated_impact: f64,
    /// Human-readable description.
    pub description: String,
}

/// An optimization plan containing ordered recommendations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OptimizationPlan {
    /// Ordered list of recommendations (highest priority first).
    pub recommendations: Vec<Recommendation>,
    /// Overall estimated impact percentage.
    pub overall_estimated_impact: f64,
    /// Warnings about potential risks.
    pub warnings: Vec<String>,
    /// Timestamp when the plan was generated.
    pub generated_at: String,
}

impl OptimizationPlan {
    /// Create a new empty plan.
    pub fn new() -> Self {
        Self {
            generated_at: chrono::Utc::now().to_rfc3339(),
            ..Default::default()
        }
    }

    /// Add a recommendation and re-sort by priority.
    pub fn add(&mut self, rec: Recommendation) {
        self.overall_estimated_impact += rec.estimated_impact * rec.confidence;
        self.recommendations.push(rec);
        self.recommendations
            .sort_by(|a, b| b.priority.cmp(&a.priority));
    }

    /// Number of recommendations.
    pub fn len(&self) -> usize {
        self.recommendations.len()
    }

    /// Check if the plan is empty.
    pub fn is_empty(&self) -> bool {
        self.recommendations.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_action_display() {
        let action = Action::EnableCompression {
            min_value_size: 1024,
        };
        assert!(action.to_string().contains("1024"));
    }

    #[test]
    fn test_priority_ordering() {
        assert!(RecommendationPriority::Critical > RecommendationPriority::High);
        assert!(RecommendationPriority::High > RecommendationPriority::Medium);
        assert!(RecommendationPriority::Medium > RecommendationPriority::Low);
    }

    #[test]
    fn test_optimization_plan_add_and_sort() {
        let mut plan = OptimizationPlan::new();

        plan.add(Recommendation {
            id: "r1".to_string(),
            rule_name: "low_rule".to_string(),
            action: Action::DisableCompression,
            priority: RecommendationPriority::Low,
            confidence: 0.8,
            estimated_impact: 5.0,
            description: "Low priority".to_string(),
        });

        plan.add(Recommendation {
            id: "r2".to_string(),
            rule_name: "high_rule".to_string(),
            action: Action::PromoteHotKeys { key_count: 10 },
            priority: RecommendationPriority::High,
            confidence: 0.9,
            estimated_impact: 20.0,
            description: "High priority".to_string(),
        });

        assert_eq!(plan.len(), 2);
        assert_eq!(plan.recommendations[0].id, "r2");
        assert_eq!(plan.recommendations[1].id, "r1");
    }

    #[test]
    fn test_plan_is_empty() {
        let plan = OptimizationPlan::new();
        assert!(plan.is_empty());
    }
}
