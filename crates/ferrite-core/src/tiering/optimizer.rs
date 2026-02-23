//! Placement optimizer for tiering decisions
//!
//! This module determines the optimal storage tier for each key based on
//! access patterns, cost calculations, and placement constraints.

use super::access::{AccessPattern, AccessPatternAnalyzer};
use super::config::{OptimizationTarget, PlacementConstraints, TierCostConfig, TieringConfig};
use super::cost::{TierCostBreakdown, TierCostCalculator};
use super::stats::{KeyAccessStats, Priority, StorageTier};

/// Result of placement optimization
#[derive(Debug, Clone)]
pub struct PlacementDecision {
    /// Recommended tier
    pub tier: StorageTier,
    /// Cost breakdown for the recommended tier
    pub cost: TierCostBreakdown,
    /// Reason for the decision
    pub reason: PlacementReason,
    /// All tier options with costs
    pub alternatives: Vec<TierCostBreakdown>,
}

/// Reason for a placement decision
#[derive(Debug, Clone, PartialEq)]
pub enum PlacementReason {
    /// Priority override (Critical/Archive)
    PriorityOverride,
    /// Key is pinned to this tier
    Pinned,
    /// Cost optimized choice
    CostOptimal,
    /// Latency constraint
    LatencyConstraint,
    /// Pattern-based policy
    PatternPolicy,
    /// Already in optimal tier
    AlreadyOptimal,
    /// Budget constraint
    BudgetConstraint,
}

impl PlacementReason {
    /// Get a human-readable description
    pub fn description(&self) -> &'static str {
        match self {
            PlacementReason::PriorityOverride => "Priority override",
            PlacementReason::Pinned => "Pinned to tier",
            PlacementReason::CostOptimal => "Cost optimized",
            PlacementReason::LatencyConstraint => "Latency requirement",
            PlacementReason::PatternPolicy => "Pattern policy match",
            PlacementReason::AlreadyOptimal => "Already optimal",
            PlacementReason::BudgetConstraint => "Budget constraint",
        }
    }
}

/// Optimizer for key placement decisions
pub struct PlacementOptimizer {
    /// Cost calculator
    cost_calculator: TierCostCalculator,
    /// Access pattern analyzer
    pattern_analyzer: AccessPatternAnalyzer,
    /// Optimization target
    optimization_target: OptimizationTarget,
    /// Memory budget (bytes)
    memory_budget: u64,
    /// Current memory usage (bytes)
    current_memory_usage: u64,
}

impl PlacementOptimizer {
    /// Create a new optimizer
    pub fn new(config: &TieringConfig) -> Self {
        Self {
            cost_calculator: TierCostCalculator::new(config.costs.clone()),
            pattern_analyzer: AccessPatternAnalyzer::new(),
            optimization_target: config.optimize_for,
            memory_budget: config.memory_budget,
            current_memory_usage: 0,
        }
    }

    /// Create with custom settings
    pub fn with_settings(
        costs: TierCostConfig,
        target: OptimizationTarget,
        memory_budget: u64,
    ) -> Self {
        Self {
            cost_calculator: TierCostCalculator::new(costs),
            pattern_analyzer: AccessPatternAnalyzer::new(),
            optimization_target: target,
            memory_budget,
            current_memory_usage: 0,
        }
    }

    /// Update current memory usage
    pub fn set_memory_usage(&mut self, bytes: u64) {
        self.current_memory_usage = bytes;
    }

    /// Update memory budget
    pub fn set_memory_budget(&mut self, bytes: u64) {
        self.memory_budget = bytes;
    }

    /// Check if memory budget allows placing key in memory
    pub fn memory_budget_allows(&self, key_size: u64) -> bool {
        self.current_memory_usage + key_size <= self.memory_budget
    }

    /// Find optimal tier for a key
    pub fn optimal_tier(
        &self,
        stats: &KeyAccessStats,
        constraints: &PlacementConstraints,
    ) -> PlacementDecision {
        // Calculate costs for all tiers
        let all_costs = self.cost_calculator.calculate_all_tiers(stats);

        // Check priority overrides first
        match stats.priority {
            Priority::Critical => {
                let cost = all_costs
                    .iter()
                    .find(|c| c.tier == StorageTier::Memory)
                    .cloned()
                    .unwrap_or_else(|| TierCostBreakdown::zero(StorageTier::Memory));
                return PlacementDecision {
                    tier: StorageTier::Memory,
                    cost,
                    reason: PlacementReason::PriorityOverride,
                    alternatives: all_costs,
                };
            }
            Priority::Archive => {
                let cost = all_costs
                    .iter()
                    .find(|c| c.tier == StorageTier::Archive)
                    .cloned()
                    .unwrap_or_else(|| TierCostBreakdown::zero(StorageTier::Archive));
                return PlacementDecision {
                    tier: StorageTier::Archive,
                    cost,
                    reason: PlacementReason::PriorityOverride,
                    alternatives: all_costs,
                };
            }
            _ => {}
        }

        // Check if pinned
        if stats.pinned {
            let cost = all_costs
                .iter()
                .find(|c| c.tier == stats.tier)
                .cloned()
                .unwrap_or_else(|| TierCostBreakdown::zero(stats.tier));
            return PlacementDecision {
                tier: stats.tier,
                cost,
                reason: PlacementReason::Pinned,
                alternatives: all_costs,
            };
        }

        // Filter by constraints
        let viable_tiers: Vec<_> = all_costs
            .iter()
            .filter(|c| constraints.allows_tier(c.tier, c.latency_impact))
            .cloned()
            .collect();

        if viable_tiers.is_empty() {
            // No viable tiers, stay in current
            let cost = all_costs
                .iter()
                .find(|c| c.tier == stats.tier)
                .cloned()
                .unwrap_or_else(|| TierCostBreakdown::zero(stats.tier));
            return PlacementDecision {
                tier: stats.tier,
                cost,
                reason: PlacementReason::LatencyConstraint,
                alternatives: all_costs,
            };
        }

        // Select based on optimization target
        let (optimal_tier, reason) = match self.optimization_target {
            OptimizationTarget::Cost => {
                let best = viable_tiers
                    .iter()
                    .min_by(|a, b| {
                        a.total
                            .partial_cmp(&b.total)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .cloned()
                    .unwrap_or_else(|| TierCostBreakdown::zero(stats.tier));
                (best, PlacementReason::CostOptimal)
            }
            OptimizationTarget::Latency => {
                let best = viable_tiers
                    .iter()
                    .min_by(|a, b| {
                        a.latency_impact
                            .partial_cmp(&b.latency_impact)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .cloned()
                    .unwrap_or_else(|| TierCostBreakdown::zero(stats.tier));
                (best, PlacementReason::LatencyConstraint)
            }
            OptimizationTarget::Balanced => {
                // Score each tier: lower is better
                // Score = normalized_cost + normalized_latency
                let min_cost = viable_tiers
                    .iter()
                    .map(|c| c.total)
                    .fold(f64::INFINITY, f64::min);
                let max_cost = viable_tiers.iter().map(|c| c.total).fold(0.0, f64::max);
                let min_latency = viable_tiers
                    .iter()
                    .map(|c| c.latency_impact)
                    .fold(f64::INFINITY, f64::min);
                let max_latency = viable_tiers
                    .iter()
                    .map(|c| c.latency_impact)
                    .fold(0.0, f64::max);

                let cost_range = if max_cost > min_cost {
                    max_cost - min_cost
                } else {
                    1.0
                };
                let latency_range = if max_latency > min_latency {
                    max_latency - min_latency
                } else {
                    1.0
                };

                let best = viable_tiers
                    .iter()
                    .min_by(|a, b| {
                        let score_a = (a.total - min_cost) / cost_range
                            + (a.latency_impact - min_latency) / latency_range;
                        let score_b = (b.total - min_cost) / cost_range
                            + (b.latency_impact - min_latency) / latency_range;
                        score_a
                            .partial_cmp(&score_b)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .cloned()
                    .unwrap_or_else(|| TierCostBreakdown::zero(stats.tier));
                (best, PlacementReason::CostOptimal)
            }
        };

        // Check if already in optimal tier
        let reason = if optimal_tier.tier == stats.tier {
            PlacementReason::AlreadyOptimal
        } else {
            reason
        };

        PlacementDecision {
            tier: optimal_tier.tier,
            cost: optimal_tier,
            reason,
            alternatives: all_costs,
        }
    }

    /// Find optimal tier with access pattern consideration
    pub fn optimal_tier_with_pattern(
        &self,
        stats: &KeyAccessStats,
        constraints: &PlacementConstraints,
    ) -> PlacementDecision {
        // Get base decision
        let mut decision = self.optimal_tier(stats, constraints);

        // Adjust based on access pattern
        let pattern = self.pattern_analyzer.detect_pattern(stats);

        match pattern {
            AccessPattern::New => {
                // Keep new keys in fast tier until we understand their pattern
                if decision.tier != StorageTier::Memory && !stats.pinned {
                    decision.tier = StorageTier::Memory;
                    decision.reason = PlacementReason::PatternPolicy;
                }
            }
            AccessPattern::WriteHeavy { .. } => {
                // Write-heavy keys should stay in memory for performance
                if decision.tier.is_colder_than(&StorageTier::Ssd) && !stats.pinned {
                    decision.tier = StorageTier::Ssd; // At least SSD
                    decision.reason = PlacementReason::PatternPolicy;
                }
            }
            AccessPattern::Periodic { period_hours, .. } if period_hours <= 24 => {
                // Frequently periodic keys should stay warm
                if decision.tier.is_colder_than(&StorageTier::Mmap) && !stats.pinned {
                    decision.tier = StorageTier::Mmap;
                    decision.reason = PlacementReason::PatternPolicy;
                }
            }
            _ => {}
        }

        decision
    }

    /// Batch optimize placements for multiple keys
    pub fn optimize_batch<'a>(
        &self,
        stats: &[(&'a [u8], KeyAccessStats)],
        constraints: &PlacementConstraints,
    ) -> Vec<(&'a [u8], PlacementDecision)> {
        stats
            .iter()
            .map(|(key, stat)| (*key, self.optimal_tier_with_pattern(stat, constraints)))
            .collect()
    }

    /// Get suboptimal keys (keys not in their optimal tier)
    pub fn find_suboptimal<'a>(
        &self,
        stats: &'a [(&'a [u8], KeyAccessStats)],
        constraints: &PlacementConstraints,
    ) -> Vec<(&'a [u8], PlacementDecision)> {
        stats
            .iter()
            .filter_map(|(key, stat)| {
                let decision = self.optimal_tier_with_pattern(stat, constraints);
                if decision.tier != stat.tier {
                    Some((*key, decision))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Calculate total potential savings
    pub fn total_savings(
        &self,
        stats: &[KeyAccessStats],
        constraints: &PlacementConstraints,
    ) -> f64 {
        stats
            .iter()
            .map(|stat| {
                let current_cost = self.cost_calculator.calculate_monthly_cost(stat, stat.tier);
                let decision = self.optimal_tier(stat, constraints);
                current_cost.total - decision.cost.total
            })
            .filter(|savings| *savings > 0.0)
            .sum()
    }

    /// Get the cost calculator
    pub fn cost_calculator(&self) -> &TierCostCalculator {
        &self.cost_calculator
    }

    /// Get the pattern analyzer
    pub fn pattern_analyzer(&self) -> &AccessPatternAnalyzer {
        &self.pattern_analyzer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config() -> TieringConfig {
        TieringConfig::default()
    }

    #[test]
    fn test_critical_priority_override() {
        let config = make_config();
        let optimizer = PlacementOptimizer::new(&config);

        let mut stats = KeyAccessStats::new(1024, StorageTier::Cloud);
        stats.priority = Priority::Critical;

        let constraints = PlacementConstraints::default();
        let decision = optimizer.optimal_tier(&stats, &constraints);

        assert_eq!(decision.tier, StorageTier::Memory);
        assert_eq!(decision.reason, PlacementReason::PriorityOverride);
    }

    #[test]
    fn test_archive_priority_override() {
        let config = make_config();
        let optimizer = PlacementOptimizer::new(&config);

        let mut stats = KeyAccessStats::new(1024, StorageTier::Memory);
        stats.priority = Priority::Archive;

        let constraints = PlacementConstraints::default();
        let decision = optimizer.optimal_tier(&stats, &constraints);

        assert_eq!(decision.tier, StorageTier::Archive);
        assert_eq!(decision.reason, PlacementReason::PriorityOverride);
    }

    #[test]
    fn test_pinned_key() {
        let config = make_config();
        let optimizer = PlacementOptimizer::new(&config);

        let mut stats = KeyAccessStats::new(1024, StorageTier::Ssd);
        stats.pinned = true;

        let constraints = PlacementConstraints::default();
        let decision = optimizer.optimal_tier(&stats, &constraints);

        assert_eq!(decision.tier, StorageTier::Ssd);
        assert_eq!(decision.reason, PlacementReason::Pinned);
    }

    #[test]
    fn test_latency_constraint() {
        let config = make_config();
        let optimizer = PlacementOptimizer::new(&config);

        let stats = KeyAccessStats::new(1024, StorageTier::Memory);
        // Very tight latency constraint
        let constraints = PlacementConstraints::with_max_latency(0.2);

        let decision = optimizer.optimal_tier(&stats, &constraints);

        // Should stay in memory due to latency constraint
        assert_eq!(decision.tier, StorageTier::Memory);
    }

    #[test]
    fn test_cost_optimization() {
        let mut config = make_config();
        config.optimize_for = OptimizationTarget::Cost;
        let optimizer = PlacementOptimizer::new(&config);

        // Cold key with no recent access
        let stats = KeyAccessStats::new(1024 * 1024, StorageTier::Memory);
        let constraints = PlacementConstraints::default();

        let decision = optimizer.optimal_tier(&stats, &constraints);

        // Should suggest cheaper tier for cold data
        assert!(decision.tier.is_colder_than(&StorageTier::Memory));
    }

    #[test]
    fn test_already_optimal() {
        let config = make_config();
        let optimizer = PlacementOptimizer::new(&config);

        // Key already in cheapest viable tier
        let mut stats = KeyAccessStats::new(1024, StorageTier::Archive);
        stats.priority = Priority::Archive;

        let constraints = PlacementConstraints::default();
        let decision = optimizer.optimal_tier(&stats, &constraints);

        assert_eq!(decision.tier, StorageTier::Archive);
    }

    #[test]
    fn test_find_suboptimal() {
        let config = make_config();
        let optimizer = PlacementOptimizer::new(&config);

        let key1: &[u8] = b"key1";
        let key2: &[u8] = b"key2";

        let stats = vec![
            (key1, KeyAccessStats::new(1024, StorageTier::Memory)), // Cold in memory
            (key2, {
                let mut s = KeyAccessStats::new(1024, StorageTier::Memory);
                s.access_counts.reads_1d = 1000; // Hot in memory
                s
            }),
        ];

        let constraints = PlacementConstraints::default();
        let suboptimal = optimizer.find_suboptimal(&stats, &constraints);

        // First key (cold) should be suboptimal
        assert!(!suboptimal.is_empty());
        assert_eq!(suboptimal[0].0, key1);
    }
}
