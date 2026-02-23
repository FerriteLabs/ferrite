//! Cost calculation for tiering decisions
//!
//! This module computes the monthly cost of keeping data in each tier,
//! taking into account storage, access, and egress costs.

use super::config::TierCostConfig;
use super::stats::{KeyAccessStats, StorageTier};
use serde::{Deserialize, Serialize};

/// Cost breakdown for a tier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierCostBreakdown {
    /// The storage tier
    pub tier: StorageTier,
    /// Storage cost per month
    pub storage_cost: f64,
    /// Read operation costs per month
    pub read_cost: f64,
    /// Write operation costs per month
    pub write_cost: f64,
    /// Egress costs per month
    pub egress_cost: f64,
    /// Total cost per month
    pub total: f64,
    /// Expected latency impact
    pub latency_impact: f64,
}

impl TierCostBreakdown {
    /// Create a zero-cost breakdown
    pub fn zero(tier: StorageTier) -> Self {
        Self {
            tier,
            storage_cost: 0.0,
            read_cost: 0.0,
            write_cost: 0.0,
            egress_cost: 0.0,
            total: 0.0,
            latency_impact: 0.0,
        }
    }

    /// Get potential savings compared to another breakdown
    pub fn savings_vs(&self, other: &TierCostBreakdown) -> f64 {
        other.total - self.total
    }

    /// Get savings percentage compared to another breakdown
    pub fn savings_pct_vs(&self, other: &TierCostBreakdown) -> f64 {
        if other.total > 0.0 {
            ((other.total - self.total) / other.total) * 100.0
        } else {
            0.0
        }
    }
}

/// Cost calculator for tiering decisions
pub struct TierCostCalculator {
    /// Cost configuration
    config: TierCostConfig,
}

impl TierCostCalculator {
    /// Create a new cost calculator
    pub fn new(config: TierCostConfig) -> Self {
        Self { config }
    }

    /// Calculate monthly cost of keeping a key in a specific tier
    pub fn calculate_monthly_cost(
        &self,
        stats: &KeyAccessStats,
        tier: StorageTier,
    ) -> TierCostBreakdown {
        let tier_cost = self.config.cost_for_tier(tier);
        let size_gb = stats.size_gb();

        // Extrapolate daily access to monthly
        let monthly_reads = stats.monthly_reads();
        let monthly_writes = stats.monthly_writes();

        // Storage cost
        let storage_cost = size_gb * tier_cost.storage_per_gb_month;

        // Access costs (significant for cloud tiers)
        let read_cost = (monthly_reads / 1000.0) * tier_cost.read_per_1k;
        let write_cost = (monthly_writes / 1000.0) * tier_cost.write_per_1k;

        // Egress cost (assuming each read transfers the full value)
        let egress_cost = if tier == StorageTier::Cloud || tier == StorageTier::Archive {
            size_gb * monthly_reads * tier_cost.egress_per_gb
        } else {
            0.0
        };

        let total = storage_cost + read_cost + write_cost + egress_cost;

        TierCostBreakdown {
            tier,
            storage_cost,
            read_cost,
            write_cost,
            egress_cost,
            total,
            latency_impact: tier_cost.read_latency_ms,
        }
    }

    /// Calculate costs for all tiers
    pub fn calculate_all_tiers(&self, stats: &KeyAccessStats) -> Vec<TierCostBreakdown> {
        StorageTier::all()
            .iter()
            .map(|&tier| self.calculate_monthly_cost(stats, tier))
            .collect()
    }

    /// Find the cheapest tier that meets latency requirements
    pub fn cheapest_tier_within_latency(
        &self,
        stats: &KeyAccessStats,
        max_latency_ms: f64,
    ) -> Option<(StorageTier, TierCostBreakdown)> {
        self.calculate_all_tiers(stats)
            .into_iter()
            .filter(|b| b.latency_impact <= max_latency_ms)
            .min_by(|a, b| {
                a.total
                    .partial_cmp(&b.total)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|b| (b.tier, b))
    }

    /// Get the cost config
    pub fn config(&self) -> &TierCostConfig {
        &self.config
    }

    /// Update cost config
    pub fn set_config(&mut self, config: TierCostConfig) {
        self.config = config;
    }
}

/// Summary of costs across all keys
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CostSummary {
    /// Total keys
    pub total_keys: u64,
    /// Total size in bytes
    pub total_size_bytes: u64,
    /// Current monthly cost
    pub monthly_cost_current: f64,
    /// Optimal monthly cost
    pub monthly_cost_optimal: f64,
    /// Cost breakdown by tier
    pub tier_costs: Vec<TierCostSummary>,
}

/// Cost summary for a single tier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierCostSummary {
    /// The tier
    pub tier: StorageTier,
    /// Number of keys in this tier
    pub key_count: u64,
    /// Total size in this tier
    pub size_bytes: u64,
    /// Monthly cost for this tier
    pub monthly_cost: f64,
}

impl CostSummary {
    /// Create a new empty summary
    pub fn new() -> Self {
        Self::default()
    }

    /// Get potential savings amount
    pub fn potential_savings(&self) -> f64 {
        self.monthly_cost_current - self.monthly_cost_optimal
    }

    /// Get potential savings percentage
    pub fn potential_savings_pct(&self) -> f64 {
        if self.monthly_cost_current > 0.0 {
            ((self.monthly_cost_current - self.monthly_cost_optimal) / self.monthly_cost_current)
                * 100.0
        } else {
            0.0
        }
    }

    /// Get total size in GB
    pub fn total_size_gb(&self) -> f64 {
        self.total_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    }

    /// Add a tier's stats to the summary
    pub fn add_tier_stats(&mut self, tier: StorageTier, keys: u64, size: u64, cost: f64) {
        self.total_keys += keys;
        self.total_size_bytes += size;
        self.monthly_cost_current += cost;

        self.tier_costs.push(TierCostSummary {
            tier,
            key_count: keys,
            size_bytes: size,
            monthly_cost: cost,
        });
    }
}

/// A key's cost analysis for reporting
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyCostAnalysis {
    /// The key (as string for reporting)
    pub key: String,
    /// Size in bytes
    pub size_bytes: u32,
    /// Current tier
    pub current_tier: StorageTier,
    /// Current monthly cost
    pub current_cost_monthly: f64,
    /// Detected access pattern
    pub access_pattern: String,
    /// Reads per day
    pub reads_per_day: u32,
    /// Writes per day
    pub writes_per_day: u32,
    /// Optimal tier
    pub optimal_tier: StorageTier,
    /// Optimal monthly cost
    pub optimal_cost_monthly: f64,
    /// Potential savings percentage
    pub potential_savings_pct: f64,
    /// Cost breakdown per tier
    pub tier_breakdown: Vec<TierCostBreakdown>,
}

impl KeyCostAnalysis {
    /// Check if the key is in its optimal tier
    pub fn is_optimal(&self) -> bool {
        self.current_tier == self.optimal_tier
    }

    /// Get potential monthly savings
    pub fn potential_savings(&self) -> f64 {
        self.current_cost_monthly - self.optimal_cost_monthly
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cost_breakdown() {
        let config = TierCostConfig::default();
        let calculator = TierCostCalculator::new(config);

        let mut stats = KeyAccessStats::new(1024 * 1024, StorageTier::Memory); // 1 MB
        stats.access_counts.reads_1d = 100;

        let memory_cost = calculator.calculate_monthly_cost(&stats, StorageTier::Memory);
        let cloud_cost = calculator.calculate_monthly_cost(&stats, StorageTier::Cloud);

        // Memory has higher storage cost but no operation costs
        assert!(memory_cost.storage_cost > cloud_cost.storage_cost);
        // Cloud has operation and egress costs
        assert!(cloud_cost.read_cost > 0.0);
    }

    #[test]
    fn test_cheapest_tier_within_latency() {
        let config = TierCostConfig::default();
        let calculator = TierCostCalculator::new(config);

        let stats = KeyAccessStats::new(1024, StorageTier::Memory);

        // With very low latency requirement (< mmap's 0.5ms), should stay in memory
        let result = calculator.cheapest_tier_within_latency(&stats, 0.2);
        assert!(result.is_some());
        assert_eq!(result.unwrap().0, StorageTier::Memory);

        // With high latency tolerance, might be cheaper elsewhere (mmap at 0.5ms costs less)
        let result = calculator.cheapest_tier_within_latency(&stats, 100.0);
        assert!(result.is_some());
    }

    #[test]
    fn test_cost_summary() {
        let mut summary = CostSummary::new();

        summary.add_tier_stats(StorageTier::Memory, 100, 1024 * 1024, 10.0);
        summary.add_tier_stats(StorageTier::Ssd, 1000, 10 * 1024 * 1024, 5.0);

        assert_eq!(summary.total_keys, 1100);
        assert_eq!(summary.monthly_cost_current, 15.0);
    }

    #[test]
    fn test_tier_cost_breakdown_savings() {
        let memory_cost = TierCostBreakdown {
            tier: StorageTier::Memory,
            storage_cost: 10.0,
            read_cost: 0.0,
            write_cost: 0.0,
            egress_cost: 0.0,
            total: 10.0,
            latency_impact: 0.1,
        };

        let ssd_cost = TierCostBreakdown {
            tier: StorageTier::Ssd,
            storage_cost: 2.0,
            read_cost: 0.0,
            write_cost: 0.0,
            egress_cost: 0.0,
            total: 2.0,
            latency_impact: 1.0,
        };

        assert_eq!(ssd_cost.savings_vs(&memory_cost), 8.0);
        assert_eq!(ssd_cost.savings_pct_vs(&memory_cost), 80.0);
    }
}
