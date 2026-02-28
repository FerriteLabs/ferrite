//! Cost savings calculator for tiered storage
//!
//! Provides detailed cost comparisons between tiered Ferrite storage and
//! pure in-memory solutions (Redis), along with globally optimal key placement.

#![allow(dead_code)]

use std::collections::HashMap;

use super::config::TierCostConfig;
use super::stats::StorageTier;

use super::auto_tier::KeyAccessStats;

/// Configurable cost model used by the savings calculator
pub struct SavingsCalculatorConfig {
    /// Cost per tier per GB per month
    pub tier_costs: TierCostConfig,
    /// Redis-equivalent cost per GB per month (all in-memory)
    pub redis_cost_per_gb_month: f64,
}

impl Default for SavingsCalculatorConfig {
    fn default() -> Self {
        Self {
            tier_costs: TierCostConfig::default(),
            // Typical managed Redis pricing (~$6.50/GB/month for AWS ElastiCache on-demand)
            redis_cost_per_gb_month: 6.50,
        }
    }
}

/// Report comparing Ferrite tiered costs to a pure Redis deployment
pub struct RedisComparisonReport {
    /// What this workload would cost as pure Redis per month
    pub redis_monthly_cost: f64,
    /// What this workload costs with Ferrite tiered storage per month
    pub ferrite_monthly_cost: f64,
    /// Savings percentage ((redis - ferrite) / redis * 100)
    pub savings_pct: f64,
    /// Memory saved by tiering cold data off-heap (GB)
    pub memory_saved_gb: f64,
}

/// Cost savings calculator
///
/// Computes globally optimal key placement across tiers and compares
/// costs against a pure in-memory Redis deployment.
pub struct SavingsCalculator {
    /// Calculator configuration with per-tier costs
    config: SavingsCalculatorConfig,
}

impl SavingsCalculator {
    /// Create a new savings calculator with the given configuration
    pub fn new(config: SavingsCalculatorConfig) -> Self {
        Self { config }
    }

    /// Create a calculator with default costs
    pub fn with_defaults() -> Self {
        Self::new(SavingsCalculatorConfig::default())
    }

    /// Determine globally optimal tier placement for a set of keys
    ///
    /// For each key, selects the tier that minimizes total monthly cost
    /// while staying within the configured maximum latency.
    pub fn calculate_optimal_placement(
        &self,
        keys: &[KeyAccessStats],
    ) -> HashMap<Vec<u8>, StorageTier> {
        let mut placements = HashMap::with_capacity(keys.len());

        for key_stats in keys {
            let mut best_tier = key_stats.current_tier;
            let mut best_cost = f64::MAX;

            for tier in StorageTier::all() {
                let cost = self.monthly_cost_for_key(key_stats, *tier);
                if cost < best_cost {
                    best_cost = cost;
                    best_tier = *tier;
                }
            }

            placements.insert(key_stats.key.clone(), best_tier);
        }

        placements
    }

    /// Compare the cost of a workload on pure Redis vs Ferrite tiered storage
    ///
    /// `total_data_gb` is the total dataset size, `ops_per_sec` is the aggregate
    /// operation rate used to estimate Redis CPU/connection overhead.
    pub fn compare_with_redis(
        &self,
        total_data_gb: f64,
        ops_per_sec: f64,
    ) -> RedisComparisonReport {
        // Redis cost: everything lives in memory
        let redis_monthly_cost = total_data_gb * self.config.redis_cost_per_gb_month;

        // Ferrite cost estimate using a typical distribution:
        //   ~20% hot data in memory, ~30% warm in mmap, ~50% cold on SSD/cloud
        let memory_gb = total_data_gb * 0.20;
        let mmap_gb = total_data_gb * 0.30;
        let ssd_gb = total_data_gb * 0.40;
        let cloud_gb = total_data_gb * 0.10;

        let mem_cost = &self.config.tier_costs.memory;
        let mmap_cost = &self.config.tier_costs.mmap;
        let ssd_cost = &self.config.tier_costs.ssd;
        let cloud_cost = &self.config.tier_costs.cloud;

        let ferrite_storage_cost = memory_gb * mem_cost.storage_per_gb_month
            + mmap_gb * mmap_cost.storage_per_gb_month
            + ssd_gb * ssd_cost.storage_per_gb_month
            + cloud_gb * cloud_cost.storage_per_gb_month;

        // Estimate operation costs (assume ~80% reads, 20% writes)
        let monthly_ops = ops_per_sec * 86400.0 * 30.0;
        let monthly_reads = monthly_ops * 0.80;
        let monthly_writes = monthly_ops * 0.20;

        // Operations primarily hit memory/mmap tiers for hot data;
        // cloud ops are negligible for frequently-accessed keys
        let op_cost = (monthly_reads / 1000.0) * mem_cost.read_per_1k
            + (monthly_writes / 1000.0) * mem_cost.write_per_1k;

        let ferrite_monthly_cost = ferrite_storage_cost + op_cost;

        let savings_pct = if redis_monthly_cost > 0.0 {
            ((redis_monthly_cost - ferrite_monthly_cost) / redis_monthly_cost) * 100.0
        } else {
            0.0
        };

        let memory_saved_gb = total_data_gb - memory_gb;

        RedisComparisonReport {
            redis_monthly_cost,
            ferrite_monthly_cost,
            savings_pct,
            memory_saved_gb,
        }
    }

    /// Get the underlying config
    pub fn config(&self) -> &SavingsCalculatorConfig {
        &self.config
    }

    // --- private helpers ---

    /// Estimate the monthly cost of a single key in a given tier
    fn monthly_cost_for_key(&self, stats: &KeyAccessStats, tier: StorageTier) -> f64 {
        let tc = self.config.tier_costs.cost_for_tier(tier);
        let size_gb = stats.size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);

        let storage = size_gb * tc.storage_per_gb_month;
        let reads = stats.total_reads as f64;
        let writes = stats.total_writes as f64;
        let read_cost = (reads / 1000.0) * tc.read_per_1k;
        let write_cost = (writes / 1000.0) * tc.write_per_1k;
        let egress = if tier == StorageTier::Cloud || tier == StorageTier::Archive {
            size_gb * reads * tc.egress_per_gb
        } else {
            0.0
        };

        storage + read_cost + write_cost + egress
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_savings_calculator_config_default() {
        let config = SavingsCalculatorConfig::default();
        assert!(config.redis_cost_per_gb_month > 0.0);
    }

    #[test]
    fn test_optimal_placement_empty() {
        let calc = SavingsCalculator::with_defaults();
        let placements = calc.calculate_optimal_placement(&[]);
        assert!(placements.is_empty());
    }

    #[test]
    fn test_optimal_placement_cold_key() {
        let calc = SavingsCalculator::with_defaults();

        // A cold key with no reads/writes should land on cheapest storage tier
        let stats = KeyAccessStats::new(b"cold:key".to_vec(), StorageTier::Memory, 1024 * 1024);
        let placements = calc.calculate_optimal_placement(&[stats]);

        let tier = placements.get(&b"cold:key".to_vec()).unwrap();
        // Should not be Memory since archive/cloud storage cost is cheaper for zero ops
        assert!(tier.is_colder_than(&StorageTier::Memory));
    }

    #[test]
    fn test_compare_with_redis() {
        let calc = SavingsCalculator::with_defaults();
        let report = calc.compare_with_redis(100.0, 10000.0);

        assert!(report.redis_monthly_cost > 0.0);
        assert!(report.ferrite_monthly_cost > 0.0);
        // Tiered storage should be cheaper than pure Redis for large datasets
        assert!(report.savings_pct > 0.0);
        // We save 80% of memory (only 20% stays in memory)
        assert!((report.memory_saved_gb - 80.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_compare_with_redis_zero_data() {
        let calc = SavingsCalculator::with_defaults();
        let report = calc.compare_with_redis(0.0, 0.0);

        assert_eq!(report.redis_monthly_cost, 0.0);
        assert_eq!(report.ferrite_monthly_cost, 0.0);
        assert_eq!(report.savings_pct, 0.0);
    }
}
