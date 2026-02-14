//! Cost Savings Calculator and Dashboard Data
//!
//! Tracks memory usage across storage tiers and calculates the equivalent
//! Redis cost to show real-time savings from tiered storage.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

/// Cloud pricing estimates (per GB per month in USD).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PricingModel {
    /// RAM cost per GB/month (e.g. AWS ElastiCache)
    pub ram_per_gb: f64,
    /// NVMe SSD cost per GB/month
    pub ssd_per_gb: f64,
    /// Object storage cost per GB/month (S3/GCS)
    pub cloud_per_gb: f64,
}

impl Default for PricingModel {
    fn default() -> Self {
        Self {
            ram_per_gb: 7.0,
            ssd_per_gb: 0.50,
            cloud_per_gb: 0.023,
        }
    }
}

/// Observed data distribution across storage tiers.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TierUsage {
    /// Bytes in hot tier (memory).
    pub hot_bytes: u64,
    /// Bytes in warm tier (mmap/SSD).
    pub warm_bytes: u64,
    /// Bytes in cold tier (disk/cloud).
    pub cold_bytes: u64,
}

impl TierUsage {
    /// Total data size across all tiers.
    pub fn total_bytes(&self) -> u64 {
        self.hot_bytes + self.warm_bytes + self.cold_bytes
    }

    fn to_gb(bytes: u64) -> f64 {
        bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    }
}

/// Computed cost comparison between Ferrite (tiered) and Redis (all-in-memory).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostComparison {
    /// What Redis would cost for the same total data size (all in RAM).
    pub redis_monthly_cost: f64,
    /// What Ferrite costs with tiered storage.
    pub ferrite_monthly_cost: f64,
    /// Absolute savings per month.
    pub savings_monthly: f64,
    /// Percentage savings (0-100).
    pub savings_percent: f64,
    /// Per-tier breakdown.
    pub tier_costs: TierCosts,
    /// Current tier usage.
    pub usage: TierUsage,
}

/// Cost breakdown by tier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierCosts {
    pub hot_cost: f64,
    pub warm_cost: f64,
    pub cold_cost: f64,
}

/// Thread-safe cost tracker that accumulates tier usage over time.
pub struct CostTracker {
    hot_bytes: AtomicU64,
    warm_bytes: AtomicU64,
    cold_bytes: AtomicU64,
    pricing: PricingModel,
}

impl CostTracker {
    /// Create a new tracker with default pricing.
    pub fn new() -> Self {
        Self::with_pricing(PricingModel::default())
    }

    /// Create with custom pricing.
    pub fn with_pricing(pricing: PricingModel) -> Self {
        Self {
            hot_bytes: AtomicU64::new(0),
            warm_bytes: AtomicU64::new(0),
            cold_bytes: AtomicU64::new(0),
            pricing,
        }
    }

    /// Update the observed tier usage.
    pub fn update_usage(&self, usage: &TierUsage) {
        self.hot_bytes.store(usage.hot_bytes, Ordering::Relaxed);
        self.warm_bytes.store(usage.warm_bytes, Ordering::Relaxed);
        self.cold_bytes.store(usage.cold_bytes, Ordering::Relaxed);
    }

    /// Record bytes added to the hot tier.
    pub fn add_hot(&self, bytes: u64) {
        self.hot_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record bytes moved to the warm tier.
    pub fn add_warm(&self, bytes: u64) {
        self.warm_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record bytes moved to the cold tier.
    pub fn add_cold(&self, bytes: u64) {
        self.cold_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Snapshot current tier usage.
    pub fn usage(&self) -> TierUsage {
        TierUsage {
            hot_bytes: self.hot_bytes.load(Ordering::Relaxed),
            warm_bytes: self.warm_bytes.load(Ordering::Relaxed),
            cold_bytes: self.cold_bytes.load(Ordering::Relaxed),
        }
    }

    /// Compute cost comparison against Redis.
    pub fn compare(&self) -> CostComparison {
        let usage = self.usage();
        let total_gb = TierUsage::to_gb(usage.total_bytes());

        let redis_cost = total_gb * self.pricing.ram_per_gb;

        let hot_cost = TierUsage::to_gb(usage.hot_bytes) * self.pricing.ram_per_gb;
        let warm_cost = TierUsage::to_gb(usage.warm_bytes) * self.pricing.ssd_per_gb;
        let cold_cost = TierUsage::to_gb(usage.cold_bytes) * self.pricing.cloud_per_gb;
        let ferrite_cost = hot_cost + warm_cost + cold_cost;

        let savings = redis_cost - ferrite_cost;
        let savings_pct = if redis_cost > 0.0 {
            (savings / redis_cost) * 100.0
        } else {
            0.0
        };

        CostComparison {
            redis_monthly_cost: redis_cost,
            ferrite_monthly_cost: ferrite_cost,
            savings_monthly: savings,
            savings_percent: savings_pct,
            tier_costs: TierCosts {
                hot_cost,
                warm_cost,
                cold_cost,
            },
            usage,
        }
    }
}

impl Default for CostTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cost_comparison_no_data() {
        let tracker = CostTracker::new();
        let cmp = tracker.compare();
        assert_eq!(cmp.redis_monthly_cost, 0.0);
        assert_eq!(cmp.ferrite_monthly_cost, 0.0);
        assert_eq!(cmp.savings_monthly, 0.0);
    }

    #[test]
    fn cost_comparison_all_hot() {
        let tracker = CostTracker::new();
        let one_gb = 1024 * 1024 * 1024;
        tracker.update_usage(&TierUsage {
            hot_bytes: one_gb,
            warm_bytes: 0,
            cold_bytes: 0,
        });
        let cmp = tracker.compare();
        // All in memory = same cost as Redis.
        assert!((cmp.redis_monthly_cost - cmp.ferrite_monthly_cost).abs() < 0.001);
        assert!(cmp.savings_percent.abs() < 0.01);
    }

    #[test]
    fn cost_comparison_tiered_saves_money() {
        let tracker = CostTracker::new();
        let one_gb = 1024 * 1024 * 1024;
        tracker.update_usage(&TierUsage {
            hot_bytes: one_gb,      // 1 GB hot
            warm_bytes: 4 * one_gb, // 4 GB warm
            cold_bytes: 5 * one_gb, // 5 GB cold
        });
        let cmp = tracker.compare();
        // Redis: 10 GB * $7 = $70
        assert!((cmp.redis_monthly_cost - 70.0).abs() < 0.1);
        // Ferrite: 1*7 + 4*0.50 + 5*0.023 = 7 + 2 + 0.115 = $9.115
        assert!((cmp.ferrite_monthly_cost - 9.115).abs() < 0.1);
        assert!(cmp.savings_percent > 85.0);
    }

    #[test]
    fn atomic_add_operations() {
        let tracker = CostTracker::new();
        tracker.add_hot(100);
        tracker.add_warm(200);
        tracker.add_cold(300);
        let usage = tracker.usage();
        assert_eq!(usage.hot_bytes, 100);
        assert_eq!(usage.warm_bytes, 200);
        assert_eq!(usage.cold_bytes, 300);
    }
}
