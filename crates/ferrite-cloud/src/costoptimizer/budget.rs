#![forbid(unsafe_code)]
//! Budget management for cost-aware intelligent tiering.
//!
//! Provides per-namespace budget tracking, cost models for storage tiers,
//! and budget reporting with projected spend.

use serde::{Deserialize, Serialize};

/// Budget configuration for a namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BudgetConfig {
    /// Namespace this budget applies to.
    pub namespace: String,
    /// Monthly spending limit in cents (USD).
    pub monthly_limit_cents: u64,
    /// Alert threshold as a percentage of the limit (0–100).
    pub alert_threshold_pct: u8,
    /// Whether automatic tier optimization is enabled.
    pub auto_optimize: bool,
    /// Target latency SLA in milliseconds.
    pub latency_sla_ms: f64,
}

impl Default for BudgetConfig {
    fn default() -> Self {
        Self {
            namespace: "default".to_string(),
            monthly_limit_cents: 100_00, // $100
            alert_threshold_pct: 80,
            auto_optimize: true,
            latency_sla_ms: 10.0,
        }
    }
}

/// Cost model for different storage tiers (in cents).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierCostModel {
    /// Memory cost per GB per month in cents.
    pub memory_per_gb_month_cents: u64,
    /// SSD cost per GB per month in cents.
    pub ssd_per_gb_month_cents: u64,
    /// S3 cost per GB per month in cents.
    pub s3_per_gb_month_cents: u64,
    /// S3 GET cost per 1 000 requests in cents.
    pub s3_get_per_1k_cents: u64,
    /// S3 PUT cost per 1 000 requests in cents.
    pub s3_put_per_1k_cents: u64,
    /// Network egress cost per GB in cents.
    pub network_per_gb_cents: u64,
}

impl Default for TierCostModel {
    fn default() -> Self {
        Self {
            memory_per_gb_month_cents: 3200, // $32/GB/month
            ssd_per_gb_month_cents: 1000,    // $10/GB/month
            s3_per_gb_month_cents: 2,        // $0.023/GB/month (rounded)
            s3_get_per_1k_cents: 0,          // $0.0004/1000 reqs (< 1 cent)
            s3_put_per_1k_cents: 1,          // $0.005/1000 reqs
            network_per_gb_cents: 9,         // $0.09/GB
        }
    }
}

/// Budget report for a billing period.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BudgetReport {
    /// Namespace this report covers.
    pub namespace: String,
    /// Billing period label (e.g. `"2025-07"`).
    pub period: String,
    /// Budget limit in cents.
    pub budget_cents: u64,
    /// Amount spent so far in cents.
    pub spent_cents: u64,
    /// Projected total spend for the period in cents.
    pub projected_cents: u64,
    /// Memory tier cost in cents.
    pub memory_cost_cents: u64,
    /// Persistent storage cost in cents.
    pub storage_cost_cents: u64,
    /// Network cost in cents.
    pub network_cost_cents: u64,
    /// API / request cost in cents.
    pub api_cost_cents: u64,
    /// Percentage saved compared to keeping everything in memory.
    pub savings_vs_all_memory_pct: f64,
    /// Current budget health status.
    pub status: BudgetStatus,
}

/// Budget health status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BudgetStatus {
    /// Spending is within normal bounds.
    OnTrack,
    /// Spending is approaching the alert threshold.
    Warning,
    /// Spending exceeds the budget.
    OverBudget,
}

impl BudgetStatus {
    /// Derive the status from current and limit values.
    pub fn from_spend(spent_cents: u64, limit_cents: u64, threshold_pct: u8) -> Self {
        if limit_cents == 0 {
            return Self::OnTrack;
        }
        let pct = (spent_cents as f64 / limit_cents as f64 * 100.0) as u8;
        if pct >= 100 {
            Self::OverBudget
        } else if pct >= threshold_pct {
            Self::Warning
        } else {
            Self::OnTrack
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_budget_config() {
        let cfg = BudgetConfig::default();
        assert_eq!(cfg.namespace, "default");
        assert_eq!(cfg.monthly_limit_cents, 100_00);
        assert_eq!(cfg.alert_threshold_pct, 80);
    }

    #[test]
    fn test_default_tier_cost_model() {
        let model = TierCostModel::default();
        assert!(model.memory_per_gb_month_cents > model.ssd_per_gb_month_cents);
        assert!(model.ssd_per_gb_month_cents > model.s3_per_gb_month_cents);
    }

    #[test]
    fn test_budget_status_on_track() {
        assert_eq!(BudgetStatus::from_spend(50_00, 100_00, 80), BudgetStatus::OnTrack);
    }

    #[test]
    fn test_budget_status_warning() {
        assert_eq!(BudgetStatus::from_spend(85_00, 100_00, 80), BudgetStatus::Warning);
    }

    #[test]
    fn test_budget_status_over_budget() {
        assert_eq!(BudgetStatus::from_spend(110_00, 100_00, 80), BudgetStatus::OverBudget);
    }
}
