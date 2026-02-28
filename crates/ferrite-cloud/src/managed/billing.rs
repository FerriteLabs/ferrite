#![allow(dead_code)]
//! Usage-based billing for managed Ferrite Cloud
//!
//! Tracks resource consumption and calculates costs for managed
//! Ferrite instances using a pay-as-you-go pricing model.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::provisioner::InstanceRecord;

/// Billing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BillingConfig {
    /// Pricing tier for this account
    pub pricing_tier: PricingTier,
    /// Billing cycle frequency
    pub billing_cycle: BillingCycle,
}

impl Default for BillingConfig {
    fn default() -> Self {
        Self {
            pricing_tier: PricingTier::PayAsYouGo,
            billing_cycle: BillingCycle::Monthly,
        }
    }
}

/// Pricing tier
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PricingTier {
    /// Pay only for what you use
    PayAsYouGo,
    /// Reserved capacity with discounted rates
    Reserved,
    /// Enterprise plan with custom pricing
    Enterprise,
}

/// Billing cycle frequency
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum BillingCycle {
    /// Monthly billing
    Monthly,
    /// Annual billing (with discount)
    Annual,
}

/// Pricing table with per-unit costs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PricingTable {
    /// Cost per vCPU-hour
    pub compute_per_vcpu_hour: f64,
    /// Cost per GB-hour of memory
    pub memory_per_gb_hour: f64,
    /// Cost per GB-month of storage
    pub storage_per_gb_month: f64,
    /// Cost per GB of network egress
    pub network_egress_per_gb: f64,
    /// Cost per million operations
    pub ops_per_million: f64,
    /// Cost per GB of backup storage
    pub backup_per_gb: f64,
}

impl Default for PricingTable {
    fn default() -> Self {
        Self {
            compute_per_vcpu_hour: 0.048,
            memory_per_gb_hour: 0.012,
            storage_per_gb_month: 0.10,
            network_egress_per_gb: 0.09,
            ops_per_million: 0.50,
            backup_per_gb: 0.03,
        }
    }
}

/// A billing period defined by start and end timestamps
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BillingPeriod {
    /// Period start time
    pub start: DateTime<Utc>,
    /// Period end time
    pub end: DateTime<Utc>,
}

/// Usage report for an instance over a billing period
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageReport {
    /// Instance identifier
    pub instance_id: String,
    /// Billing period
    pub period: BillingPeriod,
    /// Total compute hours (vCPU-hours)
    pub compute_hours: f64,
    /// Total memory usage (GB-hours)
    pub memory_gb_hours: f64,
    /// Total storage used (GB)
    pub storage_gb: f64,
    /// Total number of operations
    pub operations: u64,
    /// Total network egress (GB)
    pub network_egress_gb: f64,
    /// Total cost for the period
    pub total_cost: f64,
}

/// Cost estimate breakdown for an instance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostEstimate {
    /// Compute cost component
    pub compute_cost: f64,
    /// Memory cost component
    pub memory_cost: f64,
    /// Storage cost component
    pub storage_cost: f64,
    /// Operations cost component
    pub operations_cost: f64,
    /// Network cost component
    pub network_cost: f64,
    /// Total estimated monthly cost
    pub total_monthly: f64,
    /// Currency code
    pub currency: String,
}

/// Internal usage accumulator for an instance
#[derive(Debug, Clone, Default)]
struct UsageAccumulator {
    compute_hours: f64,
    memory_gb_hours: f64,
    storage_gb: f64,
    operations: u64,
    network_egress_bytes: u64,
}

/// Usage-based billing meter
pub struct UsageMeter {
    pricing: PricingTable,
    usage: RwLock<HashMap<String, UsageAccumulator>>,
}

impl UsageMeter {
    /// Create a new usage meter with the given pricing table
    pub fn new(pricing: PricingTable) -> Self {
        Self {
            pricing,
            usage: RwLock::new(HashMap::new()),
        }
    }

    /// Record compute usage for an instance
    pub fn record_compute_hours(&self, instance_id: &str, vcpus: u32, hours: f64) {
        let mut usage = self.usage.write();
        let acc = usage.entry(instance_id.to_string()).or_default();
        acc.compute_hours += vcpus as f64 * hours;
    }

    /// Record memory usage for an instance
    pub fn record_memory_hours(&self, instance_id: &str, gb: f64, hours: f64) {
        let mut usage = self.usage.write();
        let acc = usage.entry(instance_id.to_string()).or_default();
        acc.memory_gb_hours += gb * hours;
    }

    /// Record storage usage for an instance
    pub fn record_storage(&self, instance_id: &str, gb: f64) {
        let mut usage = self.usage.write();
        let acc = usage.entry(instance_id.to_string()).or_default();
        acc.storage_gb = gb;
    }

    /// Record operation count for an instance
    pub fn record_operations(&self, instance_id: &str, count: u64) {
        let mut usage = self.usage.write();
        let acc = usage.entry(instance_id.to_string()).or_default();
        acc.operations += count;
    }

    /// Record network egress for an instance
    pub fn record_network_egress(&self, instance_id: &str, bytes: u64) {
        let mut usage = self.usage.write();
        let acc = usage.entry(instance_id.to_string()).or_default();
        acc.network_egress_bytes += bytes;
    }

    /// Get usage report for an instance over a billing period
    pub fn get_usage(&self, instance_id: &str, period: BillingPeriod) -> UsageReport {
        let usage = self.usage.read();
        let acc = usage.get(instance_id);

        let (compute_hours, memory_gb_hours, storage_gb, operations, network_egress_gb) = match acc
        {
            Some(a) => (
                a.compute_hours,
                a.memory_gb_hours,
                a.storage_gb,
                a.operations,
                a.network_egress_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
            ),
            None => (0.0, 0.0, 0.0, 0, 0.0),
        };

        let total_cost = (compute_hours * self.pricing.compute_per_vcpu_hour)
            + (memory_gb_hours * self.pricing.memory_per_gb_hour)
            + (storage_gb * self.pricing.storage_per_gb_month)
            + (operations as f64 / 1_000_000.0 * self.pricing.ops_per_million)
            + (network_egress_gb * self.pricing.network_egress_per_gb);

        UsageReport {
            instance_id: instance_id.to_string(),
            period,
            compute_hours,
            memory_gb_hours,
            storage_gb,
            operations,
            network_egress_gb,
            total_cost,
        }
    }

    /// Estimate monthly cost for an instance based on its resource allocation
    pub fn estimate_monthly_cost(&self, instance: &InstanceRecord) -> CostEstimate {
        let hours_per_month = 730.0;
        let vcpus = instance.instance_type.vcpus();
        let memory_gb = instance.instance_type.memory_gb();

        let compute_cost = vcpus as f64 * hours_per_month * self.pricing.compute_per_vcpu_hour;
        let memory_cost = memory_gb as f64 * hours_per_month * self.pricing.memory_per_gb_hour;
        let storage_cost = 0.0; // Depends on actual usage
        let operations_cost = 0.0; // Depends on actual usage
        let network_cost = 0.0; // Depends on actual usage

        let total_monthly =
            compute_cost + memory_cost + storage_cost + operations_cost + network_cost;

        CostEstimate {
            compute_cost,
            memory_cost,
            storage_cost,
            operations_cost,
            network_cost,
            total_monthly,
            currency: "USD".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_get_usage() {
        let meter = UsageMeter::new(PricingTable::default());

        meter.record_compute_hours("inst-1", 2, 10.0);
        meter.record_memory_hours("inst-1", 4.0, 10.0);
        meter.record_storage("inst-1", 50.0);
        meter.record_operations("inst-1", 1_000_000);
        meter.record_network_egress("inst-1", 1_073_741_824); // 1 GB

        let period = BillingPeriod {
            start: Utc::now(),
            end: Utc::now(),
        };
        let report = meter.get_usage("inst-1", period);

        assert_eq!(report.instance_id, "inst-1");
        assert!((report.compute_hours - 20.0).abs() < f64::EPSILON);
        assert!((report.memory_gb_hours - 40.0).abs() < f64::EPSILON);
        assert!((report.storage_gb - 50.0).abs() < f64::EPSILON);
        assert_eq!(report.operations, 1_000_000);
        assert!(report.total_cost > 0.0);
    }

    #[test]
    fn test_estimate_monthly_cost() {
        use super::super::provisioner::{
            HealthStatus, InstanceHealth, InstanceState, InstanceType,
        };

        let meter = UsageMeter::new(PricingTable::default());
        let instance = InstanceRecord {
            id: "est-1".to_string(),
            name: "estimate-test".to_string(),
            state: InstanceState::Running,
            instance_type: InstanceType::Small,
            endpoint: "est-1.ferrite.cloud:6379".to_string(),
            replicas: 1,
            created_at: Utc::now(),
            region: "us-east-1".to_string(),
            health: InstanceHealth {
                status: HealthStatus::Healthy,
                latency_ms: 1.0,
                memory_usage_pct: 30.0,
                connections: 5,
                uptime_seconds: 3600,
            },
        };

        let estimate = meter.estimate_monthly_cost(&instance);
        assert_eq!(estimate.currency, "USD");
        assert!(estimate.compute_cost > 0.0);
        assert!(estimate.memory_cost > 0.0);
        assert!(estimate.total_monthly > 0.0);
    }
}
