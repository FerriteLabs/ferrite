//! Cost modeling for query optimization

use serde::{Deserialize, Serialize};

use super::{planner::OptimizedPlan, CostOptimizerConfig};

/// Cost estimate for a query plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostEstimate {
    /// Compute cost ($)
    pub compute: f64,
    /// Memory cost ($)
    pub memory: f64,
    /// Storage read cost ($)
    pub storage_read: f64,
    /// Storage write cost ($)
    pub storage_write: f64,
    /// Network cost ($)
    pub network: f64,
}

impl CostEstimate {
    /// Create a zero cost estimate
    pub fn zero() -> Self {
        Self {
            compute: 0.0,
            memory: 0.0,
            storage_read: 0.0,
            storage_write: 0.0,
            network: 0.0,
        }
    }

    /// Total cost
    pub fn total(&self) -> f64 {
        self.compute + self.memory + self.storage_read + self.storage_write + self.network
    }

    /// Add another estimate
    pub fn add(&self, other: &CostEstimate) -> CostEstimate {
        CostEstimate {
            compute: self.compute + other.compute,
            memory: self.memory + other.memory,
            storage_read: self.storage_read + other.storage_read,
            storage_write: self.storage_write + other.storage_write,
            network: self.network + other.network,
        }
    }

    /// Scale by factor
    pub fn scale(&self, factor: f64) -> CostEstimate {
        CostEstimate {
            compute: self.compute * factor,
            memory: self.memory * factor,
            storage_read: self.storage_read * factor,
            storage_write: self.storage_write * factor,
            network: self.network * factor,
        }
    }
}

impl Default for CostEstimate {
    fn default() -> Self {
        Self::zero()
    }
}

/// Cost per resource type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceCost {
    /// Resource name
    pub name: String,
    /// Cost per unit
    pub cost_per_unit: f64,
    /// Unit description
    pub unit: String,
}

/// Cost for a storage tier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierCost {
    /// Tier name
    pub tier: StorageTier,
    /// Read cost per operation
    pub read_cost: f64,
    /// Write cost per operation
    pub write_cost: f64,
    /// Storage cost per GB-month
    pub storage_cost: f64,
    /// Average latency in ms
    pub latency_ms: f64,
}

/// Storage tier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageTier {
    /// In-memory (fastest, most expensive)
    Memory,
    /// Local SSD
    LocalSsd,
    /// Remote SSD (e.g., EBS)
    RemoteSsd,
    /// Object storage (e.g., S3)
    ObjectStorage,
    /// Archive storage (e.g., Glacier)
    Archive,
}

impl StorageTier {
    /// Get default tier costs
    pub fn default_costs() -> Vec<TierCost> {
        vec![
            TierCost {
                tier: StorageTier::Memory,
                read_cost: 0.0,
                write_cost: 0.0,
                storage_cost: 10.0, // $10/GB-month for memory
                latency_ms: 0.01,
            },
            TierCost {
                tier: StorageTier::LocalSsd,
                read_cost: 0.00001,
                write_cost: 0.00005,
                storage_cost: 0.10, // $0.10/GB-month
                latency_ms: 0.1,
            },
            TierCost {
                tier: StorageTier::RemoteSsd,
                read_cost: 0.00004,
                write_cost: 0.00020,
                storage_cost: 0.08,
                latency_ms: 1.0,
            },
            TierCost {
                tier: StorageTier::ObjectStorage,
                read_cost: 0.0004,
                write_cost: 0.005,
                storage_cost: 0.023, // S3 standard
                latency_ms: 50.0,
            },
            TierCost {
                tier: StorageTier::Archive,
                read_cost: 0.01,
                write_cost: 0.05,
                storage_cost: 0.004,   // S3 Glacier
                latency_ms: 3600000.0, // Hours for retrieval
            },
        ]
    }
}

/// Cost model for estimating query costs
pub struct CostModel {
    /// Compute cost per vCPU-second
    compute_cost: f64,
    /// Memory cost per GB-second
    memory_cost: f64,
    /// Storage tier costs
    tier_costs: Vec<TierCost>,
    /// Network egress cost per GB
    network_cost: f64,
}

impl CostModel {
    /// Create a new cost model from config
    pub fn new(config: &CostOptimizerConfig) -> Self {
        Self {
            compute_cost: config.compute_cost_per_unit,
            memory_cost: config.memory_cost_per_unit,
            tier_costs: StorageTier::default_costs(),
            network_cost: config.network_egress_cost,
        }
    }

    /// Create with default values
    pub fn with_defaults() -> Self {
        Self::new(&CostOptimizerConfig::default())
    }

    /// Estimate cost for a plan
    pub fn estimate(&self, plan: &OptimizedPlan) -> CostEstimate {
        let compute = self.estimate_compute(plan);
        let memory = self.estimate_memory(plan);
        let storage = self.estimate_storage(plan);
        let network = self.estimate_network(plan);

        CostEstimate {
            compute,
            memory,
            storage_read: storage.0,
            storage_write: storage.1,
            network,
        }
    }

    /// Estimate compute cost
    fn estimate_compute(&self, plan: &OptimizedPlan) -> f64 {
        // Estimate based on operation complexity
        let base_ops = match plan.plan_type.as_str() {
            "scan" => plan.estimated_rows as f64 * 0.001, // 0.001 vCPU-sec per row
            "index_lookup" => (plan.estimated_rows as f64).log2() * 0.0001,
            "join" => (plan.estimated_rows as f64).powf(1.5) * 0.0001,
            "aggregate" => plan.estimated_rows as f64 * 0.002,
            "sort" => plan.estimated_rows as f64 * (plan.estimated_rows as f64).log2() * 0.0001,
            _ => plan.estimated_rows as f64 * 0.001,
        };

        base_ops * self.compute_cost
    }

    /// Estimate memory cost
    fn estimate_memory(&self, plan: &OptimizedPlan) -> f64 {
        // Estimate memory usage based on row size and operation
        let estimated_bytes = plan.estimated_rows as f64 * plan.avg_row_bytes as f64;
        let memory_gb = estimated_bytes / 1_000_000_000.0;

        // Assume operation takes avg 1 second
        memory_gb * self.memory_cost
    }

    /// Estimate storage costs (read, write)
    fn estimate_storage(&self, plan: &OptimizedPlan) -> (f64, f64) {
        let tier_cost = self.get_tier_cost(plan.storage_tier);

        let read_cost = plan.estimated_reads as f64 * tier_cost.read_cost;
        let write_cost = plan.estimated_writes as f64 * tier_cost.write_cost;

        (read_cost, write_cost)
    }

    /// Estimate network cost
    fn estimate_network(&self, plan: &OptimizedPlan) -> f64 {
        let bytes = plan.estimated_rows as f64 * plan.avg_row_bytes as f64;
        let gb = bytes / 1_000_000_000.0;

        // Only count egress if cross-region or external
        if plan.requires_network_transfer {
            gb * self.network_cost
        } else {
            0.0
        }
    }

    /// Get tier cost
    fn get_tier_cost(&self, tier: StorageTier) -> &TierCost {
        self.tier_costs
            .iter()
            .find(|t| t.tier == tier)
            .unwrap_or(&self.tier_costs[0])
    }

    /// Calculate total cost for data movement between tiers
    pub fn tier_migration_cost(&self, from: StorageTier, to: StorageTier, size_gb: f64) -> f64 {
        let from_cost = self.get_tier_cost(from);
        let to_cost = self.get_tier_cost(to);

        // Read from source + write to destination
        let read_ops = (size_gb * 1000.0) as u64; // Assume 1MB chunks
        let write_ops = read_ops;

        let read_cost = read_ops as f64 * from_cost.read_cost;
        let write_cost = write_ops as f64 * to_cost.write_cost;

        read_cost + write_cost
    }

    /// Get cost breakdown by category
    pub fn cost_breakdown(&self, plan: &OptimizedPlan) -> Vec<(String, f64)> {
        let estimate = self.estimate(plan);

        vec![
            ("Compute".to_string(), estimate.compute),
            ("Memory".to_string(), estimate.memory),
            ("Storage Read".to_string(), estimate.storage_read),
            ("Storage Write".to_string(), estimate.storage_write),
            ("Network".to_string(), estimate.network),
        ]
    }
}

impl Default for CostModel {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cost_estimate() {
        let estimate = CostEstimate {
            compute: 0.001,
            memory: 0.0001,
            storage_read: 0.0004,
            storage_write: 0.0,
            network: 0.0,
        };

        assert!((estimate.total() - 0.0015).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_scale() {
        let estimate = CostEstimate {
            compute: 1.0,
            memory: 1.0,
            storage_read: 1.0,
            storage_write: 1.0,
            network: 1.0,
        };

        let scaled = estimate.scale(2.0);
        assert!((scaled.total() - 10.0).abs() < 0.001);
    }

    #[test]
    fn test_tier_costs() {
        let costs = StorageTier::default_costs();
        assert!(!costs.is_empty());

        // Memory should be fastest
        let memory = costs
            .iter()
            .find(|c| c.tier == StorageTier::Memory)
            .unwrap();
        let ssd = costs
            .iter()
            .find(|c| c.tier == StorageTier::LocalSsd)
            .unwrap();
        assert!(memory.latency_ms < ssd.latency_ms);
    }
}
