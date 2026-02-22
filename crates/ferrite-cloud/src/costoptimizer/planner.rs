//! Cost-aware query planner

use serde::{Deserialize, Serialize};

use super::model::StorageTier;
use super::CostOptimizerError;

/// An optimized query plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizedPlan {
    /// Plan type (scan, index_lookup, join, etc.)
    pub plan_type: String,
    /// Estimated rows to process
    pub estimated_rows: u64,
    /// Estimated reads
    pub estimated_reads: u64,
    /// Estimated writes
    pub estimated_writes: u64,
    /// Average row size in bytes
    pub avg_row_bytes: u64,
    /// Estimated latency in ms
    pub estimated_latency_ms: f64,
    /// Storage tier to use
    pub storage_tier: StorageTier,
    /// Whether network transfer is needed
    pub requires_network_transfer: bool,
    /// Plan steps
    pub steps: Vec<PlanStep>,
    /// Original query
    pub query: String,
}

impl OptimizedPlan {
    /// Create a new optimized plan
    pub fn new(query: impl Into<String>, plan_type: impl Into<String>) -> Self {
        Self {
            plan_type: plan_type.into(),
            estimated_rows: 0,
            estimated_reads: 0,
            estimated_writes: 0,
            avg_row_bytes: 100,
            estimated_latency_ms: 0.0,
            storage_tier: StorageTier::Memory,
            requires_network_transfer: false,
            steps: Vec::new(),
            query: query.into(),
        }
    }

    /// Set estimated rows
    pub fn with_rows(mut self, rows: u64) -> Self {
        self.estimated_rows = rows;
        self
    }

    /// Set storage tier
    pub fn with_tier(mut self, tier: StorageTier) -> Self {
        self.storage_tier = tier;
        self
    }

    /// Add a plan step
    pub fn add_step(&mut self, step: PlanStep) {
        self.steps.push(step);
    }
}

/// A step in the execution plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanStep {
    /// Step type
    pub step_type: StepType,
    /// Target (table, index, etc.)
    pub target: String,
    /// Estimated cost for this step
    pub estimated_cost: f64,
    /// Estimated rows output
    pub output_rows: u64,
}

/// Type of plan step
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StepType {
    /// Full table/collection scan
    Scan,
    /// Index lookup
    IndexLookup,
    /// Index range scan
    IndexScan,
    /// Hash join
    HashJoin,
    /// Nested loop join
    NestedLoop,
    /// Sort
    Sort,
    /// Aggregate
    Aggregate,
    /// Filter
    Filter,
    /// Project (select columns)
    Project,
    /// Limit
    Limit,
}

/// A plan choice for comparison
#[derive(Debug, Clone)]
pub struct PlanChoice {
    /// The plan
    pub plan: OptimizedPlan,
    /// Estimated total cost
    pub estimated_cost: f64,
    /// Estimated latency
    pub estimated_latency: f64,
    /// Plan score (lower is better)
    pub score: f64,
}

/// Cost-aware query planner
pub struct CostAwarePlanner {
    /// Statistics cache for estimation
    stats_cache: parking_lot::RwLock<std::collections::HashMap<String, TableStats>>,
}

/// Table statistics for estimation
#[derive(Debug, Clone)]
struct TableStats {
    /// Estimated row count
    #[allow(dead_code)] // Planned for v0.2 — stored for cost estimation cardinality
    row_count: u64,
    /// Average row size
    #[allow(dead_code)] // Planned for v0.2 — stored for I/O cost estimation
    avg_row_size: u64,
    /// Index availability
    #[allow(dead_code)] // Planned for v0.2 — stored for index selection in query planning
    indexes: Vec<String>,
    /// Storage tier
    #[allow(dead_code)] // Planned for v0.2 — stored for tier-aware cost calculation
    tier: StorageTier,
}

impl CostAwarePlanner {
    /// Create a new planner
    pub fn new() -> Self {
        Self {
            stats_cache: parking_lot::RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Generate plan candidates for a query
    pub fn generate_candidates(
        &self,
        query: &str,
    ) -> Result<Vec<OptimizedPlan>, CostOptimizerError> {
        // Parse the query to understand what's being requested
        let query_lower = query.to_lowercase();

        let mut candidates = Vec::new();

        // Generate different plan strategies
        if query_lower.contains("select") || query_lower.contains("get") {
            candidates.extend(self.generate_read_plans(query));
        } else if query_lower.contains("insert") || query_lower.contains("set") {
            candidates.extend(self.generate_write_plans(query));
        } else if query_lower.contains("update") {
            candidates.extend(self.generate_update_plans(query));
        } else if query_lower.contains("delete") || query_lower.contains("del") {
            candidates.extend(self.generate_delete_plans(query));
        } else {
            // Default: generate a simple scan plan
            candidates.push(self.generate_default_plan(query));
        }

        if candidates.is_empty() {
            return Err(CostOptimizerError::InvalidQuery(
                "could not generate any plan candidates".to_string(),
            ));
        }

        Ok(candidates)
    }

    /// Generate read plan candidates
    fn generate_read_plans(&self, query: &str) -> Vec<OptimizedPlan> {
        let mut plans = Vec::new();

        // Plan 1: Memory scan (fastest but may not have all data)
        let mut memory_plan = OptimizedPlan::new(query, "scan")
            .with_rows(1000)
            .with_tier(StorageTier::Memory);
        memory_plan.estimated_latency_ms = 1.0;
        memory_plan.estimated_reads = 1000;
        memory_plan.add_step(PlanStep {
            step_type: StepType::Scan,
            target: "memory".to_string(),
            estimated_cost: 0.001,
            output_rows: 1000,
        });
        plans.push(memory_plan);

        // Plan 2: Index lookup (if available)
        if query.contains("where") || query.contains('=') {
            let mut index_plan = OptimizedPlan::new(query, "index_lookup")
                .with_rows(10)
                .with_tier(StorageTier::LocalSsd);
            index_plan.estimated_latency_ms = 0.5;
            index_plan.estimated_reads = 10;
            index_plan.add_step(PlanStep {
                step_type: StepType::IndexLookup,
                target: "primary".to_string(),
                estimated_cost: 0.0001,
                output_rows: 10,
            });
            plans.push(index_plan);
        }

        // Plan 3: SSD scan (for larger datasets)
        let mut ssd_plan = OptimizedPlan::new(query, "scan")
            .with_rows(100000)
            .with_tier(StorageTier::LocalSsd);
        ssd_plan.estimated_latency_ms = 10.0;
        ssd_plan.estimated_reads = 100000;
        ssd_plan.add_step(PlanStep {
            step_type: StepType::Scan,
            target: "ssd".to_string(),
            estimated_cost: 0.01,
            output_rows: 100000,
        });
        plans.push(ssd_plan);

        plans
    }

    /// Generate write plan candidates
    fn generate_write_plans(&self, query: &str) -> Vec<OptimizedPlan> {
        let mut plans = Vec::new();

        // Plan 1: Write to memory (with async flush)
        let mut memory_plan = OptimizedPlan::new(query, "write")
            .with_rows(1)
            .with_tier(StorageTier::Memory);
        memory_plan.estimated_latency_ms = 0.1;
        memory_plan.estimated_writes = 1;
        plans.push(memory_plan);

        // Plan 2: Sync write to SSD
        let mut ssd_plan = OptimizedPlan::new(query, "write")
            .with_rows(1)
            .with_tier(StorageTier::LocalSsd);
        ssd_plan.estimated_latency_ms = 1.0;
        ssd_plan.estimated_writes = 1;
        plans.push(ssd_plan);

        plans
    }

    /// Generate update plan candidates
    fn generate_update_plans(&self, query: &str) -> Vec<OptimizedPlan> {
        let mut plans = Vec::new();

        // Plan: Read-modify-write
        let mut plan = OptimizedPlan::new(query, "update")
            .with_rows(1)
            .with_tier(StorageTier::Memory);
        plan.estimated_latency_ms = 0.5;
        plan.estimated_reads = 1;
        plan.estimated_writes = 1;
        plan.add_step(PlanStep {
            step_type: StepType::IndexLookup,
            target: "primary".to_string(),
            estimated_cost: 0.0001,
            output_rows: 1,
        });
        plans.push(plan);

        plans
    }

    /// Generate delete plan candidates
    fn generate_delete_plans(&self, query: &str) -> Vec<OptimizedPlan> {
        let mut plans = Vec::new();

        // Plan: Lookup and delete
        let mut plan = OptimizedPlan::new(query, "delete")
            .with_rows(1)
            .with_tier(StorageTier::Memory);
        plan.estimated_latency_ms = 0.3;
        plan.estimated_reads = 1;
        plan.estimated_writes = 1;
        plans.push(plan);

        plans
    }

    /// Generate default plan
    fn generate_default_plan(&self, query: &str) -> OptimizedPlan {
        let mut plan = OptimizedPlan::new(query, "scan")
            .with_rows(100)
            .with_tier(StorageTier::Memory);
        plan.estimated_latency_ms = 1.0;
        plan.estimated_reads = 100;
        plan
    }

    /// Update table statistics
    pub fn update_stats(&self, table: &str, stats: TableStats) {
        self.stats_cache.write().insert(table.to_string(), stats);
    }
}

impl Default for CostAwarePlanner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_candidates() {
        let planner = CostAwarePlanner::new();

        let candidates = planner
            .generate_candidates("SELECT * FROM users WHERE id = 1")
            .unwrap();
        assert!(!candidates.is_empty());
    }

    #[test]
    fn test_plan_creation() {
        let plan = OptimizedPlan::new("test query", "scan")
            .with_rows(1000)
            .with_tier(StorageTier::Memory);

        assert_eq!(plan.estimated_rows, 1000);
        assert_eq!(plan.storage_tier, StorageTier::Memory);
    }
}
