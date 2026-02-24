//! Cost-Aware Query Optimizer
//!
//! Optimizes queries based on actual infrastructure costs including
//! compute, storage tiers, network, and caching.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                  Cost-Aware Optimizer                        │
//! ├─────────────────────────────────────────────────────────────┤
//! │  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐ │
//! │  │  Cost    │   │ Strategy │   │  Plan    │   │ Executor │ │
//! │  │  Model   │──▶│ Selector │──▶│ Rewriter │──▶│ Hints    │ │
//! │  └──────────┘   └──────────┘   └──────────┘   └──────────┘ │
//! │       │              │              │              │        │
//! │       ▼              ▼              ▼              ▼        │
//! │  Infrastructure  Cost/Perf     Optimized    Adaptive       │
//! │     Pricing      Tradeoffs       Plan       Execution      │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Features
//!
//! - **Cost Modeling**: Model compute, storage, and network costs
//! - **Strategy Selection**: Choose between cost, performance, or balanced
//! - **Plan Rewriting**: Rewrite plans to reduce costs
//! - **Adaptive Execution**: Runtime adaptation based on cost signals

mod executor;
mod model;
mod planner;
mod strategy;

pub use executor::{CostAwareExecutor, CostMetrics, ExecutionHints};
pub use model::{CostEstimate, CostModel, ResourceCost, TierCost};
pub use planner::{CostAwarePlanner, OptimizedPlan, PlanChoice};
pub use strategy::{CostConstraint, OptimizationStrategy, StrategySelector};

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Cost optimizer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostOptimizerConfig {
    /// Default optimization strategy
    pub default_strategy: OptimizationStrategy,
    /// Cost per compute unit ($/vCPU-second)
    pub compute_cost_per_unit: f64,
    /// Cost per memory unit ($/GB-second)
    pub memory_cost_per_unit: f64,
    /// Cost per storage read ($/1000 reads)
    pub storage_read_cost: f64,
    /// Cost per storage write ($/1000 writes)
    pub storage_write_cost: f64,
    /// Cost per network egress ($/GB)
    pub network_egress_cost: f64,
    /// Enable cost-based plan caching
    pub plan_caching: bool,
    /// Maximum cost budget (0 = unlimited)
    pub max_cost_budget: f64,
}

impl Default for CostOptimizerConfig {
    fn default() -> Self {
        Self {
            default_strategy: OptimizationStrategy::Balanced,
            compute_cost_per_unit: 0.0000125, // ~$0.045/vCPU-hour
            memory_cost_per_unit: 0.0000017,  // ~$0.006/GB-hour
            storage_read_cost: 0.0004,        // $0.40 per million reads
            storage_write_cost: 0.005,        // $5.00 per million writes
            network_egress_cost: 0.09,        // $0.09/GB
            plan_caching: true,
            max_cost_budget: 0.0,
        }
    }
}

impl CostOptimizerConfig {
    /// Create a new config
    pub fn new() -> Self {
        Self::default()
    }

    /// Set optimization strategy
    pub fn with_strategy(mut self, strategy: OptimizationStrategy) -> Self {
        self.default_strategy = strategy;
        self
    }

    /// Set cost budget
    pub fn with_budget(mut self, budget: f64) -> Self {
        self.max_cost_budget = budget;
        self
    }
}

/// The main cost-aware query optimizer
pub struct CostOptimizer {
    config: CostOptimizerConfig,
    cost_model: CostModel,
    strategy_selector: StrategySelector,
    planner: CostAwarePlanner,
    executor: CostAwareExecutor,
    /// Cached plans for cost efficiency
    plan_cache: parking_lot::RwLock<HashMap<String, CachedPlan>>,
}

/// A cached query plan
#[derive(Debug, Clone)]
pub struct CachedPlan {
    /// The optimized plan
    pub plan: OptimizedPlan,
    /// Estimated cost
    pub estimated_cost: CostEstimate,
    /// Actual cost from execution
    pub actual_cost: Option<CostEstimate>,
    /// Hit count
    pub hits: u64,
    /// Last used timestamp
    pub last_used: u64,
}

impl CostOptimizer {
    /// Create a new cost optimizer
    pub fn new(config: CostOptimizerConfig) -> Self {
        let cost_model = CostModel::new(&config);
        let strategy_selector = StrategySelector::new(config.default_strategy);
        let planner = CostAwarePlanner::new();
        let executor = CostAwareExecutor::new();

        Self {
            config,
            cost_model,
            strategy_selector,
            planner,
            executor,
            plan_cache: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Create with default config
    pub fn with_defaults() -> Self {
        Self::new(CostOptimizerConfig::default())
    }

    /// Optimize a query with cost awareness
    pub fn optimize(&self, query: &str) -> Result<OptimizedPlan, CostOptimizerError> {
        // Check cache first
        if self.config.plan_caching {
            if let Some(cached) = self.get_cached_plan(query) {
                return Ok(cached.plan);
            }
        }

        // Select strategy based on query characteristics
        let strategy = self.strategy_selector.select(query);

        // Generate plan candidates
        let candidates = self.planner.generate_candidates(query)?;

        // Estimate costs for each candidate
        let costed_candidates: Vec<(OptimizedPlan, CostEstimate)> = candidates
            .into_iter()
            .map(|plan| {
                let cost = self.cost_model.estimate(&plan);
                (plan, cost)
            })
            .collect();

        // Select best plan based on strategy
        let (best_plan, estimated_cost) = self.select_best_plan(costed_candidates, &strategy)?;

        // Check budget constraint
        if self.config.max_cost_budget > 0.0 && estimated_cost.total() > self.config.max_cost_budget
        {
            return Err(CostOptimizerError::BudgetExceeded {
                estimated: estimated_cost.total(),
                budget: self.config.max_cost_budget,
            });
        }

        // Cache the plan
        if self.config.plan_caching {
            self.cache_plan(query, &best_plan, &estimated_cost);
        }

        Ok(best_plan)
    }

    /// Optimize with specific strategy
    pub fn optimize_with_strategy(
        &self,
        query: &str,
        strategy: OptimizationStrategy,
    ) -> Result<OptimizedPlan, CostOptimizerError> {
        let candidates = self.planner.generate_candidates(query)?;

        let costed_candidates: Vec<(OptimizedPlan, CostEstimate)> = candidates
            .into_iter()
            .map(|plan| {
                let cost = self.cost_model.estimate(&plan);
                (plan, cost)
            })
            .collect();

        let (best_plan, _) = self.select_best_plan(costed_candidates, &strategy)?;
        Ok(best_plan)
    }

    /// Estimate cost for a query
    pub fn estimate_cost(&self, query: &str) -> Result<CostEstimate, CostOptimizerError> {
        let plan = self.optimize(query)?;
        Ok(self.cost_model.estimate(&plan))
    }

    /// Get execution hints for a plan
    pub fn get_hints(&self, plan: &OptimizedPlan) -> ExecutionHints {
        self.executor.generate_hints(plan, &self.cost_model)
    }

    /// Record actual execution metrics
    pub fn record_execution(&self, query: &str, metrics: CostMetrics) {
        let mut cache = self.plan_cache.write();
        if let Some(cached) = cache.get_mut(query) {
            cached.actual_cost = Some(CostEstimate {
                compute: metrics.cpu_time_ms as f64 * self.config.compute_cost_per_unit,
                memory: metrics.memory_bytes as f64 * self.config.memory_cost_per_unit
                    / 1_000_000_000.0,
                storage_read: metrics.storage_reads as f64 * self.config.storage_read_cost / 1000.0,
                storage_write: metrics.storage_writes as f64 * self.config.storage_write_cost
                    / 1000.0,
                network: metrics.network_bytes as f64 * self.config.network_egress_cost
                    / 1_000_000_000.0,
            });
        }
    }

    /// Select best plan based on strategy
    fn select_best_plan(
        &self,
        candidates: Vec<(OptimizedPlan, CostEstimate)>,
        strategy: &OptimizationStrategy,
    ) -> Result<(OptimizedPlan, CostEstimate), CostOptimizerError> {
        if candidates.is_empty() {
            return Err(CostOptimizerError::NoPlanFound);
        }

        let selected = match strategy {
            OptimizationStrategy::MinCost => candidates.into_iter().min_by(|a, b| {
                a.1.total()
                    .partial_cmp(&b.1.total())
                    .unwrap_or(std::cmp::Ordering::Equal)
            }),
            OptimizationStrategy::MinLatency => candidates.into_iter().min_by(|a, b| {
                a.0.estimated_latency_ms
                    .partial_cmp(&b.0.estimated_latency_ms)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }),
            OptimizationStrategy::Balanced => {
                // Score combines cost and latency
                candidates.into_iter().min_by(|a, b| {
                    let score_a = a.1.total() * 1000.0 + a.0.estimated_latency_ms;
                    let score_b = b.1.total() * 1000.0 + b.0.estimated_latency_ms;
                    score_a
                        .partial_cmp(&score_b)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
            }
            OptimizationStrategy::Custom(constraint) => candidates
                .into_iter()
                .filter(|(_, cost)| {
                    constraint
                        .max_cost
                        .map(|c| cost.total() <= c)
                        .unwrap_or(true)
                })
                .min_by(|a, b| {
                    a.1.total()
                        .partial_cmp(&b.1.total())
                        .unwrap_or(std::cmp::Ordering::Equal)
                }),
        };

        selected.ok_or(CostOptimizerError::NoPlanFound)
    }

    /// Get cached plan
    fn get_cached_plan(&self, query: &str) -> Option<CachedPlan> {
        let mut cache = self.plan_cache.write();
        if let Some(cached) = cache.get_mut(query) {
            cached.hits += 1;
            cached.last_used = current_timestamp();
            return Some(cached.clone());
        }
        None
    }

    /// Cache a plan
    fn cache_plan(&self, query: &str, plan: &OptimizedPlan, cost: &CostEstimate) {
        let cached = CachedPlan {
            plan: plan.clone(),
            estimated_cost: cost.clone(),
            actual_cost: None,
            hits: 1,
            last_used: current_timestamp(),
        };
        self.plan_cache.write().insert(query.to_string(), cached);
    }

    /// Clear plan cache
    pub fn clear_cache(&self) {
        self.plan_cache.write().clear();
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> CacheStats {
        let cache = self.plan_cache.read();
        let total_hits: u64 = cache.values().map(|c| c.hits).sum();

        CacheStats {
            entries: cache.len() as u64,
            total_hits,
            avg_hits: if cache.is_empty() {
                0.0
            } else {
                total_hits as f64 / cache.len() as f64
            },
        }
    }

    /// Get configuration
    pub fn config(&self) -> &CostOptimizerConfig {
        &self.config
    }

    /// Get overall statistics
    pub fn stats(&self) -> OptimizerStats {
        OptimizerStats {
            cache: self.cache_stats(),
            default_strategy: self.config.default_strategy,
            budget: self.config.max_cost_budget,
        }
    }
}

impl Default for CostOptimizer {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// Cache statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStats {
    /// Number of cached entries
    pub entries: u64,
    /// Total cache hits
    pub total_hits: u64,
    /// Average hits per entry
    pub avg_hits: f64,
}

/// Optimizer statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizerStats {
    /// Cache statistics
    pub cache: CacheStats,
    /// Current default strategy
    pub default_strategy: OptimizationStrategy,
    /// Current budget
    pub budget: f64,
}

/// Cost optimizer errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum CostOptimizerError {
    /// No valid plan found
    #[error("no valid plan found")]
    NoPlanFound,

    /// Budget exceeded
    #[error("estimated cost ${estimated:.4} exceeds budget ${budget:.4}")]
    BudgetExceeded { estimated: f64, budget: f64 },

    /// Invalid query
    #[error("invalid query: {0}")]
    InvalidQuery(String),

    /// Optimization failed
    #[error("optimization failed: {0}")]
    OptimizationFailed(String),
}

/// Get current timestamp
fn current_timestamp() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optimizer_creation() {
        let optimizer = CostOptimizer::with_defaults();
        assert!(optimizer.config().plan_caching);
    }

    #[test]
    fn test_config_builder() {
        let config = CostOptimizerConfig::new()
            .with_strategy(OptimizationStrategy::MinCost)
            .with_budget(1.0);

        assert_eq!(config.default_strategy, OptimizationStrategy::MinCost);
        assert_eq!(config.max_cost_budget, 1.0);
    }
}
