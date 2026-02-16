//! Cost optimizer command handlers
//!
//! Implements cost-aware query optimization commands.

use std::sync::OnceLock;

use bytes::Bytes;

use ferrite_cloud::costoptimizer::{CostOptimizer, CostOptimizerConfig, OptimizationStrategy};
use crate::protocol::Frame;

use super::{err_frame, ok_frame, HandlerContext};

/// Global cost optimizer instance
static COST_OPTIMIZER: OnceLock<CostOptimizer> = OnceLock::new();

/// Get the global cost optimizer
fn cost_optimizer() -> &'static CostOptimizer {
    COST_OPTIMIZER.get_or_init(|| CostOptimizer::new(CostOptimizerConfig::default()))
}

/// Handle COST.ESTIMATE command
///
/// COST.ESTIMATE query
pub fn handle_cost_estimate(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("COST.ESTIMATE requires a query");
    }

    let query = String::from_utf8_lossy(&args[0]).to_string();

    match cost_optimizer().estimate_cost(&query) {
        Ok(estimate) => Frame::array(vec![
            Frame::bulk("compute"),
            Frame::bulk(format!("{:.6}", estimate.compute)),
            Frame::bulk("memory"),
            Frame::bulk(format!("{:.6}", estimate.memory)),
            Frame::bulk("storage_read"),
            Frame::bulk(format!("{:.6}", estimate.storage_read)),
            Frame::bulk("storage_write"),
            Frame::bulk(format!("{:.6}", estimate.storage_write)),
            Frame::bulk("network"),
            Frame::bulk(format!("{:.6}", estimate.network)),
            Frame::bulk("total"),
            Frame::bulk(format!("{:.6}", estimate.total())),
        ]),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle COST.OPTIMIZE command
///
/// COST.OPTIMIZE query [STRATEGY min_cost|min_latency|balanced]
pub fn handle_cost_optimize(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("COST.OPTIMIZE requires a query");
    }

    let query = String::from_utf8_lossy(&args[0]).to_string();

    // Parse optional strategy
    let strategy = if args.len() >= 3 {
        let keyword = String::from_utf8_lossy(&args[1]).to_uppercase();
        if keyword == "STRATEGY" {
            let strat_str = String::from_utf8_lossy(&args[2]).to_lowercase();
            match strat_str.as_str() {
                "min_cost" => Some(OptimizationStrategy::MinCost),
                "min_latency" => Some(OptimizationStrategy::MinLatency),
                "balanced" => Some(OptimizationStrategy::Balanced),
                _ => return err_frame("invalid strategy (min_cost, min_latency, balanced)"),
            }
        } else {
            None
        }
    } else {
        None
    };

    let result = if let Some(strat) = strategy {
        cost_optimizer().optimize_with_strategy(&query, strat)
    } else {
        cost_optimizer().optimize(&query)
    };

    match result {
        Ok(plan) => {
            let steps: Vec<Frame> = plan
                .steps
                .iter()
                .map(|s| {
                    Frame::bulk(format!(
                        "{:?}: {} (cost: {:.4})",
                        s.step_type, s.target, s.estimated_cost
                    ))
                })
                .collect();

            Frame::array(vec![
                Frame::bulk("plan_type"),
                Frame::bulk(plan.plan_type),
                Frame::bulk("estimated_rows"),
                Frame::Integer(plan.estimated_rows as i64),
                Frame::bulk("estimated_latency_ms"),
                Frame::bulk(format!("{:.2}", plan.estimated_latency_ms)),
                Frame::bulk("storage_tier"),
                Frame::bulk(format!("{:?}", plan.storage_tier)),
                Frame::bulk("steps"),
                Frame::array(steps),
            ])
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle COST.HINTS command
///
/// COST.HINTS query
pub fn handle_cost_hints(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("COST.HINTS requires a query");
    }

    let query = String::from_utf8_lossy(&args[0]).to_string();

    match cost_optimizer().optimize(&query) {
        Ok(plan) => {
            let hints = cost_optimizer().get_hints(&plan);

            Frame::array(vec![
                Frame::bulk("preferred_tier"),
                Frame::bulk(format!("{:?}", hints.preferred_tier)),
                Frame::bulk("use_index"),
                Frame::bulk(if hints.use_index { "true" } else { "false" }),
                Frame::bulk("enable_caching"),
                Frame::bulk(if hints.enable_caching {
                    "true"
                } else {
                    "false"
                }),
                Frame::bulk("batch_size"),
                Frame::Integer(hints.batch_size as i64),
                Frame::bulk("parallelism"),
                Frame::Integer(hints.parallelism as i64),
                Frame::bulk("memory_budget"),
                Frame::Integer(hints.memory_budget as i64),
                Frame::bulk("timeout_ms"),
                Frame::Integer(hints.timeout_ms as i64),
            ])
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle COST.STATS command
///
/// COST.STATS
pub fn handle_cost_stats(_ctx: &HandlerContext<'_>, _args: &[Bytes]) -> Frame {
    let stats = cost_optimizer().stats();

    Frame::array(vec![
        Frame::bulk("cache_entries"),
        Frame::Integer(stats.cache.entries as i64),
        Frame::bulk("cache_total_hits"),
        Frame::Integer(stats.cache.total_hits as i64),
        Frame::bulk("cache_avg_hits"),
        Frame::bulk(format!("{:.2}", stats.cache.avg_hits)),
        Frame::bulk("default_strategy"),
        Frame::bulk(format!("{:?}", stats.default_strategy)),
        Frame::bulk("budget"),
        Frame::bulk(format!("{:.4}", stats.budget)),
    ])
}

/// Handle COST.CACHE.CLEAR command
///
/// COST.CACHE.CLEAR
pub fn handle_cost_cache_clear(_ctx: &HandlerContext<'_>, _args: &[Bytes]) -> Frame {
    cost_optimizer().clear_cache();
    ok_frame()
}

/// Handle COST.BUDGET command
///
/// COST.BUDGET [amount]
pub fn handle_cost_budget(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        // Return current budget
        let config = cost_optimizer().config();
        Frame::bulk(format!("{:.4}", config.max_cost_budget))
    } else {
        // Note: In a real implementation, we'd need mutable access to set budget
        // For now, just return the requested value
        let budget_str = String::from_utf8_lossy(&args[0]);
        match budget_str.parse::<f64>() {
            Ok(budget) => Frame::bulk(format!("Budget would be set to: {:.4}", budget)),
            Err(_) => err_frame("invalid budget amount"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optimizer_singleton() {
        let o1 = cost_optimizer();
        let o2 = cost_optimizer();
        assert!(std::ptr::eq(o1, o2));
    }
}
