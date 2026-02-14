//! Optimization strategy selection

use serde::{Deserialize, Serialize};

/// Optimization strategy
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub enum OptimizationStrategy {
    /// Minimize cost regardless of latency
    MinCost,
    /// Minimize latency regardless of cost
    MinLatency,
    /// Balance cost and latency
    #[default]
    Balanced,
    /// Custom constraints
    Custom(CostConstraint),
}


/// Cost constraints for custom strategy
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct CostConstraint {
    /// Maximum total cost ($)
    pub max_cost: Option<f64>,
    /// Maximum latency (ms)
    pub max_latency: Option<f64>,
    /// Cost weight (0-1)
    pub cost_weight: f64,
    /// Latency weight (0-1)
    pub latency_weight: f64,
}

impl Default for CostConstraint {
    fn default() -> Self {
        Self {
            max_cost: None,
            max_latency: None,
            cost_weight: 0.5,
            latency_weight: 0.5,
        }
    }
}

impl CostConstraint {
    /// Create a new constraint
    pub fn new() -> Self {
        Self::default()
    }

    /// Set maximum cost
    pub fn with_max_cost(mut self, cost: f64) -> Self {
        self.max_cost = Some(cost);
        self
    }

    /// Set maximum latency
    pub fn with_max_latency(mut self, latency: f64) -> Self {
        self.max_latency = Some(latency);
        self
    }

    /// Set weights (will be normalized)
    pub fn with_weights(mut self, cost: f64, latency: f64) -> Self {
        let total = cost + latency;
        self.cost_weight = cost / total;
        self.latency_weight = latency / total;
        self
    }
}

/// Strategy selector based on query characteristics
pub struct StrategySelector {
    /// Default strategy
    default_strategy: OptimizationStrategy,
    /// Query pattern rules
    rules: Vec<StrategyRule>,
}

/// A rule for strategy selection
#[derive(Debug, Clone)]
struct StrategyRule {
    /// Pattern to match in query
    pattern: String,
    /// Strategy to use
    strategy: OptimizationStrategy,
    /// Priority (higher = more important)
    priority: i32,
}

impl StrategySelector {
    /// Create a new strategy selector
    pub fn new(default_strategy: OptimizationStrategy) -> Self {
        let rules = vec![
            // Real-time queries should prioritize latency
            StrategyRule {
                pattern: "SUBSCRIBE".to_string(),
                strategy: OptimizationStrategy::MinLatency,
                priority: 100,
            },
            // Batch queries should prioritize cost
            StrategyRule {
                pattern: "BATCH".to_string(),
                strategy: OptimizationStrategy::MinCost,
                priority: 90,
            },
            // Analytics queries can be balanced
            StrategyRule {
                pattern: "AGGREGATE".to_string(),
                strategy: OptimizationStrategy::Balanced,
                priority: 80,
            },
            // Large scans should minimize cost
            StrategyRule {
                pattern: "SCAN".to_string(),
                strategy: OptimizationStrategy::MinCost,
                priority: 70,
            },
        ];

        Self {
            default_strategy,
            rules,
        }
    }

    /// Select strategy for a query
    pub fn select(&self, query: &str) -> OptimizationStrategy {
        let query_upper = query.to_uppercase();

        // Find matching rule with highest priority
        self.rules
            .iter()
            .filter(|r| query_upper.contains(&r.pattern))
            .max_by_key(|r| r.priority)
            .map(|r| r.strategy)
            .unwrap_or(self.default_strategy)
    }

    /// Add a custom rule
    pub fn add_rule(&mut self, pattern: &str, strategy: OptimizationStrategy, priority: i32) {
        self.rules.push(StrategyRule {
            pattern: pattern.to_uppercase(),
            strategy,
            priority,
        });
    }

    /// Set default strategy
    pub fn set_default(&mut self, strategy: OptimizationStrategy) {
        self.default_strategy = strategy;
    }
}

impl Default for StrategySelector {
    fn default() -> Self {
        Self::new(OptimizationStrategy::Balanced)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strategy_selection() {
        let selector = StrategySelector::default();

        // Default should be balanced
        let strategy = selector.select("SELECT * FROM users WHERE id = 1");
        assert_eq!(strategy, OptimizationStrategy::Balanced);

        // Batch should use min cost
        let strategy = selector.select("BATCH INSERT INTO logs");
        assert_eq!(strategy, OptimizationStrategy::MinCost);

        // Subscribe should use min latency
        let strategy = selector.select("SUBSCRIBE to:channel");
        assert_eq!(strategy, OptimizationStrategy::MinLatency);
    }

    #[test]
    fn test_cost_constraint() {
        let constraint = CostConstraint::new()
            .with_max_cost(1.0)
            .with_max_latency(100.0)
            .with_weights(0.7, 0.3);

        assert_eq!(constraint.max_cost, Some(1.0));
        assert!((constraint.cost_weight - 0.7).abs() < 0.001);
    }
}
