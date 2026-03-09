//! Query planning for federated queries.
//!
//! Supports hint-aware plan generation with cost estimation. The planner
//! produces a DAG of [`PlanStep`]s that the executor walks in dependency
//! order.

use serde::{Deserialize, Serialize};

/// Optimization hint for federated query planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlanHint {
    /// Prefer locality (route to nearest node).
    PreferLocal,
    /// Distribute evenly across nodes.
    Distribute,
    /// Route to specific node.
    RouteToNode,
    /// Parallel execution on all nodes.
    Parallel,
}

/// A query plan step.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanStep {
    pub step_id: u32,
    pub node_id: Option<String>,
    pub operation: PlanOperation,
    pub depends_on: Vec<u32>,
    /// Estimated cost (arbitrary units; lower is better).
    pub estimated_cost: u64,
}

/// Operation in a query plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PlanOperation {
    /// Scan keys on a remote node.
    RemoteScan { pattern: String },
    /// Filter results locally.
    LocalFilter { predicate: String },
    /// Merge results from multiple steps.
    Merge { strategy: super::MergeStrategy },
    /// Aggregate partial results.
    Aggregate { function: String, field: String },
    /// Sort merged results.
    Sort { field: String, ascending: bool },
    /// Limit results.
    Limit { count: usize, offset: usize },
}

/// A complete federated query plan with cost metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederatedPlan {
    pub steps: Vec<PlanStep>,
    pub hint_used: PlanHint,
    pub total_estimated_cost: u64,
    pub nodes_involved: usize,
}

// ── Cost model ────────────────────────────────────────────────────────

/// Simple cost constants for the planner.
const COST_REMOTE_SCAN: u64 = 100;
const COST_MERGE: u64 = 10;
const COST_FILTER: u64 = 5;
const COST_SORT: u64 = 20;
const COST_AGGREGATE: u64 = 15;

// ── Plan generation ───────────────────────────────────────────────────

/// Generates a federated query plan from a query string.
///
/// The `hint` parameter controls how work is distributed across nodes:
///
/// - [`PlanHint::Parallel`] — scatter to all nodes, merge results.
/// - [`PlanHint::PreferLocal`] — scan only the first (local) node; fan out
///   only if the local node is empty.
/// - [`PlanHint::Distribute`] — round-robin partition of the scan across
///   nodes (each node gets a slice).
/// - [`PlanHint::RouteToNode`] — route to the first node only.
pub fn generate_plan(query: &str, node_ids: &[String], hint: PlanHint) -> Vec<PlanStep> {
    let plan = generate_federated_plan(query, node_ids, hint);
    plan.steps
}

/// Generates a [`FederatedPlan`] with full cost metadata.
pub fn generate_federated_plan(query: &str, node_ids: &[String], hint: PlanHint) -> FederatedPlan {
    let mut steps = Vec::new();
    let mut step_id = 0u32;

    let scan_nodes: Vec<&String> = match hint {
        PlanHint::Parallel => node_ids.iter().collect(),
        PlanHint::PreferLocal | PlanHint::RouteToNode => {
            // Use only the first node (local / target).
            node_ids.iter().take(1).collect()
        }
        PlanHint::Distribute => {
            // All nodes, but the executor should shard the keyspace.
            node_ids.iter().collect()
        }
    };

    // Generate remote scan for each selected node.
    let scan_ids: Vec<u32> = scan_nodes
        .iter()
        .map(|node_id| {
            let id = step_id;
            steps.push(PlanStep {
                step_id: id,
                node_id: Some((*node_id).clone()),
                operation: PlanOperation::RemoteScan {
                    pattern: query.to_string(),
                },
                depends_on: vec![],
                estimated_cost: COST_REMOTE_SCAN,
            });
            step_id += 1;
            id
        })
        .collect();

    // Merge step (only useful when >1 scan).
    let merge_cost = if scan_ids.len() > 1 {
        COST_MERGE * scan_ids.len() as u64
    } else {
        COST_MERGE
    };

    steps.push(PlanStep {
        step_id,
        node_id: None,
        operation: PlanOperation::Merge {
            strategy: super::MergeStrategy::Concatenate,
        },
        depends_on: scan_ids.clone(),
        estimated_cost: merge_cost,
    });

    let total_cost: u64 = steps.iter().map(|s| s.estimated_cost).sum();

    FederatedPlan {
        steps,
        hint_used: hint,
        total_estimated_cost: total_cost,
        nodes_involved: scan_nodes.len(),
    }
}

/// Append a filter step to an existing plan. Returns the new step id.
pub fn append_filter(steps: &mut Vec<PlanStep>, predicate: &str, depends_on: u32) -> u32 {
    let id = steps.iter().map(|s| s.step_id).max().unwrap_or(0) + 1;
    steps.push(PlanStep {
        step_id: id,
        node_id: None,
        operation: PlanOperation::LocalFilter {
            predicate: predicate.to_string(),
        },
        depends_on: vec![depends_on],
        estimated_cost: COST_FILTER,
    });
    id
}

/// Append a sort step to an existing plan. Returns the new step id.
pub fn append_sort(
    steps: &mut Vec<PlanStep>,
    field: &str,
    ascending: bool,
    depends_on: u32,
) -> u32 {
    let id = steps.iter().map(|s| s.step_id).max().unwrap_or(0) + 1;
    steps.push(PlanStep {
        step_id: id,
        node_id: None,
        operation: PlanOperation::Sort {
            field: field.to_string(),
            ascending,
        },
        depends_on: vec![depends_on],
        estimated_cost: COST_SORT,
    });
    id
}

/// Append an aggregate step to an existing plan. Returns the new step id.
pub fn append_aggregate(
    steps: &mut Vec<PlanStep>,
    function: &str,
    field: &str,
    depends_on: u32,
) -> u32 {
    let id = steps.iter().map(|s| s.step_id).max().unwrap_or(0) + 1;
    steps.push(PlanStep {
        step_id: id,
        node_id: None,
        operation: PlanOperation::Aggregate {
            function: function.to_string(),
            field: field.to_string(),
        },
        depends_on: vec![depends_on],
        estimated_cost: COST_AGGREGATE,
    });
    id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_plan_parallel() {
        let nodes = vec!["n1".to_string(), "n2".to_string()];
        let plan = generate_plan("SELECT * FROM users:*", &nodes, PlanHint::Parallel);

        // 2 remote scans + 1 merge
        assert_eq!(plan.len(), 3);

        // Merge depends on both scans
        let merge = &plan[2];
        assert_eq!(merge.depends_on.len(), 2);
        assert!(merge.node_id.is_none());
    }

    #[test]
    fn test_generate_plan_prefer_local() {
        let nodes = vec!["local".to_string(), "remote".to_string()];
        let plan = generate_federated_plan("query", &nodes, PlanHint::PreferLocal);

        // 1 scan (local only) + 1 merge
        assert_eq!(plan.steps.len(), 2);
        assert_eq!(plan.nodes_involved, 1);
        assert_eq!(plan.steps[0].node_id.as_deref(), Some("local"));
    }

    #[test]
    fn test_generate_plan_route_to_node() {
        let nodes = vec!["target".to_string(), "other".to_string()];
        let plan = generate_federated_plan("query", &nodes, PlanHint::RouteToNode);

        assert_eq!(plan.nodes_involved, 1);
        assert_eq!(plan.hint_used, PlanHint::RouteToNode);
    }

    #[test]
    fn test_cost_estimation() {
        let nodes = vec!["n1".to_string(), "n2".to_string(), "n3".to_string()];
        let plan = generate_federated_plan("query", &nodes, PlanHint::Parallel);

        // 3 scans × 100 + 1 merge × 30 = 330
        assert_eq!(
            plan.total_estimated_cost,
            3 * COST_REMOTE_SCAN + 3 * COST_MERGE
        );
    }

    #[test]
    fn test_empty_nodes() {
        let plan = generate_plan("query", &[], PlanHint::Parallel);
        assert_eq!(plan.len(), 1); // Just the merge step
    }

    #[test]
    fn test_append_filter() {
        let nodes = vec!["n1".to_string()];
        let mut steps = generate_plan("query", &nodes, PlanHint::Parallel);
        let merge_id = steps.last().map(|s| s.step_id).unwrap_or(0);
        let filter_id = append_filter(&mut steps, "age > 21", merge_id);
        assert_eq!(steps.len(), 3);
        assert_eq!(steps.last().map(|s| s.step_id), Some(filter_id));
        assert_eq!(
            steps.last().map(|s| s.depends_on.clone()),
            Some(vec![merge_id])
        );
    }

    #[test]
    fn test_append_sort_and_aggregate() {
        let nodes = vec!["n1".to_string()];
        let mut steps = generate_plan("query", &nodes, PlanHint::Parallel);
        let merge_id = steps.last().map(|s| s.step_id).unwrap_or(0);
        let sort_id = append_sort(&mut steps, "name", true, merge_id);
        let _agg_id = append_aggregate(&mut steps, "COUNT", "id", sort_id);
        assert_eq!(steps.len(), 4);
    }
}
