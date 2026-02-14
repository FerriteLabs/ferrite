//! Query planning for federated queries.

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

/// Generates a federated query plan from a query string.
pub fn generate_plan(query: &str, node_ids: &[String], hint: PlanHint) -> Vec<PlanStep> {
    let mut steps = Vec::new();
    let mut step_id = 0u32;

    // Generate remote scan for each node
    let scan_ids: Vec<u32> = node_ids
        .iter()
        .map(|node_id| {
            let id = step_id;
            steps.push(PlanStep {
                step_id: id,
                node_id: Some(node_id.clone()),
                operation: PlanOperation::RemoteScan {
                    pattern: query.to_string(),
                },
                depends_on: vec![],
            });
            step_id += 1;
            id
        })
        .collect();

    // Merge step
    steps.push(PlanStep {
        step_id,
        node_id: None,
        operation: PlanOperation::Merge {
            strategy: super::MergeStrategy::Concatenate,
        },
        depends_on: scan_ids,
    });

    steps
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_plan() {
        let nodes = vec!["n1".to_string(), "n2".to_string()];
        let plan = generate_plan("SELECT * FROM users:*", &nodes, PlanHint::Parallel);

        // 2 remote scans + 1 merge
        assert_eq!(plan.len(), 3);

        // Merge depends on both scans
        let merge = &plan[2];
        assert_eq!(merge.depends_on.len(), 2);
        assert!(merge.node_id.is_none()); // Local operation
    }

    #[test]
    fn test_empty_nodes() {
        let plan = generate_plan("query", &[], PlanHint::Parallel);
        assert_eq!(plan.len(), 1); // Just the merge step
    }
}
