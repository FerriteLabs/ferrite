//! Scatter-gather execution for federated queries.
//!
//! Sends sub-queries to multiple nodes in parallel and merges results.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Result from a single node in a scatter-gather operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeResult {
    pub node_id: String,
    pub success: bool,
    pub data: Vec<serde_json::Value>,
    pub row_count: usize,
    pub latency_ms: u64,
    pub error: Option<String>,
}

/// Merged result from all nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergedResult {
    pub total_rows: usize,
    pub nodes_queried: usize,
    pub nodes_responded: usize,
    pub data: Vec<serde_json::Value>,
    pub total_latency_ms: u64,
    pub partial: bool,
    pub errors: Vec<String>,
}

/// Merges results from multiple nodes using concatenation.
pub fn merge_concatenate(results: &[NodeResult]) -> MergedResult {
    let mut data = Vec::new();
    let mut errors = Vec::new();
    let nodes_responded = results.iter().filter(|r| r.success).count();
    let max_latency = results.iter().map(|r| r.latency_ms).max().unwrap_or(0);

    for result in results {
        if result.success {
            data.extend(result.data.clone());
        } else if let Some(err) = &result.error {
            errors.push(format!("{}: {}", result.node_id, err));
        }
    }

    let total_rows = data.len();

    MergedResult {
        total_rows,
        nodes_queried: results.len(),
        nodes_responded,
        data,
        total_latency_ms: max_latency,
        partial: nodes_responded < results.len(),
        errors,
    }
}

/// Merges results with deduplication by a key field.
pub fn merge_union(results: &[NodeResult], dedup_key: &str) -> MergedResult {
    let mut seen = HashMap::new();
    let mut data = Vec::new();
    let mut errors = Vec::new();
    let nodes_responded = results.iter().filter(|r| r.success).count();
    let max_latency = results.iter().map(|r| r.latency_ms).max().unwrap_or(0);

    for result in results {
        if result.success {
            for item in &result.data {
                if let Some(key) = item.get(dedup_key).and_then(|v| v.as_str()) {
                    if !seen.contains_key(key) {
                        seen.insert(key.to_string(), true);
                        data.push(item.clone());
                    }
                } else {
                    data.push(item.clone());
                }
            }
        } else if let Some(err) = &result.error {
            errors.push(format!("{}: {}", result.node_id, err));
        }
    }

    MergedResult {
        total_rows: data.len(),
        nodes_queried: results.len(),
        nodes_responded,
        data,
        total_latency_ms: max_latency,
        partial: nodes_responded < results.len(),
        errors,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_concatenate() {
        let results = vec![
            NodeResult {
                node_id: "n1".to_string(),
                success: true,
                data: vec![serde_json::json!({"id": 1}), serde_json::json!({"id": 2})],
                row_count: 2,
                latency_ms: 50,
                error: None,
            },
            NodeResult {
                node_id: "n2".to_string(),
                success: true,
                data: vec![serde_json::json!({"id": 3})],
                row_count: 1,
                latency_ms: 80,
                error: None,
            },
        ];

        let merged = merge_concatenate(&results);
        assert_eq!(merged.total_rows, 3);
        assert_eq!(merged.nodes_responded, 2);
        assert!(!merged.partial);
    }

    #[test]
    fn test_merge_with_failures() {
        let results = vec![
            NodeResult {
                node_id: "n1".to_string(),
                success: true,
                data: vec![serde_json::json!({"id": 1})],
                row_count: 1,
                latency_ms: 50,
                error: None,
            },
            NodeResult {
                node_id: "n2".to_string(),
                success: false,
                data: vec![],
                row_count: 0,
                latency_ms: 100,
                error: Some("timeout".to_string()),
            },
        ];

        let merged = merge_concatenate(&results);
        assert_eq!(merged.total_rows, 1);
        assert_eq!(merged.nodes_responded, 1);
        assert!(merged.partial);
        assert_eq!(merged.errors.len(), 1);
    }

    #[test]
    fn test_merge_union_dedup() {
        let results = vec![
            NodeResult {
                node_id: "n1".to_string(),
                success: true,
                data: vec![serde_json::json!({"id": "a", "val": 1})],
                row_count: 1,
                latency_ms: 50,
                error: None,
            },
            NodeResult {
                node_id: "n2".to_string(),
                success: true,
                data: vec![
                    serde_json::json!({"id": "a", "val": 2}),
                    serde_json::json!({"id": "b", "val": 3}),
                ],
                row_count: 2,
                latency_ms: 80,
                error: None,
            },
        ];

        let merged = merge_union(&results, "id");
        assert_eq!(merged.total_rows, 2); // "a" deduplicated
    }
}
