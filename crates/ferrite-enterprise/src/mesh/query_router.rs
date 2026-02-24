//! Cross-source query routing for the Data Mesh Gateway.

#![forbid(unsafe_code)]

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::gateway::DataMeshGateway;

/// Type of operation within a query step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StepType {
    Scan,
    Filter,
    Join,
    Aggregate,
    Sort,
    Limit,
}

impl std::fmt::Display for StepType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scan => write!(f, "scan"),
            Self::Filter => write!(f, "filter"),
            Self::Join => write!(f, "join"),
            Self::Aggregate => write!(f, "aggregate"),
            Self::Sort => write!(f, "sort"),
            Self::Limit => write!(f, "limit"),
        }
    }
}

/// A single step in a query execution plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStep {
    pub source_id: String,
    pub query: String,
    pub step_type: StepType,
}

/// An execution plan describing how to satisfy a cross-source query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPlan {
    pub steps: Vec<QueryStep>,
    pub estimated_latency_ms: u64,
    pub sources_involved: Vec<String>,
}

/// Routes queries to the appropriate data sources via the gateway.
pub struct QueryRouter {
    gateway: Arc<DataMeshGateway>,
}

impl QueryRouter {
    pub fn new(gateway: Arc<DataMeshGateway>) -> Self {
        Self { gateway }
    }

    /// Analyse `query` and produce an execution plan.
    ///
    /// The current implementation performs simple namespace-prefix matching to
    /// determine which sources are involved. A production router would parse a
    /// full SQL/query AST.
    pub fn route_query(&self, query: &str) -> Result<QueryPlan, String> {
        if query.is_empty() {
            return Err("empty query".to_string());
        }

        self.gateway.record_query();

        let namespaces = self.gateway.list_namespaces();
        let mut steps = Vec::new();
        let mut sources_involved = Vec::new();

        // Simple heuristic: if the query mentions a known namespace, route to
        // the corresponding source.
        for (ns, source_id) in &namespaces {
            if query.contains(ns.as_str()) {
                steps.push(QueryStep {
                    source_id: source_id.clone(),
                    query: query.to_string(),
                    step_type: StepType::Scan,
                });
                if !sources_involved.contains(source_id) {
                    sources_involved.push(source_id.clone());
                }
            }
        }

        // Fallback: if no namespace matched, scan all sources.
        if steps.is_empty() {
            let sources = self.gateway.list_sources();
            for src in &sources {
                steps.push(QueryStep {
                    source_id: src.id.clone(),
                    query: query.to_string(),
                    step_type: StepType::Scan,
                });
                sources_involved.push(src.id.clone());
            }
        }

        let estimated_latency_ms = 10 * steps.len() as u64;

        Ok(QueryPlan {
            steps,
            estimated_latency_ms,
            sources_involved,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mesh::datasource::{DataSourceConfig, DataSourceType};

    fn make_gateway() -> Arc<DataMeshGateway> {
        let gw = DataMeshGateway::with_defaults();
        gw.add_source(DataSourceConfig::new(
            "pg1".into(),
            "Postgres".into(),
            DataSourceType::Postgres,
            "postgres://localhost/db".into(),
        ))
        .unwrap();
        gw.add_namespace("users", "pg1").unwrap();
        Arc::new(gw)
    }

    #[test]
    fn test_route_matching_namespace() {
        let gw = make_gateway();
        let router = QueryRouter::new(gw);
        let plan = router.route_query("SELECT * FROM users").unwrap();
        assert_eq!(plan.sources_involved, vec!["pg1".to_string()]);
        assert_eq!(plan.steps.len(), 1);
    }

    #[test]
    fn test_route_fallback_all_sources() {
        let gw = make_gateway();
        let router = QueryRouter::new(gw);
        let plan = router.route_query("SELECT 1").unwrap();
        assert!(!plan.sources_involved.is_empty());
    }

    #[test]
    fn test_route_empty_query_error() {
        let gw = make_gateway();
        let router = QueryRouter::new(gw);
        assert!(router.route_query("").is_err());
    }
}
