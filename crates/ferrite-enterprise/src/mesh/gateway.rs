//! Data Mesh Gateway — central registry for sources, namespaces and schemas.

#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicU64, Ordering};

use chrono::Utc;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use super::contract::DataContract;
use super::datasource::{DataSchema, DataSourceConfig, DataSourceStatus};

/// Result of a health-check probe against a data source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResult {
    pub source_id: String,
    pub healthy: bool,
    pub latency_ms: u64,
    pub message: String,
}

/// Tunables for the gateway.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayConfig {
    pub max_sources: usize,
    pub default_timeout_ms: u64,
    pub cache_query_results: bool,
    pub cache_ttl_secs: u64,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            max_sources: 64,
            default_timeout_ms: 5000,
            cache_query_results: true,
            cache_ttl_secs: 300,
        }
    }
}

/// Counters exposed via `FEDERATION.STATS`.
#[derive(Debug, Serialize, Deserialize)]
pub struct GatewayStats {
    pub sources_active: u64,
    pub queries_routed: AtomicU64,
    pub errors: AtomicU64,
    pub namespaces_registered: u64,
    pub contracts_active: u64,
}

impl Default for GatewayStats {
    fn default() -> Self {
        Self {
            sources_active: 0,
            queries_routed: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            namespaces_registered: 0,
            contracts_active: 0,
        }
    }
}

/// Thread-safe data mesh gateway backed by [`DashMap`].
pub struct DataMeshGateway {
    sources: DashMap<String, DataSourceConfig>,
    namespaces: DashMap<String, String>,
    schemas: DashMap<String, DataSchema>,
    contracts: DashMap<String, DataContract>,
    config: GatewayConfig,
    queries_routed: AtomicU64,
    errors: AtomicU64,
}

impl DataMeshGateway {
    /// Create a gateway with the given configuration.
    pub fn new(config: GatewayConfig) -> Self {
        Self {
            sources: DashMap::new(),
            namespaces: DashMap::new(),
            schemas: DashMap::new(),
            contracts: DashMap::new(),
            config,
            queries_routed: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }

    /// Create a gateway with default settings.
    pub fn with_defaults() -> Self {
        Self::new(GatewayConfig::default())
    }

    // ── Source management ─────────────────────────────────────────────

    /// Register a new data source.
    pub fn add_source(&self, config: DataSourceConfig) -> Result<(), String> {
        if self.sources.len() >= self.config.max_sources {
            return Err(format!(
                "maximum number of sources ({}) reached",
                self.config.max_sources
            ));
        }
        if self.sources.contains_key(&config.id) {
            return Err(format!("source '{}' already exists", config.id));
        }
        self.sources.insert(config.id.clone(), config);
        Ok(())
    }

    /// Remove a data source by id.
    pub fn remove_source(&self, id: &str) -> Result<(), String> {
        self.sources
            .remove(id)
            .map(|_| ())
            .ok_or_else(|| format!("source '{id}' not found"))
    }

    /// List all registered data sources.
    pub fn list_sources(&self) -> Vec<DataSourceConfig> {
        self.sources.iter().map(|r| r.value().clone()).collect()
    }

    /// Get a single data source by id.
    pub fn get_source(&self, id: &str) -> Option<DataSourceConfig> {
        self.sources.get(id).map(|r| r.value().clone())
    }

    /// Perform a health check against a data source (simulated).
    pub fn health_check(&self, id: &str) -> Result<HealthCheckResult, String> {
        let mut src = self
            .sources
            .get_mut(id)
            .ok_or_else(|| format!("source '{id}' not found"))?;

        src.last_health_check = Some(Utc::now());
        src.status = DataSourceStatus::Connected;

        Ok(HealthCheckResult {
            source_id: id.to_string(),
            healthy: true,
            latency_ms: 1,
            message: "OK".to_string(),
        })
    }

    // ── Namespace management ──────────────────────────────────────────

    /// Map a virtual namespace to a source id.
    pub fn add_namespace(&self, namespace: &str, source_id: &str) -> Result<(), String> {
        if !self.sources.contains_key(source_id) {
            return Err(format!("source '{source_id}' not found"));
        }
        self.namespaces
            .insert(namespace.to_string(), source_id.to_string());
        Ok(())
    }

    /// Resolve a namespace to its source id.
    pub fn resolve_namespace(&self, namespace: &str) -> Option<String> {
        self.namespaces.get(namespace).map(|r| r.value().clone())
    }

    /// List all (namespace, source_id) pairs.
    pub fn list_namespaces(&self) -> Vec<(String, String)> {
        self.namespaces
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect()
    }

    // ── Schema management ─────────────────────────────────────────────

    /// Store a schema under the given key.
    pub fn add_schema(&self, key: &str, schema: DataSchema) {
        self.schemas.insert(key.to_string(), schema);
    }

    // ── Contract management (delegated to ContractRegistry) ──────────

    /// Register a data contract.
    pub fn add_contract(&self, contract: DataContract) -> Result<(), String> {
        if self.contracts.contains_key(&contract.name) {
            return Err(format!("contract '{}' already exists", contract.name));
        }
        self.contracts.insert(contract.name.clone(), contract);
        Ok(())
    }

    /// List all contracts.
    pub fn list_contracts(&self) -> Vec<DataContract> {
        self.contracts.iter().map(|r| r.value().clone()).collect()
    }

    // ── Stats ─────────────────────────────────────────────────────────

    /// Record a routed query.
    pub fn record_query(&self) {
        self.queries_routed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an error.
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot current gateway statistics.
    pub fn stats(&self) -> GatewayStats {
        GatewayStats {
            sources_active: self.sources.len() as u64,
            queries_routed: AtomicU64::new(self.queries_routed.load(Ordering::Relaxed)),
            errors: AtomicU64::new(self.errors.load(Ordering::Relaxed)),
            namespaces_registered: self.namespaces.len() as u64,
            contracts_active: self.contracts.len() as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mesh::datasource::{DataSourceConfig, DataSourceType};

    fn test_source(id: &str) -> DataSourceConfig {
        DataSourceConfig::new(
            id.to_string(),
            format!("Test {id}"),
            DataSourceType::Postgres,
            "postgres://localhost/db".into(),
        )
    }

    #[test]
    fn test_add_and_list_sources() {
        let gw = DataMeshGateway::with_defaults();
        gw.add_source(test_source("pg1")).unwrap();
        gw.add_source(test_source("pg2")).unwrap();
        assert_eq!(gw.list_sources().len(), 2);
    }

    #[test]
    fn test_duplicate_source_rejected() {
        let gw = DataMeshGateway::with_defaults();
        gw.add_source(test_source("pg1")).unwrap();
        assert!(gw.add_source(test_source("pg1")).is_err());
    }

    #[test]
    fn test_remove_source() {
        let gw = DataMeshGateway::with_defaults();
        gw.add_source(test_source("pg1")).unwrap();
        gw.remove_source("pg1").unwrap();
        assert!(gw.list_sources().is_empty());
    }

    #[test]
    fn test_namespace_management() {
        let gw = DataMeshGateway::with_defaults();
        gw.add_source(test_source("pg1")).unwrap();
        gw.add_namespace("users", "pg1").unwrap();
        assert_eq!(gw.resolve_namespace("users"), Some("pg1".to_string()));
        assert_eq!(gw.list_namespaces().len(), 1);
    }

    #[test]
    fn test_namespace_requires_source() {
        let gw = DataMeshGateway::with_defaults();
        assert!(gw.add_namespace("ns", "missing").is_err());
    }

    #[test]
    fn test_health_check() {
        let gw = DataMeshGateway::with_defaults();
        gw.add_source(test_source("pg1")).unwrap();
        let result = gw.health_check("pg1").unwrap();
        assert!(result.healthy);
    }

    #[test]
    fn test_stats() {
        let gw = DataMeshGateway::with_defaults();
        gw.add_source(test_source("pg1")).unwrap();
        gw.record_query();
        gw.record_query();
        gw.record_error();
        let s = gw.stats();
        assert_eq!(s.sources_active, 1);
        assert_eq!(s.queries_routed.load(Ordering::Relaxed), 2);
        assert_eq!(s.errors.load(Ordering::Relaxed), 1);
    }
}
