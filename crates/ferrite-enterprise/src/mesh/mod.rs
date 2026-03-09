//! Data Mesh Gateway
//!
//! Provides a unified gateway for accessing heterogeneous data sources
//! through virtual namespaces, data contracts, and cross-source query routing.
//!
//! The [`MeshOrchestrator`] ties the gateway, contract registry, and query
//! router together, adding service discovery, circuit-breaker health checks,
//! and lifecycle management.

pub mod contract;
pub mod datasource;
pub mod gateway;
pub mod query_router;

pub use contract::{ContractRegistry, ContractSla, ContractStatus, DataContract};
pub use datasource::{
    DataSchema, DataSourceConfig, DataSourceStatus, DataSourceType, FieldType, SchemaField,
};
pub use gateway::{DataMeshGateway, GatewayConfig, GatewayStats};
pub use query_router::{QueryPlan, QueryRouter, QueryStep, StepType};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

// ── Circuit breaker ──────────────────────────────────────────────────────

/// Tracks consecutive failures for a data source and opens the circuit when
/// a threshold is exceeded.
#[derive(Debug)]
pub struct CircuitBreaker {
    threshold: u32,
    consecutive_failures: std::sync::atomic::AtomicU32,
    open: AtomicBool,
    half_open_after_ms: u64,
    last_failure_epoch_ms: std::sync::atomic::AtomicU64,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with the given failure threshold.
    pub fn new(threshold: u32, half_open_after_ms: u64) -> Self {
        Self {
            threshold,
            consecutive_failures: std::sync::atomic::AtomicU32::new(0),
            open: AtomicBool::new(false),
            half_open_after_ms,
            last_failure_epoch_ms: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Record a successful probe — resets the failure counter and closes the
    /// circuit.
    pub fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.open.store(false, Ordering::Relaxed);
    }

    /// Record a failure. Opens the circuit if the threshold is reached.
    pub fn record_failure(&self) {
        let prev = self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
        if prev + 1 >= self.threshold {
            self.open.store(true, Ordering::Relaxed);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            self.last_failure_epoch_ms.store(now, Ordering::Relaxed);
        }
    }

    /// Returns `true` when the circuit is open (source should be skipped).
    /// Transitions to *half-open* after `half_open_after_ms`, allowing a
    /// single probe through.
    pub fn is_open(&self) -> bool {
        if !self.open.load(Ordering::Relaxed) {
            return false;
        }
        let last = self.last_failure_epoch_ms.load(Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        // If enough time has elapsed, allow a probe (half-open).
        if now.saturating_sub(last) >= self.half_open_after_ms {
            return false;
        }
        true
    }
}

// ── Orchestrator ─────────────────────────────────────────────────────────

/// Configuration for the [`MeshOrchestrator`].
#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    /// Number of consecutive failures before a circuit breaker opens.
    pub circuit_breaker_threshold: u32,
    /// Milliseconds before an open circuit transitions to half-open.
    pub circuit_breaker_cooldown_ms: u64,
    /// Whether to automatically skip sources with open circuit breakers
    /// during query routing.
    pub skip_unhealthy_sources: bool,
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self {
            circuit_breaker_threshold: 3,
            circuit_breaker_cooldown_ms: 30_000,
            skip_unhealthy_sources: true,
        }
    }
}

/// Unified orchestrator for the data mesh.
///
/// Coordinates the [`DataMeshGateway`], [`ContractRegistry`], and
/// [`QueryRouter`], adding circuit-breaker tracking per data source and
/// orchestrated health-check sweeps.
pub struct MeshOrchestrator {
    gateway: Arc<DataMeshGateway>,
    contracts: ContractRegistry,
    router: QueryRouter,
    breakers: dashmap::DashMap<String, Arc<CircuitBreaker>>,
    config: OrchestratorConfig,
}

impl MeshOrchestrator {
    /// Create an orchestrator wrapping the given gateway.
    pub fn new(gateway_config: GatewayConfig, orch_config: OrchestratorConfig) -> Self {
        let gateway = Arc::new(DataMeshGateway::new(gateway_config));
        let router = QueryRouter::new(Arc::clone(&gateway));
        Self {
            gateway,
            contracts: ContractRegistry::new(),
            router,
            breakers: dashmap::DashMap::new(),
            config: orch_config,
        }
    }

    /// Create with all default settings.
    pub fn with_defaults() -> Self {
        Self::new(GatewayConfig::default(), OrchestratorConfig::default())
    }

    // ── Source lifecycle ──────────────────────────────────────────────

    /// Register a data source and create a circuit breaker for it.
    pub fn register_source(&self, config: DataSourceConfig) -> Result<(), String> {
        let id = config.id.clone();
        self.gateway.add_source(config)?;
        self.breakers.insert(
            id,
            Arc::new(CircuitBreaker::new(
                self.config.circuit_breaker_threshold,
                self.config.circuit_breaker_cooldown_ms,
            )),
        );
        Ok(())
    }

    /// Deregister a data source and remove its circuit breaker.
    pub fn deregister_source(&self, id: &str) -> Result<(), String> {
        self.gateway.remove_source(id)?;
        self.breakers.remove(id);
        Ok(())
    }

    // ── Health checks ────────────────────────────────────────────────

    /// Run a health check on a single source and update its circuit breaker.
    pub fn probe_source(&self, id: &str) -> Result<gateway::HealthCheckResult, String> {
        let result = self.gateway.health_check(id);
        if let Some(breaker) = self.breakers.get(id) {
            match &result {
                Ok(r) if r.healthy => breaker.record_success(),
                _ => breaker.record_failure(),
            }
        }
        result
    }

    /// Sweep all registered sources and return health results.
    pub fn probe_all(&self) -> Vec<gateway::HealthCheckResult> {
        self.gateway
            .list_sources()
            .iter()
            .filter_map(|src| self.probe_source(&src.id).ok())
            .collect()
    }

    /// Returns source ids whose circuit breakers are currently open.
    pub fn unhealthy_sources(&self) -> Vec<String> {
        self.breakers
            .iter()
            .filter(|entry| entry.value().is_open())
            .map(|entry| entry.key().clone())
            .collect()
    }

    // ── Query routing ────────────────────────────────────────────────

    /// Route a query, optionally filtering out sources with open circuit
    /// breakers.
    pub fn route_query(&self, query: &str) -> Result<QueryPlan, String> {
        let mut plan = self.router.route_query(query)?;

        if self.config.skip_unhealthy_sources {
            let unhealthy: std::collections::HashSet<String> =
                self.unhealthy_sources().into_iter().collect();
            if !unhealthy.is_empty() {
                plan.steps
                    .retain(|step| !unhealthy.contains(&step.source_id));
                plan.sources_involved.retain(|id| !unhealthy.contains(id));
                plan.estimated_latency_ms = 10 * plan.steps.len() as u64;
            }
        }

        Ok(plan)
    }

    // ── Delegation helpers ───────────────────────────────────────────

    /// Access the underlying gateway.
    pub fn gateway(&self) -> &DataMeshGateway {
        &self.gateway
    }

    /// Access the contract registry.
    pub fn contracts(&self) -> &ContractRegistry {
        &self.contracts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_source(id: &str) -> DataSourceConfig {
        DataSourceConfig::new(
            id.to_string(),
            format!("Test {id}"),
            DataSourceType::Postgres,
            "postgres://localhost/db".into(),
        )
    }

    #[test]
    fn orchestrator_register_and_probe() {
        let orch = MeshOrchestrator::with_defaults();
        orch.register_source(test_source("pg1")).unwrap();
        let result = orch.probe_source("pg1").unwrap();
        assert!(result.healthy);
        assert!(orch.unhealthy_sources().is_empty());
    }

    #[test]
    fn orchestrator_deregister() {
        let orch = MeshOrchestrator::with_defaults();
        orch.register_source(test_source("pg1")).unwrap();
        orch.deregister_source("pg1").unwrap();
        assert!(orch.gateway().list_sources().is_empty());
    }

    #[test]
    fn circuit_breaker_opens_after_threshold() {
        let cb = CircuitBreaker::new(3, 60_000);
        assert!(!cb.is_open());
        cb.record_failure();
        cb.record_failure();
        assert!(!cb.is_open());
        cb.record_failure();
        assert!(cb.is_open());
    }

    #[test]
    fn circuit_breaker_resets_on_success() {
        let cb = CircuitBreaker::new(2, 60_000);
        cb.record_failure();
        cb.record_failure();
        assert!(cb.is_open());
        cb.record_success();
        assert!(!cb.is_open());
    }

    #[test]
    fn orchestrator_route_query() {
        let orch = MeshOrchestrator::with_defaults();
        orch.register_source(test_source("pg1")).unwrap();
        orch.gateway().add_namespace("users", "pg1").unwrap();
        let plan = orch.route_query("SELECT * FROM users").unwrap();
        assert!(!plan.steps.is_empty());
    }

    #[test]
    fn probe_all_sweeps_sources() {
        let orch = MeshOrchestrator::with_defaults();
        orch.register_source(test_source("pg1")).unwrap();
        orch.register_source(test_source("pg2")).unwrap();
        let results = orch.probe_all();
        assert_eq!(results.len(), 2);
    }
}
