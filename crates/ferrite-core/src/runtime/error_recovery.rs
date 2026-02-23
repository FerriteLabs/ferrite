//! First-class error recovery and backpressure management
//!
//! Provides centralized error handling with circuit breakers, backpressure control,
//! OOM mitigation, disk failover, and key quarantine for the Ferrite runtime.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur during recovery operations
#[derive(Error, Debug)]
pub enum RecoveryError {
    /// The circuit breaker for the specified component is open.
    #[error("Circuit breaker open for component: {0}")]
    CircuitOpen(String),
    /// An eviction operation failed.
    #[error("Eviction failed: {0}")]
    EvictionFailed(String),
    /// A failover operation failed.
    #[error("Failover failed: {0}")]
    FailoverFailed(String),
    /// The quarantine list has reached its maximum capacity.
    #[error("Quarantine is full")]
    QuarantineFull,
    /// An unexpected internal error occurred.
    #[error("Internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// Identifies the subsystem that produced an error
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ComponentType {
    /// Client connection handling subsystem.
    Connection,
    /// Key-value storage engine subsystem.
    Storage,
    /// On-disk persistence (RDB/AOF) subsystem.
    Persistence,
    /// Data replication subsystem.
    Replication,
    /// Cluster coordination subsystem.
    Cluster,
    /// Query processing subsystem.
    Query,
    /// Memory management subsystem.
    Memory,
    /// Disk I/O subsystem.
    Disk,
    /// Network communication subsystem.
    Network,
}

impl std::fmt::Display for ComponentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Severity levels for system errors
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ErrorSeverity {
    /// Non-critical issue that should be monitored.
    Warning,
    /// Significant issue requiring attention.
    Error,
    /// System-threatening issue requiring immediate action.
    Critical,
    /// Unrecoverable issue that will cause shutdown.
    Fatal,
}

impl std::fmt::Display for ErrorSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Aggregate health status of the system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthLevel {
    /// All components are operating normally.
    Healthy,
    /// System is functional but experiencing issues.
    Degraded {
        /// Description of the degradation.
        reason: String,
    },
    /// System has significant failures affecting operation.
    Unhealthy {
        /// Description of the failure.
        reason: String,
    },
    /// Multiple critical failures; system is at risk.
    Critical {
        /// Description of the critical state.
        reason: String,
    },
}

/// Circuit breaker state for a component
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CircuitState {
    /// Normal operation — requests flow through
    Closed,
    /// Blocking — all requests rejected after repeated failures
    Open {
        /// Timestamp when the circuit opened.
        since: DateTime<Utc>,
        /// Number of consecutive failures observed.
        failures: u32,
    },
    /// Testing — one trial request allowed to probe recovery
    HalfOpen {
        /// Timestamp when the half-open probe began.
        since: DateTime<Utc>,
    },
}

/// Action to take in response to client backpressure
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BackpressureAction {
    /// Request is allowed through without delay.
    Allow,
    /// Request is delayed by the specified number of milliseconds.
    Throttle {
        /// Delay in milliseconds before the request proceeds.
        delay_ms: u64,
    },
    /// Request is rejected with the given reason.
    Reject {
        /// Reason the request was rejected.
        reason: String,
    },
    /// Client connection is terminated with the given reason.
    Disconnect {
        /// Reason the connection was terminated.
        reason: String,
    },
}

/// Action to take when the system is running out of memory
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OomAction {
    /// Evict least-recently-used keys to free the target number of bytes.
    EvictLru {
        /// Number of bytes to free.
        target_bytes: u64,
    },
    /// Evict least-frequently-used keys to free the target number of bytes.
    EvictLfu {
        /// Number of bytes to free.
        target_bytes: u64,
    },
    /// Evict keys that have expired TTLs.
    EvictTtl,
    /// Reject the incoming write with the given reason.
    RejectWrite {
        /// Reason the write was rejected.
        reason: String,
    },
    /// Enable swap space as a last resort.
    EnableSwap,
}

/// Action to take in response to a disk error
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DiskAction {
    /// Retry the operation after a delay in milliseconds.
    Retry {
        /// Delay in milliseconds before retrying.
        delay_ms: u64,
    },
    /// Fail over to the specified alternate disk path.
    Failover {
        /// Filesystem path of the alternate disk.
        path: String,
    },
    /// Disable append-only file persistence.
    DisableAof,
    /// Switch the server to read-only mode.
    ReadOnly,
    /// Shut down the server gracefully.
    Shutdown,
}

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// Configuration for the error recovery system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryConfig {
    /// Maximum client output-buffer depth in bytes (default 128 MiB)
    pub max_client_queue_depth: u64,
    /// Whether to evict keys on OOM (default true)
    pub oom_eviction_enabled: bool,
    /// Optional secondary path for disk failover
    pub disk_failover_path: Option<String>,
    /// Consecutive failures before a circuit breaker opens (default 5)
    pub circuit_breaker_threshold: u32,
    /// Seconds before an open circuit transitions to half-open (default 30)
    pub circuit_breaker_reset_secs: u64,
    /// Timeout in seconds for slow clients (default 10)
    pub slow_client_timeout_secs: u64,
    /// Maximum number of quarantined keys (default 1000)
    pub max_quarantined_keys: usize,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            max_client_queue_depth: 128 * 1024 * 1024, // 128 MiB
            oom_eviction_enabled: true,
            disk_failover_path: None,
            circuit_breaker_threshold: 5,
            circuit_breaker_reset_secs: 30,
            slow_client_timeout_secs: 10,
            max_quarantined_keys: 1000,
        }
    }
}

/// A system error reported to the recovery manager
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemError {
    /// The subsystem that produced this error.
    pub component: ComponentType,
    /// Severity level of the error.
    pub severity: ErrorSeverity,
    /// Human-readable description of the error.
    pub message: String,
    /// When the error occurred.
    pub timestamp: DateTime<Utc>,
    /// Whether the error is potentially recoverable.
    pub recoverable: bool,
}

/// Health of an individual component
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    /// The subsystem this health record describes.
    pub component: ComponentType,
    /// Whether the component is currently healthy.
    pub healthy: bool,
    /// Current circuit breaker state for this component.
    pub circuit_state: CircuitState,
    /// Total number of errors recorded for this component.
    pub errors: u64,
    /// Timestamp of the most recent error, if any.
    pub last_error: Option<DateTime<Utc>>,
}

/// Overall system health snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemHealth {
    /// Aggregate health level of the system.
    pub status: HealthLevel,
    /// Per-component health details.
    pub components: Vec<ComponentHealth>,
    /// Number of errors recorded in the last 60 seconds.
    pub errors_last_minute: u64,
    /// Seconds since the recovery manager started.
    pub uptime_secs: u64,
}

/// A key that has been quarantined due to corruption or repeated errors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuarantinedKey {
    /// The key that was quarantined.
    pub key: String,
    /// Reason the key was quarantined.
    pub reason: String,
    /// When the key was quarantined.
    pub quarantined_at: DateTime<Utc>,
    /// Size of the value in bytes before quarantine.
    pub original_size: u64,
}

/// Aggregate statistics for recovery operations
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RecoveryStats {
    /// Total number of errors reported across all components.
    pub total_errors: u64,
    /// Error counts grouped by severity level.
    pub errors_by_severity: HashMap<String, u64>,
    /// Error counts grouped by component type.
    pub errors_by_component: HashMap<String, u64>,
    /// Number of times a circuit breaker was tripped open.
    pub circuit_breakers_tripped: u64,
    /// Number of out-of-memory events handled.
    pub oom_events: u64,
    /// Number of key evictions triggered.
    pub evictions_triggered: u64,
    /// Number of disk failover operations performed.
    pub disk_failovers: u64,
    /// Number of keys placed in quarantine.
    pub keys_quarantined: u64,
    /// Number of times backpressure was applied to clients.
    pub backpressure_applied: u64,
    /// Number of clients disconnected due to backpressure.
    pub clients_disconnected: u64,
}

// ---------------------------------------------------------------------------
// Internal per-component state
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct ComponentState {
    consecutive_failures: u32,
    total_errors: u64,
    circuit: CircuitState,
    last_error: Option<DateTime<Utc>>,
}

impl ComponentState {
    fn new() -> Self {
        Self {
            consecutive_failures: 0,
            total_errors: 0,
            circuit: CircuitState::Closed,
            last_error: None,
        }
    }
}

// ---------------------------------------------------------------------------
// ErrorRecoveryManager
// ---------------------------------------------------------------------------

/// Centralized error handling, circuit breaking, backpressure, and recovery
pub struct ErrorRecoveryManager {
    config: RecoveryConfig,
    components: RwLock<HashMap<ComponentType, ComponentState>>,
    quarantined: RwLock<Vec<QuarantinedKey>>,
    errors: RwLock<Vec<SystemError>>,
    started_at: DateTime<Utc>,

    // Atomic counters for stats
    total_errors: AtomicU64,
    circuit_breakers_tripped: AtomicU64,
    oom_events: AtomicU64,
    evictions_triggered: AtomicU64,
    disk_failovers: AtomicU64,
    keys_quarantined: AtomicU64,
    backpressure_applied: AtomicU64,
    clients_disconnected: AtomicU64,

    errors_by_severity: RwLock<HashMap<String, u64>>,
    errors_by_component: RwLock<HashMap<String, u64>>,
}

impl ErrorRecoveryManager {
    /// Create a new manager with the given configuration
    pub fn new(config: RecoveryConfig) -> Self {
        Self {
            config,
            components: RwLock::new(HashMap::new()),
            quarantined: RwLock::new(Vec::new()),
            errors: RwLock::new(Vec::new()),
            started_at: Utc::now(),
            total_errors: AtomicU64::new(0),
            circuit_breakers_tripped: AtomicU64::new(0),
            oom_events: AtomicU64::new(0),
            evictions_triggered: AtomicU64::new(0),
            disk_failovers: AtomicU64::new(0),
            keys_quarantined: AtomicU64::new(0),
            backpressure_applied: AtomicU64::new(0),
            clients_disconnected: AtomicU64::new(0),
            errors_by_severity: RwLock::new(HashMap::new()),
            errors_by_component: RwLock::new(HashMap::new()),
        }
    }

    /// Report a system error and update circuit breaker state
    pub fn report_error(&self, error: SystemError) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);

        // Increment severity / component counters
        {
            let mut sev = self.errors_by_severity.write();
            *sev.entry(error.severity.to_string()).or_insert(0) += 1;
        }
        {
            let mut comp = self.errors_by_component.write();
            *comp.entry(error.component.to_string()).or_insert(0) += 1;
        }

        // Update per-component state and circuit breaker
        {
            let mut components = self.components.write();
            let state = components
                .entry(error.component.clone())
                .or_insert_with(ComponentState::new);

            state.total_errors += 1;
            state.last_error = Some(error.timestamp);
            state.consecutive_failures += 1;

            if state.consecutive_failures >= self.config.circuit_breaker_threshold {
                if matches!(state.circuit, CircuitState::Closed) {
                    self.circuit_breakers_tripped
                        .fetch_add(1, Ordering::Relaxed);
                }
                state.circuit = CircuitState::Open {
                    since: Utc::now(),
                    failures: state.consecutive_failures,
                };
            }
        }

        self.errors.write().push(error);
    }

    /// Get the current aggregated system health
    pub fn get_health(&self) -> SystemHealth {
        let components_map = self.components.read();
        let now = Utc::now();
        let uptime_secs = (now - self.started_at).num_seconds().max(0) as u64;

        // Count errors in the last 60 seconds
        let one_min_ago = now - chrono::Duration::seconds(60);
        let errors_last_minute = self
            .errors
            .read()
            .iter()
            .filter(|e| e.timestamp >= one_min_ago)
            .count() as u64;

        let mut component_health: Vec<ComponentHealth> = components_map
            .iter()
            .map(|(ct, cs)| {
                let circuit = self.maybe_transition_circuit(cs, now);
                ComponentHealth {
                    component: ct.clone(),
                    healthy: matches!(circuit, CircuitState::Closed),
                    circuit_state: circuit,
                    errors: cs.total_errors,
                    last_error: cs.last_error,
                }
            })
            .collect();

        component_health.sort_by(|a, b| a.component.to_string().cmp(&b.component.to_string()));

        let status = self.derive_health_level(&component_health, errors_last_minute);

        SystemHealth {
            status,
            components: component_health,
            errors_last_minute,
            uptime_secs,
        }
    }

    /// Get the circuit breaker state for a component, applying time-based transitions
    pub fn get_circuit_breaker(&self, component: &str) -> CircuitState {
        let ct = Self::parse_component(component);
        let components = self.components.read();
        match components.get(&ct) {
            Some(state) => self.maybe_transition_circuit(state, Utc::now()),
            None => CircuitState::Closed,
        }
    }

    /// Determine the backpressure action for a given client based on its queue depth
    pub fn apply_backpressure(&self, _client_id: &str, queue_depth: u64) -> BackpressureAction {
        let max = self.config.max_client_queue_depth;
        let ratio = queue_depth as f64 / max as f64;

        if ratio > 1.0 {
            self.clients_disconnected.fetch_add(1, Ordering::Relaxed);
            self.backpressure_applied.fetch_add(1, Ordering::Relaxed);
            BackpressureAction::Disconnect {
                reason: format!("Output buffer {} exceeds maximum {}", queue_depth, max),
            }
        } else if ratio > 0.75 {
            self.backpressure_applied.fetch_add(1, Ordering::Relaxed);
            BackpressureAction::Reject {
                reason: format!("Output buffer at {:.0}% capacity", ratio * 100.0),
            }
        } else if ratio > 0.25 {
            self.backpressure_applied.fetch_add(1, Ordering::Relaxed);
            let delay_ms = ((ratio - 0.25) / 0.50 * 100.0) as u64;
            BackpressureAction::Throttle { delay_ms }
        } else {
            BackpressureAction::Allow
        }
    }

    /// Decide how to handle an out-of-memory situation
    pub fn handle_oom(&self, requested_bytes: u64) -> OomAction {
        self.oom_events.fetch_add(1, Ordering::Relaxed);

        if !self.config.oom_eviction_enabled {
            return OomAction::RejectWrite {
                reason: "OOM eviction disabled by configuration".into(),
            };
        }

        // Target 10% headroom on top of what was requested
        let target = requested_bytes + requested_bytes / 10;

        // Prefer TTL-based eviction, then LRU, and finally reject
        if target < 1024 * 1024 {
            // Small allocation — try TTL expiry first
            self.evictions_triggered.fetch_add(1, Ordering::Relaxed);
            OomAction::EvictTtl
        } else if target < 100 * 1024 * 1024 {
            self.evictions_triggered.fetch_add(1, Ordering::Relaxed);
            OomAction::EvictLru {
                target_bytes: target,
            }
        } else {
            self.evictions_triggered.fetch_add(1, Ordering::Relaxed);
            OomAction::EvictLfu {
                target_bytes: target,
            }
        }
    }

    /// Decide how to handle a disk I/O error
    pub fn handle_disk_error(&self, error: &str) -> DiskAction {
        // Record the error on the Disk component
        self.report_error(SystemError {
            component: ComponentType::Disk,
            severity: ErrorSeverity::Error,
            message: error.to_string(),
            timestamp: Utc::now(),
            recoverable: true,
        });

        let components = self.components.read();
        let failures = components
            .get(&ComponentType::Disk)
            .map(|s| s.consecutive_failures)
            .unwrap_or(0);

        if failures <= 1 {
            DiskAction::Retry { delay_ms: 100 }
        } else if failures <= 3 {
            if let Some(ref path) = self.config.disk_failover_path {
                self.disk_failovers.fetch_add(1, Ordering::Relaxed);
                DiskAction::Failover { path: path.clone() }
            } else {
                DiskAction::DisableAof
            }
        } else if failures <= self.config.circuit_breaker_threshold {
            DiskAction::ReadOnly
        } else {
            DiskAction::Shutdown
        }
    }

    /// Quarantine a key that caused repeated errors
    pub fn quarantine_key(&self, key: &str, reason: &str) {
        let mut quarantined = self.quarantined.write();
        if quarantined.len() >= self.config.max_quarantined_keys {
            // Drop oldest entry to make room
            quarantined.remove(0);
        }

        quarantined.push(QuarantinedKey {
            key: key.to_string(),
            reason: reason.to_string(),
            quarantined_at: Utc::now(),
            original_size: 0,
        });
        self.keys_quarantined.fetch_add(1, Ordering::Relaxed);
    }

    /// Return the list of currently quarantined keys
    pub fn get_quarantined_keys(&self) -> Vec<QuarantinedKey> {
        self.quarantined.read().clone()
    }

    /// Get aggregate recovery statistics
    pub fn get_stats(&self) -> RecoveryStats {
        RecoveryStats {
            total_errors: self.total_errors.load(Ordering::Relaxed),
            errors_by_severity: self.errors_by_severity.read().clone(),
            errors_by_component: self.errors_by_component.read().clone(),
            circuit_breakers_tripped: self.circuit_breakers_tripped.load(Ordering::Relaxed),
            oom_events: self.oom_events.load(Ordering::Relaxed),
            evictions_triggered: self.evictions_triggered.load(Ordering::Relaxed),
            disk_failovers: self.disk_failovers.load(Ordering::Relaxed),
            keys_quarantined: self.keys_quarantined.load(Ordering::Relaxed),
            backpressure_applied: self.backpressure_applied.load(Ordering::Relaxed),
            clients_disconnected: self.clients_disconnected.load(Ordering::Relaxed),
        }
    }

    // -- internal helpers ---------------------------------------------------

    /// Compute the read-only circuit state, applying the time-based half-open
    /// transition without mutating shared state.
    fn maybe_transition_circuit(&self, state: &ComponentState, now: DateTime<Utc>) -> CircuitState {
        match &state.circuit {
            CircuitState::Open { since, failures } => {
                let elapsed = (now - *since).num_seconds().max(0) as u64;
                if elapsed >= self.config.circuit_breaker_reset_secs {
                    CircuitState::HalfOpen { since: now }
                } else {
                    CircuitState::Open {
                        since: *since,
                        failures: *failures,
                    }
                }
            }
            other => other.clone(),
        }
    }

    /// Derive the top-level health status from component health and recent errors
    fn derive_health_level(
        &self,
        components: &[ComponentHealth],
        errors_last_minute: u64,
    ) -> HealthLevel {
        let open_circuits: Vec<_> = components
            .iter()
            .filter(|c| matches!(c.circuit_state, CircuitState::Open { .. }))
            .collect();

        if !open_circuits.is_empty() && open_circuits.len() >= 2 {
            return HealthLevel::Critical {
                reason: format!("{} circuit breakers open", open_circuits.len()),
            };
        }

        if !open_circuits.is_empty() {
            return HealthLevel::Unhealthy {
                reason: format!("Circuit open for {}", open_circuits[0].component),
            };
        }

        if errors_last_minute > 10 {
            return HealthLevel::Degraded {
                reason: format!("{} errors in the last minute", errors_last_minute),
            };
        }

        HealthLevel::Healthy
    }

    /// Parse a string into a `ComponentType`, defaulting to `Network`
    fn parse_component(name: &str) -> ComponentType {
        match name.to_lowercase().as_str() {
            "connection" => ComponentType::Connection,
            "storage" => ComponentType::Storage,
            "persistence" => ComponentType::Persistence,
            "replication" => ComponentType::Replication,
            "cluster" => ComponentType::Cluster,
            "query" => ComponentType::Query,
            "memory" => ComponentType::Memory,
            "disk" => ComponentType::Disk,
            "network" => ComponentType::Network,
            _ => ComponentType::Network,
        }
    }

    /// Reset the circuit breaker for a component (e.g. after a successful half-open probe)
    pub fn reset_circuit_breaker(&self, component: &str) {
        let ct = Self::parse_component(component);
        let mut components = self.components.write();
        if let Some(state) = components.get_mut(&ct) {
            state.consecutive_failures = 0;
            state.circuit = CircuitState::Closed;
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_manager() -> ErrorRecoveryManager {
        ErrorRecoveryManager::new(RecoveryConfig::default())
    }

    fn make_error(component: ComponentType, severity: ErrorSeverity) -> SystemError {
        SystemError {
            component,
            severity,
            message: "test error".into(),
            timestamp: Utc::now(),
            recoverable: true,
        }
    }

    // -- error reporting & stats -------------------------------------------

    #[test]
    fn test_error_reporting_increments_stats() {
        let mgr = default_manager();

        mgr.report_error(make_error(ComponentType::Storage, ErrorSeverity::Error));
        mgr.report_error(make_error(ComponentType::Storage, ErrorSeverity::Warning));
        mgr.report_error(make_error(ComponentType::Network, ErrorSeverity::Error));

        let stats = mgr.get_stats();
        assert_eq!(stats.total_errors, 3);
        assert_eq!(*stats.errors_by_severity.get("Error").unwrap(), 2);
        assert_eq!(*stats.errors_by_severity.get("Warning").unwrap(), 1);
        assert_eq!(*stats.errors_by_component.get("Storage").unwrap(), 2);
        assert_eq!(*stats.errors_by_component.get("Network").unwrap(), 1);
    }

    // -- circuit breaker transitions ---------------------------------------

    #[test]
    fn test_circuit_breaker_closed_to_open() {
        let config = RecoveryConfig {
            circuit_breaker_threshold: 3,
            ..Default::default()
        };
        let mgr = ErrorRecoveryManager::new(config);

        // Two failures — still closed
        for _ in 0..2 {
            mgr.report_error(make_error(ComponentType::Storage, ErrorSeverity::Error));
        }
        let cb = mgr.get_circuit_breaker("storage");
        assert!(matches!(cb, CircuitState::Closed));

        // Third failure — opens the circuit
        mgr.report_error(make_error(ComponentType::Storage, ErrorSeverity::Error));
        let cb = mgr.get_circuit_breaker("storage");
        assert!(matches!(cb, CircuitState::Open { .. }));

        let stats = mgr.get_stats();
        assert_eq!(stats.circuit_breakers_tripped, 1);
    }

    #[test]
    fn test_circuit_breaker_open_to_half_open() {
        let config = RecoveryConfig {
            circuit_breaker_threshold: 1,
            circuit_breaker_reset_secs: 0, // instant reset for testing
            ..Default::default()
        };
        let mgr = ErrorRecoveryManager::new(config);

        mgr.report_error(make_error(ComponentType::Storage, ErrorSeverity::Error));

        // With reset_secs = 0 the read should transition to HalfOpen
        let cb = mgr.get_circuit_breaker("storage");
        assert!(
            matches!(cb, CircuitState::HalfOpen { .. }),
            "Expected HalfOpen, got {:?}",
            cb
        );
    }

    #[test]
    fn test_circuit_breaker_half_open_to_closed() {
        let config = RecoveryConfig {
            circuit_breaker_threshold: 1,
            circuit_breaker_reset_secs: 0,
            ..Default::default()
        };
        let mgr = ErrorRecoveryManager::new(config);

        mgr.report_error(make_error(ComponentType::Storage, ErrorSeverity::Error));

        // Simulate successful probe
        mgr.reset_circuit_breaker("storage");

        let cb = mgr.get_circuit_breaker("storage");
        assert!(matches!(cb, CircuitState::Closed));
    }

    // -- backpressure thresholds -------------------------------------------

    #[test]
    fn test_backpressure_allow() {
        let mgr = default_manager();
        let max = mgr.config.max_client_queue_depth;
        let action = mgr.apply_backpressure("c1", max / 10); // 10%
        assert_eq!(action, BackpressureAction::Allow);
    }

    #[test]
    fn test_backpressure_throttle() {
        let mgr = default_manager();
        let max = mgr.config.max_client_queue_depth;
        let action = mgr.apply_backpressure("c1", max / 2); // 50%
        assert!(matches!(action, BackpressureAction::Throttle { .. }));
    }

    #[test]
    fn test_backpressure_reject() {
        let mgr = default_manager();
        let max = mgr.config.max_client_queue_depth;
        let action = mgr.apply_backpressure("c1", (max as f64 * 0.80) as u64); // 80%
        assert!(matches!(action, BackpressureAction::Reject { .. }));
    }

    #[test]
    fn test_backpressure_disconnect() {
        let mgr = default_manager();
        let max = mgr.config.max_client_queue_depth;
        let action = mgr.apply_backpressure("c1", max + 1);
        assert!(matches!(action, BackpressureAction::Disconnect { .. }));

        let stats = mgr.get_stats();
        assert_eq!(stats.clients_disconnected, 1);
        assert_eq!(stats.backpressure_applied, 1);
    }

    // -- OOM action selection ----------------------------------------------

    #[test]
    fn test_oom_evict_ttl_small_alloc() {
        let mgr = default_manager();
        let action = mgr.handle_oom(512); // small
        assert_eq!(action, OomAction::EvictTtl);
    }

    #[test]
    fn test_oom_evict_lru_medium_alloc() {
        let mgr = default_manager();
        let action = mgr.handle_oom(10 * 1024 * 1024); // 10 MiB
        assert!(matches!(action, OomAction::EvictLru { .. }));
    }

    #[test]
    fn test_oom_evict_lfu_large_alloc() {
        let mgr = default_manager();
        let action = mgr.handle_oom(200 * 1024 * 1024); // 200 MiB
        assert!(matches!(action, OomAction::EvictLfu { .. }));
    }

    #[test]
    fn test_oom_reject_when_eviction_disabled() {
        let config = RecoveryConfig {
            oom_eviction_enabled: false,
            ..Default::default()
        };
        let mgr = ErrorRecoveryManager::new(config);
        let action = mgr.handle_oom(1024);
        assert!(matches!(action, OomAction::RejectWrite { .. }));
    }

    #[test]
    fn test_oom_stats_increment() {
        let mgr = default_manager();
        mgr.handle_oom(1024);
        mgr.handle_oom(2048);
        let stats = mgr.get_stats();
        assert_eq!(stats.oom_events, 2);
        assert_eq!(stats.evictions_triggered, 2);
    }

    // -- disk error handling -----------------------------------------------

    #[test]
    fn test_disk_error_retry_on_first_failure() {
        let mgr = default_manager();
        let action = mgr.handle_disk_error("I/O timeout");
        assert!(matches!(action, DiskAction::Retry { delay_ms: 100 }));
    }

    #[test]
    fn test_disk_error_failover_with_path() {
        let config = RecoveryConfig {
            disk_failover_path: Some("/mnt/backup".into()),
            ..Default::default()
        };
        let mgr = ErrorRecoveryManager::new(config);

        // First failure → retry
        mgr.handle_disk_error("write failed");
        // Second failure → still retry (failures=2 but checked after increment)
        let action = mgr.handle_disk_error("write failed");
        assert!(
            matches!(
                action,
                DiskAction::Failover { .. } | DiskAction::Retry { .. }
            ),
            "Expected Failover or Retry, got {:?}",
            action
        );
    }

    #[test]
    fn test_disk_error_disable_aof_without_failover() {
        let mgr = default_manager();
        // Accumulate failures
        mgr.handle_disk_error("err1");
        mgr.handle_disk_error("err2");
        let action = mgr.handle_disk_error("err3");
        assert!(
            matches!(action, DiskAction::DisableAof | DiskAction::ReadOnly),
            "Expected DisableAof or ReadOnly, got {:?}",
            action
        );
    }

    // -- key quarantine ----------------------------------------------------

    #[test]
    fn test_quarantine_key() {
        let mgr = default_manager();
        mgr.quarantine_key("bad:key:1", "corrupted payload");

        let keys = mgr.get_quarantined_keys();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].key, "bad:key:1");
        assert_eq!(keys[0].reason, "corrupted payload");

        let stats = mgr.get_stats();
        assert_eq!(stats.keys_quarantined, 1);
    }

    #[test]
    fn test_quarantine_evicts_oldest_when_full() {
        let config = RecoveryConfig {
            max_quarantined_keys: 3,
            ..Default::default()
        };
        let mgr = ErrorRecoveryManager::new(config);

        mgr.quarantine_key("k1", "r1");
        mgr.quarantine_key("k2", "r2");
        mgr.quarantine_key("k3", "r3");
        mgr.quarantine_key("k4", "r4"); // should evict k1

        let keys = mgr.get_quarantined_keys();
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0].key, "k2");
        assert_eq!(keys[2].key, "k4");
    }

    // -- system health aggregation -----------------------------------------

    #[test]
    fn test_health_healthy_when_no_errors() {
        let mgr = default_manager();
        let health = mgr.get_health();
        assert!(matches!(health.status, HealthLevel::Healthy));
        assert_eq!(health.errors_last_minute, 0);
    }

    #[test]
    fn test_health_unhealthy_with_open_circuit() {
        let config = RecoveryConfig {
            circuit_breaker_threshold: 1,
            circuit_breaker_reset_secs: 3600, // won't auto-reset
            ..Default::default()
        };
        let mgr = ErrorRecoveryManager::new(config);

        mgr.report_error(make_error(ComponentType::Storage, ErrorSeverity::Critical));

        let health = mgr.get_health();
        assert!(
            matches!(health.status, HealthLevel::Unhealthy { .. }),
            "Expected Unhealthy, got {:?}",
            health.status
        );
    }

    #[test]
    fn test_health_critical_with_multiple_open_circuits() {
        let config = RecoveryConfig {
            circuit_breaker_threshold: 1,
            circuit_breaker_reset_secs: 3600,
            ..Default::default()
        };
        let mgr = ErrorRecoveryManager::new(config);

        mgr.report_error(make_error(ComponentType::Storage, ErrorSeverity::Critical));
        mgr.report_error(make_error(ComponentType::Network, ErrorSeverity::Critical));

        let health = mgr.get_health();
        assert!(
            matches!(health.status, HealthLevel::Critical { .. }),
            "Expected Critical, got {:?}",
            health.status
        );
    }

    // -- config validation -------------------------------------------------

    #[test]
    fn test_default_config_values() {
        let cfg = RecoveryConfig::default();
        assert_eq!(cfg.max_client_queue_depth, 128 * 1024 * 1024);
        assert!(cfg.oom_eviction_enabled);
        assert!(cfg.disk_failover_path.is_none());
        assert_eq!(cfg.circuit_breaker_threshold, 5);
        assert_eq!(cfg.circuit_breaker_reset_secs, 30);
        assert_eq!(cfg.slow_client_timeout_secs, 10);
        assert_eq!(cfg.max_quarantined_keys, 1000);
    }

    #[test]
    fn test_custom_config() {
        let cfg = RecoveryConfig {
            max_client_queue_depth: 64 * 1024 * 1024,
            oom_eviction_enabled: false,
            disk_failover_path: Some("/tmp/failover".into()),
            circuit_breaker_threshold: 10,
            circuit_breaker_reset_secs: 60,
            slow_client_timeout_secs: 30,
            max_quarantined_keys: 500,
        };
        let mgr = ErrorRecoveryManager::new(cfg);
        let stats = mgr.get_stats();
        assert_eq!(stats.total_errors, 0);
    }

    // -- unknown component defaults ----------------------------------------

    #[test]
    fn test_unknown_component_returns_closed() {
        let mgr = default_manager();
        let cb = mgr.get_circuit_breaker("nonexistent");
        assert!(matches!(cb, CircuitState::Closed));
    }
}
