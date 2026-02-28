//! Traffic-Shifting Proxy for Zero-Downtime Redis Migration
//!
//! A transparent proxy that sits between clients and both Redis (source) and
//! Ferrite (target), enabling gradual traffic shifting during migration. Reads
//! are routed based on a configurable percentage, while writes can be dual-written
//! to both backends for consistency verification.
//!
//! # Traffic Routing
//!
//! - **Reads** — routed to source or target based on `target_pct` (0–100).
//!   Deterministic per-key routing uses `hash(key) % 100 < target_pct`.
//! - **Writes** — controlled by [`WriteMode`]: source-only, dual-write, or target-only.
//! - **Comparison mode** — optionally sends reads to *both* backends and compares responses.
//!
//! # Safety & Rollback
//!
//! The proxy continuously tracks error rates per backend. If the target error
//! rate exceeds [`TrafficProxyConfig::error_threshold_pct`], [`TrafficProxy::should_rollback`]
//! returns `true` and operators can react accordingly.

use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::time::Duration;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// WriteMode
// ---------------------------------------------------------------------------

/// Controls how write commands are routed during migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriteMode {
    /// Writes go only to the source (Redis).
    SourceOnly,
    /// Writes go to both source and target for consistency verification.
    DualWrite,
    /// Writes go only to the target (Ferrite).
    TargetOnly,
}

// ---------------------------------------------------------------------------
// Backend
// ---------------------------------------------------------------------------

/// Identifies one of the two migration backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Backend {
    /// The original Redis instance (source of truth during early migration).
    Source,
    /// The Ferrite instance being migrated to.
    Target,
}

impl fmt::Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source => write!(f, "source"),
            Self::Target => write!(f, "target"),
        }
    }
}

// ---------------------------------------------------------------------------
// ProxyState
// ---------------------------------------------------------------------------

/// Lifecycle state of the traffic proxy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProxyState {
    /// Proxy is initializing.
    Starting,
    /// Proxy is actively routing traffic.
    Running,
    /// Proxy has been paused (no traffic routed).
    Paused,
    /// Proxy rolled back — all traffic returned to source.
    RolledBack,
    /// Migration completed — all traffic now goes to target.
    Completed,
}

// ---------------------------------------------------------------------------
// TrafficProxyConfig
// ---------------------------------------------------------------------------

/// Configuration for the traffic-shifting proxy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrafficProxyConfig {
    /// Address the proxy listens on for client connections.
    pub listen_addr: String,
    /// Address of the source Redis instance.
    pub source_addr: String,
    /// Address of the target Ferrite instance.
    pub target_addr: String,
    /// Initial percentage of reads routed to the target (0–100).
    pub initial_target_pct: u8,
    /// How write commands are routed.
    pub write_mode: WriteMode,
    /// Target error-rate threshold (%) that triggers a rollback recommendation.
    pub error_threshold_pct: f64,
    /// When `true`, reads are sent to *both* backends and responses are compared.
    pub comparison_mode: bool,
    /// Timeout applied to individual backend reads.
    pub read_timeout: Duration,
    /// Number of connections to maintain per backend in the pool.
    pub connection_pool_size: usize,
}

impl Default for TrafficProxyConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:6380".to_string(),
            source_addr: "127.0.0.1:6379".to_string(),
            target_addr: "127.0.0.1:6381".to_string(),
            initial_target_pct: 0,
            write_mode: WriteMode::SourceOnly,
            error_threshold_pct: 1.0,
            comparison_mode: true,
            read_timeout: Duration::from_secs(5),
            connection_pool_size: 10,
        }
    }
}

// ---------------------------------------------------------------------------
// RoutingDecision
// ---------------------------------------------------------------------------

/// Describes where a single request should be sent and whether its response
/// is the one returned to the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingDecision {
    /// Which backend to send the request to.
    pub backend: Backend,
    /// If `true`, this backend's response is returned to the client.
    pub is_primary: bool,
}

// ---------------------------------------------------------------------------
// ProxyStats (atomic counters)
// ---------------------------------------------------------------------------

/// Atomic counters tracking proxy throughput and error rates.
pub struct ProxyStats {
    total_reads: AtomicU64,
    total_writes: AtomicU64,
    source_reads: AtomicU64,
    source_writes: AtomicU64,
    source_errors: AtomicU64,
    target_reads: AtomicU64,
    target_writes: AtomicU64,
    target_errors: AtomicU64,
    comparisons_total: AtomicU64,
    comparisons_matched: AtomicU64,
    comparisons_mismatched: AtomicU64,
    source_latency_sum_us: AtomicU64,
    target_latency_sum_us: AtomicU64,
}

impl ProxyStats {
    fn new() -> Self {
        Self {
            total_reads: AtomicU64::new(0),
            total_writes: AtomicU64::new(0),
            source_reads: AtomicU64::new(0),
            source_writes: AtomicU64::new(0),
            source_errors: AtomicU64::new(0),
            target_reads: AtomicU64::new(0),
            target_writes: AtomicU64::new(0),
            target_errors: AtomicU64::new(0),
            comparisons_total: AtomicU64::new(0),
            comparisons_matched: AtomicU64::new(0),
            comparisons_mismatched: AtomicU64::new(0),
            source_latency_sum_us: AtomicU64::new(0),
            target_latency_sum_us: AtomicU64::new(0),
        }
    }

    /// Take a point-in-time snapshot of all counters.
    fn snapshot(&self) -> ProxyStatsSnapshot {
        ProxyStatsSnapshot {
            total_reads: self.total_reads.load(Ordering::Relaxed),
            total_writes: self.total_writes.load(Ordering::Relaxed),
            source_reads: self.source_reads.load(Ordering::Relaxed),
            source_writes: self.source_writes.load(Ordering::Relaxed),
            source_errors: self.source_errors.load(Ordering::Relaxed),
            target_reads: self.target_reads.load(Ordering::Relaxed),
            target_writes: self.target_writes.load(Ordering::Relaxed),
            target_errors: self.target_errors.load(Ordering::Relaxed),
            comparisons_total: self.comparisons_total.load(Ordering::Relaxed),
            comparisons_matched: self.comparisons_matched.load(Ordering::Relaxed),
            comparisons_mismatched: self.comparisons_mismatched.load(Ordering::Relaxed),
            source_latency_sum_us: self.source_latency_sum_us.load(Ordering::Relaxed),
            target_latency_sum_us: self.target_latency_sum_us.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// ProxyStatsSnapshot
// ---------------------------------------------------------------------------

/// A serializable, point-in-time view of proxy statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyStatsSnapshot {
    /// Total read operations processed.
    pub total_reads: u64,
    /// Total write operations processed.
    pub total_writes: u64,
    /// Reads routed to the source backend.
    pub source_reads: u64,
    /// Writes routed to the source backend.
    pub source_writes: u64,
    /// Errors from the source backend.
    pub source_errors: u64,
    /// Reads routed to the target backend.
    pub target_reads: u64,
    /// Writes routed to the target backend.
    pub target_writes: u64,
    /// Errors from the target backend.
    pub target_errors: u64,
    /// Total comparison operations performed.
    pub comparisons_total: u64,
    /// Comparisons where source and target responses matched.
    pub comparisons_matched: u64,
    /// Comparisons where source and target responses differed.
    pub comparisons_mismatched: u64,
    /// Cumulative source backend latency in microseconds.
    pub source_latency_sum_us: u64,
    /// Cumulative target backend latency in microseconds.
    pub target_latency_sum_us: u64,
}

// ---------------------------------------------------------------------------
// TrafficProxy
// ---------------------------------------------------------------------------

/// The main traffic-shifting proxy engine.
///
/// Routes read and write traffic between a source Redis instance and a target
/// Ferrite instance based on a configurable percentage. Supports dual-write
/// mode, response comparison, and automatic rollback detection.
pub struct TrafficProxy {
    /// Proxy configuration.
    config: TrafficProxyConfig,
    /// Current percentage of reads routed to target (0–100).
    target_pct: AtomicU8,
    /// Atomic counters for monitoring.
    stats: ProxyStats,
    /// Current lifecycle state.
    state: RwLock<ProxyState>,
}

impl TrafficProxy {
    /// Create a new traffic proxy with the given configuration.
    pub fn new(config: TrafficProxyConfig) -> Self {
        let initial = config.initial_target_pct.min(100);
        Self {
            target_pct: AtomicU8::new(initial),
            stats: ProxyStats::new(),
            state: RwLock::new(ProxyState::Starting),
            config,
        }
    }

    /// Adjust the percentage of reads routed to the target backend.
    ///
    /// Values above 100 are clamped to 100.
    pub fn set_target_percentage(&self, pct: u8) {
        self.target_pct.store(pct.min(100), Ordering::Relaxed);
    }

    /// Decide where to route a read for the given key.
    ///
    /// Uses deterministic hashing so the same key always maps to the same
    /// backend at a given percentage.
    pub fn route_read(&self, key: &str) -> RoutingDecision {
        self.stats.total_reads.fetch_add(1, Ordering::Relaxed);

        let pct = self.target_pct.load(Ordering::Relaxed);
        let backend = if key_hash_bucket(key) < pct {
            self.stats.target_reads.fetch_add(1, Ordering::Relaxed);
            Backend::Target
        } else {
            self.stats.source_reads.fetch_add(1, Ordering::Relaxed);
            Backend::Source
        };

        RoutingDecision {
            backend,
            is_primary: true,
        }
    }

    /// Decide where to route a write for the given key.
    ///
    /// In [`WriteMode::DualWrite`] mode two decisions are returned — the
    /// source decision is marked as primary.
    pub fn route_write(&self, _key: &str) -> Vec<RoutingDecision> {
        self.stats.total_writes.fetch_add(1, Ordering::Relaxed);

        match self.config.write_mode {
            WriteMode::SourceOnly => {
                self.stats.source_writes.fetch_add(1, Ordering::Relaxed);
                vec![RoutingDecision {
                    backend: Backend::Source,
                    is_primary: true,
                }]
            }
            WriteMode::DualWrite => {
                self.stats.source_writes.fetch_add(1, Ordering::Relaxed);
                self.stats.target_writes.fetch_add(1, Ordering::Relaxed);
                vec![
                    RoutingDecision {
                        backend: Backend::Source,
                        is_primary: true,
                    },
                    RoutingDecision {
                        backend: Backend::Target,
                        is_primary: false,
                    },
                ]
            }
            WriteMode::TargetOnly => {
                self.stats.target_writes.fetch_add(1, Ordering::Relaxed);
                vec![RoutingDecision {
                    backend: Backend::Target,
                    is_primary: true,
                }]
            }
        }
    }

    /// Record the outcome of a backend response for stats tracking.
    pub fn record_response(&self, backend: Backend, success: bool, latency_us: u64) {
        match backend {
            Backend::Source => {
                self.stats
                    .source_latency_sum_us
                    .fetch_add(latency_us, Ordering::Relaxed);
                if !success {
                    self.stats.source_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
            Backend::Target => {
                self.stats
                    .target_latency_sum_us
                    .fetch_add(latency_us, Ordering::Relaxed);
                if !success {
                    self.stats.target_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// Record the result of comparing source and target responses for a key.
    pub fn record_comparison(&self, _key: &str, match_result: bool) {
        self.stats
            .comparisons_total
            .fetch_add(1, Ordering::Relaxed);
        if match_result {
            self.stats
                .comparisons_matched
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats
                .comparisons_mismatched
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Check whether the target error rate exceeds the configured threshold.
    ///
    /// Returns `true` if the proxy should rollback to source-only traffic.
    pub fn should_rollback(&self) -> bool {
        let target_reads = self.stats.target_reads.load(Ordering::Relaxed);
        let target_writes = self.stats.target_writes.load(Ordering::Relaxed);
        let total_target = target_reads + target_writes;

        if total_target == 0 {
            return false;
        }

        let target_errors = self.stats.target_errors.load(Ordering::Relaxed);
        #[allow(clippy::cast_precision_loss)]
        let error_rate = (target_errors as f64 / total_target as f64) * 100.0;
        error_rate > self.config.error_threshold_pct
    }

    /// Return a point-in-time snapshot of all proxy statistics.
    pub fn stats(&self) -> ProxyStatsSnapshot {
        self.stats.snapshot()
    }

    /// Schedule a gradual increase of target traffic percentage.
    ///
    /// Increases the target percentage by `step` every `interval_secs` seconds
    /// until 100% is reached. This method is non-blocking and records the
    /// schedule; actual stepping is expected to be driven by an external timer
    /// or event loop calling [`Self::set_target_percentage`].
    ///
    /// Returns the planned percentage steps.
    pub fn gradual_increase(&self, step: u8, _interval_secs: u64) -> Vec<u8> {
        let current = self.target_pct.load(Ordering::Relaxed);
        let step = step.max(1);
        let mut plan = Vec::new();
        let mut pct = current;
        while pct < 100 {
            pct = pct.saturating_add(step).min(100);
            plan.push(pct);
        }
        plan
    }

    /// Get the current proxy lifecycle state.
    pub fn proxy_state(&self) -> ProxyState {
        *self.state.read()
    }

    /// Transition the proxy to a new lifecycle state.
    pub fn set_proxy_state(&self, new_state: ProxyState) {
        *self.state.write() = new_state;
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Hash a key to a deterministic bucket in [0, 100).
fn key_hash_bucket(key: &str) -> u8 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    #[allow(clippy::cast_possible_truncation)]
    let bucket = (hasher.finish() % 100) as u8;
    bucket
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = TrafficProxyConfig::default();
        assert_eq!(config.listen_addr, "127.0.0.1:6380");
        assert_eq!(config.source_addr, "127.0.0.1:6379");
        assert_eq!(config.target_addr, "127.0.0.1:6381");
        assert_eq!(config.initial_target_pct, 0);
        assert_eq!(config.write_mode, WriteMode::SourceOnly);
        assert!((config.error_threshold_pct - 1.0).abs() < f64::EPSILON);
        assert!(config.comparison_mode);
        assert_eq!(config.read_timeout, Duration::from_secs(5));
        assert_eq!(config.connection_pool_size, 10);
    }

    #[test]
    fn test_route_read_zero_pct_always_source() {
        let proxy = TrafficProxy::new(TrafficProxyConfig {
            initial_target_pct: 0,
            ..Default::default()
        });

        for i in 0..200 {
            let key = format!("key:{i}");
            let decision = proxy.route_read(&key);
            assert_eq!(decision.backend, Backend::Source);
            assert!(decision.is_primary);
        }
    }

    #[test]
    fn test_route_read_100_pct_always_target() {
        let proxy = TrafficProxy::new(TrafficProxyConfig {
            initial_target_pct: 100,
            ..Default::default()
        });

        for i in 0..200 {
            let key = format!("key:{i}");
            let decision = proxy.route_read(&key);
            assert_eq!(decision.backend, Backend::Target);
            assert!(decision.is_primary);
        }
    }

    #[test]
    fn test_route_read_50_pct_distributes_traffic() {
        let proxy = TrafficProxy::new(TrafficProxyConfig {
            initial_target_pct: 50,
            ..Default::default()
        });

        let mut source_count = 0u64;
        let mut target_count = 0u64;
        let iterations = 1000;

        for i in 0..iterations {
            let key = format!("key:{i}");
            let decision = proxy.route_read(&key);
            match decision.backend {
                Backend::Source => source_count += 1,
                Backend::Target => target_count += 1,
            }
        }

        // Both backends should receive meaningful traffic (at least 20%).
        assert!(
            source_count > iterations / 5,
            "source got too few reads: {source_count}"
        );
        assert!(
            target_count > iterations / 5,
            "target got too few reads: {target_count}"
        );
    }

    #[test]
    fn test_write_routing_dual_write() {
        let proxy = TrafficProxy::new(TrafficProxyConfig {
            write_mode: WriteMode::DualWrite,
            ..Default::default()
        });

        let decisions = proxy.route_write("mykey");
        assert_eq!(decisions.len(), 2);

        let source = decisions.iter().find(|d| d.backend == Backend::Source);
        let target = decisions.iter().find(|d| d.backend == Backend::Target);

        assert!(source.is_some());
        assert!(target.is_some());
        assert!(source.expect("source present").is_primary);
        assert!(!target.expect("target present").is_primary);
    }

    #[test]
    fn test_write_routing_source_only() {
        let proxy = TrafficProxy::new(TrafficProxyConfig {
            write_mode: WriteMode::SourceOnly,
            ..Default::default()
        });

        let decisions = proxy.route_write("mykey");
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].backend, Backend::Source);
        assert!(decisions[0].is_primary);
    }

    #[test]
    fn test_error_threshold_detection() {
        let proxy = TrafficProxy::new(TrafficProxyConfig {
            initial_target_pct: 100,
            error_threshold_pct: 5.0,
            ..Default::default()
        });

        // Simulate 100 target operations, 3 errors → 3% < 5% → no rollback.
        for _ in 0..97 {
            proxy.route_read("k");
            proxy.record_response(Backend::Target, true, 100);
        }
        for _ in 0..3 {
            proxy.route_read("k");
            proxy.record_response(Backend::Target, false, 100);
        }
        assert!(!proxy.should_rollback());

        // Push error count above 5% threshold.
        for _ in 0..10 {
            proxy.route_read("k");
            proxy.record_response(Backend::Target, false, 100);
        }
        assert!(proxy.should_rollback());
    }

    #[test]
    fn test_stats_recording_and_snapshot() {
        let proxy = TrafficProxy::new(TrafficProxyConfig {
            initial_target_pct: 100,
            write_mode: WriteMode::DualWrite,
            ..Default::default()
        });

        // Reads
        proxy.route_read("a");
        proxy.route_read("b");

        // Writes
        proxy.route_write("c");

        // Responses
        proxy.record_response(Backend::Target, true, 500);
        proxy.record_response(Backend::Source, false, 200);

        // Comparisons
        proxy.record_comparison("a", true);
        proxy.record_comparison("b", false);

        let snap = proxy.stats();
        assert_eq!(snap.total_reads, 2);
        assert_eq!(snap.total_writes, 1);
        assert_eq!(snap.target_reads, 2);
        assert_eq!(snap.source_writes, 1);
        assert_eq!(snap.target_writes, 1);
        assert_eq!(snap.target_latency_sum_us, 500);
        assert_eq!(snap.source_latency_sum_us, 200);
        assert_eq!(snap.source_errors, 1);
        assert_eq!(snap.comparisons_total, 2);
        assert_eq!(snap.comparisons_matched, 1);
        assert_eq!(snap.comparisons_mismatched, 1);
    }

    #[test]
    fn test_gradual_increase() {
        let proxy = TrafficProxy::new(TrafficProxyConfig {
            initial_target_pct: 0,
            ..Default::default()
        });

        let plan = proxy.gradual_increase(25, 60);
        assert_eq!(plan, vec![25, 50, 75, 100]);

        // Apply the plan.
        for &pct in &plan {
            proxy.set_target_percentage(pct);
        }
        assert_eq!(proxy.target_pct.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn test_backend_display() {
        assert_eq!(Backend::Source.to_string(), "source");
        assert_eq!(Backend::Target.to_string(), "target");
    }

    #[test]
    fn test_proxy_state_transitions() {
        let proxy = TrafficProxy::new(TrafficProxyConfig::default());
        assert_eq!(proxy.proxy_state(), ProxyState::Starting);

        proxy.set_proxy_state(ProxyState::Running);
        assert_eq!(proxy.proxy_state(), ProxyState::Running);

        proxy.set_proxy_state(ProxyState::Completed);
        assert_eq!(proxy.proxy_state(), ProxyState::Completed);
    }

    #[test]
    fn test_write_routing_target_only() {
        let proxy = TrafficProxy::new(TrafficProxyConfig {
            write_mode: WriteMode::TargetOnly,
            ..Default::default()
        });

        let decisions = proxy.route_write("mykey");
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].backend, Backend::Target);
        assert!(decisions[0].is_primary);
    }
}
