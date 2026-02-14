//! Redis Proxy Mode — Zero-downtime migration from Redis to Ferrite
//!
//! Provides a transparent proxy that sits in front of an existing Redis instance,
//! intercepts traffic, and gradually migrates data to Ferrite with zero downtime.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────┐     ┌─────────────────────────────────────┐
//! │  Client  │────►│           Ferrite Proxy              │
//! │          │◄────│                                       │
//! └──────────┘     │  ┌───────────┐    ┌──────────────┐  │
//!                  │  │  Router   │───►│ Ferrite Core │  │
//!                  │  │           │    └──────────────┘  │
//!                  │  │           │    ┌──────────────┐  │
//!                  │  │           │───►│ Redis Backend│  │
//!                  │  └───────────┘    └──────────────┘  │
//!                  │                                       │
//!                  │  ┌───────────┐    ┌──────────────┐  │
//!                  │  │ Comparator│    │  Metrics     │  │
//!                  │  └───────────┘    └──────────────┘  │
//!                  └─────────────────────────────────────┘
//! ```
//!
//! # Modes
//!
//! - **Passthrough**: All traffic forwarded to Redis (monitoring only)
//! - **Shadow**: Dual-write to both Redis and Ferrite, compare results
//! - **Migration**: Gradual traffic shifting from Redis to Ferrite (1%→100%)
//! - **Ferrite**: All traffic served by Ferrite (proxy becomes optional)
//!
//! # Example
//!
//! ```rust,ignore
//! use ferrite::proxy::{RedisProxy, ProxyConfig, ProxyMode};
//!
//! let config = ProxyConfig {
//!     listen_addr: "127.0.0.1:6379".parse().expect("valid listen address"),
//!     redis_upstream: "127.0.0.1:6380".parse().expect("valid upstream address"),
//!     mode: ProxyMode::Shadow,
//!     ..Default::default()
//! };
//!
//! let proxy = RedisProxy::new(config).await?;
//! proxy.run().await?;
//! ```

#![allow(dead_code, unused_imports, unused_variables, unreachable_patterns)]
pub mod comparator;
pub mod router;
pub mod server;

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Proxy operating mode controlling traffic routing behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProxyMode {
    /// Forward all traffic to upstream Redis (monitoring only).
    Passthrough,
    /// Dual-write to both Redis and Ferrite; compare results for divergences.
    Shadow,
    /// Gradual traffic shifting from Redis to Ferrite based on `shift_percent`.
    Migration,
    /// All traffic served by Ferrite (proxy is transparent).
    Ferrite,
}

impl Default for ProxyMode {
    fn default() -> Self {
        Self::Passthrough
    }
}

/// Configuration for the Redis proxy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyConfig {
    /// Address the proxy listens on.
    pub listen_addr: String,

    /// Upstream Redis address.
    pub redis_upstream: String,

    /// Current proxy operating mode.
    pub mode: ProxyMode,

    /// Percentage of traffic routed to Ferrite (0-100) in Migration mode.
    pub shift_percent: u8,

    /// Maximum number of concurrent proxy connections.
    pub max_connections: usize,

    /// Timeout for upstream Redis connections.
    pub upstream_timeout: Duration,

    /// Enable result comparison logging in Shadow mode.
    pub compare_results: bool,

    /// Maximum number of divergences to log per minute.
    pub max_divergence_log_rate: u32,

    /// Key patterns to migrate first (glob patterns).
    pub priority_patterns: Vec<String>,

    /// Key patterns to exclude from migration.
    pub exclude_patterns: Vec<String>,

    /// Enable automatic rollback on error rate threshold.
    pub auto_rollback: bool,

    /// Error rate threshold (0.0-1.0) that triggers automatic rollback.
    pub rollback_threshold: f64,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:6379".to_string(),
            redis_upstream: "127.0.0.1:6380".to_string(),
            mode: ProxyMode::Passthrough,
            shift_percent: 0,
            max_connections: 10_000,
            upstream_timeout: Duration::from_secs(5),
            compare_results: true,
            max_divergence_log_rate: 100,
            priority_patterns: Vec::new(),
            exclude_patterns: Vec::new(),
            auto_rollback: true,
            rollback_threshold: 0.05,
        }
    }
}

// ---------------------------------------------------------------------------
// Proxy metrics
// ---------------------------------------------------------------------------

/// Tracks proxy operational metrics.
#[derive(Debug, Default)]
pub struct ProxyMetrics {
    pub total_commands: AtomicU64,
    pub commands_to_redis: AtomicU64,
    pub commands_to_ferrite: AtomicU64,
    pub shadow_comparisons: AtomicU64,
    pub shadow_divergences: AtomicU64,
    pub errors_redis: AtomicU64,
    pub errors_ferrite: AtomicU64,
    pub bytes_proxied: AtomicU64,
    pub active_connections: AtomicU64,
    pub migration_keys_transferred: AtomicU64,
}

impl ProxyMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a snapshot of all metric counters.
    pub fn snapshot(&self) -> ProxyMetricsSnapshot {
        ProxyMetricsSnapshot {
            total_commands: self.total_commands.load(Ordering::Relaxed),
            commands_to_redis: self.commands_to_redis.load(Ordering::Relaxed),
            commands_to_ferrite: self.commands_to_ferrite.load(Ordering::Relaxed),
            shadow_comparisons: self.shadow_comparisons.load(Ordering::Relaxed),
            shadow_divergences: self.shadow_divergences.load(Ordering::Relaxed),
            errors_redis: self.errors_redis.load(Ordering::Relaxed),
            errors_ferrite: self.errors_ferrite.load(Ordering::Relaxed),
            bytes_proxied: self.bytes_proxied.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            migration_keys_transferred: self.migration_keys_transferred.load(Ordering::Relaxed),
        }
    }

    /// Calculates the current error rate for Ferrite as a ratio of errors to
    /// total commands sent to Ferrite.
    pub fn ferrite_error_rate(&self) -> f64 {
        let total = self.commands_to_ferrite.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        self.errors_ferrite.load(Ordering::Relaxed) as f64 / total as f64
    }

    /// Calculates the divergence rate in Shadow mode.
    pub fn divergence_rate(&self) -> f64 {
        let comparisons = self.shadow_comparisons.load(Ordering::Relaxed);
        if comparisons == 0 {
            return 0.0;
        }
        self.shadow_divergences.load(Ordering::Relaxed) as f64 / comparisons as f64
    }
}

/// Point-in-time snapshot of proxy metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyMetricsSnapshot {
    pub total_commands: u64,
    pub commands_to_redis: u64,
    pub commands_to_ferrite: u64,
    pub shadow_comparisons: u64,
    pub shadow_divergences: u64,
    pub errors_redis: u64,
    pub errors_ferrite: u64,
    pub bytes_proxied: u64,
    pub active_connections: u64,
    pub migration_keys_transferred: u64,
}

// ---------------------------------------------------------------------------
// Proxy state
// ---------------------------------------------------------------------------

/// Tracks the state of an ongoing migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationState {
    /// When the migration started.
    pub started_at: chrono::DateTime<chrono::Utc>,

    /// Current shift percentage.
    pub shift_percent: u8,

    /// Number of keys successfully transferred.
    pub keys_transferred: u64,

    /// Number of keys remaining.
    pub keys_remaining: u64,

    /// Whether the migration is currently paused.
    pub paused: bool,

    /// History of mode transitions.
    pub transitions: Vec<ModeTransition>,
}

/// Records a mode transition for audit purposes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModeTransition {
    pub from: ProxyMode,
    pub to: ProxyMode,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub reason: String,
}

// ---------------------------------------------------------------------------
// Core proxy
// ---------------------------------------------------------------------------

/// The Redis proxy that mediates between clients, upstream Redis, and Ferrite.
pub struct RedisProxy {
    config: Arc<RwLock<ProxyConfig>>,
    metrics: Arc<ProxyMetrics>,
    migration_state: Arc<RwLock<Option<MigrationState>>>,
}

impl RedisProxy {
    /// Creates a new proxy instance with the given configuration.
    pub fn new(config: ProxyConfig) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            metrics: Arc::new(ProxyMetrics::new()),
            migration_state: Arc::new(RwLock::new(None)),
        }
    }

    /// Returns a reference to the proxy metrics.
    pub fn metrics(&self) -> &Arc<ProxyMetrics> {
        &self.metrics
    }

    /// Returns the current proxy mode.
    pub fn mode(&self) -> ProxyMode {
        self.config.read().mode
    }

    /// Updates the proxy mode at runtime with rollback safety.
    pub fn set_mode(&self, new_mode: ProxyMode, reason: &str) {
        let mut config = self.config.write();
        let old_mode = config.mode;
        config.mode = new_mode;

        let mut state = self.migration_state.write();
        if let Some(ref mut s) = *state {
            s.transitions.push(ModeTransition {
                from: old_mode,
                to: new_mode,
                timestamp: chrono::Utc::now(),
                reason: reason.to_string(),
            });
        }
    }

    /// Sets the traffic shift percentage for Migration mode (0-100).
    pub fn set_shift_percent(&self, percent: u8) {
        let clamped = percent.min(100);
        self.config.write().shift_percent = clamped;

        if let Some(ref mut state) = *self.migration_state.write() {
            state.shift_percent = clamped;
        }
    }

    /// Returns the current shift percentage.
    pub fn shift_percent(&self) -> u8 {
        self.config.read().shift_percent
    }

    /// Starts a new migration session.
    pub fn start_migration(&self) {
        let mut state = self.migration_state.write();
        *state = Some(MigrationState {
            started_at: chrono::Utc::now(),
            shift_percent: self.config.read().shift_percent,
            keys_transferred: 0,
            keys_remaining: 0,
            paused: false,
            transitions: vec![ModeTransition {
                from: self.config.read().mode,
                to: ProxyMode::Migration,
                timestamp: chrono::Utc::now(),
                reason: "Migration started".to_string(),
            }],
        });
        self.config.write().mode = ProxyMode::Migration;
    }

    /// Pauses the current migration.
    pub fn pause_migration(&self) {
        if let Some(ref mut state) = *self.migration_state.write() {
            state.paused = true;
        }
    }

    /// Resumes a paused migration.
    pub fn resume_migration(&self) {
        if let Some(ref mut state) = *self.migration_state.write() {
            state.paused = false;
        }
    }

    /// Returns the current migration state, if any.
    pub fn migration_state(&self) -> Option<MigrationState> {
        self.migration_state.read().clone()
    }

    /// Checks whether automatic rollback should be triggered.
    pub fn should_rollback(&self) -> bool {
        let config = self.config.read();
        if !config.auto_rollback {
            return false;
        }
        self.metrics.ferrite_error_rate() > config.rollback_threshold
    }

    /// Determines which backend should handle a command based on mode and config.
    pub fn route_command(&self, key: &str) -> RoutingDecision {
        let config = self.config.read();

        // Check exclude patterns
        for pattern in &config.exclude_patterns {
            if glob_match(pattern, key) {
                return RoutingDecision::Redis;
            }
        }

        match config.mode {
            ProxyMode::Passthrough => RoutingDecision::Redis,
            ProxyMode::Shadow => RoutingDecision::Both,
            ProxyMode::Migration => {
                // Priority patterns always go to Ferrite
                for pattern in &config.priority_patterns {
                    if glob_match(pattern, key) {
                        return RoutingDecision::Ferrite;
                    }
                }
                // Hash-based deterministic routing for consistent key placement
                let hash = simple_hash(key);
                if (hash % 100) < config.shift_percent as u64 {
                    RoutingDecision::Ferrite
                } else {
                    RoutingDecision::Redis
                }
            }
            ProxyMode::Ferrite => RoutingDecision::Ferrite,
        }
    }

    /// Returns a read-only snapshot of the current configuration.
    pub fn config_snapshot(&self) -> ProxyConfig {
        self.config.read().clone()
    }
}

/// Where a command should be routed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingDecision {
    /// Route only to upstream Redis.
    Redis,
    /// Route only to Ferrite.
    Ferrite,
    /// Route to both (shadow mode comparison).
    Both,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Simple glob matching for key pattern filtering.
fn glob_match(pattern: &str, input: &str) -> bool {
    let pattern_bytes = pattern.as_bytes();
    let input_bytes = input.as_bytes();
    glob_match_recursive(pattern_bytes, input_bytes)
}

fn glob_match_recursive(pattern: &[u8], input: &[u8]) -> bool {
    match (pattern.first(), input.first()) {
        (None, None) => true,
        (Some(b'*'), _) => {
            // Try matching zero characters or skip one input character
            glob_match_recursive(&pattern[1..], input)
                || (!input.is_empty() && glob_match_recursive(pattern, &input[1..]))
        }
        (Some(b'?'), Some(_)) => glob_match_recursive(&pattern[1..], &input[1..]),
        (Some(a), Some(b)) if a == b => glob_match_recursive(&pattern[1..], &input[1..]),
        _ => false,
    }
}

/// Deterministic hash for key-based routing (FNV-1a).
fn simple_hash(key: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in key.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors that can occur during proxy operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ProxyError {
    #[error("connection to upstream Redis failed: {0}")]
    UpstreamConnectionFailed(String),

    #[error("upstream Redis timeout after {0:?}")]
    UpstreamTimeout(Duration),

    #[error("proxy listener bind failed: {0}")]
    BindFailed(String),

    #[error("migration error: {0}")]
    MigrationError(String),

    #[error(
        "shadow comparison divergence: key={key}, redis={redis_result}, ferrite={ferrite_result}"
    )]
    ShadowDivergence {
        key: String,
        redis_result: String,
        ferrite_result: String,
    },

    #[error("automatic rollback triggered: error rate {error_rate:.2}% exceeded threshold {threshold:.2}%")]
    AutoRollback { error_rate: f64, threshold: f64 },

    #[error("max connections reached: {0}")]
    MaxConnectionsReached(usize),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ProxyConfig::default();
        assert_eq!(config.mode, ProxyMode::Passthrough);
        assert_eq!(config.shift_percent, 0);
        assert_eq!(config.max_connections, 10_000);
        assert!(config.auto_rollback);
        assert!((config.rollback_threshold - 0.05).abs() < f64::EPSILON);
    }

    #[test]
    fn test_proxy_mode_transitions() {
        let proxy = RedisProxy::new(ProxyConfig::default());

        assert_eq!(proxy.mode(), ProxyMode::Passthrough);

        proxy.set_mode(ProxyMode::Shadow, "testing");
        assert_eq!(proxy.mode(), ProxyMode::Shadow);

        proxy.set_mode(ProxyMode::Ferrite, "migration complete");
        assert_eq!(proxy.mode(), ProxyMode::Ferrite);
    }

    #[test]
    fn test_shift_percent_clamping() {
        let proxy = RedisProxy::new(ProxyConfig::default());
        proxy.set_shift_percent(150);
        assert_eq!(proxy.shift_percent(), 100);

        proxy.set_shift_percent(50);
        assert_eq!(proxy.shift_percent(), 50);
    }

    #[test]
    fn test_routing_passthrough() {
        let proxy = RedisProxy::new(ProxyConfig {
            mode: ProxyMode::Passthrough,
            ..Default::default()
        });
        assert_eq!(proxy.route_command("any_key"), RoutingDecision::Redis);
    }

    #[test]
    fn test_routing_shadow() {
        let proxy = RedisProxy::new(ProxyConfig {
            mode: ProxyMode::Shadow,
            ..Default::default()
        });
        assert_eq!(proxy.route_command("any_key"), RoutingDecision::Both);
    }

    #[test]
    fn test_routing_ferrite() {
        let proxy = RedisProxy::new(ProxyConfig {
            mode: ProxyMode::Ferrite,
            ..Default::default()
        });
        assert_eq!(proxy.route_command("any_key"), RoutingDecision::Ferrite);
    }

    #[test]
    fn test_routing_migration_with_priority_patterns() {
        let proxy = RedisProxy::new(ProxyConfig {
            mode: ProxyMode::Migration,
            shift_percent: 0,
            priority_patterns: vec!["users:*".to_string()],
            ..Default::default()
        });
        assert_eq!(proxy.route_command("users:123"), RoutingDecision::Ferrite);
        assert_eq!(proxy.route_command("orders:456"), RoutingDecision::Redis);
    }

    #[test]
    fn test_routing_migration_with_exclude_patterns() {
        let proxy = RedisProxy::new(ProxyConfig {
            mode: ProxyMode::Ferrite,
            exclude_patterns: vec!["internal:*".to_string()],
            ..Default::default()
        });
        assert_eq!(
            proxy.route_command("internal:config"),
            RoutingDecision::Redis
        );
        assert_eq!(proxy.route_command("users:1"), RoutingDecision::Ferrite);
    }

    #[test]
    fn test_routing_migration_shift_percent() {
        let proxy = RedisProxy::new(ProxyConfig {
            mode: ProxyMode::Migration,
            shift_percent: 100,
            ..Default::default()
        });
        // 100% shift means all go to Ferrite
        assert_eq!(proxy.route_command("any_key"), RoutingDecision::Ferrite);

        let proxy_zero = RedisProxy::new(ProxyConfig {
            mode: ProxyMode::Migration,
            shift_percent: 0,
            ..Default::default()
        });
        // 0% shift means all go to Redis
        assert_eq!(proxy_zero.route_command("any_key"), RoutingDecision::Redis);
    }

    #[test]
    fn test_migration_lifecycle() {
        let proxy = RedisProxy::new(ProxyConfig::default());
        assert!(proxy.migration_state().is_none());

        proxy.start_migration();
        let state = proxy.migration_state().unwrap();
        assert!(!state.paused);
        assert_eq!(state.transitions.len(), 1);

        proxy.pause_migration();
        assert!(proxy.migration_state().unwrap().paused);

        proxy.resume_migration();
        assert!(!proxy.migration_state().unwrap().paused);
    }

    #[test]
    fn test_metrics_snapshot() {
        let metrics = ProxyMetrics::new();
        metrics.total_commands.store(100, Ordering::Relaxed);
        metrics.commands_to_ferrite.store(60, Ordering::Relaxed);
        metrics.errors_ferrite.store(3, Ordering::Relaxed);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.total_commands, 100);
        assert_eq!(snapshot.commands_to_ferrite, 60);
        assert_eq!(snapshot.errors_ferrite, 3);
    }

    #[test]
    fn test_ferrite_error_rate() {
        let metrics = ProxyMetrics::new();
        assert_eq!(metrics.ferrite_error_rate(), 0.0);

        metrics.commands_to_ferrite.store(100, Ordering::Relaxed);
        metrics.errors_ferrite.store(5, Ordering::Relaxed);
        assert!((metrics.ferrite_error_rate() - 0.05).abs() < f64::EPSILON);
    }

    #[test]
    fn test_divergence_rate() {
        let metrics = ProxyMetrics::new();
        assert_eq!(metrics.divergence_rate(), 0.0);

        metrics.shadow_comparisons.store(1000, Ordering::Relaxed);
        metrics.shadow_divergences.store(10, Ordering::Relaxed);
        assert!((metrics.divergence_rate() - 0.01).abs() < f64::EPSILON);
    }

    #[test]
    fn test_should_rollback() {
        let proxy = RedisProxy::new(ProxyConfig {
            auto_rollback: true,
            rollback_threshold: 0.05,
            ..Default::default()
        });

        assert!(!proxy.should_rollback());

        proxy
            .metrics
            .commands_to_ferrite
            .store(100, Ordering::Relaxed);
        proxy.metrics.errors_ferrite.store(10, Ordering::Relaxed);
        assert!(proxy.should_rollback()); // 10% > 5% threshold
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("users:*", "users:123"));
        assert!(glob_match("users:*", "users:"));
        assert!(!glob_match("users:*", "orders:123"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("user?:*", "users:123"));
        assert!(!glob_match("user?:*", "userss:123"));
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "notexact"));
    }

    #[test]
    fn test_simple_hash_deterministic() {
        let h1 = simple_hash("test_key");
        let h2 = simple_hash("test_key");
        assert_eq!(h1, h2);

        let h3 = simple_hash("different_key");
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_proxy_mode_serialization() {
        let mode = ProxyMode::Shadow;
        let json = serde_json::to_string(&mode).unwrap();
        let deserialized: ProxyMode = serde_json::from_str(&json).unwrap();
        assert_eq!(mode, deserialized);
    }

    #[test]
    fn test_config_serialization() {
        let config = ProxyConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: ProxyConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.mode, ProxyMode::Passthrough);
        assert_eq!(deserialized.max_connections, 10_000);
    }
}
