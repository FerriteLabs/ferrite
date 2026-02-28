//! Zero-Config Smart Proxy
//!
//! Transparent reverse proxy that sits in front of existing Redis deployments
//! and adds Ferrite capabilities (tiered storage, vector search, observability)
//! without modifying application code.
#![allow(dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::RwLock;
use serde::Serialize;
use thiserror::Error;

// ── Errors ───────────────────────────────────────────────────────────────────

/// Errors from the smart proxy layer.
#[derive(Debug, Error)]
pub enum SmartProxyError {
    /// Failed to bind the listening socket.
    #[error("bind failed on {0}: {1}")]
    BindFailed(SocketAddr, String),

    /// Upstream Redis/Ferrite instance is unreachable.
    #[error("upstream unreachable at {0}: {1}")]
    UpstreamUnreachable(SocketAddr, String),

    /// Topology discovery failed.
    #[error("discovery failed: {0}")]
    DiscoveryFailed(String),

    /// Authentication with upstream failed.
    #[error("upstream auth failed: {0}")]
    AuthFailed(String),

    /// Maximum client limit reached.
    #[error("max clients reached ({0})")]
    MaxClients(u32),
}

// ── Configuration ────────────────────────────────────────────────────────────

/// Feature toggles for the smart proxy.
#[derive(Debug, Clone)]
pub struct SmartProxyFeatures {
    /// Enable tiered-storage interception for hot/cold data.
    pub enable_tiering: bool,
    /// Enable local vector search for VECTOR.* commands.
    pub enable_vector_search: bool,
    /// Enable observability passthrough (OBSERVE.* commands).
    pub enable_observability: bool,
    /// Enable local read-through caching.
    pub enable_caching: bool,
    /// Enable Prometheus metrics endpoint.
    pub enable_metrics: bool,
}

impl Default for SmartProxyFeatures {
    fn default() -> Self {
        Self {
            enable_tiering: true,
            enable_vector_search: true,
            enable_observability: true,
            enable_caching: true,
            enable_metrics: true,
        }
    }
}

/// Configuration for the smart proxy.
#[derive(Debug, Clone)]
pub struct SmartProxyConfig {
    /// Address the proxy listens on.
    pub listen_addr: SocketAddr,
    /// Upstream Redis/Ferrite address.
    pub upstream_addr: SocketAddr,
    /// Optional password for upstream authentication.
    pub upstream_password: Option<String>,
    /// Feature toggles.
    pub features: SmartProxyFeatures,
    /// Whether to auto-discover upstream topology.
    pub auto_discover: bool,
    /// Interval between topology discovery probes.
    pub discovery_interval: Duration,
    /// Maximum concurrent client connections.
    pub max_clients: u32,
    /// Enable read-through caching for GET/MGET.
    pub read_through_cache: bool,
    /// TTL for locally cached entries.
    pub cache_ttl: Duration,
}

impl Default for SmartProxyConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:6380".parse().expect("valid default addr"),
            upstream_addr: "127.0.0.1:6379".parse().expect("valid default addr"),
            upstream_password: None,
            features: SmartProxyFeatures::default(),
            auto_discover: true,
            discovery_interval: Duration::from_secs(30),
            max_clients: 10_000,
            read_through_cache: true,
            cache_ttl: Duration::from_secs(60),
        }
    }
}

// ── Intercept Action ─────────────────────────────────────────────────────────

/// Describes how the proxy should handle a given command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InterceptAction {
    /// Pass the command through to the upstream unmodified.
    Forward,
    /// Handle the command entirely within Ferrite (e.g. VECTOR.*, OBSERVE.*).
    HandleLocally,
    /// Forward the command to upstream and cache the response locally.
    CacheAndForward,
    /// Forward the command but record it in the heatmap/metrics.
    Observe,
}

// ── Upstream Topology ────────────────────────────────────────────────────────

/// Information about a single cluster node.
#[derive(Debug, Clone, Serialize)]
pub struct ClusterNodeInfo {
    /// Node address.
    pub addr: SocketAddr,
    /// Node role.
    pub role: ClusterNodeRole,
    /// Hash-slot ranges assigned to this node.
    pub slots: Vec<(u16, u16)>,
}

/// Role of a cluster node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ClusterNodeRole {
    Primary,
    Replica,
}

/// Discovered upstream topology.
#[derive(Debug, Clone)]
pub enum UpstreamTopology {
    /// Single standalone Redis/Ferrite instance.
    Standalone { addr: SocketAddr },
    /// Redis Sentinel deployment.
    Sentinel {
        master: SocketAddr,
        sentinels: Vec<SocketAddr>,
        replicas: Vec<SocketAddr>,
    },
    /// Redis Cluster deployment.
    Cluster { nodes: Vec<ClusterNodeInfo> },
}

impl std::fmt::Display for UpstreamTopology {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Standalone { addr } => write!(f, "standalone({})", addr),
            Self::Sentinel {
                master,
                sentinels,
                replicas,
            } => {
                write!(
                    f,
                    "sentinel(master={}, sentinels={}, replicas={})",
                    master,
                    sentinels.len(),
                    replicas.len()
                )
            }
            Self::Cluster { nodes } => write!(f, "cluster(nodes={})", nodes.len()),
        }
    }
}

// ── Statistics ───────────────────────────────────────────────────────────────

/// Runtime statistics for the smart proxy.
#[derive(Debug, Clone, Serialize)]
pub struct SmartProxyStats {
    /// Total commands forwarded to upstream.
    pub commands_forwarded: u64,
    /// Total commands intercepted and handled locally.
    pub commands_intercepted: u64,
    /// Total commands that resulted in a cache hit/store.
    pub commands_cached: u64,
    /// Cache hit rate (0.0–1.0).
    pub cache_hit_rate: f64,
    /// Average upstream latency in microseconds.
    pub upstream_latency_avg_us: u64,
    /// Currently connected clients.
    pub clients_connected: u32,
    /// Seconds since the proxy started.
    pub uptime_secs: u64,
    /// Human-readable description of discovered topology.
    pub discovered_topology: String,
}

// ── Proxy Handle ─────────────────────────────────────────────────────────────

/// Handle returned when the proxy is started.
#[derive(Debug)]
pub struct ProxyHandle {
    /// Address the proxy is listening on.
    pub listen_addr: SocketAddr,
    /// Instant the proxy started.
    pub started_at: Instant,
}

// ── Smart Proxy ──────────────────────────────────────────────────────────────

/// Internal proxy state.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ProxyState {
    Stopped,
    Running,
}

/// Zero-config smart proxy that adds Ferrite capabilities to any Redis deployment.
pub struct SmartProxy {
    config: SmartProxyConfig,
    state: RwLock<ProxyState>,
    topology: RwLock<Option<UpstreamTopology>>,
    intercept_rules: RwLock<HashMap<String, InterceptAction>>,
    cache: RwLock<HashMap<String, (Bytes, Instant)>>,
    started_at: RwLock<Option<Instant>>,

    // Atomic counters
    commands_forwarded: AtomicU64,
    commands_intercepted: AtomicU64,
    commands_cached: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    upstream_latency_total_us: AtomicU64,
    upstream_requests: AtomicU64,
    clients_connected: AtomicU64,
}

impl SmartProxy {
    /// Create a new smart proxy with the given configuration.
    pub fn new(config: SmartProxyConfig) -> Self {
        let mut intercept_rules = HashMap::new();
        Self::populate_default_rules(&mut intercept_rules, &config.features);

        Self {
            config,
            state: RwLock::new(ProxyState::Stopped),
            topology: RwLock::new(None),
            intercept_rules: RwLock::new(intercept_rules),
            cache: RwLock::new(HashMap::new()),
            started_at: RwLock::new(None),
            commands_forwarded: AtomicU64::new(0),
            commands_intercepted: AtomicU64::new(0),
            commands_cached: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            upstream_latency_total_us: AtomicU64::new(0),
            upstream_requests: AtomicU64::new(0),
            clients_connected: AtomicU64::new(0),
        }
    }

    /// Start accepting connections on the configured listen address.
    pub fn start(&self) -> Result<ProxyHandle, SmartProxyError> {
        let mut state = self.state.write();
        if *state == ProxyState::Running {
            return Ok(ProxyHandle {
                listen_addr: self.config.listen_addr,
                started_at: self.started_at.read().unwrap_or(Instant::now()),
            });
        }

        // Validate upstream is reachable (placeholder — real impl would probe)
        // For now, just transition state.
        *state = ProxyState::Running;
        let now = Instant::now();
        *self.started_at.write() = Some(now);

        // If auto-discover is enabled, do an initial discovery
        if self.config.auto_discover {
            if let Ok(topo) = self.discover_upstream() {
                *self.topology.write() = Some(topo);
            }
        }

        Ok(ProxyHandle {
            listen_addr: self.config.listen_addr,
            started_at: now,
        })
    }

    /// Stop the proxy gracefully.
    pub fn stop(&self) -> Result<(), SmartProxyError> {
        let mut state = self.state.write();
        *state = ProxyState::Stopped;
        *self.started_at.write() = None;
        Ok(())
    }

    /// Decide how to handle a given command.
    pub fn intercept_command(&self, cmd: &str, _args: &[Bytes]) -> InterceptAction {
        let upper = cmd.to_uppercase();

        // Check custom rules first
        let rules = self.intercept_rules.read();
        if let Some(action) = rules.get(&upper) {
            return action.clone();
        }

        // Prefix-based classification
        Self::classify_command(&upper, &self.config.features)
    }

    /// Handle a command locally using Ferrite features.
    ///
    /// Returns `Some(response)` if the command can be handled, `None` otherwise.
    pub fn handle_local(&self, cmd: &str, _args: &[Bytes]) -> Option<Bytes> {
        let upper = cmd.to_uppercase();

        // Ferrite-specific command families handled locally
        if upper.starts_with("VECTOR.")
            || upper.starts_with("OBSERVE.")
            || upper.starts_with("TIERING.")
            || upper.starts_with("API.")
        {
            self.commands_intercepted.fetch_add(1, Ordering::Relaxed);
            // Placeholder: real implementation would dispatch to Ferrite subsystems
            Some(Bytes::from_static(b"+OK\r\n"))
        } else {
            None
        }
    }

    /// Discover the upstream Redis topology.
    pub fn discover_upstream(&self) -> Result<UpstreamTopology, SmartProxyError> {
        // Placeholder: real implementation would issue INFO, CLUSTER INFO,
        // SENTINEL commands to probe the topology.
        // Default to standalone for now.
        Ok(UpstreamTopology::Standalone {
            addr: self.config.upstream_addr,
        })
    }

    /// Return current proxy statistics.
    pub fn stats(&self) -> SmartProxyStats {
        let forwarded = self.commands_forwarded.load(Ordering::Relaxed);
        let intercepted = self.commands_intercepted.load(Ordering::Relaxed);
        let cached = self.commands_cached.load(Ordering::Relaxed);
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total_cache = hits + misses;
        let hit_rate = if total_cache > 0 {
            hits as f64 / total_cache as f64
        } else {
            0.0
        };

        let total_latency = self.upstream_latency_total_us.load(Ordering::Relaxed);
        let total_requests = self.upstream_requests.load(Ordering::Relaxed);
        let avg_latency = if total_requests > 0 {
            total_latency / total_requests
        } else {
            0
        };

        let uptime = self
            .started_at
            .read()
            .map(|t| t.elapsed().as_secs())
            .unwrap_or(0);

        let topo_str = self
            .topology
            .read()
            .as_ref()
            .map(|t| t.to_string())
            .unwrap_or_else(|| "unknown".to_string());

        SmartProxyStats {
            commands_forwarded: forwarded,
            commands_intercepted: intercepted,
            commands_cached: cached,
            cache_hit_rate: hit_rate,
            upstream_latency_avg_us: avg_latency,
            clients_connected: self.clients_connected.load(Ordering::Relaxed) as u32,
            uptime_secs: uptime,
            discovered_topology: topo_str,
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /// Classify a command into an intercept action based on prefix rules.
    fn classify_command(cmd: &str, features: &SmartProxyFeatures) -> InterceptAction {
        // Always intercept: Ferrite-specific commands
        if cmd.starts_with("VECTOR.") && features.enable_vector_search {
            return InterceptAction::HandleLocally;
        }
        if cmd.starts_with("OBSERVE.") && features.enable_observability {
            return InterceptAction::HandleLocally;
        }
        if cmd.starts_with("TIERING.") && features.enable_tiering {
            return InterceptAction::HandleLocally;
        }
        if cmd.starts_with("API.") || cmd.starts_with("PROXY.") {
            return InterceptAction::HandleLocally;
        }

        // Cache & forward: read commands when caching is enabled
        if features.enable_caching {
            match cmd {
                "GET" | "MGET" | "HGET" | "HGETALL" | "LRANGE" | "SMEMBERS" | "ZRANGE" => {
                    return InterceptAction::CacheAndForward;
                }
                _ => {}
            }
        }

        // Observe & forward: write commands for heatmap tracking
        if features.enable_observability {
            match cmd {
                "SET" | "DEL" | "HSET" | "HDEL" | "LPUSH" | "RPUSH" | "LPOP" | "RPOP" | "SADD"
                | "SREM" | "ZADD" | "ZREM" | "SETEX" | "PSETEX" | "MSET" => {
                    return InterceptAction::Observe;
                }
                _ => {}
            }
        }

        // Pure forward: transactional, pub/sub, auth, etc.
        InterceptAction::Forward
    }

    /// Populate default intercept rules from feature flags.
    fn populate_default_rules(
        rules: &mut HashMap<String, InterceptAction>,
        features: &SmartProxyFeatures,
    ) {
        if features.enable_vector_search {
            for cmd in &[
                "VECTOR.ADD",
                "VECTOR.SEARCH",
                "VECTOR.DELETE",
                "VECTOR.INFO",
            ] {
                rules.insert(cmd.to_string(), InterceptAction::HandleLocally);
            }
        }
        if features.enable_observability {
            for cmd in &["OBSERVE.HEATMAP", "OBSERVE.STATS", "OBSERVE.ENABLE"] {
                rules.insert(cmd.to_string(), InterceptAction::HandleLocally);
            }
        }
        if features.enable_tiering {
            for cmd in &[
                "TIERING.STATUS",
                "TIERING.PROMOTE",
                "TIERING.DEMOTE",
                "TIERING.STATS",
            ] {
                rules.insert(cmd.to_string(), InterceptAction::HandleLocally);
            }
        }
    }

    /// Flush the local cache.
    pub fn flush_cache(&self) -> usize {
        let mut cache = self.cache.write();
        let count = cache.len();
        cache.clear();
        count
    }

    /// Return the current cache size.
    pub fn cache_size(&self) -> usize {
        self.cache.read().len()
    }

    /// Add a custom intercept rule.
    pub fn add_intercept_rule(&self, pattern: String, action: InterceptAction) {
        self.intercept_rules.write().insert(pattern, action);
    }

    /// Remove a custom intercept rule.
    pub fn remove_intercept_rule(&self, pattern: &str) -> bool {
        self.intercept_rules.write().remove(pattern).is_some()
    }

    /// List all active intercept rules.
    pub fn list_intercept_rules(&self) -> Vec<(String, InterceptAction)> {
        self.intercept_rules
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_proxy() -> SmartProxy {
        SmartProxy::new(SmartProxyConfig::default())
    }

    #[test]
    fn test_start_stop() {
        let proxy = default_proxy();
        let handle = proxy.start().expect("start");
        assert_eq!(handle.listen_addr, proxy.config.listen_addr);
        proxy.stop().expect("stop");
    }

    #[test]
    fn test_intercept_vector_commands() {
        let proxy = default_proxy();
        assert_eq!(
            proxy.intercept_command("VECTOR.SEARCH", &[]),
            InterceptAction::HandleLocally
        );
    }

    #[test]
    fn test_intercept_cache_commands() {
        let proxy = default_proxy();
        assert_eq!(
            proxy.intercept_command("GET", &[]),
            InterceptAction::CacheAndForward
        );
        assert_eq!(
            proxy.intercept_command("MGET", &[]),
            InterceptAction::CacheAndForward
        );
    }

    #[test]
    fn test_intercept_observe_commands() {
        let proxy = default_proxy();
        assert_eq!(
            proxy.intercept_command("SET", &[]),
            InterceptAction::Observe
        );
        assert_eq!(
            proxy.intercept_command("DEL", &[]),
            InterceptAction::Observe
        );
    }

    #[test]
    fn test_intercept_forward_commands() {
        let proxy = default_proxy();
        assert_eq!(
            proxy.intercept_command("MULTI", &[]),
            InterceptAction::Forward
        );
        assert_eq!(
            proxy.intercept_command("AUTH", &[]),
            InterceptAction::Forward
        );
    }

    #[test]
    fn test_handle_local_vector() {
        let proxy = default_proxy();
        assert!(proxy.handle_local("VECTOR.SEARCH", &[]).is_some());
        assert!(proxy.handle_local("GET", &[]).is_none());
    }

    #[test]
    fn test_stats_initial() {
        let proxy = default_proxy();
        let stats = proxy.stats();
        assert_eq!(stats.commands_forwarded, 0);
        assert_eq!(stats.commands_intercepted, 0);
    }

    #[test]
    fn test_cache_flush() {
        let proxy = default_proxy();
        proxy
            .cache
            .write()
            .insert("key".to_string(), (Bytes::from("val"), Instant::now()));
        assert_eq!(proxy.cache_size(), 1);
        assert_eq!(proxy.flush_cache(), 1);
        assert_eq!(proxy.cache_size(), 0);
    }

    #[test]
    fn test_custom_intercept_rules() {
        let proxy = default_proxy();
        proxy.add_intercept_rule("CUSTOM.CMD".to_string(), InterceptAction::HandleLocally);
        assert_eq!(
            proxy.intercept_command("CUSTOM.CMD", &[]),
            InterceptAction::HandleLocally
        );
        assert!(proxy.remove_intercept_rule("CUSTOM.CMD"));
        assert_eq!(
            proxy.intercept_command("CUSTOM.CMD", &[]),
            InterceptAction::Forward
        );
    }

    #[test]
    fn test_discover_upstream() {
        let proxy = default_proxy();
        let topo = proxy.discover_upstream().expect("discover");
        match topo {
            UpstreamTopology::Standalone { addr } => {
                assert_eq!(addr, proxy.config.upstream_addr);
            }
            _ => panic!("expected standalone"),
        }
    }

    #[test]
    fn test_features_disabled_caching() {
        let config = SmartProxyConfig {
            features: SmartProxyFeatures {
                enable_caching: false,
                ..SmartProxyFeatures::default()
            },
            ..SmartProxyConfig::default()
        };
        let proxy = SmartProxy::new(config);
        // With caching disabled, GET should forward instead of cache
        assert_eq!(
            proxy.intercept_command("GET", &[]),
            InterceptAction::Forward
        );
    }
}
