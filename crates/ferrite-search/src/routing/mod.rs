//! # Intelligent Query Routing
//!
//! Smart routing of queries to optimal data stores, indexes, and replicas based on
//! query patterns, data locality, and real-time performance metrics.
//!
//! ## Why Intelligent Routing?
//!
//! Modern applications have diverse query patterns:
//! - Point lookups vs range scans vs full-text search
//! - Read-heavy vs write-heavy workloads
//! - Latency-sensitive vs throughput-optimized queries
//!
//! Intelligent routing automatically directs queries to the best execution path:
//! - **Index Selection**: Choose between B-tree, hash, or vector indexes
//! - **Replica Selection**: Route reads to least-loaded replicas
//! - **Cache Routing**: Serve from cache when possible
//! - **Shard Routing**: Direct queries to correct data partition
//!
//! ## Quick Start
//!
//! ```no_run
//! use ferrite::routing::{QueryRouter, RoutingStrategy, QueryHint};
//!
//! let router = QueryRouter::new(config);
//!
//! // Automatic routing based on query analysis
//! let route = router.route_query("GET user:123")?;
//! println!("Using index: {:?}, target: {:?}", route.index, route.target);
//!
//! // Manual hints for optimization
//! let route = router.route_with_hints("SCAN users:*", &[
//!     QueryHint::PreferReplica,
//!     QueryHint::AllowStale(Duration::from_secs(5)),
//! ])?;
//! ```
//!
//! ## Redis CLI Commands
//!
//! ```text
//! # Query routing
//! ROUTE.QUERY <command> [args...]     # Get routing decision for a query
//! ROUTE.EXPLAIN <command> [args...]   # Explain routing decision
//!
//! # Routing configuration
//! ROUTE.STRATEGY [GET|SET] [strategy] # Get/set routing strategy
//! ROUTE.STATS                         # Show routing statistics
//! ROUTE.RULES [LIST|ADD|DEL]          # Manage routing rules
//!
//! # Index hints
//! ROUTE.PREFER.INDEX <name> <command> # Force specific index
//! ROUTE.PREFER.REPLICA <id> <command> # Force specific replica
//! ```
//!
//! ## Routing Strategies
//!
//! | Strategy | Description | Best For |
//! |----------|-------------|----------|
//! | Auto | ML-based automatic selection | General workloads |
//! | RoundRobin | Distribute across replicas | Balanced read loads |
//! | LeastLatency | Route to fastest node | Latency-sensitive |
//! | LeastConnections | Route to least busy | High concurrency |
//! | Locality | Prefer local/nearby nodes | Geo-distributed |
//! | Consistent | Hash-based routing | Cache affinity |

mod analyzer;
mod decision;
mod hints;
mod metrics;
mod rules;
mod strategy;

pub use analyzer::{QueryAnalyzer, QueryPattern, QueryType};
pub use decision::{RouteDecision, RouteTarget, RoutingContext};
pub use hints::{QueryHint, RoutingHints};
pub use metrics::{RoutingMetrics, TargetMetrics};
pub use rules::{RoutingRule, RoutingRuleSet};
pub use strategy::{RoutingStrategy, StrategyConfig};

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Query router configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RouterConfig {
    /// Enable intelligent routing
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Default routing strategy
    #[serde(default = "default_strategy")]
    pub default_strategy: String,

    /// Enable query caching
    #[serde(default = "default_cache_enabled")]
    pub cache_enabled: bool,

    /// Query cache TTL in milliseconds
    #[serde(default = "default_cache_ttl_ms")]
    pub cache_ttl_ms: u64,

    /// Enable adaptive routing (learns from performance)
    #[serde(default = "default_adaptive")]
    pub adaptive: bool,

    /// Metrics collection interval in milliseconds
    #[serde(default = "default_metrics_interval_ms")]
    pub metrics_interval_ms: u64,

    /// Maximum routing rules
    #[serde(default = "default_max_rules")]
    pub max_rules: usize,

    /// Stale read tolerance for replica routing
    #[serde(default = "default_stale_tolerance_ms")]
    pub stale_tolerance_ms: u64,
}

fn default_enabled() -> bool {
    true
}

fn default_strategy() -> String {
    "auto".to_string()
}

fn default_cache_enabled() -> bool {
    true
}

fn default_cache_ttl_ms() -> u64 {
    1000
}

fn default_adaptive() -> bool {
    true
}

fn default_metrics_interval_ms() -> u64 {
    5000
}

fn default_max_rules() -> usize {
    1000
}

fn default_stale_tolerance_ms() -> u64 {
    100
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            default_strategy: default_strategy(),
            cache_enabled: default_cache_enabled(),
            cache_ttl_ms: default_cache_ttl_ms(),
            adaptive: default_adaptive(),
            metrics_interval_ms: default_metrics_interval_ms(),
            max_rules: default_max_rules(),
            stale_tolerance_ms: default_stale_tolerance_ms(),
        }
    }
}

/// Index type for routing decisions
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IndexType {
    /// Primary hash index (default for point lookups)
    Hash,
    /// B-tree index (for range queries)
    BTree,
    /// Vector index (for similarity search)
    Vector,
    /// Full-text search index
    FullText,
    /// Geospatial index
    Geo,
    /// Time-series index
    TimeSeries,
    /// No specific index (scan)
    None,
}

/// Query router - main entry point for intelligent routing
pub struct QueryRouter {
    /// Configuration
    config: RouterConfig,
    /// Query analyzer
    analyzer: QueryAnalyzer,
    /// Current routing strategy
    strategy: RwLock<RoutingStrategy>,
    /// Routing rules
    rules: RwLock<RoutingRuleSet>,
    /// Per-target metrics
    metrics: Arc<RoutingMetrics>,
    /// Route decision cache
    cache: DashMap<String, CachedRoute>,
    /// Statistics
    stats: RoutingStats,
}

/// Cached routing decision
struct CachedRoute {
    decision: RouteDecision,
    cached_at: Instant,
}

/// Routing statistics
#[derive(Debug, Default)]
pub struct RoutingStats {
    /// Total queries routed
    pub total_queries: AtomicU64,
    /// Cache hits
    pub cache_hits: AtomicU64,
    /// Cache misses
    pub cache_misses: AtomicU64,
    /// Queries routed to primary
    pub primary_routes: AtomicU64,
    /// Queries routed to replicas
    pub replica_routes: AtomicU64,
    /// Queries using hash index
    pub hash_index_routes: AtomicU64,
    /// Queries using btree index
    pub btree_index_routes: AtomicU64,
    /// Queries using vector index
    pub vector_index_routes: AtomicU64,
    /// Adaptive routing adjustments
    pub adaptive_adjustments: AtomicU64,
}

impl QueryRouter {
    /// Create a new query router
    pub fn new(config: RouterConfig) -> Self {
        let strategy = match config.default_strategy.as_str() {
            "round_robin" => RoutingStrategy::RoundRobin,
            "least_latency" => RoutingStrategy::LeastLatency,
            "least_connections" => RoutingStrategy::LeastConnections,
            "locality" => RoutingStrategy::Locality,
            "consistent" => RoutingStrategy::ConsistentHash,
            _ => RoutingStrategy::Auto,
        };

        Self {
            config,
            analyzer: QueryAnalyzer::new(),
            strategy: RwLock::new(strategy),
            rules: RwLock::new(RoutingRuleSet::new()),
            metrics: Arc::new(RoutingMetrics::new()),
            cache: DashMap::new(),
            stats: RoutingStats::default(),
        }
    }

    /// Route a query to the optimal target
    pub fn route_query(&self, command: &str, args: &[Bytes]) -> RouteDecision {
        self.stats.total_queries.fetch_add(1, Ordering::Relaxed);

        // Check cache first
        let cache_key = self.make_cache_key(command, args);
        if self.config.cache_enabled {
            if let Some(cached) = self.cache.get(&cache_key) {
                if cached.cached_at.elapsed().as_millis() < self.config.cache_ttl_ms as u128 {
                    self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                    return cached.decision.clone();
                }
            }
            self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
        }

        // Analyze the query
        let pattern = self.analyzer.analyze(command, args);

        // Check routing rules
        let rules = self.rules.read();
        if let Some(rule_decision) = rules.match_query(command, args, &pattern) {
            return self.finalize_route(rule_decision, &cache_key);
        }
        drop(rules);

        // Apply routing strategy
        let strategy = self.strategy.read().clone();
        let decision = self.apply_strategy(&pattern, &strategy);

        self.finalize_route(decision, &cache_key)
    }

    /// Route with explicit hints
    pub fn route_with_hints(
        &self,
        command: &str,
        args: &[Bytes],
        hints: &[QueryHint],
    ) -> RouteDecision {
        let mut decision = self.route_query(command, args);

        // Apply hints
        for hint in hints {
            match hint {
                QueryHint::PreferPrimary => {
                    decision.target = RouteTarget::Primary;
                }
                QueryHint::PreferReplica => {
                    if decision.target == RouteTarget::Primary {
                        decision.target = RouteTarget::AnyReplica;
                    }
                }
                QueryHint::PreferLocal => {
                    decision.prefer_local = true;
                }
                QueryHint::ForceIndex(idx) => {
                    decision.index = idx.clone();
                }
                QueryHint::AllowStale(duration) => {
                    decision.stale_tolerance = Some(*duration);
                }
                QueryHint::RequireFresh => {
                    decision.stale_tolerance = None;
                    decision.target = RouteTarget::Primary;
                }
                QueryHint::TargetNode(node_id) => {
                    decision.target = RouteTarget::SpecificNode(node_id.clone());
                }
                QueryHint::Timeout(_) => {
                    // Timeout is handled at execution layer, not routing
                }
                QueryHint::Priority(p) => {
                    decision.priority = *p;
                }
                QueryHint::RetryCount(_) => {
                    // Retry count is handled at execution layer
                }
                QueryHint::NoCache => {
                    decision.cacheable = false;
                }
            }
        }

        decision
    }

    /// Explain routing decision for a query
    pub fn explain(&self, command: &str, args: &[Bytes]) -> RoutingExplanation {
        let pattern = self.analyzer.analyze(command, args);
        let strategy = self.strategy.read().clone();
        let decision = self.route_query(command, args);
        let query_type = pattern.query_type.clone();

        RoutingExplanation {
            query_type,
            detected_pattern: pattern,
            strategy_used: strategy,
            decision,
            rules_matched: vec![],
            metrics_considered: self.metrics.snapshot(),
        }
    }

    /// Get routing statistics
    pub fn stats(&self) -> &RoutingStats {
        &self.stats
    }

    /// Set routing strategy
    pub fn set_strategy(&self, strategy: RoutingStrategy) {
        *self.strategy.write() = strategy;
    }

    /// Get current strategy
    pub fn get_strategy(&self) -> RoutingStrategy {
        self.strategy.read().clone()
    }

    /// Add a routing rule
    pub fn add_rule(&self, rule: RoutingRule) -> Result<(), RoutingError> {
        let mut rules = self.rules.write();
        if rules.len() >= self.config.max_rules {
            return Err(RoutingError::TooManyRules);
        }
        rules.add(rule);
        Ok(())
    }

    /// Remove a routing rule
    pub fn remove_rule(&self, rule_id: &str) -> bool {
        self.rules.write().remove(rule_id)
    }

    /// Record execution metrics for adaptive routing
    pub fn record_execution(&self, target: &RouteTarget, latency: Duration, success: bool) {
        self.metrics.record(target, latency, success);

        if self.config.adaptive {
            self.maybe_adjust_routing();
        }
    }

    /// Clear the route cache
    pub fn clear_cache(&self) {
        self.cache.clear();
    }

    // Internal helpers

    fn make_cache_key(&self, command: &str, args: &[Bytes]) -> String {
        let mut key = command.to_uppercase();
        // Only include first few args for cache key to avoid memory explosion
        for arg in args.iter().take(3) {
            key.push(':');
            key.push_str(&String::from_utf8_lossy(arg));
        }
        if args.len() > 3 {
            key.push_str(":...");
        }
        key
    }

    fn apply_strategy(&self, pattern: &QueryPattern, strategy: &RoutingStrategy) -> RouteDecision {
        // Determine index type based on query pattern
        let index = match pattern.query_type {
            QueryType::PointLookup => IndexType::Hash,
            QueryType::RangeScan => IndexType::BTree,
            QueryType::VectorSearch => IndexType::Vector,
            QueryType::FullTextSearch => IndexType::FullText,
            QueryType::GeoQuery => IndexType::Geo,
            QueryType::TimeSeriesQuery => IndexType::TimeSeries,
            QueryType::Aggregation => IndexType::BTree,
            QueryType::Write => IndexType::Hash,
            QueryType::Unknown => IndexType::None,
        };

        // Determine target based on strategy and query type
        let target = match (strategy, pattern.is_read_only) {
            (_, false) => RouteTarget::Primary, // Writes always go to primary
            (RoutingStrategy::Auto, true) => self.auto_select_target(pattern),
            (RoutingStrategy::RoundRobin, true) => RouteTarget::AnyReplica,
            (RoutingStrategy::LeastLatency, true) => self
                .metrics
                .least_latency_target()
                .unwrap_or(RouteTarget::Primary),
            (RoutingStrategy::LeastConnections, true) => self
                .metrics
                .least_connections_target()
                .unwrap_or(RouteTarget::Primary),
            (RoutingStrategy::Locality, true) => RouteTarget::LocalPreferred,
            (RoutingStrategy::ConsistentHash, true) => RouteTarget::ConsistentHash,
            (RoutingStrategy::Primary, true) => RouteTarget::Primary,
            (RoutingStrategy::Random, true) => RouteTarget::AnyReplica,
            (RoutingStrategy::Weighted, true) => RouteTarget::AnyReplica,
            (RoutingStrategy::Custom(_), true) => self.auto_select_target(pattern),
        };

        let decision = RouteDecision { index, target, ..Default::default() };

        // Track index usage
        match decision.index {
            IndexType::Hash => self.stats.hash_index_routes.fetch_add(1, Ordering::Relaxed),
            IndexType::BTree => self
                .stats
                .btree_index_routes
                .fetch_add(1, Ordering::Relaxed),
            IndexType::Vector => self
                .stats
                .vector_index_routes
                .fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };

        // Track target type
        match decision.target {
            RouteTarget::Primary => self.stats.primary_routes.fetch_add(1, Ordering::Relaxed),
            _ => self.stats.replica_routes.fetch_add(1, Ordering::Relaxed),
        };

        decision
    }

    fn auto_select_target(&self, pattern: &QueryPattern) -> RouteTarget {
        // Use ML-like heuristics for auto selection
        if pattern.estimated_cost > 100 {
            // Heavy queries go to replicas to offload primary
            RouteTarget::AnyReplica
        } else if pattern.key_count > 10 {
            // Multi-key operations may benefit from specific routing
            RouteTarget::ConsistentHash
        } else {
            // Simple queries can use least latency
            self.metrics
                .least_latency_target()
                .unwrap_or(RouteTarget::Primary)
        }
    }

    fn finalize_route(&self, decision: RouteDecision, cache_key: &str) -> RouteDecision {
        // Cache the decision
        if self.config.cache_enabled {
            self.cache.insert(
                cache_key.to_string(),
                CachedRoute {
                    decision: decision.clone(),
                    cached_at: Instant::now(),
                },
            );
        }

        decision
    }

    fn maybe_adjust_routing(&self) {
        // Adaptive routing adjustment based on metrics
        let snapshot = self.metrics.snapshot();

        // Check if any target is significantly slower
        if let Some(slowest) = snapshot.slowest_target() {
            if let Some(fastest) = snapshot.fastest_target() {
                let ratio = slowest.avg_latency_ms / fastest.avg_latency_ms.max(1.0);
                if ratio > 3.0 {
                    // Significant disparity - adjust routing
                    self.stats
                        .adaptive_adjustments
                        .fetch_add(1, Ordering::Relaxed);
                    // Could trigger strategy change here
                }
            }
        }
    }
}

/// Routing explanation for debugging
#[derive(Clone, Debug)]
pub struct RoutingExplanation {
    /// Type of query detected
    pub query_type: QueryType,
    /// Full query pattern analysis
    pub detected_pattern: QueryPattern,
    /// Strategy used for routing
    pub strategy_used: RoutingStrategy,
    /// Final routing decision
    pub decision: RouteDecision,
    /// Rules that matched
    pub rules_matched: Vec<String>,
    /// Metrics considered
    pub metrics_considered: metrics::MetricsSnapshot,
}

/// Routing errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum RoutingError {
    #[error("Too many routing rules")]
    TooManyRules,

    #[error("Invalid rule: {0}")]
    InvalidRule(String),

    #[error("Target not available: {0}")]
    TargetUnavailable(String),

    #[error("Routing disabled")]
    Disabled,
}

/// Routing info for INFO command
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RoutingInfo {
    /// Whether routing is enabled
    pub enabled: bool,
    /// Current strategy
    pub strategy: String,
    /// Total queries routed
    pub total_queries: u64,
    /// Cache hit rate
    pub cache_hit_rate: f64,
    /// Active routing rules
    pub rule_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_router_config_default() {
        let config = RouterConfig::default();
        assert!(config.enabled);
        assert_eq!(config.default_strategy, "auto");
    }

    #[test]
    fn test_query_router_creation() {
        let router = QueryRouter::new(RouterConfig::default());
        assert_eq!(router.get_strategy(), RoutingStrategy::Auto);
    }

    #[test]
    fn test_route_point_lookup() {
        let router = QueryRouter::new(RouterConfig::default());
        let decision = router.route_query("GET", &[Bytes::from("key1")]);
        assert_eq!(decision.index, IndexType::Hash);
    }

    #[test]
    fn test_route_with_hints() {
        let router = QueryRouter::new(RouterConfig::default());
        let decision =
            router.route_with_hints("GET", &[Bytes::from("key1")], &[QueryHint::PreferPrimary]);
        assert_eq!(decision.target, RouteTarget::Primary);
    }

    #[test]
    fn test_set_strategy() {
        let router = QueryRouter::new(RouterConfig::default());
        router.set_strategy(RoutingStrategy::RoundRobin);
        assert_eq!(router.get_strategy(), RoutingStrategy::RoundRobin);
    }
}
