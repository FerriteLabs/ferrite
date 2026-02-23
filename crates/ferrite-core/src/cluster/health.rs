//! Cluster health scoring and monitoring.
//!
//! Provides per-node health scoring based on latency, error rate, and memory
//! usage. Integrates with CLUSTER INFO for migration progress and overall
//! health score. Supports automatic removal of unhealthy nodes.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tracing::warn;

use super::{ClusterManager, NodeId};

/// Health score range [0.0, 1.0] where 1.0 is perfectly healthy.
pub type HealthScore = f64;

/// Weights for the health score components.
#[derive(Debug, Clone)]
pub struct HealthWeights {
    /// Weight for latency score (0.0–1.0).
    pub latency: f64,
    /// Weight for error rate score (0.0–1.0).
    pub error_rate: f64,
    /// Weight for memory usage score (0.0–1.0).
    pub memory: f64,
}

impl Default for HealthWeights {
    fn default() -> Self {
        Self {
            latency: 0.4,
            error_rate: 0.4,
            memory: 0.2,
        }
    }
}

/// Configuration for the health monitor.
#[derive(Debug, Clone)]
pub struct HealthConfig {
    /// Interval between health checks.
    pub check_interval: Duration,
    /// Latency threshold (anything above this gets score 0).
    pub latency_threshold: Duration,
    /// Error rate threshold (0.0–1.0; above this → score 0).
    pub error_rate_threshold: f64,
    /// Memory usage fraction threshold (0.0–1.0; above this → score 0).
    pub memory_threshold: f64,
    /// Below this score a node is considered unhealthy.
    pub unhealthy_threshold: f64,
    /// Consecutive unhealthy checks before automatic removal.
    pub auto_remove_after: u32,
    /// Enable automatic unhealthy node removal.
    pub auto_remove_enabled: bool,
    /// Health score component weights.
    pub weights: HealthWeights,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(5),
            latency_threshold: Duration::from_millis(500),
            error_rate_threshold: 0.1,
            memory_threshold: 0.95,
            unhealthy_threshold: 0.3,
            auto_remove_after: 6,
            auto_remove_enabled: false,
            weights: HealthWeights::default(),
        }
    }
}

/// Raw metrics for a single node.
#[derive(Debug, Clone)]
pub struct NodeMetrics {
    /// Average round-trip latency to this node (gossip PING/PONG).
    pub avg_latency: Duration,
    /// Error rate (0.0–1.0) over the last observation window.
    pub error_rate: f64,
    /// Memory usage fraction (0.0–1.0).
    pub memory_usage: f64,
    /// When these metrics were last updated.
    pub last_updated: Instant,
}

impl Default for NodeMetrics {
    fn default() -> Self {
        Self {
            avg_latency: Duration::ZERO,
            error_rate: 0.0,
            memory_usage: 0.0,
            last_updated: Instant::now(),
        }
    }
}

/// Per-node health state.
#[derive(Debug, Clone)]
pub struct NodeHealthState {
    /// Node identifier.
    pub node_id: NodeId,
    /// Computed health score [0.0, 1.0].
    pub score: HealthScore,
    /// Raw metrics feeding the score.
    pub metrics: NodeMetrics,
    /// Consecutive unhealthy check count.
    pub consecutive_unhealthy: u32,
    /// Whether the node is considered healthy right now.
    pub healthy: bool,
}

/// Cluster-wide health summary (enriches CLUSTER INFO output).
#[derive(Debug, Clone)]
pub struct ClusterHealthSummary {
    /// Overall cluster health score (average of all node scores).
    pub cluster_score: HealthScore,
    /// Number of healthy nodes.
    pub healthy_nodes: usize,
    /// Number of unhealthy nodes.
    pub unhealthy_nodes: usize,
    /// Total active slot migrations.
    pub active_migrations: usize,
    /// Slots in transitional state (migrating or importing).
    pub transitional_slots: usize,
    /// Per-node health scores.
    pub node_scores: HashMap<NodeId, HealthScore>,
}

/// Cluster health monitor.
pub struct HealthMonitor {
    config: HealthConfig,
    cluster: Arc<ClusterManager>,
    /// Per-node health tracking.
    node_health: RwLock<HashMap<NodeId, NodeHealthState>>,
    /// Nodes that have been auto-removed.
    removed_nodes: RwLock<Vec<(NodeId, Instant)>>,
}

impl HealthMonitor {
    /// Create a new health monitor.
    pub fn new(config: HealthConfig, cluster: Arc<ClusterManager>) -> Self {
        Self {
            config,
            cluster,
            node_health: RwLock::new(HashMap::new()),
            removed_nodes: RwLock::new(Vec::new()),
        }
    }

    /// Report fresh metrics for a node. Recomputes the health score.
    pub fn report_metrics(&self, node_id: &NodeId, metrics: NodeMetrics) {
        let score = self.compute_score(&metrics);
        let healthy = score >= self.config.unhealthy_threshold;

        let mut health_map = self.node_health.write();
        let entry = health_map
            .entry(node_id.clone())
            .or_insert_with(|| NodeHealthState {
                node_id: node_id.clone(),
                score: 1.0,
                metrics: NodeMetrics::default(),
                consecutive_unhealthy: 0,
                healthy: true,
            });

        entry.metrics = metrics;
        entry.score = score;

        if healthy {
            entry.consecutive_unhealthy = 0;
            entry.healthy = true;
        } else {
            entry.consecutive_unhealthy += 1;
            entry.healthy = false;
        }
    }

    /// Run a health check cycle: evaluate all nodes and optionally remove
    /// persistently unhealthy ones.
    pub fn check_health(&self) -> Vec<NodeId> {
        let mut removed = Vec::new();

        if !self.config.auto_remove_enabled {
            return removed;
        }

        let health_map = self.node_health.read();
        let to_remove: Vec<NodeId> = health_map
            .values()
            .filter(|h| !h.healthy && h.consecutive_unhealthy >= self.config.auto_remove_after)
            .map(|h| h.node_id.clone())
            .collect();
        drop(health_map);

        for node_id in to_remove {
            warn!(node = %node_id, "Auto-removing unhealthy node from cluster");
            self.cluster.mark_node_failed(&node_id);
            self.removed_nodes
                .write()
                .push((node_id.clone(), Instant::now()));
            removed.push(node_id);
        }

        removed
    }

    /// Get health state for a specific node.
    pub fn get_node_health(&self, node_id: &NodeId) -> Option<NodeHealthState> {
        self.node_health.read().get(node_id).cloned()
    }

    /// Build a cluster-wide health summary.
    pub fn summary(&self) -> ClusterHealthSummary {
        let health_map = self.node_health.read();
        let node_scores: HashMap<NodeId, HealthScore> = health_map
            .iter()
            .map(|(id, h)| (id.clone(), h.score))
            .collect();

        let healthy = health_map.values().filter(|h| h.healthy).count();
        let unhealthy = health_map.values().filter(|h| !h.healthy).count();
        let cluster_score = if health_map.is_empty() {
            1.0
        } else {
            health_map.values().map(|h| h.score).sum::<f64>() / health_map.len() as f64
        };

        let migrating = self.cluster.get_migrating_slots().len();
        let importing = self.cluster.get_importing_slots().len();

        ClusterHealthSummary {
            cluster_score,
            healthy_nodes: healthy,
            unhealthy_nodes: unhealthy,
            active_migrations: migrating,
            transitional_slots: migrating + importing,
            node_scores,
        }
    }

    /// Format enhanced CLUSTER INFO lines (appended to standard output).
    pub fn format_cluster_info_extra(&self) -> String {
        let summary = self.summary();
        format!(
            "cluster_health_score:{:.2}\r\n\
             cluster_healthy_nodes:{}\r\n\
             cluster_unhealthy_nodes:{}\r\n\
             cluster_active_migrations:{}\r\n\
             cluster_transitional_slots:{}\r\n",
            summary.cluster_score,
            summary.healthy_nodes,
            summary.unhealthy_nodes,
            summary.active_migrations,
            summary.transitional_slots,
        )
    }

    // ── internals ────────────────────────────────────────────────────

    fn compute_score(&self, metrics: &NodeMetrics) -> HealthScore {
        let w = &self.config.weights;

        let latency_score = 1.0
            - (metrics.avg_latency.as_millis() as f64
                / self.config.latency_threshold.as_millis() as f64)
                .min(1.0);

        let error_score = 1.0 - (metrics.error_rate / self.config.error_rate_threshold).min(1.0);

        let memory_score = 1.0 - (metrics.memory_usage / self.config.memory_threshold).min(1.0);

        let raw = w.latency * latency_score + w.error_rate * error_score + w.memory * memory_score;
        raw.clamp(0.0, 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    use crate::cluster::ClusterConfig;

    fn make_cluster() -> Arc<ClusterManager> {
        let config = ClusterConfig::default();
        let addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        Arc::new(ClusterManager::new(config, "node0".to_string(), addr))
    }

    #[test]
    fn test_perfect_health_score() {
        let cluster = make_cluster();
        let monitor = HealthMonitor::new(HealthConfig::default(), cluster);

        monitor.report_metrics(
            &"node1".to_string(),
            NodeMetrics {
                avg_latency: Duration::from_millis(1),
                error_rate: 0.0,
                memory_usage: 0.1,
                last_updated: Instant::now(),
            },
        );

        let health = monitor.get_node_health(&"node1".to_string()).unwrap();
        assert!(health.score > 0.9, "score={}", health.score);
        assert!(health.healthy);
    }

    #[test]
    fn test_unhealthy_high_latency() {
        let cluster = make_cluster();
        let monitor = HealthMonitor::new(HealthConfig::default(), cluster);

        monitor.report_metrics(
            &"node1".to_string(),
            NodeMetrics {
                avg_latency: Duration::from_millis(600), // over threshold
                error_rate: 0.15,                        // over threshold
                memory_usage: 0.98,                      // over threshold
                last_updated: Instant::now(),
            },
        );

        let health = monitor.get_node_health(&"node1".to_string()).unwrap();
        assert!(health.score < 0.3, "score={}", health.score);
        assert!(!health.healthy);
    }

    #[test]
    fn test_auto_remove_disabled_by_default() {
        let cluster = make_cluster();
        let monitor = HealthMonitor::new(HealthConfig::default(), cluster);
        let removed = monitor.check_health();
        assert!(removed.is_empty());
    }

    #[test]
    fn test_auto_remove_after_threshold() {
        let cluster = make_cluster();
        let mut config = HealthConfig::default();
        config.auto_remove_enabled = true;
        config.auto_remove_after = 2;
        let monitor = HealthMonitor::new(config, cluster.clone());

        let bad = NodeMetrics {
            avg_latency: Duration::from_secs(1),
            error_rate: 0.5,
            memory_usage: 0.99,
            last_updated: Instant::now(),
        };

        // Report twice to exceed threshold.
        monitor.report_metrics(&"node1".to_string(), bad.clone());
        monitor.report_metrics(&"node1".to_string(), bad);

        let removed = monitor.check_health();
        assert!(removed.contains(&"node1".to_string()));
    }

    #[test]
    fn test_cluster_health_summary() {
        let cluster = make_cluster();
        let monitor = HealthMonitor::new(HealthConfig::default(), cluster);

        monitor.report_metrics(
            &"node1".to_string(),
            NodeMetrics {
                avg_latency: Duration::from_millis(10),
                error_rate: 0.0,
                memory_usage: 0.3,
                last_updated: Instant::now(),
            },
        );

        let summary = monitor.summary();
        assert!(summary.cluster_score > 0.5);
        assert_eq!(summary.healthy_nodes, 1);
        assert_eq!(summary.unhealthy_nodes, 0);
    }

    #[test]
    fn test_format_cluster_info_extra() {
        let cluster = make_cluster();
        let monitor = HealthMonitor::new(HealthConfig::default(), cluster);

        let extra = monitor.format_cluster_info_extra();
        assert!(extra.contains("cluster_health_score:"));
        assert!(extra.contains("cluster_active_migrations:"));
        assert!(extra.contains("cluster_transitional_slots:"));
    }

    #[test]
    fn test_consecutive_unhealthy_resets_on_healthy() {
        let cluster = make_cluster();
        let monitor = HealthMonitor::new(HealthConfig::default(), cluster);

        // Unhealthy.
        monitor.report_metrics(
            &"node1".to_string(),
            NodeMetrics {
                avg_latency: Duration::from_secs(1),
                error_rate: 0.5,
                memory_usage: 0.99,
                last_updated: Instant::now(),
            },
        );
        let h = monitor.get_node_health(&"node1".to_string()).unwrap();
        assert!(h.consecutive_unhealthy >= 1);

        // Healthy again.
        monitor.report_metrics(
            &"node1".to_string(),
            NodeMetrics {
                avg_latency: Duration::from_millis(1),
                error_rate: 0.0,
                memory_usage: 0.1,
                last_updated: Instant::now(),
            },
        );
        let h = monitor.get_node_health(&"node1".to_string()).unwrap();
        assert_eq!(h.consecutive_unhealthy, 0);
    }
}
