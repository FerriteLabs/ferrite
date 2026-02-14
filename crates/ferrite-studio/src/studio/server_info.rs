//! Server info panel for Ferrite Studio
//!
//! Provides comprehensive server information including replication status,
//! persistence state, alerts, and configuration details.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Comprehensive server information for the info panel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerInfoPanel {
    /// Server version.
    pub version: String,
    /// Server mode (standalone, cluster, sentinel).
    pub mode: String,
    /// Role (master, replica).
    pub role: String,
    /// Uptime in seconds.
    pub uptime_seconds: u64,
    /// Operating system info.
    pub os: String,
    /// Rust version used to build.
    pub rust_version: String,
    /// Process ID.
    pub process_id: u32,
    /// TCP port.
    pub tcp_port: u16,
    /// Number of configured databases.
    pub databases: u32,
    /// Replication status.
    pub replication: ReplicationInfo,
    /// Persistence status.
    pub persistence: PersistenceInfo,
    /// Active alerts / warnings.
    pub alerts: Vec<Alert>,
    /// Selected configuration parameters.
    pub config_summary: HashMap<String, String>,
}

impl ServerInfoPanel {
    /// Generate a placeholder server info panel.
    pub fn placeholder() -> Self {
        let mut config_summary = HashMap::new();
        config_summary.insert("maxmemory".to_string(), "4gb".to_string());
        config_summary.insert("maxclients".to_string(), "10000".to_string());
        config_summary.insert("timeout".to_string(), "0".to_string());
        config_summary.insert("tcp-keepalive".to_string(), "300".to_string());
        config_summary.insert("hz".to_string(), "10".to_string());

        Self {
            version: env!("CARGO_PKG_VERSION").to_string(),
            mode: "standalone".to_string(),
            role: "master".to_string(),
            uptime_seconds: 3600,
            os: std::env::consts::OS.to_string(),
            rust_version: "1.88".to_string(),
            process_id: std::process::id(),
            tcp_port: 6379,
            databases: 16,
            replication: ReplicationInfo::standalone(),
            persistence: PersistenceInfo::placeholder(),
            alerts: Vec::new(),
            config_summary,
        }
    }

    /// Generate alerts based on current state.
    pub fn check_alerts(
        memory_used: u64,
        memory_total: u64,
        slow_queries_last_min: u64,
        connected_clients: u32,
        max_clients: u32,
    ) -> Vec<Alert> {
        let mut alerts = Vec::new();

        // High memory usage.
        if memory_total > 0 {
            let usage_pct = (memory_used as f64 / memory_total as f64) * 100.0;
            if usage_pct > 90.0 {
                alerts.push(Alert {
                    level: AlertLevel::Critical,
                    category: "memory".to_string(),
                    message: format!(
                        "Memory usage critical: {:.1}% ({} / {})",
                        usage_pct,
                        format_bytes(memory_used),
                        format_bytes(memory_total)
                    ),
                });
            } else if usage_pct > 75.0 {
                alerts.push(Alert {
                    level: AlertLevel::Warning,
                    category: "memory".to_string(),
                    message: format!("Memory usage high: {:.1}%", usage_pct),
                });
            }
        }

        // Slow queries.
        if slow_queries_last_min > 10 {
            alerts.push(Alert {
                level: AlertLevel::Warning,
                category: "performance".to_string(),
                message: format!("{} slow queries in the last minute", slow_queries_last_min),
            });
        }

        // High client count.
        if max_clients > 0 {
            let client_pct = (connected_clients as f64 / max_clients as f64) * 100.0;
            if client_pct > 80.0 {
                alerts.push(Alert {
                    level: AlertLevel::Warning,
                    category: "connections".to_string(),
                    message: format!(
                        "Client connections high: {} / {} ({:.0}%)",
                        connected_clients, max_clients, client_pct
                    ),
                });
            }
        }

        alerts
    }
}

/// Replication status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationInfo {
    /// Whether replication is active.
    pub enabled: bool,
    /// Role (master / replica).
    pub role: String,
    /// Number of connected replicas (if master).
    pub connected_replicas: u32,
    /// Master host (if replica).
    pub master_host: Option<String>,
    /// Master port (if replica).
    pub master_port: Option<u16>,
    /// Replication link status.
    pub link_status: String,
    /// Replication offset.
    pub repl_offset: u64,
    /// Replication lag in seconds (if replica).
    pub lag_seconds: Option<u64>,
}

impl ReplicationInfo {
    /// Info for a standalone (non-replicated) instance.
    pub fn standalone() -> Self {
        Self {
            enabled: false,
            role: "master".to_string(),
            connected_replicas: 0,
            master_host: None,
            master_port: None,
            link_status: "n/a".to_string(),
            repl_offset: 0,
            lag_seconds: None,
        }
    }

    /// Info for a master with replicas.
    pub fn master(replicas: u32, offset: u64) -> Self {
        Self {
            enabled: true,
            role: "master".to_string(),
            connected_replicas: replicas,
            master_host: None,
            master_port: None,
            link_status: "up".to_string(),
            repl_offset: offset,
            lag_seconds: None,
        }
    }

    /// Info for a replica.
    pub fn replica(master_host: &str, master_port: u16, lag: u64) -> Self {
        Self {
            enabled: true,
            role: "replica".to_string(),
            connected_replicas: 0,
            master_host: Some(master_host.to_string()),
            master_port: Some(master_port),
            link_status: "up".to_string(),
            repl_offset: 0,
            lag_seconds: Some(lag),
        }
    }
}

/// Persistence status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceInfo {
    /// Whether AOF is enabled.
    pub aof_enabled: bool,
    /// AOF current size in bytes.
    pub aof_current_size: u64,
    /// Last AOF rewrite status.
    pub aof_last_rewrite_status: String,
    /// Whether checkpointing is enabled.
    pub checkpoint_enabled: bool,
    /// Last checkpoint timestamp (epoch secs).
    pub last_checkpoint_time: Option<u64>,
    /// Last checkpoint status.
    pub last_checkpoint_status: String,
    /// Last checkpoint duration in seconds.
    pub last_checkpoint_duration_secs: Option<u64>,
    /// Whether a save is in progress.
    pub save_in_progress: bool,
}

impl PersistenceInfo {
    /// Placeholder persistence info.
    pub fn placeholder() -> Self {
        Self {
            aof_enabled: true,
            aof_current_size: 50 * 1024 * 1024,
            aof_last_rewrite_status: "ok".to_string(),
            checkpoint_enabled: true,
            last_checkpoint_time: Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
                    .saturating_sub(300),
            ),
            last_checkpoint_status: "ok".to_string(),
            last_checkpoint_duration_secs: Some(2),
            save_in_progress: false,
        }
    }

    /// Disabled persistence.
    pub fn disabled() -> Self {
        Self {
            aof_enabled: false,
            aof_current_size: 0,
            aof_last_rewrite_status: "n/a".to_string(),
            checkpoint_enabled: false,
            last_checkpoint_time: None,
            last_checkpoint_status: "n/a".to_string(),
            last_checkpoint_duration_secs: None,
            save_in_progress: false,
        }
    }
}

/// An alert or warning indicator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    /// Alert severity level.
    pub level: AlertLevel,
    /// Category (memory, performance, connections, replication, persistence).
    pub category: String,
    /// Human-readable message.
    pub message: String,
}

/// Alert severity levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertLevel {
    /// Informational.
    Info,
    /// Warning — requires attention.
    Warning,
    /// Critical — immediate action needed.
    Critical,
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_placeholder() {
        let info = ServerInfoPanel::placeholder();
        assert!(!info.version.is_empty());
        assert_eq!(info.mode, "standalone");
        assert_eq!(info.role, "master");
        assert!(!info.config_summary.is_empty());
    }

    #[test]
    fn test_placeholder_serialization() {
        let info = ServerInfoPanel::placeholder();
        let json = serde_json::to_string(&info).unwrap();
        let deserialized: ServerInfoPanel = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.mode, "standalone");
    }

    #[test]
    fn test_replication_standalone() {
        let repl = ReplicationInfo::standalone();
        assert!(!repl.enabled);
        assert_eq!(repl.role, "master");
        assert_eq!(repl.connected_replicas, 0);
    }

    #[test]
    fn test_replication_master() {
        let repl = ReplicationInfo::master(3, 1000);
        assert!(repl.enabled);
        assert_eq!(repl.connected_replicas, 3);
        assert_eq!(repl.repl_offset, 1000);
    }

    #[test]
    fn test_replication_replica() {
        let repl = ReplicationInfo::replica("10.0.0.1", 6379, 2);
        assert!(repl.enabled);
        assert_eq!(repl.role, "replica");
        assert_eq!(repl.master_host, Some("10.0.0.1".to_string()));
        assert_eq!(repl.lag_seconds, Some(2));
    }

    #[test]
    fn test_persistence_placeholder() {
        let pers = PersistenceInfo::placeholder();
        assert!(pers.aof_enabled);
        assert!(pers.checkpoint_enabled);
        assert!(pers.last_checkpoint_time.is_some());
    }

    #[test]
    fn test_persistence_disabled() {
        let pers = PersistenceInfo::disabled();
        assert!(!pers.aof_enabled);
        assert!(!pers.checkpoint_enabled);
    }

    #[test]
    fn test_alerts_high_memory() {
        let alerts =
            ServerInfoPanel::check_alerts(950, 1000, 0, 10, 10000);
        assert!(!alerts.is_empty());
        assert_eq!(alerts[0].level, AlertLevel::Critical);
        assert_eq!(alerts[0].category, "memory");
    }

    #[test]
    fn test_alerts_warning_memory() {
        let alerts =
            ServerInfoPanel::check_alerts(800, 1000, 0, 10, 10000);
        assert!(!alerts.is_empty());
        assert_eq!(alerts[0].level, AlertLevel::Warning);
    }

    #[test]
    fn test_alerts_slow_queries() {
        let alerts =
            ServerInfoPanel::check_alerts(100, 1000, 15, 10, 10000);
        assert!(alerts.iter().any(|a| a.category == "performance"));
    }

    #[test]
    fn test_alerts_high_clients() {
        let alerts =
            ServerInfoPanel::check_alerts(100, 1000, 0, 9000, 10000);
        assert!(alerts.iter().any(|a| a.category == "connections"));
    }

    #[test]
    fn test_alerts_all_clear() {
        let alerts =
            ServerInfoPanel::check_alerts(100, 1000, 0, 10, 10000);
        assert!(alerts.is_empty());
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GB");
    }

    #[test]
    fn test_alert_serialization() {
        let alert = Alert {
            level: AlertLevel::Critical,
            category: "memory".to_string(),
            message: "High memory".to_string(),
        };
        let json = serde_json::to_string(&alert).unwrap();
        let deserialized: Alert = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.level, AlertLevel::Critical);
    }

    #[test]
    fn test_replication_serialization() {
        let repl = ReplicationInfo::master(2, 500);
        let json = serde_json::to_string(&repl).unwrap();
        let deserialized: ReplicationInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.connected_replicas, 2);
    }

    #[test]
    fn test_persistence_serialization() {
        let pers = PersistenceInfo::placeholder();
        let json = serde_json::to_string(&pers).unwrap();
        let deserialized: PersistenceInfo = serde_json::from_str(&json).unwrap();
        assert!(deserialized.aof_enabled);
    }

    #[test]
    fn test_alert_level_variants() {
        assert_ne!(AlertLevel::Info, AlertLevel::Warning);
        assert_ne!(AlertLevel::Warning, AlertLevel::Critical);
    }

    #[test]
    fn test_alerts_zero_total_memory() {
        // Should not panic with zero total memory.
        let alerts = ServerInfoPanel::check_alerts(100, 0, 0, 10, 10000);
        assert!(alerts.is_empty());
    }

    #[test]
    fn test_alerts_zero_max_clients() {
        // Should not panic with zero max clients.
        let alerts = ServerInfoPanel::check_alerts(100, 1000, 0, 10, 0);
        // No client alert when max is 0.
        assert!(!alerts.iter().any(|a| a.category == "connections"));
    }
}
