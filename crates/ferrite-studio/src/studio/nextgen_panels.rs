//! Dashboard panel data providers for next-gen features.
//!
//! Each panel method returns a fully configured [`DashboardPanel`] with
//! column definitions, refresh intervals, and category metadata ready for
//! the Studio dashboard grid.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// Visual representation type for a dashboard panel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PanelType {
    /// Tabular data view.
    Table,
    /// Time-series line chart.
    LineChart,
    /// Single-value gauge with min/max.
    Gauge,
    /// Categorical bar chart.
    BarChart,
    /// Chronological event timeline.
    Timeline,
    /// Grid of status indicators.
    StatusGrid,
}

/// Logical category a panel belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PanelCategory {
    /// Latency, throughput, and profiling panels.
    Performance,
    /// Tiering, versioning, and persistence panels.
    Storage,
    /// CRDT, edge-sync, and cluster panels.
    Replication,
    /// Auth, TLS, and audit panels.
    Security,
    /// Scaling, migration, and deployment panels.
    Operations,
    /// Schema, triggers, and change-feed panels.
    DataModel,
}

/// Data type of a column value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnType {
    /// UTF-8 text.
    String,
    /// Signed 64-bit integer.
    Integer,
    /// 64-bit floating point.
    Float,
    /// ISO-8601 timestamp.
    Timestamp,
    /// Human-readable duration (e.g. "12ms").
    Duration,
    /// Percentage value (0–100).
    Percentage,
    /// Coloured status badge.
    Badge,
    /// Inline sparkline chart.
    Sparkline,
}

// ---------------------------------------------------------------------------
// Structs
// ---------------------------------------------------------------------------

/// Definition of a single column inside a [`DashboardPanel`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Machine-readable column identifier.
    pub name: String,
    /// Human-readable header text.
    pub display_name: String,
    /// Value type carried by this column.
    pub data_type: ColumnType,
    /// Whether the column supports client-side sorting.
    pub sortable: bool,
    /// Optional CSS width hint (e.g. `"120px"`, `"auto"`).
    pub width: Option<String>,
}

/// A fully configured dashboard panel ready for rendering.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardPanel {
    /// Unique panel identifier.
    pub id: String,
    /// Display title shown in the panel header.
    pub title: String,
    /// Short description of what this panel shows.
    pub description: String,
    /// Visual representation type.
    pub panel_type: PanelType,
    /// Column definitions for tabular or chart axes.
    pub columns: Vec<ColumnDef>,
    /// How often the panel should refresh (milliseconds).
    pub refresh_interval_ms: u64,
    /// Logical category for grouping in the UI.
    pub category: PanelCategory,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn col(name: &str, display: &str, data_type: ColumnType, sortable: bool) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        display_name: display.to_string(),
        data_type,
        sortable,
        width: None,
    }
}

fn col_w(
    name: &str,
    display: &str,
    data_type: ColumnType,
    sortable: bool,
    width: &str,
) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        display_name: display.to_string(),
        data_type,
        sortable,
        width: Some(width.to_string()),
    }
}

// ---------------------------------------------------------------------------
// NextGenDashboard
// ---------------------------------------------------------------------------

/// Aggregates all next-gen feature dashboard panels.
///
/// Each method returns a [`DashboardPanel`] pre-configured with columns,
/// refresh interval, and category metadata for a specific next-gen feature.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NextGenDashboard;

impl NextGenDashboard {
    /// Create a new `NextGenDashboard` instance.
    pub fn new() -> Self {
        Self
    }

    /// Panel showing per-command latency percentiles.
    pub fn query_profiler_panel(&self) -> DashboardPanel {
        DashboardPanel {
            id: "query-profiler".to_string(),
            title: "Query Profiler".to_string(),
            description: "Per-command latency percentiles and throughput".to_string(),
            panel_type: PanelType::Table,
            columns: vec![
                col_w("command", "Command", ColumnType::String, true, "140px"),
                col("total_calls", "TotalCalls", ColumnType::Integer, true),
                col("avg_latency_us", "AvgLatencyUs", ColumnType::Float, true),
                col("p50", "P50", ColumnType::Float, true),
                col("p95", "P95", ColumnType::Float, true),
                col("p99", "P99", ColumnType::Float, true),
                col("error_rate", "ErrorRate", ColumnType::Percentage, true),
                col("ops_per_sec", "OpsPerSec", ColumnType::Integer, true),
            ],
            refresh_interval_ms: 2000,
            category: PanelCategory::Performance,
        }
    }

    /// Panel for time-travel version timeline.
    pub fn version_history_panel(&self) -> DashboardPanel {
        DashboardPanel {
            id: "version-history".to_string(),
            title: "Version History".to_string(),
            description: "Time-travel version timeline for keys".to_string(),
            panel_type: PanelType::Timeline,
            columns: vec![
                col_w("key", "Key", ColumnType::String, true, "180px"),
                col("version", "Version", ColumnType::Integer, true),
                col("timestamp", "Timestamp", ColumnType::Timestamp, true),
                col("operation", "Operation", ColumnType::Badge, true),
                col("size", "Size", ColumnType::Integer, true),
            ],
            refresh_interval_ms: 5000,
            category: PanelCategory::Storage,
        }
    }

    /// Panel for CRDT gossip state.
    pub fn crdt_status_panel(&self) -> DashboardPanel {
        DashboardPanel {
            id: "crdt-status".to_string(),
            title: "CRDT Status".to_string(),
            description: "CRDT gossip protocol state across peers".to_string(),
            panel_type: PanelType::StatusGrid,
            columns: vec![
                col_w("peer_id", "PeerId", ColumnType::String, true, "160px"),
                col("status", "Status", ColumnType::Badge, true),
                col("last_seen", "LastSeen", ColumnType::Timestamp, true),
                col("messages_sent", "MessagesSent", ColumnType::Integer, true),
                col("deltas_pending", "DeltasPending", ColumnType::Integer, true),
            ],
            refresh_interval_ms: 3000,
            category: PanelCategory::Replication,
        }
    }

    /// Panel for ML tiering predictions.
    pub fn ml_tiering_panel(&self) -> DashboardPanel {
        DashboardPanel {
            id: "ml-tiering".to_string(),
            title: "ML Tiering".to_string(),
            description: "Machine-learning tier placement predictions".to_string(),
            panel_type: PanelType::Table,
            columns: vec![
                col_w("key", "Key", ColumnType::String, true, "180px"),
                col("current_tier", "CurrentTier", ColumnType::Badge, true),
                col("predicted_tier", "PredictedTier", ColumnType::Badge, true),
                col("probability", "Probability", ColumnType::Percentage, true),
                col("confidence", "Confidence", ColumnType::Percentage, true),
            ],
            refresh_interval_ms: 10000,
            category: PanelCategory::Storage,
        }
    }

    /// Panel for traffic proxy stats during live migration.
    pub fn migration_proxy_panel(&self) -> DashboardPanel {
        DashboardPanel {
            id: "migration-proxy".to_string(),
            title: "Migration Proxy".to_string(),
            description: "Live-migration traffic proxy statistics".to_string(),
            panel_type: PanelType::Gauge,
            columns: vec![
                col_w("metric", "Metric", ColumnType::String, false, "160px"),
                col("source", "Source", ColumnType::Integer, true),
                col("target", "Target", ColumnType::Integer, true),
                col("total", "Total", ColumnType::Integer, true),
            ],
            refresh_interval_ms: 2000,
            category: PanelCategory::Operations,
        }
    }

    /// Panel for delta-sync status across edge nodes.
    pub fn edge_sync_panel(&self) -> DashboardPanel {
        DashboardPanel {
            id: "edge-sync".to_string(),
            title: "Edge Sync".to_string(),
            description: "Delta-sync status for edge nodes".to_string(),
            panel_type: PanelType::StatusGrid,
            columns: vec![
                col_w("node_id", "NodeId", ColumnType::String, true, "140px"),
                col("state", "State", ColumnType::Badge, true),
                col("local_version", "LocalVersion", ColumnType::Integer, true),
                col("remote_version", "RemoteVersion", ColumnType::Integer, true),
                col("pending_deltas", "PendingDeltas", ColumnType::Integer, true),
            ],
            refresh_interval_ms: 3000,
            category: PanelCategory::Replication,
        }
    }

    /// Panel for WASM trigger execution stats.
    pub fn wasm_triggers_panel(&self) -> DashboardPanel {
        DashboardPanel {
            id: "wasm-triggers".to_string(),
            title: "WASM Triggers".to_string(),
            description: "WebAssembly trigger execution statistics".to_string(),
            panel_type: PanelType::Table,
            columns: vec![
                col_w("module", "Module", ColumnType::String, true, "160px"),
                col("executions", "Executions", ColumnType::Integer, true),
                col("avg_latency_us", "AvgLatencyUs", ColumnType::Float, true),
                col("errors", "Errors", ColumnType::Integer, true),
                col("last_execution", "LastExecution", ColumnType::Timestamp, true),
            ],
            refresh_interval_ms: 5000,
            category: PanelCategory::DataModel,
        }
    }

    /// Panel for registered schemas.
    pub fn schema_registry_panel(&self) -> DashboardPanel {
        DashboardPanel {
            id: "schema-registry".to_string(),
            title: "Schema Registry".to_string(),
            description: "Registered schemas and their field definitions".to_string(),
            panel_type: PanelType::Table,
            columns: vec![
                col_w("name", "Name", ColumnType::String, true, "160px"),
                col("pattern", "Pattern", ColumnType::String, true),
                col("fields", "Fields", ColumnType::Integer, true),
                col("version", "Version", ColumnType::Integer, true),
                col("created_at", "CreatedAt", ColumnType::Timestamp, true),
            ],
            refresh_interval_ms: 30000,
            category: PanelCategory::DataModel,
        }
    }

    /// Panel for Kubernetes autoscaler decisions.
    pub fn autoscaler_panel(&self) -> DashboardPanel {
        DashboardPanel {
            id: "autoscaler".to_string(),
            title: "Autoscaler".to_string(),
            description: "Kubernetes autoscaler scaling decisions".to_string(),
            panel_type: PanelType::Table,
            columns: vec![
                col("timestamp", "Timestamp", ColumnType::Timestamp, true),
                col("direction", "Direction", ColumnType::Badge, true),
                col("from_replicas", "FromReplicas", ColumnType::Integer, true),
                col("to_replicas", "ToReplicas", ColumnType::Integer, true),
                col_w("reason", "Reason", ColumnType::String, false, "200px"),
                col("cost_change", "CostChange", ColumnType::Float, true),
            ],
            refresh_interval_ms: 15000,
            category: PanelCategory::Operations,
        }
    }

    /// Panel for materialized view change feed.
    pub fn change_tracker_panel(&self) -> DashboardPanel {
        DashboardPanel {
            id: "change-tracker".to_string(),
            title: "Change Tracker".to_string(),
            description: "Materialized view change feed".to_string(),
            panel_type: PanelType::Timeline,
            columns: vec![
                col("sequence", "Sequence", ColumnType::Integer, true),
                col_w("key", "Key", ColumnType::String, true, "180px"),
                col("operation", "Operation", ColumnType::Badge, true),
                col("timestamp", "Timestamp", ColumnType::Timestamp, true),
                col("subscribers", "Subscribers", ColumnType::Integer, true),
            ],
            refresh_interval_ms: 2000,
            category: PanelCategory::DataModel,
        }
    }

    /// Returns all next-gen feature panels.
    pub fn all_panels(&self) -> Vec<DashboardPanel> {
        vec![
            self.query_profiler_panel(),
            self.version_history_panel(),
            self.crdt_status_panel(),
            self.ml_tiering_panel(),
            self.migration_proxy_panel(),
            self.edge_sync_panel(),
            self.wasm_triggers_panel(),
            self.schema_registry_panel(),
            self.autoscaler_panel(),
            self.change_tracker_panel(),
        ]
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn all_panels_returns_ten() {
        let dash = NextGenDashboard::new();
        assert_eq!(dash.all_panels().len(), 10);
    }

    #[test]
    fn each_panel_has_valid_columns() {
        let dash = NextGenDashboard::new();
        for panel in dash.all_panels() {
            assert!(
                !panel.columns.is_empty(),
                "panel '{}' has no columns",
                panel.id
            );
            for col in &panel.columns {
                assert!(!col.name.is_empty(), "empty column name in panel {}", panel.id);
                assert!(
                    !col.display_name.is_empty(),
                    "empty display_name in panel {}",
                    panel.id
                );
            }
        }
    }

    #[test]
    fn panel_categories_are_correct() {
        let dash = NextGenDashboard::new();
        assert_eq!(dash.query_profiler_panel().category, PanelCategory::Performance);
        assert_eq!(dash.version_history_panel().category, PanelCategory::Storage);
        assert_eq!(dash.crdt_status_panel().category, PanelCategory::Replication);
        assert_eq!(dash.ml_tiering_panel().category, PanelCategory::Storage);
        assert_eq!(dash.migration_proxy_panel().category, PanelCategory::Operations);
        assert_eq!(dash.edge_sync_panel().category, PanelCategory::Replication);
        assert_eq!(dash.wasm_triggers_panel().category, PanelCategory::DataModel);
        assert_eq!(dash.schema_registry_panel().category, PanelCategory::DataModel);
        assert_eq!(dash.autoscaler_panel().category, PanelCategory::Operations);
        assert_eq!(dash.change_tracker_panel().category, PanelCategory::DataModel);
    }

    #[test]
    fn panel_ids_are_unique() {
        let dash = NextGenDashboard::new();
        let panels = dash.all_panels();
        let ids: HashSet<&str> = panels.iter().map(|p| p.id.as_str()).collect();
        assert_eq!(ids.len(), panels.len(), "duplicate panel IDs detected");
    }

    #[test]
    fn query_profiler_has_expected_columns() {
        let dash = NextGenDashboard::new();
        let panel = dash.query_profiler_panel();
        let names: Vec<&str> = panel.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "command",
                "total_calls",
                "avg_latency_us",
                "p50",
                "p95",
                "p99",
                "error_rate",
                "ops_per_sec",
            ]
        );
    }

    #[test]
    fn autoscaler_panel_type_is_table() {
        let dash = NextGenDashboard::new();
        assert_eq!(dash.autoscaler_panel().panel_type, PanelType::Table);
    }
}
