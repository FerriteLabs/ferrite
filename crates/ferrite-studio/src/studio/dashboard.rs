//! Unified Observability Dashboard for Ferrite Studio
//!
//! Provides a configurable dashboard that aggregates metrics, alerts, and
//! topology data into a single pane of glass. Widgets can be arranged in a
//! grid layout and fed with real-time time-series, gauge, table, heatmap, and
//! topology data.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the unified observability dashboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardConfig {
    /// How often widgets should refresh (milliseconds).
    pub refresh_interval_ms: u64,
    /// Maximum number of data points to retain per time series.
    pub max_data_points: usize,
    /// How long to keep recorded metrics (minutes).
    pub retention_minutes: u32,
    /// Whether the alerting subsystem is enabled.
    pub enable_alerts: bool,
    /// Minimum seconds between repeated alerts from the same source.
    pub alert_cooldown_secs: u64,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            refresh_interval_ms: 1000,
            max_data_points: 1000,
            retention_minutes: 60,
            enable_alerts: true,
            alert_cooldown_secs: 300,
        }
    }
}

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// Visual type of a dashboard widget.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WidgetType {
    /// Time-series line chart.
    LineChart,
    /// Categorical bar chart.
    BarChart,
    /// Single-value gauge with min/max.
    Gauge,
    /// Tabular data view.
    Table,
    /// Two-dimensional heatmap.
    Heatmap,
    /// Network/cluster topology graph.
    Topology,
    /// Stack-based flame graph.
    FlameGraph,
    /// Simple numeric counter.
    Counter,
    /// Free-form text or markdown.
    Text,
}

impl std::fmt::Display for WidgetType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LineChart => write!(f, "line_chart"),
            Self::BarChart => write!(f, "bar_chart"),
            Self::Gauge => write!(f, "gauge"),
            Self::Table => write!(f, "table"),
            Self::Heatmap => write!(f, "heatmap"),
            Self::Topology => write!(f, "topology"),
            Self::FlameGraph => write!(f, "flame_graph"),
            Self::Counter => write!(f, "counter"),
            Self::Text => write!(f, "text"),
        }
    }
}

/// Direction of a metric trend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Trend {
    /// Value is increasing.
    Up,
    /// Value is decreasing.
    Down,
    /// Value is roughly constant.
    Stable,
}

/// Severity of a dashboard alert.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertSeverity {
    /// Informational notice.
    Info,
    /// Potential problem that deserves attention.
    Warning,
    /// Urgent issue requiring immediate action.
    Critical,
}

impl std::fmt::Display for AlertSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Info => write!(f, "info"),
            Self::Warning => write!(f, "warning"),
            Self::Critical => write!(f, "critical"),
        }
    }
}

// ---------------------------------------------------------------------------
// Overview types
// ---------------------------------------------------------------------------

/// High-level overview of storage distribution across tiers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringOverview {
    /// Keys residing in the memory tier.
    pub memory_keys: u64,
    /// Bytes used by memory-tier keys.
    pub memory_bytes: u64,
    /// Keys residing on disk.
    pub disk_keys: u64,
    /// Bytes used by disk-tier keys.
    pub disk_bytes: u64,
    /// Keys residing in cloud/object storage.
    pub cloud_keys: u64,
    /// Bytes used by cloud-tier keys.
    pub cloud_bytes: u64,
    /// Estimated monthly cloud infrastructure cost (USD).
    pub estimated_monthly_cost_usd: f64,
}

/// Per-command usage statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandUsage {
    /// Command name (e.g. `GET`, `SET`).
    pub command: String,
    /// Total invocations.
    pub calls: u64,
    /// Average latency in microseconds.
    pub avg_latency_us: f64,
    /// 99th-percentile latency in microseconds.
    pub p99_latency_us: f64,
}

/// Top-level dashboard overview combining key server metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardOverview {
    /// Server uptime in seconds.
    pub server_uptime_secs: u64,
    /// Current operations per second.
    pub ops_per_sec: f64,
    /// Number of currently connected clients.
    pub connected_clients: u64,
    /// Memory currently in use (bytes).
    pub memory_used_bytes: u64,
    /// Total memory available (bytes).
    pub memory_total_bytes: u64,
    /// Total number of keys across all databases.
    pub keys_total: u64,
    /// Cache hit rate (0.0–1.0).
    pub hit_rate: f64,
    /// Cluster health status string.
    pub cluster_status: String,
    /// Replication lag in milliseconds.
    pub replication_lag_ms: u64,
    /// Storage tier distribution.
    pub tiering_distribution: TieringOverview,
    /// Most-used commands with latency info.
    pub top_commands: Vec<CommandUsage>,
    /// Timestamp when this overview was generated.
    pub generated_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Widget types
// ---------------------------------------------------------------------------

/// Size of a widget in grid units.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WidgetSize {
    /// Width in grid columns.
    pub width: u32,
    /// Height in grid rows.
    pub height: u32,
}

/// Metadata about an available widget.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WidgetInfo {
    /// Unique widget identifier.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Short description of the widget's purpose.
    pub description: String,
    /// Visual type.
    pub widget_type: WidgetType,
    /// Default size on the grid.
    pub default_size: WidgetSize,
}

/// A single data point in a time series.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPoint {
    /// When the value was recorded.
    pub timestamp: DateTime<Utc>,
    /// Observed value.
    pub value: f64,
}

/// A named time series.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesData {
    /// Series name / label.
    pub name: String,
    /// Ordered data points.
    pub points: Vec<DataPoint>,
    /// Optional CSS/hex colour hint.
    pub color: Option<String>,
}

/// A node in a topology graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyNode {
    /// Unique node identifier.
    pub id: String,
    /// Display label.
    pub label: String,
    /// Health status string.
    pub status: String,
    /// X position for layout.
    pub x: f64,
    /// Y position for layout.
    pub y: f64,
}

/// A directed edge in a topology graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyEdge {
    /// Source node id.
    pub source: String,
    /// Target node id.
    pub target: String,
    /// Edge label.
    pub label: String,
    /// Edge weight / throughput.
    pub weight: f64,
}

/// Payload carried by a widget.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WidgetContent {
    /// One or more time series for line/area charts.
    TimeSeries { series: Vec<TimeSeriesData> },
    /// A single scalar value with unit and trend indicator.
    SingleValue {
        value: f64,
        unit: String,
        trend: Trend,
    },
    /// Column-oriented tabular data.
    TableData {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    /// Two-dimensional heatmap grid.
    HeatmapData {
        labels: Vec<String>,
        values: Vec<Vec<f64>>,
    },
    /// Graph of nodes and edges.
    TopologyData {
        nodes: Vec<TopologyNode>,
        edges: Vec<TopologyEdge>,
    },
}

/// Fully resolved widget data ready for rendering.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WidgetData {
    /// Widget identifier.
    pub widget_id: String,
    /// Visual type.
    pub widget_type: WidgetType,
    /// Display title.
    pub title: String,
    /// Content payload.
    pub data: WidgetContent,
    /// When the data was last refreshed.
    pub updated_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Layout types
// ---------------------------------------------------------------------------

/// Position and size of a widget on the dashboard grid.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WidgetPlacement {
    /// Widget identifier.
    pub widget_id: String,
    /// Column offset.
    pub x: u32,
    /// Row offset.
    pub y: u32,
    /// Width in grid columns.
    pub width: u32,
    /// Height in grid rows.
    pub height: u32,
}

/// Arrangement of widgets on the dashboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardLayout {
    /// Ordered list of widget placements.
    pub widgets: Vec<WidgetPlacement>,
}

// ---------------------------------------------------------------------------
// Metric ingestion
// ---------------------------------------------------------------------------

/// A single metric data point submitted to the dashboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricPoint {
    /// Metric name (e.g. `ops_per_sec`).
    pub name: String,
    /// Observed value.
    pub value: f64,
    /// Arbitrary key-value tags.
    pub tags: HashMap<String, String>,
    /// When the metric was recorded.
    pub timestamp: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Alerts
// ---------------------------------------------------------------------------

/// An alert raised by the observability dashboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    /// Unique alert identifier.
    pub id: String,
    /// How urgent the alert is.
    pub severity: AlertSeverity,
    /// Short summary.
    pub title: String,
    /// Detailed description.
    pub message: String,
    /// Component or metric that triggered the alert.
    pub source: String,
    /// When the alert was triggered.
    pub triggered_at: DateTime<Utc>,
    /// Whether an operator has acknowledged this alert.
    pub acknowledged: bool,
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Aggregate statistics about the dashboard itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardStats {
    /// Total metric points recorded since start.
    pub total_metrics_recorded: u64,
    /// Total alerts raised since start.
    pub total_alerts: u64,
    /// Currently active (unacknowledged) alerts.
    pub active_alerts: u64,
    /// Alerts that have been acknowledged.
    pub acknowledged_alerts: u64,
    /// Number of widgets in the current layout.
    pub widgets_configured: u64,
    /// Number of data points currently stored.
    pub data_points_stored: u64,
}

// ---------------------------------------------------------------------------
// Default widgets
// ---------------------------------------------------------------------------

/// Pre-built widget definitions for the default dashboard layout.
pub fn default_widgets() -> Vec<WidgetInfo> {
    vec![
        WidgetInfo {
            id: "ops-per-sec".to_string(),
            name: "Operations/sec".to_string(),
            description: "Current throughput gauge".to_string(),
            widget_type: WidgetType::Gauge,
            default_size: WidgetSize {
                width: 2,
                height: 1,
            },
        },
        WidgetInfo {
            id: "latency-p99".to_string(),
            name: "P99 Latency".to_string(),
            description: "99th-percentile latency over time".to_string(),
            widget_type: WidgetType::LineChart,
            default_size: WidgetSize {
                width: 4,
                height: 2,
            },
        },
        WidgetInfo {
            id: "memory-usage".to_string(),
            name: "Memory Usage".to_string(),
            description: "Current memory utilization gauge".to_string(),
            widget_type: WidgetType::Gauge,
            default_size: WidgetSize {
                width: 2,
                height: 1,
            },
        },
        WidgetInfo {
            id: "connected-clients".to_string(),
            name: "Connected Clients".to_string(),
            description: "Number of active client connections".to_string(),
            widget_type: WidgetType::Counter,
            default_size: WidgetSize {
                width: 2,
                height: 1,
            },
        },
        WidgetInfo {
            id: "command-distribution".to_string(),
            name: "Command Distribution".to_string(),
            description: "Breakdown of command types".to_string(),
            widget_type: WidgetType::BarChart,
            default_size: WidgetSize {
                width: 4,
                height: 2,
            },
        },
        WidgetInfo {
            id: "slow-queries".to_string(),
            name: "Slow Queries".to_string(),
            description: "Recent slow query log".to_string(),
            widget_type: WidgetType::Table,
            default_size: WidgetSize {
                width: 4,
                height: 2,
            },
        },
        WidgetInfo {
            id: "cluster-topology".to_string(),
            name: "Cluster Topology".to_string(),
            description: "Live cluster node graph".to_string(),
            widget_type: WidgetType::Topology,
            default_size: WidgetSize {
                width: 6,
                height: 3,
            },
        },
        WidgetInfo {
            id: "key-access-heatmap".to_string(),
            name: "Key Access Heatmap".to_string(),
            description: "Hot-key access pattern heatmap".to_string(),
            widget_type: WidgetType::Heatmap,
            default_size: WidgetSize {
                width: 4,
                height: 2,
            },
        },
        WidgetInfo {
            id: "tiering-costs".to_string(),
            name: "Tiering Costs".to_string(),
            description: "Monthly cost breakdown by storage tier".to_string(),
            widget_type: WidgetType::BarChart,
            default_size: WidgetSize {
                width: 4,
                height: 2,
            },
        },
        WidgetInfo {
            id: "replication-lag".to_string(),
            name: "Replication Lag".to_string(),
            description: "Replica lag over time".to_string(),
            widget_type: WidgetType::LineChart,
            default_size: WidgetSize {
                width: 4,
                height: 2,
            },
        },
    ]
}

// ---------------------------------------------------------------------------
// Dashboard implementation
// ---------------------------------------------------------------------------

/// Unified observability dashboard for Ferrite Studio.
///
/// Aggregates metrics, alerts, and widget data into a single interface.
/// Thread-safe — all mutable state is protected by `parking_lot::RwLock`
/// or atomic counters.
pub struct Dashboard {
    config: DashboardConfig,
    started_at: Instant,
    metrics: parking_lot::RwLock<Vec<MetricPoint>>,
    alerts: parking_lot::RwLock<Vec<Alert>>,
    layout: parking_lot::RwLock<DashboardLayout>,
    total_metrics_recorded: AtomicU64,
    total_alerts: AtomicU64,
}

impl Dashboard {
    /// Create a new dashboard with the given configuration.
    pub fn new(config: DashboardConfig) -> Self {
        let widgets = default_widgets();
        let placements: Vec<WidgetPlacement> = widgets
            .iter()
            .enumerate()
            .map(|(i, w)| WidgetPlacement {
                widget_id: w.id.clone(),
                x: 0,
                y: i as u32 * 2,
                width: w.default_size.width,
                height: w.default_size.height,
            })
            .collect();

        Self {
            config,
            started_at: Instant::now(),
            metrics: parking_lot::RwLock::new(Vec::new()),
            alerts: parking_lot::RwLock::new(Vec::new()),
            layout: parking_lot::RwLock::new(DashboardLayout {
                widgets: placements,
            }),
            total_metrics_recorded: AtomicU64::new(0),
            total_alerts: AtomicU64::new(0),
        }
    }

    /// Generate a high-level dashboard overview.
    pub fn get_overview(&self) -> DashboardOverview {
        let metrics = self.metrics.read();

        let ops_per_sec = self
            .latest_metric_value(&metrics, "ops_per_sec")
            .unwrap_or(0.0);
        let connected_clients = self
            .latest_metric_value(&metrics, "connected_clients")
            .unwrap_or(0.0) as u64;
        let memory_used = self
            .latest_metric_value(&metrics, "memory_used_bytes")
            .unwrap_or(0.0) as u64;
        let memory_total = self
            .latest_metric_value(&metrics, "memory_total_bytes")
            .unwrap_or(16u64 as f64 * 1024.0 * 1024.0 * 1024.0) as u64;
        let keys_total = self
            .latest_metric_value(&metrics, "keys_total")
            .unwrap_or(0.0) as u64;
        let hit_rate = self
            .latest_metric_value(&metrics, "hit_rate")
            .unwrap_or(0.0);
        let replication_lag = self
            .latest_metric_value(&metrics, "replication_lag_ms")
            .unwrap_or(0.0) as u64;

        DashboardOverview {
            server_uptime_secs: self.started_at.elapsed().as_secs(),
            ops_per_sec,
            connected_clients,
            memory_used_bytes: memory_used,
            memory_total_bytes: memory_total,
            keys_total,
            hit_rate,
            cluster_status: "ok".to_string(),
            replication_lag_ms: replication_lag,
            tiering_distribution: TieringOverview {
                memory_keys: keys_total,
                memory_bytes: memory_used,
                disk_keys: 0,
                disk_bytes: 0,
                cloud_keys: 0,
                cloud_bytes: 0,
                estimated_monthly_cost_usd: 0.0,
            },
            top_commands: vec![],
            generated_at: Utc::now(),
        }
    }

    /// Retrieve resolved data for a specific widget.
    pub fn get_widget_data(&self, widget_id: &str) -> Option<WidgetData> {
        let widgets = default_widgets();
        let info = widgets.iter().find(|w| w.id == widget_id)?;
        let metrics = self.metrics.read();

        let content = match widget_id {
            "ops-per-sec" => WidgetContent::SingleValue {
                value: self
                    .latest_metric_value(&metrics, "ops_per_sec")
                    .unwrap_or(0.0),
                unit: "ops/s".to_string(),
                trend: Trend::Stable,
            },
            "latency-p99" => WidgetContent::TimeSeries {
                series: vec![self.build_time_series(&metrics, "latency_p99", "#ff6384")],
            },
            "memory-usage" => WidgetContent::SingleValue {
                value: self
                    .latest_metric_value(&metrics, "memory_used_bytes")
                    .unwrap_or(0.0),
                unit: "bytes".to_string(),
                trend: Trend::Stable,
            },
            "connected-clients" => WidgetContent::SingleValue {
                value: self
                    .latest_metric_value(&metrics, "connected_clients")
                    .unwrap_or(0.0),
                unit: "clients".to_string(),
                trend: Trend::Stable,
            },
            "command-distribution" => WidgetContent::TableData {
                columns: vec![
                    "Command".to_string(),
                    "Calls".to_string(),
                    "Avg Latency (µs)".to_string(),
                ],
                rows: vec![],
            },
            "slow-queries" => WidgetContent::TableData {
                columns: vec![
                    "Time".to_string(),
                    "Duration (µs)".to_string(),
                    "Command".to_string(),
                ],
                rows: vec![],
            },
            "cluster-topology" => WidgetContent::TopologyData {
                nodes: vec![TopologyNode {
                    id: "node-1".to_string(),
                    label: "Primary".to_string(),
                    status: "online".to_string(),
                    x: 0.0,
                    y: 0.0,
                }],
                edges: vec![],
            },
            "key-access-heatmap" => WidgetContent::HeatmapData {
                labels: vec![
                    "00:00".to_string(),
                    "06:00".to_string(),
                    "12:00".to_string(),
                    "18:00".to_string(),
                ],
                values: vec![vec![0.0; 4]; 4],
            },
            "tiering-costs" => WidgetContent::TableData {
                columns: vec!["Tier".to_string(), "Cost ($/mo)".to_string()],
                rows: vec![
                    vec!["Memory".to_string(), "0.00".to_string()],
                    vec!["Disk".to_string(), "0.00".to_string()],
                    vec!["Cloud".to_string(), "0.00".to_string()],
                ],
            },
            "replication-lag" => WidgetContent::TimeSeries {
                series: vec![self.build_time_series(&metrics, "replication_lag_ms", "#36a2eb")],
            },
            _ => return None,
        };

        Some(WidgetData {
            widget_id: widget_id.to_string(),
            widget_type: info.widget_type.clone(),
            title: info.name.clone(),
            data: content,
            updated_at: Utc::now(),
        })
    }

    /// List all available widgets.
    pub fn get_available_widgets(&self) -> Vec<WidgetInfo> {
        default_widgets()
    }

    /// Replace the current dashboard layout.
    pub fn configure_layout(&self, layout: DashboardLayout) {
        *self.layout.write() = layout;
    }

    /// Record an incoming metric data point.
    pub fn record_metric(&self, metric: MetricPoint) {
        self.total_metrics_recorded.fetch_add(1, Ordering::Relaxed);
        let mut metrics = self.metrics.write();
        metrics.push(metric);
        // Enforce retention window.
        if metrics.len() > self.config.max_data_points {
            let drain = metrics.len() - self.config.max_data_points;
            metrics.drain(..drain);
        }
    }

    /// Retrieve all current alerts.
    pub fn get_alerts(&self) -> Vec<Alert> {
        self.alerts.read().clone()
    }

    /// Acknowledge an alert by its id. Returns `true` if the alert was found.
    pub fn acknowledge_alert(&self, alert_id: &str) -> bool {
        let mut alerts = self.alerts.write();
        if let Some(alert) = alerts.iter_mut().find(|a| a.id == alert_id) {
            alert.acknowledged = true;
            true
        } else {
            false
        }
    }

    /// Get aggregate statistics about the dashboard.
    pub fn get_stats(&self) -> DashboardStats {
        let alerts = self.alerts.read();
        let layout = self.layout.read();
        let metrics = self.metrics.read();

        let active = alerts.iter().filter(|a| !a.acknowledged).count() as u64;
        let acked = alerts.iter().filter(|a| a.acknowledged).count() as u64;

        DashboardStats {
            total_metrics_recorded: self.total_metrics_recorded.load(Ordering::Relaxed),
            total_alerts: self.total_alerts.load(Ordering::Relaxed),
            active_alerts: active,
            acknowledged_alerts: acked,
            widgets_configured: layout.widgets.len() as u64,
            data_points_stored: metrics.len() as u64,
        }
    }

    /// Add an alert to the dashboard (used internally or by external monitors).
    pub fn add_alert(&self, alert: Alert) {
        self.total_alerts.fetch_add(1, Ordering::Relaxed);
        self.alerts.write().push(alert);
    }

    // -- private helpers ----------------------------------------------------

    fn latest_metric_value(&self, metrics: &[MetricPoint], name: &str) -> Option<f64> {
        metrics
            .iter()
            .rev()
            .find(|m| m.name == name)
            .map(|m| m.value)
    }

    fn build_time_series(
        &self,
        metrics: &[MetricPoint],
        name: &str,
        color: &str,
    ) -> TimeSeriesData {
        let points: Vec<DataPoint> = metrics
            .iter()
            .filter(|m| m.name == name)
            .map(|m| DataPoint {
                timestamp: m.timestamp,
                value: m.value,
            })
            .collect();

        TimeSeriesData {
            name: name.to_string(),
            points,
            color: Some(color.to_string()),
        }
    }
}

impl Default for Dashboard {
    fn default() -> Self {
        Self::new(DashboardConfig::default())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn dashboard() -> Dashboard {
        Dashboard::new(DashboardConfig::default())
    }

    fn sample_metric(name: &str, value: f64) -> MetricPoint {
        MetricPoint {
            name: name.to_string(),
            value,
            tags: HashMap::new(),
            timestamp: Utc::now(),
        }
    }

    fn sample_alert(id: &str, severity: AlertSeverity) -> Alert {
        Alert {
            id: id.to_string(),
            severity,
            title: format!("Alert {}", id),
            message: "Test alert".to_string(),
            source: "test".to_string(),
            triggered_at: Utc::now(),
            acknowledged: false,
        }
    }

    // -- overview -----------------------------------------------------------

    #[test]
    fn test_overview_generation() {
        let db = dashboard();
        db.record_metric(sample_metric("ops_per_sec", 5000.0));
        db.record_metric(sample_metric("connected_clients", 42.0));
        db.record_metric(sample_metric("memory_used_bytes", 1_000_000.0));
        db.record_metric(sample_metric("keys_total", 100.0));
        db.record_metric(sample_metric("hit_rate", 0.95));

        let overview = db.get_overview();
        assert_eq!(overview.ops_per_sec, 5000.0);
        assert_eq!(overview.connected_clients, 42);
        assert_eq!(overview.memory_used_bytes, 1_000_000);
        assert_eq!(overview.keys_total, 100);
        assert!((overview.hit_rate - 0.95).abs() < f64::EPSILON);
        assert_eq!(overview.cluster_status, "ok");
    }

    #[test]
    fn test_overview_empty() {
        let db = dashboard();
        let overview = db.get_overview();
        assert_eq!(overview.ops_per_sec, 0.0);
        assert_eq!(overview.connected_clients, 0);
        assert_eq!(overview.keys_total, 0);
    }

    #[test]
    fn test_overview_serialization() {
        let db = dashboard();
        let overview = db.get_overview();
        let json = serde_json::to_string(&overview).unwrap();
        let restored: DashboardOverview = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.cluster_status, overview.cluster_status);
    }

    // -- widget data --------------------------------------------------------

    #[test]
    fn test_widget_data_retrieval() {
        let db = dashboard();
        db.record_metric(sample_metric("ops_per_sec", 1234.0));

        let data = db.get_widget_data("ops-per-sec");
        assert!(data.is_some());
        let data = data.unwrap();
        assert_eq!(data.widget_id, "ops-per-sec");
        assert_eq!(data.widget_type, WidgetType::Gauge);
        if let WidgetContent::SingleValue { value, unit, .. } = &data.data {
            assert_eq!(*value, 1234.0);
            assert_eq!(unit, "ops/s");
        } else {
            panic!("expected SingleValue content");
        }
    }

    #[test]
    fn test_widget_data_missing() {
        let db = dashboard();
        assert!(db.get_widget_data("nonexistent").is_none());
    }

    #[test]
    fn test_widget_data_time_series() {
        let db = dashboard();
        db.record_metric(sample_metric("latency_p99", 100.0));
        db.record_metric(sample_metric("latency_p99", 120.0));

        let data = db.get_widget_data("latency-p99").unwrap();
        assert_eq!(data.widget_type, WidgetType::LineChart);
        if let WidgetContent::TimeSeries { series } = &data.data {
            assert_eq!(series.len(), 1);
            assert_eq!(series[0].points.len(), 2);
        } else {
            panic!("expected TimeSeries content");
        }
    }

    #[test]
    fn test_widget_data_topology() {
        let db = dashboard();
        let data = db.get_widget_data("cluster-topology").unwrap();
        assert_eq!(data.widget_type, WidgetType::Topology);
        if let WidgetContent::TopologyData { nodes, .. } = &data.data {
            assert!(!nodes.is_empty());
        } else {
            panic!("expected TopologyData content");
        }
    }

    #[test]
    fn test_widget_data_heatmap() {
        let db = dashboard();
        let data = db.get_widget_data("key-access-heatmap").unwrap();
        assert_eq!(data.widget_type, WidgetType::Heatmap);
        if let WidgetContent::HeatmapData { labels, values } = &data.data {
            assert!(!labels.is_empty());
            assert!(!values.is_empty());
        } else {
            panic!("expected HeatmapData content");
        }
    }

    #[test]
    fn test_widget_data_serialization() {
        let db = dashboard();
        let data = db.get_widget_data("ops-per-sec").unwrap();
        let json = serde_json::to_string(&data).unwrap();
        let restored: WidgetData = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.widget_id, "ops-per-sec");
    }

    // -- metric recording ---------------------------------------------------

    #[test]
    fn test_metric_recording() {
        let db = dashboard();
        for i in 0..5 {
            db.record_metric(sample_metric("cpu", i as f64));
        }
        let stats = db.get_stats();
        assert_eq!(stats.total_metrics_recorded, 5);
        assert_eq!(stats.data_points_stored, 5);
    }

    #[test]
    fn test_metric_retention_window() {
        let config = DashboardConfig {
            max_data_points: 10,
            ..DashboardConfig::default()
        };
        let db = Dashboard::new(config);
        for i in 0..25 {
            db.record_metric(sample_metric("x", i as f64));
        }
        let stats = db.get_stats();
        assert_eq!(stats.total_metrics_recorded, 25);
        assert!(stats.data_points_stored <= 10);
    }

    #[test]
    fn test_metric_with_tags() {
        let db = dashboard();
        let mut tags = HashMap::new();
        tags.insert("host".to_string(), "node-1".to_string());
        db.record_metric(MetricPoint {
            name: "cpu".to_string(),
            value: 75.0,
            tags,
            timestamp: Utc::now(),
        });
        let stats = db.get_stats();
        assert_eq!(stats.total_metrics_recorded, 1);
    }

    // -- alert lifecycle ----------------------------------------------------

    #[test]
    fn test_alert_lifecycle() {
        let db = dashboard();
        db.add_alert(sample_alert("a1", AlertSeverity::Warning));
        db.add_alert(sample_alert("a2", AlertSeverity::Critical));

        let alerts = db.get_alerts();
        assert_eq!(alerts.len(), 2);
        assert!(!alerts[0].acknowledged);

        assert!(db.acknowledge_alert("a1"));
        let alerts = db.get_alerts();
        assert!(alerts.iter().find(|a| a.id == "a1").unwrap().acknowledged);
        assert!(!alerts.iter().find(|a| a.id == "a2").unwrap().acknowledged);
    }

    #[test]
    fn test_acknowledge_nonexistent() {
        let db = dashboard();
        assert!(!db.acknowledge_alert("nope"));
    }

    #[test]
    fn test_alert_stats() {
        let db = dashboard();
        db.add_alert(sample_alert("a1", AlertSeverity::Info));
        db.add_alert(sample_alert("a2", AlertSeverity::Warning));
        db.add_alert(sample_alert("a3", AlertSeverity::Critical));
        db.acknowledge_alert("a2");

        let stats = db.get_stats();
        assert_eq!(stats.total_alerts, 3);
        assert_eq!(stats.active_alerts, 2);
        assert_eq!(stats.acknowledged_alerts, 1);
    }

    #[test]
    fn test_alert_serialization() {
        let alert = sample_alert("s1", AlertSeverity::Critical);
        let json = serde_json::to_string(&alert).unwrap();
        let restored: Alert = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.id, "s1");
        assert_eq!(restored.severity, AlertSeverity::Critical);
    }

    // -- layout configuration -----------------------------------------------

    #[test]
    fn test_layout_configuration() {
        let db = dashboard();
        let layout = DashboardLayout {
            widgets: vec![
                WidgetPlacement {
                    widget_id: "ops-per-sec".to_string(),
                    x: 0,
                    y: 0,
                    width: 4,
                    height: 2,
                },
                WidgetPlacement {
                    widget_id: "memory-usage".to_string(),
                    x: 4,
                    y: 0,
                    width: 4,
                    height: 2,
                },
            ],
        };
        db.configure_layout(layout);

        let stats = db.get_stats();
        assert_eq!(stats.widgets_configured, 2);
    }

    #[test]
    fn test_layout_serialization() {
        let layout = DashboardLayout {
            widgets: vec![WidgetPlacement {
                widget_id: "ops-per-sec".to_string(),
                x: 0,
                y: 0,
                width: 2,
                height: 1,
            }],
        };
        let json = serde_json::to_string(&layout).unwrap();
        let restored: DashboardLayout = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.widgets.len(), 1);
        assert_eq!(restored.widgets[0].widget_id, "ops-per-sec");
    }

    // -- default widgets ----------------------------------------------------

    #[test]
    fn test_default_widgets() {
        let widgets = default_widgets();
        assert_eq!(widgets.len(), 10);

        let ids: Vec<&str> = widgets.iter().map(|w| w.id.as_str()).collect();
        assert!(ids.contains(&"ops-per-sec"));
        assert!(ids.contains(&"latency-p99"));
        assert!(ids.contains(&"memory-usage"));
        assert!(ids.contains(&"connected-clients"));
        assert!(ids.contains(&"command-distribution"));
        assert!(ids.contains(&"slow-queries"));
        assert!(ids.contains(&"cluster-topology"));
        assert!(ids.contains(&"key-access-heatmap"));
        assert!(ids.contains(&"tiering-costs"));
        assert!(ids.contains(&"replication-lag"));
    }

    #[test]
    fn test_default_widgets_types() {
        let widgets = default_widgets();
        let find = |id: &str| widgets.iter().find(|w| w.id == id).unwrap();
        assert_eq!(find("ops-per-sec").widget_type, WidgetType::Gauge);
        assert_eq!(find("latency-p99").widget_type, WidgetType::LineChart);
        assert_eq!(find("cluster-topology").widget_type, WidgetType::Topology);
        assert_eq!(find("key-access-heatmap").widget_type, WidgetType::Heatmap);
        assert_eq!(find("connected-clients").widget_type, WidgetType::Counter);
    }

    #[test]
    fn test_widget_info_serialization() {
        let widgets = default_widgets();
        let json = serde_json::to_string(&widgets).unwrap();
        let restored: Vec<WidgetInfo> = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.len(), widgets.len());
    }

    // -- stats tracking -----------------------------------------------------

    #[test]
    fn test_stats_initial() {
        let db = dashboard();
        let stats = db.get_stats();
        assert_eq!(stats.total_metrics_recorded, 0);
        assert_eq!(stats.total_alerts, 0);
        assert_eq!(stats.active_alerts, 0);
        assert_eq!(stats.acknowledged_alerts, 0);
        assert_eq!(stats.data_points_stored, 0);
        // Default layout has 10 widgets.
        assert_eq!(stats.widgets_configured, 10);
    }

    #[test]
    fn test_stats_after_operations() {
        let db = dashboard();
        db.record_metric(sample_metric("a", 1.0));
        db.record_metric(sample_metric("b", 2.0));
        db.add_alert(sample_alert("x", AlertSeverity::Info));

        let stats = db.get_stats();
        assert_eq!(stats.total_metrics_recorded, 2);
        assert_eq!(stats.data_points_stored, 2);
        assert_eq!(stats.total_alerts, 1);
        assert_eq!(stats.active_alerts, 1);
    }

    // -- enum display -------------------------------------------------------

    #[test]
    fn test_widget_type_display() {
        assert_eq!(WidgetType::LineChart.to_string(), "line_chart");
        assert_eq!(WidgetType::Gauge.to_string(), "gauge");
        assert_eq!(WidgetType::Topology.to_string(), "topology");
    }

    #[test]
    fn test_alert_severity_display() {
        assert_eq!(AlertSeverity::Info.to_string(), "info");
        assert_eq!(AlertSeverity::Warning.to_string(), "warning");
        assert_eq!(AlertSeverity::Critical.to_string(), "critical");
    }

    // -- default impl -------------------------------------------------------

    #[test]
    fn test_dashboard_default() {
        let db = Dashboard::default();
        let stats = db.get_stats();
        assert_eq!(stats.widgets_configured, 10);
    }

    #[test]
    fn test_dashboard_config_default() {
        let config = DashboardConfig::default();
        assert_eq!(config.refresh_interval_ms, 1000);
        assert_eq!(config.max_data_points, 1000);
        assert_eq!(config.retention_minutes, 60);
        assert!(config.enable_alerts);
        assert_eq!(config.alert_cooldown_secs, 300);
    }

    // -- available widgets from dashboard -----------------------------------

    #[test]
    fn test_get_available_widgets() {
        let db = dashboard();
        let widgets = db.get_available_widgets();
        assert_eq!(widgets.len(), 10);
    }
}
