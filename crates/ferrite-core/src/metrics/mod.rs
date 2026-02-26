//! Metrics module for Ferrite
//!
//! This module implements observability features including Prometheus metrics,
//! structured logging, OpenTelemetry integration, and anomaly detection.

/// Anomaly detection and alerting engine.
pub mod anomaly;
/// Metrics-to-anomaly bridge for automatic observation.
pub mod bridge;
/// Production-grade metrics instrumentation with histograms and Grafana dashboard generation.
pub mod instrumentation;
pub mod otel;
mod prometheus;
mod recorder;

pub use anomaly::{
    Alert, AlertRule, AlertSeverity, AnomalyConfig, AnomalyDetector, DetectionMethod,
    DetectorSummary, RuleType, WebhookConfig, WebhookPayload,
};
pub use bridge::{BridgeConfig, MetricsBridge};
pub use otel::{init_otel, is_otel_available, OtelHandle};
pub use prometheus::MetricsServer;
pub use recorder::*;
