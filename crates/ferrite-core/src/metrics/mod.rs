//! Metrics module for Ferrite
//!
//! This module implements observability features including Prometheus metrics,
//! structured logging, and OpenTelemetry integration.

/// Production-grade metrics instrumentation with histograms and Grafana dashboard generation.
pub mod instrumentation;
pub mod otel;
mod prometheus;
mod recorder;

pub use otel::{init_otel, is_otel_available, OtelHandle};
pub use prometheus::MetricsServer;
pub use recorder::*;
