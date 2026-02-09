//! OpenTelemetry integration for Ferrite
//!
//! This module provides OpenTelemetry integration for distributed tracing
//! and metrics export via OTLP.
//!
//! # Feature Flag
//!
//! This module is only compiled when the `otel` feature is enabled:
//! ```toml
//! [dependencies]
//! ferrite = { version = "0.1", features = ["otel"] }
//! ```

use crate::config::OpenTelemetryConfig;
use crate::error::{FerriteError, Result};

#[cfg(feature = "otel")]
use opentelemetry::trace::TracerProvider;
#[cfg(feature = "otel")]
use opentelemetry_otlp::WithExportConfig;
#[cfg(feature = "otel")]
use opentelemetry_sdk::{
    runtime,
    trace::{Config as TraceConfig, Sampler, TracerProvider as SdkTracerProvider},
    Resource,
};
#[cfg(feature = "otel")]
use tracing_opentelemetry::OpenTelemetryLayer;
#[cfg(feature = "otel")]
use tracing_subscriber::layer::SubscriberExt;
#[cfg(feature = "otel")]
use tracing_subscriber::util::SubscriberInitExt;

/// OpenTelemetry tracer handle
///
/// This handle can be used to shutdown the tracer gracefully.
#[cfg(feature = "otel")]
pub struct OtelHandle {
    tracer_provider: SdkTracerProvider,
}

#[cfg(feature = "otel")]
impl OtelHandle {
    /// Shutdown the OpenTelemetry tracer
    pub fn shutdown(self) {
        // Force flush and shutdown
        drop(self.tracer_provider);
    }
}

#[cfg(not(feature = "otel"))]
/// Handle for OpenTelemetry lifecycle management (no-op when feature disabled)
pub struct OtelHandle;

#[cfg(not(feature = "otel"))]
impl OtelHandle {
    /// No-op shutdown when otel feature is disabled
    pub fn shutdown(self) {}
}

/// Initialize OpenTelemetry tracing
///
/// Sets up the OTLP exporter and integrates with the tracing subscriber.
/// Returns a handle that can be used to shutdown the tracer.
#[cfg(feature = "otel")]
pub fn init_otel(config: &OpenTelemetryConfig) -> Result<OtelHandle> {
    use opentelemetry::KeyValue;
    use opentelemetry_sdk::trace::BatchSpanProcessor;

    if !config.enabled {
        return Err(FerriteError::Config(
            "OpenTelemetry is not enabled in configuration".to_string(),
        ));
    }

    // Create OTLP span exporter
    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(&config.endpoint)
        .build_span_exporter()
        .map_err(|e| FerriteError::Config(format!("Failed to create OTLP exporter: {e}")))?;

    // Configure trace settings
    let trace_config = TraceConfig::default()
        .with_sampler(Sampler::AlwaysOn)
        .with_resource(Resource::new(vec![
            KeyValue::new("service.name", config.service_name.clone()),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        ]));

    // Build the tracer provider with batch processor
    let tracer_provider = SdkTracerProvider::builder()
        .with_config(trace_config)
        .with_span_processor(BatchSpanProcessor::builder(exporter, runtime::Tokio).build())
        .build();

    // Create the tracing layer using the tracer from provider
    let tracer = tracer_provider.tracer("ferrite");
    let otel_layer = OpenTelemetryLayer::new(tracer);

    // Initialize the subscriber with the OpenTelemetry layer
    tracing_subscriber::registry()
        .with(otel_layer)
        .with(tracing_subscriber::fmt::layer())
        .try_init()
        .map_err(|e| FerriteError::Config(format!("Failed to initialize tracing: {e}")))?;

    tracing::info!(
        "OpenTelemetry initialized with endpoint: {}",
        config.endpoint
    );

    Ok(OtelHandle { tracer_provider })
}

#[cfg(not(feature = "otel"))]
/// Initialize OpenTelemetry (returns error when feature is disabled)
pub fn init_otel(_config: &OpenTelemetryConfig) -> Result<OtelHandle> {
    Err(FerriteError::Config(
        "OpenTelemetry support is not compiled. Enable the 'otel' feature.".to_string(),
    ))
}

/// Check if OpenTelemetry feature is enabled
pub fn is_otel_available() -> bool {
    cfg!(feature = "otel")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_otel_available() {
        // Just verify the function works
        let _ = is_otel_available();
    }

    #[test]
    fn test_otel_config_default() {
        let config = OpenTelemetryConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.endpoint, "http://localhost:4317");
        assert_eq!(config.service_name, "ferrite");
        assert!(config.traces_enabled);
        assert!(config.metrics_enabled);
    }

    #[cfg(not(feature = "otel"))]
    #[test]
    fn test_init_otel_without_feature() {
        let config = OpenTelemetryConfig::default();
        let result = init_otel(&config);
        assert!(result.is_err());
    }
}
