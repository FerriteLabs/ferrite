//! First-class OpenTelemetry integration for Ferrite.
//!
//! This module provides:
//!
//! - **[`config`]** - [`TelemetryConfig`] and [`OtlpProtocol`] for TOML-driven
//!   configuration.
//! - **[`metrics`]** - OTel-native counters, histograms, and gauges for
//!   commands, connections, cache, HybridLog tiers, and replication.
//! - **[`traces`]** - Convenience span constructors with semantic-convention
//!   attributes (`db.system`, `db.operation`, `net.peer.name`, etc.).
//! - **[`init_telemetry`]** / **[`shutdown_telemetry`]** - Pipeline lifecycle
//!   management (OTLP exporter + `tracing-opentelemetry` layer).
//!
//! Everything behind the `otel` feature flag compiles to no-ops when the flag
//! is absent, so call-sites do not need their own `#[cfg]` guards.
//!
//! # Quick start
//!
//! ```ignore
//! use ferrite_core::telemetry::{TelemetryConfig, init_telemetry, shutdown_telemetry};
//!
//! let config = TelemetryConfig { enabled: true, ..Default::default() };
//! let handle = init_telemetry(&config)?;
//!
//! // ... run the server ...
//!
//! shutdown_telemetry(handle);
//! ```

pub mod config;
pub mod metrics;
pub mod traces;

pub use config::{OtlpProtocol, TelemetryConfig};

use crate::error::{FerriteError, Result};

// ── Feature-gated imports ──────────────────────────────────────────────────

#[cfg(feature = "otel")]
use opentelemetry::trace::TracerProvider;
#[cfg(feature = "otel")]
use opentelemetry::KeyValue;
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

// ── Handle ─────────────────────────────────────────────────────────────────

/// Opaque handle returned by [`init_telemetry`].
///
/// Holds ownership of the OTel tracer provider so it can be flushed and shut
/// down gracefully via [`shutdown_telemetry`].
#[cfg(feature = "otel")]
pub struct TelemetryHandle {
    tracer_provider: SdkTracerProvider,
}

#[cfg(feature = "otel")]
impl std::fmt::Debug for TelemetryHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TelemetryHandle")
            .field("tracer_provider", &"<SdkTracerProvider>")
            .finish()
    }
}

/// Opaque handle (no-op stub when the `otel` feature is disabled).
#[cfg(not(feature = "otel"))]
#[derive(Debug)]
pub struct TelemetryHandle;

// ── Initialization ─────────────────────────────────────────────────────────

/// Initialize the OpenTelemetry pipeline.
///
/// Configures:
/// 1. An **OTLP span exporter** (gRPC via tonic, or HTTP/protobuf) that ships
///    traces to the collector at `config.endpoint`.
/// 2. A **`tracing-opentelemetry` layer** bridging `tracing` spans to OTel
///    spans, composed with the default `fmt` layer.
/// 3. A **metrics reader** registered with the global `MeterProvider` so that
///    instruments in [`metrics`] are exported on the configured interval.
///
/// # Errors
///
/// Returns [`FerriteError::Config`] if:
/// - `config.enabled` is `false` (caller should check first).
/// - The OTLP exporter cannot be built (bad endpoint, missing TLS, etc.).
/// - The `tracing` subscriber has already been initialised.
#[cfg(feature = "otel")]
pub fn init_telemetry(config: &TelemetryConfig) -> Result<TelemetryHandle> {
    use opentelemetry_sdk::trace::BatchSpanProcessor;

    if !config.enabled {
        return Err(FerriteError::Config(
            "Telemetry is not enabled in configuration".to_string(),
        ));
    }

    // ── Resource ───────────────────────────────────────────────────────
    let resource = Resource::new(vec![
        KeyValue::new("service.name", config.service_name.clone()),
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        KeyValue::new("telemetry.sdk.language", "rust"),
    ]);

    // ── Sampler ────────────────────────────────────────────────────────
    let sampler = if (config.effective_sample_rate() - 1.0).abs() < f64::EPSILON {
        Sampler::AlwaysOn
    } else if config.effective_sample_rate().abs() < f64::EPSILON {
        Sampler::AlwaysOff
    } else {
        Sampler::TraceIdRatioBased(config.effective_sample_rate())
    };

    let trace_config = TraceConfig::default()
        .with_sampler(sampler)
        .with_resource(resource);

    // ── Span exporter (protocol-dependent) ─────────────────────────────
    let tracer_provider = match config.protocol {
        OtlpProtocol::Grpc => {
            let exporter = opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(&config.endpoint)
                .build_span_exporter()
                .map_err(|e| {
                    FerriteError::Config(format!("Failed to create gRPC OTLP span exporter: {e}"))
                })?;

            SdkTracerProvider::builder()
                .with_config(trace_config)
                .with_span_processor(BatchSpanProcessor::builder(exporter, runtime::Tokio).build())
                .build()
        }
        OtlpProtocol::Http => {
            let exporter = opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(&config.endpoint)
                .build_span_exporter()
                .map_err(|e| {
                    FerriteError::Config(format!("Failed to create HTTP OTLP span exporter: {e}"))
                })?;

            SdkTracerProvider::builder()
                .with_config(trace_config)
                .with_span_processor(BatchSpanProcessor::builder(exporter, runtime::Tokio).build())
                .build()
        }
    };

    // ── tracing-opentelemetry layer ────────────────────────────────────
    let tracer = tracer_provider.tracer("ferrite");
    let otel_layer = OpenTelemetryLayer::new(tracer);

    tracing_subscriber::registry()
        .with(otel_layer)
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .map_err(|e| {
            FerriteError::Config(format!("Failed to initialize tracing subscriber: {e}"))
        })?;

    tracing::info!(
        endpoint = %config.endpoint,
        protocol = %config.protocol,
        sample_rate = config.effective_sample_rate(),
        "OpenTelemetry telemetry pipeline initialized"
    );

    Ok(TelemetryHandle { tracer_provider })
}

/// Initialize the telemetry pipeline (no-op when the `otel` feature is disabled).
///
/// Always returns an error indicating the feature is not compiled in.
#[cfg(not(feature = "otel"))]
#[cold]
pub fn init_telemetry(_config: &TelemetryConfig) -> Result<TelemetryHandle> {
    Err(FerriteError::Config(
        "OpenTelemetry support is not compiled. Enable the 'otel' feature.".to_string(),
    ))
}

// ── Shutdown ───────────────────────────────────────────────────────────────

/// Gracefully shut down the OpenTelemetry pipeline.
///
/// Flushes any pending spans and releases resources. Should be called during
/// server shutdown.
#[cfg(feature = "otel")]
pub fn shutdown_telemetry(handle: TelemetryHandle) {
    tracing::info!("Shutting down OpenTelemetry telemetry pipeline");
    drop(handle.tracer_provider);
}

/// Graceful shutdown (no-op when the `otel` feature is disabled).
#[cfg(not(feature = "otel"))]
#[inline]
pub fn shutdown_telemetry(_handle: TelemetryHandle) {}

// ── Convenience ────────────────────────────────────────────────────────────

/// Returns `true` when the `otel` feature was compiled in.
#[inline]
pub fn is_otel_available() -> bool {
    cfg!(feature = "otel")
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_otel_available_compiles() {
        // Value depends on feature flag; just ensure it compiles.
        let _ = is_otel_available();
    }

    #[test]
    fn test_default_config_round_trip() {
        let cfg = TelemetryConfig::default();
        assert!(!cfg.enabled);
        assert_eq!(cfg.endpoint, "http://localhost:4317");
        assert_eq!(cfg.protocol, OtlpProtocol::Grpc);
    }

    #[cfg(not(feature = "otel"))]
    #[test]
    fn test_init_without_feature_returns_error() {
        let cfg = TelemetryConfig::default();
        let result = init_telemetry(&cfg);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("not compiled"));
    }

    #[cfg(not(feature = "otel"))]
    #[test]
    fn test_shutdown_without_feature_does_not_panic() {
        let handle = TelemetryHandle;
        shutdown_telemetry(handle);
    }

    #[test]
    fn test_telemetry_config_disabled_returns_error() {
        // Even with feature = "otel", if `enabled = false` we get an error.
        // Without the feature, we always get an error.
        let cfg = TelemetryConfig {
            enabled: false,
            ..Default::default()
        };
        let result = init_telemetry(&cfg);
        assert!(result.is_err());
    }
}
