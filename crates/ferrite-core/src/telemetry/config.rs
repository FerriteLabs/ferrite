//! Telemetry configuration for OpenTelemetry integration.
//!
//! Defines [`TelemetryConfig`] which controls the OTel pipeline:
//! endpoint, protocol (gRPC or HTTP), sampling rate, and export intervals.

use serde::Deserialize;

/// OTLP transport protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OtlpProtocol {
    /// gRPC via tonic (default, port 4317).
    Grpc,
    /// HTTP/protobuf (port 4318).
    Http,
}

impl Default for OtlpProtocol {
    #[inline]
    fn default() -> Self {
        Self::Grpc
    }
}

impl std::fmt::Display for OtlpProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Grpc => write!(f, "grpc"),
            Self::Http => write!(f, "http"),
        }
    }
}

/// Configuration for the OpenTelemetry telemetry pipeline.
///
/// Controls trace and metrics export via OTLP.
///
/// # Example (TOML)
///
/// ```toml
/// [telemetry]
/// enabled = true
/// endpoint = "http://localhost:4317"
/// protocol = "grpc"
/// service_name = "ferrite"
/// sample_rate = 1.0
/// export_interval_secs = 30
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct TelemetryConfig {
    /// Master switch for the OTel pipeline.
    pub enabled: bool,

    /// OTLP collector endpoint (e.g., `http://localhost:4317`).
    pub endpoint: String,

    /// Transport protocol to use for OTLP export.
    pub protocol: OtlpProtocol,

    /// `service.name` resource attribute reported in all telemetry.
    pub service_name: String,

    /// Trace sampling rate in the range `[0.0, 1.0]`.
    /// `1.0` means sample every trace; `0.0` disables tracing.
    pub sample_rate: f64,

    /// How often (in seconds) the metrics exporter flushes to the collector.
    pub export_interval_secs: u64,

    /// Enable distributed tracing export.
    pub traces_enabled: bool,

    /// Enable metrics export via OTLP.
    pub metrics_enabled: bool,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: "http://localhost:4317".to_string(),
            protocol: OtlpProtocol::Grpc,
            service_name: "ferrite".to_string(),
            sample_rate: 1.0,
            export_interval_secs: 30,
            traces_enabled: true,
            metrics_enabled: true,
        }
    }
}

impl TelemetryConfig {
    /// Clamp `sample_rate` to `[0.0, 1.0]`.
    #[inline]
    pub fn effective_sample_rate(&self) -> f64 {
        self.sample_rate.clamp(0.0, 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = TelemetryConfig::default();
        assert!(!cfg.enabled);
        assert_eq!(cfg.endpoint, "http://localhost:4317");
        assert_eq!(cfg.protocol, OtlpProtocol::Grpc);
        assert_eq!(cfg.service_name, "ferrite");
        assert!((cfg.sample_rate - 1.0).abs() < f64::EPSILON);
        assert_eq!(cfg.export_interval_secs, 30);
        assert!(cfg.traces_enabled);
        assert!(cfg.metrics_enabled);
    }

    #[test]
    fn test_effective_sample_rate_clamps_high() {
        let mut cfg = TelemetryConfig::default();
        cfg.sample_rate = 2.5;
        assert!((cfg.effective_sample_rate() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_effective_sample_rate_clamps_low() {
        let mut cfg = TelemetryConfig::default();
        cfg.sample_rate = -0.5;
        assert!(cfg.effective_sample_rate().abs() < f64::EPSILON);
    }

    #[test]
    fn test_otlp_protocol_default() {
        assert_eq!(OtlpProtocol::default(), OtlpProtocol::Grpc);
    }

    #[test]
    fn test_otlp_protocol_display() {
        assert_eq!(OtlpProtocol::Grpc.to_string(), "grpc");
        assert_eq!(OtlpProtocol::Http.to_string(), "http");
    }

    #[test]
    fn test_config_deserialize_from_toml() {
        let toml_str = r#"
            enabled = true
            endpoint = "http://otel-collector:4317"
            protocol = "http"
            service_name = "ferrite-test"
            sample_rate = 0.5
            export_interval_secs = 10
            traces_enabled = true
            metrics_enabled = false
        "#;
        let cfg: TelemetryConfig = toml::from_str(toml_str).expect("valid TOML");
        assert!(cfg.enabled);
        assert_eq!(cfg.endpoint, "http://otel-collector:4317");
        assert_eq!(cfg.protocol, OtlpProtocol::Http);
        assert_eq!(cfg.service_name, "ferrite-test");
        assert!((cfg.sample_rate - 0.5).abs() < f64::EPSILON);
        assert_eq!(cfg.export_interval_secs, 10);
        assert!(cfg.traces_enabled);
        assert!(!cfg.metrics_enabled);
    }
}
