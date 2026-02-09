//! Native gRPC-compatible service layer for Ferrite.
//!
//! This module provides protobuf-equivalent Rust types and service definitions
//! that model a gRPC API for Ferrite without requiring heavy `tonic`/`prost`
//! compile-time dependencies.  The [`service::FerriteService`] trait mirrors
//! what `tonic` would generate from a `.proto` file; the
//! [`service::FerriteServiceImpl`] fulfils every method by delegating to the
//! underlying storage engine.
//!
//! When you are ready to expose a real gRPC endpoint, add `tonic` and wire
//! [`service::FerriteServiceImpl`] into the generated server — all request /
//! response types in [`types`] map 1-to-1 to protobuf messages.

pub mod service;
pub mod types;

// Re-exports for convenience.
pub use service::{FerriteService, FerriteServiceImpl, ServiceRegistry};
pub use types::*;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// TLS configuration for the gRPC listener.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcTlsConfig {
    /// Path to the PEM-encoded server certificate.
    pub cert_path: String,
    /// Path to the PEM-encoded private key.
    pub key_path: String,
    /// Optional path to a CA certificate for mutual TLS.
    pub ca_path: Option<String>,
}

/// Configuration for the gRPC service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcConfig {
    /// Whether the gRPC listener is enabled.
    pub enabled: bool,
    /// Port to listen on.
    pub port: u16,
    /// Maximum inbound message size in bytes (default 4 MB).
    pub max_message_size: usize,
    /// Expose gRPC reflection service.
    pub enable_reflection: bool,
    /// Optional TLS configuration.
    pub tls: Option<GrpcTlsConfig>,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            port: 6380,
            max_message_size: 4 * 1024 * 1024, // 4 MB
            enable_reflection: true,
            tls: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors returned by gRPC service methods.
#[derive(Debug, thiserror::Error)]
pub enum GrpcError {
    /// The request was malformed or contained invalid parameters.
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// The requested key or resource was not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// An unexpected internal error occurred.
    #[error("internal error: {0}")]
    Internal(String),

    /// The caller is not authorised to perform the operation.
    #[error("unauthorized: {0}")]
    Unauthorized(String),

    /// The service is temporarily unavailable.
    #[error("unavailable: {0}")]
    Unavailable(String),
}
