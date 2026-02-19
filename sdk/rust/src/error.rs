//! Error types for the Ferrite client SDK.
//!
//! All errors produced by this crate are represented by [`FerriteError`].
//! Most variants wrap lower-level errors from the underlying Redis driver
//! while providing Ferrite-specific context where appropriate.

use thiserror::Error;

/// The primary error type returned by all Ferrite client operations.
#[derive(Debug, Error)]
pub enum FerriteError {
    /// An error originating from the underlying Redis protocol layer.
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),

    /// Failure to serialize or deserialize a value (e.g. JSON metadata).
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// The connection to the Ferrite server could not be established.
    #[error("connection error: {0}")]
    Connection(String),

    /// A command received a response that could not be parsed into the
    /// expected format.
    #[error("invalid response: {0}")]
    InvalidResponse(String),

    /// A Ferrite-specific command (vector search, semantic caching, etc.)
    /// returned an error.
    #[error("command error: {0}")]
    Command(String),

    /// A required configuration parameter is missing or invalid.
    #[error("configuration error: {0}")]
    Configuration(String),

    /// The operation timed out before completing.
    #[error("operation timed out after {0}ms")]
    Timeout(u64),

    /// The requested index does not exist.
    #[error("index not found: {0}")]
    IndexNotFound(String),

    /// The requested key does not exist.
    #[error("key not found: {0}")]
    KeyNotFound(String),
}

/// Convenience type alias used throughout the crate.
pub type Result<T> = std::result::Result<T, FerriteError>;
