//! Error types for the Ferrite client SDK.

use std::fmt;

/// Result type alias for Ferrite client operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur when interacting with a Ferrite server.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An I/O error occurred during communication.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// The server returned a RESP error response.
    #[error("server error: {0}")]
    Server(String),

    /// The RESP protocol data was malformed or unexpected.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// The response type did not match what was expected.
    #[error("unexpected response type: expected {expected}, got {actual}")]
    UnexpectedResponse {
        /// The expected type description.
        expected: &'static str,
        /// The actual type description.
        actual: String,
    },

    /// The connection to the server was closed.
    #[error("connection closed")]
    ConnectionClosed,

    /// The connection pool is exhausted and no connections are available.
    #[error("connection pool exhausted (max size: {max_size})")]
    PoolExhausted {
        /// The maximum pool size.
        max_size: usize,
    },

    /// A timeout occurred waiting for a connection or response.
    #[error("operation timed out after {0:?}")]
    Timeout(std::time::Duration),

    /// Authentication failed.
    #[error("authentication failed: {0}")]
    Auth(String),

    /// An invalid argument was provided to a command.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
}

/// Describes the type of a RESP value for error messages.
#[derive(Debug, Clone, Copy)]
pub enum ValueKind {
    /// A string value (simple or bulk).
    String,
    /// An integer value.
    Integer,
    /// An array value.
    Array,
    /// A null value.
    Null,
    /// A boolean value.
    Boolean,
    /// A double/float value.
    Double,
}

impl fmt::Display for ValueKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValueKind::String => write!(f, "string"),
            ValueKind::Integer => write!(f, "integer"),
            ValueKind::Array => write!(f, "array"),
            ValueKind::Null => write!(f, "null"),
            ValueKind::Boolean => write!(f, "boolean"),
            ValueKind::Double => write!(f, "double"),
        }
    }
}
