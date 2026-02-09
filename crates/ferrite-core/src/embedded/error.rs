//! Error types for embedded database operations

use std::io;
use thiserror::Error;

/// Result type for embedded database operations
pub type Result<T> = std::result::Result<T, EmbeddedError>;

/// Errors that can occur in embedded database operations
#[derive(Debug, Error)]
pub enum EmbeddedError {
    /// Key not found
    #[error("key not found: {0}")]
    KeyNotFound(String),

    /// Type mismatch (e.g., calling list operation on string)
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    /// Invalid argument
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// Value is not a valid integer
    #[error("value is not an integer or out of range")]
    NotAnInteger,

    /// Value is not a valid float
    #[error("value is not a valid float")]
    NotAFloat,

    /// Index out of range
    #[error("index out of range")]
    IndexOutOfRange,

    /// Key already exists (for NX operations)
    #[error("key already exists")]
    KeyExists,

    /// Key does not exist (for XX operations)
    #[error("key does not exist")]
    KeyDoesNotExist,

    /// Database index out of range
    #[error("database index out of range: {0}")]
    InvalidDatabase(u8),

    /// IO error during persistence
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// Serialization error
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Transaction error
    #[error("transaction error: {0}")]
    Transaction(String),

    /// Database is closed
    #[error("database is closed")]
    DatabaseClosed,

    /// Concurrent modification detected
    #[error("concurrent modification detected")]
    ConcurrentModification,

    /// Memory limit exceeded
    #[error("memory limit exceeded")]
    MemoryLimitExceeded,

    /// Pattern syntax error
    #[error("invalid pattern syntax: {0}")]
    InvalidPattern(String),

    /// Nested transaction error
    #[error("nested transactions are not supported")]
    NestedTransaction,

    /// Query execution error
    #[error("query error: {0}")]
    QueryError(String),

    /// Internal error
    #[error("internal error: {0}")]
    Internal(String),
}

impl EmbeddedError {
    /// Check if this is a type error
    pub fn is_wrong_type(&self) -> bool {
        matches!(self, EmbeddedError::WrongType)
    }

    /// Check if this is a key not found error
    pub fn is_not_found(&self) -> bool {
        matches!(self, EmbeddedError::KeyNotFound(_))
    }

    /// Check if this is an IO error
    pub fn is_io_error(&self) -> bool {
        matches!(self, EmbeddedError::Io(_))
    }
}

impl From<&str> for EmbeddedError {
    fn from(s: &str) -> Self {
        EmbeddedError::Internal(s.to_string())
    }
}

impl From<String> for EmbeddedError {
    fn from(s: String) -> Self {
        EmbeddedError::Internal(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = EmbeddedError::KeyNotFound("test:key".to_string());
        assert_eq!(err.to_string(), "key not found: test:key");

        let err = EmbeddedError::WrongType;
        assert!(err.to_string().contains("WRONGTYPE"));
    }

    #[test]
    fn test_error_checks() {
        let err = EmbeddedError::WrongType;
        assert!(err.is_wrong_type());
        assert!(!err.is_not_found());

        let err = EmbeddedError::KeyNotFound("key".to_string());
        assert!(err.is_not_found());
        assert!(!err.is_wrong_type());
    }

    #[test]
    fn test_error_from_string() {
        let err: EmbeddedError = "something went wrong".into();
        assert!(matches!(err, EmbeddedError::Internal(_)));
    }
}
