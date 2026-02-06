//! Error types for Ferrite
//!
//! This module defines all error types used throughout the Ferrite codebase.
//! Uses `thiserror` for ergonomic error definitions.

use std::io;
use thiserror::Error;

/// Main error type for Ferrite operations
#[derive(Error, Debug)]
pub enum FerriteError {
    /// Protocol parsing or encoding error
    #[error("Protocol error: {0}")]
    Protocol(String),

    /// Unknown or unimplemented command
    #[error("Unknown command: {0}")]
    UnknownCommand(String),

    /// Wrong number of arguments for a command
    #[error("Wrong number of arguments for '{0}' command")]
    WrongArity(String),

    /// Invalid argument value or format
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// Command syntax error
    #[error("ERR syntax error")]
    Syntax,

    /// Operation on wrong data type
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    /// Value cannot be parsed as integer
    #[error("ERR value is not an integer or out of range")]
    NotInteger,

    /// Value cannot be parsed as float
    #[error("ERR value is not a valid float")]
    NotFloat,

    /// Index is out of valid range
    #[error("ERR index out of range")]
    IndexOutOfRange,

    /// Memory limit exceeded
    #[error("OOM command not allowed when used memory > 'maxmemory'")]
    OutOfMemory,

    /// Invalid database index
    #[error("ERR invalid DB index")]
    InvalidDbIndex,

    /// Key does not exist
    #[error("Key not found: {0}")]
    KeyNotFound(String),

    /// Append-only file operation error
    #[error("AOF error: {0}")]
    Aof(String),

    /// RDB snapshot operation error
    #[error("RDB error: {0}")]
    Rdb(String),

    /// Checkpoint operation error
    #[error("Checkpoint error: {0}")]
    Checkpoint(String),

    /// Data recovery error
    #[error("Recovery error: {0}")]
    Recovery(String),

    /// Underlying I/O error
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Configuration parsing or validation error
    #[error("Configuration error: {0}")]
    Config(String),

    /// Authentication required but not provided
    #[error("NOAUTH Authentication required")]
    NoAuth,

    /// User lacks permission for the command
    #[error("NOPERM this user has no permissions to run the '{0}' command")]
    NoPermission(String),

    /// Invalid password provided
    #[error("ERR invalid password")]
    InvalidPassword,

    /// Connection was closed
    #[error("Connection closed")]
    ConnectionClosed,

    /// Connection-related error
    #[error("Connection error: {0}")]
    Connection(String),

    /// Internal server error
    #[error("Internal error: {0}")]
    Internal(String),

    /// Encryption or decryption error
    #[error("Encryption error: {0}")]
    Encryption(String),
}

/// Result type alias for Ferrite operations
pub type Result<T> = std::result::Result<T, FerriteError>;

impl FerriteError {
    /// Returns true if this error should close the connection
    #[cold]
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            FerriteError::Io(_) | FerriteError::ConnectionClosed | FerriteError::Internal(_)
        )
    }

    /// Convert error to RESP error string
    #[cold]
    pub fn to_resp_error(&self) -> String {
        match self {
            FerriteError::WrongType => {
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string()
            }
            FerriteError::NotInteger => "ERR value is not an integer or out of range".to_string(),
            FerriteError::NotFloat => "ERR value is not a valid float".to_string(),
            FerriteError::IndexOutOfRange => "ERR index out of range".to_string(),
            FerriteError::OutOfMemory => {
                "OOM command not allowed when used memory > 'maxmemory'".to_string()
            }
            FerriteError::InvalidDbIndex => "ERR invalid DB index".to_string(),
            FerriteError::NoAuth => "NOAUTH Authentication required".to_string(),
            FerriteError::NoPermission(cmd) => {
                format!("NOPERM this user has no permissions to run the '{cmd}' command")
            }
            FerriteError::InvalidPassword => "ERR invalid password".to_string(),
            FerriteError::Syntax => "ERR syntax error".to_string(),
            FerriteError::WrongArity(cmd) => {
                format!("ERR wrong number of arguments for '{cmd}' command")
            }
            FerriteError::UnknownCommand(cmd) => {
                format!("ERR unknown command '{cmd}', with args beginning with:")
            }
            _ => format!("ERR {self}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_is_fatal() {
        assert!(FerriteError::ConnectionClosed.is_fatal());
        assert!(FerriteError::Internal("test".to_string()).is_fatal());
        assert!(!FerriteError::WrongType.is_fatal());
        assert!(!FerriteError::NotInteger.is_fatal());
    }

    #[test]
    fn test_error_to_resp() {
        assert_eq!(
            FerriteError::WrongType.to_resp_error(),
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        );
        assert_eq!(
            FerriteError::WrongArity("GET".to_string()).to_resp_error(),
            "ERR wrong number of arguments for 'GET' command"
        );
    }
}
