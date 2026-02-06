//! Input validation for DoS prevention
//!
//! This module provides validation functions that enforce protocol-level limits
//! to prevent denial-of-service attacks via oversized requests, keys, values,
//! or excessive array lengths.

use crate::config::ProtocolLimits;
use crate::error::{FerriteError, Result};

/// Validate that a request frame size is within limits.
#[inline]
pub fn validate_request_size(size: usize, limits: &ProtocolLimits) -> Result<()> {
    if size > limits.max_request_size {
        return Err(FerriteError::RequestTooLarge {
            size,
            max: limits.max_request_size,
        });
    }
    Ok(())
}

/// Validate that a key size is within limits.
#[inline]
pub fn validate_key_size(key: &[u8], limits: &ProtocolLimits) -> Result<()> {
    if key.len() > limits.max_key_size {
        return Err(FerriteError::KeyTooLarge {
            size: key.len(),
            max: limits.max_key_size,
        });
    }
    Ok(())
}

/// Validate that a value size is within limits.
#[inline]
pub fn validate_value_size(value: &[u8], limits: &ProtocolLimits) -> Result<()> {
    if value.len() > limits.max_value_size {
        return Err(FerriteError::ValueTooLarge {
            size: value.len(),
            max: limits.max_value_size,
        });
    }
    Ok(())
}

/// Validate that an array length is within limits.
#[inline]
pub fn validate_array_length(len: usize, limits: &ProtocolLimits) -> Result<()> {
    if len > limits.max_array_length {
        return Err(FerriteError::TooManyElements {
            count: len,
            max: limits.max_array_length,
        });
    }
    Ok(())
}

/// Validate that a command argument count is within limits.
#[inline]
pub fn validate_argument_count(count: usize, limits: &ProtocolLimits) -> Result<()> {
    if count > limits.max_arguments {
        return Err(FerriteError::TooManyElements {
            count,
            max: limits.max_arguments,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn custom_limits() -> ProtocolLimits {
        ProtocolLimits {
            max_request_size: 1024,
            max_key_size: 64,
            max_value_size: 256,
            max_array_length: 10,
            max_arguments: 5,
            max_inline_size: 128,
        }
    }

    #[test]
    fn test_validate_request_size_ok() {
        let limits = custom_limits();
        assert!(validate_request_size(512, &limits).is_ok());
        assert!(validate_request_size(1024, &limits).is_ok());
    }

    #[test]
    fn test_validate_request_size_exceeds() {
        let limits = custom_limits();
        let err = validate_request_size(1025, &limits).unwrap_err();
        assert!(matches!(err, FerriteError::RequestTooLarge { size: 1025, max: 1024 }));
    }

    #[test]
    fn test_validate_key_size_ok() {
        let limits = custom_limits();
        assert!(validate_key_size(b"short_key", &limits).is_ok());
    }

    #[test]
    fn test_validate_key_size_exceeds() {
        let limits = custom_limits();
        let big_key = vec![b'k'; 65];
        let err = validate_key_size(&big_key, &limits).unwrap_err();
        assert!(matches!(err, FerriteError::KeyTooLarge { size: 65, max: 64 }));
    }

    #[test]
    fn test_validate_value_size_ok() {
        let limits = custom_limits();
        assert!(validate_value_size(b"hello", &limits).is_ok());
        assert!(validate_value_size(&vec![0u8; 256], &limits).is_ok());
    }

    #[test]
    fn test_validate_value_size_exceeds() {
        let limits = custom_limits();
        let big_val = vec![0u8; 257];
        let err = validate_value_size(&big_val, &limits).unwrap_err();
        assert!(matches!(err, FerriteError::ValueTooLarge { size: 257, max: 256 }));
    }

    #[test]
    fn test_validate_array_length_ok() {
        let limits = custom_limits();
        assert!(validate_array_length(10, &limits).is_ok());
        assert!(validate_array_length(0, &limits).is_ok());
    }

    #[test]
    fn test_validate_array_length_exceeds() {
        let limits = custom_limits();
        let err = validate_array_length(11, &limits).unwrap_err();
        assert!(matches!(err, FerriteError::TooManyElements { count: 11, max: 10 }));
    }

    #[test]
    fn test_validate_argument_count_ok() {
        let limits = custom_limits();
        assert!(validate_argument_count(5, &limits).is_ok());
    }

    #[test]
    fn test_validate_argument_count_exceeds() {
        let limits = custom_limits();
        let err = validate_argument_count(6, &limits).unwrap_err();
        assert!(matches!(err, FerriteError::TooManyElements { count: 6, max: 5 }));
    }

    #[test]
    fn test_default_limits_match_redis() {
        let limits = ProtocolLimits::default();
        assert_eq!(limits.max_key_size, 512);
        assert_eq!(limits.max_value_size, 512 * 1024 * 1024);
        assert_eq!(limits.max_request_size, 512 * 1024 * 1024);
        assert_eq!(limits.max_array_length, 1_048_576);
        assert_eq!(limits.max_arguments, 100_000);
        assert_eq!(limits.max_inline_size, 64 * 1024);
    }

    #[test]
    fn test_custom_limits_can_be_configured() {
        let limits = ProtocolLimits {
            max_request_size: 1_000_000,
            max_key_size: 1024,
            max_value_size: 10_000_000,
            max_array_length: 500,
            max_arguments: 200,
            max_inline_size: 8192,
        };
        assert_eq!(limits.max_request_size, 1_000_000);
        assert_eq!(limits.max_key_size, 1024);
        assert_eq!(limits.max_value_size, 10_000_000);
        assert_eq!(limits.max_array_length, 500);
        assert_eq!(limits.max_arguments, 200);
        assert_eq!(limits.max_inline_size, 8192);
    }
}
