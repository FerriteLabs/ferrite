#![allow(clippy::unwrap_used)]
//! TLS Connection Lifecycle Integration Tests
//!
//! Tests for TLS configuration and error handling including:
//! - TLS configuration creation
//! - Certificate and key loading errors
//! - mTLS configuration
//! - Error types and messages
#![cfg(feature = "tls")]

use std::fs::File;
use std::io::Write;

use ferrite::server::{TlsConfig, TlsError};

// ============================================================================
// TlsConfig Creation Tests
// ============================================================================

#[test]
fn test_tls_config_basic_creation() {
    let config = TlsConfig::new("server.crt", "server.key");

    assert_eq!(config.cert_file, "server.crt");
    assert_eq!(config.key_file, "server.key");
    assert!(config.ca_file.is_none());
    assert!(!config.require_client_cert);
}

#[test]
fn test_tls_config_from_string_types() {
    // From &str
    let config1 = TlsConfig::new("cert.pem", "key.pem");
    assert_eq!(config1.cert_file, "cert.pem");

    // From String
    let config2 = TlsConfig::new(String::from("cert.pem"), String::from("key.pem"));
    assert_eq!(config2.cert_file, "cert.pem");
}

#[test]
fn test_tls_config_clone() {
    let config = TlsConfig::new("cert.pem", "key.pem").with_client_auth("ca.pem", true);

    let cloned = config.clone();

    assert_eq!(cloned.cert_file, config.cert_file);
    assert_eq!(cloned.key_file, config.key_file);
    assert_eq!(cloned.ca_file, config.ca_file);
    assert_eq!(cloned.require_client_cert, config.require_client_cert);
}

#[test]
fn test_tls_config_debug_format() {
    let config = TlsConfig::new("cert.pem", "key.pem");
    let debug_str = format!("{:?}", config);

    assert!(debug_str.contains("cert.pem"));
    assert!(debug_str.contains("key.pem"));
    assert!(debug_str.contains("TlsConfig"));
}

// ============================================================================
// mTLS Configuration Tests
// ============================================================================

#[test]
fn test_tls_config_with_client_auth_required() {
    let config = TlsConfig::new("server.crt", "server.key").with_client_auth("ca.crt", true);

    assert_eq!(config.ca_file, Some("ca.crt".to_string()));
    assert!(config.require_client_cert);
}

#[test]
fn test_tls_config_with_client_auth_optional() {
    let config = TlsConfig::new("server.crt", "server.key").with_client_auth("ca.crt", false);

    assert_eq!(config.ca_file, Some("ca.crt".to_string()));
    assert!(!config.require_client_cert);
}

#[test]
fn test_tls_config_builder_chain() {
    // Test that builder methods return Self for chaining
    let config = TlsConfig::new("cert.pem", "key.pem").with_client_auth("ca.pem", true);

    // Verify the chain worked
    assert_eq!(config.cert_file, "cert.pem");
    assert_eq!(config.key_file, "key.pem");
    assert_eq!(config.ca_file, Some("ca.pem".to_string()));
    assert!(config.require_client_cert);
}

// ============================================================================
// TlsError Tests
// ============================================================================

#[test]
fn test_tls_error_io() {
    let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let tls_error: TlsError = io_error.into();

    let error_msg = tls_error.to_string();
    assert!(error_msg.contains("IO error"));
    assert!(error_msg.contains("file not found"));
}

#[test]
fn test_tls_error_certificate() {
    let error = TlsError::Certificate("Invalid certificate format".to_string());
    let error_msg = error.to_string();

    assert!(error_msg.contains("Certificate error"));
    assert!(error_msg.contains("Invalid certificate format"));
}

#[test]
fn test_tls_error_private_key() {
    let error = TlsError::PrivateKey("Invalid private key".to_string());
    let error_msg = error.to_string();

    assert!(error_msg.contains("Private key error"));
    assert!(error_msg.contains("Invalid private key"));
}

#[test]
fn test_tls_error_config() {
    let error = TlsError::Config("Configuration mismatch".to_string());
    let error_msg = error.to_string();

    assert!(error_msg.contains("TLS configuration error"));
    assert!(error_msg.contains("Configuration mismatch"));
}

#[test]
fn test_tls_error_debug_format() {
    let error = TlsError::Certificate("test".to_string());
    let debug_str = format!("{:?}", error);

    assert!(debug_str.contains("Certificate"));
}

// ============================================================================
// Certificate Loading Error Tests
// ============================================================================

#[test]
fn test_build_acceptor_missing_cert_file() {
    let config = TlsConfig::new("/nonexistent/path/cert.pem", "/nonexistent/path/key.pem");
    let result = config.build_acceptor();

    match result {
        Err(TlsError::Certificate(msg)) => {
            assert!(msg.contains("Failed to open"));
        }
        Err(err) => panic!("Expected Certificate error, got {:?}", err),
        Ok(_) => panic!("Expected error, got Ok"),
    }
}

#[test]
fn test_build_acceptor_missing_key_file() {
    // Create a temporary certificate file but not a key file
    let temp_dir = std::env::temp_dir();
    let cert_path = temp_dir.join("test_cert_only.pem");

    // Create a minimal PEM certificate (won't be valid but file exists)
    {
        let mut file = File::create(&cert_path).unwrap();
        writeln!(file, "-----BEGIN CERTIFICATE-----").unwrap();
        writeln!(file, "dGVzdA==").unwrap(); // base64 "test"
        writeln!(file, "-----END CERTIFICATE-----").unwrap();
    }

    let config = TlsConfig::new(cert_path.to_str().unwrap(), "/nonexistent/path/key.pem");
    let result = config.build_acceptor();

    // Should fail because key file doesn't exist
    assert!(result.is_err());

    // Cleanup
    let _ = std::fs::remove_file(&cert_path);
}

#[test]
fn test_build_acceptor_empty_cert_file() {
    let temp_dir = std::env::temp_dir();
    let cert_path = temp_dir.join("test_empty_cert.pem");
    let key_path = temp_dir.join("test_empty_key.pem");

    // Create empty files
    File::create(&cert_path).unwrap();
    File::create(&key_path).unwrap();

    let config = TlsConfig::new(cert_path.to_str().unwrap(), key_path.to_str().unwrap());
    let result = config.build_acceptor();

    // Should fail because no certificates in file
    match result {
        Err(TlsError::Certificate(msg)) => {
            assert!(msg.contains("No certificates found"));
        }
        Err(err) => panic!("Expected Certificate error, got {:?}", err),
        Ok(_) => panic!("Expected error, got Ok"),
    }

    // Cleanup
    let _ = std::fs::remove_file(&cert_path);
    let _ = std::fs::remove_file(&key_path);
}

#[test]
fn test_build_acceptor_invalid_pem_format() {
    let temp_dir = std::env::temp_dir();
    let cert_path = temp_dir.join("test_invalid_cert.pem");

    // Create file with invalid content
    {
        let mut file = File::create(&cert_path).unwrap();
        writeln!(file, "This is not a valid PEM file").unwrap();
    }

    let config = TlsConfig::new(cert_path.to_str().unwrap(), "/nonexistent/key.pem");
    let result = config.build_acceptor();

    // Should fail
    assert!(result.is_err());

    // Cleanup
    let _ = std::fs::remove_file(&cert_path);
}

// ============================================================================
// mTLS Error Tests
// ============================================================================

#[test]
fn test_build_acceptor_missing_ca_file() {
    let temp_dir = std::env::temp_dir();
    let cert_path = temp_dir.join("test_mtls_cert.pem");
    let key_path = temp_dir.join("test_mtls_key.pem");

    // Create minimal cert file
    {
        let mut file = File::create(&cert_path).unwrap();
        writeln!(file, "-----BEGIN CERTIFICATE-----").unwrap();
        writeln!(file, "dGVzdA==").unwrap();
        writeln!(file, "-----END CERTIFICATE-----").unwrap();
    }

    // Create empty key file
    File::create(&key_path).unwrap();

    let config = TlsConfig::new(cert_path.to_str().unwrap(), key_path.to_str().unwrap())
        .with_client_auth("/nonexistent/ca.pem", true);

    let result = config.build_acceptor();

    // Should fail due to missing CA file (or missing key, depending on order)
    assert!(result.is_err());

    // Cleanup
    let _ = std::fs::remove_file(&cert_path);
    let _ = std::fs::remove_file(&key_path);
}

// ============================================================================
// Path Handling Tests
// ============================================================================

#[test]
fn test_config_with_absolute_paths() {
    let config = TlsConfig::new("/etc/ssl/certs/server.crt", "/etc/ssl/private/server.key");

    assert_eq!(config.cert_file, "/etc/ssl/certs/server.crt");
    assert_eq!(config.key_file, "/etc/ssl/private/server.key");
}

#[test]
fn test_config_with_relative_paths() {
    let config = TlsConfig::new("./certs/server.crt", "./keys/server.key");

    assert_eq!(config.cert_file, "./certs/server.crt");
    assert_eq!(config.key_file, "./keys/server.key");
}

#[test]
fn test_config_with_tilde_paths() {
    let config = TlsConfig::new("~/certs/server.crt", "~/keys/server.key");

    // Note: tilde expansion is not automatic - this just stores the string
    assert_eq!(config.cert_file, "~/certs/server.crt");
}

// ============================================================================
// Error Message Quality Tests
// ============================================================================

#[test]
fn test_error_message_includes_filename() {
    let config = TlsConfig::new("/path/to/missing/cert.pem", "/path/to/key.pem");
    let result = config.build_acceptor();

    match result {
        Err(err) => {
            let error_msg = err.to_string();
            // Error should mention the problematic file
            assert!(
                error_msg.contains("cert.pem") || error_msg.contains("/path/to"),
                "Error message should include file path: {}",
                error_msg
            );
        }
        Ok(_) => panic!("Expected error, got Ok"),
    }
}

// ============================================================================
// Configuration Validation Tests
// ============================================================================

#[test]
fn test_config_allows_empty_strings() {
    // This is a valid configuration - build will fail but creation shouldn't
    let config = TlsConfig::new("", "");

    assert_eq!(config.cert_file, "");
    assert_eq!(config.key_file, "");
}

#[test]
fn test_config_allows_whitespace_paths() {
    let config = TlsConfig::new("path with spaces/cert.pem", "path with spaces/key.pem");

    assert_eq!(config.cert_file, "path with spaces/cert.pem");
    assert_eq!(config.key_file, "path with spaces/key.pem");
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_mtls_with_same_cert_as_ca() {
    // This is a valid configuration scenario (self-signed)
    let config = TlsConfig::new("cert.pem", "key.pem").with_client_auth("cert.pem", true);

    assert_eq!(config.cert_file, "cert.pem");
    assert_eq!(config.ca_file, Some("cert.pem".to_string()));
}

#[test]
fn test_config_multiple_client_auth_calls() {
    // Later call should override earlier
    let config = TlsConfig::new("cert.pem", "key.pem")
        .with_client_auth("ca1.pem", false)
        .with_client_auth("ca2.pem", true);

    assert_eq!(config.ca_file, Some("ca2.pem".to_string()));
    assert!(config.require_client_cert);
}

// ============================================================================
// Integration with TlsListener
// ============================================================================

#[test]
fn test_tls_listener_from_invalid_config() {
    let config = TlsConfig::new("/nonexistent/cert.pem", "/nonexistent/key.pem");

    // TlsListener::new should fail with proper error
    let result = ferrite::server::TlsListener::new(&config);

    assert!(result.is_err());
}

// ============================================================================
// TLS Version and Cipher Tests (Configuration only)
// ============================================================================

#[test]
fn test_default_tls_settings() {
    // Default config should use secure defaults
    let config = TlsConfig::new("cert.pem", "key.pem");

    // These are just configuration tests - actual TLS version is determined by rustls
    assert!(!config.require_client_cert);
    assert!(config.ca_file.is_none());
}

// ============================================================================
// Concurrent Access Tests
// ============================================================================

#[test]
fn test_config_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<TlsConfig>();
}

#[test]
fn test_error_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<TlsError>();
}

// ============================================================================
// Error From Trait Tests
// ============================================================================

#[test]
fn test_io_error_conversion() {
    let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "access denied");
    let tls_err: TlsError = io_err.into();

    match tls_err {
        TlsError::Io(e) => {
            assert_eq!(e.kind(), std::io::ErrorKind::PermissionDenied);
        }
        _ => panic!("Expected Io error"),
    }
}

// ============================================================================
// File Permission Tests (Unix-specific)
// ============================================================================

#[cfg(unix)]
#[test]
fn test_unreadable_cert_file() {
    use std::os::unix::fs::PermissionsExt;

    let temp_dir = std::env::temp_dir();
    let cert_path = temp_dir.join("test_unreadable_cert.pem");

    // Create file
    {
        let mut file = File::create(&cert_path).unwrap();
        writeln!(file, "-----BEGIN CERTIFICATE-----").unwrap();
        writeln!(file, "dGVzdA==").unwrap();
        writeln!(file, "-----END CERTIFICATE-----").unwrap();
    }

    // Make it unreadable
    std::fs::set_permissions(&cert_path, std::fs::Permissions::from_mode(0o000)).unwrap();

    let config = TlsConfig::new(cert_path.to_str().unwrap(), "/nonexistent/key.pem");
    let result = config.build_acceptor();

    // Should fail due to permission denied
    assert!(result.is_err());

    // Cleanup - restore permissions first
    std::fs::set_permissions(&cert_path, std::fs::Permissions::from_mode(0o644)).unwrap();
    let _ = std::fs::remove_file(&cert_path);
}

// ============================================================================
// Real Certificate Tests (would require actual certs)
// ============================================================================

// Note: Tests that require actual valid TLS certificates are not included
// as they would require generating certificates in the test environment.
// In a production test suite, you would:
// 1. Generate test certificates in CI/CD
// 2. Use rcgen crate to generate test certificates
// 3. Have a fixtures directory with pre-generated test certs
