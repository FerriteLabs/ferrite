//! TLS support for encrypted connections
//!
//! This module implements TLS support using rustls for secure client connections.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use tokio_rustls::TlsAcceptor;

/// TLS configuration options
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to certificate file (PEM format)
    pub cert_file: String,
    /// Path to private key file (PEM format)
    pub key_file: String,
    /// Path to CA certificate for client verification (optional, for mTLS)
    pub ca_file: Option<String>,
    /// Whether client certificates are required (mTLS)
    pub require_client_cert: bool,
}

/// TLS error types
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Certificate error
    #[error("Certificate error: {0}")]
    Certificate(String),

    /// Private key error
    #[error("Private key error: {0}")]
    PrivateKey(String),

    /// Configuration error
    #[error("TLS configuration error: {0}")]
    Config(String),
}

impl TlsConfig {
    /// Create a new TLS configuration with basic settings
    pub fn new(cert_file: impl Into<String>, key_file: impl Into<String>) -> Self {
        Self {
            cert_file: cert_file.into(),
            key_file: key_file.into(),
            ca_file: None,
            require_client_cert: false,
        }
    }

    /// Enable mTLS with a CA certificate
    pub fn with_client_auth(mut self, ca_file: impl Into<String>, require_cert: bool) -> Self {
        self.ca_file = Some(ca_file.into());
        self.require_client_cert = require_cert;
        self
    }

    /// Build a TLS acceptor from this configuration
    pub fn build_acceptor(&self) -> Result<TlsAcceptor, TlsError> {
        // Load certificates
        let certs = load_certs(&self.cert_file)?;

        // Load private key
        let key = load_key(&self.key_file)?;

        // Build server config
        let config = if let Some(ca_file) = &self.ca_file {
            // mTLS configuration
            let client_certs = load_certs(ca_file)?;
            let mut root_store = RootCertStore::empty();
            for cert in client_certs {
                root_store.add(cert).map_err(|e| {
                    TlsError::Certificate(format!("Failed to add CA certificate: {}", e))
                })?;
            }

            let verifier = if self.require_client_cert {
                WebPkiClientVerifier::builder(Arc::new(root_store))
                    .build()
                    .map_err(|e| TlsError::Config(format!("Failed to build verifier: {}", e)))?
            } else {
                WebPkiClientVerifier::builder(Arc::new(root_store))
                    .allow_unauthenticated()
                    .build()
                    .map_err(|e| TlsError::Config(format!("Failed to build verifier: {}", e)))?
            };

            ServerConfig::builder()
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)
                .map_err(|e| TlsError::Config(format!("Failed to build config: {}", e)))?
        } else {
            // Standard TLS without client verification
            ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|e| TlsError::Config(format!("Failed to build config: {}", e)))?
        };

        Ok(TlsAcceptor::from(Arc::new(config)))
    }
}

/// Load certificates from a PEM file
fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let file = File::open(Path::new(path))
        .map_err(|e| TlsError::Certificate(format!("Failed to open {}: {}", path, e)))?;
    let mut reader = BufReader::new(file);

    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .filter_map(|result| result.ok())
        .collect();

    if certs.is_empty() {
        return Err(TlsError::Certificate(format!(
            "No certificates found in {}",
            path
        )));
    }

    Ok(certs)
}

/// Load private key from a PEM file
fn load_key(path: &str) -> Result<PrivateKeyDer<'static>, TlsError> {
    let file = File::open(Path::new(path))
        .map_err(|e| TlsError::PrivateKey(format!("Failed to open {}: {}", path, e)))?;
    let mut reader = BufReader::new(file);

    // Try to read different key formats
    loop {
        match rustls_pemfile::read_one(&mut reader) {
            Ok(Some(rustls_pemfile::Item::Pkcs1Key(key))) => {
                return Ok(PrivateKeyDer::Pkcs1(key));
            }
            Ok(Some(rustls_pemfile::Item::Pkcs8Key(key))) => {
                return Ok(PrivateKeyDer::Pkcs8(key));
            }
            Ok(Some(rustls_pemfile::Item::Sec1Key(key))) => {
                return Ok(PrivateKeyDer::Sec1(key));
            }
            Ok(Some(_)) => {
                // Skip other items (certificates, etc.)
                continue;
            }
            Ok(None) => {
                break;
            }
            Err(e) => {
                return Err(TlsError::PrivateKey(format!(
                    "Failed to parse key from {}: {}",
                    path, e
                )));
            }
        }
    }

    Err(TlsError::PrivateKey(format!(
        "No private key found in {}",
        path
    )))
}

/// TLS listener wrapper that can accept both TLS and non-TLS connections
pub struct TlsListener {
    /// TLS acceptor for encrypted connections
    acceptor: TlsAcceptor,
}

impl TlsListener {
    /// Create a new TLS listener from configuration
    pub fn new(config: &TlsConfig) -> Result<Self, TlsError> {
        let acceptor = config.build_acceptor()?;
        Ok(Self { acceptor })
    }

    /// Get the TLS acceptor for accepting connections
    pub fn acceptor(&self) -> &TlsAcceptor {
        &self.acceptor
    }

    /// Accept a TLS connection
    pub async fn accept(
        &self,
        stream: tokio::net::TcpStream,
    ) -> Result<tokio_rustls::server::TlsStream<tokio::net::TcpStream>, std::io::Error> {
        self.acceptor.accept(stream).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_config_new() {
        let config = TlsConfig::new("cert.pem", "key.pem");
        assert_eq!(config.cert_file, "cert.pem");
        assert_eq!(config.key_file, "key.pem");
        assert!(config.ca_file.is_none());
        assert!(!config.require_client_cert);
    }

    #[test]
    fn test_tls_config_with_client_auth() {
        let config = TlsConfig::new("cert.pem", "key.pem").with_client_auth("ca.pem", true);
        assert_eq!(config.ca_file, Some("ca.pem".to_string()));
        assert!(config.require_client_cert);
    }

    #[test]
    fn test_load_certs_missing_file() {
        let result = load_certs("/nonexistent/cert.pem");
        assert!(result.is_err());
    }

    #[test]
    fn test_load_key_missing_file() {
        let result = load_key("/nonexistent/key.pem");
        assert!(result.is_err());
    }
}
