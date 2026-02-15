//! Stub TLS module when the `tls` feature is disabled.

/// TLS configuration options (stub)
#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_file: String,
    pub key_file: String,
    pub ca_file: Option<String>,
    pub require_client_cert: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Certificate error: {0}")]
    Certificate(String),
    #[error("Private key error: {0}")]
    PrivateKey(String),
    #[error("TLS configuration error: {0}")]
    Config(String),
}

impl TlsConfig {
    pub fn new(cert_file: impl Into<String>, key_file: impl Into<String>) -> Self {
        Self {
            cert_file: cert_file.into(),
            key_file: key_file.into(),
            ca_file: None,
            require_client_cert: false,
        }
    }

    pub fn with_client_auth(mut self, ca_file: impl Into<String>, require_cert: bool) -> Self {
        self.ca_file = Some(ca_file.into());
        self.require_client_cert = require_cert;
        self
    }
}

pub struct TlsListener;

impl TlsListener {
    pub fn new(_config: &TlsConfig) -> Result<Self, TlsError> {
        Err(TlsError::Config(
            "TLS is not available (compile with --features tls)".to_string(),
        ))
    }
}
