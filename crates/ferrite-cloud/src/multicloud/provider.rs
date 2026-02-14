//! Cloud provider definitions

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Cloud provider type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProviderType {
    /// Amazon Web Services
    Aws,
    /// Google Cloud Platform
    Gcp,
    /// Microsoft Azure
    Azure,
    /// Custom/On-premise
    Custom,
}

impl ProviderType {
    /// Get string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            ProviderType::Aws => "aws",
            ProviderType::Gcp => "gcp",
            ProviderType::Azure => "azure",
            ProviderType::Custom => "custom",
        }
    }

    /// Parse from string
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "aws" => Some(ProviderType::Aws),
            "gcp" => Some(ProviderType::Gcp),
            "azure" => Some(ProviderType::Azure),
            "custom" => Some(ProviderType::Custom),
            _ => None,
        }
    }
}

/// Cloud provider configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderConfig {
    /// Provider type
    pub provider_type: ProviderType,
    /// Access key or service account
    pub access_key: Option<String>,
    /// Secret key
    pub secret_key: Option<String>,
    /// API endpoint (for custom providers)
    pub endpoint: Option<String>,
    /// Additional options
    pub options: HashMap<String, String>,
}

impl ProviderConfig {
    /// Create a new provider config
    pub fn new(provider_type: ProviderType) -> Self {
        Self {
            provider_type,
            access_key: None,
            secret_key: None,
            endpoint: None,
            options: HashMap::new(),
        }
    }

    /// Set credentials
    pub fn with_credentials(mut self, access_key: String, secret_key: String) -> Self {
        self.access_key = Some(access_key);
        self.secret_key = Some(secret_key);
        self
    }

    /// Set endpoint
    pub fn with_endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    /// Add option
    pub fn with_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(key.into(), value.into());
        self
    }
}

/// Cloud provider instance
#[derive(Debug, Clone)]
pub struct CloudProvider {
    name: String,
    config: ProviderConfig,
    connected: bool,
}

impl CloudProvider {
    /// Create a new cloud provider
    pub fn new(name: impl Into<String>, config: ProviderConfig) -> Self {
        Self {
            name: name.into(),
            config,
            connected: false,
        }
    }

    /// Get provider name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get provider type
    pub fn provider_type(&self) -> ProviderType {
        self.config.provider_type
    }

    /// Get configuration
    pub fn config(&self) -> &ProviderConfig {
        &self.config
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        self.connected
    }

    /// Connect to provider
    pub fn connect(&mut self) -> Result<(), ProviderError> {
        // In a real implementation, this would establish connection
        self.connected = true;
        Ok(())
    }

    /// Disconnect from provider
    pub fn disconnect(&mut self) {
        self.connected = false;
    }

    /// Get endpoint URL for a region
    pub fn get_endpoint(&self, region: &str) -> String {
        if let Some(endpoint) = &self.config.endpoint {
            return endpoint.clone();
        }

        match self.config.provider_type {
            ProviderType::Aws => format!("https://ferrite.{}.amazonaws.com", region),
            ProviderType::Gcp => format!("https://{}-ferrite.googleapis.com", region),
            ProviderType::Azure => format!("https://ferrite.{}.azure.com", region),
            ProviderType::Custom => format!("https://ferrite.{}.local", region),
        }
    }
}

/// Provider errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum ProviderError {
    /// Connection failed
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// Authentication failed
    #[error("authentication failed: {0}")]
    AuthenticationFailed(String),

    /// Provider not found
    #[error("provider not found: {0}")]
    NotFound(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provider_creation() {
        let config = ProviderConfig::new(ProviderType::Aws)
            .with_credentials("access".to_string(), "secret".to_string());

        let provider = CloudProvider::new("aws-main", config);
        assert_eq!(provider.name(), "aws-main");
        assert_eq!(provider.provider_type(), ProviderType::Aws);
    }

    #[test]
    fn test_endpoint_generation() {
        let config = ProviderConfig::new(ProviderType::Aws);
        let provider = CloudProvider::new("aws", config);

        let endpoint = provider.get_endpoint("us-east-1");
        assert!(endpoint.contains("us-east-1"));
    }
}
