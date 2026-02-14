//! Ferrite Studio - Built-in Web UI
//!
//! A modern web-based admin interface for Ferrite, providing:
//!
//! - Visual key browser with search and filters
//! - Real-time metrics dashboard
//! - Cluster topology visualization
//! - Slow query log viewer
//! - Configuration management
//!
//! # Example
//!
//! ```ignore
//! use ferrite::studio::{Studio, StudioConfig};
//!
//! let config = StudioConfig::default();
//! let studio = Studio::new(config);
//! studio.start().await?;
//! // Access at http://localhost:8080
//! ```

mod api;
pub mod auth;
pub mod console;
pub mod cost_savings;
pub mod metrics;
pub mod query_builder;
mod server;
pub mod server_info;

pub use api::StudioApi;
pub use auth::{AuthConfig, AuthError, AuthManager, LoginRequest, LoginResponse, Role, Session};
pub use console::{
    CommandInfo, Console, ConsoleHistoryEntry, ConsoleRequest, ConsoleResponse, RespType,
};
pub use cost_savings::{CostComparison, CostTracker, PricingModel, TierCosts, TierUsage};
pub use metrics::{MetricsCollector, MetricsSnapshot};
pub use query_builder::{
    BlockType, CompiledCommand, CompiledQuery, Connection, CostLevel, ExecutionPlan, PlanStep,
    Position, QueryBlock, QueryBuilder, QueryCost, QueryHistoryEntry, QueryHistoryItem,
    QueryMetadata, QueryParameter, QueryResult, QuerySummary, QueryTemplate, QueryType,
    ResultMetadata, TemplateExample, ValidationResult, VisualQuery,
};
pub use server::Studio;
pub use server_info::{
    Alert, AlertLevel, PersistenceInfo, ReplicationInfo, ServerInfoPanel,
};

use serde::{Deserialize, Serialize};

/// Configuration for Ferrite Studio
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StudioConfig {
    /// Enable the studio web UI
    pub enabled: bool,
    /// Host to bind to
    pub host: String,
    /// Port to listen on
    pub port: u16,
    /// Enable authentication
    pub auth_enabled: bool,
    /// API key for authentication (if auth_enabled)
    pub api_key: Option<String>,
    /// Enable CORS for development
    pub cors_enabled: bool,
    /// Allowed origins for CORS
    pub allowed_origins: Vec<String>,
    /// Enable HTTPS
    pub tls_enabled: bool,
    /// Path to TLS certificate
    pub tls_cert_path: Option<String>,
    /// Path to TLS key
    pub tls_key_path: Option<String>,
    /// Maximum request body size in bytes
    pub max_body_size: usize,
    /// Request timeout in seconds
    pub request_timeout_secs: u64,
}

impl Default for StudioConfig {
    fn default() -> Self {
        Self {
            enabled: false, // Disabled by default for security
            host: "127.0.0.1".to_string(),
            port: 8080,
            auth_enabled: true,
            api_key: None,
            cors_enabled: false,
            allowed_origins: vec![],
            tls_enabled: false,
            tls_cert_path: None,
            tls_key_path: None,
            max_body_size: 10 * 1024 * 1024, // 10MB
            request_timeout_secs: 30,
        }
    }
}

impl StudioConfig {
    /// Create a development configuration (no auth, CORS enabled)
    pub fn development() -> Self {
        Self {
            enabled: true,
            host: "127.0.0.1".to_string(),
            port: 8080,
            auth_enabled: false,
            api_key: None,
            cors_enabled: true,
            allowed_origins: vec!["*".to_string()],
            tls_enabled: false,
            tls_cert_path: None,
            tls_key_path: None,
            max_body_size: 10 * 1024 * 1024,
            request_timeout_secs: 30,
        }
    }

    /// Create a production configuration
    pub fn production(api_key: String) -> Self {
        Self {
            enabled: true,
            host: "0.0.0.0".to_string(),
            port: 8080,
            auth_enabled: true,
            api_key: Some(api_key),
            cors_enabled: false,
            allowed_origins: vec![],
            tls_enabled: true,
            tls_cert_path: None,
            tls_key_path: None,
            max_body_size: 10 * 1024 * 1024,
            request_timeout_secs: 30,
        }
    }

    /// Get the bind address
    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Errors that can occur in the studio
#[derive(Debug, Clone, thiserror::Error)]
pub enum StudioError {
    /// Server error
    #[error("server error: {0}")]
    Server(String),

    /// Authentication error
    #[error("authentication required")]
    Unauthorized,

    /// Invalid request
    #[error("invalid request: {0}")]
    BadRequest(String),

    /// Not found
    #[error("not found: {0}")]
    NotFound(String),

    /// Internal error
    #[error("internal error: {0}")]
    Internal(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_studio_config_default() {
        let config = StudioConfig::default();
        assert!(!config.enabled);
        assert!(config.auth_enabled);
        assert_eq!(config.port, 8080);
        assert_eq!(config.host, "127.0.0.1");
    }

    #[test]
    fn test_studio_config_development() {
        let config = StudioConfig::development();
        assert!(config.enabled);
        assert!(!config.auth_enabled);
        assert!(config.cors_enabled);
    }

    #[test]
    fn test_studio_config_production() {
        let config = StudioConfig::production("secret-key".to_string());
        assert!(config.enabled);
        assert!(config.auth_enabled);
        assert_eq!(config.api_key, Some("secret-key".to_string()));
    }

    #[test]
    fn test_bind_addr() {
        let config = StudioConfig::default();
        assert_eq!(config.bind_addr(), "127.0.0.1:8080");
    }
}
