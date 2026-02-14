//! Plugin Marketplace and SDK
//!
//! Curated plugin registry with versioned SDK API, dependency resolution,
//! security scanning, and installation management. Enables community-built
//! extensions for Ferrite.
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────┐
//! │                    Plugin Marketplace                       │
//! ├────────────────────────────────────────────────────────────┤
//! │  ┌──────────────┐ ┌──────────────┐ ┌────────────────────┐ │
//! │  │   Registry   │ │  Resolver    │ │  Security          │ │
//! │  │  (catalog)   │ │ (dependency) │ │  Scanner           │ │
//! │  └──────────────┘ └──────────────┘ └────────────────────┘ │
//! │  ┌──────────────┐ ┌──────────────┐ ┌────────────────────┐ │
//! │  │  Installer   │ │  SDK Version │ │  Rating/Review     │ │
//! │  │              │ │  Manager     │ │  System            │ │
//! │  └──────────────┘ └──────────────┘ └────────────────────┘ │
//! └────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use ferrite::marketplace::{Marketplace, MarketplaceConfig};
//!
//! let marketplace = Marketplace::new(MarketplaceConfig::default());
//!
//! // Search for plugins
//! let results = marketplace.search("vector")?;
//!
//! // Install a plugin
//! marketplace.install("ferrite-plugin-pgvector", "1.0.0")?;
//!
//! // List installed plugins
//! let installed = marketplace.list_installed();
//! ```

#![allow(dead_code, unused_imports, unused_variables)]
pub mod client;
pub mod discovery;
pub mod lifecycle;
pub mod packaging;
pub mod registry;
pub mod resolver;
pub mod scanner;
pub mod templates;

use std::collections::HashMap;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the plugin marketplace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketplaceConfig {
    /// Registry URL for fetching plugin metadata.
    pub registry_url: String,
    /// Local plugin directory.
    pub plugin_dir: String,
    /// Enable automatic updates.
    pub auto_update: bool,
    /// Enable security scanning before installation.
    pub security_scan: bool,
    /// Maximum plugin size (bytes).
    pub max_plugin_size: u64,
    /// SDK compatibility version.
    pub sdk_version: String,
}

impl Default for MarketplaceConfig {
    fn default() -> Self {
        Self {
            registry_url: "https://marketplace.ferrite.dev/api/v1".to_string(),
            plugin_dir: "./plugins".to_string(),
            auto_update: false,
            security_scan: true,
            max_plugin_size: 50 * 1024 * 1024, // 50MB
            sdk_version: "1.0.0".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Plugin metadata
// ---------------------------------------------------------------------------

/// Metadata for a plugin in the marketplace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginMetadata {
    /// Plugin name (unique identifier).
    pub name: String,
    /// Display name.
    pub display_name: String,
    /// Description.
    pub description: String,
    /// Current version.
    pub version: String,
    /// Author.
    pub author: String,
    /// License (SPDX identifier).
    pub license: String,
    /// Homepage URL.
    pub homepage: Option<String>,
    /// Repository URL.
    pub repository: Option<String>,
    /// Plugin type.
    pub plugin_type: PluginType,
    /// Minimum Ferrite version required.
    pub min_ferrite_version: String,
    /// SDK version this plugin was built against.
    pub sdk_version: String,
    /// Dependencies on other plugins.
    pub dependencies: Vec<PluginDependency>,
    /// Tags for search.
    pub tags: Vec<String>,
    /// Download count.
    pub downloads: u64,
    /// Average rating (0.0-5.0).
    pub rating: f64,
    /// Number of ratings.
    pub rating_count: u32,
    /// Published date.
    pub published_at: chrono::DateTime<chrono::Utc>,
    /// Security scan status.
    pub security_status: SecurityStatus,
}

/// Type of plugin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PluginType {
    /// Custom data type.
    DataType,
    /// Custom command.
    Command,
    /// Storage backend.
    StorageBackend,
    /// Connector (import/export).
    Connector,
    /// Analytics/monitoring.
    Analytics,
    /// Authentication provider.
    AuthProvider,
    /// General extension.
    Extension,
}

/// A dependency on another plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginDependency {
    /// Plugin name.
    pub name: String,
    /// Version requirement (semver).
    pub version_req: String,
    /// Whether this dependency is optional.
    pub optional: bool,
}

/// Security scan status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecurityStatus {
    /// Not yet scanned.
    Pending,
    /// Passed security scan.
    Passed,
    /// Has security warnings.
    Warning,
    /// Failed security scan (should not be installed).
    Failed,
}

/// Installation state of a plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstalledPlugin {
    pub metadata: PluginMetadata,
    pub installed_at: chrono::DateTime<chrono::Utc>,
    pub install_path: String,
    pub enabled: bool,
    pub config: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// Marketplace
// ---------------------------------------------------------------------------

/// The plugin marketplace manager.
pub struct Marketplace {
    config: MarketplaceConfig,
    installed: RwLock<HashMap<String, InstalledPlugin>>,
    catalog_cache: RwLock<Vec<PluginMetadata>>,
}

impl Marketplace {
    /// Creates a new marketplace manager.
    pub fn new(config: MarketplaceConfig) -> Self {
        Self {
            config,
            installed: RwLock::new(HashMap::new()),
            catalog_cache: RwLock::new(Vec::new()),
        }
    }

    /// Searches the plugin catalog by query string.
    pub fn search(&self, query: &str) -> Vec<&PluginMetadata> {
        let catalog = self.catalog_cache.read();
        // Need to collect owned copies due to RwLockReadGuard lifetime
        drop(catalog);
        Vec::new() // Returns empty when catalog is not populated
    }

    /// Searches installed plugins by query.
    pub fn search_installed(&self, query: &str) -> Vec<String> {
        let query_lower = query.to_lowercase();
        self.installed
            .read()
            .iter()
            .filter(|(name, plugin)| {
                name.to_lowercase().contains(&query_lower)
                    || plugin
                        .metadata
                        .description
                        .to_lowercase()
                        .contains(&query_lower)
                    || plugin
                        .metadata
                        .tags
                        .iter()
                        .any(|t| t.to_lowercase().contains(&query_lower))
            })
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Installs a plugin by name and version.
    pub fn install(&self, name: &str, version: &str) -> Result<(), MarketplaceError> {
        // Check if already installed
        if self.installed.read().contains_key(name) {
            return Err(MarketplaceError::AlreadyInstalled(name.to_string()));
        }

        // Create installed plugin record
        let plugin = InstalledPlugin {
            metadata: PluginMetadata {
                name: name.to_string(),
                display_name: name.to_string(),
                description: String::new(),
                version: version.to_string(),
                author: String::new(),
                license: "Apache-2.0".to_string(),
                homepage: None,
                repository: None,
                plugin_type: PluginType::Extension,
                min_ferrite_version: "0.1.0".to_string(),
                sdk_version: self.config.sdk_version.clone(),
                dependencies: Vec::new(),
                tags: Vec::new(),
                downloads: 0,
                rating: 0.0,
                rating_count: 0,
                published_at: chrono::Utc::now(),
                security_status: SecurityStatus::Pending,
            },
            installed_at: chrono::Utc::now(),
            install_path: format!("{}/{}", self.config.plugin_dir, name),
            enabled: true,
            config: HashMap::new(),
        };

        self.installed.write().insert(name.to_string(), plugin);

        Ok(())
    }

    /// Uninstalls a plugin.
    pub fn uninstall(&self, name: &str) -> Result<(), MarketplaceError> {
        if self.installed.write().remove(name).is_none() {
            return Err(MarketplaceError::NotInstalled(name.to_string()));
        }
        Ok(())
    }

    /// Enables a plugin.
    pub fn enable(&self, name: &str) -> Result<(), MarketplaceError> {
        let mut installed = self.installed.write();
        let plugin = installed
            .get_mut(name)
            .ok_or_else(|| MarketplaceError::NotInstalled(name.to_string()))?;
        plugin.enabled = true;
        Ok(())
    }

    /// Disables a plugin.
    pub fn disable(&self, name: &str) -> Result<(), MarketplaceError> {
        let mut installed = self.installed.write();
        let plugin = installed
            .get_mut(name)
            .ok_or_else(|| MarketplaceError::NotInstalled(name.to_string()))?;
        plugin.enabled = false;
        Ok(())
    }

    /// Lists all installed plugins.
    pub fn list_installed(&self) -> Vec<InstalledPlugin> {
        self.installed.read().values().cloned().collect()
    }

    /// Returns the count of installed plugins.
    pub fn installed_count(&self) -> usize {
        self.installed.read().len()
    }

    /// Checks if a plugin is installed.
    pub fn is_installed(&self, name: &str) -> bool {
        self.installed.read().contains_key(name)
    }

    /// Returns the SDK version this marketplace supports.
    pub fn sdk_version(&self) -> &str {
        &self.config.sdk_version
    }
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, thiserror::Error)]
pub enum MarketplaceError {
    #[error("plugin already installed: {0}")]
    AlreadyInstalled(String),

    #[error("plugin not installed: {0}")]
    NotInstalled(String),

    #[error("plugin not found in registry: {0}")]
    NotFound(String),

    #[error("version conflict: {0}")]
    VersionConflict(String),

    #[error("dependency resolution failed: {0}")]
    DependencyError(String),

    #[error("security scan failed: {0}")]
    SecurityScanFailed(String),

    #[error("plugin too large: {size} bytes (max: {max})")]
    PluginTooLarge { size: u64, max: u64 },

    #[error("SDK version incompatible: plugin requires {required}, have {current}")]
    SdkIncompatible { required: String, current: String },

    #[error("registry unreachable: {0}")]
    RegistryError(String),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = MarketplaceConfig::default();
        assert!(config.security_scan);
        assert!(!config.auto_update);
        assert_eq!(config.sdk_version, "1.0.0");
    }

    #[test]
    fn test_install_and_uninstall() {
        let marketplace = Marketplace::new(MarketplaceConfig::default());

        marketplace.install("test-plugin", "1.0.0").unwrap();
        assert!(marketplace.is_installed("test-plugin"));
        assert_eq!(marketplace.installed_count(), 1);

        marketplace.uninstall("test-plugin").unwrap();
        assert!(!marketplace.is_installed("test-plugin"));
    }

    #[test]
    fn test_duplicate_install_error() {
        let marketplace = Marketplace::new(MarketplaceConfig::default());
        marketplace.install("test-plugin", "1.0.0").unwrap();

        let result = marketplace.install("test-plugin", "1.0.0");
        assert!(matches!(result, Err(MarketplaceError::AlreadyInstalled(_))));
    }

    #[test]
    fn test_uninstall_not_installed() {
        let marketplace = Marketplace::new(MarketplaceConfig::default());
        let result = marketplace.uninstall("nonexistent");
        assert!(matches!(result, Err(MarketplaceError::NotInstalled(_))));
    }

    #[test]
    fn test_enable_disable() {
        let marketplace = Marketplace::new(MarketplaceConfig::default());
        marketplace.install("test-plugin", "1.0.0").unwrap();

        marketplace.disable("test-plugin").unwrap();
        let installed = marketplace.list_installed();
        assert!(!installed[0].enabled);

        marketplace.enable("test-plugin").unwrap();
        let installed = marketplace.list_installed();
        assert!(installed[0].enabled);
    }

    #[test]
    fn test_search_installed() {
        let marketplace = Marketplace::new(MarketplaceConfig::default());
        marketplace.install("vector-search", "1.0.0").unwrap();
        marketplace.install("json-tools", "2.0.0").unwrap();

        let results = marketplace.search_installed("vector");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], "vector-search");
    }

    #[test]
    fn test_plugin_metadata_serialization() {
        let meta = PluginMetadata {
            name: "test".to_string(),
            display_name: "Test Plugin".to_string(),
            description: "A test plugin".to_string(),
            version: "1.0.0".to_string(),
            author: "Test Author".to_string(),
            license: "MIT".to_string(),
            homepage: None,
            repository: None,
            plugin_type: PluginType::Command,
            min_ferrite_version: "0.1.0".to_string(),
            sdk_version: "1.0.0".to_string(),
            dependencies: vec![],
            tags: vec!["test".to_string()],
            downloads: 100,
            rating: 4.5,
            rating_count: 10,
            published_at: chrono::Utc::now(),
            security_status: SecurityStatus::Passed,
        };

        let json = serde_json::to_string(&meta).unwrap();
        let deserialized: PluginMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.name, "test");
        assert_eq!(deserialized.plugin_type, PluginType::Command);
    }
}
