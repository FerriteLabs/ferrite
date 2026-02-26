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

#![allow(dead_code)]
pub mod client;
pub mod discovery;
pub mod installer;
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

    /// Creates a new marketplace manager pre-populated with built-in plugins.
    pub fn with_builtin_catalog(config: MarketplaceConfig) -> Self {
        let mp = Self::new(config);
        mp.set_catalog(Self::builtin_catalog());
        mp
    }

    /// Built-in example plugins for the marketplace catalog.
    fn builtin_catalog() -> Vec<PluginMetadata> {
        let now = chrono::Utc::now();
        vec![
            PluginMetadata {
                name: "geo-fence".into(),
                display_name: "Geo Fence".into(),
                description: "Geofencing data type with point-in-polygon queries".into(),
                version: "1.0.0".into(),
                author: "ferritelabs".into(),
                license: "Apache-2.0".into(),
                homepage: None,
                repository: None,
                plugin_type: PluginType::DataType,
                min_ferrite_version: "0.1.0".into(),
                sdk_version: "1.0.0".into(),
                dependencies: vec![],
                tags: vec!["geo".into(), "spatial".into()],
                downloads: 1_250,
                rating: 4.6,
                rating_count: 18,
                published_at: now,
                security_status: SecurityStatus::Passed,
            },
            PluginMetadata {
                name: "rate-limiter".into(),
                display_name: "Rate Limiter".into(),
                description: "Token bucket rate limiter with sliding window support".into(),
                version: "1.0.0".into(),
                author: "ferritelabs".into(),
                license: "Apache-2.0".into(),
                homepage: None,
                repository: None,
                plugin_type: PluginType::Extension,
                min_ferrite_version: "0.1.0".into(),
                sdk_version: "1.0.0".into(),
                dependencies: vec![],
                tags: vec!["rate-limit".into(), "throttle".into()],
                downloads: 3_400,
                rating: 4.8,
                rating_count: 42,
                published_at: now,
                security_status: SecurityStatus::Passed,
            },
            PluginMetadata {
                name: "json-schema-validator".into(),
                display_name: "JSON Schema Validator".into(),
                description: "Validate JSON values against JSON Schema on write".into(),
                version: "1.0.0".into(),
                author: "ferritelabs".into(),
                license: "Apache-2.0".into(),
                homepage: None,
                repository: None,
                plugin_type: PluginType::Command,
                min_ferrite_version: "0.1.0".into(),
                sdk_version: "1.0.0".into(),
                dependencies: vec![],
                tags: vec!["json".into(), "validation".into(), "schema".into()],
                downloads: 2_100,
                rating: 4.5,
                rating_count: 25,
                published_at: now,
                security_status: SecurityStatus::Passed,
            },
            PluginMetadata {
                name: "bloom-filter".into(),
                display_name: "Bloom Filter".into(),
                description: "Space-efficient probabilistic set membership testing".into(),
                version: "1.0.0".into(),
                author: "ferritelabs".into(),
                license: "Apache-2.0".into(),
                homepage: None,
                repository: None,
                plugin_type: PluginType::DataType,
                min_ferrite_version: "0.1.0".into(),
                sdk_version: "1.0.0".into(),
                dependencies: vec![],
                tags: vec!["probabilistic".into(), "filter".into()],
                downloads: 980,
                rating: 4.3,
                rating_count: 12,
                published_at: now,
                security_status: SecurityStatus::Passed,
            },
        ]
    }

    /// Searches the plugin catalog by query string.
    pub fn search(&self, query: &str) -> Vec<PluginMetadata> {
        let q = query.to_lowercase();
        self.catalog_cache
            .read()
            .iter()
            .filter(|p| {
                p.name.to_lowercase().contains(&q)
                    || p.description.to_lowercase().contains(&q)
                    || p.display_name.to_lowercase().contains(&q)
                    || p.tags.iter().any(|t| t.to_lowercase().contains(&q))
            })
            .cloned()
            .collect()
    }

    /// Populate the catalog cache (e.g., from a MarketplaceClient refresh).
    pub fn set_catalog(&self, catalog: Vec<PluginMetadata>) {
        *self.catalog_cache.write() = catalog;
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
    ///
    /// Creates the plugin directory, writes metadata, and optionally writes
    /// WASM binary data if provided.
    pub fn install(&self, name: &str, version: &str) -> Result<(), MarketplaceError> {
        self.install_with_data(name, version, None)
    }

    /// Installs a plugin with optional binary data (WASM module).
    pub fn install_with_data(
        &self,
        name: &str,
        version: &str,
        wasm_data: Option<&[u8]>,
    ) -> Result<(), MarketplaceError> {
        // Check if already installed
        if self.installed.read().contains_key(name) {
            return Err(MarketplaceError::AlreadyInstalled(name.to_string()));
        }

        // Check binary size limit
        if let Some(data) = wasm_data {
            if data.len() as u64 > self.config.max_plugin_size {
                return Err(MarketplaceError::PluginTooLarge {
                    size: data.len() as u64,
                    max: self.config.max_plugin_size,
                });
            }
        }

        // Look up metadata from catalog (or create minimal metadata)
        let metadata = {
            let catalog = self.catalog_cache.read();
            catalog
                .iter()
                .find(|p| p.name == name && p.version == version)
                .cloned()
                .unwrap_or_else(|| PluginMetadata {
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
                })
        };

        // Check SDK compatibility
        if metadata.sdk_version != self.config.sdk_version {
            // Allow compatible versions (same major)
            let req_major = metadata.sdk_version.split('.').next().unwrap_or("0");
            let cur_major = self.config.sdk_version.split('.').next().unwrap_or("0");
            if req_major != cur_major {
                return Err(MarketplaceError::SdkIncompatible {
                    required: metadata.sdk_version.clone(),
                    current: self.config.sdk_version.clone(),
                });
            }
        }

        let install_path = format!("{}/{}", self.config.plugin_dir, name);

        // Create plugin directory and write files
        let install_dir = std::path::Path::new(&install_path);
        if let Err(e) = std::fs::create_dir_all(install_dir) {
            tracing::warn!(path = %install_path, error = %e, "failed to create plugin directory");
            // Don't fail — directory might not be writable in embedded/test mode
        }

        // Write WASM binary if provided
        if let Some(data) = wasm_data {
            let wasm_path = install_dir.join("plugin.wasm");
            if let Err(e) = std::fs::write(&wasm_path, data) {
                tracing::warn!(path = %wasm_path.display(), error = %e, "failed to write WASM binary");
            }
        }

        // Write metadata file
        let meta_path = install_dir.join("plugin.json");
        if let Ok(json) = serde_json::to_string_pretty(&metadata) {
            if let Err(e) = std::fs::write(&meta_path, json) {
                tracing::warn!(path = %meta_path.display(), error = %e, "failed to write metadata");
            }
        }

        // Register in memory
        let plugin = InstalledPlugin {
            metadata,
            installed_at: chrono::Utc::now(),
            install_path,
            enabled: true,
            config: HashMap::new(),
        };

        self.installed.write().insert(name.to_string(), plugin);

        Ok(())
    }

    /// Uninstalls a plugin, removing its files from disk.
    pub fn uninstall(&self, name: &str) -> Result<(), MarketplaceError> {
        let removed = self.installed.write().remove(name);
        match removed {
            None => Err(MarketplaceError::NotInstalled(name.to_string())),
            Some(plugin) => {
                // Remove plugin directory from disk
                let path = std::path::Path::new(&plugin.install_path);
                if path.exists() {
                    if let Err(e) = std::fs::remove_dir_all(path) {
                        tracing::warn!(
                            path = %plugin.install_path,
                            error = %e,
                            "failed to remove plugin directory"
                        );
                    }
                }
                Ok(())
            }
        }
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

    #[test]
    fn test_search_catalog() {
        let marketplace = Marketplace::new(MarketplaceConfig::default());

        let catalog = vec![
            PluginMetadata {
                name: "vector-plugin".to_string(),
                display_name: "Vector Plugin".to_string(),
                description: "Vector similarity search".to_string(),
                version: "1.0.0".to_string(),
                author: "Test".to_string(),
                license: "MIT".to_string(),
                homepage: None,
                repository: None,
                plugin_type: PluginType::Extension,
                min_ferrite_version: "0.1.0".to_string(),
                sdk_version: "1.0.0".to_string(),
                dependencies: vec![],
                tags: vec!["ai".to_string(), "vector".to_string()],
                downloads: 100,
                rating: 4.5,
                rating_count: 10,
                published_at: chrono::Utc::now(),
                security_status: SecurityStatus::Passed,
            },
            PluginMetadata {
                name: "json-validator".to_string(),
                display_name: "JSON Validator".to_string(),
                description: "Validates JSON schemas".to_string(),
                version: "2.0.0".to_string(),
                author: "Test".to_string(),
                license: "Apache-2.0".to_string(),
                homepage: None,
                repository: None,
                plugin_type: PluginType::Command,
                min_ferrite_version: "0.1.0".to_string(),
                sdk_version: "1.0.0".to_string(),
                dependencies: vec![],
                tags: vec!["json".to_string(), "validation".to_string()],
                downloads: 500,
                rating: 4.8,
                rating_count: 50,
                published_at: chrono::Utc::now(),
                security_status: SecurityStatus::Passed,
            },
        ];
        marketplace.set_catalog(catalog);

        let results = marketplace.search("vector");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "vector-plugin");

        let results = marketplace.search("json");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "json-validator");

        let results = marketplace.search("validation");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_install_with_data() {
        let dir = tempfile::tempdir().unwrap();
        let config = MarketplaceConfig {
            plugin_dir: dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let marketplace = Marketplace::new(config);

        let wasm_data = b"\x00asm\x01\x00\x00\x00"; // minimal WASM header
        marketplace
            .install_with_data("my-plugin", "1.0.0", Some(wasm_data))
            .unwrap();

        assert!(marketplace.is_installed("my-plugin"));

        // Verify files were written
        let plugin_dir = dir.path().join("my-plugin");
        assert!(plugin_dir.exists());
        assert!(plugin_dir.join("plugin.wasm").exists());
        assert!(plugin_dir.join("plugin.json").exists());

        // Uninstall removes files
        marketplace.uninstall("my-plugin").unwrap();
        assert!(!plugin_dir.exists());
    }

    #[test]
    fn test_install_size_limit() {
        let config = MarketplaceConfig {
            max_plugin_size: 10, // 10 bytes
            ..Default::default()
        };
        let marketplace = Marketplace::new(config);

        let big_data = vec![0u8; 100]; // 100 bytes > 10 byte limit
        let result = marketplace.install_with_data("big-plugin", "1.0.0", Some(&big_data));
        assert!(matches!(result, Err(MarketplaceError::PluginTooLarge { .. })));
    }
}
