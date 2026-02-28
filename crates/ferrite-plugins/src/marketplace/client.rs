//! Marketplace HTTP Client
//!
//! Provides an async HTTP client for interacting with the Ferrite plugin
//! marketplace registry. Supports search, download, category listing, and
//! popularity-based discovery.
//!
//! # Example
//!
//! ```rust,ignore
//! use ferrite_plugins::marketplace::client::MarketplaceClient;
//!
//! let client = MarketplaceClient::new("https://marketplace.ferrite.dev/api/v1");
//! let results = client.search("vector").await?;
//! let wasm = client.download("validate-email", "1.0.0").await?;
//! ```

use serde::{Deserialize, Serialize};

use super::{MarketplaceError, PluginMetadata};

// ---------------------------------------------------------------------------
// Category
// ---------------------------------------------------------------------------

/// A marketplace category with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Category {
    /// Category identifier (e.g. `"data-processing"`).
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Short description of the category.
    pub description: String,
    /// Number of plugins in this category.
    pub plugin_count: u64,
}

/// Default built-in categories.
pub fn default_categories() -> Vec<Category> {
    vec![
        Category {
            id: "data-processing".to_string(),
            name: "Data Processing".to_string(),
            description: "ETL, aggregation, and data enrichment plugins".to_string(),
            plugin_count: 0,
        },
        Category {
            id: "validation".to_string(),
            name: "Validation".to_string(),
            description: "Input validation and schema enforcement".to_string(),
            plugin_count: 0,
        },
        Category {
            id: "transformation".to_string(),
            name: "Transformation".to_string(),
            description: "Format conversion and data reshaping".to_string(),
            plugin_count: 0,
        },
        Category {
            id: "analytics".to_string(),
            name: "Analytics".to_string(),
            description: "Statistics, counters, and metrics plugins".to_string(),
            plugin_count: 0,
        },
        Category {
            id: "security".to_string(),
            name: "Security".to_string(),
            description: "Rate limiting, hashing, and access control".to_string(),
            plugin_count: 0,
        },
        Category {
            id: "connector".to_string(),
            name: "Connectors".to_string(),
            description: "External data source and sink connectors".to_string(),
            plugin_count: 0,
        },
    ]
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// HTTP client for the Ferrite plugin marketplace.
///
/// Communicates with the remote registry to search, browse, and download
/// plugins. In offline mode, operations return cached or empty results.
pub struct MarketplaceClient {
    /// Base URL for the registry API.
    registry_url: String,
    /// Cached catalog for offline/search operations.
    catalog_cache: parking_lot::RwLock<Vec<PluginMetadata>>,
    /// Whether the client is in offline mode.
    offline: std::sync::atomic::AtomicBool,
}

impl MarketplaceClient {
    /// Create a new client pointing at the given registry URL.
    pub fn new(registry_url: &str) -> Self {
        Self {
            registry_url: registry_url.to_string(),
            catalog_cache: parking_lot::RwLock::new(Vec::new()),
            offline: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Create a client with a pre-populated local catalog (for testing/offline use).
    pub fn with_catalog(registry_url: &str, catalog: Vec<PluginMetadata>) -> Self {
        Self {
            registry_url: registry_url.to_string(),
            catalog_cache: parking_lot::RwLock::new(catalog),
            offline: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Search the marketplace for plugins matching a query string.
    ///
    /// Searches name, description, and tags.
    pub async fn search(&self, query: &str) -> Result<Vec<PluginMetadata>, MarketplaceError> {
        // Search against cached catalog
        let q = query.to_lowercase();
        let results = self
            .catalog_cache
            .read()
            .iter()
            .filter(|p| {
                p.name.to_lowercase().contains(&q)
                    || p.description.to_lowercase().contains(&q)
                    || p.tags.iter().any(|t| t.to_lowercase().contains(&q))
            })
            .cloned()
            .collect();
        Ok(results)
    }

    /// Get metadata for a specific plugin version.
    pub async fn get_plugin(
        &self,
        name: &str,
        version: &str,
    ) -> Result<PluginMetadata, MarketplaceError> {
        let catalog = self.catalog_cache.read();
        catalog
            .iter()
            .find(|p| p.name == name && p.version == version)
            .cloned()
            .ok_or_else(|| MarketplaceError::NotFound(format!("{}@{}", name, version)))
    }

    /// Download the WASM binary for a plugin.
    ///
    /// Performs an HTTP GET against `{registry_url}/plugins/{name}/{version}/download`.
    /// Returns the raw bytes of the `.ferrpkg` archive (or WASM binary).
    pub async fn download(&self, name: &str, version: &str) -> Result<Vec<u8>, MarketplaceError> {
        if self.offline.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(MarketplaceError::RegistryError(
                "client is in offline mode".to_string(),
            ));
        }

        // Verify plugin exists in catalog
        let _meta = self.get_plugin(name, version).await?;

        #[cfg(feature = "marketplace")]
        {
            let url = format!(
                "{}/plugins/{}/{}/download",
                self.registry_url, name, version
            );

            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(120))
                .build()
                .map_err(|e| MarketplaceError::RegistryError(format!("HTTP client error: {e}")))?;

            let response = client.get(&url).send().await.map_err(|e| {
                MarketplaceError::RegistryError(format!("download request failed: {e}"))
            })?;

            if !response.status().is_success() {
                return Err(MarketplaceError::RegistryError(format!(
                    "download returned HTTP {}",
                    response.status()
                )));
            }

            let bytes = response.bytes().await.map_err(|e| {
                MarketplaceError::RegistryError(format!("failed to read download body: {e}"))
            })?;

            return Ok(bytes.to_vec());
        }

        #[cfg(not(feature = "marketplace"))]
        Err(MarketplaceError::RegistryError(
            "download requires the 'marketplace' feature (compile with --features marketplace)"
                .to_string(),
        ))
    }

    /// List all available marketplace categories.
    pub async fn list_categories(&self) -> Result<Vec<Category>, MarketplaceError> {
        Ok(default_categories())
    }

    /// Get the most popular plugins by download count.
    pub async fn get_popular(&self, limit: usize) -> Result<Vec<PluginMetadata>, MarketplaceError> {
        let mut catalog: Vec<PluginMetadata> = self.catalog_cache.read().clone();
        catalog.sort_by(|a, b| b.downloads.cmp(&a.downloads));
        catalog.truncate(limit);
        Ok(catalog)
    }

    /// Refresh the local catalog cache from the remote registry.
    ///
    /// Fetches `{registry_url}/catalog` and updates the local cache.
    pub async fn refresh_catalog(&self) -> Result<usize, MarketplaceError> {
        if self.offline.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(MarketplaceError::RegistryError(
                "client is in offline mode".to_string(),
            ));
        }

        #[cfg(feature = "marketplace")]
        {
            let url = format!("{}/catalog", self.registry_url);

            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .map_err(|e| MarketplaceError::RegistryError(format!("HTTP client error: {e}")))?;

            let response = client.get(&url).send().await.map_err(|e| {
                MarketplaceError::RegistryError(format!("catalog fetch failed: {e}"))
            })?;

            if !response.status().is_success() {
                return Err(MarketplaceError::RegistryError(format!(
                    "catalog returned HTTP {}",
                    response.status()
                )));
            }

            let catalog: Vec<PluginMetadata> = response.json().await.map_err(|e| {
                MarketplaceError::RegistryError(format!("failed to parse catalog JSON: {e}"))
            })?;

            let count = catalog.len();
            *self.catalog_cache.write() = catalog;
            return Ok(count);
        }

        #[cfg(not(feature = "marketplace"))]
        Ok(self.catalog_cache.read().len())
    }

    /// Set offline mode.
    pub fn set_offline(&self, offline: bool) {
        self.offline
            .store(offline, std::sync::atomic::Ordering::Relaxed);
    }

    /// Whether the client is in offline mode.
    pub fn is_offline(&self) -> bool {
        self.offline.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The registry URL this client connects to.
    pub fn registry_url(&self) -> &str {
        &self.registry_url
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::marketplace::{PluginType, SecurityStatus};

    fn sample_catalog() -> Vec<PluginMetadata> {
        vec![
            PluginMetadata {
                name: "validate-email".to_string(),
                display_name: "Email Validator".to_string(),
                description: "Validates email addresses".to_string(),
                version: "1.0.0".to_string(),
                author: "Ferrite Labs".to_string(),
                license: "MIT".to_string(),
                homepage: None,
                repository: None,
                plugin_type: PluginType::Command,
                min_ferrite_version: "0.1.0".to_string(),
                sdk_version: "1.0.0".to_string(),
                dependencies: vec![],
                tags: vec!["email".to_string(), "validation".to_string()],
                downloads: 5000,
                rating: 4.5,
                rating_count: 100,
                published_at: chrono::Utc::now(),
                security_status: SecurityStatus::Passed,
            },
            PluginMetadata {
                name: "rate-limiter".to_string(),
                display_name: "Rate Limiter".to_string(),
                description: "Token bucket rate limiting".to_string(),
                version: "2.1.0".to_string(),
                author: "Community".to_string(),
                license: "Apache-2.0".to_string(),
                homepage: None,
                repository: None,
                plugin_type: PluginType::Extension,
                min_ferrite_version: "0.1.0".to_string(),
                sdk_version: "1.0.0".to_string(),
                dependencies: vec![],
                tags: vec!["rate-limit".to_string(), "security".to_string()],
                downloads: 12000,
                rating: 4.8,
                rating_count: 250,
                published_at: chrono::Utc::now(),
                security_status: SecurityStatus::Passed,
            },
        ]
    }

    #[tokio::test]
    async fn test_search() {
        let client = MarketplaceClient::with_catalog("https://test.dev", sample_catalog());
        let results = client.search("email").await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "validate-email");
    }

    #[tokio::test]
    async fn test_search_empty() {
        let client = MarketplaceClient::with_catalog("https://test.dev", sample_catalog());
        let results = client.search("nonexistent").await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_get_plugin() {
        let client = MarketplaceClient::with_catalog("https://test.dev", sample_catalog());
        let plugin = client.get_plugin("rate-limiter", "2.1.0").await.unwrap();
        assert_eq!(plugin.name, "rate-limiter");
    }

    #[tokio::test]
    async fn test_get_plugin_not_found() {
        let client = MarketplaceClient::with_catalog("https://test.dev", sample_catalog());
        let result = client.get_plugin("no-such-plugin", "1.0.0").await;
        assert!(matches!(result, Err(MarketplaceError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_get_popular() {
        let client = MarketplaceClient::with_catalog("https://test.dev", sample_catalog());
        let popular = client.get_popular(1).await.unwrap();
        assert_eq!(popular.len(), 1);
        assert_eq!(popular[0].name, "rate-limiter"); // highest downloads
    }

    #[tokio::test]
    async fn test_list_categories() {
        let client = MarketplaceClient::new("https://test.dev");
        let cats = client.list_categories().await.unwrap();
        assert!(!cats.is_empty());
        assert!(cats.iter().any(|c| c.id == "validation"));
    }

    #[tokio::test]
    async fn test_offline_mode() {
        let client = MarketplaceClient::with_catalog("https://test.dev", sample_catalog());
        client.set_offline(true);
        assert!(client.is_offline());

        // Search still works against cache
        let results = client.search("email").await.unwrap();
        assert_eq!(results.len(), 1);

        // Download fails in offline mode
        let result = client.download("validate-email", "1.0.0").await;
        assert!(matches!(result, Err(MarketplaceError::RegistryError(_))));
    }
}
