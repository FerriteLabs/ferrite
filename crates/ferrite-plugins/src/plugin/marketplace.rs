//! Plugin Marketplace
//!
//! Local registry for discovering, searching, and resolving plugin versions.
//! Provides a searchable catalog of available plugins with version resolution
//! and compatibility checking against the current Ferrite version.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::manifest::{Version, VersionReq};

/// Plugin marketplace providing a local registry of available plugins.
pub struct PluginMarketplace {
    /// Available plugins indexed by name
    plugins: HashMap<String, MarketplaceEntry>,
    /// Current Ferrite version for compatibility checks
    ferrite_version: Version,
}

impl PluginMarketplace {
    /// Create a new empty marketplace
    pub fn new(ferrite_version: Version) -> Self {
        Self {
            plugins: HashMap::new(),
            ferrite_version,
        }
    }

    /// Register a plugin in the marketplace
    pub fn register(&mut self, entry: MarketplaceEntry) -> Result<(), MarketplaceError> {
        if entry.name.is_empty() {
            return Err(MarketplaceError::InvalidEntry(
                "plugin name cannot be empty".to_string(),
            ));
        }
        if entry.rating < 0.0 || entry.rating > 5.0 {
            return Err(MarketplaceError::InvalidEntry(
                "rating must be between 0.0 and 5.0".to_string(),
            ));
        }
        if self.plugins.contains_key(&entry.name) {
            return Err(MarketplaceError::AlreadyExists(entry.name.clone()));
        }
        self.plugins.insert(entry.name.clone(), entry);
        Ok(())
    }

    /// Remove a plugin from the marketplace
    pub fn unregister(&mut self, name: &str) -> Result<MarketplaceEntry, MarketplaceError> {
        self.plugins
            .remove(name)
            .ok_or_else(|| MarketplaceError::NotFound(name.to_string()))
    }

    /// Search for plugins matching a query
    pub fn search(&self, query: &SearchQuery) -> Vec<&MarketplaceEntry> {
        let mut results: Vec<&MarketplaceEntry> = self
            .plugins
            .values()
            .filter(|entry| {
                // Text filter: match against name, description, or tags
                if let Some(text) = &query.text {
                    let lower = text.to_lowercase();
                    let matches_text = entry.name.to_lowercase().contains(&lower)
                        || entry.description.to_lowercase().contains(&lower)
                        || entry.tags.iter().any(|t| t.to_lowercase().contains(&lower));
                    if !matches_text {
                        return false;
                    }
                }

                // Category filter
                if let Some(cat) = &query.category {
                    if !entry.categories.contains(cat) {
                        return false;
                    }
                }

                // Tags filter: entry must contain all query tags
                if !query.tags.is_empty()
                    && !query.tags.iter().all(|qt| {
                        entry
                            .tags
                            .iter()
                            .any(|et| et.to_lowercase() == qt.to_lowercase())
                    })
                {
                    return false;
                }

                // Minimum rating filter
                if let Some(min) = query.min_rating {
                    if entry.rating < min {
                        return false;
                    }
                }

                true
            })
            .collect();

        // Sort
        match query.sort_by {
            SortBy::Downloads => results.sort_by(|a, b| b.downloads.cmp(&a.downloads)),
            SortBy::Rating => results.sort_by(|a, b| {
                b.rating
                    .partial_cmp(&a.rating)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }),
            SortBy::Recent => results.sort_by(|a, b| b.published_at.cmp(&a.published_at)),
            SortBy::Name => results.sort_by(|a, b| a.name.cmp(&b.name)),
        }

        results.truncate(query.limit);
        results
    }

    /// Get a specific plugin by name
    pub fn get(&self, name: &str) -> Option<&MarketplaceEntry> {
        self.plugins.get(name)
    }

    /// Resolve a version constraint for a named plugin.
    ///
    /// Returns the entry only if it matches the given version requirement
    /// and is compatible with the current Ferrite version.
    pub fn resolve_version(
        &self,
        name: &str,
        constraint: &VersionReq,
    ) -> Option<&MarketplaceEntry> {
        let entry = self.plugins.get(name)?;
        let entry_version = Version::parse(&entry.version).ok()?;

        if !constraint.matches(&entry_version) {
            return None;
        }

        // Check Ferrite version compatibility
        if let Ok(min_ferrite) = Version::parse(&entry.min_ferrite_version) {
            if !self.ferrite_version.is_compatible_with(&min_ferrite) {
                return None;
            }
        }

        Some(entry)
    }

    /// List all categories with their plugin counts
    pub fn list_categories(&self) -> Vec<(PluginCategory, usize)> {
        let mut counts: HashMap<PluginCategory, usize> = HashMap::new();
        for entry in self.plugins.values() {
            for cat in &entry.categories {
                *counts.entry(cat.clone()).or_default() += 1;
            }
        }
        let mut result: Vec<_> = counts.into_iter().collect();
        result.sort_by(|a, b| b.1.cmp(&a.1));
        result
    }

    /// Get the most popular plugins by download count
    pub fn top_plugins(&self, limit: usize) -> Vec<&MarketplaceEntry> {
        let mut entries: Vec<&MarketplaceEntry> = self.plugins.values().collect();
        entries.sort_by(|a, b| b.downloads.cmp(&a.downloads));
        entries.truncate(limit);
        entries
    }

    /// Get overall marketplace statistics
    pub fn stats(&self) -> MarketplaceStats {
        let total_plugins = self.plugins.len();
        let total_downloads = self.plugins.values().map(|e| e.downloads).sum();
        let average_rating = if total_plugins > 0 {
            self.plugins.values().map(|e| e.rating).sum::<f64>() / total_plugins as f64
        } else {
            0.0
        };
        let mut categories: HashMap<PluginCategory, usize> = HashMap::new();
        for entry in self.plugins.values() {
            for cat in &entry.categories {
                *categories.entry(cat.clone()).or_default() += 1;
            }
        }
        let total_categories = categories.len();

        MarketplaceStats {
            total_plugins,
            total_downloads,
            average_rating,
            total_categories,
        }
    }
}

/// A plugin entry in the marketplace
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MarketplaceEntry {
    /// Plugin name (unique identifier)
    pub name: String,
    /// Human-readable description
    pub description: String,
    /// Semantic version string (e.g. "1.2.3")
    pub version: String,
    /// Plugin author
    pub author: String,
    /// SPDX license identifier
    pub license: String,
    /// Total download count
    pub downloads: u64,
    /// Average user rating (0.0–5.0)
    pub rating: f64,
    /// Searchable tags
    pub tags: Vec<String>,
    /// Plugin categories
    pub categories: Vec<PluginCategory>,
    /// Unix timestamp of publication
    pub published_at: u64,
    /// SHA-256 checksum of the plugin artifact
    pub checksum: String,
    /// Size of the plugin artifact in bytes
    pub size_bytes: u64,
    /// Minimum compatible Ferrite version
    pub min_ferrite_version: String,
    /// Names of required plugin dependencies
    pub dependencies: Vec<String>,
}

/// Plugin categories for marketplace classification
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PluginCategory {
    /// Custom Redis-like commands
    Commands,
    /// Custom data structures
    DataTypes,
    /// Storage backend integrations
    Storage,
    /// Authentication and authorization
    Auth,
    /// Metrics and monitoring
    Metrics,
    /// Third-party service integrations
    Integration,
    /// AI and machine learning
    AI,
    /// Search and indexing
    Search,
    /// Streaming and pub/sub extensions
    Streaming,
    /// Uncategorized
    Other,
}

/// Search query for filtering marketplace plugins
#[derive(Clone, Debug, Default)]
pub struct SearchQuery {
    /// Free-text search against name, description, and tags
    pub text: Option<String>,
    /// Filter by category
    pub category: Option<PluginCategory>,
    /// Filter: entry must have all of these tags
    pub tags: Vec<String>,
    /// Filter: minimum rating threshold
    pub min_rating: Option<f64>,
    /// Sort order for results
    pub sort_by: SortBy,
    /// Maximum number of results to return
    pub limit: usize,
}

impl SearchQuery {
    /// Create a new search query with sensible defaults
    pub fn new() -> Self {
        Self {
            limit: 20,
            ..Default::default()
        }
    }

    /// Builder: set text filter
    pub fn with_text(mut self, text: &str) -> Self {
        self.text = Some(text.to_string());
        self
    }

    /// Builder: set category filter
    pub fn with_category(mut self, category: PluginCategory) -> Self {
        self.category = Some(category);
        self
    }

    /// Builder: add a required tag
    pub fn with_tag(mut self, tag: &str) -> Self {
        self.tags.push(tag.to_string());
        self
    }

    /// Builder: set minimum rating
    pub fn with_min_rating(mut self, rating: f64) -> Self {
        self.min_rating = Some(rating);
        self
    }

    /// Builder: set sort order
    pub fn with_sort(mut self, sort_by: SortBy) -> Self {
        self.sort_by = sort_by;
        self
    }

    /// Builder: set result limit
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }
}

/// Sort order for search results
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub enum SortBy {
    /// Sort by download count (descending)
    #[default]
    Downloads,
    /// Sort by rating (descending)
    Rating,
    /// Sort by publication date (most recent first)
    Recent,
    /// Sort alphabetically by name
    Name,
}

/// Resolves semver constraints and checks Ferrite version compatibility.
pub struct VersionResolver {
    /// Current Ferrite version
    ferrite_version: Version,
}

impl VersionResolver {
    /// Create a new resolver for the given Ferrite version
    pub fn new(ferrite_version: Version) -> Self {
        Self { ferrite_version }
    }

    /// Check whether a plugin version satisfies a constraint
    pub fn satisfies(&self, plugin_version: &str, constraint: &VersionReq) -> bool {
        match Version::parse(plugin_version) {
            Ok(v) => constraint.matches(&v),
            Err(_) => false,
        }
    }

    /// Check whether a plugin is compatible with the current Ferrite version
    pub fn is_compatible(&self, min_ferrite_version: &str) -> bool {
        match Version::parse(min_ferrite_version) {
            Ok(min) => self.ferrite_version.is_compatible_with(&min),
            Err(_) => false,
        }
    }

    /// Check both version constraint and Ferrite compatibility in one call
    pub fn resolve(
        &self,
        plugin_version: &str,
        constraint: &VersionReq,
        min_ferrite_version: &str,
    ) -> bool {
        self.satisfies(plugin_version, constraint) && self.is_compatible(min_ferrite_version)
    }
}

/// Overall marketplace statistics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MarketplaceStats {
    /// Total number of registered plugins
    pub total_plugins: usize,
    /// Sum of all plugin downloads
    pub total_downloads: u64,
    /// Average rating across all plugins
    pub average_rating: f64,
    /// Number of distinct categories with at least one plugin
    pub total_categories: usize,
}

/// Marketplace-specific errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum MarketplaceError {
    /// Plugin not found in the marketplace
    #[error("plugin not found: {0}")]
    NotFound(String),

    /// Plugin with this name already exists
    #[error("plugin already exists: {0}")]
    AlreadyExists(String),

    /// Entry failed validation
    #[error("invalid marketplace entry: {0}")]
    InvalidEntry(String),

    /// Version resolution failed
    #[error("version resolution error: {0}")]
    VersionError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_entry(name: &str) -> MarketplaceEntry {
        MarketplaceEntry {
            name: name.to_string(),
            description: format!("A test plugin: {}", name),
            version: "1.0.0".to_string(),
            author: "Test Author".to_string(),
            license: "MIT".to_string(),
            downloads: 100,
            rating: 4.0,
            tags: vec!["test".to_string()],
            categories: vec![PluginCategory::Commands],
            published_at: 1700000000,
            checksum: "abc123".to_string(),
            size_bytes: 1024,
            min_ferrite_version: "0.1.0".to_string(),
            dependencies: vec![],
        }
    }

    fn marketplace() -> PluginMarketplace {
        PluginMarketplace::new(Version::new(0, 1, 0))
    }

    #[test]
    fn test_register_and_get() {
        let mut mp = marketplace();
        mp.register(sample_entry("foo")).unwrap();
        assert!(mp.get("foo").is_some());
        assert!(mp.get("bar").is_none());
    }

    #[test]
    fn test_register_duplicate() {
        let mut mp = marketplace();
        mp.register(sample_entry("foo")).unwrap();
        assert!(matches!(
            mp.register(sample_entry("foo")),
            Err(MarketplaceError::AlreadyExists(_))
        ));
    }

    #[test]
    fn test_register_empty_name() {
        let mut mp = marketplace();
        let mut entry = sample_entry("x");
        entry.name = String::new();
        assert!(matches!(
            mp.register(entry),
            Err(MarketplaceError::InvalidEntry(_))
        ));
    }

    #[test]
    fn test_register_invalid_rating() {
        let mut mp = marketplace();
        let mut entry = sample_entry("x");
        entry.rating = 5.5;
        assert!(matches!(
            mp.register(entry),
            Err(MarketplaceError::InvalidEntry(_))
        ));
    }

    #[test]
    fn test_unregister() {
        let mut mp = marketplace();
        mp.register(sample_entry("foo")).unwrap();
        let removed = mp.unregister("foo").unwrap();
        assert_eq!(removed.name, "foo");
        assert!(mp.get("foo").is_none());
    }

    #[test]
    fn test_unregister_not_found() {
        let mut mp = marketplace();
        assert!(matches!(
            mp.unregister("nope"),
            Err(MarketplaceError::NotFound(_))
        ));
    }

    #[test]
    fn test_search_text() {
        let mut mp = marketplace();
        mp.register(sample_entry("redis-cache")).unwrap();
        mp.register(sample_entry("bloom-filter")).unwrap();

        let query = SearchQuery::new().with_text("redis");
        let results = mp.search(&query);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "redis-cache");
    }

    #[test]
    fn test_search_category() {
        let mut mp = marketplace();
        let mut storage = sample_entry("storage-plugin");
        storage.categories = vec![PluginCategory::Storage];
        mp.register(sample_entry("cmd-plugin")).unwrap();
        mp.register(storage).unwrap();

        let query = SearchQuery::new().with_category(PluginCategory::Storage);
        let results = mp.search(&query);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "storage-plugin");
    }

    #[test]
    fn test_search_min_rating() {
        let mut mp = marketplace();
        let mut low = sample_entry("low-rated");
        low.rating = 2.0;
        mp.register(low).unwrap();
        mp.register(sample_entry("high-rated")).unwrap(); // rating 4.0

        let query = SearchQuery::new().with_min_rating(3.0);
        let results = mp.search(&query);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "high-rated");
    }

    #[test]
    fn test_search_sort_by_name() {
        let mut mp = marketplace();
        mp.register(sample_entry("zeta")).unwrap();
        mp.register(sample_entry("alpha")).unwrap();

        let query = SearchQuery::new().with_sort(SortBy::Name);
        let results = mp.search(&query);
        assert_eq!(results[0].name, "alpha");
        assert_eq!(results[1].name, "zeta");
    }

    #[test]
    fn test_search_limit() {
        let mut mp = marketplace();
        for i in 0..10 {
            mp.register(sample_entry(&format!("plugin-{}", i))).unwrap();
        }
        let query = SearchQuery::new().with_limit(3).with_sort(SortBy::Name);
        assert_eq!(mp.search(&query).len(), 3);
    }

    #[test]
    fn test_search_tags() {
        let mut mp = marketplace();
        let mut tagged = sample_entry("tagged");
        tagged.tags = vec!["cache".to_string(), "fast".to_string()];
        mp.register(tagged).unwrap();
        mp.register(sample_entry("untagged")).unwrap();

        let query = SearchQuery::new().with_tag("cache");
        let results = mp.search(&query);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "tagged");
    }

    #[test]
    fn test_resolve_version_match() {
        let mut mp = marketplace();
        mp.register(sample_entry("foo")).unwrap();

        let constraint = VersionReq::AtLeast(Version::new(1, 0, 0));
        assert!(mp.resolve_version("foo", &constraint).is_some());
    }

    #[test]
    fn test_resolve_version_no_match() {
        let mut mp = marketplace();
        mp.register(sample_entry("foo")).unwrap();

        let constraint = VersionReq::AtLeast(Version::new(2, 0, 0));
        assert!(mp.resolve_version("foo", &constraint).is_none());
    }

    #[test]
    fn test_resolve_version_ferrite_incompat() {
        let mut mp = PluginMarketplace::new(Version::new(0, 1, 0));
        let mut entry = sample_entry("foo");
        entry.min_ferrite_version = "1.0.0".to_string();
        mp.register(entry).unwrap();

        let constraint = VersionReq::Any;
        assert!(mp.resolve_version("foo", &constraint).is_none());
    }

    #[test]
    fn test_list_categories() {
        let mut mp = marketplace();
        let mut storage = sample_entry("s1");
        storage.categories = vec![PluginCategory::Storage];
        mp.register(sample_entry("c1")).unwrap();
        mp.register(sample_entry("c2")).unwrap();
        mp.register(storage).unwrap();

        let cats = mp.list_categories();
        let commands_count = cats
            .iter()
            .find(|(c, _)| *c == PluginCategory::Commands)
            .map(|(_, n)| *n)
            .unwrap_or(0);
        assert_eq!(commands_count, 2);
    }

    #[test]
    fn test_top_plugins() {
        let mut mp = marketplace();
        let mut popular = sample_entry("popular");
        popular.downloads = 9999;
        let mut unpopular = sample_entry("unpopular");
        unpopular.downloads = 1;
        mp.register(unpopular).unwrap();
        mp.register(popular).unwrap();

        let top = mp.top_plugins(1);
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].name, "popular");
    }

    #[test]
    fn test_stats() {
        let mut mp = marketplace();
        mp.register(sample_entry("a")).unwrap();
        mp.register(sample_entry("b")).unwrap();

        let stats = mp.stats();
        assert_eq!(stats.total_plugins, 2);
        assert_eq!(stats.total_downloads, 200);
        assert!((stats.average_rating - 4.0).abs() < f64::EPSILON);
        assert_eq!(stats.total_categories, 1);
    }

    #[test]
    fn test_stats_empty() {
        let mp = marketplace();
        let stats = mp.stats();
        assert_eq!(stats.total_plugins, 0);
        assert_eq!(stats.total_downloads, 0);
        assert!((stats.average_rating - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_version_resolver_satisfies() {
        let resolver = VersionResolver::new(Version::new(0, 1, 0));
        assert!(resolver.satisfies("1.0.0", &VersionReq::AtLeast(Version::new(1, 0, 0))));
        assert!(!resolver.satisfies("0.9.0", &VersionReq::AtLeast(Version::new(1, 0, 0))));
        assert!(!resolver.satisfies("invalid", &VersionReq::Any));
    }

    #[test]
    fn test_version_resolver_compatible() {
        let resolver = VersionResolver::new(Version::new(1, 2, 0));
        assert!(resolver.is_compatible("1.0.0"));
        assert!(resolver.is_compatible("1.2.0"));
        assert!(!resolver.is_compatible("1.3.0"));
        assert!(!resolver.is_compatible("2.0.0"));
    }

    #[test]
    fn test_version_resolver_resolve() {
        let resolver = VersionResolver::new(Version::new(1, 2, 0));
        assert!(resolver.resolve(
            "2.0.0",
            &VersionReq::AtLeast(Version::new(1, 0, 0)),
            "1.0.0"
        ));
        assert!(!resolver.resolve(
            "2.0.0",
            &VersionReq::AtLeast(Version::new(1, 0, 0)),
            "2.0.0"
        ));
    }
}
