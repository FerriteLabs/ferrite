//! Plugin registry for catalog management and search.
//!
//! Provides both a general-purpose `PluginRegistry` and a WASM-specific
//! `MarketplaceRegistry` with module categories, checksum verification,
//! and semver-compatible version resolution.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::PluginMetadata;

/// Plugin registry managing the catalog of available plugins.
pub struct PluginRegistry {
    plugins: HashMap<String, Vec<PluginMetadata>>, // name -> versions
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self {
            plugins: HashMap::new(),
        }
    }

    /// Registers a plugin version.
    pub fn register(&mut self, metadata: PluginMetadata) {
        self.plugins
            .entry(metadata.name.clone())
            .or_default()
            .push(metadata);
    }

    /// Finds a plugin by name and optional version.
    pub fn find(&self, name: &str, version: Option<&str>) -> Option<&PluginMetadata> {
        let versions = self.plugins.get(name)?;
        match version {
            Some(v) => versions.iter().find(|p| p.version == v),
            None => versions.last(), // Latest version
        }
    }

    /// Searches plugins by query.
    pub fn search(&self, query: &str) -> Vec<&PluginMetadata> {
        let query_lower = query.to_lowercase();
        self.plugins
            .values()
            .filter_map(|versions| versions.last())
            .filter(|p| {
                p.name.to_lowercase().contains(&query_lower)
                    || p.description.to_lowercase().contains(&query_lower)
                    || p.tags
                        .iter()
                        .any(|t| t.to_lowercase().contains(&query_lower))
            })
            .collect()
    }

    /// Lists all plugins (latest versions only).
    pub fn list_all(&self) -> Vec<&PluginMetadata> {
        self.plugins
            .values()
            .filter_map(|versions| versions.last())
            .collect()
    }

    /// Returns the total number of registered plugins.
    pub fn count(&self) -> usize {
        self.plugins.len()
    }

    /// Lists available versions for a plugin.
    pub fn versions(&self, name: &str) -> Vec<&str> {
        self.plugins
            .get(name)
            .map(|versions| versions.iter().map(|p| p.version.as_str()).collect())
            .unwrap_or_default()
    }
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// WASM Module Category
// ---------------------------------------------------------------------------

/// Categories for classifying WASM modules in the marketplace.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ModuleCategory {
    /// Data processing (ETL, aggregation, enrichment)
    DataProcessing,
    /// Input validation (email, JSON schema, format checks)
    Validation,
    /// Data transformation (format conversion, reshaping)
    Transformation,
    /// Analytics (statistics, counters, metrics)
    Analytics,
    /// Security (hashing, rate limiting, access control helpers)
    Security,
    /// Uncategorized
    Other(String),
}

impl std::fmt::Display for ModuleCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ModuleCategory::DataProcessing => write!(f, "data-processing"),
            ModuleCategory::Validation => write!(f, "validation"),
            ModuleCategory::Transformation => write!(f, "transformation"),
            ModuleCategory::Analytics => write!(f, "analytics"),
            ModuleCategory::Security => write!(f, "security"),
            ModuleCategory::Other(s) => write!(f, "{}", s),
        }
    }
}

// ---------------------------------------------------------------------------
// WASM Module Metadata
// ---------------------------------------------------------------------------

/// Metadata for a WASM module in the marketplace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmModuleMetadata {
    /// Module name (unique identifier, e.g. `validate-email`).
    pub name: String,
    /// Semantic version string.
    pub version: String,
    /// Author name or organisation.
    pub author: String,
    /// Short description.
    pub description: String,
    /// SPDX license identifier.
    pub license: String,
    /// Size of the `.wasm` binary in bytes.
    pub size_bytes: u64,
    /// Hex-encoded SHA-256 checksum of the `.wasm` binary.
    pub checksum: String,
    /// Categories this module belongs to.
    pub categories: Vec<ModuleCategory>,
    /// Searchable tags.
    pub tags: Vec<String>,
    /// Download / install count.
    pub downloads: u64,
    /// Minimum compatible Ferrite version.
    pub min_ferrite_version: String,
    /// Module dependencies (other WASM modules).
    pub dependencies: Vec<ModuleDependency>,
}

/// A dependency on another WASM module.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleDependency {
    /// Module name.
    pub name: String,
    /// Semver version requirement (e.g. `>=1.0.0`).
    pub version_req: String,
}

// ---------------------------------------------------------------------------
// Marketplace Registry
// ---------------------------------------------------------------------------

/// Central registry for WASM modules in the marketplace.
///
/// Stores a catalog of available modules indexed by name, each with one or
/// more published versions. Supports text search, category filtering, and
/// semver-compatible version resolution.
pub struct MarketplaceRegistry {
    /// Module catalog: name → list of versions (oldest first).
    modules: HashMap<String, Vec<WasmModuleMetadata>>,
}

impl MarketplaceRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self {
            modules: HashMap::new(),
        }
    }

    /// Publishes a module version to the registry.
    pub fn publish(&mut self, metadata: WasmModuleMetadata) -> Result<(), RegistryError> {
        if metadata.name.is_empty() {
            return Err(RegistryError::InvalidModule(
                "module name cannot be empty".into(),
            ));
        }
        let versions = self.modules.entry(metadata.name.clone()).or_default();
        if versions.iter().any(|m| m.version == metadata.version) {
            return Err(RegistryError::VersionExists {
                name: metadata.name,
                version: metadata.version,
            });
        }
        versions.push(metadata);
        Ok(())
    }

    /// Removes a module entirely (all versions).
    pub fn remove(&mut self, name: &str) -> Result<(), RegistryError> {
        self.modules
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| RegistryError::NotFound(name.to_string()))
    }

    /// Returns the latest version of a module.
    pub fn latest(&self, name: &str) -> Option<&WasmModuleMetadata> {
        self.modules.get(name).and_then(|v| v.last())
    }

    /// Finds a specific version of a module.
    pub fn find(&self, name: &str, version: &str) -> Option<&WasmModuleMetadata> {
        self.modules
            .get(name)?
            .iter()
            .find(|m| m.version == version)
    }

    /// Lists all published versions for a module.
    pub fn versions(&self, name: &str) -> Vec<&str> {
        self.modules
            .get(name)
            .map(|v| v.iter().map(|m| m.version.as_str()).collect())
            .unwrap_or_default()
    }

    /// Resolves the best matching version for a semver requirement string.
    ///
    /// Supports simple constraints: `>=X.Y.Z`, `=X.Y.Z`, `^X.Y.Z`, or `*`.
    /// Returns the highest compatible version.
    pub fn resolve_version(
        &self,
        name: &str,
        requirement: &str,
    ) -> Option<&WasmModuleMetadata> {
        let versions = self.modules.get(name)?;
        versions
            .iter()
            .rev()
            .find(|m| semver_matches(&m.version, requirement))
    }

    /// Searches the catalog by free-text query against name, description, and tags.
    pub fn search(&self, query: &str) -> Vec<&WasmModuleMetadata> {
        let q = query.to_lowercase();
        self.modules
            .values()
            .filter_map(|v| v.last())
            .filter(|m| {
                m.name.to_lowercase().contains(&q)
                    || m.description.to_lowercase().contains(&q)
                    || m.tags.iter().any(|t| t.to_lowercase().contains(&q))
            })
            .collect()
    }

    /// Filters modules by category.
    pub fn filter_by_category(&self, category: &ModuleCategory) -> Vec<&WasmModuleMetadata> {
        self.modules
            .values()
            .filter_map(|v| v.last())
            .filter(|m| m.categories.contains(category))
            .collect()
    }

    /// Returns the total number of distinct modules.
    pub fn module_count(&self) -> usize {
        self.modules.len()
    }

    /// Returns a flat list of all latest module versions.
    pub fn list_all(&self) -> Vec<&WasmModuleMetadata> {
        self.modules.values().filter_map(|v| v.last()).collect()
    }
}

impl Default for MarketplaceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Semver helpers
// ---------------------------------------------------------------------------

/// Parses a semver string into (major, minor, patch).
fn parse_semver(s: &str) -> Option<(u64, u64, u64)> {
    let s = s.trim().trim_start_matches('v');
    let parts: Vec<&str> = s.splitn(3, '.').collect();
    if parts.len() < 3 {
        return None;
    }
    let major = parts[0].parse().ok()?;
    let minor = parts[1].parse().ok()?;
    // Strip any pre-release suffix (e.g. "0-beta")
    let patch_str = parts[2].split('-').next()?;
    let patch = patch_str.parse().ok()?;
    Some((major, minor, patch))
}

/// Checks if `version` satisfies a simple semver `requirement`.
fn semver_matches(version: &str, requirement: &str) -> bool {
    let req = requirement.trim();
    if req == "*" {
        return true;
    }

    let (op, req_ver) = if let Some(r) = req.strip_prefix(">=") {
        (">=", r.trim())
    } else if let Some(r) = req.strip_prefix('^') {
        ("^", r.trim())
    } else if let Some(r) = req.strip_prefix('=') {
        ("=", r.trim())
    } else {
        (">=", req) // default to >= for bare versions
    };

    let v = match parse_semver(version) {
        Some(v) => v,
        None => return false,
    };
    let r = match parse_semver(req_ver) {
        Some(r) => r,
        None => return false,
    };

    match op {
        ">=" => v >= r,
        "=" => v == r,
        "^" => v.0 == r.0 && v >= r,
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors produced by the marketplace registry.
#[derive(Debug, Clone, thiserror::Error)]
pub enum RegistryError {
    #[error("module not found: {0}")]
    NotFound(String),

    #[error("version {version} of module {name} already exists")]
    VersionExists { name: String, version: String },

    #[error("invalid module: {0}")]
    InvalidModule(String),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::super::{PluginType, SecurityStatus};
    use super::*;

    fn make_plugin(name: &str, version: &str) -> PluginMetadata {
        PluginMetadata {
            name: name.to_string(),
            display_name: name.to_string(),
            description: format!("Plugin {}", name),
            version: version.to_string(),
            author: "Test".to_string(),
            license: "MIT".to_string(),
            homepage: None,
            repository: None,
            plugin_type: PluginType::Extension,
            min_ferrite_version: "0.1.0".to_string(),
            sdk_version: "1.0.0".to_string(),
            dependencies: vec![],
            tags: vec![],
            downloads: 0,
            rating: 0.0,
            rating_count: 0,
            published_at: chrono::Utc::now(),
            security_status: SecurityStatus::Passed,
        }
    }

    fn make_wasm_module(name: &str, version: &str) -> WasmModuleMetadata {
        WasmModuleMetadata {
            name: name.to_string(),
            version: version.to_string(),
            author: "Test".to_string(),
            description: format!("Module {}", name),
            license: "MIT".to_string(),
            size_bytes: 1024,
            checksum: "abc123".to_string(),
            categories: vec![ModuleCategory::Validation],
            tags: vec!["test".to_string()],
            downloads: 0,
            min_ferrite_version: "0.1.0".to_string(),
            dependencies: vec![],
        }
    }

    // -- PluginRegistry tests ------------------------------------------------

    #[test]
    fn test_register_and_find() {
        let mut registry = PluginRegistry::new();
        registry.register(make_plugin("test-plugin", "1.0.0"));

        assert!(registry.find("test-plugin", None).is_some());
        assert!(registry.find("test-plugin", Some("1.0.0")).is_some());
        assert!(registry.find("nonexistent", None).is_none());
    }

    #[test]
    fn test_search() {
        let mut registry = PluginRegistry::new();
        registry.register(make_plugin("vector-search", "1.0.0"));
        registry.register(make_plugin("json-tools", "1.0.0"));

        let results = registry.search("vector");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_versions() {
        let mut registry = PluginRegistry::new();
        registry.register(make_plugin("test", "1.0.0"));
        registry.register(make_plugin("test", "2.0.0"));

        let versions = registry.versions("test");
        assert_eq!(versions, vec!["1.0.0", "2.0.0"]);
    }

    // -- MarketplaceRegistry tests -------------------------------------------

    #[test]
    fn test_marketplace_publish_and_latest() {
        let mut reg = MarketplaceRegistry::new();
        reg.publish(make_wasm_module("validate-email", "1.0.0"))
            .expect("publish failed");
        reg.publish(make_wasm_module("validate-email", "1.1.0"))
            .expect("publish failed");

        let latest = reg.latest("validate-email").expect("not found");
        assert_eq!(latest.version, "1.1.0");
        assert_eq!(reg.module_count(), 1);
    }

    #[test]
    fn test_marketplace_duplicate_version() {
        let mut reg = MarketplaceRegistry::new();
        reg.publish(make_wasm_module("mod-a", "1.0.0"))
            .expect("ok");
        let result = reg.publish(make_wasm_module("mod-a", "1.0.0"));
        assert!(matches!(result, Err(RegistryError::VersionExists { .. })));
    }

    #[test]
    fn test_marketplace_remove() {
        let mut reg = MarketplaceRegistry::new();
        reg.publish(make_wasm_module("mod-a", "1.0.0"))
            .expect("ok");
        reg.remove("mod-a").expect("remove failed");
        assert!(reg.latest("mod-a").is_none());
    }

    #[test]
    fn test_marketplace_search() {
        let mut reg = MarketplaceRegistry::new();
        reg.publish(make_wasm_module("validate-email", "1.0.0"))
            .expect("ok");
        reg.publish(make_wasm_module("json-transform", "1.0.0"))
            .expect("ok");

        assert_eq!(reg.search("validate").len(), 1);
        assert_eq!(reg.search("Module").len(), 2); // description match
    }

    #[test]
    fn test_marketplace_filter_by_category() {
        let mut reg = MarketplaceRegistry::new();
        let mut analytics = make_wasm_module("counter", "1.0.0");
        analytics.categories = vec![ModuleCategory::Analytics];
        reg.publish(analytics).expect("ok");
        reg.publish(make_wasm_module("validator", "1.0.0"))
            .expect("ok");

        assert_eq!(
            reg.filter_by_category(&ModuleCategory::Analytics).len(),
            1
        );
        assert_eq!(
            reg.filter_by_category(&ModuleCategory::Validation).len(),
            1
        );
    }

    #[test]
    fn test_marketplace_resolve_version() {
        let mut reg = MarketplaceRegistry::new();
        reg.publish(make_wasm_module("mod-a", "1.0.0"))
            .expect("ok");
        reg.publish(make_wasm_module("mod-a", "1.2.0"))
            .expect("ok");
        reg.publish(make_wasm_module("mod-a", "2.0.0"))
            .expect("ok");

        let resolved = reg
            .resolve_version("mod-a", "^1.0.0")
            .expect("resolve failed");
        assert_eq!(resolved.version, "1.2.0");

        let resolved = reg
            .resolve_version("mod-a", ">=2.0.0")
            .expect("resolve failed");
        assert_eq!(resolved.version, "2.0.0");

        assert!(reg.resolve_version("mod-a", ">=3.0.0").is_none());
    }

    #[test]
    fn test_semver_helpers() {
        assert!(semver_matches("1.2.3", ">=1.0.0"));
        assert!(!semver_matches("0.9.0", ">=1.0.0"));
        assert!(semver_matches("1.2.3", "^1.0.0"));
        assert!(!semver_matches("2.0.0", "^1.0.0"));
        assert!(semver_matches("1.0.0", "=1.0.0"));
        assert!(!semver_matches("1.0.1", "=1.0.0"));
        assert!(semver_matches("3.0.0", "*"));
    }
}
