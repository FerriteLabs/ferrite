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
    pub fn resolve_version(&self, name: &str, requirement: &str) -> Option<&WasmModuleMetadata> {
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

// ===========================================================================
// WASM Extension Marketplace Registry
// ===========================================================================
//
// Curated package registry for community WASM extensions with
// versioning, sandboxing, trust verification, and hot-reload.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::RwLock;

/// Configuration for the extension registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryConfig {
    /// URL of the remote registry.
    pub registry_url: String,
    /// Local cache directory for downloaded extensions.
    pub cache_dir: String,
    /// Maximum number of installed extensions.
    pub max_installed: usize,
    /// Whether to auto-update installed extensions.
    pub auto_update: bool,
    /// Whether to verify signatures on install.
    pub verify_signatures: bool,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            registry_url: "https://marketplace.ferrite.dev".into(),
            cache_dir: "./extensions_cache".into(),
            max_installed: 100,
            auto_update: false,
            verify_signatures: true,
        }
    }
}

/// Manifest describing a WASM extension package.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtensionManifest {
    pub name: String,
    pub version: String,
    pub description: String,
    pub author: String,
    pub license: String,
    pub homepage: Option<String>,
    pub repository: Option<String>,
    pub keywords: Vec<String>,
    pub permissions: Vec<Permission>,
    pub entrypoints: Vec<Entrypoint>,
    pub dependencies: Vec<ExtDependency>,
    pub min_ferrite_version: String,
    pub size_bytes: u64,
    pub checksum: String,
    pub published_at: u64,
}

/// Permissions that an extension may request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Permission {
    ReadKeys,
    WriteKeys,
    Network,
    FileSystem,
    Timer,
    PubSub,
    Metrics,
    Admin,
}

/// An entry point exposed by an extension.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entrypoint {
    pub name: String,
    pub kind: EntrypointKind,
    pub description: String,
}

/// Kinds of extension entry points.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntrypointKind {
    Command,
    Trigger,
    Transform,
    DataType,
    Protocol,
    AuthProvider,
}

/// A dependency on another extension.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtDependency {
    pub name: String,
    pub version: String,
}

/// An installed extension with runtime state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstalledExtension {
    pub manifest: ExtensionManifest,
    pub installed_at: u64,
    pub enabled: bool,
    pub state: ExtensionState,
    pub invocations: u64,
    pub errors: u64,
    pub avg_exec_us: f64,
}

/// Runtime state of an extension.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExtensionState {
    Loading,
    Active,
    Disabled,
    Error(String),
    Updating,
}

/// Detailed information about an extension.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtensionInfo {
    pub manifest: ExtensionManifest,
    pub installed: bool,
    pub state: Option<ExtensionState>,
    pub downloads: u64,
    pub rating: f64,
    pub reviews_count: u64,
}

/// Result of verifying an extension.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationResult {
    pub name: String,
    pub checksum_valid: bool,
    pub signature_valid: bool,
    pub permissions_ok: bool,
    pub issues: Vec<String>,
}

/// Statistics for the extension registry.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RegistryStats {
    pub installed: usize,
    pub active: usize,
    pub total_invocations: u64,
    pub total_errors: u64,
    pub cache_size_bytes: u64,
}

/// Errors from the extension registry.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ExtRegistryError {
    #[error("extension not found: {0}")]
    NotFound(String),
    #[error("extension already installed: {0}")]
    AlreadyInstalled(String),
    #[error("download failed: {0}")]
    DownloadFailed(String),
    #[error("verification failed: {0}")]
    VerificationFailed(String),
    #[error("incompatible version: {0}")]
    IncompatibleVersion(String),
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error("maximum installed extensions ({0}) reached")]
    MaxInstalled(usize),
}

/// WASM Extension marketplace registry.
pub struct ExtensionRegistry {
    config: RegistryConfig,
    installed: RwLock<HashMap<String, InstalledExtension>>,
    catalog: RwLock<Vec<ExtensionManifest>>,
    total_invocations: AtomicU64,
    total_errors: AtomicU64,
}

impl ExtensionRegistry {
    /// Create a new extension registry.
    pub fn new(config: RegistryConfig) -> Self {
        let registry = Self {
            config,
            installed: RwLock::new(HashMap::new()),
            catalog: RwLock::new(Vec::new()),
            total_invocations: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
        };
        registry.populate_builtin_catalog();
        registry
    }

    /// Install an extension by name with optional version.
    pub fn install(
        &self,
        name: &str,
        version: Option<&str>,
    ) -> Result<InstalledExtension, ExtRegistryError> {
        if self.installed.read().contains_key(name) {
            return Err(ExtRegistryError::AlreadyInstalled(name.into()));
        }
        if self.installed.read().len() >= self.config.max_installed {
            return Err(ExtRegistryError::MaxInstalled(self.config.max_installed));
        }

        // Find manifest from catalog
        let manifest = {
            let cat = self.catalog.read();
            cat.iter()
                .find(|m| {
                    m.name == name && version.map_or(true, |v| m.version == v)
                })
                .cloned()
                .ok_or_else(|| ExtRegistryError::NotFound(name.into()))?
        };

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let ext = InstalledExtension {
            manifest,
            installed_at: now,
            enabled: true,
            state: ExtensionState::Active,
            invocations: 0,
            errors: 0,
            avg_exec_us: 0.0,
        };

        self.installed.write().insert(name.to_string(), ext.clone());
        Ok(ext)
    }

    /// Uninstall an extension by name.
    pub fn uninstall(&self, name: &str) -> Result<(), ExtRegistryError> {
        self.installed
            .write()
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| ExtRegistryError::NotFound(name.into()))
    }

    /// Enable an installed extension.
    pub fn enable(&self, name: &str) -> Result<(), ExtRegistryError> {
        let mut installed = self.installed.write();
        let ext = installed
            .get_mut(name)
            .ok_or_else(|| ExtRegistryError::NotFound(name.into()))?;
        ext.enabled = true;
        ext.state = ExtensionState::Active;
        Ok(())
    }

    /// Disable an installed extension.
    pub fn disable(&self, name: &str) -> Result<(), ExtRegistryError> {
        let mut installed = self.installed.write();
        let ext = installed
            .get_mut(name)
            .ok_or_else(|| ExtRegistryError::NotFound(name.into()))?;
        ext.enabled = false;
        ext.state = ExtensionState::Disabled;
        Ok(())
    }

    /// Update an extension to the latest version.
    pub fn update(&self, name: &str) -> Result<InstalledExtension, ExtRegistryError> {
        // Remove the old one and re-install latest
        self.uninstall(name)?;
        self.install(name, None)
    }

    /// List all installed extensions.
    pub fn list_installed(&self) -> Vec<InstalledExtension> {
        self.installed.read().values().cloned().collect()
    }

    /// Search the catalog for extensions matching a query.
    pub fn search(&self, query: &str) -> Vec<ExtensionManifest> {
        let q = query.to_lowercase();
        self.catalog
            .read()
            .iter()
            .filter(|m| {
                m.name.to_lowercase().contains(&q)
                    || m.description.to_lowercase().contains(&q)
                    || m.keywords.iter().any(|k| k.to_lowercase().contains(&q))
            })
            .cloned()
            .collect()
    }

    /// Get detailed information about an extension.
    pub fn info(&self, name: &str) -> Option<ExtensionInfo> {
        let cat = self.catalog.read();
        let manifest = cat.iter().find(|m| m.name == name)?.clone();
        let installed = self.installed.read();
        let inst = installed.get(name);
        Some(ExtensionInfo {
            manifest,
            installed: inst.is_some(),
            state: inst.map(|i| i.state.clone()),
            downloads: 0,
            rating: 0.0,
            reviews_count: 0,
        })
    }

    /// Verify an installed extension.
    pub fn verify(&self, name: &str) -> VerificationResult {
        let installed = self.installed.read();
        let mut issues = Vec::new();
        let (checksum_valid, signature_valid, permissions_ok) =
            if let Some(ext) = installed.get(name) {
                // Simulate verification
                let ck = !ext.manifest.checksum.is_empty();
                if !ck {
                    issues.push("missing checksum".into());
                }
                let sig = self.config.verify_signatures;
                let perm = !ext.manifest.permissions.contains(&Permission::Admin);
                if !perm {
                    issues.push("extension requests Admin permission".into());
                }
                (ck, sig, perm)
            } else {
                issues.push("extension not installed".into());
                (false, false, false)
            };

        VerificationResult {
            name: name.into(),
            checksum_valid,
            signature_valid,
            permissions_ok,
            issues,
        }
    }

    /// Get registry statistics.
    pub fn stats(&self) -> RegistryStats {
        let installed = self.installed.read();
        let active = installed.values().filter(|e| e.enabled).count();
        RegistryStats {
            installed: installed.len(),
            active,
            total_invocations: self.total_invocations.load(Ordering::Relaxed),
            total_errors: self.total_errors.load(Ordering::Relaxed),
            cache_size_bytes: 0,
        }
    }

    // -- private helpers -------------------------------------------------

    fn populate_builtin_catalog(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_secs();

        let extensions = vec![
            ExtensionManifest {
                name: "rate-limiter".into(),
                version: "1.2.0".into(),
                description: "Token bucket rate limiter with sliding window".into(),
                author: "ferritelabs".into(),
                license: "Apache-2.0".into(),
                homepage: None,
                repository: Some("https://github.com/ferritelabs/ext-rate-limiter".into()),
                keywords: vec!["rate-limit".into(), "throttle".into()],
                permissions: vec![Permission::ReadKeys, Permission::WriteKeys],
                entrypoints: vec![Entrypoint {
                    name: "RATELIMIT".into(),
                    kind: EntrypointKind::Command,
                    description: "Check and increment rate limit counter".into(),
                }],
                dependencies: vec![],
                min_ferrite_version: "0.1.0".into(),
                size_bytes: 48_000,
                checksum: "abc123def456".into(),
                published_at: now,
            },
            ExtensionManifest {
                name: "json-validator".into(),
                version: "2.0.0".into(),
                description: "Validate JSON values against JSON Schema on write".into(),
                author: "ferritelabs".into(),
                license: "Apache-2.0".into(),
                homepage: None,
                repository: None,
                keywords: vec!["json".into(), "validation".into(), "schema".into()],
                permissions: vec![Permission::ReadKeys],
                entrypoints: vec![Entrypoint {
                    name: "JSON.VALIDATE".into(),
                    kind: EntrypointKind::Command,
                    description: "Validate a JSON value against a schema".into(),
                }],
                dependencies: vec![],
                min_ferrite_version: "0.1.0".into(),
                size_bytes: 62_000,
                checksum: "def789abc012".into(),
                published_at: now,
            },
            ExtensionManifest {
                name: "geo-fence".into(),
                version: "1.0.0".into(),
                description: "Geofencing with point-in-polygon queries".into(),
                author: "ferritelabs".into(),
                license: "Apache-2.0".into(),
                homepage: None,
                repository: None,
                keywords: vec!["geo".into(), "spatial".into(), "fence".into()],
                permissions: vec![Permission::ReadKeys, Permission::WriteKeys],
                entrypoints: vec![Entrypoint {
                    name: "GEO.FENCE".into(),
                    kind: EntrypointKind::DataType,
                    description: "Geofence data type".into(),
                }],
                dependencies: vec![],
                min_ferrite_version: "0.1.0".into(),
                size_bytes: 35_000,
                checksum: "geo123hash456".into(),
                published_at: now,
            },
            ExtensionManifest {
                name: "bloom-filter".into(),
                version: "1.1.0".into(),
                description: "Space-efficient probabilistic set membership".into(),
                author: "ferritelabs".into(),
                license: "MIT".into(),
                homepage: None,
                repository: None,
                keywords: vec!["probabilistic".into(), "filter".into(), "bloom".into()],
                permissions: vec![Permission::ReadKeys, Permission::WriteKeys],
                entrypoints: vec![Entrypoint {
                    name: "BF.ADD".into(),
                    kind: EntrypointKind::Command,
                    description: "Add to bloom filter".into(),
                }],
                dependencies: vec![],
                min_ferrite_version: "0.1.0".into(),
                size_bytes: 28_000,
                checksum: "bloom789hash".into(),
                published_at: now,
            },
            ExtensionManifest {
                name: "auth-jwt".into(),
                version: "1.0.0".into(),
                description: "JWT-based authentication provider for Ferrite".into(),
                author: "community".into(),
                license: "MIT".into(),
                homepage: None,
                repository: None,
                keywords: vec!["auth".into(), "jwt".into(), "security".into()],
                permissions: vec![Permission::ReadKeys, Permission::Network, Permission::Admin],
                entrypoints: vec![Entrypoint {
                    name: "AUTH.JWT".into(),
                    kind: EntrypointKind::AuthProvider,
                    description: "JWT auth provider".into(),
                }],
                dependencies: vec![],
                min_ferrite_version: "0.1.0".into(),
                size_bytes: 55_000,
                checksum: "jwt456hash789".into(),
                published_at: now,
            },
        ];
        *self.catalog.write() = extensions;
    }
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
        reg.publish(make_wasm_module("mod-a", "1.0.0")).expect("ok");
        let result = reg.publish(make_wasm_module("mod-a", "1.0.0"));
        assert!(matches!(result, Err(RegistryError::VersionExists { .. })));
    }

    #[test]
    fn test_marketplace_remove() {
        let mut reg = MarketplaceRegistry::new();
        reg.publish(make_wasm_module("mod-a", "1.0.0")).expect("ok");
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

        assert_eq!(reg.filter_by_category(&ModuleCategory::Analytics).len(), 1);
        assert_eq!(reg.filter_by_category(&ModuleCategory::Validation).len(), 1);
    }

    #[test]
    fn test_marketplace_resolve_version() {
        let mut reg = MarketplaceRegistry::new();
        reg.publish(make_wasm_module("mod-a", "1.0.0")).expect("ok");
        reg.publish(make_wasm_module("mod-a", "1.2.0")).expect("ok");
        reg.publish(make_wasm_module("mod-a", "2.0.0")).expect("ok");

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

    // -- ExtensionRegistry tests ----------------------------------------

    #[test]
    fn test_ext_registry_install_and_uninstall() {
        let reg = ExtensionRegistry::new(RegistryConfig::default());
        let ext = reg.install("rate-limiter", None).expect("install");
        assert_eq!(ext.manifest.name, "rate-limiter");
        assert!(ext.enabled);

        reg.uninstall("rate-limiter").expect("uninstall");
        assert!(reg.list_installed().is_empty());
    }

    #[test]
    fn test_ext_registry_duplicate_install() {
        let reg = ExtensionRegistry::new(RegistryConfig::default());
        reg.install("rate-limiter", None).expect("install");
        let result = reg.install("rate-limiter", None);
        assert!(matches!(result, Err(ExtRegistryError::AlreadyInstalled(_))));
    }

    #[test]
    fn test_ext_registry_not_found() {
        let reg = ExtensionRegistry::new(RegistryConfig::default());
        let result = reg.install("nonexistent", None);
        assert!(matches!(result, Err(ExtRegistryError::NotFound(_))));
    }

    #[test]
    fn test_ext_registry_enable_disable() {
        let reg = ExtensionRegistry::new(RegistryConfig::default());
        reg.install("bloom-filter", None).expect("install");

        reg.disable("bloom-filter").expect("disable");
        let list = reg.list_installed();
        assert!(!list[0].enabled);

        reg.enable("bloom-filter").expect("enable");
        let list = reg.list_installed();
        assert!(list[0].enabled);
    }

    #[test]
    fn test_ext_registry_search() {
        let reg = ExtensionRegistry::new(RegistryConfig::default());
        let results = reg.search("rate");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "rate-limiter");

        let results = reg.search("json");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_ext_registry_info() {
        let reg = ExtensionRegistry::new(RegistryConfig::default());
        let info = reg.info("geo-fence").expect("info");
        assert!(!info.installed);

        reg.install("geo-fence", None).expect("install");
        let info = reg.info("geo-fence").expect("info");
        assert!(info.installed);
    }

    #[test]
    fn test_ext_registry_verify() {
        let reg = ExtensionRegistry::new(RegistryConfig::default());
        reg.install("rate-limiter", None).expect("install");
        let result = reg.verify("rate-limiter");
        assert!(result.checksum_valid);
        assert!(result.permissions_ok);
    }

    #[test]
    fn test_ext_registry_stats() {
        let reg = ExtensionRegistry::new(RegistryConfig::default());
        reg.install("rate-limiter", None).expect("install");
        reg.install("bloom-filter", None).expect("install");
        reg.disable("bloom-filter").expect("disable");

        let stats = reg.stats();
        assert_eq!(stats.installed, 2);
        assert_eq!(stats.active, 1);
    }

    #[test]
    fn test_ext_registry_max_installed() {
        let cfg = RegistryConfig {
            max_installed: 1,
            ..Default::default()
        };
        let reg = ExtensionRegistry::new(cfg);
        reg.install("rate-limiter", None).expect("install");
        let result = reg.install("bloom-filter", None);
        assert!(matches!(result, Err(ExtRegistryError::MaxInstalled(1))));
    }

    #[test]
    fn test_ext_registry_update() {
        let reg = ExtensionRegistry::new(RegistryConfig::default());
        reg.install("rate-limiter", Some("1.2.0")).expect("install");
        let updated = reg.update("rate-limiter").expect("update");
        assert_eq!(updated.manifest.name, "rate-limiter");
    }
}
