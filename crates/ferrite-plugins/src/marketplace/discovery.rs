//! Module discovery for the WASM marketplace.
//!
//! Provides local filesystem scanning and HTTP-based registry discovery
//! of WASM modules, as well as manifest parsing for `ferrite-plugin.toml`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::registry::{ModuleCategory, WasmModuleMetadata};

// ---------------------------------------------------------------------------
// Module manifest (ferrite-plugin.toml)
// ---------------------------------------------------------------------------

/// Parsed representation of a `ferrite-plugin.toml` manifest file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleManifest {
    /// Module metadata section.
    pub module: ModuleSection,
    /// Optional dependency list.
    #[serde(default)]
    pub dependencies: HashMap<String, String>,
}

/// The `[module]` section of a manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleSection {
    pub name: String,
    pub version: String,
    pub author: String,
    pub description: String,
    #[serde(default = "default_license")]
    pub license: String,
    #[serde(default)]
    pub categories: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub min_ferrite_version: String,
}

fn default_license() -> String {
    "Apache-2.0".to_string()
}

impl ModuleManifest {
    /// Parse a manifest from TOML text.
    pub fn from_toml(text: &str) -> Result<Self, DiscoveryError> {
        toml::from_str(text)
            .map_err(|e| DiscoveryError::ManifestParse(format!("invalid TOML: {}", e)))
    }

    /// Convert the manifest into [`WasmModuleMetadata`] given wasm binary size and checksum.
    pub fn into_metadata(self, size_bytes: u64, checksum: String) -> WasmModuleMetadata {
        let categories = self
            .module
            .categories
            .iter()
            .map(|c| match c.as_str() {
                "data-processing" => ModuleCategory::DataProcessing,
                "validation" => ModuleCategory::Validation,
                "transformation" => ModuleCategory::Transformation,
                "analytics" => ModuleCategory::Analytics,
                "security" => ModuleCategory::Security,
                other => ModuleCategory::Other(other.to_string()),
            })
            .collect();

        let dependencies = self
            .dependencies
            .into_iter()
            .map(|(name, version_req)| super::registry::ModuleDependency { name, version_req })
            .collect();

        WasmModuleMetadata {
            name: self.module.name,
            version: self.module.version,
            author: self.module.author,
            description: self.module.description,
            license: self.module.license,
            size_bytes,
            checksum,
            categories,
            tags: self.module.tags,
            downloads: 0,
            min_ferrite_version: self.module.min_ferrite_version,
            dependencies,
        }
    }
}

// ---------------------------------------------------------------------------
// Local filesystem discovery
// ---------------------------------------------------------------------------

/// A discovered module on the local filesystem.
#[derive(Debug, Clone)]
pub struct DiscoveredModule {
    /// Path to the `.wasm` binary.
    pub wasm_path: PathBuf,
    /// Parsed manifest (if a `ferrite-plugin.toml` was found next to the wasm).
    pub manifest: Option<ModuleManifest>,
    /// Size of the `.wasm` binary in bytes.
    pub size_bytes: u64,
}

/// Scan a directory for `.wasm` files and their optional manifests.
///
/// For each `.wasm` file found, the scanner looks for a sibling
/// `ferrite-plugin.toml` in the same directory.
pub fn discover_local(dir: &Path) -> Result<Vec<DiscoveredModule>, DiscoveryError> {
    if !dir.is_dir() {
        return Err(DiscoveryError::Io(format!(
            "not a directory: {}",
            dir.display()
        )));
    }

    let mut modules = Vec::new();

    let entries = std::fs::read_dir(dir)
        .map_err(|e| DiscoveryError::Io(format!("failed to read {}: {}", dir.display(), e)))?;

    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "wasm") {
            let size_bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

            // Look for manifest alongside the wasm file
            let manifest_path = path.with_file_name("ferrite-plugin.toml");
            let manifest = if manifest_path.exists() {
                std::fs::read_to_string(&manifest_path)
                    .ok()
                    .and_then(|text| ModuleManifest::from_toml(&text).ok())
            } else {
                None
            };

            modules.push(DiscoveredModule {
                wasm_path: path,
                manifest,
                size_bytes,
            });
        }
    }

    Ok(modules)
}

// ---------------------------------------------------------------------------
// HTTP registry discovery
// ---------------------------------------------------------------------------

/// A remote catalog entry returned from an HTTP registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteCatalogEntry {
    pub name: String,
    pub version: String,
    pub description: String,
    pub download_url: String,
    pub checksum: String,
    pub size_bytes: u64,
}

/// Fetches a module catalog from a remote HTTP registry.
///
/// This is a placeholder that defines the expected API. Real HTTP fetching
/// requires an async HTTP client at runtime.
pub fn fetch_remote_catalog(registry_url: &str) -> Result<Vec<RemoteCatalogEntry>, DiscoveryError> {
    if registry_url.is_empty() {
        return Err(DiscoveryError::Http("registry URL is empty".into()));
    }

    // In a real implementation this would perform an HTTP GET against
    // `{registry_url}/v1/modules` and deserialize the JSON response.
    // For now return an empty catalog.
    Ok(Vec::new())
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors produced during module discovery.
#[derive(Debug, Clone, thiserror::Error)]
pub enum DiscoveryError {
    #[error("I/O error: {0}")]
    Io(String),

    #[error("manifest parse error: {0}")]
    ManifestParse(String),

    #[error("HTTP error: {0}")]
    Http(String),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_MANIFEST: &str = r#"
[module]
name = "validate-email"
version = "1.0.0"
author = "Ferrite Labs"
description = "Email validation UDF"
license = "MIT"
categories = ["validation"]
tags = ["email", "validation"]
min_ferrite_version = "0.1.0"

[dependencies]
"#;

    #[test]
    fn test_parse_manifest() {
        let manifest = ModuleManifest::from_toml(SAMPLE_MANIFEST).expect("parse failed");
        assert_eq!(manifest.module.name, "validate-email");
        assert_eq!(manifest.module.version, "1.0.0");
        assert_eq!(manifest.module.categories, vec!["validation"]);
    }

    #[test]
    fn test_manifest_into_metadata() {
        let manifest = ModuleManifest::from_toml(SAMPLE_MANIFEST).expect("parse failed");
        let meta = manifest.into_metadata(2048, "deadbeef".to_string());
        assert_eq!(meta.name, "validate-email");
        assert_eq!(meta.size_bytes, 2048);
        assert_eq!(meta.checksum, "deadbeef");
        assert_eq!(meta.categories, vec![ModuleCategory::Validation]);
    }

    #[test]
    fn test_discover_local_nonexistent() {
        let result = discover_local(Path::new("/nonexistent/path"));
        assert!(result.is_err());
    }

    #[test]
    fn test_fetch_remote_catalog_empty_url() {
        let result = fetch_remote_catalog("");
        assert!(result.is_err());
    }

    #[test]
    fn test_fetch_remote_catalog_placeholder() {
        let result = fetch_remote_catalog("https://example.com");
        assert!(result.expect("should succeed").is_empty());
    }
}
