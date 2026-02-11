//! Schema registry for managing schema definitions

use super::{Schema, SchemaConfig};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Schema registry for storing and managing schema definitions
pub struct SchemaRegistry {
    /// Schemas indexed by name -> version -> schema
    schemas: DashMap<String, HashMap<u32, Schema>>,
    /// Current/latest version for each schema
    current_versions: DashMap<String, u32>,
    /// Key pattern to schema mapping
    pattern_mapping: DashMap<String, String>,
    /// Configuration
    config: SchemaConfig,
}

impl SchemaRegistry {
    /// Create a new schema registry
    pub fn new(config: SchemaConfig) -> Self {
        Self {
            schemas: DashMap::new(),
            current_versions: DashMap::new(),
            pattern_mapping: DashMap::new(),
            config,
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(SchemaConfig::default())
    }

    /// Register a new schema
    pub fn register(&self, schema: Schema) -> Result<(), SchemaRegistryError> {
        let name = schema.name.clone();
        let version = schema.version;

        // Check if this version already exists
        if let Some(existing) = self.schemas.get(&name) {
            if existing.contains_key(&version) {
                return Err(SchemaRegistryError::VersionExists {
                    name: name.clone(),
                    version,
                });
            }
        }

        // Update pattern mapping if present
        if let Some(ref pattern) = schema.key_pattern {
            self.pattern_mapping.insert(pattern.clone(), name.clone());
        }

        // Insert schema
        self.schemas
            .entry(name.clone())
            .or_default()
            .insert(version, schema);

        // Update current version if this is newer
        self.current_versions
            .entry(name)
            .and_modify(|v| {
                if version > *v {
                    *v = version;
                }
            })
            .or_insert(version);

        Ok(())
    }

    /// Get a schema by name and optional version
    pub fn get(&self, name: &str, version: Option<u32>) -> Option<Schema> {
        let schemas = self.schemas.get(name)?;
        let v = version.or_else(|| self.current_versions.get(name).map(|v| *v))?;
        schemas.get(&v).cloned()
    }

    /// Get the current/latest version of a schema
    pub fn get_current(&self, name: &str) -> Option<Schema> {
        self.get(name, None)
    }

    /// Get all versions of a schema
    pub fn get_all_versions(&self, name: &str) -> Vec<Schema> {
        self.schemas
            .get(name)
            .map(|s| s.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Get schema for a key based on pattern matching
    pub fn get_for_key(&self, key: &str) -> Option<Schema> {
        // Try to find a matching pattern
        for entry in self.pattern_mapping.iter() {
            if pattern_matches(entry.key(), key) {
                return self.get_current(entry.value());
            }
        }
        None
    }

    /// List all registered schemas
    pub fn list(&self) -> Vec<SchemaVersion> {
        self.current_versions
            .iter()
            .filter_map(|entry| {
                let name = entry.key().clone();
                let version = *entry.value();
                self.get(&name, Some(version)).map(|schema| SchemaVersion {
                    name,
                    version,
                    field_count: schema.field_count(),
                    deprecated: schema.deprecated,
                    key_pattern: schema.key_pattern,
                })
            })
            .collect()
    }

    /// Get the current version number for a schema
    pub fn current_version(&self, name: &str) -> Option<u32> {
        self.current_versions.get(name).map(|v| *v)
    }

    /// Set the current version for a schema
    pub fn set_current_version(&self, name: &str, version: u32) -> Result<(), SchemaRegistryError> {
        // Verify the version exists
        if let Some(schemas) = self.schemas.get(name) {
            if !schemas.contains_key(&version) {
                return Err(SchemaRegistryError::VersionNotFound {
                    name: name.to_string(),
                    version,
                });
            }
        } else {
            return Err(SchemaRegistryError::SchemaNotFound(name.to_string()));
        }

        self.current_versions.insert(name.to_string(), version);
        Ok(())
    }

    /// Delete a schema (all versions)
    pub fn delete(&self, name: &str) -> Result<(), SchemaRegistryError> {
        // Remove from schemas
        if self.schemas.remove(name).is_none() {
            return Err(SchemaRegistryError::SchemaNotFound(name.to_string()));
        }

        // Remove from current versions
        self.current_versions.remove(name);

        // Remove from pattern mapping
        self.pattern_mapping.retain(|_, v| v != name);

        Ok(())
    }

    /// Delete a specific version of a schema
    pub fn delete_version(&self, name: &str, version: u32) -> Result<(), SchemaRegistryError> {
        let mut schemas = self
            .schemas
            .get_mut(name)
            .ok_or_else(|| SchemaRegistryError::SchemaNotFound(name.to_string()))?;

        if schemas.remove(&version).is_none() {
            return Err(SchemaRegistryError::VersionNotFound {
                name: name.to_string(),
                version,
            });
        }

        // If we deleted the current version, update to the highest remaining
        if let Some(current) = self.current_versions.get(name) {
            if *current == version {
                if let Some(max_version) = schemas.keys().max() {
                    self.current_versions.insert(name.to_string(), *max_version);
                } else {
                    self.current_versions.remove(name);
                }
            }
        }

        Ok(())
    }

    /// Deprecate a schema
    pub fn deprecate(&self, name: &str, message: Option<&str>) -> Result<(), SchemaRegistryError> {
        let mut schemas = self
            .schemas
            .get_mut(name)
            .ok_or_else(|| SchemaRegistryError::SchemaNotFound(name.to_string()))?;

        let version = self
            .current_versions
            .get(name)
            .map(|v| *v)
            .ok_or_else(|| SchemaRegistryError::SchemaNotFound(name.to_string()))?;

        if let Some(schema) = schemas.get_mut(&version) {
            schema.deprecated = true;
            schema.deprecation_message = message.map(|s| s.to_string());
        }

        Ok(())
    }

    /// Save schemas to disk
    pub fn save(&self, path: &Path) -> Result<(), SchemaRegistryError> {
        let data = RegistryData {
            schemas: self
                .schemas
                .iter()
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect(),
            current_versions: self
                .current_versions
                .iter()
                .map(|e| (e.key().clone(), *e.value()))
                .collect(),
            pattern_mapping: self
                .pattern_mapping
                .iter()
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect(),
        };

        let json = serde_json::to_string_pretty(&data)
            .map_err(|e| SchemaRegistryError::Io(e.to_string()))?;

        std::fs::create_dir_all(path.parent().unwrap_or(Path::new(".")))
            .map_err(|e| SchemaRegistryError::Io(e.to_string()))?;

        std::fs::write(path, json).map_err(|e| SchemaRegistryError::Io(e.to_string()))?;

        Ok(())
    }

    /// Load schemas from disk
    pub fn load(&self, path: &Path) -> Result<(), SchemaRegistryError> {
        let json =
            std::fs::read_to_string(path).map_err(|e| SchemaRegistryError::Io(e.to_string()))?;

        let data: RegistryData =
            serde_json::from_str(&json).map_err(|e| SchemaRegistryError::Io(e.to_string()))?;

        // Clear existing data
        self.schemas.clear();
        self.current_versions.clear();
        self.pattern_mapping.clear();

        // Load data
        for (name, versions) in data.schemas {
            self.schemas.insert(name, versions);
        }
        for (name, version) in data.current_versions {
            self.current_versions.insert(name, version);
        }
        for (pattern, name) in data.pattern_mapping {
            self.pattern_mapping.insert(pattern, name);
        }

        Ok(())
    }

    /// Get schema count
    pub fn count(&self) -> usize {
        self.schemas.len()
    }

    /// Get total version count across all schemas
    pub fn version_count(&self) -> usize {
        self.schemas.iter().map(|e| e.value().len()).sum()
    }
}

/// Schema version summary
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemaVersion {
    /// Schema name
    pub name: String,
    /// Version number
    pub version: u32,
    /// Number of fields
    pub field_count: usize,
    /// Whether deprecated
    pub deprecated: bool,
    /// Key pattern if defined
    pub key_pattern: Option<String>,
}

/// Schema registry error
#[derive(Debug)]
pub enum SchemaRegistryError {
    /// Schema not found
    SchemaNotFound(String),
    /// Version not found
    VersionNotFound { name: String, version: u32 },
    /// Version already exists
    VersionExists { name: String, version: u32 },
    /// I/O error
    Io(String),
}

impl std::fmt::Display for SchemaRegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaRegistryError::SchemaNotFound(name) => {
                write!(f, "Schema '{}' not found", name)
            }
            SchemaRegistryError::VersionNotFound { name, version } => {
                write!(f, "Schema '{}' version {} not found", name, version)
            }
            SchemaRegistryError::VersionExists { name, version } => {
                write!(f, "Schema '{}' version {} already exists", name, version)
            }
            SchemaRegistryError::Io(msg) => write!(f, "I/O error: {}", msg),
        }
    }
}

impl std::error::Error for SchemaRegistryError {}

/// Serializable registry data
#[derive(Serialize, Deserialize)]
struct RegistryData {
    schemas: HashMap<String, HashMap<u32, Schema>>,
    current_versions: HashMap<String, u32>,
    pattern_mapping: HashMap<String, String>,
}

/// Check if a pattern matches a key
fn pattern_matches(pattern: &str, key: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    if let Some(prefix) = pattern.strip_suffix('*') {
        return key.starts_with(prefix);
    }

    if let Some(suffix) = pattern.strip_prefix('*') {
        return key.ends_with(suffix);
    }

    if pattern.contains('*') {
        // Simple glob matching
        let parts: Vec<&str> = pattern.split('*').collect();
        if parts.len() == 2 {
            return key.starts_with(parts[0]) && key.ends_with(parts[1]);
        }
    }

    pattern == key
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{FieldType, Schema};

    #[test]
    fn test_registry_register_and_get() {
        let registry = SchemaRegistry::with_defaults();

        let schema = Schema::builder("user")
            .version(1)
            .field("id", FieldType::String, true)
            .build();

        registry.register(schema).unwrap();

        let retrieved = registry.get_current("user").unwrap();
        assert_eq!(retrieved.name, "user");
        assert_eq!(retrieved.version, 1);
    }

    #[test]
    fn test_registry_multiple_versions() {
        let registry = SchemaRegistry::with_defaults();

        let v1 = Schema::builder("user").version(1).build();
        let v2 = Schema::builder("user").version(2).build();

        registry.register(v1).unwrap();
        registry.register(v2).unwrap();

        assert_eq!(registry.current_version("user"), Some(2));

        let s1 = registry.get("user", Some(1)).unwrap();
        assert_eq!(s1.version, 1);

        let s2 = registry.get("user", Some(2)).unwrap();
        assert_eq!(s2.version, 2);
    }

    #[test]
    fn test_registry_pattern_matching() {
        let registry = SchemaRegistry::with_defaults();

        let schema = Schema::builder("user")
            .version(1)
            .key_pattern("users:*")
            .build();

        registry.register(schema).unwrap();

        let matched = registry.get_for_key("users:123").unwrap();
        assert_eq!(matched.name, "user");

        assert!(registry.get_for_key("orders:456").is_none());
    }

    #[test]
    fn test_pattern_matches() {
        assert!(pattern_matches("users:*", "users:123"));
        assert!(pattern_matches("users:*", "users:"));
        assert!(!pattern_matches("users:*", "orders:123"));
        assert!(pattern_matches("*:events", "user:events"));
        assert!(pattern_matches("user:*:profile", "user:123:profile"));
        assert!(pattern_matches("*", "anything"));
    }

    #[test]
    fn test_registry_delete() {
        let registry = SchemaRegistry::with_defaults();

        let schema = Schema::builder("user").version(1).build();
        registry.register(schema).unwrap();

        assert!(registry.get_current("user").is_some());

        registry.delete("user").unwrap();

        assert!(registry.get_current("user").is_none());
    }

    #[test]
    fn test_registry_deprecate() {
        let registry = SchemaRegistry::with_defaults();

        let schema = Schema::builder("user").version(1).build();
        registry.register(schema).unwrap();

        registry
            .deprecate("user", Some("Use user_v2 instead"))
            .unwrap();

        let schema = registry.get_current("user").unwrap();
        assert!(schema.deprecated);
        assert_eq!(
            schema.deprecation_message,
            Some("Use user_v2 instead".to_string())
        );
    }
}
