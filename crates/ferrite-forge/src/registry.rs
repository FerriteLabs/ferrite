//! Thread-safe registry of loaded Forge modules with versioning support.

use crate::module::Module;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Module version info returned by `FN.SHOW` / `FN.VERSIONS`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ModuleVersion {
    pub name: String,
    pub version: String,
    pub is_default: bool,
    pub loaded_at_ms: u64,
    pub call_count: u64,
    pub signed_by: Option<String>,
}

/// Internal entry that pairs a module with versioning metadata.
#[derive(Debug)]
struct VersionEntry {
    module: Arc<Module>,
    version: String,
    is_default: bool,
    loaded_at_ms: u64,
    call_count: AtomicU64,
    signed_by: Option<String>,
}

impl VersionEntry {
    fn to_module_version(&self, name: &str) -> ModuleVersion {
        ModuleVersion {
            name: name.to_string(),
            version: self.version.clone(),
            is_default: self.is_default,
            loaded_at_ms: self.loaded_at_ms,
            call_count: self.call_count.load(Ordering::Relaxed),
            signed_by: self.signed_by.clone(),
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// In-memory registry indexed by module name.
///
/// Persistence and cross-replica replication are handled by the FN.LOAD /
/// FN.DROP command handlers in the top-level `ferrite` crate (see ADR-019
/// §Module storage, replication, and replica hydration).  This registry is
/// the per-process hot cache.
///
/// Supports versioned modules: `insert_versioned("name:v2", module)` stores
/// a module with version tag `v2`.  The default version is resolved by
/// `get()` and can be changed with `promote()`.
#[derive(Debug, Default)]
pub struct ModuleRegistry {
    modules: RwLock<HashMap<String, Arc<Module>>>,
    /// Versioned storage: outer key = base name, inner key = version tag.
    versions: RwLock<HashMap<String, Vec<VersionEntry>>>,
}

impl ModuleRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or replace a module.  Returns the previous value if any.
    pub fn insert(&self, module: Module) -> Option<Arc<Module>> {
        let name = module.meta.name.clone();
        let arc = Arc::new(module);

        // Also register as version "v1" (default) in the versioned store.
        let entry = VersionEntry {
            module: Arc::clone(&arc),
            version: "v1".to_string(),
            is_default: true,
            loaded_at_ms: now_ms(),
            call_count: AtomicU64::new(0),
            signed_by: None,
        };
        self.versions.write().insert(name.clone(), vec![entry]);

        self.modules.write().insert(name, arc)
    }

    /// Look up a module by name (returns the default version).
    pub fn get(&self, name: &str) -> Option<Arc<Module>> {
        self.modules.read().get(name).cloned()
    }

    /// Remove a module and all its versions.  Returns the removed default, if any.
    pub fn remove(&self, name: &str) -> Option<Arc<Module>> {
        self.versions.write().remove(name);
        self.modules.write().remove(name)
    }

    pub fn len(&self) -> usize {
        self.modules.read().len()
    }
    pub fn is_empty(&self) -> bool {
        self.modules.read().is_empty()
    }

    /// Snapshot of registered module names — useful for `FN.LIST`.
    pub fn names(&self) -> Vec<String> {
        let mut out: Vec<String> = self.modules.read().keys().cloned().collect();
        out.sort();
        out
    }

    // ── Versioning API ──────────────────────────────────────────────

    /// Parse "name:version" into (name, version).  Bare "name" defaults to "v1".
    fn parse_name_version(name_version: &str) -> (String, String) {
        if let Some((name, ver)) = name_version.rsplit_once(':') {
            if !ver.is_empty() {
                return (name.to_string(), ver.to_string());
            }
        }
        (name_version.to_string(), "v1".to_string())
    }

    /// Load a versioned module: `"name:v2"` stores under name with version v2.
    ///
    /// The first version loaded for a name becomes the default.  Subsequent
    /// versions are non-default until `promote()` is called.
    ///
    /// Returns the version tag that was assigned.
    pub fn insert_versioned(
        &self,
        name_version: &str,
        module: Module,
        signed_by: Option<String>,
    ) -> String {
        let (base_name, version) = Self::parse_name_version(name_version);
        let arc = Arc::new(module);

        let mut vers = self.versions.write();
        let entries = vers.entry(base_name.clone()).or_default();

        // Remove existing entry for the same version tag (replace).
        entries.retain(|e| e.version != version);

        let is_first = entries.is_empty();
        let entry = VersionEntry {
            module: Arc::clone(&arc),
            version: version.clone(),
            is_default: is_first,
            loaded_at_ms: now_ms(),
            call_count: AtomicU64::new(0),
            signed_by,
        };
        entries.push(entry);

        // If this is the first (or only) version, update the flat map.
        if is_first {
            self.modules.write().insert(base_name, arc);
        }

        version
    }

    /// Promote a specific version to be the default for a module name.
    pub fn promote(&self, name: &str, version: &str) -> bool {
        let mut vers = self.versions.write();
        let Some(entries) = vers.get_mut(name) else {
            return false;
        };
        let found = entries.iter().any(|e| e.version == version);
        if !found {
            return false;
        }
        let mut promoted_module = None;
        for entry in entries.iter_mut() {
            if entry.version == version {
                entry.is_default = true;
                promoted_module = Some(Arc::clone(&entry.module));
            } else {
                entry.is_default = false;
            }
        }
        if let Some(m) = promoted_module {
            self.modules.write().insert(name.to_string(), m);
        }
        true
    }

    /// Get info for the default version of a module (for `FN.SHOW`).
    pub fn show(&self, name: &str) -> Option<ModuleVersion> {
        let vers = self.versions.read();
        let entries = vers.get(name)?;
        entries
            .iter()
            .find(|e| e.is_default)
            .or(entries.first())
            .map(|e| e.to_module_version(name))
    }

    /// List all versions of a module (for `FN.VERSIONS`).
    pub fn versions(&self, name: &str) -> Vec<ModuleVersion> {
        let vers = self.versions.read();
        match vers.get(name) {
            Some(entries) => entries.iter().map(|e| e.to_module_version(name)).collect(),
            None => Vec::new(),
        }
    }

    /// Increment call count for the default version of a module.
    pub fn increment_call_count(&self, name: &str) {
        let vers = self.versions.read();
        if let Some(entries) = vers.get(name) {
            if let Some(entry) = entries.iter().find(|e| e.is_default) {
                entry.call_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("no such module: {0}")]
    NotFound(String),
    #[error("module already exists: {0}")]
    AlreadyExists(String),
    #[error("no such version: {0}:{1}")]
    VersionNotFound(String, String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::module::ModuleAcl;

    #[test]
    fn insert_get_remove_roundtrip() {
        let r = ModuleRegistry::new();
        assert!(r.is_empty());
        r.insert(Module::new("m1", vec![1, 2, 3], ModuleAcl::default()));
        assert_eq!(r.len(), 1);
        assert!(r.get("m1").is_some());
        assert!(r.get("missing").is_none());
        let removed = r.remove("m1").expect("present");
        assert_eq!(removed.meta.name, "m1");
        assert!(r.is_empty());
    }

    #[test]
    fn names_are_sorted() {
        let r = ModuleRegistry::new();
        r.insert(Module::new("z", vec![], ModuleAcl::default()));
        r.insert(Module::new("a", vec![], ModuleAcl::default()));
        r.insert(Module::new("m", vec![], ModuleAcl::default()));
        assert_eq!(r.names(), vec!["a".to_string(), "m".into(), "z".into()]);
    }

    #[test]
    fn insert_versioned_stores_with_version() {
        let r = ModuleRegistry::new();
        let v = r.insert_versioned(
            "myfn:v1",
            Module::new("myfn", vec![1, 2], ModuleAcl::default()),
            None,
        );
        assert_eq!(v, "v1");
        let v2 = r.insert_versioned(
            "myfn:v2",
            Module::new("myfn", vec![3, 4], ModuleAcl::default()),
            Some("alice".to_string()),
        );
        assert_eq!(v2, "v2");

        let versions = r.versions("myfn");
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].version, "v1");
        assert!(versions[0].is_default);
        assert_eq!(versions[1].version, "v2");
        assert!(!versions[1].is_default);
        assert_eq!(versions[1].signed_by, Some("alice".to_string()));
    }

    #[test]
    fn promote_changes_default() {
        let r = ModuleRegistry::new();
        r.insert_versioned(
            "myfn:v1",
            Module::new("myfn", vec![1], ModuleAcl::default()),
            None,
        );
        r.insert_versioned(
            "myfn:v2",
            Module::new("myfn", vec![2], ModuleAcl::default()),
            None,
        );

        // v1 is default initially.
        let info = r.show("myfn").unwrap();
        assert_eq!(info.version, "v1");
        assert!(info.is_default);

        // Promote v2.
        assert!(r.promote("myfn", "v2"));
        let info = r.show("myfn").unwrap();
        assert_eq!(info.version, "v2");
        assert!(info.is_default);

        // The default module bytes should reflect v2.
        let m = r.get("myfn").unwrap();
        assert_eq!(m.bytes, vec![2]);

        // Promoting a nonexistent version returns false.
        assert!(!r.promote("myfn", "v99"));
        // Promoting a nonexistent module returns false.
        assert!(!r.promote("nonexistent", "v1"));
    }

    #[test]
    fn show_returns_info() {
        let r = ModuleRegistry::new();
        r.insert(Module::new("showtest", vec![10, 20], ModuleAcl::default()));
        let info = r.show("showtest").unwrap();
        assert_eq!(info.name, "showtest");
        assert_eq!(info.version, "v1");
        assert!(info.is_default);
        assert!(info.loaded_at_ms > 0);
        assert_eq!(info.call_count, 0);

        assert!(r.show("nonexistent").is_none());
    }

    #[test]
    fn versions_lists_all() {
        let r = ModuleRegistry::new();
        r.insert_versioned(
            "multi:v1",
            Module::new("multi", vec![1], ModuleAcl::default()),
            None,
        );
        r.insert_versioned(
            "multi:v2",
            Module::new("multi", vec![2], ModuleAcl::default()),
            Some("bob".to_string()),
        );
        r.insert_versioned(
            "multi:v3",
            Module::new("multi", vec![3], ModuleAcl::default()),
            None,
        );

        let v = r.versions("multi");
        assert_eq!(v.len(), 3);
        let tags: Vec<&str> = v.iter().map(|mv| mv.version.as_str()).collect();
        assert_eq!(tags, vec!["v1", "v2", "v3"]);

        // Empty for nonexistent module.
        assert!(r.versions("nope").is_empty());
    }

    #[test]
    fn insert_versioned_bare_name_defaults_to_v1() {
        let r = ModuleRegistry::new();
        let v = r.insert_versioned(
            "bare",
            Module::new("bare", vec![1], ModuleAcl::default()),
            None,
        );
        assert_eq!(v, "v1");
        let info = r.show("bare").unwrap();
        assert_eq!(info.version, "v1");
    }

    #[test]
    fn increment_call_count_updates() {
        let r = ModuleRegistry::new();
        r.insert(Module::new("counter", vec![1], ModuleAcl::default()));
        r.increment_call_count("counter");
        r.increment_call_count("counter");
        let info = r.show("counter").unwrap();
        assert_eq!(info.call_count, 2);
    }
}
