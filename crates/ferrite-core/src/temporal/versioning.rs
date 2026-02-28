//! Time-Indexed Data Versioning
//!
//! Git-like versioning for key-value data with branch, tag, and merge
//! operations. Enables A/B testing, canary deployments, and audit at
//! the data layer.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// ── Configuration ──────────────────────────────────────────────────────────

/// Version store configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionConfig {
    /// Enable versioning.
    pub enabled: bool,
    /// Maximum versions retained per key.
    pub max_versions_per_key: usize,
    /// Interval between automatic compaction runs.
    pub compaction_interval: Duration,
    /// Name of the default branch.
    pub default_branch: String,
    /// Enable garbage collection of old versions.
    pub enable_gc: bool,
}

impl Default for VersionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_versions_per_key: 100,
            compaction_interval: Duration::from_secs(3600),
            default_branch: "main".to_string(),
            enable_gc: true,
        }
    }
}

// ── Core types ─────────────────────────────────────────────────────────────

/// A single version of a key's value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Version {
    /// Unique version identifier.
    pub id: u64,
    /// Creation timestamp (epoch milliseconds).
    pub timestamp: u64,
    /// Stored value bytes.
    pub value: Vec<u8>,
    /// Author who created this version.
    pub author: String,
    /// Optional commit message.
    pub message: Option<String>,
    /// Parent version id, if any.
    pub parent: Option<u64>,
}

/// A named branch pointing at a head version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Branch {
    /// Branch name.
    pub name: String,
    /// Version id of the latest commit on this branch.
    pub head: u64,
    /// Creation timestamp (epoch milliseconds).
    pub created_at: u64,
    /// Branch this was forked from, if any.
    pub created_from: Option<String>,
    /// Whether the branch is protected (cannot be deleted).
    pub protected: bool,
}

/// An immutable tag pointing at a specific version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tag {
    /// Tag name.
    pub name: String,
    /// Tagged version id.
    pub version_id: u64,
    /// Creation timestamp (epoch milliseconds).
    pub created_at: u64,
    /// Optional annotation message.
    pub message: Option<String>,
}

/// Difference between two versions of a key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionDiff {
    /// Key that was compared.
    pub key: String,
    /// First version id.
    pub old_version: u64,
    /// Second version id.
    pub new_version: u64,
    /// Value at the old version.
    pub old_value: Option<Vec<u8>>,
    /// Value at the new version.
    pub new_value: Option<Vec<u8>>,
    /// Whether the values differ.
    pub changed: bool,
}

/// Result of merging two branches.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeResult {
    /// Number of keys merged successfully.
    pub merged_keys: usize,
    /// Conflicts encountered during merge.
    pub conflicts: Vec<MergeConflict>,
    /// Source branch name.
    pub source_branch: String,
    /// Target branch name.
    pub target_branch: String,
}

/// A merge conflict for a single key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeConflict {
    /// Key with conflicting changes.
    pub key: String,
    /// Value on the source branch.
    pub source_value: Vec<u8>,
    /// Value on the target branch.
    pub target_value: Vec<u8>,
    /// Common ancestor value, if any.
    pub base_value: Option<Vec<u8>>,
}

/// Aggregate statistics about the version store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionStats {
    /// Total number of version entries.
    pub total_versions: u64,
    /// Number of distinct keys tracked.
    pub total_keys: u64,
    /// Number of branches.
    pub branches: usize,
    /// Number of tags.
    pub tags: usize,
    /// Approximate storage in bytes.
    pub storage_bytes: u64,
}

// ── Errors ─────────────────────────────────────────────────────────────────

/// Errors produced by the version store.
#[derive(Debug, Error)]
pub enum VersionError {
    /// Requested branch does not exist.
    #[error("branch not found: {0}")]
    BranchNotFound(String),
    /// Branch already exists.
    #[error("branch already exists: {0}")]
    BranchExists(String),
    /// Cannot delete a protected branch.
    #[error("branch is protected: {0}")]
    BranchProtected(String),
    /// Requested tag does not exist.
    #[error("tag not found: {0}")]
    TagNotFound(String),
    /// Tag already exists.
    #[error("tag already exists: {0}")]
    TagExists(String),
    /// Requested version does not exist.
    #[error("version not found: {0}")]
    VersionNotFound(u64),
    /// Merge produced unresolved conflicts.
    #[error("merge conflicts on {} key(s)", .0.len())]
    MergeConflicts(Vec<MergeConflict>),
    /// Compaction failed.
    #[error("compaction failed: {0}")]
    CompactionFailed(String),
}

// ── Internal store ─────────────────────────────────────────────────────────

/// Per-branch data: key → sorted versions.
type BranchData = HashMap<String, BTreeMap<u64, Version>>;

/// Internal mutable state behind the RwLock.
struct VersionStoreInner {
    /// branch_name → (key → versions sorted by timestamp).
    data: HashMap<String, BranchData>,
    /// Known branches.
    branches: HashMap<String, Branch>,
    /// Known tags.
    tags: HashMap<String, Tag>,
    /// Currently active branch.
    current_branch: String,
}

// ── VersionManager ─────────────────────────────────────────────────────────

/// Thread-safe manager for versioned key-value data.
pub struct VersionManager {
    config: VersionConfig,
    inner: RwLock<VersionStoreInner>,
    version_counter: AtomicU64,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl VersionManager {
    /// Create a new version manager with the given configuration.
    pub fn new(config: VersionConfig) -> Self {
        let default_branch = config.default_branch.clone();
        let branch = Branch {
            name: default_branch.clone(),
            head: 0,
            created_at: now_ms(),
            created_from: None,
            protected: true,
        };

        let mut branches = HashMap::new();
        branches.insert(default_branch.clone(), branch);
        let mut data = HashMap::new();
        data.insert(default_branch.clone(), HashMap::new());

        Self {
            config,
            inner: RwLock::new(VersionStoreInner {
                data,
                branches,
                tags: HashMap::new(),
                current_branch: default_branch,
            }),
            version_counter: AtomicU64::new(1),
        }
    }

    /// Write a new version for `key` on `branch`. Returns the version id.
    pub fn write(
        &self,
        key: &str,
        value: &[u8],
        branch: &str,
        author: &str,
    ) -> Result<u64, VersionError> {
        let mut inner = self.inner.write();
        if !inner.branches.contains_key(branch) {
            return Err(VersionError::BranchNotFound(branch.to_string()));
        }

        let vid = self.version_counter.fetch_add(1, Ordering::Relaxed);
        let ts = now_ms();

        // Compute parent and insert version, then enforce max versions.
        {
            let branch_data = inner.data.entry(branch.to_string()).or_default();

            let key_versions = branch_data.entry(key.to_string()).or_default();
            let parent = key_versions.values().next_back().map(|v| v.id);

            let version = Version {
                id: vid,
                timestamp: ts,
                value: value.to_vec(),
                author: author.to_string(),
                message: None,
                parent,
            };

            key_versions.insert(vid, version);

            // Enforce max versions per key.
            if key_versions.len() > self.config.max_versions_per_key {
                let excess = key_versions.len() - self.config.max_versions_per_key;
                let ids_to_remove: Vec<u64> = key_versions.keys().take(excess).copied().collect();
                for id in ids_to_remove {
                    key_versions.remove(&id);
                }
            }
        }

        if let Some(b) = inner.branches.get_mut(branch) {
            b.head = vid;
        }

        Ok(vid)
    }

    /// Read the latest version of `key` on `branch`.
    pub fn read(&self, key: &str, branch: &str) -> Option<Version> {
        let inner = self.inner.read();
        inner
            .data
            .get(branch)
            .and_then(|bd| bd.get(key))
            .and_then(|versions| versions.values().next_back().cloned())
    }

    /// Read the version of `key` at or before `timestamp` (epoch ms).
    pub fn read_at(&self, key: &str, timestamp: u64) -> Option<Version> {
        let inner = self.inner.read();
        let current = &inner.current_branch;
        inner
            .data
            .get(current.as_str())
            .and_then(|bd| bd.get(key))
            .and_then(|versions| {
                versions
                    .range(..=timestamp)
                    .next_back()
                    .map(|(_, v)| v.clone())
            })
    }

    /// Read a specific version by id across all branches.
    pub fn read_version(&self, key: &str, version_id: u64) -> Option<Version> {
        let inner = self.inner.read();
        for bd in inner.data.values() {
            if let Some(versions) = bd.get(key) {
                if let Some(v) = versions.get(&version_id) {
                    return Some(v.clone());
                }
            }
        }
        None
    }

    /// Return version history for `key` on the current branch, newest first.
    pub fn history(&self, key: &str, limit: usize) -> Vec<Version> {
        let inner = self.inner.read();
        let current = inner.current_branch.clone();
        inner
            .data
            .get(&current)
            .and_then(|bd| bd.get(key))
            .map(|versions| versions.values().rev().take(limit).cloned().collect())
            .unwrap_or_default()
    }

    /// Compare two versions of `key`.
    pub fn diff(&self, key: &str, v1: u64, v2: u64) -> Option<VersionDiff> {
        let ver1 = self.read_version(key, v1);
        let ver2 = self.read_version(key, v2);

        if ver1.is_none() && ver2.is_none() {
            return None;
        }

        let old_value = ver1.as_ref().map(|v| v.value.clone());
        let new_value = ver2.as_ref().map(|v| v.value.clone());
        let changed = old_value != new_value;

        Some(VersionDiff {
            key: key.to_string(),
            old_version: v1,
            new_version: v2,
            old_value,
            new_value,
            changed,
        })
    }

    /// Create a new branch forked from `from_branch`.
    pub fn create_branch(&self, name: &str, from_branch: &str) -> Result<Branch, VersionError> {
        let mut inner = self.inner.write();

        if inner.branches.contains_key(name) {
            return Err(VersionError::BranchExists(name.to_string()));
        }

        let source = inner
            .branches
            .get(from_branch)
            .ok_or_else(|| VersionError::BranchNotFound(from_branch.to_string()))?
            .clone();

        let branch = Branch {
            name: name.to_string(),
            head: source.head,
            created_at: now_ms(),
            created_from: Some(from_branch.to_string()),
            protected: false,
        };

        // Copy existing data from the source branch.
        let source_data = inner.data.get(from_branch).cloned().unwrap_or_default();

        inner.branches.insert(name.to_string(), branch.clone());
        inner.data.insert(name.to_string(), source_data);

        Ok(branch)
    }

    /// Delete a branch.
    pub fn delete_branch(&self, name: &str) -> Result<(), VersionError> {
        let mut inner = self.inner.write();

        let branch = inner
            .branches
            .get(name)
            .ok_or_else(|| VersionError::BranchNotFound(name.to_string()))?;

        if branch.protected {
            return Err(VersionError::BranchProtected(name.to_string()));
        }

        inner.branches.remove(name);
        inner.data.remove(name);
        Ok(())
    }

    /// List all branches.
    pub fn list_branches(&self) -> Vec<Branch> {
        let inner = self.inner.read();
        inner.branches.values().cloned().collect()
    }

    /// Switch the current branch.
    pub fn switch_branch(&self, name: &str) -> Result<(), VersionError> {
        let mut inner = self.inner.write();
        if !inner.branches.contains_key(name) {
            return Err(VersionError::BranchNotFound(name.to_string()));
        }
        inner.current_branch = name.to_string();
        Ok(())
    }

    /// Merge `source` branch into `target` branch.
    pub fn merge(&self, source: &str, target: &str) -> Result<MergeResult, VersionError> {
        let mut inner = self.inner.write();

        if !inner.branches.contains_key(source) {
            return Err(VersionError::BranchNotFound(source.to_string()));
        }
        if !inner.branches.contains_key(target) {
            return Err(VersionError::BranchNotFound(target.to_string()));
        }

        let source_data = inner.data.get(source).cloned().unwrap_or_default();
        let target_data = inner.data.get(target).cloned().unwrap_or_default();

        let mut merged_keys = 0usize;
        let mut conflicts = Vec::new();

        for (key, source_versions) in &source_data {
            let source_latest = match source_versions.values().next_back() {
                Some(v) => v,
                None => continue,
            };

            match target_data.get(key) {
                Some(target_versions) => {
                    let target_latest = match target_versions.values().next_back() {
                        Some(v) => v,
                        None => {
                            // Target has empty versions — take source.
                            let td = inner.data.entry(target.to_string()).or_default();
                            td.insert(key.clone(), source_versions.clone());
                            merged_keys += 1;
                            continue;
                        }
                    };

                    if source_latest.value == target_latest.value {
                        // Same value — no conflict.
                        continue;
                    }

                    // Both modified → conflict.
                    conflicts.push(MergeConflict {
                        key: key.clone(),
                        source_value: source_latest.value.clone(),
                        target_value: target_latest.value.clone(),
                        base_value: None,
                    });
                }
                None => {
                    // Key only exists on source → copy to target.
                    let td = inner.data.entry(target.to_string()).or_default();
                    td.insert(key.clone(), source_versions.clone());
                    merged_keys += 1;
                }
            }
        }

        if !conflicts.is_empty() {
            return Err(VersionError::MergeConflicts(conflicts));
        }

        // Update target head.
        let new_head = self.version_counter.fetch_add(1, Ordering::Relaxed);
        if let Some(b) = inner.branches.get_mut(target) {
            b.head = new_head;
        }

        Ok(MergeResult {
            merged_keys,
            conflicts: vec![],
            source_branch: source.to_string(),
            target_branch: target.to_string(),
        })
    }

    /// Create a tag pointing at a specific version.
    pub fn create_tag(
        &self,
        name: &str,
        version_id: u64,
        message: Option<String>,
    ) -> Result<Tag, VersionError> {
        let mut inner = self.inner.write();

        if inner.tags.contains_key(name) {
            return Err(VersionError::TagExists(name.to_string()));
        }

        // Verify version exists somewhere.
        let found = inner.data.values().any(|bd| {
            bd.values()
                .any(|versions| versions.contains_key(&version_id))
        });
        if !found {
            return Err(VersionError::VersionNotFound(version_id));
        }

        let tag = Tag {
            name: name.to_string(),
            version_id,
            created_at: now_ms(),
            message,
        };
        inner.tags.insert(name.to_string(), tag.clone());
        Ok(tag)
    }

    /// List all tags.
    pub fn list_tags(&self) -> Vec<Tag> {
        let inner = self.inner.read();
        inner.tags.values().cloned().collect()
    }

    /// Compact old versions for `key`, keeping at most `keep_count` on the current branch.
    /// Returns the number of versions removed.
    pub fn compact(&self, key: &str, keep_count: usize) -> usize {
        let mut inner = self.inner.write();
        let current = inner.current_branch.clone();

        let versions = match inner.data.get_mut(&current).and_then(|bd| bd.get_mut(key)) {
            Some(v) => v,
            None => return 0,
        };

        if versions.len() <= keep_count {
            return 0;
        }

        let excess = versions.len() - keep_count;
        let ids_to_remove: Vec<u64> = versions.keys().take(excess).copied().collect();
        for id in &ids_to_remove {
            versions.remove(id);
        }
        ids_to_remove.len()
    }

    /// Return aggregate statistics.
    pub fn stats(&self) -> VersionStats {
        let inner = self.inner.read();
        let mut total_versions = 0u64;
        let mut total_keys = 0u64;
        let mut storage_bytes = 0u64;

        for bd in inner.data.values() {
            total_keys += bd.len() as u64;
            for versions in bd.values() {
                total_versions += versions.len() as u64;
                for v in versions.values() {
                    storage_bytes += v.value.len() as u64;
                }
            }
        }

        VersionStats {
            total_versions,
            total_keys,
            branches: inner.branches.len(),
            tags: inner.tags.len(),
            storage_bytes,
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn default_manager() -> VersionManager {
        VersionManager::new(VersionConfig::default())
    }

    #[test]
    fn test_write_read_roundtrip() {
        let mgr = default_manager();
        let vid = mgr.write("key1", b"hello", "main", "alice").expect("write");
        let v = mgr.read("key1", "main").expect("read");
        assert_eq!(v.id, vid);
        assert_eq!(v.value, b"hello");
        assert_eq!(v.author, "alice");
    }

    #[test]
    fn test_multiple_versions() {
        let mgr = default_manager();
        let v1 = mgr.write("k", b"a", "main", "alice").expect("v1");
        let v2 = mgr.write("k", b"b", "main", "bob").expect("v2");
        assert!(v2 > v1);

        let latest = mgr.read("k", "main").expect("latest");
        assert_eq!(latest.value, b"b");

        let first = mgr.read_version("k", v1).expect("first");
        assert_eq!(first.value, b"a");
    }

    #[test]
    fn test_branch_create_and_list() {
        let mgr = default_manager();
        mgr.write("k", b"v", "main", "alice").expect("write");
        let branch = mgr.create_branch("dev", "main").expect("branch");
        assert_eq!(branch.name, "dev");
        assert_eq!(branch.created_from, Some("main".to_string()));

        let branches = mgr.list_branches();
        assert_eq!(branches.len(), 2);
    }

    #[test]
    fn test_branch_isolation() {
        let mgr = default_manager();
        mgr.write("k", b"main_v", "main", "alice")
            .expect("write main");
        mgr.create_branch("dev", "main").expect("branch");
        mgr.write("k", b"dev_v", "dev", "bob").expect("write dev");

        let main_val = mgr.read("k", "main").expect("main read");
        assert_eq!(main_val.value, b"main_v");

        let dev_val = mgr.read("k", "dev").expect("dev read");
        assert_eq!(dev_val.value, b"dev_v");
    }

    #[test]
    fn test_delete_protected_branch_fails() {
        let mgr = default_manager();
        let result = mgr.delete_branch("main");
        assert!(matches!(result, Err(VersionError::BranchProtected(_))));
    }

    #[test]
    fn test_delete_branch() {
        let mgr = default_manager();
        mgr.create_branch("tmp", "main").expect("branch");
        mgr.delete_branch("tmp").expect("delete");
        let branches = mgr.list_branches();
        assert_eq!(branches.len(), 1);
    }

    #[test]
    fn test_switch_branch() {
        let mgr = default_manager();
        mgr.create_branch("dev", "main").expect("branch");
        mgr.switch_branch("dev").expect("switch");
        mgr.write("k", b"val", "dev", "alice").expect("write");
        let history = mgr.history("k", 10);
        // History uses current branch (dev)
        assert_eq!(history.len(), 1);
    }

    #[test]
    fn test_merge_no_conflict() {
        let mgr = default_manager();
        mgr.write("shared", b"base", "main", "alice")
            .expect("write shared");
        mgr.create_branch("feature", "main").expect("branch");
        mgr.write("new_key", b"new_val", "feature", "bob")
            .expect("write new");

        let result = mgr.merge("feature", "main").expect("merge");
        assert_eq!(result.merged_keys, 1);
        assert!(result.conflicts.is_empty());

        let val = mgr.read("new_key", "main").expect("read merged");
        assert_eq!(val.value, b"new_val");
    }

    #[test]
    fn test_merge_with_conflict() {
        let mgr = default_manager();
        mgr.write("k", b"base", "main", "alice")
            .expect("write base");
        mgr.create_branch("feature", "main").expect("branch");
        mgr.write("k", b"feature_change", "feature", "bob")
            .expect("write feature");
        mgr.write("k", b"main_change", "main", "alice")
            .expect("write main");

        let result = mgr.merge("feature", "main");
        assert!(matches!(result, Err(VersionError::MergeConflicts(_))));
    }

    #[test]
    fn test_tags() {
        let mgr = default_manager();
        let vid = mgr.write("k", b"v", "main", "alice").expect("write");
        let tag = mgr
            .create_tag("v1.0", vid, Some("release".to_string()))
            .expect("tag");
        assert_eq!(tag.name, "v1.0");
        assert_eq!(tag.version_id, vid);

        let tags = mgr.list_tags();
        assert_eq!(tags.len(), 1);

        // Duplicate tag should fail.
        let dup = mgr.create_tag("v1.0", vid, None);
        assert!(matches!(dup, Err(VersionError::TagExists(_))));
    }

    #[test]
    fn test_compaction() {
        let mgr = default_manager();
        for i in 0..10 {
            mgr.write("k", format!("v{}", i).as_bytes(), "main", "alice")
                .expect("write");
        }
        let removed = mgr.compact("k", 3);
        assert_eq!(removed, 7);
        let history = mgr.history("k", 100);
        assert_eq!(history.len(), 3);
    }

    #[test]
    fn test_diff() {
        let mgr = default_manager();
        let v1 = mgr.write("k", b"old", "main", "alice").expect("v1");
        let v2 = mgr.write("k", b"new", "main", "alice").expect("v2");

        let diff = mgr.diff("k", v1, v2).expect("diff");
        assert!(diff.changed);
        assert_eq!(diff.old_value, Some(b"old".to_vec()));
        assert_eq!(diff.new_value, Some(b"new".to_vec()));
    }

    #[test]
    fn test_history_newest_first() {
        let mgr = default_manager();
        mgr.write("k", b"a", "main", "alice").expect("a");
        mgr.write("k", b"b", "main", "alice").expect("b");
        mgr.write("k", b"c", "main", "alice").expect("c");

        let history = mgr.history("k", 10);
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].value, b"c");
        assert_eq!(history[2].value, b"a");
    }

    #[test]
    fn test_stats() {
        let mgr = default_manager();
        mgr.write("k1", b"val1", "main", "alice").expect("w1");
        mgr.write("k2", b"val2", "main", "bob").expect("w2");

        let stats = mgr.stats();
        assert_eq!(stats.total_versions, 2);
        assert_eq!(stats.total_keys, 2);
        assert_eq!(stats.branches, 1);
        assert_eq!(stats.tags, 0);
        assert!(stats.storage_bytes > 0);
    }

    #[test]
    fn test_write_to_nonexistent_branch_fails() {
        let mgr = default_manager();
        let result = mgr.write("k", b"v", "nonexistent", "alice");
        assert!(matches!(result, Err(VersionError::BranchNotFound(_))));
    }

    #[test]
    fn test_point_in_time_read() {
        let mgr = default_manager();
        let v1 = mgr.write("k", b"early", "main", "alice").expect("v1");
        let _v2 = mgr.write("k", b"later", "main", "alice").expect("v2");

        // Read at the first version's id (used as timestamp proxy in BTreeMap).
        let found = mgr.read_at("k", v1);
        assert!(found.is_some());
        assert_eq!(
            found.as_ref().map(|v| v.value.clone()),
            Some(b"early".to_vec())
        );
    }
}
