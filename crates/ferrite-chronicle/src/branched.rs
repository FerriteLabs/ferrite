//! BranchedKv: a generic Store-like wrapper that adds copy-on-write branches.

use crate::registry::{BranchId, BranchRegistry, RegistryError};
use parking_lot::RwLock;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

/// History trace: each entry is `(branch_id, value_or_tombstone)`.
pub type BranchHistory = Vec<(String, Option<Vec<u8>>)>;

/// A timestamped overlay entry for time-travel support.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TimestampedEntry {
    pub value: Option<Vec<u8>>, // None = tombstone
    pub timestamp_ms: u64,
}

/// Retention policy for time-travel data.
#[derive(Clone, Copy, Debug)]
pub struct RetentionPolicy {
    pub max_entries_per_key: usize, // 0 = unlimited
    pub max_age_ms: u64,            // 0 = unlimited
}

/// Describes how a key differs between two branches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiffOp {
    /// Key exists in right but not left.
    Added,
    /// Key exists in left but not right.
    Removed,
    /// Key exists in both with different values.
    Modified,
}

/// One entry in the diff between two branches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffEntry {
    pub key: String,
    pub op: DiffOp,
    pub left: Option<Vec<u8>>,
    pub right: Option<Vec<u8>>,
}

/// Merge strategy when two branches have conflicting values for the same key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeStrategy {
    /// Last-write-wins: source branch values overwrite destination (default).
    Lww,
    /// Always prefer the source branch's value on conflict.
    PreferSrc,
    /// Always prefer the destination branch's value on conflict.
    PreferDst,
}

/// Minimal KV trait the wrapper needs from the underlying store.
/// Production deployments will impl this against the real `ferrite-core::Store`;
/// tests use [`InMemoryKv`].
pub trait BaseKv: Send + Sync + 'static {
    fn get(&self, key: &str) -> Option<Vec<u8>>;
    fn set(&self, key: &str, value: Vec<u8>);
    fn del(&self, key: &str) -> bool;
}

/// Reference implementation backed by parking_lot RwLock + HashMap.
#[derive(Debug, Default, Clone)]
pub struct InMemoryKv {
    inner: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl InMemoryKv {
    pub fn new() -> Self {
        Self::default()
    }

    /// Dump every (key, value) pair.  Used for snapshots.
    pub fn entries(&self) -> Vec<(String, Vec<u8>)> {
        self.inner
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Bulk-insert previously-dumped entries (overwrites existing keys).
    pub fn restore(&self, entries: Vec<(String, Vec<u8>)>) {
        let mut g = self.inner.write();
        for (k, v) in entries {
            g.insert(k, v);
        }
    }
}

impl BaseKv for InMemoryKv {
    fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.inner.read().get(key).cloned()
    }
    fn set(&self, key: &str, value: Vec<u8>) {
        self.inner.write().insert(key.into(), value);
    }
    fn del(&self, key: &str) -> bool {
        self.inner.write().remove(key).is_some()
    }
}

/// Per-branch overlay table.  Stores either a value (write-through) or a
/// tombstone marker (delete-through).
#[derive(Debug, Default)]
struct Overlay {
    entries: HashMap<String, Option<Vec<u8>>>, // None = tombstone
    writes: u64,
    deletes: u64,
    /// Stack of snapshots (HashMap of entries) taken via `snapshot`.
    snapshots: Vec<HashMap<String, Option<Vec<u8>>>>,
    /// Per-key timestamped write history for time-travel reads.
    history: HashMap<String, Vec<TimestampedEntry>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BranchStats {
    pub overlay_keys: usize,
    pub writes: u64,
    pub deletes: u64,
    pub snapshots: usize,
}

/// One key→value-or-tombstone overlay entry as dumped/replayed.
pub type OverlayEntry = (String, Option<Vec<u8>>);

/// Dump of one branch's overlay: the branch id and its entries.
pub type OverlayDump = (BranchId, Vec<OverlayEntry>);

/// Dump of one branch's time-travel history: `(branch_id, Vec<(key, entries)>)`.
pub type HistoryDump = (BranchId, Vec<(String, Vec<TimestampedEntry>)>);

/// Dump of one branch's overlay together with its [`BranchStats`].
pub type OverlayDumpWithStats = (BranchId, Vec<OverlayEntry>, BranchStats);

/// Dump of one branch's rollback-snapshot stack: `(branch, Vec<snapshot_entries>)`.
pub type SnapshotStackDump = (BranchId, Vec<Vec<OverlayEntry>>);

#[derive(Debug)]
pub struct BranchedKv<S: BaseKv> {
    base: S,
    registry: BranchRegistry,
    overlays: Arc<RwLock<HashMap<BranchId, Overlay>>>,
    /// Currently-active branch for this handle.  None = main.
    active: parking_lot::RwLock<Option<BranchId>>,
}

impl<S: BaseKv> BranchedKv<S> {
    pub fn new(base: S, registry: BranchRegistry) -> Self {
        Self {
            base,
            registry,
            overlays: Arc::default(),
            active: parking_lot::RwLock::new(None),
        }
    }

    pub fn registry(&self) -> &BranchRegistry {
        &self.registry
    }

    pub fn create_branch(
        &self,
        parent: Option<BranchId>,
        tenant: impl Into<String>,
    ) -> Result<BranchId, crate::registry::RegistryError> {
        let m = self.registry.create(parent, tenant, 0)?;
        self.overlays
            .write()
            .insert(m.id.clone(), Overlay::default());
        Ok(m.id)
    }

    pub fn use_branch(&self, b: Option<BranchId>) {
        *self.active.write() = b;
    }

    pub fn active(&self) -> Option<BranchId> {
        self.active.read().clone()
    }

    /// Read with branch semantics: walk the active branch's overlay
    /// chain (descendant → ancestor → main) until a hit.
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let active = self.active.read().clone();
        if let Some(branch) = active {
            let chain = self.registry.ancestry(&branch).ok()?;
            // Walk descendant-first: chain is [root, ..., branch], reverse.
            let overlays = self.overlays.read();
            for id in chain.iter().rev() {
                if let Some(o) = overlays.get(id) {
                    if let Some(slot) = o.entries.get(key) {
                        return slot.clone(); // None = tombstone => return None
                    }
                }
            }
        }
        self.base.get(key)
    }

    /// Write into the active branch's overlay (or main if no branch active).
    pub fn set(&self, key: &str, value: Vec<u8>) {
        let active = self.active.read().clone();
        if let Some(branch) = active {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let mut overlays = self.overlays.write();
            let o = overlays.entry(branch).or_default();
            o.history
                .entry(key.into())
                .or_default()
                .push(TimestampedEntry {
                    value: Some(value.clone()),
                    timestamp_ms: now_ms,
                });
            o.entries.insert(key.into(), Some(value));
            o.writes += 1;
            return;
        }
        self.base.set(key, value);
    }

    /// Write into the active branch's overlay with an explicit timestamp.
    pub fn set_with_timestamp(&self, key: &str, value: Vec<u8>, timestamp_ms: u64) {
        let active = self.active.read().clone();
        if let Some(branch) = active {
            let mut overlays = self.overlays.write();
            let o = overlays.entry(branch).or_default();
            o.history
                .entry(key.into())
                .or_default()
                .push(TimestampedEntry {
                    value: Some(value.clone()),
                    timestamp_ms,
                });
            o.entries.insert(key.into(), Some(value));
            o.writes += 1;
            return;
        }
        self.base.set(key, value);
    }

    pub fn del(&self, key: &str) -> bool {
        let active = self.active.read().clone();
        if let Some(branch) = active {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let mut overlays = self.overlays.write();
            let o = overlays.entry(branch).or_default();
            let existed = o.entries.contains_key(key) || self.base.get(key).is_some();
            o.history
                .entry(key.into())
                .or_default()
                .push(TimestampedEntry {
                    value: None,
                    timestamp_ms: now_ms,
                });
            o.entries.insert(key.into(), None);
            o.deletes += 1;
            return existed;
        }
        self.base.del(key)
    }

    /// Take a point-in-time snapshot of the active branch's overlay.
    /// Returns the snapshot index (the value passed to [`Self::rollback`]).
    pub fn snapshot(&self) -> Option<usize> {
        let active = self.active.read().clone()?;
        let mut overlays = self.overlays.write();
        let o = overlays.entry(active).or_default();
        o.snapshots.push(o.entries.clone());
        Some(o.snapshots.len() - 1)
    }

    /// Restore the active branch's overlay to a previously-taken snapshot.
    /// Snapshots taken after `index` are discarded.
    pub fn rollback(&self, index: usize) -> bool {
        let Some(active) = self.active.read().clone() else {
            return false;
        };
        let mut overlays = self.overlays.write();
        let Some(o) = overlays.get_mut(&active) else {
            return false;
        };
        if index >= o.snapshots.len() {
            return false;
        }
        o.entries = o.snapshots[index].clone();
        o.snapshots.truncate(index + 1);
        true
    }

    pub fn stats(&self, branch: &str) -> BranchStats {
        let overlays = self.overlays.read();
        overlays
            .get(branch)
            .map_or(BranchStats::default(), |o| BranchStats {
                overlay_keys: o.entries.len(),
                writes: o.writes,
                deletes: o.deletes,
                snapshots: o.snapshots.len(),
            })
    }

    /// Merge a branch's overlay into its parent (or main).  Conflicts
    /// (a key written on both sides) follow last-writer-wins from the
    /// merging branch.  Returns the count of keys merged.
    pub fn merge(&self, branch: &str) -> usize {
        self.merge_into(branch, None, MergeStrategy::Lww)
            .unwrap_or(0)
    }

    /// Merge `src` into `dst` (or its parent / main when `dst` is `None`).
    /// Returns the count of keys merged on success.
    pub fn merge_into(
        &self,
        src: &str,
        dst: Option<&str>,
        strategy: MergeStrategy,
    ) -> Result<usize, RegistryError> {
        let dst_id = match dst {
            Some(d) => {
                // Validate destination exists.
                self.registry
                    .get(d)
                    .ok_or_else(|| RegistryError::NotFound(d.into()))?;
                Some(d.to_string())
            }
            None => {
                // Default: immediate parent, or main if no parent.
                match self.registry.ancestry(src) {
                    Ok(c) if c.len() >= 2 => Some(c[c.len() - 2].clone()),
                    _ => None,
                }
            }
        };

        let mut overlays = self.overlays.write();
        let Some(src_overlay) = overlays.remove(src) else {
            return Err(RegistryError::NotFound(src.into()));
        };
        let n = src_overlay.entries.len();

        match dst_id {
            Some(did) => {
                let parent = overlays.entry(did).or_default();
                for (k, v) in src_overlay.entries {
                    match strategy {
                        MergeStrategy::PreferDst => {
                            parent.entries.entry(k).or_insert(v);
                        }
                        MergeStrategy::Lww | MergeStrategy::PreferSrc => {
                            parent.entries.insert(k, v);
                        }
                    }
                }
            }
            None => {
                // Merge into main.
                for (k, v) in src_overlay.entries {
                    match strategy {
                        MergeStrategy::PreferDst => {
                            if self.base.get(&k).is_none() {
                                match v {
                                    Some(value) => self.base.set(&k, value),
                                    None => {
                                        self.base.del(&k);
                                    }
                                }
                            }
                        }
                        MergeStrategy::Lww | MergeStrategy::PreferSrc => match v {
                            Some(value) => self.base.set(&k, value),
                            None => {
                                self.base.del(&k);
                            }
                        },
                    }
                }
            }
        }
        Ok(n)
    }

    /// Compare the overlays of two branches and return a sorted list of
    /// differences.  Only overlay-level entries are compared (values in the
    /// base store that are *not* overridden by either branch are excluded).
    pub fn diff(&self, left: &str, right: &str) -> Result<Vec<DiffEntry>, RegistryError> {
        // Validate both branches exist.
        self.registry
            .get(left)
            .ok_or_else(|| RegistryError::NotFound(left.into()))?;
        self.registry
            .get(right)
            .ok_or_else(|| RegistryError::NotFound(right.into()))?;

        let overlays = self.overlays.read();
        let empty: HashMap<String, Option<Vec<u8>>> = HashMap::new();
        let left_entries = overlays.get(left).map_or(&empty, |o| &o.entries);
        let right_entries = overlays.get(right).map_or(&empty, |o| &o.entries);

        let mut all_keys = BTreeSet::new();
        all_keys.extend(left_entries.keys().cloned());
        all_keys.extend(right_entries.keys().cloned());

        let mut result = Vec::new();
        for key in all_keys {
            let lv = left_entries.get(&key);
            let rv = right_entries.get(&key);
            match (lv, rv) {
                (Some(l), Some(r)) if l != r => {
                    result.push(DiffEntry {
                        key,
                        op: DiffOp::Modified,
                        left: l.clone(),
                        right: r.clone(),
                    });
                }
                (Some(_), Some(_)) => {} // identical
                (Some(l), None) => {
                    result.push(DiffEntry {
                        key,
                        op: DiffOp::Removed,
                        left: l.clone(),
                        right: None,
                    });
                }
                (None, Some(r)) => {
                    result.push(DiffEntry {
                        key,
                        op: DiffOp::Added,
                        left: None,
                        right: r.clone(),
                    });
                }
                (None, None) => {}
            }
        }
        Ok(result)
    }

    /// Trace a key through the ancestry chain of `branch`, returning
    /// `(branch_id, value_or_none)` for every branch that has an overlay
    /// entry for the key, plus the base value under `"main"`.
    pub fn history(&self, branch: &str, key: &str) -> Result<BranchHistory, RegistryError> {
        let chain = self.registry.ancestry(branch)?;
        let overlays = self.overlays.read();
        let mut result = Vec::new();

        // Walk root-first through ancestors.
        for id in &chain {
            if let Some(o) = overlays.get(id) {
                if let Some(slot) = o.entries.get(key) {
                    result.push((id.clone(), slot.clone()));
                }
            }
        }

        // Always append the base/main value at the end.
        result.push(("main".to_string(), self.base.get(key)));
        Ok(result)
    }

    /// Dump every branch's overlay as `(branch_id, entries)`.  `entries`
    /// preserves tombstones (`None`) so a [`Self::replay_overlay`] round-trip
    /// is faithful.  Snapshots/stats are *not* preserved.
    pub fn snapshot_overlays(&self) -> Vec<OverlayDump> {
        self.overlays
            .read()
            .iter()
            .map(|(id, o)| {
                let entries = o
                    .entries
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                (id.clone(), entries)
            })
            .collect()
    }

    /// Replay a previously-dumped overlay against `branch`, creating the
    /// overlay slot if missing.  Existing entries with the same key are
    /// overwritten.
    pub fn replay_overlay(&self, branch: BranchId, entries: Vec<(String, Option<Vec<u8>>)>) {
        let mut overlays = self.overlays.write();
        let o = overlays.entry(branch).or_default();
        for (k, v) in entries {
            o.entries.insert(k, v);
        }
    }

    /// Like [`Self::snapshot_overlays`] but also returns each branch's
    /// `BranchStats` (writes/deletes counters) so they can be persisted.
    pub fn snapshot_overlays_with_stats(&self) -> Vec<OverlayDumpWithStats> {
        self.overlays
            .read()
            .iter()
            .map(|(id, o)| {
                let entries = o
                    .entries
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let stats = BranchStats {
                    overlay_keys: o.entries.len(),
                    writes: o.writes,
                    deletes: o.deletes,
                    snapshots: o.snapshots.len(),
                };
                (id.clone(), entries, stats)
            })
            .collect()
    }

    /// Replay overlay entries and restore the stats counters for `branch`.
    /// Existing entries with the same key are overwritten and counters
    /// are *replaced* (not added) with the provided values.
    pub fn replay_overlay_with_stats(
        &self,
        branch: BranchId,
        entries: Vec<(String, Option<Vec<u8>>)>,
        stats: BranchStats,
    ) {
        let mut overlays = self.overlays.write();
        let o = overlays.entry(branch).or_default();
        for (k, v) in entries {
            o.entries.insert(k, v);
        }
        o.writes = stats.writes;
        o.deletes = stats.deletes;
    }

    // ── Time-travel (AS OF) reads ──────────────────────────────────

    /// Read a key as it existed at a specific timestamp.
    /// Walks the branch ancestry looking for the most recent entry <= timestamp.
    pub fn get_as_of(&self, key: &str, timestamp_ms: u64) -> Option<Vec<u8>> {
        let active = self.active.read().clone();
        if let Some(branch) = active {
            if let Ok(chain) = self.registry.ancestry(&branch) {
                let overlays = self.overlays.read();
                for id in chain.iter().rev() {
                    if let Some(o) = overlays.get(id) {
                        if let Some(entries) = o.history.get(key) {
                            if let Some(e) = entries
                                .iter()
                                .rev()
                                .find(|e| e.timestamp_ms <= timestamp_ms)
                            {
                                return e.value.clone();
                            }
                        }
                    }
                }
            }
        }
        self.base.get(key)
    }

    /// Get the write history of a key on the active branch.
    /// Returns entries in reverse chronological order.
    pub fn key_history(&self, key: &str, limit: usize) -> Vec<TimestampedEntry> {
        let active = self.active.read().clone();
        if let Some(branch) = active {
            let overlays = self.overlays.read();
            if let Some(o) = overlays.get(&branch) {
                if let Some(entries) = o.history.get(key) {
                    let mut result: Vec<TimestampedEntry> = entries.clone();
                    result.reverse();
                    if limit > 0 {
                        result.truncate(limit);
                    }
                    return result;
                }
            }
        }
        Vec::new()
    }

    /// Apply retention policy, pruning old history entries across all branches.
    /// Returns the number of entries pruned.
    pub fn apply_retention(&self, policy: &RetentionPolicy, now_ms: u64) -> usize {
        let mut overlays = self.overlays.write();
        let mut pruned = 0;
        for o in overlays.values_mut() {
            for entries in o.history.values_mut() {
                let before = entries.len();
                if policy.max_age_ms > 0 {
                    entries.retain(|e| now_ms.saturating_sub(e.timestamp_ms) <= policy.max_age_ms);
                }
                if policy.max_entries_per_key > 0 && entries.len() > policy.max_entries_per_key {
                    let excess = entries.len() - policy.max_entries_per_key;
                    entries.drain(..excess);
                }
                pruned += before - entries.len();
            }
        }
        pruned
    }

    /// Dump every branch's time-travel history.
    pub fn snapshot_history(&self) -> Vec<HistoryDump> {
        self.overlays
            .read()
            .iter()
            .map(|(id, o)| {
                let hist: Vec<(String, Vec<TimestampedEntry>)> = o
                    .history
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                (id.clone(), hist)
            })
            .collect()
    }

    /// Replay previously-dumped time-travel history for a branch.
    pub fn replay_history(&self, branch: BranchId, entries: Vec<(String, Vec<TimestampedEntry>)>) {
        let mut overlays = self.overlays.write();
        let o = overlays.entry(branch).or_default();
        for (k, v) in entries {
            o.history.entry(k).or_default().extend(v);
        }
    }

    /// Dump every branch's rollback-snapshot stack as
    /// `(branch_id, Vec<snapshot_entries>)`.  The returned snapshot
    /// stacks preserve insertion order so [`Self::replay_snapshots`]
    /// produces a faithful round-trip.
    pub fn snapshot_stacks(&self) -> Vec<SnapshotStackDump> {
        self.overlays
            .read()
            .iter()
            .map(|(id, o)| {
                let stacks: Vec<Vec<OverlayEntry>> = o
                    .snapshots
                    .iter()
                    .map(|snap| snap.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                    .collect();
                (id.clone(), stacks)
            })
            .collect()
    }

    /// Replace `branch`'s rollback-snapshot stack with `stack`.  The
    /// overlay slot is created if missing so the call is safe even
    /// before any writes.
    pub fn replay_snapshots(&self, branch: BranchId, stack: Vec<Vec<OverlayEntry>>) {
        let mut overlays = self.overlays.write();
        let o = overlays.entry(branch).or_default();
        o.snapshots = stack
            .into_iter()
            .map(|entries| entries.into_iter().collect())
            .collect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_falls_through_to_base_when_no_branch() {
        let base = InMemoryKv::new();
        base.set("k", b"v".to_vec());
        let bk = BranchedKv::new(base, BranchRegistry::new());
        assert_eq!(bk.get("k"), Some(b"v".to_vec()));
    }

    #[test]
    fn write_on_branch_does_not_affect_main() {
        let base = InMemoryKv::new();
        base.set("k", b"main".to_vec());
        let bk = BranchedKv::new(base.clone(), BranchRegistry::new());
        let b = bk.create_branch(None, "t").unwrap();
        bk.use_branch(Some(b));
        bk.set("k", b"branch".to_vec());
        assert_eq!(bk.get("k"), Some(b"branch".to_vec()));
        bk.use_branch(None);
        assert_eq!(bk.get("k"), Some(b"main".to_vec()));
        assert_eq!(base.get("k"), Some(b"main".to_vec()));
    }

    #[test]
    fn delete_on_branch_tombstones_then_falls_through_to_main_after_drop() {
        let base = InMemoryKv::new();
        base.set("k", b"main".to_vec());
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let b = bk.create_branch(None, "t").unwrap();
        bk.use_branch(Some(b));
        assert!(bk.del("k"));
        assert_eq!(bk.get("k"), None);
        bk.use_branch(None);
        assert_eq!(bk.get("k"), Some(b"main".to_vec()));
    }

    #[test]
    fn nested_branch_inherits_parent_overlay() {
        let base = InMemoryKv::new();
        base.set("k", b"main".to_vec());
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let p = bk.create_branch(None, "t").unwrap();
        bk.use_branch(Some(p.clone()));
        bk.set("k", b"parent".to_vec());
        let c = bk.create_branch(Some(p), "t").unwrap();
        bk.use_branch(Some(c));
        // Child should see parent's overlay.
        assert_eq!(bk.get("k"), Some(b"parent".to_vec()));
        bk.set("k", b"child".to_vec());
        assert_eq!(bk.get("k"), Some(b"child".to_vec()));
    }

    #[test]
    fn merge_to_main_promotes_writes_and_deletes_overlay() {
        let base = InMemoryKv::new();
        base.set("k", b"main".to_vec());
        let bk = BranchedKv::new(base.clone(), BranchRegistry::new());
        let b = bk.create_branch(None, "t").unwrap();
        bk.use_branch(Some(b.clone()));
        bk.set("k", b"branch".to_vec());
        bk.set("new", b"yes".to_vec());
        bk.use_branch(None);
        let merged = bk.merge(&b);
        assert_eq!(merged, 2);
        assert_eq!(base.get("k"), Some(b"branch".to_vec()));
        assert_eq!(base.get("new"), Some(b"yes".to_vec()));
    }

    #[test]
    fn merge_promotes_tombstone_to_main_delete() {
        let base = InMemoryKv::new();
        base.set("k", b"main".to_vec());
        let bk = BranchedKv::new(base.clone(), BranchRegistry::new());
        let b = bk.create_branch(None, "t").unwrap();
        bk.use_branch(Some(b.clone()));
        bk.del("k");
        bk.use_branch(None);
        bk.merge(&b);
        assert_eq!(base.get("k"), None);
    }

    #[test]
    fn merge_into_parent_branch_not_main() {
        let base = InMemoryKv::new();
        base.set("k", b"main".to_vec());
        let bk = BranchedKv::new(base.clone(), BranchRegistry::new());
        let p = bk.create_branch(None, "t").unwrap();
        let c = bk.create_branch(Some(p.clone()), "t").unwrap();
        bk.use_branch(Some(c.clone()));
        bk.set("k", b"child".to_vec());
        bk.use_branch(Some(p.clone()));
        bk.merge(&c);
        // Child overlay was promoted to parent, NOT to main.
        assert_eq!(bk.get("k"), Some(b"child".to_vec()));
        assert_eq!(base.get("k"), Some(b"main".to_vec()));
    }

    #[test]
    fn snapshot_then_rollback_restores_overlay() {
        let base = InMemoryKv::new();
        base.set("k", b"main".to_vec());
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let b = bk.create_branch(None, "t").unwrap();
        bk.use_branch(Some(b.clone()));
        bk.set("k", b"v1".to_vec());
        let snap = bk.snapshot().unwrap();
        bk.set("k", b"v2".to_vec());
        bk.set("new", b"x".to_vec());
        assert!(bk.rollback(snap));
        assert_eq!(bk.get("k"), Some(b"v1".to_vec()));
        assert_eq!(bk.get("new"), None);
    }

    #[test]
    fn rollback_with_invalid_index_is_noop() {
        let base = InMemoryKv::new();
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let b = bk.create_branch(None, "t").unwrap();
        bk.use_branch(Some(b));
        bk.set("k", b"v".to_vec());
        assert!(!bk.rollback(99));
        assert_eq!(bk.get("k"), Some(b"v".to_vec()));
    }

    #[test]
    fn stats_count_writes_and_deletes() {
        let base = InMemoryKv::new();
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let b = bk.create_branch(None, "t").unwrap();
        bk.use_branch(Some(b.clone()));
        bk.set("k1", b"v".to_vec());
        bk.set("k2", b"v".to_vec());
        bk.del("k1");
        let snap = bk.snapshot().unwrap();
        assert_eq!(snap, 0);
        let s = bk.stats(&b);
        assert_eq!(s.writes, 2);
        assert_eq!(s.deletes, 1);
        assert_eq!(s.snapshots, 1);
        assert_eq!(s.overlay_keys, 2); // k1 (tombstone) + k2
    }

    #[test]
    fn snapshot_and_replay_overlays_round_trip() {
        let base = InMemoryKv::new();
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let b = bk.create_branch(None, "t").unwrap();
        bk.use_branch(Some(b.clone()));
        bk.set("a", b"1".to_vec());
        bk.set("b", b"2".to_vec());
        bk.del("a");

        let dump = bk.snapshot_overlays();
        assert_eq!(dump.len(), 1);

        // Build a fresh BranchedKv and replay.
        let base2 = InMemoryKv::new();
        let bk2 = BranchedKv::new(base2, BranchRegistry::new());
        for meta in bk.registry().all() {
            bk2.registry().restore(meta);
        }
        for (id, entries) in dump {
            bk2.replay_overlay(id, entries);
        }
        bk2.use_branch(Some(b));
        assert_eq!(bk2.get("a"), None); // tombstone preserved
        assert_eq!(bk2.get("b"), Some(b"2".to_vec()));
    }

    #[test]
    fn in_memory_kv_entries_round_trip() {
        let kv = InMemoryKv::new();
        kv.set("x", b"1".to_vec());
        kv.set("y", b"2".to_vec());
        let dump = kv.entries();
        let kv2 = InMemoryKv::new();
        kv2.restore(dump);
        assert_eq!(kv2.get("x"), Some(b"1".to_vec()));
        assert_eq!(kv2.get("y"), Some(b"2".to_vec()));
    }

    #[test]
    fn snapshot_overlays_with_stats_round_trip() {
        let base = InMemoryKv::new();
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let b = bk.create_branch(None, "t").unwrap();
        bk.use_branch(Some(b.clone()));
        bk.set("a", b"1".to_vec());
        bk.set("b", b"2".to_vec());
        bk.del("a");
        let original = bk.stats(&b);
        assert_eq!(original.writes, 2);
        assert_eq!(original.deletes, 1);

        let dump = bk.snapshot_overlays_with_stats();
        let base2 = InMemoryKv::new();
        let bk2 = BranchedKv::new(base2, BranchRegistry::new());
        for meta in bk.registry().all() {
            bk2.registry().restore(meta);
        }
        for (id, entries, stats) in dump {
            bk2.replay_overlay_with_stats(id, entries, stats);
        }
        let restored = bk2.stats(&b);
        assert_eq!(restored.writes, 2);
        assert_eq!(restored.deletes, 1);
        assert_eq!(restored.overlay_keys, 2);
    }

    #[test]
    fn snapshot_stacks_round_trip() {
        let base = InMemoryKv::new();
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let b = bk.create_branch(None, "snap-test").unwrap();
        bk.use_branch(Some(b.clone()));
        bk.set("k", b"v0".to_vec());
        let s0 = bk.snapshot().unwrap();
        bk.set("k", b"v1".to_vec());
        let _s1 = bk.snapshot().unwrap();
        bk.set("k", b"v2".to_vec());
        assert_eq!(bk.stats(&b).snapshots, 2);

        let dump = bk.snapshot_stacks();
        assert_eq!(dump.len(), 1);
        assert_eq!(dump[0].1.len(), 2);

        let base2 = InMemoryKv::new();
        let bk2 = BranchedKv::new(base2, BranchRegistry::new());
        for meta in bk.registry().all() {
            bk2.registry().restore(meta);
        }
        for (id, entries, stats) in bk.snapshot_overlays_with_stats() {
            bk2.replay_overlay_with_stats(id, entries, stats);
        }
        for (id, stack) in dump {
            bk2.replay_snapshots(id, stack);
        }
        bk2.use_branch(Some(b.clone()));
        // Rolling back to s0 must restore the exact value we had then.
        assert!(bk2.rollback(s0));
        assert_eq!(bk2.get("k"), Some(b"v0".to_vec()));
    }

    #[test]
    fn diff_added_removed_modified() {
        let base = InMemoryKv::new();
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let left = bk.create_branch(None, "t").unwrap();
        let right = bk.create_branch(None, "t").unwrap();

        bk.use_branch(Some(left.clone()));
        bk.set("shared", b"left-val".to_vec());
        bk.set("only-left", b"L".to_vec());

        bk.use_branch(Some(right.clone()));
        bk.set("shared", b"right-val".to_vec());
        bk.set("only-right", b"R".to_vec());

        let diff = bk.diff(&left, &right).unwrap();
        assert_eq!(diff.len(), 3);

        let by_key: HashMap<&str, &DiffEntry> = diff.iter().map(|e| (e.key.as_str(), e)).collect();

        let only_left = by_key["only-left"];
        assert_eq!(only_left.op, DiffOp::Removed);
        assert_eq!(only_left.left, Some(b"L".to_vec()));
        assert!(only_left.right.is_none());

        let only_right = by_key["only-right"];
        assert_eq!(only_right.op, DiffOp::Added);
        assert!(only_right.left.is_none());
        assert_eq!(only_right.right, Some(b"R".to_vec()));

        let shared = by_key["shared"];
        assert_eq!(shared.op, DiffOp::Modified);
        assert_eq!(shared.left, Some(b"left-val".to_vec()));
        assert_eq!(shared.right, Some(b"right-val".to_vec()));
    }

    #[test]
    fn diff_identical_branches_is_empty() {
        let base = InMemoryKv::new();
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let a = bk.create_branch(None, "t").unwrap();
        let b = bk.create_branch(None, "t").unwrap();

        bk.use_branch(Some(a.clone()));
        bk.set("k", b"v".to_vec());
        bk.use_branch(Some(b.clone()));
        bk.set("k", b"v".to_vec());

        let diff = bk.diff(&a, &b).unwrap();
        assert!(diff.is_empty());
    }

    #[test]
    fn diff_with_tombstones() {
        let base = InMemoryKv::new();
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let left = bk.create_branch(None, "t").unwrap();
        let right = bk.create_branch(None, "t").unwrap();

        bk.use_branch(Some(left.clone()));
        bk.set("k", b"alive".to_vec());
        bk.use_branch(Some(right.clone()));
        bk.set("k", b"temp".to_vec());
        bk.del("k"); // tombstone

        let diff = bk.diff(&left, &right).unwrap();
        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].op, DiffOp::Modified);
        assert_eq!(diff[0].left, Some(b"alive".to_vec()));
        assert_eq!(diff[0].right, None); // tombstone
    }

    #[test]
    fn diff_nonexistent_branch_errors() {
        let base = InMemoryKv::new();
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let a = bk.create_branch(None, "t").unwrap();
        assert!(bk.diff(&a, "nonexistent").is_err());
        assert!(bk.diff("nonexistent", &a).is_err());
    }

    #[test]
    fn merge_with_prefer_src_strategy() {
        let base = InMemoryKv::new();
        base.set("k", b"main".to_vec());
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let parent = bk.create_branch(None, "t").unwrap();
        let child = bk.create_branch(Some(parent.clone()), "t").unwrap();

        bk.use_branch(Some(parent.clone()));
        bk.set("conflict", b"parent-val".to_vec());
        bk.use_branch(Some(child.clone()));
        bk.set("conflict", b"child-val".to_vec());
        bk.set("new", b"child-only".to_vec());

        let n = bk
            .merge_into(&child, Some(&parent), MergeStrategy::PreferSrc)
            .unwrap();
        assert_eq!(n, 2);
        bk.use_branch(Some(parent.clone()));
        assert_eq!(bk.get("conflict"), Some(b"child-val".to_vec()));
        assert_eq!(bk.get("new"), Some(b"child-only".to_vec()));
    }

    #[test]
    fn merge_with_prefer_dst_strategy() {
        let base = InMemoryKv::new();
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let parent = bk.create_branch(None, "t").unwrap();
        let child = bk.create_branch(Some(parent.clone()), "t").unwrap();

        bk.use_branch(Some(parent.clone()));
        bk.set("conflict", b"parent-val".to_vec());
        bk.use_branch(Some(child.clone()));
        bk.set("conflict", b"child-val".to_vec());
        bk.set("new", b"child-only".to_vec());

        let n = bk
            .merge_into(&child, Some(&parent), MergeStrategy::PreferDst)
            .unwrap();
        assert_eq!(n, 2);
        bk.use_branch(Some(parent.clone()));
        // Conflict should keep the parent's value.
        assert_eq!(bk.get("conflict"), Some(b"parent-val".to_vec()));
        // Non-conflicting key merges through.
        assert_eq!(bk.get("new"), Some(b"child-only".to_vec()));
    }

    #[test]
    fn merge_into_explicit_destination() {
        let base = InMemoryKv::new();
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let a = bk.create_branch(None, "t").unwrap();
        let b_id = bk.create_branch(None, "t").unwrap();

        bk.use_branch(Some(a.clone()));
        bk.set("k", b"from-a".to_vec());

        let n = bk.merge_into(&a, Some(&b_id), MergeStrategy::Lww).unwrap();
        assert_eq!(n, 1);
        bk.use_branch(Some(b_id));
        assert_eq!(bk.get("k"), Some(b"from-a".to_vec()));
    }

    #[test]
    fn merge_nonexistent_source_errors() {
        let base = InMemoryKv::new();
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let result = bk.merge_into("nonexistent", None, MergeStrategy::Lww);
        assert!(result.is_err());
    }

    #[test]
    fn history_traces_key_through_ancestry() {
        let base = InMemoryKv::new();
        base.set("k", b"main-val".to_vec());
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let parent = bk.create_branch(None, "t").unwrap();
        let child = bk.create_branch(Some(parent.clone()), "t").unwrap();

        bk.use_branch(Some(parent.clone()));
        bk.set("k", b"parent-val".to_vec());
        bk.use_branch(Some(child.clone()));
        bk.set("k", b"child-val".to_vec());

        let hist = bk.history(&child, "k").unwrap();
        // Should have: parent entry, child entry, main entry.
        assert_eq!(hist.len(), 3);
        assert_eq!(hist[0].0, parent);
        assert_eq!(hist[0].1, Some(b"parent-val".to_vec()));
        assert_eq!(hist[1].0, child);
        assert_eq!(hist[1].1, Some(b"child-val".to_vec()));
        assert_eq!(hist[2].0, "main");
        assert_eq!(hist[2].1, Some(b"main-val".to_vec()));
    }

    #[test]
    fn history_key_not_in_overlay_shows_main_only() {
        let base = InMemoryKv::new();
        base.set("k", b"main-val".to_vec());
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let b = bk.create_branch(None, "t").unwrap();

        let hist = bk.history(&b, "k").unwrap();
        assert_eq!(hist.len(), 1);
        assert_eq!(hist[0].0, "main");
        assert_eq!(hist[0].1, Some(b"main-val".to_vec()));
    }

    #[test]
    fn save_load_round_trip_after_merge() {
        let base = InMemoryKv::new();
        base.set("k", b"main".to_vec());
        let bk = BranchedKv::new(base.clone(), BranchRegistry::new());
        let b = bk.create_branch(None, "t").unwrap();

        bk.use_branch(Some(b.clone()));
        bk.set("k", b"branch".to_vec());
        bk.set("new", b"added".to_vec());
        bk.use_branch(None);
        bk.merge(&b);

        // Snapshot and replay to a new BranchedKv.
        let dump = bk.snapshot_overlays_with_stats();
        let base2 = InMemoryKv::new();
        base2.restore(base.entries());
        let bk2 = BranchedKv::new(base2.clone(), BranchRegistry::new());
        for meta in bk.registry().all() {
            bk2.registry().restore(meta);
        }
        for (id, entries, stats) in dump {
            bk2.replay_overlay_with_stats(id, entries, stats);
        }
        // The merged values must be in main.
        assert_eq!(base2.get("k"), Some(b"branch".to_vec()));
        assert_eq!(base2.get("new"), Some(b"added".to_vec()));
    }

    // ── Time-travel (P3) tests ─────────────────────────────────────

    #[test]
    fn set_with_timestamp_records_history() {
        let base = InMemoryKv::new();
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let b = bk.create_branch(None, "t").unwrap();
        bk.use_branch(Some(b));
        bk.set_with_timestamp("k", b"v1".to_vec(), 1000);
        bk.set_with_timestamp("k", b"v2".to_vec(), 2000);
        let hist = bk.key_history("k", 0);
        assert_eq!(hist.len(), 2);
        assert_eq!(hist[0].timestamp_ms, 2000);
        assert_eq!(hist[0].value, Some(b"v2".to_vec()));
        assert_eq!(hist[1].timestamp_ms, 1000);
        assert_eq!(hist[1].value, Some(b"v1".to_vec()));
    }

    #[test]
    fn get_as_of_returns_correct_value_at_different_timestamps() {
        let base = InMemoryKv::new();
        base.set("k", b"main".to_vec());
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let b = bk.create_branch(None, "t").unwrap();
        bk.use_branch(Some(b));
        bk.set_with_timestamp("k", b"v1".to_vec(), 1000);
        bk.set_with_timestamp("k", b"v2".to_vec(), 2000);
        bk.set_with_timestamp("k", b"v3".to_vec(), 3000);

        assert_eq!(bk.get_as_of("k", 3000), Some(b"v3".to_vec()));
        assert_eq!(bk.get_as_of("k", 2500), Some(b"v2".to_vec()));
        assert_eq!(bk.get_as_of("k", 2000), Some(b"v2".to_vec()));
        assert_eq!(bk.get_as_of("k", 1500), Some(b"v1".to_vec()));
        assert_eq!(bk.get_as_of("k", 1000), Some(b"v1".to_vec()));
        // Before any branch write, falls through to base
        assert_eq!(bk.get_as_of("k", 500), Some(b"main".to_vec()));
    }

    #[test]
    fn key_history_returns_reverse_chronological_order() {
        let base = InMemoryKv::new();
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let b = bk.create_branch(None, "t").unwrap();
        bk.use_branch(Some(b));
        bk.set_with_timestamp("k", b"a".to_vec(), 100);
        bk.set_with_timestamp("k", b"b".to_vec(), 200);
        bk.set_with_timestamp("k", b"c".to_vec(), 300);

        let hist = bk.key_history("k", 0);
        assert_eq!(hist.len(), 3);
        assert_eq!(hist[0].timestamp_ms, 300);
        assert_eq!(hist[1].timestamp_ms, 200);
        assert_eq!(hist[2].timestamp_ms, 100);
    }

    #[test]
    fn key_history_with_limit() {
        let base = InMemoryKv::new();
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let b = bk.create_branch(None, "t").unwrap();
        bk.use_branch(Some(b));
        bk.set_with_timestamp("k", b"a".to_vec(), 100);
        bk.set_with_timestamp("k", b"b".to_vec(), 200);
        bk.set_with_timestamp("k", b"c".to_vec(), 300);

        let hist = bk.key_history("k", 2);
        assert_eq!(hist.len(), 2);
        assert_eq!(hist[0].timestamp_ms, 300);
        assert_eq!(hist[1].timestamp_ms, 200);
    }

    #[test]
    fn apply_retention_prunes_old_entries() {
        let base = InMemoryKv::new();
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let b = bk.create_branch(None, "t").unwrap();
        bk.use_branch(Some(b));
        bk.set_with_timestamp("k", b"old".to_vec(), 1000);
        bk.set_with_timestamp("k", b"mid".to_vec(), 5000);
        bk.set_with_timestamp("k", b"new".to_vec(), 9000);

        let policy = RetentionPolicy {
            max_entries_per_key: 0,
            max_age_ms: 5000,
        };
        let pruned = bk.apply_retention(&policy, 10000);
        assert_eq!(pruned, 1);
        let hist = bk.key_history("k", 0);
        assert_eq!(hist.len(), 2);
        assert_eq!(hist[0].value, Some(b"new".to_vec()));
        assert_eq!(hist[1].value, Some(b"mid".to_vec()));
    }

    #[test]
    fn retention_by_count() {
        let base = InMemoryKv::new();
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let b = bk.create_branch(None, "t").unwrap();
        bk.use_branch(Some(b));
        bk.set_with_timestamp("k", b"a".to_vec(), 100);
        bk.set_with_timestamp("k", b"b".to_vec(), 200);
        bk.set_with_timestamp("k", b"c".to_vec(), 300);
        bk.set_with_timestamp("k", b"d".to_vec(), 400);

        let policy = RetentionPolicy {
            max_entries_per_key: 2,
            max_age_ms: 0,
        };
        let pruned = bk.apply_retention(&policy, 500);
        assert_eq!(pruned, 2);
        let hist = bk.key_history("k", 0);
        assert_eq!(hist.len(), 2);
        // Newest are kept
        assert_eq!(hist[0].value, Some(b"d".to_vec()));
        assert_eq!(hist[1].value, Some(b"c".to_vec()));
    }

    #[test]
    fn retention_by_age() {
        let base = InMemoryKv::new();
        let bk = BranchedKv::new(base, BranchRegistry::new());
        let b = bk.create_branch(None, "t").unwrap();
        bk.use_branch(Some(b));
        bk.set_with_timestamp("k", b"old".to_vec(), 100);
        bk.set_with_timestamp("k", b"recent".to_vec(), 900);

        let policy = RetentionPolicy {
            max_entries_per_key: 0,
            max_age_ms: 200,
        };
        let pruned = bk.apply_retention(&policy, 1000);
        assert_eq!(pruned, 1);
        let hist = bk.key_history("k", 0);
        assert_eq!(hist.len(), 1);
        assert_eq!(hist[0].value, Some(b"recent".to_vec()));
    }
}
