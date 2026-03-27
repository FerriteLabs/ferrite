//! CHR.* command handlers — Chronicle branched key-value store (experimental).
//!
//! Subcommands: `CHR.BRANCH tenant [parent]`, `CHR.USE branch_id`,
//! `CHR.SET key value`, `CHR.GET key`, `CHR.DEL key`,
//! `CHR.STATS [branch]`, `CHR.SNAPSHOT`, `CHR.ROLLBACK index`,
//! `CHR.DIFF left right`, `CHR.MERGE branch [INTO target] [STRATEGY lww|src|dst]`,
//! `CHR.BRANCHES tenant`, `CHR.HISTORY branch key`,
//! `CHR.ASOF key timestamp_ms`, `CHR.KEYHISTORY key [limit]`,
//! `CHR.RETENTION max_entries max_age_ms`, `CHR.GC`,
//! `CHR.CONFIG`, `CHR.SAVE`, `CHR.LOAD`, `CHR.HELP`.
//!
//! Backed by a `BranchedKv<InMemoryKv>` with branch-aware reads/writes.
//! Full state is persisted to the Store under `__ferrite:chronicle:data`
//! so branches survive restarts.  See ADR-021 for production wiring.

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::{Store, Value};
use ferrite_chronicle::{
    BranchMeta, BranchRegistry, BranchStats, BranchedKv, InMemoryKv, MergeStrategy,
    RetentionPolicy, TimestampedEntry,
};

use super::moonshot_limits::{validate_branch_name, validate_key, validate_tenant, validate_value};
use super::{bulk, err_frame, ok_frame, warn_experimental};

const CHRONICLE_STORE_KEY: &str = "__ferrite:chronicle:data";

static MUTATION_COUNTER: AtomicU64 = AtomicU64::new(0);
static BKV: OnceLock<BranchedKv<InMemoryKv>> = OnceLock::new();
static LOADED_FROM_STORE: OnceLock<bool> = OnceLock::new();

fn bkv() -> &'static BranchedKv<InMemoryKv> {
    BKV.get_or_init(|| BranchedKv::new(InMemoryKv::default(), BranchRegistry::new()))
}

/// Serialisable branch stats for persistence.
#[derive(serde::Serialize, serde::Deserialize, Default, Clone, Copy)]
struct BranchStatsSnap {
    overlay_keys: usize,
    writes: u64,
    deletes: u64,
    snapshots: usize,
}

impl From<BranchStats> for BranchStatsSnap {
    fn from(s: BranchStats) -> Self {
        Self {
            overlay_keys: s.overlay_keys,
            writes: s.writes,
            deletes: s.deletes,
            snapshots: s.snapshots,
        }
    }
}

impl From<BranchStatsSnap> for BranchStats {
    fn from(s: BranchStatsSnap) -> Self {
        Self {
            overlay_keys: s.overlay_keys,
            writes: s.writes,
            deletes: s.deletes,
            snapshots: s.snapshots,
        }
    }
}

/// Serialisable overlay entry for a single branch.
#[derive(serde::Serialize, serde::Deserialize, Default)]
struct OverlaySnap {
    branch_id: String,
    entries: Vec<(String, Option<Vec<u8>>)>,
    stats: BranchStatsSnap,
}

/// Serialisable time-travel history for a single branch.
#[derive(serde::Serialize, serde::Deserialize, Default)]
struct HistorySnap {
    branch_id: String,
    entries: Vec<(String, Vec<TimestampedEntry>)>,
}

/// Serialisable snapshot stack for a single branch.
#[derive(serde::Serialize, serde::Deserialize, Default)]
struct SnapshotStackSnap {
    branch_id: String,
    stack: Vec<Vec<(String, Option<Vec<u8>>)>>,
}

#[derive(serde::Serialize, serde::Deserialize, Default)]
struct ChronicleSnapshot {
    branches: Vec<BranchMeta>,
    main_entries: Vec<(String, Vec<u8>)>,
    overlays: Vec<OverlaySnap>,
    snapshot_stacks: Vec<SnapshotStackSnap>,
    history: Vec<HistorySnap>,
}

fn ensure_loaded_from_store(store: &Store) {
    LOADED_FROM_STORE.get_or_init(|| {
        let key = Bytes::from(CHRONICLE_STORE_KEY);
        if let Some(Value::String(data)) = store.get(0, &key) {
            if let Ok(snap) = serde_json::from_slice::<ChronicleSnapshot>(&data) {
                restore_snapshot(&snap);
                tracing::info!(
                    "Chronicle: restored {} branch(es) from Store",
                    snap.branches.len()
                );
            }
        }
        true
    });
}

fn restore_snapshot(snap: &ChronicleSnapshot) {
    let b = bkv();
    // Restore branch metadata.
    for meta in &snap.branches {
        b.registry().restore(meta.clone());
    }
    // Restore main entries.
    b.use_branch(None);
    for (k, v) in &snap.main_entries {
        b.set(k, v.clone());
    }
    // Restore overlays with stats.
    for os in &snap.overlays {
        b.replay_overlay_with_stats(os.branch_id.clone(), os.entries.clone(), os.stats.into());
    }
    // Restore snapshot stacks.
    for ss in &snap.snapshot_stacks {
        b.replay_snapshots(ss.branch_id.clone(), ss.stack.clone());
    }
    // Restore history.
    for hs in &snap.history {
        b.replay_history(hs.branch_id.clone(), hs.entries.clone());
    }
}

fn build_snapshot() -> ChronicleSnapshot {
    let b = bkv();

    ChronicleSnapshot {
        branches: b.registry().all(),
        main_entries: Vec::new(),
        overlays: b
            .snapshot_overlays_with_stats()
            .into_iter()
            .map(|(branch_id, entries, stats)| OverlaySnap {
                branch_id,
                entries,
                stats: stats.into(),
            })
            .collect(),
        snapshot_stacks: b
            .snapshot_stacks()
            .into_iter()
            .map(|(branch_id, stack)| SnapshotStackSnap { branch_id, stack })
            .collect(),
        history: b
            .snapshot_history()
            .into_iter()
            .map(|(branch_id, entries)| HistorySnap { branch_id, entries })
            .collect(),
    }
}

fn persist_to_store(store: &Store) -> Result<(), String> {
    let snap = build_snapshot();
    let json =
        serde_json::to_vec(&snap).map_err(|e| format!("serialize chronicle snapshot: {e}"))?;
    store.set(
        0,
        Bytes::from(CHRONICLE_STORE_KEY),
        Value::String(Bytes::from(json)),
    );
    Ok(())
}

/// Execute a CHR.* (Chronicle branched KV) command without Store persistence.
///
/// This variant is used for backward-compatible dispatch and testing.
/// State is held in process-local singletons — see [`chronicle_command_with_store`]
/// for the production entry point.
pub fn chronicle_command(subcommand: &str, args: &[String]) -> Frame {
    warn_experimental("CHR");
    match subcommand.to_uppercase().as_str() {
        "BRANCH" => branch_create(args),
        "USE" => use_branch(args),
        "SET" => set(args),
        "GET" => get(args),
        "DEL" => del(args),
        "STATS" => stats(args),
        "SNAPSHOT" => snapshot(),
        "ROLLBACK" => rollback(args),
        "DIFF" => diff(args),
        "MERGE" => merge(args),
        "BRANCHES" => branches(args),
        "HISTORY" => history(args),
        "ASOF" => as_of(args),
        "KEYHISTORY" => key_history(args),
        "RETENTION" => retention(args),
        "GC" => gc(),
        "CONFIG" => config(),
        "HELP" | "" => help(),
        other => err_frame(&format!("unknown CHR subcommand '{}'", other)),
    }
}

/// Execute a CHR.* (Chronicle branched KV) command with Store-backed persistence.
///
/// Auto-loads state from Store on first call, and auto-persists after
/// mutating operations (SET, DEL, BRANCH, SNAPSHOT, ROLLBACK, MERGE, GC, RETENTION).
/// Use `CHR.SAVE` / `CHR.LOAD` for explicit persistence control.
///
/// # Subcommands
///
/// | Command | Mutating | Description |
/// |---------|----------|-------------|
/// | `CHR.BRANCH` | Yes | Create a branch |
/// | `CHR.USE` | No | Switch active branch |
/// | `CHR.SET` | Yes | Set a key-value on the active branch |
/// | `CHR.GET` | No | Get a value (branch-aware) |
/// | `CHR.DEL` | Yes | Delete a key on the active branch |
/// | `CHR.STATS` | No | Show branch statistics |
/// | `CHR.SNAPSHOT` | Yes | Take a point-in-time snapshot |
/// | `CHR.ROLLBACK` | Yes | Rollback to a snapshot |
/// | `CHR.DIFF` | No | Compare two branches |
/// | `CHR.MERGE` | Yes | Merge a branch into parent/target |
/// | `CHR.BRANCHES` | No | List branches for a tenant |
/// | `CHR.HISTORY` | No | Key history across branch ancestry |
/// | `CHR.ASOF` | No | Time-travel read |
/// | `CHR.KEYHISTORY` | No | Write history for a key |
/// | `CHR.RETENTION` | Yes | Apply retention policy |
/// | `CHR.GC` | Yes | Garbage-collect expired branches |
/// | `CHR.CONFIG` | No | Show Chronicle configuration |
/// | `CHR.SAVE` | No | Persist state to Store |
/// | `CHR.LOAD` | No | Reload state from Store |
/// | `CHR.HELP` | No | Show help |
pub fn chronicle_command_with_store(
    store: &Arc<Store>,
    subcommand: &str,
    args: &[String],
) -> Frame {
    warn_experimental("CHR");
    if !super::moonshot_config::is_enabled("CHR") {
        return err_frame("ERR CHR.* commands are disabled in moonshot configuration");
    }
    ensure_loaded_from_store(store);

    let upper = subcommand.to_uppercase();
    let is_mutating = matches!(
        upper.as_str(),
        "SET" | "DEL" | "BRANCH" | "SNAPSHOT" | "ROLLBACK" | "MERGE" | "GC" | "RETENTION"
    );

    let result = match upper.as_str() {
        "BRANCH" => branch_create(args),
        "USE" => use_branch(args),
        "SET" => set(args),
        "GET" => get(args),
        "DEL" => del(args),
        "STATS" => stats(args),
        "SNAPSHOT" => snapshot(),
        "ROLLBACK" => rollback(args),
        "DIFF" => diff(args),
        "MERGE" => merge(args),
        "BRANCHES" => branches(args),
        "HISTORY" => history(args),
        "ASOF" => as_of(args),
        "KEYHISTORY" => key_history(args),
        "RETENTION" => retention(args),
        "GC" => gc(),
        "CONFIG" => config(),
        "SAVE" => match persist_to_store(store) {
            Ok(()) => ok_frame(),
            Err(e) => err_frame(&format!("save: {e}")),
        },
        "LOAD" => {
            let key = Bytes::from(CHRONICLE_STORE_KEY);
            match store.get(0, &key) {
                Some(Value::String(data)) => {
                    match serde_json::from_slice::<ChronicleSnapshot>(&data) {
                        Ok(snap) => {
                            restore_snapshot(&snap);
                            ok_frame()
                        }
                        Err(e) => err_frame(&format!("load: invalid snapshot: {e}")),
                    }
                }
                _ => err_frame("load: no chronicle snapshot in store"),
            }
        }
        "HELP" | "" => help(),
        other => return err_frame(&format!("unknown CHR subcommand '{}'", other)),
    };

    if is_mutating && !matches!(result, Frame::Error(_)) && super::should_persist(&MUTATION_COUNTER)
    {
        if let Err(e) = persist_to_store(store) {
            tracing::warn!("Failed to persist chronicle data: {}", e);
        }
    }

    result
}

// ── Subcommand implementations ──────────────────────────────────────────

fn branch_create(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("usage: CHR.BRANCH tenant [parent]");
    }
    let tenant = &args[0];
    if let Err(f) = validate_tenant(tenant) {
        return f;
    }
    let parent = if args.len() > 1 {
        if let Err(f) = validate_branch_name(&args[1]) {
            return f;
        }
        Some(args[1].clone())
    } else {
        None
    };
    match bkv().create_branch(parent, tenant.as_str()) {
        Ok(id) => bulk(id),
        Err(e) => err_frame(&format!("branch create: {e}")),
    }
}

fn use_branch(args: &[String]) -> Frame {
    if args.is_empty() {
        bkv().use_branch(None);
        return ok_frame();
    }
    let branch = &args[0];
    if branch == "main" || branch == "MAIN" {
        bkv().use_branch(None);
        return ok_frame();
    }
    if let Err(f) = validate_branch_name(branch) {
        return f;
    }
    // Validate branch exists.
    if bkv().registry().get(branch).is_none() {
        return err_frame(&format!("branch '{}' not found", branch));
    }
    bkv().use_branch(Some(branch.clone()));
    ok_frame()
}

fn set(args: &[String]) -> Frame {
    if args.len() != 2 {
        return err_frame("usage: CHR.SET key value");
    }
    if let Err(f) = validate_key(&args[0]) {
        return f;
    }
    if let Err(f) = validate_value(&args[1]) {
        return f;
    }
    bkv().set(&args[0], args[1].as_bytes().to_vec());
    ok_frame()
}

fn get(args: &[String]) -> Frame {
    if args.len() != 1 {
        return err_frame("usage: CHR.GET key");
    }
    if let Err(f) = validate_key(&args[0]) {
        return f;
    }
    match bkv().get(&args[0]) {
        Some(v) => Frame::Bulk(Some(Bytes::from(v))),
        None => Frame::Bulk(None),
    }
}

fn del(args: &[String]) -> Frame {
    if args.len() != 1 {
        return err_frame("usage: CHR.DEL key");
    }
    if let Err(f) = validate_key(&args[0]) {
        return f;
    }
    if bkv().del(&args[0]) {
        ok_frame()
    } else {
        Frame::Bulk(None)
    }
}

fn stats(args: &[String]) -> Frame {
    let branch = if args.is_empty() {
        match bkv().active() {
            Some(b) => b,
            None => {
                return Frame::Array(Some(vec![
                    bulk("active_branch"),
                    bulk("main"),
                    bulk("branches_total"),
                    Frame::Integer(bkv().registry().all().len() as i64),
                ]))
            }
        }
    } else {
        args[0].clone()
    };
    let s = bkv().stats(&branch);
    Frame::Array(Some(vec![
        bulk("branch"),
        bulk(&branch),
        bulk("overlay_keys"),
        Frame::Integer(s.overlay_keys as i64),
        bulk("writes"),
        Frame::Integer(s.writes as i64),
        bulk("deletes"),
        Frame::Integer(s.deletes as i64),
        bulk("snapshots"),
        Frame::Integer(s.snapshots as i64),
    ]))
}

fn snapshot() -> Frame {
    match bkv().snapshot() {
        Some(idx) => Frame::Integer(idx as i64),
        None => err_frame("no active branch — switch to a branch first with CHR.USE"),
    }
}

fn rollback(args: &[String]) -> Frame {
    if args.len() != 1 {
        return err_frame("usage: CHR.ROLLBACK index");
    }
    let index: usize = match args[0].parse() {
        Ok(n) => n,
        Err(_) => return err_frame("index must be a non-negative integer"),
    };
    if bkv().rollback(index) {
        ok_frame()
    } else {
        err_frame("rollback failed: invalid index or no active branch")
    }
}

fn diff(args: &[String]) -> Frame {
    if args.len() != 2 {
        return err_frame("usage: CHR.DIFF left right");
    }
    match bkv().diff(&args[0], &args[1]) {
        Ok(entries) => {
            let items: Vec<Frame> = entries
                .iter()
                .map(|e| {
                    Frame::Array(Some(vec![
                        bulk("key"),
                        bulk(&e.key),
                        bulk("op"),
                        bulk(format!("{:?}", e.op)),
                    ]))
                })
                .collect();
            Frame::Array(Some(items))
        }
        Err(e) => err_frame(&format!("diff: {e}")),
    }
}

fn merge(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("usage: CHR.MERGE branch [INTO target] [STRATEGY lww|src|dst]");
    }
    let src = &args[0];
    let mut target: Option<&str> = None;
    let mut strategy = MergeStrategy::Lww;

    let mut i = 1;
    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "INTO" => {
                if i + 1 >= args.len() {
                    return err_frame("INTO requires a target branch name");
                }
                target = Some(&args[i + 1]);
                i += 2;
            }
            "STRATEGY" => {
                if i + 1 >= args.len() {
                    return err_frame("STRATEGY requires a value (lww, src, dst)");
                }
                strategy = match args[i + 1].to_uppercase().as_str() {
                    "LWW" => MergeStrategy::Lww,
                    "SRC" | "PREFERSRC" => MergeStrategy::PreferSrc,
                    "DST" | "PREFERDST" => MergeStrategy::PreferDst,
                    other => {
                        return err_frame(&format!(
                            "unknown strategy '{}'; use lww, src, or dst",
                            other
                        ))
                    }
                };
                i += 2;
            }
            _ => {
                return err_frame(&format!("unexpected argument '{}'", args[i]));
            }
        }
    }

    match bkv().merge_into(src, target, strategy) {
        Ok(n) => Frame::Integer(n as i64),
        Err(e) => err_frame(&format!("merge: {e}")),
    }
}

fn branches(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("usage: CHR.BRANCHES tenant");
    }
    let tenant = &args[0];
    let list = bkv().registry().list(tenant);
    let items: Vec<Frame> = list
        .iter()
        .map(|m| {
            Frame::Array(Some(vec![
                bulk("id"),
                bulk(&m.id),
                bulk("tenant"),
                bulk(&m.tenant),
                bulk("parent"),
                match &m.parent {
                    Some(p) => bulk(p),
                    None => Frame::Bulk(None),
                },
                bulk("created_at_ms"),
                Frame::Integer(m.created_at_ms as i64),
            ]))
        })
        .collect();
    Frame::Array(Some(items))
}

fn history(args: &[String]) -> Frame {
    if args.len() != 2 {
        return err_frame("usage: CHR.HISTORY branch key");
    }
    match bkv().history(&args[0], &args[1]) {
        Ok(entries) => {
            let items: Vec<Frame> = entries
                .iter()
                .map(|(branch, val)| {
                    Frame::Array(Some(vec![
                        bulk("branch"),
                        bulk(branch),
                        bulk("value"),
                        match val {
                            Some(v) => Frame::Bulk(Some(Bytes::from(v.clone()))),
                            None => Frame::Bulk(None),
                        },
                    ]))
                })
                .collect();
            Frame::Array(Some(items))
        }
        Err(e) => err_frame(&format!("history: {e}")),
    }
}

fn as_of(args: &[String]) -> Frame {
    if args.len() != 2 {
        return err_frame("usage: CHR.ASOF key timestamp_ms");
    }
    if let Err(f) = validate_key(&args[0]) {
        return f;
    }
    let ts: u64 = match args[1].parse() {
        Ok(n) => n,
        Err(_) => return err_frame("timestamp_ms must be a non-negative integer"),
    };
    match bkv().get_as_of(&args[0], ts) {
        Some(v) => Frame::Bulk(Some(Bytes::from(v))),
        None => Frame::Bulk(None),
    }
}

fn key_history(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("usage: CHR.KEYHISTORY key [limit]");
    }
    if let Err(f) = validate_key(&args[0]) {
        return f;
    }
    let limit: usize = if args.len() > 1 {
        match args[1].parse() {
            Ok(n) => n,
            Err(_) => return err_frame("limit must be a non-negative integer"),
        }
    } else {
        0
    };
    let entries = bkv().key_history(&args[0], limit);
    let items: Vec<Frame> = entries
        .iter()
        .map(|e| {
            Frame::Array(Some(vec![
                bulk("value"),
                match &e.value {
                    Some(v) => Frame::Bulk(Some(Bytes::from(v.clone()))),
                    None => Frame::Bulk(None),
                },
                bulk("timestamp_ms"),
                Frame::Integer(e.timestamp_ms as i64),
            ]))
        })
        .collect();
    Frame::Array(Some(items))
}

fn retention(args: &[String]) -> Frame {
    if args.len() != 2 {
        return err_frame("usage: CHR.RETENTION max_entries max_age_ms");
    }
    let max_entries: usize = match args[0].parse() {
        Ok(n) => n,
        Err(_) => return err_frame("max_entries must be a non-negative integer"),
    };
    let max_age_ms: u64 = match args[1].parse() {
        Ok(n) => n,
        Err(_) => return err_frame("max_age_ms must be a non-negative integer"),
    };
    let policy = RetentionPolicy {
        max_entries_per_key: max_entries,
        max_age_ms,
    };
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let pruned = bkv().apply_retention(&policy, now_ms);
    Frame::Integer(pruned as i64)
}

fn gc() -> Frame {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let policy = ferrite_chronicle::GcPolicy::default();
    let result = ferrite_chronicle::gc::collect(bkv().registry(), &policy, now_ms);
    Frame::Array(Some(vec![
        bulk("branches_scanned"),
        Frame::Integer(result.branches_scanned as i64),
        bulk("branches_collected"),
        Frame::Integer(result.branches_collected as i64),
        bulk("overlays_freed"),
        Frame::Integer(result.overlays_freed as i64),
    ]))
}

fn config() -> Frame {
    let cfg = super::moonshot_config::get();
    Frame::Array(Some(vec![
        bulk("enabled"),
        bulk(cfg.chronicle.enabled.to_string()),
        bulk("max_total_branches"),
        Frame::Integer(cfg.chronicle.max_total_branches as i64),
        bulk("max_branch_age_seconds"),
        Frame::Integer(cfg.chronicle.max_branch_age_seconds as i64),
        bulk("retention_max_entries_per_key"),
        Frame::Integer(cfg.chronicle.retention_max_entries_per_key as i64),
        bulk("retention_max_age_seconds"),
        Frame::Integer(cfg.chronicle.retention_max_age_seconds as i64),
    ]))
}

fn help() -> Frame {
    let lines = [
        "CHR.BRANCH tenant [parent] - Create a new branch, returns branch ID",
        "CHR.USE branch_id - Switch to a branch (or 'main' for the base)",
        "CHR.SET key value - Set a key on the active branch",
        "CHR.GET key - Get a value (reads through branch ancestry)",
        "CHR.DEL key - Delete a key on the active branch",
        "CHR.STATS [branch] - Show stats for the active or specified branch",
        "CHR.SNAPSHOT - Take a snapshot of the active branch's overlay",
        "CHR.ROLLBACK index - Restore overlay to snapshot at index",
        "CHR.DIFF left right - Compare overlays of two branches",
        "CHR.MERGE branch [INTO target] [STRATEGY lww|src|dst] - Merge branch",
        "CHR.BRANCHES tenant - List branches for a tenant",
        "CHR.HISTORY branch key - Trace key through branch ancestry",
        "CHR.ASOF key timestamp_ms - Time-travel read at a specific timestamp",
        "CHR.KEYHISTORY key [limit] - Write history for key on active branch",
        "CHR.RETENTION max_entries max_age_ms - Apply retention policy",
        "CHR.GC - Garbage-collect expired branches",
        "CHR.CONFIG - Show Chronicle configuration",
        "CHR.SAVE - Persist all state to Store",
        "CHR.LOAD - Restore state from Store",
        "CHR.HELP - Show this help",
    ];
    Frame::Array(Some(lines.iter().map(|l| bulk(*l)).collect()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| (*x).to_string()).collect()
    }

    #[test]
    fn set_get_roundtrip() {
        // Use main branch for a simple roundtrip.
        bkv().use_branch(None);
        assert!(matches!(
            chronicle_command("SET", &s(&["chr:test:k1", "hello"])),
            Frame::Simple(_)
        ));
        match chronicle_command("GET", &s(&["chr:test:k1"])) {
            Frame::Bulk(Some(b)) => assert_eq!(&b[..], b"hello"),
            other => panic!("expected hello, got {:?}", other),
        }
    }

    #[test]
    fn branch_create_and_use() {
        let result = chronicle_command("BRANCH", &s(&["test-tenant"]));
        let branch_id = match &result {
            Frame::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
            other => panic!("expected branch id, got {:?}", other),
        };
        assert!(matches!(
            chronicle_command("USE", &s(&[&branch_id])),
            Frame::Simple(_)
        ));
        // Switch back to main.
        assert!(matches!(
            chronicle_command("USE", &s(&["main"])),
            Frame::Simple(_)
        ));
    }

    #[test]
    fn del_on_main() {
        bkv().use_branch(None);
        chronicle_command("SET", &s(&["chr:del:k", "val"]));
        assert!(matches!(
            chronicle_command("DEL", &s(&["chr:del:k"])),
            Frame::Simple(_)
        ));
        assert!(matches!(
            chronicle_command("GET", &s(&["chr:del:k"])),
            Frame::Bulk(None)
        ));
    }

    #[test]
    fn stats_returns_array() {
        if let Frame::Array(Some(items)) = chronicle_command("STATS", &[]) {
            assert!(items.len() >= 4, "STATS should return at least 4 elements");
        } else {
            panic!("STATS should be an array");
        }
    }

    #[test]
    fn unknown_subcommand_errors() {
        assert!(matches!(chronicle_command("WAT", &[]), Frame::Error(_)));
    }

    #[test]
    fn missing_args_errors() {
        assert!(matches!(
            chronicle_command("SET", &s(&["only-key"])),
            Frame::Error(_)
        ));
        assert!(matches!(chronicle_command("GET", &[]), Frame::Error(_)));
        assert!(matches!(chronicle_command("BRANCH", &[]), Frame::Error(_)));
    }

    #[test]
    fn save_load_via_store() {
        let store = Arc::new(Store::new(1));
        bkv().use_branch(None);
        let _ = chronicle_command_with_store(&store, "SET", &s(&["chr:save:k", "v"]));
        assert!(matches!(
            chronicle_command_with_store(&store, "SAVE", &[]),
            Frame::Simple(_)
        ));
        assert!(matches!(
            chronicle_command_with_store(&store, "LOAD", &[]),
            Frame::Simple(_)
        ));
    }

    #[test]
    fn branches_list() {
        let _ = chronicle_command("BRANCH", &s(&["list-tenant"]));
        if let Frame::Array(Some(items)) = chronicle_command("BRANCHES", &s(&["list-tenant"])) {
            assert!(!items.is_empty(), "should list at least 1 branch");
        } else {
            panic!("BRANCHES should return an array");
        }
    }

    #[test]
    fn gc_returns_result() {
        if let Frame::Array(Some(items)) = chronicle_command("GC", &[]) {
            assert_eq!(items.len(), 6, "GC should return 6-element array");
        } else {
            panic!("GC should be an array");
        }
    }

    #[test]
    fn config_returns_array() {
        if let Frame::Array(Some(items)) = chronicle_command("CONFIG", &[]) {
            assert!(
                items.len() >= 10,
                "CONFIG should return at least 10 elements"
            );
        } else {
            panic!("CONFIG should be an array");
        }
    }

    #[test]
    fn help_returns_array() {
        if let Frame::Array(Some(items)) = chronicle_command("HELP", &[]) {
            assert!(!items.is_empty(), "HELP should return help lines");
        } else {
            panic!("HELP should be an array");
        }
    }
}
