//! MEM.* command handlers — Mnemo Agent Memory OS (experimental).
//!
//! Subcommands: `MEM.PUT agent session kind content [META json]`,
//! `MEM.GET id`, `MEM.RECALL agent [LIMIT n] [KIND kind]`,
//! `MEM.FORGET agent`, `MEM.SUMMARIZE agent [STRATEGY session|time] [THRESHOLD n]`,
//! `MEM.STATS`, `MEM.SAVE`, `MEM.LOAD`, `MEM.HELP`.
//!
//! Full state — the in-memory record map — is persisted to the Store under
//! `__ferrite:mnemo:data` so it survives restarts.  See ADR-018 for the
//! production replication and tiered-storage path.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use parking_lot::RwLock;

use crate::protocol::Frame;
use crate::storage::{Store, Value};
use ferrite_mnemo::{
    InMemoryMnemoStore, MemoryKind, MemoryRecord, MemoryRecordBuilder, RecallFilter, Scope,
    SummarizeStrategy,
};

use super::moonshot_limits::{validate_agent_id, validate_key, validate_meta, validate_value};
use super::{bulk, err_frame, ok_frame, warn_experimental};

const MNEMO_STORE_KEY: &str = "__ferrite:mnemo:data";
const DEFAULT_TENANT: &str = "default";

static MUTATION_COUNTER: AtomicU64 = AtomicU64::new(0);
static STORE: OnceLock<InMemoryMnemoStore> = OnceLock::new();
static LOADED_FROM_STORE: OnceLock<bool> = OnceLock::new();
/// Maps record_id → (agent_id, session_id) for cross-agent GET lookups.
type MnemoIdIndex = RwLock<HashMap<String, (String, Option<String>)>>;
static ID_INDEX: OnceLock<MnemoIdIndex> = OnceLock::new();

fn mnemo_store() -> &'static InMemoryMnemoStore {
    STORE.get_or_init(InMemoryMnemoStore::new)
}

fn id_index() -> &'static MnemoIdIndex {
    ID_INDEX.get_or_init(|| RwLock::new(HashMap::new()))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn parse_kind(s: &str) -> Result<MemoryKind, Frame> {
    match s.to_ascii_lowercase().as_str() {
        "working" => Ok(MemoryKind::Working),
        "semantic" => Ok(MemoryKind::Semantic),
        "episodic" => Ok(MemoryKind::Episodic),
        "procedural" => Ok(MemoryKind::Procedural),
        "summary" => Ok(MemoryKind::Summary),
        other => Err(err_frame(&format!(
            "unknown memory kind '{}'; expected working|semantic|episodic|procedural|summary",
            other
        ))),
    }
}

fn kind_to_str(k: MemoryKind) -> &'static str {
    match k {
        MemoryKind::Working => "working",
        MemoryKind::Semantic => "semantic",
        MemoryKind::Episodic => "episodic",
        MemoryKind::Procedural => "procedural",
        MemoryKind::Summary => "summary",
    }
}

// ---------------------------------------------------------------------------
// Snapshot persistence
// ---------------------------------------------------------------------------

#[derive(serde::Serialize, serde::Deserialize, Default)]
struct MnemoSnapshot {
    records: Vec<MemoryRecord>,
}

fn ensure_loaded_from_store(store: &Store) {
    LOADED_FROM_STORE.get_or_init(|| {
        let key = Bytes::from(MNEMO_STORE_KEY);
        if let Some(Value::String(data)) = store.get(0, &key) {
            if let Ok(snap) = serde_json::from_slice::<MnemoSnapshot>(&data) {
                let ms = mnemo_store();
                let idx = id_index();
                let mut loaded = 0usize;
                for rec in snap.records {
                    let agent_id = rec.agent_id.clone();
                    let record_id = rec.id.clone();
                    let session_id = rec.session_id.clone();
                    let scope = Scope::new(&rec.tenant_id, &agent_id);
                    if ms.put(&scope, rec).is_ok() {
                        idx.write().insert(record_id, (agent_id, session_id));
                        loaded += 1;
                    }
                }
                tracing::info!("Mnemo: restored {} record(s) from Store", loaded);
            }
        }
        true
    });
}

fn persist_to_store(store: &Store) -> Result<(), String> {
    let ms = mnemo_store();
    let idx = id_index();

    // Collect all unique agents from the index, then recall each agent's records.
    let agents: Vec<String> = {
        let guard = idx.read();
        let mut set: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for (agent, _) in guard.values() {
            set.insert(agent.clone());
        }
        set.into_iter().collect()
    };

    let mut all_records = Vec::new();
    let ts = now_ms();
    for agent in &agents {
        let scope = Scope::new(DEFAULT_TENANT, agent);
        let result = ms.recall(
            &scope,
            ts,
            &RecallFilter {
                limit: 0,
                ..Default::default()
            },
        );
        all_records.extend(result.records);
    }

    let snap = MnemoSnapshot {
        records: all_records,
    };
    let json = serde_json::to_vec(&snap).map_err(|e| format!("serialize mnemo snapshot: {e}"))?;
    store.set(
        0,
        Bytes::from(MNEMO_STORE_KEY),
        Value::String(Bytes::from(json)),
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Execute a MEM.* (Mnemo Agent Memory) command without Store persistence.
///
/// This variant is used for backward-compatible dispatch and testing.
/// State is held in process-local singletons — see [`mnemo_command_with_store`]
/// for the production entry point.
pub fn mnemo_command(subcommand: &str, args: &[String]) -> Frame {
    warn_experimental("MEM");
    match subcommand.to_uppercase().as_str() {
        "PUT" => cmd_put(args),
        "GET" => cmd_get(args),
        "RECALL" => cmd_recall(args),
        "FORGET" => cmd_forget(args),
        "SUMMARIZE" => cmd_summarize(args),
        "STATS" => cmd_stats(),
        "HELP" | "" => help(),
        other => err_frame(&format!("unknown MEM subcommand '{}'", other)),
    }
}

/// Execute a MEM.* (Mnemo Agent Memory) command with Store-backed persistence.
///
/// Auto-loads state from Store on first call, and auto-persists after
/// mutating operations (PUT, FORGET, SUMMARIZE). Use `MEM.SAVE` / `MEM.LOAD`
/// for explicit persistence control.
///
/// # Subcommands
///
/// | Command | Mutating | Description |
/// |---------|----------|-------------|
/// | `MEM.PUT` | Yes | Store a memory record |
/// | `MEM.GET` | No | Retrieve a record by ID |
/// | `MEM.RECALL` | No | Recall memories for an agent |
/// | `MEM.FORGET` | Yes | Delete all records for an agent |
/// | `MEM.SUMMARIZE` | Yes | Summarize agent memory |
/// | `MEM.STATS` | No | Show memory statistics |
/// | `MEM.SAVE` | No | Persist state to Store |
/// | `MEM.LOAD` | No | Reload state from Store |
/// | `MEM.HELP` | No | Show help |
pub fn mnemo_command_with_store(store: &Arc<Store>, subcommand: &str, args: &[String]) -> Frame {
    warn_experimental("MEM");
    if !super::moonshot_config::is_enabled("MEM") {
        return err_frame("ERR MEM.* commands are disabled in moonshot configuration");
    }
    ensure_loaded_from_store(store);

    let upper = subcommand.to_uppercase();
    let is_mutating = matches!(upper.as_str(), "PUT" | "FORGET" | "SUMMARIZE");

    let result = match upper.as_str() {
        "PUT" => cmd_put(args),
        "GET" => cmd_get(args),
        "RECALL" => cmd_recall(args),
        "FORGET" => cmd_forget(args),
        "SUMMARIZE" => cmd_summarize(args),
        "STATS" => cmd_stats(),
        "SAVE" => match persist_to_store(store) {
            Ok(()) => ok_frame(),
            Err(e) => err_frame(&format!("save: {e}")),
        },
        "LOAD" => {
            let key = Bytes::from(MNEMO_STORE_KEY);
            match store.get(0, &key) {
                Some(Value::String(data)) => match serde_json::from_slice::<MnemoSnapshot>(&data) {
                    Ok(snap) => {
                        let ms = mnemo_store();
                        let idx = id_index();
                        for rec in snap.records {
                            let agent_id = rec.agent_id.clone();
                            let record_id = rec.id.clone();
                            let session_id = rec.session_id.clone();
                            let scope = Scope::new(&rec.tenant_id, &agent_id);
                            if ms.put(&scope, rec).is_ok() {
                                idx.write().insert(record_id, (agent_id, session_id));
                            }
                        }
                        ok_frame()
                    }
                    Err(e) => err_frame(&format!("load: invalid snapshot: {e}")),
                },
                _ => err_frame("load: no mnemo snapshot in store"),
            }
        }
        "HELP" | "" => help(),
        other => return err_frame(&format!("unknown MEM subcommand '{}'", other)),
    };

    if is_mutating && !matches!(result, Frame::Error(_)) && super::should_persist(&MUTATION_COUNTER)
    {
        if let Err(e) = persist_to_store(store) {
            tracing::warn!("Failed to persist mnemo data: {}", e);
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Subcommand implementations
// ---------------------------------------------------------------------------

/// MEM.PUT agent session kind content [META json]
fn cmd_put(args: &[String]) -> Frame {
    if args.len() < 4 {
        return err_frame("usage: MEM.PUT agent session kind content [META json]");
    }

    let agent = &args[0];
    if let Err(f) = validate_agent_id(agent) {
        return f;
    }
    let session = &args[1];
    let kind = match parse_kind(&args[2]) {
        Ok(k) => k,
        Err(f) => return f,
    };
    let content = &args[3];
    if let Err(f) = validate_value(content) {
        return f;
    }

    // Optional META json at positions 4..5
    let mut metadata: HashMap<String, serde_json::Value> = HashMap::new();
    if args.len() >= 6 && args[4].eq_ignore_ascii_case("META") {
        if args.len() > 6 {
            return err_frame("unexpected extra arguments after META value");
        }
        if let Err(f) = validate_meta(&args[5]) {
            return f;
        }
        match serde_json::from_str::<HashMap<String, serde_json::Value>>(&args[5]) {
            Ok(m) => metadata = m,
            Err(e) => return err_frame(&format!("invalid META json: {e}")),
        }
    } else if args.len() > 4 {
        return err_frame("unexpected arguments after content; use META <json> for metadata");
    }

    let id = uuid::Uuid::new_v4().to_string();
    let ts = now_ms();

    let mut builder = MemoryRecordBuilder::new()
        .id(&id)
        .tenant(DEFAULT_TENANT)
        .agent(agent)
        .kind(kind)
        .content(content)
        .importance(0.5)
        .created_at(ts);

    if !session.eq_ignore_ascii_case("NONE") && !session.is_empty() {
        builder = builder.session(session);
    }

    let mut record = match builder.build() {
        Ok(r) => r,
        Err(e) => return err_frame(&format!("build record: {e}")),
    };
    record.metadata = metadata;

    let scope = Scope::new(DEFAULT_TENANT, agent);
    let session_for_index = record.session_id.clone();
    match mnemo_store().put(&scope, record) {
        Ok(()) => {
            id_index()
                .write()
                .insert(id.clone(), (agent.clone(), session_for_index));
            bulk(id)
        }
        Err(e) => err_frame(&format!("put failed: {e}")),
    }
}

/// MEM.GET id
fn cmd_get(args: &[String]) -> Frame {
    if args.len() != 1 {
        return err_frame("usage: MEM.GET id");
    }
    let record_id = &args[0];
    if let Err(f) = validate_key(record_id) {
        return f;
    }
    let ms = mnemo_store();

    // Look up the agent and session from the ID index
    let Some((agent, session)) = id_index().read().get(record_id).cloned() else {
        return Frame::Bulk(None);
    };

    let mut scope = Scope::new(DEFAULT_TENANT, &agent);
    if let Some(s) = session {
        scope = scope.with_session(s);
    }
    match ms.get(&scope, record_id, now_ms()) {
        Ok(r) => record_to_frame(&r),
        Err(_) => Frame::Bulk(None),
    }
}

/// MEM.RECALL agent [LIMIT n] [KIND kind]
fn cmd_recall(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("usage: MEM.RECALL agent [LIMIT n] [KIND kind]");
    }
    let agent = &args[0];
    if let Err(f) = validate_agent_id(agent) {
        return f;
    }
    let mut limit: usize = 10;
    let mut kind_filter: Option<MemoryKind> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].to_ascii_uppercase().as_str() {
            "LIMIT" => {
                i += 1;
                if i >= args.len() {
                    return err_frame("LIMIT requires a number");
                }
                match args[i].parse::<usize>() {
                    Ok(n) => limit = n,
                    Err(_) => return err_frame("LIMIT value must be a non-negative integer"),
                }
            }
            "KIND" => {
                i += 1;
                if i >= args.len() {
                    return err_frame("KIND requires a value");
                }
                match parse_kind(&args[i]) {
                    Ok(k) => kind_filter = Some(k),
                    Err(f) => return f,
                }
            }
            other => {
                return err_frame(&format!("unknown RECALL option '{other}'"));
            }
        }
        i += 1;
    }

    let scope = Scope::new(DEFAULT_TENANT, agent);
    let filter = RecallFilter {
        kind: kind_filter,
        limit,
        ..Default::default()
    };
    let result = mnemo_store().recall(&scope, now_ms(), &filter);

    let entries: Vec<Frame> = result
        .records
        .iter()
        .map(|r| {
            Frame::Array(Some(vec![
                bulk("id"),
                bulk(&r.id),
                bulk("kind"),
                bulk(kind_to_str(r.kind)),
                bulk("importance"),
                bulk(format!("{:.2}", r.importance)),
                bulk("content"),
                bulk(&r.content),
            ]))
        })
        .collect();

    Frame::Array(Some(entries))
}

/// MEM.FORGET agent — GDPR forget
fn cmd_forget(args: &[String]) -> Frame {
    if args.len() != 1 {
        return err_frame("usage: MEM.FORGET agent");
    }
    let agent = &args[0];
    if let Err(f) = validate_agent_id(agent) {
        return f;
    }
    let n = mnemo_store().forget_agent(DEFAULT_TENANT, agent);
    // Clean the ID index
    id_index().write().retain(|_, (a, _)| a != agent);
    Frame::Integer(n as i64)
}

/// MEM.SUMMARIZE agent [STRATEGY session|time] [THRESHOLD n]
fn cmd_summarize(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("usage: MEM.SUMMARIZE agent [STRATEGY session|time] [THRESHOLD n]");
    }
    let agent = &args[0];
    let mut strategy_type = "session";
    let mut threshold: usize = 5;

    let mut i = 1;
    while i < args.len() {
        match args[i].to_ascii_uppercase().as_str() {
            "STRATEGY" => {
                i += 1;
                if i >= args.len() {
                    return err_frame("STRATEGY requires a value (session|time)");
                }
                strategy_type = match args[i].to_ascii_lowercase().as_str() {
                    "session" => "session",
                    "time" => "time",
                    other => {
                        return err_frame(&format!(
                            "unknown strategy '{}'; expected session|time",
                            other
                        ));
                    }
                };
            }
            "THRESHOLD" => {
                i += 1;
                if i >= args.len() {
                    return err_frame("THRESHOLD requires a number");
                }
                match args[i].parse::<usize>() {
                    Ok(n) if n > 0 => threshold = n,
                    _ => return err_frame("THRESHOLD must be a positive integer"),
                }
            }
            other => {
                return err_frame(&format!("unknown SUMMARIZE option '{other}'"));
            }
        }
        i += 1;
    }

    let strategy = match strategy_type {
        "time" => SummarizeStrategy::TimeWindow {
            window_ms: threshold as u64 * 1000,
        },
        _ => SummarizeStrategy::SessionThreshold {
            max_records: threshold,
        },
    };

    // Recall all records for this agent (no limit).
    let scope = Scope::new(DEFAULT_TENANT, agent);
    let ts = now_ms();
    let result = mnemo_store().recall(
        &scope,
        ts,
        &RecallFilter {
            limit: 0,
            ..Default::default()
        },
    );

    let (sum_result, summaries) = ferrite_mnemo::summarize(&result.records, agent, &strategy, ts);

    // Insert the created summaries back into the store.
    let ms = mnemo_store();
    let idx = id_index();
    for summary in &summaries {
        let record_id = summary.id.clone();
        let session_for_idx = summary.session_id.clone();
        if ms.put(&scope, summary.clone()).is_ok() {
            idx.write()
                .insert(record_id, (agent.clone(), session_for_idx));
        }
    }

    Frame::Integer(sum_result.summaries_created as i64)
}

/// MEM.STATS
fn cmd_stats() -> Frame {
    let ms = mnemo_store();
    let total = ms.len();
    Frame::Array(Some(vec![
        bulk("total_records"),
        Frame::Integer(total as i64),
    ]))
}

fn record_to_frame(r: &MemoryRecord) -> Frame {
    Frame::Array(Some(vec![
        bulk("agent"),
        bulk(&r.agent_id),
        bulk("session"),
        bulk(r.session_id.as_deref().unwrap_or("")),
        bulk("kind"),
        bulk(kind_to_str(r.kind)),
        bulk("content"),
        bulk(&r.content),
        bulk("importance"),
        bulk(format!("{:.2}", r.importance)),
        bulk("access_count"),
        Frame::Integer(r.access_count as i64),
        bulk("created_at"),
        Frame::Integer(r.created_at as i64),
        bulk("last_accessed"),
        Frame::Integer(r.last_accessed as i64),
    ]))
}

fn help() -> Frame {
    let lines = [
        "MEM.PUT agent session kind content [META json] - Store a memory record, returns ID",
        "MEM.GET id - Retrieve a record by ID (bumps access count)",
        "MEM.RECALL agent [LIMIT n] [KIND kind] - Recall records with hybrid scoring",
        "MEM.FORGET agent - Delete all records for an agent (GDPR)",
        "MEM.SUMMARIZE agent [STRATEGY session|time] [THRESHOLD n] - Summarize agent memories",
        "MEM.STATS - Show store statistics",
        "MEM.SAVE - Persist all records to Store",
        "MEM.LOAD - Restore records from Store",
        "MEM.HELP - Show this help",
    ];
    Frame::Array(Some(lines.iter().map(|l| bulk(*l)).collect()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| (*x).to_string()).collect()
    }

    fn extract_bulk(frame: &Frame) -> String {
        match frame {
            Frame::Bulk(Some(b)) => String::from_utf8(b.to_vec()).unwrap(),
            other => panic!("expected Bulk, got {:?}", other),
        }
    }

    #[test]
    fn put_returns_id() {
        let result = mnemo_command("PUT", &s(&["agent-1", "sess-1", "semantic", "hello world"]));
        let id = extract_bulk(&result);
        assert!(!id.is_empty(), "PUT should return a non-empty ID");
        // UUID v4 format
        assert_eq!(id.len(), 36, "PUT should return a UUID v4 (36 chars)");
    }

    #[test]
    fn put_get_roundtrip() {
        let store = Arc::new(Store::new(1));
        let put_result = mnemo_command_with_store(
            &store,
            "PUT",
            &s(&["agent-rt", "sess-1", "episodic", "test content"]),
        );
        let id = extract_bulk(&put_result);

        let get_result = mnemo_command_with_store(&store, "GET", &s(&[&id]));
        match &get_result {
            Frame::Array(Some(fields)) => {
                // Find "content" key and check its value
                let mut found_content = false;
                for i in (0..fields.len()).step_by(2) {
                    if extract_bulk(&fields[i]) == "content" {
                        assert_eq!(
                            extract_bulk(&fields[i + 1]),
                            "test content",
                            "GET content field should match PUT input"
                        );
                        found_content = true;
                    }
                }
                assert!(found_content, "content field not found in GET response");
            }
            other => panic!("expected Array, got {:?}", other),
        }
    }

    #[test]
    fn recall_returns_records() {
        let _ = mnemo_command("PUT", &s(&["agent-recall", "s1", "semantic", "memory one"]));
        let _ = mnemo_command("PUT", &s(&["agent-recall", "s1", "working", "memory two"]));
        let result = mnemo_command("RECALL", &s(&["agent-recall", "LIMIT", "10"]));
        match &result {
            Frame::Array(Some(entries)) => {
                assert!(entries.len() >= 2, "expected at least 2 records");
            }
            other => panic!("expected Array, got {:?}", other),
        }
    }

    #[test]
    fn recall_filters_by_kind() {
        let _ = mnemo_command(
            "PUT",
            &s(&["agent-kind-filter", "s1", "semantic", "sem mem"]),
        );
        let _ = mnemo_command(
            "PUT",
            &s(&["agent-kind-filter", "s1", "episodic", "ep mem"]),
        );
        let result = mnemo_command("RECALL", &s(&["agent-kind-filter", "KIND", "semantic"]));
        match &result {
            Frame::Array(Some(entries)) => {
                for entry in entries {
                    if let Frame::Array(Some(fields)) = entry {
                        for i in (0..fields.len()).step_by(2) {
                            if extract_bulk(&fields[i]) == "kind" {
                                assert_eq!(
                                    extract_bulk(&fields[i + 1]),
                                    "semantic",
                                    "RECALL KIND filter should return only semantic records"
                                );
                            }
                        }
                    }
                }
            }
            other => panic!("expected Array, got {:?}", other),
        }
    }

    #[test]
    fn forget_removes_agent_records() {
        let _ = mnemo_command(
            "PUT",
            &s(&["agent-forget", "s1", "semantic", "to be forgotten"]),
        );
        let result = mnemo_command("FORGET", &s(&["agent-forget"]));
        match result {
            Frame::Integer(n) => assert!(n >= 1, "expected at least 1 deleted"),
            other => panic!("expected Integer, got {:?}", other),
        }
    }

    #[test]
    fn stats_returns_total() {
        let result = mnemo_command("STATS", &[]);
        match &result {
            Frame::Array(Some(fields)) => {
                assert_eq!(
                    extract_bulk(&fields[0]),
                    "total_records",
                    "STATS first field should be total_records"
                );
            }
            other => panic!("expected Array, got {:?}", other),
        }
    }

    #[test]
    fn unknown_subcommand_errors() {
        assert!(matches!(mnemo_command("ZZZ", &[]), Frame::Error(_)));
    }

    #[test]
    fn missing_args_errors() {
        assert!(matches!(mnemo_command("PUT", &s(&["a"])), Frame::Error(_)));
        assert!(matches!(mnemo_command("GET", &[]), Frame::Error(_)));
        assert!(matches!(mnemo_command("RECALL", &[]), Frame::Error(_)));
        assert!(matches!(mnemo_command("FORGET", &[]), Frame::Error(_)));
    }

    #[test]
    fn invalid_kind_errors() {
        let result = mnemo_command("PUT", &s(&["a", "s", "invalid_kind", "content"]));
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn put_with_meta() {
        let result = mnemo_command(
            "PUT",
            &s(&[
                "agent-meta",
                "s1",
                "semantic",
                "with metadata",
                "META",
                r#"{"source":"chat"}"#,
            ]),
        );
        let id = extract_bulk(&result);
        assert_eq!(
            id.len(),
            36,
            "PUT with META should return a UUID v4 (36 chars)"
        );
    }

    #[test]
    fn help_returns_lines() {
        let result = mnemo_command("HELP", &[]);
        match &result {
            Frame::Array(Some(lines)) => assert!(!lines.is_empty()),
            other => panic!("expected Array, got {:?}", other),
        }
    }

    #[test]
    fn summarize_returns_count() {
        // Insert enough records to trigger summarization (threshold = 2)
        let _ = mnemo_command("PUT", &s(&["agent-sum", "s-sum", "semantic", "mem 1"]));
        let _ = mnemo_command("PUT", &s(&["agent-sum", "s-sum", "episodic", "mem 2"]));
        let _ = mnemo_command("PUT", &s(&["agent-sum", "s-sum", "working", "mem 3"]));

        let result = mnemo_command(
            "SUMMARIZE",
            &s(&["agent-sum", "STRATEGY", "session", "THRESHOLD", "2"]),
        );
        match result {
            Frame::Integer(n) => assert!(n >= 1, "expected at least 1 summary, got {n}"),
            other => panic!("expected Integer, got {:?}", other),
        }
    }

    #[test]
    fn summarize_below_threshold_returns_zero() {
        let _ = mnemo_command("PUT", &s(&["agent-sum-lo", "s-x", "semantic", "lone"]));
        let result = mnemo_command(
            "SUMMARIZE",
            &s(&["agent-sum-lo", "STRATEGY", "session", "THRESHOLD", "10"]),
        );
        match result {
            Frame::Integer(0) => {}
            other => panic!("expected Integer(0), got {:?}", other),
        }
    }

    #[test]
    fn summarize_missing_args_errors() {
        assert!(matches!(mnemo_command("SUMMARIZE", &[]), Frame::Error(_)));
    }

    #[test]
    fn summarize_invalid_strategy_errors() {
        let result = mnemo_command("SUMMARIZE", &s(&["agent-x", "STRATEGY", "invalid"]));
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn save_load_via_store() {
        let store = Arc::new(Store::new(1));
        let _ = mnemo_command_with_store(
            &store,
            "PUT",
            &s(&["agent-save", "s1", "semantic", "persist me"]),
        );
        assert!(matches!(
            mnemo_command_with_store(&store, "SAVE", &[]),
            Frame::Simple(_)
        ));
        assert!(matches!(
            mnemo_command_with_store(&store, "LOAD", &[]),
            Frame::Simple(_)
        ));
    }

    #[test]
    fn put_rejects_empty_agent_id() {
        let result = mnemo_command("PUT", &s(&["", "s1", "semantic", "content"]));
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn put_rejects_oversized_meta() {
        let big_meta = format!(
            "{{\"data\":\"{}\"}}",
            "x".repeat(super::super::moonshot_limits::MAX_META_LEN)
        );
        let result = mnemo_command(
            "PUT",
            &s(&["agent", "s1", "semantic", "content", "META", &big_meta]),
        );
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn moonshot_config_integration() {
        let cfg = super::super::moonshot_config::get();
        assert!(cfg.mnemo.enabled);
    }
}
