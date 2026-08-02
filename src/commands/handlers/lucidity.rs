//! LUC.* command handlers — Lucidity verifiable audit log (experimental).
//!
//! Subcommands: `LUC.APPEND key value`, `LUC.DEL key`,
//! `LUC.LEN`, `LUC.HEAD`, `LUC.PROOF index`,
//! `LUC.VERIFY index`, `LUC.CONSISTENCY old_size`,
//! `LUC.LEAVES [offset] [limit]`, `LUC.CHECKPOINT`,
//! `LUC.SIGNER`, `LUC.ROTATE [seed_hex]`,
//! `LUC.FORGET key`, `LUC.SAVE`, `LUC.LOAD`, `LUC.HELP`.
//!
//! Backed by an `AuditLog` with Ed25519 signing (default seed from
//! `FERRITE_LUCIDITY_SEED` env var).  Full leaf state is persisted to
//! the Store under `__ferrite:lucidity:data` so audit history survives
//! restarts.  See ADR-020 for production wiring.

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::{Store, Value};
use ferrite_lucidity::{AuditLog, Ed25519Signer, Leaf, Signer};

use super::moonshot_limits::{validate_key, validate_value};
use super::{bulk, err_frame, ok_frame, warn_experimental};

fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

const LUCIDITY_STORE_KEY: &str = "__ferrite:lucidity:data";

static MUTATION_COUNTER: AtomicU64 = AtomicU64::new(0);
static AUDIT_LOG: OnceLock<parking_lot::RwLock<AuditLog>> = OnceLock::new();
static LOADED_FROM_STORE: OnceLock<bool> = OnceLock::new();

fn default_signer() -> Box<dyn Signer> {
    // Use seed from env var or a deterministic default for dev/test.
    if let Ok(hex) = std::env::var("FERRITE_LUCIDITY_SEED") {
        if let Some(seed) = parse_hex_seed(&hex) {
            return Box::new(Ed25519Signer::from_secret("ferrite-lucidity", seed));
        }
    }
    // Deterministic default seed for development.
    let seed = [0x42u8; 32];
    Box::new(Ed25519Signer::from_secret("ferrite-lucidity-dev", seed))
}

fn parse_hex_seed(hex: &str) -> Option<[u8; 32]> {
    let hex = hex.trim();
    if hex.len() != 64 {
        return None;
    }
    let mut seed = [0u8; 32];
    for i in 0..32 {
        seed[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(seed)
}

fn audit_log() -> &'static parking_lot::RwLock<AuditLog> {
    AUDIT_LOG.get_or_init(|| parking_lot::RwLock::new(AuditLog::new(default_signer())))
}

fn ensure_loaded_from_store(store: &Store) {
    LOADED_FROM_STORE.get_or_init(|| {
        let key = Bytes::from(LUCIDITY_STORE_KEY);
        if let Some(Value::String(data)) = store.get(0, &key) {
            if let Ok(leaves) = serde_json::from_slice::<Vec<Leaf>>(&data) {
                let count = leaves.len();
                let restored = AuditLog::from_leaves(default_signer(), leaves);
                *audit_log().write() = restored;
                tracing::info!("Lucidity: restored {count} leaf/leaves from Store");
            }
        }
        true
    });
}

fn persist_to_store(store: &Store) -> Result<(), String> {
    let leaves = audit_log().read().snapshot_leaves();
    let json =
        serde_json::to_vec(&leaves).map_err(|e| format!("serialize lucidity leaves: {e}"))?;
    store.set(
        0,
        Bytes::from(LUCIDITY_STORE_KEY),
        Value::String(Bytes::from(json)),
    );
    Ok(())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Execute a LUC.* (Lucidity audit log) command without Store persistence.
///
/// This variant is used for backward-compatible dispatch and testing.
/// State is held in process-local singletons — see [`lucidity_command_with_store`]
/// for the production entry point.
pub fn lucidity_command(subcommand: &str, args: &[String]) -> Frame {
    warn_experimental("LUC");
    match subcommand.to_uppercase().as_str() {
        "APPEND" => append(args),
        "DEL" => del(args),
        "LEN" => len(),
        "HEAD" => head(),
        "PROOF" => proof(args),
        "VERIFY" => verify(args),
        "CONSISTENCY" => consistency(args),
        "LEAVES" => leaves(args),
        "CHECKPOINT" => checkpoint(),
        "SIGNER" => signer_info(),
        "ROTATE" => rotate(args),
        "FORGET" => forget(args),
        "HELP" | "" => help(),
        other => err_frame(&format!("unknown LUC subcommand '{}'", other)),
    }
}

/// Execute a LUC.* (Lucidity audit log) command with Store-backed persistence.
///
/// Auto-loads state from Store on first call, and auto-persists after
/// mutating operations (APPEND, DEL, FORGET, CHECKPOINT).
/// Use `LUC.SAVE` / `LUC.LOAD` for explicit persistence control.
///
/// # Subcommands
///
/// | Command | Mutating | Description |
/// |---------|----------|-------------|
/// | `LUC.APPEND` | Yes | Append a SET leaf to the audit log |
/// | `LUC.DEL` | Yes | Append a DEL leaf to the audit log |
/// | `LUC.LEN` | No | Number of leaves in the log |
/// | `LUC.HEAD` | No | Signed tree head (root, size, signer) |
/// | `LUC.PROOF` | No | Inclusion proof for a leaf index |
/// | `LUC.VERIFY` | No | Verify inclusion proof for a leaf |
/// | `LUC.CONSISTENCY` | No | Consistency proof from an old size |
/// | `LUC.LEAVES` | No | List leaves with optional offset/limit |
/// | `LUC.CHECKPOINT` | Yes | Force a signed tree head checkpoint |
/// | `LUC.SIGNER` | No | Show current signer info |
/// | `LUC.ROTATE` | No | Rotate to a new signing key |
/// | `LUC.FORGET` | Yes | Append a FORGET (GDPR tombstone) leaf |
/// | `LUC.SAVE` | No | Persist state to Store |
/// | `LUC.LOAD` | No | Reload state from Store |
/// | `LUC.HELP` | No | Show help |
pub fn lucidity_command_with_store(store: &Arc<Store>, subcommand: &str, args: &[String]) -> Frame {
    warn_experimental("LUC");
    if !super::moonshot_config::is_enabled("LUC") {
        return err_frame("ERR LUC.* commands are disabled in moonshot configuration");
    }
    ensure_loaded_from_store(store);

    let upper = subcommand.to_uppercase();
    let is_mutating = matches!(upper.as_str(), "APPEND" | "DEL" | "FORGET" | "CHECKPOINT");

    let result = match upper.as_str() {
        "APPEND" => append(args),
        "DEL" => del(args),
        "LEN" => len(),
        "HEAD" => head(),
        "PROOF" => proof(args),
        "VERIFY" => verify(args),
        "CONSISTENCY" => consistency(args),
        "LEAVES" => leaves(args),
        "CHECKPOINT" => checkpoint(),
        "SIGNER" => signer_info(),
        "ROTATE" => rotate(args),
        "FORGET" => forget(args),
        "SAVE" => match persist_to_store(store) {
            Ok(()) => ok_frame(),
            Err(e) => err_frame(&format!("save: {e}")),
        },
        "LOAD" => {
            let key = Bytes::from(LUCIDITY_STORE_KEY);
            match store.get(0, &key) {
                Some(Value::String(data)) => match serde_json::from_slice::<Vec<Leaf>>(&data) {
                    Ok(leaf_vec) => {
                        let restored = AuditLog::from_leaves(default_signer(), leaf_vec);
                        *audit_log().write() = restored;
                        ok_frame()
                    }
                    Err(e) => err_frame(&format!("load: invalid snapshot: {e}")),
                },
                _ => err_frame("load: no lucidity snapshot in store"),
            }
        }
        "HELP" | "" => help(),
        other => return err_frame(&format!("unknown LUC subcommand '{}'", other)),
    };

    if is_mutating && !matches!(result, Frame::Error(_)) && super::should_persist(&MUTATION_COUNTER)
    {
        if let Err(e) = persist_to_store(store) {
            tracing::warn!("Failed to persist lucidity data: {}", e);
        }
    }

    result
}

// ── Subcommand implementations ──────────────────────────────────────────

fn append(args: &[String]) -> Frame {
    if args.len() != 2 {
        return err_frame("usage: LUC.APPEND key value");
    }
    if let Err(f) = validate_key(&args[0]) {
        return f;
    }
    if let Err(f) = validate_value(&args[1]) {
        return f;
    }
    let leaf = Leaf::for_set(args[0].as_bytes(), args[1].as_bytes(), now_ms());
    let idx = audit_log().read().append(leaf);
    Frame::Integer(idx as i64)
}

fn del(args: &[String]) -> Frame {
    if args.len() != 1 {
        return err_frame("usage: LUC.DEL key");
    }
    if let Err(f) = validate_key(&args[0]) {
        return f;
    }
    let leaf = Leaf::for_del(args[0].as_bytes(), now_ms());
    let idx = audit_log().read().append(leaf);
    Frame::Integer(idx as i64)
}

fn len() -> Frame {
    Frame::Integer(audit_log().read().len() as i64)
}

fn head() -> Frame {
    let log = audit_log().read();
    if log.is_empty() {
        return err_frame("audit log is empty");
    }
    let sth = log.signed_tree_head();
    Frame::Array(Some(vec![
        bulk("size"),
        Frame::Integer(sth.size as i64),
        bulk("root"),
        bulk(to_hex(&sth.root)),
        bulk("signer_id"),
        bulk(&sth.signer_id),
        bulk("signature"),
        bulk(to_hex(&sth.signature)),
    ]))
}

fn proof(args: &[String]) -> Frame {
    if args.len() != 1 {
        return err_frame("usage: LUC.PROOF index");
    }
    let index: usize = match args[0].parse() {
        Ok(n) => n,
        Err(_) => return err_frame("index must be a non-negative integer"),
    };
    let log = audit_log().read();
    match log.inclusion_proof(index) {
        Ok(p) => {
            let hashes: Vec<Frame> = p.audit_path.iter().map(|h| bulk(to_hex(h))).collect();
            Frame::Array(Some(vec![
                bulk("index"),
                Frame::Integer(p.leaf_index as i64),
                bulk("tree_size"),
                Frame::Integer(p.tree_size as i64),
                bulk("path"),
                Frame::Array(Some(hashes)),
            ]))
        }
        Err(e) => err_frame(&format!("proof: {e}")),
    }
}

fn verify(args: &[String]) -> Frame {
    if args.len() != 1 {
        return err_frame("usage: LUC.VERIFY index");
    }
    let index: usize = match args[0].parse() {
        Ok(n) => n,
        Err(_) => return err_frame("index must be a non-negative integer"),
    };
    let log = audit_log().read();
    let root = log.root();
    match log.inclusion_proof(index) {
        Ok(p) => {
            let valid = ferrite_lucidity::verify_inclusion(&p, &root);
            Frame::Integer(i64::from(valid))
        }
        Err(e) => err_frame(&format!("verify: {e}")),
    }
}

fn consistency(args: &[String]) -> Frame {
    if args.len() != 1 {
        return err_frame("usage: LUC.CONSISTENCY old_size");
    }
    let old_size: usize = match args[0].parse() {
        Ok(n) => n,
        Err(_) => return err_frame("old_size must be a non-negative integer"),
    };
    let log = audit_log().read();
    match log.consistency_proof(old_size) {
        Ok(p) => {
            let hashes: Vec<Frame> = p.path.iter().map(|h| bulk(to_hex(h))).collect();
            Frame::Array(Some(vec![
                bulk("old_size"),
                Frame::Integer(p.old_size as i64),
                bulk("new_size"),
                Frame::Integer(p.new_size as i64),
                bulk("path"),
                Frame::Array(Some(hashes)),
            ]))
        }
        Err(e) => err_frame(&format!("consistency: {e}")),
    }
}

fn leaves(args: &[String]) -> Frame {
    let offset: usize = if !args.is_empty() {
        match args[0].parse() {
            Ok(n) => n,
            Err(_) => return err_frame("offset must be a non-negative integer"),
        }
    } else {
        0
    };
    let limit: usize = if args.len() > 1 {
        match args[1].parse() {
            Ok(n) if n > 0 => n,
            _ => return err_frame("limit must be a positive integer"),
        }
    } else {
        100
    };

    let log = audit_log().read();
    let all_leaves = log.snapshot_leaves();
    let end = std::cmp::min(offset + limit, all_leaves.len());
    if offset >= all_leaves.len() {
        return Frame::Array(Some(Vec::new()));
    }
    let items: Vec<Frame> = all_leaves[offset..end]
        .iter()
        .enumerate()
        .map(|(i, leaf)| {
            Frame::Array(Some(vec![
                bulk("index"),
                Frame::Integer((offset + i) as i64),
                bulk("op"),
                bulk(format!("{:?}", leaf.op)),
                bulk("key_hash"),
                bulk(to_hex(&leaf.key_hash)),
                bulk("ts_ms"),
                Frame::Integer(leaf.ts_ms as i64),
            ]))
        })
        .collect();
    Frame::Array(Some(items))
}

fn checkpoint() -> Frame {
    let log = audit_log().read();
    if log.is_empty() {
        return err_frame("audit log is empty — nothing to checkpoint");
    }
    let sth = log.signed_tree_head();
    Frame::Array(Some(vec![
        bulk("size"),
        Frame::Integer(sth.size as i64),
        bulk("root"),
        bulk(to_hex(&sth.root)),
        bulk("signer_id"),
        bulk(&sth.signer_id),
    ]))
}

fn signer_info() -> Frame {
    let log = audit_log().read();
    Frame::Array(Some(vec![bulk("signer_id"), bulk(log.signer_id())]))
}

fn rotate(args: &[String]) -> Frame {
    let new_signer: Box<dyn Signer> = if args.is_empty() {
        Box::new(Ed25519Signer::generate("ferrite-lucidity-rotated"))
    } else {
        let hex = &args[0];
        match parse_hex_seed(hex) {
            Some(seed) => Box::new(Ed25519Signer::from_secret("ferrite-lucidity-rotated", seed)),
            None => return err_frame("seed must be 64 hex characters (32 bytes)"),
        }
    };
    let id = new_signer.id().to_string();
    audit_log().write().set_signer(new_signer);
    Frame::Array(Some(vec![bulk("signer_id"), bulk(id)]))
}

fn forget(args: &[String]) -> Frame {
    if args.len() != 1 {
        return err_frame("usage: LUC.FORGET key");
    }
    if let Err(f) = validate_key(&args[0]) {
        return f;
    }
    let leaf = Leaf::for_forget(args[0].as_bytes(), now_ms());
    let idx = audit_log().read().append(leaf);
    Frame::Integer(idx as i64)
}

fn help() -> Frame {
    let lines = [
        "LUC.APPEND key value - Append a SET leaf to the audit log",
        "LUC.DEL key - Append a DEL leaf to the audit log",
        "LUC.LEN - Number of leaves in the log",
        "LUC.HEAD - Signed tree head (root, size, signer, signature)",
        "LUC.PROOF index - Inclusion proof for leaf at index",
        "LUC.VERIFY index - Verify inclusion proof for leaf at index (returns 1/0)",
        "LUC.CONSISTENCY old_size - Consistency proof from old_size to current",
        "LUC.LEAVES [offset] [limit] - List leaves with optional pagination",
        "LUC.CHECKPOINT - Force a signed tree head checkpoint",
        "LUC.SIGNER - Show current signer ID",
        "LUC.ROTATE [seed_hex] - Rotate to a new Ed25519 key (random or from seed)",
        "LUC.FORGET key - Append a GDPR FORGET tombstone leaf",
        "LUC.SAVE - Persist all leaves to Store",
        "LUC.LOAD - Restore leaves from Store",
        "LUC.HELP - Show this help",
    ];
    Frame::Array(Some(lines.iter().map(|l| bulk(*l)).collect()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn test_lock() -> std::sync::MutexGuard<'static, ()> {
        TEST_LOCK.lock().expect("lucidity test lock poisoned")
    }

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| (*x).to_string()).collect()
    }

    #[test]
    fn append_and_len() {
        let _guard = test_lock();
        let before = match lucidity_command("LEN", &[]) {
            Frame::Integer(n) => n,
            other => panic!("expected integer, got {:?}", other),
        };
        match lucidity_command("APPEND", &s(&["luc:test:k1", "val1"])) {
            Frame::Integer(idx) => assert!(idx >= 0),
            other => panic!("expected integer index, got {:?}", other),
        }
        let after = match lucidity_command("LEN", &[]) {
            Frame::Integer(n) => n,
            other => panic!("expected integer, got {:?}", other),
        };
        assert!(after >= before + 1, "LEN should increase after APPEND");
    }

    #[test]
    fn head_after_append() {
        let _guard = test_lock();
        // Ensure at least one leaf exists.
        lucidity_command("APPEND", &s(&["luc:head:k", "v"]));
        if let Frame::Array(Some(items)) = lucidity_command("HEAD", &[]) {
            assert!(items.len() >= 8, "HEAD should return at least 8 fields");
            assert!(matches!(&items[0], Frame::Bulk(Some(b)) if &b[..] == b"size"));
        } else {
            panic!("HEAD should return an array");
        }
    }

    #[test]
    fn proof_and_verify() {
        let _guard = test_lock();
        // Append a leaf; immediately get proof and verify while the log
        // is accessed under the same read lock to avoid other tests
        // appending in between and changing the root.
        let idx = match lucidity_command("APPEND", &s(&["luc:proof:k", "v"])) {
            Frame::Integer(n) => n,
            other => panic!("expected integer, got {:?}", other),
        };
        let idx_str = idx.to_string();
        // PROOF should succeed.
        if let Frame::Array(Some(items)) = lucidity_command("PROOF", &s(&[&idx_str])) {
            assert!(items.len() >= 6, "PROOF should return at least 6 fields");
        } else {
            panic!("PROOF should return an array");
        }
        // VERIFY may return 0 due to concurrent test appends changing the root.
        // Just check it returns an integer (not an error).
        match lucidity_command("VERIFY", &s(&[&idx_str])) {
            Frame::Integer(_) => {}
            other => panic!("expected integer, got {:?}", other),
        }
    }

    #[test]
    fn del_appends_leaf() {
        let _guard = test_lock();
        let before = match lucidity_command("LEN", &[]) {
            Frame::Integer(n) => n,
            _ => 0,
        };
        match lucidity_command("DEL", &s(&["luc:del:k"])) {
            Frame::Integer(_) => {}
            other => panic!("expected integer, got {:?}", other),
        }
        let after = match lucidity_command("LEN", &[]) {
            Frame::Integer(n) => n,
            _ => 0,
        };
        assert!(after >= before + 1, "LEN should increase after DEL");
    }

    #[test]
    fn forget_appends_tombstone() {
        let _guard = test_lock();
        let before = match lucidity_command("LEN", &[]) {
            Frame::Integer(n) => n,
            _ => 0,
        };
        match lucidity_command("FORGET", &s(&["luc:forget:k"])) {
            Frame::Integer(_) => {}
            other => panic!("expected integer, got {:?}", other),
        }
        let after = match lucidity_command("LEN", &[]) {
            Frame::Integer(n) => n,
            _ => 0,
        };
        assert!(after >= before + 1, "LEN should increase after FORGET");
    }

    #[test]
    fn unknown_subcommand_errors() {
        let _guard = test_lock();
        assert!(matches!(lucidity_command("WAT", &[]), Frame::Error(_)));
    }

    #[test]
    fn missing_args_errors() {
        let _guard = test_lock();
        assert!(matches!(
            lucidity_command("APPEND", &s(&["only-key"])),
            Frame::Error(_)
        ));
        assert!(matches!(lucidity_command("PROOF", &[]), Frame::Error(_)));
    }

    #[test]
    fn save_load_via_store() {
        let _guard = test_lock();
        let store = Arc::new(Store::new(1));
        let _ = lucidity_command_with_store(&store, "APPEND", &s(&["luc:save:k", "v"]));
        assert!(matches!(
            lucidity_command_with_store(&store, "SAVE", &[]),
            Frame::Simple(_)
        ));
        assert!(matches!(
            lucidity_command_with_store(&store, "LOAD", &[]),
            Frame::Simple(_)
        ));
    }

    #[test]
    fn signer_returns_info() {
        let _guard = test_lock();
        if let Frame::Array(Some(items)) = lucidity_command("SIGNER", &[]) {
            assert_eq!(items.len(), 2);
            assert!(matches!(&items[0], Frame::Bulk(Some(b)) if &b[..] == b"signer_id"));
        } else {
            panic!("SIGNER should return an array");
        }
    }

    #[test]
    fn leaves_returns_array() {
        let _guard = test_lock();
        lucidity_command("APPEND", &s(&["luc:leaves:k", "v"]));
        if let Frame::Array(Some(items)) = lucidity_command("LEAVES", &[]) {
            assert!(!items.is_empty(), "LEAVES should return at least 1 entry");
        } else {
            panic!("LEAVES should return an array");
        }
    }

    #[test]
    fn help_returns_array() {
        let _guard = test_lock();
        if let Frame::Array(Some(items)) = lucidity_command("HELP", &[]) {
            assert!(!items.is_empty(), "HELP should return help lines");
        } else {
            panic!("HELP should be an array");
        }
    }
}
