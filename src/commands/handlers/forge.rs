//! FN.* command handlers — Forge WASM in-DB functions (experimental).
//!
//! Subcommands: `FN.LOAD name hex_bytes`, `FN.DROP name`,
//! `FN.CALL name key input`, `FN.CALL_RO name key input`,
//! `FN.LIST`, `FN.STATS`, `FN.SHOW name`, `FN.VERSIONS name`,
//! `FN.PROMOTE name version`, `FN.BUDGET [rate capacity]`,
//! `FN.SAVE`, `FN.LOAD_FROM_STORE`, `FN.HELP`.
//!
//! Module registry state is persisted to the Store under
//! `__ferrite:forge:data` so it survives restarts.  See ADR-019
//! for the production replication and WASM runtime wiring.

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::{Store, Value};
use ferrite_forge::rate_limiter::CallBudget;
use ferrite_forge::telemetry::TelemetrySnapshot;
use ferrite_forge::{Module, ModuleAcl, ModuleRegistry};

use super::moonshot_limits::validate_module_name;
use super::{bulk, err_frame, ok_frame, warn_experimental};

const FORGE_STORE_KEY: &str = "__ferrite:forge:data";

static MUTATION_COUNTER: AtomicU64 = AtomicU64::new(0);
static REGISTRY: OnceLock<ModuleRegistry> = OnceLock::new();
static LOADED_FROM_STORE: OnceLock<bool> = OnceLock::new();
static CALL_BUDGET: OnceLock<CallBudget> = OnceLock::new();

fn registry() -> &'static ModuleRegistry {
    REGISTRY.get_or_init(ModuleRegistry::new)
}

fn call_budget() -> &'static CallBudget {
    CALL_BUDGET.get_or_init(|| CallBudget::new(100.0, 50))
}

#[derive(serde::Serialize, serde::Deserialize, Default)]
struct ForgeSnapshot {
    modules: Vec<Vec<u8>>,
}

fn ensure_loaded_from_store(store: &Store) {
    LOADED_FROM_STORE.get_or_init(|| {
        let key = Bytes::from(FORGE_STORE_KEY);
        if let Some(Value::String(data)) = store.get(0, &key) {
            if let Ok(snap) = serde_json::from_slice::<ForgeSnapshot>(&data) {
                let reg = registry();
                let mut restored = 0usize;
                for encoded in snap.modules {
                    if let Ok(module) = Module::decode(&encoded) {
                        reg.insert(module);
                        restored += 1;
                    }
                }
                tracing::info!("Forge: restored {restored} module(s) from Store");
            }
        }
        true
    });
}

fn persist_to_store(store: &Store) -> Result<(), String> {
    let reg = registry();
    let names = reg.names();
    let mut modules = Vec::with_capacity(names.len());
    for name in &names {
        if let Some(m) = reg.get(name) {
            let encoded = m
                .encode()
                .map_err(|e| format!("encode module '{}': {e}", name))?;
            modules.push(encoded);
        }
    }
    let snap = ForgeSnapshot { modules };
    let json = serde_json::to_vec(&snap).map_err(|e| format!("serialize forge snapshot: {e}"))?;
    store.set(
        0,
        Bytes::from(FORGE_STORE_KEY),
        Value::String(Bytes::from(json)),
    );
    Ok(())
}

/// Execute a FN.* (Forge WASM) command without Store persistence.
///
/// This variant is used for backward-compatible dispatch and testing.
/// State is held in process-local singletons — see [`forge_command_with_store`]
/// for the production entry point.
pub fn forge_command(subcommand: &str, args: &[String], _db: u8) -> Frame {
    warn_experimental("FN");
    match subcommand.to_uppercase().as_str() {
        "LOAD" => load(args),
        "DROP" => drop_module(args),
        "CALL" => call(args),
        "CALL_RO" => call(args),
        "LIST" => list(),
        "STATS" => stats(),
        "SHOW" => show(args),
        "VERSIONS" => versions(args),
        "PROMOTE" => promote(args),
        "BUDGET" => budget(args),
        "HELP" | "" => help(),
        other => err_frame(&format!("unknown FN subcommand '{}'", other)),
    }
}

/// Execute a FN.* (Forge WASM) command with Store-backed persistence.
///
/// Auto-loads state from Store on first call, and auto-persists after
/// mutating operations (LOAD, DROP, CALL, PROMOTE).
/// Use `FN.SAVE` / `FN.LOAD_FROM_STORE` for explicit persistence control.
///
/// # Subcommands
///
/// | Command | Mutating | Description |
/// |---------|----------|-------------|
/// | `FN.LOAD` | Yes | Load a WASM module |
/// | `FN.DROP` | Yes | Remove a module |
/// | `FN.CALL` | Yes | Call a module function |
/// | `FN.CALL_RO` | No | Call a module (read-only) |
/// | `FN.LIST` | No | List loaded modules |
/// | `FN.STATS` | No | Show telemetry stats |
/// | `FN.SHOW` | No | Show module info |
/// | `FN.VERSIONS` | No | List module versions |
/// | `FN.PROMOTE` | Yes | Promote a version to default |
/// | `FN.BUDGET` | No | Show/set call budget |
/// | `FN.SAVE` | No | Persist state to Store |
/// | `FN.LOAD_FROM_STORE` | No | Reload state from Store |
/// | `FN.HELP` | No | Show help |
pub fn forge_command_with_store(
    store: &Arc<Store>,
    subcommand: &str,
    args: &[String],
    db: u8,
) -> Frame {
    warn_experimental("FN");
    if !super::moonshot_config::is_enabled("FN") {
        return err_frame("ERR FN.* commands are disabled in moonshot configuration");
    }
    ensure_loaded_from_store(store);

    let upper = subcommand.to_uppercase();
    let is_mutating = matches!(upper.as_str(), "LOAD" | "DROP" | "CALL" | "PROMOTE");

    let result = match upper.as_str() {
        "LOAD" => load(args),
        "DROP" => drop_module(args),
        "CALL" => call(args),
        "CALL_RO" => call(args),
        "LIST" => list(),
        "STATS" => stats(),
        "SHOW" => show(args),
        "VERSIONS" => versions(args),
        "PROMOTE" => promote(args),
        "BUDGET" => budget(args),
        "SAVE" => match persist_to_store(store) {
            Ok(()) => ok_frame(),
            Err(e) => err_frame(&format!("save: {e}")),
        },
        "LOAD_FROM_STORE" => {
            let key = Bytes::from(FORGE_STORE_KEY);
            match store.get(0, &key) {
                Some(Value::String(data)) => match serde_json::from_slice::<ForgeSnapshot>(&data) {
                    Ok(snap) => {
                        let reg = registry();
                        for encoded in snap.modules {
                            if let Ok(module) = Module::decode(&encoded) {
                                reg.insert(module);
                            }
                        }
                        ok_frame()
                    }
                    Err(e) => err_frame(&format!("load: invalid snapshot: {e}")),
                },
                _ => err_frame("load: no forge snapshot in store"),
            }
        }
        "HELP" | "" => help(),
        other => return err_frame(&format!("unknown FN subcommand '{}'", other)),
    };

    let _ = db; // reserved for StoreHostContext in runtime builds

    if is_mutating && !matches!(result, Frame::Error(_)) && super::should_persist(&MUTATION_COUNTER)
    {
        if let Err(e) = persist_to_store(store) {
            tracing::warn!("Failed to persist forge data: {}", e);
        }
    }

    result
}

// ── Subcommand implementations ───────────────────────────────────────

fn load(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("usage: FN.LOAD name hex_bytes [read_keys write_keys]");
    }
    let name = &args[0];
    if let Err(f) = validate_module_name(name) {
        return f;
    }
    let hex_str = &args[1];
    let wasm_bytes = match hex_decode(hex_str) {
        Ok(b) => b,
        Err(e) => return err_frame(&format!("invalid hex: {e}")),
    };

    let acl = if args.len() >= 4 {
        ModuleAcl {
            read_keys: args[2].split(',').map(|s| s.to_string()).collect(),
            write_keys: args[3].split(',').map(|s| s.to_string()).collect(),
        }
    } else {
        ModuleAcl::default()
    };

    let module = Module::new(name.clone(), wasm_bytes, acl);
    let sha = module.meta.sha256.clone();
    let size = module.meta.size_bytes;
    registry().insert(module);
    ferrite_forge::telemetry::record_module_loaded();

    Frame::Array(Some(vec![
        bulk("name"),
        bulk(name.clone()),
        bulk("sha256"),
        bulk(sha),
        bulk("size"),
        Frame::Integer(size as i64),
    ]))
}

fn drop_module(args: &[String]) -> Frame {
    if args.len() != 1 {
        return err_frame("usage: FN.DROP name");
    }
    let name = &args[0];
    match registry().remove(name) {
        Some(_) => {
            ferrite_forge::telemetry::record_module_unloaded();
            ok_frame()
        }
        None => err_frame(&format!("module '{}' not found", name)),
    }
}

fn call(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("usage: FN.CALL name key [input]");
    }
    let name = &args[0];

    // Rate-limit per module name.
    if let Err(wait) = call_budget().try_acquire(name) {
        return err_frame(&format!(
            "rate limited: retry after {:.1}ms",
            wait.as_secs_f64() * 1000.0
        ));
    }

    let reg = registry();
    let Some(_module) = reg.get(name) else {
        ferrite_forge::telemetry::record_call_error(name, "module not found");
        return err_frame(&format!("module '{}' not loaded", name));
    };

    reg.increment_call_count(name);

    // Without the `forge-runtime` feature, we cannot actually execute WASM.
    // Record the call and return a stub response.
    let input = args.get(2).cloned().unwrap_or_default();
    ferrite_forge::telemetry::record_call(name, std::time::Duration::from_millis(0));
    Frame::Array(Some(vec![
        bulk("module"),
        bulk(name.clone()),
        bulk("result"),
        bulk(format!("(no runtime) input_len={}", input.len())),
    ]))
}

fn list() -> Frame {
    let names = registry().names();
    if names.is_empty() {
        return Frame::Array(Some(vec![]));
    }
    Frame::Array(Some(names.into_iter().map(bulk).collect()))
}

fn stats() -> Frame {
    let snap = TelemetrySnapshot::capture();
    let reg = registry();
    Frame::Array(Some(vec![
        bulk("modules_loaded"),
        Frame::Integer(reg.len() as i64),
        bulk("calls_total"),
        Frame::Integer(snap.calls_total as i64),
        bulk("errors_total"),
        Frame::Integer(snap.errors_total as i64),
        bulk("budget_rate"),
        bulk(format!("{:.1}", call_budget().rate())),
        bulk("budget_capacity"),
        Frame::Integer(call_budget().capacity() as i64),
    ]))
}

fn show(args: &[String]) -> Frame {
    if args.len() != 1 {
        return err_frame("usage: FN.SHOW name");
    }
    match registry().show(&args[0]) {
        Some(info) => Frame::Array(Some(vec![
            bulk("name"),
            bulk(info.name),
            bulk("version"),
            bulk(info.version),
            bulk("is_default"),
            Frame::Integer(i64::from(info.is_default)),
            bulk("loaded_at_ms"),
            Frame::Integer(info.loaded_at_ms as i64),
            bulk("call_count"),
            Frame::Integer(info.call_count as i64),
            bulk("signed_by"),
            match info.signed_by {
                Some(s) => bulk(s),
                None => Frame::Bulk(None),
            },
        ])),
        None => Frame::Bulk(None),
    }
}

fn versions(args: &[String]) -> Frame {
    if args.len() != 1 {
        return err_frame("usage: FN.VERSIONS name");
    }
    let vers = registry().versions(&args[0]);
    if vers.is_empty() {
        return Frame::Array(Some(vec![]));
    }
    let items: Vec<Frame> = vers
        .into_iter()
        .map(|v| {
            Frame::Array(Some(vec![
                bulk(v.version),
                Frame::Integer(i64::from(v.is_default)),
                Frame::Integer(v.call_count as i64),
            ]))
        })
        .collect();
    Frame::Array(Some(items))
}

fn promote(args: &[String]) -> Frame {
    if args.len() != 2 {
        return err_frame("usage: FN.PROMOTE name version");
    }
    if registry().promote(&args[0], &args[1]) {
        ok_frame()
    } else {
        err_frame(&format!(
            "version '{}' not found for module '{}'",
            args[1], args[0]
        ))
    }
}

fn budget(args: &[String]) -> Frame {
    if args.is_empty() {
        return Frame::Array(Some(vec![
            bulk("rate"),
            bulk(format!("{:.1}", call_budget().rate())),
            bulk("capacity"),
            Frame::Integer(call_budget().capacity() as i64),
        ]));
    }
    if args.len() != 2 {
        return err_frame("usage: FN.BUDGET [rate capacity]");
    }
    let rate: f64 = match args[0].parse() {
        Ok(r) if r > 0.0 => r,
        _ => return err_frame("rate must be a positive number"),
    };
    let capacity: u64 = match args[1].parse() {
        Ok(c) if c > 0 => c,
        _ => return err_frame("capacity must be a positive integer"),
    };
    call_budget().reconfigure(rate, capacity);
    ok_frame()
}

fn help() -> Frame {
    Frame::Array(Some(vec![
        bulk("FN.LOAD name hex_bytes [read_keys write_keys] — Load a WASM module"),
        bulk("FN.DROP name — Remove a module"),
        bulk("FN.CALL name key [input] — Call a module function"),
        bulk("FN.CALL_RO name key [input] — Call a module (read-only)"),
        bulk("FN.LIST — List loaded modules"),
        bulk("FN.STATS — Show telemetry statistics"),
        bulk("FN.SHOW name — Show module info"),
        bulk("FN.VERSIONS name — List module versions"),
        bulk("FN.PROMOTE name version — Promote a version to default"),
        bulk("FN.BUDGET [rate capacity] — Show/set call rate budget"),
        bulk("FN.SAVE — Persist state to Store"),
        bulk("FN.LOAD_FROM_STORE — Reload state from Store"),
        bulk("FN.HELP — Show this help"),
    ]))
}

// ── Hex decoding helper ──────────────────────────────────────────────

fn hex_decode(hex: &str) -> Result<Vec<u8>, String> {
    if hex.len() % 2 != 0 {
        return Err("odd-length hex string".to_string());
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).map_err(|e| e.to_string()))
        .collect()
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn args(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn load_and_list_round_trip() {
        let res = forge_command("LOAD", &args(&["test-mod-fl", "0061"]), 0);
        assert!(
            !matches!(&res, Frame::Error(_)),
            "LOAD should succeed: {res:?}"
        );
        let res = forge_command("LIST", &args(&[]), 0);
        assert!(
            !matches!(&res, Frame::Error(_)),
            "LIST should succeed: {res:?}"
        );
    }

    #[test]
    fn drop_unknown_module_errors() {
        let res = forge_command("DROP", &args(&["nonexistent-mod-drop"]), 0);
        assert!(
            matches!(&res, Frame::Error(_)),
            "DROP of unknown module should error: {res:?}"
        );
    }

    #[test]
    fn call_nonexistent_module_errors() {
        let res = forge_command("CALL", &args(&["nonexistent-fn", "key1", "input1"]), 0);
        assert!(
            matches!(&res, Frame::Error(_)),
            "CALL of unknown module should error: {res:?}"
        );
    }

    #[test]
    fn stats_returns_array() {
        let res = forge_command("STATS", &args(&[]), 0);
        assert!(
            matches!(&res, Frame::Array(Some(items)) if !items.is_empty()),
            "STATS should return a non-empty array: {res:?}"
        );
    }

    #[test]
    fn show_missing_returns_nil() {
        let res = forge_command("SHOW", &args(&["missing-show-mod"]), 0);
        assert!(
            matches!(&res, Frame::Bulk(None)),
            "SHOW of missing module should return nil: {res:?}"
        );
    }

    #[test]
    fn versions_missing_returns_empty() {
        let res = forge_command("VERSIONS", &args(&["missing-ver-mod"]), 0);
        assert!(
            matches!(&res, Frame::Array(Some(items)) if items.is_empty()),
            "VERSIONS of missing module should return empty array: {res:?}"
        );
    }

    #[test]
    fn help_returns_array() {
        let res = forge_command("HELP", &args(&[]), 0);
        assert!(
            matches!(&res, Frame::Array(Some(items)) if !items.is_empty()),
            "HELP should return a non-empty array: {res:?}"
        );
    }

    #[test]
    fn budget_show_returns_array() {
        let res = forge_command("BUDGET", &args(&[]), 0);
        assert!(
            matches!(&res, Frame::Array(Some(items)) if !items.is_empty()),
            "BUDGET show should return array: {res:?}"
        );
    }

    #[test]
    fn unknown_subcommand_errors() {
        let res = forge_command("BADCMD", &args(&[]), 0);
        assert!(
            matches!(&res, Frame::Error(_)),
            "unknown subcommand should error: {res:?}"
        );
    }

    #[test]
    fn load_invalid_hex_errors() {
        let res = forge_command("LOAD", &args(&["bad-hex-mod", "ZZZZ"]), 0);
        assert!(
            matches!(&res, Frame::Error(_)),
            "invalid hex should error: {res:?}"
        );
    }

    #[test]
    fn load_missing_args_errors() {
        let res = forge_command("LOAD", &args(&[]), 0);
        assert!(
            matches!(&res, Frame::Error(_)),
            "LOAD with no args should error: {res:?}"
        );
    }

    #[test]
    fn promote_missing_module_errors() {
        let res = forge_command("PROMOTE", &args(&["no-such-mod", "v2"]), 0);
        assert!(
            matches!(&res, Frame::Error(_)),
            "PROMOTE of missing module should error: {res:?}"
        );
    }

    #[test]
    fn hex_decode_valid() {
        assert_eq!(hex_decode("0061").unwrap(), vec![0x00, 0x61]);
        assert_eq!(hex_decode("ff").unwrap(), vec![0xff]);
        assert_eq!(hex_decode("").unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn hex_decode_invalid() {
        assert!(hex_decode("0").is_err()); // odd length
        assert!(hex_decode("zz").is_err()); // invalid chars
    }

    #[test]
    fn persist_and_restore_snapshot() {
        let store = Arc::new(Store::new(16));
        ensure_loaded_from_store(&store);

        let reg = registry();
        reg.insert(Module::new(
            "snap-test-mod",
            vec![0, 1, 2],
            ModuleAcl::default(),
        ));
        let res = persist_to_store(&store);
        assert!(res.is_ok(), "persist should succeed: {res:?}");

        let key = Bytes::from(FORGE_STORE_KEY);
        assert!(store.get(0, &key).is_some(), "snapshot should be in store");
    }
}
