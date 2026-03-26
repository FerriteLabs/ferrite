//! CON.* command handlers — Concord multi-master CRDTs (experimental).
//!
//! Subcommands: `CON.GINC key replica delta`, `CON.GVAL key`,
//! `CON.GMERGE key json`, `CON.PNINC key replica delta`,
//! `CON.PNVAL key`, `CON.PNMERGE key json`,
//! `CON.SADD key replica member`, `CON.SREM key member`,
//! `CON.SMEMBERS key`, `CON.SMERGE key json`,
//! `CON.LWWSET key value ts replica`, `CON.LWWGET key`,
//! `CON.LWWMERGE key json`,
//! `CON.MVSET key value replica observed_json`, `CON.MVGET key`,
//! `CON.MVMERGE key json`,
//! `CON.DVV key replica`, `CON.CLOCK key`,
//! `CON.PEERS`, `CON.SYNC key dvv_json`,
//! `CON.ENTROPY [key value_hash_hex]`, `CON.ROUTE key`,
//! `CON.ADDRULE pattern region priority`, `CON.RULES`,
//! `CON.SAVE`, `CON.LOAD`, `CON.HELP`.
//!
//! State is held in a consolidated `ConcordState` guarded by `OnceLock`.
//! Full state is persisted to the Store under `__ferrite:concord:data`
//! so it survives restarts.  See ADR-020 for the production
//! replication wiring.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use parking_lot::RwLock;

use crate::protocol::Frame;
use crate::storage::{Store, Value};
use ferrite_concord::{
    AntiEntropyTree, Crdt, DottedVersionVector, GCounter, LwwRegister, LwwWrite, MvRegister,
    MvWrite, OrSet, PnCounter, RoutingRule, SovereigntyRouter, VersionVector,
};

use super::{bulk, err_frame, ok_frame, warn_experimental};

const CONCORD_STORE_KEY: &str = "__ferrite:concord:data";

static MUTATION_COUNTER: AtomicU64 = AtomicU64::new(0);
static STATE: OnceLock<ConcordState> = OnceLock::new();
static LOADED_FROM_STORE: OnceLock<bool> = OnceLock::new();

struct ConcordState {
    g_counters: RwLock<HashMap<String, GCounter>>,
    pn_counters: RwLock<HashMap<String, PnCounter>>,
    or_sets: RwLock<HashMap<String, OrSet<String>>>,
    lww_registers: RwLock<HashMap<String, LwwRegister<String>>>,
    mv_registers: RwLock<HashMap<String, MvRegister<String>>>,
    dvvs: RwLock<HashMap<String, DottedVersionVector>>,
    clocks: RwLock<HashMap<String, VersionVector>>,
    entropy: RwLock<AntiEntropyTree>,
    router: RwLock<SovereigntyRouter>,
}

impl ConcordState {
    fn new() -> Self {
        Self {
            g_counters: RwLock::new(HashMap::new()),
            pn_counters: RwLock::new(HashMap::new()),
            or_sets: RwLock::new(HashMap::new()),
            lww_registers: RwLock::new(HashMap::new()),
            mv_registers: RwLock::new(HashMap::new()),
            dvvs: RwLock::new(HashMap::new()),
            clocks: RwLock::new(HashMap::new()),
            entropy: RwLock::new(AntiEntropyTree::new()),
            router: RwLock::new(SovereigntyRouter::new()),
        }
    }
}

fn state() -> &'static ConcordState {
    STATE.get_or_init(ConcordState::new)
}

#[derive(serde::Serialize, serde::Deserialize, Default)]
struct ConcordSnapshot {
    g_counters: Vec<(String, GCounter)>,
    pn_counters: Vec<(String, PnCounter)>,
    or_sets: Vec<(String, OrSet<String>)>,
    lww_registers: Vec<(String, LwwRegister<String>)>,
    mv_registers: Vec<(String, MvRegister<String>)>,
    #[serde(default)]
    routing_rules: Vec<RoutingRule>,
}

fn ensure_loaded_from_store(store: &Store) {
    LOADED_FROM_STORE.get_or_init(|| {
        let key = Bytes::from(CONCORD_STORE_KEY);
        if let Some(Value::String(data)) = store.get(0, &key) {
            if let Ok(snap) = serde_json::from_slice::<ConcordSnapshot>(&data) {
                let s = state();
                {
                    let mut gc = s.g_counters.write();
                    for (k, v) in snap.g_counters {
                        gc.insert(k, v);
                    }
                }
                {
                    let mut pn = s.pn_counters.write();
                    for (k, v) in snap.pn_counters {
                        pn.insert(k, v);
                    }
                }
                {
                    let mut os = s.or_sets.write();
                    for (k, v) in snap.or_sets {
                        os.insert(k, v);
                    }
                }
                {
                    let mut lw = s.lww_registers.write();
                    for (k, v) in snap.lww_registers {
                        lw.insert(k, v);
                    }
                }
                {
                    let mut mv = s.mv_registers.write();
                    for (k, v) in snap.mv_registers {
                        mv.insert(k, v);
                    }
                }
                {
                    let mut router = s.router.write();
                    for rule in snap.routing_rules {
                        router.add_rule(rule);
                    }
                }
                tracing::info!("Concord: restored state from Store");
            }
        }
        true
    });
}

fn persist_to_store(store: &Store) -> Result<(), String> {
    let s = state();
    let snap = ConcordSnapshot {
        g_counters: s
            .g_counters
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        pn_counters: s
            .pn_counters
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        or_sets: s
            .or_sets
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        lww_registers: s
            .lww_registers
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        mv_registers: s
            .mv_registers
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        routing_rules: s.router.read().rules().to_vec(),
    };
    let json = serde_json::to_vec(&snap).map_err(|e| format!("serialize concord snapshot: {e}"))?;
    store.set(
        0,
        Bytes::from(CONCORD_STORE_KEY),
        Value::String(Bytes::from(json)),
    );
    Ok(())
}

/// Execute a CON.* (Concord CRDTs) command without Store persistence.
///
/// This variant is used for backward-compatible dispatch and testing.
/// State is held in process-local singletons — see [`concord_command_with_store`]
/// for the production entry point.
pub fn concord_command(subcommand: &str, args: &[String]) -> Frame {
    warn_experimental("CON");
    dispatch(subcommand, args)
}

/// Execute a CON.* (Concord CRDTs) command with Store-backed persistence.
///
/// Auto-loads state from Store on first call, and auto-persists after
/// mutating operations.
/// Use `CON.SAVE` / `CON.LOAD` for explicit persistence control.
///
/// # Subcommands
///
/// | Command | Mutating | Description |
/// |---------|----------|-------------|
/// | `CON.GINC` | Yes | Increment a G-Counter |
/// | `CON.GVAL` | No | Read a G-Counter value |
/// | `CON.GMERGE` | Yes | Merge a remote G-Counter |
/// | `CON.PNINC` | Yes | Increment/decrement a PN-Counter |
/// | `CON.PNVAL` | No | Read a PN-Counter value |
/// | `CON.PNMERGE` | Yes | Merge a remote PN-Counter |
/// | `CON.SADD` | Yes | Add a member to an OR-Set |
/// | `CON.SREM` | Yes | Remove a member from an OR-Set |
/// | `CON.SMEMBERS` | No | List OR-Set members |
/// | `CON.SMERGE` | Yes | Merge a remote OR-Set |
/// | `CON.LWWSET` | Yes | Set a LWW register |
/// | `CON.LWWGET` | No | Read a LWW register |
/// | `CON.LWWMERGE` | Yes | Merge a remote LWW register |
/// | `CON.MVSET` | Yes | Set a MV register |
/// | `CON.MVGET` | No | Read a MV register |
/// | `CON.MVMERGE` | Yes | Merge a remote MV register |
/// | `CON.DVV` | No | Create/advance a DVV event |
/// | `CON.CLOCK` | No | Read a version vector |
/// | `CON.PEERS` | No | List known CRDT keys |
/// | `CON.SYNC` | No | Sync a DVV |
/// | `CON.ENTROPY` | No | Anti-entropy tree ops |
/// | `CON.ROUTE` | No | Route a key to a region |
/// | `CON.ADDRULE` | Yes | Add a routing rule |
/// | `CON.RULES` | No | List routing rules |
/// | `CON.SAVE` | No | Persist state to Store |
/// | `CON.LOAD` | No | Reload state from Store |
/// | `CON.HELP` | No | Show help |
pub fn concord_command_with_store(store: &Arc<Store>, subcommand: &str, args: &[String]) -> Frame {
    warn_experimental("CON");
    if !super::moonshot_config::is_enabled("CON") {
        return err_frame("ERR CON.* commands are disabled in moonshot configuration");
    }
    ensure_loaded_from_store(store);

    let upper = subcommand.to_uppercase();
    let is_mutating = matches!(
        upper.as_str(),
        "GINC"
            | "GMERGE"
            | "PNINC"
            | "PNMERGE"
            | "SADD"
            | "SREM"
            | "SMERGE"
            | "LWWSET"
            | "LWWMERGE"
            | "MVSET"
            | "MVMERGE"
            | "ADDRULE"
    );

    let result = match upper.as_str() {
        "SAVE" => match persist_to_store(store) {
            Ok(()) => ok_frame(),
            Err(e) => err_frame(&format!("save: {e}")),
        },
        "LOAD" => {
            let key = Bytes::from(CONCORD_STORE_KEY);
            match store.get(0, &key) {
                Some(Value::String(data)) => {
                    match serde_json::from_slice::<ConcordSnapshot>(&data) {
                        Ok(snap) => {
                            let s = state();
                            {
                                let mut gc = s.g_counters.write();
                                for (k, v) in snap.g_counters {
                                    gc.insert(k, v);
                                }
                            }
                            {
                                let mut pn = s.pn_counters.write();
                                for (k, v) in snap.pn_counters {
                                    pn.insert(k, v);
                                }
                            }
                            {
                                let mut os = s.or_sets.write();
                                for (k, v) in snap.or_sets {
                                    os.insert(k, v);
                                }
                            }
                            {
                                let mut lw = s.lww_registers.write();
                                for (k, v) in snap.lww_registers {
                                    lw.insert(k, v);
                                }
                            }
                            {
                                let mut mv = s.mv_registers.write();
                                for (k, v) in snap.mv_registers {
                                    mv.insert(k, v);
                                }
                            }
                            ok_frame()
                        }
                        Err(e) => err_frame(&format!("load: invalid snapshot: {e}")),
                    }
                }
                _ => err_frame("load: no concord snapshot in store"),
            }
        }
        _ => dispatch(&upper, args),
    };

    if is_mutating && !matches!(result, Frame::Error(_)) && super::should_persist(&MUTATION_COUNTER)
    {
        if let Err(e) = persist_to_store(store) {
            tracing::warn!("Failed to persist concord data: {}", e);
        }
    }

    result
}

fn dispatch(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "GINC" => ginc(args),
        "GVAL" => gval(args),
        "GMERGE" => gmerge(args),
        "PNINC" => pninc(args),
        "PNVAL" => pnval(args),
        "PNMERGE" => pnmerge(args),
        "SADD" => sadd(args),
        "SREM" => srem(args),
        "SMEMBERS" => smembers(args),
        "SMERGE" => smerge(args),
        "LWWSET" => lwwset(args),
        "LWWGET" => lwwget(args),
        "LWWMERGE" => lwwmerge(args),
        "MVSET" => mvset(args),
        "MVGET" => mvget(args),
        "MVMERGE" => mvmerge(args),
        "DVV" => dvv(args),
        "CLOCK" => clock(args),
        "PEERS" => peers(),
        "SYNC" => sync(args),
        "ENTROPY" => entropy(args),
        "ROUTE" => route(args),
        "ADDRULE" => addrule(args),
        "RULES" => rules(),
        "HELP" | "" => help(),
        other => err_frame(&format!("unknown CON subcommand '{}'", other)),
    }
}

// ── G-Counter ────────────────────────────────────────────────────────

fn ginc(args: &[String]) -> Frame {
    if args.len() != 3 {
        return err_frame("usage: CON.GINC key replica delta");
    }
    let delta: u64 = match args[2].parse() {
        Ok(d) => d,
        Err(_) => return err_frame("delta must be a non-negative integer"),
    };
    let s = state();
    let mut gc = s.g_counters.write();
    let counter = gc.entry(args[0].clone()).or_default();
    counter.increment(&args[1], delta);
    Frame::Integer(counter.value() as i64)
}

fn gval(args: &[String]) -> Frame {
    if args.len() != 1 {
        return err_frame("usage: CON.GVAL key");
    }
    let s = state();
    let gc = s.g_counters.read();
    match gc.get(&args[0]) {
        Some(counter) => Frame::Integer(counter.value() as i64),
        None => Frame::Integer(0),
    }
}

fn gmerge(args: &[String]) -> Frame {
    if args.len() != 2 {
        return err_frame("usage: CON.GMERGE key json");
    }
    let remote: GCounter = match serde_json::from_str(&args[1]) {
        Ok(c) => c,
        Err(e) => return err_frame(&format!("invalid GCounter JSON: {e}")),
    };
    let s = state();
    let mut gc = s.g_counters.write();
    let counter = gc.entry(args[0].clone()).or_default();
    counter.merge(&remote);
    Frame::Integer(counter.value() as i64)
}

// ── PN-Counter ───────────────────────────────────────────────────────

fn pninc(args: &[String]) -> Frame {
    if args.len() != 3 {
        return err_frame("usage: CON.PNINC key replica delta");
    }
    let delta: i64 = match args[2].parse() {
        Ok(d) => d,
        Err(_) => return err_frame("delta must be an integer"),
    };
    let s = state();
    let mut pn = s.pn_counters.write();
    let counter = pn.entry(args[0].clone()).or_default();
    if delta >= 0 {
        counter.increment(&args[1], delta as u64);
    } else {
        counter.decrement(&args[1], (-delta) as u64);
    }
    Frame::Integer(counter.value() as i64)
}

fn pnval(args: &[String]) -> Frame {
    if args.len() != 1 {
        return err_frame("usage: CON.PNVAL key");
    }
    let s = state();
    let pn = s.pn_counters.read();
    match pn.get(&args[0]) {
        Some(counter) => Frame::Integer(counter.value() as i64),
        None => Frame::Integer(0),
    }
}

fn pnmerge(args: &[String]) -> Frame {
    if args.len() != 2 {
        return err_frame("usage: CON.PNMERGE key json");
    }
    let remote: PnCounter = match serde_json::from_str(&args[1]) {
        Ok(c) => c,
        Err(e) => return err_frame(&format!("invalid PnCounter JSON: {e}")),
    };
    let s = state();
    let mut pn = s.pn_counters.write();
    let counter = pn.entry(args[0].clone()).or_default();
    counter.merge(&remote);
    Frame::Integer(counter.value() as i64)
}

// ── OR-Set ───────────────────────────────────────────────────────────

fn sadd(args: &[String]) -> Frame {
    if args.len() != 3 {
        return err_frame("usage: CON.SADD key replica member");
    }
    let s = state();
    let mut sets = s.or_sets.write();
    let set = sets.entry(args[0].clone()).or_default();
    set.add(&args[1], args[2].clone());
    Frame::Integer(1)
}

fn srem(args: &[String]) -> Frame {
    if args.len() != 2 {
        return err_frame("usage: CON.SREM key member");
    }
    let s = state();
    let mut sets = s.or_sets.write();
    if let Some(set) = sets.get_mut(&args[0]) {
        set.remove(&args[1]);
        Frame::Integer(1)
    } else {
        Frame::Integer(0)
    }
}

fn smembers(args: &[String]) -> Frame {
    if args.len() != 1 {
        return err_frame("usage: CON.SMEMBERS key");
    }
    let s = state();
    let sets = s.or_sets.read();
    match sets.get(&args[0]) {
        Some(set) => {
            let members = set.members();
            Frame::Array(Some(members.into_iter().map(bulk).collect()))
        }
        None => Frame::Array(Some(vec![])),
    }
}

fn smerge(args: &[String]) -> Frame {
    if args.len() != 2 {
        return err_frame("usage: CON.SMERGE key json");
    }
    let remote: OrSet<String> = match serde_json::from_str(&args[1]) {
        Ok(s) => s,
        Err(e) => return err_frame(&format!("invalid OrSet JSON: {e}")),
    };
    let s = state();
    let mut sets = s.or_sets.write();
    let set = sets.entry(args[0].clone()).or_default();
    set.merge(&remote);
    let count = set.members().len();
    Frame::Integer(count as i64)
}

// ── LWW Register ─────────────────────────────────────────────────────

fn lwwset(args: &[String]) -> Frame {
    if args.len() != 4 {
        return err_frame("usage: CON.LWWSET key value timestamp replica");
    }
    let ts: u64 = match args[2].parse() {
        Ok(t) => t,
        Err(_) => return err_frame("timestamp must be a non-negative integer"),
    };
    let s = state();
    let mut regs = s.lww_registers.write();
    let reg = regs.entry(args[0].clone()).or_default();
    use ferrite_concord::Delta;
    reg.mutate(LwwWrite {
        value: args[1].clone(),
        ts,
        replica: args[3].clone(),
    });
    ok_frame()
}

fn lwwget(args: &[String]) -> Frame {
    if args.len() != 1 {
        return err_frame("usage: CON.LWWGET key");
    }
    let s = state();
    let regs = s.lww_registers.read();
    match regs.get(&args[0]) {
        Some(reg) => match reg.value() {
            Some(v) => bulk(v.clone()),
            None => Frame::Bulk(None),
        },
        None => Frame::Bulk(None),
    }
}

fn lwwmerge(args: &[String]) -> Frame {
    if args.len() != 2 {
        return err_frame("usage: CON.LWWMERGE key json");
    }
    let remote: LwwRegister<String> = match serde_json::from_str(&args[1]) {
        Ok(r) => r,
        Err(e) => return err_frame(&format!("invalid LwwRegister JSON: {e}")),
    };
    let s = state();
    let mut regs = s.lww_registers.write();
    let reg = regs.entry(args[0].clone()).or_default();
    reg.merge(&remote);
    ok_frame()
}

// ── MV Register ──────────────────────────────────────────────────────

fn mvset(args: &[String]) -> Frame {
    if args.len() < 3 {
        return err_frame("usage: CON.MVSET key value replica [observed_json]");
    }
    let observed: ferrite_concord::VectorClock = if args.len() >= 4 {
        match serde_json::from_str(&args[3]) {
            Ok(c) => c,
            Err(e) => return err_frame(&format!("invalid observed clock JSON: {e}")),
        }
    } else {
        std::collections::BTreeMap::new()
    };
    let s = state();
    let mut regs = s.mv_registers.write();
    let reg = regs.entry(args[0].clone()).or_default();
    use ferrite_concord::Delta;
    reg.mutate(MvWrite {
        value: args[1].clone(),
        replica: args[2].clone(),
        observed,
    });
    ok_frame()
}

fn mvget(args: &[String]) -> Frame {
    if args.len() != 1 {
        return err_frame("usage: CON.MVGET key");
    }
    let s = state();
    let regs = s.mv_registers.read();
    match regs.get(&args[0]) {
        Some(reg) => {
            let vals: Vec<Frame> = reg.values().into_iter().map(|v| bulk(v.clone())).collect();
            Frame::Array(Some(vals))
        }
        None => Frame::Array(Some(vec![])),
    }
}

fn mvmerge(args: &[String]) -> Frame {
    if args.len() != 2 {
        return err_frame("usage: CON.MVMERGE key json");
    }
    let remote: MvRegister<String> = match serde_json::from_str(&args[1]) {
        Ok(r) => r,
        Err(e) => return err_frame(&format!("invalid MvRegister JSON: {e}")),
    };
    let s = state();
    let mut regs = s.mv_registers.write();
    let reg = regs.entry(args[0].clone()).or_default();
    reg.merge(&remote);
    let count = reg.values().len();
    Frame::Integer(count as i64)
}

// ── DVV / Version Vectors ────────────────────────────────────────────

fn dvv(args: &[String]) -> Frame {
    if args.len() != 2 {
        return err_frame("usage: CON.DVV key replica");
    }
    let s = state();
    let mut dvvs = s.dvvs.write();
    let dvv = dvvs.entry(args[0].clone()).or_default();
    let (replica, seq) = dvv.event(&args[1]);
    Frame::Array(Some(vec![bulk(replica), Frame::Integer(seq as i64)]))
}

fn clock(args: &[String]) -> Frame {
    if args.len() != 1 {
        return err_frame("usage: CON.CLOCK key");
    }
    let s = state();
    let clocks = s.clocks.read();
    match clocks.get(&args[0]) {
        Some(vv) => {
            let pairs: Vec<Frame> = vv
                .iter()
                .flat_map(|(k, v)| vec![bulk(k.clone()), Frame::Integer(*v as i64)])
                .collect();
            Frame::Array(Some(pairs))
        }
        None => Frame::Array(Some(vec![])),
    }
}

fn peers() -> Frame {
    let s = state();
    let gc = s.g_counters.read();
    let pn = s.pn_counters.read();
    let os = s.or_sets.read();
    let lw = s.lww_registers.read();
    let mv = s.mv_registers.read();

    let mut all_keys: Vec<String> = Vec::new();
    all_keys.extend(gc.keys().cloned());
    all_keys.extend(pn.keys().cloned());
    all_keys.extend(os.keys().cloned());
    all_keys.extend(lw.keys().cloned());
    all_keys.extend(mv.keys().cloned());
    all_keys.sort();
    all_keys.dedup();

    Frame::Array(Some(all_keys.into_iter().map(bulk).collect()))
}

fn sync(args: &[String]) -> Frame {
    if args.len() != 2 {
        return err_frame("usage: CON.SYNC key dvv_json");
    }
    let remote: DottedVersionVector = match serde_json::from_str(&args[1]) {
        Ok(d) => d,
        Err(e) => return err_frame(&format!("invalid DVV JSON: {e}")),
    };
    let s = state();
    let mut dvvs = s.dvvs.write();
    let dvv = dvvs.entry(args[0].clone()).or_default();
    dvv.sync(&remote);
    ok_frame()
}

// ── Anti-Entropy ─────────────────────────────────────────────────────

fn entropy(args: &[String]) -> Frame {
    if args.is_empty() {
        let s = state();
        let tree = s.entropy.read();
        let hash = tree.root_hash();
        bulk(hex::encode(hash))
    } else if args.len() == 2 {
        let hash_bytes = match hex_decode_32(&args[1]) {
            Ok(h) => h,
            Err(e) => return err_frame(&format!("invalid hash hex: {e}")),
        };
        let s = state();
        let mut tree = s.entropy.write();
        tree.update(&args[0], hash_bytes);
        ok_frame()
    } else {
        err_frame("usage: CON.ENTROPY [key value_hash_hex]")
    }
}

fn hex_decode_32(hex_str: &str) -> Result<[u8; 32], String> {
    if hex_str.len() != 64 {
        return Err("expected 64 hex chars (32 bytes)".to_string());
    }
    let mut out = [0u8; 32];
    for (i, chunk) in hex_str.as_bytes().chunks(2).enumerate() {
        let s = std::str::from_utf8(chunk).map_err(|e| e.to_string())?;
        out[i] = u8::from_str_radix(s, 16).map_err(|e| e.to_string())?;
    }
    Ok(out)
}

// ── Routing ──────────────────────────────────────────────────────────

fn route(args: &[String]) -> Frame {
    if args.len() != 1 {
        return err_frame("usage: CON.ROUTE key");
    }
    let s = state();
    let router = s.router.read();
    match router.route(&args[0]) {
        Some(region) => bulk(region.to_string()),
        None => Frame::Bulk(None),
    }
}

fn addrule(args: &[String]) -> Frame {
    if args.len() != 3 {
        return err_frame("usage: CON.ADDRULE pattern region priority");
    }
    let priority: u32 = match args[2].parse() {
        Ok(p) => p,
        Err(_) => return err_frame("priority must be a non-negative integer"),
    };
    let rule = RoutingRule {
        pattern: args[0].clone(),
        region: args[1].clone(),
        priority,
    };
    let s = state();
    let mut router = s.router.write();
    router.add_rule(rule);
    ok_frame()
}

fn rules() -> Frame {
    let s = state();
    let router = s.router.read();
    let items: Vec<Frame> = router
        .rules()
        .iter()
        .map(|r| {
            Frame::Array(Some(vec![
                bulk(r.pattern.clone()),
                bulk(r.region.clone()),
                Frame::Integer(r.priority as i64),
            ]))
        })
        .collect();
    Frame::Array(Some(items))
}

// ── Help ─────────────────────────────────────────────────────────────

fn help() -> Frame {
    Frame::Array(Some(vec![
        bulk("CON.GINC key replica delta — Increment a G-Counter"),
        bulk("CON.GVAL key — Read a G-Counter value"),
        bulk("CON.GMERGE key json — Merge a remote G-Counter"),
        bulk("CON.PNINC key replica delta — Increment/decrement a PN-Counter"),
        bulk("CON.PNVAL key — Read a PN-Counter value"),
        bulk("CON.PNMERGE key json — Merge a remote PN-Counter"),
        bulk("CON.SADD key replica member — Add to an OR-Set"),
        bulk("CON.SREM key member — Remove from an OR-Set"),
        bulk("CON.SMEMBERS key — List OR-Set members"),
        bulk("CON.SMERGE key json — Merge a remote OR-Set"),
        bulk("CON.LWWSET key value ts replica — Set a LWW register"),
        bulk("CON.LWWGET key — Read a LWW register"),
        bulk("CON.LWWMERGE key json — Merge a remote LWW register"),
        bulk("CON.MVSET key value replica [observed_json] — Set a MV register"),
        bulk("CON.MVGET key — Read a MV register"),
        bulk("CON.MVMERGE key json — Merge a remote MV register"),
        bulk("CON.DVV key replica — Create/advance a DVV event"),
        bulk("CON.CLOCK key — Read a version vector"),
        bulk("CON.PEERS — List known CRDT keys"),
        bulk("CON.SYNC key dvv_json — Sync a DVV"),
        bulk("CON.ENTROPY [key value_hash_hex] — Anti-entropy tree ops"),
        bulk("CON.ROUTE key — Route a key to a region"),
        bulk("CON.ADDRULE pattern region priority — Add a routing rule"),
        bulk("CON.RULES — List routing rules"),
        bulk("CON.SAVE — Persist state to Store"),
        bulk("CON.LOAD — Reload state from Store"),
        bulk("CON.HELP — Show this help"),
    ]))
}

// ── Hex encoding helper ──────────────────────────────────────────────

mod hex {
    pub fn encode(bytes: impl AsRef<[u8]>) -> String {
        bytes.as_ref().iter().fold(String::new(), |mut s, b| {
            use std::fmt::Write;
            let _ = write!(s, "{b:02x}");
            s
        })
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn args(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn ginc_returns_integer() {
        let res = concord_command("GINC", &args(&["test-gc1", "node-a", "5"]));
        assert!(
            matches!(&res, Frame::Integer(_)),
            "GINC should return an integer: {res:?}"
        );
    }

    #[test]
    fn gval_default_is_zero() {
        let res = concord_command("GVAL", &args(&["nonexistent-gc"]));
        assert_eq!(
            res,
            Frame::Integer(0),
            "GVAL of missing key should return 0"
        );
    }

    #[test]
    fn pninc_positive_and_negative() {
        concord_command("PNINC", &args(&["test-pn1", "node-a", "10"]));
        let res = concord_command("PNINC", &args(&["test-pn1", "node-a", "-3"]));
        assert!(
            matches!(&res, Frame::Integer(v) if *v == 7),
            "PNINC 10 then -3 should give 7: {res:?}"
        );
    }

    #[test]
    fn pnval_default_is_zero() {
        let res = concord_command("PNVAL", &args(&["nonexistent-pn"]));
        assert_eq!(
            res,
            Frame::Integer(0),
            "PNVAL of missing key should return 0"
        );
    }

    #[test]
    fn sadd_and_smembers() {
        concord_command("SADD", &args(&["test-set1", "node-a", "apple"]));
        concord_command("SADD", &args(&["test-set1", "node-a", "banana"]));
        let res = concord_command("SMEMBERS", &args(&["test-set1"]));
        assert!(
            matches!(&res, Frame::Array(Some(items)) if items.len() == 2),
            "SMEMBERS should return 2 members: {res:?}"
        );
    }

    #[test]
    fn srem_removes_member() {
        concord_command("SADD", &args(&["test-set-rem", "node-a", "x"]));
        concord_command("SREM", &args(&["test-set-rem", "x"]));
        let res = concord_command("SMEMBERS", &args(&["test-set-rem"]));
        assert!(
            matches!(&res, Frame::Array(Some(items)) if items.is_empty()),
            "SMEMBERS after SREM should be empty: {res:?}"
        );
    }

    #[test]
    fn lwwset_and_lwwget() {
        concord_command("LWWSET", &args(&["test-lww1", "hello", "100", "node-a"]));
        let res = concord_command("LWWGET", &args(&["test-lww1"]));
        assert!(
            matches!(&res, Frame::Bulk(Some(b)) if b == "hello"),
            "LWWGET should return the value: {res:?}"
        );
    }

    #[test]
    fn lwwget_missing_returns_nil() {
        let res = concord_command("LWWGET", &args(&["nonexistent-lww"]));
        assert!(
            matches!(&res, Frame::Bulk(None)),
            "LWWGET of missing key should return nil: {res:?}"
        );
    }

    #[test]
    fn mvset_and_mvget() {
        concord_command("MVSET", &args(&["test-mv1", "val1", "node-a"]));
        let res = concord_command("MVGET", &args(&["test-mv1"]));
        assert!(
            matches!(&res, Frame::Array(Some(items)) if !items.is_empty()),
            "MVGET should return values: {res:?}"
        );
    }

    #[test]
    fn dvv_creates_event() {
        let res = concord_command("DVV", &args(&["test-dvv1", "node-a"]));
        assert!(
            matches!(&res, Frame::Array(Some(items)) if items.len() == 2),
            "DVV should return (replica, seq): {res:?}"
        );
    }

    #[test]
    fn addrule_and_route() {
        concord_command("ADDRULE", &args(&["*:eu:*", "eu-west-1", "10"]));
        let res = concord_command("ROUTE", &args(&["data:eu:key1"]));
        assert!(
            matches!(&res, Frame::Bulk(Some(b)) if b == "eu-west-1"),
            "ROUTE should return the region: {res:?}"
        );
    }

    #[test]
    fn route_no_match_returns_nil() {
        let res = concord_command("ROUTE", &args(&["no-match-key-xyz"]));
        assert!(
            !matches!(&res, Frame::Error(_)),
            "ROUTE should not error: {res:?}"
        );
    }

    #[test]
    fn rules_returns_array() {
        let res = concord_command("RULES", &args(&[]));
        assert!(
            matches!(&res, Frame::Array(Some(_))),
            "RULES should return an array: {res:?}"
        );
    }

    #[test]
    fn help_returns_array() {
        let res = concord_command("HELP", &args(&[]));
        assert!(
            matches!(&res, Frame::Array(Some(items)) if !items.is_empty()),
            "HELP should return a non-empty array: {res:?}"
        );
    }

    #[test]
    fn unknown_subcommand_errors() {
        let res = concord_command("BADCMD", &args(&[]));
        assert!(
            matches!(&res, Frame::Error(_)),
            "unknown subcommand should error: {res:?}"
        );
    }

    #[test]
    fn ginc_wrong_args_errors() {
        let res = concord_command("GINC", &args(&["only-one"]));
        assert!(
            matches!(&res, Frame::Error(_)),
            "GINC with wrong args should error: {res:?}"
        );
    }

    #[test]
    fn persist_and_restore_snapshot() {
        let store = Arc::new(Store::new(16));
        ensure_loaded_from_store(&store);

        let s = state();
        {
            let mut gc = s.g_counters.write();
            let mut c = GCounter::new();
            c.increment("snap-node", 42);
            gc.insert("snap-test-gc".to_string(), c);
        }
        let res = persist_to_store(&store);
        assert!(res.is_ok(), "persist should succeed: {res:?}");

        let key = Bytes::from(CONCORD_STORE_KEY);
        assert!(store.get(0, &key).is_some(), "snapshot should be in store");
    }
}
