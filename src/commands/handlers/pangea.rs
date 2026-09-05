//! PNG.* command handlers — Pangea NUMA-aware tiered allocator (experimental).
//!
//! Subcommands: `PNG.ALLOC key value`, `PNG.READ key`, `PNG.FREE key`,
//! `PNG.STATS`, `PNG.TOPOLOGY`, `PNG.MIGRATE key target_node`,
//! `PNG.KEYS [prefix]`, `PNG.NODE node_id`, `PNG.POLICY [name]`,
//! `PNG.TIER key`, `PNG.EVALUATE [dram_fraction]`,
//! `PNG.TIERPOLICY [promote_threshold demote_after_ms pressure]`,
//! `PNG.DETECT`, `PNG.BENCH [iterations]`,
//! `PNG.SIZING working_set_gib [hot_ratio]`,
//! `PNG.SAVE`, `PNG.LOAD`, `PNG.HELP`.
//!
//! Backed by a 2-node `NumaTopology<InMemoryCxlAllocator>` with
//! HashMod routing.  Full key→bytes state is persisted to the Store
//! under `__ferrite:pangea:data` so allocations survive restarts;
//! routing is re-applied at load time so keys may land on different
//! nodes if topology changes.  See ADR-023 for production wiring.

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::{Store, Value};
use ferrite_pangea::{
    InMemoryCxlAllocator, NumaTopology, PolicyEngine, RoutingPolicy, Tier, TierPolicy,
};

use super::moonshot_limits::{validate_key, validate_value};
use super::{bulk, err_frame, ok_frame, warn_experimental};

const PANGEA_STORE_KEY: &str = "__ferrite:pangea:data";
const DEFAULT_ALLOCATOR_SIZE: usize = 1024 * 1024; // 1 MiB
const DEFAULT_PAGE_SIZE: usize = 256;

static MUTATION_COUNTER: AtomicU64 = AtomicU64::new(0);
static TOPO: OnceLock<NumaTopology<InMemoryCxlAllocator>> = OnceLock::new();
static LOADED_FROM_STORE: OnceLock<bool> = OnceLock::new();
static POLICY_ENGINE: OnceLock<std::sync::RwLock<PolicyEngine>> = OnceLock::new();

fn topo() -> &'static NumaTopology<InMemoryCxlAllocator> {
    TOPO.get_or_init(|| {
        let nodes = (0..2)
            .map(|_| InMemoryCxlAllocator::shared(DEFAULT_ALLOCATOR_SIZE, DEFAULT_PAGE_SIZE))
            .collect();
        NumaTopology::new(nodes, RoutingPolicy::HashMod)
    })
}

fn policy_engine() -> &'static std::sync::RwLock<PolicyEngine> {
    POLICY_ENGINE.get_or_init(|| std::sync::RwLock::new(PolicyEngine::new(TierPolicy::default())))
}

#[derive(serde::Serialize, serde::Deserialize, Default)]
struct PangeaSnapshot {
    keys: Vec<(String, Vec<u8>)>,
}

fn ensure_loaded_from_store(store: &Store) {
    LOADED_FROM_STORE.get_or_init(|| {
        let key = Bytes::from(PANGEA_STORE_KEY);
        if let Some(Value::String(data)) = store.get(0, &key) {
            if let Ok(snap) = serde_json::from_slice::<PangeaSnapshot>(&data) {
                let restored = topo().replay_keys(snap.keys);
                tracing::info!("Pangea: restored {restored} key/value pair(s) from Store");
            }
        }
        true
    });
}

fn persist_to_store(store: &Store) -> Result<(), String> {
    let snap = PangeaSnapshot {
        keys: topo().snapshot_keys(),
    };
    let json = serde_json::to_vec(&snap).map_err(|e| format!("serialize pangea snapshot: {e}"))?;
    store.set(
        0,
        Bytes::from(PANGEA_STORE_KEY),
        Value::String(Bytes::from(json)),
    );
    Ok(())
}

/// Execute a PNG.* (Pangea NUMA allocator) command without Store persistence.
///
/// This variant is used for backward-compatible dispatch and testing.
/// State is held in process-local singletons — see [`pangea_command_with_store`]
/// for the production entry point.
pub fn pangea_command(subcommand: &str, args: &[String]) -> Frame {
    warn_experimental("PNG");
    match subcommand.to_uppercase().as_str() {
        "ALLOC" => alloc(args),
        "READ" => read(args),
        "FREE" => free(args),
        "STATS" => stats(),
        "TOPOLOGY" => topology(),
        "MIGRATE" => migrate(args),
        "KEYS" => keys(args),
        "NODE" => node(args),
        "POLICY" => policy(args),
        "TIER" => tier(args),
        "EVALUATE" => evaluate(args),
        "TIERPOLICY" => tier_policy(args),
        "DETECT" => detect_cxl(),
        "BENCH" => bench(args),
        "SIZING" => sizing(args),
        "HELP" | "" => help(),
        other => err_frame(&format!("unknown PNG subcommand '{}'", other)),
    }
}

/// Execute a PNG.* (Pangea NUMA allocator) command with Store-backed persistence.
///
/// Auto-loads state from Store on first call, and auto-persists after
/// mutating operations (ALLOC, FREE, MIGRATE, POLICY, TIERPOLICY).
/// Use `PNG.SAVE` / `PNG.LOAD` for explicit persistence control.
///
/// # Subcommands
///
/// | Command | Mutating | Description |
/// |---------|----------|-------------|
/// | `PNG.ALLOC` | Yes | Allocate a key-value on a NUMA node |
/// | `PNG.READ` | No | Read a value by key |
/// | `PNG.FREE` | Yes | Free an allocation |
/// | `PNG.STATS` | No | Show allocator statistics |
/// | `PNG.TOPOLOGY` | No | Show NUMA topology |
/// | `PNG.MIGRATE` | Yes | Migrate a key to a different node |
/// | `PNG.KEYS` | No | List allocated keys |
/// | `PNG.NODE` | No | Show node details |
/// | `PNG.POLICY` | Yes | Set/show routing policy |
/// | `PNG.TIER` | No | Show tier for a key |
/// | `PNG.EVALUATE` | No | Evaluate tier distribution |
/// | `PNG.TIERPOLICY` | Yes | Set tier promotion/demotion policy |
/// | `PNG.DETECT` | No | Detect CXL devices |
/// | `PNG.BENCH` | No | Run allocation benchmark |
/// | `PNG.SIZING` | No | Recommend tier sizing |
/// | `PNG.SAVE` | No | Persist state to Store |
/// | `PNG.LOAD` | No | Reload state from Store |
/// | `PNG.HELP` | No | Show help |
pub fn pangea_command_with_store(store: &Arc<Store>, subcommand: &str, args: &[String]) -> Frame {
    warn_experimental("PNG");
    if !super::moonshot_config::is_enabled("PNG") {
        return err_frame("ERR PNG.* commands are disabled in moonshot configuration");
    }
    ensure_loaded_from_store(store);

    let upper = subcommand.to_uppercase();
    let is_mutating = matches!(
        upper.as_str(),
        "ALLOC" | "FREE" | "MIGRATE" | "POLICY" | "TIERPOLICY"
    );

    let result = match upper.as_str() {
        "ALLOC" => alloc(args),
        "READ" => read(args),
        "FREE" => free(args),
        "STATS" => stats(),
        "TOPOLOGY" => topology(),
        "MIGRATE" => migrate(args),
        "KEYS" => keys(args),
        "NODE" => node(args),
        "POLICY" => policy(args),
        "TIER" => tier(args),
        "EVALUATE" => evaluate(args),
        "TIERPOLICY" => tier_policy(args),
        "DETECT" => detect_cxl(),
        "BENCH" => bench(args),
        "SIZING" => sizing(args),
        "SAVE" => match persist_to_store(store) {
            Ok(()) => ok_frame(),
            Err(e) => err_frame(&format!("save: {e}")),
        },
        "LOAD" => {
            let key = Bytes::from(PANGEA_STORE_KEY);
            match store.get(0, &key) {
                Some(Value::String(data)) => {
                    match serde_json::from_slice::<PangeaSnapshot>(&data) {
                        Ok(snap) => {
                            topo().replay_keys(snap.keys);
                            ok_frame()
                        }
                        Err(e) => err_frame(&format!("load: invalid snapshot: {e}")),
                    }
                }
                _ => err_frame("load: no pangea snapshot in store"),
            }
        }
        "HELP" | "" => help(),
        other => return err_frame(&format!("unknown PNG subcommand '{}'", other)),
    };

    if is_mutating && !matches!(result, Frame::Error(_)) && super::should_persist(&MUTATION_COUNTER)
    {
        if let Err(e) = persist_to_store(store) {
            tracing::warn!("Failed to persist pangea data: {}", e);
        }
    }

    result
}

fn alloc(args: &[String]) -> Frame {
    if args.len() != 2 {
        return err_frame("usage: PNG.ALLOC key value");
    }
    if let Err(f) = validate_key(&args[0]) {
        return f;
    }
    if let Err(f) = validate_value(&args[1]) {
        return f;
    }
    // Replace existing allocation, if any.
    topo().free(&args[0]);
    match topo().allocate(&args[0], args[1].as_bytes()) {
        Ok(loc) => Frame::Array(Some(vec![
            bulk("node"),
            Frame::Integer(loc.node as i64),
            bulk("page"),
            Frame::Integer(loc.page as i64),
        ])),
        Err(e) => err_frame(&format!("allocate failed: {e}")),
    }
}

fn read(args: &[String]) -> Frame {
    if args.len() != 1 {
        return err_frame("usage: PNG.READ key");
    }
    if let Err(f) = validate_key(&args[0]) {
        return f;
    }
    match topo().read(&args[0]) {
        Some(b) => Frame::Bulk(Some(Bytes::from(b))),
        None => Frame::Bulk(None),
    }
}

fn free(args: &[String]) -> Frame {
    if args.len() != 1 {
        return err_frame("usage: PNG.FREE key");
    }
    if let Err(f) = validate_key(&args[0]) {
        return f;
    }
    if topo().free(&args[0]) {
        ok_frame()
    } else {
        Frame::Bulk(None)
    }
}

fn stats() -> Frame {
    Frame::Array(Some(vec![
        bulk("nodes"),
        Frame::Integer(topo().node_count() as i64),
        bulk("free_bytes"),
        Frame::Integer(topo().free_bytes() as i64),
    ]))
}

fn topology() -> Frame {
    let t = topo();
    let nc = t.node_count();
    let mut items: Vec<Frame> = vec![
        bulk("nodes"),
        Frame::Integer(nc as i64),
        bulk("free_bytes"),
        Frame::Integer(t.free_bytes() as i64),
        bulk("routing_policy"),
        bulk(format!("{:?}", t.routing_policy())),
    ];
    let mut per_node = Vec::new();
    for i in 0..nc {
        per_node.push(Frame::Array(Some(vec![
            bulk("node"),
            Frame::Integer(i as i64),
            bulk("free_bytes"),
            Frame::Integer(t.node_free_bytes(i).unwrap_or(0) as i64),
        ])));
    }
    items.push(bulk("per_node"));
    items.push(Frame::Array(Some(per_node)));
    Frame::Array(Some(items))
}

fn migrate(args: &[String]) -> Frame {
    if args.len() != 2 {
        return err_frame("usage: PNG.MIGRATE key target_node");
    }
    let key = &args[0];
    if let Err(f) = validate_key(key) {
        return f;
    }
    let target_node: usize = match args[1].parse() {
        Ok(n) => n,
        Err(_) => return err_frame("target_node must be a non-negative integer"),
    };
    if target_node >= topo().node_count() {
        return err_frame(&format!(
            "target_node {} out of range (0..{})",
            target_node,
            topo().node_count()
        ));
    }
    // Read current data
    let Some(data) = topo().read(key) else {
        return Frame::Bulk(None);
    };
    // Free from current node
    topo().free(key);
    // Re-allocate on target node
    match topo().allocate_on_node(key, &data, target_node) {
        Ok(loc) => Frame::Array(Some(vec![
            bulk("node"),
            Frame::Integer(loc.node as i64),
            bulk("page"),
            Frame::Integer(loc.page as i64),
        ])),
        Err(e) => err_frame(&format!("migrate failed: {e}")),
    }
}

fn keys(args: &[String]) -> Frame {
    let list = if args.is_empty() {
        topo().keys()
    } else {
        topo().keys_with_prefix(&args[0])
    };
    Frame::Array(Some(list.into_iter().map(bulk).collect()))
}

fn node(args: &[String]) -> Frame {
    if args.len() != 1 {
        return err_frame("usage: PNG.NODE node_id");
    }
    let node_id: usize = match args[0].parse() {
        Ok(n) => n,
        Err(_) => return err_frame("node_id must be a non-negative integer"),
    };
    let Some(stats) = topo().node_stats(node_id) else {
        return err_frame(&format!("node {} does not exist", node_id));
    };
    let node_keys = topo().node_keys(node_id);
    Frame::Array(Some(vec![
        bulk("pages_total"),
        Frame::Integer(stats.pages_total as i64),
        bulk("pages_free"),
        Frame::Integer((stats.pages_total - stats.pages_used) as i64),
        bulk("keys"),
        Frame::Integer(node_keys.len() as i64),
        bulk("key_list"),
        Frame::Array(Some(node_keys.into_iter().map(bulk).collect())),
    ]))
}

fn policy(args: &[String]) -> Frame {
    if args.is_empty() {
        return bulk(format!("{:?}", topo().routing_policy()));
    }
    let new_policy = match args[0].to_uppercase().as_str() {
        "HASHMOD" => RoutingPolicy::HashMod,
        "ROUNDROBIN" => RoutingPolicy::RoundRobin,
        "LEASTUSED" => RoutingPolicy::LeastUsed,
        other => {
            return err_frame(&format!(
                "unknown policy '{}'; use HashMod, RoundRobin, or LeastUsed",
                other
            ))
        }
    };
    topo().set_routing_policy(new_policy);
    ok_frame()
}

fn tier(args: &[String]) -> Frame {
    if args.len() != 1 {
        return err_frame("usage: PNG.TIER key");
    }
    let engine = policy_engine().read().expect("policy engine lock poisoned");
    match engine.stats(&args[0]) {
        Some(s) => {
            let tier_name = match s.tier {
                Tier::Dram => "DRAM",
                Tier::Cxl => "CXL",
                Tier::Disk => "DISK",
            };
            Frame::Array(Some(vec![
                bulk("tier"),
                bulk(tier_name),
                bulk("reads"),
                Frame::Integer(s.reads as i64),
                bulk("last_access_ms"),
                Frame::Integer(s.last_access_ms as i64),
            ]))
        }
        None => Frame::Bulk(None),
    }
}

fn evaluate(args: &[String]) -> Frame {
    let dram_fraction: f64 = if args.is_empty() {
        0.5
    } else {
        match args[0].parse() {
            Ok(v) => v,
            Err(_) => return err_frame("dram_fraction must be a float"),
        }
    };
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let engine = policy_engine().read().expect("policy engine lock poisoned");
    let plan = engine.evaluate(now_ms, dram_fraction);
    Frame::Array(Some(vec![
        bulk("promotions"),
        Frame::Integer(plan.promotions.len() as i64),
        bulk("demotions"),
        Frame::Integer(plan.demotions.len() as i64),
        bulk("promote_keys"),
        Frame::Array(Some(plan.promotions.into_iter().map(bulk).collect())),
        bulk("demote_keys"),
        Frame::Array(Some(plan.demotions.into_iter().map(bulk).collect())),
    ]))
}

fn tier_policy(args: &[String]) -> Frame {
    if args.is_empty() {
        let engine = policy_engine().read().expect("policy engine lock poisoned");
        let p = engine.policy();
        return Frame::Array(Some(vec![
            bulk("promote_threshold"),
            Frame::Integer(p.promote_threshold as i64),
            bulk("demote_after_ms"),
            Frame::Integer(p.demote_after_ms as i64),
            bulk("dram_pressure_threshold"),
            bulk(format!("{}", p.dram_pressure_threshold)),
            bulk("max_promotions_per_cycle"),
            Frame::Integer(p.max_promotions_per_cycle as i64),
            bulk("max_demotions_per_cycle"),
            Frame::Integer(p.max_demotions_per_cycle as i64),
        ]));
    }
    if args.len() != 3 {
        return err_frame("usage: PNG.TIERPOLICY [promote_threshold demote_after_ms pressure]");
    }
    let promote_threshold: u64 = match args[0].parse() {
        Ok(v) => v,
        Err(_) => return err_frame("promote_threshold must be a positive integer"),
    };
    let demote_after_ms: u64 = match args[1].parse() {
        Ok(v) => v,
        Err(_) => return err_frame("demote_after_ms must be a positive integer"),
    };
    let pressure: f64 = match args[2].parse() {
        Ok(v) => v,
        Err(_) => return err_frame("pressure must be a float"),
    };
    let mut engine = policy_engine()
        .write()
        .expect("policy engine lock poisoned");
    let old = engine.policy().clone();
    engine.set_policy(TierPolicy {
        promote_threshold,
        demote_after_ms,
        dram_pressure_threshold: pressure,
        max_promotions_per_cycle: old.max_promotions_per_cycle,
        max_demotions_per_cycle: old.max_demotions_per_cycle,
    });
    ok_frame()
}

fn detect_cxl() -> Frame {
    let cap = ferrite_pangea::feature::detect();
    let mut items = vec![
        bulk("available"),
        Frame::Integer(i64::from(cap.available)),
        bulk("detection_method"),
        bulk(cap.detection_method.to_string()),
        bulk("numa_nodes"),
        Frame::Integer(cap.numa_nodes as i64),
    ];
    if let Some(ref v) = cap.version {
        items.push(bulk("version"));
        items.push(bulk(v.clone()));
    }
    if let Some(dt) = cap.device_type {
        items.push(bulk("device_type"));
        items.push(bulk(dt.to_string()));
    }
    if let Some(bytes) = cap.capacity_bytes {
        items.push(bulk("capacity_bytes"));
        items.push(Frame::Integer(bytes as i64));
    }
    Frame::Array(Some(items))
}

fn bench(args: &[String]) -> Frame {
    let iterations: usize = if args.is_empty() {
        1000
    } else {
        match args[0].parse() {
            Ok(n) if n > 0 => n,
            _ => return err_frame("usage: PNG.BENCH [iterations] (positive integer)"),
        }
    };

    // Seed some keys into the topology for benchmarking
    let bench_keys: Vec<String> = (0..10).map(|i| format!("__bench:{i}")).collect();
    for key in &bench_keys {
        let _ = topo().free(key);
        let _ = topo().allocate(key, b"benchmark-payload-data");
    }

    let result = ferrite_pangea::benchmark::benchmark_reads(
        "pangea-inmem",
        &bench_keys,
        |k| topo().read(k),
        iterations,
    );

    // Clean up bench keys
    for key in &bench_keys {
        let _ = topo().free(key);
    }

    Frame::Array(Some(vec![
        bulk("tier"),
        bulk(&result.tier_name),
        bulk("operations"),
        Frame::Integer(result.operations as i64),
        bulk("p50_ns"),
        Frame::Integer(result.p50_ns as i64),
        bulk("p99_ns"),
        Frame::Integer(result.p99_ns as i64),
        bulk("throughput_ops_sec"),
        bulk(format!("{:.2}", result.throughput_ops_sec)),
        bulk("total_duration_us"),
        Frame::Integer(result.total_duration.as_micros() as i64),
    ]))
}

/// Default DRAM cost per GiB/month (approximate market average).
const DEFAULT_DRAM_PRICE: f64 = 5.50;
/// Default CXL cost per GiB/month (approximate market average).
const DEFAULT_CXL_PRICE: f64 = 2.75;

fn sizing(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("usage: PNG.SIZING working_set_gib [hot_ratio]");
    }
    let working_set_gib: f64 = match args[0].parse() {
        Ok(v) if v > 0.0 => v,
        _ => return err_frame("working_set_gib must be a positive number"),
    };
    let hot_ratio: f64 = if args.len() > 1 {
        match args[1].parse() {
            Ok(v) if (0.0..=1.0).contains(&v) => v,
            _ => return err_frame("hot_ratio must be a float between 0.0 and 1.0"),
        }
    } else {
        0.5
    };

    let rec = ferrite_pangea::sizing::recommend(
        working_set_gib,
        DEFAULT_DRAM_PRICE,
        DEFAULT_CXL_PRICE,
        hot_ratio,
    );

    Frame::Array(Some(vec![
        bulk("working_set_gib"),
        bulk(format!("{:.2}", rec.working_set_gib)),
        bulk("dram_only_cost_monthly_usd"),
        bulk(format!("{:.2}", rec.dram_only_cost_monthly_usd)),
        bulk("cxl_hybrid_cost_monthly_usd"),
        bulk(format!("{:.2}", rec.cxl_hybrid_cost_monthly_usd)),
        bulk("savings_pct"),
        bulk(format!("{:.1}", rec.savings_pct)),
        bulk("recommended_dram_gib"),
        bulk(format!("{:.2}", rec.recommended_dram_gib)),
        bulk("recommended_cxl_gib"),
        bulk(format!("{:.2}", rec.recommended_cxl_gib)),
        bulk("estimated_p99_impact_pct"),
        bulk(format!("{:.1}", rec.estimated_p99_impact_pct)),
    ]))
}

fn help() -> Frame {
    let lines = [
        "PNG.ALLOC key value - Allocate, returns {node, page}",
        "PNG.READ key - Read bytes by key",
        "PNG.FREE key - Free the page bound to key (OK or nil)",
        "PNG.STATS - Topology stats (nodes + free_bytes)",
        "PNG.TOPOLOGY - Detailed topology: nodes, per-node free, routing policy",
        "PNG.MIGRATE key target_node - Move key to a different NUMA node",
        "PNG.KEYS [prefix] - List allocated keys, optionally filtered by prefix",
        "PNG.NODE node_id - Per-node stats: pages, free pages, allocated keys",
        "PNG.POLICY [HashMod|RoundRobin|LeastUsed] - Get or set routing policy",
        "PNG.TIER key - Show tier and access stats for a key",
        "PNG.EVALUATE [dram_fraction] - Evaluate tier migration policy",
        "PNG.TIERPOLICY [promote_threshold demote_after_ms pressure] - Get or set tier policy",
        "PNG.DETECT - Run CXL hardware detection and return capabilities",
        "PNG.BENCH [iterations] - Run a quick benchmark of the current allocator",
        "PNG.SIZING working_set_gib [hot_ratio] - Estimate $/GiB savings with CXL vs DRAM-only",
        "PNG.SAVE - Persist all key→bytes pairs to Store",
        "PNG.LOAD - Restore key→bytes pairs from Store (replay via allocate)",
        "PNG.HELP - Show this help",
    ];
    Frame::Array(Some(lines.iter().map(|l| bulk(*l)).collect()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| (*x).to_string()).collect()
    }

    #[test]
    fn alloc_read_free_roundtrip() {
        let key = "test:png:k1";
        assert!(matches!(
            pangea_command("ALLOC", &s(&[key, "hello"])),
            Frame::Array(_)
        ));
        match pangea_command("READ", &s(&[key])) {
            Frame::Bulk(Some(b)) => {
                assert_eq!(&b[..], b"hello", "READ should return allocated value");
            }
            other => panic!("expected hello, got {:?}", other),
        }
        assert!(matches!(
            pangea_command("FREE", &s(&[key])),
            Frame::Simple(_)
        ));
        assert!(matches!(
            pangea_command("READ", &s(&[key])),
            Frame::Bulk(None)
        ));
    }

    #[test]
    fn stats_returns_array() {
        if let Frame::Array(Some(items)) = pangea_command("STATS", &[]) {
            assert_eq!(items.len(), 4, "STATS should return 4-element array");
        } else {
            panic!("STATS should be an array");
        }
    }

    #[test]
    fn unknown_subcommand_errors() {
        assert!(matches!(pangea_command("WAT", &[]), Frame::Error(_)));
    }

    #[test]
    fn missing_args_errors() {
        assert!(matches!(
            pangea_command("ALLOC", &s(&["only-key"])),
            Frame::Error(_)
        ));
        assert!(matches!(pangea_command("READ", &[]), Frame::Error(_)));
    }

    #[test]
    fn save_load_via_store() {
        let store = Arc::new(Store::new(1));
        let _ = pangea_command_with_store(&store, "ALLOC", &s(&["png:save:k", "v"]));
        let raw = store.get(0, &Bytes::from(PANGEA_STORE_KEY));
        assert!(raw.is_some(), "stats should be in store after mutation");
        assert!(matches!(
            pangea_command_with_store(&store, "SAVE", &[]),
            Frame::Simple(_)
        ));
        assert!(matches!(
            pangea_command_with_store(&store, "LOAD", &[]),
            Frame::Simple(_)
        ));
    }

    #[test]
    fn topology_returns_detailed_info() {
        // Ensure topology is initialized by allocating something.
        let _ = pangea_command("ALLOC", &s(&["test:topo:k", "data"]));
        if let Frame::Array(Some(items)) = pangea_command("TOPOLOGY", &[]) {
            // Should contain nodes, free_bytes, routing_policy, per_node
            assert!(items.len() >= 8, "TOPOLOGY should have at least 8 fields");
            // First pair is nodes
            assert!(matches!(&items[0], Frame::Bulk(Some(b)) if &b[..] == b"nodes"));
        } else {
            panic!("TOPOLOGY should return an array");
        }
    }

    #[test]
    fn keys_lists_allocated() {
        let _ = pangea_command("ALLOC", &s(&["test:keys:a", "1"]));
        let _ = pangea_command("ALLOC", &s(&["test:keys:b", "2"]));
        if let Frame::Array(Some(items)) = pangea_command("KEYS", &[]) {
            let strs: Vec<_> = items
                .iter()
                .filter_map(|f| match f {
                    Frame::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
                    _ => None,
                })
                .collect();
            assert!(strs.contains(&"test:keys:a".to_string()));
            assert!(strs.contains(&"test:keys:b".to_string()));
        } else {
            panic!("KEYS should return an array");
        }
    }

    #[test]
    fn keys_with_prefix_filters() {
        let _ = pangea_command("ALLOC", &s(&["test:pfx:yes", "1"]));
        let _ = pangea_command("ALLOC", &s(&["other:pfx:no", "2"]));
        if let Frame::Array(Some(items)) = pangea_command("KEYS", &s(&["test:pfx:"])) {
            let strs: Vec<_> = items
                .iter()
                .filter_map(|f| match f {
                    Frame::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
                    _ => None,
                })
                .collect();
            assert!(strs.contains(&"test:pfx:yes".to_string()));
            assert!(!strs.contains(&"other:pfx:no".to_string()));
        } else {
            panic!("KEYS with prefix should return an array");
        }
    }

    #[test]
    fn node_returns_stats() {
        let _ = pangea_command("ALLOC", &s(&["test:node:k", "data"]));
        if let Frame::Array(Some(items)) = pangea_command("NODE", &s(&["0"])) {
            assert!(items.len() >= 8, "NODE should return at least 8 fields");
            assert!(matches!(&items[0], Frame::Bulk(Some(b)) if &b[..] == b"pages_total"));
        } else {
            panic!("NODE should return an array");
        }
    }

    #[test]
    fn node_invalid_errors() {
        assert!(matches!(
            pangea_command("NODE", &s(&["999"])),
            Frame::Error(_)
        ));
        assert!(matches!(
            pangea_command("NODE", &s(&["abc"])),
            Frame::Error(_)
        ));
        assert!(matches!(pangea_command("NODE", &[]), Frame::Error(_)));
    }

    #[test]
    fn policy_get_and_set() {
        // GET returns a string
        if let Frame::Bulk(Some(b)) = pangea_command("POLICY", &[]) {
            let p = String::from_utf8_lossy(&b);
            assert!(!p.is_empty());
        } else {
            panic!("POLICY with no args should return current policy");
        }
        // SET to RoundRobin
        assert!(matches!(
            pangea_command("POLICY", &s(&["RoundRobin"])),
            Frame::Simple(_)
        ));
        if let Frame::Bulk(Some(b)) = pangea_command("POLICY", &[]) {
            assert_eq!(
                &b[..],
                b"RoundRobin",
                "POLICY should reflect updated RoundRobin setting"
            );
        }
        // Restore to HashMod
        assert!(matches!(
            pangea_command("POLICY", &s(&["HashMod"])),
            Frame::Simple(_)
        ));
        // Invalid policy
        assert!(matches!(
            pangea_command("POLICY", &s(&["BadPolicy"])),
            Frame::Error(_)
        ));
    }

    #[test]
    fn migrate_moves_key_to_target_node() {
        let key = "test:migrate:k";
        let _ = pangea_command("ALLOC", &s(&[key, "migrated"]));
        // Get current location
        let old_loc = topo().locator(key).unwrap();
        let target = if old_loc.node == 0 { 1 } else { 0 };
        match pangea_command("MIGRATE", &s(&[key, &target.to_string()])) {
            Frame::Array(Some(items)) => {
                // Verify it moved to target node
                assert!(matches!(&items[1], Frame::Integer(n) if *n == target as i64));
            }
            other => panic!("MIGRATE should return array, got {:?}", other),
        }
        // Data should still be readable
        match pangea_command("READ", &s(&[key])) {
            Frame::Bulk(Some(b)) => assert_eq!(
                &b[..],
                b"migrated",
                "READ after MIGRATE should return original data"
            ),
            other => panic!("READ after MIGRATE should return data, got {:?}", other),
        }
    }

    #[test]
    fn migrate_nonexistent_key_returns_nil() {
        assert!(matches!(
            pangea_command("MIGRATE", &s(&["no:such:key", "0"])),
            Frame::Bulk(None)
        ));
    }

    #[test]
    fn migrate_invalid_args_error() {
        assert!(matches!(
            pangea_command("MIGRATE", &s(&["k"])),
            Frame::Error(_)
        ));
        assert!(matches!(
            pangea_command("MIGRATE", &s(&["k", "999"])),
            Frame::Error(_)
        ));
        assert!(matches!(
            pangea_command("MIGRATE", &s(&["k", "abc"])),
            Frame::Error(_)
        ));
    }

    #[test]
    fn tier_for_tracked_key() {
        let key = "test:tier:k1";
        // Record access in the policy engine so TIER can find it
        {
            let engine = policy_engine().read().unwrap();
            engine.record_access(key, 42_000, Tier::Dram);
        }
        match pangea_command("TIER", &s(&[key])) {
            Frame::Array(Some(items)) => {
                assert_eq!(items.len(), 6, "TIER should return 6-element array");
                assert!(matches!(&items[0], Frame::Bulk(Some(b)) if &b[..] == b"tier"));
                assert!(matches!(&items[1], Frame::Bulk(Some(b)) if &b[..] == b"DRAM"));
                assert!(matches!(&items[2], Frame::Bulk(Some(b)) if &b[..] == b"reads"));
                assert!(matches!(&items[3], Frame::Integer(1)));
                assert!(matches!(&items[4], Frame::Bulk(Some(b)) if &b[..] == b"last_access_ms"));
                assert!(matches!(&items[5], Frame::Integer(42_000)));
            }
            other => panic!("TIER should return array, got {:?}", other),
        }
    }

    #[test]
    fn tier_for_unknown_key_returns_nil() {
        assert!(matches!(
            pangea_command("TIER", &s(&["no:such:tier:key"])),
            Frame::Bulk(None)
        ));
    }

    #[test]
    fn evaluate_returns_plan() {
        if let Frame::Array(Some(items)) = pangea_command("EVALUATE", &[]) {
            assert_eq!(items.len(), 8, "EVALUATE should return 8-element array");
            assert!(matches!(&items[0], Frame::Bulk(Some(b)) if &b[..] == b"promotions"));
            assert!(matches!(&items[2], Frame::Bulk(Some(b)) if &b[..] == b"demotions"));
            assert!(matches!(&items[4], Frame::Bulk(Some(b)) if &b[..] == b"promote_keys"));
            assert!(matches!(&items[6], Frame::Bulk(Some(b)) if &b[..] == b"demote_keys"));
        } else {
            panic!("EVALUATE should return an array");
        }
    }

    #[test]
    fn evaluate_with_bad_arg_errors() {
        assert!(matches!(
            pangea_command("EVALUATE", &s(&["notanumber"])),
            Frame::Error(_)
        ));
    }

    #[test]
    fn tierpolicy_get_and_set() {
        // GET current policy
        if let Frame::Array(Some(items)) = pangea_command("TIERPOLICY", &[]) {
            assert!(items.len() >= 10);
            assert!(matches!(&items[0], Frame::Bulk(Some(b)) if &b[..] == b"promote_threshold"));
        } else {
            panic!("TIERPOLICY with no args should return current policy");
        }
        // SET new policy
        assert!(matches!(
            pangea_command("TIERPOLICY", &s(&["20", "30000", "0.9"])),
            Frame::Simple(_)
        ));
        // Verify updated values
        if let Frame::Array(Some(items)) = pangea_command("TIERPOLICY", &[]) {
            assert!(matches!(&items[1], Frame::Integer(20)));
            assert!(matches!(&items[3], Frame::Integer(30_000)));
        } else {
            panic!("TIERPOLICY should return updated policy");
        }
        // Wrong number of args
        assert!(matches!(
            pangea_command("TIERPOLICY", &s(&["20"])),
            Frame::Error(_)
        ));
    }

    #[test]
    fn detect_returns_capability_array() {
        if let Frame::Array(Some(items)) = pangea_command("DETECT", &[]) {
            // Minimum fields: available, detection_method, numa_nodes (6 items = 3 pairs)
            assert!(items.len() >= 6, "DETECT should return at least 6 items");
            assert!(matches!(&items[0], Frame::Bulk(Some(b)) if &b[..] == b"available"));
            assert!(matches!(&items[2], Frame::Bulk(Some(b)) if &b[..] == b"detection_method"));
            assert!(matches!(&items[4], Frame::Bulk(Some(b)) if &b[..] == b"numa_nodes"));
        } else {
            panic!("DETECT should return an array");
        }
    }

    #[test]
    fn bench_returns_stats() {
        if let Frame::Array(Some(items)) = pangea_command("BENCH", &s(&["100"])) {
            assert!(items.len() >= 12, "BENCH should return at least 12 items");
            assert!(matches!(&items[0], Frame::Bulk(Some(b)) if &b[..] == b"tier"));
            assert!(matches!(&items[2], Frame::Bulk(Some(b)) if &b[..] == b"operations"));
            if let Frame::Integer(ops) = &items[3] {
                assert_eq!(*ops, 100, "BENCH operations should match requested count");
            } else {
                panic!("operations should be integer");
            }
            assert!(matches!(&items[4], Frame::Bulk(Some(b)) if &b[..] == b"p50_ns"));
            assert!(matches!(&items[6], Frame::Bulk(Some(b)) if &b[..] == b"p99_ns"));
        } else {
            panic!("BENCH should return an array");
        }
    }

    #[test]
    fn bench_default_iterations() {
        // No args → defaults to 1000 iterations
        if let Frame::Array(Some(items)) = pangea_command("BENCH", &[]) {
            if let Frame::Integer(ops) = &items[3] {
                assert_eq!(*ops, 1000, "BENCH default should use 1000 iterations");
            }
        } else {
            panic!("BENCH with no args should work");
        }
    }

    #[test]
    fn bench_invalid_arg_errors() {
        assert!(matches!(
            pangea_command("BENCH", &s(&["0"])),
            Frame::Error(_)
        ));
        assert!(matches!(
            pangea_command("BENCH", &s(&["-1"])),
            Frame::Error(_)
        ));
        assert!(matches!(
            pangea_command("BENCH", &s(&["abc"])),
            Frame::Error(_)
        ));
    }

    #[test]
    fn sizing_returns_recommendation() {
        if let Frame::Array(Some(items)) = pangea_command("SIZING", &s(&["100"])) {
            assert_eq!(
                items.len(),
                14,
                "SIZING should return 14 items (7 key-value pairs)"
            );
            assert!(matches!(&items[0], Frame::Bulk(Some(b)) if &b[..] == b"working_set_gib"));
            assert!(matches!(&items[6], Frame::Bulk(Some(b)) if &b[..] == b"savings_pct"));
        } else {
            panic!("SIZING should return an array");
        }
    }

    #[test]
    fn sizing_with_hot_ratio() {
        if let Frame::Array(Some(items)) = pangea_command("SIZING", &s(&["200", "0.7"])) {
            // recommended_dram_gib should be 200*0.7 = 140
            assert!(matches!(&items[8], Frame::Bulk(Some(b)) if &b[..] == b"recommended_dram_gib"));
            if let Frame::Bulk(Some(b)) = &items[9] {
                let val: f64 = String::from_utf8_lossy(b).parse().unwrap();
                assert!((val - 140.0).abs() < 0.01);
            } else {
                panic!("expected bulk string for recommended_dram_gib value");
            }
        } else {
            panic!("SIZING with hot_ratio should return an array");
        }
    }

    #[test]
    fn sizing_bad_args_errors() {
        assert!(matches!(pangea_command("SIZING", &[]), Frame::Error(_)));
        assert!(matches!(
            pangea_command("SIZING", &s(&["abc"])),
            Frame::Error(_)
        ));
        assert!(matches!(
            pangea_command("SIZING", &s(&["-1"])),
            Frame::Error(_)
        ));
        assert!(matches!(
            pangea_command("SIZING", &s(&["100", "2.0"])),
            Frame::Error(_)
        ));
    }

    #[test]
    fn alloc_rejects_empty_key() {
        let result = pangea_command("ALLOC", &s(&["", "value"]));
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn alloc_rejects_oversized_key() {
        let big_key = "x".repeat(super::super::moonshot_limits::MAX_KEY_LEN + 1);
        let result = pangea_command("ALLOC", &s(&[&big_key, "value"]));
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn moonshot_config_integration() {
        let cfg = super::super::moonshot_config::get();
        assert!(cfg.pangea.enabled);
    }
}
