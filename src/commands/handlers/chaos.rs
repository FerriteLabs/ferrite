//! CHAOS.* command handlers
//!
//! CHAOS.INJECT, CHAOS.HEAL, CHAOS.LIST, CHAOS.SCENARIO, CHAOS.STATS,
//! CHAOS.HELP
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use bytes::Bytes;

use crate::protocol::Frame;
use ferrite_core::observability::chaos::{
    ChaosConfig, ChaosScenario, FaultSpec, FaultTarget, RuntimeChaosEngine, RuntimeFaultType,
};

use super::err_frame;

/// Global runtime chaos engine singleton.
static CHAOS_ENGINE: OnceLock<RuntimeChaosEngine> = OnceLock::new();

fn get_engine() -> &'static RuntimeChaosEngine {
    CHAOS_ENGINE.get_or_init(|| {
        RuntimeChaosEngine::new(ChaosConfig {
            enabled: true,
            require_flag: false,
            max_active_faults: 50,
            safety_key_prefix: None,
        })
    })
}

/// Dispatch a `CHAOS` subcommand.
pub fn chaos_command(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "INJECT" => handle_inject(args),
        "HEAL" => handle_heal(args),
        "LIST" => handle_list(),
        "SCENARIO" => handle_scenario(args),
        "STATS" => handle_stats(),
        "HELP" | "" => handle_help(),
        other => err_frame(&format!("unknown CHAOS subcommand '{}'", other)),
    }
}

// ---------------------------------------------------------------------------
// CHAOS.INJECT
// ---------------------------------------------------------------------------

fn handle_inject(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("CHAOS.INJECT requires a fault type: LATENCY|ERROR|SLOW_IO|TIMEOUT|DROP");
    }

    let fault_kind = args[0].to_uppercase();
    let rest = &args[1..];

    let (fault_type, mut target, mut duration, mut probability) = match fault_kind.as_str() {
        "LATENCY" => {
            let delay_ms: u64 = rest.first().and_then(|s| s.parse().ok()).unwrap_or(100);
            let jitter_ms = parse_opt_kv(rest, "JITTER").unwrap_or(0);
            (
                RuntimeFaultType::Latency { delay_ms, jitter_ms },
                FaultTarget::All,
                None,
                1.0_f64,
            )
        }
        "ERROR" => {
            let rate: f64 = rest.first().and_then(|s| s.parse().ok()).unwrap_or(0.5);
            (
                RuntimeFaultType::ErrorRate { rate },
                FaultTarget::All,
                None,
                1.0,
            )
        }
        "SLOW_IO" => {
            let read_ms: u64 = rest.first().and_then(|s| s.parse().ok()).unwrap_or(100);
            let write_ms: u64 = rest.get(1).and_then(|s| s.parse().ok()).unwrap_or(100);
            (
                RuntimeFaultType::SlowIo { read_ms, write_ms },
                FaultTarget::All,
                None,
                1.0,
            )
        }
        "TIMEOUT" => {
            let timeout_ms: u64 = rest.first().and_then(|s| s.parse().ok()).unwrap_or(5000);
            (
                RuntimeFaultType::Timeout { timeout_ms },
                FaultTarget::All,
                None,
                1.0,
            )
        }
        "DROP" => {
            let rate: f64 = rest.first().and_then(|s| s.parse().ok()).unwrap_or(0.5);
            (
                RuntimeFaultType::RandomDrop { rate },
                FaultTarget::All,
                None,
                1.0,
            )
        }
        other => return err_frame(&format!("unknown fault type '{}'", other)),
    };

    // Parse optional modifiers from remaining args
    if let Some(v) = parse_opt_kv_str(rest, "KEY") {
        target = FaultTarget::KeyPattern(v);
    }
    if let Some(v) = parse_opt_kv_str(rest, "COMMAND") {
        target = FaultTarget::Command(v);
    }
    if let Some(v) = parse_opt_kv::<u64>(rest, "DURATION") {
        duration = Some(Duration::from_secs(v));
    }
    if let Some(v) = parse_opt_kv::<f64>(rest, "PROBABILITY") {
        probability = v;
    }

    let spec = FaultSpec {
        fault_type,
        target,
        duration,
        probability,
        description: format!("Injected via CHAOS.INJECT {}", fault_kind),
        created_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    };

    match get_engine().inject(spec) {
        Ok(id) => {
            let mut map = HashMap::new();
            map.insert(Bytes::from_static(b"fault_id"), Frame::Integer(id as i64));
            map.insert(
                Bytes::from_static(b"status"),
                Frame::Bulk(Some(Bytes::from("injected"))),
            );
            Frame::Map(map)
        }
        Err(e) => err_frame(&format!("CHAOS.INJECT failed: {}", e)),
    }
}

// ---------------------------------------------------------------------------
// CHAOS.HEAL
// ---------------------------------------------------------------------------

fn handle_heal(args: &[String]) -> Frame {
    let engine = get_engine();
    match args.first().map(|s| s.to_uppercase()).as_deref() {
        Some("ALL") => {
            let count = engine.heal_all();
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"healed"),
                Frame::Integer(count as i64),
            );
            Frame::Map(map)
        }
        Some(id_str) => match id_str.parse::<u64>() {
            Ok(id) => match engine.heal(id) {
                Ok(()) => Frame::Simple(Bytes::from("OK")),
                Err(e) => err_frame(&format!("CHAOS.HEAL failed: {}", e)),
            },
            Err(_) => err_frame("CHAOS.HEAL requires a fault_id or ALL"),
        },
        None => err_frame("CHAOS.HEAL requires a fault_id or ALL"),
    }
}

// ---------------------------------------------------------------------------
// CHAOS.LIST
// ---------------------------------------------------------------------------

fn handle_list() -> Frame {
    let faults = get_engine().active_faults();
    let items: Vec<Frame> = faults
        .iter()
        .map(|f| {
            let mut map = HashMap::new();
            map.insert(Bytes::from_static(b"id"), Frame::Integer(f.id as i64));
            map.insert(
                Bytes::from_static(b"fault_type"),
                Frame::Bulk(Some(Bytes::from(f.fault_type.clone()))),
            );
            map.insert(
                Bytes::from_static(b"target"),
                Frame::Bulk(Some(Bytes::from(f.target.clone()))),
            );
            map.insert(
                Bytes::from_static(b"triggered"),
                Frame::Integer(f.triggered as i64),
            );
            Frame::Map(map)
        })
        .collect();
    Frame::Array(Some(items))
}

// ---------------------------------------------------------------------------
// CHAOS.SCENARIO
// ---------------------------------------------------------------------------

fn handle_scenario(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("CHAOS.SCENARIO requires: PARTITION|CASCADE|DEGRADE");
    }
    let kind = args[0].to_uppercase();
    let scenario = match kind.as_str() {
        "PARTITION" => {
            let secs: u64 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(30);
            ChaosScenario::NetworkPartition {
                duration: Duration::from_secs(secs),
            }
        }
        "CASCADE" => {
            let start: f64 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(0.1);
            let inc: f64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(0.1);
            let secs: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(60);
            ChaosScenario::CascadingFailure {
                start_rate: start,
                increase_rate: inc,
                duration: Duration::from_secs(secs),
            }
        }
        "DEGRADE" => {
            let target: u64 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(500);
            let step: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(100);
            let steps: u32 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(5);
            ChaosScenario::GracefulDegradation {
                target_latency_ms: target,
                step_ms: step,
                steps,
            }
        }
        other => return err_frame(&format!("unknown scenario '{}'", other)),
    };

    let result = get_engine().run_scenario(scenario);
    let mut map = HashMap::new();
    map.insert(
        Bytes::from_static(b"scenario"),
        Frame::Bulk(Some(Bytes::from(result.scenario.clone()))),
    );
    map.insert(
        Bytes::from_static(b"faults_injected"),
        Frame::Integer(result.faults_injected as i64),
    );
    Frame::Map(map)
}

// ---------------------------------------------------------------------------
// CHAOS.STATS
// ---------------------------------------------------------------------------

fn handle_stats() -> Frame {
    let stats = get_engine().stats();
    let mut map = HashMap::new();
    map.insert(
        Bytes::from_static(b"active_faults"),
        Frame::Integer(stats.active_faults as i64),
    );
    map.insert(
        Bytes::from_static(b"total_injected"),
        Frame::Integer(stats.total_injected as i64),
    );
    map.insert(
        Bytes::from_static(b"total_healed"),
        Frame::Integer(stats.total_healed as i64),
    );
    map.insert(
        Bytes::from_static(b"total_triggered"),
        Frame::Integer(stats.total_triggered as i64),
    );
    map.insert(
        Bytes::from_static(b"scenarios_run"),
        Frame::Integer(stats.scenarios_run as i64),
    );
    Frame::Map(map)
}

// ---------------------------------------------------------------------------
// CHAOS.HELP
// ---------------------------------------------------------------------------

fn handle_help() -> Frame {
    let help = vec![
        "CHAOS.INJECT LATENCY delay_ms [JITTER ms] [KEY pattern] [COMMAND cmd] [DURATION secs] [PROBABILITY 0.0-1.0]",
        "CHAOS.INJECT ERROR rate [COMMAND cmd] [DURATION secs]",
        "CHAOS.INJECT SLOW_IO read_ms write_ms [DURATION secs]",
        "CHAOS.INJECT TIMEOUT timeout_ms [COMMAND cmd]",
        "CHAOS.INJECT DROP rate [KEY pattern]",
        "CHAOS.HEAL <fault_id> | ALL",
        "CHAOS.LIST",
        "CHAOS.SCENARIO PARTITION|CASCADE|DEGRADE [params]",
        "CHAOS.STATS",
        "CHAOS.HELP",
    ];
    Frame::Array(Some(
        help.iter().map(|s| Frame::Bulk(Some(Bytes::from(*s)))).collect(),
    ))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_opt_kv<T: std::str::FromStr>(args: &[String], key: &str) -> Option<T> {
    args.iter()
        .position(|s| s.eq_ignore_ascii_case(key))
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse::<T>().ok())
}

fn parse_opt_kv_str(args: &[String], key: &str) -> Option<String> {
    args.iter()
        .position(|s| s.eq_ignore_ascii_case(key))
        .and_then(|i| args.get(i + 1))
        .cloned()
}
