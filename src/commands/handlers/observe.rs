//! Observability command handlers
//!
//! OBSERVE.* commands for real-time monitoring and diagnostics.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::OnceLock;

use bytes::Bytes;

use crate::protocol::Frame;
use ferrite_core::observability::key_heatmap::{HeatmapOp, KeyHeatmap};

use super::err_frame;

/// Global key heatmap instance.
static KEY_HEATMAP: OnceLock<KeyHeatmap> = OnceLock::new();

/// Return the global heatmap singleton.
pub fn global_heatmap() -> &'static KeyHeatmap {
    KEY_HEATMAP.get_or_init(|| KeyHeatmap::new(4096, 256))
}

/// Record a key access from the command execution path.
pub fn record_command_access(key: &[u8], is_write: bool) {
    let op = if is_write {
        HeatmapOp::Set
    } else {
        HeatmapOp::Get
    };
    global_heatmap().record_access(key, op);
}

/// Dispatch an `OBSERVE` subcommand.
pub fn observe(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "HOTKEYS" => observe_hotkeys(args),
        "HOTPREFIXES" => observe_hotprefixes(args),
        "HEATMAP" => observe_heatmap(),
        "RECORD" => observe_record(args),
        "RESET" => observe_reset(),
        "STATS" => observe_stats(),
        "LATENCY" => observe_latency(args),
        "HELP" => observe_help(),
        _ => err_frame(&format!("unknown OBSERVE subcommand '{}'", subcommand)),
    }
}

/// OBSERVE.HOTKEYS [TOP n]
fn observe_hotkeys(args: &[String]) -> Frame {
    let mut top_n: usize = 10;

    let mut i = 0;
    while i < args.len() {
        if args[i].to_uppercase() == "TOP" {
            i += 1;
            if i < args.len() {
                top_n = args[i].parse().unwrap_or(10);
            }
        }
        i += 1;
    }

    let hot_keys = global_heatmap().get_hot_keys(top_n);
    let frames: Vec<Frame> = hot_keys
        .into_iter()
        .map(|entry| {
            let key_str = String::from_utf8_lossy(&entry.key).to_string();
            let secs_ago = entry.last_access.elapsed().as_secs();
            Frame::array(vec![
                Frame::bulk("key"),
                Frame::bulk(Bytes::from(key_str)),
                Frame::bulk("ops_count"),
                Frame::Integer(entry.access_count as i64),
                Frame::bulk("last_access_secs_ago"),
                Frame::Integer(secs_ago as i64),
            ])
        })
        .collect();
    Frame::array(frames)
}

/// OBSERVE.HOTPREFIXES [TOP n]
fn observe_hotprefixes(args: &[String]) -> Frame {
    let mut top_n: usize = 10;

    let mut i = 0;
    while i < args.len() {
        if args[i].to_uppercase() == "TOP" {
            i += 1;
            if i < args.len() {
                top_n = args[i].parse().unwrap_or(10);
            }
        }
        i += 1;
    }

    let prefixes = global_heatmap().get_hot_prefixes(top_n);
    let frames: Vec<Frame> = prefixes
        .into_iter()
        .map(|entry| {
            Frame::array(vec![
                Frame::bulk("prefix"),
                Frame::bulk(Bytes::from(entry.prefix)),
                Frame::bulk("key_count"),
                Frame::Integer(entry.key_count as i64),
                Frame::bulk("total_accesses"),
                Frame::Integer(entry.total_accesses as i64),
            ])
        })
        .collect();
    Frame::array(frames)
}

/// OBSERVE.HEATMAP — return slot-level heatmap as array of 16384 access counts.
fn observe_heatmap() -> Frame {
    let slots = global_heatmap().get_slot_heatmap();
    let frames: Vec<Frame> = slots.iter().map(|&count| Frame::Integer(count as i64)).collect();
    Frame::array(frames)
}

/// OBSERVE.RECORD key op — manually record a key access (for testing).
fn observe_record(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("OBSERVE RECORD requires <key> <op>");
    }

    let key = args[0].as_bytes();
    let op = match args[1].to_uppercase().as_str() {
        "GET" => HeatmapOp::Get,
        "SET" => HeatmapOp::Set,
        "DEL" => HeatmapOp::Del,
        "EXPIRE" => HeatmapOp::Expire,
        "SCAN" => HeatmapOp::Scan,
        _ => HeatmapOp::Get,
    };

    global_heatmap().record_access(key, op);
    Frame::simple("OK")
}

/// OBSERVE.RESET — reset all heatmap data.
fn observe_reset() -> Frame {
    global_heatmap().reset();
    Frame::simple("OK")
}

/// OBSERVE.STATS — return heatmap statistics.
fn observe_stats() -> Frame {
    let snapshot = global_heatmap().snapshot();

    let top_slot = snapshot
        .slot_distribution
        .iter()
        .copied()
        .max()
        .unwrap_or(0);

    let unique_keys = snapshot.hot_keys.len();

    let mut map = HashMap::new();
    map.insert(
        Bytes::from_static(b"total_ops"),
        Frame::Integer(snapshot.total_ops as i64),
    );
    map.insert(
        Bytes::from_static(b"unique_keys"),
        Frame::Integer(unique_keys as i64),
    );
    map.insert(
        Bytes::from_static(b"top_slot"),
        Frame::Integer(top_slot as i64),
    );
    Frame::Map(map)
}

/// OBSERVE.LATENCY [command_name] — return latency percentiles placeholder.
fn observe_latency(args: &[String]) -> Frame {
    // Latency tracking is a placeholder; return default percentile structure
    let cmd = args.first().map(|s| s.as_str()).unwrap_or("ALL");

    let mut map = HashMap::new();
    map.insert(
        Bytes::from_static(b"command"),
        Frame::bulk(Bytes::from(cmd.to_string())),
    );
    map.insert(Bytes::from_static(b"p50"), Frame::Double(0.0));
    map.insert(Bytes::from_static(b"p90"), Frame::Double(0.0));
    map.insert(Bytes::from_static(b"p99"), Frame::Double(0.0));
    map.insert(Bytes::from_static(b"p99.9"), Frame::Double(0.0));
    Frame::Map(map)
}

/// OBSERVE.HELP
fn observe_help() -> Frame {
    Frame::array(vec![
        Frame::bulk("OBSERVE.HOTKEYS [TOP n]"),
        Frame::bulk("  Return the top N hottest keys from the KeyHeatmap."),
        Frame::bulk("OBSERVE.HOTPREFIXES [TOP n]"),
        Frame::bulk("  Return the hottest key prefixes."),
        Frame::bulk("OBSERVE.HEATMAP"),
        Frame::bulk("  Return slot-level heatmap as array of 16384 access counts."),
        Frame::bulk("OBSERVE.RECORD key op"),
        Frame::bulk("  Manually record a key access (for testing)."),
        Frame::bulk("OBSERVE.RESET"),
        Frame::bulk("  Reset all heatmap data."),
        Frame::bulk("OBSERVE.STATS"),
        Frame::bulk("  Return heatmap statistics (total_ops, unique_keys, top_slot)."),
        Frame::bulk("OBSERVE.LATENCY [command_name]"),
        Frame::bulk("  Return latency percentiles (P50, P90, P99, P99.9)."),
    ])
}
