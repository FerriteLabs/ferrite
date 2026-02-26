//! Edge runtime command handlers
//!
//! Commands:
//! - EDGE.STATUS  — Get edge node runtime status
//! - EDGE.STATS   — Get sync statistics
//! - EDGE.CONFIG  — Get edge configuration
//! - EDGE.SYNC    — Trigger manual sync (placeholder)
//! - EDGE.PREFIXES — List replicated prefixes

use std::sync::OnceLock;

use bytes::Bytes;
use parking_lot::RwLock;

use crate::protocol::Frame;
use ferrite_cloud::edge::EdgeRuntime;

use super::err_frame;

/// Global edge runtime instance.
static EDGE_RUNTIME: OnceLock<RwLock<EdgeRuntime>> = OnceLock::new();

fn get_runtime() -> &'static RwLock<EdgeRuntime> {
    EDGE_RUNTIME.get_or_init(|| RwLock::new(EdgeRuntime::with_defaults()))
}

/// Dispatch an `EDGE` subcommand.
pub fn handle_edge(subcommand: &str, args: &[Bytes]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "STATUS" => edge_status(),
        "STATS" => edge_stats(),
        "CONFIG" => edge_config(),
        "SYNC" => edge_sync(args),
        "PREFIXES" => edge_prefixes(),
        _ => err_frame(&format!("unknown EDGE subcommand: {}", subcommand)),
    }
}

/// EDGE.STATUS — returns current runtime status as a map.
fn edge_status() -> Frame {
    let rt = get_runtime().read();
    let summary = rt.summary();

    let mut map = std::collections::HashMap::new();
    for (k, v) in &summary {
        map.insert(
            Bytes::from(k.clone()),
            Frame::Bulk(Bytes::from(v.clone())),
        );
    }
    Frame::Map(map)
}

/// EDGE.STATS — returns sync statistics.
fn edge_stats() -> Frame {
    let rt = get_runtime().read();
    let stats = rt.sync_stats();

    let mut map = std::collections::HashMap::new();
    map.insert(
        Bytes::from_static(b"total_syncs"),
        Frame::Integer(stats.total_syncs as i64),
    );
    map.insert(
        Bytes::from_static(b"failed_syncs"),
        Frame::Integer(stats.failed_syncs as i64),
    );
    map.insert(
        Bytes::from_static(b"bytes_sent"),
        Frame::Integer(stats.bytes_sent as i64),
    );
    map.insert(
        Bytes::from_static(b"bytes_received"),
        Frame::Integer(stats.bytes_received as i64),
    );
    map.insert(
        Bytes::from_static(b"keys_synced"),
        Frame::Integer(stats.keys_synced as i64),
    );
    map.insert(
        Bytes::from_static(b"conflicts_resolved"),
        Frame::Integer(stats.conflicts_resolved as i64),
    );
    map.insert(
        Bytes::from_static(b"sync_lag_ms"),
        Frame::Integer(stats.sync_lag_ms as i64),
    );
    Frame::Map(map)
}

/// EDGE.CONFIG — returns current edge node configuration.
fn edge_config() -> Frame {
    let rt = get_runtime().read();
    let cfg = rt.config();

    let mut map = std::collections::HashMap::new();
    map.insert(
        Bytes::from_static(b"node_id"),
        Frame::Bulk(Bytes::from(cfg.node_id.clone())),
    );
    map.insert(
        Bytes::from_static(b"max_memory"),
        Frame::Integer(cfg.max_memory as i64),
    );
    map.insert(
        Bytes::from_static(b"max_disk"),
        Frame::Integer(cfg.max_disk as i64),
    );
    map.insert(
        Bytes::from_static(b"max_keys"),
        Frame::Integer(cfg.max_keys as i64),
    );
    map.insert(
        Bytes::from_static(b"sync_policy"),
        Frame::Bulk(Bytes::from(format!("{:?}", cfg.sync_policy))),
    );
    map.insert(
        Bytes::from_static(b"conflict_resolution"),
        Frame::Bulk(Bytes::from(format!("{:?}", cfg.conflict_resolution))),
    );
    map.insert(
        Bytes::from_static(b"compression"),
        Frame::Integer(if cfg.compression { 1 } else { 0 }),
    );
    map.insert(
        Bytes::from_static(b"sync_interval_secs"),
        Frame::Integer(cfg.sync_interval.as_secs() as i64),
    );
    Frame::Map(map)
}

/// EDGE.SYNC — trigger a manual sync (placeholder; real I/O not implemented).
fn edge_sync(_args: &[Bytes]) -> Frame {
    // In a full implementation this would initiate an async sync with the
    // upstream cluster. For now we record a zero-length sync.
    let mut rt = get_runtime().write();
    rt.record_sync(0, 0, 0, std::time::Duration::from_millis(0));
    Frame::Simple(Bytes::from("OK"))
}

/// EDGE.PREFIXES — list the replicated key prefixes.
fn edge_prefixes() -> Frame {
    let rt = get_runtime().read();
    let prefixes = rt.replicated_prefixes();
    Frame::array(
        prefixes
            .iter()
            .map(|p| Frame::Bulk(Bytes::from(p.clone())))
            .collect(),
    )
}
