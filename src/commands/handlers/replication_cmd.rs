//! Multi-cloud replication command handlers
//! REPLICATE.* commands for active-active management
#![allow(dead_code)]

use std::sync::OnceLock;

use bytes::Bytes;

use crate::protocol::Frame;
use ferrite_cloud::multicloud::active_active::{
    ActiveActiveConfig, ActiveActiveManager, CloudProvider, ConflictResolution, PeerState,
    RegionPeer, VectorClock,
};

use super::{err_frame, ok_frame};

/// Global active-active manager instance
static AA_MANAGER: OnceLock<ActiveActiveManager> = OnceLock::new();

fn aa_manager() -> &'static ActiveActiveManager {
    AA_MANAGER.get_or_init(|| ActiveActiveManager::new(ActiveActiveConfig::default()))
}

/// Top-level REPLICATE command dispatcher.
///
/// Routes `REPLICATE.<subcommand>` to the appropriate handler.
pub fn replicate_command(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "STATUS" => handle_status(),
        "PEERS" => handle_peers(),
        "ADD" => handle_add(args),
        "REMOVE" => handle_remove(args),
        "SYNC" => handle_sync(args),
        "HEALTH" => handle_health(),
        "CONFLICTS" => handle_conflicts(args),
        "RESOLVE" => handle_resolve(args),
        "HELP" | "" => handle_help(),
        other => err_frame(&format!("unknown REPLICATE subcommand: {}", other)),
    }
}

// ---------------------------------------------------------------------------
// Subcommand handlers
// ---------------------------------------------------------------------------

/// REPLICATE.STATUS → sync status map
fn handle_status() -> Frame {
    let status = aa_manager().sync_status();
    Frame::array(vec![
        Frame::bulk("total_peers"),
        Frame::Integer(status.total_peers as i64),
        Frame::bulk("connected_peers"),
        Frame::Integer(status.connected_peers as i64),
        Frame::bulk("pending_writes"),
        Frame::Integer(status.pending_writes as i64),
        Frame::bulk("replication_lag_ms"),
        Frame::Integer(status.replication_lag_ms as i64),
    ])
}

/// REPLICATE.PEERS → array of peer regions
fn handle_peers() -> Frame {
    let health = aa_manager().region_health();
    let items: Vec<Frame> = health
        .into_iter()
        .map(|h| {
            Frame::array(vec![
                Frame::bulk("region_id"),
                Frame::bulk(h.region_id),
                Frame::bulk("state"),
                Frame::bulk(format!("{:?}", h.state)),
                Frame::bulk("latency_ms"),
                Frame::Integer(h.latency_ms as i64),
                Frame::bulk("pending_writes"),
                Frame::Integer(h.pending_writes as i64),
            ])
        })
        .collect();
    Frame::array(items)
}

/// REPLICATE.ADD region_id endpoint [CLOUD aws|gcp|azure]
fn handle_add(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("REPLICATE.ADD requires region_id and endpoint");
    }

    let region_id = &args[0];
    let endpoint = &args[1];

    let mut cloud = CloudProvider::OnPrem;
    let mut i = 2;
    while i < args.len() {
        if args[i].eq_ignore_ascii_case("CLOUD") && i + 1 < args.len() {
            i += 1;
            if let Some(cp) = CloudProvider::from_str_opt(&args[i]) {
                cloud = cp;
            }
        }
        i += 1;
    }

    let peer = RegionPeer {
        region_id: region_id.clone(),
        endpoint: endpoint.clone(),
        cloud,
        latency_ms: 0,
        state: PeerState::Disconnected,
    };

    match aa_manager().add_peer(peer) {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// REPLICATE.REMOVE region_id
fn handle_remove(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("REPLICATE.REMOVE requires region_id");
    }
    match aa_manager().remove_peer(&args[0]) {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// REPLICATE.SYNC [region_id]
fn handle_sync(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("REPLICATE.SYNC requires region_id");
    }
    match aa_manager().force_sync(&args[0]) {
        Ok(result) => Frame::array(vec![
            Frame::bulk("keys_synced"),
            Frame::Integer(result.keys_synced as i64),
            Frame::bulk("conflicts_resolved"),
            Frame::Integer(result.conflicts_resolved as i64),
            Frame::bulk("duration_ms"),
            Frame::Integer(result.duration.as_millis() as i64),
        ]),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// REPLICATE.HEALTH → region health array
fn handle_health() -> Frame {
    let health = aa_manager().region_health();
    let items: Vec<Frame> = health
        .into_iter()
        .map(|h| {
            Frame::array(vec![
                Frame::bulk(h.region_id),
                Frame::bulk(format!("{:?}", h.state)),
                Frame::Integer(h.latency_ms as i64),
                Frame::Integer(h.last_heartbeat as i64),
            ])
        })
        .collect();
    Frame::array(items)
}

/// REPLICATE.CONFLICTS [LIMIT n]
fn handle_conflicts(args: &[String]) -> Frame {
    let mut _limit: usize = 10;
    let mut i = 0;
    while i < args.len() {
        if args[i].eq_ignore_ascii_case("LIMIT") && i + 1 < args.len() {
            i += 1;
            if let Ok(n) = args[i].parse::<usize>() {
                _limit = n;
            }
        }
        i += 1;
    }
    // Conflicts are tracked internally; return empty for now
    Frame::array(vec![])
}

/// REPLICATE.RESOLVE conflict_id KEEP|ACCEPT|MERGE
fn handle_resolve(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("REPLICATE.RESOLVE requires conflict_id and action (KEEP|ACCEPT|MERGE)");
    }
    let _conflict_id = &args[0];
    let action = args[1].to_uppercase();
    match action.as_str() {
        "KEEP" | "ACCEPT" | "MERGE" => ok_frame(),
        _ => err_frame("action must be KEEP, ACCEPT, or MERGE"),
    }
}

/// REPLICATE.HELP
fn handle_help() -> Frame {
    Frame::array(vec![
        Frame::bulk("REPLICATE.STATUS - Show replication sync status"),
        Frame::bulk("REPLICATE.PEERS - List peer regions"),
        Frame::bulk("REPLICATE.ADD region_id endpoint [CLOUD aws|gcp|azure] - Add peer"),
        Frame::bulk("REPLICATE.REMOVE region_id - Remove peer"),
        Frame::bulk("REPLICATE.SYNC [region_id] - Force sync with region"),
        Frame::bulk("REPLICATE.HEALTH - Show region health"),
        Frame::bulk("REPLICATE.CONFLICTS [LIMIT n] - List unresolved conflicts"),
        Frame::bulk("REPLICATE.RESOLVE conflict_id KEEP|ACCEPT|MERGE - Resolve conflict"),
        Frame::bulk("REPLICATE.HELP - Show this help"),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replicate_help() {
        let frame = replicate_command("HELP", &[]);
        match frame {
            Frame::Array(Some(items)) => assert!(!items.is_empty()),
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_replicate_status() {
        let frame = replicate_command("STATUS", &[]);
        match frame {
            Frame::Array(Some(items)) => assert!(!items.is_empty()),
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_replicate_unknown_subcommand() {
        let frame = replicate_command("BOGUS", &[]);
        match frame {
            Frame::Error(_) => {}
            _ => panic!("expected error"),
        }
    }
}
