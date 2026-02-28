//! MESH.* command handlers
//!
//! MESH.DISCOVER, MESH.PEERS, MESH.PEER.ADD, MESH.PEER.REMOVE, MESH.PEER.BAN,
//! MESH.PEER.INFO, MESH.TOPOLOGY, MESH.SYNC, MESH.BROADCAST, MESH.ROUTE,
//! MESH.HEALTH, MESH.STATS, MESH.HELP
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::OnceLock;

use bytes::Bytes;

use crate::protocol::Frame;
use ferrite_cloud::edge::mesh::{EdgeMeshManager, MeshConfig, PeerRole};

use super::err_frame;

/// Global mesh manager singleton.
static MESH_MANAGER: OnceLock<EdgeMeshManager> = OnceLock::new();

fn get_manager() -> &'static EdgeMeshManager {
    MESH_MANAGER.get_or_init(|| EdgeMeshManager::new(MeshConfig::default()))
}

/// Dispatch a `MESH` subcommand.
pub fn mesh_command(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "DISCOVER" => handle_discover(args),
        "PEERS" => handle_peers(),
        "PEER.ADD" => handle_peer_add(args),
        "PEER.REMOVE" => handle_peer_remove(args),
        "PEER.BAN" => handle_peer_ban(args),
        "PEER.INFO" => handle_peer_info(args),
        "TOPOLOGY" => handle_topology(),
        "SYNC" => handle_sync(args),
        "BROADCAST" => handle_broadcast(args),
        "ROUTE" => handle_route(args),
        "HEALTH" => handle_health(),
        "STATS" => handle_stats(),
        "HELP" | "" => handle_help(),
        other => err_frame(&format!("unknown MESH subcommand '{}'", other)),
    }
}

/// MESH.DISCOVER [MDNS|BOOTSTRAP addr1,addr2] — start discovery.
fn handle_discover(args: &[String]) -> Frame {
    match get_manager().start_discovery() {
        Ok(()) => {
            let method = args
                .first()
                .map(|s| s.as_str())
                .unwrap_or("MDNS");
            Frame::Simple(Bytes::from(format!("OK discovery started ({})", method)))
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// MESH.PEERS — list all peers.
fn handle_peers() -> Frame {
    let peers = get_manager().peers();
    if peers.is_empty() {
        return Frame::Array(Some(vec![Frame::Bulk(Some(Bytes::from("(no peers)")))]));
    }
    let frames: Vec<Frame> = peers
        .iter()
        .map(|p| {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"id"),
                Frame::Bulk(Some(Bytes::from(p.id.clone()))),
            );
            map.insert(
                Bytes::from_static(b"addr"),
                Frame::Bulk(Some(Bytes::from(p.addr.to_string()))),
            );
            map.insert(
                Bytes::from_static(b"role"),
                Frame::Bulk(Some(Bytes::from(format!("{:?}", p.role)))),
            );
            map.insert(
                Bytes::from_static(b"state"),
                Frame::Bulk(Some(Bytes::from(format!("{:?}", p.state)))),
            );
            map.insert(
                Bytes::from_static(b"latency_ms"),
                Frame::Integer(p.latency_ms as i64),
            );
            Frame::Map(map)
        })
        .collect();
    Frame::Array(Some(frames))
}

/// MESH.PEER.ADD addr [ROLE full|readonly|relay] — manually add a peer.
fn handle_peer_add(args: &[String]) -> Frame {
    let addr_str = match args.first() {
        Some(a) => a,
        None => return err_frame("MESH.PEER.ADD requires an address"),
    };
    let addr = match addr_str.parse() {
        Ok(a) => a,
        Err(_) => return err_frame(&format!("invalid address: {}", addr_str)),
    };
    let role = args
        .iter()
        .position(|a| a.to_uppercase() == "ROLE")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse::<PeerRole>().ok())
        .unwrap_or(PeerRole::Full);

    match get_manager().add_peer(addr, role) {
        Ok(peer) => {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"id"),
                Frame::Bulk(Some(Bytes::from(peer.id))),
            );
            map.insert(
                Bytes::from_static(b"addr"),
                Frame::Bulk(Some(Bytes::from(peer.addr.to_string()))),
            );
            map.insert(
                Bytes::from_static(b"role"),
                Frame::Bulk(Some(Bytes::from(format!("{:?}", peer.role)))),
            );
            map.insert(
                Bytes::from_static(b"state"),
                Frame::Bulk(Some(Bytes::from(format!("{:?}", peer.state)))),
            );
            Frame::Map(map)
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// MESH.PEER.REMOVE peer_id — remove a peer.
fn handle_peer_remove(args: &[String]) -> Frame {
    let peer_id = match args.first() {
        Some(id) => id,
        None => return err_frame("MESH.PEER.REMOVE requires a peer_id"),
    };
    match get_manager().remove_peer(peer_id) {
        Ok(()) => Frame::Simple(Bytes::from("OK")),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// MESH.PEER.BAN peer_id reason — ban a peer.
fn handle_peer_ban(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("MESH.PEER.BAN requires a peer_id");
    }
    let peer_id = &args[0];
    let reason = args.get(1).map(|s| s.as_str()).unwrap_or("no reason");
    match get_manager().ban_peer(peer_id, reason) {
        Ok(()) => Frame::Simple(Bytes::from("OK")),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// MESH.PEER.INFO peer_id — peer details.
fn handle_peer_info(args: &[String]) -> Frame {
    let peer_id = match args.first() {
        Some(id) => id,
        None => return err_frame("MESH.PEER.INFO requires a peer_id"),
    };
    match get_manager().peer_info(peer_id) {
        Some(p) => {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"id"),
                Frame::Bulk(Some(Bytes::from(p.id))),
            );
            map.insert(
                Bytes::from_static(b"addr"),
                Frame::Bulk(Some(Bytes::from(p.addr.to_string()))),
            );
            map.insert(
                Bytes::from_static(b"role"),
                Frame::Bulk(Some(Bytes::from(format!("{:?}", p.role)))),
            );
            map.insert(
                Bytes::from_static(b"state"),
                Frame::Bulk(Some(Bytes::from(format!("{:?}", p.state)))),
            );
            map.insert(
                Bytes::from_static(b"latency_ms"),
                Frame::Integer(p.latency_ms as i64),
            );
            map.insert(
                Bytes::from_static(b"bandwidth_kbps"),
                Frame::Integer(p.bandwidth_kbps as i64),
            );
            map.insert(
                Bytes::from_static(b"keys_shared"),
                Frame::Integer(p.keys_shared as i64),
            );
            map.insert(
                Bytes::from_static(b"sync_lag_ms"),
                Frame::Integer(p.sync_lag_ms as i64),
            );
            if let Some(ref region) = p.region {
                map.insert(
                    Bytes::from_static(b"region"),
                    Frame::Bulk(Some(Bytes::from(region.clone()))),
                );
            }
            Frame::Map(map)
        }
        None => err_frame(&format!("peer '{}' not found", peer_id)),
    }
}

/// MESH.TOPOLOGY — full mesh topology.
fn handle_topology() -> Frame {
    let topo = get_manager().topology();
    let mut map = HashMap::new();
    map.insert(
        Bytes::from_static(b"self_id"),
        Frame::Bulk(Some(Bytes::from(topo.self_id))),
    );
    map.insert(
        Bytes::from_static(b"peers"),
        Frame::Integer(topo.peers.len() as i64),
    );
    map.insert(
        Bytes::from_static(b"edges"),
        Frame::Integer(topo.edges.len() as i64),
    );
    map.insert(
        Bytes::from_static(b"clusters"),
        Frame::Integer(topo.clusters.len() as i64),
    );
    let cluster_frames: Vec<Frame> = topo
        .clusters
        .iter()
        .map(|c| {
            Frame::Bulk(Some(Bytes::from(format!(
                "{}: {} peers (region: {})",
                c.id,
                c.peers.len(),
                c.region.as_deref().unwrap_or("unknown")
            ))))
        })
        .collect();
    map.insert(
        Bytes::from_static(b"cluster_detail"),
        Frame::Array(Some(cluster_frames)),
    );
    Frame::Map(map)
}

/// MESH.SYNC [peer_id] — force sync.
fn handle_sync(args: &[String]) -> Frame {
    let peer_id = match args.first() {
        Some(id) => id.clone(),
        None => {
            // Sync with all active peers
            let peers = get_manager().peers();
            let mut total_sent = 0u64;
            let mut total_recv = 0u64;
            let mut syncs = 0usize;
            for p in &peers {
                if let Ok(r) = get_manager().sync_with(&p.id) {
                    total_sent += r.keys_sent;
                    total_recv += r.keys_received;
                    syncs += 1;
                }
            }
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"peers_synced"),
                Frame::Integer(syncs as i64),
            );
            map.insert(
                Bytes::from_static(b"keys_sent"),
                Frame::Integer(total_sent as i64),
            );
            map.insert(
                Bytes::from_static(b"keys_received"),
                Frame::Integer(total_recv as i64),
            );
            return Frame::Map(map);
        }
    };
    match get_manager().sync_with(&peer_id) {
        Ok(r) => {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"peer_id"),
                Frame::Bulk(Some(Bytes::from(r.peer_id))),
            );
            map.insert(
                Bytes::from_static(b"keys_sent"),
                Frame::Integer(r.keys_sent as i64),
            );
            map.insert(
                Bytes::from_static(b"keys_received"),
                Frame::Integer(r.keys_received as i64),
            );
            map.insert(
                Bytes::from_static(b"conflicts_resolved"),
                Frame::Integer(r.conflicts_resolved as i64),
            );
            map.insert(
                Bytes::from_static(b"duration_ms"),
                Frame::Integer(r.duration_ms as i64),
            );
            Frame::Map(map)
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// MESH.BROADCAST message — broadcast to all peers.
fn handle_broadcast(args: &[String]) -> Frame {
    let msg = match args.first() {
        Some(m) => m,
        None => return err_frame("MESH.BROADCAST requires a message"),
    };
    match get_manager().broadcast(msg.as_bytes()) {
        Ok(r) => {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"peers_reached"),
                Frame::Integer(r.peers_reached as i64),
            );
            map.insert(
                Bytes::from_static(b"peers_failed"),
                Frame::Integer(r.peers_failed as i64),
            );
            map.insert(
                Bytes::from_static(b"total_bytes"),
                Frame::Integer(r.total_bytes as i64),
            );
            Frame::Map(map)
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// MESH.ROUTE target_peer_id — optimal route.
fn handle_route(args: &[String]) -> Frame {
    let target = match args.first() {
        Some(t) => t,
        None => return err_frame("MESH.ROUTE requires a target peer_id"),
    };
    match get_manager().optimal_route(target) {
        Some(path) => {
            let frames: Vec<Frame> = path
                .into_iter()
                .map(|hop| Frame::Bulk(Some(Bytes::from(hop))))
                .collect();
            Frame::Array(Some(frames))
        }
        None => err_frame(&format!("no route to peer '{}'", target)),
    }
}

/// MESH.HEALTH — mesh health.
fn handle_health() -> Frame {
    let h = get_manager().health();
    let mut map = HashMap::new();
    map.insert(
        Bytes::from_static(b"total_peers"),
        Frame::Integer(h.total_peers as i64),
    );
    map.insert(
        Bytes::from_static(b"active_peers"),
        Frame::Integer(h.active_peers as i64),
    );
    map.insert(
        Bytes::from_static(b"unreachable_peers"),
        Frame::Integer(h.unreachable_peers as i64),
    );
    map.insert(
        Bytes::from_static(b"avg_latency_ms"),
        Frame::Double(h.avg_latency_ms),
    );
    map.insert(
        Bytes::from_static(b"sync_lag_ms"),
        Frame::Double(h.sync_lag_ms),
    );
    map.insert(
        Bytes::from_static(b"bandwidth_utilization_pct"),
        Frame::Double(h.bandwidth_utilization_pct),
    );
    Frame::Map(map)
}

/// MESH.STATS — operational statistics.
fn handle_stats() -> Frame {
    let s = get_manager().stats();
    let mut map = HashMap::new();
    map.insert(
        Bytes::from_static(b"total_peers_discovered"),
        Frame::Integer(s.total_peers_discovered as i64),
    );
    map.insert(
        Bytes::from_static(b"total_syncs"),
        Frame::Integer(s.total_syncs as i64),
    );
    map.insert(
        Bytes::from_static(b"total_bytes_transferred"),
        Frame::Integer(s.total_bytes_transferred as i64),
    );
    map.insert(
        Bytes::from_static(b"total_conflicts"),
        Frame::Integer(s.total_conflicts as i64),
    );
    map.insert(
        Bytes::from_static(b"broadcasts"),
        Frame::Integer(s.broadcasts as i64),
    );
    map.insert(
        Bytes::from_static(b"nat_traversals"),
        Frame::Integer(s.nat_traversals as i64),
    );
    map.insert(
        Bytes::from_static(b"uptime_secs"),
        Frame::Integer(s.uptime_secs as i64),
    );
    Frame::Map(map)
}

/// MESH.HELP — usage information.
fn handle_help() -> Frame {
    let lines = vec![
        "MESH.DISCOVER [MDNS|BOOTSTRAP addr1,addr2]   - Start peer discovery",
        "MESH.PEERS                                    - List all peers",
        "MESH.PEER.ADD addr [ROLE full|readonly|relay]  - Add a peer",
        "MESH.PEER.REMOVE peer_id                      - Remove a peer",
        "MESH.PEER.BAN peer_id reason                  - Ban a peer",
        "MESH.PEER.INFO peer_id                        - Peer details",
        "MESH.TOPOLOGY                                 - Full mesh topology",
        "MESH.SYNC [peer_id]                           - Force sync",
        "MESH.BROADCAST message                        - Broadcast to all peers",
        "MESH.ROUTE target_peer_id                     - Optimal route to peer",
        "MESH.HEALTH                                   - Mesh health summary",
        "MESH.STATS                                    - Operational statistics",
        "MESH.HELP                                     - Show this help",
    ];
    let frames: Vec<Frame> = lines
        .into_iter()
        .map(|l| Frame::Bulk(Some(Bytes::from(l))))
        .collect();
    Frame::Array(Some(frames))
}
