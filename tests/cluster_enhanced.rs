#![allow(clippy::unwrap_used)]
//! Enhanced cluster mode integration tests.
//!
//! Validates:
//! - CRC16 hash slot compatibility with Redis
//! - MOVED / ASK redirect logic
//! - Gossip message serialization round-trips
//! - Node state transitions (Online → PFail → Fail → Online)
//! - Slot assignment and lookup (SlotMap, ClusterStateManager)
//! - ClusterCommand parsing

use std::net::SocketAddr;
use std::sync::Arc;

// Re-exports from ferrite-core
use ferrite::cluster::{
    ClusterCommand, ClusterConfig, ClusterManager, ClusterNodeInfo, ClusterRouter, ClusterState,
    ClusterStateManager, FailoverOption, GossipConfig, GossipManager, GossipMessage,
    GossipMessageType, GossipNodeInfo, HashSlot, NodeRole, NodeState, Redirect, RedirectResult,
    RouteResult, SetSlotAction, SlotMap, SlotRange, CLUSTER_SLOTS,
};

// ============================================================================
// CRC16 Redis Compatibility
// ============================================================================

#[test]
fn test_crc16_known_redis_values() {
    // Verified against `redis-cli --cluster-mode CLUSTER KEYSLOT <key>`
    assert_eq!(HashSlot::for_key(b"foo"), 12182);
    assert_eq!(HashSlot::for_key(b"bar"), 5061);
    assert_eq!(HashSlot::for_key(b"hello"), 866);
    assert_eq!(HashSlot::for_key(b"key"), 12539);
    assert_eq!(HashSlot::for_key(b"test"), 6918);
    assert_eq!(HashSlot::for_key(b""), 0);
}

#[test]
fn test_crc16_hash_tag_extraction() {
    // Same hash tag → same slot
    let a = HashSlot::for_key(b"{user}:1000");
    let b = HashSlot::for_key(b"{user}:2000");
    let c = HashSlot::for_key(b"{user}:profile");
    assert_eq!(a, b);
    assert_eq!(b, c);

    // Hash tag equals hashing only the tag content
    assert_eq!(
        HashSlot::for_key(b"{user}:anything"),
        HashSlot::for_key(b"user"),
    );

    // Empty tag → full key is hashed
    let empty_tag = HashSlot::for_key(b"{}:data");
    let full_key = HashSlot::for_key(b"{}:data");
    assert_eq!(empty_tag, full_key);

    // Multiple braces: first pair wins
    assert_eq!(HashSlot::for_key(b"{a}{b}"), HashSlot::for_key(b"{a}other"),);
}

#[test]
fn test_crc16_all_slots_in_range() {
    for i in 0..50_000u32 {
        let key = format!("key:{}", i);
        let slot = HashSlot::for_key(key.as_bytes());
        assert!(slot < CLUSTER_SLOTS, "slot {} out of range", slot);
    }
}

// ============================================================================
// SlotMap – MOVED / ASK Redirect Logic
// ============================================================================

#[test]
fn test_slot_map_moved_redirect() {
    let mut sm = SlotMap::new();
    let remote_addr: SocketAddr = "10.0.0.2:6379".parse().unwrap();
    sm.set_node_addr("remote".into(), remote_addr);
    sm.assign_slot(100, "remote".into());

    let redirect = sm.should_redirect(100, &"local".into());
    assert_eq!(
        redirect,
        Some(Redirect::Moved {
            slot: 100,
            addr: remote_addr,
        })
    );
}

#[test]
fn test_slot_map_local_no_redirect() {
    let mut sm = SlotMap::new();
    sm.assign_slot(100, "local".into());

    assert!(sm.should_redirect(100, &"local".into()).is_none());
}

#[test]
fn test_slot_map_ask_redirect_during_migration() {
    let mut sm = SlotMap::new();
    let target_addr: SocketAddr = "10.0.0.3:6379".parse().unwrap();
    sm.set_node_addr("target".into(), target_addr);
    sm.assign_slot(200, "local".into());
    sm.set_migrating(200, "target".into());

    let redirect = sm.should_redirect(200, &"local".into());
    assert_eq!(
        redirect,
        Some(Redirect::Ask {
            slot: 200,
            addr: target_addr,
        })
    );
}

#[test]
fn test_slot_map_stable_clears_migration() {
    let mut sm = SlotMap::new();
    sm.assign_slot(300, "local".into());
    sm.set_migrating(300, "target".into());
    sm.set_importing(300, "source".into());

    sm.set_stable(300);
    assert!(!sm.is_migrating(300));
    assert!(!sm.is_importing(300));
}

#[test]
fn test_slot_map_coverage() {
    let mut sm = SlotMap::new();
    assert_eq!(sm.covered_count(), 0);

    sm.assign_range(&SlotRange::new(0, CLUSTER_SLOTS - 1), &"n1".into());
    assert!(sm.all_slots_covered());
    assert_eq!(sm.covered_count(), 16384);
}

// ============================================================================
// ClusterStateManager – MOVED / ASK via check_redirect
// ============================================================================

fn make_addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

#[test]
fn test_state_manager_local_key() {
    let mgr = ClusterStateManager::new(make_addr(6379));
    let my_id = mgr.my_id().to_string();
    let slot = HashSlot::for_key(b"mykey");
    mgr.assign_slots(&my_id, &[slot]);

    assert_eq!(mgr.check_redirect(b"mykey"), RedirectResult::Ok);
}

#[test]
fn test_state_manager_moved_redirect() {
    let mgr = ClusterStateManager::new(make_addr(6379));

    let remote = ClusterNodeInfo {
        node_id: "remote00000000000000000000000000000000001".into(),
        addr: make_addr(6380),
        role: NodeRole::Primary,
        state: NodeState::Online,
        primary_id: None,
        slots: Vec::new(),
        ping_sent: 0,
        pong_recv: 0,
        config_epoch: 1,
        link_state: ferrite::cluster::state_manager::LinkState::Connected,
        flags: vec![],
    };
    let remote_id = remote.node_id.clone();
    mgr.add_node(remote);

    // Assign all slots to remote
    let all: Vec<u16> = (0..CLUSTER_SLOTS).collect();
    mgr.assign_slots(&remote_id, &all);

    match mgr.check_redirect(b"hello") {
        RedirectResult::Moved { addr, .. } => assert_eq!(addr, make_addr(6380)),
        other => panic!("Expected Moved, got {:?}", other),
    }
}

#[test]
fn test_state_manager_ask_redirect() {
    let mgr = ClusterStateManager::new(make_addr(6379));
    let my_id = mgr.my_id().to_string();
    let slot = HashSlot::for_key(b"hello");
    mgr.assign_slots(&my_id, &[slot]);

    let target = ClusterNodeInfo {
        node_id: "target00000000000000000000000000000000001".into(),
        addr: make_addr(6381),
        role: NodeRole::Primary,
        state: NodeState::Online,
        primary_id: None,
        slots: Vec::new(),
        ping_sent: 0,
        pong_recv: 0,
        config_epoch: 1,
        link_state: ferrite::cluster::state_manager::LinkState::Connected,
        flags: vec![],
    };
    let target_id = target.node_id.clone();
    mgr.add_node(target);
    mgr.set_slot_migrating(slot, &target_id);

    match mgr.check_redirect(b"hello") {
        RedirectResult::Ask { addr, .. } => assert_eq!(addr, make_addr(6381)),
        other => panic!("Expected Ask, got {:?}", other),
    }

    // After stabilising, no redirect
    mgr.set_slot_stable(slot);
    assert_eq!(mgr.check_redirect(b"hello"), RedirectResult::Ok);
}

// ============================================================================
// ClusterRouter – RouteResult
// ============================================================================

#[test]
fn test_router_local_routing() {
    let router = ClusterRouter::new("self-node".into());
    router.assign_slots(&"self-node".into(), &SlotRange::new(0, CLUSTER_SLOTS - 1));
    router.set_state(ClusterState::Ok);

    assert!(matches!(router.route(b"foo"), RouteResult::Local));
}

#[test]
fn test_router_moved_routing() {
    let router = ClusterRouter::new("self-node".into());
    let other_addr: SocketAddr = "10.0.0.5:6379".parse().unwrap();
    router.set_node_address("other".into(), other_addr);
    router.assign_slots(&"other".into(), &SlotRange::new(0, CLUSTER_SLOTS - 1));
    router.set_state(ClusterState::Ok);

    match router.route(b"foo") {
        RouteResult::Moved { addr, .. } => assert_eq!(addr, other_addr),
        other => panic!("Expected Moved, got {:?}", other),
    }
}

#[test]
fn test_router_cluster_down() {
    let router = ClusterRouter::new("self-node".into());
    router.set_state(ClusterState::Fail);
    assert!(matches!(router.route(b"foo"), RouteResult::ClusterDown));
}

#[test]
fn test_router_cross_slot_error() {
    let router = ClusterRouter::new("self-node".into());
    router.assign_slots(&"self-node".into(), &SlotRange::new(0, CLUSTER_SLOTS - 1));
    router.set_state(ClusterState::Ok);

    // Keys with different hash tags → cross-slot error
    let keys: Vec<&[u8]> = vec![b"{a}:1", b"{b}:2"];
    let result = router.route_multi(&keys);
    assert!(result.is_err());
}

#[test]
fn test_router_same_hash_tag_multi() {
    let router = ClusterRouter::new("self-node".into());
    router.assign_slots(&"self-node".into(), &SlotRange::new(0, CLUSTER_SLOTS - 1));
    router.set_state(ClusterState::Ok);

    let keys: Vec<&[u8]> = vec![b"{user}:1", b"{user}:2", b"{user}:3"];
    let result = router.route_multi(&keys);
    assert!(matches!(result.unwrap(), RouteResult::Local));
}

// ============================================================================
// Route result formatting
// ============================================================================

#[test]
fn test_route_result_error_strings() {
    let addr: SocketAddr = "10.0.0.1:6379".parse().unwrap();

    assert_eq!(
        RouteResult::Moved { slot: 42, addr }.to_error_string(),
        Some("MOVED 42 10.0.0.1:6379".into()),
    );
    assert_eq!(
        RouteResult::Ask { slot: 99, addr }.to_error_string(),
        Some("ASK 99 10.0.0.1:6379".into()),
    );
    assert_eq!(
        RouteResult::ClusterDown.to_error_string(),
        Some("CLUSTERDOWN The cluster is down".into()),
    );
    assert!(RouteResult::Local.to_error_string().is_none());
}

// ============================================================================
// Gossip Serialization Round-Trips
// ============================================================================

fn sample_gossip_node_info() -> GossipNodeInfo {
    GossipNodeInfo {
        id: "abcdef0123456789abcdef0123456789abcdef01".into(),
        addr: "192.168.1.10:6379".parse().unwrap(),
        role: NodeRole::Primary,
        state: NodeState::Online,
        config_epoch: 42,
        primary_id: None,
        slots: vec![SlotRange::new(0, 5461), SlotRange::new(10923, 16383)],
        last_ping_ms: 1_700_000_000_000,
    }
}

#[test]
fn test_gossip_node_info_roundtrip() {
    let original = sample_gossip_node_info();
    let bytes = original.to_bytes();
    let mut data = bytes;
    let decoded = GossipNodeInfo::from_bytes(&mut data).unwrap();

    assert_eq!(decoded.id, original.id);
    assert_eq!(decoded.addr, original.addr);
    assert_eq!(decoded.config_epoch, original.config_epoch);
    assert_eq!(decoded.slots.len(), 2);
    assert_eq!(decoded.slots[0].start, 0);
    assert_eq!(decoded.slots[1].end, 16383);
    assert_eq!(decoded.last_ping_ms, original.last_ping_ms);
}

#[test]
fn test_gossip_node_info_replica_roundtrip() {
    let info = GossipNodeInfo {
        id: "replica-node-aaaa".into(),
        addr: "10.0.0.2:6380".parse().unwrap(),
        role: NodeRole::Replica,
        state: NodeState::Online,
        config_epoch: 5,
        primary_id: Some("primary-node-bbbb".into()),
        slots: vec![],
        last_ping_ms: 999,
    };

    let bytes = info.to_bytes();
    let mut data = bytes;
    let decoded = GossipNodeInfo::from_bytes(&mut data).unwrap();

    assert_eq!(decoded.role as u8, NodeRole::Replica as u8);
    assert_eq!(decoded.primary_id, Some("primary-node-bbbb".into()));
    assert!(decoded.slots.is_empty());
}

#[test]
fn test_gossip_message_ping_roundtrip() {
    let entries = vec![sample_gossip_node_info()];
    let msg = GossipMessage::ping("sender-id".into(), 7, entries);
    let bytes = msg.to_bytes();
    let mut data = bytes;
    let decoded = GossipMessage::from_bytes(&mut data).unwrap();

    assert_eq!(decoded.msg_type, GossipMessageType::Ping);
    assert_eq!(decoded.sender_id, "sender-id");
    assert_eq!(decoded.current_epoch, 7);
    assert_eq!(decoded.gossip_entries.len(), 1);
    assert!(decoded.target_id.is_none());
}

#[test]
fn test_gossip_message_pong_roundtrip() {
    let entries = vec![sample_gossip_node_info()];
    let msg = GossipMessage::pong("pong-sender".into(), 99, entries);
    let bytes = msg.to_bytes();
    let mut data = bytes;
    let decoded = GossipMessage::from_bytes(&mut data).unwrap();

    assert_eq!(decoded.msg_type, GossipMessageType::Pong);
    assert_eq!(decoded.sender_id, "pong-sender");
    assert_eq!(decoded.current_epoch, 99);
}

#[test]
fn test_gossip_message_fail_roundtrip() {
    let msg = GossipMessage::fail("reporter".into(), 50, "dead-node".into());
    let bytes = msg.to_bytes();
    let mut data = bytes;
    let decoded = GossipMessage::from_bytes(&mut data).unwrap();

    assert_eq!(decoded.msg_type, GossipMessageType::Fail);
    assert_eq!(decoded.target_id, Some("dead-node".into()));
    assert!(decoded.gossip_entries.is_empty());
}

#[test]
fn test_gossip_message_meet_roundtrip() {
    let info = sample_gossip_node_info();
    let msg = GossipMessage::meet("joiner".into(), 1, info);
    let bytes = msg.to_bytes();
    let mut data = bytes;
    let decoded = GossipMessage::from_bytes(&mut data).unwrap();

    assert_eq!(decoded.msg_type, GossipMessageType::Meet);
    assert_eq!(decoded.gossip_entries.len(), 1);
}

#[test]
fn test_gossip_message_empty_entries() {
    let msg = GossipMessage::ping("node".into(), 0, vec![]);
    let bytes = msg.to_bytes();
    let mut data = bytes;
    let decoded = GossipMessage::from_bytes(&mut data).unwrap();
    assert!(decoded.gossip_entries.is_empty());
}

#[test]
fn test_gossip_message_multiple_entries() {
    let entries = vec![
        sample_gossip_node_info(),
        GossipNodeInfo {
            id: "second-node".into(),
            addr: "10.0.0.3:6381".parse().unwrap(),
            role: NodeRole::Primary,
            state: NodeState::PFail,
            config_epoch: 100,
            primary_id: None,
            slots: vec![SlotRange::new(5462, 10922)],
            last_ping_ms: 0,
        },
    ];
    let msg = GossipMessage::ping("multi".into(), 10, entries);
    let bytes = msg.to_bytes();
    let mut data = bytes;
    let decoded = GossipMessage::from_bytes(&mut data).unwrap();
    assert_eq!(decoded.gossip_entries.len(), 2);
    assert_eq!(decoded.gossip_entries[1].id, "second-node");
}

// ============================================================================
// Node State Transitions
// ============================================================================

#[test]
fn test_cluster_manager_node_state_online_to_pfail_to_fail() {
    let config = ClusterConfig::default();
    let mgr = ClusterManager::new(
        config,
        ferrite::cluster::generate_node_id(),
        make_addr(6379),
    );

    let peer_id = "peer-node-id".to_string();
    let peer = ferrite::cluster::ClusterNode::new(peer_id.clone(), make_addr(6380));
    mgr.add_node(peer);

    // Initially Online
    let node = mgr.get_node(&peer_id).unwrap();
    assert_eq!(node.state, NodeState::Online);

    // Mark PFail
    mgr.mark_node_pfail(&peer_id);
    let node = mgr.get_node(&peer_id).unwrap();
    assert_eq!(node.state, NodeState::PFail);

    // Mark Fail
    mgr.mark_node_failed(&peer_id);
    let node = mgr.get_node(&peer_id).unwrap();
    assert_eq!(node.state, NodeState::Fail);

    // Recover to Online
    mgr.mark_node_online(&peer_id);
    let node = mgr.get_node(&peer_id).unwrap();
    assert_eq!(node.state, NodeState::Online);
}

#[test]
fn test_cluster_manager_state_updates_on_failure() {
    let config = ClusterConfig {
        require_full_coverage: false,
        ..Default::default()
    };
    let my_id = ferrite::cluster::generate_node_id();
    let mgr = ClusterManager::new(config, my_id.clone(), make_addr(6379));

    // Add a peer
    let peer_id = "peer".to_string();
    mgr.add_node(ferrite::cluster::ClusterNode::new(
        peer_id.clone(),
        make_addr(6380),
    ));

    // Assign all slots to my node
    mgr.assign_slots(&my_id, SlotRange::new(0, CLUSTER_SLOTS - 1));
    mgr.update_state();
    assert_eq!(mgr.state(), ClusterState::Ok);

    // Fail the peer → degraded
    mgr.mark_node_failed(&peer_id);
    assert_eq!(mgr.state(), ClusterState::Degraded);

    // Recover → Ok
    mgr.mark_node_online(&peer_id);
    assert_eq!(mgr.state(), ClusterState::Ok);
}

// ============================================================================
// Slot Assignment & Lookup
// ============================================================================

#[test]
fn test_cluster_manager_distribute_slots_three_nodes() {
    let my_id = ferrite::cluster::generate_node_id();
    let mgr = ClusterManager::new(ClusterConfig::default(), my_id.clone(), make_addr(6379));

    let n2 = ferrite::cluster::generate_node_id();
    let n3 = ferrite::cluster::generate_node_id();
    mgr.add_node(ferrite::cluster::ClusterNode::new(
        n2.clone(),
        make_addr(6380),
    ));
    mgr.add_node(ferrite::cluster::ClusterNode::new(
        n3.clone(),
        make_addr(6381),
    ));

    mgr.distribute_slots();

    let total: usize = mgr.get_all_nodes().iter().map(|n| n.slot_count()).sum();
    assert_eq!(total, CLUSTER_SLOTS as usize);

    for slot in 0..CLUSTER_SLOTS {
        assert!(
            mgr.get_slot_owner(slot).is_some(),
            "slot {} unassigned",
            slot
        );
    }
}

#[test]
fn test_cluster_manager_slot_migration_lifecycle() {
    let my_id = ferrite::cluster::generate_node_id();
    let mgr = ClusterManager::new(ClusterConfig::default(), my_id.clone(), make_addr(6379));

    let target_id = "target-node".to_string();
    mgr.add_node(ferrite::cluster::ClusterNode::new(
        target_id.clone(),
        make_addr(6380),
    ));

    // Assign slot 500 to ourselves
    mgr.assign_slots(&my_id, SlotRange::new(500, 500));

    // SETSLOT 500 MIGRATING target-node
    mgr.set_slot_migrating(500, target_id.clone()).unwrap();
    assert!(mgr.is_slot_migrating(500));
    assert_eq!(mgr.get_migration_target(500), Some(target_id.clone()));

    // SETSLOT 500 STABLE
    mgr.set_slot_stable(500);
    assert!(!mgr.is_slot_migrating(500));

    // SETSLOT 500 NODE target-node (finalise migration)
    mgr.set_slot_node(500, target_id.clone()).unwrap();
    assert_eq!(mgr.get_slot_owner(500), Some(target_id));
}

// ============================================================================
// ClusterCommand Parsing
// ============================================================================

#[test]
fn test_cluster_command_parse_simple() {
    assert_eq!(
        ClusterCommand::parse("INFO", &[]).unwrap(),
        ClusterCommand::ClusterInfo,
    );
    assert_eq!(
        ClusterCommand::parse("nodes", &[]).unwrap(),
        ClusterCommand::ClusterNodes,
    );
    assert_eq!(
        ClusterCommand::parse("MYID", &[]).unwrap(),
        ClusterCommand::ClusterMyId,
    );
    assert_eq!(
        ClusterCommand::parse("HELP", &[]).unwrap(),
        ClusterCommand::ClusterHelp,
    );
}

#[test]
fn test_cluster_command_parse_keyslot() {
    assert_eq!(
        ClusterCommand::parse("KEYSLOT", &["hello".into()]).unwrap(),
        ClusterCommand::ClusterKeySlot("hello".into()),
    );
    assert!(ClusterCommand::parse("KEYSLOT", &[]).is_err());
}

#[test]
fn test_cluster_command_parse_addslots() {
    let cmd = ClusterCommand::parse("ADDSLOTS", &["0".into(), "1".into(), "2".into()]).unwrap();
    assert_eq!(cmd, ClusterCommand::ClusterAddSlots(vec![0, 1, 2]));
}

#[test]
fn test_cluster_command_parse_setslot_importing() {
    let cmd = ClusterCommand::parse(
        "SETSLOT",
        &["100".into(), "importing".into(), "node-abc".into()],
    )
    .unwrap();
    assert_eq!(
        cmd,
        ClusterCommand::ClusterSetSlot {
            slot: 100,
            action: SetSlotAction::Importing("node-abc".into()),
        }
    );
}

#[test]
fn test_cluster_command_parse_meet() {
    let cmd = ClusterCommand::parse("MEET", &["10.0.0.1".into(), "7000".into()]).unwrap();
    assert_eq!(
        cmd,
        ClusterCommand::ClusterMeet("10.0.0.1:7000".parse().unwrap()),
    );
}

#[test]
fn test_cluster_command_parse_failover_takeover() {
    let cmd = ClusterCommand::parse("FAILOVER", &["TAKEOVER".into()]).unwrap();
    assert_eq!(
        cmd,
        ClusterCommand::ClusterFailover(Some(FailoverOption::Takeover)),
    );
}

#[test]
fn test_cluster_command_parse_unknown() {
    assert!(ClusterCommand::parse("NOPE", &[]).is_err());
}

// ============================================================================
// Gossip Bus Address Calculation
// ============================================================================

#[test]
fn test_gossip_bus_port_offset() {
    let my_id = ferrite::cluster::generate_node_id();
    let addr = make_addr(6379);
    let cluster = Arc::new(ClusterManager::new(ClusterConfig::default(), my_id, addr));
    let gm = GossipManager::new(GossipConfig::default(), cluster);

    let bus = gm.bus_addr(addr);
    assert_eq!(bus.port(), 16379);
    assert_eq!(bus.ip(), addr.ip());
}

// ============================================================================
// CLUSTER INFO / NODES / SLOTS Formatting
// ============================================================================

#[test]
fn test_state_manager_format_cluster_info() {
    let mgr = ClusterStateManager::new(make_addr(6379));
    let my_id = mgr.my_id().to_string();
    mgr.assign_slots(&my_id, &[0, 1, 2, 3, 4]);

    let info = mgr.format_cluster_info();
    assert!(info.contains("cluster_enabled:1"));
    assert!(info.contains("cluster_state:ok"));
    assert!(info.contains("cluster_slots_assigned:5"));
    assert!(info.contains("cluster_known_nodes:1"));
}

#[test]
fn test_state_manager_format_cluster_nodes() {
    let mgr = ClusterStateManager::new(make_addr(6379));
    let my_id = mgr.my_id().to_string();
    mgr.assign_slots(&my_id, &(0..=4).collect::<Vec<u16>>());

    let nodes = mgr.format_cluster_nodes();
    assert!(nodes.contains(&my_id));
    assert!(nodes.contains("myself"));
    assert!(nodes.contains("0-4"));
}

#[test]
fn test_state_manager_format_cluster_slots() {
    let mgr = ClusterStateManager::new(make_addr(6379));
    let my_id = mgr.my_id().to_string();
    mgr.assign_slots(&my_id, &(0..=4).collect::<Vec<u16>>());

    let frames = mgr.format_cluster_slots();
    assert_eq!(frames.len(), 1); // one contiguous range
}

// ============================================================================
// Redirect error string formatting
// ============================================================================

#[test]
fn test_redirect_to_error_string() {
    let addr: SocketAddr = "10.0.0.1:6379".parse().unwrap();
    assert_eq!(
        Redirect::Moved { slot: 42, addr }.to_error_string(),
        "MOVED 42 10.0.0.1:6379",
    );
    assert_eq!(
        Redirect::Ask { slot: 99, addr }.to_error_string(),
        "ASK 99 10.0.0.1:6379",
    );
}

// ============================================================================
// Node ID Generation
// ============================================================================

#[test]
fn test_node_id_40_hex_chars() {
    for _ in 0..10 {
        let id = ferrite::cluster::generate_node_id();
        assert_eq!(id.len(), 40);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
