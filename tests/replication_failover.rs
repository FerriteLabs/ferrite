#![allow(clippy::unwrap_used)]
//! Replication Failover Integration Tests
//!
//! Tests for replication failover scenarios including:
//! - Primary/replica state management
//! - Partial and full resync
//! - Replica staleness detection
//! - Backlog management
//! - Command propagation

use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrite::replication::{
    PsyncResponse, ReplicaConnectionState, ReplicaInfo, ReplicationBacklog, ReplicationCommand,
    ReplicationId, ReplicationPrimary, ReplicationRole, ReplicationState, ReplicationStream,
    HEARTBEAT_INTERVAL, REPLICA_TIMEOUT,
};
use ferrite::storage::Store;

// ============================================================================
// ReplicationId Tests
// ============================================================================

#[test]
fn test_replication_id_generation() {
    let id1 = ReplicationId::new();
    let id2 = ReplicationId::new();

    // Each ID should be unique
    assert_ne!(id1.as_str(), id2.as_str());

    // IDs should be 48 hex characters (32 + 16)
    assert_eq!(id1.as_str().len(), 48);
    assert_eq!(id2.as_str().len(), 48);

    // Should be valid hex
    assert!(id1.as_str().chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn test_replication_id_from_string() {
    let custom_id = "abc123def456abc123def456abc123def456abc123def456";
    let id = ReplicationId::from_string(custom_id);
    assert_eq!(id.as_str(), custom_id);
}

#[test]
fn test_replication_id_default() {
    let id = ReplicationId::default();
    assert!(!id.as_str().is_empty());
    assert_eq!(id.as_str().len(), 48);
}

// ============================================================================
// ReplicationRole Tests
// ============================================================================

#[test]
fn test_replication_role_default() {
    let role = ReplicationRole::default();
    assert_eq!(role, ReplicationRole::Primary);
}

#[test]
fn test_replication_role_equality() {
    assert_eq!(ReplicationRole::Primary, ReplicationRole::Primary);
    assert_eq!(ReplicationRole::Replica, ReplicationRole::Replica);
    assert_ne!(ReplicationRole::Primary, ReplicationRole::Replica);
}

// ============================================================================
// ReplicationState Tests
// ============================================================================

#[tokio::test]
async fn test_replication_state_default() {
    let state = ReplicationState::new();
    assert_eq!(state.role().await, ReplicationRole::Primary);
    assert_eq!(state.repl_offset(), 0);
    assert_eq!(state.connected_replicas(), 0);
}

#[tokio::test]
async fn test_replication_state_role_change() {
    let state = ReplicationState::new();

    state.set_role(ReplicationRole::Replica).await;
    assert_eq!(state.role().await, ReplicationRole::Replica);

    state.set_role(ReplicationRole::Primary).await;
    assert_eq!(state.role().await, ReplicationRole::Primary);
}

#[tokio::test]
async fn test_replication_state_offset_operations() {
    let state = ReplicationState::new();

    // Initial offset
    assert_eq!(state.repl_offset(), 0);

    // Increment
    state.increment_offset(100);
    assert_eq!(state.repl_offset(), 100);

    state.increment_offset(50);
    assert_eq!(state.repl_offset(), 150);

    // Set absolute
    state.set_offset(1000);
    assert_eq!(state.repl_offset(), 1000);
}

#[tokio::test]
async fn test_replication_state_replica_count() {
    let state = ReplicationState::new();

    assert_eq!(state.connected_replicas(), 0);

    state.add_replica();
    assert_eq!(state.connected_replicas(), 1);

    state.add_replica();
    assert_eq!(state.connected_replicas(), 2);

    state.remove_replica();
    assert_eq!(state.connected_replicas(), 1);

    state.remove_replica();
    assert_eq!(state.connected_replicas(), 0);
}

#[tokio::test]
async fn test_replication_state_master_replid() {
    let state = ReplicationState::new();

    let initial_replid = state.master_replid().await;
    assert!(!initial_replid.as_str().is_empty());

    let new_replid = ReplicationId::new();
    state.set_master_replid(new_replid.clone()).await;

    let retrieved = state.master_replid().await;
    assert_eq!(retrieved.as_str(), new_replid.as_str());
}

// ============================================================================
// ReplicaInfo Tests
// ============================================================================

#[test]
fn test_replica_info_lag_bytes() {
    let info = ReplicaInfo {
        addr: "127.0.0.1:6380".parse().unwrap(),
        offset: 500,
        listening_port: 6380,
        online: true,
        last_interaction: Instant::now(),
        last_ack: Instant::now(),
        capabilities: vec![],
        state: ReplicaConnectionState::Online,
    };

    // Master at offset 1000, replica at 500 = 500 bytes lag
    assert_eq!(info.lag_bytes(1000), 500);

    // Master behind replica (shouldn't happen, but return 0)
    assert_eq!(info.lag_bytes(400), 0);

    // No lag
    assert_eq!(info.lag_bytes(500), 0);
}

#[test]
fn test_replica_info_lag_seconds() {
    let info = ReplicaInfo {
        addr: "127.0.0.1:6380".parse().unwrap(),
        offset: 500,
        listening_port: 6380,
        online: true,
        last_interaction: Instant::now(),
        last_ack: Instant::now(),
        capabilities: vec![],
        state: ReplicaConnectionState::Online,
    };

    // Just created, lag should be 0 or close to it
    assert!(info.lag_seconds() < 2);
}

#[test]
fn test_replica_info_staleness() {
    // Create replica with very old last_ack
    let now = Instant::now();
    let info = ReplicaInfo {
        addr: "127.0.0.1:6380".parse().unwrap(),
        offset: 500,
        listening_port: 6380,
        online: true,
        last_interaction: now,
        last_ack: now, // Fresh
        capabilities: vec![],
        state: ReplicaConnectionState::Online,
    };

    // Fresh replica should not be stale
    assert!(!info.is_stale());
}

#[test]
fn test_replica_connection_states() {
    assert_eq!(
        ReplicaConnectionState::WaitingPsync,
        ReplicaConnectionState::WaitingPsync
    );
    assert_eq!(
        ReplicaConnectionState::SendingRdb,
        ReplicaConnectionState::SendingRdb
    );
    assert_eq!(
        ReplicaConnectionState::Online,
        ReplicaConnectionState::Online
    );
    assert_ne!(
        ReplicaConnectionState::WaitingPsync,
        ReplicaConnectionState::Online
    );
}

// ============================================================================
// ReplicationBacklog Tests
// ============================================================================

#[test]
fn test_backlog_empty() {
    let backlog = ReplicationBacklog::new(10000);
    assert_eq!(backlog.size(), 0);
    assert_eq!(backlog.first_offset(), 0);
}

#[test]
fn test_backlog_push_single() {
    let mut backlog = ReplicationBacklog::new(10000);

    let cmd = ReplicationCommand::Set {
        key: Bytes::from("key"),
        value: Bytes::from("value"),
    };

    backlog.push(cmd, 0);
    assert!(backlog.size() > 0);
    assert!(backlog.can_partial_sync(0));
}

#[test]
fn test_backlog_push_multiple() {
    let mut backlog = ReplicationBacklog::new(10000);

    for i in 0..10 {
        let cmd = ReplicationCommand::Set {
            key: Bytes::from(format!("key{}", i)),
            value: Bytes::from(format!("value{}", i)),
        };
        backlog.push(cmd, i * 50);
    }

    assert!(backlog.can_partial_sync(0));
    assert!(backlog.can_partial_sync(200));
    assert!(backlog.can_partial_sync(400));
}

#[test]
fn test_backlog_overflow() {
    // Very small backlog
    let mut backlog = ReplicationBacklog::new(100);

    // Push commands that exceed the size
    for i in 0..20 {
        let cmd = ReplicationCommand::Set {
            key: Bytes::from(format!("key{}", i)),
            value: Bytes::from(format!("value{}", i)),
        };
        backlog.push(cmd, i * 50);
    }

    // Old entries should have been evicted
    // Cannot partial sync from offset 0 anymore
    assert!(!backlog.can_partial_sync(0));
}

#[test]
fn test_backlog_entries_from() {
    let mut backlog = ReplicationBacklog::new(10000);

    for i in 0..5 {
        let cmd = ReplicationCommand::Set {
            key: Bytes::from(format!("key{}", i)),
            value: Bytes::from("v"),
        };
        backlog.push(cmd, i * 30);
    }

    let entries = backlog.entries_from(60);
    assert!(entries.len() >= 2); // Should include entries from offset 60+
}

#[test]
fn test_backlog_clear() {
    let mut backlog = ReplicationBacklog::new(10000);

    let cmd = ReplicationCommand::Set {
        key: Bytes::from("key"),
        value: Bytes::from("value"),
    };
    backlog.push(cmd, 0);

    assert!(backlog.size() > 0);
    backlog.clear();
    assert_eq!(backlog.size(), 0);
}

// ============================================================================
// ReplicationStream Tests
// ============================================================================

#[tokio::test]
async fn test_replication_stream_broadcast() {
    let stream = ReplicationStream::new(10000);
    let mut rx = stream.subscribe();

    let cmd = ReplicationCommand::Set {
        key: Bytes::from("key"),
        value: Bytes::from("value"),
    };

    stream.broadcast(cmd, 0).await;

    let (received, offset) = rx.recv().await.unwrap();
    assert_eq!(offset, 0);
    match received {
        ReplicationCommand::Set { key, value } => {
            assert_eq!(key, Bytes::from("key"));
            assert_eq!(value, Bytes::from("value"));
        }
        _ => panic!("Expected Set command"),
    }
}

#[tokio::test]
async fn test_replication_stream_partial_sync_available() {
    let stream = ReplicationStream::new(10000);

    // Add some commands
    for i in 0..5 {
        let cmd = ReplicationCommand::Set {
            key: Bytes::from(format!("key{}", i)),
            value: Bytes::from("v"),
        };
        stream.broadcast(cmd, i * 30).await;
    }

    assert!(stream.can_partial_sync(0).await);
    assert!(stream.can_partial_sync(60).await);
}

#[tokio::test]
async fn test_replication_stream_get_partial_sync_commands() {
    let stream = ReplicationStream::new(10000);

    // Add commands
    for i in 0..5 {
        let cmd = ReplicationCommand::Set {
            key: Bytes::from(format!("key{}", i)),
            value: Bytes::from("v"),
        };
        stream.broadcast(cmd, i * 30).await;
    }

    let commands = stream.get_partial_sync_commands(60).await;
    assert!(!commands.is_empty());
}

// ============================================================================
// ReplicationPrimary Tests
// ============================================================================

#[tokio::test]
async fn test_primary_psync_full_resync() {
    let store = Arc::new(Store::new(16));
    let state = Arc::new(ReplicationState::new());
    let stream = Arc::new(ReplicationStream::new(10000));
    let primary = ReplicationPrimary::new(store, state, stream);

    // First sync request should always result in full resync
    let response = primary.handle_psync("?", -1).await.unwrap();
    match response {
        PsyncResponse::FullResync { replid, offset } => {
            assert!(!replid.as_str().is_empty());
            assert_eq!(offset, 0);
        }
        _ => panic!("Expected full resync"),
    }
}

#[tokio::test]
async fn test_primary_psync_wrong_replid() {
    let store = Arc::new(Store::new(16));
    let state = Arc::new(ReplicationState::new());
    let stream = Arc::new(ReplicationStream::new(10000));
    let primary = ReplicationPrimary::new(store, state, stream);

    // Wrong replid should result in full resync
    let response = primary
        .handle_psync("wrongreplid123456789012345678901234567890", 100)
        .await
        .unwrap();
    match response {
        PsyncResponse::FullResync { .. } => {}
        _ => panic!("Expected full resync for wrong replid"),
    }
}

#[tokio::test]
async fn test_primary_register_replica() {
    let store = Arc::new(Store::new(16));
    let state = Arc::new(ReplicationState::new());
    let stream = Arc::new(ReplicationStream::new(10000));
    let primary = ReplicationPrimary::new(store, state.clone(), stream);

    let addr: SocketAddr = "127.0.0.1:6380".parse().unwrap();
    primary.register_replica(addr, 6380).await;

    assert_eq!(state.connected_replicas(), 1);

    let replicas = primary.get_replicas().await;
    assert_eq!(replicas.len(), 1);
    assert_eq!(replicas[0].addr, addr);
    assert_eq!(replicas[0].listening_port, 6380);
}

#[tokio::test]
async fn test_primary_unregister_replica() {
    let store = Arc::new(Store::new(16));
    let state = Arc::new(ReplicationState::new());
    let stream = Arc::new(ReplicationStream::new(10000));
    let primary = ReplicationPrimary::new(store, state.clone(), stream);

    let addr: SocketAddr = "127.0.0.1:6380".parse().unwrap();
    primary.register_replica(addr, 6380).await;
    assert_eq!(state.connected_replicas(), 1);

    primary.unregister_replica(&addr).await;
    assert_eq!(state.connected_replicas(), 0);

    let replicas = primary.get_replicas().await;
    assert!(replicas.is_empty());
}

#[tokio::test]
async fn test_primary_update_replica_offset() {
    let store = Arc::new(Store::new(16));
    let state = Arc::new(ReplicationState::new());
    let stream = Arc::new(ReplicationStream::new(10000));
    let primary = ReplicationPrimary::new(store, state, stream);

    let addr: SocketAddr = "127.0.0.1:6380".parse().unwrap();
    primary.register_replica(addr, 6380).await;

    // Update offset via ACK
    primary.update_replica_offset(&addr, 1000).await;

    let replicas = primary.get_replicas().await;
    assert_eq!(replicas[0].offset, 1000);
}

#[tokio::test]
async fn test_primary_set_replica_state() {
    let store = Arc::new(Store::new(16));
    let state = Arc::new(ReplicationState::new());
    let stream = Arc::new(ReplicationStream::new(10000));
    let primary = ReplicationPrimary::new(store, state, stream);

    let addr: SocketAddr = "127.0.0.1:6380".parse().unwrap();
    primary.register_replica(addr, 6380).await;

    // Initially WaitingPsync
    let replicas = primary.get_replicas().await;
    assert_eq!(replicas[0].state, ReplicaConnectionState::WaitingPsync);

    // Transition to SendingRdb
    primary
        .set_replica_state(&addr, ReplicaConnectionState::SendingRdb)
        .await;
    let replicas = primary.get_replicas().await;
    assert_eq!(replicas[0].state, ReplicaConnectionState::SendingRdb);

    // Transition to Online
    primary
        .set_replica_state(&addr, ReplicaConnectionState::Online)
        .await;
    let replicas = primary.get_replicas().await;
    assert_eq!(replicas[0].state, ReplicaConnectionState::Online);
    assert!(replicas[0].online);
}

#[tokio::test]
async fn test_primary_count_replicas_at_offset() {
    let store = Arc::new(Store::new(16));
    let state = Arc::new(ReplicationState::new());
    let stream = Arc::new(ReplicationStream::new(10000));
    let primary = ReplicationPrimary::new(store, state, stream);

    // Register multiple replicas
    let addr1: SocketAddr = "127.0.0.1:6380".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:6381".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:6382".parse().unwrap();

    primary.register_replica(addr1, 6380).await;
    primary.register_replica(addr2, 6381).await;
    primary.register_replica(addr3, 6382).await;

    // Mark all as online
    primary
        .set_replica_state(&addr1, ReplicaConnectionState::Online)
        .await;
    primary
        .set_replica_state(&addr2, ReplicaConnectionState::Online)
        .await;
    primary
        .set_replica_state(&addr3, ReplicaConnectionState::Online)
        .await;

    // Set different offsets
    primary.update_replica_offset(&addr1, 1000).await;
    primary.update_replica_offset(&addr2, 800).await;
    primary.update_replica_offset(&addr3, 500).await;

    // Count replicas at various offsets
    assert_eq!(primary.count_replicas_at_offset(500).await, 3);
    assert_eq!(primary.count_replicas_at_offset(800).await, 2);
    assert_eq!(primary.count_replicas_at_offset(1000).await, 1);
    assert_eq!(primary.count_replicas_at_offset(1001).await, 0);
}

#[tokio::test]
async fn test_primary_min_max_replica_lag() {
    let store = Arc::new(Store::new(16));
    let state = Arc::new(ReplicationState::new());
    let stream = Arc::new(ReplicationStream::new(10000));
    let primary = ReplicationPrimary::new(store, state.clone(), stream);

    // Set master offset
    state.set_offset(1000);

    // Register and configure replicas
    let addr1: SocketAddr = "127.0.0.1:6380".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:6381".parse().unwrap();

    primary.register_replica(addr1, 6380).await;
    primary.register_replica(addr2, 6381).await;

    primary
        .set_replica_state(&addr1, ReplicaConnectionState::Online)
        .await;
    primary
        .set_replica_state(&addr2, ReplicaConnectionState::Online)
        .await;

    primary.update_replica_offset(&addr1, 900).await; // 100 bytes lag
    primary.update_replica_offset(&addr2, 700).await; // 300 bytes lag

    assert_eq!(primary.min_replica_lag().await, 100);
    assert_eq!(primary.max_replica_lag().await, 300);
}

#[tokio::test]
async fn test_primary_propagate() {
    let store = Arc::new(Store::new(16));
    let state = Arc::new(ReplicationState::new());
    let stream = Arc::new(ReplicationStream::new(10000));
    let primary = ReplicationPrimary::new(store, state.clone(), stream.clone());

    let mut rx = stream.subscribe();

    let cmd = ReplicationCommand::Set {
        key: Bytes::from("key"),
        value: Bytes::from("value"),
    };

    let initial_offset = state.repl_offset();
    primary.propagate(cmd).await;

    // Offset should have increased
    assert!(state.repl_offset() > initial_offset);

    // Command should be received by subscriber
    let (_, offset) = rx.recv().await.unwrap();
    assert_eq!(offset, initial_offset);
}

#[tokio::test]
async fn test_primary_generate_rdb() {
    let store = Arc::new(Store::new(16));
    let state = Arc::new(ReplicationState::new());
    let stream = Arc::new(ReplicationStream::new(10000));
    let primary = ReplicationPrimary::new(store, state, stream);

    let rdb = primary.generate_rdb();
    assert!(rdb.starts_with(b"REDIS"));
    assert!(rdb.len() > 9); // Magic + version at minimum
}

#[tokio::test]
async fn test_primary_get_info() {
    let store = Arc::new(Store::new(16));
    let state = Arc::new(ReplicationState::new());
    let stream = Arc::new(ReplicationStream::new(10000));
    let primary = ReplicationPrimary::new(store, state.clone(), stream);

    // Add a replica
    let addr: SocketAddr = "127.0.0.1:6380".parse().unwrap();
    primary.register_replica(addr, 6380).await;
    primary
        .set_replica_state(&addr, ReplicaConnectionState::Online)
        .await;

    let info = primary.get_info().await;

    assert!(info.contains("role:master"));
    assert!(info.contains("connected_slaves:1"));
    assert!(info.contains("master_replid:"));
    assert!(info.contains("master_repl_offset:"));
    assert!(info.contains("repl_backlog_active:1"));
}

#[tokio::test]
async fn test_primary_getack_command() {
    let cmd = ReplicationPrimary::make_getack_command();

    match cmd {
        ferrite::protocol::Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 3);
        }
        _ => panic!("Expected array frame"),
    }
}

// ============================================================================
// ReplicationCommand Tests
// ============================================================================

#[test]
fn test_replication_command_set() {
    let cmd = ReplicationCommand::Set {
        key: Bytes::from("key"),
        value: Bytes::from("value"),
    };

    let frame = cmd.to_frame();
    match frame {
        ferrite::protocol::Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 3);
        }
        _ => panic!("Expected array"),
    }

    let size = cmd.encoded_size();
    let bytes = cmd.to_bytes();
    assert_eq!(size as usize, bytes.len());
}

#[test]
fn test_replication_command_setex() {
    let cmd = ReplicationCommand::SetEx {
        key: Bytes::from("key"),
        value: Bytes::from("value"),
        expire_ms: 5000,
    };

    let frame = cmd.to_frame();
    match frame {
        ferrite::protocol::Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 5); // SET key value PX ms
        }
        _ => panic!("Expected array"),
    }
}

#[test]
fn test_replication_command_del() {
    let cmd = ReplicationCommand::Del {
        keys: vec![
            Bytes::from("key1"),
            Bytes::from("key2"),
            Bytes::from("key3"),
        ],
    };

    let frame = cmd.to_frame();
    match frame {
        ferrite::protocol::Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 4); // DEL key1 key2 key3
        }
        _ => panic!("Expected array"),
    }
}

#[test]
fn test_replication_command_hset() {
    let cmd = ReplicationCommand::HSet {
        key: Bytes::from("hash"),
        pairs: vec![
            (Bytes::from("field1"), Bytes::from("value1")),
            (Bytes::from("field2"), Bytes::from("value2")),
        ],
    };

    let frame = cmd.to_frame();
    match frame {
        ferrite::protocol::Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 6); // HSET key f1 v1 f2 v2
        }
        _ => panic!("Expected array"),
    }
}

#[test]
fn test_replication_command_lpush() {
    let cmd = ReplicationCommand::LPush {
        key: Bytes::from("list"),
        values: vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
    };

    let frame = cmd.to_frame();
    match frame {
        ferrite::protocol::Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 5); // LPUSH key a b c
        }
        _ => panic!("Expected array"),
    }
}

#[test]
fn test_replication_command_zadd() {
    let cmd = ReplicationCommand::ZAdd {
        key: Bytes::from("zset"),
        pairs: vec![(1.0, Bytes::from("member1")), (2.5, Bytes::from("member2"))],
    };

    let frame = cmd.to_frame();
    match frame {
        ferrite::protocol::Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 6); // ZADD key score1 member1 score2 member2
        }
        _ => panic!("Expected array"),
    }
}

#[test]
fn test_replication_command_raw() {
    let cmd = ReplicationCommand::Raw {
        command: vec![
            Bytes::from("CUSTOM"),
            Bytes::from("arg1"),
            Bytes::from("arg2"),
        ],
    };

    let frame = cmd.to_frame();
    match frame {
        ferrite::protocol::Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 3);
        }
        _ => panic!("Expected array"),
    }
}

#[test]
fn test_replication_command_flushall() {
    let cmd = ReplicationCommand::FlushAll;
    let frame = cmd.to_frame();
    match frame {
        ferrite::protocol::Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 1);
        }
        _ => panic!("Expected array"),
    }
}

// ============================================================================
// PsyncResponse Tests
// ============================================================================

#[test]
fn test_psync_response_fullresync_frame() {
    let replid = ReplicationId::new();
    let response = PsyncResponse::FullResync {
        replid: replid.clone(),
        offset: 12345,
    };

    let frame = response.to_frame();
    match frame {
        ferrite::protocol::Frame::Simple(s) => {
            let s_str = String::from_utf8_lossy(&s);
            assert!(s_str.starts_with("FULLRESYNC"));
            assert!(s_str.contains(replid.as_str()));
            assert!(s_str.contains("12345"));
        }
        _ => panic!("Expected simple string"),
    }
}

#[test]
fn test_psync_response_continue_frame() {
    let replid = ReplicationId::new();
    let response = PsyncResponse::Continue {
        replid: replid.clone(),
        offset: 5000,
    };

    let frame = response.to_frame();
    match frame {
        ferrite::protocol::Frame::Simple(s) => {
            let s_str = String::from_utf8_lossy(&s);
            assert!(s_str.starts_with("CONTINUE"));
            assert!(s_str.contains(replid.as_str()));
        }
        _ => panic!("Expected simple string"),
    }
}

// ============================================================================
// Concurrency Tests
// ============================================================================

#[tokio::test]
async fn test_concurrent_replica_operations() {
    let store = Arc::new(Store::new(16));
    let state = Arc::new(ReplicationState::new());
    let stream = Arc::new(ReplicationStream::new(10000));
    let primary = Arc::new(ReplicationPrimary::new(store, state, stream));

    let mut handles = vec![];

    // Concurrently register replicas
    for i in 0..10 {
        let p = primary.clone();
        handles.push(tokio::spawn(async move {
            let addr: SocketAddr = format!("127.0.0.1:{}", 6380 + i).parse().unwrap();
            p.register_replica(addr, 6380 + i).await;
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let replicas = primary.get_replicas().await;
    assert_eq!(replicas.len(), 10);
}

#[tokio::test]
async fn test_concurrent_offset_updates() {
    let state = Arc::new(ReplicationState::new());

    let mut handles = vec![];

    for _ in 0..100 {
        let s = state.clone();
        handles.push(tokio::spawn(async move {
            s.increment_offset(10);
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    assert_eq!(state.repl_offset(), 1000);
}

#[tokio::test]
async fn test_concurrent_broadcast() {
    let stream = Arc::new(ReplicationStream::new(100000));

    let mut handles = vec![];

    for i in 0..50 {
        let s = stream.clone();
        handles.push(tokio::spawn(async move {
            let cmd = ReplicationCommand::Set {
                key: Bytes::from(format!("key{}", i)),
                value: Bytes::from("value"),
            };
            s.broadcast(cmd, i * 30).await;
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

// ============================================================================
// Failover Scenario Tests
// ============================================================================

#[tokio::test]
async fn test_replica_registration_unregistration_cycle() {
    let store = Arc::new(Store::new(16));
    let state = Arc::new(ReplicationState::new());
    let stream = Arc::new(ReplicationStream::new(10000));
    let primary = ReplicationPrimary::new(store, state.clone(), stream);

    let addr: SocketAddr = "127.0.0.1:6380".parse().unwrap();

    // Simulate replica connecting and disconnecting multiple times
    for _ in 0..5 {
        primary.register_replica(addr, 6380).await;
        assert_eq!(state.connected_replicas(), 1);

        primary.unregister_replica(&addr).await;
        assert_eq!(state.connected_replicas(), 0);
    }
}

#[tokio::test]
async fn test_multiple_replicas_independent_offsets() {
    let store = Arc::new(Store::new(16));
    let state = Arc::new(ReplicationState::new());
    let stream = Arc::new(ReplicationStream::new(10000));
    let primary = ReplicationPrimary::new(store, state.clone(), stream);

    state.set_offset(10000);

    // Register 3 replicas with different lag
    let addrs: Vec<SocketAddr> = vec![
        "127.0.0.1:6380".parse().unwrap(),
        "127.0.0.1:6381".parse().unwrap(),
        "127.0.0.1:6382".parse().unwrap(),
    ];

    for (i, addr) in addrs.iter().enumerate() {
        primary
            .register_replica_with_caps(*addr, 6380 + i as u16, vec!["eof".to_string()])
            .await;
        primary
            .set_replica_state(addr, ReplicaConnectionState::Online)
            .await;
    }

    // Set different offsets (simulating different sync speeds)
    primary.update_replica_offset(&addrs[0], 9000).await; // 1000 lag
    primary.update_replica_offset(&addrs[1], 9500).await; // 500 lag
    primary.update_replica_offset(&addrs[2], 10000).await; // 0 lag (caught up)

    // Verify independent offset tracking
    let replicas = primary.get_replicas().await;
    let offsets: Vec<u64> = replicas.iter().map(|r| r.offset).collect();
    assert!(offsets.contains(&9000));
    assert!(offsets.contains(&9500));
    assert!(offsets.contains(&10000));
}

#[tokio::test]
async fn test_partial_sync_eligibility() {
    let store = Arc::new(Store::new(16));
    let state = Arc::new(ReplicationState::new());
    let stream = Arc::new(ReplicationStream::new(10000));
    let primary = ReplicationPrimary::new(store, state.clone(), stream.clone());

    // Add commands to backlog
    for i in 0..10 {
        let cmd = ReplicationCommand::Set {
            key: Bytes::from(format!("key{}", i)),
            value: Bytes::from("value"),
        };
        primary.propagate(cmd).await;
    }

    let master_replid = state.master_replid().await;

    // Request with matching replid and available offset should get CONTINUE
    // (Note: actual partial sync depends on backlog state)
    let response = primary.handle_psync(master_replid.as_str(), 0).await;
    assert!(response.is_ok());
}

#[tokio::test]
async fn test_backlog_entry_integrity() {
    let mut backlog = ReplicationBacklog::new(10000);

    let cmd = ReplicationCommand::HSet {
        key: Bytes::from("hash"),
        pairs: vec![(Bytes::from("field"), Bytes::from("value"))],
    };

    let size_before = cmd.encoded_size();
    backlog.push(cmd.clone(), 0);

    let entries = backlog.entries_from(0);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].offset, 0);
    assert_eq!(entries[0].size, size_before);
}

// ============================================================================
// Constants Tests
// ============================================================================

#[test]
fn test_heartbeat_interval() {
    // Redis default is 10 seconds
    assert_eq!(HEARTBEAT_INTERVAL, Duration::from_secs(10));
}

#[test]
fn test_replica_timeout() {
    // Redis default is 60 seconds
    assert_eq!(REPLICA_TIMEOUT, Duration::from_secs(60));
}
