#![allow(clippy::unwrap_used)]
use super::*;
use std::sync::Arc;
use std::time::{Duration, Instant};
use bytes::Bytes;
use crate::auth::Acl;
use crate::commands::blocking;
use crate::commands::BlockingListManager;
use crate::commands::parser::Command;
use crate::protocol::Frame;
use crate::runtime::SubscriptionManager;
use crate::storage::Store;

fn create_executor() -> CommandExecutor {
    let store = Arc::new(Store::new(16));
    let pubsub_manager = Arc::new(SubscriptionManager::new());
    let acl = Arc::new(Acl::new());
    let blocking_manager = Arc::new(BlockingListManager::new());
    let blocking_stream_manager = Arc::new(blocking::BlockingStreamManager::new());
    let blocking_zset_manager = Arc::new(blocking::BlockingSortedSetManager::new());
    CommandExecutor::new(
        store,
        pubsub_manager,
        acl,
        blocking_manager,
        blocking_stream_manager,
        blocking_zset_manager,
    )
}

#[tokio::test]
async fn test_get_set() {
    let executor = create_executor();

    // SET
    let response = executor
        .execute(
            Command::Set {
                key: Bytes::from("key"),
                value: Bytes::from("value"),
                options: Default::default(),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));

    // GET
    let response = executor
        .execute(
            Command::Get {
                key: Bytes::from("key"),
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("value")),
        other => panic!("Expected bulk string, got {:?}", other),
    }
}

#[tokio::test]
async fn test_del() {
    let executor = create_executor();

    // SET
    executor
        .execute(
            Command::Set {
                key: Bytes::from("key"),
                value: Bytes::from("value"),
                options: Default::default(),
            },
            0,
        )
        .await;

    // DEL
    let response = executor
        .execute(
            Command::Del {
                keys: vec![Bytes::from("key")],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(1)));

    // GET should return null
    let response = executor
        .execute(
            Command::Get {
                key: Bytes::from("key"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Bulk(None)));
}

#[tokio::test]
async fn test_ping() {
    let executor = create_executor();

    let response = executor.execute(Command::Ping { message: None }, 0).await;
    assert!(matches!(response, Frame::Simple(s) if s == "PONG"));

    let response = executor
        .execute(
            Command::Ping {
                message: Some(Bytes::from("hello")),
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("hello")),
        other => panic!("Expected bulk string, got {:?}", other),
    }
}

#[tokio::test]
async fn test_set_nx() {
    let executor = create_executor();

    // SET with NX on non-existing key should succeed
    let response = executor
        .execute(
            Command::Set {
                key: Bytes::from("key"),
                value: Bytes::from("value1"),
                options: crate::commands::parser::SetOptions {
                    nx: true,
                    ..Default::default()
                },
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));

    // SET with NX on existing key should fail
    let response = executor
        .execute(
            Command::Set {
                key: Bytes::from("key"),
                value: Bytes::from("value2"),
                options: crate::commands::parser::SetOptions {
                    nx: true,
                    ..Default::default()
                },
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Bulk(None)));

    // Value should still be value1
    let response = executor
        .execute(
            Command::Get {
                key: Bytes::from("key"),
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("value1")),
        other => panic!("Expected bulk string, got {:?}", other),
    }
}

#[tokio::test]
async fn test_replicaof() {
    let executor = create_executor();

    // REPLICAOF NO ONE
    let response = executor
        .execute(
            Command::ReplicaOf {
                host: None,
                port: None,
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));

    // REPLICAOF host port
    let response = executor
        .execute(
            Command::ReplicaOf {
                host: Some("127.0.0.1".to_string()),
                port: Some(6379),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));
}

#[tokio::test]
async fn test_replconf() {
    let executor = create_executor();

    // REPLCONF listening-port
    let response = executor
        .execute(
            Command::ReplConf {
                options: vec![("listening-port".to_string(), "6380".to_string())],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));

    // REPLCONF capa
    let response = executor
        .execute(
            Command::ReplConf {
                options: vec![
                    ("capa".to_string(), "eof".to_string()),
                    ("capa".to_string(), "psync2".to_string()),
                ],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));

    // REPLCONF GETACK
    let response = executor
        .execute(
            Command::ReplConf {
                options: vec![("getack".to_string(), "*".to_string())],
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 3);
        }
        other => panic!("Expected array for GETACK response, got {:?}", other),
    }
}

#[tokio::test]
async fn test_psync() {
    let executor = create_executor();

    // PSYNC ? -1 (first sync)
    let response = executor
        .execute(
            Command::Psync {
                replication_id: "?".to_string(),
                offset: -1,
            },
            0,
        )
        .await;
    match response {
        Frame::Simple(s) => {
            assert!(s.starts_with(b"FULLRESYNC"));
        }
        other => panic!("Expected simple string response, got {:?}", other),
    }

    // PSYNC with existing replid
    let response = executor
        .execute(
            Command::Psync {
                replication_id: "8371445ee47aed9eb27c89cdd67c6bf579a93f99".to_string(),
                offset: 100,
            },
            0,
        )
        .await;
    match response {
        Frame::Simple(s) => {
            // Either FULLRESYNC or CONTINUE
            assert!(s.starts_with(b"FULLRESYNC") || s.starts_with(b"CONTINUE"));
        }
        other => panic!("Expected simple string response, got {:?}", other),
    }
}

#[tokio::test]
async fn test_auth() {
    let executor = create_executor();

    // AUTH with default user (empty password for nopass user)
    let response = executor
        .execute(
            Command::Auth {
                username: None,
                password: String::new(),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));

    // AUTH with username and password
    let response = executor
        .execute(
            Command::Auth {
                username: Some("default".to_string()),
                password: "secret".to_string(),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));
}

#[tokio::test]
async fn test_acl_whoami() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Acl {
                subcommand: "WHOAMI".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("default")),
        other => panic!("Expected bulk string, got {:?}", other),
    }
}

#[tokio::test]
async fn test_acl_help() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Acl {
                subcommand: "HELP".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => {
            assert!(!arr.is_empty());
        }
        other => panic!("Expected array response for ACL HELP, got {:?}", other),
    }
}

#[tokio::test]
async fn test_acl_cat() {
    let executor = create_executor();

    // ACL CAT without arguments - list categories
    let response = executor
        .execute(
            Command::Acl {
                subcommand: "CAT".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => {
            // Should contain categories like "read", "write", etc.
            assert!(!arr.is_empty());
        }
        other => panic!("Expected array response for ACL CAT, got {:?}", other),
    }
}

#[tokio::test]
async fn test_acl_genpass() {
    let executor = create_executor();

    // ACL GENPASS - generate random password
    let response = executor
        .execute(
            Command::Acl {
                subcommand: "GENPASS".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => {
            // Default length is 64
            assert_eq!(data.len(), 64);
        }
        other => panic!("Expected bulk string response for ACL GENPASS, got {:?}", other),
    }

    // ACL GENPASS with length
    let response = executor
        .execute(
            Command::Acl {
                subcommand: "GENPASS".to_string(),
                args: vec!["32".to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => {
            assert_eq!(data.len(), 32);
        }
        other => panic!("Expected bulk string response for ACL GENPASS, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_info() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "INFO".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => {
            let info = String::from_utf8_lossy(&data);
            assert!(info.contains("cluster_state:ok"));
            assert!(info.contains("cluster_slots_assigned:16384"));
        }
        other => panic!("Expected bulk string for CLUSTER INFO, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_nodes() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "NODES".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => {
            let info = String::from_utf8_lossy(&data);
            assert!(info.contains("myself,master"));
            assert!(info.contains("connected"));
        }
        other => panic!("Expected bulk string for CLUSTER NODES, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_slots() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "SLOTS".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => {
            assert!(!arr.is_empty());
        }
        other => panic!("Expected array for CLUSTER SLOTS, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_keyslot() {
    let executor = create_executor();

    // CLUSTER KEYSLOT for a simple key
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "KEYSLOT".to_string(),
                args: vec!["mykey".to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Integer(slot) => {
            // Slot should be in valid range
            assert!((0..16384).contains(&slot));
        }
        other => panic!("Expected integer for CLUSTER KEYSLOT, got {:?}", other),
    }

    // CLUSTER KEYSLOT with hash tag
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "KEYSLOT".to_string(),
                args: vec!["{user1000}.following".to_string()],
            },
            0,
        )
        .await;
    let slot1 = match response {
        Frame::Integer(slot) => slot,
        other => panic!("Expected integer for CLUSTER KEYSLOT, got {:?}", other),
    };

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "KEYSLOT".to_string(),
                args: vec!["{user1000}.followers".to_string()],
            },
            0,
        )
        .await;
    let slot2 = match response {
        Frame::Integer(slot) => slot,
        other => panic!("Expected integer for CLUSTER KEYSLOT, got {:?}", other),
    };

    // Same hash tag should produce same slot
    assert_eq!(slot1, slot2);
}

#[tokio::test]
async fn test_cluster_myid() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "MYID".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => {
            // Node ID should be 40 hex characters
            assert_eq!(data.len(), 40);
        }
        other => panic!("Expected bulk string for CLUSTER MYID, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_help() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "HELP".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => {
            assert!(!arr.is_empty());
        }
        other => panic!("Expected array for CLUSTER HELP, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_meet() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "MEET".to_string(),
                args: vec!["127.0.0.1".to_string(), "6380".to_string()],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));
}

#[tokio::test]
async fn test_cluster_addslots() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "ADDSLOTS".to_string(),
                args: vec!["0".to_string(), "1".to_string(), "2".to_string()],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));
}

#[tokio::test]
async fn test_cluster_delslots() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "DELSLOTS".to_string(),
                args: vec!["0".to_string(), "1".to_string()],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));
}

#[tokio::test]
async fn test_cluster_setslot() {
    let executor = create_executor();

    // SETSLOT STABLE
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "SETSLOT".to_string(),
                args: vec!["0".to_string(), "STABLE".to_string()],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));

    // SETSLOT IMPORTING
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "SETSLOT".to_string(),
                args: vec![
                    "0".to_string(),
                    "IMPORTING".to_string(),
                    "nodeid".to_string(),
                ],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));
}

#[tokio::test]
async fn test_cluster_countkeysinslot() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "COUNTKEYSINSLOT".to_string(),
                args: vec!["100".to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Integer(count) => {
            assert!(count >= 0);
        }
        other => panic!("Expected integer for CLUSTER COUNTKEYSINSLOT, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_getkeysinslot() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "GETKEYSINSLOT".to_string(),
                args: vec!["100".to_string(), "10".to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(_)) => {
            // Empty or contains keys
        }
        other => panic!("Expected array for CLUSTER GETKEYSINSLOT, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_failover() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "FAILOVER".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));
}

#[tokio::test]
async fn test_cluster_reset() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "RESET".to_string(),
                args: vec!["SOFT".to_string()],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));
}

#[tokio::test]
async fn test_cluster_unknown_subcommand() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "UNKNOWN".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Error(_) => {}
        other => panic!("Expected error for unknown CLUSTER subcommand, got {:?}", other),
    }
}

// ACL Integration Tests

#[tokio::test]
async fn test_execute_with_acl_default_user() {
    let executor = create_executor();

    // Default user should be able to execute commands
    let response = executor
        .execute_with_acl(
            Command::Set {
                key: Bytes::from("key"),
                value: Bytes::from("value"),
                options: Default::default(),
            },
            0,
            "default",
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));

    let response = executor
        .execute_with_acl(
            Command::Get {
                key: Bytes::from("key"),
            },
            0,
            "default",
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("value")),
        other => panic!("Expected bulk string, got {:?}", other),
    }
}

#[tokio::test]
async fn test_execute_with_acl_restricted_user() {
    let store = Arc::new(Store::new(16));
    let pubsub_manager = Arc::new(SubscriptionManager::new());
    let acl = Arc::new(Acl::new());
    let blocking_manager = Arc::new(BlockingListManager::new());
    let blocking_stream_manager = Arc::new(blocking::BlockingStreamManager::new());
    let blocking_zset_manager = Arc::new(blocking::BlockingSortedSetManager::new());

    // Create a restricted user that can only GET, not SET
    acl.set_user(
        "readonly",
        &[
            "on".to_string(),
            "nopass".to_string(),
            "+GET".to_string(),
            "~*".to_string(), // All keys
        ],
    )
    .await
    .unwrap();

    let executor = CommandExecutor::new(
        store,
        pubsub_manager,
        acl,
        blocking_manager,
        blocking_stream_manager,
        blocking_zset_manager,
    );

    // Readonly user cannot SET (command not allowed)
    let response = executor
        .execute_with_acl(
            Command::Set {
                key: Bytes::from("key"),
                value: Bytes::from("value"),
                options: Default::default(),
            },
            0,
            "readonly",
        )
        .await;
    match response {
        Frame::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(err_str.contains("NOPERM") || err_str.contains("permission"));
        }
        other => panic!("Expected error for denied command, got {:?}", other),
    }

    // But can GET (after setting via default user)
    executor
        .execute_with_acl(
            Command::Set {
                key: Bytes::from("key"),
                value: Bytes::from("value"),
                options: Default::default(),
            },
            0,
            "default",
        )
        .await;

    let response = executor
        .execute_with_acl(
            Command::Get {
                key: Bytes::from("key"),
            },
            0,
            "readonly",
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("value")),
        other => panic!("Expected bulk string for GET, got {:?}", other),
    }
}

#[tokio::test]
async fn test_execute_with_acl_key_pattern() {
    let store = Arc::new(Store::new(16));
    let pubsub_manager = Arc::new(SubscriptionManager::new());
    let acl = Arc::new(Acl::new());
    let blocking_manager = Arc::new(BlockingListManager::new());
    let blocking_stream_manager = Arc::new(blocking::BlockingStreamManager::new());
    let blocking_zset_manager = Arc::new(blocking::BlockingSortedSetManager::new());

    // Create a user that can only access keys starting with "user:"
    acl.set_user(
        "app",
        &[
            "on".to_string(),
            "nopass".to_string(),
            "+@all".to_string(),   // All commands
            "~user:*".to_string(), // Only user:* keys
        ],
    )
    .await
    .unwrap();

    let executor = CommandExecutor::new(
        store,
        pubsub_manager,
        acl,
        blocking_manager,
        blocking_stream_manager,
        blocking_zset_manager,
    );

    // App user can access user:123
    let response = executor
        .execute_with_acl(
            Command::Set {
                key: Bytes::from("user:123"),
                value: Bytes::from("data"),
                options: Default::default(),
            },
            0,
            "app",
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));

    // App user cannot access admin:settings (key pattern mismatch)
    let response = executor
        .execute_with_acl(
            Command::Set {
                key: Bytes::from("admin:settings"),
                value: Bytes::from("data"),
                options: Default::default(),
            },
            0,
            "app",
        )
        .await;
    match response {
        Frame::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(err_str.contains("NOPERM") || err_str.contains("permission"));
        }
        other => panic!("Expected error for key access denied, got {:?}", other),
    }
}

#[tokio::test]
async fn test_execute_with_acl_nonexistent_user() {
    let executor = create_executor();

    // Non-existent user should get an error
    let response = executor
        .execute_with_acl(
            Command::Get {
                key: Bytes::from("key"),
            },
            0,
            "nonexistent",
        )
        .await;
    match response {
        Frame::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(err_str.contains("not found") || err_str.contains("User"));
        }
        other => panic!("Expected error for non-existent user, got {:?}", other),
    }
}

#[tokio::test]
async fn test_command_meta_correctness() {
    // Test that meta() returns correct information for various commands
    let get_cmd = Command::Get {
        key: Bytes::from("mykey"),
    };
    let meta = get_cmd.meta();
    assert_eq!(meta.name, "GET");
    assert_eq!(meta.category, "string");
    assert_eq!(meta.keys, vec![Bytes::from("mykey")]);
    assert!(matches!(meta.permission, Permission::Read));

    let set_cmd = Command::Set {
        key: Bytes::from("mykey"),
        value: Bytes::from("value"),
        options: Default::default(),
    };
    let meta = set_cmd.meta();
    assert_eq!(meta.name, "SET");
    assert_eq!(meta.category, "string");
    assert!(matches!(meta.permission, Permission::Write));

    let del_cmd = Command::Del {
        keys: vec![Bytes::from("k1"), Bytes::from("k2")],
    };
    let meta = del_cmd.meta();
    assert_eq!(meta.name, "DEL");
    assert_eq!(meta.keys.len(), 2);
    assert!(matches!(meta.permission, Permission::Write));

    let zadd_cmd = Command::ZAdd {
        key: Bytes::from("myset"),
        options: Default::default(),
        pairs: vec![(1.0, Bytes::from("member"))],
    };
    let meta = zadd_cmd.meta();
    assert_eq!(meta.name, "ZADD");
    assert_eq!(meta.category, "sortedset");
}

// List command tests through executor

#[tokio::test]
async fn test_list_lpush_rpush() {
    let executor = create_executor();

    // LPUSH
    let response = executor
        .execute(
            Command::LPush {
                key: Bytes::from("mylist"),
                values: vec![Bytes::from("a"), Bytes::from("b")],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(2)));

    // RPUSH
    let response = executor
        .execute(
            Command::RPush {
                key: Bytes::from("mylist"),
                values: vec![Bytes::from("c")],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(3)));

    // LLEN
    let response = executor
        .execute(
            Command::LLen {
                key: Bytes::from("mylist"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(3)));
}

#[tokio::test]
async fn test_list_lpop_rpop() {
    let executor = create_executor();

    // Setup list
    executor
        .execute(
            Command::RPush {
                key: Bytes::from("mylist"),
                values: vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            },
            0,
        )
        .await;

    // LPOP
    let response = executor
        .execute(
            Command::LPop {
                key: Bytes::from("mylist"),
                count: None,
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("a")),
        other => panic!("Expected bulk string, got {:?}", other),
    }

    // RPOP
    let response = executor
        .execute(
            Command::RPop {
                key: Bytes::from("mylist"),
                count: None,
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("c")),
        other => panic!("Expected bulk string, got {:?}", other),
    }
}

#[tokio::test]
async fn test_list_lrange() {
    let executor = create_executor();

    // Setup list
    executor
        .execute(
            Command::RPush {
                key: Bytes::from("mylist"),
                values: vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            },
            0,
        )
        .await;

    // LRANGE 0 -1 (all elements)
    let response = executor
        .execute(
            Command::LRange {
                key: Bytes::from("mylist"),
                start: 0,
                stop: -1,
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 3);
        }
        other => panic!("Expected array, got {:?}", other),
    }
}

// Hash command tests through executor

#[tokio::test]
async fn test_hash_hset_hget() {
    let executor = create_executor();

    // HSET
    let response = executor
        .execute(
            Command::HSet {
                key: Bytes::from("myhash"),
                pairs: vec![
                    (Bytes::from("field1"), Bytes::from("value1")),
                    (Bytes::from("field2"), Bytes::from("value2")),
                ],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(2)));

    // HGET
    let response = executor
        .execute(
            Command::HGet {
                key: Bytes::from("myhash"),
                field: Bytes::from("field1"),
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("value1")),
        other => panic!("Expected bulk string, got {:?}", other),
    }

    // HGET non-existent field
    let response = executor
        .execute(
            Command::HGet {
                key: Bytes::from("myhash"),
                field: Bytes::from("nonexistent"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Bulk(None)));
}

#[tokio::test]
async fn test_hash_hgetall() {
    let executor = create_executor();

    // HSET
    executor
        .execute(
            Command::HSet {
                key: Bytes::from("myhash"),
                pairs: vec![
                    (Bytes::from("f1"), Bytes::from("v1")),
                    (Bytes::from("f2"), Bytes::from("v2")),
                ],
            },
            0,
        )
        .await;

    // HGETALL
    let response = executor
        .execute(
            Command::HGetAll {
                key: Bytes::from("myhash"),
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => {
            // Should return field-value pairs (4 elements for 2 pairs)
            assert_eq!(arr.len(), 4);
        }
        other => panic!("Expected array, got {:?}", other),
    }
}

// Set command tests through executor

#[tokio::test]
async fn test_set_sadd_smembers() {
    let executor = create_executor();

    // SADD
    let response = executor
        .execute(
            Command::SAdd {
                key: Bytes::from("myset"),
                members: vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(3)));

    // SADD duplicate
    let response = executor
        .execute(
            Command::SAdd {
                key: Bytes::from("myset"),
                members: vec![Bytes::from("a")],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(0)));

    // SMEMBERS
    let response = executor
        .execute(
            Command::SMembers {
                key: Bytes::from("myset"),
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 3);
        }
        other => panic!("Expected array, got {:?}", other),
    }
}

#[tokio::test]
async fn test_set_sismember() {
    let executor = create_executor();

    executor
        .execute(
            Command::SAdd {
                key: Bytes::from("myset"),
                members: vec![Bytes::from("hello")],
            },
            0,
        )
        .await;

    // SISMEMBER - exists
    let response = executor
        .execute(
            Command::SIsMember {
                key: Bytes::from("myset"),
                member: Bytes::from("hello"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(1)));

    // SISMEMBER - not exists
    let response = executor
        .execute(
            Command::SIsMember {
                key: Bytes::from("myset"),
                member: Bytes::from("world"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(0)));
}

// Sorted set command tests through executor

#[tokio::test]
async fn test_zset_zadd_zrange() {
    let executor = create_executor();

    // ZADD
    let response = executor
        .execute(
            Command::ZAdd {
                key: Bytes::from("myzset"),
                options: Default::default(),
                pairs: vec![
                    (1.0, Bytes::from("one")),
                    (2.0, Bytes::from("two")),
                    (3.0, Bytes::from("three")),
                ],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(3)));

    // ZRANGE
    let response = executor
        .execute(
            Command::ZRange {
                key: Bytes::from("myzset"),
                start: 0,
                stop: -1,
                with_scores: false,
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 3);
        }
        other => panic!("Expected array, got {:?}", other),
    }
}

#[tokio::test]
async fn test_zset_zscore() {
    let executor = create_executor();

    executor
        .execute(
            Command::ZAdd {
                key: Bytes::from("myzset"),
                options: Default::default(),
                pairs: vec![(1.5, Bytes::from("member"))],
            },
            0,
        )
        .await;

    // ZSCORE
    let response = executor
        .execute(
            Command::ZScore {
                key: Bytes::from("myzset"),
                member: Bytes::from("member"),
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => {
            let score: f64 = String::from_utf8_lossy(&data).parse().unwrap();
            assert!((score - 1.5).abs() < 0.001);
        }
        other => panic!("Expected bulk string with score, got {:?}", other),
    }
}

// Database isolation tests

#[tokio::test]
async fn test_database_isolation() {
    let executor = create_executor();

    // SET in db 0
    executor
        .execute(
            Command::Set {
                key: Bytes::from("key"),
                value: Bytes::from("db0-value"),
                options: Default::default(),
            },
            0,
        )
        .await;

    // SET in db 1
    executor
        .execute(
            Command::Set {
                key: Bytes::from("key"),
                value: Bytes::from("db1-value"),
                options: Default::default(),
            },
            1,
        )
        .await;

    // GET from db 0
    let response = executor
        .execute(
            Command::Get {
                key: Bytes::from("key"),
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("db0-value")),
        other => panic!("Expected bulk string, got {:?}", other),
    }

    // GET from db 1
    let response = executor
        .execute(
            Command::Get {
                key: Bytes::from("key"),
            },
            1,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("db1-value")),
        other => panic!("Expected bulk string, got {:?}", other),
    }

    // GET from db 2 (should not exist)
    let response = executor
        .execute(
            Command::Get {
                key: Bytes::from("key"),
            },
            2,
        )
        .await;
    assert!(matches!(response, Frame::Bulk(None)));
}

// INCR/DECR tests

#[tokio::test]
async fn test_incr_decr() {
    let executor = create_executor();

    // SET initial value
    executor
        .execute(
            Command::Set {
                key: Bytes::from("counter"),
                value: Bytes::from("10"),
                options: Default::default(),
            },
            0,
        )
        .await;

    // INCR
    let response = executor
        .execute(
            Command::Incr {
                key: Bytes::from("counter"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(11)));

    // DECR
    let response = executor
        .execute(
            Command::Decr {
                key: Bytes::from("counter"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(10)));

    // INCRBY
    let response = executor
        .execute(
            Command::IncrBy {
                key: Bytes::from("counter"),
                delta: 5,
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(15)));

    // DECRBY
    let response = executor
        .execute(
            Command::DecrBy {
                key: Bytes::from("counter"),
                delta: 3,
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(12)));
}

#[tokio::test]
async fn test_incr_non_integer() {
    let executor = create_executor();

    // SET a non-integer value
    executor
        .execute(
            Command::Set {
                key: Bytes::from("str"),
                value: Bytes::from("hello"),
                options: Default::default(),
            },
            0,
        )
        .await;

    // INCR should fail
    let response = executor
        .execute(
            Command::Incr {
                key: Bytes::from("str"),
            },
            0,
        )
        .await;
    match response {
        Frame::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(err_str.contains("not an integer") || err_str.contains("integer"));
        }
        other => panic!("Expected error for INCR on non-integer, got {:?}", other),
    }
}

// TTL and expiration tests

#[tokio::test]
async fn test_expire_ttl() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("key"),
                value: Bytes::from("value"),
                options: Default::default(),
            },
            0,
        )
        .await;

    // TTL on key without expiry
    let response = executor
        .execute(
            Command::Ttl {
                key: Bytes::from("key"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(-1)));

    // EXPIRE
    let response = executor
        .execute(
            Command::Expire {
                key: Bytes::from("key"),
                seconds: 100,
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(1)));

    // TTL should now return remaining time
    let response = executor
        .execute(
            Command::Ttl {
                key: Bytes::from("key"),
            },
            0,
        )
        .await;
    match response {
        Frame::Integer(ttl) => assert!(ttl > 0 && ttl <= 100),
        other => panic!("Expected positive TTL, got {:?}", other),
    }
}

#[tokio::test]
async fn test_ttl_nonexistent_key() {
    let executor = create_executor();

    // TTL on non-existent key
    let response = executor
        .execute(
            Command::Ttl {
                key: Bytes::from("nonexistent"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(-2)));
}

// MGET/MSET tests

#[tokio::test]
async fn test_mset_mget() {
    let executor = create_executor();

    // MSET
    let response = executor
        .execute(
            Command::MSet {
                pairs: vec![
                    (Bytes::from("k1"), Bytes::from("v1")),
                    (Bytes::from("k2"), Bytes::from("v2")),
                    (Bytes::from("k3"), Bytes::from("v3")),
                ],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));

    // MGET
    let response = executor
        .execute(
            Command::MGet {
                keys: vec![
                    Bytes::from("k1"),
                    Bytes::from("k2"),
                    Bytes::from("nonexistent"),
                    Bytes::from("k3"),
                ],
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 4);
            // Third element should be null
            assert!(matches!(arr[2], Frame::Bulk(None)));
        }
        other => panic!("Expected array, got {:?}", other),
    }
}

// EXISTS tests

#[tokio::test]
async fn test_exists_multiple_keys() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("k1"),
                value: Bytes::from("v1"),
                options: Default::default(),
            },
            0,
        )
        .await;
    executor
        .execute(
            Command::Set {
                key: Bytes::from("k2"),
                value: Bytes::from("v2"),
                options: Default::default(),
            },
            0,
        )
        .await;

    // EXISTS with multiple keys (some exist, some don't)
    let response = executor
        .execute(
            Command::Exists {
                keys: vec![Bytes::from("k1"), Bytes::from("k2"), Bytes::from("k3")],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(2)));
}

// TYPE command test

#[tokio::test]
async fn test_type_command() {
    let executor = create_executor();

    // String type
    executor
        .execute(
            Command::Set {
                key: Bytes::from("str"),
                value: Bytes::from("value"),
                options: Default::default(),
            },
            0,
        )
        .await;
    let response = executor
        .execute(
            Command::Type {
                key: Bytes::from("str"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "string"));

    // List type
    executor
        .execute(
            Command::LPush {
                key: Bytes::from("list"),
                values: vec![Bytes::from("a")],
            },
            0,
        )
        .await;
    let response = executor
        .execute(
            Command::Type {
                key: Bytes::from("list"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "list"));

    // Hash type
    executor
        .execute(
            Command::HSet {
                key: Bytes::from("hash"),
                pairs: vec![(Bytes::from("f"), Bytes::from("v"))],
            },
            0,
        )
        .await;
    let response = executor
        .execute(
            Command::Type {
                key: Bytes::from("hash"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "hash"));

    // Non-existent key
    let response = executor
        .execute(
            Command::Type {
                key: Bytes::from("nonexistent"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "none"));
}

// DBSIZE command test

#[tokio::test]
async fn test_dbsize() {
    let executor = create_executor();

    // Empty database
    let response = executor.execute(Command::DbSize, 0).await;
    assert!(matches!(response, Frame::Integer(0)));

    // Add some keys
    executor
        .execute(
            Command::Set {
                key: Bytes::from("k1"),
                value: Bytes::from("v1"),
                options: Default::default(),
            },
            0,
        )
        .await;
    executor
        .execute(
            Command::Set {
                key: Bytes::from("k2"),
                value: Bytes::from("v2"),
                options: Default::default(),
            },
            0,
        )
        .await;

    let response = executor.execute(Command::DbSize, 0).await;
    assert!(matches!(response, Frame::Integer(2)));
}

// ECHO command test

#[tokio::test]
async fn test_echo() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Echo {
                message: Bytes::from("Hello World"),
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("Hello World")),
        other => panic!("Expected bulk string, got {:?}", other),
    }
}

// RENAME command test

#[tokio::test]
async fn test_rename() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("oldkey"),
                value: Bytes::from("value"),
                options: Default::default(),
            },
            0,
        )
        .await;

    // RENAME
    let response = executor
        .execute(
            Command::Rename {
                key: Bytes::from("oldkey"),
                new_key: Bytes::from("newkey"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));

    // Old key should not exist
    let response = executor
        .execute(
            Command::Get {
                key: Bytes::from("oldkey"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Bulk(None)));

    // New key should have the value
    let response = executor
        .execute(
            Command::Get {
                key: Bytes::from("newkey"),
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("value")),
        other => panic!("Expected bulk string, got {:?}", other),
    }
}

// ==================== STRING COMMAND TESTS ====================

#[tokio::test]
async fn test_append() {
    let executor = create_executor();

    // APPEND to non-existent key creates new key
    let response = executor
        .execute(
            Command::Append {
                key: Bytes::from("mykey"),
                value: Bytes::from("Hello"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(5)));

    // APPEND to existing key
    let response = executor
        .execute(
            Command::Append {
                key: Bytes::from("mykey"),
                value: Bytes::from(" World"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(11)));

    // Verify result
    let response = executor
        .execute(
            Command::Get {
                key: Bytes::from("mykey"),
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("Hello World")),
        other => panic!("Expected bulk string, got {:?}", other),
    }
}

#[tokio::test]
async fn test_strlen() {
    let executor = create_executor();

    // Non-existent key returns 0
    let response = executor
        .execute(
            Command::StrLen {
                key: Bytes::from("nokey"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(0)));

    // Create key
    executor
        .execute(
            Command::Set {
                key: Bytes::from("mykey"),
                value: Bytes::from("Hello World"),
                options: Default::default(),
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::StrLen {
                key: Bytes::from("mykey"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(11)));
}

#[tokio::test]
async fn test_getrange() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("mykey"),
                value: Bytes::from("Hello World"),
                options: Default::default(),
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::GetRange {
                key: Bytes::from("mykey"),
                start: 0,
                end: 4,
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("Hello")),
        other => panic!("Expected bulk string, got {:?}", other),
    }

    // Negative indices
    let response = executor
        .execute(
            Command::GetRange {
                key: Bytes::from("mykey"),
                start: -5,
                end: -1,
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("World")),
        other => panic!("Expected bulk string, got {:?}", other),
    }
}

#[tokio::test]
async fn test_setrange() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("key"),
                value: Bytes::from("Hello World"),
                options: Default::default(),
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::SetRange {
                key: Bytes::from("key"),
                offset: 6,
                value: Bytes::from("Redis"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(11)));

    let response = executor
        .execute(
            Command::Get {
                key: Bytes::from("key"),
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("Hello Redis")),
        other => panic!("Expected bulk string, got {:?}", other),
    }
}

#[tokio::test]
async fn test_incrby_decrby() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("counter"),
                value: Bytes::from("10"),
                options: Default::default(),
            },
            0,
        )
        .await;

    // INCRBY
    let response = executor
        .execute(
            Command::IncrBy {
                key: Bytes::from("counter"),
                delta: 5,
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(15)));

    // DECRBY
    let response = executor
        .execute(
            Command::DecrBy {
                key: Bytes::from("counter"),
                delta: 3,
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(12)));
}

#[tokio::test]
async fn test_incrbyfloat() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("mykey"),
                value: Bytes::from("10.5"),
                options: Default::default(),
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::IncrByFloat {
                key: Bytes::from("mykey"),
                delta: 0.1,
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => {
            let val: f64 = String::from_utf8_lossy(&data).parse().unwrap();
            assert!((val - 10.6).abs() < 0.001);
        }
        other => panic!("Expected bulk string, got {:?}", other),
    }
}

#[tokio::test]
async fn test_getdel() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("mykey"),
                value: Bytes::from("value"),
                options: Default::default(),
            },
            0,
        )
        .await;

    // GETDEL returns value and deletes
    let response = executor
        .execute(
            Command::GetDel {
                key: Bytes::from("mykey"),
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("value")),
        other => panic!("Expected bulk string, got {:?}", other),
    }

    // Key should no longer exist
    let response = executor
        .execute(
            Command::Get {
                key: Bytes::from("mykey"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Bulk(None)));
}

#[tokio::test]
async fn test_setex() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::SetEx {
                key: Bytes::from("mykey"),
                seconds: 10,
                value: Bytes::from("value"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));

    // Key should exist with TTL
    let response = executor
        .execute(
            Command::Ttl {
                key: Bytes::from("mykey"),
            },
            0,
        )
        .await;
    match response {
        Frame::Integer(ttl) => assert!(ttl > 0 && ttl <= 10),
        other => panic!("Expected integer, got {:?}", other),
    }
}

#[tokio::test]
async fn test_psetex() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::PSetEx {
                key: Bytes::from("mykey"),
                milliseconds: 10000,
                value: Bytes::from("value"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));

    // Key should exist with TTL in milliseconds
    let response = executor
        .execute(
            Command::PTtl {
                key: Bytes::from("mykey"),
            },
            0,
        )
        .await;
    match response {
        Frame::Integer(ttl) => assert!(ttl > 0 && ttl <= 10000),
        other => panic!("Expected integer, got {:?}", other),
    }
}

// ==================== BITMAP COMMAND TESTS ====================

#[tokio::test]
async fn test_setbit_getbit() {
    let executor = create_executor();

    // SETBIT - setting a bit
    let response = executor
        .execute(
            Command::SetBit {
                key: Bytes::from("mykey"),
                offset: 7,
                value: 1,
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(0))); // Previous value was 0

    // GETBIT
    let response = executor
        .execute(
            Command::GetBit {
                key: Bytes::from("mykey"),
                offset: 7,
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(1)));

    // GETBIT - unset bit
    let response = executor
        .execute(
            Command::GetBit {
                key: Bytes::from("mykey"),
                offset: 0,
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(0)));
}

#[tokio::test]
async fn test_bitcount() {
    let executor = create_executor();

    // Set "foobar" (which has specific bit pattern)
    executor
        .execute(
            Command::Set {
                key: Bytes::from("mykey"),
                value: Bytes::from("foobar"),
                options: Default::default(),
            },
            0,
        )
        .await;

    // Count all bits
    let response = executor
        .execute(
            Command::BitCount {
                key: Bytes::from("mykey"),
                start: None,
                end: None,
                bit_mode: false,
            },
            0,
        )
        .await;
    match response {
        Frame::Integer(count) => assert!(count > 0),
        other => panic!("Expected integer, got {:?}", other),
    }

    // Count bits in range
    let response = executor
        .execute(
            Command::BitCount {
                key: Bytes::from("mykey"),
                start: Some(0),
                end: Some(0),
                bit_mode: false,
            },
            0,
        )
        .await;
    match response {
        Frame::Integer(count) => assert!(count >= 0),
        other => panic!("Expected integer, got {:?}", other),
    }
}

#[tokio::test]
async fn test_bitop_and() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("key1"),
                value: Bytes::from("foof"),
                options: Default::default(),
            },
            0,
        )
        .await;
    executor
        .execute(
            Command::Set {
                key: Bytes::from("key2"),
                value: Bytes::from("foof"),
                options: Default::default(),
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::BitOp {
                operation: crate::commands::bitmap::BitOperation::And,
                destkey: Bytes::from("destkey"),
                keys: vec![Bytes::from("key1"), Bytes::from("key2")],
            },
            0,
        )
        .await;
    match response {
        Frame::Integer(len) => assert_eq!(len, 4),
        other => panic!("Expected integer, got {:?}", other),
    }
}

#[tokio::test]
async fn test_bitop_or() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("key1"),
                value: Bytes::from("foof"),
                options: Default::default(),
            },
            0,
        )
        .await;
    executor
        .execute(
            Command::Set {
                key: Bytes::from("key2"),
                value: Bytes::from("foof"),
                options: Default::default(),
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::BitOp {
                operation: crate::commands::bitmap::BitOperation::Or,
                destkey: Bytes::from("destkey"),
                keys: vec![Bytes::from("key1"), Bytes::from("key2")],
            },
            0,
        )
        .await;
    match response {
        Frame::Integer(len) => assert_eq!(len, 4),
        other => panic!("Expected integer, got {:?}", other),
    }
}

#[tokio::test]
async fn test_bitpos() {
    let executor = create_executor();

    // Set a key with known bit pattern (all 1s followed by some 0s)
    executor
        .execute(
            Command::Set {
                key: Bytes::from("mykey"),
                value: Bytes::from_static(&[0xff, 0xf0, 0x00]),
                options: Default::default(),
            },
            0,
        )
        .await;

    // Find first 0 bit
    let response = executor
        .execute(
            Command::BitPos {
                key: Bytes::from("mykey"),
                bit: 0,
                start: None,
                end: None,
                bit_mode: false,
            },
            0,
        )
        .await;
    match response {
        Frame::Integer(pos) => assert!(pos >= 0),
        other => panic!("Expected integer, got {:?}", other),
    }
}

// ==================== HASH COMMAND TESTS ====================

#[tokio::test]
async fn test_hdel() {
    let executor = create_executor();

    executor
        .execute(
            Command::HSet {
                key: Bytes::from("myhash"),
                pairs: vec![
                    (Bytes::from("field1"), Bytes::from("value1")),
                    (Bytes::from("field2"), Bytes::from("value2")),
                ],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::HDel {
                key: Bytes::from("myhash"),
                fields: vec![Bytes::from("field1")],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(1)));

    // Verify field is deleted
    let response = executor
        .execute(
            Command::HExists {
                key: Bytes::from("myhash"),
                field: Bytes::from("field1"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(0)));
}

#[tokio::test]
async fn test_hexists() {
    let executor = create_executor();

    executor
        .execute(
            Command::HSet {
                key: Bytes::from("myhash"),
                pairs: vec![(Bytes::from("field1"), Bytes::from("value1"))],
            },
            0,
        )
        .await;

    // Existing field
    let response = executor
        .execute(
            Command::HExists {
                key: Bytes::from("myhash"),
                field: Bytes::from("field1"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(1)));

    // Non-existent field
    let response = executor
        .execute(
            Command::HExists {
                key: Bytes::from("myhash"),
                field: Bytes::from("nofield"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(0)));
}

#[tokio::test]
async fn test_hlen() {
    let executor = create_executor();

    executor
        .execute(
            Command::HSet {
                key: Bytes::from("myhash"),
                pairs: vec![
                    (Bytes::from("field1"), Bytes::from("value1")),
                    (Bytes::from("field2"), Bytes::from("value2")),
                    (Bytes::from("field3"), Bytes::from("value3")),
                ],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::HLen {
                key: Bytes::from("myhash"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(3)));
}

#[tokio::test]
async fn test_hkeys_hvals() {
    let executor = create_executor();

    executor
        .execute(
            Command::HSet {
                key: Bytes::from("myhash"),
                pairs: vec![
                    (Bytes::from("f1"), Bytes::from("v1")),
                    (Bytes::from("f2"), Bytes::from("v2")),
                ],
            },
            0,
        )
        .await;

    // HKEYS
    let response = executor
        .execute(
            Command::HKeys {
                key: Bytes::from("myhash"),
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => assert_eq!(arr.len(), 2),
        other => panic!("Expected array, got {:?}", other),
    }

    // HVALS
    let response = executor
        .execute(
            Command::HVals {
                key: Bytes::from("myhash"),
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => assert_eq!(arr.len(), 2),
        other => panic!("Expected array, got {:?}", other),
    }
}

#[tokio::test]
async fn test_hincrby() {
    let executor = create_executor();

    executor
        .execute(
            Command::HSet {
                key: Bytes::from("myhash"),
                pairs: vec![(Bytes::from("counter"), Bytes::from("10"))],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::HIncrBy {
                key: Bytes::from("myhash"),
                field: Bytes::from("counter"),
                delta: 5,
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(15)));
}

#[tokio::test]
async fn test_hincrbyfloat() {
    let executor = create_executor();

    executor
        .execute(
            Command::HSet {
                key: Bytes::from("myhash"),
                pairs: vec![(Bytes::from("counter"), Bytes::from("10.5"))],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::HIncrByFloat {
                key: Bytes::from("myhash"),
                field: Bytes::from("counter"),
                delta: 0.1,
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => {
            let val: f64 = String::from_utf8_lossy(&data).parse().unwrap();
            assert!((val - 10.6).abs() < 0.001);
        }
        other => panic!("Expected bulk string, got {:?}", other),
    }
}

#[tokio::test]
async fn test_hmget() {
    let executor = create_executor();

    executor
        .execute(
            Command::HSet {
                key: Bytes::from("myhash"),
                pairs: vec![
                    (Bytes::from("f1"), Bytes::from("v1")),
                    (Bytes::from("f2"), Bytes::from("v2")),
                ],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::HMGet {
                key: Bytes::from("myhash"),
                fields: vec![Bytes::from("f1"), Bytes::from("f3"), Bytes::from("f2")],
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 3);
            assert!(matches!(&arr[0], Frame::Bulk(Some(d)) if d == &Bytes::from("v1")));
            assert!(matches!(&arr[1], Frame::Bulk(None)));
            assert!(matches!(&arr[2], Frame::Bulk(Some(d)) if d == &Bytes::from("v2")));
        }
        other => panic!("Expected array, got {:?}", other),
    }
}

#[tokio::test]
async fn test_hsetnx() {
    let executor = create_executor();

    // First set should succeed
    let response = executor
        .execute(
            Command::HSetNx {
                key: Bytes::from("myhash"),
                field: Bytes::from("field1"),
                value: Bytes::from("value1"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(1)));

    // Second set should fail (field exists)
    let response = executor
        .execute(
            Command::HSetNx {
                key: Bytes::from("myhash"),
                field: Bytes::from("field1"),
                value: Bytes::from("newvalue"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(0)));
}

#[tokio::test]
async fn test_hstrlen() {
    let executor = create_executor();

    executor
        .execute(
            Command::HSet {
                key: Bytes::from("myhash"),
                pairs: vec![(Bytes::from("field1"), Bytes::from("Hello"))],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::HStrLen {
                key: Bytes::from("myhash"),
                field: Bytes::from("field1"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(5)));
}

// ==================== LIST COMMAND TESTS ====================

#[tokio::test]
async fn test_llen() {
    let executor = create_executor();

    executor
        .execute(
            Command::RPush {
                key: Bytes::from("mylist"),
                values: vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::LLen {
                key: Bytes::from("mylist"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(3)));
}

#[tokio::test]
async fn test_lindex() {
    let executor = create_executor();

    executor
        .execute(
            Command::RPush {
                key: Bytes::from("mylist"),
                values: vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            },
            0,
        )
        .await;

    // Positive index
    let response = executor
        .execute(
            Command::LIndex {
                key: Bytes::from("mylist"),
                index: 0,
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("a")),
        other => panic!("Expected bulk string, got {:?}", other),
    }

    // Negative index
    let response = executor
        .execute(
            Command::LIndex {
                key: Bytes::from("mylist"),
                index: -1,
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("c")),
        other => panic!("Expected bulk string, got {:?}", other),
    }
}

#[tokio::test]
async fn test_lset() {
    let executor = create_executor();

    executor
        .execute(
            Command::RPush {
                key: Bytes::from("mylist"),
                values: vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::LSet {
                key: Bytes::from("mylist"),
                index: 1,
                value: Bytes::from("B"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));

    // Verify
    let response = executor
        .execute(
            Command::LIndex {
                key: Bytes::from("mylist"),
                index: 1,
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("B")),
        other => panic!("Expected bulk string, got {:?}", other),
    }
}

#[tokio::test]
async fn test_linsert() {
    let executor = create_executor();

    executor
        .execute(
            Command::RPush {
                key: Bytes::from("mylist"),
                values: vec![Bytes::from("a"), Bytes::from("c")],
            },
            0,
        )
        .await;

    // Insert BEFORE
    let response = executor
        .execute(
            Command::LInsert {
                key: Bytes::from("mylist"),
                before: true,
                pivot: Bytes::from("c"),
                value: Bytes::from("b"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(3)));

    // Verify order: a, b, c
    let response = executor
        .execute(
            Command::LRange {
                key: Bytes::from("mylist"),
                start: 0,
                stop: -1,
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 3);
        }
        other => panic!("Expected array, got {:?}", other),
    }
}

#[tokio::test]
async fn test_lpos() {
    let executor = create_executor();

    executor
        .execute(
            Command::RPush {
                key: Bytes::from("mylist"),
                values: vec![
                    Bytes::from("a"),
                    Bytes::from("b"),
                    Bytes::from("c"),
                    Bytes::from("b"),
                ],
            },
            0,
        )
        .await;

    // Find first occurrence
    let response = executor
        .execute(
            Command::LPos {
                key: Bytes::from("mylist"),
                element: Bytes::from("b"),
                rank: None,
                count: None,
                maxlen: None,
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(1)));
}

// ==================== SET COMMAND TESTS ====================

#[tokio::test]
async fn test_scard() {
    let executor = create_executor();

    executor
        .execute(
            Command::SAdd {
                key: Bytes::from("myset"),
                members: vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::SCard {
                key: Bytes::from("myset"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(3)));
}

#[tokio::test]
async fn test_srem() {
    let executor = create_executor();

    executor
        .execute(
            Command::SAdd {
                key: Bytes::from("myset"),
                members: vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::SRem {
                key: Bytes::from("myset"),
                members: vec![Bytes::from("b"), Bytes::from("nonexist")],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(1)));

    // Verify set size
    let response = executor
        .execute(
            Command::SCard {
                key: Bytes::from("myset"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(2)));
}

#[tokio::test]
async fn test_spop() {
    let executor = create_executor();

    executor
        .execute(
            Command::SAdd {
                key: Bytes::from("myset"),
                members: vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            },
            0,
        )
        .await;

    // Pop one element
    let response = executor
        .execute(
            Command::SPop {
                key: Bytes::from("myset"),
                count: None,
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Bulk(Some(_))));

    // Verify set size is reduced
    let response = executor
        .execute(
            Command::SCard {
                key: Bytes::from("myset"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(2)));
}

#[tokio::test]
async fn test_srandmember() {
    let executor = create_executor();

    executor
        .execute(
            Command::SAdd {
                key: Bytes::from("myset"),
                members: vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            },
            0,
        )
        .await;

    // Random member without count
    let response = executor
        .execute(
            Command::SRandMember {
                key: Bytes::from("myset"),
                count: None,
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Bulk(Some(_))));

    // Random members with count
    let response = executor
        .execute(
            Command::SRandMember {
                key: Bytes::from("myset"),
                count: Some(2),
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => assert_eq!(arr.len(), 2),
        other => panic!("Expected array, got {:?}", other),
    }
}

#[tokio::test]
async fn test_sdiff() {
    let executor = create_executor();

    executor
        .execute(
            Command::SAdd {
                key: Bytes::from("set1"),
                members: vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            },
            0,
        )
        .await;
    executor
        .execute(
            Command::SAdd {
                key: Bytes::from("set2"),
                members: vec![Bytes::from("b"), Bytes::from("c"), Bytes::from("d")],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::SDiff {
                keys: vec![Bytes::from("set1"), Bytes::from("set2")],
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => assert_eq!(arr.len(), 1), // Only "a"
        other => panic!("Expected array, got {:?}", other),
    }
}

#[tokio::test]
async fn test_sinter() {
    let executor = create_executor();

    executor
        .execute(
            Command::SAdd {
                key: Bytes::from("set1"),
                members: vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            },
            0,
        )
        .await;
    executor
        .execute(
            Command::SAdd {
                key: Bytes::from("set2"),
                members: vec![Bytes::from("b"), Bytes::from("c"), Bytes::from("d")],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::SInter {
                keys: vec![Bytes::from("set1"), Bytes::from("set2")],
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => assert_eq!(arr.len(), 2), // "b" and "c"
        other => panic!("Expected array, got {:?}", other),
    }
}

#[tokio::test]
async fn test_sunion() {
    let executor = create_executor();

    executor
        .execute(
            Command::SAdd {
                key: Bytes::from("set1"),
                members: vec![Bytes::from("a"), Bytes::from("b")],
            },
            0,
        )
        .await;
    executor
        .execute(
            Command::SAdd {
                key: Bytes::from("set2"),
                members: vec![Bytes::from("b"), Bytes::from("c")],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::SUnion {
                keys: vec![Bytes::from("set1"), Bytes::from("set2")],
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => assert_eq!(arr.len(), 3), // "a", "b", "c"
        other => panic!("Expected array, got {:?}", other),
    }
}

// ==================== SORTED SET COMMAND TESTS ====================

#[tokio::test]
async fn test_zcard() {
    let executor = create_executor();

    executor
        .execute(
            Command::ZAdd {
                key: Bytes::from("myzset"),
                options: Default::default(),
                pairs: vec![
                    (1.0, Bytes::from("a")),
                    (2.0, Bytes::from("b")),
                    (3.0, Bytes::from("c")),
                ],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::ZCard {
                key: Bytes::from("myzset"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(3)));
}

#[tokio::test]
async fn test_zcount() {
    let executor = create_executor();

    executor
        .execute(
            Command::ZAdd {
                key: Bytes::from("myzset"),
                options: Default::default(),
                pairs: vec![
                    (1.0, Bytes::from("a")),
                    (2.0, Bytes::from("b")),
                    (3.0, Bytes::from("c")),
                ],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::ZCount {
                key: Bytes::from("myzset"),
                min: 1.0,
                max: 2.0,
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(2)));
}

#[tokio::test]
async fn test_zrem() {
    let executor = create_executor();

    executor
        .execute(
            Command::ZAdd {
                key: Bytes::from("myzset"),
                options: Default::default(),
                pairs: vec![
                    (1.0, Bytes::from("a")),
                    (2.0, Bytes::from("b")),
                    (3.0, Bytes::from("c")),
                ],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::ZRem {
                key: Bytes::from("myzset"),
                members: vec![Bytes::from("b")],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(1)));

    let response = executor
        .execute(
            Command::ZCard {
                key: Bytes::from("myzset"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(2)));
}

#[tokio::test]
async fn test_zrank() {
    let executor = create_executor();

    executor
        .execute(
            Command::ZAdd {
                key: Bytes::from("myzset"),
                options: Default::default(),
                pairs: vec![
                    (1.0, Bytes::from("a")),
                    (2.0, Bytes::from("b")),
                    (3.0, Bytes::from("c")),
                ],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::ZRank {
                key: Bytes::from("myzset"),
                member: Bytes::from("b"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(1))); // 0-indexed

    // Non-existent member
    let response = executor
        .execute(
            Command::ZRank {
                key: Bytes::from("myzset"),
                member: Bytes::from("nonexist"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Bulk(None)));
}

#[tokio::test]
async fn test_zrevrank() {
    let executor = create_executor();

    executor
        .execute(
            Command::ZAdd {
                key: Bytes::from("myzset"),
                options: Default::default(),
                pairs: vec![
                    (1.0, Bytes::from("a")),
                    (2.0, Bytes::from("b")),
                    (3.0, Bytes::from("c")),
                ],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::ZRevRank {
                key: Bytes::from("myzset"),
                member: Bytes::from("b"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(1))); // 0-indexed from highest
}

#[tokio::test]
async fn test_zincrby() {
    let executor = create_executor();

    executor
        .execute(
            Command::ZAdd {
                key: Bytes::from("myzset"),
                options: Default::default(),
                pairs: vec![(1.0, Bytes::from("member"))],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::ZIncrBy {
                key: Bytes::from("myzset"),
                increment: 2.5,
                member: Bytes::from("member"),
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => {
            let score: f64 = String::from_utf8_lossy(&data).parse().unwrap();
            assert!((score - 3.5).abs() < 0.001);
        }
        other => panic!("Expected bulk string, got {:?}", other),
    }
}

// ==================== KEY COMMAND TESTS ====================

#[tokio::test]
async fn test_unlink() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("key1"),
                value: Bytes::from("value1"),
                options: Default::default(),
            },
            0,
        )
        .await;
    executor
        .execute(
            Command::Set {
                key: Bytes::from("key2"),
                value: Bytes::from("value2"),
                options: Default::default(),
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::Unlink {
                keys: vec![
                    Bytes::from("key1"),
                    Bytes::from("key2"),
                    Bytes::from("nonexist"),
                ],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(2)));
}

#[tokio::test]
async fn test_persist() {
    let executor = create_executor();

    executor
        .execute(
            Command::SetEx {
                key: Bytes::from("mykey"),
                seconds: 100,
                value: Bytes::from("value"),
            },
            0,
        )
        .await;

    // Verify it has TTL
    let response = executor
        .execute(
            Command::Ttl {
                key: Bytes::from("mykey"),
            },
            0,
        )
        .await;
    match response {
        Frame::Integer(ttl) => assert!(ttl > 0),
        other => panic!("Expected positive TTL, got {:?}", other),
    }

    // PERSIST removes TTL
    let response = executor
        .execute(
            Command::Persist {
                key: Bytes::from("mykey"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(1)));

    // Verify no TTL
    let response = executor
        .execute(
            Command::Ttl {
                key: Bytes::from("mykey"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(-1)));
}

#[tokio::test]
async fn test_pttl() {
    let executor = create_executor();

    executor
        .execute(
            Command::PSetEx {
                key: Bytes::from("mykey"),
                milliseconds: 5000,
                value: Bytes::from("value"),
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::PTtl {
                key: Bytes::from("mykey"),
            },
            0,
        )
        .await;
    match response {
        Frame::Integer(ttl) => assert!(ttl > 0 && ttl <= 5000),
        other => panic!("Expected integer, got {:?}", other),
    }
}

#[tokio::test]
async fn test_touch() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("key1"),
                value: Bytes::from("value1"),
                options: Default::default(),
            },
            0,
        )
        .await;
    executor
        .execute(
            Command::Set {
                key: Bytes::from("key2"),
                value: Bytes::from("value2"),
                options: Default::default(),
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::Touch {
                keys: vec![
                    Bytes::from("key1"),
                    Bytes::from("key2"),
                    Bytes::from("nonexist"),
                ],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(2)));
}

#[tokio::test]
async fn test_keys_pattern() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("hello"),
                value: Bytes::from("value"),
                options: Default::default(),
            },
            0,
        )
        .await;
    executor
        .execute(
            Command::Set {
                key: Bytes::from("hallo"),
                value: Bytes::from("value"),
                options: Default::default(),
            },
            0,
        )
        .await;
    executor
        .execute(
            Command::Set {
                key: Bytes::from("world"),
                value: Bytes::from("value"),
                options: Default::default(),
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::Keys {
                pattern: "h*llo".to_string(),
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => assert_eq!(arr.len(), 2),
        other => panic!("Expected array, got {:?}", other),
    }
}

#[tokio::test]
async fn test_scan() {
    let executor = create_executor();

    // Add some keys
    for i in 0..10 {
        executor
            .execute(
                Command::Set {
                    key: Bytes::from(format!("key{}", i)),
                    value: Bytes::from("value"),
                    options: Default::default(),
                },
                0,
            )
            .await;
    }

    let response = executor
        .execute(
            Command::Scan {
                cursor: 0,
                pattern: None,
                count: Some(5),
                type_filter: None,
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 2); // cursor and keys array
        }
        other => panic!("Expected array, got {:?}", other),
    }
}

// ==================== GEO COMMAND TESTS ====================

#[tokio::test]
async fn test_geoadd() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::GeoAdd {
                key: Bytes::from("mygeo"),
                items: vec![
                    (-122.419, 37.775, Bytes::from("San Francisco")),
                    (-74.006, 40.7128, Bytes::from("New York")),
                ],
                nx: false,
                xx: false,
                ch: false,
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(2)));
}

#[tokio::test]
async fn test_geodist() {
    let executor = create_executor();

    executor
        .execute(
            Command::GeoAdd {
                key: Bytes::from("mygeo"),
                items: vec![
                    (-122.419, 37.775, Bytes::from("San Francisco")),
                    (-74.006, 40.7128, Bytes::from("New York")),
                ],
                nx: false,
                xx: false,
                ch: false,
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::GeoDist {
                key: Bytes::from("mygeo"),
                member1: Bytes::from("San Francisco"),
                member2: Bytes::from("New York"),
                unit: "km".to_string(),
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => {
            let dist: f64 = String::from_utf8_lossy(&data).parse().unwrap();
            // Distance should be around 4000km
            assert!(dist > 3000.0 && dist < 5000.0);
        }
        other => panic!("Expected bulk string with distance, got {:?}", other),
    }
}

#[tokio::test]
async fn test_geopos() {
    let executor = create_executor();

    executor
        .execute(
            Command::GeoAdd {
                key: Bytes::from("mygeo"),
                items: vec![(-122.419, 37.775, Bytes::from("San Francisco"))],
                nx: false,
                xx: false,
                ch: false,
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::GeoPos {
                key: Bytes::from("mygeo"),
                members: vec![Bytes::from("San Francisco")],
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 1);
        }
        other => panic!("Expected array, got {:?}", other),
    }
}

#[tokio::test]
async fn test_geohash() {
    let executor = create_executor();

    executor
        .execute(
            Command::GeoAdd {
                key: Bytes::from("mygeo"),
                items: vec![(-122.419, 37.775, Bytes::from("San Francisco"))],
                nx: false,
                xx: false,
                ch: false,
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::GeoHash {
                key: Bytes::from("mygeo"),
                members: vec![Bytes::from("San Francisco")],
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 1);
            assert!(matches!(&arr[0], Frame::Bulk(Some(_))));
        }
        other => panic!("Expected array, got {:?}", other),
    }
}

// ==================== HYPERLOGLOG COMMAND TESTS ====================

#[tokio::test]
async fn test_pfadd() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::PfAdd {
                key: Bytes::from("hll"),
                elements: vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(1))); // Modified

    // Adding same elements should return 0
    let response = executor
        .execute(
            Command::PfAdd {
                key: Bytes::from("hll"),
                elements: vec![Bytes::from("a"), Bytes::from("b")],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(0))); // Not modified
}

#[tokio::test]
async fn test_pfcount() {
    let executor = create_executor();

    executor
        .execute(
            Command::PfAdd {
                key: Bytes::from("hll"),
                elements: vec![
                    Bytes::from("a"),
                    Bytes::from("b"),
                    Bytes::from("c"),
                    Bytes::from("d"),
                    Bytes::from("e"),
                ],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::PfCount {
                keys: vec![Bytes::from("hll")],
            },
            0,
        )
        .await;
    match response {
        Frame::Integer(count) => {
            assert!((4..=6).contains(&count));
        }
        other => panic!("Expected integer, got {:?}", other),
    }
}

#[tokio::test]
async fn test_pfmerge() {
    let executor = create_executor();

    executor
        .execute(
            Command::PfAdd {
                key: Bytes::from("hll1"),
                elements: vec![Bytes::from("a"), Bytes::from("b")],
            },
            0,
        )
        .await;
    executor
        .execute(
            Command::PfAdd {
                key: Bytes::from("hll2"),
                elements: vec![Bytes::from("c"), Bytes::from("d")],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::PfMerge {
                destkey: Bytes::from("hll_merged"),
                sourcekeys: vec![Bytes::from("hll1"), Bytes::from("hll2")],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));

    let response = executor
        .execute(
            Command::PfCount {
                keys: vec![Bytes::from("hll_merged")],
            },
            0,
        )
        .await;
    match response {
        Frame::Integer(count) => assert!((3..=5).contains(&count)),
        other => panic!("Expected integer, got {:?}", other),
    }
}

// ==================== STREAM COMMAND TESTS ====================

#[tokio::test]
async fn test_xadd() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::XAdd {
                key: Bytes::from("mystream"),
                id: Some("*".to_string()),
                fields: vec![
                    (Bytes::from("field1"), Bytes::from("value1")),
                    (Bytes::from("field2"), Bytes::from("value2")),
                ],
                maxlen: None,
                minid: None,
                nomkstream: false,
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(id)) => {
            // Auto-generated ID contains timestamp
            let id_str = String::from_utf8_lossy(&id);
            assert!(id_str.contains("-"));
        }
        other => panic!("Expected bulk string with stream ID, got {:?}", other),
    }
}

#[tokio::test]
async fn test_xlen() {
    let executor = create_executor();

    // Add entries
    for _ in 0..3 {
        executor
            .execute(
                Command::XAdd {
                    key: Bytes::from("mystream"),
                    id: Some("*".to_string()),
                    fields: vec![(Bytes::from("f"), Bytes::from("v"))],
                    maxlen: None,
                    minid: None,
                    nomkstream: false,
                },
                0,
            )
            .await;
    }

    let response = executor
        .execute(
            Command::XLen {
                key: Bytes::from("mystream"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(3)));
}

#[tokio::test]
async fn test_xrange() {
    let executor = create_executor();

    // Add entries
    for i in 0..3 {
        executor
            .execute(
                Command::XAdd {
                    key: Bytes::from("mystream"),
                    id: Some("*".to_string()),
                    fields: vec![(Bytes::from("num"), Bytes::from(i.to_string()))],
                    maxlen: None,
                    minid: None,
                    nomkstream: false,
                },
                0,
            )
            .await;
    }

    let response = executor
        .execute(
            Command::XRange {
                key: Bytes::from("mystream"),
                start: "-".to_string(),
                end: "+".to_string(),
                count: None,
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => assert_eq!(arr.len(), 3),
        other => panic!("Expected array, got {:?}", other),
    }
}

// ==================== SERVER COMMAND TESTS ====================

#[tokio::test]
async fn test_flushdb() {
    let executor = create_executor();

    // FLUSHDB returns OK (current implementation is a stub)
    let response = executor.execute(Command::FlushDb { r#async: false }, 0).await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));
}

#[tokio::test]
async fn test_flushall() {
    let executor = create_executor();

    // FLUSHALL returns OK (current implementation is a stub)
    let response = executor.execute(Command::FlushAll { r#async: false }, 0).await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));
}

#[tokio::test]
async fn test_object_encoding() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("mykey"),
                value: Bytes::from("hello"),
                options: Default::default(),
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::Object {
                subcommand: "ENCODING".to_string(),
                key: Some(Bytes::from("mykey")),
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(_)) => {} // Should return encoding type
        other => panic!("Expected bulk string with encoding, got {:?}", other),
    }
}

#[tokio::test]
async fn test_copy() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("source"),
                value: Bytes::from("value"),
                options: Default::default(),
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::Copy {
                source: Bytes::from("source"),
                destination: Bytes::from("dest"),
                dest_db: None,
                replace: false,
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(1)));

    // Verify destination exists
    let response = executor
        .execute(
            Command::Get {
                key: Bytes::from("dest"),
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("value")),
        other => panic!("Expected bulk string, got {:?}", other),
    }
}

#[tokio::test]
async fn test_renamenx() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("oldkey"),
                value: Bytes::from("value"),
                options: Default::default(),
            },
            0,
        )
        .await;

    // RENAMENX to new key should succeed
    let response = executor
        .execute(
            Command::RenameNx {
                key: Bytes::from("oldkey"),
                new_key: Bytes::from("newkey"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(1)));

    // Create another key
    executor
        .execute(
            Command::Set {
                key: Bytes::from("anotherkey"),
                value: Bytes::from("value2"),
                options: Default::default(),
            },
            0,
        )
        .await;

    // RENAMENX should fail if dest exists
    let response = executor
        .execute(
            Command::RenameNx {
                key: Bytes::from("newkey"),
                new_key: Bytes::from("anotherkey"),
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(0)));
}

#[tokio::test]
async fn test_expireat() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("mykey"),
                value: Bytes::from("value"),
                options: Default::default(),
            },
            0,
        )
        .await;

    // Set expire time to future timestamp
    let future_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600; // 1 hour from now

    let response = executor
        .execute(
            Command::ExpireAt {
                key: Bytes::from("mykey"),
                timestamp: future_time,
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Integer(1)));

    // Verify TTL is set
    let response = executor
        .execute(
            Command::Ttl {
                key: Bytes::from("mykey"),
            },
            0,
        )
        .await;
    match response {
        Frame::Integer(ttl) => assert!(ttl > 0),
        other => panic!("Expected positive TTL, got {:?}", other),
    }
}

// ========================================================================
// Additional Comprehensive Tests
// ========================================================================

// RESP3 Protocol Tests

#[tokio::test]
async fn test_hello_resp2_response() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Hello {
                protocol_version: Some(2),
                auth: None,
                client_name: None,
            },
            0,
        )
        .await;

    // HELLO with RESP2 should return array
    match response {
        Frame::Array(_) => {} // Expected
        other => panic!("Expected array response for HELLO 2, got {:?}", other),
    }
}

#[tokio::test]
async fn test_hello_resp3_response() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Hello {
                protocol_version: Some(3),
                auth: None,
                client_name: None,
            },
            0,
        )
        .await;

    // HELLO with RESP3 should return map
    match response {
        Frame::Map(_) => {} // Expected
        other => panic!("Expected map response for HELLO 3, got {:?}", other),
    }
}

#[tokio::test]
async fn test_hello_invalid_protocol_version() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Hello {
                protocol_version: Some(4), // Invalid protocol version
                auth: None,
                client_name: None,
            },
            0,
        )
        .await;

    match response {
        Frame::Error(msg) => {
            let err_str = String::from_utf8_lossy(&msg);
            assert!(err_str.contains("NOPROTO") || err_str.contains("unsupported"));
        }
        other => panic!("Expected error for invalid protocol version, got {:?}", other),
    }
}

// Blocking Command Tests

#[tokio::test]
async fn test_blpop_returns_immediately_when_list_has_elements() {
    let executor = create_executor();

    // Push a value first
    executor
        .execute(
            Command::LPush {
                key: Bytes::from("mylist"),
                values: vec![Bytes::from("value1")],
            },
            0,
        )
        .await;

    // BLPOP should return immediately when list has elements
    let response = executor
        .execute(
            Command::BLPop {
                keys: vec![Bytes::from("mylist")],
                timeout: 1.0,
            },
            0,
        )
        .await;

    match response {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 2);
        }
        other => panic!("Expected array response, got {:?}", other),
    }
}

#[tokio::test]
async fn test_blpop_timeout_when_list_empty() {
    let executor = create_executor();

    // BLPOP on empty list with very short timeout
    let start = Instant::now();
    let response = executor
        .execute(
            Command::BLPop {
                keys: vec![Bytes::from("nonexistent")],
                timeout: 0.1, // 100ms timeout
            },
            0,
        )
        .await;

    let elapsed = start.elapsed();

    // Should timeout and return null
    match response {
        Frame::Null | Frame::Bulk(None) | Frame::Array(None) => {
            // Verify it actually waited
            assert!(elapsed >= Duration::from_millis(90));
        }
        other => panic!("Expected null response after timeout, got {:?}", other),
    }
}

#[tokio::test]
async fn test_brpop_returns_immediately_when_list_has_elements() {
    let executor = create_executor();

    executor
        .execute(
            Command::RPush {
                key: Bytes::from("mylist"),
                values: vec![Bytes::from("value1")],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::BRPop {
                keys: vec![Bytes::from("mylist")],
                timeout: 1.0,
            },
            0,
        )
        .await;

    match response {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 2);
        }
        other => panic!("Expected array response, got {:?}", other),
    }
}

#[tokio::test]
async fn test_blpop_checks_multiple_keys() {
    let executor = create_executor();

    // Push to second list
    executor
        .execute(
            Command::LPush {
                key: Bytes::from("list2"),
                values: vec![Bytes::from("value2")],
            },
            0,
        )
        .await;

    // BLPOP should check all keys and return from first available
    let response = executor
        .execute(
            Command::BLPop {
                keys: vec![Bytes::from("list1"), Bytes::from("list2")],
                timeout: 1.0,
            },
            0,
        )
        .await;

    match response {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 2);
        }
        other => panic!("Expected array response, got {:?}", other),
    }
}

#[tokio::test]
async fn test_brpoplpush_moves_element() {
    let executor = create_executor();

    executor
        .execute(
            Command::RPush {
                key: Bytes::from("source"),
                values: vec![Bytes::from("value1")],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::BRPopLPush {
                source: Bytes::from("source"),
                destination: Bytes::from("dest"),
                timeout: 1.0,
            },
            0,
        )
        .await;

    match response {
        Frame::Bulk(Some(data)) => {
            assert_eq!(data, Bytes::from("value1"));
        }
        other => panic!("Expected bulk string response, got {:?}", other),
    }

    // Verify value was moved to destination
    let dest_response = executor
        .execute(
            Command::LLen {
                key: Bytes::from("dest"),
            },
            0,
        )
        .await;
    assert!(matches!(dest_response, Frame::Integer(1)));
}

#[tokio::test]
async fn test_blmove_with_directions() {
    let executor = create_executor();

    executor
        .execute(
            Command::RPush {
                key: Bytes::from("source"),
                values: vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::BLMove {
                source: Bytes::from("source"),
                destination: Bytes::from("dest"),
                wherefrom: crate::commands::parser::ListDirection::Right,
                whereto: crate::commands::parser::ListDirection::Left,
                timeout: 1.0,
            },
            0,
        )
        .await;

    match response {
        Frame::Bulk(Some(data)) => {
            assert_eq!(data, Bytes::from("c"));
        }
        other => panic!("Expected bulk string response, got {:?}", other),
    }
}

#[tokio::test]
async fn test_blmpop_pops_from_list() {
    let executor = create_executor();

    executor
        .execute(
            Command::LPush {
                key: Bytes::from("list1"),
                values: vec![Bytes::from("a"), Bytes::from("b")],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::BLMPop {
                timeout: 1.0,
                keys: vec![Bytes::from("list1")],
                direction: crate::commands::parser::ListDirection::Left,
                count: 1,
            },
            0,
        )
        .await;

    // Should return array with key and popped elements
    match response {
        Frame::Array(Some(_)) => {} // Expected
        other => panic!("Expected array response for BLMPOP, got {:?}", other),
    }
}

#[tokio::test]
async fn test_bzpopmin_pops_minimum() {
    let executor = create_executor();

    // Add sorted set members
    executor
        .execute(
            Command::ZAdd {
                key: Bytes::from("myzset"),
                options: Default::default(),
                pairs: vec![(1.0, Bytes::from("one")), (2.0, Bytes::from("two"))],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::BZPopMin {
                keys: vec![Bytes::from("myzset")],
                timeout: 1.0,
            },
            0,
        )
        .await;

    match response {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 3); // [key, member, score]
        }
        other => panic!("Expected array response for BZPOPMIN, got {:?}", other),
    }
}

#[tokio::test]
async fn test_bzpopmax_pops_maximum() {
    let executor = create_executor();

    executor
        .execute(
            Command::ZAdd {
                key: Bytes::from("myzset"),
                options: Default::default(),
                pairs: vec![(1.0, Bytes::from("one")), (2.0, Bytes::from("two"))],
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::BZPopMax {
                keys: vec![Bytes::from("myzset")],
                timeout: 1.0,
            },
            0,
        )
        .await;

    match response {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 3); // [key, member, score]
        }
        other => panic!("Expected array response for BZPOPMAX, got {:?}", other),
    }
}

// Transaction Command Tests

#[tokio::test]
async fn test_multi_returns_connection_level_error() {
    let executor = create_executor();

    // MULTI should return error indicating it's handled at connection level
    let response = executor.execute(Command::Multi, 0).await;
    match response {
        Frame::Error(msg) => {
            let err_str = String::from_utf8_lossy(&msg);
            assert!(err_str.contains("connection level"));
        }
        other => panic!("Expected error for MULTI, got {:?}", other),
    }
}

#[tokio::test]
async fn test_exec_returns_connection_level_error() {
    let executor = create_executor();

    // EXEC should return error indicating it's handled at connection level
    let response = executor.execute(Command::Exec, 0).await;
    match response {
        Frame::Error(msg) => {
            let err_str = String::from_utf8_lossy(&msg);
            assert!(err_str.contains("connection level"));
        }
        other => panic!("Expected error for EXEC, got {:?}", other),
    }
}

#[tokio::test]
async fn test_discard_returns_connection_level_error() {
    let executor = create_executor();

    // DISCARD should return error indicating it's handled at connection level
    let response = executor.execute(Command::Discard, 0).await;
    match response {
        Frame::Error(msg) => {
            let err_str = String::from_utf8_lossy(&msg);
            assert!(err_str.contains("connection level"));
        }
        other => panic!("Expected error for DISCARD, got {:?}", other),
    }
}

#[tokio::test]
async fn test_watch_returns_connection_level_error() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Watch {
                keys: vec![Bytes::from("key1"), Bytes::from("key2")],
            },
            0,
        )
        .await;

    match response {
        Frame::Error(msg) => {
            let err_str = String::from_utf8_lossy(&msg);
            assert!(err_str.contains("connection level"));
        }
        other => panic!("Expected error for WATCH, got {:?}", other),
    }
}

#[tokio::test]
async fn test_unwatch_returns_connection_level_error() {
    let executor = create_executor();

    let response = executor.execute(Command::Unwatch, 0).await;

    match response {
        Frame::Error(msg) => {
            let err_str = String::from_utf8_lossy(&msg);
            assert!(err_str.contains("connection level"));
        }
        other => panic!("Expected error for UNWATCH, got {:?}", other),
    }
}

// Error Handling Tests

#[tokio::test]
async fn test_wrongtype_error_list_command_on_string() {
    let executor = create_executor();

    // Set a string value
    executor
        .execute(
            Command::Set {
                key: Bytes::from("mykey"),
                value: Bytes::from("stringvalue"),
                options: Default::default(),
            },
            0,
        )
        .await;

    // Try to use list command on string
    let response = executor
        .execute(
            Command::LPush {
                key: Bytes::from("mykey"),
                values: vec![Bytes::from("value")],
            },
            0,
        )
        .await;

    match response {
        Frame::Error(msg) => {
            let err_str = String::from_utf8_lossy(&msg);
            assert!(err_str.contains("WRONGTYPE") || err_str.contains("wrong"));
        }
        other => panic!("Expected WRONGTYPE error, got {:?}", other),
    }
}

#[tokio::test]
async fn test_wrongtype_error_hash_command_on_string() {
    let executor = create_executor();

    // Set a string value
    executor
        .execute(
            Command::Set {
                key: Bytes::from("mykey"),
                value: Bytes::from("stringvalue"),
                options: Default::default(),
            },
            0,
        )
        .await;

    // Try to use hash command on string
    let response = executor
        .execute(
            Command::HSet {
                key: Bytes::from("mykey"),
                pairs: vec![(Bytes::from("field"), Bytes::from("value"))],
            },
            0,
        )
        .await;

    match response {
        Frame::Error(msg) => {
            let err_str = String::from_utf8_lossy(&msg);
            assert!(err_str.contains("WRONGTYPE") || err_str.contains("wrong"));
        }
        other => panic!("Expected WRONGTYPE error, got {:?}", other),
    }
}

#[tokio::test]
async fn test_wrongtype_error_set_command_on_list() {
    let executor = create_executor();

    // Create a list
    executor
        .execute(
            Command::LPush {
                key: Bytes::from("mykey"),
                values: vec![Bytes::from("value")],
            },
            0,
        )
        .await;

    // Try to use set command on list
    let response = executor
        .execute(
            Command::SAdd {
                key: Bytes::from("mykey"),
                members: vec![Bytes::from("member")],
            },
            0,
        )
        .await;

    match response {
        Frame::Error(msg) => {
            let err_str = String::from_utf8_lossy(&msg);
            assert!(err_str.contains("WRONGTYPE") || err_str.contains("wrong"));
        }
        other => panic!("Expected WRONGTYPE error, got {:?}", other),
    }
}

#[tokio::test]
async fn test_incr_integer_overflow() {
    let executor = create_executor();

    // Set to max i64
    executor
        .execute(
            Command::Set {
                key: Bytes::from("counter"),
                value: Bytes::from(i64::MAX.to_string()),
                options: Default::default(),
            },
            0,
        )
        .await;

    // Try to increment
    let response = executor
        .execute(
            Command::Incr {
                key: Bytes::from("counter"),
            },
            0,
        )
        .await;

    match response {
        Frame::Error(msg) => {
            let err_str = String::from_utf8_lossy(&msg);
            assert!(err_str.contains("overflow") || err_str.contains("out of range"));
        }
        other => panic!("Expected overflow error, got {:?}", other),
    }
}

#[tokio::test]
async fn test_lindex_returns_null_for_out_of_range() {
    let executor = create_executor();

    executor
        .execute(
            Command::LPush {
                key: Bytes::from("mylist"),
                values: vec![Bytes::from("a"), Bytes::from("b")],
            },
            0,
        )
        .await;

    // Access index way out of range
    let response = executor
        .execute(
            Command::LIndex {
                key: Bytes::from("mylist"),
                index: 100,
            },
            0,
        )
        .await;

    assert!(matches!(response, Frame::Bulk(None)));
}

#[tokio::test]
async fn test_lset_returns_error_for_out_of_range() {
    let executor = create_executor();

    executor
        .execute(
            Command::LPush {
                key: Bytes::from("mylist"),
                values: vec![Bytes::from("a")],
            },
            0,
        )
        .await;

    // Try to set index out of range
    let response = executor
        .execute(
            Command::LSet {
                key: Bytes::from("mylist"),
                index: 10,
                value: Bytes::from("new"),
            },
            0,
        )
        .await;

    match response {
        Frame::Error(msg) => {
            let err_str = String::from_utf8_lossy(&msg);
            assert!(err_str.contains("index out of range") || err_str.contains("out of range"));
        }
        other => panic!("Expected out of range error, got {:?}", other),
    }
}

// Additional Command Variant Tests

#[tokio::test]
async fn test_getex_sets_expiration() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("mykey"),
                value: Bytes::from("myvalue"),
                options: Default::default(),
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::GetEx {
                key: Bytes::from("mykey"),
                ex: Some(60),
                px: None,
                exat: None,
                pxat: None,
                persist: false,
            },
            0,
        )
        .await;

    match response {
        Frame::Bulk(Some(data)) => {
            assert_eq!(data, Bytes::from("myvalue"));
        }
        other => panic!("Expected bulk string, got {:?}", other),
    }

    // Verify TTL was set
    let ttl_response = executor
        .execute(
            Command::Ttl {
                key: Bytes::from("mykey"),
            },
            0,
        )
        .await;

    match ttl_response {
        Frame::Integer(ttl) => {
            assert!(ttl > 0 && ttl <= 60);
        }
        other => panic!("Expected positive TTL, got {:?}", other),
    }
}

#[tokio::test]
async fn test_getex_removes_expiration_with_persist() {
    let executor = create_executor();

    // Set with expiration
    executor
        .execute(
            Command::SetEx {
                key: Bytes::from("mykey"),
                seconds: 100,
                value: Bytes::from("myvalue"),
            },
            0,
        )
        .await;

    // Get and persist
    let response = executor
        .execute(
            Command::GetEx {
                key: Bytes::from("mykey"),
                ex: None,
                px: None,
                exat: None,
                pxat: None,
                persist: true,
            },
            0,
        )
        .await;

    match response {
        Frame::Bulk(Some(_)) => {}
        other => panic!("Expected bulk string, got {:?}", other),
    }

    // Verify TTL was removed
    let ttl_response = executor
        .execute(
            Command::Ttl {
                key: Bytes::from("mykey"),
            },
            0,
        )
        .await;

    assert!(matches!(ttl_response, Frame::Integer(-1)));
}

#[tokio::test]
async fn test_lcs_returns_longest_common_substring() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("key1"),
                value: Bytes::from("ohmytext"),
                options: Default::default(),
            },
            0,
        )
        .await;

    executor
        .execute(
            Command::Set {
                key: Bytes::from("key2"),
                value: Bytes::from("mynewtext"),
                options: Default::default(),
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::Lcs {
                key1: Bytes::from("key1"),
                key2: Bytes::from("key2"),
                len_only: false,
                idx: false,
                min_match_len: 0,
                with_match_len: false,
            },
            0,
        )
        .await;

    match response {
        Frame::Bulk(Some(_)) => {} // Expected LCS string
        other => panic!("Expected bulk string for LCS, got {:?}", other),
    }
}

#[tokio::test]
async fn test_lcs_len_only_returns_integer() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("key1"),
                value: Bytes::from("ohmytext"),
                options: Default::default(),
            },
            0,
        )
        .await;

    executor
        .execute(
            Command::Set {
                key: Bytes::from("key2"),
                value: Bytes::from("mynewtext"),
                options: Default::default(),
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::Lcs {
                key1: Bytes::from("key1"),
                key2: Bytes::from("key2"),
                len_only: true,
                idx: false,
                min_match_len: 0,
                with_match_len: false,
            },
            0,
        )
        .await;

    match response {
        Frame::Integer(len) => {
            assert!(len >= 0);
        }
        other => panic!("Expected integer for LCS LEN, got {:?}", other),
    }
}

#[tokio::test]
async fn test_msetnx_succeeds_when_all_keys_new() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::MSetNx {
                pairs: vec![
                    (Bytes::from("key1"), Bytes::from("val1")),
                    (Bytes::from("key2"), Bytes::from("val2")),
                ],
            },
            0,
        )
        .await;

    // Should succeed since keys don't exist
    assert!(matches!(response, Frame::Integer(1)));

    // Verify keys were set
    let get_response = executor
        .execute(
            Command::Get {
                key: Bytes::from("key1"),
            },
            0,
        )
        .await;

    match get_response {
        Frame::Bulk(Some(data)) => {
            assert_eq!(data, Bytes::from("val1"));
        }
        other => panic!("Expected value, got {:?}", other),
    }
}

#[tokio::test]
async fn test_msetnx_fails_when_one_key_exists() {
    let executor = create_executor();

    // Set one key first
    executor
        .execute(
            Command::Set {
                key: Bytes::from("key1"),
                value: Bytes::from("existing"),
                options: Default::default(),
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::MSetNx {
                pairs: vec![
                    (Bytes::from("key1"), Bytes::from("val1")),
                    (Bytes::from("key2"), Bytes::from("val2")),
                ],
            },
            0,
        )
        .await;

    // Should fail since key1 exists
    assert!(matches!(response, Frame::Integer(0)));

    // Verify key1 was not changed
    let get_response = executor
        .execute(
            Command::Get {
                key: Bytes::from("key1"),
            },
            0,
        )
        .await;

    match get_response {
        Frame::Bulk(Some(data)) => {
            assert_eq!(data, Bytes::from("existing"));
        }
        other => panic!("Expected existing value, got {:?}", other),
    }
}

#[tokio::test]
async fn test_setrange_modifies_string_at_offset() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("mykey"),
                value: Bytes::from("Hello World"),
                options: Default::default(),
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::SetRange {
                key: Bytes::from("mykey"),
                offset: 6,
                value: Bytes::from("Redis"),
            },
            0,
        )
        .await;

    match response {
        Frame::Integer(len) => {
            assert!(len > 0);
        }
        other => panic!("Expected integer, got {:?}", other),
    }

    // Verify the change
    let get_response = executor
        .execute(
            Command::Get {
                key: Bytes::from("mykey"),
            },
            0,
        )
        .await;

    match get_response {
        Frame::Bulk(Some(data)) => {
            assert_eq!(data, Bytes::from("Hello Redis"));
        }
        other => panic!("Expected modified value, got {:?}", other),
    }
}

#[tokio::test]
async fn test_getrange_with_positive_indices() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("mykey"),
                value: Bytes::from("This is a string"),
                options: Default::default(),
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::GetRange {
                key: Bytes::from("mykey"),
                start: 0,
                end: 3,
            },
            0,
        )
        .await;

    match response {
        Frame::Bulk(Some(data)) => {
            assert_eq!(data, Bytes::from("This"));
        }
        other => panic!("Expected bulk string, got {:?}", other),
    }
}

#[tokio::test]
async fn test_getrange_with_negative_indices() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("mykey"),
                value: Bytes::from("This is a string"),
                options: Default::default(),
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::GetRange {
                key: Bytes::from("mykey"),
                start: -3,
                end: -1,
            },
            0,
        )
        .await;

    match response {
        Frame::Bulk(Some(data)) => {
            assert_eq!(data, Bytes::from("ing"));
        }
        other => panic!("Expected bulk string, got {:?}", other),
    }
}

#[tokio::test]
async fn test_getset_atomically_replaces_value() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("mykey"),
                value: Bytes::from("old"),
                options: Default::default(),
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::GetSet {
                key: Bytes::from("mykey"),
                value: Bytes::from("new"),
            },
            0,
        )
        .await;

    match response {
        Frame::Bulk(Some(data)) => {
            assert_eq!(data, Bytes::from("old"));
        }
        other => panic!("Expected old value, got {:?}", other),
    }

    // Verify new value
    let get_response = executor
        .execute(
            Command::Get {
                key: Bytes::from("mykey"),
            },
            0,
        )
        .await;

    match get_response {
        Frame::Bulk(Some(data)) => {
            assert_eq!(data, Bytes::from("new"));
        }
        other => panic!("Expected new value, got {:?}", other),
    }
}

#[tokio::test]
async fn test_incrbyfloat_with_positive_delta() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("mykey"),
                value: Bytes::from("10.5"),
                options: Default::default(),
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::IncrByFloat {
                key: Bytes::from("mykey"),
                delta: 0.1,
            },
            0,
        )
        .await;

    match response {
        Frame::Bulk(Some(_)) => {} // Expected string representation of float
        other => panic!("Expected bulk string for INCRBYFLOAT, got {:?}", other),
    }
}

#[tokio::test]
async fn test_incrbyfloat_with_negative_delta() {
    let executor = create_executor();

    executor
        .execute(
            Command::Set {
                key: Bytes::from("mykey"),
                value: Bytes::from("10.5"),
                options: Default::default(),
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::IncrByFloat {
                key: Bytes::from("mykey"),
                delta: -5.5,
            },
            0,
        )
        .await;

    match response {
        Frame::Bulk(Some(_)) => {} // Expected
        other => panic!("Expected bulk string, got {:?}", other),
    }
}

#[tokio::test]
async fn test_config_resetstat() {
    let executor = create_executor();

    // CONFIG RESETSTAT should return OK
    let response = executor
        .execute(
            Command::Config {
                subcommand: "RESETSTAT".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    assert!(
        matches!(response, Frame::Simple(s) if s == "OK"),
        "Expected OK for CONFIG RESETSTAT"
    );
}

#[tokio::test]
async fn test_config_unknown_subcommand() {
    let executor = create_executor();

    // CONFIG with unknown subcommand should error
    let response = executor
        .execute(
            Command::Config {
                subcommand: "UNKNOWNSUBCMD".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("Unknown CONFIG subcommand"),
                "Expected unknown subcommand error"
            );
        }
        other => panic!("Expected error for unknown CONFIG subcommand, got {:?}", other),
    }
}

#[tokio::test]
async fn test_ping_pong() {
    let executor = create_executor();

    // PING without argument
    let response = executor.execute(Command::Ping { message: None }, 0).await;
    assert!(matches!(response, Frame::Simple(s) if s == "PONG"));

    // PING with argument
    let response = executor
        .execute(
            Command::Ping {
                message: Some(Bytes::from("hello")),
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("hello")),
        other => panic!("Expected bulk string, got {:?}", other),
    }
}

#[tokio::test]
async fn test_rename_nonexistent_key_fails() {
    let executor = create_executor();

    // Try to rename non-existent key
    let response = executor
        .execute(
            Command::Rename {
                key: Bytes::from("nonexistent"),
                new_key: Bytes::from("newkey"),
            },
            0,
        )
        .await;
    match response {
        Frame::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(err_str.contains("no such key"));
        }
        other => panic!("Expected error for renaming non-existent key, got {:?}", other),
    }
}

#[tokio::test]
async fn test_time() {
    let executor = create_executor();

    let response = executor.execute(Command::Time, 0).await;
    match response {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 2);
            // First element: Unix timestamp in seconds
            assert!(matches!(&arr[0], Frame::Bulk(Some(_))));
            // Second element: Microseconds
            assert!(matches!(&arr[1], Frame::Bulk(Some(_))));
        }
        other => panic!("Expected array for TIME, got {:?}", other),
    }
}

#[tokio::test]
async fn test_client_info() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Client {
                subcommand: "INFO".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    // CLIENT INFO should return a bulk string with client info
    match response {
        Frame::Bulk(Some(_)) => {} // Expected
        other => panic!("Expected bulk string for CLIENT INFO, got {:?}", other),
    }
}

#[tokio::test]
async fn test_debug_sleep() {
    let executor = create_executor();

    let start = std::time::Instant::now();
    let response = executor
        .execute(
            Command::Debug {
                subcommand: "SLEEP".to_string(),
                args: vec![Bytes::from("0.01")], // 10ms
            },
            0,
        )
        .await;
    let elapsed = start.elapsed();

    match response {
        Frame::Simple(ref s) if s.as_ref() == b"OK" => {}
        other => panic!("Expected Frame::Simple(OK), got {:?}", other),
    }
    assert!(
        elapsed.as_millis() >= 10,
        "DEBUG SLEEP should have slept at least 10ms"
    );
}

// ── Redis Cluster protocol compatibility tests ────────────────────────────

/// Helper: create an executor with a ClusterStateManager attached.
fn create_cluster_executor() -> CommandExecutor {
    use crate::cluster::ClusterStateManager;
    let store = Arc::new(Store::new(16));
    let pubsub_manager = Arc::new(SubscriptionManager::new());
    let acl = Arc::new(Acl::new());
    let blocking_manager = Arc::new(BlockingListManager::new());
    let blocking_stream_manager = Arc::new(blocking::BlockingStreamManager::new());
    let blocking_zset_manager = Arc::new(blocking::BlockingSortedSetManager::new());

    let addr: std::net::SocketAddr = "127.0.0.1:6379".parse().unwrap();
    let csm = Arc::new(ClusterStateManager::new(addr));

    let mut executor = CommandExecutor::new(
        store,
        pubsub_manager,
        acl,
        blocking_manager,
        blocking_stream_manager,
        blocking_zset_manager,
    );
    executor.set_cluster_state(csm);
    executor
}

#[tokio::test]
async fn test_cluster_info_with_state_manager() {
    let executor = create_cluster_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "INFO".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => {
            let info = String::from_utf8_lossy(&data);
            // With ClusterStateManager, cluster_enabled should be 1
            assert!(info.contains("cluster_enabled:1"), "info: {}", info);
            assert!(info.contains("cluster_state:ok"), "info: {}", info);
            assert!(info.contains("cluster_known_nodes:1"), "info: {}", info);
        }
        other => panic!("Expected bulk string for CLUSTER INFO, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_nodes_with_state_manager() {
    let executor = create_cluster_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "NODES".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => {
            let info = String::from_utf8_lossy(&data);
            // Should contain the "myself" flag and "master" role
            assert!(info.contains("myself"), "nodes: {}", info);
            assert!(info.contains("master"), "nodes: {}", info);
        }
        other => panic!("Expected bulk string for CLUSTER NODES, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_myid_with_state_manager() {
    let executor = create_cluster_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "MYID".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => {
            // ClusterStateManager generates a random 40-char hex ID
            assert_eq!(data.len(), 40, "Node ID should be 40 characters, got {}", data.len());
        }
        other => panic!("Expected bulk string for CLUSTER MYID, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_addslots_with_state_manager() {
    let executor = create_cluster_executor();

    // Add some slots
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "ADDSLOTS".to_string(),
                args: vec!["0".to_string(), "1".to_string(), "2".to_string()],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));

    // Verify via CLUSTER INFO that slots are now assigned
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "INFO".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => {
            let info = String::from_utf8_lossy(&data);
            assert!(info.contains("cluster_slots_assigned:3"), "info: {}", info);
        }
        other => panic!("Expected bulk string for CLUSTER INFO, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_delslots_with_state_manager() {
    let executor = create_cluster_executor();

    // Add slots first
    executor
        .execute(
            Command::Cluster {
                subcommand: "ADDSLOTS".to_string(),
                args: vec!["10".to_string(), "11".to_string(), "12".to_string()],
            },
            0,
        )
        .await;

    // Delete one slot
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "DELSLOTS".to_string(),
                args: vec!["11".to_string()],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));

    // Verify via CLUSTER INFO that only 2 slots remain
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "INFO".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => {
            let info = String::from_utf8_lossy(&data);
            assert!(info.contains("cluster_slots_assigned:2"), "info: {}", info);
        }
        other => panic!("Expected bulk string, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_setslot_with_state_manager() {
    let executor = create_cluster_executor();

    // Add a slot first so the MIGRATING / IMPORTING tests have context
    executor
        .execute(
            Command::Cluster {
                subcommand: "ADDSLOTS".to_string(),
                args: vec!["500".to_string()],
            },
            0,
        )
        .await;

    // SETSLOT MIGRATING
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "SETSLOT".to_string(),
                args: vec![
                    "500".to_string(),
                    "MIGRATING".to_string(),
                    "target_node_id_00000000000000000000000".to_string(),
                ],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));

    // SETSLOT STABLE (cancel migration)
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "SETSLOT".to_string(),
                args: vec!["500".to_string(), "STABLE".to_string()],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));

    // SETSLOT IMPORTING
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "SETSLOT".to_string(),
                args: vec![
                    "500".to_string(),
                    "IMPORTING".to_string(),
                    "source_node_id_00000000000000000000000".to_string(),
                ],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));

    // SETSLOT NODE
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "SETSLOT".to_string(),
                args: vec![
                    "500".to_string(),
                    "NODE".to_string(),
                    "target_node_id_00000000000000000000000".to_string(),
                ],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));
}

#[tokio::test]
async fn test_cluster_forget_standalone() {
    let executor = create_executor();

    // In standalone mode, FORGET should return an error
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "FORGET".to_string(),
                args: vec!["some_node_id_0000000000000000000000000000".to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Error(_) => {}
        other => panic!("Expected error for FORGET in standalone mode, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_forget_with_state_manager() {
    let executor = create_cluster_executor();

    // Cannot forget ourselves
    let csm = executor.cluster_state_manager().unwrap();
    let my_id = csm.my_id().to_string();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "FORGET".to_string(),
                args: vec![my_id],
            },
            0,
        )
        .await;
    match response {
        Frame::Error(data) => {
            let msg = String::from_utf8_lossy(&data);
            assert!(msg.contains("can't forget myself"), "error: {}", msg);
        }
        other => panic!("Expected error when trying to forget self, got {:?}", other),
    }

    // Forget a non-existent node should error
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "FORGET".to_string(),
                args: vec!["nonexistent000000000000000000000000000000".to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Error(_) => {}
        other => panic!("Expected error for unknown node ID, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_forget_missing_args() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "FORGET".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Error(_) => {}
        other => panic!("Expected error for missing FORGET args, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_set_config_epoch() {
    let executor = create_cluster_executor();

    // Set a valid epoch
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "SET-CONFIG-EPOCH".to_string(),
                args: vec!["42".to_string()],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));
}

#[tokio::test]
async fn test_cluster_set_config_epoch_zero() {
    let executor = create_cluster_executor();

    // Epoch 0 is invalid
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "SET-CONFIG-EPOCH".to_string(),
                args: vec!["0".to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Error(_) => {}
        other => panic!("Expected error for epoch 0, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_set_config_epoch_missing_args() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "SET-CONFIG-EPOCH".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Error(_) => {}
        other => panic!("Expected error for missing args, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_countkeysinslot_with_real_keys() {
    let executor = create_executor();

    // Insert a key, then count keys in its slot
    let key = Bytes::from("testkey");
    executor
        .execute(
            Command::Set {
                key: key.clone(),
                value: Bytes::from("value"),
                options: Default::default(),
            },
            0,
        )
        .await;

    // Get the slot for this key
    let slot = crate::cluster::HashSlot::for_key(&key);

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "COUNTKEYSINSLOT".to_string(),
                args: vec![slot.to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Integer(count) => {
            assert!(count >= 1, "Expected at least 1 key in slot {}, got {}", slot, count);
        }
        other => panic!("Expected integer for COUNTKEYSINSLOT, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_getkeysinslot_with_real_keys() {
    let executor = create_executor();

    // Insert a key
    let key = Bytes::from("slotkey");
    executor
        .execute(
            Command::Set {
                key: key.clone(),
                value: Bytes::from("val"),
                options: Default::default(),
            },
            0,
        )
        .await;

    let slot = crate::cluster::HashSlot::for_key(&key);

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "GETKEYSINSLOT".to_string(),
                args: vec![slot.to_string(), "100".to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => {
            assert!(!arr.is_empty(), "Expected at least 1 key in slot {}", slot);
            // The returned key should match our key
            match &arr[0] {
                Frame::Bulk(Some(k)) => {
                    assert_eq!(k.as_ref(), key.as_ref(), "Returned key mismatch");
                }
                other => panic!("Expected bulk string in GETKEYSINSLOT result, got {:?}", other),
            }
        }
        other => panic!("Expected array for GETKEYSINSLOT, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_countkeysinslot_empty() {
    let executor = create_executor();

    // Slot 0 should have 0 keys in a fresh store
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "COUNTKEYSINSLOT".to_string(),
                args: vec!["0".to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Integer(0) => {}
        Frame::Integer(n) => panic!("Expected 0 keys in empty store, got {}", n),
        other => panic!("Expected integer, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_countkeysinslot_invalid() {
    let executor = create_executor();

    // Out of range slot
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "COUNTKEYSINSLOT".to_string(),
                args: vec!["16384".to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Error(_) => {}
        other => panic!("Expected error for out-of-range slot, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_getkeysinslot_empty() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "GETKEYSINSLOT".to_string(),
                args: vec!["0".to_string(), "10".to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => {
            assert!(arr.is_empty(), "Expected empty array for empty store");
        }
        other => panic!("Expected array for GETKEYSINSLOT, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_keyslot_known_values() {
    let executor = create_executor();

    // Known CRC16 test vectors
    let test_cases = vec![
        ("foo", 12182),
        ("bar", 5061),
        ("hello", 866),
    ];

    for (key, expected_slot) in test_cases {
        let response = executor
            .execute(
                Command::Cluster {
                    subcommand: "KEYSLOT".to_string(),
                    args: vec![key.to_string()],
                },
                0,
            )
            .await;
        match response {
            Frame::Integer(slot) => {
                assert_eq!(
                    slot, expected_slot,
                    "KEYSLOT for '{}': expected {}, got {}",
                    key, expected_slot, slot
                );
            }
            other => panic!("Expected integer for CLUSTER KEYSLOT, got {:?}", other),
        }
    }
}

#[tokio::test]
async fn test_cluster_keyslot_hash_tags() {
    let executor = create_executor();

    // Keys with same hash tag must produce the same slot
    let response1 = executor
        .execute(
            Command::Cluster {
                subcommand: "KEYSLOT".to_string(),
                args: vec!["{user}:1000".to_string()],
            },
            0,
        )
        .await;
    let slot1 = match response1 {
        Frame::Integer(s) => s,
        other => panic!("Expected integer, got {:?}", other),
    };

    let response2 = executor
        .execute(
            Command::Cluster {
                subcommand: "KEYSLOT".to_string(),
                args: vec!["{user}:2000".to_string()],
            },
            0,
        )
        .await;
    let slot2 = match response2 {
        Frame::Integer(s) => s,
        other => panic!("Expected integer, got {:?}", other),
    };

    let response3 = executor
        .execute(
            Command::Cluster {
                subcommand: "KEYSLOT".to_string(),
                args: vec!["{user}:profile".to_string()],
            },
            0,
        )
        .await;
    let slot3 = match response3 {
        Frame::Integer(s) => s,
        other => panic!("Expected integer, got {:?}", other),
    };

    assert_eq!(slot1, slot2, "Same hash tag should produce same slot");
    assert_eq!(slot2, slot3, "Same hash tag should produce same slot");
}

#[tokio::test]
async fn test_cluster_keyslot_empty_hash_tag() {
    let executor = create_executor();

    // Empty hash tag (e.g. "{}key") should use the full key
    let response1 = executor
        .execute(
            Command::Cluster {
                subcommand: "KEYSLOT".to_string(),
                args: vec!["{}key".to_string()],
            },
            0,
        )
        .await;
    let slot1 = match response1 {
        Frame::Integer(s) => s,
        other => panic!("Expected integer, got {:?}", other),
    };

    let response2 = executor
        .execute(
            Command::Cluster {
                subcommand: "KEYSLOT".to_string(),
                args: vec!["{}key".to_string()],
            },
            0,
        )
        .await;
    let slot2 = match response2 {
        Frame::Integer(s) => s,
        other => panic!("Expected integer, got {:?}", other),
    };

    assert_eq!(slot1, slot2, "Deterministic hashing");
}

#[tokio::test]
async fn test_cluster_keyslot_no_args() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "KEYSLOT".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Error(_) => {}
        other => panic!("Expected error for KEYSLOT with no args, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_meet_with_state_manager() {
    let executor = create_cluster_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "MEET".to_string(),
                args: vec!["127.0.0.1".to_string(), "6380".to_string()],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));

    // After MEET, CLUSTER INFO should show 2 known nodes
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "INFO".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => {
            let info = String::from_utf8_lossy(&data);
            assert!(info.contains("cluster_known_nodes:2"), "info: {}", info);
        }
        other => panic!("Expected bulk string, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_meet_invalid_ip() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "MEET".to_string(),
                args: vec!["not_an_ip".to_string(), "6380".to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Error(_) => {}
        other => panic!("Expected error for invalid IP, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_meet_invalid_port() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "MEET".to_string(),
                args: vec!["127.0.0.1".to_string(), "not_a_port".to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Error(_) => {}
        other => panic!("Expected error for invalid port, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_meet_missing_args() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "MEET".to_string(),
                args: vec!["127.0.0.1".to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Error(_) => {}
        other => panic!("Expected error for missing port argument, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_shards_standalone() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "SHARDS".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 1, "Expected 1 shard in standalone mode");
        }
        other => panic!("Expected array for CLUSTER SHARDS, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_shards_with_state_manager() {
    let executor = create_cluster_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "SHARDS".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => {
            assert!(!arr.is_empty(), "Expected at least 1 shard");
        }
        other => panic!("Expected array for CLUSTER SHARDS, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_failover_force() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "FAILOVER".to_string(),
                args: vec!["FORCE".to_string()],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));
}

#[tokio::test]
async fn test_cluster_failover_takeover() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "FAILOVER".to_string(),
                args: vec!["TAKEOVER".to_string()],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));
}

#[tokio::test]
async fn test_cluster_failover_invalid_option() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "FAILOVER".to_string(),
                args: vec!["INVALID".to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Error(_) => {}
        other => panic!("Expected error for invalid FAILOVER option, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_replicate_missing_args() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "REPLICATE".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Error(_) => {}
        other => panic!("Expected error for missing REPLICATE args, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_reset_hard() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "RESET".to_string(),
                args: vec!["HARD".to_string()],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));
}

#[tokio::test]
async fn test_cluster_flushslots() {
    let executor = create_cluster_executor();

    // Add some slots first
    executor
        .execute(
            Command::Cluster {
                subcommand: "ADDSLOTS".to_string(),
                args: vec!["0".to_string(), "1".to_string()],
            },
            0,
        )
        .await;

    // Flush all slots
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "FLUSHSLOTS".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));

    // Verify 0 slots assigned
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "INFO".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Bulk(Some(data)) => {
            let info = String::from_utf8_lossy(&data);
            assert!(info.contains("cluster_slots_assigned:0"), "info: {}", info);
        }
        other => panic!("Expected bulk string, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_saveconfig() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "SAVECONFIG".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));
}

#[tokio::test]
async fn test_cluster_setslot_missing_args() {
    let executor = create_executor();

    // Too few args
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "SETSLOT".to_string(),
                args: vec!["0".to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Error(_) => {}
        other => panic!("Expected error for too few SETSLOT args, got {:?}", other),
    }

    // IMPORTING without node ID
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "SETSLOT".to_string(),
                args: vec!["0".to_string(), "IMPORTING".to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Error(_) => {}
        other => panic!("Expected error for IMPORTING without node ID, got {:?}", other),
    }

    // MIGRATING without node ID
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "SETSLOT".to_string(),
                args: vec!["0".to_string(), "MIGRATING".to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Error(_) => {}
        other => panic!("Expected error for MIGRATING without node ID, got {:?}", other),
    }

    // NODE without node ID
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "SETSLOT".to_string(),
                args: vec!["0".to_string(), "NODE".to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Error(_) => {}
        other => panic!("Expected error for NODE without node ID, got {:?}", other),
    }

    // Invalid action
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "SETSLOT".to_string(),
                args: vec!["0".to_string(), "INVALID".to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Error(_) => {}
        other => panic!("Expected error for invalid SETSLOT action, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_slots_with_state_manager_and_assigned_slots() {
    let executor = create_cluster_executor();

    // Add all slots to ourselves
    let all_slots: Vec<String> = (0u16..16384).map(|s| s.to_string()).collect();
    // Add in batches to avoid a massive command (use ADDSLOTS with a range)
    executor
        .execute(
            Command::Cluster {
                subcommand: "ADDSLOTS".to_string(),
                args: all_slots,
            },
            0,
        )
        .await;

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "SLOTS".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => {
            // Should have at least one slot range entry
            assert!(!arr.is_empty(), "Expected non-empty SLOTS response");
        }
        other => panic!("Expected array for CLUSTER SLOTS, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_addslots_invalid_slot() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "ADDSLOTS".to_string(),
                args: vec!["16384".to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Error(_) => {}
        other => panic!("Expected error for slot 16384, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_addslots_missing_args() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "ADDSLOTS".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Error(_) => {}
        other => panic!("Expected error for empty ADDSLOTS, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_getkeysinslot_missing_args() {
    let executor = create_executor();

    // Missing count argument
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "GETKEYSINSLOT".to_string(),
                args: vec!["0".to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Error(_) => {}
        other => panic!("Expected error for missing count, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_getkeysinslot_invalid_slot() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "GETKEYSINSLOT".to_string(),
                args: vec!["99999".to_string(), "10".to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Error(_) => {}
        other => panic!("Expected error for out-of-range slot, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_getkeysinslot_count_limit() {
    let executor = create_executor();

    // Insert multiple keys that hash to the same slot using hash tags
    for i in 0..5 {
        executor
            .execute(
                Command::Set {
                    key: Bytes::from(format!("{{sameSlot}}:key{}", i)),
                    value: Bytes::from("val"),
                    options: Default::default(),
                },
                0,
            )
            .await;
    }

    let slot = crate::cluster::HashSlot::for_key(b"{sameSlot}:key0");

    // Request only 2 keys
    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "GETKEYSINSLOT".to_string(),
                args: vec![slot.to_string(), "2".to_string()],
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 2, "Expected exactly 2 keys, got {}", arr.len());
        }
        other => panic!("Expected array for GETKEYSINSLOT, got {:?}", other),
    }
}

#[tokio::test]
async fn test_cluster_help_contents() {
    let executor = create_executor();

    let response = executor
        .execute(
            Command::Cluster {
                subcommand: "HELP".to_string(),
                args: vec![],
            },
            0,
        )
        .await;
    match response {
        Frame::Array(Some(arr)) => {
            // Should contain entries for all subcommands
            let help_text: Vec<String> = arr
                .iter()
                .filter_map(|f| match f {
                    Frame::Bulk(Some(data)) => {
                        Some(String::from_utf8_lossy(data).to_string())
                    }
                    _ => None,
                })
                .collect();

            // Check that important subcommands are documented
            let has = |needle: &str| help_text.iter().any(|s| s.contains(needle));
            assert!(has("INFO"), "HELP should mention INFO");
            assert!(has("NODES"), "HELP should mention NODES");
            assert!(has("SLOTS"), "HELP should mention SLOTS");
            assert!(has("SHARDS"), "HELP should mention SHARDS");
            assert!(has("KEYSLOT"), "HELP should mention KEYSLOT");
            assert!(has("MYID"), "HELP should mention MYID");
            assert!(has("FORGET"), "HELP should mention FORGET");
            assert!(has("ADDSLOTS"), "HELP should mention ADDSLOTS");
            assert!(has("DELSLOTS"), "HELP should mention DELSLOTS");
            assert!(has("SETSLOT"), "HELP should mention SETSLOT");
            assert!(has("FAILOVER"), "HELP should mention FAILOVER");
            assert!(has("SET-CONFIG-EPOCH"), "HELP should mention SET-CONFIG-EPOCH");
        }
        other => panic!("Expected array for CLUSTER HELP, got {:?}", other),
    }
}

// ── Redis compatibility: additional executor tests ────────────────────

#[tokio::test]
async fn test_randomkey_empty() {
    let executor = create_executor();
    let response = executor.execute(Command::RandomKey, 0).await;
    assert!(matches!(response, Frame::Null));
}

#[tokio::test]
async fn test_randomkey_with_keys() {
    let executor = create_executor();
    executor.execute(
        Command::Set { key: Bytes::from("a"), value: Bytes::from("1"), options: Default::default() }, 0
    ).await;
    executor.execute(
        Command::Set { key: Bytes::from("b"), value: Bytes::from("2"), options: Default::default() }, 0
    ).await;

    let response = executor.execute(Command::RandomKey, 0).await;
    match response {
        Frame::Bulk(Some(key)) => {
            assert!(key == Bytes::from_static(b"a") || key == Bytes::from_static(b"b"));
        }
        other => panic!("Expected bulk string from RANDOMKEY, got {:?}", other),
    }
}

#[tokio::test]
async fn test_object_refcount() {
    let executor = create_executor();
    executor.execute(
        Command::Set { key: Bytes::from("k"), value: Bytes::from("v"), options: Default::default() }, 0
    ).await;

    let response = executor.execute(
        Command::Object { subcommand: "REFCOUNT".to_string(), key: Some(Bytes::from("k")) }, 0
    ).await;
    assert!(matches!(response, Frame::Integer(1)));
}

#[tokio::test]
async fn test_object_refcount_missing_key() {
    let executor = create_executor();
    let response = executor.execute(
        Command::Object { subcommand: "REFCOUNT".to_string(), key: Some(Bytes::from("missing")) }, 0
    ).await;
    assert!(matches!(response, Frame::Null));
}

#[tokio::test]
async fn test_object_idletime() {
    let executor = create_executor();
    executor.execute(
        Command::Set { key: Bytes::from("k"), value: Bytes::from("v"), options: Default::default() }, 0
    ).await;

    let response = executor.execute(
        Command::Object { subcommand: "IDLETIME".to_string(), key: Some(Bytes::from("k")) }, 0
    ).await;
    // Idle time should be 0 or a small number right after creation
    match response {
        Frame::Integer(n) => assert!(n >= 0),
        Frame::Null => {} // acceptable if store doesn't track idletime
        other => panic!("Expected integer or null from OBJECT IDLETIME, got {:?}", other),
    }
}

#[tokio::test]
async fn test_object_freq() {
    let executor = create_executor();
    executor.execute(
        Command::Set { key: Bytes::from("k"), value: Bytes::from("v"), options: Default::default() }, 0
    ).await;

    let response = executor.execute(
        Command::Object { subcommand: "FREQ".to_string(), key: Some(Bytes::from("k")) }, 0
    ).await;
    match response {
        Frame::Integer(n) => assert!(n >= 0),
        Frame::Null => {} // acceptable if store doesn't track freq
        other => panic!("Expected integer or null from OBJECT FREQ, got {:?}", other),
    }
}

#[tokio::test]
async fn test_object_help() {
    let executor = create_executor();
    let response = executor.execute(
        Command::Object { subcommand: "HELP".to_string(), key: None }, 0
    ).await;
    match response {
        Frame::Array(Some(arr)) => {
            assert!(!arr.is_empty(), "OBJECT HELP should return help text");
        }
        other => panic!("Expected array from OBJECT HELP, got {:?}", other),
    }
}

#[tokio::test]
async fn test_object_encoding_int() {
    let executor = create_executor();
    executor.execute(
        Command::Set { key: Bytes::from("intkey"), value: Bytes::from("12345"), options: Default::default() }, 0
    ).await;

    let response = executor.execute(
        Command::Object { subcommand: "ENCODING".to_string(), key: Some(Bytes::from("intkey")) }, 0
    ).await;
    match response {
        Frame::Bulk(Some(enc)) => assert_eq!(enc, Bytes::from("int")),
        other => panic!("Expected 'int' encoding, got {:?}", other),
    }
}

#[tokio::test]
async fn test_object_encoding_embstr() {
    let executor = create_executor();
    executor.execute(
        Command::Set { key: Bytes::from("strkey"), value: Bytes::from("hello"), options: Default::default() }, 0
    ).await;

    let response = executor.execute(
        Command::Object { subcommand: "ENCODING".to_string(), key: Some(Bytes::from("strkey")) }, 0
    ).await;
    match response {
        Frame::Bulk(Some(enc)) => assert_eq!(enc, Bytes::from("embstr")),
        other => panic!("Expected 'embstr' encoding, got {:?}", other),
    }
}

#[tokio::test]
async fn test_object_encoding_missing() {
    let executor = create_executor();
    let response = executor.execute(
        Command::Object { subcommand: "ENCODING".to_string(), key: Some(Bytes::from("nope")) }, 0
    ).await;
    assert!(matches!(response, Frame::Null));
}

#[tokio::test]
async fn test_object_unknown_subcommand() {
    let executor = create_executor();
    let response = executor.execute(
        Command::Object { subcommand: "NOPE".to_string(), key: Some(Bytes::from("k")) }, 0
    ).await;
    match response {
        Frame::Error(msg) => {
            let s = String::from_utf8_lossy(&msg);
            assert!(s.contains("Unknown subcommand"));
        }
        other => panic!("Expected error for unknown OBJECT subcommand, got {:?}", other),
    }
}

#[tokio::test]
async fn test_scan_with_pattern() {
    let executor = create_executor();
    for i in 0..5 {
        executor.execute(
            Command::Set {
                key: Bytes::from(format!("user:{}", i)),
                value: Bytes::from("v"),
                options: Default::default(),
            }, 0
        ).await;
    }
    executor.execute(
        Command::Set { key: Bytes::from("other"), value: Bytes::from("v"), options: Default::default() }, 0
    ).await;

    // Full iteration collecting all matched keys
    let mut all_keys = Vec::new();
    let mut cursor = 0u64;
    loop {
        let response = executor.execute(
            Command::Scan {
                cursor,
                pattern: Some("user:*".to_string()),
                count: Some(100),
                type_filter: None,
            }, 0
        ).await;
        match response {
            Frame::Array(Some(arr)) => {
                if let Frame::Bulk(Some(c)) = &arr[0] {
                    cursor = String::from_utf8_lossy(c).parse().unwrap_or(0);
                }
                if let Frame::Array(Some(keys)) = &arr[1] {
                    for k in keys {
                        if let Frame::Bulk(Some(data)) = k {
                            all_keys.push(String::from_utf8_lossy(data).to_string());
                        }
                    }
                }
            }
            other => panic!("Expected array from SCAN, got {:?}", other),
        }
        if cursor == 0 { break; }
    }
    assert_eq!(all_keys.len(), 5, "SCAN MATCH user:* should find 5 keys");
}

#[tokio::test]
async fn test_scan_with_type_filter() {
    let executor = create_executor();
    executor.execute(
        Command::Set { key: Bytes::from("str1"), value: Bytes::from("v"), options: Default::default() }, 0
    ).await;
    executor.execute(Command::LPush { key: Bytes::from("list1"), values: vec![Bytes::from("a")] }, 0).await;

    let mut all_keys = Vec::new();
    let mut cursor = 0u64;
    loop {
        let response = executor.execute(
            Command::Scan { cursor, pattern: None, count: Some(100), type_filter: Some("string".to_string()) }, 0
        ).await;
        match response {
            Frame::Array(Some(arr)) => {
                if let Frame::Bulk(Some(c)) = &arr[0] {
                    cursor = String::from_utf8_lossy(c).parse().unwrap_or(0);
                }
                if let Frame::Array(Some(keys)) = &arr[1] {
                    for k in keys {
                        if let Frame::Bulk(Some(data)) = k {
                            all_keys.push(String::from_utf8_lossy(data).to_string());
                        }
                    }
                }
            }
            other => panic!("Expected array from SCAN, got {:?}", other),
        }
        if cursor == 0 { break; }
    }
    assert_eq!(all_keys.len(), 1, "SCAN TYPE string should find 1 key");
    assert_eq!(all_keys[0], "str1");
}

#[tokio::test]
async fn test_flushdb_clears_data() {
    let executor = create_executor();
    executor.execute(
        Command::Set { key: Bytes::from("a"), value: Bytes::from("1"), options: Default::default() }, 0
    ).await;
    executor.execute(
        Command::Set { key: Bytes::from("b"), value: Bytes::from("2"), options: Default::default() }, 0
    ).await;

    let response = executor.execute(Command::DbSize, 0).await;
    assert!(matches!(response, Frame::Integer(2)));

    let response = executor.execute(Command::FlushDb { r#async: false }, 0).await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));

    let response = executor.execute(Command::DbSize, 0).await;
    assert!(matches!(response, Frame::Integer(0)));
}

#[tokio::test]
async fn test_flushall_clears_all_dbs() {
    let executor = create_executor();
    executor.execute(
        Command::Set { key: Bytes::from("a"), value: Bytes::from("1"), options: Default::default() }, 0
    ).await;
    executor.execute(
        Command::Set { key: Bytes::from("b"), value: Bytes::from("2"), options: Default::default() }, 1
    ).await;

    let response = executor.execute(Command::FlushAll { r#async: false }, 0).await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));

    let response = executor.execute(Command::DbSize, 0).await;
    assert!(matches!(response, Frame::Integer(0)));
    let response = executor.execute(Command::DbSize, 1).await;
    assert!(matches!(response, Frame::Integer(0)));
}

#[tokio::test]
async fn test_flushdb_async_option() {
    let executor = create_executor();
    executor.execute(
        Command::Set { key: Bytes::from("x"), value: Bytes::from("y"), options: Default::default() }, 0
    ).await;

    let response = executor.execute(Command::FlushDb { r#async: true }, 0).await;
    assert!(matches!(response, Frame::Simple(s) if s == "OK"));
}

#[tokio::test]
async fn test_wait_standalone() {
    let executor = create_executor();
    let response = executor.execute(
        Command::Wait { numreplicas: 1, timeout: 100 }, 0
    ).await;
    assert!(matches!(response, Frame::Integer(0)), "WAIT should return 0 in standalone mode");
}

#[tokio::test]
async fn test_wait_zero_replicas() {
    let executor = create_executor();
    let response = executor.execute(
        Command::Wait { numreplicas: 0, timeout: 0 }, 0
    ).await;
    assert!(matches!(response, Frame::Integer(0)));
}

#[tokio::test]
async fn test_command_count() {
    let executor = create_executor();
    let response = executor.execute(
        Command::CommandCmd { subcommand: Some("COUNT".to_string()), args: vec![] }, 0
    ).await;
    match response {
        Frame::Integer(n) => assert!(n > 50, "COMMAND COUNT should report many commands, got {}", n),
        other => panic!("Expected integer from COMMAND COUNT, got {:?}", other),
    }
}

#[tokio::test]
async fn test_command_info_known() {
    let executor = create_executor();
    let response = executor.execute(
        Command::CommandCmd { subcommand: Some("INFO".to_string()), args: vec![Bytes::from("get")] }, 0
    ).await;
    match response {
        Frame::Array(Some(arr)) => assert!(!arr.is_empty(), "COMMAND INFO get should return info"),
        other => panic!("Expected array from COMMAND INFO, got {:?}", other),
    }
}

#[tokio::test]
async fn test_command_list() {
    let executor = create_executor();
    let response = executor.execute(
        Command::CommandCmd { subcommand: Some("LIST".to_string()), args: vec![] }, 0
    ).await;
    match response {
        Frame::Array(Some(arr)) => assert!(arr.len() > 50, "COMMAND LIST should list many commands"),
        other => panic!("Expected array from COMMAND LIST, got {:?}", other),
    }
}

#[tokio::test]
async fn test_command_docs_specific() {
    let executor = create_executor();
    let response = executor.execute(
        Command::CommandCmd { subcommand: Some("DOCS".to_string()), args: vec![Bytes::from("get")] }, 0
    ).await;
    match response {
        Frame::Array(Some(arr)) => {
            // Should have at least command name + doc map
            assert!(arr.len() >= 2, "COMMAND DOCS get should return docs");
        }
        other => panic!("Expected array from COMMAND DOCS, got {:?}", other),
    }
}

#[tokio::test]
async fn test_command_docs_all() {
    let executor = create_executor();
    let response = executor.execute(
        Command::CommandCmd { subcommand: Some("DOCS".to_string()), args: vec![] }, 0
    ).await;
    match response {
        Frame::Array(Some(arr)) => {
            assert!(!arr.is_empty(), "COMMAND DOCS should return docs for all commands");
        }
        other => panic!("Expected array from COMMAND DOCS (all), got {:?}", other),
    }
}

#[tokio::test]
async fn test_command_getkeys() {
    let executor = create_executor();
    let response = executor.execute(
        Command::CommandCmd {
            subcommand: Some("GETKEYS".to_string()),
            args: vec![Bytes::from("GET"), Bytes::from("mykey")],
        }, 0
    ).await;
    match response {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 1, "COMMAND GETKEYS for GET should return 1 key");
        }
        other => panic!("Expected array from COMMAND GETKEYS, got {:?}", other),
    }
}

#[tokio::test]
async fn test_command_help() {
    let executor = create_executor();
    let response = executor.execute(
        Command::CommandCmd { subcommand: Some("HELP".to_string()), args: vec![] }, 0
    ).await;
    match response {
        Frame::Array(Some(arr)) => {
            assert!(!arr.is_empty(), "COMMAND HELP should return help text");
        }
        other => panic!("Expected array from COMMAND HELP, got {:?}", other),
    }
}

#[tokio::test]
async fn test_command_no_subcommand() {
    let executor = create_executor();
    let response = executor.execute(
        Command::CommandCmd { subcommand: None, args: vec![] }, 0
    ).await;
    match response {
        Frame::Array(Some(arr)) => {
            assert!(!arr.is_empty(), "COMMAND with no subcommand should return all command info");
        }
        other => panic!("Expected array from COMMAND, got {:?}", other),
    }
}

#[tokio::test]
async fn test_debug_segfault_returns_error() {
    let executor = create_executor();
    let response = executor.execute(
        Command::Debug { subcommand: "SEGFAULT".to_string(), args: vec![] }, 0
    ).await;
    match response {
        Frame::Error(msg) => {
            let s = String::from_utf8_lossy(&msg);
            assert!(s.contains("not supported"));
        }
        other => panic!("Expected error from DEBUG SEGFAULT, got {:?}", other),
    }
}

#[tokio::test]
async fn test_debug_object() {
    let executor = create_executor();
    let response = executor.execute(
        Command::Debug { subcommand: "OBJECT".to_string(), args: vec![Bytes::from("mykey")] }, 0
    ).await;
    match response {
        Frame::Bulk(Some(data)) => {
            let s = String::from_utf8_lossy(&data);
            assert!(s.contains("encoding"), "DEBUG OBJECT should include encoding info");
        }
        other => panic!("Expected bulk string from DEBUG OBJECT, got {:?}", other),
    }
}

#[tokio::test]
async fn test_dbsize_multiple_databases() {
    let executor = create_executor();

    executor.execute(
        Command::Set { key: Bytes::from("a"), value: Bytes::from("1"), options: Default::default() }, 0
    ).await;
    executor.execute(
        Command::Set { key: Bytes::from("b"), value: Bytes::from("2"), options: Default::default() }, 1
    ).await;
    executor.execute(
        Command::Set { key: Bytes::from("c"), value: Bytes::from("3"), options: Default::default() }, 1
    ).await;

    let r0 = executor.execute(Command::DbSize, 0).await;
    assert!(matches!(r0, Frame::Integer(1)));

    let r1 = executor.execute(Command::DbSize, 1).await;
    assert!(matches!(r1, Frame::Integer(2)));
}
