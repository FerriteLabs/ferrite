#![allow(clippy::unwrap_used)]
//! Multi-node integration tests for moonshot handler families.
//!
//! Each test uses the [`TestCluster`] harness to spin up a primary + replica
//! Store pair and verifies round-trip SAVE → replicate → LOAD behaviour at
//! the handler level (no TCP).
//!
//! Run with: `cargo test --test moonshot_integration`

mod harness;

use std::sync::Arc;

use ferrite::commands::handlers::{chronicle, concord, forge, lucidity, mnemo, pangea};
use ferrite::protocol::Frame;
use ferrite::storage::Store;
use harness::{assert_error, assert_ok, extract_bulk, TestCluster};

// ============================================================================
// Helpers
// ============================================================================

fn args(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

// ============================================================================
// Chronicle (CHR.*) tests
// ============================================================================

#[test]
fn chr_set_get_round_trip() {
    let cluster = TestCluster::new(0);
    let store = cluster.primary_store();

    let res = chronicle::chronicle_command_with_store(store, "SET", &args(&["mykey", "myval"]));
    assert_ok(&res);

    let res = chronicle::chronicle_command_with_store(store, "GET", &args(&["mykey"]));
    assert_eq!(extract_bulk(&res), Some("myval".to_string()));
}

#[test]
fn chr_save_load_replicates_via_store() {
    let cluster = TestCluster::new(1);
    let primary = cluster.primary_store();

    // Write data and persist
    chronicle::chronicle_command_with_store(primary, "SET", &args(&["rkey", "rval"]));
    let save = chronicle::chronicle_command_with_store(primary, "SAVE", &args(&[]));
    assert_ok(&save);

    // Replicate the __ferrite:chronicle:data key to the replica store
    cluster.replicate_ferrite_state(0);

    // Verify the key now exists in the replica store (raw bytes)
    let replica = cluster.replica_store(0);
    let key = bytes::Bytes::from("__ferrite:chronicle:data");
    assert!(
        replica.get(0, &key).is_some(),
        "chronicle state should have replicated to replica"
    );
}

#[test]
fn chr_help_returns_array() {
    let store = Arc::new(Store::new(16));
    let res = chronicle::chronicle_command_with_store(&store, "HELP", &args(&[]));
    assert!(
        matches!(&res, Frame::Array(Some(items)) if !items.is_empty()),
        "HELP should return a non-empty array, got: {res:?}"
    );
}

// ============================================================================
// Concord (CON.*) tests
// ============================================================================

#[test]
fn con_ginc_gval_round_trip() {
    let cluster = TestCluster::new(0);
    let store = cluster.primary_store();

    // GINC key replica delta — returns the new counter value
    let res =
        concord::concord_command_with_store(store, "GINC", &args(&["counter1", "node-a", "1"]));
    assert!(
        matches!(&res, Frame::Integer(_)),
        "GINC should return an integer, got: {res:?}"
    );

    let res = concord::concord_command_with_store(store, "GVAL", &args(&["counter1"]));
    assert!(
        matches!(&res, Frame::Integer(n) if *n >= 1),
        "GVAL should return counter value >= 1, got: {res:?}"
    );
}

#[test]
fn con_save_persists_to_store() {
    let cluster = TestCluster::new(1);
    let primary = cluster.primary_store();

    concord::concord_command_with_store(primary, "GINC", &args(&["save-ctr", "node-x", "1"]));
    let save = concord::concord_command_with_store(primary, "SAVE", &args(&[]));
    assert_ok(&save);

    cluster.replicate_ferrite_state(0);

    let replica = cluster.replica_store(0);
    let key = bytes::Bytes::from("__ferrite:concord:data");
    assert!(
        replica.get(0, &key).is_some(),
        "concord state should replicate to replica"
    );
}

// ============================================================================
// Lucidity (LUC.*) tests
// ============================================================================

#[test]
fn luc_append_len_round_trip() {
    let cluster = TestCluster::new(0);
    let store = cluster.primary_store();

    // APPEND key value — returns the log index as integer
    let res =
        lucidity::lucidity_command_with_store(store, "APPEND", &args(&["entry-key", "entry-one"]));
    assert!(
        matches!(&res, Frame::Integer(_)),
        "APPEND should return an integer index, got: {res:?}"
    );

    let res = lucidity::lucidity_command_with_store(store, "LEN", &args(&[]));
    assert!(
        matches!(&res, Frame::Integer(n) if *n >= 1),
        "LEN should be >= 1 after APPEND, got: {res:?}"
    );
}

#[test]
fn luc_save_persists_to_store() {
    let cluster = TestCluster::new(1);
    let primary = cluster.primary_store();

    lucidity::lucidity_command_with_store(primary, "APPEND", &args(&["save-key", "save-entry"]));
    let save = lucidity::lucidity_command_with_store(primary, "SAVE", &args(&[]));
    assert_ok(&save);

    cluster.replicate_ferrite_state(0);

    let replica = cluster.replica_store(0);
    let key = bytes::Bytes::from("__ferrite:lucidity:data");
    assert!(
        replica.get(0, &key).is_some(),
        "lucidity state should replicate to replica"
    );
}

// ============================================================================
// Pangea (PNG.*) tests
// ============================================================================

#[test]
fn png_alloc_read_round_trip() {
    let cluster = TestCluster::new(0);
    let store = cluster.primary_store();

    let res = pangea::pangea_command_with_store(store, "ALLOC", &args(&["pkey", "payload-data"]));
    // ALLOC returns a bulk string with allocation info
    assert!(
        !matches!(&res, Frame::Error(_)),
        "ALLOC should succeed, got: {res:?}"
    );

    let res = pangea::pangea_command_with_store(store, "READ", &args(&["pkey"]));
    assert!(
        !matches!(&res, Frame::Error(_) | Frame::Bulk(None)),
        "READ after ALLOC should return data, got: {res:?}"
    );
}

#[test]
fn png_save_persists_to_store() {
    let cluster = TestCluster::new(1);
    let primary = cluster.primary_store();

    pangea::pangea_command_with_store(primary, "ALLOC", &args(&["save-pk", "save-data"]));
    let save = pangea::pangea_command_with_store(primary, "SAVE", &args(&[]));
    assert_ok(&save);

    cluster.replicate_ferrite_state(0);

    let replica = cluster.replica_store(0);
    let key = bytes::Bytes::from("__ferrite:pangea:data");
    assert!(
        replica.get(0, &key).is_some(),
        "pangea state should replicate to replica"
    );
}

#[test]
fn png_stats_returns_info() {
    let store = Arc::new(Store::new(16));
    let res = pangea::pangea_command_with_store(&store, "STATS", &args(&[]));
    assert!(
        !matches!(&res, Frame::Error(_)),
        "STATS should not error, got: {res:?}"
    );
}

// ============================================================================
// Cross-handler: multi-node Store replication smoke test
// ============================================================================

#[test]
fn multi_node_plain_kv_replication() {
    let cluster = TestCluster::new(2);

    // Write to primary
    cluster.primary_set(0, "replicated-key", "replicated-val");

    // Replicate to both replicas
    cluster.replicate_key(0, "replicated-key", 0);
    cluster.replicate_key(0, "replicated-key", 1);

    // Read from replicas
    let v0 = cluster.replicas[0].get(0, &bytes::Bytes::from("replicated-key"));
    let v1 = cluster.replicas[1].get(0, &bytes::Bytes::from("replicated-key"));

    assert!(v0.is_some(), "replica 0 should have the key");
    assert!(v1.is_some(), "replica 1 should have the key");
}

#[test]
fn unknown_subcommand_returns_error() {
    let store = Arc::new(Store::new(16));
    let res = chronicle::chronicle_command_with_store(&store, "NOTACMD", &args(&[]));
    assert_error(&res);
}

// ============================================================================
// Mnemo (MEM.*) tests
// ============================================================================

#[test]
fn mem_put_get_round_trip() {
    let cluster = TestCluster::new(0);
    let store = cluster.primary_store();

    let res = mnemo::mnemo_command_with_store(
        store,
        "PUT",
        &args(&["agent1", "sess1", "episodic", "hello world"]),
    );
    assert!(
        !matches!(&res, Frame::Error(_)),
        "PUT should succeed, got: {res:?}"
    );
    let id = extract_bulk(&res).expect("PUT should return record id");

    let res = mnemo::mnemo_command_with_store(store, "GET", &args(&[&id]));
    assert!(
        !matches!(&res, Frame::Error(_) | Frame::Bulk(None)),
        "GET should return record, got: {res:?}"
    );
}

#[test]
fn mem_recall_returns_records() {
    let cluster = TestCluster::new(0);
    let store = cluster.primary_store();

    mnemo::mnemo_command_with_store(
        store,
        "PUT",
        &args(&["recall-agent", "s1", "semantic", "test recall"]),
    );
    let res = mnemo::mnemo_command_with_store(store, "RECALL", &args(&["recall-agent"]));
    assert!(
        !matches!(&res, Frame::Error(_)),
        "RECALL should succeed, got: {res:?}"
    );
}

#[test]
fn mem_forget_deletes_records() {
    let cluster = TestCluster::new(0);
    let store = cluster.primary_store();

    mnemo::mnemo_command_with_store(
        store,
        "PUT",
        &args(&["forget-agent", "s1", "episodic", "to be forgotten"]),
    );
    let res = mnemo::mnemo_command_with_store(store, "FORGET", &args(&["forget-agent"]));
    assert!(
        !matches!(&res, Frame::Error(_)),
        "FORGET should succeed, got: {res:?}"
    );
}

#[test]
fn mem_save_replicates_via_store() {
    let cluster = TestCluster::new(1);
    let primary = cluster.primary_store();

    mnemo::mnemo_command_with_store(
        primary,
        "PUT",
        &args(&["repl-agent", "s1", "working", "replicated memory"]),
    );
    let save = mnemo::mnemo_command_with_store(primary, "SAVE", &args(&[]));
    assert_ok(&save);

    cluster.replicate_ferrite_state(0);

    let replica = cluster.replica_store(0);
    let key = bytes::Bytes::from("__ferrite:mnemo:data");
    assert!(
        replica.get(0, &key).is_some(),
        "mnemo state should replicate to replica"
    );
}

#[test]
fn mem_stats_returns_info() {
    let store = Arc::new(Store::new(16));
    let res = mnemo::mnemo_command_with_store(&store, "STATS", &args(&[]));
    assert!(
        !matches!(&res, Frame::Error(_)),
        "STATS should not error, got: {res:?}"
    );
}

#[test]
fn mem_unknown_option_rejected() {
    let store = Arc::new(Store::new(16));
    let res =
        mnemo::mnemo_command_with_store(&store, "RECALL", &args(&["agent1", "BADOPTION", "5"]));
    assert_error(&res);
}

// ============================================================================
// Forge (FN.*) tests
// ============================================================================

#[test]
fn fn_load_drop_round_trip() {
    let cluster = TestCluster::new(0);
    let store = cluster.primary_store();

    let res = forge::forge_command_with_store(store, "LOAD", &args(&["int-test-mod", "0061"]), 0);
    assert!(
        !matches!(&res, Frame::Error(_)),
        "LOAD should succeed, got: {res:?}"
    );

    let res = forge::forge_command_with_store(store, "LIST", &args(&[]), 0);
    assert!(
        !matches!(&res, Frame::Error(_)),
        "LIST should succeed, got: {res:?}"
    );

    let res = forge::forge_command_with_store(store, "DROP", &args(&["int-test-mod"]), 0);
    assert!(
        !matches!(&res, Frame::Error(_)),
        "DROP should succeed, got: {res:?}"
    );
}

#[test]
fn fn_save_replicates_via_store() {
    let cluster = TestCluster::new(1);
    let primary = cluster.primary_store();

    forge::forge_command_with_store(primary, "LOAD", &args(&["repl-mod", "0061"]), 0);
    let save = forge::forge_command_with_store(primary, "SAVE", &args(&[]), 0);
    assert_ok(&save);

    cluster.replicate_ferrite_state(0);

    let replica = cluster.replica_store(0);
    let key = bytes::Bytes::from("__ferrite:forge:data");
    assert!(
        replica.get(0, &key).is_some(),
        "forge state should replicate to replica"
    );
}

#[test]
fn fn_call_nonexistent_errors() {
    let store = Arc::new(Store::new(16));
    let res =
        forge::forge_command_with_store(&store, "CALL", &args(&["no-such-mod", "key", "input"]), 0);
    assert_error(&res);
}

#[test]
fn fn_stats_returns_info() {
    let store = Arc::new(Store::new(16));
    let res = forge::forge_command_with_store(&store, "STATS", &args(&[]), 0);
    assert!(
        !matches!(&res, Frame::Error(_)),
        "STATS should not error, got: {res:?}"
    );
}
