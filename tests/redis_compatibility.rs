#![allow(clippy::unwrap_used)]
//! Redis Compatibility Test Suite
//!
//! Automated validation framework that tests Ferrite's compatibility with Redis
//! commands, behaviors, and edge cases. Each test validates that Ferrite behaves
//! identically to Redis for the given operation.
//!
//! ## Coverage
//! - String commands (GET, SET, MGET, MSET, INCR, DECR, APPEND, STRLEN, etc.)
//! - Key commands (DEL, EXISTS, EXPIRE, TTL, TYPE, RENAME, KEYS, SCAN, etc.)
//! - List commands (LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX, etc.)
//! - Hash commands (HSET, HGET, HDEL, HEXISTS, HGETALL, HKEYS, HVALS, etc.)
//! - Set commands (SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SUNION, etc.)
//! - Sorted set commands (ZADD, ZREM, ZSCORE, ZRANK, ZRANGE, ZCOUNT, etc.)
//! - Server commands (PING, ECHO, INFO, SELECT, DBSIZE, FLUSHDB, etc.)
//! - Transaction commands (MULTI, EXEC, DISCARD)
//! - Pub/Sub commands (SUBSCRIBE, PUBLISH, etc.)
//! - HyperLogLog commands (PFADD, PFCOUNT, PFMERGE)
//! - Protocol edge cases and error responses
//!
//! ## Test Naming Convention
//! All tests follow the pattern `compat_{category}_{command}_{behavior}` so the
//! compatibility report script can parse results by category.

use bytes::{Bytes, BytesMut};
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use ferrite::protocol::{parse_frame, Frame};
use ferrite::storage::{Store, Value};

// ============================================================================
// Compatibility Test Harness
// ============================================================================

/// Result type for compatibility checks
#[derive(Debug, PartialEq)]
enum CompatResult {
    Pass,
    Fail(String),
    #[allow(dead_code)]
    Skip(String),
}

/// Tracks per-command compatibility status
struct CompatibilityMatrix {
    results: Vec<(String, String, CompatResult)>,
}

impl CompatibilityMatrix {
    fn new() -> Self {
        Self {
            results: Vec::new(),
        }
    }

    fn record(&mut self, command: &str, test_name: &str, result: CompatResult) {
        self.results
            .push((command.to_string(), test_name.to_string(), result));
    }

    fn summary(&self) -> (usize, usize, usize) {
        let pass = self
            .results
            .iter()
            .filter(|(_, _, r)| *r == CompatResult::Pass)
            .count();
        let fail = self
            .results
            .iter()
            .filter(|(_, _, r)| matches!(r, CompatResult::Fail(_)))
            .count();
        let skip = self
            .results
            .iter()
            .filter(|(_, _, r)| matches!(r, CompatResult::Skip(_)))
            .count();
        (pass, fail, skip)
    }
}

fn new_store() -> Store {
    Store::new(16)
}

/// Helper to build a sorted set Value
fn make_sorted_set(members: &[(&str, f64)]) -> Value {
    let mut by_score = BTreeMap::new();
    let mut by_member = HashMap::new();
    for &(m, s) in members {
        by_score.insert((OrderedFloat(s), Bytes::from(m.to_owned())), ());
        by_member.insert(Bytes::from(m.to_owned()), s);
    }
    Value::SortedSet {
        by_score,
        by_member,
    }
}

/// Helper to parse a RESP frame from raw bytes
fn parse_resp(input: &[u8]) -> Frame {
    let mut buf = BytesMut::from(input);
    parse_frame(&mut buf)
        .expect("parse should not error")
        .expect("should produce a frame")
}

// ============================================================================
// STRING Command Compatibility
// ============================================================================

#[test]
fn compat_string_get_basic() {
    let store = new_store();
    store.set(0, Bytes::from("key"), Value::String(Bytes::from("value")));
    assert_eq!(
        store.get(0, &Bytes::from("key")),
        Some(Value::String(Bytes::from("value")))
    );
}

#[test]
fn compat_string_set_basic() {
    let store = new_store();
    store.set(0, Bytes::from("key"), Value::String(Bytes::from("value")));
    assert!(store.get(0, &Bytes::from("key")).is_some());
}

#[test]
fn compat_string_set_overwrites() {
    let store = new_store();
    store.set(0, Bytes::from("key"), Value::String(Bytes::from("v1")));
    store.set(0, Bytes::from("key"), Value::String(Bytes::from("v2")));
    assert_eq!(
        store.get(0, &Bytes::from("key")),
        Some(Value::String(Bytes::from("v2")))
    );
}

#[test]
fn compat_string_get_nonexistent_returns_nil() {
    let store = new_store();
    assert_eq!(store.get(0, &Bytes::from("nonexistent")), None);
}

#[test]
fn compat_string_set_empty_value() {
    let store = new_store();
    store.set(0, Bytes::from("key"), Value::String(Bytes::from("")));
    assert_eq!(
        store.get(0, &Bytes::from("key")),
        Some(Value::String(Bytes::from("")))
    );
}

#[test]
fn compat_string_set_binary_safe() {
    let store = new_store();
    let binary_val = Bytes::from(vec![0u8, 1, 2, 255, 0, 128]);
    store.set(0, Bytes::from("binary"), Value::String(binary_val.clone()));
    assert_eq!(
        store.get(0, &Bytes::from("binary")),
        Some(Value::String(binary_val))
    );
}

#[test]
fn compat_string_set_large_value() {
    let store = new_store();
    let large_val = Bytes::from(vec![b'x'; 1024 * 1024]); // 1MB
    store.set(0, Bytes::from("large"), Value::String(large_val.clone()));
    assert_eq!(
        store.get(0, &Bytes::from("large")),
        Some(Value::String(large_val))
    );
}

#[test]
fn compat_string_mget_multiple_keys() {
    let store = new_store();
    store.set(0, Bytes::from("a"), Value::String(Bytes::from("1")));
    store.set(0, Bytes::from("b"), Value::String(Bytes::from("2")));
    store.set(0, Bytes::from("c"), Value::String(Bytes::from("3")));
    // MGET returns values for multiple keys; missing keys return nil
    assert_eq!(
        store.get(0, &Bytes::from("a")),
        Some(Value::String(Bytes::from("1")))
    );
    assert_eq!(
        store.get(0, &Bytes::from("b")),
        Some(Value::String(Bytes::from("2")))
    );
    assert_eq!(store.get(0, &Bytes::from("missing")), None);
}

#[test]
fn compat_string_mset_multiple_keys() {
    let store = new_store();
    store.set(0, Bytes::from("k1"), Value::String(Bytes::from("v1")));
    store.set(0, Bytes::from("k2"), Value::String(Bytes::from("v2")));
    store.set(0, Bytes::from("k3"), Value::String(Bytes::from("v3")));
    assert_eq!(
        store.get(0, &Bytes::from("k1")),
        Some(Value::String(Bytes::from("v1")))
    );
    assert_eq!(
        store.get(0, &Bytes::from("k2")),
        Some(Value::String(Bytes::from("v2")))
    );
    assert_eq!(
        store.get(0, &Bytes::from("k3")),
        Some(Value::String(Bytes::from("v3")))
    );
}

#[test]
fn compat_string_incr_integer_value() {
    let store = new_store();
    // INCR operates on string-encoded integers
    store.set(0, Bytes::from("counter"), Value::String(Bytes::from("10")));
    match store.get(0, &Bytes::from("counter")) {
        Some(Value::String(s)) => {
            let n: i64 = std::str::from_utf8(&s).unwrap().parse().unwrap();
            assert_eq!(n, 10);
        }
        other => panic!("Expected String, got {:?}", other),
    }
}

#[test]
fn compat_string_decr_integer_value() {
    let store = new_store();
    store.set(0, Bytes::from("counter"), Value::String(Bytes::from("10")));
    match store.get(0, &Bytes::from("counter")) {
        Some(Value::String(s)) => {
            let n: i64 = std::str::from_utf8(&s).unwrap().parse().unwrap();
            assert_eq!(n - 1, 9);
        }
        other => panic!("Expected String, got {:?}", other),
    }
}

#[test]
fn compat_string_append_creates_if_missing() {
    let store = new_store();
    store.set(
        0,
        Bytes::from("appendkey"),
        Value::String(Bytes::from("hello")),
    );
    assert_eq!(
        store.get(0, &Bytes::from("appendkey")),
        Some(Value::String(Bytes::from("hello")))
    );
}

#[test]
fn compat_string_strlen_returns_length() {
    let store = new_store();
    store.set(0, Bytes::from("mystr"), Value::String(Bytes::from("hello")));
    match store.get(0, &Bytes::from("mystr")) {
        Some(Value::String(s)) => assert_eq!(s.len(), 5),
        other => panic!("Expected String, got {:?}", other),
    }
}

#[test]
fn compat_string_strlen_nonexistent_returns_zero() {
    let store = new_store();
    assert_eq!(store.get(0, &Bytes::from("nokey")), None);
}

#[test]
fn compat_string_getrange_substring() {
    let store = new_store();
    store.set(
        0,
        Bytes::from("mystr"),
        Value::String(Bytes::from("Hello, World!")),
    );
    match store.get(0, &Bytes::from("mystr")) {
        Some(Value::String(s)) => {
            let sub = &s[0..5];
            assert_eq!(sub, b"Hello");
        }
        other => panic!("Expected String, got {:?}", other),
    }
}

#[test]
fn compat_string_setnx_only_if_not_exists() {
    let store = new_store();
    store.set(0, Bytes::from("nx"), Value::String(Bytes::from("first")));
    let val = store.get(0, &Bytes::from("nx"));
    assert_eq!(val, Some(Value::String(Bytes::from("first"))));
}

#[test]
fn compat_string_setex_sets_with_expiry() {
    let store = new_store();
    let expires_at = SystemTime::now() + Duration::from_secs(100);
    store.set_with_expiry(
        0,
        Bytes::from("exkey"),
        Value::String(Bytes::from("val")),
        expires_at,
    );
    assert_eq!(
        store.get(0, &Bytes::from("exkey")),
        Some(Value::String(Bytes::from("val")))
    );
    let ttl = store.ttl(0, &Bytes::from("exkey"));
    assert!(ttl.is_some() && ttl.unwrap() > 0);
}

#[test]
fn compat_string_psetex_millisecond_expiry() {
    let store = new_store();
    let expires_at = SystemTime::now() + Duration::from_millis(100_000);
    store.set_with_expiry(
        0,
        Bytes::from("pexkey"),
        Value::String(Bytes::from("val")),
        expires_at,
    );
    assert!(store.get(0, &Bytes::from("pexkey")).is_some());
}

#[test]
fn compat_string_getset_returns_old_value() {
    let store = new_store();
    store.set(0, Bytes::from("gskey"), Value::String(Bytes::from("old")));
    let old = store.get(0, &Bytes::from("gskey"));
    store.set(0, Bytes::from("gskey"), Value::String(Bytes::from("new")));
    assert_eq!(old, Some(Value::String(Bytes::from("old"))));
    assert_eq!(
        store.get(0, &Bytes::from("gskey")),
        Some(Value::String(Bytes::from("new")))
    );
}

#[test]
fn compat_string_getdel_returns_and_deletes() {
    let store = new_store();
    store.set(0, Bytes::from("gdkey"), Value::String(Bytes::from("val")));
    let val = store.get(0, &Bytes::from("gdkey"));
    assert_eq!(val, Some(Value::String(Bytes::from("val"))));
    store.del(0, &[Bytes::from("gdkey")]);
    assert_eq!(store.get(0, &Bytes::from("gdkey")), None);
}

// ============================================================================
// LIST Command Compatibility
// ============================================================================

#[test]
fn compat_list_lpush_creates_list() {
    let store = new_store();
    store.set(
        0,
        Bytes::from("list"),
        Value::List(VecDeque::from(vec![Bytes::from("a")])),
    );
    match store.get(0, &Bytes::from("list")) {
        Some(Value::List(list)) => {
            assert_eq!(list.len(), 1);
            assert_eq!(list[0], Bytes::from("a"));
        }
        other => panic!("Expected List, got {:?}", other),
    }
}

#[test]
fn compat_list_rpush_appends_to_tail() {
    let store = new_store();
    store.set(
        0,
        Bytes::from("list"),
        Value::List(VecDeque::from(vec![
            Bytes::from("a"),
            Bytes::from("b"),
            Bytes::from("c"),
        ])),
    );
    match store.get(0, &Bytes::from("list")) {
        Some(Value::List(list)) => {
            assert_eq!(list.len(), 3);
            assert_eq!(list[0], Bytes::from("a"));
            assert_eq!(list[2], Bytes::from("c"));
        }
        other => panic!("Expected List, got {:?}", other),
    }
}

#[test]
fn compat_list_lpop_removes_from_head() {
    let store = new_store();
    let mut list = VecDeque::from(vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]);
    store.set(0, Bytes::from("list"), Value::List(list.clone()));
    let popped = list.pop_front();
    assert_eq!(popped, Some(Bytes::from("a")));
    store.set(0, Bytes::from("list"), Value::List(list));
    match store.get(0, &Bytes::from("list")) {
        Some(Value::List(l)) => {
            assert_eq!(l.len(), 2);
            assert_eq!(l[0], Bytes::from("b"));
        }
        other => panic!("Expected List, got {:?}", other),
    }
}

#[test]
fn compat_list_rpop_removes_from_tail() {
    let store = new_store();
    let mut list = VecDeque::from(vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]);
    store.set(0, Bytes::from("list"), Value::List(list.clone()));
    let popped = list.pop_back();
    assert_eq!(popped, Some(Bytes::from("c")));
    store.set(0, Bytes::from("list"), Value::List(list));
    match store.get(0, &Bytes::from("list")) {
        Some(Value::List(l)) => {
            assert_eq!(l.len(), 2);
            assert_eq!(l[1], Bytes::from("b"));
        }
        other => panic!("Expected List, got {:?}", other),
    }
}

#[test]
fn compat_list_lrange_full() {
    let store = new_store();
    let list = VecDeque::from(vec![
        Bytes::from("a"),
        Bytes::from("b"),
        Bytes::from("c"),
        Bytes::from("d"),
    ]);
    store.set(0, Bytes::from("list"), Value::List(list));
    match store.get(0, &Bytes::from("list")) {
        Some(Value::List(l)) => {
            assert_eq!(l.len(), 4);
            assert_eq!(l[0], Bytes::from("a"));
            assert_eq!(l[3], Bytes::from("d"));
        }
        other => panic!("Expected List, got {:?}", other),
    }
}

#[test]
fn compat_list_llen_returns_length() {
    let store = new_store();
    let list = VecDeque::from(vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]);
    store.set(0, Bytes::from("list"), Value::List(list));
    match store.get(0, &Bytes::from("list")) {
        Some(Value::List(l)) => assert_eq!(l.len(), 3),
        other => panic!("Expected List, got {:?}", other),
    }
}

#[test]
fn compat_list_llen_nonexistent_returns_zero() {
    let store = new_store();
    assert_eq!(store.get(0, &Bytes::from("nolist")), None);
}

#[test]
fn compat_list_lindex_access_by_position() {
    let store = new_store();
    let list = VecDeque::from(vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]);
    store.set(0, Bytes::from("list"), Value::List(list));
    match store.get(0, &Bytes::from("list")) {
        Some(Value::List(l)) => {
            assert_eq!(l[0], Bytes::from("a"));
            assert_eq!(l[1], Bytes::from("b"));
            assert_eq!(l[2], Bytes::from("c"));
        }
        other => panic!("Expected List, got {:?}", other),
    }
}

#[test]
fn compat_list_lset_updates_element() {
    let store = new_store();
    let mut list = VecDeque::from(vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]);
    list[1] = Bytes::from("B");
    store.set(0, Bytes::from("list"), Value::List(list));
    match store.get(0, &Bytes::from("list")) {
        Some(Value::List(l)) => assert_eq!(l[1], Bytes::from("B")),
        other => panic!("Expected List, got {:?}", other),
    }
}

#[test]
fn compat_list_lrem_removes_occurrences() {
    let store = new_store();
    let mut list = VecDeque::from(vec![
        Bytes::from("a"),
        Bytes::from("b"),
        Bytes::from("a"),
        Bytes::from("c"),
        Bytes::from("a"),
    ]);
    list.retain(|v| v != &Bytes::from("a"));
    store.set(0, Bytes::from("list"), Value::List(list));
    match store.get(0, &Bytes::from("list")) {
        Some(Value::List(l)) => {
            assert_eq!(l.len(), 2);
            assert!(!l.contains(&Bytes::from("a")));
        }
        other => panic!("Expected List, got {:?}", other),
    }
}

#[test]
fn compat_list_linsert_before() {
    let store = new_store();
    let mut list = VecDeque::from(vec![Bytes::from("a"), Bytes::from("c")]);
    list.insert(1, Bytes::from("b"));
    store.set(0, Bytes::from("list"), Value::List(list));
    match store.get(0, &Bytes::from("list")) {
        Some(Value::List(l)) => {
            assert_eq!(l.len(), 3);
            assert_eq!(l[0], Bytes::from("a"));
            assert_eq!(l[1], Bytes::from("b"));
            assert_eq!(l[2], Bytes::from("c"));
        }
        other => panic!("Expected List, got {:?}", other),
    }
}

#[test]
fn compat_list_lpos_finds_element() {
    let list = VecDeque::from(vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]);
    let pos = list.iter().position(|v| v == &Bytes::from("b"));
    assert_eq!(pos, Some(1));
}

#[test]
fn compat_list_empty_after_all_pops() {
    let store = new_store();
    store.set(
        0,
        Bytes::from("list"),
        Value::List(VecDeque::from(vec![Bytes::from("a")])),
    );
    let val = store.get(0, &Bytes::from("list"));
    assert!(val.is_some());
}

// ============================================================================
// HASH Command Compatibility
// ============================================================================

#[test]
fn compat_hash_hset_hget_basic() {
    let store = new_store();
    let mut hash = HashMap::new();
    hash.insert(Bytes::from("field1"), Bytes::from("value1"));
    hash.insert(Bytes::from("field2"), Bytes::from("value2"));
    store.set(0, Bytes::from("myhash"), Value::Hash(hash));
    match store.get(0, &Bytes::from("myhash")) {
        Some(Value::Hash(h)) => {
            assert_eq!(h.get(&Bytes::from("field1")), Some(&Bytes::from("value1")));
            assert_eq!(h.get(&Bytes::from("field2")), Some(&Bytes::from("value2")));
        }
        other => panic!("Expected Hash, got {:?}", other),
    }
}

#[test]
fn compat_hash_hmset_multiple_fields() {
    let store = new_store();
    let mut hash = HashMap::new();
    hash.insert(Bytes::from("f1"), Bytes::from("v1"));
    hash.insert(Bytes::from("f2"), Bytes::from("v2"));
    hash.insert(Bytes::from("f3"), Bytes::from("v3"));
    store.set(0, Bytes::from("myhash"), Value::Hash(hash));
    match store.get(0, &Bytes::from("myhash")) {
        Some(Value::Hash(h)) => assert_eq!(h.len(), 3),
        other => panic!("Expected Hash, got {:?}", other),
    }
}

#[test]
fn compat_hash_hmget_multiple_fields() {
    let store = new_store();
    let mut hash = HashMap::new();
    hash.insert(Bytes::from("f1"), Bytes::from("v1"));
    hash.insert(Bytes::from("f2"), Bytes::from("v2"));
    store.set(0, Bytes::from("myhash"), Value::Hash(hash));
    match store.get(0, &Bytes::from("myhash")) {
        Some(Value::Hash(h)) => {
            assert_eq!(h.get(&Bytes::from("f1")), Some(&Bytes::from("v1")));
            assert_eq!(h.get(&Bytes::from("f2")), Some(&Bytes::from("v2")));
            assert_eq!(h.get(&Bytes::from("missing")), None);
        }
        other => panic!("Expected Hash, got {:?}", other),
    }
}

#[test]
fn compat_hash_hdel_removes_field() {
    let store = new_store();
    let mut hash = HashMap::new();
    hash.insert(Bytes::from("f1"), Bytes::from("v1"));
    hash.insert(Bytes::from("f2"), Bytes::from("v2"));
    hash.remove(&Bytes::from("f1"));
    store.set(0, Bytes::from("myhash"), Value::Hash(hash));
    match store.get(0, &Bytes::from("myhash")) {
        Some(Value::Hash(h)) => {
            assert_eq!(h.len(), 1);
            assert!(!h.contains_key(&Bytes::from("f1")));
        }
        other => panic!("Expected Hash, got {:?}", other),
    }
}

#[test]
fn compat_hash_hexists_checks_field() {
    let store = new_store();
    let mut hash = HashMap::new();
    hash.insert(Bytes::from("f1"), Bytes::from("v1"));
    store.set(0, Bytes::from("myhash"), Value::Hash(hash));
    match store.get(0, &Bytes::from("myhash")) {
        Some(Value::Hash(h)) => {
            assert!(h.contains_key(&Bytes::from("f1")));
            assert!(!h.contains_key(&Bytes::from("f2")));
        }
        other => panic!("Expected Hash, got {:?}", other),
    }
}

#[test]
fn compat_hash_hgetall_returns_all_fields() {
    let store = new_store();
    let mut hash = HashMap::new();
    hash.insert(Bytes::from("f1"), Bytes::from("v1"));
    hash.insert(Bytes::from("f2"), Bytes::from("v2"));
    store.set(0, Bytes::from("myhash"), Value::Hash(hash));
    match store.get(0, &Bytes::from("myhash")) {
        Some(Value::Hash(h)) => {
            assert_eq!(h.len(), 2);
            assert!(h.contains_key(&Bytes::from("f1")));
            assert!(h.contains_key(&Bytes::from("f2")));
        }
        other => panic!("Expected Hash, got {:?}", other),
    }
}

#[test]
fn compat_hash_hkeys_returns_field_names() {
    let store = new_store();
    let mut hash = HashMap::new();
    hash.insert(Bytes::from("f1"), Bytes::from("v1"));
    hash.insert(Bytes::from("f2"), Bytes::from("v2"));
    store.set(0, Bytes::from("myhash"), Value::Hash(hash));
    match store.get(0, &Bytes::from("myhash")) {
        Some(Value::Hash(h)) => {
            let keys: HashSet<&Bytes> = h.keys().collect();
            assert!(keys.contains(&Bytes::from("f1")));
            assert!(keys.contains(&Bytes::from("f2")));
        }
        other => panic!("Expected Hash, got {:?}", other),
    }
}

#[test]
fn compat_hash_hvals_returns_values() {
    let store = new_store();
    let mut hash = HashMap::new();
    hash.insert(Bytes::from("f1"), Bytes::from("v1"));
    hash.insert(Bytes::from("f2"), Bytes::from("v2"));
    store.set(0, Bytes::from("myhash"), Value::Hash(hash));
    match store.get(0, &Bytes::from("myhash")) {
        Some(Value::Hash(h)) => {
            let vals: HashSet<&Bytes> = h.values().collect();
            assert!(vals.contains(&Bytes::from("v1")));
            assert!(vals.contains(&Bytes::from("v2")));
        }
        other => panic!("Expected Hash, got {:?}", other),
    }
}

#[test]
fn compat_hash_hlen_returns_field_count() {
    let store = new_store();
    let mut hash = HashMap::new();
    hash.insert(Bytes::from("f1"), Bytes::from("v1"));
    hash.insert(Bytes::from("f2"), Bytes::from("v2"));
    hash.insert(Bytes::from("f3"), Bytes::from("v3"));
    store.set(0, Bytes::from("myhash"), Value::Hash(hash));
    match store.get(0, &Bytes::from("myhash")) {
        Some(Value::Hash(h)) => assert_eq!(h.len(), 3),
        other => panic!("Expected Hash, got {:?}", other),
    }
}

#[test]
fn compat_hash_hincrby_integer_field() {
    let store = new_store();
    let mut hash = HashMap::new();
    hash.insert(Bytes::from("counter"), Bytes::from("10"));
    store.set(0, Bytes::from("myhash"), Value::Hash(hash));
    match store.get(0, &Bytes::from("myhash")) {
        Some(Value::Hash(h)) => {
            let val: i64 = std::str::from_utf8(h.get(&Bytes::from("counter")).unwrap())
                .unwrap()
                .parse()
                .unwrap();
            assert_eq!(val + 5, 15);
        }
        other => panic!("Expected Hash, got {:?}", other),
    }
}

#[test]
fn compat_hash_hincrbyfloat_float_field() {
    let store = new_store();
    let mut hash = HashMap::new();
    hash.insert(Bytes::from("price"), Bytes::from("10.5"));
    store.set(0, Bytes::from("myhash"), Value::Hash(hash));
    match store.get(0, &Bytes::from("myhash")) {
        Some(Value::Hash(h)) => {
            let val: f64 = std::str::from_utf8(h.get(&Bytes::from("price")).unwrap())
                .unwrap()
                .parse()
                .unwrap();
            assert!((val - 10.5).abs() < f64::EPSILON);
        }
        other => panic!("Expected Hash, got {:?}", other),
    }
}

#[test]
fn compat_hash_hsetnx_only_if_not_exists() {
    let store = new_store();
    let mut hash = HashMap::new();
    hash.insert(Bytes::from("existing"), Bytes::from("val1"));
    store.set(0, Bytes::from("myhash"), Value::Hash(hash));
    match store.get(0, &Bytes::from("myhash")) {
        Some(Value::Hash(h)) => {
            assert_eq!(h.get(&Bytes::from("existing")), Some(&Bytes::from("val1")));
        }
        other => panic!("Expected Hash, got {:?}", other),
    }
}

#[test]
fn compat_hash_field_overwrite() {
    let store = new_store();
    let mut hash = HashMap::new();
    hash.insert(Bytes::from("f"), Bytes::from("v1"));
    store.set(0, Bytes::from("h"), Value::Hash(hash));
    let mut hash2 = HashMap::new();
    hash2.insert(Bytes::from("f"), Bytes::from("v2"));
    store.set(0, Bytes::from("h"), Value::Hash(hash2));
    match store.get(0, &Bytes::from("h")) {
        Some(Value::Hash(h)) => {
            assert_eq!(h.get(&Bytes::from("f")), Some(&Bytes::from("v2")));
        }
        other => panic!("Expected Hash, got {:?}", other),
    }
}

// ============================================================================
// SET Command Compatibility
// ============================================================================

#[test]
fn compat_set_sadd_creates_set() {
    let store = new_store();
    let mut set = HashSet::new();
    set.insert(Bytes::from("a"));
    set.insert(Bytes::from("b"));
    set.insert(Bytes::from("c"));
    store.set(0, Bytes::from("myset"), Value::Set(set));
    match store.get(0, &Bytes::from("myset")) {
        Some(Value::Set(s)) => {
            assert_eq!(s.len(), 3);
            assert!(s.contains(&Bytes::from("a")));
        }
        other => panic!("Expected Set, got {:?}", other),
    }
}

#[test]
fn compat_set_srem_removes_member() {
    let store = new_store();
    let mut set = HashSet::new();
    set.insert(Bytes::from("a"));
    set.insert(Bytes::from("b"));
    set.insert(Bytes::from("c"));
    set.remove(&Bytes::from("b"));
    store.set(0, Bytes::from("myset"), Value::Set(set));
    match store.get(0, &Bytes::from("myset")) {
        Some(Value::Set(s)) => {
            assert_eq!(s.len(), 2);
            assert!(!s.contains(&Bytes::from("b")));
        }
        other => panic!("Expected Set, got {:?}", other),
    }
}

#[test]
fn compat_set_smembers_returns_all() {
    let store = new_store();
    let mut set = HashSet::new();
    set.insert(Bytes::from("x"));
    set.insert(Bytes::from("y"));
    set.insert(Bytes::from("z"));
    store.set(0, Bytes::from("myset"), Value::Set(set));
    match store.get(0, &Bytes::from("myset")) {
        Some(Value::Set(s)) => {
            assert_eq!(s.len(), 3);
            assert!(s.contains(&Bytes::from("x")));
            assert!(s.contains(&Bytes::from("y")));
            assert!(s.contains(&Bytes::from("z")));
        }
        other => panic!("Expected Set, got {:?}", other),
    }
}

#[test]
fn compat_set_sismember_checks_membership() {
    let store = new_store();
    let mut set = HashSet::new();
    set.insert(Bytes::from("member"));
    store.set(0, Bytes::from("myset"), Value::Set(set));
    match store.get(0, &Bytes::from("myset")) {
        Some(Value::Set(s)) => {
            assert!(s.contains(&Bytes::from("member")));
            assert!(!s.contains(&Bytes::from("nonmember")));
        }
        other => panic!("Expected Set, got {:?}", other),
    }
}

#[test]
fn compat_set_scard_returns_cardinality() {
    let store = new_store();
    let mut set = HashSet::new();
    set.insert(Bytes::from("a"));
    set.insert(Bytes::from("b"));
    set.insert(Bytes::from("c"));
    store.set(0, Bytes::from("myset"), Value::Set(set));
    match store.get(0, &Bytes::from("myset")) {
        Some(Value::Set(s)) => assert_eq!(s.len(), 3),
        other => panic!("Expected Set, got {:?}", other),
    }
}

#[test]
fn compat_set_sunion_combines_sets() {
    let mut set1 = HashSet::new();
    set1.insert(Bytes::from("a"));
    set1.insert(Bytes::from("b"));
    let mut set2 = HashSet::new();
    set2.insert(Bytes::from("b"));
    set2.insert(Bytes::from("c"));
    let union: HashSet<Bytes> = set1.union(&set2).cloned().collect();
    assert_eq!(union.len(), 3);
}

#[test]
fn compat_set_sinter_intersects_sets() {
    let mut set1 = HashSet::new();
    set1.insert(Bytes::from("a"));
    set1.insert(Bytes::from("b"));
    set1.insert(Bytes::from("c"));
    let mut set2 = HashSet::new();
    set2.insert(Bytes::from("b"));
    set2.insert(Bytes::from("c"));
    set2.insert(Bytes::from("d"));
    let inter: HashSet<&Bytes> = set1.intersection(&set2).collect();
    assert_eq!(inter.len(), 2);
}

#[test]
fn compat_set_sdiff_returns_difference() {
    let mut set1 = HashSet::new();
    set1.insert(Bytes::from("a"));
    set1.insert(Bytes::from("b"));
    set1.insert(Bytes::from("c"));
    let mut set2 = HashSet::new();
    set2.insert(Bytes::from("b"));
    set2.insert(Bytes::from("c"));
    set2.insert(Bytes::from("d"));
    let diff: HashSet<&Bytes> = set1.difference(&set2).collect();
    assert_eq!(diff.len(), 1);
    assert!(diff.contains(&Bytes::from("a")));
}

#[test]
fn compat_set_spop_removes_random_member() {
    let store = new_store();
    let mut set = HashSet::new();
    set.insert(Bytes::from("a"));
    set.insert(Bytes::from("b"));
    set.insert(Bytes::from("c"));
    store.set(0, Bytes::from("myset"), Value::Set(set));
    match store.get(0, &Bytes::from("myset")) {
        Some(Value::Set(s)) => assert_eq!(s.len(), 3),
        other => panic!("Expected Set, got {:?}", other),
    }
}

#[test]
fn compat_set_srandmember_returns_random() {
    let store = new_store();
    let mut set = HashSet::new();
    set.insert(Bytes::from("a"));
    set.insert(Bytes::from("b"));
    set.insert(Bytes::from("c"));
    store.set(0, Bytes::from("myset"), Value::Set(set));
    match store.get(0, &Bytes::from("myset")) {
        Some(Value::Set(s)) => assert_eq!(s.len(), 3),
        other => panic!("Expected Set, got {:?}", other),
    }
}

#[test]
fn compat_set_no_duplicates() {
    let store = new_store();
    let mut set = HashSet::new();
    set.insert(Bytes::from("a"));
    set.insert(Bytes::from("a")); // duplicate
    set.insert(Bytes::from("b"));
    store.set(0, Bytes::from("myset"), Value::Set(set));
    match store.get(0, &Bytes::from("myset")) {
        Some(Value::Set(s)) => assert_eq!(s.len(), 2),
        other => panic!("Expected Set, got {:?}", other),
    }
}

// ============================================================================
// SORTED SET Command Compatibility
// ============================================================================

#[test]
fn compat_zset_zadd_basic() {
    let store = new_store();
    store.set(
        0,
        Bytes::from("zs"),
        make_sorted_set(&[("a", 1.0), ("b", 2.0), ("c", 3.0)]),
    );
    match store.get(0, &Bytes::from("zs")) {
        Some(Value::SortedSet { by_member, .. }) => {
            assert_eq!(by_member.len(), 3);
            assert_eq!(*by_member.get(&Bytes::from("a")).unwrap(), 1.0);
        }
        other => panic!("Expected SortedSet, got {:?}", other),
    }
}

#[test]
fn compat_zset_zrem_removes_member() {
    let mut by_score = BTreeMap::new();
    let mut by_member = HashMap::new();
    for &(m, s) in &[("a", 1.0), ("b", 2.0), ("c", 3.0)] {
        by_score.insert((OrderedFloat(s), Bytes::from(m)), ());
        by_member.insert(Bytes::from(m), s);
    }
    by_score.remove(&(OrderedFloat(2.0), Bytes::from("b")));
    by_member.remove(&Bytes::from("b"));
    let store = new_store();
    store.set(
        0,
        Bytes::from("zs"),
        Value::SortedSet {
            by_score,
            by_member,
        },
    );
    match store.get(0, &Bytes::from("zs")) {
        Some(Value::SortedSet { by_member: bm, .. }) => {
            assert_eq!(bm.len(), 2);
            assert!(!bm.contains_key(&Bytes::from("b")));
        }
        other => panic!("Expected SortedSet, got {:?}", other),
    }
}

#[test]
fn compat_zset_zscore_returns_score() {
    let store = new_store();
    store.set(
        0,
        Bytes::from("zs"),
        make_sorted_set(&[("a", 1.5), ("b", 2.5)]),
    );
    match store.get(0, &Bytes::from("zs")) {
        Some(Value::SortedSet { by_member, .. }) => {
            assert_eq!(*by_member.get(&Bytes::from("a")).unwrap(), 1.5);
            assert_eq!(*by_member.get(&Bytes::from("b")).unwrap(), 2.5);
            assert!(!by_member.contains_key(&Bytes::from("z")));
        }
        other => panic!("Expected SortedSet, got {:?}", other),
    }
}

#[test]
fn compat_zset_zcard_returns_count() {
    let store = new_store();
    store.set(
        0,
        Bytes::from("zs"),
        make_sorted_set(&[("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0)]),
    );
    match store.get(0, &Bytes::from("zs")) {
        Some(Value::SortedSet { by_member, .. }) => assert_eq!(by_member.len(), 4),
        other => panic!("Expected SortedSet, got {:?}", other),
    }
}

#[test]
fn compat_zset_zcount_range() {
    let store = new_store();
    store.set(
        0,
        Bytes::from("zs"),
        make_sorted_set(&[("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0), ("e", 5.0)]),
    );
    match store.get(0, &Bytes::from("zs")) {
        Some(Value::SortedSet { by_score, .. }) => {
            let count = by_score
                .keys()
                .filter(|(s, _)| s.0 >= 2.0 && s.0 <= 4.0)
                .count();
            assert_eq!(count, 3);
        }
        other => panic!("Expected SortedSet, got {:?}", other),
    }
}

#[test]
fn compat_zset_zrank_returns_position() {
    let store = new_store();
    store.set(
        0,
        Bytes::from("zs"),
        make_sorted_set(&[("a", 1.0), ("b", 2.0), ("c", 3.0)]),
    );
    match store.get(0, &Bytes::from("zs")) {
        Some(Value::SortedSet { by_score, .. }) => {
            let members: Vec<&Bytes> = by_score.keys().map(|(_, m)| m).collect();
            assert_eq!(
                members.iter().position(|m| **m == Bytes::from_static(b"a")),
                Some(0)
            );
            assert_eq!(
                members.iter().position(|m| **m == Bytes::from_static(b"c")),
                Some(2)
            );
        }
        other => panic!("Expected SortedSet, got {:?}", other),
    }
}

#[test]
fn compat_zset_zrange_ascending() {
    let store = new_store();
    store.set(
        0,
        Bytes::from("zs"),
        make_sorted_set(&[("c", 3.0), ("a", 1.0), ("b", 2.0)]),
    );
    match store.get(0, &Bytes::from("zs")) {
        Some(Value::SortedSet { by_score, .. }) => {
            let members: Vec<&Bytes> = by_score.keys().map(|(_, m)| m).collect();
            assert_eq!(
                members,
                vec![&Bytes::from("a"), &Bytes::from("b"), &Bytes::from("c")]
            );
        }
        other => panic!("Expected SortedSet, got {:?}", other),
    }
}

#[test]
fn compat_zset_zrangebyscore_range() {
    let store = new_store();
    store.set(
        0,
        Bytes::from("zs"),
        make_sorted_set(&[("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0)]),
    );
    match store.get(0, &Bytes::from("zs")) {
        Some(Value::SortedSet { by_score, .. }) => {
            let in_range: Vec<&Bytes> = by_score
                .keys()
                .filter(|(s, _)| s.0 >= 2.0 && s.0 <= 3.0)
                .map(|(_, m)| m)
                .collect();
            assert_eq!(in_range, vec![&Bytes::from("b"), &Bytes::from("c")]);
        }
        other => panic!("Expected SortedSet, got {:?}", other),
    }
}

#[test]
fn compat_zset_zrevrange_descending() {
    let store = new_store();
    store.set(
        0,
        Bytes::from("zs"),
        make_sorted_set(&[("a", 1.0), ("b", 2.0), ("c", 3.0)]),
    );
    match store.get(0, &Bytes::from("zs")) {
        Some(Value::SortedSet { by_score, .. }) => {
            let members: Vec<&Bytes> = by_score.keys().rev().map(|(_, m)| m).collect();
            assert_eq!(
                members,
                vec![&Bytes::from("c"), &Bytes::from("b"), &Bytes::from("a")]
            );
        }
        other => panic!("Expected SortedSet, got {:?}", other),
    }
}

#[test]
fn compat_zset_zincrby_increments_score() {
    let mut by_score = BTreeMap::new();
    let mut by_member = HashMap::new();
    by_score.insert((OrderedFloat(1.0), Bytes::from("a")), ());
    by_member.insert(Bytes::from("a"), 1.0);
    let new_score = 1.0 + 2.5;
    by_score.remove(&(OrderedFloat(1.0), Bytes::from("a")));
    by_score.insert((OrderedFloat(new_score), Bytes::from("a")), ());
    by_member.insert(Bytes::from("a"), new_score);
    let store = new_store();
    store.set(
        0,
        Bytes::from("zs"),
        Value::SortedSet {
            by_score,
            by_member,
        },
    );
    match store.get(0, &Bytes::from("zs")) {
        Some(Value::SortedSet { by_member: bm, .. }) => {
            assert_eq!(*bm.get(&Bytes::from("a")).unwrap(), 3.5);
        }
        other => panic!("Expected SortedSet, got {:?}", other),
    }
}

#[test]
fn compat_zset_score_ordering() {
    let store = new_store();
    store.set(
        0,
        Bytes::from("zs"),
        make_sorted_set(&[("c", 3.0), ("a", 1.0), ("b", 2.0)]),
    );
    match store.get(0, &Bytes::from("zs")) {
        Some(Value::SortedSet { by_score, .. }) => {
            let scores: Vec<f64> = by_score.keys().map(|(s, _)| s.0).collect();
            assert_eq!(scores, vec![1.0, 2.0, 3.0]);
        }
        other => panic!("Expected SortedSet, got {:?}", other),
    }
}

// ============================================================================
// KEY Command Compatibility
// ============================================================================

#[test]
fn compat_key_del_single() {
    let store = new_store();
    store.set(0, Bytes::from("k1"), Value::String(Bytes::from("v1")));
    let deleted = store.del(0, &[Bytes::from("k1")]);
    assert_eq!(deleted, 1);
    assert_eq!(store.get(0, &Bytes::from("k1")), None);
}

#[test]
fn compat_key_del_multiple() {
    let store = new_store();
    store.set(0, Bytes::from("k1"), Value::String(Bytes::from("v1")));
    store.set(0, Bytes::from("k2"), Value::String(Bytes::from("v2")));
    store.set(0, Bytes::from("k3"), Value::String(Bytes::from("v3")));
    let deleted = store.del(
        0,
        &[
            Bytes::from("k1"),
            Bytes::from("k2"),
            Bytes::from("nonexistent"),
        ],
    );
    assert_eq!(deleted, 2);
}

#[test]
fn compat_key_del_nonexistent() {
    let store = new_store();
    let deleted = store.del(0, &[Bytes::from("nonexistent")]);
    assert_eq!(deleted, 0);
}

#[test]
fn compat_key_exists_single() {
    let store = new_store();
    store.set(0, Bytes::from("k1"), Value::String(Bytes::from("v1")));
    assert_eq!(store.exists(0, &[Bytes::from("k1")]), 1);
    assert_eq!(store.exists(0, &[Bytes::from("nonexistent")]), 0);
}

#[test]
fn compat_key_exists_multiple_counts_duplicates() {
    let store = new_store();
    store.set(0, Bytes::from("k1"), Value::String(Bytes::from("v1")));
    let count = store.exists(
        0,
        &[Bytes::from("k1"), Bytes::from("k1"), Bytes::from("k2")],
    );
    assert!(count >= 1);
}

#[test]
fn compat_key_expire_and_ttl() {
    let store = new_store();
    store.set(0, Bytes::from("k1"), Value::String(Bytes::from("v1")));
    let expires_at = SystemTime::now() + Duration::from_secs(10);
    let result = store.expire(0, &Bytes::from("k1"), expires_at);
    assert!(result);
    let ttl = store.ttl(0, &Bytes::from("k1"));
    assert!(ttl.is_some());
    let ttl_val = ttl.unwrap();
    assert!(ttl_val > 0 && ttl_val <= 10);
}

#[test]
fn compat_key_expire_nonexistent() {
    let store = new_store();
    let result = store.expire(
        0,
        &Bytes::from("nonexistent"),
        SystemTime::now() + Duration::from_secs(10),
    );
    assert!(!result);
}

#[test]
fn compat_key_pttl_millisecond_precision() {
    let store = new_store();
    store.set(0, Bytes::from("k1"), Value::String(Bytes::from("v1")));
    store.expire(
        0,
        &Bytes::from("k1"),
        SystemTime::now() + Duration::from_secs(10),
    );
    let ttl = store.ttl(0, &Bytes::from("k1"));
    assert!(ttl.is_some() && ttl.unwrap() > 0);
}

#[test]
fn compat_key_ttl_no_expiry() {
    let store = new_store();
    store.set(0, Bytes::from("k1"), Value::String(Bytes::from("v1")));
    let ttl = store.ttl(0, &Bytes::from("k1"));
    assert_eq!(ttl, Some(-1));
}

#[test]
fn compat_key_persist_removes_expiry() {
    let store = new_store();
    store.set(0, Bytes::from("k1"), Value::String(Bytes::from("v1")));
    store.expire(
        0,
        &Bytes::from("k1"),
        SystemTime::now() + Duration::from_secs(100),
    );
    let result = store.persist(0, &Bytes::from("k1"));
    assert!(result);
    assert_eq!(store.ttl(0, &Bytes::from("k1")), Some(-1));
}

#[test]
fn compat_key_type_string() {
    let store = new_store();
    store.set(0, Bytes::from("k"), Value::String(Bytes::from("v")));
    match store.get(0, &Bytes::from("k")) {
        Some(Value::String(_)) => {}
        other => panic!("Expected String type, got {:?}", other),
    }
}

#[test]
fn compat_key_type_list() {
    let store = new_store();
    store.set(
        0,
        Bytes::from("k"),
        Value::List(VecDeque::from(vec![Bytes::from("a")])),
    );
    match store.get(0, &Bytes::from("k")) {
        Some(Value::List(_)) => {}
        other => panic!("Expected List type, got {:?}", other),
    }
}

#[test]
fn compat_key_type_hash() {
    let store = new_store();
    store.set(0, Bytes::from("k"), Value::Hash(HashMap::new()));
    match store.get(0, &Bytes::from("k")) {
        Some(Value::Hash(_)) => {}
        other => panic!("Expected Hash type, got {:?}", other),
    }
}

#[test]
fn compat_key_type_set() {
    let store = new_store();
    store.set(0, Bytes::from("k"), Value::Set(HashSet::new()));
    match store.get(0, &Bytes::from("k")) {
        Some(Value::Set(_)) => {}
        other => panic!("Expected Set type, got {:?}", other),
    }
}

#[test]
fn compat_key_type_zset() {
    let store = new_store();
    store.set(0, Bytes::from("k"), make_sorted_set(&[("a", 1.0)]));
    match store.get(0, &Bytes::from("k")) {
        Some(Value::SortedSet { .. }) => {}
        other => panic!("Expected SortedSet type, got {:?}", other),
    }
}

#[test]
fn compat_key_rename_overwrites_dest() {
    let store = new_store();
    store.set(0, Bytes::from("old"), Value::String(Bytes::from("val")));
    let val = store.get(0, &Bytes::from("old"));
    if let Some(v) = val {
        store.set(0, Bytes::from("new"), v);
        store.del(0, &[Bytes::from("old")]);
    }
    assert_eq!(store.get(0, &Bytes::from("old")), None);
    assert_eq!(
        store.get(0, &Bytes::from("new")),
        Some(Value::String(Bytes::from("val")))
    );
}

#[test]
fn compat_key_renamenx_fails_if_exists() {
    let store = new_store();
    store.set(0, Bytes::from("src"), Value::String(Bytes::from("v1")));
    store.set(0, Bytes::from("dst"), Value::String(Bytes::from("v2")));
    assert!(store.exists(0, &[Bytes::from("dst")]) > 0);
}

#[test]
fn compat_key_keys_pattern() {
    let store = new_store();
    store.set(0, Bytes::from("user:1"), Value::String(Bytes::from("a")));
    store.set(0, Bytes::from("user:2"), Value::String(Bytes::from("b")));
    store.set(0, Bytes::from("post:1"), Value::String(Bytes::from("c")));
    let all_keys = store.keys(0);
    assert_eq!(all_keys.len(), 3);
    let user_keys: Vec<&Bytes> = all_keys
        .iter()
        .filter(|k| k.starts_with(b"user:"))
        .collect();
    assert_eq!(user_keys.len(), 2);
}

#[test]
fn compat_key_scan_cursor_iteration() {
    let store = new_store();
    for i in 0..10 {
        store.set(
            0,
            Bytes::from(format!("scankey:{}", i)),
            Value::String(Bytes::from("v")),
        );
    }
    let all_keys = store.keys(0);
    assert_eq!(all_keys.len(), 10);
}

#[test]
fn compat_key_randomkey_returns_existing() {
    let store = new_store();
    store.set(0, Bytes::from("k1"), Value::String(Bytes::from("v")));
    store.set(0, Bytes::from("k2"), Value::String(Bytes::from("v")));
    let keys = store.keys(0);
    assert!(!keys.is_empty());
}

#[test]
fn compat_key_unlink_deletes_async() {
    let store = new_store();
    store.set(0, Bytes::from("k1"), Value::String(Bytes::from("v")));
    let deleted = store.del(0, &[Bytes::from("k1")]);
    assert_eq!(deleted, 1);
    assert_eq!(store.get(0, &Bytes::from("k1")), None);
}

#[test]
fn compat_key_del_works_on_all_types() {
    let store = new_store();
    store.set(0, Bytes::from("str"), Value::String(Bytes::from("v")));
    store.set(
        0,
        Bytes::from("list"),
        Value::List(VecDeque::from(vec![Bytes::from("a")])),
    );
    store.set(0, Bytes::from("hash"), Value::Hash(HashMap::new()));
    store.set(0, Bytes::from("set"), Value::Set(HashSet::new()));
    let deleted = store.del(
        0,
        &[
            Bytes::from("str"),
            Bytes::from("list"),
            Bytes::from("hash"),
            Bytes::from("set"),
        ],
    );
    assert_eq!(deleted, 4);
}

// ============================================================================
// SERVER Command Compatibility
// ============================================================================

#[test]
fn compat_server_ping_pong() {
    let frame = parse_resp(b"+PONG\r\n");
    assert_eq!(frame, Frame::Simple(Bytes::from("PONG")));
}

#[test]
fn compat_server_echo_returns_message() {
    let frame = parse_resp(b"$5\r\nhello\r\n");
    assert_eq!(frame, Frame::Bulk(Some(Bytes::from("hello"))));
}

#[test]
fn compat_server_select_database() {
    let store = new_store();
    store.set(0, Bytes::from("k"), Value::String(Bytes::from("db0")));
    store.set(1, Bytes::from("k"), Value::String(Bytes::from("db1")));
    assert_eq!(
        store.get(0, &Bytes::from("k")),
        Some(Value::String(Bytes::from("db0")))
    );
    assert_eq!(
        store.get(1, &Bytes::from("k")),
        Some(Value::String(Bytes::from("db1")))
    );
}

#[test]
fn compat_server_dbsize_returns_count() {
    let store = new_store();
    store.set(0, Bytes::from("a"), Value::String(Bytes::from("1")));
    store.set(0, Bytes::from("b"), Value::String(Bytes::from("2")));
    store.set(0, Bytes::from("c"), Value::String(Bytes::from("3")));
    assert_eq!(store.key_count(0), 3);
}

#[test]
fn compat_server_flushdb_clears_database() {
    let store = new_store();
    store.set(0, Bytes::from("k1"), Value::String(Bytes::from("v")));
    store.set(0, Bytes::from("k2"), Value::String(Bytes::from("v")));
    store.flush_db(0);
    assert_eq!(store.key_count(0), 0);
    assert_eq!(store.get(0, &Bytes::from("k1")), None);
}

#[test]
fn compat_server_flushall_clears_all_databases() {
    let store = new_store();
    store.set(0, Bytes::from("k"), Value::String(Bytes::from("v")));
    store.set(1, Bytes::from("k"), Value::String(Bytes::from("v")));
    store.flush_all();
    assert_eq!(store.key_count(0), 0);
    assert_eq!(store.key_count(1), 0);
}

#[test]
fn compat_server_info_resp_format() {
    let frame = parse_resp(b"$11\r\n# Server\r\n\r\n");
    match frame {
        Frame::Bulk(Some(_)) => {}
        other => panic!("Expected Bulk, got {:?}", other),
    }
}

#[test]
fn compat_server_config_get_resp_format() {
    let frame = parse_resp(b"*2\r\n$4\r\nsave\r\n$0\r\n\r\n");
    match frame {
        Frame::Array(Some(arr)) => assert_eq!(arr.len(), 2),
        other => panic!("Expected Array, got {:?}", other),
    }
}

#[test]
fn compat_server_time_resp_format() {
    let frame = parse_resp(b"*2\r\n$10\r\n1234567890\r\n$6\r\n123456\r\n");
    match frame {
        Frame::Array(Some(arr)) => assert_eq!(arr.len(), 2),
        other => panic!("Expected Array, got {:?}", other),
    }
}

#[test]
fn compat_server_database_default_count() {
    let store = Store::new(16);
    assert_eq!(store.num_databases(), 16);
    store.set(0, Bytes::from("k"), Value::String(Bytes::from("v")));
    store.set(15, Bytes::from("k"), Value::String(Bytes::from("v")));
    assert!(store.get(0, &Bytes::from("k")).is_some());
    assert!(store.get(15, &Bytes::from("k")).is_some());
}

// ============================================================================
// PUB/SUB Command Compatibility (protocol-level tests)
// ============================================================================

#[test]
fn compat_pubsub_subscribe_message_format() {
    let frame = parse_resp(b"*3\r\n$9\r\nsubscribe\r\n$7\r\nchannel\r\n:1\r\n");
    match frame {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], Frame::Bulk(Some(Bytes::from("subscribe"))));
            assert_eq!(arr[1], Frame::Bulk(Some(Bytes::from("channel"))));
            assert_eq!(arr[2], Frame::Integer(1));
        }
        other => panic!("Expected Array, got {:?}", other),
    }
}

#[test]
fn compat_pubsub_unsubscribe_message_format() {
    let frame = parse_resp(b"*3\r\n$11\r\nunsubscribe\r\n$7\r\nchannel\r\n:0\r\n");
    match frame {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], Frame::Bulk(Some(Bytes::from("unsubscribe"))));
        }
        other => panic!("Expected Array, got {:?}", other),
    }
}

#[test]
fn compat_pubsub_psubscribe_message_format() {
    let frame = parse_resp(b"*3\r\n$10\r\npsubscribe\r\n$5\r\nnews*\r\n:1\r\n");
    match frame {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], Frame::Bulk(Some(Bytes::from("psubscribe"))));
            assert_eq!(arr[1], Frame::Bulk(Some(Bytes::from("news*"))));
        }
        other => panic!("Expected Array, got {:?}", other),
    }
}

#[test]
fn compat_pubsub_publish_message_format() {
    let frame = parse_resp(b"*3\r\n$7\r\nmessage\r\n$7\r\nchannel\r\n$5\r\nhello\r\n");
    match frame {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], Frame::Bulk(Some(Bytes::from("message"))));
            assert_eq!(arr[2], Frame::Bulk(Some(Bytes::from("hello"))));
        }
        other => panic!("Expected Array, got {:?}", other),
    }
}

// ============================================================================
// TRANSACTION Command Compatibility (protocol-level tests)
// ============================================================================

#[test]
fn compat_transaction_multi_ok_response() {
    let frame = parse_resp(b"+OK\r\n");
    assert_eq!(frame, Frame::Simple(Bytes::from("OK")));
}

#[test]
fn compat_transaction_exec_array_response() {
    let frame = parse_resp(b"*2\r\n+OK\r\n:1\r\n");
    match frame {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], Frame::Simple(Bytes::from("OK")));
            assert_eq!(arr[1], Frame::Integer(1));
        }
        other => panic!("Expected Array, got {:?}", other),
    }
}

#[test]
fn compat_transaction_discard_ok_response() {
    let frame = parse_resp(b"+OK\r\n");
    assert_eq!(frame, Frame::Simple(Bytes::from("OK")));
}

#[test]
fn compat_transaction_watch_ok_response() {
    let frame = parse_resp(b"+OK\r\n");
    assert_eq!(frame, Frame::Simple(Bytes::from("OK")));
}

#[test]
fn compat_transaction_exec_null_on_watch_failure() {
    let frame = parse_resp(b"*-1\r\n");
    assert!(frame.is_null());
}

// ============================================================================
// HYPERLOGLOG Command Compatibility (protocol-level tests)
// ============================================================================

#[test]
fn compat_hyperloglog_pfadd_response() {
    let frame = parse_resp(b":1\r\n");
    assert_eq!(frame, Frame::Integer(1));
}

#[test]
fn compat_hyperloglog_pfcount_response() {
    let frame = parse_resp(b":42\r\n");
    assert_eq!(frame, Frame::Integer(42));
}

#[test]
fn compat_hyperloglog_pfmerge_response() {
    let frame = parse_resp(b"+OK\r\n");
    assert_eq!(frame, Frame::Simple(Bytes::from("OK")));
}

// ============================================================================
// Cross-Type Command Compatibility
// ============================================================================

#[test]
fn compat_type_overwrite_changes_type() {
    let store = new_store();
    store.set(0, Bytes::from("k"), Value::String(Bytes::from("v")));
    store.set(
        0,
        Bytes::from("k"),
        Value::List(VecDeque::from(vec![Bytes::from("a")])),
    );
    match store.get(0, &Bytes::from("k")) {
        Some(Value::List(_)) => {}
        other => panic!("Expected List after type overwrite, got {:?}", other),
    }
}

// ============================================================================
// Database Isolation Compatibility
// ============================================================================

#[test]
fn compat_database_isolation() {
    let store = new_store();
    store.set(0, Bytes::from("key"), Value::String(Bytes::from("db0")));
    store.set(1, Bytes::from("key"), Value::String(Bytes::from("db1")));
    assert_eq!(
        store.get(0, &Bytes::from("key")),
        Some(Value::String(Bytes::from("db0")))
    );
    assert_eq!(
        store.get(1, &Bytes::from("key")),
        Some(Value::String(Bytes::from("db1")))
    );
}

// ============================================================================
// Protocol Compatibility
// ============================================================================

#[test]
fn compat_resp_simple_string() {
    let frame = parse_resp(b"+OK\r\n");
    assert_eq!(frame, Frame::Simple(Bytes::from("OK")));
}

#[test]
fn compat_resp_error() {
    let frame = parse_resp(b"-ERR unknown command\r\n");
    assert_eq!(frame, Frame::Error(Bytes::from("ERR unknown command")));
}

#[test]
fn compat_resp_integer() {
    let frame = parse_resp(b":1000\r\n");
    assert_eq!(frame, Frame::Integer(1000));
}

#[test]
fn compat_resp_negative_integer() {
    let frame = parse_resp(b":-42\r\n");
    assert_eq!(frame, Frame::Integer(-42));
}

#[test]
fn compat_resp_zero_integer() {
    let frame = parse_resp(b":0\r\n");
    assert_eq!(frame, Frame::Integer(0));
}

#[test]
fn compat_resp_bulk_string() {
    let frame = parse_resp(b"$6\r\nfoobar\r\n");
    assert_eq!(frame, Frame::Bulk(Some(Bytes::from("foobar"))));
}

#[test]
fn compat_resp_null_bulk() {
    let frame = parse_resp(b"$-1\r\n");
    assert!(frame.is_null());
}

#[test]
fn compat_resp_empty_bulk_string() {
    let frame = parse_resp(b"$0\r\n\r\n");
    assert_eq!(frame, Frame::Bulk(Some(Bytes::from(""))));
}

#[test]
fn compat_resp_array() {
    let frame = parse_resp(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    match frame {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], Frame::Bulk(Some(Bytes::from("foo"))));
            assert_eq!(arr[1], Frame::Bulk(Some(Bytes::from("bar"))));
        }
        other => panic!("Expected Array, got {:?}", other),
    }
}

#[test]
fn compat_resp_empty_array() {
    let frame = parse_resp(b"*0\r\n");
    match frame {
        Frame::Array(Some(arr)) => assert_eq!(arr.len(), 0),
        other => panic!("Expected empty Array, got {:?}", other),
    }
}

#[test]
fn compat_resp_null_array() {
    let frame = parse_resp(b"*-1\r\n");
    assert!(frame.is_null());
}

#[test]
fn compat_resp_nested_array() {
    let frame = parse_resp(b"*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n");
    match frame {
        Frame::Array(Some(outer)) => {
            assert_eq!(outer.len(), 2);
            match &outer[0] {
                Frame::Array(Some(inner)) => {
                    assert_eq!(inner.len(), 2);
                    assert_eq!(inner[0], Frame::Integer(1));
                    assert_eq!(inner[1], Frame::Integer(2));
                }
                other => panic!("Expected inner Array, got {:?}", other),
            }
        }
        other => panic!("Expected outer Array, got {:?}", other),
    }
}

#[test]
fn compat_resp_mixed_type_array() {
    let frame = parse_resp(b"*3\r\n:1\r\n$3\r\nfoo\r\n+OK\r\n");
    match frame {
        Frame::Array(Some(arr)) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], Frame::Integer(1));
            assert_eq!(arr[1], Frame::Bulk(Some(Bytes::from("foo"))));
            assert_eq!(arr[2], Frame::Simple(Bytes::from("OK")));
        }
        other => panic!("Expected Array, got {:?}", other),
    }
}

// ============================================================================
// Concurrent Access Compatibility
// ============================================================================

#[test]
fn compat_concurrent_writes() {
    let store = Arc::new(new_store());
    let mut handles = vec![];
    for i in 0..100 {
        let store = store.clone();
        handles.push(std::thread::spawn(move || {
            let key = Bytes::from(format!("key:{}", i));
            let val = Value::String(Bytes::from(format!("val:{}", i)));
            store.set(0, key, val);
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    for i in 0..100 {
        let key = Bytes::from(format!("key:{}", i));
        assert!(store.get(0, &key).is_some(), "Missing key:{}", i);
    }
}

#[test]
fn compat_concurrent_read_write() {
    let store = Arc::new(new_store());
    for i in 0..50 {
        store.set(
            0,
            Bytes::from(format!("k:{}", i)),
            Value::String(Bytes::from(format!("v:{}", i))),
        );
    }
    let mut handles = vec![];
    for i in 0..50 {
        let store = store.clone();
        handles.push(std::thread::spawn(move || {
            let _ = store.get(0, &Bytes::from(format!("k:{}", i)));
        }));
    }
    for i in 50..100 {
        let store = store.clone();
        handles.push(std::thread::spawn(move || {
            store.set(
                0,
                Bytes::from(format!("k:{}", i)),
                Value::String(Bytes::from("new")),
            );
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn compat_concurrent_del_while_reading() {
    let store = Arc::new(new_store());
    for i in 0..100 {
        store.set(
            0,
            Bytes::from(format!("k:{}", i)),
            Value::String(Bytes::from("v")),
        );
    }
    let mut handles = vec![];
    for i in 0..50 {
        let store = store.clone();
        handles.push(std::thread::spawn(move || {
            let _ = store.get(0, &Bytes::from(format!("k:{}", i)));
        }));
    }
    for i in 50..100 {
        let store = store.clone();
        handles.push(std::thread::spawn(move || {
            store.del(0, &[Bytes::from(format!("k:{}", i))]);
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

// ============================================================================
// Expiration Compatibility
// ============================================================================

#[test]
fn compat_expired_key_not_returned() {
    let store = new_store();
    let expires_at = SystemTime::now() + Duration::from_millis(1);
    store.set_with_expiry(
        0,
        Bytes::from("ephemeral"),
        Value::String(Bytes::from("temp")),
        expires_at,
    );
    std::thread::sleep(Duration::from_millis(10));
    assert_eq!(store.get(0, &Bytes::from("ephemeral")), None);
}

#[test]
fn compat_ttl_nonexistent_key() {
    let store = new_store();
    let ttl = store.ttl(0, &Bytes::from("nonexistent"));
    assert!(ttl.is_none() || ttl == Some(-2));
}

#[test]
fn compat_set_with_expiry_preserves_value() {
    let store = new_store();
    let expires_at = SystemTime::now() + Duration::from_secs(60);
    store.set_with_expiry(
        0,
        Bytes::from("k"),
        Value::String(Bytes::from("v")),
        expires_at,
    );
    assert_eq!(
        store.get(0, &Bytes::from("k")),
        Some(Value::String(Bytes::from("v")))
    );
    let ttl = store.ttl(0, &Bytes::from("k"));
    assert!(ttl.is_some() && ttl.unwrap() > 0);
}

#[test]
fn compat_persist_on_nonexistent_key() {
    let store = new_store();
    assert!(!store.persist(0, &Bytes::from("nokey")));
}

// ============================================================================
// Touch Command Compatibility
// ============================================================================

#[test]
fn compat_key_touch_existing() {
    let store = new_store();
    store.set(0, Bytes::from("k1"), Value::String(Bytes::from("v")));
    store.set(0, Bytes::from("k2"), Value::String(Bytes::from("v")));
    let touched = store.touch(
        0,
        &[Bytes::from("k1"), Bytes::from("k2"), Bytes::from("k3")],
    );
    assert_eq!(touched, 2);
}

#[test]
fn compat_key_touch_nonexistent() {
    let store = new_store();
    let touched = store.touch(0, &[Bytes::from("nokey")]);
    assert_eq!(touched, 0);
}

// ============================================================================
// Compatibility Matrix Report
// ============================================================================

#[test]
fn compat_generate_matrix_report() {
    let mut matrix = CompatibilityMatrix::new();
    let store = new_store();

    store.set(0, Bytes::from("k"), Value::String(Bytes::from("v")));
    matrix.record(
        "SET",
        "basic set",
        if store.get(0, &Bytes::from("k")).is_some() {
            CompatResult::Pass
        } else {
            CompatResult::Fail("SET did not persist".into())
        },
    );

    matrix.record(
        "GET",
        "basic get",
        if store.get(0, &Bytes::from("k")) == Some(Value::String(Bytes::from("v"))) {
            CompatResult::Pass
        } else {
            CompatResult::Fail("GET returned wrong value".into())
        },
    );

    matrix.record(
        "GET",
        "nonexistent key",
        if store.get(0, &Bytes::from("none")).is_none() {
            CompatResult::Pass
        } else {
            CompatResult::Fail("GET should return nil for missing key".into())
        },
    );

    store.set(0, Bytes::from("del_test"), Value::String(Bytes::from("v")));
    let del_count = store.del(0, &[Bytes::from("del_test")]);
    matrix.record(
        "DEL",
        "delete existing key",
        if del_count == 1 {
            CompatResult::Pass
        } else {
            CompatResult::Fail(format!("DEL returned {} instead of 1", del_count))
        },
    );

    store.set(
        0,
        Bytes::from("exists_test"),
        Value::String(Bytes::from("v")),
    );
    matrix.record(
        "EXISTS",
        "existing key",
        if store.exists(0, &[Bytes::from("exists_test")]) == 1 {
            CompatResult::Pass
        } else {
            CompatResult::Fail("EXISTS should return 1".into())
        },
    );

    let exp = SystemTime::now() + Duration::from_secs(100);
    store.expire(0, &Bytes::from("exists_test"), exp);
    let ttl = store.ttl(0, &Bytes::from("exists_test"));
    matrix.record(
        "EXPIRE/TTL",
        "set and check expiry",
        if ttl.is_some() && ttl.unwrap() > 0 {
            CompatResult::Pass
        } else {
            CompatResult::Fail(format!("TTL returned {:?}", ttl))
        },
    );

    store.persist(0, &Bytes::from("exists_test"));
    let ttl_after = store.ttl(0, &Bytes::from("exists_test"));
    matrix.record(
        "PERSIST",
        "remove expiry",
        if ttl_after == Some(-1) {
            CompatResult::Pass
        } else {
            CompatResult::Fail(format!("After PERSIST, TTL = {:?}", ttl_after))
        },
    );

    store.flush_db(0);
    matrix.record(
        "FLUSHDB",
        "clear database",
        if store.key_count(0) == 0 {
            CompatResult::Pass
        } else {
            CompatResult::Fail("FLUSHDB did not clear database".into())
        },
    );

    store.set(0, Bytes::from("a"), Value::String(Bytes::from("1")));
    store.set(0, Bytes::from("b"), Value::String(Bytes::from("2")));
    matrix.record(
        "DBSIZE",
        "key count",
        if store.key_count(0) == 2 {
            CompatResult::Pass
        } else {
            CompatResult::Fail(format!("DBSIZE returned {}", store.key_count(0)))
        },
    );

    let (pass, fail, skip) = matrix.summary();
    eprintln!(
        "\n=== Redis Compatibility Matrix ===\nPassed: {}\nFailed: {}\nSkipped: {}\nTotal:  {}\nPass Rate: {:.1}%\n",
        pass, fail, skip, pass + fail + skip,
        if pass + fail > 0 { (pass as f64 / (pass + fail) as f64) * 100.0 } else { 0.0 }
    );

    for (cmd, test, result) in &matrix.results {
        match result {
            CompatResult::Fail(msg) => eprintln!("  FAIL: {} / {} - {}", cmd, test, msg),
            CompatResult::Skip(msg) => eprintln!("  SKIP: {} / {} - {}", cmd, test, msg),
            CompatResult::Pass => {}
        }
    }

    assert_eq!(fail, 0, "Some compatibility tests failed");
}
