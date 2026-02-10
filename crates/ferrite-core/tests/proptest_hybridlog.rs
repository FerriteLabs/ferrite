//! Property-based tests for HybridLog storage engine correctness.
//!
//! These tests verify invariants that must hold regardless of input data,
//! operation ordering, or concurrency patterns.

use bytes::Bytes;
use proptest::prelude::*;
use std::collections::HashMap;

use ferrite_core::storage::{Store, Value};

// ============================================================================
// Test Strategies
// ============================================================================

/// Strategy: generate arbitrary Redis-like keys
fn key_strategy() -> impl Strategy<Value = String> {
    "[a-z]{1,8}:[a-z0-9]{1,16}"
}

/// Strategy: generate arbitrary values (various sizes)
fn value_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..1024)
}

// ============================================================================
// Storage Engine Property Tests
// ============================================================================

proptest! {
    /// GET after SET always returns the exact value that was set.
    #[test]
    fn prop_set_get_roundtrip(
        key in key_strategy(),
        value in value_strategy(),
    ) {
        let store = Store::new(16);
        let key_bytes = Bytes::from(key);
        let val = Value::String(Bytes::from(value.clone()));
        store.set(0, key_bytes.clone(), val);

        match store.get(0, &key_bytes) {
            Some(Value::String(data)) => {
                prop_assert_eq!(data.as_ref(), &value[..]);
            }
            other => prop_assert!(false, "Expected String value, got {:?}", other),
        }
    }

    /// DEL after SET always removes the key.
    #[test]
    fn prop_del_removes_key(
        key in key_strategy(),
        value in value_strategy(),
    ) {
        let store = Store::new(16);
        let key_bytes = Bytes::from(key);
        store.set(0, key_bytes.clone(), Value::String(Bytes::from(value)));
        let deleted = store.del(0, &[key_bytes.clone()]);
        prop_assert_eq!(deleted, 1);
        prop_assert!(store.get(0, &key_bytes).is_none());
    }

    /// Multiple SETs to same key — last write wins.
    #[test]
    fn prop_last_write_wins(
        key in key_strategy(),
        values in prop::collection::vec(value_strategy(), 2..10),
    ) {
        let store = Store::new(16);
        let key_bytes = Bytes::from(key);
        let last = values.last().unwrap().clone();

        for v in &values {
            store.set(0, key_bytes.clone(), Value::String(Bytes::from(v.clone())));
        }

        match store.get(0, &key_bytes) {
            Some(Value::String(data)) => {
                prop_assert_eq!(data.as_ref(), &last[..]);
            }
            other => prop_assert!(false, "Expected String value, got {:?}", other),
        }
    }

    /// key_count equals number of distinct live keys.
    #[test]
    fn prop_dbsize_consistent(
        ops in prop::collection::vec(
            (key_strategy(), prop::bool::ANY),
            1..50,
        ),
    ) {
        let store = Store::new(16);
        let mut live_keys = std::collections::HashSet::new();

        for (key, is_set) in &ops {
            let key_bytes = Bytes::from(key.clone());
            if *is_set {
                store.set(0, key_bytes, Value::String(Bytes::from("v")));
                live_keys.insert(key.clone());
            } else {
                store.del(0, &[key_bytes]);
                live_keys.remove(key);
            }
        }

        prop_assert_eq!(store.key_count(0), live_keys.len() as u64);
    }

    /// Model-based test: Store behaves like a HashMap.
    #[test]
    fn prop_store_matches_hashmap_model(
        ops in prop::collection::vec(
            prop_oneof![
                (key_strategy(), value_strategy()).prop_map(|(k, v)| ("SET", k, v)),
                (key_strategy(), Just(vec![])).prop_map(|(k, v)| ("DEL", k, v)),
                (key_strategy(), Just(vec![])).prop_map(|(k, v)| ("GET", k, v)),
            ],
            1..100,
        ),
    ) {
        let store = Store::new(16);
        let mut model: HashMap<String, Vec<u8>> = HashMap::new();

        for (op, key, value) in &ops {
            let key_bytes = Bytes::from(key.clone());
            match op.as_ref() {
                "SET" => {
                    store.set(0, key_bytes, Value::String(Bytes::from(value.clone())));
                    model.insert(key.clone(), value.clone());
                }
                "DEL" => {
                    store.del(0, &[key_bytes]);
                    model.remove(key);
                }
                "GET" => {
                    let store_result = store.get(0, &key_bytes);
                    let model_result = model.get(key);
                    match (store_result, model_result) {
                        (Some(Value::String(data)), Some(expected)) => {
                            prop_assert_eq!(data.as_ref(), &expected[..]);
                        }
                        (None, None) => {} // Both agree key doesn't exist
                        (Some(_), None) => prop_assert!(false, "Store has key but model doesn't"),
                        (None, Some(_)) => prop_assert!(false, "Model has key but store doesn't"),
                        _ => prop_assert!(false, "Unexpected value type in store"),
                    }
                }
                _ => unreachable!(),
            }
        }

        // Final consistency check
        prop_assert_eq!(store.key_count(0), model.len() as u64);
    }
}
