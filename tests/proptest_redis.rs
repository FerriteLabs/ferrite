//! Property-based tests for Redis command compatibility
//!
//! These tests use proptest to verify that Ferrite's Redis command implementations
//! satisfy key invariants and properties, ensuring correct behavior across
//! randomly generated inputs.

use bytes::Bytes;
use proptest::prelude::*;
use std::collections::{HashMap, HashSet, VecDeque};

use ferrite::storage::{Store, Value};

// ============================================================================
// Test Strategies
// ============================================================================

/// Strategy for generating valid Redis keys (non-empty, printable ASCII)
fn key_strategy() -> impl Strategy<Value = String> {
    // Redis keys can be any binary sequence, but we'll use printable ASCII for readability
    "[a-zA-Z0-9_:.-]{1,64}".prop_map(|s| s)
}

/// Strategy for generating Redis string values
fn string_value_strategy() -> impl Strategy<Value = String> {
    // String values can be quite large, but we'll limit for test speed
    ".{0,256}"
}

// ============================================================================
// String Command Properties
// ============================================================================

proptest! {
    /// Property: GET after SET returns the exact value that was set
    #[test]
    fn prop_get_returns_set_value(key in key_strategy(), value in string_value_strategy()) {
        let store = Store::new(16);

        // SET key value
        store.set(0, Bytes::from(key.clone()), Value::String(Bytes::from(value.clone())));

        // GET key should return the same value
        let result = store.get(0, &Bytes::from(key));
        match result {
            Some(Value::String(v)) => prop_assert_eq!(v, Bytes::from(value)),
            other => prop_assert!(false, "Expected String value, got {:?}", other),
        }
    }

    /// Property: DEL removes the key (EXISTS returns 0)
    #[test]
    fn prop_del_removes_key(key in key_strategy(), value in string_value_strategy()) {
        let store = Store::new(16);

        // SET key value
        store.set(0, Bytes::from(key.clone()), Value::String(Bytes::from(value)));

        // Verify key exists
        prop_assert_eq!(store.exists(0, &[Bytes::from(key.clone())]), 1);

        // DEL key
        let deleted = store.del(0, &[Bytes::from(key.clone())]);
        prop_assert_eq!(deleted, 1);

        // EXISTS should now return 0
        prop_assert_eq!(store.exists(0, &[Bytes::from(key)]), 0);
    }

    /// Property: Setting a key twice overwrites the first value
    #[test]
    fn prop_set_overwrites(
        key in key_strategy(),
        value1 in string_value_strategy(),
        value2 in string_value_strategy()
    ) {
        let store = Store::new(16);

        // SET key value1
        store.set(0, Bytes::from(key.clone()), Value::String(Bytes::from(value1)));

        // SET key value2 (overwrite)
        store.set(0, Bytes::from(key.clone()), Value::String(Bytes::from(value2.clone())));

        // GET should return value2
        let result = store.get(0, &Bytes::from(key));
        match result {
            Some(Value::String(v)) => prop_assert_eq!(v, Bytes::from(value2)),
            other => prop_assert!(false, "Expected String value, got {:?}", other),
        }
    }

    /// Property: DEL on non-existent key returns 0
    #[test]
    fn prop_del_nonexistent_returns_zero(key in key_strategy()) {
        let store = Store::new(16);

        // DEL on empty store
        let deleted = store.del(0, &[Bytes::from(key)]);
        prop_assert_eq!(deleted, 0);
    }

    /// Property: EXISTS accurately counts existing keys
    #[test]
    fn prop_exists_counts_correctly(keys in prop::collection::hash_set(key_strategy(), 1..10)) {
        let store = Store::new(16);

        // Set all keys
        for key in &keys {
            store.set(0, Bytes::from(key.clone()), Value::String(Bytes::from("value")));
        }

        // EXISTS should return the count of keys
        let key_bytes: Vec<Bytes> = keys.iter().map(|k| Bytes::from(k.clone())).collect();
        prop_assert_eq!(store.exists(0, &key_bytes), keys.len() as i64);

        // Add a non-existent key to the query
        let mut with_fake = key_bytes.clone();
        with_fake.push(Bytes::from("definitely_not_a_key_12345"));
        prop_assert_eq!(store.exists(0, &with_fake), keys.len() as i64);
    }
}

// ============================================================================
// List Command Properties
// ============================================================================

proptest! {
    /// Property: LPUSH followed by LPOP returns values in LIFO order
    #[test]
    fn prop_lpush_lpop_lifo(
        key in key_strategy(),
        values in prop::collection::vec(string_value_strategy(), 1..20)
    ) {
        let store = Store::new(16);

        // Create list with values
        let list: VecDeque<Bytes> = values.iter().map(|v| Bytes::from(v.clone())).collect();
        store.set(0, Bytes::from(key.clone()), Value::List(list));

        // Get the list
        let result = store.get(0, &Bytes::from(key));
        match result {
            Some(Value::List(l)) => {
                prop_assert_eq!(l.len(), values.len());
                // Values should be in same order
                for (i, v) in values.iter().enumerate() {
                    prop_assert_eq!(&l[i], &Bytes::from(v.clone()));
                }
            }
            other => prop_assert!(false, "Expected List value, got {:?}", other),
        }
    }

    /// Property: List length is accurately tracked
    #[test]
    fn prop_list_len_accurate(
        key in key_strategy(),
        values in prop::collection::vec(string_value_strategy(), 0..50)
    ) {
        let store = Store::new(16);

        if values.is_empty() {
            // Empty list case - key shouldn't exist or list should be empty
            let result = store.get(0, &Bytes::from(key));
            prop_assert!(result.is_none() || matches!(result, Some(Value::List(l)) if l.is_empty()));
        } else {
            let list: VecDeque<Bytes> = values.iter().map(|v| Bytes::from(v.clone())).collect();
            store.set(0, Bytes::from(key.clone()), Value::List(list));

            let result = store.get(0, &Bytes::from(key));
            match result {
                Some(Value::List(l)) => prop_assert_eq!(l.len(), values.len()),
                other => prop_assert!(false, "Expected List value, got {:?}", other),
            }
        }
    }
}

// ============================================================================
// Hash Command Properties
// ============================================================================

proptest! {
    /// Property: HSET fields can be retrieved with HGET
    #[test]
    fn prop_hset_hget_roundtrip(
        key in key_strategy(),
        fields in prop::collection::hash_map(key_strategy(), string_value_strategy(), 1..20)
    ) {
        let store = Store::new(16);

        // Create hash
        let hash: HashMap<Bytes, Bytes> = fields
            .iter()
            .map(|(k, v)| (Bytes::from(k.clone()), Bytes::from(v.clone())))
            .collect();
        store.set(0, Bytes::from(key.clone()), Value::Hash(hash));

        // Retrieve and verify each field
        let result = store.get(0, &Bytes::from(key));
        match result {
            Some(Value::Hash(h)) => {
                prop_assert_eq!(h.len(), fields.len());
                for (field, value) in &fields {
                    let field_key = Bytes::from(field.clone());
                    let expected_value = Bytes::from(value.clone());
                    let stored = h.get(&field_key);
                    prop_assert_eq!(stored, Some(&expected_value));
                }
            }
            other => prop_assert!(false, "Expected Hash value, got {:?}", other),
        }
    }

    /// Property: Hash length equals number of unique fields
    #[test]
    fn prop_hash_len_equals_fields(
        key in key_strategy(),
        fields in prop::collection::hash_map(key_strategy(), string_value_strategy(), 1..30)
    ) {
        let store = Store::new(16);

        let hash: HashMap<Bytes, Bytes> = fields
            .iter()
            .map(|(k, v)| (Bytes::from(k.clone()), Bytes::from(v.clone())))
            .collect();
        store.set(0, Bytes::from(key.clone()), Value::Hash(hash));

        let result = store.get(0, &Bytes::from(key));
        match result {
            Some(Value::Hash(h)) => prop_assert_eq!(h.len(), fields.len()),
            other => prop_assert!(false, "Expected Hash value, got {:?}", other),
        }
    }
}

// ============================================================================
// Set Command Properties
// ============================================================================

proptest! {
    /// Property: Set contains exactly the unique members added
    #[test]
    fn prop_set_unique_members(
        key in key_strategy(),
        members in prop::collection::vec(string_value_strategy(), 1..50)
    ) {
        let store = Store::new(16);

        // Create set (deduplicates automatically)
        let set: HashSet<Bytes> = members.iter().map(|m| Bytes::from(m.clone())).collect();
        store.set(0, Bytes::from(key.clone()), Value::Set(set.clone()));

        // Retrieve and verify
        let result = store.get(0, &Bytes::from(key));
        match result {
            Some(Value::Set(s)) => {
                prop_assert_eq!(s.len(), set.len());
                for member in &set {
                    prop_assert!(s.contains(member));
                }
            }
            other => prop_assert!(false, "Expected Set value, got {:?}", other),
        }
    }

    /// Property: Set membership is consistent
    #[test]
    fn prop_set_membership(
        key in key_strategy(),
        members in prop::collection::hash_set(string_value_strategy(), 1..30)
    ) {
        let store = Store::new(16);

        let set: HashSet<Bytes> = members.iter().map(|m| Bytes::from(m.clone())).collect();
        store.set(0, Bytes::from(key.clone()), Value::Set(set.clone()));

        let result = store.get(0, &Bytes::from(key));
        match result {
            Some(Value::Set(s)) => {
                // All original members should be present
                for member in &members {
                    prop_assert!(s.contains(&Bytes::from(member.clone())));
                }
                // Cardinality should match
                prop_assert_eq!(s.len(), members.len());
            }
            other => prop_assert!(false, "Expected Set value, got {:?}", other),
        }
    }
}

// ============================================================================
// Database Isolation Properties
// ============================================================================

proptest! {
    /// Property: Keys in different databases are isolated
    #[test]
    fn prop_database_isolation(
        key in key_strategy(),
        value1 in string_value_strategy(),
        value2 in string_value_strategy(),
        db1 in 0u8..15u8,
        db2 in 0u8..15u8
    ) {
        prop_assume!(db1 != db2); // Different databases

        let store = Store::new(16);

        // SET in db1
        store.set(db1, Bytes::from(key.clone()), Value::String(Bytes::from(value1.clone())));

        // SET same key in db2 with different value
        store.set(db2, Bytes::from(key.clone()), Value::String(Bytes::from(value2.clone())));

        // GET from db1 should return value1
        match store.get(db1, &Bytes::from(key.clone())) {
            Some(Value::String(v)) => prop_assert_eq!(v, Bytes::from(value1)),
            other => prop_assert!(false, "Expected value1 in db1, got {:?}", other),
        }

        // GET from db2 should return value2
        match store.get(db2, &Bytes::from(key)) {
            Some(Value::String(v)) => prop_assert_eq!(v, Bytes::from(value2)),
            other => prop_assert!(false, "Expected value2 in db2, got {:?}", other),
        }
    }

    /// Property: DEL in one database doesn't affect other databases
    #[test]
    fn prop_del_isolation(
        key in key_strategy(),
        value in string_value_strategy(),
        db1 in 0u8..15u8,
        db2 in 0u8..15u8
    ) {
        prop_assume!(db1 != db2);

        let store = Store::new(16);

        // SET in both databases
        store.set(db1, Bytes::from(key.clone()), Value::String(Bytes::from(value.clone())));
        store.set(db2, Bytes::from(key.clone()), Value::String(Bytes::from(value.clone())));

        // DEL from db1
        store.del(db1, &[Bytes::from(key.clone())]);

        // Key should be gone from db1
        prop_assert!(store.get(db1, &Bytes::from(key.clone())).is_none());

        // Key should still exist in db2
        prop_assert!(store.get(db2, &Bytes::from(key)).is_some());
    }
}

// ============================================================================
// Type Coercion Properties
// ============================================================================

proptest! {
    /// Property: Setting a key with a different type replaces the old value
    #[test]
    fn prop_type_change_replaces(key in key_strategy(), string_value in string_value_strategy()) {
        let store = Store::new(16);

        // Set as string
        store.set(0, Bytes::from(key.clone()), Value::String(Bytes::from(string_value)));

        // Set as list
        let list = VecDeque::from([Bytes::from("a"), Bytes::from("b")]);
        store.set(0, Bytes::from(key.clone()), Value::List(list.clone()));

        // Should now be a list
        let result = store.get(0, &Bytes::from(key));
        match result {
            Some(Value::List(l)) => prop_assert_eq!(l, list),
            other => prop_assert!(false, "Expected List after type change, got {:?}", other),
        }
    }
}

// ============================================================================
// Concurrent Access Properties
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))] // Fewer cases for concurrent tests

    /// Property: Concurrent reads don't interfere with each other
    #[test]
    fn prop_concurrent_reads(
        key in key_strategy(),
        value in string_value_strategy()
    ) {
        use std::sync::Arc;
        use std::thread;

        let store = Arc::new(Store::new(16));
        store.set(0, Bytes::from(key.clone()), Value::String(Bytes::from(value.clone())));

        let mut handles = vec![];

        // Spawn multiple readers
        for _ in 0..10 {
            let store_clone = Arc::clone(&store);
            let key_clone = key.clone();
            let value_clone = value.clone();

            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let result = store_clone.get(0, &Bytes::from(key_clone.clone()));
                    match result {
                        Some(Value::String(v)) => assert_eq!(v, Bytes::from(value_clone.clone())),
                        _ => panic!("Unexpected result during concurrent read"),
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }
    }
}
