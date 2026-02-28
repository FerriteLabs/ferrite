//! Property-based tests for persistence correctness
#![allow(clippy::unwrap_used)]

use proptest::prelude::*;

use bytes::Bytes;
use std::collections::{HashMap, HashSet, VecDeque};

use ferrite::persistence::{generate_rdb, load_rdb, replay_aof, AofCommand, AofEntry};
use ferrite::storage::{Store, Value};

// ============================================================================
// Test Strategies
// ============================================================================

fn key_strategy() -> impl Strategy<Value = String> {
    "[a-z]{1,20}"
}

fn value_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 1..100)
}

/// Strategy for generating a random AOF Set command
fn aof_set_strategy() -> impl Strategy<Value = (String, Vec<u8>)> {
    (key_strategy(), value_strategy())
}

// ============================================================================
// AOF Replay Properties
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    /// Write N SET operations, replay AOF, verify same state
    #[test]
    fn prop_aof_replay_consistent(
        ops in prop::collection::vec(aof_set_strategy(), 1..50)
    ) {
        // Build expected state by applying ops to a reference store
        let reference_store = Store::new(16);
        let mut entries = Vec::new();

        for (key, value) in &ops {
            let k = Bytes::from(key.clone());
            let v = Bytes::from(value.clone());
            reference_store.set(0, k, Value::String(v));

            entries.push(AofEntry::new(
                0,
                AofCommand::Set {
                    key: key.as_bytes().to_vec(),
                    value: value.clone(),
                },
            ));
        }

        // Replay AOF entries into a fresh store
        let replay_store = Store::new(16);
        replay_aof(entries, &replay_store);

        // Verify both stores have identical state
        let ref_keys = reference_store.keys(0);
        let replay_keys = replay_store.keys(0);

        // Same number of keys
        prop_assert_eq!(ref_keys.len(), replay_keys.len(),
            "Key count mismatch: ref={}, replay={}", ref_keys.len(), replay_keys.len());

        // Each key has the same value
        for key in &ref_keys {
            let ref_val = reference_store.get(0, key);
            let replay_val = replay_store.get(0, key);
            prop_assert_eq!(ref_val, replay_val,
                "Value mismatch for key {:?}", key);
        }
    }

    /// AOF replay with mixed Set and Del operations
    #[test]
    fn prop_aof_replay_with_deletes(
        sets in prop::collection::vec(aof_set_strategy(), 5..30),
        del_indices in prop::collection::vec(0usize..30, 1..10)
    ) {
        let reference_store = Store::new(16);
        let mut entries = Vec::new();

        // Apply all SETs
        for (key, value) in &sets {
            let k = Bytes::from(key.clone());
            let v = Bytes::from(value.clone());
            reference_store.set(0, k, Value::String(v));

            entries.push(AofEntry::new(
                0,
                AofCommand::Set {
                    key: key.as_bytes().to_vec(),
                    value: value.clone(),
                },
            ));
        }

        // Apply some DELs
        let unique_keys: Vec<_> = sets.iter().map(|(k, _)| k.clone()).collect();
        for &idx in &del_indices {
            let idx = idx % unique_keys.len();
            let key = &unique_keys[idx];
            reference_store.del(0, &[Bytes::from(key.clone())]);

            entries.push(AofEntry::new(
                0,
                AofCommand::Del {
                    keys: vec![key.as_bytes().to_vec()],
                },
            ));
        }

        // Replay into fresh store
        let replay_store = Store::new(16);
        replay_aof(entries, &replay_store);

        // Verify identical state
        let ref_keys = reference_store.keys(0);
        let replay_keys = replay_store.keys(0);
        prop_assert_eq!(ref_keys.len(), replay_keys.len());

        for key in &ref_keys {
            let ref_val = reference_store.get(0, key);
            let replay_val = replay_store.get(0, key);
            prop_assert_eq!(ref_val, replay_val);
        }
    }
}

// ============================================================================
// RDB Checkpoint Roundtrip Properties
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(30))]

    /// Checkpoint (RDB) → restore → verify identical data for strings
    #[test]
    fn prop_checkpoint_roundtrip(
        entries in prop::collection::vec(aof_set_strategy(), 1..50)
    ) {
        let source_store = Store::new(16);

        // Populate source store
        for (key, value) in &entries {
            source_store.set(
                0,
                Bytes::from(key.clone()),
                Value::String(Bytes::from(value.clone())),
            );
        }

        // Generate RDB snapshot
        let rdb_data = generate_rdb(&source_store);

        // Load into a fresh store
        let target_store = Store::new(16);
        let loaded = load_rdb(&rdb_data, &target_store).unwrap();

        // Verify key count
        let source_keys = source_store.keys(0);
        let target_keys = target_store.keys(0);
        prop_assert_eq!(source_keys.len(), target_keys.len(),
            "Key count: source={}, target={}, loaded={}", source_keys.len(), target_keys.len(), loaded);

        // Verify each key-value pair
        for key in &source_keys {
            let source_val = source_store.get(0, key);
            let target_val = target_store.get(0, key);
            prop_assert_eq!(source_val, target_val,
                "Mismatch for key {:?}", key);
        }
    }

    /// Checkpoint roundtrip with multiple data types
    #[test]
    fn prop_checkpoint_roundtrip_multi_type(
        string_entries in prop::collection::vec(aof_set_strategy(), 1..10),
        list_key in key_strategy(),
        list_values in prop::collection::vec(value_strategy(), 1..20),
        hash_key in key_strategy(),
        hash_fields in prop::collection::hash_map(key_strategy(), value_strategy(), 1..10),
        set_key in key_strategy(),
        set_members in prop::collection::hash_set(key_strategy(), 1..15),
    ) {
        let source_store = Store::new(16);

        // Add strings
        for (key, value) in &string_entries {
            source_store.set(0, Bytes::from(key.clone()), Value::String(Bytes::from(value.clone())));
        }

        // Add a list
        let list: VecDeque<Bytes> = list_values.iter().map(|v| Bytes::from(v.clone())).collect();
        source_store.set(0, Bytes::from(list_key.clone()), Value::List(list));

        // Add a hash
        let hash: HashMap<Bytes, Bytes> = hash_fields
            .iter()
            .map(|(k, v)| (Bytes::from(k.clone()), Bytes::from(v.clone())))
            .collect();
        source_store.set(0, Bytes::from(hash_key.clone()), Value::Hash(hash));

        // Add a set
        let set: HashSet<Bytes> = set_members.iter().map(|m| Bytes::from(m.clone())).collect();
        source_store.set(0, Bytes::from(set_key.clone()), Value::Set(set));

        // Generate and reload RDB
        let rdb_data = generate_rdb(&source_store);
        let target_store = Store::new(16);
        load_rdb(&rdb_data, &target_store).unwrap();

        // Verify all keys exist in target
        let source_keys = source_store.keys(0);
        for key in &source_keys {
            let source_val = source_store.get(0, key);
            let target_val = target_store.get(0, key);
            prop_assert_eq!(source_val, target_val,
                "Data mismatch for key {:?}", key);
        }
    }
}
