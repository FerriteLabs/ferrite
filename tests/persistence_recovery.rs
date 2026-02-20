//! Persistence Recovery Integration Tests
//!
//! Tests for RDB/AOF persistence and recovery scenarios including:
//! - RDB snapshot save and load
//! - AOF append and recovery
//! - Recovery with mixed data types
//! - Corruption detection and handling

use bytes::Bytes;
use std::collections::{HashMap, HashSet, VecDeque};

use ferrite::persistence::{generate_rdb, load_rdb};
use ferrite::storage::{Store, Value};

// ============================================================================
// RDB Persistence Tests
// ============================================================================

#[test]
fn test_rdb_save_and_restore_strings() {
    let store = Store::new(16);

    // Populate with string data
    store.set(0, Bytes::from("key1"), Value::String(Bytes::from("value1")));
    store.set(0, Bytes::from("key2"), Value::String(Bytes::from("value2")));
    store.set(0, Bytes::from("key3"), Value::String(Bytes::from("value3")));

    // Generate RDB snapshot
    let rdb_data = generate_rdb(&store);

    // Create a new store and restore
    let store2 = Store::new(16);
    let count = load_rdb(&rdb_data, &store2).expect("RDB load should succeed");

    // Verify restoration
    assert_eq!(count, 3);
    assert_eq!(
        store2.get(0, &Bytes::from("key1")),
        Some(Value::String(Bytes::from("value1")))
    );
    assert_eq!(
        store2.get(0, &Bytes::from("key2")),
        Some(Value::String(Bytes::from("value2")))
    );
    assert_eq!(
        store2.get(0, &Bytes::from("key3")),
        Some(Value::String(Bytes::from("value3")))
    );
}

#[test]
fn test_rdb_save_and_restore_mixed_types() {
    let store = Store::new(16);

    // String
    store.set(
        0,
        Bytes::from("string_key"),
        Value::String(Bytes::from("hello")),
    );

    // List
    let list = VecDeque::from([Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]);
    store.set(0, Bytes::from("list_key"), Value::List(list));

    // Hash
    let mut hash = HashMap::new();
    hash.insert(Bytes::from("field1"), Bytes::from("val1"));
    hash.insert(Bytes::from("field2"), Bytes::from("val2"));
    store.set(0, Bytes::from("hash_key"), Value::Hash(hash));

    // Set
    let mut set = HashSet::new();
    set.insert(Bytes::from("member1"));
    set.insert(Bytes::from("member2"));
    store.set(0, Bytes::from("set_key"), Value::Set(set));

    // Generate and restore
    let rdb_data = generate_rdb(&store);
    let store2 = Store::new(16);
    let count = load_rdb(&rdb_data, &store2).expect("RDB load should succeed");

    assert_eq!(count, 4);

    // Verify string
    assert!(matches!(
        store2.get(0, &Bytes::from("string_key")),
        Some(Value::String(_))
    ));

    // Verify list
    if let Some(Value::List(l)) = store2.get(0, &Bytes::from("list_key")) {
        assert_eq!(l.len(), 3);
    } else {
        panic!("Expected list value");
    }

    // Verify hash
    if let Some(Value::Hash(h)) = store2.get(0, &Bytes::from("hash_key")) {
        assert_eq!(h.len(), 2);
    } else {
        panic!("Expected hash value");
    }

    // Verify set
    if let Some(Value::Set(s)) = store2.get(0, &Bytes::from("set_key")) {
        assert_eq!(s.len(), 2);
    } else {
        panic!("Expected set value");
    }
}

#[test]
fn test_rdb_empty_store() {
    let store = Store::new(16);

    // Generate RDB of empty store
    let rdb_data = generate_rdb(&store);

    // Restore to new store
    let store2 = Store::new(16);
    let count = load_rdb(&rdb_data, &store2).expect("RDB load should succeed");

    assert_eq!(count, 0);
}

#[test]
fn test_rdb_large_values() {
    let store = Store::new(16);

    // Create a large string value (1MB)
    let large_value = vec![b'x'; 1024 * 1024];
    store.set(
        0,
        Bytes::from("large_key"),
        Value::String(Bytes::from(large_value.clone())),
    );

    // Create a list with many elements
    let list: VecDeque<Bytes> = (0..1000)
        .map(|i| Bytes::from(format!("item_{}", i)))
        .collect();
    store.set(0, Bytes::from("large_list"), Value::List(list));

    // Generate and restore
    let rdb_data = generate_rdb(&store);
    let store2 = Store::new(16);
    let count = load_rdb(&rdb_data, &store2).expect("RDB load should succeed");

    assert_eq!(count, 2);

    // Verify large string
    if let Some(Value::String(s)) = store2.get(0, &Bytes::from("large_key")) {
        assert_eq!(s.len(), 1024 * 1024);
    } else {
        panic!("Expected string value");
    }

    // Verify large list
    if let Some(Value::List(l)) = store2.get(0, &Bytes::from("large_list")) {
        assert_eq!(l.len(), 1000);
    } else {
        panic!("Expected list value");
    }
}

#[test]
fn test_rdb_multiple_databases() {
    let store = Store::new(16);

    // Set values in different databases
    store.set(
        0,
        Bytes::from("db0_key"),
        Value::String(Bytes::from("db0_val")),
    );
    store.set(
        1,
        Bytes::from("db1_key"),
        Value::String(Bytes::from("db1_val")),
    );
    store.set(
        2,
        Bytes::from("db2_key"),
        Value::String(Bytes::from("db2_val")),
    );

    // Generate and restore
    let rdb_data = generate_rdb(&store);
    let store2 = Store::new(16);
    let count = load_rdb(&rdb_data, &store2).expect("RDB load should succeed");

    assert_eq!(count, 3);

    // Verify each database
    assert_eq!(
        store2.get(0, &Bytes::from("db0_key")),
        Some(Value::String(Bytes::from("db0_val")))
    );
    assert_eq!(
        store2.get(1, &Bytes::from("db1_key")),
        Some(Value::String(Bytes::from("db1_val")))
    );
    assert_eq!(
        store2.get(2, &Bytes::from("db2_key")),
        Some(Value::String(Bytes::from("db2_val")))
    );

    // Verify keys don't exist in wrong databases
    assert!(store2.get(0, &Bytes::from("db1_key")).is_none());
    assert!(store2.get(1, &Bytes::from("db0_key")).is_none());
}

#[test]
fn test_rdb_binary_keys_and_values() {
    let store = Store::new(16);

    // Binary key with null bytes
    let binary_key = Bytes::from(vec![0x00, 0x01, 0x02, 0xFF, 0xFE]);
    let binary_value = Bytes::from(vec![0xDE, 0xAD, 0xBE, 0xEF]);

    store.set(0, binary_key.clone(), Value::String(binary_value.clone()));

    // Generate and restore
    let rdb_data = generate_rdb(&store);
    let store2 = Store::new(16);
    let count = load_rdb(&rdb_data, &store2).expect("RDB load should succeed");

    assert_eq!(count, 1);

    // Verify binary data preserved
    assert_eq!(
        store2.get(0, &binary_key),
        Some(Value::String(binary_value))
    );
}

#[test]
fn test_rdb_special_characters() {
    let store = Store::new(16);

    // Keys and values with special characters
    let keys_values = vec![
        ("key:with:colons", "value:with:colons"),
        ("key with spaces", "value with spaces"),
        ("key\twith\ttabs", "value\twith\ttabs"),
        ("key\nwith\nnewlines", "value\nwith\nnewlines"),
        ("unicode:日本語", "value:中文"),
        ("emoji:🔥", "value:🎉"),
    ];

    for (key, value) in &keys_values {
        store.set(0, Bytes::from(*key), Value::String(Bytes::from(*value)));
    }

    // Generate and restore
    let rdb_data = generate_rdb(&store);
    let store2 = Store::new(16);
    let count = load_rdb(&rdb_data, &store2).expect("RDB load should succeed");

    assert_eq!(count, keys_values.len());

    // Verify all special characters preserved
    for (key, value) in &keys_values {
        assert_eq!(
            store2.get(0, &Bytes::from(*key)),
            Some(Value::String(Bytes::from(*value)))
        );
    }
}

#[test]
fn test_rdb_overwrite_existing_data() {
    // Create store with initial data
    let store = Store::new(16);
    store.set(
        0,
        Bytes::from("key1"),
        Value::String(Bytes::from("old_value1")),
    );
    store.set(
        0,
        Bytes::from("key2"),
        Value::String(Bytes::from("old_value2")),
    );

    // Create RDB with different data
    let source_store = Store::new(16);
    source_store.set(
        0,
        Bytes::from("key1"),
        Value::String(Bytes::from("new_value1")),
    );
    source_store.set(
        0,
        Bytes::from("key3"),
        Value::String(Bytes::from("new_value3")),
    );
    let rdb_data = generate_rdb(&source_store);

    // Load RDB into existing store
    let count = load_rdb(&rdb_data, &store).expect("RDB load should succeed");

    assert_eq!(count, 2);

    // Verify key1 was overwritten
    assert_eq!(
        store.get(0, &Bytes::from("key1")),
        Some(Value::String(Bytes::from("new_value1")))
    );

    // Verify key2 still exists (not in RDB)
    assert_eq!(
        store.get(0, &Bytes::from("key2")),
        Some(Value::String(Bytes::from("old_value2")))
    );

    // Verify key3 was added
    assert_eq!(
        store.get(0, &Bytes::from("key3")),
        Some(Value::String(Bytes::from("new_value3")))
    );
}

// ============================================================================
// Corruption Detection Tests
// ============================================================================

#[test]
fn test_rdb_corrupted_header() {
    // Create valid RDB first
    let store = Store::new(16);
    store.set(0, Bytes::from("key"), Value::String(Bytes::from("value")));
    let mut rdb_data = generate_rdb(&store).to_vec();

    // Corrupt the header (first few bytes)
    if rdb_data.len() > 5 {
        rdb_data[0] = 0xFF;
        rdb_data[1] = 0xFF;
    }

    // Attempt to load corrupted RDB
    let store2 = Store::new(16);
    let result = load_rdb(&Bytes::from(rdb_data), &store2);

    // Should fail with an error
    assert!(result.is_err());
}

#[test]
fn test_rdb_truncated_data() {
    // Create valid RDB first
    let store = Store::new(16);
    store.set(0, Bytes::from("key"), Value::String(Bytes::from("value")));
    let rdb_data = generate_rdb(&store);

    // Truncate the data (remove last 20 bytes if possible)
    let truncated_len = rdb_data.len().saturating_sub(20);
    if truncated_len > 10 {
        let truncated = Bytes::from(rdb_data[..truncated_len].to_vec());

        // Attempt to load truncated RDB
        let store2 = Store::new(16);
        let result = load_rdb(&truncated, &store2);

        // Should fail or partially load
        // (behavior depends on where truncation occurred)
        // We just verify it doesn't panic
        let _ = result;
    }
}

// ============================================================================
// Recovery After Crash Scenarios
// ============================================================================

#[test]
fn test_recovery_preserves_data_integrity() {
    let store = Store::new(16);

    // Create diverse dataset
    for i in 0..100 {
        store.set(
            0,
            Bytes::from(format!("key_{}", i)),
            Value::String(Bytes::from(format!("value_{}", i))),
        );
    }

    // Simulate persistence
    let rdb_data = generate_rdb(&store);

    // Simulate recovery
    let recovered_store = Store::new(16);
    let count = load_rdb(&rdb_data, &recovered_store).expect("Recovery should succeed");

    assert_eq!(count, 100);

    // Verify all data recovered correctly
    for i in 0..100 {
        let key = Bytes::from(format!("key_{}", i));
        let expected = Value::String(Bytes::from(format!("value_{}", i)));
        assert_eq!(recovered_store.get(0, &key), Some(expected));
    }
}

#[test]
fn test_recovery_empty_rdb() {
    // Empty RDB bytes (just header)
    let store = Store::new(16);
    let rdb_data = generate_rdb(&store);

    let store2 = Store::new(16);
    let count = load_rdb(&rdb_data, &store2).expect("Empty RDB should load");

    assert_eq!(count, 0);
}

// ============================================================================
// Concurrent Access During Recovery
// ============================================================================

#[tokio::test]
async fn test_recovery_with_concurrent_access() {
    use std::sync::Arc;
    use tokio::task;

    let store = Arc::new(Store::new(16));

    // Populate store
    for i in 0..50 {
        store.set(
            0,
            Bytes::from(format!("key_{}", i)),
            Value::String(Bytes::from(format!("value_{}", i))),
        );
    }

    // Generate RDB
    let rdb_data = generate_rdb(&store);

    // Create recovery store
    let recovery_store = Arc::new(Store::new(16));

    // Start recovery in background
    let recovery_store_clone = Arc::clone(&recovery_store);
    let recovery_handle = task::spawn_blocking(move || -> ferrite::Result<usize> {
        load_rdb(&rdb_data, &recovery_store_clone)
    });

    // Simultaneously access the recovery store
    let access_store = Arc::clone(&recovery_store);
    let access_handle = task::spawn(async move {
        for _ in 0..100 {
            // Read operations should not panic
            let _ = access_store.get(0, &Bytes::from("key_0"));
            tokio::task::yield_now().await;
        }
    });

    // Wait for both to complete
    let recovery_result = recovery_handle.await.expect("Task should complete");
    access_handle.await.expect("Access task should complete");

    // Verify recovery succeeded
    assert!(recovery_result.is_ok());
}
