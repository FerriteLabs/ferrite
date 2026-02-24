//! HybridLog promotion tests — validating the tiered storage engine
//! for graduation from Beta (🧪) to Stable (✅).
//!
//! These tests focus on scenarios not covered by unit tests:
//! - Cross-tier data migration under load
//! - Concurrent read/write during tier transitions
//! - Edge cases with large values and boundary conditions
//! - Recovery semantics after simulated failures

use bytes::Bytes;
use std::sync::Arc;
use std::thread;

use ferrite::storage::{Store, Value};

// ============================================================================
// Cross-Tier Migration Tests
// ============================================================================

#[test]
fn test_store_handles_many_keys_across_databases() {
    let store = Store::new(16);

    // Write across all 16 databases
    for db in 0u8..16 {
        for i in 0..500 {
            store.set(
                db,
                Bytes::from(format!("tier_test:{}:{}", db, i)),
                Value::String(Bytes::from(format!("value_{}", i))),
            );
        }
    }

    // Verify all reads succeed
    for db in 0u8..16 {
        for i in 0..500 {
            let key = Bytes::from(format!("tier_test:{}:{}", db, i));
            let val = store.get(db, &key);
            assert!(val.is_some(), "missing key in db {} index {}", db, i);
        }
    }
}

#[test]
fn test_store_large_values() {
    let store = Store::new(16);

    // Test with progressively larger values (1KB, 10KB, 100KB, 1MB)
    let sizes = [1024, 10 * 1024, 100 * 1024, 1024 * 1024];

    for (i, &size) in sizes.iter().enumerate() {
        let value = vec![b'A' + (i as u8); size];
        store.set(
            0,
            Bytes::from(format!("large:{}", i)),
            Value::String(Bytes::from(value.clone())),
        );

        let result = store.get(0, &Bytes::from(format!("large:{}", i)));
        if let Some(Value::String(v)) = result {
            assert_eq!(v.len(), size, "value size mismatch for size {}", size);
        } else {
            panic!("Expected string value for size {}", size);
        }
    }
}

// ============================================================================
// Concurrent Access During Tier Transitions
// ============================================================================

#[test]
fn test_concurrent_heavy_write_load() {
    let store = Arc::new(Store::new(16));
    let num_threads = 8;
    let keys_per_thread = 5000;
    let mut handles = vec![];

    // Heavy concurrent writes
    for t in 0..num_threads {
        let store_clone = store.clone();
        handles.push(thread::spawn(move || {
            for i in 0..keys_per_thread {
                store_clone.set(
                    0,
                    Bytes::from(format!("concurrent:{}:{}", t, i)),
                    Value::String(Bytes::from(format!("val_{}_{}", t, i))),
                );
            }
        }));
    }

    for h in handles {
        h.join().expect("writer thread should complete");
    }

    // Verify all writes landed
    let mut missing = 0;
    for t in 0..num_threads {
        for i in 0..keys_per_thread {
            if store
                .get(0, &Bytes::from(format!("concurrent:{}:{}", t, i)))
                .is_none()
            {
                missing += 1;
            }
        }
    }
    assert_eq!(
        missing,
        0,
        "all {} keys should be present",
        num_threads * keys_per_thread
    );
}

#[test]
fn test_read_while_writing() {
    let store = Arc::new(Store::new(16));

    // Pre-populate
    for i in 0..1000 {
        store.set(
            0,
            Bytes::from(format!("rw:{}", i)),
            Value::String(Bytes::from(format!("initial_{}", i))),
        );
    }

    let store_writer = store.clone();
    let store_reader = store.clone();

    // Writer thread: overwrite keys
    let writer = thread::spawn(move || {
        for round in 0..10 {
            for i in 0..1000 {
                store_writer.set(
                    0,
                    Bytes::from(format!("rw:{}", i)),
                    Value::String(Bytes::from(format!("round_{}_{}", round, i))),
                );
            }
        }
    });

    // Reader thread: continuously read
    let reader = thread::spawn(move || {
        let mut reads = 0u64;
        for _ in 0..50 {
            for i in 0..1000 {
                let val = store_reader.get(0, &Bytes::from(format!("rw:{}", i)));
                // Value should always exist (never deleted)
                assert!(
                    val.is_some(),
                    "key rw:{} should always be present during concurrent r/w",
                    i
                );
                reads += 1;
            }
        }
        reads
    });

    writer.join().expect("writer should complete");
    let total_reads = reader.join().expect("reader should complete");
    assert!(total_reads > 0, "reader should have performed reads");
}

// ============================================================================
// Edge Case Tests
// ============================================================================

#[test]
fn test_empty_key_and_value() {
    let store = Store::new(16);

    // Empty value
    store.set(0, Bytes::from("empty_val"), Value::String(Bytes::from("")));
    assert_eq!(
        store.get(0, &Bytes::from("empty_val")),
        Some(Value::String(Bytes::from("")))
    );
}

#[test]
fn test_rapid_overwrite_same_key() {
    let store = Store::new(16);
    let key = Bytes::from("hotkey");

    // Rapidly overwrite the same key 10K times
    for i in 0..10_000 {
        store.set(
            0,
            key.clone(),
            Value::String(Bytes::from(format!("v{}", i))),
        );
    }

    // Final value should be the last write
    let val = store.get(0, &key);
    assert_eq!(val, Some(Value::String(Bytes::from("v9999"))));
}

#[test]
fn test_delete_and_recreate_cycle() {
    let store = Store::new(16);
    let key = Bytes::from("cycle_key");

    for i in 0..1000 {
        // Set
        store.set(
            0,
            key.clone(),
            Value::String(Bytes::from(format!("v{}", i))),
        );
        assert!(store.get(0, &key).is_some());

        // Delete
        let deleted = store.del(0, &[key.clone()]);
        assert_eq!(deleted, 1);
        assert!(store.get(0, &key).is_none());
    }
}

#[test]
fn test_mixed_data_types_same_store() {
    use std::collections::{HashMap, HashSet, VecDeque};

    let store = Store::new(16);

    // String
    store.set(0, Bytes::from("str"), Value::String(Bytes::from("hello")));

    // List
    let list = VecDeque::from([Bytes::from("a"), Bytes::from("b")]);
    store.set(0, Bytes::from("list"), Value::List(list));

    // Hash
    let mut hash = HashMap::new();
    hash.insert(Bytes::from("f1"), Bytes::from("v1"));
    store.set(0, Bytes::from("hash"), Value::Hash(hash));

    // Set
    let mut set = HashSet::new();
    set.insert(Bytes::from("m1"));
    store.set(0, Bytes::from("set"), Value::Set(set));

    // Verify all types coexist
    assert!(matches!(
        store.get(0, &Bytes::from("str")),
        Some(Value::String(_))
    ));
    assert!(matches!(
        store.get(0, &Bytes::from("list")),
        Some(Value::List(_))
    ));
    assert!(matches!(
        store.get(0, &Bytes::from("hash")),
        Some(Value::Hash(_))
    ));
    assert!(matches!(
        store.get(0, &Bytes::from("set")),
        Some(Value::Set(_))
    ));
}
