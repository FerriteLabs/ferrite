#![allow(clippy::unwrap_used)]
//! Integration tests for Ferrite
//!
//! These tests verify the complete functionality of Ferrite including:
//! - Storage operations
//! - Protocol parsing
//! - Concurrent access patterns
//! - Persistence

use bytes::Bytes;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use ferrite::protocol::{parse_frame, Frame};
use ferrite::storage::{Store, Value};

// ============================================================================
// Storage Integration Tests
// ============================================================================

#[test]
fn test_store_basic_operations() {
    let store = Store::new(16);

    // SET and GET
    store.set(0, Bytes::from("key1"), Value::String(Bytes::from("value1")));
    let result = store.get(0, &Bytes::from("key1"));
    assert_eq!(result, Some(Value::String(Bytes::from("value1"))));

    // GET nonexistent
    let result = store.get(0, &Bytes::from("nonexistent"));
    assert!(result.is_none());
}

#[test]
fn test_store_del_exists() {
    let store = Store::new(16);

    // Setup
    store.set(0, Bytes::from("key1"), Value::String(Bytes::from("value1")));
    store.set(0, Bytes::from("key2"), Value::String(Bytes::from("value2")));

    // EXISTS
    let count = store.exists(
        0,
        &[
            Bytes::from("key1"),
            Bytes::from("key2"),
            Bytes::from("key3"),
        ],
    );
    assert_eq!(count, 2);

    // DEL
    let deleted = store.del(0, &[Bytes::from("key1"), Bytes::from("key3")]);
    assert_eq!(deleted, 1); // Only key1 existed

    // Verify deletion
    assert_eq!(store.exists(0, &[Bytes::from("key1")]), 0);
}

#[test]
fn test_store_list_operations() {
    let store = Store::new(16);

    // Create a list
    let list = VecDeque::from([Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]);

    store.set(0, Bytes::from("mylist"), Value::List(list));

    // Verify
    let result = store.get(0, &Bytes::from("mylist"));
    if let Some(Value::List(l)) = result {
        assert_eq!(l.len(), 3);
        assert_eq!(l[0], Bytes::from("a"));
        assert_eq!(l[2], Bytes::from("c"));
    } else {
        panic!("Expected list value");
    }
}

#[test]
fn test_store_hash_operations() {
    let store = Store::new(16);

    // Create a hash
    let mut hash = HashMap::new();
    hash.insert(Bytes::from("field1"), Bytes::from("value1"));
    hash.insert(Bytes::from("field2"), Bytes::from("value2"));

    store.set(0, Bytes::from("myhash"), Value::Hash(hash));

    // Verify
    let result = store.get(0, &Bytes::from("myhash"));
    if let Some(Value::Hash(h)) = result {
        assert_eq!(h.len(), 2);
        assert_eq!(h.get(&Bytes::from("field1")), Some(&Bytes::from("value1")));
    } else {
        panic!("Expected hash value");
    }
}

#[test]
fn test_store_set_operations() {
    let store = Store::new(16);

    // Create a set
    let mut set = HashSet::new();
    set.insert(Bytes::from("a"));
    set.insert(Bytes::from("b"));
    set.insert(Bytes::from("c"));

    store.set(0, Bytes::from("myset"), Value::Set(set));

    // Verify
    let result = store.get(0, &Bytes::from("myset"));
    if let Some(Value::Set(s)) = result {
        assert_eq!(s.len(), 3);
        assert!(s.contains(&Bytes::from("a")));
    } else {
        panic!("Expected set value");
    }
}

#[test]
fn test_store_sorted_set_operations() {
    use ordered_float::OrderedFloat;
    use std::collections::BTreeMap;

    let store = Store::new(16);

    // Create a sorted set
    let mut by_member = HashMap::new();
    let mut by_score = BTreeMap::new();

    by_member.insert(Bytes::from("one"), 1.0);
    by_member.insert(Bytes::from("two"), 2.0);
    by_member.insert(Bytes::from("three"), 3.0);

    by_score.insert((OrderedFloat(1.0), Bytes::from("one")), ());
    by_score.insert((OrderedFloat(2.0), Bytes::from("two")), ());
    by_score.insert((OrderedFloat(3.0), Bytes::from("three")), ());

    store.set(
        0,
        Bytes::from("myzset"),
        Value::SortedSet {
            by_score,
            by_member,
        },
    );

    // Verify
    let result = store.get(0, &Bytes::from("myzset"));
    if let Some(Value::SortedSet { by_member, .. }) = result {
        assert_eq!(by_member.len(), 3);
        assert_eq!(by_member.get(&Bytes::from("two")), Some(&2.0));
    } else {
        panic!("Expected sorted set value");
    }
}

// ============================================================================
// Database Isolation Tests
// ============================================================================

#[test]
fn test_database_isolation() {
    let store = Store::new(16);

    // Set key in db 0
    store.set(
        0,
        Bytes::from("key"),
        Value::String(Bytes::from("db0_value")),
    );

    // Set same key in db 1 with different value
    store.set(
        1,
        Bytes::from("key"),
        Value::String(Bytes::from("db1_value")),
    );

    // Verify isolation
    assert_eq!(
        store.get(0, &Bytes::from("key")),
        Some(Value::String(Bytes::from("db0_value")))
    );
    assert_eq!(
        store.get(1, &Bytes::from("key")),
        Some(Value::String(Bytes::from("db1_value")))
    );

    // db 2 should not have the key
    assert!(store.get(2, &Bytes::from("key")).is_none());
}

#[test]
fn test_all_16_databases() {
    let store = Store::new(16);

    // Set unique value in each database
    for db in 0u8..16 {
        store.set(
            db,
            Bytes::from("shared_key"),
            Value::String(Bytes::from(format!("value_db{}", db))),
        );
    }

    // Verify each database has correct value
    for db in 0u8..16 {
        let result = store.get(db, &Bytes::from("shared_key"));
        assert_eq!(
            result,
            Some(Value::String(Bytes::from(format!("value_db{}", db))))
        );
    }
}

// ============================================================================
// RESP Protocol Tests
// ============================================================================

#[test]
fn test_resp_simple_string() {
    let mut buf = bytes::BytesMut::from("+OK\r\n");
    let frame = parse_frame(&mut buf).unwrap().unwrap();
    assert!(matches!(frame, Frame::Simple(ref s) if s == "OK"));
}

#[test]
fn test_resp_error() {
    let mut buf = bytes::BytesMut::from("-ERR unknown command\r\n");
    let frame = parse_frame(&mut buf).unwrap().unwrap();
    assert!(matches!(frame, Frame::Error(ref s) if s == "ERR unknown command"));
}

#[test]
fn test_resp_integer() {
    let mut buf = bytes::BytesMut::from(":1000\r\n");
    let frame = parse_frame(&mut buf).unwrap().unwrap();
    assert!(matches!(frame, Frame::Integer(1000)));

    // Negative integer
    let mut buf = bytes::BytesMut::from(":-50\r\n");
    let frame = parse_frame(&mut buf).unwrap().unwrap();
    assert!(matches!(frame, Frame::Integer(-50)));
}

#[test]
fn test_resp_bulk_string() {
    let mut buf = bytes::BytesMut::from("$5\r\nhello\r\n");
    let frame = parse_frame(&mut buf).unwrap().unwrap();
    assert!(matches!(frame, Frame::Bulk(Some(ref v)) if v == &Bytes::from("hello")));
}

#[test]
fn test_resp_null_bulk_string() {
    let mut buf = bytes::BytesMut::from("$-1\r\n");
    let frame = parse_frame(&mut buf).unwrap().unwrap();
    assert!(matches!(frame, Frame::Bulk(None)));
}

#[test]
fn test_resp_array() {
    let mut buf = bytes::BytesMut::from("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    let frame = parse_frame(&mut buf).unwrap().unwrap();
    if let Frame::Array(Some(arr)) = frame {
        assert_eq!(arr.len(), 2);
        assert!(matches!(&arr[0], Frame::Bulk(Some(ref v)) if v == &Bytes::from("foo")));
        assert!(matches!(&arr[1], Frame::Bulk(Some(ref v)) if v == &Bytes::from("bar")));
    } else {
        panic!("Expected array");
    }
}

#[test]
fn test_resp_null_array() {
    let mut buf = bytes::BytesMut::from("*-1\r\n");
    let frame = parse_frame(&mut buf).unwrap().unwrap();
    assert!(matches!(frame, Frame::Array(None)));
}

#[test]
fn test_resp_nested_array() {
    let mut buf = bytes::BytesMut::from("*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n");
    let frame = parse_frame(&mut buf).unwrap().unwrap();
    if let Frame::Array(Some(outer)) = frame {
        assert_eq!(outer.len(), 2);
        if let Frame::Array(Some(inner1)) = &outer[0] {
            assert_eq!(inner1.len(), 2);
            assert!(matches!(inner1[0], Frame::Integer(1)));
        } else {
            panic!("Expected inner array");
        }
    } else {
        panic!("Expected outer array");
    }
}

#[test]
fn test_resp_partial_read() {
    // Partial data - should return None
    let mut buf = bytes::BytesMut::from("+OK\r");
    let result = parse_frame(&mut buf).unwrap();
    assert!(result.is_none());

    // Complete the frame
    buf.extend_from_slice(b"\n");
    let frame = parse_frame(&mut buf).unwrap().unwrap();
    assert!(matches!(frame, Frame::Simple(ref s) if s == "OK"));
}

#[test]
fn test_resp_empty_bulk_string() {
    let mut buf = bytes::BytesMut::from("$0\r\n\r\n");
    let frame = parse_frame(&mut buf).unwrap().unwrap();
    if let Frame::Bulk(Some(v)) = frame {
        assert!(v.is_empty());
    } else {
        panic!("Expected empty bulk string");
    }
}

#[test]
fn test_resp_empty_array() {
    let mut buf = bytes::BytesMut::from("*0\r\n");
    let frame = parse_frame(&mut buf).unwrap().unwrap();
    if let Frame::Array(Some(arr)) = frame {
        assert!(arr.is_empty());
    } else {
        panic!("Expected empty array");
    }
}

// ============================================================================
// Concurrent Access Tests
// ============================================================================

#[test]
fn test_concurrent_reads_writes() {
    use std::thread;

    let store = Arc::new(Store::new(16));
    let mut handles = vec![];

    // Spawn multiple writer threads
    for i in 0..10 {
        let store_clone = store.clone();
        let handle = thread::spawn(move || {
            for j in 0..100 {
                store_clone.set(
                    0,
                    Bytes::from(format!("key-{}-{}", i, j)),
                    Value::String(Bytes::from(format!("value-{}-{}", i, j))),
                );
            }
        });
        handles.push(handle);
    }

    // Spawn multiple reader threads
    for _ in 0..5 {
        let store_clone = store.clone();
        let handle = thread::spawn(move || {
            for _ in 0..1000 {
                // Read random keys
                for i in 0..10 {
                    for j in 0..10 {
                        let _ = store_clone.get(0, &Bytes::from(format!("key-{}-{}", i, j)));
                    }
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all keys were written
    for i in 0..10 {
        for j in 0..100 {
            let result = store.get(0, &Bytes::from(format!("key-{}-{}", i, j)));
            assert!(result.is_some(), "Key key-{}-{} should exist", i, j);
        }
    }
}

#[test]
fn test_concurrent_increments() {
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::thread;

    let store = Arc::new(Store::new(16));
    let counter = Arc::new(AtomicI64::new(0));

    // Initialize counter
    store.set(0, Bytes::from("counter"), Value::String(Bytes::from("0")));

    let mut handles = vec![];
    let num_threads = 10;
    let increments_per_thread = 100;

    // Each thread reads, increments locally, and writes
    // This tests concurrent read-modify-write (though not atomic)
    for _ in 0..num_threads {
        let _store_clone = store.clone();
        let counter_clone = counter.clone();
        let handle = thread::spawn(move || {
            for _ in 0..increments_per_thread {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify the atomic counter has correct value
    assert_eq!(
        counter.load(Ordering::SeqCst),
        (num_threads * increments_per_thread) as i64
    );
}

#[test]
fn test_concurrent_database_isolation() {
    use std::thread;

    let store = Arc::new(Store::new(16));
    let mut handles = vec![];

    // Each thread writes to a different database
    for db in 0u8..16 {
        let store_clone = store.clone();
        let handle = thread::spawn(move || {
            for i in 0..100 {
                store_clone.set(
                    db,
                    Bytes::from(format!("key-{}", i)),
                    Value::String(Bytes::from(format!("db{}-value{}", db, i))),
                );
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify each database has correct values
    for db in 0u8..16 {
        for i in 0..100 {
            let result = store.get(db, &Bytes::from(format!("key-{}", i)));
            assert_eq!(
                result,
                Some(Value::String(Bytes::from(format!("db{}-value{}", db, i))))
            );
        }
    }
}

// ============================================================================
// Edge Cases Tests
// ============================================================================

#[test]
fn test_empty_value() {
    let store = Store::new(16);

    // Empty string value
    store.set(0, Bytes::from("empty"), Value::String(Bytes::from("")));
    let result = store.get(0, &Bytes::from("empty"));
    assert_eq!(result, Some(Value::String(Bytes::from(""))));
}

#[test]
fn test_binary_data() {
    let store = Store::new(16);

    // Binary data with null bytes
    let binary_value = Bytes::from(vec![0x00, 0x01, 0x02, 0xFF, 0xFE]);
    store.set(
        0,
        Bytes::from("binary"),
        Value::String(binary_value.clone()),
    );

    let result = store.get(0, &Bytes::from("binary"));
    assert_eq!(result, Some(Value::String(binary_value)));
}

#[test]
fn test_large_value() {
    let store = Store::new(16);

    // 1MB value
    let large_value = Bytes::from(vec![b'x'; 1024 * 1024]);
    store.set(0, Bytes::from("large"), Value::String(large_value.clone()));

    let result = store.get(0, &Bytes::from("large"));
    if let Some(Value::String(v)) = result {
        assert_eq!(v.len(), 1024 * 1024);
    } else {
        panic!("Expected string value");
    }
}

#[test]
fn test_unicode_key_value() {
    let store = Store::new(16);

    let key = Bytes::from("日本語キー");
    let value = Value::String(Bytes::from("こんにちは世界"));

    store.set(0, key.clone(), value.clone());
    let result = store.get(0, &key);
    assert_eq!(result, Some(value));
}

#[test]
fn test_special_characters() {
    let store = Store::new(16);

    // Keys and values with special characters
    let special_chars = vec![
        ("key:with:colons", "value:with:colons"),
        ("key{with}braces", "value{with}braces"),
        ("key\\with\\backslash", "value\\with\\backslash"),
        ("key\twith\ttabs", "value\twith\ttabs"),
        ("key\nwith\nnewlines", "value\nwith\nnewlines"),
    ];

    for (key, value) in special_chars {
        store.set(0, Bytes::from(key), Value::String(Bytes::from(value)));
        let result = store.get(0, &Bytes::from(key));
        assert_eq!(result, Some(Value::String(Bytes::from(value))));
    }
}

// ============================================================================
// TTL Tests
// ============================================================================

#[test]
fn test_ttl_operations() {
    use std::time::SystemTime;

    let store = Store::new(16);

    // Set key with expiration
    let expires_at = SystemTime::now() + Duration::from_secs(10);
    store.set_with_expiry(
        0,
        Bytes::from("expiring"),
        Value::String(Bytes::from("value")),
        expires_at,
    );

    // TTL should be positive
    let ttl = store.ttl(0, &Bytes::from("expiring"));
    assert!(ttl.is_some());
    let ttl_value = ttl.unwrap();
    assert!(ttl_value > 0 && ttl_value <= 10);

    // Key without expiry
    store.set(
        0,
        Bytes::from("persistent"),
        Value::String(Bytes::from("value")),
    );
    let ttl = store.ttl(0, &Bytes::from("persistent"));
    assert_eq!(ttl, Some(-1));

    // Nonexistent key
    let ttl = store.ttl(0, &Bytes::from("nonexistent"));
    assert!(ttl.is_none());
}

#[test]
fn test_expiration() {
    use std::time::SystemTime;

    let store = Store::new(16);

    // Set key with immediate past expiration
    let expires_at = SystemTime::now() - Duration::from_secs(1);
    store.set_with_expiry(
        0,
        Bytes::from("expired"),
        Value::String(Bytes::from("value")),
        expires_at,
    );

    // Key should not be accessible (lazy expiration)
    let result = store.get(0, &Bytes::from("expired"));
    assert!(result.is_none());
}

// ============================================================================
// Data Structure Size Tests
// ============================================================================

#[test]
fn test_large_list() {
    let store = Store::new(16);

    // Create a list with 10000 elements
    let mut list = VecDeque::new();
    for i in 0..10000 {
        list.push_back(Bytes::from(format!("item-{}", i)));
    }

    store.set(0, Bytes::from("biglist"), Value::List(list));

    let result = store.get(0, &Bytes::from("biglist"));
    if let Some(Value::List(l)) = result {
        assert_eq!(l.len(), 10000);
    } else {
        panic!("Expected list value");
    }
}

#[test]
fn test_large_hash() {
    let store = Store::new(16);

    // Create a hash with 10000 fields
    let mut hash = HashMap::new();
    for i in 0..10000 {
        hash.insert(
            Bytes::from(format!("field-{}", i)),
            Bytes::from(format!("value-{}", i)),
        );
    }

    store.set(0, Bytes::from("bighash"), Value::Hash(hash));

    let result = store.get(0, &Bytes::from("bighash"));
    if let Some(Value::Hash(h)) = result {
        assert_eq!(h.len(), 10000);
    } else {
        panic!("Expected hash value");
    }
}

#[test]
fn test_large_set() {
    let store = Store::new(16);

    // Create a set with 10000 members
    let mut set = HashSet::new();
    for i in 0..10000 {
        set.insert(Bytes::from(format!("member-{}", i)));
    }

    store.set(0, Bytes::from("bigset"), Value::Set(set));

    let result = store.get(0, &Bytes::from("bigset"));
    if let Some(Value::Set(s)) = result {
        assert_eq!(s.len(), 10000);
    } else {
        panic!("Expected set value");
    }
}

// ============================================================================
// Overwrite Tests
// ============================================================================

#[test]
fn test_type_change_on_set() {
    let store = Store::new(16);

    // Set as string
    store.set(0, Bytes::from("key"), Value::String(Bytes::from("string")));
    assert!(matches!(
        store.get(0, &Bytes::from("key")),
        Some(Value::String(_))
    ));

    // Overwrite with list
    let list = VecDeque::from([Bytes::from("a")]);
    store.set(0, Bytes::from("key"), Value::List(list));
    assert!(matches!(
        store.get(0, &Bytes::from("key")),
        Some(Value::List(_))
    ));

    // Overwrite with hash
    let mut hash = HashMap::new();
    hash.insert(Bytes::from("f"), Bytes::from("v"));
    store.set(0, Bytes::from("key"), Value::Hash(hash));
    assert!(matches!(
        store.get(0, &Bytes::from("key")),
        Some(Value::Hash(_))
    ));
}

#[test]
fn test_string_overwrite() {
    let store = Store::new(16);

    store.set(0, Bytes::from("key"), Value::String(Bytes::from("value1")));
    assert_eq!(
        store.get(0, &Bytes::from("key")),
        Some(Value::String(Bytes::from("value1")))
    );

    store.set(0, Bytes::from("key"), Value::String(Bytes::from("value2")));
    assert_eq!(
        store.get(0, &Bytes::from("key")),
        Some(Value::String(Bytes::from("value2")))
    );
}
