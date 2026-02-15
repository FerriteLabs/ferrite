//! SCAN family commands implementation
//!
//! Provides cursor-based iteration over keys and collection elements.
//! SCAN iterates keys, HSCAN iterates hash fields, SSCAN iterates set members,
//! and ZSCAN iterates sorted set members.

use bytes::Bytes;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::protocol::Frame;
use crate::storage::{Store, Value};
use std::sync::Arc;

/// Default number of elements to return per SCAN call
const DEFAULT_COUNT: usize = 10;

/// Compute a hash for cursor-based iteration
fn hash_key(key: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Match a key against a glob pattern
fn match_pattern(pattern: &str, key: &str) -> bool {
    glob_match(pattern, key)
}

/// Simple glob matching supporting * and ?
fn glob_match(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();
    glob_match_chars(&pattern, &text)
}

fn glob_match_chars(pattern: &[char], text: &[char]) -> bool {
    let mut p = 0;
    let mut t = 0;
    let mut star_p = None;
    let mut star_t = None;

    while t < text.len() {
        if p < pattern.len() && (pattern[p] == '?' || pattern[p] == text[t]) {
            p += 1;
            t += 1;
        } else if p < pattern.len() && pattern[p] == '*' {
            star_p = Some(p);
            star_t = Some(t);
            p += 1;
        } else if let (Some(sp), Some(st)) = (star_p, star_t) {
            p = sp + 1;
            star_t = Some(st + 1);
            t = st + 1;
        } else {
            return false;
        }
    }

    while p < pattern.len() && pattern[p] == '*' {
        p += 1;
    }

    p == pattern.len()
}

/// SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
pub fn scan(
    store: &Arc<Store>,
    db: u8,
    cursor: u64,
    pattern: Option<&str>,
    count: Option<usize>,
    type_filter: Option<&str>,
) -> Frame {
    let count = count.unwrap_or(DEFAULT_COUNT);

    // Collect all keys that match criteria
    let mut keys: Vec<(u64, Bytes)> = Vec::new();

    for key in store.keys(db) {
        let value = match store.get(db, &key) {
            Some(value) => value,
            None => continue,
        };

        // Apply type filter if specified
        if let Some(type_name) = type_filter {
            let value_type = match value {
                Value::String(_) => "string",
                Value::List(_) => "list",
                Value::Hash(_) => "hash",
                Value::Set(_) => "set",
                Value::SortedSet { .. } => "zset",
                Value::Stream(_) => "stream",
                Value::HyperLogLog(_) => "string", // HLL is reported as string in Redis
            };
            if !value_type.eq_ignore_ascii_case(type_name) {
                continue;
            }
        }

        // Apply pattern filter if specified
        if let Some(pat) = pattern {
            if let Ok(key_str) = std::str::from_utf8(&key) {
                if !match_pattern(pat, key_str) {
                    continue;
                }
            } else {
                continue; // Skip non-UTF8 keys when pattern matching
            }
        }

        let key_hash = hash_key(&key);
        keys.push((key_hash, key));
    }

    // Sort by hash for consistent ordering
    keys.sort_by_key(|(h, _)| *h);

    // Find starting position based on cursor
    let start_pos = if cursor == 0 {
        0
    } else {
        keys.iter()
            .position(|(h, _)| *h > cursor)
            .unwrap_or(keys.len())
    };

    // Collect results
    let mut results: Vec<Frame> = Vec::new();
    let mut next_cursor: u64 = 0;

    for (i, (key_hash, key)) in keys.iter().enumerate().skip(start_pos) {
        if results.len() >= count {
            next_cursor = *key_hash;
            break;
        }
        results.push(Frame::Bulk(Some(key.clone())));

        // If this is the last element, cursor is 0 (iteration complete)
        if i == keys.len() - 1 {
            next_cursor = 0;
        }
    }

    // Return [cursor, [keys...]]
    Frame::Array(Some(vec![
        Frame::Bulk(Some(Bytes::from(next_cursor.to_string()))),
        Frame::Array(Some(results)),
    ]))
}

/// ZSCAN key cursor [MATCH pattern] [COUNT count]
pub fn zscan(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    cursor: u64,
    pattern: Option<&str>,
    count: Option<usize>,
) -> Frame {
    let count = count.unwrap_or(DEFAULT_COUNT);

    match store.get(db, key) {
        Some(Value::SortedSet { by_member, .. }) => {
            // Collect all members that match pattern
            let mut members: Vec<(u64, Bytes, f64)> = Vec::new();

            for (member, score) in by_member.iter() {
                // Apply pattern filter if specified
                if let Some(pat) = pattern {
                    if let Ok(member_str) = std::str::from_utf8(member) {
                        if !match_pattern(pat, member_str) {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }

                let member_hash = hash_key(member);
                members.push((member_hash, member.clone(), *score));
            }

            // Sort by hash for consistent ordering
            members.sort_by_key(|(h, _, _)| *h);

            // Find starting position based on cursor
            let start_pos = if cursor == 0 {
                0
            } else {
                members
                    .iter()
                    .position(|(h, _, _)| *h > cursor)
                    .unwrap_or(members.len())
            };

            // Collect results (member-score pairs)
            let mut results: Vec<Frame> = Vec::new();
            let mut next_cursor: u64 = 0;

            for (i, (member_hash, member, score)) in members.iter().enumerate().skip(start_pos) {
                if results.len() >= count * 2 {
                    next_cursor = *member_hash;
                    break;
                }
                results.push(Frame::Bulk(Some(member.clone())));
                results.push(Frame::Bulk(Some(Bytes::from(score.to_string()))));

                if i == members.len() - 1 {
                    next_cursor = 0;
                }
            }

            Frame::Array(Some(vec![
                Frame::Bulk(Some(Bytes::from(next_cursor.to_string()))),
                Frame::Array(Some(results)),
            ]))
        }
        Some(_) => {
            Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())
        }
        None => {
            // Return empty result for non-existent key
            Frame::Array(Some(vec![
                Frame::Bulk(Some(Bytes::from("0"))),
                Frame::Array(Some(vec![])),
            ]))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("foo*", "foobar"));
        assert!(glob_match("*bar", "foobar"));
        assert!(glob_match("foo*bar", "fooXXXbar"));
        assert!(glob_match("f?o", "foo"));
        assert!(glob_match("f??", "foo"));
        assert!(!glob_match("foo", "bar"));
        assert!(!glob_match("foo*", "bar"));
        assert!(!glob_match("f?o", "fooo"));
    }

    #[test]
    fn test_scan_empty_db() {
        use crate::storage::Store;

        let store = Arc::new(Store::new(16));

        let result = scan(&store, 0, 0, None, None, None);

        if let Frame::Array(Some(arr)) = result {
            assert_eq!(arr.len(), 2);
            // Cursor should be 0 (iteration complete)
            if let Frame::Bulk(Some(cursor)) = &arr[0] {
                assert_eq!(cursor.as_ref(), b"0");
            }
            // Results should be empty
            if let Frame::Array(Some(keys)) = &arr[1] {
                assert!(keys.is_empty());
            }
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_scan_with_keys() {
        use crate::storage::Store;

        let store = Arc::new(Store::new(16));

        // Add some keys
        store.set(0, Bytes::from("key1"), Value::String(Bytes::from("val1")));
        store.set(0, Bytes::from("key2"), Value::String(Bytes::from("val2")));
        store.set(0, Bytes::from("key3"), Value::String(Bytes::from("val3")));

        // Scan all keys
        let result = scan(&store, 0, 0, None, Some(100), None);

        if let Frame::Array(Some(arr)) = result {
            assert_eq!(arr.len(), 2);
            if let Frame::Array(Some(keys)) = &arr[1] {
                assert_eq!(keys.len(), 3);
            }
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_scan_with_pattern() {
        use crate::storage::Store;

        let store = Arc::new(Store::new(16));

        // Add some keys
        store.set(0, Bytes::from("user:1"), Value::String(Bytes::from("val1")));
        store.set(0, Bytes::from("user:2"), Value::String(Bytes::from("val2")));
        store.set(
            0,
            Bytes::from("session:1"),
            Value::String(Bytes::from("val3")),
        );

        // Scan with pattern
        let result = scan(&store, 0, 0, Some("user:*"), Some(100), None);

        if let Frame::Array(Some(arr)) = result {
            if let Frame::Array(Some(keys)) = &arr[1] {
                assert_eq!(keys.len(), 2);
            }
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_scan_with_type_filter() {
        use crate::storage::Store;
        use std::collections::VecDeque;

        let store = Arc::new(Store::new(16));

        // Add different types
        store.set(0, Bytes::from("str"), Value::String(Bytes::from("val")));
        store.set(
            0,
            Bytes::from("list"),
            Value::List(VecDeque::from([Bytes::from("a")])),
        );

        // Scan for strings only
        let result = scan(&store, 0, 0, None, Some(100), Some("string"));

        if let Frame::Array(Some(arr)) = result {
            if let Frame::Array(Some(keys)) = &arr[1] {
                assert_eq!(keys.len(), 1);
            }
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_hscan() {
        use crate::commands::hashes;
        use crate::storage::Store;
        use std::collections::HashMap;

        let store = Arc::new(Store::new(16));

        // Create a hash
        let mut hash = HashMap::new();
        hash.insert(Bytes::from("field1"), Bytes::from("value1"));
        hash.insert(Bytes::from("field2"), Bytes::from("value2"));
        store.set(0, Bytes::from("myhash"), Value::Hash(hash));

        let result = hashes::hscan(&store, 0, &Bytes::from("myhash"), 0, None, Some(100), false);

        if let Frame::Array(Some(arr)) = result {
            if let Frame::Array(Some(items)) = &arr[1] {
                // Should have 4 items (2 field-value pairs)
                assert_eq!(items.len(), 4);
            }
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_sscan() {
        use crate::commands::sets;
        use crate::storage::Store;
        use std::collections::HashSet;

        let store = Arc::new(Store::new(16));

        // Create a set
        let mut set = HashSet::new();
        set.insert(Bytes::from("member1"));
        set.insert(Bytes::from("member2"));
        set.insert(Bytes::from("member3"));
        store.set(0, Bytes::from("myset"), Value::Set(set));

        let result = sets::sscan(&store, 0, &Bytes::from("myset"), 0, None, Some(100));

        if let Frame::Array(Some(arr)) = result {
            if let Frame::Array(Some(members)) = &arr[1] {
                assert_eq!(members.len(), 3);
            }
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_zscan() {
        use crate::storage::Store;
        use ordered_float::OrderedFloat;
        use std::collections::{BTreeMap, HashMap};

        let store = Arc::new(Store::new(16));

        // Create a sorted set
        let mut by_member = HashMap::new();
        let mut by_score = BTreeMap::new();

        by_member.insert(Bytes::from("a"), 1.0);
        by_member.insert(Bytes::from("b"), 2.0);
        by_score.insert((OrderedFloat(1.0), Bytes::from("a")), ());
        by_score.insert((OrderedFloat(2.0), Bytes::from("b")), ());

        store.set(
            0,
            Bytes::from("myzset"),
            Value::SortedSet {
                by_member,
                by_score,
            },
        );

        let result = zscan(&store, 0, &Bytes::from("myzset"), 0, None, Some(100));

        if let Frame::Array(Some(arr)) = result {
            if let Frame::Array(Some(items)) = &arr[1] {
                // Should have 4 items (2 member-score pairs)
                assert_eq!(items.len(), 4);
            }
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_scan_nonexistent_key() {
        use crate::commands::hashes;
        use crate::storage::Store;

        let store = Arc::new(Store::new(16));

        let result = hashes::hscan(&store, 0, &Bytes::from("nonexistent"), 0, None, None, false);

        if let Frame::Array(Some(arr)) = result {
            if let Frame::Bulk(Some(cursor)) = &arr[0] {
                assert_eq!(cursor.as_ref(), b"0");
            }
            if let Frame::Array(Some(items)) = &arr[1] {
                assert!(items.is_empty());
            }
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_scan_wrong_type() {
        use crate::commands::hashes;
        use crate::storage::Store;

        let store = Arc::new(Store::new(16));

        // Create a string
        store.set(
            0,
            Bytes::from("mystring"),
            Value::String(Bytes::from("value")),
        );

        // Try to HSCAN a string
        let result = hashes::hscan(&store, 0, &Bytes::from("mystring"), 0, None, None, false);
        assert!(matches!(result, Frame::Error(_)));
    }
}
