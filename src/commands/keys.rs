//! Key management command implementations
//!
//! This module implements key-level commands like KEYS, TYPE, RENAME, COPY, etc.

use std::sync::Arc;
use std::time::SystemTime;

use bytes::Bytes;
use rand::Rng;

use crate::protocol::Frame;
use crate::storage::{Store, Value};

const EMBSTR_MAX_LEN: usize = 44;
const LISTPACK_MAX_ENTRIES: usize = 512;
const HASH_LISTPACK_MAX_ENTRIES: usize = 512;
const SET_INTSET_MAX_ENTRIES: usize = 512;
const SET_LISTPACK_MAX_ENTRIES: usize = 128;
const ZSET_LISTPACK_MAX_ENTRIES: usize = 128;

/// KEYS command - find all keys matching pattern
/// Supports glob-style patterns: * ? [abc] [^abc] [a-z]
pub fn keys(store: &Arc<Store>, db: u8, pattern: &str) -> Frame {
    let all_keys = store.keys(db);
    let matched: Vec<Frame> = all_keys
        .into_iter()
        .filter(|key| {
            if let Ok(key_str) = std::str::from_utf8(key) {
                glob_match(pattern, key_str)
            } else {
                // For binary keys, try raw pattern matching
                glob_match_bytes(pattern.as_bytes(), key)
            }
        })
        .map(|key| Frame::Bulk(Some(key)))
        .collect();

    Frame::Array(Some(matched))
}

/// TYPE command - get the type of a key
pub fn key_type(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(value) => {
            let type_name = match value {
                Value::String(_) => "string",
                Value::List(_) => "list",
                Value::Hash(_) => "hash",
                Value::Set(_) => "set",
                Value::SortedSet { .. } => "zset",
                Value::Stream(_) => "stream",
                Value::HyperLogLog(_) => "string", // HLL is stored as string in Redis
            };
            Frame::simple(type_name)
        }
        None => Frame::simple("none"),
    }
}

/// RENAME command - rename a key
pub fn rename(store: &Arc<Store>, db: u8, key: &Bytes, new_key: &Bytes) -> Frame {
    // Get the value and TTL of the old key
    match store.get_entry(db, key) {
        Some((value, expires_at)) => {
            // Delete old key
            store.del(db, &[key.clone()]);

            // Set new key with same value and TTL
            store.set_entry(db, new_key.clone(), value, expires_at);

            Frame::simple("OK")
        }
        None => Frame::error("ERR no such key"),
    }
}

/// RENAMENX command - rename a key only if new key doesn't exist
pub fn renamenx(store: &Arc<Store>, db: u8, key: &Bytes, new_key: &Bytes) -> Frame {
    // Check if new key exists
    if store.exists(db, &[new_key.clone()]) > 0 {
        return Frame::Integer(0);
    }

    // Get the value and TTL of the old key
    match store.get_entry(db, key) {
        Some((value, expires_at)) => {
            // Delete old key
            store.del(db, &[key.clone()]);

            // Set new key with same value and TTL
            store.set_entry(db, new_key.clone(), value, expires_at);

            Frame::Integer(1)
        }
        None => Frame::error("ERR no such key"),
    }
}

/// COPY command - copy a key to another key
pub fn copy(
    store: &Arc<Store>,
    src_db: u8,
    key: &Bytes,
    dest_key: &Bytes,
    dest_db: Option<u8>,
    replace: bool,
) -> Frame {
    let dest_db = dest_db.unwrap_or(src_db);

    // Get source value
    let entry = match store.get_entry(src_db, key) {
        Some(entry) => entry,
        None => return Frame::Integer(0),
    };

    let (value, expires_at) = entry;

    // Check if destination exists
    if store.exists(dest_db, &[dest_key.clone()]) > 0 && !replace {
        return Frame::Integer(0);
    }

    // Copy the value
    store.set_entry(dest_db, dest_key.clone(), value, expires_at);

    Frame::Integer(1)
}

/// UNLINK command - delete keys asynchronously (same as DEL for now)
pub fn unlink(store: &Arc<Store>, db: u8, keys: &[Bytes]) -> Frame {
    // In a full implementation, this would be async
    // For now, it's the same as DEL
    Frame::Integer(store.del(db, keys))
}

/// TOUCH command - update last access time of keys
pub fn touch(store: &Arc<Store>, db: u8, keys: &[Bytes]) -> Frame {
    Frame::Integer(store.touch(db, keys))
}

/// RANDOMKEY command - get a random key
pub fn randomkey(store: &Arc<Store>, db: u8) -> Frame {
    let keys = store.keys(db);
    if keys.is_empty() {
        return Frame::Null;
    }

    let idx = rand::thread_rng().gen_range(0..keys.len());
    Frame::Bulk(Some(keys[idx].clone()))
}

/// OBJECT ENCODING command - get the encoding of a key
pub fn object_encoding(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(value) => {
            let encoding = match value {
                Value::String(data) => {
                    // Check if it's an integer
                    if let Ok(s) = std::str::from_utf8(&data) {
                        if s.parse::<i64>().is_ok() {
                            "int"
                        } else if data.len() <= EMBSTR_MAX_LEN {
                            "embstr"
                        } else {
                            "raw"
                        }
                    } else {
                        "raw"
                    }
                }
                Value::List(list) => {
                    if list.len() <= LISTPACK_MAX_ENTRIES {
                        "listpack"
                    } else {
                        "quicklist"
                    }
                }
                Value::Hash(hash) => {
                    if hash.len() <= HASH_LISTPACK_MAX_ENTRIES {
                        "listpack"
                    } else {
                        "hashtable"
                    }
                }
                Value::Set(set) => {
                    // Check if all members are integers
                    let all_ints = set.iter().all(|m| {
                        std::str::from_utf8(m)
                            .map(|s| s.parse::<i64>().is_ok())
                            .unwrap_or(false)
                    });
                    if all_ints && set.len() <= SET_INTSET_MAX_ENTRIES {
                        "intset"
                    } else if set.len() <= SET_LISTPACK_MAX_ENTRIES {
                        "listpack"
                    } else {
                        "hashtable"
                    }
                }
                Value::SortedSet { by_member, .. } => {
                    if by_member.len() <= ZSET_LISTPACK_MAX_ENTRIES {
                        "listpack"
                    } else {
                        "skiplist"
                    }
                }
                Value::Stream(_) => "stream",
                Value::HyperLogLog(_) => "raw",
            };
            Frame::Bulk(Some(Bytes::from(encoding)))
        }
        None => Frame::Null,
    }
}

/// OBJECT FREQ command - get access frequency (LFU)
pub fn object_freq(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    match store.object_freq(db, key) {
        Some(count) => Frame::Integer(count as i64),
        None => Frame::Null,
    }
}

/// OBJECT IDLETIME command - get idle time in seconds
pub fn object_idletime(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    match store.object_idletime(db, key) {
        Some(idle) => Frame::Integer(idle),
        None => Frame::Null,
    }
}

/// OBJECT REFCOUNT command - get reference count (always 1 in our impl)
pub fn object_refcount(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    if store.get(db, key).is_some() {
        Frame::Integer(1)
    } else {
        Frame::Null
    }
}

/// EXPIREAT command - set expiration to Unix timestamp (seconds)
pub fn expireat(store: &Arc<Store>, db: u8, key: &Bytes, timestamp: u64) -> Frame {
    // Convert Unix timestamp to SystemTime
    let now_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    if timestamp <= now_unix {
        // Already expired - delete the key
        store.del(db, &[key.clone()]);
        return Frame::Integer(1);
    }

    let secs_from_now = timestamp - now_unix;
    let expires_at = SystemTime::now() + std::time::Duration::from_secs(secs_from_now);

    if store.expire(db, key, expires_at) {
        Frame::Integer(1)
    } else {
        Frame::Integer(0)
    }
}

/// PEXPIREAT command - set expiration to Unix timestamp (milliseconds)
pub fn pexpireat(store: &Arc<Store>, db: u8, key: &Bytes, timestamp_ms: u64) -> Frame {
    let now_unix_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    if timestamp_ms <= now_unix_ms {
        // Already expired - delete the key
        store.del(db, &[key.clone()]);
        return Frame::Integer(1);
    }

    let ms_from_now = timestamp_ms - now_unix_ms;
    let expires_at = SystemTime::now() + std::time::Duration::from_millis(ms_from_now);

    if store.expire(db, key, expires_at) {
        Frame::Integer(1)
    } else {
        Frame::Integer(0)
    }
}

/// EXPIRETIME command - get expiration as Unix timestamp (seconds)
pub fn expiretime(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    match store.get_entry(db, key) {
        Some((_, Some(expires_at))) => {
            let now = SystemTime::now();
            if expires_at <= now {
                Frame::Integer(-2) // Key expired
            } else {
                let unix_secs = expires_at
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                Frame::Integer(unix_secs as i64)
            }
        }
        Some((_, None)) => Frame::Integer(-1), // No TTL
        None => Frame::Integer(-2),            // Key doesn't exist
    }
}

/// PEXPIRETIME command - get expiration as Unix timestamp (milliseconds)
pub fn pexpiretime(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    match store.get_entry(db, key) {
        Some((_, Some(expires_at))) => {
            let now = SystemTime::now();
            if expires_at <= now {
                Frame::Integer(-2)
            } else {
                let unix_ms = expires_at
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                Frame::Integer(unix_ms as i64)
            }
        }
        Some((_, None)) => Frame::Integer(-1),
        None => Frame::Integer(-2),
    }
}

// ============================================================================
// Pattern matching helpers
// ============================================================================

/// Glob-style pattern matching for strings
fn glob_match(pattern: &str, text: &str) -> bool {
    glob_match_impl(pattern.as_bytes(), text.as_bytes())
}

/// Glob-style pattern matching for bytes
fn glob_match_bytes(pattern: &[u8], text: &[u8]) -> bool {
    glob_match_impl(pattern, text)
}

/// Core glob matching implementation
fn glob_match_impl(pattern: &[u8], text: &[u8]) -> bool {
    let mut p = 0;
    let mut t = 0;
    let mut star_p = None;
    let mut star_t = 0;

    while t < text.len() {
        if p < pattern.len() {
            match pattern[p] {
                b'*' => {
                    star_p = Some(p);
                    star_t = t;
                    p += 1;
                    continue;
                }
                b'?' => {
                    p += 1;
                    t += 1;
                    continue;
                }
                b'[' => {
                    // Character class
                    if let Some((matches, new_p)) = match_char_class(&pattern[p..], text[t]) {
                        if matches {
                            p += new_p;
                            t += 1;
                            continue;
                        }
                    }
                }
                b'\\' if p + 1 < pattern.len() => {
                    // Escaped character
                    if pattern[p + 1] == text[t] {
                        p += 2;
                        t += 1;
                        continue;
                    }
                }
                c => {
                    if c == text[t] {
                        p += 1;
                        t += 1;
                        continue;
                    }
                }
            }
        }

        // No match - try to backtrack to last *
        if let Some(sp) = star_p {
            p = sp + 1;
            star_t += 1;
            t = star_t;
        } else {
            return false;
        }
    }

    // Check remaining pattern (should only be *)
    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }

    p == pattern.len()
}

/// Match a character class like [abc], [^abc], [a-z]
/// Returns (matched, bytes_consumed) or None if invalid
fn match_char_class(pattern: &[u8], ch: u8) -> Option<(bool, usize)> {
    if pattern.is_empty() || pattern[0] != b'[' {
        return None;
    }

    let mut i = 1;
    let negate = if i < pattern.len() && pattern[i] == b'^' {
        i += 1;
        true
    } else {
        false
    };

    let mut matched = false;
    let mut prev_char = None;

    while i < pattern.len() && pattern[i] != b']' {
        if pattern[i] == b'-'
            && prev_char.is_some()
            && i + 1 < pattern.len()
            && pattern[i + 1] != b']'
        {
            // Range like a-z
            let start = prev_char.unwrap_or_default();
            let end = pattern[i + 1];
            if ch >= start && ch <= end {
                matched = true;
            }
            i += 2;
            prev_char = None;
        } else {
            if pattern[i] == ch {
                matched = true;
            }
            prev_char = Some(pattern[i]);
            i += 1;
        }
    }

    if i < pattern.len() && pattern[i] == b']' {
        Some((if negate { !matched } else { matched }, i + 1))
    } else {
        None // Unclosed bracket
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_store() -> Arc<Store> {
        Arc::new(Store::new(16))
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("hello*", "hello"));
        assert!(glob_match("hello*", "helloworld"));
        assert!(glob_match("*world", "helloworld"));
        assert!(glob_match("h?llo", "hello"));
        assert!(glob_match("h?llo", "hallo"));
        assert!(!glob_match("h?llo", "hllo"));
        assert!(glob_match("h[ae]llo", "hello"));
        assert!(glob_match("h[ae]llo", "hallo"));
        assert!(!glob_match("h[ae]llo", "hillo"));
        assert!(glob_match("h[^ae]llo", "hillo"));
        assert!(!glob_match("h[^ae]llo", "hello"));
        assert!(glob_match("h[a-z]llo", "hello"));
        assert!(glob_match("user:*", "user:123"));
        assert!(glob_match("user:*:name", "user:123:name"));
    }

    #[test]
    fn test_keys_pattern() {
        let store = create_store();

        store.set(0, Bytes::from("user:1"), Value::String(Bytes::from("a")));
        store.set(0, Bytes::from("user:2"), Value::String(Bytes::from("b")));
        store.set(0, Bytes::from("user:3"), Value::String(Bytes::from("c")));
        store.set(0, Bytes::from("session:1"), Value::String(Bytes::from("d")));

        let result = keys(&store, 0, "user:*");
        match result {
            Frame::Array(Some(arr)) => {
                assert_eq!(arr.len(), 3);
            }
            _ => panic!("Expected array"),
        }

        let result = keys(&store, 0, "*:1");
        match result {
            Frame::Array(Some(arr)) => {
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_type_command() {
        let store = create_store();
        let key = Bytes::from("key");

        // String
        store.set(0, key.clone(), Value::String(Bytes::from("value")));
        assert!(matches!(key_type(&store, 0, &key), Frame::Simple(s) if s == "string"));

        // List
        store.set(
            0,
            key.clone(),
            Value::List(std::collections::VecDeque::new()),
        );
        assert!(matches!(key_type(&store, 0, &key), Frame::Simple(s) if s == "list"));

        // Non-existent
        assert!(
            matches!(key_type(&store, 0, &Bytes::from("nonexistent")), Frame::Simple(s) if s == "none")
        );
    }

    #[test]
    fn test_rename() {
        let store = create_store();
        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");

        store.set(0, key1.clone(), Value::String(Bytes::from("value")));

        let result = rename(&store, 0, &key1, &key2);
        assert!(matches!(result, Frame::Simple(s) if s == "OK"));

        // Old key should be gone
        assert!(store.get(0, &key1).is_none());

        // New key should have the value
        match store.get(0, &key2) {
            Some(Value::String(data)) => assert_eq!(data, Bytes::from("value")),
            _ => panic!("Expected string value"),
        }
    }

    #[test]
    fn test_renamenx() {
        let store = create_store();
        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");
        let key3 = Bytes::from("key3");

        store.set(0, key1.clone(), Value::String(Bytes::from("value1")));
        store.set(0, key2.clone(), Value::String(Bytes::from("value2")));

        // Should fail - key2 exists
        let result = renamenx(&store, 0, &key1, &key2);
        assert!(matches!(result, Frame::Integer(0)));

        // Should succeed - key3 doesn't exist
        let result = renamenx(&store, 0, &key1, &key3);
        assert!(matches!(result, Frame::Integer(1)));
    }

    #[test]
    fn test_copy() {
        let store = create_store();
        let src = Bytes::from("src");
        let dst = Bytes::from("dst");

        store.set(0, src.clone(), Value::String(Bytes::from("value")));

        let result = copy(&store, 0, &src, &dst, None, false);
        assert!(matches!(result, Frame::Integer(1)));

        // Both keys should exist
        assert!(store.get(0, &src).is_some());
        assert!(store.get(0, &dst).is_some());
    }

    #[test]
    fn test_randomkey() {
        let store = create_store();

        // Empty db
        let result = randomkey(&store, 0);
        assert!(matches!(result, Frame::Null));

        // With keys
        store.set(0, Bytes::from("key1"), Value::String(Bytes::from("a")));
        store.set(0, Bytes::from("key2"), Value::String(Bytes::from("b")));

        let result = randomkey(&store, 0);
        assert!(matches!(result, Frame::Bulk(Some(_))));
    }
}
