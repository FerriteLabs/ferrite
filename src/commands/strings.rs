//! String command implementations
//!
//! This module implements string-specific commands like INCR, DECR, APPEND, STRLEN.

use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::{Store, Value};

/// Helper for generating wrongtype errors (marked cold for optimization)
#[cold]
#[inline(never)]
fn handle_wrongtype_error() -> Frame {
    Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
}

/// Helper for generating integer parse errors (marked cold for optimization)
#[cold]
#[inline(never)]
fn handle_not_integer_error() -> Frame {
    Frame::error("ERR value is not an integer or out of range")
}

/// Helper for generating overflow errors (marked cold for optimization)
#[cold]
#[inline(never)]
fn handle_overflow_error() -> Frame {
    Frame::error("ERR increment or decrement would overflow")
}

/// Helper for generating float parse errors (marked cold for optimization)
#[cold]
#[inline(never)]
fn handle_not_float_error() -> Frame {
    Frame::error("ERR value is not a valid float")
}

/// Helper for generating NaN/Infinity errors (marked cold for optimization)
#[cold]
#[inline(never)]
fn handle_nan_infinity_error() -> Frame {
    Frame::error("ERR increment would produce NaN or Infinity")
}

/// Cache for small integer string representations.
/// This avoids allocations for commonly used counter values.
const CACHED_INT_MIN: i64 = -100;
const CACHED_INT_MAX: i64 = 10000;

/// Static cache of pre-allocated Bytes for small integers
static INT_CACHE: OnceLock<Vec<Bytes>> = OnceLock::new();

/// Get or initialize the integer cache
#[inline]
fn get_int_cache() -> &'static Vec<Bytes> {
    INT_CACHE.get_or_init(|| {
        (CACHED_INT_MIN..=CACHED_INT_MAX)
            .map(|i| Bytes::from(i.to_string()))
            .collect()
    })
}

/// Convert an integer to Bytes, using the cache for small values.
/// This is a performance optimization for INCR/DECR operations.
#[inline]
fn int_to_bytes(value: i64) -> Bytes {
    if (CACHED_INT_MIN..=CACHED_INT_MAX).contains(&value) {
        let cache = get_int_cache();
        let index = (value - CACHED_INT_MIN) as usize;
        cache[index].clone()
    } else {
        Bytes::from(value.to_string())
    }
}

/// INCR command - increment integer value
#[inline]
pub fn incr(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    incrby(store, db, key, 1)
}

/// DECR command - decrement integer value
#[inline]
pub fn decr(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    incrby(store, db, key, -1)
}

/// INCRBY command - increment by delta
#[inline]
pub fn incrby(store: &Arc<Store>, db: u8, key: &Bytes, delta: i64) -> Frame {
    // Get current value
    let current = match store.get(db, key) {
        Some(Value::String(data)) => match std::str::from_utf8(&data) {
            Ok(s) => match s.parse::<i64>() {
                Ok(n) => n,
                Err(_) => return handle_not_integer_error(),
            },
            Err(_) => return handle_not_integer_error(),
        },
        Some(_) => return handle_wrongtype_error(),
        None => 0,
    };

    // Check for overflow
    let new_value = match current.checked_add(delta) {
        Some(n) => n,
        None => return handle_overflow_error(),
    };

    // Store new value - use cached Bytes for common small integers
    store.set(db, key.clone(), Value::String(int_to_bytes(new_value)));

    Frame::Integer(new_value)
}

/// DECRBY command - decrement by delta
#[inline]
pub fn decrby(store: &Arc<Store>, db: u8, key: &Bytes, delta: i64) -> Frame {
    // DECRBY is just INCRBY with negated delta
    match delta.checked_neg() {
        Some(neg_delta) => incrby(store, db, key, neg_delta),
        None => handle_overflow_error(),
    }
}

/// APPEND command - append to string value
#[inline]
pub fn append(store: &Arc<Store>, db: u8, key: &Bytes, value: &Bytes) -> Frame {
    // Get current value and append
    let new_value = match store.get(db, key) {
        Some(Value::String(data)) => {
            // Pre-allocate capacity to avoid reallocation
            let mut vec = Vec::with_capacity(data.len() + value.len());
            vec.extend_from_slice(&data[..]);
            vec.extend_from_slice(&value[..]);
            Bytes::from(vec)
        }
        Some(_) => return handle_wrongtype_error(),
        None => value.clone(),
    };

    let len = new_value.len() as i64;
    store.set(db, key.clone(), Value::String(new_value));

    Frame::Integer(len)
}

/// STRLEN command - get string length
#[inline]
pub fn strlen(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(Value::String(data)) => Frame::Integer(data.len() as i64),
        Some(_) => handle_wrongtype_error(),
        None => Frame::Integer(0),
    }
}

/// GETRANGE command - get substring
#[inline]
pub fn getrange(store: &Arc<Store>, db: u8, key: &Bytes, start: i64, end: i64) -> Frame {
    match store.get(db, key) {
        Some(Value::String(data)) => {
            let len = data.len() as i64;
            if len == 0 {
                return Frame::bulk(Bytes::new());
            }

            // Handle negative indices (from end)
            let start = if start < 0 {
                (len + start).max(0) as usize
            } else {
                start.min(len) as usize
            };

            let end = if end < 0 {
                (len + end).max(0) as usize
            } else {
                end.min(len - 1) as usize
            };

            if start > end || start >= data.len() {
                Frame::bulk(Bytes::new())
            } else {
                Frame::bulk(data.slice(start..=end))
            }
        }
        Some(_) => handle_wrongtype_error(),
        None => Frame::bulk(Bytes::new()),
    }
}

/// SETRANGE command - overwrite part of string at offset
#[inline]
pub fn setrange(store: &Arc<Store>, db: u8, key: &Bytes, offset: i64, value: &Bytes) -> Frame {
    let offset = offset as usize;

    // Get current value or empty
    let mut current = match store.get(db, key) {
        Some(Value::String(data)) => {
            // Pre-allocate capacity for the final size if we know we need to extend
            let end = offset + value.len();
            if end > data.len() {
                let mut vec = Vec::with_capacity(end);
                vec.extend_from_slice(&data[..]);
                vec
            } else {
                data.to_vec()
            }
        }
        Some(_) => return handle_wrongtype_error(),
        None => {
            // Pre-allocate for offset + value length
            Vec::with_capacity(offset + value.len())
        }
    };

    // Extend with null bytes if needed
    if offset > current.len() {
        current.resize(offset, 0);
    }

    // Overwrite or extend
    let end = offset + value.len();
    if end > current.len() {
        current.resize(end, 0);
    }

    current[offset..end].copy_from_slice(value);

    let new_len = current.len() as i64;
    store.set(db, key.clone(), Value::String(Bytes::from(current)));

    Frame::Integer(new_len)
}

/// SETNX command - set if not exists
pub fn setnx(store: &Arc<Store>, db: u8, key: &Bytes, value: &Bytes) -> Frame {
    if store.get(db, key).is_some() {
        Frame::Integer(0)
    } else {
        store.set(db, key.clone(), Value::String(value.clone()));
        Frame::Integer(1)
    }
}

/// SETEX command - set with expiration in seconds
pub fn setex(store: &Arc<Store>, db: u8, key: &Bytes, seconds: u64, value: &Bytes) -> Frame {
    let expires_at = SystemTime::now() + Duration::from_secs(seconds);
    store.set_with_expiry(db, key.clone(), Value::String(value.clone()), expires_at);
    Frame::simple("OK")
}

/// PSETEX command - set with expiration in milliseconds
pub fn psetex(store: &Arc<Store>, db: u8, key: &Bytes, milliseconds: u64, value: &Bytes) -> Frame {
    let expires_at = SystemTime::now() + Duration::from_millis(milliseconds);
    store.set_with_expiry(db, key.clone(), Value::String(value.clone()), expires_at);
    Frame::simple("OK")
}

/// GETSET command - atomically set and return old value
pub fn getset(store: &Arc<Store>, db: u8, key: &Bytes, value: &Bytes) -> Frame {
    let old_value = store.get(db, key);
    let response = match old_value {
        Some(Value::String(data)) => Frame::bulk(data),
        _ => Frame::null(),
    };

    store.set(db, key.clone(), Value::String(value.clone()));
    response
}

/// GETDEL command - get and delete
pub fn getdel(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    // First get the value, then delete
    match store.get(db, key) {
        Some(Value::String(data)) => {
            store.del(db, &[key.clone()]);
            Frame::bulk(data)
        }
        _ => Frame::null(),
    }
}

/// INCRBYFLOAT command - increment by float delta
#[inline]
pub fn incrbyfloat(store: &Arc<Store>, db: u8, key: &Bytes, delta: f64) -> Frame {
    // Get current value
    let current = match store.get(db, key) {
        Some(Value::String(data)) => match std::str::from_utf8(&data) {
            Ok(s) => match s.parse::<f64>() {
                Ok(n) => n,
                Err(_) => return handle_not_float_error(),
            },
            Err(_) => return handle_not_float_error(),
        },
        Some(_) => return handle_wrongtype_error(),
        None => 0.0,
    };

    let new_value = current + delta;

    // Check for infinity/NaN
    if new_value.is_infinite() || new_value.is_nan() {
        return handle_nan_infinity_error();
    }

    // Format the result - remove trailing zeros for clean output
    let result_str = format_float(new_value);
    store.set(
        db,
        key.clone(),
        Value::String(Bytes::from(result_str.clone())),
    );

    Frame::bulk(Bytes::from(result_str))
}

/// Format float like Redis does (remove unnecessary trailing zeros)
fn format_float(value: f64) -> String {
    // Format with enough precision
    let s = format!("{:.17}", value);
    // Trim trailing zeros after decimal point
    let s = s.trim_end_matches('0');
    // If we trimmed all digits after the decimal, keep one zero
    let s = if s.ends_with('.') {
        format!("{}0", s)
    } else {
        s.to_string()
    };
    s
}

/// GETEX command - get value and optionally set expiration
#[allow(clippy::too_many_arguments)]
pub fn getex(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    ex: Option<u64>,
    px: Option<u64>,
    exat: Option<u64>,
    pxat: Option<u64>,
    persist: bool,
) -> Frame {
    // Get the value first
    let value = match store.get(db, key) {
        Some(Value::String(data)) => data,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => return Frame::null(),
    };

    // Handle expiration options
    if persist {
        // Remove expiration
        store.persist(db, key);
    } else if let Some(seconds) = ex {
        let expires_at = SystemTime::now() + Duration::from_secs(seconds);
        store.expire(db, key, expires_at);
    } else if let Some(milliseconds) = px {
        let expires_at = SystemTime::now() + Duration::from_millis(milliseconds);
        store.expire(db, key, expires_at);
    } else if let Some(timestamp) = exat {
        // Unix timestamp in seconds
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if timestamp > now_unix {
            let duration = Duration::from_secs(timestamp - now_unix);
            let expires_at = SystemTime::now() + duration;
            store.expire(db, key, expires_at);
        } else {
            // Already expired, delete the key
            store.del(db, &[key.clone()]);
            return Frame::null();
        }
    } else if let Some(timestamp_ms) = pxat {
        // Unix timestamp in milliseconds
        let now_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        if timestamp_ms > now_unix_ms {
            let duration = Duration::from_millis(timestamp_ms - now_unix_ms);
            let expires_at = SystemTime::now() + duration;
            store.expire(db, key, expires_at);
        } else {
            // Already expired, delete the key
            store.del(db, &[key.clone()]);
            return Frame::null();
        }
    }

    Frame::bulk(value)
}

/// MSETNX command - set multiple keys only if none exist
pub fn msetnx(store: &Arc<Store>, db: u8, pairs: &[(Bytes, Bytes)]) -> Frame {
    // First check if any key exists
    for (key, _) in pairs {
        if store.get(db, key).is_some() {
            return Frame::Integer(0);
        }
    }

    // None exist, set all
    for (key, value) in pairs {
        store.set(db, key.clone(), Value::String(value.clone()));
    }

    Frame::Integer(1)
}

/// LCS command - longest common subsequence
#[allow(clippy::too_many_arguments)]
pub fn lcs(
    store: &Arc<Store>,
    db: u8,
    key1: &Bytes,
    key2: &Bytes,
    len_only: bool,
    idx: bool,
    min_match_len: usize,
    with_match_len: bool,
) -> Frame {
    // Get both values
    let val1 = match store.get(db, key1) {
        Some(Value::String(data)) => data,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => Bytes::new(),
    };

    let val2 = match store.get(db, key2) {
        Some(Value::String(data)) => data,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => Bytes::new(),
    };

    let s1 = val1.as_ref();
    let s2 = val2.as_ref();
    let m = s1.len();
    let n = s2.len();

    // Build DP table with pre-allocated capacity
    let mut dp = Vec::with_capacity(m + 1);
    for _ in 0..=m {
        dp.push(vec![0usize; n + 1]);
    }
    for i in 1..=m {
        for j in 1..=n {
            if s1[i - 1] == s2[j - 1] {
                dp[i][j] = dp[i - 1][j - 1] + 1;
            } else {
                dp[i][j] = dp[i - 1][j].max(dp[i][j - 1]);
            }
        }
    }

    let lcs_len = dp[m][n];

    if len_only {
        return Frame::Integer(lcs_len as i64);
    }

    if idx {
        // Return matches with indices
        let matches = find_lcs_matches(s1, s2, &dp, min_match_len);

        let mut result = Vec::new();

        // "matches" array
        result.push(Frame::bulk(Bytes::from("matches")));
        let match_frames: Vec<Frame> = matches
            .iter()
            .map(|(start1, end1, start2, end2, match_len)| {
                let mut match_arr = vec![
                    Frame::array(vec![
                        Frame::Integer(*start1 as i64),
                        Frame::Integer(*end1 as i64),
                    ]),
                    Frame::array(vec![
                        Frame::Integer(*start2 as i64),
                        Frame::Integer(*end2 as i64),
                    ]),
                ];
                if with_match_len {
                    match_arr.push(Frame::Integer(*match_len as i64));
                }
                Frame::array(match_arr)
            })
            .collect();
        result.push(Frame::array(match_frames));

        // "len"
        result.push(Frame::bulk(Bytes::from("len")));
        result.push(Frame::Integer(lcs_len as i64));

        return Frame::array(result);
    }

    // Reconstruct LCS string
    let mut lcs = Vec::with_capacity(lcs_len);
    let mut i = m;
    let mut j = n;
    while i > 0 && j > 0 {
        if s1[i - 1] == s2[j - 1] {
            lcs.push(s1[i - 1]);
            i -= 1;
            j -= 1;
        } else if dp[i - 1][j] > dp[i][j - 1] {
            i -= 1;
        } else {
            j -= 1;
        }
    }
    lcs.reverse();

    Frame::bulk(Bytes::from(lcs))
}

/// Find matching segments for LCS IDX option
fn find_lcs_matches(
    s1: &[u8],
    s2: &[u8],
    dp: &[Vec<usize>],
    min_match_len: usize,
) -> Vec<(usize, usize, usize, usize, usize)> {
    let m = s1.len();
    let n = s2.len();
    let mut matches = Vec::new();

    let mut i = m;
    let mut j = n;

    while i > 0 && j > 0 {
        if s1[i - 1] == s2[j - 1] {
            // Found a match, trace back to find the extent
            let end1 = i - 1;
            let end2 = j - 1;
            let mut match_len = 0;

            while i > 0 && j > 0 && s1[i - 1] == s2[j - 1] {
                match_len += 1;
                i -= 1;
                j -= 1;
            }

            let start1 = i;
            let start2 = j;

            if match_len >= min_match_len {
                matches.push((start1, end1, start2, end2, match_len));
            }
        } else if dp[i - 1][j] > dp[i][j - 1] {
            i -= 1;
        } else {
            j -= 1;
        }
    }

    matches
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn create_store() -> Arc<Store> {
        Arc::new(Store::new(16))
    }

    #[test]
    fn test_incr_new_key() {
        let store = create_store();
        let key = Bytes::from("counter");

        let response = incr(&store, 0, &key);
        assert!(matches!(response, Frame::Integer(1)));

        let response = incr(&store, 0, &key);
        assert!(matches!(response, Frame::Integer(2)));
    }

    #[test]
    fn test_incr_existing_value() {
        let store = create_store();
        let key = Bytes::from("counter");

        store.set(0, key.clone(), Value::String(Bytes::from("10")));

        let response = incr(&store, 0, &key);
        assert!(matches!(response, Frame::Integer(11)));
    }

    #[test]
    fn test_incr_not_integer() {
        let store = create_store();
        let key = Bytes::from("key");

        store.set(0, key.clone(), Value::String(Bytes::from("not a number")));

        let response = incr(&store, 0, &key);
        assert!(matches!(response, Frame::Error(_)));
    }

    #[test]
    fn test_decr() {
        let store = create_store();
        let key = Bytes::from("counter");

        store.set(0, key.clone(), Value::String(Bytes::from("10")));

        let response = decr(&store, 0, &key);
        assert!(matches!(response, Frame::Integer(9)));
    }

    #[test]
    fn test_incrby() {
        let store = create_store();
        let key = Bytes::from("counter");

        store.set(0, key.clone(), Value::String(Bytes::from("10")));

        let response = incrby(&store, 0, &key, 5);
        assert!(matches!(response, Frame::Integer(15)));
    }

    #[test]
    fn test_decrby() {
        let store = create_store();
        let key = Bytes::from("counter");

        store.set(0, key.clone(), Value::String(Bytes::from("10")));

        let response = decrby(&store, 0, &key, 3);
        assert!(matches!(response, Frame::Integer(7)));
    }

    #[test]
    fn test_append() {
        let store = create_store();
        let key = Bytes::from("key");

        // Append to non-existing key
        let response = append(&store, 0, &key, &Bytes::from("Hello"));
        assert!(matches!(response, Frame::Integer(5)));

        // Append to existing key
        let response = append(&store, 0, &key, &Bytes::from(" World"));
        assert!(matches!(response, Frame::Integer(11)));

        // Verify value
        match store.get(0, &key) {
            Some(Value::String(data)) => assert_eq!(data, Bytes::from("Hello World")),
            _ => panic!("Expected string value"),
        }
    }

    #[test]
    fn test_strlen() {
        let store = create_store();
        let key = Bytes::from("key");

        // Non-existing key
        let response = strlen(&store, 0, &key);
        assert!(matches!(response, Frame::Integer(0)));

        // Existing key
        store.set(0, key.clone(), Value::String(Bytes::from("Hello")));
        let response = strlen(&store, 0, &key);
        assert!(matches!(response, Frame::Integer(5)));
    }

    #[test]
    fn test_getrange() {
        let store = create_store();
        let key = Bytes::from("key");

        store.set(0, key.clone(), Value::String(Bytes::from("Hello World")));

        // Normal range
        let response = getrange(&store, 0, &key, 0, 4);
        match response {
            Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("Hello")),
            _ => panic!("Expected bulk string"),
        }

        // Negative indices
        let response = getrange(&store, 0, &key, -5, -1);
        match response {
            Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("World")),
            _ => panic!("Expected bulk string"),
        }

        // Non-existing key
        let response = getrange(&store, 0, &Bytes::from("nonexistent"), 0, 10);
        match response {
            Frame::Bulk(Some(data)) => assert!(data.is_empty()),
            _ => panic!("Expected empty bulk string"),
        }
    }

    #[test]
    fn test_setrange() {
        let store = create_store();
        let key = Bytes::from("key");

        store.set(0, key.clone(), Value::String(Bytes::from("Hello World")));

        // Overwrite part of string
        let response = setrange(&store, 0, &key, 6, &Bytes::from("Redis"));
        assert!(matches!(response, Frame::Integer(11)));

        match store.get(0, &key) {
            Some(Value::String(data)) => assert_eq!(data, Bytes::from("Hello Redis")),
            _ => panic!("Expected string value"),
        }

        // Padding with null bytes
        let key2 = Bytes::from("key2");
        let response = setrange(&store, 0, &key2, 5, &Bytes::from("test"));
        assert!(matches!(response, Frame::Integer(9)));
    }

    #[test]
    fn test_setnx() {
        let store = create_store();
        let key = Bytes::from("key");

        // Set non-existing key
        let response = setnx(&store, 0, &key, &Bytes::from("value1"));
        assert!(matches!(response, Frame::Integer(1)));

        // Try to set existing key
        let response = setnx(&store, 0, &key, &Bytes::from("value2"));
        assert!(matches!(response, Frame::Integer(0)));

        // Value should still be original
        match store.get(0, &key) {
            Some(Value::String(data)) => assert_eq!(data, Bytes::from("value1")),
            _ => panic!("Expected string value"),
        }
    }

    #[test]
    fn test_setex() {
        let store = create_store();
        let key = Bytes::from("key");

        let response = setex(&store, 0, &key, 100, &Bytes::from("value"));
        assert!(matches!(response, Frame::Simple(s) if s == "OK"));

        // Value should be set
        match store.get(0, &key) {
            Some(Value::String(data)) => assert_eq!(data, Bytes::from("value")),
            _ => panic!("Expected string value"),
        }

        // Should have TTL
        let ttl = store.ttl(0, &key);
        assert!(ttl.is_some());
        assert!(ttl.unwrap() > 0);
    }

    #[test]
    fn test_psetex() {
        let store = create_store();
        let key = Bytes::from("key");

        let response = psetex(&store, 0, &key, 100000, &Bytes::from("value"));
        assert!(matches!(response, Frame::Simple(s) if s == "OK"));

        // Value should be set
        match store.get(0, &key) {
            Some(Value::String(data)) => assert_eq!(data, Bytes::from("value")),
            _ => panic!("Expected string value"),
        }
    }

    #[test]
    fn test_getset() {
        let store = create_store();
        let key = Bytes::from("key");

        // GetSet on non-existing key
        let response = getset(&store, 0, &key, &Bytes::from("value1"));
        assert!(matches!(response, Frame::Bulk(None)));

        // GetSet on existing key
        let response = getset(&store, 0, &key, &Bytes::from("value2"));
        match response {
            Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("value1")),
            _ => panic!("Expected bulk string"),
        }

        // Verify new value
        match store.get(0, &key) {
            Some(Value::String(data)) => assert_eq!(data, Bytes::from("value2")),
            _ => panic!("Expected string value"),
        }
    }

    #[test]
    fn test_getdel() {
        let store = create_store();
        let key = Bytes::from("key");

        store.set(0, key.clone(), Value::String(Bytes::from("value")));

        // Get and delete
        let response = getdel(&store, 0, &key);
        match response {
            Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("value")),
            _ => panic!("Expected bulk string"),
        }

        // Key should be deleted
        assert!(store.get(0, &key).is_none());

        // GetDel on non-existing key
        let response = getdel(&store, 0, &key);
        assert!(matches!(response, Frame::Bulk(None)));
    }

    #[test]
    fn test_int_to_bytes_cached() {
        // Test values within cache range
        assert_eq!(int_to_bytes(0), Bytes::from("0"));
        assert_eq!(int_to_bytes(1), Bytes::from("1"));
        assert_eq!(int_to_bytes(100), Bytes::from("100"));
        assert_eq!(int_to_bytes(-1), Bytes::from("-1"));
        assert_eq!(int_to_bytes(-100), Bytes::from("-100"));
        assert_eq!(int_to_bytes(10000), Bytes::from("10000"));

        // Verify cache returns same instance (clone of same Arc)
        let bytes1 = int_to_bytes(42);
        let bytes2 = int_to_bytes(42);
        // Both should have same content
        assert_eq!(bytes1, bytes2);
    }

    #[test]
    fn test_int_to_bytes_uncached() {
        // Test values outside cache range
        assert_eq!(int_to_bytes(-101), Bytes::from("-101"));
        assert_eq!(int_to_bytes(10001), Bytes::from("10001"));
        assert_eq!(int_to_bytes(100000), Bytes::from("100000"));
        assert_eq!(int_to_bytes(-1000), Bytes::from("-1000"));
        assert_eq!(int_to_bytes(i64::MAX), Bytes::from(i64::MAX.to_string()));
        assert_eq!(int_to_bytes(i64::MIN), Bytes::from(i64::MIN.to_string()));
    }

    #[test]
    fn test_incr_uses_cache() {
        let store = create_store();
        let key = Bytes::from("counter");

        // Increment to a cached value and verify storage
        let response = incr(&store, 0, &key);
        assert!(matches!(response, Frame::Integer(1)));

        // Verify the stored value is correct
        match store.get(0, &key) {
            Some(Value::String(data)) => assert_eq!(data, Bytes::from("1")),
            _ => panic!("Expected string value"),
        }

        // Increment many times within cache range
        for i in 2..=100 {
            let response = incr(&store, 0, &key);
            assert!(matches!(response, Frame::Integer(n) if n == i));
        }
    }
}
