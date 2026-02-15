//! Hash command implementations
//!
//! This module implements hash-specific commands like HSET, HGET, HMSET, HMGET.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::{Store, Value};

/// HSET command - set field(s) in hash
pub fn hset(store: &Arc<Store>, db: u8, key: &Bytes, pairs: &[(Bytes, Bytes)]) -> Frame {
    let mut hash = match store.get(db, key) {
        Some(Value::Hash(existing)) => existing,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => HashMap::new(),
    };

    let mut new_fields = 0i64;
    for (field, value) in pairs {
        if !hash.contains_key(field) {
            new_fields += 1;
        }
        hash.insert(field.clone(), value.clone());
    }

    store.set(db, key.clone(), Value::Hash(hash));
    Frame::Integer(new_fields)
}

/// HGET command - get a single field from hash
pub fn hget(store: &Arc<Store>, db: u8, key: &Bytes, field: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(Value::Hash(hash)) => match hash.get(field) {
            Some(value) => Frame::bulk(value.clone()),
            None => Frame::null(),
        },
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::null(),
    }
}

/// HMSET command - set multiple fields in hash (returns OK)
pub fn hmset(store: &Arc<Store>, db: u8, key: &Bytes, pairs: &[(Bytes, Bytes)]) -> Frame {
    let mut hash = match store.get(db, key) {
        Some(Value::Hash(existing)) => existing,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => HashMap::new(),
    };

    for (field, value) in pairs {
        hash.insert(field.clone(), value.clone());
    }

    store.set(db, key.clone(), Value::Hash(hash));
    Frame::simple("OK")
}

/// HMGET command - get multiple fields from hash
pub fn hmget(store: &Arc<Store>, db: u8, key: &Bytes, fields: &[Bytes]) -> Frame {
    let values: Vec<Frame> = match store.get(db, key) {
        Some(Value::Hash(hash)) => fields
            .iter()
            .map(|field| match hash.get(field) {
                Some(value) => Frame::bulk(value.clone()),
                None => Frame::null(),
            })
            .collect(),
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => fields.iter().map(|_| Frame::null()).collect(),
    };

    Frame::array(values)
}

/// HDEL command - delete field(s) from hash
pub fn hdel(store: &Arc<Store>, db: u8, key: &Bytes, fields: &[Bytes]) -> Frame {
    let mut hash = match store.get(db, key) {
        Some(Value::Hash(existing)) => existing,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => return Frame::Integer(0),
    };

    let mut deleted = 0i64;
    for field in fields {
        if hash.remove(field).is_some() {
            deleted += 1;
        }
    }

    if hash.is_empty() {
        store.del(db, &[key.clone()]);
    } else {
        store.set(db, key.clone(), Value::Hash(hash));
    }

    Frame::Integer(deleted)
}

/// HEXISTS command - check if field exists in hash
pub fn hexists(store: &Arc<Store>, db: u8, key: &Bytes, field: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(Value::Hash(hash)) => {
            if hash.contains_key(field) {
                Frame::Integer(1)
            } else {
                Frame::Integer(0)
            }
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::Integer(0),
    }
}

/// HGETALL command - get all fields and values from hash
pub fn hgetall(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(Value::Hash(hash)) => {
            let mut result = Vec::with_capacity(hash.len() * 2);
            for (field, value) in hash.iter() {
                result.push(Frame::bulk(field.clone()));
                result.push(Frame::bulk(value.clone()));
            }
            Frame::array(result)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::array(vec![]),
    }
}

/// HKEYS command - get all field names from hash
pub fn hkeys(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(Value::Hash(hash)) => {
            let result: Vec<Frame> = hash.keys().map(|k| Frame::bulk(k.clone())).collect();
            Frame::array(result)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::array(vec![]),
    }
}

/// HVALS command - get all values from hash
pub fn hvals(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(Value::Hash(hash)) => {
            let result: Vec<Frame> = hash.values().map(|v| Frame::bulk(v.clone())).collect();
            Frame::array(result)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::array(vec![]),
    }
}

/// HLEN command - get number of fields in hash
pub fn hlen(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(Value::Hash(hash)) => Frame::Integer(hash.len() as i64),
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::Integer(0),
    }
}

/// HSETNX command - set field only if it doesn't exist
pub fn hsetnx(store: &Arc<Store>, db: u8, key: &Bytes, field: &Bytes, value: &Bytes) -> Frame {
    let mut hash = match store.get(db, key) {
        Some(Value::Hash(existing)) => existing,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => HashMap::new(),
    };

    if hash.contains_key(field) {
        return Frame::Integer(0);
    }

    hash.insert(field.clone(), value.clone());
    store.set(db, key.clone(), Value::Hash(hash));
    Frame::Integer(1)
}

/// HINCRBY command - increment field by integer
pub fn hincrby(store: &Arc<Store>, db: u8, key: &Bytes, field: &Bytes, delta: i64) -> Frame {
    let mut hash = match store.get(db, key) {
        Some(Value::Hash(existing)) => existing,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => HashMap::new(),
    };

    let current = match hash.get(field) {
        Some(value) => match std::str::from_utf8(value) {
            Ok(s) => match s.parse::<i64>() {
                Ok(n) => n,
                Err(_) => return Frame::error("ERR hash value is not an integer"),
            },
            Err(_) => return Frame::error("ERR hash value is not an integer"),
        },
        None => 0,
    };

    let new_value = match current.checked_add(delta) {
        Some(n) => n,
        None => return Frame::error("ERR increment or decrement would overflow"),
    };

    hash.insert(field.clone(), Bytes::from(new_value.to_string()));
    store.set(db, key.clone(), Value::Hash(hash));
    Frame::Integer(new_value)
}

/// HINCRBYFLOAT command - increment field by float
pub fn hincrbyfloat(store: &Arc<Store>, db: u8, key: &Bytes, field: &Bytes, delta: f64) -> Frame {
    let mut hash = match store.get(db, key) {
        Some(Value::Hash(existing)) => existing,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => HashMap::new(),
    };

    let current = match hash.get(field) {
        Some(value) => match std::str::from_utf8(value) {
            Ok(s) => match s.parse::<f64>() {
                Ok(n) => n,
                Err(_) => return Frame::error("ERR hash value is not a float"),
            },
            Err(_) => return Frame::error("ERR hash value is not a float"),
        },
        None => 0.0,
    };

    let new_value = current + delta;

    // Check for infinity/NaN
    if new_value.is_infinite() || new_value.is_nan() {
        return Frame::error("ERR increment would produce NaN or Infinity");
    }

    // Format the float to strip unnecessary trailing zeros
    let formatted = format!("{}", new_value);
    hash.insert(field.clone(), Bytes::from(formatted.clone()));
    store.set(db, key.clone(), Value::Hash(hash));
    Frame::bulk(formatted)
}

/// HSTRLEN command - get length of field value
pub fn hstrlen(store: &Arc<Store>, db: u8, key: &Bytes, field: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(Value::Hash(hash)) => match hash.get(field) {
            Some(value) => Frame::Integer(value.len() as i64),
            None => Frame::Integer(0),
        },
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::Integer(0),
    }
}

/// HRANDFIELD command - get random fields from hash
/// count: number of fields to return (negative means allow duplicates)
/// withvalues: also return values
pub fn hrandfield(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    count: Option<i64>,
    withvalues: bool,
) -> Frame {
    use rand::Rng;

    match store.get(db, key) {
        Some(Value::Hash(hash)) => {
            if hash.is_empty() {
                return if count.is_some() {
                    Frame::Array(Some(vec![]))
                } else {
                    Frame::Null
                };
            }

            let fields: Vec<&Bytes> = hash.keys().collect();
            let mut rng = rand::thread_rng();

            match count {
                None => {
                    // Return single random field
                    let idx = rng.gen_range(0..fields.len());
                    Frame::Bulk(Some(fields[idx].clone()))
                }
                Some(0) => Frame::Array(Some(vec![])),
                Some(cnt) if cnt > 0 => {
                    // Return cnt unique fields
                    use rand::seq::SliceRandom;
                    let cnt = (cnt as usize).min(fields.len());
                    let mut indices: Vec<usize> = (0..fields.len()).collect();
                    indices.shuffle(&mut rng);

                    let selected: Vec<&Bytes> =
                        indices.iter().take(cnt).map(|&i| fields[i]).collect();

                    if withvalues {
                        let mut result = Vec::with_capacity(cnt * 2);
                        for field in selected {
                            result.push(Frame::Bulk(Some(field.clone())));
                            result.push(Frame::Bulk(hash.get(field).cloned()));
                        }
                        Frame::Array(Some(result))
                    } else {
                        let result: Vec<Frame> = selected
                            .iter()
                            .map(|f| Frame::Bulk(Some((*f).clone())))
                            .collect();
                        Frame::Array(Some(result))
                    }
                }
                Some(cnt) => {
                    // cnt < 0: return |cnt| fields, possibly with duplicates
                    let cnt = (-cnt) as usize;

                    if withvalues {
                        let mut result = Vec::with_capacity(cnt * 2);
                        for _ in 0..cnt {
                            let idx = rng.gen_range(0..fields.len());
                            let field = fields[idx];
                            result.push(Frame::Bulk(Some(field.clone())));
                            result.push(Frame::Bulk(hash.get(field).cloned()));
                        }
                        Frame::Array(Some(result))
                    } else {
                        let result: Vec<Frame> = (0..cnt)
                            .map(|_| {
                                let idx = rng.gen_range(0..fields.len());
                                Frame::Bulk(Some(fields[idx].clone()))
                            })
                            .collect();
                        Frame::Array(Some(result))
                    }
                }
            }
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => {
            if count.is_some() {
                Frame::Array(Some(vec![]))
            } else {
                Frame::Null
            }
        }
    }
}

/// Simple glob pattern matching for SCAN commands
fn matches_pattern(pattern: &str, text: &str) -> bool {
    let pattern_chars: Vec<char> = pattern.chars().collect();
    let text_chars: Vec<char> = text.chars().collect();

    fn match_helper(pattern: &[char], text: &[char]) -> bool {
        match (pattern.first(), text.first()) {
            (None, None) => true,
            (Some('*'), _) => {
                // Try matching * with empty string, or consume one char from text
                match_helper(&pattern[1..], text)
                    || (!text.is_empty() && match_helper(pattern, &text[1..]))
            }
            (Some('?'), Some(_)) => match_helper(&pattern[1..], &text[1..]),
            (Some(p), Some(t)) if *p == *t => match_helper(&pattern[1..], &text[1..]),
            (Some('['), _) => {
                // Handle character class [abc] or [a-z]
                if let Some(end) = pattern.iter().position(|&c| c == ']') {
                    let class = &pattern[1..end];
                    if let Some(&t) = text.first() {
                        let matches_class = class.contains(&t)
                            || (class.len() >= 3
                                && class
                                    .windows(3)
                                    .any(|w| w[1] == '-' && t >= w[0] && t <= w[2]));
                        if matches_class {
                            return match_helper(&pattern[end + 1..], &text[1..]);
                        }
                    }
                }
                false
            }
            (Some('\\'), _) if pattern.len() > 1 => {
                // Escaped character
                if text.first() == Some(&pattern[1]) {
                    match_helper(&pattern[2..], &text[1..])
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    match_helper(&pattern_chars, &text_chars)
}

/// HSCAN command - incrementally iterate over hash fields
/// cursor: iteration cursor (0 to start)
/// pattern: optional glob pattern to match fields
/// count: hint for number of elements to return
pub fn hscan(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    cursor: u64,
    pattern: Option<&str>,
    count: Option<usize>,
    novalues: bool,
) -> Frame {
    let count = count.unwrap_or(10);

    match store.get(db, key) {
        Some(Value::Hash(hash)) => {
            if hash.is_empty() {
                return Frame::Array(Some(vec![
                    Frame::Bulk(Some(Bytes::from("0"))),
                    Frame::Array(Some(vec![])),
                ]));
            }

            // Collect all fields (sorted for deterministic iteration)
            let mut fields: Vec<(&Bytes, &Bytes)> = hash.iter().collect();
            fields.sort_by(|a, b| a.0.cmp(b.0));
            let total = fields.len();

            // Use cursor as starting index
            let start = cursor as usize;
            if start >= total {
                // Cursor past the end, return 0 cursor with empty results
                return Frame::Array(Some(vec![
                    Frame::Bulk(Some(Bytes::from("0"))),
                    Frame::Array(Some(vec![])),
                ]));
            }

            let mut result_items = Vec::new();
            let mut scanned = 0;
            let mut pos = start;

            while pos < total && scanned < count.max(10) {
                let (field, value) = fields[pos];

                let matches = match pattern {
                    Some(p) => matches_pattern(p, &String::from_utf8_lossy(field)),
                    None => true,
                };

                if matches {
                    result_items.push(Frame::Bulk(Some(field.clone())));
                    if !novalues {
                        result_items.push(Frame::Bulk(Some(value.clone())));
                    }
                }

                pos += 1;
                scanned += 1;
            }

            // Calculate next cursor
            let next_cursor = if pos >= total {
                0 // Iteration complete
            } else {
                pos as u64
            };

            Frame::Array(Some(vec![
                Frame::Bulk(Some(Bytes::from(next_cursor.to_string()))),
                Frame::Array(Some(result_items)),
            ]))
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::Array(Some(vec![
            Frame::Bulk(Some(Bytes::from("0"))),
            Frame::Array(Some(vec![])),
        ])),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_store() -> Arc<Store> {
        Arc::new(Store::new(16))
    }

    #[test]
    fn test_hset_hget() {
        let store = create_store();
        let key = Bytes::from("hash");
        let field = Bytes::from("field1");
        let value = Bytes::from("value1");

        // HSET new field
        let response = hset(&store, 0, &key, &[(field.clone(), value.clone())]);
        assert!(matches!(response, Frame::Integer(1)));

        // HGET
        let response = hget(&store, 0, &key, &field);
        match response {
            Frame::Bulk(Some(data)) => assert_eq!(data, value),
            _ => panic!("Expected bulk string"),
        }

        // HSET existing field (should return 0)
        let response = hset(
            &store,
            0,
            &key,
            &[(field.clone(), Bytes::from("new_value"))],
        );
        assert!(matches!(response, Frame::Integer(0)));
    }

    #[test]
    fn test_hget_nonexistent() {
        let store = create_store();
        let key = Bytes::from("hash");
        let field = Bytes::from("field1");

        // HGET on non-existent key
        let response = hget(&store, 0, &key, &field);
        assert!(matches!(response, Frame::Bulk(None)));

        // HSET then HGET non-existent field
        hset(
            &store,
            0,
            &key,
            &[(Bytes::from("other"), Bytes::from("value"))],
        );
        let response = hget(&store, 0, &key, &field);
        assert!(matches!(response, Frame::Bulk(None)));
    }

    #[test]
    fn test_hmset_hmget() {
        let store = create_store();
        let key = Bytes::from("hash");

        // HMSET
        let pairs = vec![
            (Bytes::from("f1"), Bytes::from("v1")),
            (Bytes::from("f2"), Bytes::from("v2")),
            (Bytes::from("f3"), Bytes::from("v3")),
        ];
        let response = hmset(&store, 0, &key, &pairs);
        assert!(matches!(response, Frame::Simple(s) if s == "OK"));

        // HMGET
        let fields = vec![
            Bytes::from("f1"),
            Bytes::from("f2"),
            Bytes::from("nonexistent"),
        ];
        let response = hmget(&store, 0, &key, &fields);
        match response {
            Frame::Array(Some(arr)) => {
                assert_eq!(arr.len(), 3);
                match &arr[0] {
                    Frame::Bulk(Some(data)) => assert_eq!(*data, Bytes::from("v1")),
                    _ => panic!("Expected bulk string"),
                }
                match &arr[1] {
                    Frame::Bulk(Some(data)) => assert_eq!(*data, Bytes::from("v2")),
                    _ => panic!("Expected bulk string"),
                }
                assert!(matches!(arr[2], Frame::Bulk(None)));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_hdel() {
        let store = create_store();
        let key = Bytes::from("hash");

        // Set up hash
        hset(
            &store,
            0,
            &key,
            &[
                (Bytes::from("f1"), Bytes::from("v1")),
                (Bytes::from("f2"), Bytes::from("v2")),
            ],
        );

        // Delete one field
        let response = hdel(&store, 0, &key, &[Bytes::from("f1")]);
        assert!(matches!(response, Frame::Integer(1)));

        // Verify deletion
        let response = hget(&store, 0, &key, &Bytes::from("f1"));
        assert!(matches!(response, Frame::Bulk(None)));

        // Delete non-existent field
        let response = hdel(&store, 0, &key, &[Bytes::from("nonexistent")]);
        assert!(matches!(response, Frame::Integer(0)));

        // Delete remaining field (hash should be removed)
        let response = hdel(&store, 0, &key, &[Bytes::from("f2")]);
        assert!(matches!(response, Frame::Integer(1)));

        let response = hlen(&store, 0, &key);
        assert!(matches!(response, Frame::Integer(0)));
    }

    #[test]
    fn test_hexists() {
        let store = create_store();
        let key = Bytes::from("hash");

        // Non-existent key
        let response = hexists(&store, 0, &key, &Bytes::from("field"));
        assert!(matches!(response, Frame::Integer(0)));

        // Set up hash
        hset(&store, 0, &key, &[(Bytes::from("f1"), Bytes::from("v1"))]);

        // Existing field
        let response = hexists(&store, 0, &key, &Bytes::from("f1"));
        assert!(matches!(response, Frame::Integer(1)));

        // Non-existing field
        let response = hexists(&store, 0, &key, &Bytes::from("f2"));
        assert!(matches!(response, Frame::Integer(0)));
    }

    #[test]
    fn test_hgetall() {
        let store = create_store();
        let key = Bytes::from("hash");

        // Empty hash
        let response = hgetall(&store, 0, &key);
        match response {
            Frame::Array(Some(arr)) => assert!(arr.is_empty()),
            _ => panic!("Expected empty array"),
        }

        // Set up hash
        hset(
            &store,
            0,
            &key,
            &[
                (Bytes::from("f1"), Bytes::from("v1")),
                (Bytes::from("f2"), Bytes::from("v2")),
            ],
        );

        let response = hgetall(&store, 0, &key);
        match response {
            Frame::Array(Some(arr)) => {
                assert_eq!(arr.len(), 4);
                // Order is not guaranteed, so just check we have 4 elements
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_hkeys_hvals() {
        let store = create_store();
        let key = Bytes::from("hash");

        // Set up hash
        hset(
            &store,
            0,
            &key,
            &[
                (Bytes::from("f1"), Bytes::from("v1")),
                (Bytes::from("f2"), Bytes::from("v2")),
            ],
        );

        // HKEYS
        let response = hkeys(&store, 0, &key);
        match response {
            Frame::Array(Some(arr)) => assert_eq!(arr.len(), 2),
            _ => panic!("Expected array"),
        }

        // HVALS
        let response = hvals(&store, 0, &key);
        match response {
            Frame::Array(Some(arr)) => assert_eq!(arr.len(), 2),
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_hlen() {
        let store = create_store();
        let key = Bytes::from("hash");

        // Non-existent
        let response = hlen(&store, 0, &key);
        assert!(matches!(response, Frame::Integer(0)));

        // After adding fields
        hset(
            &store,
            0,
            &key,
            &[
                (Bytes::from("f1"), Bytes::from("v1")),
                (Bytes::from("f2"), Bytes::from("v2")),
                (Bytes::from("f3"), Bytes::from("v3")),
            ],
        );

        let response = hlen(&store, 0, &key);
        assert!(matches!(response, Frame::Integer(3)));
    }

    #[test]
    fn test_hsetnx() {
        let store = create_store();
        let key = Bytes::from("hash");
        let field = Bytes::from("field");

        // HSETNX on new field
        let response = hsetnx(&store, 0, &key, &field, &Bytes::from("value1"));
        assert!(matches!(response, Frame::Integer(1)));

        // HSETNX on existing field
        let response = hsetnx(&store, 0, &key, &field, &Bytes::from("value2"));
        assert!(matches!(response, Frame::Integer(0)));

        // Value should still be value1
        let response = hget(&store, 0, &key, &field);
        match response {
            Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("value1")),
            _ => panic!("Expected bulk string"),
        }
    }

    #[test]
    fn test_hincrby() {
        let store = create_store();
        let key = Bytes::from("hash");
        let field = Bytes::from("counter");

        // HINCRBY on non-existent field (starts at 0)
        let response = hincrby(&store, 0, &key, &field, 5);
        assert!(matches!(response, Frame::Integer(5)));

        // HINCRBY on existing field
        let response = hincrby(&store, 0, &key, &field, 3);
        assert!(matches!(response, Frame::Integer(8)));

        // HINCRBY with negative
        let response = hincrby(&store, 0, &key, &field, -2);
        assert!(matches!(response, Frame::Integer(6)));
    }

    #[test]
    fn test_hincrby_not_integer() {
        let store = create_store();
        let key = Bytes::from("hash");
        let field = Bytes::from("field");

        // Set non-integer value
        hset(
            &store,
            0,
            &key,
            &[(field.clone(), Bytes::from("not_an_int"))],
        );

        // HINCRBY should fail
        let response = hincrby(&store, 0, &key, &field, 1);
        assert!(matches!(response, Frame::Error(_)));
    }

    #[test]
    fn test_hincrbyfloat() {
        let store = create_store();
        let key = Bytes::from("hash");
        let field = Bytes::from("counter");

        // HINCRBYFLOAT on non-existent field
        let response = hincrbyfloat(&store, 0, &key, &field, 1.5);
        match response {
            Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("1.5")),
            _ => panic!("Expected bulk string"),
        }

        // HINCRBYFLOAT on existing field
        let response = hincrbyfloat(&store, 0, &key, &field, 0.5);
        match response {
            Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("2")),
            _ => panic!("Expected bulk string"),
        }
    }

    #[test]
    fn test_hstrlen() {
        let store = create_store();
        let key = Bytes::from("hash");

        // Non-existent
        let response = hstrlen(&store, 0, &key, &Bytes::from("field"));
        assert!(matches!(response, Frame::Integer(0)));

        // Set value
        hset(
            &store,
            0,
            &key,
            &[(Bytes::from("field"), Bytes::from("hello"))],
        );

        let response = hstrlen(&store, 0, &key, &Bytes::from("field"));
        assert!(matches!(response, Frame::Integer(5)));
    }

    #[test]
    fn test_wrongtype_error() {
        let store = create_store();
        let key = Bytes::from("string_key");

        // Set a string value
        store.set(0, key.clone(), Value::String(Bytes::from("value")));

        // All hash commands should return WRONGTYPE error
        let response = hget(&store, 0, &key, &Bytes::from("field"));
        assert!(matches!(response, Frame::Error(_)));

        let response = hset(&store, 0, &key, &[(Bytes::from("f"), Bytes::from("v"))]);
        assert!(matches!(response, Frame::Error(_)));

        let response = hdel(&store, 0, &key, &[Bytes::from("field")]);
        assert!(matches!(response, Frame::Error(_)));
    }
}
