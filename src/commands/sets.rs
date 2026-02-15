//! Set command implementations
//!
//! This module implements set-specific commands like SADD, SREM, SMEMBERS, etc.

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::{Store, Value};

/// SADD command - add member(s) to set
pub fn sadd(store: &Arc<Store>, db: u8, key: &Bytes, members: &[Bytes]) -> Frame {
    let mut set = match store.get(db, key) {
        Some(Value::Set(existing)) => existing,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => HashSet::new(),
    };

    let mut added = 0i64;
    for member in members {
        if set.insert(member.clone()) {
            added += 1;
        }
    }

    store.set(db, key.clone(), Value::Set(set));
    Frame::Integer(added)
}

/// SREM command - remove member(s) from set
pub fn srem(store: &Arc<Store>, db: u8, key: &Bytes, members: &[Bytes]) -> Frame {
    let mut set = match store.get(db, key) {
        Some(Value::Set(existing)) => existing,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => return Frame::Integer(0),
    };

    let mut removed = 0i64;
    for member in members {
        if set.remove(member) {
            removed += 1;
        }
    }

    if set.is_empty() {
        store.del(db, &[key.clone()]);
    } else {
        store.set(db, key.clone(), Value::Set(set));
    }

    Frame::Integer(removed)
}

/// SMEMBERS command - get all members of set
pub fn smembers(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(Value::Set(set)) => {
            let result: Vec<Frame> = set.iter().map(|m| Frame::bulk(m.clone())).collect();
            Frame::array(result)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::array(vec![]),
    }
}

/// SISMEMBER command - check if member is in set
pub fn sismember(store: &Arc<Store>, db: u8, key: &Bytes, member: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(Value::Set(set)) => {
            if set.contains(member) {
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

/// SMISMEMBER command - check if multiple members are in set
pub fn smismember(store: &Arc<Store>, db: u8, key: &Bytes, members: &[Bytes]) -> Frame {
    match store.get(db, key) {
        Some(Value::Set(set)) => {
            let results: Vec<Frame> = members
                .iter()
                .map(|m| {
                    if set.contains(m) {
                        Frame::Integer(1)
                    } else {
                        Frame::Integer(0)
                    }
                })
                .collect();
            Frame::array(results)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => {
            let results: Vec<Frame> = members.iter().map(|_| Frame::Integer(0)).collect();
            Frame::array(results)
        }
    }
}

/// SCARD command - get the number of members in set
pub fn scard(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(Value::Set(set)) => Frame::Integer(set.len() as i64),
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::Integer(0),
    }
}

/// SUNION command - get the union of multiple sets
pub fn sunion(store: &Arc<Store>, db: u8, keys: &[Bytes]) -> Frame {
    let mut result_set: HashSet<Bytes> = HashSet::new();

    for key in keys {
        match store.get(db, key) {
            Some(Value::Set(set)) => {
                result_set.extend(set);
            }
            Some(_) => {
                return Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                )
            }
            None => {}
        }
    }

    let result: Vec<Frame> = result_set.iter().map(|m| Frame::bulk(m.clone())).collect();
    Frame::array(result)
}

/// SUNIONSTORE command - store the union of multiple sets
pub fn sunionstore(store: &Arc<Store>, db: u8, destination: &Bytes, keys: &[Bytes]) -> Frame {
    let mut result_set: HashSet<Bytes> = HashSet::new();

    for key in keys {
        match store.get(db, key) {
            Some(Value::Set(set)) => {
                result_set.extend(set);
            }
            Some(_) => {
                return Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                )
            }
            None => {}
        }
    }

    let len = result_set.len() as i64;
    if result_set.is_empty() {
        store.del(db, &[destination.clone()]);
    } else {
        store.set(db, destination.clone(), Value::Set(result_set));
    }

    Frame::Integer(len)
}

/// SINTER command - get the intersection of multiple sets
pub fn sinter(store: &Arc<Store>, db: u8, keys: &[Bytes]) -> Frame {
    let Some(first_key) = keys.first() else {
        return Frame::array(vec![]);
    };

    // Start with the first set
    let mut result_set = match store.get(db, first_key) {
        Some(Value::Set(set)) => set,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => return Frame::array(vec![]), // Empty intersection
    };

    // Intersect with remaining sets
    for key in &keys[1..] {
        match store.get(db, key) {
            Some(Value::Set(set)) => {
                result_set = result_set.intersection(&set).cloned().collect();
            }
            Some(_) => {
                return Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                )
            }
            None => return Frame::array(vec![]), // Empty intersection
        }
    }

    let result: Vec<Frame> = result_set.iter().map(|m| Frame::bulk(m.clone())).collect();
    Frame::array(result)
}

/// SINTERSTORE command - store the intersection of multiple sets
pub fn sinterstore(store: &Arc<Store>, db: u8, destination: &Bytes, keys: &[Bytes]) -> Frame {
    let Some(first_key) = keys.first() else {
        store.del(db, &[destination.clone()]);
        return Frame::Integer(0);
    };

    // Start with the first set
    let mut result_set = match store.get(db, first_key) {
        Some(Value::Set(set)) => set,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => {
            store.del(db, &[destination.clone()]);
            return Frame::Integer(0);
        }
    };

    // Intersect with remaining sets
    for key in &keys[1..] {
        match store.get(db, key) {
            Some(Value::Set(set)) => {
                result_set = result_set.intersection(&set).cloned().collect();
            }
            Some(_) => {
                return Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                )
            }
            None => {
                store.del(db, &[destination.clone()]);
                return Frame::Integer(0);
            }
        }
    }

    let len = result_set.len() as i64;
    if result_set.is_empty() {
        store.del(db, &[destination.clone()]);
    } else {
        store.set(db, destination.clone(), Value::Set(result_set));
    }

    Frame::Integer(len)
}

/// SDIFF command - get the difference of the first set from all other sets
pub fn sdiff(store: &Arc<Store>, db: u8, keys: &[Bytes]) -> Frame {
    let Some(first_key) = keys.first() else {
        return Frame::array(vec![]);
    };

    // Start with the first set
    let mut result_set = match store.get(db, first_key) {
        Some(Value::Set(set)) => set,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => return Frame::array(vec![]),
    };

    // Subtract remaining sets
    for key in &keys[1..] {
        match store.get(db, key) {
            Some(Value::Set(set)) => {
                result_set = result_set.difference(&set).cloned().collect();
            }
            Some(_) => {
                return Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                )
            }
            None => {}
        }
    }

    let result: Vec<Frame> = result_set.iter().map(|m| Frame::bulk(m.clone())).collect();
    Frame::array(result)
}

/// SDIFFSTORE command - store the difference of the first set from all other sets
pub fn sdiffstore(store: &Arc<Store>, db: u8, destination: &Bytes, keys: &[Bytes]) -> Frame {
    let Some(first_key) = keys.first() else {
        store.del(db, &[destination.clone()]);
        return Frame::Integer(0);
    };

    // Start with the first set
    let mut result_set = match store.get(db, first_key) {
        Some(Value::Set(set)) => set,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => {
            store.del(db, &[destination.clone()]);
            return Frame::Integer(0);
        }
    };

    // Subtract remaining sets
    for key in &keys[1..] {
        match store.get(db, key) {
            Some(Value::Set(set)) => {
                result_set = result_set.difference(&set).cloned().collect();
            }
            Some(_) => {
                return Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                )
            }
            None => {}
        }
    }

    let len = result_set.len() as i64;
    if result_set.is_empty() {
        store.del(db, &[destination.clone()]);
    } else {
        store.set(db, destination.clone(), Value::Set(result_set));
    }

    Frame::Integer(len)
}

/// SPOP command - remove and return random member(s) from set
pub fn spop(store: &Arc<Store>, db: u8, key: &Bytes, count: Option<usize>) -> Frame {
    let mut set = match store.get(db, key) {
        Some(Value::Set(existing)) => existing,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => {
            return if count.is_some() {
                Frame::array(vec![])
            } else {
                Frame::null()
            }
        }
    };

    let count = count.unwrap_or(1);
    let mut popped = Vec::with_capacity(count.min(set.len()));

    // Pop elements (we take first `count` elements since HashSet doesn't have random access)
    for _ in 0..count {
        if let Some(member) = set.iter().next().cloned() {
            set.remove(&member);
            popped.push(member);
        } else {
            break;
        }
    }

    if set.is_empty() {
        store.del(db, &[key.clone()]);
    } else {
        store.set(db, key.clone(), Value::Set(set));
    }

    if count == 1 {
        // Single pop returns a bulk string or null
        match popped.into_iter().next() {
            Some(item) => Frame::bulk(item),
            None => Frame::null(),
        }
    } else {
        // Multiple pop returns an array
        Frame::array(popped.into_iter().map(Frame::bulk).collect())
    }
}

/// SRANDMEMBER command - get random member(s) from set without removing
pub fn srandmember(store: &Arc<Store>, db: u8, key: &Bytes, count: Option<i64>) -> Frame {
    match store.get(db, key) {
        Some(Value::Set(set)) => {
            if set.is_empty() {
                return if count.is_some() {
                    Frame::array(vec![])
                } else {
                    Frame::null()
                };
            }

            match count {
                None => {
                    // Return single random element
                    if let Some(member) = set.iter().next() {
                        Frame::bulk(member.clone())
                    } else {
                        Frame::null()
                    }
                }
                Some(0) => Frame::array(vec![]),
                Some(n) if n > 0 => {
                    // Return up to n unique elements
                    let result: Vec<Frame> = set
                        .iter()
                        .take(n as usize)
                        .map(|m| Frame::bulk(m.clone()))
                        .collect();
                    Frame::array(result)
                }
                Some(n) => {
                    // n < 0: Return |n| elements, may contain duplicates
                    let count = (-n) as usize;
                    let set_vec: Vec<_> = set.iter().collect();
                    if set_vec.is_empty() {
                        return Frame::array(vec![]);
                    }
                    let result: Vec<Frame> = (0..count)
                        .map(|i| Frame::bulk(set_vec[i % set_vec.len()].clone()))
                        .collect();
                    Frame::array(result)
                }
            }
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => {
            if count.is_some() {
                Frame::array(vec![])
            } else {
                Frame::null()
            }
        }
    }
}

/// SINTERCARD command - return the cardinality of the intersection of sets
/// SINTERCARD numkeys key [key ...] [LIMIT limit]
pub fn sintercard(store: &Arc<Store>, db: u8, keys: &[Bytes], limit: Option<usize>) -> Frame {
    let Some(first_key) = keys.first() else {
        return Frame::Integer(0);
    };

    // Start with the first set
    let mut result_set = match store.get(db, first_key) {
        Some(Value::Set(set)) => set,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => return Frame::Integer(0), // Empty intersection
    };

    // Intersect with remaining sets
    for key in &keys[1..] {
        match store.get(db, key) {
            Some(Value::Set(set)) => {
                result_set = result_set.intersection(&set).cloned().collect();
                // Early exit if empty
                if result_set.is_empty() {
                    return Frame::Integer(0);
                }
                // Early exit if we have a limit and result exceeds it
                if let Some(lim) = limit {
                    if lim > 0 && result_set.len() >= lim {
                        return Frame::Integer(lim as i64);
                    }
                }
            }
            Some(_) => {
                return Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                )
            }
            None => return Frame::Integer(0), // Empty intersection
        }
    }

    let cardinality = result_set.len() as i64;
    if let Some(lim) = limit {
        if lim > 0 && cardinality > lim as i64 {
            return Frame::Integer(lim as i64);
        }
    }
    Frame::Integer(cardinality)
}

/// SSCAN key cursor [MATCH pattern] [COUNT count]
/// Incrementally iterate over set members
pub fn sscan(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    cursor: u64,
    pattern: Option<&str>,
    count: Option<usize>,
) -> Frame {
    let count = count.unwrap_or(10);

    match store.get(db, key) {
        Some(Value::Set(set)) => {
            // Collect all members that match pattern
            let mut members: Vec<(u64, Bytes)> = Vec::new();

            for member in set.iter() {
                // Apply pattern filter if specified
                if let Some(pat) = pattern {
                    if let Ok(member_str) = std::str::from_utf8(member) {
                        if !matches_pattern(pat, member_str) {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }

                let member_hash = hash_bytes(member);
                members.push((member_hash, member.clone()));
            }

            // Sort by hash for consistent ordering
            members.sort_by_key(|(h, _)| *h);

            // Find starting position based on cursor
            let start_pos = if cursor == 0 {
                0
            } else {
                members
                    .iter()
                    .position(|(h, _)| *h > cursor)
                    .unwrap_or(members.len())
            };

            // Collect results
            let mut results: Vec<Frame> = Vec::new();
            let mut next_cursor: u64 = 0;

            for (i, (member_hash, member)) in members.iter().enumerate().skip(start_pos) {
                if results.len() >= count {
                    next_cursor = *member_hash;
                    break;
                }
                results.push(Frame::Bulk(Some(member.clone())));

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

/// Simple glob pattern matching for SCAN commands
fn matches_pattern(pattern: &str, text: &str) -> bool {
    matches_pattern_impl(pattern.as_bytes(), text.as_bytes())
}

fn matches_pattern_impl(pattern: &[u8], text: &[u8]) -> bool {
    let mut p = 0;
    let mut t = 0;
    let mut star_p = usize::MAX;
    let mut star_t = usize::MAX;

    while t < text.len() {
        if p < pattern.len() {
            match pattern[p] {
                b'*' => {
                    star_p = p;
                    star_t = t;
                    p += 1;
                    continue;
                }
                b'?' => {
                    p += 1;
                    t += 1;
                    continue;
                }
                b'\\' if p + 1 < pattern.len() => {
                    if pattern[p + 1] == text[t] {
                        p += 2;
                        t += 1;
                        continue;
                    }
                }
                b'[' => {
                    if let Some((matched, new_p)) = match_bracket(&pattern[p..], text[t]) {
                        if matched {
                            p += new_p;
                            t += 1;
                            continue;
                        }
                    }
                }
                c if c == text[t] => {
                    p += 1;
                    t += 1;
                    continue;
                }
                _ => {}
            }
        }

        // Backtrack to star if possible
        if star_p != usize::MAX {
            p = star_p + 1;
            star_t += 1;
            t = star_t;
        } else {
            return false;
        }
    }

    // Skip trailing stars
    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }

    p == pattern.len()
}

fn match_bracket(pattern: &[u8], c: u8) -> Option<(bool, usize)> {
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
    while i < pattern.len() && pattern[i] != b']' {
        if i + 2 < pattern.len() && pattern[i + 1] == b'-' && pattern[i + 2] != b']' {
            // Range like [a-z]
            if c >= pattern[i] && c <= pattern[i + 2] {
                matched = true;
            }
            i += 3;
        } else {
            if pattern[i] == c {
                matched = true;
            }
            i += 1;
        }
    }

    if i < pattern.len() && pattern[i] == b']' {
        Some((matched != negate, i + 1))
    } else {
        None
    }
}

/// Hash bytes for consistent cursor ordering
fn hash_bytes(data: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    data.hash(&mut hasher);
    hasher.finish()
}

/// SMOVE command - move member from one set to another
pub fn smove(
    store: &Arc<Store>,
    db: u8,
    source: &Bytes,
    destination: &Bytes,
    member: &Bytes,
) -> Frame {
    // Get source set
    let mut src_set = match store.get(db, source) {
        Some(Value::Set(existing)) => existing,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => return Frame::Integer(0),
    };

    // Check if member exists in source
    if !src_set.remove(member) {
        return Frame::Integer(0);
    }

    // Get or create destination set
    let mut dst_set = match store.get(db, destination) {
        Some(Value::Set(existing)) => existing,
        Some(_) => {
            // Put the member back since destination is wrong type
            src_set.insert(member.clone());
            store.set(db, source.clone(), Value::Set(src_set));
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            );
        }
        None => HashSet::new(),
    };

    // Add to destination
    dst_set.insert(member.clone());

    // Update both sets
    if src_set.is_empty() {
        store.del(db, &[source.clone()]);
    } else {
        store.set(db, source.clone(), Value::Set(src_set));
    }
    store.set(db, destination.clone(), Value::Set(dst_set));

    Frame::Integer(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_store() -> Arc<Store> {
        Arc::new(Store::new(16))
    }

    #[test]
    fn test_sadd_scard() {
        let store = create_store();
        let key = Bytes::from("set");

        // Add single member
        let response = sadd(&store, 0, &key, &[Bytes::from("a")]);
        assert!(matches!(response, Frame::Integer(1)));

        // Add multiple members
        let response = sadd(
            &store,
            0,
            &key,
            &[Bytes::from("b"), Bytes::from("c"), Bytes::from("a")],
        );
        assert!(matches!(response, Frame::Integer(2))); // "a" already exists

        // Check cardinality
        let response = scard(&store, 0, &key);
        assert!(matches!(response, Frame::Integer(3)));
    }

    #[test]
    fn test_srem() {
        let store = create_store();
        let key = Bytes::from("set");

        sadd(
            &store,
            0,
            &key,
            &[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
        );

        // Remove existing member
        let response = srem(&store, 0, &key, &[Bytes::from("a")]);
        assert!(matches!(response, Frame::Integer(1)));

        // Remove non-existing member
        let response = srem(&store, 0, &key, &[Bytes::from("nonexistent")]);
        assert!(matches!(response, Frame::Integer(0)));

        // Check cardinality
        let response = scard(&store, 0, &key);
        assert!(matches!(response, Frame::Integer(2)));
    }

    #[test]
    fn test_smembers() {
        let store = create_store();
        let key = Bytes::from("set");

        // Empty set
        let response = smembers(&store, 0, &key);
        match response {
            Frame::Array(Some(arr)) => assert!(arr.is_empty()),
            _ => panic!("Expected empty array"),
        }

        sadd(&store, 0, &key, &[Bytes::from("a"), Bytes::from("b")]);

        let response = smembers(&store, 0, &key);
        match response {
            Frame::Array(Some(arr)) => assert_eq!(arr.len(), 2),
            _ => panic!("Expected array with 2 members"),
        }
    }

    #[test]
    fn test_sismember() {
        let store = create_store();
        let key = Bytes::from("set");

        sadd(&store, 0, &key, &[Bytes::from("a"), Bytes::from("b")]);

        // Check existing member
        let response = sismember(&store, 0, &key, &Bytes::from("a"));
        assert!(matches!(response, Frame::Integer(1)));

        // Check non-existing member
        let response = sismember(&store, 0, &key, &Bytes::from("c"));
        assert!(matches!(response, Frame::Integer(0)));

        // Check on non-existing key
        let response = sismember(&store, 0, &Bytes::from("nonexistent"), &Bytes::from("a"));
        assert!(matches!(response, Frame::Integer(0)));
    }

    #[test]
    fn test_smismember() {
        let store = create_store();
        let key = Bytes::from("set");

        sadd(&store, 0, &key, &[Bytes::from("a"), Bytes::from("b")]);

        let response = smismember(
            &store,
            0,
            &key,
            &[Bytes::from("a"), Bytes::from("c"), Bytes::from("b")],
        );
        match response {
            Frame::Array(Some(arr)) => {
                assert_eq!(arr.len(), 3);
                assert!(matches!(arr[0], Frame::Integer(1))); // a exists
                assert!(matches!(arr[1], Frame::Integer(0))); // c doesn't exist
                assert!(matches!(arr[2], Frame::Integer(1))); // b exists
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_sunion() {
        let store = create_store();
        let key1 = Bytes::from("set1");
        let key2 = Bytes::from("set2");

        sadd(&store, 0, &key1, &[Bytes::from("a"), Bytes::from("b")]);
        sadd(&store, 0, &key2, &[Bytes::from("b"), Bytes::from("c")]);

        let response = sunion(&store, 0, &[key1, key2]);
        match response {
            Frame::Array(Some(arr)) => assert_eq!(arr.len(), 3), // {a, b, c}
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_sinter() {
        let store = create_store();
        let key1 = Bytes::from("set1");
        let key2 = Bytes::from("set2");

        sadd(&store, 0, &key1, &[Bytes::from("a"), Bytes::from("b")]);
        sadd(&store, 0, &key2, &[Bytes::from("b"), Bytes::from("c")]);

        let response = sinter(&store, 0, &[key1, key2]);
        match response {
            Frame::Array(Some(arr)) => {
                assert_eq!(arr.len(), 1); // {b}
                match &arr[0] {
                    Frame::Bulk(Some(data)) => assert_eq!(*data, Bytes::from("b")),
                    _ => panic!("Expected bulk string"),
                }
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_sdiff() {
        let store = create_store();
        let key1 = Bytes::from("set1");
        let key2 = Bytes::from("set2");

        sadd(
            &store,
            0,
            &key1,
            &[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
        );
        sadd(&store, 0, &key2, &[Bytes::from("b"), Bytes::from("c")]);

        let response = sdiff(&store, 0, &[key1, key2]);
        match response {
            Frame::Array(Some(arr)) => {
                assert_eq!(arr.len(), 1); // {a}
                match &arr[0] {
                    Frame::Bulk(Some(data)) => assert_eq!(*data, Bytes::from("a")),
                    _ => panic!("Expected bulk string"),
                }
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_sunionstore() {
        let store = create_store();
        let key1 = Bytes::from("set1");
        let key2 = Bytes::from("set2");
        let dest = Bytes::from("dest");

        sadd(&store, 0, &key1, &[Bytes::from("a"), Bytes::from("b")]);
        sadd(&store, 0, &key2, &[Bytes::from("b"), Bytes::from("c")]);

        let response = sunionstore(&store, 0, &dest, &[key1, key2]);
        assert!(matches!(response, Frame::Integer(3)));

        // Verify destination
        let response = scard(&store, 0, &dest);
        assert!(matches!(response, Frame::Integer(3)));
    }

    #[test]
    fn test_sinterstore() {
        let store = create_store();
        let key1 = Bytes::from("set1");
        let key2 = Bytes::from("set2");
        let dest = Bytes::from("dest");

        sadd(&store, 0, &key1, &[Bytes::from("a"), Bytes::from("b")]);
        sadd(&store, 0, &key2, &[Bytes::from("b"), Bytes::from("c")]);

        let response = sinterstore(&store, 0, &dest, &[key1, key2]);
        assert!(matches!(response, Frame::Integer(1)));

        // Verify destination
        let response = scard(&store, 0, &dest);
        assert!(matches!(response, Frame::Integer(1)));
    }

    #[test]
    fn test_sdiffstore() {
        let store = create_store();
        let key1 = Bytes::from("set1");
        let key2 = Bytes::from("set2");
        let dest = Bytes::from("dest");

        sadd(
            &store,
            0,
            &key1,
            &[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
        );
        sadd(&store, 0, &key2, &[Bytes::from("b"), Bytes::from("c")]);

        let response = sdiffstore(&store, 0, &dest, &[key1, key2]);
        assert!(matches!(response, Frame::Integer(1)));

        // Verify destination
        let response = scard(&store, 0, &dest);
        assert!(matches!(response, Frame::Integer(1)));
    }

    #[test]
    fn test_spop() {
        let store = create_store();
        let key = Bytes::from("set");

        sadd(
            &store,
            0,
            &key,
            &[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
        );

        // Pop single element
        let response = spop(&store, 0, &key, None);
        match response {
            Frame::Bulk(Some(_)) => {}
            _ => panic!("Expected bulk string"),
        }

        // Check cardinality decreased
        let response = scard(&store, 0, &key);
        assert!(matches!(response, Frame::Integer(2)));

        // Pop from non-existing key
        let response = spop(&store, 0, &Bytes::from("nonexistent"), None);
        assert!(matches!(response, Frame::Bulk(None)));
    }

    #[test]
    fn test_srandmember() {
        let store = create_store();
        let key = Bytes::from("set");

        sadd(
            &store,
            0,
            &key,
            &[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
        );

        // Get single random element
        let response = srandmember(&store, 0, &key, None);
        match response {
            Frame::Bulk(Some(_)) => {}
            _ => panic!("Expected bulk string"),
        }

        // Check cardinality unchanged
        let response = scard(&store, 0, &key);
        assert!(matches!(response, Frame::Integer(3)));

        // Get multiple elements
        let response = srandmember(&store, 0, &key, Some(2));
        match response {
            Frame::Array(Some(arr)) => assert!(arr.len() <= 2),
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_smove() {
        let store = create_store();
        let src = Bytes::from("src");
        let dst = Bytes::from("dst");

        sadd(&store, 0, &src, &[Bytes::from("a"), Bytes::from("b")]);
        sadd(&store, 0, &dst, &[Bytes::from("c")]);

        // Move existing member
        let response = smove(&store, 0, &src, &dst, &Bytes::from("a"));
        assert!(matches!(response, Frame::Integer(1)));

        // Verify source
        let response = sismember(&store, 0, &src, &Bytes::from("a"));
        assert!(matches!(response, Frame::Integer(0)));

        // Verify destination
        let response = sismember(&store, 0, &dst, &Bytes::from("a"));
        assert!(matches!(response, Frame::Integer(1)));

        // Move non-existing member
        let response = smove(&store, 0, &src, &dst, &Bytes::from("nonexistent"));
        assert!(matches!(response, Frame::Integer(0)));
    }

    #[test]
    fn test_wrongtype_error() {
        let store = create_store();
        let key = Bytes::from("string_key");

        // Set a string value
        store.set(0, key.clone(), Value::String(Bytes::from("value")));

        // All set commands should return WRONGTYPE error
        let response = sadd(&store, 0, &key, &[Bytes::from("a")]);
        assert!(matches!(response, Frame::Error(_)));

        let response = srem(&store, 0, &key, &[Bytes::from("a")]);
        assert!(matches!(response, Frame::Error(_)));

        let response = smembers(&store, 0, &key);
        assert!(matches!(response, Frame::Error(_)));

        let response = sismember(&store, 0, &key, &Bytes::from("a"));
        assert!(matches!(response, Frame::Error(_)));
    }
}
