//! List command implementations
//!
//! This module implements Redis list commands: LPUSH, RPUSH, LPOP, RPOP,
//! LRANGE, LLEN, LINDEX, LSET, LREM, LINSERT.

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;

use super::parser::ListDirection;
use crate::protocol::Frame;
use crate::storage::{Store, Value};

/// LPUSH command - prepend values to list
pub fn lpush(store: &Arc<Store>, db: u8, key: &Bytes, values: &[Bytes]) -> Frame {
    let mut list = match store.get(db, key) {
        Some(Value::List(existing)) => existing,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => VecDeque::new(),
    };

    // Prepend values in order (first value ends up at head)
    for value in values.iter().rev() {
        list.push_front(value.clone());
    }

    let len = list.len() as i64;
    store.set(db, key.clone(), Value::List(list));

    Frame::Integer(len)
}

/// RPUSH command - append values to list
pub fn rpush(store: &Arc<Store>, db: u8, key: &Bytes, values: &[Bytes]) -> Frame {
    let mut list = match store.get(db, key) {
        Some(Value::List(existing)) => existing,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => VecDeque::new(),
    };

    // Append values
    for value in values {
        list.push_back(value.clone());
    }

    let len = list.len() as i64;
    store.set(db, key.clone(), Value::List(list));

    Frame::Integer(len)
}

/// LPOP command - remove and return first element(s)
pub fn lpop(store: &Arc<Store>, db: u8, key: &Bytes, count: Option<usize>) -> Frame {
    let mut list = match store.get(db, key) {
        Some(Value::List(existing)) => existing,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => return Frame::null(),
    };

    match count {
        None => {
            // Single element pop
            let result = list.pop_front();
            if list.is_empty() {
                store.del(db, &[key.clone()]);
            } else {
                store.set(db, key.clone(), Value::List(list));
            }
            match result {
                Some(v) => Frame::bulk(v),
                None => Frame::null(),
            }
        }
        Some(count) => {
            // Multi-element pop returns array
            let mut results = Vec::with_capacity(count.min(list.len()));
            for _ in 0..count {
                if let Some(v) = list.pop_front() {
                    results.push(Frame::bulk(v));
                } else {
                    break;
                }
            }
            if results.is_empty() {
                store.del(db, &[key.clone()]);
                return Frame::null();
            }
            if list.is_empty() {
                store.del(db, &[key.clone()]);
            } else {
                store.set(db, key.clone(), Value::List(list));
            }
            Frame::array(results)
        }
    }
}

/// RPOP command - remove and return last element(s)
pub fn rpop(store: &Arc<Store>, db: u8, key: &Bytes, count: Option<usize>) -> Frame {
    let mut list = match store.get(db, key) {
        Some(Value::List(existing)) => existing,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => return Frame::null(),
    };

    match count {
        None => {
            // Single element pop
            let result = list.pop_back();
            if list.is_empty() {
                store.del(db, &[key.clone()]);
            } else {
                store.set(db, key.clone(), Value::List(list));
            }
            match result {
                Some(v) => Frame::bulk(v),
                None => Frame::null(),
            }
        }
        Some(count) => {
            // Multi-element pop returns array
            let mut results = Vec::with_capacity(count.min(list.len()));
            for _ in 0..count {
                if let Some(v) = list.pop_back() {
                    results.push(Frame::bulk(v));
                } else {
                    break;
                }
            }
            if results.is_empty() {
                store.del(db, &[key.clone()]);
                return Frame::null();
            }
            if list.is_empty() {
                store.del(db, &[key.clone()]);
            } else {
                store.set(db, key.clone(), Value::List(list));
            }
            Frame::array(results)
        }
    }
}

/// LRANGE command - get range of elements
pub fn lrange(store: &Arc<Store>, db: u8, key: &Bytes, start: i64, stop: i64) -> Frame {
    match store.get(db, key) {
        Some(Value::List(list)) => {
            let len = list.len() as i64;
            if len == 0 {
                return Frame::array(vec![]);
            }

            // Convert negative indices
            let start = if start < 0 {
                (len + start).max(0) as usize
            } else {
                start.min(len) as usize
            };

            let stop = if stop < 0 {
                (len + stop).max(0) as usize
            } else {
                stop.min(len - 1) as usize
            };

            if start > stop || start >= list.len() {
                return Frame::array(vec![]);
            }

            let results: Vec<Frame> = list
                .iter()
                .skip(start)
                .take(stop - start + 1)
                .map(|v| Frame::bulk(v.clone()))
                .collect();

            Frame::array(results)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::array(vec![]),
    }
}

/// LLEN command - get list length
pub fn llen(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(Value::List(list)) => Frame::Integer(list.len() as i64),
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::Integer(0),
    }
}

/// LINDEX command - get element by index
pub fn lindex(store: &Arc<Store>, db: u8, key: &Bytes, index: i64) -> Frame {
    match store.get(db, key) {
        Some(Value::List(list)) => {
            let len = list.len() as i64;
            let idx = if index < 0 {
                (len + index) as usize
            } else {
                index as usize
            };

            if idx >= list.len() {
                Frame::null()
            } else {
                Frame::bulk(list[idx].clone())
            }
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::null(),
    }
}

/// LSET command - set element at index
pub fn lset(store: &Arc<Store>, db: u8, key: &Bytes, index: i64, value: &Bytes) -> Frame {
    let mut list = match store.get(db, key) {
        Some(Value::List(existing)) => existing,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => return Frame::error("ERR no such key"),
    };

    let len = list.len() as i64;
    let idx = if index < 0 {
        (len + index) as usize
    } else {
        index as usize
    };

    if idx >= list.len() {
        return Frame::error("ERR index out of range");
    }

    list[idx] = value.clone();
    store.set(db, key.clone(), Value::List(list));

    Frame::simple("OK")
}

/// LREM command - remove elements by value
pub fn lrem(store: &Arc<Store>, db: u8, key: &Bytes, count: i64, value: &Bytes) -> Frame {
    let mut list = match store.get(db, key) {
        Some(Value::List(existing)) => existing,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => return Frame::Integer(0),
    };

    let mut removed = 0;

    match count.cmp(&0) {
        std::cmp::Ordering::Greater => {
            // Remove first N occurrences from head
            let mut to_remove = count as usize;
            list.retain(|v| {
                if to_remove > 0 && v == value {
                    to_remove -= 1;
                    removed += 1;
                    false
                } else {
                    true
                }
            });
        }
        std::cmp::Ordering::Less => {
            // Remove first N occurrences from tail (iterate backwards)
            let mut to_remove = (-count) as usize;
            let len = list.len();
            let mut indices_to_remove = Vec::new();

            for i in (0..len).rev() {
                if to_remove > 0 && list[i] == *value {
                    indices_to_remove.push(i);
                    to_remove -= 1;
                }
            }

            for idx in indices_to_remove {
                list.remove(idx);
                removed += 1;
            }
        }
        std::cmp::Ordering::Equal => {
            // Remove all occurrences
            list.retain(|v| {
                if v == value {
                    removed += 1;
                    false
                } else {
                    true
                }
            });
        }
    }

    if list.is_empty() {
        store.del(db, &[key.clone()]);
    } else {
        store.set(db, key.clone(), Value::List(list));
    }

    Frame::Integer(removed)
}

/// LINSERT command - insert element before/after pivot
pub fn linsert(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    before: bool,
    pivot: &Bytes,
    value: &Bytes,
) -> Frame {
    let mut list = match store.get(db, key) {
        Some(Value::List(existing)) => existing,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => return Frame::Integer(-1),
    };

    // Find pivot
    let pos = list.iter().position(|v| v == pivot);

    match pos {
        Some(idx) => {
            let insert_idx = if before { idx } else { idx + 1 };
            list.insert(insert_idx, value.clone());
            let len = list.len() as i64;
            store.set(db, key.clone(), Value::List(list));
            Frame::Integer(len)
        }
        None => Frame::Integer(-1),
    }
}

/// LPUSHX command - prepend value only if list exists
pub fn lpushx(store: &Arc<Store>, db: u8, key: &Bytes, values: &[Bytes]) -> Frame {
    let mut list = match store.get(db, key) {
        Some(Value::List(existing)) => existing,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => return Frame::Integer(0),
    };

    for value in values.iter().rev() {
        list.push_front(value.clone());
    }

    let len = list.len() as i64;
    store.set(db, key.clone(), Value::List(list));

    Frame::Integer(len)
}

/// RPUSHX command - append value only if list exists
pub fn rpushx(store: &Arc<Store>, db: u8, key: &Bytes, values: &[Bytes]) -> Frame {
    let mut list = match store.get(db, key) {
        Some(Value::List(existing)) => existing,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => return Frame::Integer(0),
    };

    for value in values {
        list.push_back(value.clone());
    }

    let len = list.len() as i64;
    store.set(db, key.clone(), Value::List(list));

    Frame::Integer(len)
}

/// LMOVE command - atomically move an element from source to destination
pub fn lmove(
    store: &Arc<Store>,
    db: u8,
    source: &Bytes,
    destination: &Bytes,
    wherefrom: ListDirection,
    whereto: ListDirection,
) -> Frame {
    // Get source list
    let mut source_list = match store.get(db, source) {
        Some(Value::List(existing)) => existing,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => return Frame::null(),
    };

    // Pop from source
    let element = match wherefrom {
        ListDirection::Left => source_list.pop_front(),
        ListDirection::Right => source_list.pop_back(),
    };

    let element = match element {
        Some(e) => e,
        None => return Frame::null(),
    };

    // Get destination list (could be same as source)
    let mut dest_list = if source == destination {
        source_list.clone()
    } else {
        match store.get(db, destination) {
            Some(Value::List(existing)) => existing,
            Some(_) => {
                return Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                )
            }
            None => VecDeque::new(),
        }
    };

    // Push to destination
    match whereto {
        ListDirection::Left => dest_list.push_front(element.clone()),
        ListDirection::Right => dest_list.push_back(element.clone()),
    }

    // Update storage
    if source == destination {
        // Source and destination are the same - just update once
        if dest_list.is_empty() {
            store.del(db, &[source.clone()]);
        } else {
            store.set(db, source.clone(), Value::List(dest_list));
        }
    } else {
        // Update source
        if source_list.is_empty() {
            store.del(db, &[source.clone()]);
        } else {
            store.set(db, source.clone(), Value::List(source_list));
        }
        // Update destination
        store.set(db, destination.clone(), Value::List(dest_list));
    }

    Frame::bulk(element)
}

/// LTRIM command - trim list to specified range
pub fn ltrim(store: &Arc<Store>, db: u8, key: &Bytes, start: i64, stop: i64) -> Frame {
    let list = match store.get(db, key) {
        Some(Value::List(existing)) => existing,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => return Frame::simple("OK"),
    };

    let len = list.len() as i64;

    // Convert negative indices
    let start = if start < 0 {
        (len + start).max(0) as usize
    } else {
        start.min(len) as usize
    };

    let stop = if stop < 0 {
        (len + stop).max(-1) as usize
    } else {
        stop.min(len - 1) as usize
    };

    if start > stop || start >= list.len() {
        // Empty result
        store.del(db, &[key.clone()]);
        return Frame::simple("OK");
    }

    // Create trimmed list
    let trimmed: VecDeque<Bytes> = list
        .into_iter()
        .skip(start)
        .take(stop - start + 1)
        .collect();

    if trimmed.is_empty() {
        store.del(db, &[key.clone()]);
    } else {
        store.set(db, key.clone(), Value::List(trimmed));
    }

    Frame::simple("OK")
}

/// LPOS command - find index of element in list
pub fn lpos(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    element: &Bytes,
    rank: Option<i64>,
    count: Option<usize>,
    maxlen: Option<usize>,
) -> Frame {
    match store.get(db, key) {
        Some(Value::List(list)) => {
            let len = list.len();
            let maxlen = maxlen.unwrap_or(len).min(len);
            let rank = rank.unwrap_or(1);

            // Determine iteration direction based on rank
            let reverse = rank < 0;
            let mut matches_to_skip = rank.unsigned_abs() as usize;
            if matches_to_skip == 0 {
                return Frame::error("ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list");
            }
            matches_to_skip -= 1; // Skip N-1 matches, return N-th match

            let count = count.unwrap_or(1);
            let return_multiple = count != 1 || count == 0;

            let mut results = Vec::new();
            let mut scanned = 0;

            if reverse {
                // Search from right to left
                for i in (0..len).rev() {
                    if scanned >= maxlen {
                        break;
                    }
                    scanned += 1;

                    if list[i] == *element {
                        if matches_to_skip > 0 {
                            matches_to_skip -= 1;
                            continue;
                        }
                        results.push(i as i64);
                        if count > 0 && results.len() >= count {
                            break;
                        }
                    }
                }
            } else {
                // Search from left to right
                for (i, item) in list.iter().enumerate() {
                    if scanned >= maxlen {
                        break;
                    }
                    scanned += 1;

                    if item == element {
                        if matches_to_skip > 0 {
                            matches_to_skip -= 1;
                            continue;
                        }
                        results.push(i as i64);
                        if count > 0 && results.len() >= count {
                            break;
                        }
                    }
                }
            }

            if return_multiple {
                Frame::array(results.into_iter().map(Frame::Integer).collect())
            } else if results.is_empty() {
                Frame::null()
            } else {
                Frame::Integer(results[0])
            }
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::null(),
    }
}

/// RPOPLPUSH command - atomically pop from source and push to destination
pub fn rpoplpush(store: &Arc<Store>, db: u8, source: &Bytes, destination: &Bytes) -> Frame {
    lmove(
        store,
        db,
        source,
        destination,
        ListDirection::Right,
        ListDirection::Left,
    )
}

/// LMPOP command - pop elements from multiple lists
pub fn lmpop(
    store: &Arc<Store>,
    db: u8,
    keys: &[Bytes],
    direction: ListDirection,
    count: usize,
) -> Frame {
    // Find first non-empty list
    for key in keys {
        let mut list = match store.get(db, key) {
            Some(Value::List(existing)) => existing,
            Some(_) => continue, // Wrong type, skip
            None => continue,
        };

        if list.is_empty() {
            continue;
        }

        // Pop elements
        let mut results = Vec::with_capacity(count.min(list.len()));
        for _ in 0..count {
            let element = match direction {
                ListDirection::Left => list.pop_front(),
                ListDirection::Right => list.pop_back(),
            };
            match element {
                Some(e) => results.push(Frame::bulk(e)),
                None => break,
            }
        }

        // Update storage
        if list.is_empty() {
            store.del(db, &[key.clone()]);
        } else {
            store.set(db, key.clone(), Value::List(list));
        }

        // Return [key, [elements...]]
        return Frame::array(vec![Frame::bulk(key.clone()), Frame::array(results)]);
    }

    // No non-empty list found
    Frame::null()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_store() -> Arc<Store> {
        Arc::new(Store::new(16))
    }

    #[test]
    fn test_lpush_rpush() {
        let store = create_store();
        let key = Bytes::from("mylist");

        // LPUSH
        let response = lpush(&store, 0, &key, &[Bytes::from("world")]);
        assert!(matches!(response, Frame::Integer(1)));

        let response = lpush(&store, 0, &key, &[Bytes::from("hello")]);
        assert!(matches!(response, Frame::Integer(2)));

        // RPUSH
        let response = rpush(&store, 0, &key, &[Bytes::from("!")]);
        assert!(matches!(response, Frame::Integer(3)));

        // LRANGE to verify order
        let response = lrange(&store, 0, &key, 0, -1);
        match response {
            Frame::Array(Some(items)) => {
                assert_eq!(items.len(), 3);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_lpop_rpop() {
        let store = create_store();
        let key = Bytes::from("mylist");

        rpush(
            &store,
            0,
            &key,
            &[Bytes::from("one"), Bytes::from("two"), Bytes::from("three")],
        );

        // LPOP
        let response = lpop(&store, 0, &key, None);
        match response {
            Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("one")),
            _ => panic!("Expected bulk string"),
        }

        // RPOP
        let response = rpop(&store, 0, &key, None);
        match response {
            Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("three")),
            _ => panic!("Expected bulk string"),
        }
    }

    #[test]
    fn test_lrange() {
        let store = create_store();
        let key = Bytes::from("mylist");

        rpush(
            &store,
            0,
            &key,
            &[Bytes::from("one"), Bytes::from("two"), Bytes::from("three")],
        );

        // Full range
        let response = lrange(&store, 0, &key, 0, -1);
        match response {
            Frame::Array(Some(items)) => assert_eq!(items.len(), 3),
            _ => panic!("Expected array"),
        }

        // Partial range
        let response = lrange(&store, 0, &key, 0, 1);
        match response {
            Frame::Array(Some(items)) => assert_eq!(items.len(), 2),
            _ => panic!("Expected array"),
        }

        // Negative indices
        let response = lrange(&store, 0, &key, -2, -1);
        match response {
            Frame::Array(Some(items)) => assert_eq!(items.len(), 2),
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_llen() {
        let store = create_store();
        let key = Bytes::from("mylist");

        // Non-existing list
        let response = llen(&store, 0, &key);
        assert!(matches!(response, Frame::Integer(0)));

        rpush(&store, 0, &key, &[Bytes::from("one"), Bytes::from("two")]);

        let response = llen(&store, 0, &key);
        assert!(matches!(response, Frame::Integer(2)));
    }

    #[test]
    fn test_lindex() {
        let store = create_store();
        let key = Bytes::from("mylist");

        rpush(
            &store,
            0,
            &key,
            &[Bytes::from("one"), Bytes::from("two"), Bytes::from("three")],
        );

        // Positive index
        let response = lindex(&store, 0, &key, 0);
        match response {
            Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("one")),
            _ => panic!("Expected bulk string"),
        }

        // Negative index
        let response = lindex(&store, 0, &key, -1);
        match response {
            Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("three")),
            _ => panic!("Expected bulk string"),
        }

        // Out of range
        let response = lindex(&store, 0, &key, 100);
        assert!(matches!(response, Frame::Bulk(None)));
    }

    #[test]
    fn test_lset() {
        let store = create_store();
        let key = Bytes::from("mylist");

        rpush(
            &store,
            0,
            &key,
            &[Bytes::from("one"), Bytes::from("two"), Bytes::from("three")],
        );

        let response = lset(&store, 0, &key, 1, &Bytes::from("TWO"));
        assert!(matches!(response, Frame::Simple(s) if s == "OK"));

        let response = lindex(&store, 0, &key, 1);
        match response {
            Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("TWO")),
            _ => panic!("Expected bulk string"),
        }
    }

    #[test]
    fn test_lrem() {
        let store = create_store();
        let key = Bytes::from("mylist");

        rpush(
            &store,
            0,
            &key,
            &[
                Bytes::from("hello"),
                Bytes::from("hello"),
                Bytes::from("world"),
                Bytes::from("hello"),
            ],
        );

        // Remove 2 occurrences from head
        let response = lrem(&store, 0, &key, 2, &Bytes::from("hello"));
        assert!(matches!(response, Frame::Integer(2)));

        let response = llen(&store, 0, &key);
        assert!(matches!(response, Frame::Integer(2)));
    }

    #[test]
    fn test_linsert() {
        let store = create_store();
        let key = Bytes::from("mylist");

        rpush(
            &store,
            0,
            &key,
            &[Bytes::from("Hello"), Bytes::from("World")],
        );

        // Insert before
        let response = linsert(
            &store,
            0,
            &key,
            true,
            &Bytes::from("World"),
            &Bytes::from("There"),
        );
        assert!(matches!(response, Frame::Integer(3)));

        // Insert after
        let response = linsert(
            &store,
            0,
            &key,
            false,
            &Bytes::from("World"),
            &Bytes::from("!"),
        );
        assert!(matches!(response, Frame::Integer(4)));
    }

    #[test]
    fn test_lpushx_rpushx() {
        let store = create_store();
        let key = Bytes::from("mylist");
        let nonexistent = Bytes::from("nonexistent");

        // LPUSHX on non-existing key
        let response = lpushx(&store, 0, &nonexistent, &[Bytes::from("value")]);
        assert!(matches!(response, Frame::Integer(0)));

        // Create list and LPUSHX
        rpush(&store, 0, &key, &[Bytes::from("world")]);
        let response = lpushx(&store, 0, &key, &[Bytes::from("hello")]);
        assert!(matches!(response, Frame::Integer(2)));

        // RPUSHX
        let response = rpushx(&store, 0, &key, &[Bytes::from("!")]);
        assert!(matches!(response, Frame::Integer(3)));
    }

    #[test]
    fn test_wrongtype_error() {
        let store = create_store();
        let key = Bytes::from("string_key");

        // Set as string
        store.set(0, key.clone(), Value::String(Bytes::from("value")));

        // Try list operation
        let response = lpush(&store, 0, &key, &[Bytes::from("value")]);
        assert!(matches!(response, Frame::Error(_)));
    }
}
