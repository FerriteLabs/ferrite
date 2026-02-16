//! Key management command handlers
//!
//! This module contains handlers for key operations:
//! - DUMP/RESTORE (key serialization)
//! - SORT (sorting lists/sets)
//! - MOVE (move key between databases)
//! - MIGRATE (migrate keys between servers)
//! - SWAPDB (swap databases)

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::{Duration, SystemTime};

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::{Store, Value};
use std::sync::Arc;

/// Handle DUMP command - serialize a key's value
pub fn dump(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(value) => {
            let serialized = match value {
                Value::String(data) => {
                    let mut result = vec![0u8]; // type marker for string
                    result.extend_from_slice(&data);
                    result
                }
                Value::List(list) => {
                    let mut result = vec![1u8]; // type marker for list
                    for item in list.iter() {
                        let len = item.len() as u32;
                        result.extend_from_slice(&len.to_le_bytes());
                        result.extend_from_slice(item);
                    }
                    result
                }
                Value::Hash(hash) => {
                    let mut result = vec![2u8]; // type marker for hash
                    for (k, v) in hash.iter() {
                        let klen = k.len() as u32;
                        let vlen = v.len() as u32;
                        result.extend_from_slice(&klen.to_le_bytes());
                        result.extend_from_slice(k);
                        result.extend_from_slice(&vlen.to_le_bytes());
                        result.extend_from_slice(v);
                    }
                    result
                }
                Value::Set(set) => {
                    let mut result = vec![3u8]; // type marker for set
                    for item in set.iter() {
                        let len = item.len() as u32;
                        result.extend_from_slice(&len.to_le_bytes());
                        result.extend_from_slice(item);
                    }
                    result
                }
                Value::SortedSet { by_member, .. } => {
                    let mut result = vec![4u8]; // type marker for sorted set
                    for (member, score) in by_member.iter() {
                        let mlen = member.len() as u32;
                        result.extend_from_slice(&mlen.to_le_bytes());
                        result.extend_from_slice(member);
                        result.extend_from_slice(&score.to_le_bytes());
                    }
                    result
                }
                _ => {
                    return Frame::error("ERR DUMP not supported for this data type");
                }
            };
            Frame::Bulk(Some(Bytes::from(serialized)))
        }
        None => Frame::Null,
    }
}

/// Handle RESTORE command - restore a key from serialized data
pub fn restore(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    ttl: u64,
    data: &Bytes,
    replace: bool,
) -> Frame {
    if store.get(db, key).is_some() && !replace {
        return Frame::error("BUSYKEY Target key name already exists.");
    }

    if data.is_empty() {
        return Frame::error("ERR DUMP payload version or checksum are wrong");
    }

    let value = match data[0] {
        0 => Value::String(Bytes::copy_from_slice(&data[1..])),
        1 => {
            let mut list = VecDeque::new();
            let mut pos = 1;
            while pos + 4 <= data.len() {
                let len =
                    u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                        as usize;
                pos += 4;
                if pos + len > data.len() {
                    return Frame::error("ERR DUMP payload version or checksum are wrong");
                }
                list.push_back(Bytes::copy_from_slice(&data[pos..pos + len]));
                pos += len;
            }
            Value::List(list)
        }
        2 => {
            let mut hash = HashMap::new();
            let mut pos = 1;
            while pos + 4 <= data.len() {
                let klen =
                    u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                        as usize;
                pos += 4;
                if pos + klen > data.len() {
                    return Frame::error("ERR DUMP payload version or checksum are wrong");
                }
                let key = Bytes::copy_from_slice(&data[pos..pos + klen]);
                pos += klen;
                if pos + 4 > data.len() {
                    return Frame::error("ERR DUMP payload version or checksum are wrong");
                }
                let vlen =
                    u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                        as usize;
                pos += 4;
                if pos + vlen > data.len() {
                    return Frame::error("ERR DUMP payload version or checksum are wrong");
                }
                let value = Bytes::copy_from_slice(&data[pos..pos + vlen]);
                pos += vlen;
                hash.insert(key, value);
            }
            Value::Hash(hash)
        }
        3 => {
            let mut set = HashSet::new();
            let mut pos = 1;
            while pos + 4 <= data.len() {
                let len =
                    u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                        as usize;
                pos += 4;
                if pos + len > data.len() {
                    return Frame::error("ERR DUMP payload version or checksum are wrong");
                }
                set.insert(Bytes::copy_from_slice(&data[pos..pos + len]));
                pos += len;
            }
            Value::Set(set)
        }
        4 => {
            let mut by_score = BTreeMap::new();
            let mut by_member = HashMap::new();
            let mut pos = 1;
            while pos + 4 <= data.len() {
                let mlen =
                    u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                        as usize;
                pos += 4;
                if pos + mlen > data.len() {
                    return Frame::error("ERR DUMP payload version or checksum are wrong");
                }
                let member = Bytes::copy_from_slice(&data[pos..pos + mlen]);
                pos += mlen;
                if pos + 8 > data.len() {
                    return Frame::error("ERR DUMP payload version or checksum are wrong");
                }
                let score = f64::from_le_bytes([
                    data[pos],
                    data[pos + 1],
                    data[pos + 2],
                    data[pos + 3],
                    data[pos + 4],
                    data[pos + 5],
                    data[pos + 6],
                    data[pos + 7],
                ]);
                pos += 8;
                by_score.insert((ordered_float::OrderedFloat(score), member.clone()), ());
                by_member.insert(member, score);
            }
            Value::SortedSet {
                by_score,
                by_member,
            }
        }
        _ => {
            return Frame::error("ERR DUMP payload version or checksum are wrong");
        }
    };

    if ttl > 0 {
        let expires_at = SystemTime::now() + Duration::from_millis(ttl);
        store.set_with_expiry(db, key.clone(), value, expires_at);
    } else {
        store.set(db, key.clone(), value);
    }

    Frame::simple("OK")
}

/// Handle SORT command - basic implementation
pub fn sort(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(Value::List(list)) => {
            let mut items: Vec<Bytes> = list.iter().cloned().collect();
            sort_items(&mut items);
            Frame::array(items.into_iter().map(|b| Frame::Bulk(Some(b))).collect())
        }
        Some(Value::Set(set)) => {
            let mut items: Vec<Bytes> = set.iter().cloned().collect();
            sort_items(&mut items);
            Frame::array(items.into_iter().map(|b| Frame::Bulk(Some(b))).collect())
        }
        Some(Value::SortedSet { by_member, .. }) => {
            let mut items: Vec<Bytes> = by_member.keys().cloned().collect();
            sort_items(&mut items);
            Frame::array(items.into_iter().map(|b| Frame::Bulk(Some(b))).collect())
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::array(vec![]),
    }
}

/// Helper function to sort items (numeric if possible, otherwise lexicographic)
fn sort_items(items: &mut [Bytes]) {
    items.sort_by(|a, b| {
        let a_num: Option<f64> = std::str::from_utf8(a).ok().and_then(|s| s.parse().ok());
        let b_num: Option<f64> = std::str::from_utf8(b).ok().and_then(|s| s.parse().ok());
        match (a_num, b_num) {
            (Some(an), Some(bn)) => an.partial_cmp(&bn).unwrap_or(std::cmp::Ordering::Equal),
            _ => a.cmp(b),
        }
    });
}

/// Handle MOVE command - move a key from current database to another
pub fn move_key(store: &Arc<Store>, current_db: u8, key: &Bytes, target_db: u8) -> Frame {
    if target_db >= 16 {
        return Frame::error("ERR invalid DB index");
    }

    if current_db == target_db {
        return Frame::Integer(0);
    }

    let value = store.get(current_db, key);

    match value {
        Some(val) => {
            if store.exists(target_db, &[key.clone()]) > 0 {
                return Frame::Integer(0);
            }
            store.del(current_db, &[key.clone()]);
            store.set(target_db, key.clone(), val);
            Frame::Integer(1)
        }
        None => Frame::Integer(0),
    }
}

/// Handle MIGRATE command - atomically migrate keys to another server
#[allow(clippy::too_many_arguments)]
pub fn migrate(
    _host: &str,
    _port: u16,
    _key: Option<&Bytes>,
    _destination_db: u8,
    _timeout: i64,
    _copy: bool,
    _replace: bool,
) -> Frame {
    // MIGRATE requires network I/O to another server
    // For now, return NOKEY since we can't actually migrate
    Frame::simple("NOKEY")
}

/// Handle SWAPDB command - swap two databases atomically
pub fn swapdb(index1: u8, index2: u8) -> Frame {
    if index1 >= 16 || index2 >= 16 {
        return Frame::error("ERR invalid DB index");
    }

    if index1 == index2 {
        return Frame::simple("OK");
    }

    // Placeholder - actual implementation would need special Store support
    Frame::simple("OK")
}

/// Handle WAIT command - wait for replication acknowledgment
pub fn wait(_numreplicas: i64, _timeout: i64) -> Frame {
    // No replicas in standalone mode
    Frame::Integer(0)
}

/// Handle SHUTDOWN command
pub fn shutdown(_nosave: bool, _save: bool, _now: bool, _force: bool, abort: bool) -> Frame {
    if abort {
        return Frame::simple("OK");
    }
    Frame::simple("OK")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Store;

    fn create_test_store() -> Arc<Store> {
        Arc::new(Store::new(16))
    }

    #[test]
    fn test_dump_nonexistent_key() {
        let store = create_test_store();
        let result = dump(&store, 0, &Bytes::from("nonexistent"));
        assert!(matches!(result, Frame::Null));
    }

    #[test]
    fn test_dump_string() {
        let store = create_test_store();
        store.set(0, Bytes::from("key"), Value::String(Bytes::from("hello")));
        let result = dump(&store, 0, &Bytes::from("key"));
        assert!(matches!(result, Frame::Bulk(Some(_))));
    }

    #[test]
    fn test_restore_empty_data() {
        let store = create_test_store();
        let result = restore(&store, 0, &Bytes::from("key"), 0, &Bytes::new(), false);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_move_key_nonexistent() {
        let store = create_test_store();
        let result = move_key(&store, 0, &Bytes::from("nonexistent"), 1);
        match result {
            Frame::Integer(0) => {}
            _ => panic!("Expected Integer(0)"),
        }
    }

    #[test]
    fn test_move_key_same_db() {
        let store = create_test_store();
        let result = move_key(&store, 0, &Bytes::from("key"), 0);
        match result {
            Frame::Integer(0) => {}
            _ => panic!("Expected Integer(0)"),
        }
    }

    #[test]
    fn test_swapdb_invalid_index() {
        let result = swapdb(0, 20);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_swapdb_same_index() {
        let result = swapdb(0, 0);
        assert!(matches!(result, Frame::Simple(_)));
    }

    #[test]
    fn test_wait_standalone() {
        let result = wait(1, 1000);
        match result {
            Frame::Integer(0) => {}
            _ => panic!("Expected Integer(0)"),
        }
    }
}
