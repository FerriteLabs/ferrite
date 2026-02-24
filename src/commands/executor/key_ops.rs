//! Key management command helper methods on CommandExecutor.

use std::time::{Duration, SystemTime};

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::Value;

use super::CommandExecutor;

impl CommandExecutor {
    pub(super) fn expire(&self, db: u8, key: &Bytes, milliseconds: u64) -> Frame {
        let expires_at = SystemTime::now() + Duration::from_millis(milliseconds);
        if self.store.expire(db, key, expires_at) {
            Frame::Integer(1)
        } else {
            Frame::Integer(0)
        }
    }

    pub(super) fn ttl(&self, db: u8, key: &Bytes) -> Frame {
        match self.store.ttl(db, key) {
            Some(ttl) => Frame::Integer(ttl),
            None => Frame::Integer(-2), // Key doesn't exist
        }
    }

    pub(super) fn pttl(&self, db: u8, key: &Bytes) -> Frame {
        // For now, same as TTL but in milliseconds
        // We'd need to store expiry differently for millisecond precision
        match self.store.ttl(db, key) {
            Some(ttl) if ttl >= 0 => Frame::Integer(ttl * 1000),
            Some(ttl) => Frame::Integer(ttl),
            None => Frame::Integer(-2),
        }
    }

    pub(super) fn persist(&self, db: u8, key: &Bytes) -> Frame {
        // Remove expiration from key, returns 1 if key existed with expiry
        if self.store.persist(db, key) {
            Frame::Integer(1)
        } else {
            Frame::Integer(0)
        }
    }

    pub(super) fn dump(&self, db: u8, key: &Bytes) -> Frame {
        // Get the value
        match self.store.get(db, key) {
            Some(value) => {
                // Serialize the value - in a real implementation this would use
                // Redis RDB format. For now, we use a simple format.
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
                        // Stream and HyperLogLog not supported in DUMP
                        return Frame::error("ERR DUMP not supported for this data type");
                    }
                };
                Frame::Bulk(Some(Bytes::from(serialized)))
            }
            None => Frame::Null,
        }
    }

    /// RESTORE command - restore a key from serialized data
    pub(super) fn restore(
        &self,
        db: u8,
        key: &Bytes,
        ttl: u64,
        data: &Bytes,
        replace: bool,
    ) -> Frame {
        // Check if key exists and replace flag
        if self.store.get(db, key).is_some() && !replace {
            return Frame::error("BUSYKEY Target key name already exists.");
        }

        if data.is_empty() {
            return Frame::error("ERR DUMP payload version or checksum are wrong");
        }

        // Deserialize the value based on type marker
        let value = match data[0] {
            0 => {
                // String
                Value::String(Bytes::copy_from_slice(&data[1..]))
            }
            1 => {
                // List
                let mut list = std::collections::VecDeque::new();
                let mut pos = 1;
                while pos + 4 <= data.len() {
                    let len = u32::from_le_bytes([
                        data[pos],
                        data[pos + 1],
                        data[pos + 2],
                        data[pos + 3],
                    ]) as usize;
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
                // Hash
                let mut hash = std::collections::HashMap::new();
                let mut pos = 1;
                while pos + 4 <= data.len() {
                    let klen = u32::from_le_bytes([
                        data[pos],
                        data[pos + 1],
                        data[pos + 2],
                        data[pos + 3],
                    ]) as usize;
                    pos += 4;
                    if pos + klen > data.len() {
                        return Frame::error("ERR DUMP payload version or checksum are wrong");
                    }
                    let key = Bytes::copy_from_slice(&data[pos..pos + klen]);
                    pos += klen;
                    if pos + 4 > data.len() {
                        return Frame::error("ERR DUMP payload version or checksum are wrong");
                    }
                    let vlen = u32::from_le_bytes([
                        data[pos],
                        data[pos + 1],
                        data[pos + 2],
                        data[pos + 3],
                    ]) as usize;
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
                // Set
                let mut set = std::collections::HashSet::new();
                let mut pos = 1;
                while pos + 4 <= data.len() {
                    let len = u32::from_le_bytes([
                        data[pos],
                        data[pos + 1],
                        data[pos + 2],
                        data[pos + 3],
                    ]) as usize;
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
                // Sorted set
                let mut by_score = std::collections::BTreeMap::new();
                let mut by_member = std::collections::HashMap::new();
                let mut pos = 1;
                while pos + 4 <= data.len() {
                    let mlen = u32::from_le_bytes([
                        data[pos],
                        data[pos + 1],
                        data[pos + 2],
                        data[pos + 3],
                    ]) as usize;
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

        // Set the value with optional TTL
        if ttl > 0 {
            let expires_at = SystemTime::now() + Duration::from_millis(ttl);
            self.store
                .set_with_expiry(db, key.clone(), value, expires_at);
        } else {
            self.store.set(db, key.clone(), value);
        }

        Frame::simple("OK")
    }

    /// SORT command - basic implementation
    pub(super) fn sort(&self, db: u8, key: &Bytes) -> Frame {
        match self.store.get(db, key) {
            Some(Value::List(list)) => {
                let mut items: Vec<Bytes> = list.iter().cloned().collect();
                // Try numeric sort, fall back to lexicographic
                items.sort_by(|a, b| {
                    let a_num: Option<f64> =
                        std::str::from_utf8(a).ok().and_then(|s| s.parse().ok());
                    let b_num: Option<f64> =
                        std::str::from_utf8(b).ok().and_then(|s| s.parse().ok());
                    match (a_num, b_num) {
                        (Some(an), Some(bn)) => {
                            an.partial_cmp(&bn).unwrap_or(std::cmp::Ordering::Equal)
                        }
                        _ => a.cmp(b),
                    }
                });
                Frame::array(items.into_iter().map(|b| Frame::Bulk(Some(b))).collect())
            }
            Some(Value::Set(set)) => {
                let mut items: Vec<Bytes> = set.iter().cloned().collect();
                items.sort_by(|a, b| {
                    let a_num: Option<f64> =
                        std::str::from_utf8(a).ok().and_then(|s| s.parse().ok());
                    let b_num: Option<f64> =
                        std::str::from_utf8(b).ok().and_then(|s| s.parse().ok());
                    match (a_num, b_num) {
                        (Some(an), Some(bn)) => {
                            an.partial_cmp(&bn).unwrap_or(std::cmp::Ordering::Equal)
                        }
                        _ => a.cmp(b),
                    }
                });
                Frame::array(items.into_iter().map(|b| Frame::Bulk(Some(b))).collect())
            }
            Some(Value::SortedSet { by_member, .. }) => {
                let mut items: Vec<Bytes> = by_member.keys().cloned().collect();
                items.sort_by(|a, b| {
                    let a_num: Option<f64> =
                        std::str::from_utf8(a).ok().and_then(|s| s.parse().ok());
                    let b_num: Option<f64> =
                        std::str::from_utf8(b).ok().and_then(|s| s.parse().ok());
                    match (a_num, b_num) {
                        (Some(an), Some(bn)) => {
                            an.partial_cmp(&bn).unwrap_or(std::cmp::Ordering::Equal)
                        }
                        _ => a.cmp(b),
                    }
                });
                Frame::array(items.into_iter().map(|b| Frame::Bulk(Some(b))).collect())
            }
            Some(_) => {
                Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
            }
            None => Frame::array(vec![]),
        }
    }
}
