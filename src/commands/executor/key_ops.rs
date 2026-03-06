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

    /// SORT command — supports BY, GET, DESC, ALPHA, LIMIT, STORE options
    pub(super) fn sort(
        &self,
        db: u8,
        key: &Bytes,
        options: &crate::commands::parser::SortOptions,
    ) -> Frame {
        // Extract elements from the source key
        let mut items: Vec<Bytes> = match self.store.get(db, key) {
            Some(Value::List(list)) => list.iter().cloned().collect(),
            Some(Value::Set(set)) => set.iter().cloned().collect(),
            Some(Value::SortedSet { by_member, .. }) => by_member.keys().cloned().collect(),
            Some(_) => {
                return Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                );
            }
            None => return Frame::array(vec![]),
        };

        // BY pattern: sort by external key values instead of element values
        if let Some(ref by_pattern) = options.by {
            // BY nosort — skip sorting entirely
            if by_pattern != "nosort" {
                items.sort_by(|a, b| {
                    let a_key = Self::derive_sort_key(by_pattern, a);
                    let b_key = Self::derive_sort_key(by_pattern, b);
                    let a_val = self.sort_lookup_value(db, &a_key, options.alpha);
                    let b_val = self.sort_lookup_value(db, &b_key, options.alpha);
                    a_val.cmp(&b_val)
                });
            }
        } else if options.alpha {
            items.sort_by(|a, b| a.cmp(b));
        } else {
            items.sort_by(|a, b| {
                let a_num = Self::parse_sort_float(a);
                let b_num = Self::parse_sort_float(b);
                a_num
                    .partial_cmp(&b_num)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }

        // DESC reverses the order
        if options.desc {
            items.reverse();
        }

        // LIMIT offset count
        if let Some((offset, count)) = options.limit {
            let start = if offset < 0 { 0usize } else { offset as usize };
            let end = if count < 0 {
                items.len()
            } else {
                (start + count as usize).min(items.len())
            };
            if start >= items.len() {
                items.clear();
            } else {
                items = items[start..end].to_vec();
            }
        }

        // Build result: apply GET patterns if any, otherwise return elements
        let result_frames: Vec<Frame> = if options.get.is_empty() {
            items.iter().map(|b| Frame::Bulk(Some(b.clone()))).collect()
        } else {
            let mut frames = Vec::with_capacity(items.len() * options.get.len());
            for item in &items {
                for pattern in &options.get {
                    if pattern == "#" {
                        frames.push(Frame::Bulk(Some(item.clone())));
                    } else {
                        let derived = Self::derive_sort_key(pattern, item);
                        let key_bytes = Bytes::from(derived);
                        match self.store.get(db, &key_bytes) {
                            Some(Value::String(v)) => frames.push(Frame::Bulk(Some(v))),
                            _ => frames.push(Frame::Null),
                        }
                    }
                }
            }
            frames
        };

        // STORE saves result to destination key
        if let Some(ref dest) = options.store {
            // Collect Bytes from the result frames
            let list: std::collections::VecDeque<Bytes> = result_frames
                .iter()
                .map(|f| match f {
                    Frame::Bulk(Some(b)) => b.clone(),
                    _ => Bytes::new(),
                })
                .collect();
            let count = list.len() as i64;
            self.store.set(db, dest.clone(), Value::List(list));
            Frame::Integer(count)
        } else {
            Frame::array(result_frames)
        }
    }

    /// Derive a lookup key by replacing `*` in the pattern with the element value.
    fn derive_sort_key(pattern: &str, element: &Bytes) -> String {
        let elem_str = String::from_utf8_lossy(element);
        // Handle hash field patterns: "key_*->field"
        pattern.replace('*', &elem_str)
    }

    /// Look up an external key's value for BY sorting.
    fn sort_lookup_value(&self, db: u8, key: &str, alpha: bool) -> SortKey {
        let key_bytes = Bytes::from(key.to_string());

        // Handle hash field dereference: "key->field"
        if let Some(arrow_pos) = key.find("->") {
            let hash_key = Bytes::from(key[..arrow_pos].to_string());
            let field = Bytes::from(key[arrow_pos + 2..].to_string());
            if let Some(Value::Hash(hash)) = self.store.get(db, &hash_key) {
                if let Some(val) = hash.get(&field) {
                    return if alpha {
                        SortKey::Alpha(val.to_vec())
                    } else {
                        SortKey::Numeric(Self::parse_sort_float(val))
                    };
                }
            }
            return SortKey::Numeric(0.0);
        }

        match self.store.get(db, &key_bytes) {
            Some(Value::String(v)) => {
                if alpha {
                    SortKey::Alpha(v.to_vec())
                } else {
                    SortKey::Numeric(Self::parse_sort_float(&v))
                }
            }
            _ => SortKey::Numeric(0.0),
        }
    }

    fn parse_sort_float(data: &[u8]) -> f64 {
        std::str::from_utf8(data)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0)
    }
}

/// Sort key for BY-pattern comparisons.
#[derive(PartialEq)]
enum SortKey {
    Numeric(f64),
    Alpha(Vec<u8>),
}

impl Eq for SortKey {}

impl PartialOrd for SortKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (SortKey::Numeric(a), SortKey::Numeric(b)) => {
                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
            }
            (SortKey::Alpha(a), SortKey::Alpha(b)) => a.cmp(b),
            _ => std::cmp::Ordering::Equal,
        }
    }
}
