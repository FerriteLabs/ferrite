//! Replication stream
//!
//! This module implements the replication stream that propagates commands
//! from the primary to replicas.

use bytes::{Bytes, BytesMut};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

use crate::protocol::{encode_frame, Frame};

/// Commands that can be replicated
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub enum ReplicationCommand {
    /// SET key value
    Set { key: Bytes, value: Bytes },
    /// SET key value with expiry (milliseconds)
    SetEx {
        key: Bytes,
        value: Bytes,
        expire_ms: u64,
    },
    /// SETNX key value (SET if Not eXists)
    SetNx { key: Bytes, value: Bytes },
    /// SETEX key seconds value (SET with expiration in seconds)
    SetExSeconds {
        key: Bytes,
        seconds: u64,
        value: Bytes,
    },
    /// PSETEX key milliseconds value (SET with expiration in ms)
    PSetEx {
        key: Bytes,
        milliseconds: u64,
        value: Bytes,
    },
    /// MSET key value [key value ...]
    MSet { pairs: Vec<(Bytes, Bytes)> },
    /// APPEND key value
    Append { key: Bytes, value: Bytes },
    /// SETRANGE key offset value
    SetRange {
        key: Bytes,
        offset: usize,
        value: Bytes,
    },
    /// INCR key
    Incr { key: Bytes },
    /// DECR key
    Decr { key: Bytes },
    /// INCRBY key increment
    IncrBy { key: Bytes, delta: i64 },
    /// DECRBY key decrement
    DecrBy { key: Bytes, delta: i64 },
    /// INCRBYFLOAT key increment
    IncrByFloat { key: Bytes, delta: f64 },
    /// DEL key [key ...]
    Del { keys: Vec<Bytes> },
    /// UNLINK key [key ...] (async DEL)
    Unlink { keys: Vec<Bytes> },
    /// EXPIRE key seconds
    Expire { key: Bytes, seconds: u64 },
    /// PEXPIRE key milliseconds
    PExpire { key: Bytes, milliseconds: u64 },
    /// EXPIREAT key timestamp
    ExpireAt { key: Bytes, timestamp: u64 },
    /// PEXPIREAT key milliseconds-timestamp
    PExpireAt { key: Bytes, timestamp_ms: u64 },
    /// PERSIST key (remove expiry)
    Persist { key: Bytes },
    /// RENAME key newkey
    Rename { key: Bytes, newkey: Bytes },
    /// LPUSH key value [value ...]
    LPush { key: Bytes, values: Vec<Bytes> },
    /// RPUSH key value [value ...]
    RPush { key: Bytes, values: Vec<Bytes> },
    /// LPUSHX key value
    LPushX { key: Bytes, value: Bytes },
    /// RPUSHX key value
    RPushX { key: Bytes, value: Bytes },
    /// LPOP key [count]
    LPop { key: Bytes, count: Option<usize> },
    /// RPOP key [count]
    RPop { key: Bytes, count: Option<usize> },
    /// LSET key index value
    LSet {
        key: Bytes,
        index: i64,
        value: Bytes,
    },
    /// LTRIM key start stop
    LTrim { key: Bytes, start: i64, stop: i64 },
    /// LINSERT key BEFORE|AFTER pivot value
    LInsert {
        key: Bytes,
        before: bool,
        pivot: Bytes,
        value: Bytes,
    },
    /// LREM key count value
    LRem {
        key: Bytes,
        count: i64,
        value: Bytes,
    },
    /// HSET key field value [field value ...]
    HSet {
        key: Bytes,
        pairs: Vec<(Bytes, Bytes)>,
    },
    /// HSETNX key field value
    HSetNx {
        key: Bytes,
        field: Bytes,
        value: Bytes,
    },
    /// HDEL key field [field ...]
    HDel { key: Bytes, fields: Vec<Bytes> },
    /// HINCRBY key field increment
    HIncrBy {
        key: Bytes,
        field: Bytes,
        delta: i64,
    },
    /// HINCRBYFLOAT key field increment
    HIncrByFloat {
        key: Bytes,
        field: Bytes,
        delta: f64,
    },
    /// SADD key member [member ...]
    SAdd { key: Bytes, members: Vec<Bytes> },
    /// SREM key member [member ...]
    SRem { key: Bytes, members: Vec<Bytes> },
    /// SPOP key [count]
    SPop { key: Bytes, count: Option<usize> },
    /// SMOVE source destination member
    SMove {
        source: Bytes,
        dest: Bytes,
        member: Bytes,
    },
    /// ZADD key score member [score member ...]
    ZAdd {
        key: Bytes,
        pairs: Vec<(f64, Bytes)>,
    },
    /// ZREM key member [member ...]
    ZRem { key: Bytes, members: Vec<Bytes> },
    /// ZINCRBY key increment member
    ZIncrBy {
        key: Bytes,
        delta: f64,
        member: Bytes,
    },
    /// ZREMRANGEBYRANK key start stop
    ZRemRangeByRank { key: Bytes, start: i64, stop: i64 },
    /// ZREMRANGEBYSCORE key min max
    ZRemRangeByScore { key: Bytes, min: f64, max: f64 },
    /// ZREMRANGEBYLEX key min max
    ZRemRangeByLex { key: Bytes, min: Bytes, max: Bytes },
    /// XADD key ID field value [field value ...]
    XAdd {
        key: Bytes,
        id: Bytes,
        fields: Vec<(Bytes, Bytes)>,
    },
    /// XDEL key ID [ID ...]
    XDel { key: Bytes, ids: Vec<Bytes> },
    /// XTRIM key MAXLEN|MINID [~] threshold
    XTrim {
        key: Bytes,
        strategy: Bytes,
        threshold: Bytes,
    },
    /// PFADD key element [element ...]
    PfAdd { key: Bytes, elements: Vec<Bytes> },
    /// PFMERGE destkey sourcekey [sourcekey ...]
    PfMerge { dest: Bytes, sources: Vec<Bytes> },
    /// FLUSHDB
    FlushDb { db: u8 },
    /// FLUSHALL
    FlushAll,
    /// SELECT db (for context switching in replication)
    Select { db: u8 },
    /// Raw command (for commands not explicitly handled)
    Raw { command: Vec<Bytes> },
}

impl ReplicationCommand {
    /// Convert to RESP frame for transmission
    pub fn to_frame(&self) -> Frame {
        match self {
            ReplicationCommand::Set { key, value } => Frame::array(vec![
                Frame::bulk("SET"),
                Frame::bulk(key.clone()),
                Frame::bulk(value.clone()),
            ]),
            ReplicationCommand::SetEx {
                key,
                value,
                expire_ms,
            } => Frame::array(vec![
                Frame::bulk("SET"),
                Frame::bulk(key.clone()),
                Frame::bulk(value.clone()),
                Frame::bulk("PX"),
                Frame::bulk(expire_ms.to_string()),
            ]),
            ReplicationCommand::SetNx { key, value } => Frame::array(vec![
                Frame::bulk("SETNX"),
                Frame::bulk(key.clone()),
                Frame::bulk(value.clone()),
            ]),
            ReplicationCommand::SetExSeconds {
                key,
                seconds,
                value,
            } => Frame::array(vec![
                Frame::bulk("SETEX"),
                Frame::bulk(key.clone()),
                Frame::bulk(seconds.to_string()),
                Frame::bulk(value.clone()),
            ]),
            ReplicationCommand::PSetEx {
                key,
                milliseconds,
                value,
            } => Frame::array(vec![
                Frame::bulk("PSETEX"),
                Frame::bulk(key.clone()),
                Frame::bulk(milliseconds.to_string()),
                Frame::bulk(value.clone()),
            ]),
            ReplicationCommand::MSet { pairs } => {
                let mut frames = vec![Frame::bulk("MSET")];
                for (k, v) in pairs {
                    frames.push(Frame::bulk(k.clone()));
                    frames.push(Frame::bulk(v.clone()));
                }
                Frame::array(frames)
            }
            ReplicationCommand::Append { key, value } => Frame::array(vec![
                Frame::bulk("APPEND"),
                Frame::bulk(key.clone()),
                Frame::bulk(value.clone()),
            ]),
            ReplicationCommand::SetRange { key, offset, value } => Frame::array(vec![
                Frame::bulk("SETRANGE"),
                Frame::bulk(key.clone()),
                Frame::bulk(offset.to_string()),
                Frame::bulk(value.clone()),
            ]),
            ReplicationCommand::Incr { key } => {
                Frame::array(vec![Frame::bulk("INCR"), Frame::bulk(key.clone())])
            }
            ReplicationCommand::Decr { key } => {
                Frame::array(vec![Frame::bulk("DECR"), Frame::bulk(key.clone())])
            }
            ReplicationCommand::IncrBy { key, delta } => Frame::array(vec![
                Frame::bulk("INCRBY"),
                Frame::bulk(key.clone()),
                Frame::bulk(delta.to_string()),
            ]),
            ReplicationCommand::DecrBy { key, delta } => Frame::array(vec![
                Frame::bulk("DECRBY"),
                Frame::bulk(key.clone()),
                Frame::bulk(delta.to_string()),
            ]),
            ReplicationCommand::IncrByFloat { key, delta } => Frame::array(vec![
                Frame::bulk("INCRBYFLOAT"),
                Frame::bulk(key.clone()),
                Frame::bulk(delta.to_string()),
            ]),
            ReplicationCommand::Del { keys } => {
                let mut frames = vec![Frame::bulk("DEL")];
                frames.extend(keys.iter().map(|k| Frame::bulk(k.clone())));
                Frame::array(frames)
            }
            ReplicationCommand::Unlink { keys } => {
                let mut frames = vec![Frame::bulk("UNLINK")];
                frames.extend(keys.iter().map(|k| Frame::bulk(k.clone())));
                Frame::array(frames)
            }
            ReplicationCommand::Expire { key, seconds } => Frame::array(vec![
                Frame::bulk("EXPIRE"),
                Frame::bulk(key.clone()),
                Frame::bulk(seconds.to_string()),
            ]),
            ReplicationCommand::PExpire { key, milliseconds } => Frame::array(vec![
                Frame::bulk("PEXPIRE"),
                Frame::bulk(key.clone()),
                Frame::bulk(milliseconds.to_string()),
            ]),
            ReplicationCommand::ExpireAt { key, timestamp } => Frame::array(vec![
                Frame::bulk("EXPIREAT"),
                Frame::bulk(key.clone()),
                Frame::bulk(timestamp.to_string()),
            ]),
            ReplicationCommand::PExpireAt { key, timestamp_ms } => Frame::array(vec![
                Frame::bulk("PEXPIREAT"),
                Frame::bulk(key.clone()),
                Frame::bulk(timestamp_ms.to_string()),
            ]),
            ReplicationCommand::Persist { key } => {
                Frame::array(vec![Frame::bulk("PERSIST"), Frame::bulk(key.clone())])
            }
            ReplicationCommand::Rename { key, newkey } => Frame::array(vec![
                Frame::bulk("RENAME"),
                Frame::bulk(key.clone()),
                Frame::bulk(newkey.clone()),
            ]),
            ReplicationCommand::LPush { key, values } => {
                let mut frames = vec![Frame::bulk("LPUSH"), Frame::bulk(key.clone())];
                frames.extend(values.iter().map(|v| Frame::bulk(v.clone())));
                Frame::array(frames)
            }
            ReplicationCommand::RPush { key, values } => {
                let mut frames = vec![Frame::bulk("RPUSH"), Frame::bulk(key.clone())];
                frames.extend(values.iter().map(|v| Frame::bulk(v.clone())));
                Frame::array(frames)
            }
            ReplicationCommand::LPushX { key, value } => Frame::array(vec![
                Frame::bulk("LPUSHX"),
                Frame::bulk(key.clone()),
                Frame::bulk(value.clone()),
            ]),
            ReplicationCommand::RPushX { key, value } => Frame::array(vec![
                Frame::bulk("RPUSHX"),
                Frame::bulk(key.clone()),
                Frame::bulk(value.clone()),
            ]),
            ReplicationCommand::LPop { key, count } => {
                let mut frames = vec![Frame::bulk("LPOP"), Frame::bulk(key.clone())];
                if let Some(c) = count {
                    frames.push(Frame::bulk(c.to_string()));
                }
                Frame::array(frames)
            }
            ReplicationCommand::RPop { key, count } => {
                let mut frames = vec![Frame::bulk("RPOP"), Frame::bulk(key.clone())];
                if let Some(c) = count {
                    frames.push(Frame::bulk(c.to_string()));
                }
                Frame::array(frames)
            }
            ReplicationCommand::LSet { key, index, value } => Frame::array(vec![
                Frame::bulk("LSET"),
                Frame::bulk(key.clone()),
                Frame::bulk(index.to_string()),
                Frame::bulk(value.clone()),
            ]),
            ReplicationCommand::LTrim { key, start, stop } => Frame::array(vec![
                Frame::bulk("LTRIM"),
                Frame::bulk(key.clone()),
                Frame::bulk(start.to_string()),
                Frame::bulk(stop.to_string()),
            ]),
            ReplicationCommand::LInsert {
                key,
                before,
                pivot,
                value,
            } => Frame::array(vec![
                Frame::bulk("LINSERT"),
                Frame::bulk(key.clone()),
                Frame::bulk(if *before { "BEFORE" } else { "AFTER" }),
                Frame::bulk(pivot.clone()),
                Frame::bulk(value.clone()),
            ]),
            ReplicationCommand::LRem { key, count, value } => Frame::array(vec![
                Frame::bulk("LREM"),
                Frame::bulk(key.clone()),
                Frame::bulk(count.to_string()),
                Frame::bulk(value.clone()),
            ]),
            ReplicationCommand::HSet { key, pairs } => {
                let mut frames = vec![Frame::bulk("HSET"), Frame::bulk(key.clone())];
                for (field, value) in pairs {
                    frames.push(Frame::bulk(field.clone()));
                    frames.push(Frame::bulk(value.clone()));
                }
                Frame::array(frames)
            }
            ReplicationCommand::HSetNx { key, field, value } => Frame::array(vec![
                Frame::bulk("HSETNX"),
                Frame::bulk(key.clone()),
                Frame::bulk(field.clone()),
                Frame::bulk(value.clone()),
            ]),
            ReplicationCommand::HDel { key, fields } => {
                let mut frames = vec![Frame::bulk("HDEL"), Frame::bulk(key.clone())];
                frames.extend(fields.iter().map(|f| Frame::bulk(f.clone())));
                Frame::array(frames)
            }
            ReplicationCommand::HIncrBy { key, field, delta } => Frame::array(vec![
                Frame::bulk("HINCRBY"),
                Frame::bulk(key.clone()),
                Frame::bulk(field.clone()),
                Frame::bulk(delta.to_string()),
            ]),
            ReplicationCommand::HIncrByFloat { key, field, delta } => Frame::array(vec![
                Frame::bulk("HINCRBYFLOAT"),
                Frame::bulk(key.clone()),
                Frame::bulk(field.clone()),
                Frame::bulk(delta.to_string()),
            ]),
            ReplicationCommand::SAdd { key, members } => {
                let mut frames = vec![Frame::bulk("SADD"), Frame::bulk(key.clone())];
                frames.extend(members.iter().map(|m| Frame::bulk(m.clone())));
                Frame::array(frames)
            }
            ReplicationCommand::SRem { key, members } => {
                let mut frames = vec![Frame::bulk("SREM"), Frame::bulk(key.clone())];
                frames.extend(members.iter().map(|m| Frame::bulk(m.clone())));
                Frame::array(frames)
            }
            ReplicationCommand::SPop { key, count } => {
                let mut frames = vec![Frame::bulk("SPOP"), Frame::bulk(key.clone())];
                if let Some(c) = count {
                    frames.push(Frame::bulk(c.to_string()));
                }
                Frame::array(frames)
            }
            ReplicationCommand::SMove {
                source,
                dest,
                member,
            } => Frame::array(vec![
                Frame::bulk("SMOVE"),
                Frame::bulk(source.clone()),
                Frame::bulk(dest.clone()),
                Frame::bulk(member.clone()),
            ]),
            ReplicationCommand::ZAdd { key, pairs } => {
                let mut frames = vec![Frame::bulk("ZADD"), Frame::bulk(key.clone())];
                for (score, member) in pairs {
                    frames.push(Frame::bulk(score.to_string()));
                    frames.push(Frame::bulk(member.clone()));
                }
                Frame::array(frames)
            }
            ReplicationCommand::ZRem { key, members } => {
                let mut frames = vec![Frame::bulk("ZREM"), Frame::bulk(key.clone())];
                frames.extend(members.iter().map(|m| Frame::bulk(m.clone())));
                Frame::array(frames)
            }
            ReplicationCommand::ZIncrBy { key, delta, member } => Frame::array(vec![
                Frame::bulk("ZINCRBY"),
                Frame::bulk(key.clone()),
                Frame::bulk(delta.to_string()),
                Frame::bulk(member.clone()),
            ]),
            ReplicationCommand::ZRemRangeByRank { key, start, stop } => Frame::array(vec![
                Frame::bulk("ZREMRANGEBYRANK"),
                Frame::bulk(key.clone()),
                Frame::bulk(start.to_string()),
                Frame::bulk(stop.to_string()),
            ]),
            ReplicationCommand::ZRemRangeByScore { key, min, max } => Frame::array(vec![
                Frame::bulk("ZREMRANGEBYSCORE"),
                Frame::bulk(key.clone()),
                Frame::bulk(min.to_string()),
                Frame::bulk(max.to_string()),
            ]),
            ReplicationCommand::ZRemRangeByLex { key, min, max } => Frame::array(vec![
                Frame::bulk("ZREMRANGEBYLEX"),
                Frame::bulk(key.clone()),
                Frame::bulk(min.clone()),
                Frame::bulk(max.clone()),
            ]),
            ReplicationCommand::XAdd { key, id, fields } => {
                let mut frames = vec![
                    Frame::bulk("XADD"),
                    Frame::bulk(key.clone()),
                    Frame::bulk(id.clone()),
                ];
                for (f, v) in fields {
                    frames.push(Frame::bulk(f.clone()));
                    frames.push(Frame::bulk(v.clone()));
                }
                Frame::array(frames)
            }
            ReplicationCommand::XDel { key, ids } => {
                let mut frames = vec![Frame::bulk("XDEL"), Frame::bulk(key.clone())];
                frames.extend(ids.iter().map(|id| Frame::bulk(id.clone())));
                Frame::array(frames)
            }
            ReplicationCommand::XTrim {
                key,
                strategy,
                threshold,
            } => Frame::array(vec![
                Frame::bulk("XTRIM"),
                Frame::bulk(key.clone()),
                Frame::bulk(strategy.clone()),
                Frame::bulk(threshold.clone()),
            ]),
            ReplicationCommand::PfAdd { key, elements } => {
                let mut frames = vec![Frame::bulk("PFADD"), Frame::bulk(key.clone())];
                frames.extend(elements.iter().map(|e| Frame::bulk(e.clone())));
                Frame::array(frames)
            }
            ReplicationCommand::PfMerge { dest, sources } => {
                let mut frames = vec![Frame::bulk("PFMERGE"), Frame::bulk(dest.clone())];
                frames.extend(sources.iter().map(|s| Frame::bulk(s.clone())));
                Frame::array(frames)
            }
            ReplicationCommand::FlushDb { db } => {
                // First SELECT the db, then FLUSHDB
                Frame::array(vec![Frame::bulk("SELECT"), Frame::bulk(db.to_string())])
            }
            ReplicationCommand::FlushAll => Frame::array(vec![Frame::bulk("FLUSHALL")]),
            ReplicationCommand::Select { db } => {
                Frame::array(vec![Frame::bulk("SELECT"), Frame::bulk(db.to_string())])
            }
            ReplicationCommand::Raw { command } => {
                Frame::array(command.iter().map(|b| Frame::bulk(b.clone())).collect())
            }
        }
    }

    /// Get the exact size of this command when RESP-encoded
    ///
    /// This calculates the actual byte count by encoding the frame,
    /// ensuring accurate offset tracking for replication.
    pub fn encoded_size(&self) -> u64 {
        let frame = self.to_frame();
        let mut buf = BytesMut::new();
        encode_frame(&frame, &mut buf);
        buf.len() as u64
    }

    /// Encode this command to bytes
    pub fn to_bytes(&self) -> BytesMut {
        let frame = self.to_frame();
        let mut buf = BytesMut::new();
        encode_frame(&frame, &mut buf);
        buf
    }
}

/// Entry in the replication backlog
#[derive(Debug, Clone)]
pub struct BacklogEntry {
    /// The command
    pub command: ReplicationCommand,
    /// Offset at which this command starts
    pub offset: u64,
    /// Size of this command (for offset tracking)
    pub size: u64,
}

/// Replication backlog for partial resynchronization
pub struct ReplicationBacklog {
    /// Backlog entries
    entries: VecDeque<BacklogEntry>,
    /// Maximum size of the backlog in bytes
    max_size: u64,
    /// Current size in bytes
    current_size: u64,
    /// First offset in the backlog
    first_offset: u64,
}

impl ReplicationBacklog {
    /// Create a new backlog with given max size
    pub fn new(max_size: u64) -> Self {
        Self {
            entries: VecDeque::new(),
            max_size,
            current_size: 0,
            first_offset: 0,
        }
    }

    /// Add a command to the backlog
    pub fn push(&mut self, command: ReplicationCommand, offset: u64) {
        let size = command.encoded_size();
        let entry = BacklogEntry {
            command,
            offset,
            size,
        };

        self.entries.push_back(entry);
        self.current_size += size;

        // Trim old entries if we exceed max size
        while self.current_size > self.max_size && !self.entries.is_empty() {
            if let Some(old) = self.entries.pop_front() {
                self.current_size -= old.size;
                self.first_offset = self
                    .entries
                    .front()
                    .map(|e| e.offset)
                    .unwrap_or(offset + size);
            }
        }
    }

    /// Check if we can serve a partial sync from the given offset
    pub fn can_partial_sync(&self, offset: u64) -> bool {
        offset >= self.first_offset && offset <= self.last_offset()
    }

    /// Get the last offset in the backlog
    pub fn last_offset(&self) -> u64 {
        self.entries
            .back()
            .map(|e| e.offset + e.size)
            .unwrap_or(self.first_offset)
    }

    /// Get entries starting from the given offset
    pub fn entries_from(&self, offset: u64) -> Vec<&BacklogEntry> {
        self.entries.iter().filter(|e| e.offset >= offset).collect()
    }

    /// Get the first offset in the backlog
    pub fn first_offset(&self) -> u64 {
        self.first_offset
    }

    /// Get current size
    pub fn size(&self) -> u64 {
        self.current_size
    }

    /// Clear the backlog
    pub fn clear(&mut self) {
        self.entries.clear();
        self.current_size = 0;
    }
}

/// Replication stream that broadcasts commands to replicas
pub struct ReplicationStream {
    /// Broadcast sender for commands
    sender: broadcast::Sender<(ReplicationCommand, u64)>,
    /// Backlog for partial resync
    backlog: RwLock<ReplicationBacklog>,
}

impl ReplicationStream {
    /// Create a new replication stream
    pub fn new(backlog_size: u64) -> Self {
        let (sender, _) = broadcast::channel(1024);
        Self {
            sender,
            backlog: RwLock::new(ReplicationBacklog::new(backlog_size)),
        }
    }

    /// Broadcast a command to all replicas
    pub async fn broadcast(&self, command: ReplicationCommand, offset: u64) {
        // Add to backlog
        let mut backlog = self.backlog.write().await;
        backlog.push(command.clone(), offset);
        drop(backlog);

        // Broadcast to connected replicas (ignore errors if no receivers)
        let _ = self.sender.send((command, offset));
    }

    /// Subscribe to the replication stream
    pub fn subscribe(&self) -> broadcast::Receiver<(ReplicationCommand, u64)> {
        self.sender.subscribe()
    }

    /// Check if partial sync is possible from the given offset
    pub async fn can_partial_sync(&self, offset: u64) -> bool {
        self.backlog.read().await.can_partial_sync(offset)
    }

    /// Get commands for partial sync from the given offset
    pub async fn get_partial_sync_commands(&self, offset: u64) -> Vec<ReplicationCommand> {
        self.backlog
            .read()
            .await
            .entries_from(offset)
            .iter()
            .map(|e| e.command.clone())
            .collect()
    }
}

/// Shared replication stream
pub type SharedReplicationStream = Arc<ReplicationStream>;

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn test_replication_command_to_frame() {
        let cmd = ReplicationCommand::Set {
            key: Bytes::from("key"),
            value: Bytes::from("value"),
        };
        let frame = cmd.to_frame();
        match frame {
            Frame::Array(Some(arr)) => {
                assert_eq!(arr.len(), 3);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_backlog() {
        let mut backlog = ReplicationBacklog::new(1000);

        let cmd = ReplicationCommand::Set {
            key: Bytes::from("key"),
            value: Bytes::from("value"),
        };

        backlog.push(cmd.clone(), 0);
        assert!(backlog.can_partial_sync(0));

        // Add more entries
        for i in 1..10 {
            backlog.push(cmd.clone(), i * 20);
        }

        assert!(backlog.can_partial_sync(0));
        assert!(backlog.can_partial_sync(100));
    }

    #[tokio::test]
    async fn test_replication_stream() {
        let stream = ReplicationStream::new(10000);
        let mut rx = stream.subscribe();

        let cmd = ReplicationCommand::Set {
            key: Bytes::from("key"),
            value: Bytes::from("value"),
        };

        stream.broadcast(cmd.clone(), 0).await;

        let (received_cmd, offset) = rx.recv().await.unwrap();
        assert_eq!(offset, 0);
        match received_cmd {
            ReplicationCommand::Set { key, value } => {
                assert_eq!(key, Bytes::from("key"));
                assert_eq!(value, Bytes::from("value"));
            }
            _ => panic!("Expected Set command"),
        }
    }

    #[test]
    fn test_encoded_size_accurate() {
        // Test that encoded_size returns the actual RESP byte count
        let cmd = ReplicationCommand::Set {
            key: Bytes::from("key"),
            value: Bytes::from("value"),
        };

        // Expected: *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n = 33 bytes
        let size = cmd.encoded_size();
        let bytes = cmd.to_bytes();
        assert_eq!(size as usize, bytes.len());
        assert_eq!(
            bytes.as_ref(),
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
        );
    }

    #[test]
    fn test_new_commands_to_frame() {
        // Test INCR
        let cmd = ReplicationCommand::Incr {
            key: Bytes::from("counter"),
        };
        let frame = cmd.to_frame();
        match frame {
            Frame::Array(Some(arr)) => {
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("Expected array"),
        }

        // Test LPOP with count
        let cmd = ReplicationCommand::LPop {
            key: Bytes::from("list"),
            count: Some(5),
        };
        let frame = cmd.to_frame();
        match frame {
            Frame::Array(Some(arr)) => {
                assert_eq!(arr.len(), 3); // LPOP, key, count
            }
            _ => panic!("Expected array"),
        }

        // Test HINCRBY
        let cmd = ReplicationCommand::HIncrBy {
            key: Bytes::from("hash"),
            field: Bytes::from("field"),
            delta: 10,
        };
        let size = cmd.encoded_size();
        let bytes = cmd.to_bytes();
        assert_eq!(size as usize, bytes.len());
    }

    #[test]
    fn test_all_commands_encode_correctly() {
        // Verify all command variants encode without panic
        let commands = vec![
            ReplicationCommand::Set {
                key: Bytes::from("k"),
                value: Bytes::from("v"),
            },
            ReplicationCommand::SetEx {
                key: Bytes::from("k"),
                value: Bytes::from("v"),
                expire_ms: 1000,
            },
            ReplicationCommand::SetNx {
                key: Bytes::from("k"),
                value: Bytes::from("v"),
            },
            ReplicationCommand::Incr {
                key: Bytes::from("k"),
            },
            ReplicationCommand::Decr {
                key: Bytes::from("k"),
            },
            ReplicationCommand::IncrBy {
                key: Bytes::from("k"),
                delta: 5,
            },
            ReplicationCommand::Del {
                keys: vec![Bytes::from("k1"), Bytes::from("k2")],
            },
            ReplicationCommand::LPush {
                key: Bytes::from("list"),
                values: vec![Bytes::from("v1"), Bytes::from("v2")],
            },
            ReplicationCommand::LPop {
                key: Bytes::from("list"),
                count: None,
            },
            ReplicationCommand::HSet {
                key: Bytes::from("hash"),
                pairs: vec![(Bytes::from("f1"), Bytes::from("v1"))],
            },
            ReplicationCommand::SAdd {
                key: Bytes::from("set"),
                members: vec![Bytes::from("m1")],
            },
            ReplicationCommand::ZAdd {
                key: Bytes::from("zset"),
                pairs: vec![(1.0, Bytes::from("m1"))],
            },
            ReplicationCommand::FlushDb { db: 0 },
            ReplicationCommand::FlushAll,
            ReplicationCommand::Select { db: 1 },
        ];

        for cmd in commands {
            let size = cmd.encoded_size();
            let bytes = cmd.to_bytes();
            assert_eq!(
                size as usize,
                bytes.len(),
                "Size mismatch for command: {:?}",
                cmd
            );
        }
    }
}
