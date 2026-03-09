//! Redis-compatible keyspace notifications.
//!
//! When enabled (via `CONFIG SET notify-keyspace-events`), mutations publish
//! notification messages to two families of pub/sub channels:
//!
//! - `__keyspace@<db>__:<key>` — receives the event name (e.g., "set", "del")
//! - `__keyevent@<db>__:<event>` — receives the key name
//!
//! The notification flags follow the Redis convention:
//!
//! | Flag | Meaning |
//! |------|---------|
//! | K    | Keyspace events (`__keyspace@<db>__:` prefix) |
//! | E    | Keyevent events (`__keyevent@<db>__:` prefix) |
//! | g    | Generic commands: DEL, EXPIRE, RENAME, ... |
//! | $    | String commands |
//! | l    | List commands |
//! | s    | Set commands |
//! | h    | Hash commands |
//! | z    | Sorted set commands |
//! | x    | Expired events |
//! | e    | Evicted events |
//! | t    | Stream commands |
//! | m    | Key miss events |
//! | A    | Alias for "g$lshzxet" (all events) |

use bytes::Bytes;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::runtime::SubscriptionManager;

/// Notification event type flags (bitmask)
const FLAG_K: u32 = 1 << 0; // Keyspace
const FLAG_E: u32 = 1 << 1; // Keyevent
const FLAG_G: u32 = 1 << 2; // Generic
const FLAG_STRING: u32 = 1 << 3; // $ — String commands
const FLAG_LIST: u32 = 1 << 4; // l — List commands
const FLAG_SET: u32 = 1 << 5; // s — Set commands
const FLAG_HASH: u32 = 1 << 6; // h — Hash commands
const FLAG_ZSET: u32 = 1 << 7; // z — Sorted set commands
const FLAG_EXPIRED: u32 = 1 << 8; // x — Expired
const FLAG_EVICTED: u32 = 1 << 9; // e — Evicted
const FLAG_STREAM: u32 = 1 << 10; // t — Stream
const FLAG_KEYMISS: u32 = 1 << 11; // m — Key miss

const FLAG_ALL: u32 = FLAG_G
    | FLAG_STRING
    | FLAG_LIST
    | FLAG_SET
    | FLAG_HASH
    | FLAG_ZSET
    | FLAG_EXPIRED
    | FLAG_EVICTED
    | FLAG_STREAM;

/// Categorizes a Redis command name into its notification flag.
fn event_flag_for_command(event: &str) -> u32 {
    match event {
        // Generic commands
        "del" | "unlink" | "expire" | "expireat" | "pexpire" | "pexpireat" | "rename"
        | "rename_to" | "rename_from" | "persist" | "copy_to" | "restore" | "move" => FLAG_G,
        // String commands
        "set" | "setex" | "psetex" | "setnx" | "getset" | "mset" | "msetnx" | "setrange"
        | "incr" | "decr" | "incrby" | "decrby" | "incrbyfloat" | "append" | "getdel" | "getex" => {
            FLAG_STRING
        }
        // List commands
        "lpush" | "rpush" | "rpoplpush" | "linsert" | "lset" | "lrem" | "ltrim" | "lpop"
        | "rpop" | "lmove" | "lmpop" => FLAG_LIST,
        // Set commands
        "sadd" | "srem" | "spop" | "sinterstore" | "sunionstore" | "sdiffstore" | "smove" => {
            FLAG_SET
        }
        // Hash commands
        "hset" | "hincrby" | "hincrbyfloat" | "hdel" | "hmset" | "hsetnx" => FLAG_HASH,
        // Sorted set commands
        "zadd" | "zincrby" | "zrem" | "zrangestore" | "zinterstore" | "zunionstore"
        | "zdiffstore" | "zpopmin" | "zpopmax" | "bzpopmin" | "bzpopmax" | "zmpop" => FLAG_ZSET,
        // Stream commands
        "xadd" | "xtrim" | "xdel" | "xgroup-create" | "xgroup-delconsumer" | "xgroup-destroy"
        | "xgroup-setid" | "xclaim" | "xautoclaim" => FLAG_STREAM,
        // Lifecycle events
        "expired" => FLAG_EXPIRED,
        "evicted" => FLAG_EVICTED,
        "keymiss" => FLAG_KEYMISS,
        // JSON commands map to string
        "json.set" | "json.del" | "json.numincrby" => FLAG_STRING,
        // Bloom filter commands map to set
        "bf.add" | "bf.madd" | "bf.reserve" => FLAG_SET,
        _ => 0,
    }
}

/// Manages keyspace notification configuration and dispatch.
pub struct KeyspaceNotifier {
    /// Bitmask of enabled notification types
    flags: AtomicU32,
}

impl KeyspaceNotifier {
    /// Create a new notifier with notifications disabled (Redis default).
    pub fn new() -> Self {
        Self {
            flags: AtomicU32::new(0),
        }
    }

    /// Parse a Redis `notify-keyspace-events` config string into the flag bitmask.
    ///
    /// Returns `None` if the string is invalid.
    pub fn parse_flags(config: &str) -> Option<u32> {
        if config.is_empty() {
            return Some(0);
        }

        let mut flags = 0u32;
        for ch in config.chars() {
            match ch {
                'K' => flags |= FLAG_K,
                'E' => flags |= FLAG_E,
                'g' => flags |= FLAG_G,
                '$' => flags |= FLAG_STRING,
                'l' => flags |= FLAG_LIST,
                's' => flags |= FLAG_SET,
                'h' => flags |= FLAG_HASH,
                'z' => flags |= FLAG_ZSET,
                'x' => flags |= FLAG_EXPIRED,
                'e' => flags |= FLAG_EVICTED,
                't' => flags |= FLAG_STREAM,
                'm' => flags |= FLAG_KEYMISS,
                'A' => flags |= FLAG_ALL,
                _ => return None,
            }
        }

        // If neither K nor E is set, notifications are disabled (Redis behavior)
        if flags & (FLAG_K | FLAG_E) == 0 {
            return Some(0);
        }

        Some(flags)
    }

    /// Format the current flags back to the Redis config string.
    pub fn flags_to_string(flags: u32) -> String {
        if flags == 0 {
            return String::new();
        }
        let mut s = String::with_capacity(12);
        if flags & FLAG_K != 0 {
            s.push('K');
        }
        if flags & FLAG_E != 0 {
            s.push('E');
        }
        if flags & FLAG_G != 0 {
            s.push('g');
        }
        if flags & FLAG_STRING != 0 {
            s.push('$');
        }
        if flags & FLAG_LIST != 0 {
            s.push('l');
        }
        if flags & FLAG_SET != 0 {
            s.push('s');
        }
        if flags & FLAG_HASH != 0 {
            s.push('h');
        }
        if flags & FLAG_ZSET != 0 {
            s.push('z');
        }
        if flags & FLAG_EXPIRED != 0 {
            s.push('x');
        }
        if flags & FLAG_EVICTED != 0 {
            s.push('e');
        }
        if flags & FLAG_STREAM != 0 {
            s.push('t');
        }
        if flags & FLAG_KEYMISS != 0 {
            s.push('m');
        }
        s
    }

    /// Update the notification flags from a config string.
    pub fn set_flags(&self, config: &str) -> Result<(), String> {
        match Self::parse_flags(config) {
            Some(flags) => {
                self.flags.store(flags, Ordering::Relaxed);
                Ok(())
            }
            None => Err(format!("Invalid notify-keyspace-events value: {}", config)),
        }
    }

    /// Get the current flags as a config string.
    pub fn get_flags(&self) -> String {
        Self::flags_to_string(self.flags.load(Ordering::Relaxed))
    }

    /// Check if any notifications are enabled.
    pub fn is_enabled(&self) -> bool {
        self.flags.load(Ordering::Relaxed) != 0
    }

    /// Notify about a key event if the corresponding notification type is enabled.
    ///
    /// `event` is the lowercase command/event name (e.g., "set", "del", "expired").
    /// `key` is the affected key. `db` is the database index.
    pub fn notify(&self, pubsub: &Arc<SubscriptionManager>, db: u8, event: &str, key: &Bytes) {
        let flags = self.flags.load(Ordering::Relaxed);
        if flags == 0 {
            return;
        }

        let event_flag = event_flag_for_command(event);
        if event_flag == 0 || flags & event_flag == 0 {
            return;
        }

        // __keyspace@<db>__:<key> → event name
        if flags & FLAG_K != 0 {
            let channel = Bytes::from(format!(
                "__keyspace@{}__:{}",
                db,
                String::from_utf8_lossy(key)
            ));
            let message = Bytes::from(event.to_string());
            pubsub.publish(&channel, &message);
        }

        // __keyevent@<db>__:<event> → key name
        if flags & FLAG_E != 0 {
            let channel = Bytes::from(format!("__keyevent@{}__:{}", db, event));
            pubsub.publish(&channel, key);
        }
    }
}

impl Default for KeyspaceNotifier {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared keyspace notifier type
pub type SharedKeyspaceNotifier = Arc<KeyspaceNotifier>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_flags_empty() {
        assert_eq!(KeyspaceNotifier::parse_flags(""), Some(0));
    }

    #[test]
    fn test_parse_flags_disabled_without_ke() {
        // Without K or E, notifications are disabled
        assert_eq!(KeyspaceNotifier::parse_flags("g$l"), Some(0));
    }

    #[test]
    fn test_parse_flags_all() {
        let flags = KeyspaceNotifier::parse_flags("KEA").unwrap();
        assert_ne!(flags & FLAG_K, 0);
        assert_ne!(flags & FLAG_E, 0);
        assert_ne!(flags & FLAG_G, 0);
        assert_ne!(flags & FLAG_STRING, 0);
    }

    #[test]
    fn test_parse_flags_specific() {
        let flags = KeyspaceNotifier::parse_flags("Kg$").unwrap();
        assert_ne!(flags & FLAG_K, 0);
        assert_ne!(flags & FLAG_G, 0);
        assert_ne!(flags & FLAG_STRING, 0);
        assert_eq!(flags & FLAG_E, 0);
        assert_eq!(flags & FLAG_LIST, 0);
    }

    #[test]
    fn test_parse_flags_invalid() {
        assert!(KeyspaceNotifier::parse_flags("KQ").is_none());
    }

    #[test]
    fn test_roundtrip() {
        let original = "KEg$lshzxet";
        let flags = KeyspaceNotifier::parse_flags(original).unwrap();
        let output = KeyspaceNotifier::flags_to_string(flags);
        // All characters should be present
        for ch in original.chars() {
            assert!(output.contains(ch), "missing char: {}", ch);
        }
    }

    #[test]
    fn test_event_flag_mapping() {
        assert_eq!(event_flag_for_command("set"), FLAG_STRING);
        assert_eq!(event_flag_for_command("del"), FLAG_G);
        assert_eq!(event_flag_for_command("lpush"), FLAG_LIST);
        assert_eq!(event_flag_for_command("sadd"), FLAG_SET);
        assert_eq!(event_flag_for_command("hset"), FLAG_HASH);
        assert_eq!(event_flag_for_command("zadd"), FLAG_ZSET);
        assert_eq!(event_flag_for_command("xadd"), FLAG_STREAM);
        assert_eq!(event_flag_for_command("expired"), FLAG_EXPIRED);
        assert_eq!(event_flag_for_command("evicted"), FLAG_EVICTED);
    }

    #[test]
    fn test_set_and_get_flags() {
        let notifier = KeyspaceNotifier::new();
        assert!(!notifier.is_enabled());

        notifier.set_flags("KEg").unwrap();
        assert!(notifier.is_enabled());
        let config = notifier.get_flags();
        assert!(config.contains('K'));
        assert!(config.contains('E'));
        assert!(config.contains('g'));
    }
}
