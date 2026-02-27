#![allow(dead_code)]
//! Extended ACL command implementations
//!
//! Provides ACL LOG, ACL GENPASS, ACL CAT, and ACL DRYRUN.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;

use crate::protocol::Frame;

/// Maximum number of ACL log entries retained.
const MAX_ACL_LOG_ENTRIES: usize = 128;

/// A single ACL security event.
#[derive(Debug, Clone)]
pub struct AclLogEntry {
    pub count: u64,
    pub reason: String,
    pub context: String,
    pub object: String,
    pub username: String,
    pub age_seconds: f64,
    pub client_info: String,
    pub entry_id: u64,
    pub timestamp_created: u64,
}

/// Thread-safe ACL log that tracks denied commands.
#[derive(Debug)]
pub struct AclLog {
    entries: RwLock<Vec<AclLogEntry>>,
    next_id: RwLock<u64>,
}

impl AclLog {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(Vec::new()),
            next_id: RwLock::new(1),
        }
    }

    /// Record a denied command event.
    pub fn add_entry(
        &self,
        reason: &str,
        context: &str,
        object: &str,
        username: &str,
        client_info: &str,
    ) {
        let mut entries = self.entries.write();
        let mut next_id = self.next_id.write();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let entry = AclLogEntry {
            count: 1,
            reason: reason.to_string(),
            context: context.to_string(),
            object: object.to_string(),
            username: username.to_string(),
            age_seconds: 0.0,
            client_info: client_info.to_string(),
            entry_id: *next_id,
            timestamp_created: now,
        };
        *next_id += 1;

        entries.push(entry);
        if entries.len() > MAX_ACL_LOG_ENTRIES {
            entries.remove(0);
        }
    }

    /// Reset the ACL log, returning the number of cleared entries.
    pub fn reset(&self) -> usize {
        let mut entries = self.entries.write();
        let count = entries.len();
        entries.clear();
        count
    }

    /// Get log entries (most recent first), optionally limited by count.
    pub fn get_entries(&self, count: Option<usize>) -> Vec<AclLogEntry> {
        let entries = self.entries.read();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut result: Vec<AclLogEntry> = entries
            .iter()
            .rev()
            .map(|e| {
                let mut entry = e.clone();
                entry.age_seconds = (now - e.timestamp_created) as f64;
                entry
            })
            .collect();

        if let Some(c) = count {
            result.truncate(c);
        }
        result
    }
}

impl Default for AclLog {
    fn default() -> Self {
        Self::new()
    }
}

pub type SharedAclLog = Arc<AclLog>;

/// ACL LOG [RESET | count]
pub fn acl_log(acl_log: &SharedAclLog, args: &[String]) -> Frame {
    if !args.is_empty() {
        let arg = args[0].to_uppercase();
        if arg == "RESET" {
            acl_log.reset();
            return Frame::simple("OK");
        }
        // Try to parse as count
        if let Ok(count) = args[0].parse::<usize>() {
            let entries = acl_log.get_entries(Some(count));
            return entries_to_frame(&entries);
        }
        return Frame::error("ERR Invalid ACL LOG argument");
    }

    let entries = acl_log.get_entries(None);
    entries_to_frame(&entries)
}

fn entries_to_frame(entries: &[AclLogEntry]) -> Frame {
    let frames: Vec<Frame> = entries
        .iter()
        .map(|e| {
            Frame::array(vec![
                Frame::bulk("count"),
                Frame::Integer(e.count as i64),
                Frame::bulk("reason"),
                Frame::bulk(e.reason.clone()),
                Frame::bulk("context"),
                Frame::bulk(e.context.clone()),
                Frame::bulk("object"),
                Frame::bulk(e.object.clone()),
                Frame::bulk("username"),
                Frame::bulk(e.username.clone()),
                Frame::bulk("age-seconds"),
                Frame::bulk(format!("{:.3}", e.age_seconds)),
                Frame::bulk("client-info"),
                Frame::bulk(e.client_info.clone()),
                Frame::bulk("entry-id"),
                Frame::Integer(e.entry_id as i64),
                Frame::bulk("timestamp-created"),
                Frame::Integer(e.timestamp_created as i64),
            ])
        })
        .collect();
    Frame::array(frames)
}

/// ACL GENPASS [bits]
/// Generate a random hex password. Default 256 bits → 64 hex chars.
/// The argument specifies the output length in characters for compatibility.
pub fn acl_genpass(args: &[String]) -> Frame {
    use rand::Rng;

    let hex_chars: usize = if !args.is_empty() {
        match args[0].parse::<usize>() {
            Ok(n) if n > 0 => n,
            _ => return Frame::error("ERR ACL GENPASS argument must be a positive number"),
        }
    } else {
        64
    };

    let password: String = (0..hex_chars)
        .map(|_| {
            let idx: u8 = rand::thread_rng().gen_range(0..16);
            char::from(if idx < 10 { b'0' + idx } else { b'a' + idx - 10 })
        })
        .collect();

    Frame::bulk(password)
}

/// All Redis command categories
const CATEGORIES: &[&str] = &[
    "read",
    "write",
    "set",
    "sortedset",
    "list",
    "hash",
    "string",
    "bitmap",
    "hyperloglog",
    "geo",
    "stream",
    "pubsub",
    "admin",
    "fast",
    "slow",
    "blocking",
    "dangerous",
    "connection",
    "transaction",
    "scripting",
    "server",
    "generic",
    "keyspace",
];

/// Commands grouped by category (simplified mapping)
fn commands_in_category(category: &str) -> Vec<&'static str> {
    match category.to_lowercase().as_str() {
        "string" => vec![
            "APPEND", "DECR", "DECRBY", "GET", "GETDEL", "GETEX", "GETRANGE", "GETSET",
            "INCR", "INCRBY", "INCRBYFLOAT", "LCS", "MGET", "MSET", "MSETNX", "PSETEX",
            "SET", "SETEX", "SETNX", "SETRANGE", "STRLEN", "SUBSTR",
        ],
        "list" => vec![
            "BLMOVE", "BLPOP", "BRPOP", "BRPOPLPUSH", "LINDEX", "LINSERT", "LLEN",
            "LMOVE", "LPOP", "LPOS", "LPUSH", "LPUSHX", "LRANGE", "LREM", "LSET",
            "LTRIM", "RPOP", "RPOPLPUSH", "RPUSH", "RPUSHX",
        ],
        "set" => vec![
            "SADD", "SCARD", "SDIFF", "SDIFFSTORE", "SINTER", "SINTERCARD", "SINTERSTORE",
            "SISMEMBER", "SMEMBERS", "SMISMEMBER", "SMOVE", "SPOP", "SRANDMEMBER",
            "SREM", "SSCAN", "SUNION", "SUNIONSTORE",
        ],
        "sortedset" => vec![
            "BZMPOP", "BZPOPMAX", "BZPOPMIN", "ZADD", "ZCARD", "ZCOUNT", "ZDIFF",
            "ZDIFFSTORE", "ZINCRBY", "ZINTER", "ZINTERCARD", "ZINTERSTORE", "ZLEXCOUNT",
            "ZMPOP", "ZMSCORE", "ZPOPMAX", "ZPOPMIN", "ZRANDMEMBER", "ZRANGE",
            "ZRANGEBYLEX", "ZRANGEBYSCORE", "ZRANGESTORE", "ZRANK", "ZREM",
            "ZREMRANGEBYLEX", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE", "ZREVRANGE",
            "ZREVRANGEBYLEX", "ZREVRANGEBYSCORE", "ZREVRANK", "ZSCAN", "ZSCORE",
            "ZUNION", "ZUNIONSTORE",
        ],
        "hash" => vec![
            "HDEL", "HEXISTS", "HGET", "HGETALL", "HINCRBY", "HINCRBYFLOAT", "HKEYS",
            "HLEN", "HMGET", "HMSET", "HRANDFIELD", "HSCAN", "HSET", "HSETNX",
            "HSTRLEN", "HVALS",
        ],
        "bitmap" => vec!["BITCOUNT", "BITFIELD", "BITOP", "BITPOS", "GETBIT", "SETBIT"],
        "hyperloglog" => vec!["PFADD", "PFCOUNT", "PFMERGE"],
        "geo" => vec![
            "GEOADD", "GEODIST", "GEOHASH", "GEOPOS", "GEORADIUS",
            "GEORADIUSBYMEMBER", "GEOSEARCH", "GEOSEARCHSTORE",
        ],
        "stream" => vec![
            "XACK", "XADD", "XAUTOCLAIM", "XCLAIM", "XDEL", "XGROUP", "XINFO",
            "XLEN", "XPENDING", "XRANGE", "XREAD", "XREADGROUP", "XREVRANGE", "XTRIM",
        ],
        "pubsub" => vec![
            "PSUBSCRIBE", "PUBLISH", "PUBSUB", "PUNSUBSCRIBE", "SUBSCRIBE",
            "UNSUBSCRIBE",
        ],
        "admin" => vec![
            "ACL", "BGREWRITEAOF", "BGSAVE", "COMMAND", "CONFIG", "DBSIZE", "DEBUG",
            "FAILOVER", "FLUSHALL", "FLUSHDB", "INFO", "LASTSAVE", "LATENCY",
            "LOLWUT", "MEMORY", "MODULE", "MONITOR", "PSYNC", "REPLICAOF", "RESET",
            "SAVE", "SHUTDOWN", "SLAVEOF", "SLOWLOG", "SWAPDB",
        ],
        "fast" => vec![
            "EXISTS", "GET", "HGET", "HGETALL", "LINDEX", "LLEN", "SADD", "SCARD",
            "SISMEMBER", "TTL", "TYPE", "ZADD", "ZCARD", "ZSCORE",
        ],
        "slow" => vec![
            "FLUSHALL", "FLUSHDB", "KEYS", "SAVE", "SORT", "SMEMBERS", "SUNION",
        ],
        "blocking" => vec!["BLPOP", "BRPOP", "BLMOVE", "BRPOPLPUSH", "BZPOPMAX", "BZPOPMIN", "WAIT"],
        "dangerous" => vec![
            "ACL", "BGREWRITEAOF", "BGSAVE", "CONFIG", "DEBUG", "FLUSHALL",
            "FLUSHDB", "KEYS", "REPLICAOF", "RESTORE", "SAVE", "SHUTDOWN", "SLAVEOF",
            "SORT",
        ],
        "connection" => vec![
            "AUTH", "CLIENT", "ECHO", "HELLO", "PING", "QUIT", "RESET", "SELECT",
        ],
        "transaction" => vec!["DISCARD", "EXEC", "MULTI", "UNWATCH", "WATCH"],
        "scripting" => vec!["EVAL", "EVALSHA", "SCRIPT"],
        "server" => vec![
            "BGREWRITEAOF", "BGSAVE", "COMMAND", "CONFIG", "DBSIZE", "DEBUG",
            "FLUSHALL", "FLUSHDB", "INFO", "LASTSAVE", "SAVE", "SHUTDOWN", "SLOWLOG",
            "TIME",
        ],
        "generic" | "keyspace" => vec![
            "COPY", "DEL", "DUMP", "EXISTS", "EXPIRE", "EXPIREAT", "EXPIRETIME",
            "KEYS", "MOVE", "OBJECT", "PERSIST", "PEXPIRE", "PEXPIREAT",
            "PEXPIRETIME", "PTTL", "RANDOMKEY", "RENAME", "RENAMENX", "RESTORE",
            "SCAN", "SORT", "TOUCH", "TTL", "TYPE", "UNLINK", "WAIT",
        ],
        "read" => vec![
            "DBSIZE", "EXISTS", "GET", "GETRANGE", "HEXISTS", "HGET", "HGETALL",
            "HKEYS", "HLEN", "HMGET", "HVALS", "KEYS", "LINDEX", "LLEN", "LRANGE",
            "MGET", "OBJECT", "PTTL", "RANDOMKEY", "SCARD", "SDIFF", "SINTER",
            "SISMEMBER", "SMEMBERS", "SORT", "STRLEN", "SUNION", "TTL", "TYPE",
            "ZCARD", "ZCOUNT", "ZLEXCOUNT", "ZRANGE", "ZRANGEBYLEX",
            "ZRANGEBYSCORE", "ZRANK", "ZREVRANGE", "ZREVRANGEBYLEX",
            "ZREVRANGEBYSCORE", "ZREVRANK", "ZSCORE",
        ],
        "write" => vec![
            "APPEND", "DECR", "DECRBY", "DEL", "EXPIRE", "EXPIREAT", "FLUSHALL",
            "FLUSHDB", "HDEL", "HINCRBY", "HINCRBYFLOAT", "HMSET", "HSET", "HSETNX",
            "INCR", "INCRBY", "INCRBYFLOAT", "LINSERT", "LMOVE", "LPOP", "LPUSH",
            "LPUSHX", "LREM", "LSET", "LTRIM", "MSET", "MSETNX", "PERSIST",
            "PEXPIRE", "PEXPIREAT", "PSETEX", "RENAME", "RENAMENX", "RPOP",
            "RPOPLPUSH", "RPUSH", "RPUSHX", "SADD", "SDIFFSTORE", "SET", "SETEX",
            "SETNX", "SETRANGE", "SINTERSTORE", "SMOVE", "SPOP", "SREM",
            "SUNIONSTORE", "UNLINK", "ZADD", "ZDIFFSTORE", "ZINCRBY",
            "ZINTERSTORE", "ZPOPMAX", "ZPOPMIN", "ZRANGESTORE", "ZREM",
            "ZREMRANGEBYLEX", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE", "ZUNIONSTORE",
        ],
        _ => vec![],
    }
}

/// ACL CAT [category]
pub fn acl_cat(args: &[String]) -> Frame {
    if args.is_empty() {
        // List all categories
        let frames: Vec<Frame> = CATEGORIES.iter().map(|c| Frame::bulk(*c)).collect();
        Frame::array(frames)
    } else {
        let category = &args[0];
        let commands = commands_in_category(category);
        if commands.is_empty() && !CATEGORIES.contains(&category.to_lowercase().as_str()) {
            return Frame::error(format!(
                "ERR Unknown ACL cat category '{}'",
                category
            ));
        }
        let frames: Vec<Frame> = commands.into_iter().map(Frame::bulk).collect();
        Frame::array(frames)
    }
}

/// ACL DRYRUN username command [arg ...]
/// Tests whether a user can execute a command without running it.
pub fn acl_dryrun(args: &[String]) -> Frame {
    if args.len() < 2 {
        return Frame::error("ERR wrong number of arguments for 'acl|dryrun' command");
    }

    let _username = &args[0];
    let _command = &args[1];

    // In a full implementation, we'd check the ACL rules for the user.
    // For now, the default user has full access.
    Frame::simple("OK")
}
