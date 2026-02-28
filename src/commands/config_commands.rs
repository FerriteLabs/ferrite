#![allow(dead_code)]
//! Extended CONFIG command support
//!
//! Provides CONFIG RESETSTAT, CONFIG REWRITE, extended CONFIG SET parameters,
//! and COMMAND sub-commands (COUNT, DOCS, GETKEYS, LIST).

use crate::protocol::Frame;

/// CONFIG RESETSTAT — reset internal statistics counters.
pub fn config_resetstat() -> Frame {
    // Acknowledged; counters would be reset in a full implementation.
    Frame::simple("OK")
}

/// CONFIG REWRITE — rewrite the configuration file with in-memory values.
///
/// Stub: always returns OK since Ferrite does not yet persist config edits.
pub fn config_rewrite() -> Frame {
    Frame::simple("OK")
}

/// CONFIG SET for additional parameters not covered by the main handler.
///
/// Supports: `hz`, `lfu-log-factor`, `lfu-decay-time`, `activedefrag`,
/// `lazyfree-lazy-expire`.
pub fn config_set_extended(param: &str, value: &str) -> Frame {
    match param {
        "hz" | "lfu-log-factor" | "lfu-decay-time" => {
            // Validate it is a valid integer
            if value.parse::<u64>().is_err() {
                return Frame::error(format!(
                    "ERR Invalid argument '{value}' for CONFIG SET '{param}'"
                ));
            }
            Frame::simple("OK")
        }
        "activedefrag" | "lazyfree-lazy-expire" => {
            match value.to_lowercase().as_str() {
                "yes" | "no" => Frame::simple("OK"),
                _ => Frame::error(format!(
                    "ERR Invalid argument '{value}' for CONFIG SET '{param}'"
                )),
            }
        }
        _ => Frame::error(format!(
            "ERR Unsupported CONFIG parameter: {param}"
        )),
    }
}

/// COMMAND COUNT — return the total number of commands supported.
///
/// Returns an approximate count; this value should be updated as new
/// commands are added.
pub fn command_count() -> Frame {
    // Approximate count of implemented commands
    Frame::Integer(200)
}

/// COMMAND DOCS [command ...] — return documentation for commands.
///
/// Returns an array of `[command-name, doc-map]` pairs. When no arguments
/// are given, returns docs for all known commands (stub returns empty).
pub fn command_docs(args: &[String]) -> Frame {
    if args.is_empty() {
        return Frame::array(vec![]);
    }

    let mut results: Vec<Frame> = Vec::new();
    for cmd in args {
        let name = cmd.to_uppercase();
        results.push(Frame::bulk(name.clone()));
        // Minimal doc stub per command
        results.push(Frame::array(vec![
            Frame::bulk("summary"),
            Frame::bulk(format!("The {name} command")),
            Frame::bulk("since"),
            Frame::bulk("1.0.0"),
            Frame::bulk("group"),
            Frame::bulk("generic"),
            Frame::bulk("complexity"),
            Frame::bulk("O(1)"),
        ]));
    }
    Frame::array(results)
}

/// COMMAND GETKEYS command [arg ...] — return the keys for a given command.
///
/// Returns an array of keys that the command would access. This is a
/// simplified implementation that handles common patterns.
pub fn command_getkeys(args: &[String]) -> Frame {
    if args.is_empty() {
        return Frame::error("ERR Invalid arguments for 'command|getkeys'");
    }

    let cmd = args[0].to_uppercase();
    let keys: Vec<Frame> = match cmd.as_str() {
        // Single-key commands: key is args[1]
        "GET" | "SET" | "DEL" | "EXISTS" | "TYPE" | "TTL" | "PTTL" | "EXPIRE" | "PEXPIRE"
        | "PERSIST" | "INCR" | "DECR" | "INCRBY" | "DECRBY" | "INCRBYFLOAT" | "APPEND"
        | "STRLEN" | "GETRANGE" | "SETRANGE" | "SETNX" | "GETSET" | "LPUSH" | "RPUSH"
        | "LPOP" | "RPOP" | "LLEN" | "LRANGE" | "LINDEX" | "LSET" | "SADD" | "SREM"
        | "SCARD" | "SMEMBERS" | "SISMEMBER" | "SRANDMEMBER" | "SPOP" | "ZADD" | "ZREM"
        | "ZSCORE" | "ZCARD" | "ZRANGE" | "ZRANK" | "HSET" | "HGET" | "HDEL" | "HLEN"
        | "HGETALL" | "HKEYS" | "HVALS" | "XADD" | "XLEN" | "XRANGE" | "XREVRANGE"
        | "XREAD" | "XINFO" | "XDEL" | "XTRIM" => {
            if args.len() > 1 {
                vec![Frame::bulk(args[1].clone())]
            } else {
                vec![]
            }
        }
        // Multi-key commands: MGET key [key ...]
        "MGET" => args[1..].iter().map(|k| Frame::bulk(k.clone())).collect(),
        // MSET key value [key value ...]
        "MSET" | "MSETNX" => args[1..]
            .chunks(2)
            .filter_map(|chunk| chunk.first().map(|k| Frame::bulk(k.clone())))
            .collect(),
        _ => {
            // Default: first argument is the key
            if args.len() > 1 {
                vec![Frame::bulk(args[1].clone())]
            } else {
                vec![]
            }
        }
    };

    Frame::array(keys)
}

/// COMMAND LIST [FILTERBY MODULE|ACLCAT|PATTERN filter] — list commands.
///
/// Returns an array of command names. When a filter is provided, results
/// are narrowed accordingly (stub returns all known commands regardless
/// of filter).
pub fn command_list(args: &[String]) -> Frame {
    // Known commands (representative subset)
    let commands = vec![
        "ping", "echo", "quit", "select", "auth", "dbsize", "flushdb", "flushall",
        "get", "set", "del", "exists", "type", "keys", "scan", "ttl", "pttl",
        "expire", "pexpire", "expireat", "pexpireat", "persist", "rename", "renamenx",
        "randomkey", "dump", "restore", "object", "wait", "touch", "unlink",
        "append", "incr", "decr", "incrby", "decrby", "incrbyfloat", "strlen",
        "getrange", "setrange", "setnx", "setex", "psetex", "mget", "mset", "msetnx",
        "getset", "getdel", "getex",
        "lpush", "rpush", "lpop", "rpop", "llen", "lrange", "lindex", "lset",
        "linsert", "lrem", "ltrim", "lpos", "lmove", "rpoplpush",
        "sadd", "srem", "scard", "smembers", "sismember", "srandmember", "spop",
        "sunion", "sunionstore", "sinter", "sinterstore", "sintercard", "sdiff", "sdiffstore",
        "smove",
        "zadd", "zrem", "zscore", "zcard", "zrange", "zrangebyscore", "zrangebylex",
        "zrevrange", "zrevrangebyscore", "zrevrangebylex", "zrank", "zrevrank",
        "zincrby", "zcount", "zlexcount", "zrangestore", "zunionstore", "zinterstore",
        "zdiffstore", "zmscore", "zpopmin", "zpopmax",
        "hset", "hget", "hdel", "hlen", "hgetall", "hkeys", "hvals", "hexists",
        "hincrby", "hincrbyfloat", "hmset", "hmget", "hsetnx", "hrandfield", "hscan",
        "pfadd", "pfcount", "pfmerge",
        "xadd", "xlen", "xrange", "xrevrange", "xread", "xinfo", "xdel", "xtrim",
        "xgroup", "xreadgroup", "xack", "xclaim", "xautoclaim", "xpending",
        "subscribe", "unsubscribe", "publish", "psubscribe", "punsubscribe",
        "multi", "exec", "discard", "watch", "unwatch",
        "config", "info", "slowlog", "debug", "command", "client", "acl",
        "cluster", "latency", "memory", "module",
        "eval", "evalsha", "script",
        "bitcount", "bitfield", "bitop", "bitpos", "getbit", "setbit",
        "geoadd", "geodist", "geohash", "geopos", "georadius", "georadiusbymember",
        "geosearch", "geosearchstore",
        "copy", "sort", "sort_ro", "object",
        "blpop", "brpop", "blmove", "brpoplpush",
        "bzpopmin", "bzpopmax",
    ];

    let filter_pattern = if args.len() >= 2 {
        let filter_type = args[0].to_uppercase();
        if filter_type == "FILTERBY" && args.len() >= 4 && args[1].to_uppercase() == "PATTERN" {
            Some(args[2].clone())
        } else {
            None
        }
    } else {
        None
    };

    let result: Vec<Frame> = commands
        .iter()
        .filter(|cmd| {
            if let Some(ref pat) = filter_pattern {
                glob_match(pat, cmd)
            } else {
                true
            }
        })
        .map(|cmd| Frame::bulk(*cmd))
        .collect();

    Frame::array(result)
}

/// Simple glob-style pattern matching (supports `*` and `?`).
fn glob_match(pattern: &str, value: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let val: Vec<char> = value.chars().collect();
    glob_match_inner(&pat, &val)
}

/// Recursive glob matcher.
fn glob_match_inner(pat: &[char], val: &[char]) -> bool {
    if pat.is_empty() {
        return val.is_empty();
    }
    match pat[0] {
        '*' => {
            // Try matching rest of pattern at every position
            for i in 0..=val.len() {
                if glob_match_inner(&pat[1..], &val[i..]) {
                    return true;
                }
            }
            false
        }
        '?' => {
            if val.is_empty() {
                false
            } else {
                glob_match_inner(&pat[1..], &val[1..])
            }
        }
        c => {
            if val.is_empty() || val[0] != c {
                false
            } else {
                glob_match_inner(&pat[1..], &val[1..])
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_resetstat() {
        match config_resetstat() {
            Frame::Simple(s) => assert_eq!(s.as_ref(), b"OK"),
            _ => panic!("expected simple OK"),
        }
    }

    #[test]
    fn test_config_rewrite() {
        match config_rewrite() {
            Frame::Simple(s) => assert_eq!(s.as_ref(), b"OK"),
            _ => panic!("expected simple OK"),
        }
    }

    #[test]
    fn test_config_set_hz_valid() {
        match config_set_extended("hz", "10") {
            Frame::Simple(s) => assert_eq!(s.as_ref(), b"OK"),
            _ => panic!("expected simple OK"),
        }
    }

    #[test]
    fn test_config_set_hz_invalid() {
        match config_set_extended("hz", "notanumber") {
            Frame::Error(_) => {} // expected
            _ => panic!("expected error"),
        }
    }

    #[test]
    fn test_config_set_activedefrag() {
        match config_set_extended("activedefrag", "yes") {
            Frame::Simple(s) => assert_eq!(s.as_ref(), b"OK"),
            _ => panic!("expected simple OK"),
        }
    }

    #[test]
    fn test_config_set_unsupported() {
        match config_set_extended("unknown-param", "val") {
            Frame::Error(_) => {} // expected
            _ => panic!("expected error"),
        }
    }

    #[test]
    fn test_command_count() {
        match command_count() {
            Frame::Integer(n) => assert!(n > 0),
            _ => panic!("expected integer"),
        }
    }

    #[test]
    fn test_command_docs_empty() {
        match command_docs(&[]) {
            Frame::Array(Some(v)) => assert!(v.is_empty()),
            _ => panic!("expected empty array"),
        }
    }

    #[test]
    fn test_command_docs_with_arg() {
        let args = vec!["GET".to_string()];
        match command_docs(&args) {
            Frame::Array(Some(v)) => assert!(!v.is_empty()),
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_command_getkeys_get() {
        let args = vec!["GET".to_string(), "mykey".to_string()];
        match command_getkeys(&args) {
            Frame::Array(Some(v)) => assert_eq!(v.len(), 1),
            _ => panic!("expected array with one key"),
        }
    }

    #[test]
    fn test_command_getkeys_mget() {
        let args = vec![
            "MGET".to_string(),
            "k1".to_string(),
            "k2".to_string(),
            "k3".to_string(),
        ];
        match command_getkeys(&args) {
            Frame::Array(Some(v)) => assert_eq!(v.len(), 3),
            _ => panic!("expected array with three keys"),
        }
    }

    #[test]
    fn test_command_list() {
        match command_list(&[]) {
            Frame::Array(Some(v)) => assert!(!v.is_empty()),
            _ => panic!("expected non-empty array"),
        }
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("get*", "getset"));
        assert!(glob_match("*set", "getset"));
        assert!(glob_match("g?t", "get"));
        assert!(!glob_match("g?t", "gest"));
    }
}
