//! Ferrite WASM Playground
//!
//! Compiles Ferrite to WebAssembly for browser-based interactive use.
//! Provides a JavaScript-friendly API for executing Redis commands.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::Instant;

/// Statistics about the playground instance.
#[derive(Clone, Debug, Default)]
pub struct PlaygroundStats {
    /// Number of keys currently stored.
    pub keys_count: u64,
    /// Total number of commands executed.
    pub commands_executed: u64,
    /// Estimated memory usage in bytes.
    pub memory_bytes: u64,
    /// Milliseconds since the instance was created.
    pub uptime_ms: u64,
}

/// Internal value representation for the playground store.
#[derive(Clone, Debug)]
enum StoredValue {
    String(String),
    List(VecDeque<String>),
    Hash(HashMap<String, String>),
    Set(HashSet<String>),
    SortedSet(BTreeMap<String, f64>),
}

/// A self-contained, in-memory Ferrite instance for browser-based use.
///
/// Supports a subset of Redis commands with redis-cli–style output formatting.
pub struct PlaygroundInstance {
    data: HashMap<String, StoredValue>,
    expiries: HashMap<String, Instant>,
    commands_executed: u64,
    created_at: Instant,
}

impl PlaygroundInstance {
    /// Create a new, empty playground instance.
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            expiries: HashMap::new(),
            commands_executed: 0,
            created_at: Instant::now(),
        }
    }

    /// Parse and execute a single Redis command, returning a redis-cli–style response.
    pub fn execute_command(&mut self, input: &str) -> String {
        self.commands_executed += 1;
        self.evict_expired();

        let tokens = parse_tokens(input);
        if tokens.is_empty() {
            return "(error) ERR no command given".to_string();
        }

        let cmd = tokens[0].to_uppercase();
        let args: Vec<&str> = tokens[1..].iter().map(|s| s.as_str()).collect();

        match cmd.as_str() {
            "PING" => self.cmd_ping(&args),
            "ECHO" => self.cmd_echo(&args),
            "SET" => self.cmd_set(&args),
            "GET" => self.cmd_get(&args),
            "DEL" => self.cmd_del(&args),
            "KEYS" => self.cmd_keys(&args),
            "EXPIRE" => self.cmd_expire(&args),
            "TTL" => self.cmd_ttl(&args),
            "LPUSH" => self.cmd_lpush(&args),
            "RPUSH" => self.cmd_rpush(&args),
            "LPOP" => self.cmd_lpop(&args),
            "RPOP" => self.cmd_rpop(&args),
            "LRANGE" => self.cmd_lrange(&args),
            "LLEN" => self.cmd_llen(&args),
            "HSET" => self.cmd_hset(&args),
            "HGET" => self.cmd_hget(&args),
            "HGETALL" => self.cmd_hgetall(&args),
            "HDEL" => self.cmd_hdel(&args),
            "SADD" => self.cmd_sadd(&args),
            "SMEMBERS" => self.cmd_smembers(&args),
            "SCARD" => self.cmd_scard(&args),
            "ZADD" => self.cmd_zadd(&args),
            "ZRANGE" => self.cmd_zrange(&args),
            "ZSCORE" => self.cmd_zscore(&args),
            "DBSIZE" => self.cmd_dbsize(),
            "FLUSHDB" => self.cmd_flushdb(),
            "INFO" => self.cmd_info(),
            "HELP" => self.cmd_help(),
            _ => format!("(error) ERR unknown command '{cmd}'"),
        }
    }

    /// Execute multiple commands, returning one response per command.
    pub fn execute_batch(&mut self, commands: &[&str]) -> Vec<String> {
        commands
            .iter()
            .map(|cmd| self.execute_command(cmd))
            .collect()
    }

    /// Clear all data and reset the instance.
    pub fn reset(&mut self) {
        self.data.clear();
        self.expiries.clear();
        self.commands_executed = 0;
        self.created_at = Instant::now();
    }

    /// Return current statistics.
    pub fn stats(&self) -> PlaygroundStats {
        let memory_bytes = self.estimate_memory();
        PlaygroundStats {
            keys_count: self.data.len() as u64,
            commands_executed: self.commands_executed,
            memory_bytes,
            uptime_ms: self.created_at.elapsed().as_millis() as u64,
        }
    }

    /// Pre-load sample data useful for tutorials and demos.
    pub fn load_tutorial_data(&mut self) {
        // 5 string keys
        self.data
            .insert("user:1".into(), StoredValue::String("Alice".into()));
        self.data
            .insert("user:2".into(), StoredValue::String("Bob".into()));
        self.data
            .insert("user:3".into(), StoredValue::String("Charlie".into()));
        self.data
            .insert("user:4".into(), StoredValue::String("Diana".into()));
        self.data
            .insert("user:5".into(), StoredValue::String("Eve".into()));

        // 1 list (queue:tasks with 3 items)
        self.data.insert(
            "queue:tasks".into(),
            StoredValue::List(VecDeque::from([
                "send-email".into(),
                "resize-image".into(),
                "generate-report".into(),
            ])),
        );

        // 1 hash (config:app with 4 fields)
        let mut app_config = HashMap::new();
        app_config.insert("name".into(), "Ferrite Demo".into());
        app_config.insert("version".into(), "0.1.0".into());
        app_config.insert("max_connections".into(), "100".into());
        app_config.insert("timeout_ms".into(), "5000".into());
        self.data
            .insert("config:app".into(), StoredValue::Hash(app_config));

        // 1 set (tags:popular with 5 members)
        let mut tags = HashSet::new();
        tags.insert("rust".into());
        tags.insert("database".into());
        tags.insert("cache".into());
        tags.insert("performance".into());
        tags.insert("open-source".into());
        self.data
            .insert("tags:popular".into(), StoredValue::Set(tags));

        // 1 sorted set (leaderboard with 5 scores)
        let mut leaderboard = BTreeMap::new();
        leaderboard.insert("alice".into(), 1500.0);
        leaderboard.insert("bob".into(), 1200.0);
        leaderboard.insert("charlie".into(), 1800.0);
        leaderboard.insert("diana".into(), 900.0);
        leaderboard.insert("eve".into(), 2100.0);
        self.data
            .insert("leaderboard".into(), StoredValue::SortedSet(leaderboard));
    }

    // ── Expiry helpers ───────────────────────────────────────────────────────

    fn evict_expired(&mut self) {
        let now = Instant::now();
        let expired: Vec<String> = self
            .expiries
            .iter()
            .filter(|(_, &exp)| now >= exp)
            .map(|(k, _)| k.clone())
            .collect();
        for key in expired {
            self.data.remove(&key);
            self.expiries.remove(&key);
        }
    }

    fn estimate_memory(&self) -> u64 {
        let mut bytes: u64 = 0;
        for (k, v) in &self.data {
            bytes += k.len() as u64;
            match v {
                StoredValue::String(s) => bytes += s.len() as u64,
                StoredValue::List(l) => {
                    for item in l {
                        bytes += item.len() as u64;
                    }
                }
                StoredValue::Hash(h) => {
                    for (hk, hv) in h {
                        bytes += (hk.len() + hv.len()) as u64;
                    }
                }
                StoredValue::Set(s) => {
                    for m in s {
                        bytes += m.len() as u64;
                    }
                }
                StoredValue::SortedSet(z) => {
                    for (m, _) in z {
                        bytes += m.len() as u64 + 8;
                    }
                }
            }
        }
        bytes
    }

    // ── Command implementations ──────────────────────────────────────────────

    fn cmd_ping(&self, args: &[&str]) -> String {
        if args.is_empty() {
            "PONG".to_string()
        } else {
            format!("\"{}\"", args.join(" "))
        }
    }

    fn cmd_echo(&self, args: &[&str]) -> String {
        if args.is_empty() {
            "(error) ERR wrong number of arguments for 'echo' command".to_string()
        } else {
            format!("\"{}\"", args.join(" "))
        }
    }

    fn cmd_set(&mut self, args: &[&str]) -> String {
        if args.len() < 2 {
            return "(error) ERR wrong number of arguments for 'set' command".to_string();
        }
        let key = args[0].to_string();
        let value = args[1].to_string();
        self.data.insert(key, StoredValue::String(value));
        "OK".to_string()
    }

    fn cmd_get(&self, args: &[&str]) -> String {
        if args.len() != 1 {
            return "(error) ERR wrong number of arguments for 'get' command".to_string();
        }
        match self.data.get(args[0]) {
            Some(StoredValue::String(s)) => format!("\"{s}\""),
            Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value"
                .to_string(),
            None => "(nil)".to_string(),
        }
    }

    fn cmd_del(&mut self, args: &[&str]) -> String {
        if args.is_empty() {
            return "(error) ERR wrong number of arguments for 'del' command".to_string();
        }
        let mut count = 0u64;
        for key in args {
            if self.data.remove(*key).is_some() {
                self.expiries.remove(*key);
                count += 1;
            }
        }
        format!("(integer) {count}")
    }

    fn cmd_keys(&self, args: &[&str]) -> String {
        if args.len() != 1 {
            return "(error) ERR wrong number of arguments for 'keys' command".to_string();
        }
        let pattern = args[0];
        let keys: Vec<&String> = if pattern == "*" {
            self.data.keys().collect()
        } else {
            self.data
                .keys()
                .filter(|k| glob_match(pattern, k))
                .collect()
        };

        if keys.is_empty() {
            return "(empty array)".to_string();
        }
        keys.iter()
            .enumerate()
            .map(|(i, k)| format!("{}) \"{}\"", i + 1, k))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn cmd_expire(&mut self, args: &[&str]) -> String {
        if args.len() != 2 {
            return "(error) ERR wrong number of arguments for 'expire' command".to_string();
        }
        let key = args[0];
        let secs: u64 = match args[1].parse() {
            Ok(v) => v,
            Err(_) => return "(error) ERR value is not an integer or out of range".to_string(),
        };
        if self.data.contains_key(key) {
            let deadline = Instant::now() + std::time::Duration::from_secs(secs);
            self.expiries.insert(key.to_string(), deadline);
            "(integer) 1".to_string()
        } else {
            "(integer) 0".to_string()
        }
    }

    fn cmd_ttl(&self, args: &[&str]) -> String {
        if args.len() != 1 {
            return "(error) ERR wrong number of arguments for 'ttl' command".to_string();
        }
        let key = args[0];
        if !self.data.contains_key(key) {
            return "(integer) -2".to_string();
        }
        match self.expiries.get(key) {
            Some(&deadline) => {
                let now = Instant::now();
                if now >= deadline {
                    "(integer) -2".to_string()
                } else {
                    let remaining = (deadline - now).as_secs() as i64;
                    format!("(integer) {remaining}")
                }
            }
            None => "(integer) -1".to_string(),
        }
    }

    // ── List commands ────────────────────────────────────────────────────────

    fn cmd_lpush(&mut self, args: &[&str]) -> String {
        if args.len() < 2 {
            return "(error) ERR wrong number of arguments for 'lpush' command".to_string();
        }
        let key = args[0].to_string();
        let list = self.get_or_create_list(&key);
        match list {
            Some(l) => {
                for val in &args[1..] {
                    l.push_front((*val).to_string());
                }
                format!("(integer) {}", l.len())
            }
            None => "(error) WRONGTYPE Operation against a key holding the wrong kind of value"
                .to_string(),
        }
    }

    fn cmd_rpush(&mut self, args: &[&str]) -> String {
        if args.len() < 2 {
            return "(error) ERR wrong number of arguments for 'rpush' command".to_string();
        }
        let key = args[0].to_string();
        let list = self.get_or_create_list(&key);
        match list {
            Some(l) => {
                for val in &args[1..] {
                    l.push_back((*val).to_string());
                }
                format!("(integer) {}", l.len())
            }
            None => "(error) WRONGTYPE Operation against a key holding the wrong kind of value"
                .to_string(),
        }
    }

    fn cmd_lpop(&mut self, args: &[&str]) -> String {
        if args.len() != 1 {
            return "(error) ERR wrong number of arguments for 'lpop' command".to_string();
        }
        match self.data.get_mut(args[0]) {
            Some(StoredValue::List(l)) => match l.pop_front() {
                Some(v) => format!("\"{v}\""),
                None => "(nil)".to_string(),
            },
            Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value"
                .to_string(),
            None => "(nil)".to_string(),
        }
    }

    fn cmd_rpop(&mut self, args: &[&str]) -> String {
        if args.len() != 1 {
            return "(error) ERR wrong number of arguments for 'rpop' command".to_string();
        }
        match self.data.get_mut(args[0]) {
            Some(StoredValue::List(l)) => match l.pop_back() {
                Some(v) => format!("\"{v}\""),
                None => "(nil)".to_string(),
            },
            Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value"
                .to_string(),
            None => "(nil)".to_string(),
        }
    }

    fn cmd_lrange(&self, args: &[&str]) -> String {
        if args.len() != 3 {
            return "(error) ERR wrong number of arguments for 'lrange' command".to_string();
        }
        let start: i64 = match args[1].parse() {
            Ok(v) => v,
            Err(_) => return "(error) ERR value is not an integer or out of range".to_string(),
        };
        let stop: i64 = match args[2].parse() {
            Ok(v) => v,
            Err(_) => return "(error) ERR value is not an integer or out of range".to_string(),
        };
        match self.data.get(args[0]) {
            Some(StoredValue::List(l)) => {
                let len = l.len() as i64;
                let s = normalize_index(start, len);
                let e = normalize_index(stop, len);
                if s > e || s >= len as usize {
                    return "(empty array)".to_string();
                }
                let end = e.min(len as usize - 1);
                let items: Vec<String> = (s..=end)
                    .filter_map(|i| l.get(i))
                    .enumerate()
                    .map(|(idx, v)| format!("{}) \"{}\"", idx + 1, v))
                    .collect();
                if items.is_empty() {
                    "(empty array)".to_string()
                } else {
                    items.join("\n")
                }
            }
            Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value"
                .to_string(),
            None => "(empty array)".to_string(),
        }
    }

    fn cmd_llen(&self, args: &[&str]) -> String {
        if args.len() != 1 {
            return "(error) ERR wrong number of arguments for 'llen' command".to_string();
        }
        match self.data.get(args[0]) {
            Some(StoredValue::List(l)) => format!("(integer) {}", l.len()),
            Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value"
                .to_string(),
            None => "(integer) 0".to_string(),
        }
    }

    // ── Hash commands ────────────────────────────────────────────────────────

    fn cmd_hset(&mut self, args: &[&str]) -> String {
        if args.len() < 3 || (args.len() - 1) % 2 != 0 {
            return "(error) ERR wrong number of arguments for 'hset' command".to_string();
        }
        let key = args[0].to_string();
        let hash = self.get_or_create_hash(&key);
        match hash {
            Some(h) => {
                let mut added = 0u64;
                for pair in args[1..].chunks(2) {
                    if !h.contains_key(pair[0]) {
                        added += 1;
                    }
                    h.insert(pair[0].to_string(), pair[1].to_string());
                }
                format!("(integer) {added}")
            }
            None => "(error) WRONGTYPE Operation against a key holding the wrong kind of value"
                .to_string(),
        }
    }

    fn cmd_hget(&self, args: &[&str]) -> String {
        if args.len() != 2 {
            return "(error) ERR wrong number of arguments for 'hget' command".to_string();
        }
        match self.data.get(args[0]) {
            Some(StoredValue::Hash(h)) => match h.get(args[1]) {
                Some(v) => format!("\"{v}\""),
                None => "(nil)".to_string(),
            },
            Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value"
                .to_string(),
            None => "(nil)".to_string(),
        }
    }

    fn cmd_hgetall(&self, args: &[&str]) -> String {
        if args.len() != 1 {
            return "(error) ERR wrong number of arguments for 'hgetall' command".to_string();
        }
        match self.data.get(args[0]) {
            Some(StoredValue::Hash(h)) => {
                if h.is_empty() {
                    return "(empty array)".to_string();
                }
                let mut items: Vec<String> = Vec::new();
                let mut idx = 1;
                // Sort for deterministic output
                let mut pairs: Vec<_> = h.iter().collect();
                pairs.sort_by_key(|(k, _)| k.clone());
                for (k, v) in pairs {
                    items.push(format!("{idx}) \"{k}\""));
                    idx += 1;
                    items.push(format!("{idx}) \"{v}\""));
                    idx += 1;
                }
                items.join("\n")
            }
            Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value"
                .to_string(),
            None => "(empty array)".to_string(),
        }
    }

    fn cmd_hdel(&mut self, args: &[&str]) -> String {
        if args.len() < 2 {
            return "(error) ERR wrong number of arguments for 'hdel' command".to_string();
        }
        match self.data.get_mut(args[0]) {
            Some(StoredValue::Hash(h)) => {
                let mut count = 0u64;
                for field in &args[1..] {
                    if h.remove(*field).is_some() {
                        count += 1;
                    }
                }
                format!("(integer) {count}")
            }
            Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value"
                .to_string(),
            None => "(integer) 0".to_string(),
        }
    }

    // ── Set commands ─────────────────────────────────────────────────────────

    fn cmd_sadd(&mut self, args: &[&str]) -> String {
        if args.len() < 2 {
            return "(error) ERR wrong number of arguments for 'sadd' command".to_string();
        }
        let key = args[0].to_string();
        let set = self.get_or_create_set(&key);
        match set {
            Some(s) => {
                let mut added = 0u64;
                for member in &args[1..] {
                    if s.insert((*member).to_string()) {
                        added += 1;
                    }
                }
                format!("(integer) {added}")
            }
            None => "(error) WRONGTYPE Operation against a key holding the wrong kind of value"
                .to_string(),
        }
    }

    fn cmd_smembers(&self, args: &[&str]) -> String {
        if args.len() != 1 {
            return "(error) ERR wrong number of arguments for 'smembers' command".to_string();
        }
        match self.data.get(args[0]) {
            Some(StoredValue::Set(s)) => {
                if s.is_empty() {
                    return "(empty array)".to_string();
                }
                let mut members: Vec<&String> = s.iter().collect();
                members.sort();
                members
                    .iter()
                    .enumerate()
                    .map(|(i, m)| format!("{}) \"{}\"", i + 1, m))
                    .collect::<Vec<_>>()
                    .join("\n")
            }
            Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value"
                .to_string(),
            None => "(empty array)".to_string(),
        }
    }

    fn cmd_scard(&self, args: &[&str]) -> String {
        if args.len() != 1 {
            return "(error) ERR wrong number of arguments for 'scard' command".to_string();
        }
        match self.data.get(args[0]) {
            Some(StoredValue::Set(s)) => format!("(integer) {}", s.len()),
            Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value"
                .to_string(),
            None => "(integer) 0".to_string(),
        }
    }

    // ── Sorted set commands ──────────────────────────────────────────────────

    fn cmd_zadd(&mut self, args: &[&str]) -> String {
        if args.len() < 3 || (args.len() - 1) % 2 != 0 {
            return "(error) ERR wrong number of arguments for 'zadd' command".to_string();
        }
        let key = args[0].to_string();
        let zset = self.get_or_create_zset(&key);
        match zset {
            Some(z) => {
                let mut added = 0u64;
                for pair in args[1..].chunks(2) {
                    let score: f64 = match pair[0].parse() {
                        Ok(v) => v,
                        Err(_) => return "(error) ERR value is not a valid float".to_string(),
                    };
                    let member = pair[1].to_string();
                    if !z.contains_key(&member) {
                        added += 1;
                    }
                    z.insert(member, score);
                }
                format!("(integer) {added}")
            }
            None => "(error) WRONGTYPE Operation against a key holding the wrong kind of value"
                .to_string(),
        }
    }

    fn cmd_zrange(&self, args: &[&str]) -> String {
        if args.len() < 3 {
            return "(error) ERR wrong number of arguments for 'zrange' command".to_string();
        }
        let with_scores = args.len() > 3 && args[3].to_uppercase() == "WITHSCORES";
        let start: i64 = match args[1].parse() {
            Ok(v) => v,
            Err(_) => return "(error) ERR value is not an integer or out of range".to_string(),
        };
        let stop: i64 = match args[2].parse() {
            Ok(v) => v,
            Err(_) => return "(error) ERR value is not an integer or out of range".to_string(),
        };

        match self.data.get(args[0]) {
            Some(StoredValue::SortedSet(z)) => {
                // Sort by score ascending, then by member name
                let mut entries: Vec<(&String, &f64)> = z.iter().collect();
                entries.sort_by(|a, b| {
                    a.1.partial_cmp(b.1)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| a.0.cmp(b.0))
                });

                let len = entries.len() as i64;
                let s = normalize_index(start, len);
                let e = normalize_index(stop, len);
                if s > e || s >= entries.len() {
                    return "(empty array)".to_string();
                }
                let end = e.min(entries.len() - 1);
                let slice = &entries[s..=end];

                if slice.is_empty() {
                    return "(empty array)".to_string();
                }

                if with_scores {
                    let mut items = Vec::new();
                    let mut idx = 1;
                    for (member, score) in slice {
                        items.push(format!("{idx}) \"{member}\""));
                        idx += 1;
                        items.push(format!("{idx}) \"{score}\""));
                        idx += 1;
                    }
                    items.join("\n")
                } else {
                    slice
                        .iter()
                        .enumerate()
                        .map(|(i, (m, _))| format!("{}) \"{}\"", i + 1, m))
                        .collect::<Vec<_>>()
                        .join("\n")
                }
            }
            Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value"
                .to_string(),
            None => "(empty array)".to_string(),
        }
    }

    fn cmd_zscore(&self, args: &[&str]) -> String {
        if args.len() != 2 {
            return "(error) ERR wrong number of arguments for 'zscore' command".to_string();
        }
        match self.data.get(args[0]) {
            Some(StoredValue::SortedSet(z)) => match z.get(args[1]) {
                Some(score) => format!("\"{score}\""),
                None => "(nil)".to_string(),
            },
            Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value"
                .to_string(),
            None => "(nil)".to_string(),
        }
    }

    // ── Server commands ──────────────────────────────────────────────────────

    fn cmd_dbsize(&self) -> String {
        format!("(integer) {}", self.data.len())
    }

    fn cmd_flushdb(&mut self) -> String {
        self.data.clear();
        self.expiries.clear();
        "OK".to_string()
    }

    fn cmd_info(&self) -> String {
        let stats = self.stats();
        format!(
            "# Server\r\n\
             ferrite_version:0.1.0\r\n\
             mode:playground\r\n\
             \r\n\
             # Keyspace\r\n\
             keys:{}\r\n\
             commands_executed:{}\r\n\
             memory_bytes:{}\r\n\
             uptime_ms:{}",
            stats.keys_count, stats.commands_executed, stats.memory_bytes, stats.uptime_ms
        )
    }

    fn cmd_help(&self) -> String {
        "Ferrite Playground — Supported commands:\n\
         String:  SET, GET, DEL, KEYS, EXPIRE, TTL\n\
         List:    LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN\n\
         Hash:    HSET, HGET, HGETALL, HDEL\n\
         Set:     SADD, SMEMBERS, SCARD\n\
         ZSet:    ZADD, ZRANGE, ZSCORE\n\
         Server:  PING, ECHO, DBSIZE, FLUSHDB, INFO, HELP"
            .to_string()
    }

    // ── Mutable collection helpers ───────────────────────────────────────────

    fn get_or_create_list(&mut self, key: &str) -> Option<&mut VecDeque<String>> {
        if !self.data.contains_key(key) {
            self.data
                .insert(key.to_string(), StoredValue::List(VecDeque::new()));
        }
        match self.data.get_mut(key) {
            Some(StoredValue::List(l)) => Some(l),
            _ => None,
        }
    }

    fn get_or_create_hash(&mut self, key: &str) -> Option<&mut HashMap<String, String>> {
        if !self.data.contains_key(key) {
            self.data
                .insert(key.to_string(), StoredValue::Hash(HashMap::new()));
        }
        match self.data.get_mut(key) {
            Some(StoredValue::Hash(h)) => Some(h),
            _ => None,
        }
    }

    fn get_or_create_set(&mut self, key: &str) -> Option<&mut HashSet<String>> {
        if !self.data.contains_key(key) {
            self.data
                .insert(key.to_string(), StoredValue::Set(HashSet::new()));
        }
        match self.data.get_mut(key) {
            Some(StoredValue::Set(s)) => Some(s),
            _ => None,
        }
    }

    fn get_or_create_zset(&mut self, key: &str) -> Option<&mut BTreeMap<String, f64>> {
        if !self.data.contains_key(key) {
            self.data
                .insert(key.to_string(), StoredValue::SortedSet(BTreeMap::new()));
        }
        match self.data.get_mut(key) {
            Some(StoredValue::SortedSet(z)) => Some(z),
            _ => None,
        }
    }
}

impl Default for PlaygroundInstance {
    fn default() -> Self {
        Self::new()
    }
}

// ── Helper functions ─────────────────────────────────────────────────────────

/// Normalize a Redis-style index (supports negative indexing).
fn normalize_index(index: i64, len: i64) -> usize {
    if len == 0 {
        return 0;
    }
    if index < 0 {
        let adjusted = len + index;
        if adjusted < 0 {
            0
        } else {
            adjusted as usize
        }
    } else {
        index as usize
    }
}

/// Simple glob-style pattern matching (supports `*` and `?`).
fn glob_match(pattern: &str, text: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let txt: Vec<char> = text.chars().collect();
    glob_match_inner(&pat, &txt)
}

fn glob_match_inner(pattern: &[char], text: &[char]) -> bool {
    match (pattern.first(), text.first()) {
        (None, None) => true,
        (Some('*'), _) => {
            // Try skipping the * or consuming one character from text
            glob_match_inner(&pattern[1..], text)
                || (!text.is_empty() && glob_match_inner(pattern, &text[1..]))
        }
        (Some('?'), Some(_)) => glob_match_inner(&pattern[1..], &text[1..]),
        (Some(p), Some(t)) if *p == *t => glob_match_inner(&pattern[1..], &text[1..]),
        _ => false,
    }
}

/// Parse input into tokens, respecting double-quoted strings.
fn parse_tokens(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '"' {
            in_quotes = !in_quotes;
        } else if ch.is_whitespace() && !in_quotes {
            if !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
        } else {
            current.push(ch);
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ping() {
        let mut pg = PlaygroundInstance::new();
        assert_eq!(pg.execute_command("PING"), "PONG");
        assert_eq!(pg.execute_command("PING hello"), "\"hello\"");
    }

    #[test]
    fn test_set_get() {
        let mut pg = PlaygroundInstance::new();
        assert_eq!(pg.execute_command("SET foo bar"), "OK");
        assert_eq!(pg.execute_command("GET foo"), "\"bar\"");
        assert_eq!(pg.execute_command("GET missing"), "(nil)");
    }

    #[test]
    fn test_del() {
        let mut pg = PlaygroundInstance::new();
        pg.execute_command("SET a 1");
        pg.execute_command("SET b 2");
        assert_eq!(pg.execute_command("DEL a b c"), "(integer) 2");
    }

    #[test]
    fn test_keys() {
        let mut pg = PlaygroundInstance::new();
        pg.execute_command("SET user:1 a");
        pg.execute_command("SET user:2 b");
        pg.execute_command("SET other c");
        let result = pg.execute_command("KEYS user:*");
        assert!(result.contains("user:1"));
        assert!(result.contains("user:2"));
        assert!(!result.contains("other"));
    }

    #[test]
    fn test_list_ops() {
        let mut pg = PlaygroundInstance::new();
        assert_eq!(pg.execute_command("LPUSH mylist c b a"), "(integer) 3");
        assert_eq!(pg.execute_command("LLEN mylist"), "(integer) 3");
        assert_eq!(pg.execute_command("LPOP mylist"), "\"a\"");
        assert_eq!(pg.execute_command("RPOP mylist"), "\"c\"");
        assert_eq!(pg.execute_command("LLEN mylist"), "(integer) 1");
    }

    #[test]
    fn test_lrange() {
        let mut pg = PlaygroundInstance::new();
        pg.execute_command("RPUSH mylist a b c d");
        let result = pg.execute_command("LRANGE mylist 0 -1");
        assert!(result.contains("\"a\""));
        assert!(result.contains("\"d\""));
    }

    #[test]
    fn test_hash_ops() {
        let mut pg = PlaygroundInstance::new();
        assert_eq!(pg.execute_command("HSET h f1 v1 f2 v2"), "(integer) 2");
        assert_eq!(pg.execute_command("HGET h f1"), "\"v1\"");
        assert_eq!(pg.execute_command("HDEL h f1"), "(integer) 1");
        assert_eq!(pg.execute_command("HGET h f1"), "(nil)");
    }

    #[test]
    fn test_hgetall() {
        let mut pg = PlaygroundInstance::new();
        pg.execute_command("HSET myhash name Alice age 30");
        let result = pg.execute_command("HGETALL myhash");
        assert!(result.contains("\"name\""));
        assert!(result.contains("\"Alice\""));
    }

    #[test]
    fn test_set_ops() {
        let mut pg = PlaygroundInstance::new();
        assert_eq!(pg.execute_command("SADD myset a b c"), "(integer) 3");
        assert_eq!(pg.execute_command("SADD myset c d"), "(integer) 1");
        assert_eq!(pg.execute_command("SCARD myset"), "(integer) 4");
        let members = pg.execute_command("SMEMBERS myset");
        assert!(members.contains("\"a\""));
        assert!(members.contains("\"d\""));
    }

    #[test]
    fn test_sorted_set_ops() {
        let mut pg = PlaygroundInstance::new();
        assert_eq!(
            pg.execute_command("ZADD lb 100 alice 200 bob"),
            "(integer) 2"
        );
        assert_eq!(pg.execute_command("ZSCORE lb alice"), "\"100\"");
        let range = pg.execute_command("ZRANGE lb 0 -1 WITHSCORES");
        assert!(range.contains("\"alice\""));
        assert!(range.contains("\"100\""));
    }

    #[test]
    fn test_dbsize_and_flushdb() {
        let mut pg = PlaygroundInstance::new();
        pg.execute_command("SET a 1");
        pg.execute_command("SET b 2");
        assert_eq!(pg.execute_command("DBSIZE"), "(integer) 2");
        assert_eq!(pg.execute_command("FLUSHDB"), "OK");
        assert_eq!(pg.execute_command("DBSIZE"), "(integer) 0");
    }

    #[test]
    fn test_echo() {
        let mut pg = PlaygroundInstance::new();
        assert_eq!(pg.execute_command("ECHO hello world"), "\"hello world\"");
    }

    #[test]
    fn test_execute_batch() {
        let mut pg = PlaygroundInstance::new();
        let results = pg.execute_batch(&["SET x 1", "GET x"]);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], "OK");
        assert_eq!(results[1], "\"1\"");
    }

    #[test]
    fn test_stats() {
        let mut pg = PlaygroundInstance::new();
        pg.execute_command("SET a 1");
        pg.execute_command("SET b 2");
        let stats = pg.stats();
        assert_eq!(stats.keys_count, 2);
        assert_eq!(stats.commands_executed, 2);
    }

    #[test]
    fn test_reset() {
        let mut pg = PlaygroundInstance::new();
        pg.execute_command("SET a 1");
        pg.reset();
        assert_eq!(pg.stats().keys_count, 0);
        assert_eq!(pg.stats().commands_executed, 0);
    }

    #[test]
    fn test_tutorial_data() {
        let mut pg = PlaygroundInstance::new();
        pg.load_tutorial_data();
        // 5 strings + 1 list + 1 hash + 1 set + 1 sorted set = 9
        assert_eq!(pg.stats().keys_count, 9);
        assert_eq!(pg.execute_command("GET user:1"), "\"Alice\"");
        assert_eq!(pg.execute_command("LLEN queue:tasks"), "(integer) 3");
        assert_eq!(pg.execute_command("SCARD tags:popular"), "(integer) 5");
    }

    #[test]
    fn test_info() {
        let mut pg = PlaygroundInstance::new();
        let info = pg.execute_command("INFO");
        assert!(info.contains("ferrite_version:0.1.0"));
        assert!(info.contains("mode:playground"));
    }

    #[test]
    fn test_wrongtype() {
        let mut pg = PlaygroundInstance::new();
        pg.execute_command("SET mykey hello");
        let result = pg.execute_command("LPUSH mykey val");
        assert!(result.contains("WRONGTYPE"));
    }

    #[test]
    fn test_quoted_string_parsing() {
        let mut pg = PlaygroundInstance::new();
        pg.execute_command("SET greeting \"Hello, World!\"");
        assert_eq!(pg.execute_command("GET greeting"), "\"Hello, World!\"");
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("user:*", "user:123"));
        assert!(!glob_match("user:*", "other:123"));
        assert!(glob_match("h?llo", "hello"));
        assert!(!glob_match("h?llo", "hllo"));
    }
}
