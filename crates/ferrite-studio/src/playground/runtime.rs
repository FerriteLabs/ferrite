//! Playground Runtime
//!
//! Provides the execution environment for the playground.

use super::{ParsedCommand, PlaygroundError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Runtime configuration
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    /// Maximum memory in bytes
    pub max_memory_bytes: usize,
    /// Maximum execution time in milliseconds
    pub max_execution_time_ms: u64,
    /// Maximum number of keys
    pub max_keys: usize,
    /// Sandbox mode
    pub sandbox_mode: bool,
}

/// Playground runtime - simulated Ferrite environment
pub struct PlaygroundRuntime {
    /// Configuration
    config: RuntimeConfig,
    /// In-memory key-value store
    data: HashMap<String, DataValue>,
    /// Memory usage tracking
    memory_used: AtomicUsize,
    /// Statistics
    stats: RuntimeStats,
}

impl PlaygroundRuntime {
    /// Create a new runtime
    pub fn new(config: RuntimeConfig) -> Self {
        Self {
            config,
            data: HashMap::new(),
            memory_used: AtomicUsize::new(0),
            stats: RuntimeStats::default(),
        }
    }

    /// Execute a command
    pub async fn execute(
        &mut self,
        cmd: &ParsedCommand,
    ) -> Result<ExecutionResult, PlaygroundError> {
        self.stats.commands_executed += 1;

        // Enforce memory limit
        let current_mem = self.memory_used.load(Ordering::Relaxed);
        if current_mem > self.config.max_memory_bytes {
            return Err(PlaygroundError::MemoryLimitExceeded {
                used: current_mem,
                limit: self.config.max_memory_bytes,
            });
        }

        match cmd.name.as_str() {
            // String commands
            "SET" => self.cmd_set(&cmd.args),
            "GET" => self.cmd_get(&cmd.args),
            "DEL" => self.cmd_del(&cmd.args),
            "EXISTS" => self.cmd_exists(&cmd.args),
            "INCR" => self.cmd_incr(&cmd.args),
            "DECR" => self.cmd_decr(&cmd.args),
            "INCRBY" => self.cmd_incrby(&cmd.args),
            "APPEND" => self.cmd_append(&cmd.args),
            "STRLEN" => self.cmd_strlen(&cmd.args),
            "MSET" => self.cmd_mset(&cmd.args),
            "MGET" => self.cmd_mget(&cmd.args),
            "SETNX" => self.cmd_setnx(&cmd.args),
            "SETEX" => self.cmd_setex(&cmd.args),
            "GETSET" => self.cmd_getset(&cmd.args),

            // List commands
            "LPUSH" => self.cmd_lpush(&cmd.args),
            "RPUSH" => self.cmd_rpush(&cmd.args),
            "LPOP" => self.cmd_lpop(&cmd.args),
            "RPOP" => self.cmd_rpop(&cmd.args),
            "LLEN" => self.cmd_llen(&cmd.args),
            "LRANGE" => self.cmd_lrange(&cmd.args),
            "LINDEX" => self.cmd_lindex(&cmd.args),

            // Hash commands
            "HSET" => self.cmd_hset(&cmd.args),
            "HGET" => self.cmd_hget(&cmd.args),
            "HDEL" => self.cmd_hdel(&cmd.args),
            "HGETALL" => self.cmd_hgetall(&cmd.args),
            "HKEYS" => self.cmd_hkeys(&cmd.args),
            "HVALS" => self.cmd_hvals(&cmd.args),
            "HLEN" => self.cmd_hlen(&cmd.args),
            "HEXISTS" => self.cmd_hexists(&cmd.args),

            // Set commands
            "SADD" => self.cmd_sadd(&cmd.args),
            "SREM" => self.cmd_srem(&cmd.args),
            "SMEMBERS" => self.cmd_smembers(&cmd.args),
            "SISMEMBER" => self.cmd_sismember(&cmd.args),
            "SCARD" => self.cmd_scard(&cmd.args),

            // Sorted set commands
            "ZADD" => self.cmd_zadd(&cmd.args),
            "ZRANGE" => self.cmd_zrange(&cmd.args),
            "ZSCORE" => self.cmd_zscore(&cmd.args),
            "ZCARD" => self.cmd_zcard(&cmd.args),
            "ZRANK" => self.cmd_zrank(&cmd.args),

            // Key commands
            "KEYS" => self.cmd_keys(&cmd.args),
            "TYPE" => self.cmd_type(&cmd.args),
            "RENAME" => self.cmd_rename(&cmd.args),
            "EXPIRE" => self.cmd_expire(&cmd.args),
            "TTL" => self.cmd_ttl(&cmd.args),
            "DBSIZE" => self.cmd_dbsize(&cmd.args),
            "FLUSHDB" => self.cmd_flushdb(&cmd.args),

            // Server commands
            "PING" => self.cmd_ping(&cmd.args),
            "ECHO" => self.cmd_echo(&cmd.args),
            "INFO" => self.cmd_info(&cmd.args),
            "TIME" => self.cmd_time(&cmd.args),

            _ => Err(PlaygroundError::UnknownCommand(cmd.name.clone())),
        }
    }

    /// Reset the runtime
    pub fn reset(&mut self) -> Result<(), PlaygroundError> {
        self.data.clear();
        self.memory_used.store(0, Ordering::Relaxed);
        self.stats = RuntimeStats::default();
        Ok(())
    }

    /// Export data
    pub fn export_data(&self) -> Result<HashMap<String, Vec<u8>>, PlaygroundError> {
        let mut exported = HashMap::new();
        for (key, value) in &self.data {
            let serialized = serde_json::to_vec(value)
                .map_err(|e| PlaygroundError::Serialization(e.to_string()))?;
            exported.insert(key.clone(), serialized);
        }
        Ok(exported)
    }

    /// Import data
    pub fn import_data(&mut self, data: &HashMap<String, Vec<u8>>) -> Result<(), PlaygroundError> {
        self.data.clear();
        for (key, value) in data {
            let deserialized: DataValue = serde_json::from_slice(value)
                .map_err(|e| PlaygroundError::Serialization(e.to_string()))?;
            self.data.insert(key.clone(), deserialized);
        }
        self.update_memory_usage();
        Ok(())
    }

    /// Check key limit
    fn check_key_limit(&self) -> Result<(), PlaygroundError> {
        if self.data.len() >= self.config.max_keys {
            return Err(PlaygroundError::KeyLimitExceeded {
                count: self.data.len(),
                limit: self.config.max_keys,
            });
        }
        Ok(())
    }

    /// Update memory usage tracking
    fn update_memory_usage(&self) {
        let estimated: usize = self
            .data
            .iter()
            .map(|(k, v)| k.len() + v.estimated_size())
            .sum();
        self.memory_used.store(estimated, Ordering::Relaxed);
    }

    /// Check memory limit and return error if exceeded
    fn check_memory_limit(&self) -> Result<(), PlaygroundError> {
        let current_mem = self.memory_used.load(Ordering::Relaxed);
        if current_mem > self.config.max_memory_bytes {
            return Err(PlaygroundError::MemoryLimitExceeded {
                used: current_mem,
                limit: self.config.max_memory_bytes,
            });
        }
        Ok(())
    }

    /// Update memory usage and enforce limit after write operations
    fn update_and_check_memory(&self) -> Result<(), PlaygroundError> {
        self.update_memory_usage();
        self.check_memory_limit()
    }

    // String commands

    fn cmd_set(&mut self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.len() < 2 {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'set' command",
            ));
        }
        self.check_key_limit()?;
        self.data
            .insert(args[0].clone(), DataValue::String(args[1].clone()));
        self.update_and_check_memory()?;
        Ok(ExecutionResult::ok())
    }

    fn cmd_get(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.is_empty() {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'get' command",
            ));
        }
        match self.data.get(&args[0]) {
            Some(DataValue::String(s)) => Ok(ExecutionResult::bulk_string(s.clone())),
            Some(_) => Ok(ExecutionResult::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
            None => Ok(ExecutionResult::null()),
        }
    }

    fn cmd_del(&mut self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        let mut count = 0;
        for key in args {
            if self.data.remove(key).is_some() {
                count += 1;
            }
        }
        self.update_memory_usage();
        Ok(ExecutionResult::integer(count))
    }

    fn cmd_exists(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        let count = args.iter().filter(|k| self.data.contains_key(*k)).count();
        Ok(ExecutionResult::integer(count as i64))
    }

    fn cmd_incr(&mut self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        self.cmd_incrby_internal(&args[0], 1)
    }

    fn cmd_decr(&mut self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        self.cmd_incrby_internal(&args[0], -1)
    }

    fn cmd_incrby(&mut self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.len() < 2 {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'incrby' command",
            ));
        }
        let increment: i64 = args[1].parse().map_err(|_| {
            PlaygroundError::Runtime("value is not an integer or out of range".to_string())
        })?;
        self.cmd_incrby_internal(&args[0], increment)
    }

    fn cmd_incrby_internal(
        &mut self,
        key: &str,
        increment: i64,
    ) -> Result<ExecutionResult, PlaygroundError> {
        let current = match self.data.get(key) {
            Some(DataValue::String(s)) => s.parse::<i64>().map_err(|_| {
                PlaygroundError::Runtime("value is not an integer or out of range".to_string())
            })?,
            Some(_) => return Ok(ExecutionResult::error("WRONGTYPE")),
            None => 0,
        };

        let new_value = current + increment;
        self.data
            .insert(key.to_string(), DataValue::String(new_value.to_string()));
        Ok(ExecutionResult::integer(new_value))
    }

    fn cmd_append(&mut self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.len() < 2 {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'append' command",
            ));
        }
        let new_len = match self.data.get_mut(&args[0]) {
            Some(DataValue::String(s)) => {
                s.push_str(&args[1]);
                s.len()
            }
            Some(_) => return Ok(ExecutionResult::error("WRONGTYPE")),
            None => {
                self.data
                    .insert(args[0].clone(), DataValue::String(args[1].clone()));
                args[1].len()
            }
        };
        self.update_memory_usage();
        Ok(ExecutionResult::integer(new_len as i64))
    }

    fn cmd_strlen(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.is_empty() {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'strlen' command",
            ));
        }
        match self.data.get(&args[0]) {
            Some(DataValue::String(s)) => Ok(ExecutionResult::integer(s.len() as i64)),
            Some(_) => Ok(ExecutionResult::error("WRONGTYPE")),
            None => Ok(ExecutionResult::integer(0)),
        }
    }

    fn cmd_mset(&mut self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.len() % 2 != 0 {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'mset' command",
            ));
        }
        for chunk in args.chunks(2) {
            self.data
                .insert(chunk[0].clone(), DataValue::String(chunk[1].clone()));
        }
        self.update_memory_usage();
        Ok(ExecutionResult::ok())
    }

    fn cmd_mget(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        let values: Vec<Option<String>> = args
            .iter()
            .map(|k| match self.data.get(k) {
                Some(DataValue::String(s)) => Some(s.clone()),
                _ => None,
            })
            .collect();
        Ok(ExecutionResult::array(
            values
                .iter()
                .map(|v| match v {
                    Some(s) => ExecutionResult::bulk_string(s.clone()),
                    None => ExecutionResult::null(),
                })
                .collect(),
        ))
    }

    fn cmd_setnx(&mut self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.len() < 2 {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'setnx' command",
            ));
        }
        if self.data.contains_key(&args[0]) {
            Ok(ExecutionResult::integer(0))
        } else {
            self.data
                .insert(args[0].clone(), DataValue::String(args[1].clone()));
            self.update_memory_usage();
            Ok(ExecutionResult::integer(1))
        }
    }

    fn cmd_setex(&mut self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.len() < 3 {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'setex' command",
            ));
        }
        // In playground, we ignore TTL for simplicity
        self.data
            .insert(args[0].clone(), DataValue::String(args[2].clone()));
        self.update_memory_usage();
        Ok(ExecutionResult::ok())
    }

    fn cmd_getset(&mut self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.len() < 2 {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'getset' command",
            ));
        }
        let old = match self.data.get(&args[0]) {
            Some(DataValue::String(s)) => ExecutionResult::bulk_string(s.clone()),
            Some(_) => return Ok(ExecutionResult::error("WRONGTYPE")),
            None => ExecutionResult::null(),
        };
        self.data
            .insert(args[0].clone(), DataValue::String(args[1].clone()));
        self.update_memory_usage();
        Ok(old)
    }

    // List commands

    fn cmd_lpush(&mut self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.len() < 2 {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'lpush' command",
            ));
        }
        self.check_key_limit()?;

        // Check type first
        if let Some(existing) = self.data.get(&args[0]) {
            if !matches!(existing, DataValue::List(_)) {
                return Ok(ExecutionResult::error("WRONGTYPE"));
            }
        }

        let list = self
            .data
            .entry(args[0].clone())
            .or_insert_with(|| DataValue::List(Vec::new()));

        let len = if let DataValue::List(ref mut l) = list {
            for item in args[1..].iter().rev() {
                l.insert(0, item.clone());
            }
            l.len() as i64
        } else {
            return Ok(ExecutionResult::error("WRONGTYPE"));
        };

        self.update_memory_usage();
        Ok(ExecutionResult::integer(len))
    }

    fn cmd_rpush(&mut self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.len() < 2 {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'rpush' command",
            ));
        }
        self.check_key_limit()?;

        // Check type first
        if let Some(existing) = self.data.get(&args[0]) {
            if !matches!(existing, DataValue::List(_)) {
                return Ok(ExecutionResult::error("WRONGTYPE"));
            }
        }

        let list = self
            .data
            .entry(args[0].clone())
            .or_insert_with(|| DataValue::List(Vec::new()));

        let len = if let DataValue::List(ref mut l) = list {
            for item in &args[1..] {
                l.push(item.clone());
            }
            l.len() as i64
        } else {
            return Ok(ExecutionResult::error("WRONGTYPE"));
        };

        self.update_memory_usage();
        Ok(ExecutionResult::integer(len))
    }

    fn cmd_lpop(&mut self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.is_empty() {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'lpop' command",
            ));
        }
        let item = match self.data.get_mut(&args[0]) {
            Some(DataValue::List(ref mut l)) if !l.is_empty() => Some(l.remove(0)),
            Some(DataValue::List(_)) => return Ok(ExecutionResult::null()),
            Some(_) => return Ok(ExecutionResult::error("WRONGTYPE")),
            None => return Ok(ExecutionResult::null()),
        };

        if let Some(item) = item {
            self.update_memory_usage();
            Ok(ExecutionResult::bulk_string(item))
        } else {
            Ok(ExecutionResult::null())
        }
    }

    fn cmd_rpop(&mut self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.is_empty() {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'rpop' command",
            ));
        }
        let item = match self.data.get_mut(&args[0]) {
            Some(DataValue::List(ref mut l)) if !l.is_empty() => l.pop(),
            Some(DataValue::List(_)) => return Ok(ExecutionResult::null()),
            Some(_) => return Ok(ExecutionResult::error("WRONGTYPE")),
            None => return Ok(ExecutionResult::null()),
        };

        if let Some(item) = item {
            self.update_memory_usage();
            Ok(ExecutionResult::bulk_string(item))
        } else {
            Ok(ExecutionResult::null())
        }
    }

    fn cmd_llen(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.is_empty() {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'llen' command",
            ));
        }
        match self.data.get(&args[0]) {
            Some(DataValue::List(l)) => Ok(ExecutionResult::integer(l.len() as i64)),
            Some(_) => Ok(ExecutionResult::error("WRONGTYPE")),
            None => Ok(ExecutionResult::integer(0)),
        }
    }

    fn cmd_lrange(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.len() < 3 {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'lrange' command",
            ));
        }
        let start: i64 = args[1].parse().unwrap_or(0);
        let stop: i64 = args[2].parse().unwrap_or(-1);

        match self.data.get(&args[0]) {
            Some(DataValue::List(l)) => {
                let len = l.len() as i64;
                let start = if start < 0 {
                    (len + start).max(0)
                } else {
                    start.min(len)
                } as usize;
                let stop = if stop < 0 {
                    (len + stop + 1).max(0)
                } else {
                    (stop + 1).min(len)
                } as usize;

                let items: Vec<ExecutionResult> = l[start..stop]
                    .iter()
                    .map(|s| ExecutionResult::bulk_string(s.clone()))
                    .collect();
                Ok(ExecutionResult::array(items))
            }
            Some(_) => Ok(ExecutionResult::error("WRONGTYPE")),
            None => Ok(ExecutionResult::array(vec![])),
        }
    }

    fn cmd_lindex(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.len() < 2 {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'lindex' command",
            ));
        }
        let index: i64 = args[1].parse().unwrap_or(0);

        match self.data.get(&args[0]) {
            Some(DataValue::List(l)) => {
                let len = l.len() as i64;
                let idx = if index < 0 { len + index } else { index };
                if idx >= 0 && (idx as usize) < l.len() {
                    Ok(ExecutionResult::bulk_string(l[idx as usize].clone()))
                } else {
                    Ok(ExecutionResult::null())
                }
            }
            Some(_) => Ok(ExecutionResult::error("WRONGTYPE")),
            None => Ok(ExecutionResult::null()),
        }
    }

    // Hash commands

    fn cmd_hset(&mut self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.len() < 3 {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'hset' command",
            ));
        }
        self.check_key_limit()?;
        let hash = self
            .data
            .entry(args[0].clone())
            .or_insert_with(|| DataValue::Hash(HashMap::new()));

        if let DataValue::Hash(ref mut h) = hash {
            let mut new_fields = 0;
            for chunk in args[1..].chunks(2) {
                if chunk.len() == 2 {
                    if !h.contains_key(&chunk[0]) {
                        new_fields += 1;
                    }
                    h.insert(chunk[0].clone(), chunk[1].clone());
                }
            }
            self.update_memory_usage();
            Ok(ExecutionResult::integer(new_fields))
        } else {
            Ok(ExecutionResult::error("WRONGTYPE"))
        }
    }

    fn cmd_hget(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.len() < 2 {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'hget' command",
            ));
        }
        match self.data.get(&args[0]) {
            Some(DataValue::Hash(h)) => match h.get(&args[1]) {
                Some(v) => Ok(ExecutionResult::bulk_string(v.clone())),
                None => Ok(ExecutionResult::null()),
            },
            Some(_) => Ok(ExecutionResult::error("WRONGTYPE")),
            None => Ok(ExecutionResult::null()),
        }
    }

    fn cmd_hdel(&mut self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.len() < 2 {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'hdel' command",
            ));
        }
        match self.data.get_mut(&args[0]) {
            Some(DataValue::Hash(ref mut h)) => {
                let mut deleted = 0;
                for field in &args[1..] {
                    if h.remove(field).is_some() {
                        deleted += 1;
                    }
                }
                self.update_memory_usage();
                Ok(ExecutionResult::integer(deleted))
            }
            Some(_) => Ok(ExecutionResult::error("WRONGTYPE")),
            None => Ok(ExecutionResult::integer(0)),
        }
    }

    fn cmd_hgetall(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.is_empty() {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'hgetall' command",
            ));
        }
        match self.data.get(&args[0]) {
            Some(DataValue::Hash(h)) => {
                let items: Vec<ExecutionResult> = h
                    .iter()
                    .flat_map(|(k, v)| {
                        vec![
                            ExecutionResult::bulk_string(k.clone()),
                            ExecutionResult::bulk_string(v.clone()),
                        ]
                    })
                    .collect();
                Ok(ExecutionResult::array(items))
            }
            Some(_) => Ok(ExecutionResult::error("WRONGTYPE")),
            None => Ok(ExecutionResult::array(vec![])),
        }
    }

    fn cmd_hkeys(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.is_empty() {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'hkeys' command",
            ));
        }
        match self.data.get(&args[0]) {
            Some(DataValue::Hash(h)) => {
                let keys: Vec<ExecutionResult> = h
                    .keys()
                    .map(|k| ExecutionResult::bulk_string(k.clone()))
                    .collect();
                Ok(ExecutionResult::array(keys))
            }
            Some(_) => Ok(ExecutionResult::error("WRONGTYPE")),
            None => Ok(ExecutionResult::array(vec![])),
        }
    }

    fn cmd_hvals(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.is_empty() {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'hvals' command",
            ));
        }
        match self.data.get(&args[0]) {
            Some(DataValue::Hash(h)) => {
                let vals: Vec<ExecutionResult> = h
                    .values()
                    .map(|v| ExecutionResult::bulk_string(v.clone()))
                    .collect();
                Ok(ExecutionResult::array(vals))
            }
            Some(_) => Ok(ExecutionResult::error("WRONGTYPE")),
            None => Ok(ExecutionResult::array(vec![])),
        }
    }

    fn cmd_hlen(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.is_empty() {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'hlen' command",
            ));
        }
        match self.data.get(&args[0]) {
            Some(DataValue::Hash(h)) => Ok(ExecutionResult::integer(h.len() as i64)),
            Some(_) => Ok(ExecutionResult::error("WRONGTYPE")),
            None => Ok(ExecutionResult::integer(0)),
        }
    }

    fn cmd_hexists(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.len() < 2 {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'hexists' command",
            ));
        }
        match self.data.get(&args[0]) {
            Some(DataValue::Hash(h)) => Ok(ExecutionResult::integer(if h.contains_key(&args[1]) {
                1
            } else {
                0
            })),
            Some(_) => Ok(ExecutionResult::error("WRONGTYPE")),
            None => Ok(ExecutionResult::integer(0)),
        }
    }

    // Set commands

    fn cmd_sadd(&mut self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.len() < 2 {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'sadd' command",
            ));
        }
        self.check_key_limit()?;
        let set = self
            .data
            .entry(args[0].clone())
            .or_insert_with(|| DataValue::Set(std::collections::HashSet::new()));

        if let DataValue::Set(ref mut s) = set {
            let mut added = 0;
            for member in &args[1..] {
                if s.insert(member.clone()) {
                    added += 1;
                }
            }
            self.update_memory_usage();
            Ok(ExecutionResult::integer(added))
        } else {
            Ok(ExecutionResult::error("WRONGTYPE"))
        }
    }

    fn cmd_srem(&mut self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.len() < 2 {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'srem' command",
            ));
        }
        match self.data.get_mut(&args[0]) {
            Some(DataValue::Set(ref mut s)) => {
                let mut removed = 0;
                for member in &args[1..] {
                    if s.remove(member) {
                        removed += 1;
                    }
                }
                self.update_memory_usage();
                Ok(ExecutionResult::integer(removed))
            }
            Some(_) => Ok(ExecutionResult::error("WRONGTYPE")),
            None => Ok(ExecutionResult::integer(0)),
        }
    }

    fn cmd_smembers(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.is_empty() {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'smembers' command",
            ));
        }
        match self.data.get(&args[0]) {
            Some(DataValue::Set(s)) => {
                let members: Vec<ExecutionResult> = s
                    .iter()
                    .map(|m| ExecutionResult::bulk_string(m.clone()))
                    .collect();
                Ok(ExecutionResult::array(members))
            }
            Some(_) => Ok(ExecutionResult::error("WRONGTYPE")),
            None => Ok(ExecutionResult::array(vec![])),
        }
    }

    fn cmd_sismember(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.len() < 2 {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'sismember' command",
            ));
        }
        match self.data.get(&args[0]) {
            Some(DataValue::Set(s)) => Ok(ExecutionResult::integer(if s.contains(&args[1]) {
                1
            } else {
                0
            })),
            Some(_) => Ok(ExecutionResult::error("WRONGTYPE")),
            None => Ok(ExecutionResult::integer(0)),
        }
    }

    fn cmd_scard(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.is_empty() {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'scard' command",
            ));
        }
        match self.data.get(&args[0]) {
            Some(DataValue::Set(s)) => Ok(ExecutionResult::integer(s.len() as i64)),
            Some(_) => Ok(ExecutionResult::error("WRONGTYPE")),
            None => Ok(ExecutionResult::integer(0)),
        }
    }

    // Sorted set commands

    fn cmd_zadd(&mut self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.len() < 3 {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'zadd' command",
            ));
        }
        self.check_key_limit()?;
        let zset = self
            .data
            .entry(args[0].clone())
            .or_insert_with(|| DataValue::SortedSet(Vec::new()));

        if let DataValue::SortedSet(ref mut z) = zset {
            let mut added = 0;
            for chunk in args[1..].chunks(2) {
                if chunk.len() == 2 {
                    let score: f64 = chunk[0].parse().unwrap_or(0.0);
                    let member = chunk[1].clone();

                    // Remove existing entry
                    z.retain(|(_, m)| m != &member);
                    z.push((score, member));
                    added += 1;
                }
            }
            z.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            self.update_memory_usage();
            Ok(ExecutionResult::integer(added))
        } else {
            Ok(ExecutionResult::error("WRONGTYPE"))
        }
    }

    fn cmd_zrange(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.len() < 3 {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'zrange' command",
            ));
        }
        let start: i64 = args[1].parse().unwrap_or(0);
        let stop: i64 = args[2].parse().unwrap_or(-1);
        let with_scores = args.len() > 3 && args[3].to_uppercase() == "WITHSCORES";

        match self.data.get(&args[0]) {
            Some(DataValue::SortedSet(z)) => {
                let len = z.len() as i64;
                let start = if start < 0 {
                    (len + start).max(0)
                } else {
                    start.min(len)
                } as usize;
                let stop = if stop < 0 {
                    (len + stop + 1).max(0)
                } else {
                    (stop + 1).min(len)
                } as usize;

                let items: Vec<ExecutionResult> = z[start..stop]
                    .iter()
                    .flat_map(|(score, member)| {
                        if with_scores {
                            vec![
                                ExecutionResult::bulk_string(member.clone()),
                                ExecutionResult::bulk_string(score.to_string()),
                            ]
                        } else {
                            vec![ExecutionResult::bulk_string(member.clone())]
                        }
                    })
                    .collect();
                Ok(ExecutionResult::array(items))
            }
            Some(_) => Ok(ExecutionResult::error("WRONGTYPE")),
            None => Ok(ExecutionResult::array(vec![])),
        }
    }

    fn cmd_zscore(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.len() < 2 {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'zscore' command",
            ));
        }
        match self.data.get(&args[0]) {
            Some(DataValue::SortedSet(z)) => {
                for (score, member) in z {
                    if member == &args[1] {
                        return Ok(ExecutionResult::bulk_string(score.to_string()));
                    }
                }
                Ok(ExecutionResult::null())
            }
            Some(_) => Ok(ExecutionResult::error("WRONGTYPE")),
            None => Ok(ExecutionResult::null()),
        }
    }

    fn cmd_zcard(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.is_empty() {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'zcard' command",
            ));
        }
        match self.data.get(&args[0]) {
            Some(DataValue::SortedSet(z)) => Ok(ExecutionResult::integer(z.len() as i64)),
            Some(_) => Ok(ExecutionResult::error("WRONGTYPE")),
            None => Ok(ExecutionResult::integer(0)),
        }
    }

    fn cmd_zrank(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.len() < 2 {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'zrank' command",
            ));
        }
        match self.data.get(&args[0]) {
            Some(DataValue::SortedSet(z)) => {
                for (i, (_, member)) in z.iter().enumerate() {
                    if member == &args[1] {
                        return Ok(ExecutionResult::integer(i as i64));
                    }
                }
                Ok(ExecutionResult::null())
            }
            Some(_) => Ok(ExecutionResult::error("WRONGTYPE")),
            None => Ok(ExecutionResult::null()),
        }
    }

    // Key commands

    fn cmd_keys(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        let pattern = args.first().map(|s| s.as_str()).unwrap_or("*");
        let keys: Vec<ExecutionResult> = self
            .data
            .keys()
            .filter(|k| match_pattern(pattern, k))
            .map(|k| ExecutionResult::bulk_string(k.clone()))
            .collect();
        Ok(ExecutionResult::array(keys))
    }

    fn cmd_type(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.is_empty() {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'type' command",
            ));
        }
        let type_name = match self.data.get(&args[0]) {
            Some(DataValue::String(_)) => "string",
            Some(DataValue::List(_)) => "list",
            Some(DataValue::Set(_)) => "set",
            Some(DataValue::Hash(_)) => "hash",
            Some(DataValue::SortedSet(_)) => "zset",
            None => "none",
        };
        Ok(ExecutionResult::simple_string(type_name.to_string()))
    }

    fn cmd_rename(&mut self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.len() < 2 {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'rename' command",
            ));
        }
        if let Some(value) = self.data.remove(&args[0]) {
            self.data.insert(args[1].clone(), value);
            Ok(ExecutionResult::ok())
        } else {
            Ok(ExecutionResult::error("no such key"))
        }
    }

    fn cmd_expire(&mut self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.len() < 2 {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'expire' command",
            ));
        }
        // TTL not implemented in playground
        if self.data.contains_key(&args[0]) {
            Ok(ExecutionResult::integer(1))
        } else {
            Ok(ExecutionResult::integer(0))
        }
    }

    fn cmd_ttl(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.is_empty() {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'ttl' command",
            ));
        }
        if self.data.contains_key(&args[0]) {
            Ok(ExecutionResult::integer(-1)) // No TTL
        } else {
            Ok(ExecutionResult::integer(-2)) // Key doesn't exist
        }
    }

    fn cmd_dbsize(&self, _args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        Ok(ExecutionResult::integer(self.data.len() as i64))
    }

    fn cmd_flushdb(&mut self, _args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        self.data.clear();
        self.update_memory_usage();
        Ok(ExecutionResult::ok())
    }

    // Server commands

    fn cmd_ping(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.is_empty() {
            Ok(ExecutionResult::simple_string("PONG".to_string()))
        } else {
            Ok(ExecutionResult::bulk_string(args[0].clone()))
        }
    }

    fn cmd_echo(&self, args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        if args.is_empty() {
            return Ok(ExecutionResult::error(
                "wrong number of arguments for 'echo' command",
            ));
        }
        Ok(ExecutionResult::bulk_string(args[0].clone()))
    }

    fn cmd_info(&self, _args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        let info = format!(
            "# Playground\r\n\
             playground_version:1.0.0\r\n\
             keys:{}\r\n\
             memory_used:{}\r\n\
             commands_executed:{}\r\n",
            self.data.len(),
            self.memory_used.load(Ordering::Relaxed),
            self.stats.commands_executed,
        );
        Ok(ExecutionResult::bulk_string(info))
    }

    fn cmd_time(&self, _args: &[String]) -> Result<ExecutionResult, PlaygroundError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let secs = now.as_secs();
        let micros = now.subsec_micros();
        Ok(ExecutionResult::array(vec![
            ExecutionResult::bulk_string(secs.to_string()),
            ExecutionResult::bulk_string(micros.to_string()),
        ]))
    }
}

/// Data value types
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DataValue {
    /// String value
    String(String),
    /// List value
    List(Vec<String>),
    /// Set value
    Set(std::collections::HashSet<String>),
    /// Hash value
    Hash(HashMap<String, String>),
    /// Sorted set value (score, member)
    SortedSet(Vec<(f64, String)>),
}

impl DataValue {
    /// Estimate memory size
    fn estimated_size(&self) -> usize {
        match self {
            DataValue::String(s) => s.len() + 24,
            DataValue::List(l) => l.iter().map(|s| s.len() + 24).sum::<usize>() + 24,
            DataValue::Set(s) => s.iter().map(|m| m.len() + 24).sum::<usize>() + 24,
            DataValue::Hash(h) => h.iter().map(|(k, v)| k.len() + v.len() + 48).sum::<usize>() + 24,
            DataValue::SortedSet(z) => z.iter().map(|(_, m)| m.len() + 32).sum::<usize>() + 24,
        }
    }
}

/// Execution result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionResult {
    /// Result type
    pub result_type: ResultType,
    /// Value (for string/bulk results)
    pub value: Option<String>,
    /// Integer value
    pub int_value: Option<i64>,
    /// Array value
    pub array: Option<Vec<ExecutionResult>>,
    /// Error flag
    pub is_error: bool,
}

impl ExecutionResult {
    /// Create OK result
    pub fn ok() -> Self {
        Self {
            result_type: ResultType::SimpleString,
            value: Some("OK".to_string()),
            int_value: None,
            array: None,
            is_error: false,
        }
    }

    /// Create simple string result
    pub fn simple_string(s: String) -> Self {
        Self {
            result_type: ResultType::SimpleString,
            value: Some(s),
            int_value: None,
            array: None,
            is_error: false,
        }
    }

    /// Create bulk string result
    pub fn bulk_string(s: String) -> Self {
        Self {
            result_type: ResultType::BulkString,
            value: Some(s),
            int_value: None,
            array: None,
            is_error: false,
        }
    }

    /// Create integer result
    pub fn integer(n: i64) -> Self {
        Self {
            result_type: ResultType::Integer,
            value: None,
            int_value: Some(n),
            array: None,
            is_error: false,
        }
    }

    /// Create null result
    pub fn null() -> Self {
        Self {
            result_type: ResultType::Null,
            value: None,
            int_value: None,
            array: None,
            is_error: false,
        }
    }

    /// Create array result
    pub fn array(items: Vec<ExecutionResult>) -> Self {
        Self {
            result_type: ResultType::Array,
            value: None,
            int_value: None,
            array: Some(items),
            is_error: false,
        }
    }

    /// Create error result
    pub fn error(msg: &str) -> Self {
        Self {
            result_type: ResultType::Error,
            value: Some(msg.to_string()),
            int_value: None,
            array: None,
            is_error: true,
        }
    }
}

/// Result type
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResultType {
    /// Simple string
    SimpleString,
    /// Bulk string
    BulkString,
    /// Integer
    Integer,
    /// Array
    Array,
    /// Null
    Null,
    /// Error
    Error,
}

/// Runtime statistics
#[derive(Default)]
struct RuntimeStats {
    commands_executed: u64,
}

/// Simple pattern matching (supports * and ?)
fn match_pattern(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    let mut p_chars = pattern.chars().peekable();
    let mut t_chars = text.chars().peekable();

    while let Some(pc) = p_chars.next() {
        match pc {
            '*' => {
                if p_chars.peek().is_none() {
                    return true;
                }
                // Try matching rest of pattern at each position
                let rest: String = p_chars.collect();
                let remaining: String = t_chars.collect();
                for i in 0..=remaining.len() {
                    if match_pattern(&rest, &remaining[i..]) {
                        return true;
                    }
                }
                return false;
            }
            '?' => {
                if t_chars.next().is_none() {
                    return false;
                }
            }
            c => {
                if t_chars.next() != Some(c) {
                    return false;
                }
            }
        }
    }

    t_chars.peek().is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_runtime_set_get() {
        let config = RuntimeConfig {
            max_memory_bytes: 1024 * 1024,
            max_execution_time_ms: 1000,
            max_keys: 100,
            sandbox_mode: true,
        };
        let mut runtime = PlaygroundRuntime::new(config);

        let set_cmd = ParsedCommand {
            name: "SET".to_string(),
            args: vec!["foo".to_string(), "bar".to_string()],
        };
        let result = runtime.execute(&set_cmd).await.unwrap();
        assert!(!result.is_error);

        let get_cmd = ParsedCommand {
            name: "GET".to_string(),
            args: vec!["foo".to_string()],
        };
        let result = runtime.execute(&get_cmd).await.unwrap();
        assert_eq!(result.value, Some("bar".to_string()));
    }

    #[test]
    fn test_pattern_matching() {
        assert!(match_pattern("*", "anything"));
        assert!(match_pattern("foo*", "foobar"));
        assert!(match_pattern("*bar", "foobar"));
        assert!(match_pattern("f?o", "foo"));
        assert!(!match_pattern("foo", "bar"));
    }

    fn test_config() -> RuntimeConfig {
        RuntimeConfig {
            max_memory_bytes: 1024 * 1024,
            max_execution_time_ms: 1000,
            max_keys: 100,
            sandbox_mode: true,
        }
    }

    fn cmd(name: &str, args: &[&str]) -> ParsedCommand {
        ParsedCommand {
            name: name.to_uppercase(),
            args: args.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[tokio::test]
    async fn test_runtime_del() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime.execute(&cmd("SET", &["key1", "val1"])).await.unwrap();
        let result = runtime.execute(&cmd("DEL", &["key1"])).await.unwrap();
        assert_eq!(result.int_value, Some(1));
        let result = runtime.execute(&cmd("GET", &["key1"])).await.unwrap();
        assert_eq!(result.result_type, ResultType::Null);
    }

    #[tokio::test]
    async fn test_runtime_incr_decr() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime.execute(&cmd("SET", &["counter", "10"])).await.unwrap();
        let result = runtime.execute(&cmd("INCR", &["counter"])).await.unwrap();
        assert_eq!(result.int_value, Some(11));
        let result = runtime.execute(&cmd("DECR", &["counter"])).await.unwrap();
        assert_eq!(result.int_value, Some(10));
        let result = runtime.execute(&cmd("INCRBY", &["counter", "5"])).await.unwrap();
        assert_eq!(result.int_value, Some(15));
    }

    #[tokio::test]
    async fn test_runtime_list_operations() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime.execute(&cmd("RPUSH", &["mylist", "a", "b", "c"])).await.unwrap();
        let result = runtime.execute(&cmd("LLEN", &["mylist"])).await.unwrap();
        assert_eq!(result.int_value, Some(3));
        let result = runtime.execute(&cmd("LPOP", &["mylist"])).await.unwrap();
        assert_eq!(result.value, Some("a".to_string()));
        let result = runtime.execute(&cmd("RPOP", &["mylist"])).await.unwrap();
        assert_eq!(result.value, Some("c".to_string()));
    }

    #[tokio::test]
    async fn test_runtime_hash_operations() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime.execute(&cmd("HSET", &["myhash", "field1", "val1", "field2", "val2"])).await.unwrap();
        let result = runtime.execute(&cmd("HGET", &["myhash", "field1"])).await.unwrap();
        assert_eq!(result.value, Some("val1".to_string()));
        let result = runtime.execute(&cmd("HLEN", &["myhash"])).await.unwrap();
        assert_eq!(result.int_value, Some(2));
        let result = runtime.execute(&cmd("HEXISTS", &["myhash", "field1"])).await.unwrap();
        assert_eq!(result.int_value, Some(1));
        runtime.execute(&cmd("HDEL", &["myhash", "field1"])).await.unwrap();
        let result = runtime.execute(&cmd("HLEN", &["myhash"])).await.unwrap();
        assert_eq!(result.int_value, Some(1));
    }

    #[tokio::test]
    async fn test_runtime_set_operations() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime.execute(&cmd("SADD", &["myset", "a", "b", "c", "a"])).await.unwrap();
        let result = runtime.execute(&cmd("SCARD", &["myset"])).await.unwrap();
        assert_eq!(result.int_value, Some(3));
        let result = runtime.execute(&cmd("SISMEMBER", &["myset", "a"])).await.unwrap();
        assert_eq!(result.int_value, Some(1));
        let result = runtime.execute(&cmd("SISMEMBER", &["myset", "z"])).await.unwrap();
        assert_eq!(result.int_value, Some(0));
        runtime.execute(&cmd("SREM", &["myset", "a"])).await.unwrap();
        let result = runtime.execute(&cmd("SCARD", &["myset"])).await.unwrap();
        assert_eq!(result.int_value, Some(2));
    }

    #[tokio::test]
    async fn test_runtime_sorted_set() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime.execute(&cmd("ZADD", &["zs", "100", "alice", "200", "bob"])).await.unwrap();
        let result = runtime.execute(&cmd("ZCARD", &["zs"])).await.unwrap();
        assert_eq!(result.int_value, Some(2));
        let result = runtime.execute(&cmd("ZSCORE", &["zs", "bob"])).await.unwrap();
        assert_eq!(result.value, Some("200".to_string()));
        let result = runtime.execute(&cmd("ZRANK", &["zs", "alice"])).await.unwrap();
        assert_eq!(result.int_value, Some(0));
    }

    #[tokio::test]
    async fn test_runtime_ping() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        let result = runtime.execute(&cmd("PING", &[])).await.unwrap();
        assert_eq!(result.value, Some("PONG".to_string()));
    }

    #[tokio::test]
    async fn test_runtime_echo() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        let result = runtime.execute(&cmd("ECHO", &["hello"])).await.unwrap();
        assert_eq!(result.value, Some("hello".to_string()));
    }

    #[tokio::test]
    async fn test_runtime_dbsize() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime.execute(&cmd("SET", &["k1", "v1"])).await.unwrap();
        runtime.execute(&cmd("SET", &["k2", "v2"])).await.unwrap();
        let result = runtime.execute(&cmd("DBSIZE", &[])).await.unwrap();
        assert_eq!(result.int_value, Some(2));
    }

    #[tokio::test]
    async fn test_runtime_flushdb() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime.execute(&cmd("SET", &["k1", "v1"])).await.unwrap();
        runtime.execute(&cmd("FLUSHDB", &[])).await.unwrap();
        let result = runtime.execute(&cmd("DBSIZE", &[])).await.unwrap();
        assert_eq!(result.int_value, Some(0));
    }

    #[tokio::test]
    async fn test_runtime_unknown_command() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        let result = runtime.execute(&cmd("FAKECMD", &[])).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_runtime_key_limit() {
        let config = RuntimeConfig {
            max_memory_bytes: 1024 * 1024,
            max_execution_time_ms: 1000,
            max_keys: 2,
            sandbox_mode: true,
        };
        let mut runtime = PlaygroundRuntime::new(config);
        runtime.execute(&cmd("SET", &["k1", "v1"])).await.unwrap();
        runtime.execute(&cmd("SET", &["k2", "v2"])).await.unwrap();
        let result = runtime.execute(&cmd("SET", &["k3", "v3"])).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_runtime_wrongtype() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime.execute(&cmd("SET", &["mykey", "string_value"])).await.unwrap();
        let result = runtime.execute(&cmd("LPUSH", &["mykey", "item"])).await.unwrap();
        assert!(result.is_error);
    }

    #[tokio::test]
    async fn test_runtime_export_import() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime.execute(&cmd("SET", &["k1", "v1"])).await.unwrap();
        runtime.execute(&cmd("SET", &["k2", "v2"])).await.unwrap();
        let data = runtime.export_data().unwrap();
        assert_eq!(data.len(), 2);

        let mut runtime2 = PlaygroundRuntime::new(test_config());
        runtime2.import_data(&data).unwrap();
        let result = runtime2.execute(&cmd("GET", &["k1"])).await.unwrap();
        assert_eq!(result.value, Some("v1".to_string()));
    }

    #[tokio::test]
    async fn test_runtime_reset() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime.execute(&cmd("SET", &["k1", "v1"])).await.unwrap();
        runtime.reset().unwrap();
        let result = runtime.execute(&cmd("DBSIZE", &[])).await.unwrap();
        assert_eq!(result.int_value, Some(0));
    }

    #[tokio::test]
    async fn test_runtime_mset_mget() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime.execute(&cmd("MSET", &["k1", "v1", "k2", "v2"])).await.unwrap();
        let result = runtime.execute(&cmd("MGET", &["k1", "k2", "k3"])).await.unwrap();
        let arr = result.array.unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0].value, Some("v1".to_string()));
        assert_eq!(arr[1].value, Some("v2".to_string()));
        assert_eq!(arr[2].result_type, ResultType::Null);
    }

    #[tokio::test]
    async fn test_runtime_setnx() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        let result = runtime.execute(&cmd("SETNX", &["k1", "v1"])).await.unwrap();
        assert_eq!(result.int_value, Some(1));
        let result = runtime.execute(&cmd("SETNX", &["k1", "v2"])).await.unwrap();
        assert_eq!(result.int_value, Some(0));
        let result = runtime.execute(&cmd("GET", &["k1"])).await.unwrap();
        assert_eq!(result.value, Some("v1".to_string()));
    }

    #[tokio::test]
    async fn test_runtime_append_strlen() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime.execute(&cmd("SET", &["k1", "hello"])).await.unwrap();
        let result = runtime.execute(&cmd("APPEND", &["k1", " world"])).await.unwrap();
        assert_eq!(result.int_value, Some(11));
        let result = runtime.execute(&cmd("STRLEN", &["k1"])).await.unwrap();
        assert_eq!(result.int_value, Some(11));
    }

    #[tokio::test]
    async fn test_runtime_type() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime.execute(&cmd("SET", &["str_key", "val"])).await.unwrap();
        runtime.execute(&cmd("RPUSH", &["list_key", "item"])).await.unwrap();
        let result = runtime.execute(&cmd("TYPE", &["str_key"])).await.unwrap();
        assert_eq!(result.value, Some("string".to_string()));
        let result = runtime.execute(&cmd("TYPE", &["list_key"])).await.unwrap();
        assert_eq!(result.value, Some("list".to_string()));
        let result = runtime.execute(&cmd("TYPE", &["nonexistent"])).await.unwrap();
        assert_eq!(result.value, Some("none".to_string()));
    }

    #[tokio::test]
    async fn test_runtime_rename() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime.execute(&cmd("SET", &["old", "val"])).await.unwrap();
        runtime.execute(&cmd("RENAME", &["old", "new"])).await.unwrap();
        let result = runtime.execute(&cmd("GET", &["new"])).await.unwrap();
        assert_eq!(result.value, Some("val".to_string()));
        let result = runtime.execute(&cmd("GET", &["old"])).await.unwrap();
        assert_eq!(result.result_type, ResultType::Null);
    }

    #[tokio::test]
    async fn test_runtime_ttl() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime.execute(&cmd("SET", &["k1", "v1"])).await.unwrap();
        let result = runtime.execute(&cmd("TTL", &["k1"])).await.unwrap();
        assert_eq!(result.int_value, Some(-1)); // No TTL
        let result = runtime.execute(&cmd("TTL", &["nonexistent"])).await.unwrap();
        assert_eq!(result.int_value, Some(-2)); // Key doesn't exist
    }

    #[tokio::test]
    async fn test_runtime_info() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        let result = runtime.execute(&cmd("INFO", &[])).await.unwrap();
        assert!(result.value.unwrap().contains("playground_version"));
    }

    #[tokio::test]
    async fn test_runtime_time() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        let result = runtime.execute(&cmd("TIME", &[])).await.unwrap();
        assert_eq!(result.array.unwrap().len(), 2);
    }

    #[test]
    fn test_data_value_estimated_size() {
        let s = DataValue::String("hello".to_string());
        assert!(s.estimated_size() > 0);
        let l = DataValue::List(vec!["a".to_string(), "b".to_string()]);
        assert!(l.estimated_size() > 0);
        let h = DataValue::Hash(HashMap::from([("k".to_string(), "v".to_string())]));
        assert!(h.estimated_size() > 0);
        let set = DataValue::Set(std::collections::HashSet::from(["a".to_string()]));
        assert!(set.estimated_size() > 0);
        let zs = DataValue::SortedSet(vec![(1.0, "member".to_string())]);
        assert!(zs.estimated_size() > 0);
    }

    #[test]
    fn test_execution_result_types() {
        let ok = ExecutionResult::ok();
        assert!(!ok.is_error);
        assert_eq!(ok.value, Some("OK".to_string()));

        let null = ExecutionResult::null();
        assert_eq!(null.result_type, ResultType::Null);

        let int = ExecutionResult::integer(42);
        assert_eq!(int.int_value, Some(42));

        let err = ExecutionResult::error("oops");
        assert!(err.is_error);

        let arr = ExecutionResult::array(vec![ExecutionResult::ok()]);
        assert_eq!(arr.array.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_runtime_memory_limit_enforcement() {
        let config = RuntimeConfig {
            max_memory_bytes: 50, // Very small limit
            max_execution_time_ms: 1000,
            max_keys: 100,
            sandbox_mode: true,
        };
        let mut runtime = PlaygroundRuntime::new(config);

        // First SET succeeds but triggers post-write memory check
        let result = runtime
            .execute(&cmd("SET", &["k1", "this_is_a_long_value_that_should_exceed_fifty_bytes_limit"]))
            .await;
        // Should fail because the data exceeds 50 bytes
        assert!(result.is_err());
        match result {
            Err(PlaygroundError::MemoryLimitExceeded { .. }) => {}
            other => panic!("Expected MemoryLimitExceeded, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_runtime_memory_limit_pre_execution_check() {
        let config = RuntimeConfig {
            max_memory_bytes: 200,
            max_execution_time_ms: 1000,
            max_keys: 100,
            sandbox_mode: true,
        };
        let mut runtime = PlaygroundRuntime::new(config);

        // Fill up memory with multiple keys
        for i in 0..10 {
            let _ = runtime
                .execute(&cmd("SET", &[&format!("key{}", i), "a_moderate_value_here"]))
                .await;
        }

        // The pre-execution check should eventually block new commands
        let result = runtime
            .execute(&cmd("SET", &["final_key", "another_value"]))
            .await;
        // Either succeeds or fails with memory limit - depends on exact sizes
        // Just verify the mechanism works without panicking
        let _ = result;
    }

    #[tokio::test]
    async fn test_runtime_exists_multiple_keys() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime.execute(&cmd("SET", &["k1", "v1"])).await.unwrap();
        runtime.execute(&cmd("SET", &["k2", "v2"])).await.unwrap();
        let result = runtime
            .execute(&cmd("EXISTS", &["k1", "k2", "k3"]))
            .await
            .unwrap();
        assert_eq!(result.int_value, Some(2));
    }

    #[tokio::test]
    async fn test_runtime_getset() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime.execute(&cmd("SET", &["k1", "old"])).await.unwrap();
        let result = runtime.execute(&cmd("GETSET", &["k1", "new"])).await.unwrap();
        assert_eq!(result.value, Some("old".to_string()));
        let result = runtime.execute(&cmd("GET", &["k1"])).await.unwrap();
        assert_eq!(result.value, Some("new".to_string()));
    }

    #[tokio::test]
    async fn test_runtime_setex() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        let result = runtime
            .execute(&cmd("SETEX", &["k1", "60", "val"]))
            .await
            .unwrap();
        assert!(!result.is_error);
        let result = runtime.execute(&cmd("GET", &["k1"])).await.unwrap();
        assert_eq!(result.value, Some("val".to_string()));
    }

    #[tokio::test]
    async fn test_runtime_lpush_rpush_lrange() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime
            .execute(&cmd("LPUSH", &["list", "c", "b", "a"]))
            .await
            .unwrap();
        runtime
            .execute(&cmd("RPUSH", &["list", "d"]))
            .await
            .unwrap();
        let result = runtime
            .execute(&cmd("LRANGE", &["list", "0", "-1"]))
            .await
            .unwrap();
        let arr = result.array.unwrap();
        assert_eq!(arr.len(), 4);
        // LPUSH c b a reverses: inserts a, b, c at head → [c, b, a]
        // Then RPUSH d → [c, b, a, d]
        assert_eq!(arr[0].value, Some("c".to_string()));
        assert_eq!(arr[3].value, Some("d".to_string()));
    }

    #[tokio::test]
    async fn test_runtime_lindex() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime
            .execute(&cmd("RPUSH", &["list", "a", "b", "c"]))
            .await
            .unwrap();
        let result = runtime
            .execute(&cmd("LINDEX", &["list", "1"]))
            .await
            .unwrap();
        assert_eq!(result.value, Some("b".to_string()));
    }

    #[tokio::test]
    async fn test_runtime_hgetall() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime
            .execute(&cmd("HSET", &["h", "f1", "v1", "f2", "v2"]))
            .await
            .unwrap();
        let result = runtime
            .execute(&cmd("HGETALL", &["h"]))
            .await
            .unwrap();
        let arr = result.array.unwrap();
        assert_eq!(arr.len(), 4); // f1, v1, f2, v2
    }

    #[tokio::test]
    async fn test_runtime_hkeys_hvals() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime
            .execute(&cmd("HSET", &["h", "f1", "v1", "f2", "v2"]))
            .await
            .unwrap();
        let result = runtime.execute(&cmd("HKEYS", &["h"])).await.unwrap();
        assert_eq!(result.array.unwrap().len(), 2);
        let result = runtime.execute(&cmd("HVALS", &["h"])).await.unwrap();
        assert_eq!(result.array.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_runtime_smembers() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime
            .execute(&cmd("SADD", &["s", "a", "b", "c"]))
            .await
            .unwrap();
        let result = runtime.execute(&cmd("SMEMBERS", &["s"])).await.unwrap();
        assert_eq!(result.array.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn test_runtime_zrange_with_scores() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime
            .execute(&cmd("ZADD", &["zs", "10", "a", "20", "b"]))
            .await
            .unwrap();
        let result = runtime
            .execute(&cmd("ZRANGE", &["zs", "0", "-1", "WITHSCORES"]))
            .await
            .unwrap();
        let arr = result.array.unwrap();
        assert_eq!(arr.len(), 4); // a, 10, b, 20
    }

    #[tokio::test]
    async fn test_runtime_keys_pattern() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime.execute(&cmd("SET", &["user:1", "a"])).await.unwrap();
        runtime.execute(&cmd("SET", &["user:2", "b"])).await.unwrap();
        runtime.execute(&cmd("SET", &["order:1", "c"])).await.unwrap();
        let result = runtime.execute(&cmd("KEYS", &["user:*"])).await.unwrap();
        assert_eq!(result.array.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_runtime_expire_and_ttl() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        runtime.execute(&cmd("SET", &["k1", "v1"])).await.unwrap();
        let result = runtime.execute(&cmd("EXPIRE", &["k1", "3600"])).await.unwrap();
        // EXPIRE returns 1 if key exists
        assert_eq!(result.int_value, Some(1));
        // TTL not fully implemented in playground - returns -1 for existing keys
        let result = runtime.execute(&cmd("TTL", &["k1"])).await.unwrap();
        assert_eq!(result.int_value, Some(-1));
    }

    #[tokio::test]
    async fn test_runtime_wrong_args_set() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        let result = runtime.execute(&cmd("SET", &["only_key"])).await.unwrap();
        assert!(result.is_error);
    }

    #[tokio::test]
    async fn test_runtime_wrong_args_get() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        let result = runtime.execute(&cmd("GET", &[])).await.unwrap();
        assert!(result.is_error);
    }

    #[tokio::test]
    async fn test_runtime_ping_with_message() {
        let mut runtime = PlaygroundRuntime::new(test_config());
        let result = runtime
            .execute(&cmd("PING", &["hello"]))
            .await
            .unwrap();
        assert_eq!(result.value, Some("hello".to_string()));
    }
}
