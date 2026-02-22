//! Lua scripting commands
//!
//! This module implements Redis-compatible Lua scripting commands (EVAL, EVALSHA, SCRIPT).
//! It provides a full Lua 5.4 environment with `redis.call`, `redis.pcall`,
//! `redis.status_reply`, `redis.error_reply`, `redis.log`, and `redis.sha1hex`.
//!
//! Type conversions follow the Redis specification:
//!   - Integer reply -> Lua number
//!   - Bulk string -> Lua string
//!   - Array -> Lua table (1-indexed)
//!   - Nil/Null -> Lua `false`
//!   - Lua number -> Integer reply
//!   - Lua string -> Bulk string
//!   - Lua table with "ok" -> Status reply
//!   - Lua table with "err" -> Error reply
//!   - Lua table (array) -> Array reply
//!   - Lua boolean false/nil -> Null reply
//!   - Lua boolean true -> Integer 1

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use mlua::{Lua, MultiValue, Result as LuaResult, Value as LuaValue};
use parking_lot::RwLock;
use tracing;

use crate::protocol::Frame;
use crate::storage::Store;

/// SHA1 hash of a script
pub type ScriptHash = String;

/// Redis log levels for redis.log()
const LOG_DEBUG: i64 = 0;
const LOG_VERBOSE: i64 = 1;
const LOG_NOTICE: i64 = 2;
const LOG_WARNING: i64 = 3;

/// Script cache for storing compiled scripts
#[derive(Default)]
pub struct ScriptCache {
    /// Map from SHA1 hash to script source
    scripts: RwLock<HashMap<ScriptHash, String>>,
}

impl ScriptCache {
    /// Create a new empty script cache
    pub fn new() -> Self {
        Self {
            scripts: RwLock::new(HashMap::new()),
        }
    }

    /// Load a script and return its SHA1 hash
    pub fn load(&self, script: &str) -> ScriptHash {
        let hash = sha1_hex(script.as_bytes());
        self.scripts
            .write()
            .insert(hash.clone(), script.to_string());
        hash
    }

    /// Get a script by its SHA1 hash
    pub fn get(&self, hash: &str) -> Option<String> {
        self.scripts.read().get(hash).cloned()
    }

    /// Check if a script exists by its SHA1 hash
    pub fn exists(&self, hash: &str) -> bool {
        self.scripts.read().contains_key(hash)
    }

    /// Flush all cached scripts
    pub fn flush(&self) {
        self.scripts.write().clear();
    }
}

/// Calculate SHA1 hash of data and return the lowercase hex string.
///
/// This is a self-contained SHA-1 implementation (FIPS 180-4) that produces
/// hashes compatible with Redis EVALSHA lookups. The algorithm processes
/// 512-bit blocks and produces a 160-bit (20-byte / 40 hex char) digest.
#[allow(clippy::many_single_char_names)]
fn sha1_hex(data: &[u8]) -> String {
    // Initial hash values (FIPS 180-4 section 5.3.1)
    let mut h0: u32 = 0x67452301;
    let mut h1: u32 = 0xEFCDAB89;
    let mut h2: u32 = 0x98BADCFE;
    let mut h3: u32 = 0x10325476;
    let mut h4: u32 = 0xC3D2E1F0;

    // Pre-processing: adding padding bits
    let bit_len = (data.len() as u64) * 8;
    let mut msg = data.to_vec();
    msg.push(0x80);
    while (msg.len() % 64) != 56 {
        msg.push(0x00);
    }
    // Append original length in bits as 64-bit big-endian
    msg.extend_from_slice(&bit_len.to_be_bytes());

    // Process each 512-bit (64-byte) block
    for chunk in msg.chunks(64) {
        let mut w = [0u32; 80];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                chunk[i * 4],
                chunk[i * 4 + 1],
                chunk[i * 4 + 2],
                chunk[i * 4 + 3],
            ]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }

        let mut a = h0;
        let mut b = h1;
        let mut c = h2;
        let mut d = h3;
        let mut e = h4;

        for (i, item) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A827999u32),
                20..=39 => (b ^ c ^ d, 0x6ED9EBA1u32),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDCu32),
                _ => (b ^ c ^ d, 0xCA62C1D6u32),
            };

            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(*item);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }

        h0 = h0.wrapping_add(a);
        h1 = h1.wrapping_add(b);
        h2 = h2.wrapping_add(c);
        h3 = h3.wrapping_add(d);
        h4 = h4.wrapping_add(e);
    }

    format!(
        "{:08x}{:08x}{:08x}{:08x}{:08x}",
        h0, h1, h2, h3, h4
    )
}

/// Lua script executor
pub struct ScriptExecutor {
    cache: Arc<ScriptCache>,
    store: Arc<Store>,
}

impl ScriptExecutor {
    /// Create a new script executor with the given cache and store
    pub fn new(cache: Arc<ScriptCache>, store: Arc<Store>) -> Self {
        Self { cache, store }
    }

    /// Execute a Lua script (EVAL)
    pub fn eval(&self, db: u8, script: &str, keys: &[Bytes], args: &[Bytes]) -> Frame {
        // Cache the script
        self.cache.load(script);

        self.execute_script(db, script, keys, args)
    }

    /// Execute a cached script by SHA1 hash (EVALSHA)
    pub fn evalsha(&self, db: u8, sha1: &str, keys: &[Bytes], args: &[Bytes]) -> Frame {
        match self.cache.get(sha1) {
            Some(script) => self.execute_script(db, &script, keys, args),
            None => Frame::error("NOSCRIPT No matching script. Please use EVAL."),
        }
    }

    /// Execute script source in a sandboxed Lua environment
    fn execute_script(&self, db: u8, script: &str, keys: &[Bytes], args: &[Bytes]) -> Frame {
        let lua = Lua::new();

        // Set up KEYS, ARGV, and redis.* functions
        if let Err(e) = self.setup_lua_env(&lua, db, keys, args) {
            return Frame::error(format!("ERR Error running script: {}", e));
        }

        // Execute the script
        match lua.load(script).eval::<LuaValue>() {
            Ok(value) => lua_value_to_frame(value),
            Err(e) => Frame::error(format!("ERR Error running script: {}", e)),
        }
    }

    /// Set up Lua environment with KEYS, ARGV, and redis.* functions
    fn setup_lua_env(&self, lua: &Lua, db: u8, keys: &[Bytes], args: &[Bytes]) -> LuaResult<()> {
        let globals = lua.globals();

        // Create KEYS table (1-indexed, as per Redis convention)
        let keys_table = lua.create_table()?;
        for (i, key) in keys.iter().enumerate() {
            keys_table.set(i + 1, String::from_utf8_lossy(key).to_string())?;
        }
        globals.set("KEYS", keys_table)?;

        // Create ARGV table (1-indexed)
        let argv_table = lua.create_table()?;
        for (i, arg) in args.iter().enumerate() {
            argv_table.set(i + 1, String::from_utf8_lossy(arg).to_string())?;
        }
        globals.set("ARGV", argv_table)?;

        // Create redis table with call, pcall, and helper functions
        let redis_table = lua.create_table()?;

        // redis.call(cmd, ...) -- Execute Redis command, raise error on failure
        let store_clone = self.store.clone();
        let call_fn = lua.create_function(move |lua_ctx, args: MultiValue| {
            redis_call(lua_ctx, &store_clone, db, args, false)
        })?;
        redis_table.set("call", call_fn)?;

        // redis.pcall(cmd, ...) -- Execute Redis command, return error as table
        let store_clone2 = self.store.clone();
        let pcall_fn = lua.create_function(move |lua_ctx, args: MultiValue| {
            redis_call(lua_ctx, &store_clone2, db, args, true)
        })?;
        redis_table.set("pcall", pcall_fn)?;

        // redis.status_reply(msg) -- Return a status reply table {ok = msg}
        let status_reply_fn = lua.create_function(|lua_ctx, msg: String| {
            let table = lua_ctx.create_table()?;
            table.set("ok", msg)?;
            Ok(LuaValue::Table(table))
        })?;
        redis_table.set("status_reply", status_reply_fn)?;

        // redis.error_reply(msg) -- Return an error reply table {err = msg}
        let error_reply_fn = lua.create_function(|lua_ctx, msg: String| {
            let table = lua_ctx.create_table()?;
            table.set("err", msg)?;
            Ok(LuaValue::Table(table))
        })?;
        redis_table.set("error_reply", error_reply_fn)?;

        // redis.log(level, msg) -- Log a message at the given level
        let log_fn = lua.create_function(|_lua_ctx, (level, msg): (i64, String)| {
            match level {
                l if l == LOG_DEBUG => tracing::debug!(target: "lua_script", "{}", msg),
                l if l == LOG_VERBOSE => tracing::info!(target: "lua_script", "{}", msg),
                l if l == LOG_NOTICE => tracing::info!(target: "lua_script", "{}", msg),
                l if l == LOG_WARNING => tracing::warn!(target: "lua_script", "{}", msg),
                _ => tracing::info!(target: "lua_script", "{}", msg),
            }
            Ok(())
        })?;
        redis_table.set("log", log_fn)?;

        // redis.sha1hex(data) -- Return the SHA1 hex digest of a string
        let sha1hex_fn = lua.create_function(|_lua_ctx, data: String| {
            Ok(sha1_hex(data.as_bytes()))
        })?;
        redis_table.set("sha1hex", sha1hex_fn)?;

        // Redis log level constants
        redis_table.set("LOG_DEBUG", LOG_DEBUG)?;
        redis_table.set("LOG_VERBOSE", LOG_VERBOSE)?;
        redis_table.set("LOG_NOTICE", LOG_NOTICE)?;
        redis_table.set("LOG_WARNING", LOG_WARNING)?;

        globals.set("redis", redis_table)?;

        Ok(())
    }

    /// Load a script into cache (SCRIPT LOAD)
    pub fn script_load(&self, script: &str) -> Frame {
        let hash = self.cache.load(script);
        Frame::bulk(Bytes::from(hash))
    }

    /// Check if scripts exist (SCRIPT EXISTS)
    pub fn script_exists(&self, hashes: &[String]) -> Frame {
        let results: Vec<Frame> = hashes
            .iter()
            .map(|h| Frame::Integer(if self.cache.exists(h) { 1 } else { 0 }))
            .collect();
        Frame::array(results)
    }

    /// Flush all scripts (SCRIPT FLUSH)
    pub fn script_flush(&self) -> Frame {
        self.cache.flush();
        Frame::simple("OK")
    }
}

/// Execute a Redis command from Lua (used by redis.call and redis.pcall)
fn redis_call(
    lua: &Lua,
    store: &Store,
    db: u8,
    args: MultiValue,
    is_pcall: bool,
) -> LuaResult<LuaValue> {
    let args_vec: Vec<LuaValue> = args.into_iter().collect();

    if args_vec.is_empty() {
        if is_pcall {
            let table = lua.create_table()?;
            table.set(
                "err",
                "Please specify at least one argument for redis.call()",
            )?;
            return Ok(LuaValue::Table(table));
        }
        return Err(mlua::Error::RuntimeError(
            "Please specify at least one argument for redis.call()".to_string(),
        ));
    }

    // Get command name (first argument must be a string)
    let cmd_name = match &args_vec[0] {
        LuaValue::String(s) => s.to_str()?.to_uppercase(),
        _ => {
            if is_pcall {
                let table = lua.create_table()?;
                table.set("err", "Wrong type for argument 1")?;
                return Ok(LuaValue::Table(table));
            }
            return Err(mlua::Error::RuntimeError(
                "Wrong type for argument 1".to_string(),
            ));
        }
    };

    // Convert remaining args to bytes
    let cmd_args: Vec<Bytes> = args_vec[1..]
        .iter()
        .filter_map(lua_value_to_bytes)
        .collect();

    // Execute the command directly on the store
    let result = execute_redis_command(store, db, &cmd_name, &cmd_args);

    // Convert result back to Lua value
    frame_to_lua_value(lua, result, is_pcall)
}

/// Convert Lua value to Bytes for passing as Redis command arguments
fn lua_value_to_bytes(value: &LuaValue) -> Option<Bytes> {
    match value {
        LuaValue::String(s) => Some(Bytes::from(s.as_bytes().to_vec())),
        LuaValue::Integer(n) => Some(Bytes::from(n.to_string())),
        LuaValue::Number(n) => Some(Bytes::from(n.to_string())),
        LuaValue::Boolean(b) => Some(Bytes::from(if *b { "1" } else { "0" })),
        _ => None,
    }
}

/// Execute a Redis command on the store from within a Lua script.
///
/// This function handles the subset of Redis commands that are commonly used
/// from Lua scripts. Additional commands can be added as needed.
fn execute_redis_command(store: &Store, db: u8, cmd: &str, args: &[Bytes]) -> Frame {
    use crate::storage::Value;
    use std::collections::{HashMap as StdHashMap, HashSet, VecDeque};

    match cmd {
        "GET" => {
            if args.len() != 1 {
                return Frame::error("ERR wrong number of arguments for 'get' command");
            }
            match store.get(db, &args[0]) {
                Some(Value::String(data)) => Frame::bulk(data),
                Some(_) => Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ),
                None => Frame::null(),
            }
        }
        "SET" => {
            if args.len() < 2 {
                return Frame::error("ERR wrong number of arguments for 'set' command");
            }
            store.set(db, args[0].clone(), Value::String(args[1].clone()));
            Frame::simple("OK")
        }
        "DEL" => {
            if args.is_empty() {
                return Frame::error("ERR wrong number of arguments for 'del' command");
            }
            Frame::Integer(store.del(db, args))
        }
        "EXISTS" => {
            if args.is_empty() {
                return Frame::error("ERR wrong number of arguments for 'exists' command");
            }
            Frame::Integer(store.exists(db, args))
        }
        "INCR" => {
            if args.len() != 1 {
                return Frame::error("ERR wrong number of arguments for 'incr' command");
            }
            incr_by(store, db, &args[0], 1)
        }
        "DECR" => {
            if args.len() != 1 {
                return Frame::error("ERR wrong number of arguments for 'decr' command");
            }
            incr_by(store, db, &args[0], -1)
        }
        "INCRBY" => {
            if args.len() != 2 {
                return Frame::error("ERR wrong number of arguments for 'incrby' command");
            }
            let delta: i64 = match String::from_utf8_lossy(&args[1]).parse() {
                Ok(v) => v,
                Err(_) => return Frame::error("ERR value is not an integer or out of range"),
            };
            incr_by(store, db, &args[0], delta)
        }
        "DECRBY" => {
            if args.len() != 2 {
                return Frame::error("ERR wrong number of arguments for 'decrby' command");
            }
            let delta: i64 = match String::from_utf8_lossy(&args[1]).parse() {
                Ok(v) => v,
                Err(_) => return Frame::error("ERR value is not an integer or out of range"),
            };
            incr_by(store, db, &args[0], -delta)
        }
        "MGET" => {
            if args.is_empty() {
                return Frame::error("ERR wrong number of arguments for 'mget' command");
            }
            let results: Vec<Frame> = args
                .iter()
                .map(|key| match store.get(db, key) {
                    Some(Value::String(data)) => Frame::bulk(data),
                    _ => Frame::null(),
                })
                .collect();
            Frame::array(results)
        }
        "MSET" => {
            if args.len() < 2 || args.len() % 2 != 0 {
                return Frame::error("ERR wrong number of arguments for 'mset' command");
            }
            for chunk in args.chunks(2) {
                store.set(db, chunk[0].clone(), Value::String(chunk[1].clone()));
            }
            Frame::simple("OK")
        }
        "APPEND" => {
            if args.len() != 2 {
                return Frame::error("ERR wrong number of arguments for 'append' command");
            }
            match store.get(db, &args[0]) {
                Some(Value::String(existing)) => {
                    let mut new_val = existing.to_vec();
                    new_val.extend_from_slice(&args[1]);
                    let len = new_val.len() as i64;
                    store.set(db, args[0].clone(), Value::String(Bytes::from(new_val)));
                    Frame::Integer(len)
                }
                Some(_) => Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ),
                None => {
                    let len = args[1].len() as i64;
                    store.set(db, args[0].clone(), Value::String(args[1].clone()));
                    Frame::Integer(len)
                }
            }
        }
        "STRLEN" => {
            if args.len() != 1 {
                return Frame::error("ERR wrong number of arguments for 'strlen' command");
            }
            match store.get(db, &args[0]) {
                Some(Value::String(data)) => Frame::Integer(data.len() as i64),
                Some(_) => Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ),
                None => Frame::Integer(0),
            }
        }
        "HGET" => {
            if args.len() != 2 {
                return Frame::error("ERR wrong number of arguments for 'hget' command");
            }
            match store.get(db, &args[0]) {
                Some(Value::Hash(hash)) => match hash.get(&args[1]) {
                    Some(value) => Frame::bulk(value.clone()),
                    None => Frame::null(),
                },
                Some(_) => Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ),
                None => Frame::null(),
            }
        }
        "HSET" => {
            if args.len() < 3 || (args.len() - 1) % 2 != 0 {
                return Frame::error("ERR wrong number of arguments for 'hset' command");
            }
            let mut count = 0;
            let mut hash = match store.get(db, &args[0]) {
                Some(Value::Hash(h)) => h,
                Some(_) => {
                    return Frame::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                }
                None => StdHashMap::new(),
            };
            for chunk in args[1..].chunks(2) {
                if hash.insert(chunk[0].clone(), chunk[1].clone()).is_none() {
                    count += 1;
                }
            }
            store.set(db, args[0].clone(), Value::Hash(hash));
            Frame::Integer(count)
        }
        "HDEL" => {
            if args.len() < 2 {
                return Frame::error("ERR wrong number of arguments for 'hdel' command");
            }
            match store.get(db, &args[0]) {
                Some(Value::Hash(mut hash)) => {
                    let mut count = 0;
                    for field in &args[1..] {
                        if hash.remove(field).is_some() {
                            count += 1;
                        }
                    }
                    if hash.is_empty() {
                        store.del(db, std::slice::from_ref(&args[0]));
                    } else {
                        store.set(db, args[0].clone(), Value::Hash(hash));
                    }
                    Frame::Integer(count)
                }
                Some(_) => Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ),
                None => Frame::Integer(0),
            }
        }
        "HEXISTS" => {
            if args.len() != 2 {
                return Frame::error("ERR wrong number of arguments for 'hexists' command");
            }
            match store.get(db, &args[0]) {
                Some(Value::Hash(hash)) => {
                    Frame::Integer(if hash.contains_key(&args[1]) { 1 } else { 0 })
                }
                Some(_) => Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ),
                None => Frame::Integer(0),
            }
        }
        "HGETALL" => {
            if args.len() != 1 {
                return Frame::error("ERR wrong number of arguments for 'hgetall' command");
            }
            match store.get(db, &args[0]) {
                Some(Value::Hash(hash)) => {
                    let mut frames = Vec::with_capacity(hash.len() * 2);
                    for (k, v) in &hash {
                        frames.push(Frame::bulk(k.clone()));
                        frames.push(Frame::bulk(v.clone()));
                    }
                    Frame::array(frames)
                }
                Some(_) => Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ),
                None => Frame::array(vec![]),
            }
        }
        "HMSET" => {
            if args.len() < 3 || (args.len() - 1) % 2 != 0 {
                return Frame::error("ERR wrong number of arguments for 'hmset' command");
            }
            let mut hash = match store.get(db, &args[0]) {
                Some(Value::Hash(h)) => h,
                Some(_) => {
                    return Frame::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                }
                None => StdHashMap::new(),
            };
            for chunk in args[1..].chunks(2) {
                hash.insert(chunk[0].clone(), chunk[1].clone());
            }
            store.set(db, args[0].clone(), Value::Hash(hash));
            Frame::simple("OK")
        }
        "HMGET" => {
            if args.len() < 2 {
                return Frame::error("ERR wrong number of arguments for 'hmget' command");
            }
            match store.get(db, &args[0]) {
                Some(Value::Hash(hash)) => {
                    let results: Vec<Frame> = args[1..]
                        .iter()
                        .map(|field| match hash.get(field) {
                            Some(v) => Frame::bulk(v.clone()),
                            None => Frame::null(),
                        })
                        .collect();
                    Frame::array(results)
                }
                Some(_) => Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ),
                None => {
                    Frame::array(args[1..].iter().map(|_| Frame::null()).collect())
                }
            }
        }
        "LPUSH" => {
            if args.len() < 2 {
                return Frame::error("ERR wrong number of arguments for 'lpush' command");
            }
            let mut list = match store.get(db, &args[0]) {
                Some(Value::List(l)) => l,
                Some(_) => {
                    return Frame::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                }
                None => VecDeque::new(),
            };
            for value in args[1..].iter().rev() {
                list.push_front(value.clone());
            }
            let len = list.len();
            store.set(db, args[0].clone(), Value::List(list));
            Frame::Integer(len as i64)
        }
        "RPUSH" => {
            if args.len() < 2 {
                return Frame::error("ERR wrong number of arguments for 'rpush' command");
            }
            let mut list = match store.get(db, &args[0]) {
                Some(Value::List(l)) => l,
                Some(_) => {
                    return Frame::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                }
                None => VecDeque::new(),
            };
            for value in &args[1..] {
                list.push_back(value.clone());
            }
            let len = list.len();
            store.set(db, args[0].clone(), Value::List(list));
            Frame::Integer(len as i64)
        }
        "LPOP" => {
            if args.is_empty() || args.len() > 2 {
                return Frame::error("ERR wrong number of arguments for 'lpop' command");
            }
            match store.get(db, &args[0]) {
                Some(Value::List(mut list)) => {
                    let result = list.pop_front();
                    if list.is_empty() {
                        store.del(db, std::slice::from_ref(&args[0]));
                    } else {
                        store.set(db, args[0].clone(), Value::List(list));
                    }
                    match result {
                        Some(v) => Frame::bulk(v),
                        None => Frame::null(),
                    }
                }
                Some(_) => Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ),
                None => Frame::null(),
            }
        }
        "RPOP" => {
            if args.is_empty() || args.len() > 2 {
                return Frame::error("ERR wrong number of arguments for 'rpop' command");
            }
            match store.get(db, &args[0]) {
                Some(Value::List(mut list)) => {
                    let result = list.pop_back();
                    if list.is_empty() {
                        store.del(db, std::slice::from_ref(&args[0]));
                    } else {
                        store.set(db, args[0].clone(), Value::List(list));
                    }
                    match result {
                        Some(v) => Frame::bulk(v),
                        None => Frame::null(),
                    }
                }
                Some(_) => Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ),
                None => Frame::null(),
            }
        }
        "LRANGE" => {
            if args.len() != 3 {
                return Frame::error("ERR wrong number of arguments for 'lrange' command");
            }
            let start: i64 = match String::from_utf8_lossy(&args[1]).parse() {
                Ok(v) => v,
                Err(_) => return Frame::error("ERR value is not an integer or out of range"),
            };
            let stop: i64 = match String::from_utf8_lossy(&args[2]).parse() {
                Ok(v) => v,
                Err(_) => return Frame::error("ERR value is not an integer or out of range"),
            };
            match store.get(db, &args[0]) {
                Some(Value::List(list)) => {
                    let len = list.len() as i64;
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
                    if start > stop {
                        return Frame::array(vec![]);
                    }
                    let frames: Vec<Frame> = list
                        .iter()
                        .skip(start)
                        .take(stop - start + 1)
                        .map(|v| Frame::bulk(v.clone()))
                        .collect();
                    Frame::array(frames)
                }
                Some(_) => Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ),
                None => Frame::array(vec![]),
            }
        }
        "LLEN" => {
            if args.len() != 1 {
                return Frame::error("ERR wrong number of arguments for 'llen' command");
            }
            match store.get(db, &args[0]) {
                Some(Value::List(list)) => Frame::Integer(list.len() as i64),
                Some(_) => Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ),
                None => Frame::Integer(0),
            }
        }
        "SADD" => {
            if args.len() < 2 {
                return Frame::error("ERR wrong number of arguments for 'sadd' command");
            }
            let mut set = match store.get(db, &args[0]) {
                Some(Value::Set(s)) => s,
                Some(_) => {
                    return Frame::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                }
                None => HashSet::new(),
            };
            let mut count = 0;
            for member in &args[1..] {
                if set.insert(member.clone()) {
                    count += 1;
                }
            }
            store.set(db, args[0].clone(), Value::Set(set));
            Frame::Integer(count)
        }
        "SREM" => {
            if args.len() < 2 {
                return Frame::error("ERR wrong number of arguments for 'srem' command");
            }
            match store.get(db, &args[0]) {
                Some(Value::Set(mut set)) => {
                    let mut count = 0;
                    for member in &args[1..] {
                        if set.remove(member) {
                            count += 1;
                        }
                    }
                    if set.is_empty() {
                        store.del(db, std::slice::from_ref(&args[0]));
                    } else {
                        store.set(db, args[0].clone(), Value::Set(set));
                    }
                    Frame::Integer(count)
                }
                Some(_) => Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ),
                None => Frame::Integer(0),
            }
        }
        "SISMEMBER" => {
            if args.len() != 2 {
                return Frame::error("ERR wrong number of arguments for 'sismember' command");
            }
            match store.get(db, &args[0]) {
                Some(Value::Set(set)) => {
                    Frame::Integer(if set.contains(&args[1]) { 1 } else { 0 })
                }
                Some(_) => Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ),
                None => Frame::Integer(0),
            }
        }
        "SMEMBERS" => {
            if args.len() != 1 {
                return Frame::error("ERR wrong number of arguments for 'smembers' command");
            }
            match store.get(db, &args[0]) {
                Some(Value::Set(set)) => Frame::array(set.into_iter().map(Frame::bulk).collect()),
                Some(_) => Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ),
                None => Frame::array(vec![]),
            }
        }
        "SCARD" => {
            if args.len() != 1 {
                return Frame::error("ERR wrong number of arguments for 'scard' command");
            }
            match store.get(db, &args[0]) {
                Some(Value::Set(set)) => Frame::Integer(set.len() as i64),
                Some(_) => Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ),
                None => Frame::Integer(0),
            }
        }
        "TYPE" => {
            if args.len() != 1 {
                return Frame::error("ERR wrong number of arguments for 'type' command");
            }
            match store.get(db, &args[0]) {
                Some(Value::String(_)) => Frame::simple("string"),
                Some(Value::List(_)) => Frame::simple("list"),
                Some(Value::Hash(_)) => Frame::simple("hash"),
                Some(Value::Set(_)) => Frame::simple("set"),
                Some(Value::SortedSet { .. }) => Frame::simple("zset"),
                Some(Value::Stream(_)) => Frame::simple("stream"),
                Some(Value::HyperLogLog(_)) => Frame::simple("string"),
                None => Frame::simple("none"),
            }
        }
        "TTL" => {
            if args.len() != 1 {
                return Frame::error("ERR wrong number of arguments for 'ttl' command");
            }
            match store.ttl(db, &args[0]) {
                Some(ttl) => Frame::Integer(ttl),
                None => Frame::Integer(-2),
            }
        }
        "PTTL" => {
            if args.len() != 1 {
                return Frame::error("ERR wrong number of arguments for 'pttl' command");
            }
            match store.ttl(db, &args[0]) {
                Some(ttl) if ttl >= 0 => Frame::Integer(ttl * 1000),
                Some(ttl) => Frame::Integer(ttl),
                None => Frame::Integer(-2),
            }
        }
        "HLEN" => {
            if args.len() != 1 {
                return Frame::error("ERR wrong number of arguments for 'hlen' command");
            }
            match store.get(db, &args[0]) {
                Some(Value::Hash(hash)) => Frame::Integer(hash.len() as i64),
                Some(_) => Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ),
                None => Frame::Integer(0),
            }
        }
        "KEYS" => {
            if args.len() != 1 {
                return Frame::error("ERR wrong number of arguments for 'keys' command");
            }
            let pattern = String::from_utf8_lossy(&args[0]);
            let all_keys = store.keys(db);
            if pattern == "*" {
                Frame::array(all_keys.into_iter().map(Frame::bulk).collect())
            } else {
                let frames: Vec<Frame> = all_keys
                    .into_iter()
                    .filter(|k| {
                        let key_str = String::from_utf8_lossy(k);
                        simple_pattern_match(&pattern, &key_str)
                    })
                    .map(Frame::bulk)
                    .collect();
                Frame::array(frames)
            }
        }
        "PING" => {
            if args.is_empty() {
                Frame::simple("PONG")
            } else if args.len() == 1 {
                Frame::bulk(args[0].clone())
            } else {
                Frame::error("ERR wrong number of arguments for 'ping' command")
            }
        }
        "ECHO" => {
            if args.len() != 1 {
                return Frame::error("ERR wrong number of arguments for 'echo' command");
            }
            Frame::bulk(args[0].clone())
        }
        _ => Frame::error(format!("ERR unknown command '{}' in Lua script", cmd)),
    }
}

/// Simple glob-style pattern matching (supports * and ? wildcards)
fn simple_pattern_match(pattern: &str, text: &str) -> bool {
    let pattern_chars: Vec<char> = pattern.chars().collect();
    let text_chars: Vec<char> = text.chars().collect();
    match_helper(&pattern_chars, &text_chars, 0, 0)
}

fn match_helper(pattern: &[char], text: &[char], pi: usize, ti: usize) -> bool {
    if pi == pattern.len() {
        return ti == text.len();
    }
    if pattern[pi] == '*' {
        for i in ti..=text.len() {
            if match_helper(pattern, text, pi + 1, i) {
                return true;
            }
        }
        return false;
    }
    if ti == text.len() {
        return false;
    }
    if pattern[pi] == '?' || pattern[pi] == text[ti] {
        return match_helper(pattern, text, pi + 1, ti + 1);
    }
    false
}

/// Helper function for INCR/DECR/INCRBY
fn incr_by(store: &Store, db: u8, key: &Bytes, delta: i64) -> Frame {
    use crate::storage::Value;

    match store.get(db, key) {
        Some(Value::String(data)) => {
            let s = String::from_utf8_lossy(&data);
            match s.parse::<i64>() {
                Ok(val) => {
                    let new_val = val.saturating_add(delta);
                    store.set(
                        db,
                        key.clone(),
                        Value::String(Bytes::from(new_val.to_string())),
                    );
                    Frame::Integer(new_val)
                }
                Err(_) => Frame::error("ERR value is not an integer or out of range"),
            }
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => {
            store.set(
                db,
                key.clone(),
                Value::String(Bytes::from(delta.to_string())),
            );
            Frame::Integer(delta)
        }
    }
}

/// Convert Frame to Lua value (used when returning Redis command results to Lua)
fn frame_to_lua_value(lua: &Lua, frame: Frame, is_pcall: bool) -> LuaResult<LuaValue> {
    match frame {
        Frame::Simple(s) => {
            let table = lua.create_table()?;
            let lua_str = lua.create_string(&s)?;
            table.set("ok", lua_str)?;
            Ok(LuaValue::Table(table))
        }
        Frame::Error(e) => {
            if is_pcall {
                let table = lua.create_table()?;
                let lua_str = lua.create_string(&e)?;
                table.set("err", lua_str)?;
                Ok(LuaValue::Table(table))
            } else {
                let err_str = String::from_utf8_lossy(&e).into_owned();
                Err(mlua::Error::RuntimeError(err_str))
            }
        }
        Frame::Integer(n) => Ok(LuaValue::Integer(n)),
        Frame::Bulk(Some(b)) => {
            let s = lua.create_string(&b)?;
            Ok(LuaValue::String(s))
        }
        Frame::Bulk(None) => Ok(LuaValue::Boolean(false)),
        Frame::Null => Ok(LuaValue::Boolean(false)),
        Frame::Array(Some(frames)) => {
            let table = lua.create_table()?;
            for (i, f) in frames.into_iter().enumerate() {
                let value = frame_to_lua_value(lua, f, is_pcall)?;
                table.set(i + 1, value)?;
            }
            Ok(LuaValue::Table(table))
        }
        Frame::Array(None) => Ok(LuaValue::Boolean(false)),
        _ => Ok(LuaValue::Nil),
    }
}

/// Convert Lua value to Frame (used when returning script results to the client)
fn lua_value_to_frame(value: LuaValue) -> Frame {
    match value {
        LuaValue::Nil => Frame::null(),
        LuaValue::Boolean(false) => Frame::null(),
        LuaValue::Boolean(true) => Frame::Integer(1),
        LuaValue::Integer(n) => Frame::Integer(n),
        LuaValue::Number(n) => {
            // Redis truncates floats to integers when possible
            if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
                Frame::Integer(n as i64)
            } else {
                Frame::bulk(Bytes::from(n.to_string()))
            }
        }
        LuaValue::String(s) => Frame::bulk(Bytes::from(s.as_bytes().to_vec())),
        LuaValue::Table(t) => {
            // Check if it's a status reply table {ok = "..."}
            if let Ok(LuaValue::String(s)) = t.get::<LuaValue>("ok") {
                return Frame::simple(s.to_string_lossy());
            }
            // Check if it's an error reply table {err = "..."}
            if let Ok(LuaValue::String(s)) = t.get::<LuaValue>("err") {
                return Frame::error(s.to_string_lossy());
            }

            // Otherwise treat as an array (1-indexed Lua table)
            let mut frames = Vec::new();
            let mut i = 1;
            while let Ok(v) = t.get::<LuaValue>(i) {
                if matches!(v, LuaValue::Nil) {
                    break;
                }
                frames.push(lua_value_to_frame(v));
                i += 1;
            }
            Frame::array(frames)
        }
        _ => Frame::null(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Helper ──────────────────────────────────────────────────────────────

    fn make_executor() -> (Arc<ScriptCache>, ScriptExecutor) {
        let cache = Arc::new(ScriptCache::new());
        let store = Arc::new(Store::new(16));
        let executor = ScriptExecutor::new(cache.clone(), store);
        (cache, executor)
    }

    fn make_executor_with_store() -> (Arc<ScriptCache>, Arc<Store>, ScriptExecutor) {
        let cache = Arc::new(ScriptCache::new());
        let store = Arc::new(Store::new(16));
        let executor = ScriptExecutor::new(cache.clone(), store.clone());
        (cache, store, executor)
    }

    // ── SHA1 correctness ────────────────────────────────────────────────────

    #[test]
    fn test_sha1_hex_empty_string() {
        // SHA1("") = da39a3ee5e6b4b0d3255bfef95601890afd80709
        assert_eq!(sha1_hex(b""), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
    }

    #[test]
    fn test_sha1_hex_known_vector() {
        // SHA1("The quick brown fox jumps over the lazy dog")
        assert_eq!(
            sha1_hex(b"The quick brown fox jumps over the lazy dog"),
            "2fd4e1c67a2d28fced849ee1bb76e7391b93eb12"
        );
    }

    #[test]
    fn test_sha1_hex_return_1() {
        // SHA1("return 1") should be 40 hex chars and deterministic
        let hash = sha1_hex(b"return 1");
        assert_eq!(hash.len(), 40);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
        // Verify determinism
        assert_eq!(sha1_hex(b"return 1"), hash);
    }

    // ── Script Cache ────────────────────────────────────────────────────────

    #[test]
    fn test_script_cache_load_get_exists() {
        let cache = ScriptCache::new();
        let script = "return 1";
        let hash = cache.load(script);

        assert_eq!(hash.len(), 40);
        assert!(cache.exists(&hash));
        assert_eq!(cache.get(&hash), Some(script.to_string()));
    }

    #[test]
    fn test_script_cache_flush() {
        let cache = ScriptCache::new();
        let hash = cache.load("return 1");
        assert!(cache.exists(&hash));

        cache.flush();
        assert!(!cache.exists(&hash));
        assert_eq!(cache.get(&hash), None);
    }

    #[test]
    fn test_script_cache_nonexistent() {
        let cache = ScriptCache::new();
        assert!(!cache.exists("nonexistent"));
        assert_eq!(cache.get("nonexistent"), None);
    }

    #[test]
    fn test_script_cache_deterministic_hash() {
        let cache = ScriptCache::new();
        let h1 = cache.load("return 'hello'");
        let h2 = cache.load("return 'hello'");
        assert_eq!(h1, h2);
    }

    // ── Basic EVAL ──────────────────────────────────────────────────────────

    #[test]
    fn test_eval_return_integer() {
        let (_cache, executor) = make_executor();
        let result = executor.eval(0, "return 42", &[], &[]);
        assert_eq!(result, Frame::Integer(42));
    }

    #[test]
    fn test_eval_return_string() {
        let (_cache, executor) = make_executor();
        let result = executor.eval(0, "return 'hello'", &[], &[]);
        assert_eq!(result, Frame::bulk(Bytes::from("hello")));
    }

    #[test]
    fn test_eval_return_true() {
        let (_cache, executor) = make_executor();
        let result = executor.eval(0, "return true", &[], &[]);
        assert_eq!(result, Frame::Integer(1));
    }

    #[test]
    fn test_eval_return_false() {
        let (_cache, executor) = make_executor();
        let result = executor.eval(0, "return false", &[], &[]);
        assert!(result.is_null());
    }

    #[test]
    fn test_eval_return_nil() {
        let (_cache, executor) = make_executor();
        let result = executor.eval(0, "return nil", &[], &[]);
        assert!(result.is_null());
    }

    #[test]
    fn test_eval_return_table_as_array() {
        let (_cache, executor) = make_executor();
        let result = executor.eval(0, "return {1, 2, 3}", &[], &[]);
        match result {
            Frame::Array(Some(frames)) => {
                assert_eq!(frames.len(), 3);
                assert_eq!(frames[0], Frame::Integer(1));
                assert_eq!(frames[1], Frame::Integer(2));
                assert_eq!(frames[2], Frame::Integer(3));
            }
            _ => panic!("Expected array, got {:?}", result),
        }
    }

    #[test]
    fn test_eval_return_nested_table() {
        let (_cache, executor) = make_executor();
        let result = executor.eval(0, "return {1, {2, 3}}", &[], &[]);
        match result {
            Frame::Array(Some(frames)) => {
                assert_eq!(frames.len(), 2);
                assert_eq!(frames[0], Frame::Integer(1));
                match &frames[1] {
                    Frame::Array(Some(inner)) => {
                        assert_eq!(inner.len(), 2);
                        assert_eq!(inner[0], Frame::Integer(2));
                        assert_eq!(inner[1], Frame::Integer(3));
                    }
                    _ => panic!("Expected inner array"),
                }
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_eval_return_float_truncated_to_integer() {
        let (_cache, executor) = make_executor();
        // Redis truncates 3.0 to integer 3
        let result = executor.eval(0, "return 3.0", &[], &[]);
        assert_eq!(result, Frame::Integer(3));
    }

    // ── KEYS and ARGV ───────────────────────────────────────────────────────

    #[test]
    fn test_eval_keys_access() {
        let (_cache, executor) = make_executor();
        let keys = vec![Bytes::from("key1"), Bytes::from("key2")];

        let result = executor.eval(0, "return KEYS[1]", &keys, &[]);
        assert_eq!(result, Frame::bulk(Bytes::from("key1")));

        let result = executor.eval(0, "return KEYS[2]", &keys, &[]);
        assert_eq!(result, Frame::bulk(Bytes::from("key2")));

        let result = executor.eval(0, "return #KEYS", &keys, &[]);
        assert_eq!(result, Frame::Integer(2));
    }

    #[test]
    fn test_eval_argv_access() {
        let (_cache, executor) = make_executor();
        let args = vec![Bytes::from("arg1"), Bytes::from("arg2"), Bytes::from("arg3")];

        let result = executor.eval(0, "return ARGV[1]", &[], &args);
        assert_eq!(result, Frame::bulk(Bytes::from("arg1")));

        let result = executor.eval(0, "return #ARGV", &[], &args);
        assert_eq!(result, Frame::Integer(3));
    }

    #[test]
    fn test_eval_keys_and_argv_together() {
        let (_cache, executor) = make_executor();
        let keys = vec![Bytes::from("mykey")];
        let args = vec![Bytes::from("myvalue")];

        let script = "return KEYS[1] .. ':' .. ARGV[1]";
        let result = executor.eval(0, script, &keys, &args);
        assert_eq!(result, Frame::bulk(Bytes::from("mykey:myvalue")));
    }

    // ── redis.call for GET/SET/DEL/INCR ─────────────────────────────────────

    #[test]
    fn test_redis_call_set_get() {
        let (_cache, executor) = make_executor();
        let keys = vec![Bytes::from("mykey")];
        let args = vec![Bytes::from("myvalue")];

        let script = r#"
            redis.call('SET', KEYS[1], ARGV[1])
            return redis.call('GET', KEYS[1])
        "#;

        let result = executor.eval(0, script, &keys, &args);
        assert_eq!(result, Frame::bulk(Bytes::from("myvalue")));
    }

    #[test]
    fn test_redis_call_del() {
        let (_cache, executor) = make_executor();
        let keys = vec![Bytes::from("delkey")];

        let script = r#"
            redis.call('SET', KEYS[1], 'value')
            local count = redis.call('DEL', KEYS[1])
            local val = redis.call('GET', KEYS[1])
            if val == false then
                return count
            end
            return -1
        "#;

        let result = executor.eval(0, script, &keys, &[]);
        assert_eq!(result, Frame::Integer(1));
    }

    #[test]
    fn test_redis_call_incr() {
        let (_cache, executor) = make_executor();
        let keys = vec![Bytes::from("counter")];

        let script = r#"
            redis.call('SET', KEYS[1], '10')
            redis.call('INCR', KEYS[1])
            redis.call('INCR', KEYS[1])
            return redis.call('GET', KEYS[1])
        "#;

        let result = executor.eval(0, script, &keys, &[]);
        assert_eq!(result, Frame::bulk(Bytes::from("12")));
    }

    #[test]
    fn test_redis_call_incr_nonexistent() {
        let (_cache, executor) = make_executor();
        let keys = vec![Bytes::from("newcounter")];

        let result = executor.eval(0, "return redis.call('INCR', KEYS[1])", &keys, &[]);
        assert_eq!(result, Frame::Integer(1));
    }

    #[test]
    fn test_redis_call_incrby() {
        let (_cache, executor) = make_executor();
        let keys = vec![Bytes::from("counter")];

        let script = r#"
            redis.call('SET', KEYS[1], '5')
            return redis.call('INCRBY', KEYS[1], '10')
        "#;

        let result = executor.eval(0, script, &keys, &[]);
        assert_eq!(result, Frame::Integer(15));
    }

    #[test]
    fn test_redis_call_decr() {
        let (_cache, executor) = make_executor();
        let keys = vec![Bytes::from("counter")];

        let script = r#"
            redis.call('SET', KEYS[1], '10')
            return redis.call('DECR', KEYS[1])
        "#;

        let result = executor.eval(0, script, &keys, &[]);
        assert_eq!(result, Frame::Integer(9));
    }

    #[test]
    fn test_redis_call_exists() {
        let (_cache, executor) = make_executor();
        let keys = vec![Bytes::from("existskey")];

        let script = r#"
            local before = redis.call('EXISTS', KEYS[1])
            redis.call('SET', KEYS[1], 'val')
            local after = redis.call('EXISTS', KEYS[1])
            return {before, after}
        "#;

        let result = executor.eval(0, script, &keys, &[]);
        match result {
            Frame::Array(Some(frames)) => {
                assert_eq!(frames[0], Frame::Integer(0));
                assert_eq!(frames[1], Frame::Integer(1));
            }
            _ => panic!("Expected array, got {:?}", result),
        }
    }

    // ── redis.call for Hash commands ────────────────────────────────────────

    #[test]
    fn test_redis_call_hset_hget() {
        let (_cache, executor) = make_executor();
        let keys = vec![Bytes::from("myhash")];

        let script = r#"
            redis.call('HSET', KEYS[1], 'field1', 'value1', 'field2', 'value2')
            return redis.call('HGET', KEYS[1], 'field1')
        "#;

        let result = executor.eval(0, script, &keys, &[]);
        assert_eq!(result, Frame::bulk(Bytes::from("value1")));
    }

    #[test]
    fn test_redis_call_hgetall() {
        let (_cache, executor) = make_executor();
        let keys = vec![Bytes::from("myhash")];

        let script = r#"
            redis.call('HSET', KEYS[1], 'f1', 'v1')
            local result = redis.call('HGETALL', KEYS[1])
            return #result
        "#;

        let result = executor.eval(0, script, &keys, &[]);
        assert_eq!(result, Frame::Integer(2)); // [field, value]
    }

    // ── redis.call for List commands ────────────────────────────────────────

    #[test]
    fn test_redis_call_lpush_rpush_lrange() {
        let (_cache, executor) = make_executor();
        let keys = vec![Bytes::from("mylist")];

        let script = r#"
            redis.call('RPUSH', KEYS[1], 'a', 'b', 'c')
            redis.call('LPUSH', KEYS[1], 'z')
            return redis.call('LRANGE', KEYS[1], '0', '-1')
        "#;

        let result = executor.eval(0, script, &keys, &[]);
        match result {
            Frame::Array(Some(frames)) => {
                assert_eq!(frames.len(), 4);
                assert_eq!(frames[0], Frame::bulk(Bytes::from("z")));
                assert_eq!(frames[1], Frame::bulk(Bytes::from("a")));
                assert_eq!(frames[2], Frame::bulk(Bytes::from("b")));
                assert_eq!(frames[3], Frame::bulk(Bytes::from("c")));
            }
            _ => panic!("Expected array, got {:?}", result),
        }
    }

    #[test]
    fn test_redis_call_lpop_rpop() {
        let (_cache, executor) = make_executor();
        let keys = vec![Bytes::from("mylist")];

        let script = r#"
            redis.call('RPUSH', KEYS[1], 'a', 'b', 'c')
            local left = redis.call('LPOP', KEYS[1])
            local right = redis.call('RPOP', KEYS[1])
            return {left, right}
        "#;

        let result = executor.eval(0, script, &keys, &[]);
        match result {
            Frame::Array(Some(frames)) => {
                assert_eq!(frames[0], Frame::bulk(Bytes::from("a")));
                assert_eq!(frames[1], Frame::bulk(Bytes::from("c")));
            }
            _ => panic!("Expected array, got {:?}", result),
        }
    }

    #[test]
    fn test_redis_call_llen() {
        let (_cache, executor) = make_executor();
        let keys = vec![Bytes::from("mylist")];

        let script = r#"
            redis.call('RPUSH', KEYS[1], 'a', 'b', 'c')
            return redis.call('LLEN', KEYS[1])
        "#;

        let result = executor.eval(0, script, &keys, &[]);
        assert_eq!(result, Frame::Integer(3));
    }

    // ── redis.call for Set commands ─────────────────────────────────────────

    #[test]
    fn test_redis_call_sadd_scard() {
        let (_cache, executor) = make_executor();
        let keys = vec![Bytes::from("myset")];

        let script = r#"
            redis.call('SADD', KEYS[1], 'a', 'b', 'c', 'a')
            return redis.call('SCARD', KEYS[1])
        "#;

        let result = executor.eval(0, script, &keys, &[]);
        assert_eq!(result, Frame::Integer(3));
    }

    #[test]
    fn test_redis_call_sismember() {
        let (_cache, executor) = make_executor();
        let keys = vec![Bytes::from("myset")];

        let script = r#"
            redis.call('SADD', KEYS[1], 'hello')
            local yes = redis.call('SISMEMBER', KEYS[1], 'hello')
            local no = redis.call('SISMEMBER', KEYS[1], 'world')
            return {yes, no}
        "#;

        let result = executor.eval(0, script, &keys, &[]);
        match result {
            Frame::Array(Some(frames)) => {
                assert_eq!(frames[0], Frame::Integer(1));
                assert_eq!(frames[1], Frame::Integer(0));
            }
            _ => panic!("Expected array, got {:?}", result),
        }
    }

    // ── redis.pcall error handling ──────────────────────────────────────────

    #[test]
    fn test_redis_pcall_returns_error_table() {
        let (_cache, executor) = make_executor();

        let script = r#"
            local result = redis.pcall('SET')
            return result.err
        "#;

        let result = executor.eval(0, script, &[], &[]);
        match &result {
            Frame::Bulk(Some(b)) => {
                let s = String::from_utf8_lossy(b);
                assert!(
                    s.contains("wrong number of arguments"),
                    "Expected wrong arity error, got: {}",
                    s
                );
            }
            _ => panic!("Expected bulk string error message, got {:?}", result),
        }
    }

    #[test]
    fn test_redis_pcall_wrongtype_returns_error_table() {
        let (_cache, executor) = make_executor();
        let keys = vec![Bytes::from("mykey")];

        let script = r#"
            redis.call('RPUSH', KEYS[1], 'a')
            local result = redis.pcall('INCR', KEYS[1])
            if result.err then
                return result.err
            end
            return 'no error'
        "#;

        let result = executor.eval(0, script, &keys, &[]);
        match &result {
            Frame::Bulk(Some(b)) => {
                let s = String::from_utf8_lossy(b);
                assert!(s.contains("WRONGTYPE"), "Expected WRONGTYPE error, got: {}", s);
            }
            _ => panic!("Expected bulk string, got {:?}", result),
        }
    }

    #[test]
    fn test_redis_call_raises_on_error() {
        let (_cache, executor) = make_executor();
        let keys = vec![Bytes::from("mykey")];

        let script = r#"
            redis.call('RPUSH', KEYS[1], 'a')
            redis.call('INCR', KEYS[1])
            return 'should not reach here'
        "#;

        let result = executor.eval(0, script, &keys, &[]);
        match &result {
            Frame::Error(e) => {
                let s = String::from_utf8_lossy(e);
                assert!(s.contains("WRONGTYPE"), "Expected WRONGTYPE in error, got: {}", s);
            }
            _ => panic!("Expected error, got {:?}", result),
        }
    }

    #[test]
    fn test_redis_call_no_args_raises() {
        let (_cache, executor) = make_executor();

        let result = executor.eval(0, "redis.call()\nreturn 'nope'", &[], &[]);
        assert!(matches!(result, Frame::Error(_)));
    }

    // ── redis.status_reply / redis.error_reply ──────────────────────────────

    #[test]
    fn test_redis_status_reply() {
        let (_cache, executor) = make_executor();
        let result = executor.eval(0, "return redis.status_reply('PONG')", &[], &[]);
        assert_eq!(result, Frame::simple("PONG"));
    }

    #[test]
    fn test_redis_error_reply() {
        let (_cache, executor) = make_executor();
        let result = executor.eval(0, "return redis.error_reply('ERR custom error')", &[], &[]);
        assert_eq!(result, Frame::error("ERR custom error"));
    }

    // ── redis.sha1hex ───────────────────────────────────────────────────────

    #[test]
    fn test_redis_sha1hex() {
        let (_cache, executor) = make_executor();

        let result = executor.eval(0, "return redis.sha1hex('')", &[], &[]);
        assert_eq!(
            result,
            Frame::bulk(Bytes::from("da39a3ee5e6b4b0d3255bfef95601890afd80709"))
        );
    }

    #[test]
    fn test_redis_sha1hex_nonempty() {
        let (_cache, executor) = make_executor();

        let result = executor.eval(0, "return redis.sha1hex('hello')", &[], &[]);
        match result {
            Frame::Bulk(Some(b)) => {
                let s = String::from_utf8_lossy(&b);
                assert_eq!(s.len(), 40);
                assert!(s.chars().all(|c| c.is_ascii_hexdigit()));
            }
            _ => panic!("Expected bulk string"),
        }
    }

    // ── redis.log ───────────────────────────────────────────────────────────

    #[test]
    fn test_redis_log_does_not_error() {
        let (_cache, executor) = make_executor();

        let script = r#"
            redis.log(redis.LOG_DEBUG, 'debug message')
            redis.log(redis.LOG_VERBOSE, 'verbose message')
            redis.log(redis.LOG_NOTICE, 'notice message')
            redis.log(redis.LOG_WARNING, 'warning message')
            return 'OK'
        "#;

        let result = executor.eval(0, script, &[], &[]);
        assert_eq!(result, Frame::bulk(Bytes::from("OK")));
    }

    // ── EVALSHA workflow ────────────────────────────────────────────────────

    #[test]
    fn test_evalsha_after_eval() {
        let (cache, executor) = make_executor();

        let script = "return 'cached'";
        let hash = sha1_hex(script.as_bytes());

        executor.eval(0, script, &[], &[]);

        let result = executor.evalsha(0, &hash, &[], &[]);
        assert_eq!(result, Frame::bulk(Bytes::from("cached")));
        assert!(cache.exists(&hash));
    }

    #[test]
    fn test_evalsha_after_script_load() {
        let (cache, executor) = make_executor();

        let script = "return 42";
        let hash = cache.load(script);

        let result = executor.evalsha(0, &hash, &[], &[]);
        assert_eq!(result, Frame::Integer(42));
    }

    #[test]
    fn test_evalsha_noscript_error() {
        let (_cache, executor) = make_executor();

        let result = executor.evalsha(0, "nonexistent_hash", &[], &[]);
        match &result {
            Frame::Error(e) => {
                let s = String::from_utf8_lossy(e);
                assert!(s.contains("NOSCRIPT"), "Expected NOSCRIPT error, got: {}", s);
            }
            _ => panic!("Expected error, got {:?}", result),
        }
    }

    #[test]
    fn test_evalsha_with_keys_and_args() {
        let (cache, executor) = make_executor();

        let script = r#"
            redis.call('SET', KEYS[1], ARGV[1])
            return redis.call('GET', KEYS[1])
        "#;
        let hash = cache.load(script);

        let keys = vec![Bytes::from("sha_key")];
        let args = vec![Bytes::from("sha_value")];

        let result = executor.evalsha(0, &hash, &keys, &args);
        assert_eq!(result, Frame::bulk(Bytes::from("sha_value")));
    }

    // ── SCRIPT EXISTS / FLUSH ───────────────────────────────────────────────

    #[test]
    fn test_script_exists() {
        let (cache, executor) = make_executor();

        let hash1 = cache.load("return 1");
        let hash2 = "nonexistent".to_string();

        let result = executor.script_exists(&[hash1.clone(), hash2]);
        match result {
            Frame::Array(Some(frames)) => {
                assert_eq!(frames.len(), 2);
                assert_eq!(frames[0], Frame::Integer(1));
                assert_eq!(frames[1], Frame::Integer(0));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_script_exists_empty() {
        let (_cache, executor) = make_executor();
        let result = executor.script_exists(&[]);
        assert_eq!(result, Frame::array(vec![]));
    }

    #[test]
    fn test_script_load() {
        let (_cache, executor) = make_executor();

        let result = executor.script_load("return 'loaded'");
        match &result {
            Frame::Bulk(Some(b)) => {
                let hash = String::from_utf8_lossy(b);
                assert_eq!(hash.len(), 40);
            }
            _ => panic!("Expected bulk string with hash"),
        }
    }

    #[test]
    fn test_script_flush() {
        let (cache, executor) = make_executor();

        let hash = cache.load("return 1");
        assert!(cache.exists(&hash));

        let result = executor.script_flush();
        assert_eq!(result, Frame::simple("OK"));
        assert!(!cache.exists(&hash));
    }

    #[test]
    fn test_script_flush_then_evalsha_returns_noscript() {
        let (cache, executor) = make_executor();

        let hash = cache.load("return 'test'");
        executor.script_flush();

        let result = executor.evalsha(0, &hash, &[], &[]);
        assert!(matches!(result, Frame::Error(_)));
    }

    // ── Type conversions ────────────────────────────────────────────────────

    #[test]
    fn test_type_conversion_nil_to_null() {
        let (_cache, executor) = make_executor();

        let script = r#"
            local val = redis.call('GET', 'nonexistent')
            if val == false then
                return 'correctly_false'
            end
            return 'wrong'
        "#;

        let result = executor.eval(0, script, &[], &[]);
        assert_eq!(result, Frame::bulk(Bytes::from("correctly_false")));
    }

    #[test]
    fn test_type_conversion_integer_roundtrip() {
        let (_cache, executor) = make_executor();

        let script = r#"
            redis.call('SET', 'key', '42')
            local val = redis.call('INCR', 'key')
            return val
        "#;

        let result = executor.eval(0, script, &[], &[]);
        assert_eq!(result, Frame::Integer(43));
    }

    #[test]
    fn test_type_conversion_bulk_string_roundtrip() {
        let (_cache, executor) = make_executor();

        let script = r#"
            redis.call('SET', 'key', 'hello world')
            return redis.call('GET', 'key')
        "#;

        let result = executor.eval(0, script, &[], &[]);
        assert_eq!(result, Frame::bulk(Bytes::from("hello world")));
    }

    #[test]
    fn test_type_conversion_status_reply() {
        let (_cache, executor) = make_executor();

        let script = r#"
            local result = redis.call('SET', 'key', 'value')
            return result.ok
        "#;

        let result = executor.eval(0, script, &[], &[]);
        assert_eq!(result, Frame::bulk(Bytes::from("OK")));
    }

    #[test]
    fn test_type_conversion_array_roundtrip() {
        let (_cache, executor) = make_executor();

        let script = r#"
            redis.call('RPUSH', 'mylist', 'a', 'b', 'c')
            return redis.call('LRANGE', 'mylist', '0', '-1')
        "#;

        let result = executor.eval(0, script, &[], &[]);
        match result {
            Frame::Array(Some(frames)) => {
                assert_eq!(frames.len(), 3);
                assert_eq!(frames[0], Frame::bulk(Bytes::from("a")));
                assert_eq!(frames[1], Frame::bulk(Bytes::from("b")));
                assert_eq!(frames[2], Frame::bulk(Bytes::from("c")));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_type_conversion_lua_number_to_integer() {
        let (_cache, executor) = make_executor();
        // 3.14 has a fractional part so it should be returned as a bulk string
        let result = executor.eval(0, "return 3.14", &[], &[]);
        assert_eq!(result, Frame::bulk(Bytes::from("3.14")));
    }

    // ── Error cases ─────────────────────────────────────────────────────────

    #[test]
    fn test_eval_syntax_error() {
        let (_cache, executor) = make_executor();

        let result = executor.eval(0, "this is not valid lua!!!", &[], &[]);
        match &result {
            Frame::Error(e) => {
                let s = String::from_utf8_lossy(e);
                assert!(s.contains("Error running script"), "Expected script error, got: {}", s);
            }
            _ => panic!("Expected error for syntax error, got {:?}", result),
        }
    }

    #[test]
    fn test_eval_runtime_error() {
        let (_cache, executor) = make_executor();

        let result = executor.eval(0, "error('custom runtime error')", &[], &[]);
        match &result {
            Frame::Error(e) => {
                let s = String::from_utf8_lossy(e);
                assert!(s.contains("custom runtime error"), "Expected custom error, got: {}", s);
            }
            _ => panic!("Expected error for runtime error, got {:?}", result),
        }
    }

    #[test]
    fn test_eval_unknown_command_in_script() {
        let (_cache, executor) = make_executor();

        let result = executor.eval(0, "return redis.call('NONEXISTENT', 'key')", &[], &[]);
        match &result {
            Frame::Error(e) => {
                let s = String::from_utf8_lossy(e);
                assert!(s.contains("unknown command"), "Expected unknown command error, got: {}", s);
            }
            _ => panic!("Expected error, got {:?}", result),
        }
    }

    // ── Database isolation ──────────────────────────────────────────────────

    #[test]
    fn test_eval_respects_db_number() {
        let (_cache, _store, executor) = make_executor_with_store();

        executor.eval(0, "redis.call('SET', 'key', 'db0val')", &[], &[]);
        executor.eval(1, "redis.call('SET', 'key', 'db1val')", &[], &[]);

        let result_db0 = executor.eval(0, "return redis.call('GET', 'key')", &[], &[]);
        assert_eq!(result_db0, Frame::bulk(Bytes::from("db0val")));

        let result_db1 = executor.eval(1, "return redis.call('GET', 'key')", &[], &[]);
        assert_eq!(result_db1, Frame::bulk(Bytes::from("db1val")));
    }

    // ── Persistence across calls ────────────────────────────────────────────

    #[test]
    fn test_eval_data_persists_between_calls() {
        let (_cache, executor) = make_executor();

        executor.eval(0, "redis.call('SET', 'persistent', 'data')", &[], &[]);
        let result = executor.eval(0, "return redis.call('GET', 'persistent')", &[], &[]);
        assert_eq!(result, Frame::bulk(Bytes::from("data")));
    }

    // ── Additional commands in scripts ──────────────────────────────────────

    #[test]
    fn test_redis_call_mget() {
        let (_cache, executor) = make_executor();

        let script = r#"
            redis.call('SET', 'k1', 'v1')
            redis.call('SET', 'k2', 'v2')
            return redis.call('MGET', 'k1', 'k2', 'k3')
        "#;

        let result = executor.eval(0, script, &[], &[]);
        match result {
            Frame::Array(Some(frames)) => {
                assert_eq!(frames.len(), 3);
                assert_eq!(frames[0], Frame::bulk(Bytes::from("v1")));
                assert_eq!(frames[1], Frame::bulk(Bytes::from("v2")));
                assert!(frames[2].is_null());
            }
            _ => panic!("Expected array, got {:?}", result),
        }
    }

    #[test]
    fn test_redis_call_mset() {
        let (_cache, executor) = make_executor();

        let script = r#"
            redis.call('MSET', 'k1', 'v1', 'k2', 'v2')
            local v1 = redis.call('GET', 'k1')
            local v2 = redis.call('GET', 'k2')
            return {v1, v2}
        "#;

        let result = executor.eval(0, script, &[], &[]);
        match result {
            Frame::Array(Some(frames)) => {
                assert_eq!(frames[0], Frame::bulk(Bytes::from("v1")));
                assert_eq!(frames[1], Frame::bulk(Bytes::from("v2")));
            }
            _ => panic!("Expected array, got {:?}", result),
        }
    }

    #[test]
    fn test_redis_call_append_strlen() {
        let (_cache, executor) = make_executor();

        let script = r#"
            redis.call('SET', 'mykey', 'hello')
            redis.call('APPEND', 'mykey', ' world')
            local len = redis.call('STRLEN', 'mykey')
            local val = redis.call('GET', 'mykey')
            return {len, val}
        "#;

        let result = executor.eval(0, script, &[], &[]);
        match result {
            Frame::Array(Some(frames)) => {
                assert_eq!(frames[0], Frame::Integer(11));
                assert_eq!(frames[1], Frame::bulk(Bytes::from("hello world")));
            }
            _ => panic!("Expected array, got {:?}", result),
        }
    }

    #[test]
    fn test_redis_call_type() {
        let (_cache, executor) = make_executor();

        let script = r#"
            redis.call('SET', 'str', 'value')
            redis.call('RPUSH', 'list', 'a')
            redis.call('HSET', 'hash', 'f', 'v')
            redis.call('SADD', 'set', 'a')
            local t1 = redis.call('TYPE', 'str')
            local t2 = redis.call('TYPE', 'list')
            local t3 = redis.call('TYPE', 'hash')
            local t4 = redis.call('TYPE', 'set')
            local t5 = redis.call('TYPE', 'nonexistent')
            return {t1.ok, t2.ok, t3.ok, t4.ok, t5.ok}
        "#;

        let result = executor.eval(0, script, &[], &[]);
        match result {
            Frame::Array(Some(frames)) => {
                assert_eq!(frames[0], Frame::bulk(Bytes::from("string")));
                assert_eq!(frames[1], Frame::bulk(Bytes::from("list")));
                assert_eq!(frames[2], Frame::bulk(Bytes::from("hash")));
                assert_eq!(frames[3], Frame::bulk(Bytes::from("set")));
                assert_eq!(frames[4], Frame::bulk(Bytes::from("none")));
            }
            _ => panic!("Expected array, got {:?}", result),
        }
    }

    #[test]
    fn test_redis_call_ping() {
        let (_cache, executor) = make_executor();

        let script = r#"
            local result = redis.call('PING')
            return result.ok
        "#;

        let result = executor.eval(0, script, &[], &[]);
        assert_eq!(result, Frame::bulk(Bytes::from("PONG")));
    }

    // ── Complex script patterns ─────────────────────────────────────────────

    #[test]
    fn test_conditional_set_script() {
        let (_cache, executor) = make_executor();
        let keys = vec![Bytes::from("mykey")];
        let args = vec![Bytes::from("newvalue")];

        let script = r#"
            local current = redis.call('GET', KEYS[1])
            if current == false then
                redis.call('SET', KEYS[1], ARGV[1])
                return 1
            end
            return 0
        "#;

        let result = executor.eval(0, script, &keys, &args);
        assert_eq!(result, Frame::Integer(1));

        let result = executor.eval(0, script, &keys, &args);
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_counter_with_max_script() {
        let (_cache, executor) = make_executor();
        let keys = vec![Bytes::from("counter")];

        let script = r#"
            local current = redis.call('INCR', KEYS[1])
            if current > 3 then
                redis.call('SET', KEYS[1], '0')
                return 0
            end
            return current
        "#;

        assert_eq!(executor.eval(0, script, &keys, &[]), Frame::Integer(1));
        assert_eq!(executor.eval(0, script, &keys, &[]), Frame::Integer(2));
        assert_eq!(executor.eval(0, script, &keys, &[]), Frame::Integer(3));
        assert_eq!(executor.eval(0, script, &keys, &[]), Frame::Integer(0));
    }

    // ── Script kill (stub) ──────────────────────────────────────────────────

    #[test]
    fn test_script_kill_stub() {
        let (_cache, executor) = make_executor();
        let result = executor.eval(0, "return 'alive'", &[], &[]);
        assert_eq!(result, Frame::bulk(Bytes::from("alive")));
    }

    // ── Edge cases ──────────────────────────────────────────────────────────

    #[test]
    fn test_eval_empty_script() {
        let (_cache, executor) = make_executor();
        let result = executor.eval(0, "", &[], &[]);
        assert!(result.is_null());
    }

    #[test]
    fn test_eval_multiline_script() {
        let (_cache, executor) = make_executor();

        let script = r#"
            local a = 1
            local b = 2
            local c = a + b
            return c
        "#;

        let result = executor.eval(0, script, &[], &[]);
        assert_eq!(result, Frame::Integer(3));
    }

    #[test]
    fn test_eval_string_concatenation_in_lua() {
        let (_cache, executor) = make_executor();
        let result = executor.eval(0, "return 'hello' .. ' ' .. 'world'", &[], &[]);
        assert_eq!(result, Frame::bulk(Bytes::from("hello world")));
    }

    #[test]
    fn test_eval_tonumber_conversion() {
        let (_cache, executor) = make_executor();

        let script = r#"
            redis.call('SET', 'key', '42')
            local val = redis.call('GET', 'key')
            return tonumber(val) + 8
        "#;

        let result = executor.eval(0, script, &[], &[]);
        assert_eq!(result, Frame::Integer(50));
    }

    #[test]
    fn test_eval_large_return_array() {
        let (_cache, executor) = make_executor();

        let script = r#"
            local result = {}
            for i = 1, 100 do
                result[i] = i
            end
            return result
        "#;

        let result = executor.eval(0, script, &[], &[]);
        match result {
            Frame::Array(Some(frames)) => {
                assert_eq!(frames.len(), 100);
                assert_eq!(frames[0], Frame::Integer(1));
                assert_eq!(frames[99], Frame::Integer(100));
            }
            _ => panic!("Expected array of 100 elements"),
        }
    }

    // ── SCRIPT LOAD then EVALSHA round-trip ─────────────────────────────────

    #[test]
    fn test_script_load_then_evalsha_roundtrip() {
        let (_cache, executor) = make_executor();

        let load_result = executor.script_load("return KEYS[1] .. ':' .. ARGV[1]");
        let hash = match &load_result {
            Frame::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
            _ => panic!("Expected hash from SCRIPT LOAD"),
        };

        let keys = vec![Bytes::from("loaded_key")];
        let args = vec![Bytes::from("loaded_val")];
        let result = executor.evalsha(0, &hash, &keys, &args);
        assert_eq!(result, Frame::bulk(Bytes::from("loaded_key:loaded_val")));
    }

    #[test]
    fn test_script_load_exists_flush_roundtrip() {
        let (_cache, executor) = make_executor();

        let load_result = executor.script_load("return 1");
        let hash = match &load_result {
            Frame::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
            _ => panic!("Expected hash"),
        };

        // EXISTS should find it
        let exists_result = executor.script_exists(&[hash.clone()]);
        match exists_result {
            Frame::Array(Some(frames)) => assert_eq!(frames[0], Frame::Integer(1)),
            _ => panic!("Expected array"),
        }

        // FLUSH
        executor.script_flush();

        // EXISTS should not find it
        let exists_result = executor.script_exists(&[hash.clone()]);
        match exists_result {
            Frame::Array(Some(frames)) => assert_eq!(frames[0], Frame::Integer(0)),
            _ => panic!("Expected array"),
        }

        // EVALSHA should fail
        let result = executor.evalsha(0, &hash, &[], &[]);
        assert!(matches!(result, Frame::Error(_)));
    }
}
