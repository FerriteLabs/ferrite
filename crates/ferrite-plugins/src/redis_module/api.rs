//! Core Redis Module API types and functions
//!
//! Provides compatibility types that mirror the Redis Module API, allowing
//! existing Redis module concepts to work within Ferrite's architecture.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

/// Redis Module API version supported by this compatibility layer.
pub const REDISMODULE_API_VERSION: u32 = 1;

/// Status codes returned by Redis Module API functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// Operation succeeded
    Ok,
    /// Operation failed
    Err,
}

/// Key access mode for `RedisModule_OpenKey`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyMode {
    /// Read-only access
    Read,
    /// Read-write access
    Write,
}

/// Redis data types as seen by modules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    /// Key does not exist
    Empty,
    /// String value
    String,
    /// List value
    List,
    /// Hash value
    Hash,
    /// Set value
    Set,
    /// Sorted set value
    ZSet,
    /// Module-defined custom type
    Module,
}

/// Context passed to module command functions.
///
/// This is the Ferrite equivalent of `RedisModuleCtx` in the Redis Module API.
/// It provides access to the key space, reply functions, and server state.
pub struct RedisModuleCtx {
    /// Currently selected database index
    pub db: u8,
    /// Client ID that triggered this call
    pub client_id: u64,
    /// Opened keys in this context (key name → handle)
    open_keys: HashMap<String, RedisModuleKey>,
    /// Reply buffer accumulated by reply functions
    reply: Vec<ReplyValue>,
    /// Whether the context has been marked as blocked
    #[allow(dead_code)] // Planned for v0.2 — stored for blocking command support
    blocked: bool,
}

impl RedisModuleCtx {
    /// Create a new module context.
    pub fn new(db: u8, client_id: u64) -> Self {
        Self {
            db,
            client_id,
            open_keys: HashMap::new(),
            reply: Vec::new(),
            blocked: false,
        }
    }

    /// Open a key for reading or writing (`RedisModule_OpenKey`).
    pub fn open_key(&mut self, name: &str, mode: KeyMode) -> &mut RedisModuleKey {
        self.open_keys
            .entry(name.to_string())
            .or_insert_with(|| RedisModuleKey::new(name.to_string(), mode))
    }

    /// Close a key handle (`RedisModule_CloseKey`).
    pub fn close_key(&mut self, name: &str) {
        self.open_keys.remove(name);
    }

    /// Close all open keys — called automatically when the context is dropped.
    pub fn close_all_keys(&mut self) {
        self.open_keys.clear();
    }

    // ── Reply helpers ──────────────────────────────────────────────

    /// Reply with a simple string (`RedisModule_ReplyWithSimpleString`).
    pub fn reply_with_simple_string(&mut self, s: &str) -> Status {
        self.reply.push(ReplyValue::SimpleString(s.to_string()));
        Status::Ok
    }

    /// Reply with an error (`RedisModule_ReplyWithError`).
    pub fn reply_with_error(&mut self, err: &str) -> Status {
        self.reply.push(ReplyValue::Error(err.to_string()));
        Status::Ok
    }

    /// Reply with a long long integer (`RedisModule_ReplyWithLongLong`).
    pub fn reply_with_long_long(&mut self, value: i64) -> Status {
        self.reply.push(ReplyValue::Integer(value));
        Status::Ok
    }

    /// Reply with a bulk string (`RedisModule_ReplyWithString`).
    pub fn reply_with_string(&mut self, s: &RedisModuleString) -> Status {
        self.reply.push(ReplyValue::BulkString(s.as_bytes()));
        Status::Ok
    }

    /// Reply with a bulk string from raw bytes (`RedisModule_ReplyWithStringBuffer`).
    pub fn reply_with_string_buffer(&mut self, buf: &[u8]) -> Status {
        self.reply.push(ReplyValue::BulkString(buf.to_vec()));
        Status::Ok
    }

    /// Reply with NULL (`RedisModule_ReplyWithNull`).
    pub fn reply_with_null(&mut self) -> Status {
        self.reply.push(ReplyValue::Null);
        Status::Ok
    }

    /// Begin an array reply (`RedisModule_ReplyWithArray`).
    pub fn reply_with_array(&mut self, len: usize) -> Status {
        self.reply.push(ReplyValue::Array(len));
        Status::Ok
    }

    /// Consume and return accumulated replies.
    pub fn take_replies(&mut self) -> Vec<ReplyValue> {
        std::mem::take(&mut self.reply)
    }

    /// Create a new `RedisModuleString` from a `&str`.
    pub fn create_string(&self, s: &str) -> RedisModuleString {
        RedisModuleString::from_str(s)
    }

    /// Create a new `RedisModuleString` from raw bytes.
    pub fn create_string_from_bytes(&self, bytes: &[u8]) -> RedisModuleString {
        RedisModuleString::new(bytes.to_vec())
    }
}

impl Drop for RedisModuleCtx {
    fn drop(&mut self) {
        self.close_all_keys();
    }
}

/// A string value used by the Redis Module API (`RedisModuleString`).
///
/// Wraps a byte vector and provides conversion helpers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisModuleString {
    data: Vec<u8>,
}

impl RedisModuleString {
    /// Create from raw bytes.
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    /// Create from a UTF-8 string.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        Self {
            data: s.as_bytes().to_vec(),
        }
    }

    /// Return the underlying bytes.
    pub fn as_bytes(&self) -> Vec<u8> {
        self.data.clone()
    }

    /// Try to interpret the bytes as UTF-8.
    pub fn to_string_lossy(&self) -> String {
        String::from_utf8_lossy(&self.data).into_owned()
    }

    /// Length in bytes.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Whether the string is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Try to parse the string as an `i64`.
    pub fn to_i64(&self) -> Result<i64, std::num::ParseIntError> {
        self.to_string_lossy().parse()
    }

    /// Try to parse the string as an `f64`.
    pub fn to_f64(&self) -> Result<f64, std::num::ParseFloatError> {
        self.to_string_lossy().parse()
    }

    /// Convert to `Bytes`.
    pub fn to_bytes(&self) -> Bytes {
        Bytes::from(self.data.clone())
    }
}

impl From<&str> for RedisModuleString {
    fn from(s: &str) -> Self {
        Self::from_str(s)
    }
}

impl From<String> for RedisModuleString {
    fn from(s: String) -> Self {
        Self::new(s.into_bytes())
    }
}

impl From<Vec<u8>> for RedisModuleString {
    fn from(data: Vec<u8>) -> Self {
        Self::new(data)
    }
}

/// A key handle returned by `RedisModule_OpenKey`.
///
/// Provides read/write access to a key's value within the module context.
#[derive(Debug)]
pub struct RedisModuleKey {
    /// Key name
    name: String,
    /// Access mode
    mode: KeyMode,
    /// Current value (if loaded)
    value: Option<Vec<u8>>,
    /// Key type
    key_type: KeyType,
}

impl RedisModuleKey {
    /// Create a new key handle.
    pub fn new(name: String, mode: KeyMode) -> Self {
        Self {
            name,
            mode,
            value: None,
            key_type: KeyType::Empty,
        }
    }

    /// Get the key name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the access mode.
    pub fn mode(&self) -> KeyMode {
        self.mode
    }

    /// Get the key type (`RedisModule_KeyType`).
    pub fn key_type(&self) -> KeyType {
        self.key_type
    }

    /// Set the value for a string key (`RedisModule_StringSet`).
    pub fn string_set(&mut self, value: &RedisModuleString) -> Status {
        if self.mode != KeyMode::Write {
            return Status::Err;
        }
        self.value = Some(value.as_bytes());
        self.key_type = KeyType::String;
        Status::Ok
    }

    /// Direct memory access to the string value (`RedisModule_StringDMA`).
    ///
    /// Returns a reference to the underlying bytes if the key holds a string.
    pub fn string_dma(&self) -> Option<&[u8]> {
        if self.key_type != KeyType::String {
            return None;
        }
        self.value.as_deref()
    }

    /// Truncate the string value to the given length (`RedisModule_StringTruncate`).
    pub fn string_truncate(&mut self, new_len: usize) -> Status {
        if self.mode != KeyMode::Write || self.key_type != KeyType::String {
            return Status::Err;
        }
        if let Some(ref mut v) = self.value {
            v.truncate(new_len);
        }
        Status::Ok
    }

    /// Delete the key (`RedisModule_DeleteKey`).
    pub fn delete(&mut self) -> Status {
        if self.mode != KeyMode::Write {
            return Status::Err;
        }
        self.value = None;
        self.key_type = KeyType::Empty;
        Status::Ok
    }

    /// Get the value length in bytes (`RedisModule_ValueLength`).
    pub fn value_length(&self) -> usize {
        self.value.as_ref().map_or(0, |v| v.len())
    }
}

/// Reply value accumulated by the context's reply functions.
#[derive(Debug, Clone, PartialEq)]
pub enum ReplyValue {
    /// `+OK\r\n`
    SimpleString(String),
    /// `-ERR ...\r\n`
    Error(String),
    /// `:<integer>\r\n`
    Integer(i64),
    /// `$<len>\r\n<bytes>\r\n`
    BulkString(Vec<u8>),
    /// `*<len>\r\n`
    Array(usize),
    /// `$-1\r\n`
    Null,
}

/// Reply type returned by `RedisModule_Call` (`RedisModuleCallReply`).
#[derive(Debug, Clone)]
pub enum RedisModuleCallReply {
    /// String reply
    String(String),
    /// Error reply
    Error(String),
    /// Integer reply
    Integer(i64),
    /// Nil reply
    Nil,
    /// Array of sub-replies
    Array(Vec<RedisModuleCallReply>),
}

impl RedisModuleCallReply {
    /// Returns the reply type discriminant for pattern matching.
    pub fn reply_type(&self) -> &'static str {
        match self {
            RedisModuleCallReply::String(_) => "string",
            RedisModuleCallReply::Error(_) => "error",
            RedisModuleCallReply::Integer(_) => "integer",
            RedisModuleCallReply::Nil => "nil",
            RedisModuleCallReply::Array(_) => "array",
        }
    }
}

/// Shared, thread-safe reference to a `RedisModuleCtx`.
pub type SharedRedisModuleCtx = Arc<RwLock<RedisModuleCtx>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_module_string_from_str() {
        let s = RedisModuleString::from_str("hello");
        assert_eq!(s.to_string_lossy(), "hello");
        assert_eq!(s.len(), 5);
        assert!(!s.is_empty());
    }

    #[test]
    fn test_redis_module_string_conversions() {
        let s: RedisModuleString = "test".into();
        assert_eq!(s.to_i64(), Err("test".parse::<i64>().unwrap_err()));

        let n = RedisModuleString::from_str("42");
        assert_eq!(n.to_i64().unwrap(), 42);
        assert!((n.to_f64().unwrap() - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_redis_module_ctx_reply() {
        let mut ctx = RedisModuleCtx::new(0, 1);
        ctx.reply_with_simple_string("OK");
        ctx.reply_with_long_long(100);
        ctx.reply_with_null();

        let replies = ctx.take_replies();
        assert_eq!(replies.len(), 3);
        assert_eq!(replies[0], ReplyValue::SimpleString("OK".to_string()));
        assert_eq!(replies[1], ReplyValue::Integer(100));
        assert_eq!(replies[2], ReplyValue::Null);
    }

    #[test]
    fn test_redis_module_key_string_ops() {
        let mut key = RedisModuleKey::new("mykey".to_string(), KeyMode::Write);
        assert_eq!(key.key_type(), KeyType::Empty);

        let val = RedisModuleString::from_str("hello");
        assert_eq!(key.string_set(&val), Status::Ok);
        assert_eq!(key.key_type(), KeyType::String);
        assert_eq!(key.string_dma(), Some(b"hello".as_slice()));
        assert_eq!(key.value_length(), 5);

        assert_eq!(key.string_truncate(3), Status::Ok);
        assert_eq!(key.string_dma(), Some(b"hel".as_slice()));
    }

    #[test]
    fn test_redis_module_key_read_only() {
        let mut key = RedisModuleKey::new("mykey".to_string(), KeyMode::Read);
        let val = RedisModuleString::from_str("hello");
        assert_eq!(key.string_set(&val), Status::Err);
    }

    #[test]
    fn test_redis_module_ctx_open_close_key() {
        let mut ctx = RedisModuleCtx::new(0, 1);
        let key = ctx.open_key("test", KeyMode::Write);
        let val = RedisModuleString::from_str("value");
        key.string_set(&val);

        ctx.close_key("test");
    }

    #[test]
    fn test_redis_module_call_reply() {
        let reply = RedisModuleCallReply::String("OK".to_string());
        assert_eq!(reply.reply_type(), "string");

        let reply = RedisModuleCallReply::Array(vec![
            RedisModuleCallReply::Integer(1),
            RedisModuleCallReply::Nil,
        ]);
        assert_eq!(reply.reply_type(), "array");
    }
}
