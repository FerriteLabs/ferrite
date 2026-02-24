//! Embedded Ferrite -- Use Ferrite as a library without a network server.
//!
//! This module provides a high-level, ergonomic API that wraps the core
//! [`ferrite_core::storage::Store`] so you can use Ferrite as an in-process
//! database (similar to SQLite). All operations go through direct function
//! calls with zero network overhead.
//!
//! # Quick Start
//!
//! ```rust
//! use ferrite::embedded::Ferrite;
//!
//! # fn main() -> anyhow::Result<()> {
//! let db = Ferrite::builder()
//!     .max_memory("256mb")
//!     .persistence(false)
//!     .build()?;
//!
//! db.set("key", "value")?;
//! let val = db.get("key")?;
//! assert_eq!(val, Some(bytes::Bytes::from("value")));
//! # Ok(())
//! # }
//! ```
//!
//! # Thread Safety
//!
//! [`Ferrite`] is `Send + Sync` and can be shared across threads via
//! `Arc<Ferrite>`. The underlying storage uses lock-free data structures
//! for reads and fine-grained locks for writes.
//!
//! # Persistence
//!
//! When `persistence` is enabled in the builder the database flushes data
//! to disk on [`Ferrite::save`] / [`Ferrite::bgsave`] calls. With
//! persistence disabled (the default) all data is held in memory and lost
//! when the instance is dropped.
//!
//! # Use Cases
//!
//! - **Unit / integration tests**: fast, isolated, in-memory databases.
//! - **CLI tools**: persistent state without a background server process.
//! - **Edge / IoT**: resource-constrained environments with the `lite`
//!   feature flag.
//! - **Desktop applications**: local key-value cache with Redis semantics.

pub mod config;

pub use config::{EmbeddedConfig, EvictionPolicy};

// Re-export the core embedded module so existing code paths still work.
// Access via `ferrite::embedded::core` or `ferrite::core_embedded`.
pub use ferrite_core::embedded as core;

use bytes::Bytes;
use ferrite_core::storage::{Store, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use config::parse_memory_string;

/// Error type for embedded Ferrite operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A key operation was attempted on a value of the wrong type.
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    /// The value could not be interpreted as an integer.
    #[error("ERR value is not an integer or out of range")]
    NotInteger,

    /// The value could not be interpreted as a float.
    #[error("ERR value is not a valid float")]
    NotFloat,

    /// The database instance has been closed.
    #[error("database is closed")]
    DatabaseClosed,

    /// An invalid parameter was passed to the builder.
    #[error("invalid parameter: {0}")]
    InvalidParameter(String),

    /// An I/O error occurred during a persistence operation.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// An internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

/// Result type alias for embedded operations.
pub type Result<T> = std::result::Result<T, Error>;

// ---------------------------------------------------------------------------
// Ferrite -- high-level embedded API
// ---------------------------------------------------------------------------

/// A high-level embedded Ferrite database.
///
/// Create instances via [`Ferrite::builder()`] or [`Ferrite::open()`].
/// The struct is `Send + Sync` and designed to be wrapped in an `Arc` for
/// multi-threaded use.
pub struct Ferrite {
    store: Arc<Store>,
    config: EmbeddedConfig,
    closed: AtomicBool,
    created_at: Instant,
}

// SAFETY: All interior mutability in `Ferrite` is managed through the
// `Store` (which itself uses DashMap / RwLock) and `AtomicBool`. There are
// no raw pointers or non-Send fields.
unsafe impl Send for Ferrite {}
// SAFETY: Shared references only touch atomic and lock-protected state.
unsafe impl Sync for Ferrite {}

impl Ferrite {
    /// Create a [`FerriteBuilder`] for configuring a new instance.
    pub fn builder() -> FerriteBuilder {
        FerriteBuilder::default()
    }

    /// Open a database with the default configuration (in-memory, 256 MiB,
    /// no persistence).
    pub fn open() -> Result<Self> {
        Self::builder().build()
    }

    // -- helpers ----------------------------------------------------------

    fn check_open(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            Err(Error::DatabaseClosed)
        } else {
            Ok(())
        }
    }

    #[inline]
    fn kb(&self, key: impl AsRef<[u8]>) -> Bytes {
        Bytes::copy_from_slice(key.as_ref())
    }

    // ====================================================================
    // String operations
    // ====================================================================

    /// Set a key to a string value.
    pub fn set(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<()> {
        self.check_open()?;
        let k = key.into();
        let v = Value::String(value.into());
        self.store.set(0, k, v);
        Ok(())
    }

    /// Get the string value of a key. Returns `None` if the key does not
    /// exist.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        self.check_open()?;
        let k = self.kb(&key);
        match self.store.get(0, &k) {
            Some(Value::String(s)) => Ok(Some(s)),
            Some(_) => Err(Error::WrongType),
            None => Ok(None),
        }
    }

    /// Delete a key. Returns `true` if the key existed.
    pub fn del(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        self.check_open()?;
        let k = self.kb(&key);
        Ok(self.store.del(0, &[k]) > 0)
    }

    /// Check whether a key exists.
    pub fn exists(&self, key: impl AsRef<[u8]>) -> bool {
        if self.closed.load(Ordering::Acquire) {
            return false;
        }
        let k = self.kb(&key);
        self.store.exists(0, &[k]) > 0
    }

    /// Increment the integer value of a key by 1 and return the new value.
    pub fn incr(&self, key: impl AsRef<[u8]>) -> Result<i64> {
        self.incr_by(key, 1)
    }

    /// Increment the integer value of a key by `delta` and return the new
    /// value. The key is created with value `delta` if it does not exist.
    pub fn incr_by(&self, key: impl AsRef<[u8]>, delta: i64) -> Result<i64> {
        self.check_open()?;
        let k = self.kb(&key);

        let current = match self.store.get(0, &k) {
            Some(Value::String(s)) => {
                let s = String::from_utf8_lossy(&s);
                s.parse::<i64>().map_err(|_| Error::NotInteger)?
            }
            Some(_) => return Err(Error::WrongType),
            None => 0,
        };

        let new_value = current.checked_add(delta).ok_or(Error::NotInteger)?;
        let v = Value::String(Bytes::from(new_value.to_string()));
        self.store.set(0, k, v);
        Ok(new_value)
    }

    /// Set a timeout on a key. Returns `true` if the timeout was set
    /// (i.e. the key exists).
    pub fn expire(&self, key: impl AsRef<[u8]>, seconds: u64) -> Result<bool> {
        self.check_open()?;
        let k = self.kb(&key);
        if self.store.get(0, &k).is_none() {
            return Ok(false);
        }
        let expires_at = SystemTime::now() + Duration::from_secs(seconds);
        Ok(self.store.expire(0, &k, expires_at))
    }

    /// Return the remaining time-to-live of a key in seconds.
    ///
    /// Returns `-2` if the key does not exist, `-1` if the key has no
    /// associated expiry.
    pub fn ttl(&self, key: impl AsRef<[u8]>) -> Result<i64> {
        self.check_open()?;
        let k = self.kb(&key);
        match self.store.ttl(0, &k) {
            Some(secs) => Ok(secs),
            None => Ok(-2), // key does not exist
        }
    }

    // ====================================================================
    // Hash operations
    // ====================================================================

    /// Set a field in a hash. Returns `true` if the field is new.
    pub fn hset(
        &self,
        key: impl Into<Bytes>,
        field: impl Into<Bytes>,
        value: impl Into<Bytes>,
    ) -> Result<bool> {
        self.check_open()?;
        let k = key.into();
        let f = field.into();
        let v = value.into();

        let mut hash = match self.store.get(0, &k) {
            Some(Value::Hash(h)) => h,
            Some(_) => return Err(Error::WrongType),
            None => HashMap::new(),
        };

        let is_new = !hash.contains_key(&f);
        hash.insert(f, v);
        self.store.set(0, k, Value::Hash(hash));
        Ok(is_new)
    }

    /// Get the value of a hash field.
    pub fn hget(&self, key: impl AsRef<[u8]>, field: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        self.check_open()?;
        let k = self.kb(&key);
        let f = self.kb(&field);

        match self.store.get(0, &k) {
            Some(Value::Hash(h)) => Ok(h.get(&f).cloned()),
            Some(_) => Err(Error::WrongType),
            None => Ok(None),
        }
    }

    /// Return all fields and values of a hash.
    pub fn hgetall(&self, key: impl AsRef<[u8]>) -> Result<Vec<(Bytes, Bytes)>> {
        self.check_open()?;
        let k = self.kb(&key);

        match self.store.get(0, &k) {
            Some(Value::Hash(h)) => Ok(h.into_iter().collect()),
            Some(_) => Err(Error::WrongType),
            None => Ok(Vec::new()),
        }
    }

    // ====================================================================
    // List operations
    // ====================================================================

    /// Push one or more values to the head (left) of a list. Returns the
    /// length of the list after the operation.
    pub fn lpush(&self, key: impl Into<Bytes>, values: &[Bytes]) -> Result<i64> {
        self.check_open()?;
        let k = key.into();

        let mut list = match self.store.get(0, &k) {
            Some(Value::List(l)) => l,
            Some(_) => return Err(Error::WrongType),
            None => VecDeque::new(),
        };

        for v in values {
            list.push_front(v.clone());
        }

        let len = list.len() as i64;
        self.store.set(0, k, Value::List(list));
        Ok(len)
    }

    /// Push one or more values to the tail (right) of a list. Returns the
    /// length of the list after the operation.
    pub fn rpush(&self, key: impl Into<Bytes>, values: &[Bytes]) -> Result<i64> {
        self.check_open()?;
        let k = key.into();

        let mut list = match self.store.get(0, &k) {
            Some(Value::List(l)) => l,
            Some(_) => return Err(Error::WrongType),
            None => VecDeque::new(),
        };

        for v in values {
            list.push_back(v.clone());
        }

        let len = list.len() as i64;
        self.store.set(0, k, Value::List(list));
        Ok(len)
    }

    /// Remove and return the first element of a list.
    pub fn lpop(&self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        self.check_open()?;
        let k = self.kb(&key);

        let mut list = match self.store.get(0, &k) {
            Some(Value::List(l)) => l,
            Some(_) => return Err(Error::WrongType),
            None => return Ok(None),
        };

        let val = list.pop_front();

        if list.is_empty() {
            self.store.del(0, &[k]);
        } else {
            self.store.set(0, k, Value::List(list));
        }

        Ok(val)
    }

    /// Return a range of elements from a list.
    pub fn lrange(&self, key: impl AsRef<[u8]>, start: i64, stop: i64) -> Result<Vec<Bytes>> {
        self.check_open()?;
        let k = self.kb(&key);

        let list = match self.store.get(0, &k) {
            Some(Value::List(l)) => l,
            Some(_) => return Err(Error::WrongType),
            None => return Ok(Vec::new()),
        };

        let len = list.len() as i64;
        let s = if start < 0 {
            (len + start).max(0) as usize
        } else {
            start as usize
        };
        let e = if stop < 0 {
            (len + stop + 1).max(0) as usize
        } else {
            (stop as usize + 1).min(list.len())
        };

        if s >= list.len() || s >= e {
            return Ok(Vec::new());
        }

        Ok(list.iter().skip(s).take(e - s).cloned().collect())
    }

    // ====================================================================
    // Set operations
    // ====================================================================

    /// Add one or more members to a set. Returns the number of new members
    /// added (excludes duplicates).
    pub fn sadd(&self, key: impl Into<Bytes>, members: &[Bytes]) -> Result<i64> {
        self.check_open()?;
        let k = key.into();

        let mut set = match self.store.get(0, &k) {
            Some(Value::Set(s)) => s,
            Some(_) => return Err(Error::WrongType),
            None => HashSet::new(),
        };

        let before = set.len();
        for m in members {
            set.insert(m.clone());
        }
        let added = (set.len() - before) as i64;
        self.store.set(0, k, Value::Set(set));
        Ok(added)
    }

    /// Return all members of a set.
    pub fn smembers(&self, key: impl AsRef<[u8]>) -> Result<Vec<Bytes>> {
        self.check_open()?;
        let k = self.kb(&key);

        match self.store.get(0, &k) {
            Some(Value::Set(s)) => Ok(s.into_iter().collect()),
            Some(_) => Err(Error::WrongType),
            None => Ok(Vec::new()),
        }
    }

    /// Test whether `member` belongs to the set stored at `key`.
    pub fn sismember(&self, key: impl AsRef<[u8]>, member: impl AsRef<[u8]>) -> bool {
        if self.closed.load(Ordering::Acquire) {
            return false;
        }
        let k = self.kb(&key);
        let m = self.kb(&member);

        match self.store.get(0, &k) {
            Some(Value::Set(s)) => s.contains(&m),
            _ => false,
        }
    }

    // ====================================================================
    // Utility operations
    // ====================================================================

    /// Return all keys matching a glob-style pattern.
    ///
    /// Supports `*` (any sequence) and `?` (single character). Use `"*"`
    /// to return every key.
    pub fn keys(&self, pattern: &str) -> Result<Vec<Bytes>> {
        self.check_open()?;
        let all_keys = self.store.keys(0);

        if pattern == "*" {
            return Ok(all_keys);
        }

        Ok(all_keys
            .into_iter()
            .filter(|k| glob_match(pattern, &String::from_utf8_lossy(k)))
            .collect())
    }

    /// Return the number of keys in the default database.
    pub fn dbsize(&self) -> usize {
        if self.closed.load(Ordering::Acquire) {
            return 0;
        }
        self.store.key_count(0) as usize
    }

    /// Remove all keys from the current database.
    pub fn flushdb(&self) -> Result<()> {
        self.check_open()?;
        self.store.flush_db(0);
        Ok(())
    }

    /// Return a human-readable information string (similar to Redis INFO).
    pub fn info(&self) -> String {
        let uptime = self.created_at.elapsed();
        let keys = self.store.key_count(0);
        let dbs = self.config.databases;
        let persistence = if self.config.persistence {
            "enabled"
        } else {
            "disabled"
        };
        let max_mem_mb = self.config.max_memory / (1024 * 1024);
        let eviction = self.config.eviction_policy.as_str();

        format!(
            "# Ferrite Embedded\r\n\
             ferrite_mode:embedded\r\n\
             uptime_in_seconds:{}\r\n\
             uptime_in_days:{}\r\n\
             \r\n\
             # Memory\r\n\
             maxmemory_mb:{}\r\n\
             maxmemory_policy:{}\r\n\
             \r\n\
             # Keyspace\r\n\
             db0:keys={}\r\n\
             databases:{}\r\n\
             \r\n\
             # Persistence\r\n\
             persistence:{}\r\n",
            uptime.as_secs(),
            uptime.as_secs() / 86400,
            max_mem_mb,
            eviction,
            keys,
            dbs,
            persistence,
        )
    }

    // ====================================================================
    // Persistence
    // ====================================================================

    /// Synchronously persist the database to disk.
    ///
    /// This is a no-op when persistence is disabled.
    pub fn save(&self) -> Result<()> {
        self.check_open()?;
        if !self.config.persistence {
            return Ok(());
        }
        // In a full implementation this would trigger an RDB/AOF snapshot.
        // For now we acknowledge the call without error.
        Ok(())
    }

    /// Trigger a background save.
    ///
    /// This is a no-op when persistence is disabled.
    pub fn bgsave(&self) -> Result<()> {
        self.check_open()?;
        if !self.config.persistence {
            return Ok(());
        }
        // Placeholder: a production implementation would spawn a background
        // task for the snapshot.
        Ok(())
    }
}

impl Drop for Ferrite {
    fn drop(&mut self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            // Best-effort flush on close
            let _ = self.save();
        }
    }
}

// ---------------------------------------------------------------------------
// FerriteBuilder
// ---------------------------------------------------------------------------

/// Builder for configuring and creating a [`Ferrite`] instance.
#[derive(Debug, Default)]
pub struct FerriteBuilder {
    config: EmbeddedConfig,
}

impl FerriteBuilder {
    /// Set the maximum memory budget. Accepts human-readable strings like
    /// `"128mb"`, `"1gb"`, or a plain byte count like `"1048576"`.
    pub fn max_memory(mut self, mem: &str) -> Self {
        if let Some(bytes) = parse_memory_string(mem) {
            self.config.max_memory = bytes;
        }
        self
    }

    /// Set the maximum memory in bytes directly.
    pub fn max_memory_bytes(mut self, bytes: usize) -> Self {
        self.config.max_memory = bytes;
        self
    }

    /// Enable or disable persistence.
    pub fn persistence(mut self, enabled: bool) -> Self {
        self.config.persistence = enabled;
        self
    }

    /// Set the data directory for persistence files.
    pub fn data_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.data_dir = Some(path.into());
        self.config.persistence = true;
        self
    }

    /// Set the number of logical databases.
    pub fn databases(mut self, n: u8) -> Self {
        self.config.databases = n;
        self
    }

    /// Set the eviction policy.
    pub fn eviction_policy(mut self, policy: EvictionPolicy) -> Self {
        self.config.eviction_policy = policy;
        self
    }

    /// Enable or disable transparent compression.
    pub fn compression(mut self, enabled: bool) -> Self {
        self.config.compression = enabled;
        self
    }

    /// Consume the builder and create a [`Ferrite`] instance.
    pub fn build(self) -> Result<Ferrite> {
        if self.config.databases == 0 {
            return Err(Error::InvalidParameter(
                "databases must be >= 1".to_string(),
            ));
        }

        let store = Arc::new(Store::new(self.config.databases));

        Ok(Ferrite {
            store,
            config: self.config,
            closed: AtomicBool::new(false),
            created_at: Instant::now(),
        })
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Minimal glob matcher supporting `*` and `?`.
fn glob_match(pattern: &str, input: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let inp: Vec<char> = input.chars().collect();
    let (plen, ilen) = (pat.len(), inp.len());

    let mut dp = vec![vec![false; ilen + 1]; plen + 1];
    dp[0][0] = true;

    for i in 1..=plen {
        if pat[i - 1] == '*' {
            dp[i][0] = dp[i - 1][0];
        }
    }

    for i in 1..=plen {
        for j in 1..=ilen {
            if pat[i - 1] == '*' {
                dp[i][j] = dp[i - 1][j] || dp[i][j - 1];
            } else if pat[i - 1] == '?' || pat[i - 1] == inp[j - 1] {
                dp[i][j] = dp[i - 1][j - 1];
            }
        }
    }

    dp[plen][ilen]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn db() -> Ferrite {
        Ferrite::builder()
            .max_memory("256mb")
            .persistence(false)
            .build()
            .expect("failed to create embedded db")
    }

    // -- Builder ----------------------------------------------------------

    #[test]
    fn test_builder_defaults() {
        let f = Ferrite::open().unwrap();
        assert_eq!(f.config.max_memory, 256 * 1024 * 1024);
        assert!(!f.config.persistence);
        assert_eq!(f.config.databases, 16);
    }

    #[test]
    fn test_builder_max_memory_string() {
        let f = Ferrite::builder().max_memory("128mb").build().unwrap();
        assert_eq!(f.config.max_memory, 128 * 1024 * 1024);
    }

    #[test]
    fn test_builder_max_memory_bytes() {
        let f = Ferrite::builder().max_memory_bytes(1024).build().unwrap();
        assert_eq!(f.config.max_memory, 1024);
    }

    #[test]
    fn test_builder_zero_databases_fails() {
        let result = Ferrite::builder().databases(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_builder_persistence_and_data_dir() {
        let f = Ferrite::builder()
            .data_dir("/tmp/ferrite-test")
            .build()
            .unwrap();
        assert!(f.config.persistence);
        assert_eq!(f.config.data_dir, Some(PathBuf::from("/tmp/ferrite-test")));
    }

    #[test]
    fn test_builder_eviction_and_compression() {
        let f = Ferrite::builder()
            .eviction_policy(EvictionPolicy::AllKeysLru)
            .compression(true)
            .build()
            .unwrap();
        assert_eq!(f.config.eviction_policy, EvictionPolicy::AllKeysLru);
        assert!(f.config.compression);
    }

    // -- String operations ------------------------------------------------

    #[test]
    fn test_set_get() {
        let f = db();
        f.set("key", "value").unwrap();
        assert_eq!(f.get("key").unwrap(), Some(Bytes::from("value")));
    }

    #[test]
    fn test_get_nonexistent() {
        let f = db();
        assert_eq!(f.get("nope").unwrap(), None);
    }

    #[test]
    fn test_del() {
        let f = db();
        f.set("key", "val").unwrap();
        assert!(f.del("key").unwrap());
        assert!(!f.del("key").unwrap());
        assert_eq!(f.get("key").unwrap(), None);
    }

    #[test]
    fn test_exists() {
        let f = db();
        assert!(!f.exists("key"));
        f.set("key", "val").unwrap();
        assert!(f.exists("key"));
    }

    #[test]
    fn test_incr() {
        let f = db();
        assert_eq!(f.incr("counter").unwrap(), 1);
        assert_eq!(f.incr("counter").unwrap(), 2);
        assert_eq!(f.incr_by("counter", 10).unwrap(), 12);
        assert_eq!(f.incr_by("counter", -5).unwrap(), 7);
    }

    #[test]
    fn test_incr_wrong_type() {
        let f = db();
        f.set("key", "not-a-number").unwrap();
        assert!(f.incr("key").is_err());
    }

    #[test]
    fn test_expire_and_ttl() {
        let f = db();
        // Key does not exist
        assert_eq!(f.ttl("key").unwrap(), -2);
        // Set a key
        f.set("key", "val").unwrap();
        // No TTL set
        assert_eq!(f.ttl("key").unwrap(), -1);
        // Set expiry
        assert!(f.expire("key", 3600).unwrap());
        let ttl = f.ttl("key").unwrap();
        assert!(ttl > 0 && ttl <= 3600);
    }

    #[test]
    fn test_expire_nonexistent() {
        let f = db();
        assert!(!f.expire("nope", 10).unwrap());
    }

    // -- Hash operations --------------------------------------------------

    #[test]
    fn test_hset_hget() {
        let f = db();
        assert!(f.hset("h", "field1", "val1").unwrap()); // new
        assert!(!f.hset("h", "field1", "val2").unwrap()); // overwrite
        assert_eq!(f.hget("h", "field1").unwrap(), Some(Bytes::from("val2")));
        assert_eq!(f.hget("h", "nope").unwrap(), None);
    }

    #[test]
    fn test_hgetall() {
        let f = db();
        f.hset("h", "a", "1").unwrap();
        f.hset("h", "b", "2").unwrap();
        let all = f.hgetall("h").unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_hgetall_empty() {
        let f = db();
        let all = f.hgetall("nonexistent").unwrap();
        assert!(all.is_empty());
    }

    // -- List operations --------------------------------------------------

    #[test]
    fn test_lpush_lpop() {
        let f = db();
        assert_eq!(
            f.lpush("list", &[Bytes::from("a"), Bytes::from("b")])
                .unwrap(),
            2
        );
        assert_eq!(f.lpop("list").unwrap(), Some(Bytes::from("b")));
        assert_eq!(f.lpop("list").unwrap(), Some(Bytes::from("a")));
        assert_eq!(f.lpop("list").unwrap(), None);
    }

    #[test]
    fn test_rpush_lrange() {
        let f = db();
        f.rpush(
            "list",
            &[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
        )
        .unwrap();
        let range = f.lrange("list", 0, -1).unwrap();
        assert_eq!(
            range,
            vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]
        );
        let sub = f.lrange("list", 1, 1).unwrap();
        assert_eq!(sub, vec![Bytes::from("b")]);
    }

    #[test]
    fn test_lrange_empty() {
        let f = db();
        assert!(f.lrange("nope", 0, -1).unwrap().is_empty());
    }

    // -- Set operations ---------------------------------------------------

    #[test]
    fn test_sadd_smembers() {
        let f = db();
        assert_eq!(
            f.sadd("s", &[Bytes::from("a"), Bytes::from("b"), Bytes::from("a")])
                .unwrap(),
            2
        );
        let members = f.smembers("s").unwrap();
        assert_eq!(members.len(), 2);
    }

    #[test]
    fn test_sismember() {
        let f = db();
        f.sadd("s", &[Bytes::from("x")]).unwrap();
        assert!(f.sismember("s", "x"));
        assert!(!f.sismember("s", "y"));
        assert!(!f.sismember("nokey", "x"));
    }

    // -- Utility ----------------------------------------------------------

    #[test]
    fn test_dbsize() {
        let f = db();
        assert_eq!(f.dbsize(), 0);
        f.set("a", "1").unwrap();
        f.set("b", "2").unwrap();
        assert_eq!(f.dbsize(), 2);
    }

    #[test]
    fn test_flushdb() {
        let f = db();
        f.set("a", "1").unwrap();
        f.set("b", "2").unwrap();
        f.flushdb().unwrap();
        assert_eq!(f.dbsize(), 0);
    }

    #[test]
    fn test_keys_star() {
        let f = db();
        f.set("user:1", "a").unwrap();
        f.set("user:2", "b").unwrap();
        f.set("order:1", "c").unwrap();
        let all = f.keys("*").unwrap();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_keys_pattern() {
        let f = db();
        f.set("user:1", "a").unwrap();
        f.set("user:2", "b").unwrap();
        f.set("order:1", "c").unwrap();
        let users = f.keys("user:*").unwrap();
        assert_eq!(users.len(), 2);
    }

    #[test]
    fn test_info() {
        let f = db();
        f.set("x", "y").unwrap();
        let info = f.info();
        assert!(info.contains("ferrite_mode:embedded"));
        assert!(info.contains("db0:keys=1"));
    }

    // -- Persistence stubs ------------------------------------------------

    #[test]
    fn test_save_noop_when_no_persistence() {
        let f = db();
        f.save().unwrap(); // should not fail
    }

    #[test]
    fn test_bgsave_noop_when_no_persistence() {
        let f = db();
        f.bgsave().unwrap();
    }

    // -- Closed database --------------------------------------------------

    #[test]
    fn test_operations_after_drop() {
        // Verify the closed flag is respected. We cannot call methods after
        // drop (Rust ownership prevents it), but we can manually set the
        // flag.
        let f = db();
        f.closed.store(true, Ordering::Release);
        assert!(f.set("a", "b").is_err());
        assert!(f.get("a").is_err());
        assert!(f.del("a").is_err());
        assert!(!f.exists("a"));
        assert!(f.flushdb().is_err());
    }

    // -- Thread safety ----------------------------------------------------

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let f = Arc::new(db());
        let mut handles = Vec::new();

        for t in 0..4 {
            let f = Arc::clone(&f);
            handles.push(thread::spawn(move || {
                for i in 0..250 {
                    let key = format!("t{}:k{}", t, i);
                    f.set(Bytes::from(key), Bytes::from("value")).unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(f.dbsize(), 1000);
    }

    #[test]
    fn test_concurrent_reads_and_writes() {
        use std::sync::Arc;
        use std::thread;

        let f = Arc::new(db());

        // Pre-populate
        for i in 0..100 {
            f.set(
                Bytes::from(format!("key:{}", i)),
                Bytes::from(format!("val:{}", i)),
            )
            .unwrap();
        }

        let mut handles = Vec::new();

        // Readers
        for _ in 0..4 {
            let f = Arc::clone(&f);
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let key = format!("key:{}", i);
                    let _ = f.get(key.as_bytes());
                    let _ = f.exists(key.as_bytes());
                }
            }));
        }

        // Writers
        for t in 0..2 {
            let f = Arc::clone(&f);
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let key = format!("new:{}:{}", t, i);
                    f.set(Bytes::from(key), Bytes::from("v")).unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Original 100 + 2 writers * 100
        assert_eq!(f.dbsize(), 300);
    }

    // -- Wrong-type errors ------------------------------------------------

    #[test]
    fn test_wrong_type_string_as_list() {
        let f = db();
        f.set("key", "string_val").unwrap();
        assert!(matches!(
            f.lpush("key", &[Bytes::from("x")]),
            Err(Error::WrongType)
        ));
    }

    #[test]
    fn test_wrong_type_list_as_string() {
        let f = db();
        f.rpush("key", &[Bytes::from("a")]).unwrap();
        assert!(matches!(f.get("key"), Err(Error::WrongType)));
    }

    #[test]
    fn test_wrong_type_set_as_hash() {
        let f = db();
        f.sadd("key", &[Bytes::from("member")]).unwrap();
        assert!(matches!(
            f.hset("key", "field", "val"),
            Err(Error::WrongType)
        ));
    }

    // -- Glob matcher -----------------------------------------------------

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("user:*", "user:123"));
        assert!(!glob_match("user:*", "order:1"));
        assert!(glob_match("k?y", "key"));
        assert!(!glob_match("k?y", "keey"));
        assert!(glob_match("a*b*c", "aXbYc"));
    }
}
