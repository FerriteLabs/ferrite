//! Core database implementation for embedded mode

use super::error::{EmbeddedError, Result};
use super::{DEFAULT_MEMORY_LIMIT, DEFAULT_NUM_DATABASES};
use crate::storage::{Store, Value};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// Sync mode for persistence
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub enum SyncMode {
    /// No sync (fastest, data loss on crash)
    None,
    /// Sync on commit (default)
    #[default]
    Normal,
    /// Sync every write (slowest, safest)
    Full,
}

/// Configuration for embedded database
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EmbeddedConfig {
    /// Data directory (None for in-memory only)
    pub path: Option<PathBuf>,
    /// Maximum memory for hot data
    pub memory_limit: usize,
    /// Enable write-ahead logging
    pub wal_enabled: bool,
    /// Sync mode
    pub sync_mode: SyncMode,
    /// Number of databases (default: 16)
    pub num_databases: u8,
}

impl Default for EmbeddedConfig {
    fn default() -> Self {
        Self {
            path: None,
            memory_limit: DEFAULT_MEMORY_LIMIT,
            wal_enabled: true,
            sync_mode: SyncMode::Normal,
            num_databases: DEFAULT_NUM_DATABASES,
        }
    }
}

/// Key type enumeration
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyType {
    /// String type
    String,
    /// List type
    List,
    /// Hash type
    Hash,
    /// Set type
    Set,
    /// Sorted set type
    ZSet,
    /// Stream type
    Stream,
    /// HyperLogLog type
    HyperLogLog,
}

impl KeyType {
    /// Get the Redis type string
    pub fn as_str(&self) -> &'static str {
        match self {
            KeyType::String => "string",
            KeyType::List => "list",
            KeyType::Hash => "hash",
            KeyType::Set => "set",
            KeyType::ZSet => "zset",
            KeyType::Stream => "stream",
            KeyType::HyperLogLog => "hyperloglog",
        }
    }
}

/// Database statistics
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DatabaseStats {
    /// Total number of keys
    pub total_keys: u64,
    /// Memory usage in bytes (estimated)
    pub memory_bytes: u64,
    /// Number of gets
    pub gets: u64,
    /// Number of sets
    pub sets: u64,
    /// Number of deletes
    pub deletes: u64,
    /// Number of expired keys
    pub expired_keys: u64,
    /// Uptime in seconds
    pub uptime_secs: u64,
}

/// Execution statistics returned by a FerriteQL query.
#[derive(Clone, Debug, Default)]
pub struct QueryResultStats {
    /// Number of keys scanned during execution.
    pub keys_scanned: u64,
    /// Number of rows examined (before filtering).
    pub rows_examined: u64,
    /// Wall-clock execution time in milliseconds.
    pub execution_time_ms: f64,
}

/// A single row returned by a FerriteQL query.
#[derive(Clone, Debug)]
pub struct QueryResultRow {
    values: Vec<QueryValue>,
}

impl QueryResultRow {
    /// Returns a slice of the values in this row.
    pub fn values(&self) -> &[QueryValue] {
        &self.values
    }
}

/// A value in a query result.
#[derive(Clone, Debug)]
pub enum QueryValue {
    /// SQL NULL
    Null,
    /// Integer value
    Int(i64),
    /// Floating-point value
    Float(f64),
    /// UTF-8 string value
    String(String),
}

impl std::fmt::Display for QueryValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryValue::Null => write!(f, "NULL"),
            QueryValue::Int(n) => write!(f, "{}", n),
            QueryValue::Float(n) => write!(f, "{:.2}", n),
            QueryValue::String(s) => write!(f, "{}", s),
        }
    }
}

/// The result of a FerriteQL query, containing columns, rows, and optional
/// execution statistics.
#[derive(Clone, Debug)]
pub struct QueryResult {
    columns: Option<Vec<String>>,
    rows: Vec<QueryResultRow>,
    stats: Option<QueryResultStats>,
    message: Option<String>,
}

impl QueryResult {
    /// Returns the column names, if available.
    pub fn columns(&self) -> Option<&[String]> {
        self.columns.as_deref()
    }

    /// Returns the result rows.
    pub fn rows(&self) -> &[QueryResultRow] {
        &self.rows
    }

    /// Returns execution statistics, if collected.
    pub fn stats(&self) -> Option<&QueryResultStats> {
        self.stats.as_ref()
    }

    /// Returns the number of rows in the result.
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Returns an informational message (e.g. for DDL statements).
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }
}

/// Embedded database instance
pub struct Database {
    store: Arc<Store>,
    config: EmbeddedConfig,
    current_db: u8,
    closed: AtomicBool,
    created_at: SystemTime,
}

impl Database {
    /// Open a database (creates if doesn't exist)
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_config(EmbeddedConfig {
            path: Some(path.as_ref().to_path_buf()),
            ..Default::default()
        })
    }

    /// Open with custom configuration
    pub fn open_with_config(config: EmbeddedConfig) -> Result<Self> {
        let store = Arc::new(Store::new(config.num_databases));

        Ok(Self {
            store,
            config,
            current_db: 0,
            closed: AtomicBool::new(false),
            created_at: SystemTime::now(),
        })
    }

    /// Create an in-memory database
    pub fn memory() -> Result<Self> {
        Self::open_with_config(EmbeddedConfig {
            path: None,
            ..Default::default()
        })
    }

    /// Check if database is closed
    fn check_open(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            Err(EmbeddedError::DatabaseClosed)
        } else {
            Ok(())
        }
    }

    /// Get the current database index
    pub fn current_db(&self) -> u8 {
        self.current_db
    }

    /// Select a database
    pub fn select(&mut self, db: u8) -> Result<()> {
        self.check_open()?;
        if db >= self.config.num_databases {
            return Err(EmbeddedError::InvalidDatabase(db));
        }
        self.current_db = db;
        Ok(())
    }

    /// Close the database (flushes data)
    pub fn close(self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        self.sync()?;
        Ok(())
    }

    /// Force sync to disk
    pub fn sync(&self) -> Result<()> {
        // In a full implementation, this would sync the persistence layer
        Ok(())
    }

    /// Get database statistics
    pub fn stats(&self) -> DatabaseStats {
        let uptime = SystemTime::now()
            .duration_since(self.created_at)
            .unwrap_or_default()
            .as_secs();

        let total_keys = self.store.key_count(self.current_db);

        DatabaseStats {
            total_keys,
            memory_bytes: 0, // Would need memory tracking
            uptime_secs: uptime,
            ..Default::default()
        }
    }

    /// Get database configuration
    pub fn config(&self) -> &EmbeddedConfig {
        &self.config
    }

    // ==================== String Commands ====================

    /// GET key
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>> {
        self.check_open()?;
        let key = Bytes::copy_from_slice(key.as_ref());

        match self.store.get(self.current_db, &key) {
            Some(Value::String(s)) => Ok(Some(s.to_vec())),
            Some(_) => Err(EmbeddedError::WrongType),
            None => Ok(None),
        }
    }

    /// GET as string (convenience)
    pub fn get_str<K: AsRef<str>>(&self, key: K) -> Result<Option<String>> {
        match self.get(key.as_ref().as_bytes())? {
            Some(bytes) => {
                Ok(Some(String::from_utf8(bytes).map_err(|e| {
                    EmbeddedError::InvalidArgument(e.to_string())
                })?))
            }
            None => Ok(None),
        }
    }

    /// SET key value
    pub fn set<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> Result<()> {
        self.check_open()?;
        let key = Bytes::copy_from_slice(key.as_ref());
        let value = Value::String(Bytes::copy_from_slice(value.as_ref()));
        self.store.set(self.current_db, key, value);
        Ok(())
    }

    /// SET with expiry
    pub fn set_ex<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        value: V,
        ttl: Duration,
    ) -> Result<()> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());
        let value = Value::String(Bytes::copy_from_slice(value.as_ref()));
        let expires_at = SystemTime::now() + ttl;
        self.store
            .set_with_expiry(self.current_db, key_bytes, value, expires_at);
        Ok(())
    }

    /// SET if not exists
    pub fn set_nx<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> Result<bool> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        if self.store.get(self.current_db, &key_bytes).is_some() {
            return Ok(false);
        }

        let value = Value::String(Bytes::copy_from_slice(value.as_ref()));
        self.store.set(self.current_db, key_bytes, value);
        Ok(true)
    }

    /// SET if exists
    pub fn set_xx<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> Result<bool> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        if self.store.get(self.current_db, &key_bytes).is_none() {
            return Ok(false);
        }

        let value = Value::String(Bytes::copy_from_slice(value.as_ref()));
        self.store.set(self.current_db, key_bytes, value);
        Ok(true)
    }

    /// INCR
    pub fn incr<K: AsRef<[u8]>>(&self, key: K) -> Result<i64> {
        self.incr_by(key, 1)
    }

    /// INCRBY
    pub fn incr_by<K: AsRef<[u8]>>(&self, key: K, delta: i64) -> Result<i64> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        let current = match self.store.get(self.current_db, &key_bytes) {
            Some(Value::String(s)) => {
                let s = String::from_utf8_lossy(&s);
                s.parse::<i64>().map_err(|_| EmbeddedError::NotAnInteger)?
            }
            Some(_) => return Err(EmbeddedError::WrongType),
            None => 0,
        };

        let new_value = current
            .checked_add(delta)
            .ok_or(EmbeddedError::NotAnInteger)?;

        let value = Value::String(Bytes::from(new_value.to_string()));
        self.store.set(self.current_db, key_bytes, value);
        Ok(new_value)
    }

    /// INCRBYFLOAT
    pub fn incr_by_float<K: AsRef<[u8]>>(&self, key: K, delta: f64) -> Result<f64> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        let current = match self.store.get(self.current_db, &key_bytes) {
            Some(Value::String(s)) => {
                let s = String::from_utf8_lossy(&s);
                s.parse::<f64>().map_err(|_| EmbeddedError::NotAFloat)?
            }
            Some(_) => return Err(EmbeddedError::WrongType),
            None => 0.0,
        };

        let new_value = current + delta;
        if new_value.is_nan() || new_value.is_infinite() {
            return Err(EmbeddedError::NotAFloat);
        }

        let value = Value::String(Bytes::from(new_value.to_string()));
        self.store.set(self.current_db, key_bytes, value);
        Ok(new_value)
    }

    /// DECR
    pub fn decr<K: AsRef<[u8]>>(&self, key: K) -> Result<i64> {
        self.incr_by(key, -1)
    }

    /// DECRBY
    pub fn decr_by<K: AsRef<[u8]>>(&self, key: K, delta: i64) -> Result<i64> {
        self.incr_by(key, -delta)
    }

    /// APPEND
    pub fn append<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> Result<usize> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        let current = match self.store.get(self.current_db, &key_bytes) {
            Some(Value::String(s)) => s.to_vec(),
            Some(_) => return Err(EmbeddedError::WrongType),
            None => Vec::new(),
        };

        let mut new_value = current;
        new_value.extend_from_slice(value.as_ref());
        let len = new_value.len();

        let value = Value::String(Bytes::from(new_value));
        self.store.set(self.current_db, key_bytes, value);
        Ok(len)
    }

    /// STRLEN
    pub fn strlen<K: AsRef<[u8]>>(&self, key: K) -> Result<usize> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        match self.store.get(self.current_db, &key_bytes) {
            Some(Value::String(s)) => Ok(s.len()),
            Some(_) => Err(EmbeddedError::WrongType),
            None => Ok(0),
        }
    }

    /// MGET
    pub fn mget<K: AsRef<[u8]>>(&self, keys: &[K]) -> Result<Vec<Option<Vec<u8>>>> {
        self.check_open()?;
        let mut results = Vec::with_capacity(keys.len());

        for key in keys {
            let key_bytes = Bytes::copy_from_slice(key.as_ref());
            match self.store.get(self.current_db, &key_bytes) {
                Some(Value::String(s)) => results.push(Some(s.to_vec())),
                _ => results.push(None),
            }
        }

        Ok(results)
    }

    /// MSET
    pub fn mset<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, pairs: &[(K, V)]) -> Result<()> {
        self.check_open()?;

        for (key, value) in pairs {
            let key_bytes = Bytes::copy_from_slice(key.as_ref());
            let value = Value::String(Bytes::copy_from_slice(value.as_ref()));
            self.store.set(self.current_db, key_bytes, value);
        }

        Ok(())
    }

    // ==================== Hash Commands ====================

    /// HSET
    pub fn hset<K: AsRef<[u8]>, F: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        field: F,
        value: V,
    ) -> Result<bool> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());
        let field_bytes = Bytes::copy_from_slice(field.as_ref());
        let value_bytes = Bytes::copy_from_slice(value.as_ref());

        let mut hash = match self.store.get(self.current_db, &key_bytes) {
            Some(Value::Hash(h)) => h,
            Some(_) => return Err(EmbeddedError::WrongType),
            None => HashMap::new(),
        };

        let is_new = !hash.contains_key(&field_bytes);
        hash.insert(field_bytes, value_bytes);

        self.store
            .set(self.current_db, key_bytes, Value::Hash(hash));
        Ok(is_new)
    }

    /// HGET
    pub fn hget<K: AsRef<[u8]>, F: AsRef<[u8]>>(
        &self,
        key: K,
        field: F,
    ) -> Result<Option<Vec<u8>>> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());
        let field_bytes = Bytes::copy_from_slice(field.as_ref());

        match self.store.get(self.current_db, &key_bytes) {
            Some(Value::Hash(h)) => Ok(h.get(&field_bytes).map(|v| v.to_vec())),
            Some(_) => Err(EmbeddedError::WrongType),
            None => Ok(None),
        }
    }

    /// HMSET
    pub fn hmset<K: AsRef<[u8]>, F: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        pairs: &[(F, V)],
    ) -> Result<()> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        let mut hash = match self.store.get(self.current_db, &key_bytes) {
            Some(Value::Hash(h)) => h,
            Some(_) => return Err(EmbeddedError::WrongType),
            None => HashMap::new(),
        };

        for (field, value) in pairs {
            let field_bytes = Bytes::copy_from_slice(field.as_ref());
            let value_bytes = Bytes::copy_from_slice(value.as_ref());
            hash.insert(field_bytes, value_bytes);
        }

        self.store
            .set(self.current_db, key_bytes, Value::Hash(hash));
        Ok(())
    }

    /// HMGET
    pub fn hmget<K: AsRef<[u8]>, F: AsRef<[u8]>>(
        &self,
        key: K,
        fields: &[F],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        let hash = match self.store.get(self.current_db, &key_bytes) {
            Some(Value::Hash(h)) => h,
            Some(_) => return Err(EmbeddedError::WrongType),
            None => return Ok(vec![None; fields.len()]),
        };

        let mut results = Vec::with_capacity(fields.len());
        for field in fields {
            let field_bytes = Bytes::copy_from_slice(field.as_ref());
            results.push(hash.get(&field_bytes).map(|v| v.to_vec()));
        }

        Ok(results)
    }

    /// HGETALL
    pub fn hgetall<K: AsRef<[u8]>>(&self, key: K) -> Result<HashMap<Vec<u8>, Vec<u8>>> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        match self.store.get(self.current_db, &key_bytes) {
            Some(Value::Hash(h)) => Ok(h.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect()),
            Some(_) => Err(EmbeddedError::WrongType),
            None => Ok(HashMap::new()),
        }
    }

    /// HDEL
    pub fn hdel<K: AsRef<[u8]>, F: AsRef<[u8]>>(&self, key: K, fields: &[F]) -> Result<usize> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        let mut hash = match self.store.get(self.current_db, &key_bytes) {
            Some(Value::Hash(h)) => h,
            Some(_) => return Err(EmbeddedError::WrongType),
            None => return Ok(0),
        };

        let mut removed = 0;
        for field in fields {
            let field_bytes = Bytes::copy_from_slice(field.as_ref());
            if hash.remove(&field_bytes).is_some() {
                removed += 1;
            }
        }

        if hash.is_empty() {
            self.store.del(self.current_db, &[key_bytes]);
        } else {
            self.store
                .set(self.current_db, key_bytes, Value::Hash(hash));
        }

        Ok(removed)
    }

    /// HEXISTS
    pub fn hexists<K: AsRef<[u8]>, F: AsRef<[u8]>>(&self, key: K, field: F) -> Result<bool> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());
        let field_bytes = Bytes::copy_from_slice(field.as_ref());

        match self.store.get(self.current_db, &key_bytes) {
            Some(Value::Hash(h)) => Ok(h.contains_key(&field_bytes)),
            Some(_) => Err(EmbeddedError::WrongType),
            None => Ok(false),
        }
    }

    /// HINCRBY
    pub fn hincrby<K: AsRef<[u8]>, F: AsRef<[u8]>>(
        &self,
        key: K,
        field: F,
        delta: i64,
    ) -> Result<i64> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());
        let field_bytes = Bytes::copy_from_slice(field.as_ref());

        let mut hash = match self.store.get(self.current_db, &key_bytes) {
            Some(Value::Hash(h)) => h,
            Some(_) => return Err(EmbeddedError::WrongType),
            None => HashMap::new(),
        };

        let current = match hash.get(&field_bytes) {
            Some(v) => {
                let s = String::from_utf8_lossy(v);
                s.parse::<i64>().map_err(|_| EmbeddedError::NotAnInteger)?
            }
            None => 0,
        };

        let new_value = current
            .checked_add(delta)
            .ok_or(EmbeddedError::NotAnInteger)?;

        hash.insert(field_bytes, Bytes::from(new_value.to_string()));
        self.store
            .set(self.current_db, key_bytes, Value::Hash(hash));
        Ok(new_value)
    }

    /// HKEYS
    pub fn hkeys<K: AsRef<[u8]>>(&self, key: K) -> Result<Vec<Vec<u8>>> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        match self.store.get(self.current_db, &key_bytes) {
            Some(Value::Hash(h)) => Ok(h.keys().map(|k| k.to_vec()).collect()),
            Some(_) => Err(EmbeddedError::WrongType),
            None => Ok(vec![]),
        }
    }

    /// HVALS
    pub fn hvals<K: AsRef<[u8]>>(&self, key: K) -> Result<Vec<Vec<u8>>> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        match self.store.get(self.current_db, &key_bytes) {
            Some(Value::Hash(h)) => Ok(h.values().map(|v| v.to_vec()).collect()),
            Some(_) => Err(EmbeddedError::WrongType),
            None => Ok(vec![]),
        }
    }

    /// HLEN
    pub fn hlen<K: AsRef<[u8]>>(&self, key: K) -> Result<usize> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        match self.store.get(self.current_db, &key_bytes) {
            Some(Value::Hash(h)) => Ok(h.len()),
            Some(_) => Err(EmbeddedError::WrongType),
            None => Ok(0),
        }
    }

    // ==================== List Commands ====================

    /// LPUSH
    pub fn lpush<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, values: &[V]) -> Result<usize> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        let mut list = match self.store.get(self.current_db, &key_bytes) {
            Some(Value::List(l)) => l,
            Some(_) => return Err(EmbeddedError::WrongType),
            None => VecDeque::new(),
        };

        for value in values.iter() {
            list.push_front(Bytes::copy_from_slice(value.as_ref()));
        }

        let len = list.len();
        self.store
            .set(self.current_db, key_bytes, Value::List(list));
        Ok(len)
    }

    /// RPUSH
    pub fn rpush<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, values: &[V]) -> Result<usize> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        let mut list = match self.store.get(self.current_db, &key_bytes) {
            Some(Value::List(l)) => l,
            Some(_) => return Err(EmbeddedError::WrongType),
            None => VecDeque::new(),
        };

        for value in values {
            list.push_back(Bytes::copy_from_slice(value.as_ref()));
        }

        let len = list.len();
        self.store
            .set(self.current_db, key_bytes, Value::List(list));
        Ok(len)
    }

    /// LPOP
    pub fn lpop<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        let mut list = match self.store.get(self.current_db, &key_bytes) {
            Some(Value::List(l)) => l,
            Some(_) => return Err(EmbeddedError::WrongType),
            None => return Ok(None),
        };

        let value = list.pop_front();

        if list.is_empty() {
            self.store.del(self.current_db, &[key_bytes]);
        } else {
            self.store
                .set(self.current_db, key_bytes, Value::List(list));
        }

        Ok(value.map(|v| v.to_vec()))
    }

    /// RPOP
    pub fn rpop<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        let mut list = match self.store.get(self.current_db, &key_bytes) {
            Some(Value::List(l)) => l,
            Some(_) => return Err(EmbeddedError::WrongType),
            None => return Ok(None),
        };

        let value = list.pop_back();

        if list.is_empty() {
            self.store.del(self.current_db, &[key_bytes]);
        } else {
            self.store
                .set(self.current_db, key_bytes, Value::List(list));
        }

        Ok(value.map(|v| v.to_vec()))
    }

    /// LRANGE
    pub fn lrange<K: AsRef<[u8]>>(&self, key: K, start: i64, stop: i64) -> Result<Vec<Vec<u8>>> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        let list = match self.store.get(self.current_db, &key_bytes) {
            Some(Value::List(l)) => l,
            Some(_) => return Err(EmbeddedError::WrongType),
            None => return Ok(vec![]),
        };

        let len = list.len() as i64;
        let start_idx = if start < 0 {
            (len + start).max(0) as usize
        } else {
            start as usize
        };
        let stop_idx = if stop < 0 {
            (len + stop + 1).max(0) as usize
        } else {
            (stop as usize + 1).min(list.len())
        };

        if start_idx >= list.len() || start_idx >= stop_idx {
            return Ok(vec![]);
        }

        Ok(list
            .iter()
            .skip(start_idx)
            .take(stop_idx - start_idx)
            .map(|v| v.to_vec())
            .collect())
    }

    /// LINDEX
    pub fn lindex<K: AsRef<[u8]>>(&self, key: K, index: i64) -> Result<Option<Vec<u8>>> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        let list = match self.store.get(self.current_db, &key_bytes) {
            Some(Value::List(l)) => l,
            Some(_) => return Err(EmbeddedError::WrongType),
            None => return Ok(None),
        };

        let len = list.len() as i64;
        let idx = if index < 0 { len + index } else { index };

        if idx < 0 || idx >= len {
            return Ok(None);
        }

        Ok(list.get(idx as usize).map(|v| v.to_vec()))
    }

    /// LSET
    pub fn lset<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, index: i64, value: V) -> Result<()> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        let mut list = match self.store.get(self.current_db, &key_bytes) {
            Some(Value::List(l)) => l,
            Some(_) => return Err(EmbeddedError::WrongType),
            None => {
                return Err(EmbeddedError::KeyNotFound(
                    String::from_utf8_lossy(key.as_ref()).to_string(),
                ))
            }
        };

        let len = list.len() as i64;
        let idx = if index < 0 { len + index } else { index };

        if idx < 0 || idx >= len {
            return Err(EmbeddedError::IndexOutOfRange);
        }

        list[idx as usize] = Bytes::copy_from_slice(value.as_ref());
        self.store
            .set(self.current_db, key_bytes, Value::List(list));
        Ok(())
    }

    /// LLEN
    pub fn llen<K: AsRef<[u8]>>(&self, key: K) -> Result<usize> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        match self.store.get(self.current_db, &key_bytes) {
            Some(Value::List(l)) => Ok(l.len()),
            Some(_) => Err(EmbeddedError::WrongType),
            None => Ok(0),
        }
    }

    /// LTRIM
    pub fn ltrim<K: AsRef<[u8]>>(&self, key: K, start: i64, stop: i64) -> Result<()> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        let list = match self.store.get(self.current_db, &key_bytes) {
            Some(Value::List(l)) => l,
            Some(_) => return Err(EmbeddedError::WrongType),
            None => return Ok(()),
        };

        let len = list.len() as i64;
        let start_idx = if start < 0 {
            (len + start).max(0) as usize
        } else {
            start as usize
        };
        let stop_idx = if stop < 0 {
            (len + stop + 1).max(0) as usize
        } else {
            (stop as usize + 1).min(list.len())
        };

        if start_idx >= list.len() || start_idx >= stop_idx {
            self.store.del(self.current_db, &[key_bytes]);
            return Ok(());
        }

        let trimmed: VecDeque<Bytes> = list
            .iter()
            .skip(start_idx)
            .take(stop_idx - start_idx)
            .cloned()
            .collect();

        if trimmed.is_empty() {
            self.store.del(self.current_db, &[key_bytes]);
        } else {
            self.store
                .set(self.current_db, key_bytes, Value::List(trimmed));
        }
        Ok(())
    }

    // ==================== Set Commands ====================

    /// SADD
    pub fn sadd<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, members: &[V]) -> Result<usize> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        let mut set = match self.store.get(self.current_db, &key_bytes) {
            Some(Value::Set(s)) => s,
            Some(_) => return Err(EmbeddedError::WrongType),
            None => HashSet::new(),
        };

        let original_len = set.len();
        for member in members {
            set.insert(Bytes::copy_from_slice(member.as_ref()));
        }

        let added = set.len() - original_len;
        self.store.set(self.current_db, key_bytes, Value::Set(set));
        Ok(added)
    }

    /// SREM
    pub fn srem<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, members: &[V]) -> Result<usize> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        let mut set = match self.store.get(self.current_db, &key_bytes) {
            Some(Value::Set(s)) => s,
            Some(_) => return Err(EmbeddedError::WrongType),
            None => return Ok(0),
        };

        let mut removed = 0;
        for member in members {
            let member_bytes = Bytes::copy_from_slice(member.as_ref());
            if set.remove(&member_bytes) {
                removed += 1;
            }
        }

        if set.is_empty() {
            self.store.del(self.current_db, &[key_bytes]);
        } else {
            self.store.set(self.current_db, key_bytes, Value::Set(set));
        }

        Ok(removed)
    }

    /// SMEMBERS
    pub fn smembers<K: AsRef<[u8]>>(&self, key: K) -> Result<HashSet<Vec<u8>>> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        match self.store.get(self.current_db, &key_bytes) {
            Some(Value::Set(s)) => Ok(s.iter().map(|v| v.to_vec()).collect()),
            Some(_) => Err(EmbeddedError::WrongType),
            None => Ok(HashSet::new()),
        }
    }

    /// SISMEMBER
    pub fn sismember<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, member: V) -> Result<bool> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());
        let member_bytes = Bytes::copy_from_slice(member.as_ref());

        match self.store.get(self.current_db, &key_bytes) {
            Some(Value::Set(s)) => Ok(s.contains(&member_bytes)),
            Some(_) => Err(EmbeddedError::WrongType),
            None => Ok(false),
        }
    }

    /// SCARD
    pub fn scard<K: AsRef<[u8]>>(&self, key: K) -> Result<usize> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        match self.store.get(self.current_db, &key_bytes) {
            Some(Value::Set(s)) => Ok(s.len()),
            Some(_) => Err(EmbeddedError::WrongType),
            None => Ok(0),
        }
    }

    // ==================== Key Commands ====================

    /// DEL
    pub fn del<K: AsRef<[u8]>>(&self, keys: &[K]) -> Result<usize> {
        self.check_open()?;
        let key_bytes: Vec<Bytes> = keys
            .iter()
            .map(|k| Bytes::copy_from_slice(k.as_ref()))
            .collect();

        let deleted = self.store.del(self.current_db, &key_bytes);
        Ok(deleted as usize)
    }

    /// EXISTS
    pub fn exists<K: AsRef<[u8]>>(&self, keys: &[K]) -> Result<usize> {
        self.check_open()?;
        let key_bytes: Vec<Bytes> = keys
            .iter()
            .map(|k| Bytes::copy_from_slice(k.as_ref()))
            .collect();

        let count = self.store.exists(self.current_db, &key_bytes);
        Ok(count as usize)
    }

    /// EXPIRE
    pub fn expire<K: AsRef<[u8]>>(&self, key: K, ttl: Duration) -> Result<bool> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        if self.store.get(self.current_db, &key_bytes).is_none() {
            return Ok(false);
        }

        let expires_at = SystemTime::now() + ttl;
        Ok(self.store.expire(self.current_db, &key_bytes, expires_at))
    }

    /// EXPIREAT (takes Unix timestamp)
    pub fn expire_at<K: AsRef<[u8]>>(&self, key: K, timestamp: SystemTime) -> Result<bool> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        if self.store.get(self.current_db, &key_bytes).is_none() {
            return Ok(false);
        }

        // Use SystemTime directly for expiration
        let expires_at = timestamp;

        Ok(self.store.expire(self.current_db, &key_bytes, expires_at))
    }

    /// TTL (returns seconds until expiry, or None if no expiry)
    pub fn ttl<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Duration>> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        if self.store.get(self.current_db, &key_bytes).is_none() {
            return Ok(None);
        }

        match self.store.ttl(self.current_db, &key_bytes) {
            Some(ms) if ms > 0 => Ok(Some(Duration::from_millis(ms as u64))),
            Some(_) => Ok(None), // Already expired or no TTL
            None => Ok(None),
        }
    }

    /// PERSIST (remove expiry)
    pub fn persist<K: AsRef<[u8]>>(&self, key: K) -> Result<bool> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        // Get the value and re-set it without expiry
        match self.store.get(self.current_db, &key_bytes) {
            Some(value) => {
                self.store.set(self.current_db, key_bytes, value);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// TYPE
    pub fn key_type<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<KeyType>> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        match self.store.get(self.current_db, &key_bytes) {
            Some(value) => Ok(Some(match value {
                Value::String(_) => KeyType::String,
                Value::List(_) => KeyType::List,
                Value::Hash(_) => KeyType::Hash,
                Value::Set(_) => KeyType::Set,
                Value::SortedSet { .. } => KeyType::ZSet,
                Value::Stream(_) => KeyType::Stream,
                Value::HyperLogLog(_) => KeyType::HyperLogLog,
            })),
            None => Ok(None),
        }
    }

    /// RENAME
    pub fn rename<K: AsRef<[u8]>, N: AsRef<[u8]>>(&self, key: K, new_key: N) -> Result<()> {
        self.check_open()?;
        let key_bytes = Bytes::copy_from_slice(key.as_ref());
        let new_key_bytes = Bytes::copy_from_slice(new_key.as_ref());

        let value = self.store.get(self.current_db, &key_bytes).ok_or_else(|| {
            EmbeddedError::KeyNotFound(String::from_utf8_lossy(key.as_ref()).to_string())
        })?;

        self.store.del(self.current_db, &[key_bytes]);
        self.store.set(self.current_db, new_key_bytes, value);
        Ok(())
    }

    /// DBSIZE
    pub fn dbsize(&self) -> Result<usize> {
        self.check_open()?;
        Ok(self.store.key_count(self.current_db) as usize)
    }

    /// FLUSHDB
    pub fn flushdb(&self) -> Result<()> {
        self.check_open()?;
        self.store.flush_db(self.current_db);
        Ok(())
    }

    /// FLUSHALL
    pub fn flushall(&self) -> Result<()> {
        self.check_open()?;
        self.store.flush_all();
        Ok(())
    }

    /// Execute a FerriteQL query and return the result.
    ///
    /// Supports SELECT, INSERT, UPDATE, DELETE, CREATE VIEW, DROP VIEW,
    /// and EXPLAIN statements.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ferrite_core::embedded::Database;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Database::memory()?;
    /// db.hset("hash:user:1", "name", "Alice")?;
    /// let result = db.query("SELECT name FROM hash:user:*")?;
    /// assert!(result.row_count() > 0);
    /// # Ok(())
    /// # }
    /// ```
    pub fn query(&self, query: &str) -> Result<QueryResult> {
        use crate::query::{QueryConfig, QueryEngine};

        self.check_open()?;

        let engine = QueryEngine::new(self.store.clone(), QueryConfig::default());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EmbeddedError::Internal(format!("runtime error: {}", e)))?;

        let result = rt.block_on(engine.execute(query, self.current_db));

        match result {
            Ok(rs) => {
                let columns = if rs.columns.is_empty() {
                    None
                } else {
                    Some(rs.columns.clone())
                };

                let rows = rs
                    .rows
                    .iter()
                    .map(|row| {
                        let values = row
                            .values
                            .iter()
                            .map(|v| match v {
                                crate::query::Value::Null => QueryValue::Null,
                                crate::query::Value::Bool(b) => QueryValue::String(b.to_string()),
                                crate::query::Value::Int(n) => QueryValue::Int(*n),
                                crate::query::Value::Float(f) => QueryValue::Float(*f),
                                crate::query::Value::String(s) => QueryValue::String(s.clone()),
                                crate::query::Value::Bytes(b) => {
                                    QueryValue::String(String::from_utf8_lossy(b).to_string())
                                }
                                crate::query::Value::Array(a) => {
                                    QueryValue::String(format!("{:?}", a))
                                }
                                crate::query::Value::Map(m) => {
                                    QueryValue::String(format!("{:?}", m))
                                }
                            })
                            .collect();
                        QueryResultRow { values }
                    })
                    .collect();

                let stats = Some(QueryResultStats {
                    keys_scanned: rs.stats.keys_scanned,
                    rows_examined: rs.stats.rows_examined,
                    execution_time_ms: rs.stats.execution_time_us as f64 / 1000.0,
                });

                Ok(QueryResult {
                    columns,
                    rows,
                    stats,
                    message: None,
                })
            }
            Err(e) => Err(EmbeddedError::QueryError(e.to_string())),
        }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        let _ = self.sync();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_database() {
        let db = Database::memory().unwrap();
        assert_eq!(db.current_db(), 0);
    }

    #[test]
    fn test_set_get() {
        let db = Database::memory().unwrap();
        db.set("key", "value").unwrap();
        assert_eq!(db.get("key").unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn test_get_str() {
        let db = Database::memory().unwrap();
        db.set("key", "hello world").unwrap();
        assert_eq!(db.get_str("key").unwrap(), Some("hello world".to_string()));
    }

    #[test]
    fn test_set_nx() {
        let db = Database::memory().unwrap();
        assert!(db.set_nx("key", "value1").unwrap());
        assert!(!db.set_nx("key", "value2").unwrap());
        assert_eq!(db.get("key").unwrap(), Some(b"value1".to_vec()));
    }

    #[test]
    fn test_set_xx() {
        let db = Database::memory().unwrap();
        assert!(!db.set_xx("key", "value").unwrap());
        db.set("key", "value1").unwrap();
        assert!(db.set_xx("key", "value2").unwrap());
        assert_eq!(db.get("key").unwrap(), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_incr() {
        let db = Database::memory().unwrap();
        assert_eq!(db.incr("counter").unwrap(), 1);
        assert_eq!(db.incr("counter").unwrap(), 2);
        assert_eq!(db.incr_by("counter", 10).unwrap(), 12);
    }

    #[test]
    fn test_append() {
        let db = Database::memory().unwrap();
        assert_eq!(db.append("key", "hello").unwrap(), 5);
        assert_eq!(db.append("key", " world").unwrap(), 11);
        assert_eq!(db.get("key").unwrap(), Some(b"hello world".to_vec()));
    }

    #[test]
    fn test_hash_operations() {
        let db = Database::memory().unwrap();
        assert!(db.hset("user:123", "name", "Alice").unwrap());
        assert!(!db.hset("user:123", "name", "Bob").unwrap());
        assert_eq!(db.hget("user:123", "name").unwrap(), Some(b"Bob".to_vec()));
        assert_eq!(db.hlen("user:123").unwrap(), 1);
    }

    #[test]
    fn test_list_operations() {
        let db = Database::memory().unwrap();
        assert_eq!(db.rpush("list", &["a", "b", "c"]).unwrap(), 3);
        assert_eq!(db.lpush("list", &["x", "y"]).unwrap(), 5);
        assert_eq!(db.llen("list").unwrap(), 5);
        assert_eq!(db.lpop("list").unwrap(), Some(b"y".to_vec()));
        assert_eq!(db.rpop("list").unwrap(), Some(b"c".to_vec()));
    }

    #[test]
    fn test_set_operations() {
        let db = Database::memory().unwrap();
        assert_eq!(db.sadd("tags", &["rust", "redis", "rust"]).unwrap(), 2);
        assert!(db.sismember("tags", "rust").unwrap());
        assert!(!db.sismember("tags", "python").unwrap());
        assert_eq!(db.scard("tags").unwrap(), 2);
    }

    #[test]
    fn test_del_exists() {
        let db = Database::memory().unwrap();
        db.set("key1", "value1").unwrap();
        db.set("key2", "value2").unwrap();
        assert_eq!(db.exists(&["key1", "key2", "key3"]).unwrap(), 2);
        assert_eq!(db.del(&["key1", "key3"]).unwrap(), 1);
        assert_eq!(db.exists(&["key1"]).unwrap(), 0);
    }

    #[test]
    fn test_select_database() {
        let mut db = Database::memory().unwrap();
        db.set("key", "value0").unwrap();
        db.select(1).unwrap();
        assert!(db.get("key").unwrap().is_none());
        db.set("key", "value1").unwrap();
        db.select(0).unwrap();
        assert_eq!(db.get("key").unwrap(), Some(b"value0".to_vec()));
    }

    #[test]
    fn test_wrong_type_error() {
        let db = Database::memory().unwrap();
        db.set("string_key", "value").unwrap();
        assert!(db.lpush("string_key", &["item"]).is_err());
    }

    #[test]
    fn test_dbsize() {
        let db = Database::memory().unwrap();
        assert_eq!(db.dbsize().unwrap(), 0);
        db.set("key1", "value1").unwrap();
        db.set("key2", "value2").unwrap();
        assert_eq!(db.dbsize().unwrap(), 2);
    }

    #[test]
    fn test_flushdb() {
        let mut db = Database::memory().unwrap();
        db.set("key1", "value1").unwrap();
        db.select(1).unwrap();
        db.set("key2", "value2").unwrap();
        db.flushdb().unwrap();
        assert_eq!(db.dbsize().unwrap(), 0);
        db.select(0).unwrap();
        assert_eq!(db.dbsize().unwrap(), 1);
    }

    #[test]
    fn test_query_select() {
        let db = Database::memory().unwrap();
        db.hset("hash:user:1", "name", "Alice").unwrap();
        db.hset("hash:user:1", "age", "30").unwrap();
        db.hset("hash:user:2", "name", "Bob").unwrap();
        db.hset("hash:user:2", "age", "25").unwrap();

        let result = db.query("SELECT name, age FROM hash:user:*").unwrap();
        assert!(result.columns().is_some());
        assert!(result.row_count() >= 2);
    }

    #[test]
    fn test_query_invalid() {
        let db = Database::memory().unwrap();
        let result = db.query("INVALID SYNTAX HERE");
        assert!(result.is_err());
    }
}
