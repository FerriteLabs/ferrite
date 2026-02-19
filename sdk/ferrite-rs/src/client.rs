//! Synchronous (blocking) Ferrite client.
//!
//! This module wraps the async client in a dedicated Tokio runtime,
//! providing a blocking API suitable for scripts, tests, and non-async code.

use crate::connection::ConnectionConfig;
use crate::error::Result;
use crate::pool::PoolConfig;
use crate::types::{ToArg, Value};

/// A blocking Ferrite client.
///
/// Internally spawns a single-threaded Tokio runtime and delegates to
/// [`AsyncClient`](crate::AsyncClient).
///
/// # Examples
///
/// ```ignore
/// use ferrite_rs::Client;
///
/// let client = Client::connect("127.0.0.1", 6379)?;
/// client.set("key", "value")?;
/// let val = client.get("key")?;
/// println!("{:?}", val);
/// ```
pub struct Client {
    inner: crate::async_client::AsyncClient,
    rt: tokio::runtime::Runtime,
}

impl Client {
    /// Connect to a Ferrite server with default settings.
    pub fn connect(host: impl Into<String>, port: u16) -> Result<Self> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(crate::error::Error::Io)?;

        let inner = rt.block_on(crate::async_client::AsyncClient::connect(host, port))?;
        Ok(Self { inner, rt })
    }

    /// Connect using a full [`ConnectionConfig`].
    pub fn connect_with(config: ConnectionConfig) -> Result<Self> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(crate::error::Error::Io)?;

        let inner = rt.block_on(crate::async_client::AsyncClient::connect_with(config))?;
        Ok(Self { inner, rt })
    }

    /// Connect using a full [`PoolConfig`].
    pub fn connect_pooled(config: PoolConfig) -> Result<Self> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(crate::error::Error::Io)?;

        let inner = rt.block_on(crate::async_client::AsyncClient::connect_pooled(config))?;
        Ok(Self { inner, rt })
    }

    // ── String commands ─────────────────────────────────────────────────

    /// SET key value.
    pub fn set(&self, key: impl ToArg, value: impl ToArg) -> Result<Value> {
        self.rt
            .block_on(async { self.inner.set(key, value).await?.execute().await })
    }

    /// GET key.
    pub fn get(&self, key: impl ToArg) -> Result<Value> {
        self.rt.block_on(self.inner.get(key))
    }

    /// DEL key [key ...].
    pub fn del(&self, keys: &[impl ToArg]) -> Result<i64> {
        self.rt.block_on(self.inner.del(keys))
    }

    /// EXISTS key [key ...].
    pub fn exists(&self, keys: &[impl ToArg]) -> Result<i64> {
        self.rt.block_on(self.inner.exists(keys))
    }

    /// INCR key.
    pub fn incr(&self, key: impl ToArg) -> Result<i64> {
        self.rt.block_on(self.inner.incr(key))
    }

    /// INCRBY key increment.
    pub fn incrby(&self, key: impl ToArg, delta: i64) -> Result<i64> {
        self.rt.block_on(self.inner.incrby(key, delta))
    }

    /// DECR key.
    pub fn decr(&self, key: impl ToArg) -> Result<i64> {
        self.rt.block_on(self.inner.decr(key))
    }

    /// MGET key [key ...].
    pub fn mget(&self, keys: &[impl ToArg]) -> Result<Vec<Value>> {
        self.rt.block_on(self.inner.mget(keys))
    }

    /// MSET key value [key value ...].
    pub fn mset(&self, pairs: &[(impl ToArg, impl ToArg)]) -> Result<Value> {
        self.rt.block_on(self.inner.mset(pairs))
    }

    /// TTL key.
    pub fn ttl(&self, key: impl ToArg) -> Result<i64> {
        self.rt.block_on(self.inner.ttl(key))
    }

    /// EXPIRE key seconds.
    pub fn expire(&self, key: impl ToArg, seconds: u64) -> Result<bool> {
        self.rt.block_on(self.inner.expire(key, seconds))
    }

    // ── List commands ───────────────────────────────────────────────────

    /// LPUSH key element [element ...].
    pub fn lpush(&self, key: impl ToArg, values: &[impl ToArg]) -> Result<i64> {
        self.rt.block_on(self.inner.lpush(key, values))
    }

    /// RPUSH key element [element ...].
    pub fn rpush(&self, key: impl ToArg, values: &[impl ToArg]) -> Result<i64> {
        self.rt.block_on(self.inner.rpush(key, values))
    }

    /// LPOP key.
    pub fn lpop(&self, key: impl ToArg) -> Result<Value> {
        self.rt.block_on(self.inner.lpop(key))
    }

    /// RPOP key.
    pub fn rpop(&self, key: impl ToArg) -> Result<Value> {
        self.rt.block_on(self.inner.rpop(key))
    }

    /// LRANGE key start stop.
    pub fn lrange(&self, key: impl ToArg, start: i64, stop: i64) -> Result<Vec<Value>> {
        self.rt.block_on(self.inner.lrange(key, start, stop))
    }

    /// LLEN key.
    pub fn llen(&self, key: impl ToArg) -> Result<i64> {
        self.rt.block_on(self.inner.llen(key))
    }

    // ── Hash commands ───────────────────────────────────────────────────

    /// HSET key field value [field value ...].
    pub fn hset(&self, key: impl ToArg, fields: &[(impl ToArg, impl ToArg)]) -> Result<i64> {
        self.rt.block_on(self.inner.hset(key, fields))
    }

    /// HGET key field.
    pub fn hget(&self, key: impl ToArg, field: impl ToArg) -> Result<Value> {
        self.rt.block_on(self.inner.hget(key, field))
    }

    /// HDEL key field [field ...].
    pub fn hdel(&self, key: impl ToArg, fields: &[impl ToArg]) -> Result<i64> {
        self.rt.block_on(self.inner.hdel(key, fields))
    }

    /// HGETALL key.
    pub fn hgetall(&self, key: impl ToArg) -> Result<Vec<Value>> {
        self.rt.block_on(self.inner.hgetall(key))
    }

    // ── Set commands ────────────────────────────────────────────────────

    /// SADD key member [member ...].
    pub fn sadd(&self, key: impl ToArg, members: &[impl ToArg]) -> Result<i64> {
        self.rt.block_on(self.inner.sadd(key, members))
    }

    /// SREM key member [member ...].
    pub fn srem(&self, key: impl ToArg, members: &[impl ToArg]) -> Result<i64> {
        self.rt.block_on(self.inner.srem(key, members))
    }

    /// SMEMBERS key.
    pub fn smembers(&self, key: impl ToArg) -> Result<Vec<Value>> {
        self.rt.block_on(self.inner.smembers(key))
    }

    /// SISMEMBER key member.
    pub fn sismember(&self, key: impl ToArg, member: impl ToArg) -> Result<bool> {
        self.rt.block_on(self.inner.sismember(key, member))
    }

    // ── Sorted set commands ─────────────────────────────────────────────

    /// ZADD key score member [score member ...].
    pub fn zadd(&self, key: impl ToArg, members: &[(f64, impl ToArg)]) -> Result<i64> {
        self.rt.block_on(self.inner.zadd(key, members))
    }

    /// ZRANGE key start stop [WITHSCORES].
    pub fn zrange(
        &self,
        key: impl ToArg,
        start: i64,
        stop: i64,
        withscores: bool,
    ) -> Result<Vec<Value>> {
        self.rt
            .block_on(self.inner.zrange(key, start, stop, withscores))
    }

    /// ZCARD key.
    pub fn zcard(&self, key: impl ToArg) -> Result<i64> {
        self.rt.block_on(self.inner.zcard(key))
    }

    // ── Server commands ─────────────────────────────────────────────────

    /// PING.
    pub fn ping(&self) -> Result<Value> {
        self.rt.block_on(self.inner.ping())
    }

    /// INFO [section].
    pub fn info(&self, section: Option<&str>) -> Result<Value> {
        self.rt.block_on(self.inner.info(section))
    }

    /// DBSIZE.
    pub fn dbsize(&self) -> Result<i64> {
        self.rt.block_on(self.inner.dbsize())
    }

    /// FLUSHDB [ASYNC].
    pub fn flushdb(&self, r#async: bool) -> Result<Value> {
        self.rt.block_on(self.inner.flushdb(r#async))
    }

    /// KEYS pattern.
    pub fn keys(&self, pattern: impl ToArg) -> Result<Vec<Value>> {
        self.rt.block_on(self.inner.keys(pattern))
    }

    // ── Raw ─────────────────────────────────────────────────────────────

    /// Execute an arbitrary command.
    pub fn execute(&self, args: &[impl ToArg]) -> Result<Value> {
        self.rt.block_on(self.inner.execute(args))
    }
}
