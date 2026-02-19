//! Async Ferrite client.
//!
//! This module provides the primary async client for interacting with a
//! Ferrite (or Redis-compatible) server over TCP.

use bytes::Bytes;

use crate::connection::ConnectionConfig;
use crate::error::Result;
use crate::pool::{Pool, PoolConfig};
use crate::types::{ToArg, Value};

/// An asynchronous Ferrite client.
///
/// Wraps a connection pool and exposes ergonomic async methods for all
/// supported command groups.
///
/// # Examples
///
/// ```ignore
/// use ferrite_rs::AsyncClient;
///
/// let client = AsyncClient::connect("127.0.0.1", 6379).await?;
/// client.set("key", "value").execute().await?;
/// let val = client.get("key").await?;
/// ```
pub struct AsyncClient {
    pool: Pool,
}

impl AsyncClient {
    /// Connect to a Ferrite server with default settings.
    pub async fn connect(host: impl Into<String>, port: u16) -> Result<Self> {
        let config = PoolConfig {
            connection: ConnectionConfig::from_addr(host, port),
            ..Default::default()
        };
        Ok(Self {
            pool: Pool::new(config),
        })
    }

    /// Connect using a full [`ConnectionConfig`].
    pub async fn connect_with(config: ConnectionConfig) -> Result<Self> {
        let pool_config = PoolConfig {
            connection: config,
            ..Default::default()
        };
        Ok(Self {
            pool: Pool::new(pool_config),
        })
    }

    /// Connect using a full [`PoolConfig`] with custom pool settings.
    pub async fn connect_pooled(config: PoolConfig) -> Result<Self> {
        Ok(Self {
            pool: Pool::new(config),
        })
    }

    // ── String commands ─────────────────────────────────────────────────

    /// SET key value — returns a builder for optional modifiers (EX, NX, etc.).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Simple set
    /// client.set("key", "value").execute().await?;
    ///
    /// // Set with expiry and NX
    /// client.set("key", "value").ex(3600).nx().execute().await?;
    /// ```
    pub async fn set<'a>(
        &'a self,
        key: impl ToArg,
        value: impl ToArg,
    ) -> Result<SetCommandHandle<'a>> {
        let pooled = self.pool.get().await?;
        Ok(SetCommandHandle {
            conn: pooled,
            key: key.to_arg(),
            value: value.to_arg(),
            args: Vec::new(),
        })
    }

    /// GET key — retrieve the string value of a key.
    pub async fn get(&self, key: impl ToArg) -> Result<Value> {
        let mut conn = self.pool.get().await?;
        crate::commands::strings::get(conn.conn(), key).await
    }

    /// DEL key [key ...] — delete one or more keys.
    pub async fn del(&self, keys: &[impl ToArg]) -> Result<i64> {
        let mut conn = self.pool.get().await?;
        crate::commands::strings::del(conn.conn(), keys).await
    }

    /// EXISTS key [key ...] — check how many of the given keys exist.
    pub async fn exists(&self, keys: &[impl ToArg]) -> Result<i64> {
        let mut conn = self.pool.get().await?;
        crate::commands::strings::exists(conn.conn(), keys).await
    }

    /// INCR key — increment integer value by 1.
    pub async fn incr(&self, key: impl ToArg) -> Result<i64> {
        let mut conn = self.pool.get().await?;
        crate::commands::strings::incr(conn.conn(), key).await
    }

    /// INCRBY key increment — increment integer value.
    pub async fn incrby(&self, key: impl ToArg, delta: i64) -> Result<i64> {
        let mut conn = self.pool.get().await?;
        crate::commands::strings::incrby(conn.conn(), key, delta).await
    }

    /// DECR key — decrement integer value by 1.
    pub async fn decr(&self, key: impl ToArg) -> Result<i64> {
        let mut conn = self.pool.get().await?;
        crate::commands::strings::decr(conn.conn(), key).await
    }

    /// MGET key [key ...] — get values of multiple keys.
    pub async fn mget(&self, keys: &[impl ToArg]) -> Result<Vec<Value>> {
        let mut conn = self.pool.get().await?;
        crate::commands::strings::mget(conn.conn(), keys).await
    }

    /// MSET key value [key value ...] — set multiple key-value pairs.
    pub async fn mset(&self, pairs: &[(impl ToArg, impl ToArg)]) -> Result<Value> {
        let mut conn = self.pool.get().await?;
        crate::commands::strings::mset(conn.conn(), pairs).await
    }

    /// APPEND key value — append a value to a key.
    pub async fn append(&self, key: impl ToArg, value: impl ToArg) -> Result<i64> {
        let mut conn = self.pool.get().await?;
        crate::commands::strings::append(conn.conn(), key, value).await
    }

    /// TTL key — get the time-to-live of a key in seconds.
    pub async fn ttl(&self, key: impl ToArg) -> Result<i64> {
        let mut conn = self.pool.get().await?;
        crate::commands::strings::ttl(conn.conn(), key).await
    }

    /// EXPIRE key seconds — set a timeout on a key.
    pub async fn expire(&self, key: impl ToArg, seconds: u64) -> Result<bool> {
        let mut conn = self.pool.get().await?;
        crate::commands::strings::expire(conn.conn(), key, seconds).await
    }

    /// PERSIST key — remove the timeout on a key.
    pub async fn persist(&self, key: impl ToArg) -> Result<bool> {
        let mut conn = self.pool.get().await?;
        crate::commands::strings::persist(conn.conn(), key).await
    }

    // ── List commands ───────────────────────────────────────────────────

    /// LPUSH key element [element ...] — prepend elements to a list.
    pub async fn lpush(&self, key: impl ToArg, values: &[impl ToArg]) -> Result<i64> {
        let mut conn = self.pool.get().await?;
        crate::commands::lists::lpush(conn.conn(), key, values).await
    }

    /// RPUSH key element [element ...] — append elements to a list.
    pub async fn rpush(&self, key: impl ToArg, values: &[impl ToArg]) -> Result<i64> {
        let mut conn = self.pool.get().await?;
        crate::commands::lists::rpush(conn.conn(), key, values).await
    }

    /// LPOP key — remove and get the first element.
    pub async fn lpop(&self, key: impl ToArg) -> Result<Value> {
        let mut conn = self.pool.get().await?;
        crate::commands::lists::lpop(conn.conn(), key).await
    }

    /// RPOP key — remove and get the last element.
    pub async fn rpop(&self, key: impl ToArg) -> Result<Value> {
        let mut conn = self.pool.get().await?;
        crate::commands::lists::rpop(conn.conn(), key).await
    }

    /// LRANGE key start stop — get a range of elements from a list.
    pub async fn lrange(
        &self,
        key: impl ToArg,
        start: i64,
        stop: i64,
    ) -> Result<Vec<Value>> {
        let mut conn = self.pool.get().await?;
        crate::commands::lists::lrange(conn.conn(), key, start, stop).await
    }

    /// LLEN key — get the length of a list.
    pub async fn llen(&self, key: impl ToArg) -> Result<i64> {
        let mut conn = self.pool.get().await?;
        crate::commands::lists::llen(conn.conn(), key).await
    }

    // ── Hash commands ───────────────────────────────────────────────────

    /// HSET key field value [field value ...] — set fields in a hash.
    pub async fn hset(
        &self,
        key: impl ToArg,
        fields: &[(impl ToArg, impl ToArg)],
    ) -> Result<i64> {
        let mut conn = self.pool.get().await?;
        crate::commands::hashes::hset(conn.conn(), key, fields).await
    }

    /// HGET key field — get the value of a hash field.
    pub async fn hget(&self, key: impl ToArg, field: impl ToArg) -> Result<Value> {
        let mut conn = self.pool.get().await?;
        crate::commands::hashes::hget(conn.conn(), key, field).await
    }

    /// HDEL key field [field ...] — delete hash fields.
    pub async fn hdel(&self, key: impl ToArg, fields: &[impl ToArg]) -> Result<i64> {
        let mut conn = self.pool.get().await?;
        crate::commands::hashes::hdel(conn.conn(), key, fields).await
    }

    /// HGETALL key — get all fields and values in a hash.
    pub async fn hgetall(&self, key: impl ToArg) -> Result<Vec<Value>> {
        let mut conn = self.pool.get().await?;
        crate::commands::hashes::hgetall(conn.conn(), key).await
    }

    /// HEXISTS key field — check if a field exists in a hash.
    pub async fn hexists(&self, key: impl ToArg, field: impl ToArg) -> Result<bool> {
        let mut conn = self.pool.get().await?;
        crate::commands::hashes::hexists(conn.conn(), key, field).await
    }

    /// HLEN key — get the number of fields in a hash.
    pub async fn hlen(&self, key: impl ToArg) -> Result<i64> {
        let mut conn = self.pool.get().await?;
        crate::commands::hashes::hlen(conn.conn(), key).await
    }

    // ── Set commands ────────────────────────────────────────────────────

    /// SADD key member [member ...] — add members to a set.
    pub async fn sadd(&self, key: impl ToArg, members: &[impl ToArg]) -> Result<i64> {
        let mut conn = self.pool.get().await?;
        crate::commands::sets::sadd(conn.conn(), key, members).await
    }

    /// SREM key member [member ...] — remove members from a set.
    pub async fn srem(&self, key: impl ToArg, members: &[impl ToArg]) -> Result<i64> {
        let mut conn = self.pool.get().await?;
        crate::commands::sets::srem(conn.conn(), key, members).await
    }

    /// SMEMBERS key — get all members of a set.
    pub async fn smembers(&self, key: impl ToArg) -> Result<Vec<Value>> {
        let mut conn = self.pool.get().await?;
        crate::commands::sets::smembers(conn.conn(), key).await
    }

    /// SISMEMBER key member — check if a value is in a set.
    pub async fn sismember(&self, key: impl ToArg, member: impl ToArg) -> Result<bool> {
        let mut conn = self.pool.get().await?;
        crate::commands::sets::sismember(conn.conn(), key, member).await
    }

    /// SCARD key — get the number of members in a set.
    pub async fn scard(&self, key: impl ToArg) -> Result<i64> {
        let mut conn = self.pool.get().await?;
        crate::commands::sets::scard(conn.conn(), key).await
    }

    // ── Sorted set commands ─────────────────────────────────────────────

    /// ZADD key score member [score member ...] — add members with scores.
    pub async fn zadd(
        &self,
        key: impl ToArg,
        members: &[(f64, impl ToArg)],
    ) -> Result<i64> {
        let mut conn = self.pool.get().await?;
        crate::commands::sorted_sets::zadd(conn.conn(), key, members).await
    }

    /// ZREM key member [member ...] — remove members from a sorted set.
    pub async fn zrem(&self, key: impl ToArg, members: &[impl ToArg]) -> Result<i64> {
        let mut conn = self.pool.get().await?;
        crate::commands::sorted_sets::zrem(conn.conn(), key, members).await
    }

    /// ZRANGE key start stop [WITHSCORES] — return a range by index.
    pub async fn zrange(
        &self,
        key: impl ToArg,
        start: i64,
        stop: i64,
        withscores: bool,
    ) -> Result<Vec<Value>> {
        let mut conn = self.pool.get().await?;
        crate::commands::sorted_sets::zrange(conn.conn(), key, start, stop, withscores).await
    }

    /// ZSCORE key member — get the score of a member.
    pub async fn zscore(&self, key: impl ToArg, member: impl ToArg) -> Result<Value> {
        let mut conn = self.pool.get().await?;
        crate::commands::sorted_sets::zscore(conn.conn(), key, member).await
    }

    /// ZCARD key — get the number of members in a sorted set.
    pub async fn zcard(&self, key: impl ToArg) -> Result<i64> {
        let mut conn = self.pool.get().await?;
        crate::commands::sorted_sets::zcard(conn.conn(), key).await
    }

    // ── Server commands ─────────────────────────────────────────────────

    /// PING — test server connectivity.
    pub async fn ping(&self) -> Result<Value> {
        let mut conn = self.pool.get().await?;
        crate::commands::server::ping(conn.conn(), None).await
    }

    /// INFO [section] — get server information.
    pub async fn info(&self, section: Option<&str>) -> Result<Value> {
        let mut conn = self.pool.get().await?;
        crate::commands::server::info(conn.conn(), section).await
    }

    /// DBSIZE — return the number of keys in the current database.
    pub async fn dbsize(&self) -> Result<i64> {
        let mut conn = self.pool.get().await?;
        crate::commands::server::dbsize(conn.conn()).await
    }

    /// FLUSHDB [ASYNC] — remove all keys from the current database.
    pub async fn flushdb(&self, r#async: bool) -> Result<Value> {
        let mut conn = self.pool.get().await?;
        crate::commands::server::flushdb(conn.conn(), r#async).await
    }

    /// KEYS pattern — find all keys matching a glob pattern.
    pub async fn keys(&self, pattern: impl ToArg) -> Result<Vec<Value>> {
        let mut conn = self.pool.get().await?;
        crate::commands::server::keys(conn.conn(), pattern).await
    }

    /// SCAN cursor [MATCH pattern] [COUNT count] — incrementally iterate keys.
    pub async fn scan(
        &self,
        cursor: u64,
        pattern: Option<&str>,
        count: Option<u64>,
    ) -> Result<(u64, Vec<Value>)> {
        let mut conn = self.pool.get().await?;
        crate::commands::server::scan(conn.conn(), cursor, pattern, count).await
    }

    // ── Raw command execution ───────────────────────────────────────────

    /// Execute an arbitrary command with raw arguments.
    ///
    /// Useful for commands not yet covered by the typed API.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let val = client.execute(&["CLIENT", "ID"]).await?;
    /// ```
    pub async fn execute(&self, args: &[impl ToArg]) -> Result<Value> {
        let raw: Vec<Bytes> = args.iter().map(|a| a.to_arg()).collect();
        let mut conn = self.pool.get().await?;
        conn.conn().execute(&raw).await
    }
}

// ── SetCommandHandle (builder returned by AsyncClient::set) ─────────────────

/// Builder for SET with options, returned by [`AsyncClient::set`].
pub struct SetCommandHandle<'a> {
    conn: crate::pool::PooledConnection<'a>,
    key: Bytes,
    value: Bytes,
    args: Vec<Bytes>,
}

impl<'a> SetCommandHandle<'a> {
    /// Set expiry in seconds (EX).
    pub fn ex(mut self, seconds: u64) -> Self {
        self.args.push(Bytes::from("EX"));
        self.args.push(seconds.to_arg());
        self
    }

    /// Set expiry in milliseconds (PX).
    pub fn px(mut self, milliseconds: u64) -> Self {
        self.args.push(Bytes::from("PX"));
        self.args.push(milliseconds.to_arg());
        self
    }

    /// Only set if the key does not exist (NX).
    pub fn nx(mut self) -> Self {
        self.args.push(Bytes::from("NX"));
        self
    }

    /// Only set if the key already exists (XX).
    pub fn xx(mut self) -> Self {
        self.args.push(Bytes::from("XX"));
        self
    }

    /// Keep the existing TTL (KEEPTTL).
    pub fn keepttl(mut self) -> Self {
        self.args.push(Bytes::from("KEEPTTL"));
        self
    }

    /// Execute the SET command.
    pub async fn execute(mut self) -> Result<Value> {
        let mut cmd = vec![Bytes::from("SET"), self.key, self.value];
        cmd.append(&mut self.args);
        self.conn.conn().execute(&cmd).await
    }
}
