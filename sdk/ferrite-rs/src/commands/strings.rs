//! String command builders (GET, SET, INCR, DECR, MGET, MSET, APPEND, etc.).

use bytes::Bytes;

use crate::commands::{arg, exec};
use crate::connection::Connection;
use crate::error::Result;
use crate::types::{ToArg, Value};

// ── SET with builder ────────────────────────────────────────────────────────

/// Builder for the SET command with optional EX, PX, NX, XX modifiers.
///
/// # Example
/// ```ignore
/// let val = SetCommand::new(&mut conn, "key", "value")
///     .ex(3600)
///     .nx()
///     .execute()
///     .await?;
/// ```
pub struct SetCommand<'a> {
    conn: &'a mut Connection,
    args: Vec<Bytes>,
}

impl<'a> SetCommand<'a> {
    /// Create a new SET command.
    pub fn new(conn: &'a mut Connection, key: impl ToArg, value: impl ToArg) -> Self {
        Self {
            conn,
            args: vec![Bytes::from("SET"), arg(key), arg(value)],
        }
    }

    /// Set expiry in seconds (EX).
    pub fn ex(mut self, seconds: u64) -> Self {
        self.args.push(Bytes::from("EX"));
        self.args.push(arg(seconds));
        self
    }

    /// Set expiry in milliseconds (PX).
    pub fn px(mut self, milliseconds: u64) -> Self {
        self.args.push(Bytes::from("PX"));
        self.args.push(arg(milliseconds));
        self
    }

    /// Set expiry as a Unix timestamp in seconds (EXAT).
    pub fn exat(mut self, timestamp: u64) -> Self {
        self.args.push(Bytes::from("EXAT"));
        self.args.push(arg(timestamp));
        self
    }

    /// Set expiry as a Unix timestamp in milliseconds (PXAT).
    pub fn pxat(mut self, timestamp: u64) -> Self {
        self.args.push(Bytes::from("PXAT"));
        self.args.push(arg(timestamp));
        self
    }

    /// Only set if the key does not already exist (NX).
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

    /// Return the old value stored at key (GET).
    pub fn get(mut self) -> Self {
        self.args.push(Bytes::from("GET"));
        self
    }

    /// Execute the SET command.
    pub async fn execute(self) -> Result<Value> {
        exec(self.conn, self.args).await
    }
}

// ── Standalone string commands ──────────────────────────────────────────────

/// GET key — retrieve the value of a key.
pub async fn get(conn: &mut Connection, key: impl ToArg) -> Result<Value> {
    exec(conn, vec![Bytes::from("GET"), arg(key)]).await
}

/// DEL key [key ...] — delete one or more keys.
pub async fn del(conn: &mut Connection, keys: &[impl ToArg]) -> Result<i64> {
    let mut args = vec![Bytes::from("DEL")];
    for k in keys {
        args.push(k.to_arg());
    }
    exec(conn, args).await?.into_integer()
}

/// EXISTS key [key ...] — check if keys exist.
pub async fn exists(conn: &mut Connection, keys: &[impl ToArg]) -> Result<i64> {
    let mut args = vec![Bytes::from("EXISTS")];
    for k in keys {
        args.push(k.to_arg());
    }
    exec(conn, args).await?.into_integer()
}

/// INCR key — increment integer value by 1.
pub async fn incr(conn: &mut Connection, key: impl ToArg) -> Result<i64> {
    exec(conn, vec![Bytes::from("INCR"), arg(key)])
        .await?
        .into_integer()
}

/// INCRBY key increment — increment integer value by given amount.
pub async fn incrby(conn: &mut Connection, key: impl ToArg, delta: i64) -> Result<i64> {
    exec(conn, vec![Bytes::from("INCRBY"), arg(key), arg(delta)])
        .await?
        .into_integer()
}

/// DECR key — decrement integer value by 1.
pub async fn decr(conn: &mut Connection, key: impl ToArg) -> Result<i64> {
    exec(conn, vec![Bytes::from("DECR"), arg(key)])
        .await?
        .into_integer()
}

/// DECRBY key decrement — decrement integer value by given amount.
pub async fn decrby(conn: &mut Connection, key: impl ToArg, delta: i64) -> Result<i64> {
    exec(conn, vec![Bytes::from("DECRBY"), arg(key), arg(delta)])
        .await?
        .into_integer()
}

/// MGET key [key ...] — get the values of multiple keys.
pub async fn mget(conn: &mut Connection, keys: &[impl ToArg]) -> Result<Vec<Value>> {
    let mut args = vec![Bytes::from("MGET")];
    for k in keys {
        args.push(k.to_arg());
    }
    exec(conn, args).await?.into_array()
}

/// MSET key value [key value ...] — set multiple key-value pairs.
pub async fn mset(conn: &mut Connection, pairs: &[(impl ToArg, impl ToArg)]) -> Result<Value> {
    let mut args = vec![Bytes::from("MSET")];
    for (k, v) in pairs {
        args.push(k.to_arg());
        args.push(v.to_arg());
    }
    exec(conn, args).await
}

/// APPEND key value — append a value to a key.
pub async fn append(conn: &mut Connection, key: impl ToArg, value: impl ToArg) -> Result<i64> {
    exec(conn, vec![Bytes::from("APPEND"), arg(key), arg(value)])
        .await?
        .into_integer()
}

/// STRLEN key — get the length of a string value.
pub async fn strlen(conn: &mut Connection, key: impl ToArg) -> Result<i64> {
    exec(conn, vec![Bytes::from("STRLEN"), arg(key)])
        .await?
        .into_integer()
}

/// GETRANGE key start end — get a substring of the string value.
pub async fn getrange(
    conn: &mut Connection,
    key: impl ToArg,
    start: i64,
    end: i64,
) -> Result<Value> {
    exec(
        conn,
        vec![Bytes::from("GETRANGE"), arg(key), arg(start), arg(end)],
    )
    .await
}

/// SETNX key value — set a key only if it does not exist.
pub async fn setnx(conn: &mut Connection, key: impl ToArg, value: impl ToArg) -> Result<bool> {
    let result = exec(conn, vec![Bytes::from("SETNX"), arg(key), arg(value)])
        .await?
        .into_integer()?;
    Ok(result == 1)
}

/// SETEX key seconds value — set a key with an expiry in seconds.
pub async fn setex(
    conn: &mut Connection,
    key: impl ToArg,
    seconds: u64,
    value: impl ToArg,
) -> Result<Value> {
    exec(
        conn,
        vec![Bytes::from("SETEX"), arg(key), arg(seconds), arg(value)],
    )
    .await
}

/// TTL key — get the remaining time to live of a key in seconds.
pub async fn ttl(conn: &mut Connection, key: impl ToArg) -> Result<i64> {
    exec(conn, vec![Bytes::from("TTL"), arg(key)])
        .await?
        .into_integer()
}

/// EXPIRE key seconds — set a timeout on a key.
pub async fn expire(conn: &mut Connection, key: impl ToArg, seconds: u64) -> Result<bool> {
    let result = exec(conn, vec![Bytes::from("EXPIRE"), arg(key), arg(seconds)])
        .await?
        .into_integer()?;
    Ok(result == 1)
}

/// PERSIST key — remove the existing timeout on a key.
pub async fn persist(conn: &mut Connection, key: impl ToArg) -> Result<bool> {
    let result = exec(conn, vec![Bytes::from("PERSIST"), arg(key)])
        .await?
        .into_integer()?;
    Ok(result == 1)
}

/// TYPE key — determine the type stored at key.
pub async fn key_type(conn: &mut Connection, key: impl ToArg) -> Result<Value> {
    exec(conn, vec![Bytes::from("TYPE"), arg(key)]).await
}
