//! Hash command builders (HSET, HGET, HDEL, HGETALL, HMSET, HMGET, etc.).

use bytes::Bytes;

use crate::commands::{arg, exec};
use crate::connection::Connection;
use crate::error::Result;
use crate::types::{ToArg, Value};

/// HSET key field value [field value ...] — set fields in a hash.
pub async fn hset(
    conn: &mut Connection,
    key: impl ToArg,
    fields: &[(impl ToArg, impl ToArg)],
) -> Result<i64> {
    let mut args = vec![Bytes::from("HSET"), arg(key)];
    for (f, v) in fields {
        args.push(f.to_arg());
        args.push(v.to_arg());
    }
    exec(conn, args).await?.into_integer()
}

/// HGET key field — get a single field from a hash.
pub async fn hget(conn: &mut Connection, key: impl ToArg, field: impl ToArg) -> Result<Value> {
    exec(conn, vec![Bytes::from("HGET"), arg(key), arg(field)]).await
}

/// HDEL key field [field ...] — delete fields from a hash.
pub async fn hdel(
    conn: &mut Connection,
    key: impl ToArg,
    fields: &[impl ToArg],
) -> Result<i64> {
    let mut args = vec![Bytes::from("HDEL"), arg(key)];
    for f in fields {
        args.push(f.to_arg());
    }
    exec(conn, args).await?.into_integer()
}

/// HGETALL key — get all fields and values in a hash.
pub async fn hgetall(conn: &mut Connection, key: impl ToArg) -> Result<Vec<Value>> {
    exec(conn, vec![Bytes::from("HGETALL"), arg(key)])
        .await?
        .into_array()
}

/// HMGET key field [field ...] — get values of multiple fields.
pub async fn hmget(
    conn: &mut Connection,
    key: impl ToArg,
    fields: &[impl ToArg],
) -> Result<Vec<Value>> {
    let mut args = vec![Bytes::from("HMGET"), arg(key)];
    for f in fields {
        args.push(f.to_arg());
    }
    exec(conn, args).await?.into_array()
}

/// HEXISTS key field — check if a field exists in a hash.
pub async fn hexists(conn: &mut Connection, key: impl ToArg, field: impl ToArg) -> Result<bool> {
    let result = exec(conn, vec![Bytes::from("HEXISTS"), arg(key), arg(field)])
        .await?
        .into_integer()?;
    Ok(result == 1)
}

/// HLEN key — get the number of fields in a hash.
pub async fn hlen(conn: &mut Connection, key: impl ToArg) -> Result<i64> {
    exec(conn, vec![Bytes::from("HLEN"), arg(key)])
        .await?
        .into_integer()
}

/// HKEYS key — get all field names in a hash.
pub async fn hkeys(conn: &mut Connection, key: impl ToArg) -> Result<Vec<Value>> {
    exec(conn, vec![Bytes::from("HKEYS"), arg(key)])
        .await?
        .into_array()
}

/// HVALS key — get all values in a hash.
pub async fn hvals(conn: &mut Connection, key: impl ToArg) -> Result<Vec<Value>> {
    exec(conn, vec![Bytes::from("HVALS"), arg(key)])
        .await?
        .into_array()
}

/// HINCRBY key field increment — increment the integer value of a hash field.
pub async fn hincrby(
    conn: &mut Connection,
    key: impl ToArg,
    field: impl ToArg,
    delta: i64,
) -> Result<i64> {
    exec(
        conn,
        vec![Bytes::from("HINCRBY"), arg(key), arg(field), arg(delta)],
    )
    .await?
    .into_integer()
}

/// HSETNX key field value — set a field only if it does not exist.
pub async fn hsetnx(
    conn: &mut Connection,
    key: impl ToArg,
    field: impl ToArg,
    value: impl ToArg,
) -> Result<bool> {
    let result = exec(
        conn,
        vec![Bytes::from("HSETNX"), arg(key), arg(field), arg(value)],
    )
    .await?
    .into_integer()?;
    Ok(result == 1)
}
