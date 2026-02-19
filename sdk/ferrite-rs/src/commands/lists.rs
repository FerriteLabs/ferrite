//! List command builders (LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, etc.).

use bytes::Bytes;

use crate::commands::{arg, exec};
use crate::connection::Connection;
use crate::error::Result;
use crate::types::{ToArg, Value};

/// LPUSH key element [element ...] — prepend elements to a list.
pub async fn lpush(conn: &mut Connection, key: impl ToArg, values: &[impl ToArg]) -> Result<i64> {
    let mut args = vec![Bytes::from("LPUSH"), arg(key)];
    for v in values {
        args.push(v.to_arg());
    }
    exec(conn, args).await?.into_integer()
}

/// RPUSH key element [element ...] — append elements to a list.
pub async fn rpush(conn: &mut Connection, key: impl ToArg, values: &[impl ToArg]) -> Result<i64> {
    let mut args = vec![Bytes::from("RPUSH"), arg(key)];
    for v in values {
        args.push(v.to_arg());
    }
    exec(conn, args).await?.into_integer()
}

/// LPOP key — remove and get the first element.
pub async fn lpop(conn: &mut Connection, key: impl ToArg) -> Result<Value> {
    exec(conn, vec![Bytes::from("LPOP"), arg(key)]).await
}

/// RPOP key — remove and get the last element.
pub async fn rpop(conn: &mut Connection, key: impl ToArg) -> Result<Value> {
    exec(conn, vec![Bytes::from("RPOP"), arg(key)]).await
}

/// LRANGE key start stop — get a range of elements.
pub async fn lrange(
    conn: &mut Connection,
    key: impl ToArg,
    start: i64,
    stop: i64,
) -> Result<Vec<Value>> {
    exec(
        conn,
        vec![Bytes::from("LRANGE"), arg(key), arg(start), arg(stop)],
    )
    .await?
    .into_array()
}

/// LLEN key — get the length of a list.
pub async fn llen(conn: &mut Connection, key: impl ToArg) -> Result<i64> {
    exec(conn, vec![Bytes::from("LLEN"), arg(key)])
        .await?
        .into_integer()
}

/// LINDEX key index — get an element by its index.
pub async fn lindex(conn: &mut Connection, key: impl ToArg, index: i64) -> Result<Value> {
    exec(conn, vec![Bytes::from("LINDEX"), arg(key), arg(index)]).await
}

/// LSET key index element — set the value of an element by its index.
pub async fn lset(
    conn: &mut Connection,
    key: impl ToArg,
    index: i64,
    value: impl ToArg,
) -> Result<Value> {
    exec(
        conn,
        vec![Bytes::from("LSET"), arg(key), arg(index), arg(value)],
    )
    .await
}

/// LREM key count element — remove elements from a list.
pub async fn lrem(
    conn: &mut Connection,
    key: impl ToArg,
    count: i64,
    element: impl ToArg,
) -> Result<i64> {
    exec(
        conn,
        vec![Bytes::from("LREM"), arg(key), arg(count), arg(element)],
    )
    .await?
    .into_integer()
}

/// LTRIM key start stop — trim a list to the specified range.
pub async fn ltrim(
    conn: &mut Connection,
    key: impl ToArg,
    start: i64,
    stop: i64,
) -> Result<Value> {
    exec(
        conn,
        vec![Bytes::from("LTRIM"), arg(key), arg(start), arg(stop)],
    )
    .await
}
