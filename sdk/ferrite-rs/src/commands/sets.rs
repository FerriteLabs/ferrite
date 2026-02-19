//! Set command builders (SADD, SREM, SMEMBERS, SISMEMBER, SCARD, etc.).

use bytes::Bytes;

use crate::commands::{arg, exec};
use crate::connection::Connection;
use crate::error::Result;
use crate::types::{ToArg, Value};

/// SADD key member [member ...] — add members to a set.
pub async fn sadd(
    conn: &mut Connection,
    key: impl ToArg,
    members: &[impl ToArg],
) -> Result<i64> {
    let mut args = vec![Bytes::from("SADD"), arg(key)];
    for m in members {
        args.push(m.to_arg());
    }
    exec(conn, args).await?.into_integer()
}

/// SREM key member [member ...] — remove members from a set.
pub async fn srem(
    conn: &mut Connection,
    key: impl ToArg,
    members: &[impl ToArg],
) -> Result<i64> {
    let mut args = vec![Bytes::from("SREM"), arg(key)];
    for m in members {
        args.push(m.to_arg());
    }
    exec(conn, args).await?.into_integer()
}

/// SMEMBERS key — get all members of a set.
pub async fn smembers(conn: &mut Connection, key: impl ToArg) -> Result<Vec<Value>> {
    exec(conn, vec![Bytes::from("SMEMBERS"), arg(key)])
        .await?
        .into_array()
}

/// SISMEMBER key member — check if a value is a member of a set.
pub async fn sismember(
    conn: &mut Connection,
    key: impl ToArg,
    member: impl ToArg,
) -> Result<bool> {
    let result = exec(
        conn,
        vec![Bytes::from("SISMEMBER"), arg(key), arg(member)],
    )
    .await?
    .into_integer()?;
    Ok(result == 1)
}

/// SCARD key — get the number of members in a set.
pub async fn scard(conn: &mut Connection, key: impl ToArg) -> Result<i64> {
    exec(conn, vec![Bytes::from("SCARD"), arg(key)])
        .await?
        .into_integer()
}

/// SUNION key [key ...] — return the union of multiple sets.
pub async fn sunion(conn: &mut Connection, keys: &[impl ToArg]) -> Result<Vec<Value>> {
    let mut args = vec![Bytes::from("SUNION")];
    for k in keys {
        args.push(k.to_arg());
    }
    exec(conn, args).await?.into_array()
}

/// SINTER key [key ...] — return the intersection of multiple sets.
pub async fn sinter(conn: &mut Connection, keys: &[impl ToArg]) -> Result<Vec<Value>> {
    let mut args = vec![Bytes::from("SINTER")];
    for k in keys {
        args.push(k.to_arg());
    }
    exec(conn, args).await?.into_array()
}

/// SDIFF key [key ...] — return the difference of multiple sets.
pub async fn sdiff(conn: &mut Connection, keys: &[impl ToArg]) -> Result<Vec<Value>> {
    let mut args = vec![Bytes::from("SDIFF")];
    for k in keys {
        args.push(k.to_arg());
    }
    exec(conn, args).await?.into_array()
}

/// SPOP key — remove and return a random member.
pub async fn spop(conn: &mut Connection, key: impl ToArg) -> Result<Value> {
    exec(conn, vec![Bytes::from("SPOP"), arg(key)]).await
}

/// SRANDMEMBER key [count] — get one or more random members without removing them.
pub async fn srandmember(
    conn: &mut Connection,
    key: impl ToArg,
    count: Option<i64>,
) -> Result<Value> {
    let mut args = vec![Bytes::from("SRANDMEMBER"), arg(key)];
    if let Some(c) = count {
        args.push(arg(c));
    }
    exec(conn, args).await
}
