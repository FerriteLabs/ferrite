//! Sorted set command builders (ZADD, ZREM, ZRANGE, ZSCORE, ZCARD, etc.).

use bytes::Bytes;

use crate::commands::{arg, exec};
use crate::connection::Connection;
use crate::error::Result;
use crate::types::{ToArg, Value};

/// ZADD key score member [score member ...] — add members with scores.
pub async fn zadd(
    conn: &mut Connection,
    key: impl ToArg,
    members: &[(f64, impl ToArg)],
) -> Result<i64> {
    let mut args = vec![Bytes::from("ZADD"), arg(key)];
    for (score, member) in members {
        args.push(arg(*score));
        args.push(member.to_arg());
    }
    exec(conn, args).await?.into_integer()
}

/// ZREM key member [member ...] — remove members from a sorted set.
pub async fn zrem(
    conn: &mut Connection,
    key: impl ToArg,
    members: &[impl ToArg],
) -> Result<i64> {
    let mut args = vec![Bytes::from("ZREM"), arg(key)];
    for m in members {
        args.push(m.to_arg());
    }
    exec(conn, args).await?.into_integer()
}

/// ZRANGE key min max [WITHSCORES] — return a range of members by index.
pub async fn zrange(
    conn: &mut Connection,
    key: impl ToArg,
    start: i64,
    stop: i64,
    withscores: bool,
) -> Result<Vec<Value>> {
    let mut args = vec![Bytes::from("ZRANGE"), arg(key), arg(start), arg(stop)];
    if withscores {
        args.push(Bytes::from("WITHSCORES"));
    }
    exec(conn, args).await?.into_array()
}

/// ZREVRANGE key start stop [WITHSCORES] — return a range by index, high to low.
pub async fn zrevrange(
    conn: &mut Connection,
    key: impl ToArg,
    start: i64,
    stop: i64,
    withscores: bool,
) -> Result<Vec<Value>> {
    let mut args = vec![Bytes::from("ZREVRANGE"), arg(key), arg(start), arg(stop)];
    if withscores {
        args.push(Bytes::from("WITHSCORES"));
    }
    exec(conn, args).await?.into_array()
}

/// ZSCORE key member — get the score of a member.
pub async fn zscore(conn: &mut Connection, key: impl ToArg, member: impl ToArg) -> Result<Value> {
    exec(conn, vec![Bytes::from("ZSCORE"), arg(key), arg(member)]).await
}

/// ZCARD key — get the number of members in a sorted set.
pub async fn zcard(conn: &mut Connection, key: impl ToArg) -> Result<i64> {
    exec(conn, vec![Bytes::from("ZCARD"), arg(key)])
        .await?
        .into_integer()
}

/// ZCOUNT key min max — count members with scores in a range.
pub async fn zcount(
    conn: &mut Connection,
    key: impl ToArg,
    min: impl ToArg,
    max: impl ToArg,
) -> Result<i64> {
    exec(
        conn,
        vec![Bytes::from("ZCOUNT"), arg(key), arg(min), arg(max)],
    )
    .await?
    .into_integer()
}

/// ZINCRBY key increment member — increment the score of a member.
pub async fn zincrby(
    conn: &mut Connection,
    key: impl ToArg,
    increment: f64,
    member: impl ToArg,
) -> Result<Value> {
    exec(
        conn,
        vec![
            Bytes::from("ZINCRBY"),
            arg(key),
            arg(increment),
            arg(member),
        ],
    )
    .await
}

/// ZRANK key member — determine the index of a member.
pub async fn zrank(conn: &mut Connection, key: impl ToArg, member: impl ToArg) -> Result<Value> {
    exec(conn, vec![Bytes::from("ZRANK"), arg(key), arg(member)]).await
}

/// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count] — return members by score range.
pub async fn zrangebyscore(
    conn: &mut Connection,
    key: impl ToArg,
    min: impl ToArg,
    max: impl ToArg,
    withscores: bool,
    limit: Option<(i64, i64)>,
) -> Result<Vec<Value>> {
    let mut args = vec![Bytes::from("ZRANGEBYSCORE"), arg(key), arg(min), arg(max)];
    if withscores {
        args.push(Bytes::from("WITHSCORES"));
    }
    if let Some((offset, count)) = limit {
        args.push(Bytes::from("LIMIT"));
        args.push(arg(offset));
        args.push(arg(count));
    }
    exec(conn, args).await?.into_array()
}
