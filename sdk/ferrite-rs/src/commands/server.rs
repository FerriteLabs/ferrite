//! Server and administrative command builders (PING, INFO, DBSIZE, FLUSHDB, etc.).

use bytes::Bytes;

use crate::commands::{arg, exec};
use crate::connection::Connection;
use crate::error::Result;
use crate::types::{ToArg, Value};

/// PING [message] — test connectivity.
pub async fn ping(conn: &mut Connection, message: Option<&str>) -> Result<Value> {
    let mut args = vec![Bytes::from("PING")];
    if let Some(msg) = message {
        args.push(arg(msg));
    }
    exec(conn, args).await
}

/// ECHO message — echo a message.
pub async fn echo(conn: &mut Connection, message: impl ToArg) -> Result<Value> {
    exec(conn, vec![Bytes::from("ECHO"), arg(message)]).await
}

/// INFO [section] — get server information.
pub async fn info(conn: &mut Connection, section: Option<&str>) -> Result<Value> {
    let mut args = vec![Bytes::from("INFO")];
    if let Some(s) = section {
        args.push(arg(s));
    }
    exec(conn, args).await
}

/// DBSIZE — return the number of keys in the selected database.
pub async fn dbsize(conn: &mut Connection) -> Result<i64> {
    exec(conn, vec![Bytes::from("DBSIZE")])
        .await?
        .into_integer()
}

/// FLUSHDB [ASYNC] — remove all keys from the current database.
pub async fn flushdb(conn: &mut Connection, r#async: bool) -> Result<Value> {
    let mut args = vec![Bytes::from("FLUSHDB")];
    if r#async {
        args.push(Bytes::from("ASYNC"));
    }
    exec(conn, args).await
}

/// FLUSHALL [ASYNC] — remove all keys from all databases.
pub async fn flushall(conn: &mut Connection, r#async: bool) -> Result<Value> {
    let mut args = vec![Bytes::from("FLUSHALL")];
    if r#async {
        args.push(Bytes::from("ASYNC"));
    }
    exec(conn, args).await
}

/// SELECT index — change the selected database.
pub async fn select(conn: &mut Connection, database: u8) -> Result<Value> {
    exec(conn, vec![Bytes::from("SELECT"), arg(database as u32)]).await
}

/// CLIENT SETNAME name — set the connection name.
pub async fn client_setname(conn: &mut Connection, name: impl ToArg) -> Result<Value> {
    exec(
        conn,
        vec![Bytes::from("CLIENT"), Bytes::from("SETNAME"), arg(name)],
    )
    .await
}

/// CLIENT GETNAME — get the connection name.
pub async fn client_getname(conn: &mut Connection) -> Result<Value> {
    exec(
        conn,
        vec![Bytes::from("CLIENT"), Bytes::from("GETNAME")],
    )
    .await
}

/// CONFIG GET parameter — get a configuration parameter.
pub async fn config_get(conn: &mut Connection, parameter: impl ToArg) -> Result<Vec<Value>> {
    exec(
        conn,
        vec![Bytes::from("CONFIG"), Bytes::from("GET"), arg(parameter)],
    )
    .await?
    .into_array()
}

/// CONFIG SET parameter value — set a configuration parameter.
pub async fn config_set(
    conn: &mut Connection,
    parameter: impl ToArg,
    value: impl ToArg,
) -> Result<Value> {
    exec(
        conn,
        vec![
            Bytes::from("CONFIG"),
            Bytes::from("SET"),
            arg(parameter),
            arg(value),
        ],
    )
    .await
}

/// KEYS pattern — find all keys matching a glob-style pattern.
pub async fn keys(conn: &mut Connection, pattern: impl ToArg) -> Result<Vec<Value>> {
    exec(conn, vec![Bytes::from("KEYS"), arg(pattern)])
        .await?
        .into_array()
}

/// SCAN cursor [MATCH pattern] [COUNT count] — incrementally iterate the key space.
pub async fn scan(
    conn: &mut Connection,
    cursor: u64,
    pattern: Option<&str>,
    count: Option<u64>,
) -> Result<(u64, Vec<Value>)> {
    let mut args = vec![Bytes::from("SCAN"), arg(cursor)];
    if let Some(p) = pattern {
        args.push(Bytes::from("MATCH"));
        args.push(arg(p));
    }
    if let Some(c) = count {
        args.push(Bytes::from("COUNT"));
        args.push(arg(c));
    }
    let result = exec(conn, args).await?.into_array()?;
    if result.len() != 2 {
        return Err(crate::error::Error::Protocol(
            "SCAN response must have 2 elements".into(),
        ));
    }
    let mut iter = result.into_iter();
    let cursor_val = iter.next().expect("checked length");
    let keys_val = iter.next().expect("checked length");

    let next_cursor = match cursor_val {
        crate::types::Value::String(b) => {
            let s = std::str::from_utf8(&b)
                .map_err(|_| crate::error::Error::Protocol("invalid cursor UTF-8".into()))?;
            s.parse::<u64>()
                .map_err(|_| crate::error::Error::Protocol("invalid cursor integer".into()))?
        }
        _ => {
            return Err(crate::error::Error::Protocol(
                "unexpected cursor type".into(),
            ))
        }
    };

    let keys = keys_val.into_array()?;
    Ok((next_cursor, keys))
}
