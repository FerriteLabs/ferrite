//! Command builder traits and modules.
//!
//! Each sub-module provides command builders for a specific command group
//! (strings, lists, hashes, sets, sorted sets, server). All builders follow
//! the pattern of accumulating options and executing via `.execute().await`.

pub mod hashes;
pub mod lists;
pub mod server;
pub mod sets;
pub mod sorted_sets;
pub mod strings;

use bytes::Bytes;

use crate::connection::Connection;
use crate::error::Result;
use crate::types::{ToArg, Value};

/// Helper to build a RESP command argument vector and execute it.
pub(crate) async fn exec(conn: &mut Connection, args: Vec<Bytes>) -> Result<Value> {
    conn.execute(&args).await
}

/// Helper to create a Bytes arg from anything that implements ToArg.
#[inline]
pub(crate) fn arg<T: ToArg>(val: T) -> Bytes {
    val.to_arg()
}
