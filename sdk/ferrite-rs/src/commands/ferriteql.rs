//! FerriteQL query command builders.
//!
//! # Example
//!
//! ```ignore
//! use ferrite_rs::AsyncClient;
//!
//! let client = AsyncClient::connect("127.0.0.1", 6379).await?;
//!
//! // Execute a FerriteQL query
//! let results = client.query("FROM users:* WHERE $.active = true SELECT $.name, $.email")
//!     .limit(100)
//!     .execute().await?;
//!
//! // Create a materialized view
//! client.create_view("active_users")
//!     .query("FROM users:* WHERE $.active = true")
//!     .refresh_interval(60)
//!     .execute().await?;
//! ```

use bytes::Bytes;

use crate::commands::{arg, exec};
use crate::connection::Connection;
use crate::error::Result;
use crate::types::{ToArg, Value};

// ---------------------------------------------------------------------------
// QUERY (FerriteQL)
// ---------------------------------------------------------------------------

/// Builder for the QUERY command (FerriteQL execution).
pub struct QueryCommand<'a> {
    conn: &'a mut Connection,
    args: Vec<Bytes>,
}

impl<'a> QueryCommand<'a> {
    /// Create a new QUERY command with a FerriteQL expression.
    pub fn new(conn: &'a mut Connection, fql: impl ToArg) -> Self {
        Self {
            conn,
            args: vec![Bytes::from("QUERY"), arg(fql)],
        }
    }

    /// Limit the number of results.
    pub fn limit(mut self, n: u64) -> Self {
        self.args.push(Bytes::from("LIMIT"));
        self.args.push(arg(n));
        self
    }

    /// Set the output format (json, table, raw).
    pub fn format(mut self, fmt: &str) -> Self {
        self.args.push(Bytes::from("FORMAT"));
        self.args.push(Bytes::from(fmt.to_string()));
        self
    }

    /// Enable EXPLAIN mode (returns query plan instead of results).
    pub fn explain(mut self) -> Self {
        self.args.push(Bytes::from("EXPLAIN"));
        self
    }

    /// Execute the command.
    pub async fn execute(self) -> Result<Value> {
        exec(self.conn, self.args).await
    }
}

// ---------------------------------------------------------------------------
// VIEW operations
// ---------------------------------------------------------------------------

/// Builder for the VIEW.CREATE command.
pub struct CreateViewCommand<'a> {
    conn: &'a mut Connection,
    args: Vec<Bytes>,
}

impl<'a> CreateViewCommand<'a> {
    /// Create a new VIEW.CREATE command.
    pub fn new(conn: &'a mut Connection, name: impl ToArg) -> Self {
        Self {
            conn,
            args: vec![Bytes::from("VIEW.CREATE"), arg(name)],
        }
    }

    /// Set the FerriteQL query that defines the view.
    pub fn query(mut self, fql: &str) -> Self {
        self.args.push(Bytes::from("AS"));
        self.args.push(Bytes::from(fql.to_string()));
        self
    }

    /// Set the refresh interval in seconds (0 = eager/real-time).
    pub fn refresh_interval(mut self, seconds: u64) -> Self {
        self.args.push(Bytes::from("REFRESH"));
        self.args.push(arg(seconds));
        self
    }

    /// Execute the command.
    pub async fn execute(self) -> Result<Value> {
        exec(self.conn, self.args).await
    }
}

/// Builder for the VIEW.QUERY command.
pub struct QueryViewCommand<'a> {
    conn: &'a mut Connection,
    args: Vec<Bytes>,
}

impl<'a> QueryViewCommand<'a> {
    /// Query a materialized view.
    pub fn new(conn: &'a mut Connection, name: impl ToArg) -> Self {
        Self {
            conn,
            args: vec![Bytes::from("VIEW.QUERY"), arg(name)],
        }
    }

    /// Execute the command.
    pub async fn execute(self) -> Result<Value> {
        exec(self.conn, self.args).await
    }
}

/// Builder for the VIEW.DROP command.
pub struct DropViewCommand<'a> {
    conn: &'a mut Connection,
    args: Vec<Bytes>,
}

impl<'a> DropViewCommand<'a> {
    /// Drop a materialized view.
    pub fn new(conn: &'a mut Connection, name: impl ToArg) -> Self {
        Self {
            conn,
            args: vec![Bytes::from("VIEW.DROP"), arg(name)],
        }
    }

    /// Execute the command.
    pub async fn execute(self) -> Result<Value> {
        exec(self.conn, self.args).await
    }
}
