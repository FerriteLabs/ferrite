//! Semantic caching command builders for Ferrite.
//!
//! # Example
//!
//! ```ignore
//! use ferrite_rs::AsyncClient;
//!
//! let client = AsyncClient::connect("127.0.0.1", 6379).await?;
//!
//! // Cache a response by semantic meaning
//! client.semantic_set("What is the capital of France?", "Paris is the capital of France.")
//!     .ttl(3600)
//!     .execute().await?;
//!
//! // Retrieve semantically similar cached content
//! let cached = client.semantic_get("France's capital city?")
//!     .threshold(0.85)
//!     .execute().await?;
//! ```

use bytes::Bytes;

use crate::commands::{arg, exec};
use crate::connection::Connection;
use crate::error::Result;
use crate::types::{ToArg, Value};

// ---------------------------------------------------------------------------
// SEMANTIC.SET
// ---------------------------------------------------------------------------

/// Builder for the SEMANTIC.SET command.
pub struct SemanticSetCommand<'a> {
    conn: &'a mut Connection,
    args: Vec<Bytes>,
}

impl<'a> SemanticSetCommand<'a> {
    /// Create a new SEMANTIC.SET command.
    pub fn new(conn: &'a mut Connection, query: impl ToArg, response: impl ToArg) -> Self {
        Self {
            conn,
            args: vec![Bytes::from("SEMANTIC.SET"), arg(query), arg(response)],
        }
    }

    /// Set TTL in seconds.
    pub fn ttl(mut self, seconds: u64) -> Self {
        self.args.push(Bytes::from("TTL"));
        self.args.push(arg(seconds));
        self
    }

    /// Tag this cache entry with metadata.
    pub fn tag(mut self, tag: &str) -> Self {
        self.args.push(Bytes::from("TAG"));
        self.args.push(Bytes::from(tag.to_string()));
        self
    }

    /// Execute the command.
    pub async fn execute(self) -> Result<Value> {
        exec(self.conn, self.args).await
    }
}

// ---------------------------------------------------------------------------
// SEMANTIC.GET
// ---------------------------------------------------------------------------

/// Builder for the SEMANTIC.GET command.
pub struct SemanticGetCommand<'a> {
    conn: &'a mut Connection,
    args: Vec<Bytes>,
}

impl<'a> SemanticGetCommand<'a> {
    /// Create a new SEMANTIC.GET command.
    pub fn new(conn: &'a mut Connection, query: impl ToArg) -> Self {
        Self {
            conn,
            args: vec![Bytes::from("SEMANTIC.GET"), arg(query)],
        }
    }

    /// Set the similarity threshold (0.0-1.0). Default: 0.85.
    pub fn threshold(mut self, threshold: f64) -> Self {
        self.args.push(Bytes::from("THRESHOLD"));
        self.args.push(Bytes::from(threshold.to_string()));
        self
    }

    /// Return up to N cached results.
    pub fn top(mut self, k: u32) -> Self {
        self.args.push(Bytes::from("TOP"));
        self.args.push(arg(k));
        self
    }

    /// Execute the command.
    pub async fn execute(self) -> Result<Value> {
        exec(self.conn, self.args).await
    }
}

// ---------------------------------------------------------------------------
// SEMANTIC.DEL
// ---------------------------------------------------------------------------

/// Builder for the SEMANTIC.DEL command.
pub struct SemanticDelCommand<'a> {
    conn: &'a mut Connection,
    args: Vec<Bytes>,
}

impl<'a> SemanticDelCommand<'a> {
    /// Delete semantically cached entries matching a query.
    pub fn new(conn: &'a mut Connection, query: impl ToArg) -> Self {
        Self {
            conn,
            args: vec![Bytes::from("SEMANTIC.DEL"), arg(query)],
        }
    }

    /// Set the similarity threshold for deletion.
    pub fn threshold(mut self, threshold: f64) -> Self {
        self.args.push(Bytes::from("THRESHOLD"));
        self.args.push(Bytes::from(threshold.to_string()));
        self
    }

    /// Execute the command.
    pub async fn execute(self) -> Result<Value> {
        exec(self.conn, self.args).await
    }
}

// ---------------------------------------------------------------------------
// SEMANTIC.STATS
// ---------------------------------------------------------------------------

/// Builder for the SEMANTIC.STATS command.
pub struct SemanticStatsCommand<'a> {
    conn: &'a mut Connection,
    args: Vec<Bytes>,
}

impl<'a> SemanticStatsCommand<'a> {
    /// Get semantic cache statistics.
    pub fn new(conn: &'a mut Connection) -> Self {
        Self {
            conn,
            args: vec![Bytes::from("SEMANTIC.STATS")],
        }
    }

    /// Execute the command.
    pub async fn execute(self) -> Result<Value> {
        exec(self.conn, self.args).await
    }
}
