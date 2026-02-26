//! Vector search command builders for Ferrite-specific vector operations.
//!
//! # Example
//!
//! ```ignore
//! use ferrite_rs::AsyncClient;
//!
//! let client = AsyncClient::connect("127.0.0.1", 6379).await?;
//!
//! // Create a vector index
//! client.vector_create("docs", 384)
//!     .distance("cosine")
//!     .index_type("hnsw")
//!     .execute().await?;
//!
//! // Add a vector
//! client.vector_add("docs", "doc:1", &[0.1, 0.2, 0.3])
//!     .metadata(r#"{"title": "Hello"}"#)
//!     .execute().await?;
//!
//! // Search
//! let results = client.vector_search("docs", &[0.1, 0.2, 0.3])
//!     .top(10)
//!     .execute().await?;
//! ```

use bytes::Bytes;

use crate::commands::{arg, exec};
use crate::connection::Connection;
use crate::error::Result;
use crate::types::{ToArg, Value};

// ---------------------------------------------------------------------------
// VECTOR.CREATE
// ---------------------------------------------------------------------------

/// Builder for the VECTOR.CREATE command.
pub struct VectorCreateCommand<'a> {
    conn: &'a mut Connection,
    args: Vec<Bytes>,
}

impl<'a> VectorCreateCommand<'a> {
    /// Create a new VECTOR.CREATE command.
    pub fn new(conn: &'a mut Connection, index: impl ToArg, dimension: u32) -> Self {
        Self {
            conn,
            args: vec![
                Bytes::from("VECTOR.CREATE"),
                arg(index),
                Bytes::from("DIM"),
                arg(dimension),
            ],
        }
    }

    /// Set the distance metric (cosine, euclidean, dot_product).
    pub fn distance(mut self, metric: &str) -> Self {
        self.args.push(Bytes::from("DISTANCE"));
        self.args.push(Bytes::from(metric.to_string()));
        self
    }

    /// Set the index type (hnsw, ivf, flat).
    pub fn index_type(mut self, itype: &str) -> Self {
        self.args.push(Bytes::from("TYPE"));
        self.args.push(Bytes::from(itype.to_string()));
        self
    }

    /// Set HNSW M parameter (connections per node).
    pub fn m(mut self, m: u32) -> Self {
        self.args.push(Bytes::from("M"));
        self.args.push(arg(m));
        self
    }

    /// Set HNSW ef_construction parameter.
    pub fn ef_construction(mut self, ef: u32) -> Self {
        self.args.push(Bytes::from("EF_CONSTRUCTION"));
        self.args.push(arg(ef));
        self
    }

    /// Set maximum number of elements.
    pub fn max_elements(mut self, max: u64) -> Self {
        self.args.push(Bytes::from("MAX_ELEMENTS"));
        self.args.push(arg(max));
        self
    }

    /// Execute the command.
    pub async fn execute(self) -> Result<Value> {
        exec(self.conn, self.args).await
    }
}

// ---------------------------------------------------------------------------
// VECTOR.ADD
// ---------------------------------------------------------------------------

/// Builder for the VECTOR.ADD command.
pub struct VectorAddCommand<'a> {
    conn: &'a mut Connection,
    args: Vec<Bytes>,
}

impl<'a> VectorAddCommand<'a> {
    /// Create a new VECTOR.ADD command.
    pub fn new(conn: &'a mut Connection, index: impl ToArg, id: impl ToArg, vector: &[f32]) -> Self {
        let vec_str = format!(
            "[{}]",
            vector
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
        Self {
            conn,
            args: vec![
                Bytes::from("VECTOR.ADD"),
                arg(index),
                arg(id),
                Bytes::from(vec_str),
            ],
        }
    }

    /// Attach JSON metadata to the vector.
    pub fn metadata(mut self, meta: &str) -> Self {
        self.args.push(Bytes::from(meta.to_string()));
        self
    }

    /// Add from text (auto-embedding). Server must have an embedding provider configured.
    pub fn text(mut self, text: &str) -> Self {
        // Replace the vector argument with TEXT directive
        if self.args.len() >= 4 {
            self.args[3] = Bytes::from("TEXT");
            self.args.push(Bytes::from(text.to_string()));
        }
        self
    }

    /// Execute the command.
    pub async fn execute(self) -> Result<Value> {
        exec(self.conn, self.args).await
    }
}

// ---------------------------------------------------------------------------
// VECTOR.SEARCH
// ---------------------------------------------------------------------------

/// Builder for the VECTOR.SEARCH command.
pub struct VectorSearchCommand<'a> {
    conn: &'a mut Connection,
    args: Vec<Bytes>,
}

impl<'a> VectorSearchCommand<'a> {
    /// Create a new VECTOR.SEARCH command.
    pub fn new(conn: &'a mut Connection, index: impl ToArg, vector: &[f32]) -> Self {
        let vec_str = format!(
            "[{}]",
            vector
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
        Self {
            conn,
            args: vec![
                Bytes::from("VECTOR.SEARCH"),
                arg(index),
                Bytes::from(vec_str),
            ],
        }
    }

    /// Number of results to return (default: 10).
    pub fn top(mut self, k: u32) -> Self {
        self.args.push(Bytes::from("TOP"));
        self.args.push(arg(k));
        self
    }

    /// Set the ef_search parameter for HNSW (runtime quality vs speed).
    pub fn ef_search(mut self, ef: u32) -> Self {
        self.args.push(Bytes::from("EF_SEARCH"));
        self.args.push(arg(ef));
        self
    }

    /// Apply a metadata filter expression.
    pub fn filter(mut self, filter_expr: &str) -> Self {
        self.args.push(Bytes::from("FILTER"));
        self.args.push(Bytes::from(filter_expr.to_string()));
        self
    }

    /// Search by text (auto-embedding).
    pub fn text(mut self, text: &str) -> Self {
        if self.args.len() >= 3 {
            self.args[2] = Bytes::from("TEXT");
            self.args.push(Bytes::from(text.to_string()));
        }
        self
    }

    /// Execute the command.
    pub async fn execute(self) -> Result<Value> {
        exec(self.conn, self.args).await
    }
}

// ---------------------------------------------------------------------------
// VECTOR.DELETE
// ---------------------------------------------------------------------------

/// Builder for the VECTOR.DELETE command.
pub struct VectorDeleteCommand<'a> {
    conn: &'a mut Connection,
    args: Vec<Bytes>,
}

impl<'a> VectorDeleteCommand<'a> {
    /// Create a new VECTOR.DELETE command.
    pub fn new(conn: &'a mut Connection, index: impl ToArg, id: impl ToArg) -> Self {
        Self {
            conn,
            args: vec![Bytes::from("VECTOR.DELETE"), arg(index), arg(id)],
        }
    }

    /// Execute the command.
    pub async fn execute(self) -> Result<Value> {
        exec(self.conn, self.args).await
    }
}

// ---------------------------------------------------------------------------
// VECTOR.INFO
// ---------------------------------------------------------------------------

/// Builder for the VECTOR.INFO command.
pub struct VectorInfoCommand<'a> {
    conn: &'a mut Connection,
    args: Vec<Bytes>,
}

impl<'a> VectorInfoCommand<'a> {
    /// Create a new VECTOR.INFO command.
    pub fn new(conn: &'a mut Connection, index: impl ToArg) -> Self {
        Self {
            conn,
            args: vec![Bytes::from("VECTOR.INFO"), arg(index)],
        }
    }

    /// Execute the command.
    pub async fn execute(self) -> Result<Value> {
        exec(self.conn, self.args).await
    }
}
