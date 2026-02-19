//! Ferrite extension commands that go beyond standard Redis operations.
//!
//! These commands are sent as raw `COMMAND arg1 arg2 ...` invocations over the
//! Redis protocol. They rely on Ferrite-specific server-side modules for vector
//! search, semantic caching, time-series, document store, graph, FerriteQL,
//! and time-travel queries.

use redis::{AsyncCommands, FromRedisValue, RedisResult, Value};
use serde::{Deserialize, Serialize};

use crate::error::{FerriteError, Result};

// ---------------------------------------------------------------------------
// Shared result / option types
// ---------------------------------------------------------------------------

/// A single result from a vector similarity search.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSearchResult {
    /// The key/ID of the matched item.
    pub id: String,
    /// The similarity score (higher is more similar for cosine, lower for L2).
    pub score: f64,
    /// Optional metadata stored alongside the vector.
    pub metadata: Option<serde_json::Value>,
}

/// Options for vector search queries.
#[derive(Debug, Clone, Default)]
pub struct VectorSearchOptions {
    /// Maximum number of results to return.
    pub top_k: usize,
    /// Optional metadata filter expression.
    pub filter: Option<String>,
    /// Whether to include the raw vectors in results.
    pub include_vectors: bool,
}

impl VectorSearchOptions {
    /// Create a new options builder starting with sensible defaults.
    pub fn new() -> Self {
        Self {
            top_k: 10,
            filter: None,
            include_vectors: false,
        }
    }

    pub fn top_k(mut self, k: usize) -> Self {
        self.top_k = k;
        self
    }

    pub fn filter(mut self, expr: &str) -> Self {
        self.filter = Some(expr.to_string());
        self
    }

    pub fn include_vectors(mut self, yes: bool) -> Self {
        self.include_vectors = yes;
        self
    }
}

/// A single result from a semantic search.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticSearchResult {
    pub id: String,
    pub score: f64,
    pub text: Option<String>,
    pub metadata: Option<serde_json::Value>,
}

/// A single time-series sample.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesSample {
    /// Timestamp in milliseconds since the Unix epoch.
    pub timestamp: i64,
    /// The sample value.
    pub value: f64,
}

/// A single result from a document search.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentResult {
    pub id: String,
    pub document: serde_json::Value,
    pub score: Option<f64>,
}

/// A result row from a graph query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

/// A result from a FerriteQL query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub count: usize,
}

/// A historical version of a key returned by time-travel queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeTravelEntry {
    /// Timestamp in milliseconds since the Unix epoch.
    pub timestamp: i64,
    /// The value at that point in time.
    pub value: String,
}

// ---------------------------------------------------------------------------
// Extension trait -- the real implementation
// ---------------------------------------------------------------------------

/// Extension trait that adds Ferrite-specific commands to any async Redis
/// connection.
///
/// This is implemented for [`redis::aio::MultiplexedConnection`] and is also
/// used internally by [`crate::FerriteClient`].
#[allow(async_fn_in_trait)]
pub trait FerriteCommands {
    /// Return a mutable reference to the underlying async connection.
    fn connection_mut(&mut self) -> &mut redis::aio::MultiplexedConnection;

    // -- Vector Search ------------------------------------------------------

    /// Create a vector index.
    ///
    /// ```text
    /// VECTOR.INDEX.CREATE <index> DIM <dim> DISTANCE <metric> TYPE <algo>
    /// ```
    async fn vector_create_index(
        &mut self,
        index: &str,
        dimensions: usize,
        distance: &str,
        index_type: &str,
    ) -> Result<()> {
        redis::cmd("VECTOR.INDEX.CREATE")
            .arg(index)
            .arg("DIM")
            .arg(dimensions)
            .arg("DISTANCE")
            .arg(distance)
            .arg("TYPE")
            .arg(index_type)
            .query_async(self.connection_mut())
            .await?;
        Ok(())
    }

    /// Add a vector to an index with optional metadata.
    async fn vector_set(
        &mut self,
        index: &str,
        id: &str,
        vector: &[f32],
        metadata: Option<&serde_json::Value>,
    ) -> Result<()> {
        let vec_str = serde_json::to_string(vector)?;
        let mut cmd = redis::cmd("VECTOR.ADD");
        cmd.arg(index).arg(id).arg(&vec_str);
        if let Some(meta) = metadata {
            cmd.arg(serde_json::to_string(meta)?);
        }
        cmd.query_async(self.connection_mut()).await?;
        Ok(())
    }

    /// Perform a k-nearest-neighbor vector search.
    async fn vector_search(
        &mut self,
        index: &str,
        query_vector: &[f32],
        options: &VectorSearchOptions,
    ) -> Result<Vec<VectorSearchResult>> {
        let vec_str = serde_json::to_string(query_vector)?;
        let mut cmd = redis::cmd("VECTOR.SEARCH");
        cmd.arg(index).arg(&vec_str).arg("TOP").arg(options.top_k);
        if let Some(ref f) = options.filter {
            cmd.arg("FILTER").arg(f);
        }
        if options.include_vectors {
            cmd.arg("INCLUDE_VECTORS");
        }
        let raw: Value = cmd.query_async(self.connection_mut()).await?;
        parse_vector_search_results(raw)
    }

    /// Shorthand for `vector_search` with only `top_k`.
    async fn vector_knn(
        &mut self,
        index: &str,
        query_vector: &[f32],
        k: usize,
    ) -> Result<Vec<VectorSearchResult>> {
        self.vector_search(index, query_vector, &VectorSearchOptions::new().top_k(k))
            .await
    }

    // -- Semantic Caching ---------------------------------------------------

    /// Store a value with a semantic (natural-language) key.
    ///
    /// ```text
    /// SEMANTIC.SET <text> <value> [THRESHOLD <f64>]
    /// ```
    async fn semantic_set(&mut self, text: &str, value: &str) -> Result<()> {
        redis::cmd("SEMANTIC.SET")
            .arg(text)
            .arg(value)
            .query_async(self.connection_mut())
            .await?;
        Ok(())
    }

    /// Retrieve a value whose semantic key is similar to `text` above
    /// the given similarity threshold.
    async fn semantic_search(
        &mut self,
        text: &str,
        threshold: f64,
    ) -> Result<Vec<SemanticSearchResult>> {
        let raw: Value = redis::cmd("SEMANTIC.GET")
            .arg(text)
            .arg(threshold)
            .query_async(self.connection_mut())
            .await?;
        parse_semantic_search_results(raw)
    }

    // -- Time Series --------------------------------------------------------

    /// Append a sample to a time-series key.
    ///
    /// Pass `timestamp` as `"*"` to let the server auto-assign.
    async fn ts_add(
        &mut self,
        key: &str,
        timestamp: &str,
        value: f64,
    ) -> Result<i64> {
        let ts: i64 = redis::cmd("TS.ADD")
            .arg(key)
            .arg(timestamp)
            .arg(value)
            .query_async(self.connection_mut())
            .await?;
        Ok(ts)
    }

    /// Query a range of samples from a time-series key.
    async fn ts_range(
        &mut self,
        key: &str,
        from: &str,
        to: &str,
    ) -> Result<Vec<TimeSeriesSample>> {
        let raw: Value = redis::cmd("TS.RANGE")
            .arg(key)
            .arg(from)
            .arg(to)
            .query_async(self.connection_mut())
            .await?;
        parse_ts_samples(raw)
    }

    /// Fetch the latest sample for a time-series key.
    async fn ts_get(&mut self, key: &str) -> Result<Option<TimeSeriesSample>> {
        let raw: Value = redis::cmd("TS.GET")
            .arg(key)
            .query_async(self.connection_mut())
            .await?;
        parse_ts_single_sample(raw)
    }

    // -- Document Store -----------------------------------------------------

    /// Insert or replace a JSON document.
    async fn doc_set(
        &mut self,
        collection: &str,
        id: &str,
        document: &serde_json::Value,
    ) -> Result<()> {
        redis::cmd("DOC.INSERT")
            .arg(collection)
            .arg(id)
            .arg(serde_json::to_string(document)?)
            .query_async(self.connection_mut())
            .await?;
        Ok(())
    }

    /// Search documents in a collection with a JSON query.
    async fn doc_search(
        &mut self,
        collection: &str,
        query: &serde_json::Value,
    ) -> Result<Vec<DocumentResult>> {
        let raw: Value = redis::cmd("DOC.FIND")
            .arg(collection)
            .arg(serde_json::to_string(query)?)
            .query_async(self.connection_mut())
            .await?;
        parse_document_results(raw)
    }

    // -- Graph --------------------------------------------------------------

    /// Execute a Cypher-like graph query.
    async fn graph_query(
        &mut self,
        graph: &str,
        cypher: &str,
    ) -> Result<GraphResult> {
        let raw: Value = redis::cmd("GRAPH.QUERY")
            .arg(graph)
            .arg(cypher)
            .query_async(self.connection_mut())
            .await?;
        parse_graph_result(raw)
    }

    // -- FerriteQL ----------------------------------------------------------

    /// Execute a FerriteQL query.
    ///
    /// ```text
    /// QUERY FROM users:* WHERE $.active = true SELECT $.name LIMIT 10
    /// ```
    async fn query(&mut self, ferrite_ql: &str) -> Result<QueryResult> {
        let raw: Value = redis::cmd("QUERY")
            .arg(ferrite_ql)
            .query_async(self.connection_mut())
            .await?;
        parse_query_result(raw)
    }

    // -- Time Travel --------------------------------------------------------

    /// Retrieve the value of a key at a previous point in time.
    ///
    /// `as_of` is a human-friendly offset such as `"-1h"` or `"-24h"`, or a
    /// Unix timestamp in milliseconds.
    async fn time_travel(
        &mut self,
        key: &str,
        as_of: &str,
    ) -> Result<Option<String>> {
        let raw: Value = redis::cmd("GET")
            .arg(key)
            .arg("AS")
            .arg("OF")
            .arg(as_of)
            .query_async(self.connection_mut())
            .await?;
        match raw {
            Value::Nil => Ok(None),
            Value::BulkString(bytes) => {
                Ok(Some(String::from_utf8_lossy(&bytes).into_owned()))
            }
            _ => Ok(Some(format!("{:?}", raw))),
        }
    }

    /// Retrieve the full mutation history of a key.
    async fn history(
        &mut self,
        key: &str,
        since: &str,
    ) -> Result<Vec<TimeTravelEntry>> {
        let raw: Value = redis::cmd("HISTORY")
            .arg(key)
            .arg("SINCE")
            .arg(since)
            .query_async(self.connection_mut())
            .await?;
        parse_history_results(raw)
    }
}

// ---------------------------------------------------------------------------
// Parsers (private helpers)
// ---------------------------------------------------------------------------

fn parse_vector_search_results(raw: Value) -> Result<Vec<VectorSearchResult>> {
    let mut results = Vec::new();
    if let Value::Array(items) = raw {
        // Expected format: array of [id, score, metadata?, ...]
        let mut i = 0;
        while i < items.len() {
            let id = value_to_string(&items[i])?;
            let score = value_to_f64(&items.get(i + 1).cloned().unwrap_or(Value::Nil))?;
            let metadata = items.get(i + 2).and_then(|v| {
                value_to_string(v).ok().and_then(|s| serde_json::from_str(&s).ok())
            });
            results.push(VectorSearchResult {
                id,
                score,
                metadata,
            });
            i += 3;
        }
    }
    Ok(results)
}

fn parse_semantic_search_results(raw: Value) -> Result<Vec<SemanticSearchResult>> {
    let mut results = Vec::new();
    if let Value::Array(items) = raw {
        let mut i = 0;
        while i < items.len() {
            let id = value_to_string(&items[i])?;
            let score = value_to_f64(&items.get(i + 1).cloned().unwrap_or(Value::Nil))?;
            let text = items.get(i + 2).and_then(|v| value_to_string(v).ok());
            results.push(SemanticSearchResult {
                id,
                score,
                text,
                metadata: None,
            });
            i += 3;
        }
    } else if let Value::BulkString(bytes) = raw {
        // Single result returned directly
        results.push(SemanticSearchResult {
            id: String::new(),
            score: 1.0,
            text: Some(String::from_utf8_lossy(&bytes).into_owned()),
            metadata: None,
        });
    }
    Ok(results)
}

fn parse_ts_samples(raw: Value) -> Result<Vec<TimeSeriesSample>> {
    let mut samples = Vec::new();
    if let Value::Array(items) = raw {
        for item in items {
            if let Value::Array(pair) = item {
                if pair.len() >= 2 {
                    let timestamp = value_to_i64(&pair[0])?;
                    let value = value_to_f64(&pair[1])?;
                    samples.push(TimeSeriesSample { timestamp, value });
                }
            }
        }
    }
    Ok(samples)
}

fn parse_ts_single_sample(raw: Value) -> Result<Option<TimeSeriesSample>> {
    match raw {
        Value::Nil => Ok(None),
        Value::Array(pair) if pair.len() >= 2 => {
            let timestamp = value_to_i64(&pair[0])?;
            let value = value_to_f64(&pair[1])?;
            Ok(Some(TimeSeriesSample { timestamp, value }))
        }
        _ => Ok(None),
    }
}

fn parse_document_results(raw: Value) -> Result<Vec<DocumentResult>> {
    let mut results = Vec::new();
    if let Value::Array(items) = raw {
        for item in items {
            if let Value::Array(fields) = item {
                let id = if !fields.is_empty() {
                    value_to_string(&fields[0])?
                } else {
                    String::new()
                };
                let doc_str = if fields.len() > 1 {
                    value_to_string(&fields[1])?
                } else {
                    "{}".to_string()
                };
                let document: serde_json::Value =
                    serde_json::from_str(&doc_str).unwrap_or(serde_json::Value::Null);
                results.push(DocumentResult {
                    id,
                    document,
                    score: None,
                });
            }
        }
    }
    Ok(results)
}

fn parse_graph_result(raw: Value) -> Result<GraphResult> {
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    if let Value::Array(items) = raw {
        // First element: column names
        if let Some(Value::Array(cols)) = items.first() {
            for col in cols {
                columns.push(value_to_string(col).unwrap_or_default());
            }
        }
        // Remaining elements: data rows
        for item in items.iter().skip(1) {
            if let Value::Array(row_vals) = item {
                let row: Vec<serde_json::Value> = row_vals
                    .iter()
                    .map(|v| {
                        value_to_string(v)
                            .map(|s| serde_json::Value::String(s))
                            .unwrap_or(serde_json::Value::Null)
                    })
                    .collect();
                rows.push(row);
            }
        }
    }
    Ok(GraphResult { columns, rows })
}

fn parse_query_result(raw: Value) -> Result<QueryResult> {
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    if let Value::Array(items) = raw {
        if let Some(Value::Array(cols)) = items.first() {
            for col in cols {
                columns.push(value_to_string(col).unwrap_or_default());
            }
        }
        for item in items.iter().skip(1) {
            if let Value::Array(row_vals) = item {
                let row: Vec<serde_json::Value> = row_vals
                    .iter()
                    .map(|v| {
                        value_to_string(v)
                            .map(|s| serde_json::Value::String(s))
                            .unwrap_or(serde_json::Value::Null)
                    })
                    .collect();
                rows.push(row);
            }
        }
    }
    let count = rows.len();
    Ok(QueryResult {
        columns,
        rows,
        count,
    })
}

fn parse_history_results(raw: Value) -> Result<Vec<TimeTravelEntry>> {
    let mut entries = Vec::new();
    if let Value::Array(items) = raw {
        for item in items {
            if let Value::Array(pair) = item {
                if pair.len() >= 2 {
                    let timestamp = value_to_i64(&pair[0])?;
                    let value = value_to_string(&pair[1])?;
                    entries.push(TimeTravelEntry { timestamp, value });
                }
            }
        }
    }
    Ok(entries)
}

// ---------------------------------------------------------------------------
// Value extraction helpers
// ---------------------------------------------------------------------------

fn value_to_string(val: &Value) -> Result<String> {
    match val {
        Value::BulkString(bytes) => Ok(String::from_utf8_lossy(bytes).into_owned()),
        Value::SimpleString(s) => Ok(s.clone()),
        Value::Int(i) => Ok(i.to_string()),
        Value::Double(f) => Ok(f.to_string()),
        Value::Nil => Err(FerriteError::InvalidResponse("unexpected nil value".into())),
        other => Ok(format!("{:?}", other)),
    }
}

fn value_to_f64(val: &Value) -> Result<f64> {
    match val {
        Value::Double(f) => Ok(*f),
        Value::Int(i) => Ok(*i as f64),
        Value::BulkString(bytes) => {
            let s = String::from_utf8_lossy(bytes);
            s.parse::<f64>()
                .map_err(|_| FerriteError::InvalidResponse(format!("cannot parse f64: {}", s)))
        }
        Value::SimpleString(s) => s
            .parse::<f64>()
            .map_err(|_| FerriteError::InvalidResponse(format!("cannot parse f64: {}", s))),
        _ => Err(FerriteError::InvalidResponse(
            "expected numeric value".into(),
        )),
    }
}

fn value_to_i64(val: &Value) -> Result<i64> {
    match val {
        Value::Int(i) => Ok(*i),
        Value::BulkString(bytes) => {
            let s = String::from_utf8_lossy(bytes);
            s.parse::<i64>()
                .map_err(|_| FerriteError::InvalidResponse(format!("cannot parse i64: {}", s)))
        }
        Value::SimpleString(s) => s
            .parse::<i64>()
            .map_err(|_| FerriteError::InvalidResponse(format!("cannot parse i64: {}", s))),
        _ => Err(FerriteError::InvalidResponse(
            "expected integer value".into(),
        )),
    }
}
