//! Ferrite Protocol Extensions
//!
//! Defines protocol extensions for Ferrite-specific features beyond standard Redis commands.
//! Client SDKs use these definitions to support vector search, semantic caching,
//! time-travel queries, CDC subscriptions, CRDT operations, and triggers.
//!
//! # Example
//!
//! ```ignore
//! use ferrite::sdk::protocol_ext::{ExtendedCommand, DistanceMetric};
//!
//! let cmd = ExtendedCommand::VectorSearch {
//!     index: "embeddings".to_string(),
//!     query_vector: vec![0.1, 0.2, 0.3],
//!     top_k: 10,
//!     filter: None,
//! };
//! let args = cmd.to_resp_args();
//! ```

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Trait defining the extended command interface for Ferrite-specific features.
///
/// Client SDKs implement this trait to provide access to Ferrite's protocol
/// extensions beyond standard Redis commands.
pub trait FerriteProtocolExtension {
    /// Error type returned by extended operations
    type Error;

    /// Execute a vector similarity search
    fn vector_search(
        &self,
        index: &str,
        query_vector: &[f32],
        top_k: usize,
        filter: Option<&str>,
    ) -> Result<ExtendedResponse, Self::Error>;

    /// Add a vector to an index
    fn vector_add(
        &self,
        index: &str,
        id: &str,
        vector: &[f32],
        metadata: Option<&HashMap<String, String>>,
    ) -> Result<ExtendedResponse, Self::Error>;

    /// Create a vector index
    fn vector_create(
        &self,
        name: &str,
        dimensions: u32,
        distance_metric: DistanceMetric,
        index_type: IndexType,
    ) -> Result<ExtendedResponse, Self::Error>;

    /// Store a semantic cache entry
    fn semantic_set(
        &self,
        query: &str,
        response: &str,
        ttl: Option<u64>,
    ) -> Result<ExtendedResponse, Self::Error>;

    /// Retrieve a semantic cache entry by similarity
    fn semantic_get(&self, query: &str, threshold: f64) -> Result<ExtendedResponse, Self::Error>;

    /// Query a key's value at a point in time
    fn time_travel(&self, key: &str, as_of: u64) -> Result<ExtendedResponse, Self::Error>;

    /// Retrieve the history of changes for a key
    fn history(
        &self,
        key: &str,
        since: Option<u64>,
        limit: Option<usize>,
    ) -> Result<ExtendedResponse, Self::Error>;

    /// Execute a Ferrite query language statement
    fn ferrite_query(&self, query_string: &str) -> Result<ExtendedResponse, Self::Error>;

    /// Subscribe to change data capture events
    fn cdc_subscribe(&self, pattern: &str, format: &str) -> Result<ExtendedResponse, Self::Error>;

    /// Perform a CRDT increment operation
    fn crdt_incr(&self, key: &str, delta: i64) -> Result<ExtendedResponse, Self::Error>;

    /// Create an event-driven trigger
    fn trigger_create(
        &self,
        name: &str,
        event: &str,
        pattern: &str,
        action: &str,
    ) -> Result<ExtendedResponse, Self::Error>;
}

/// Ferrite-specific extended commands beyond standard Redis.
///
/// Each variant maps to a Ferrite protocol command with its associated parameters.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ExtendedCommand {
    /// Search for similar vectors in an index
    VectorSearch {
        /// Name of the vector index
        index: String,
        /// Query vector for similarity search
        query_vector: Vec<f32>,
        /// Number of nearest neighbors to return
        top_k: usize,
        /// Optional filter expression
        filter: Option<String>,
    },

    /// Add a vector to an index
    VectorAdd {
        /// Name of the vector index
        index: String,
        /// Unique identifier for the vector
        id: String,
        /// The vector data
        vector: Vec<f32>,
        /// Optional metadata key-value pairs
        metadata: Option<HashMap<String, String>>,
    },

    /// Create a new vector index
    VectorCreate {
        /// Index name
        name: String,
        /// Number of dimensions
        dimensions: u32,
        /// Distance metric for similarity
        distance_metric: DistanceMetric,
        /// Index algorithm type
        index_type: IndexType,
    },

    /// Store a query-response pair in the semantic cache
    SemanticSet {
        /// The query string to cache
        query: String,
        /// The response to associate with the query
        response: String,
        /// Optional TTL in seconds
        ttl: Option<u64>,
    },

    /// Retrieve a cached response by semantic similarity
    SemanticGet {
        /// The query to match against
        query: String,
        /// Minimum similarity threshold (0.0 to 1.0)
        threshold: f64,
    },

    /// Query a key's value at a specific point in time
    TimeTravel {
        /// The key to look up
        key: String,
        /// Unix timestamp (milliseconds) to query at
        as_of: u64,
    },

    /// Retrieve the change history for a key
    History {
        /// The key to get history for
        key: String,
        /// Optional start timestamp (milliseconds) filter
        since: Option<u64>,
        /// Optional maximum number of entries to return
        limit: Option<usize>,
    },

    /// Execute a Ferrite query language statement
    FerriteQuery {
        /// The query string to execute
        query_string: String,
    },

    /// Subscribe to change data capture events matching a pattern
    CdcSubscribe {
        /// Key pattern to subscribe to (glob-style)
        pattern: String,
        /// Output format (e.g., "json", "protobuf")
        format: String,
    },

    /// Perform a conflict-free replicated data type increment
    CrdtIncr {
        /// The CRDT counter key
        key: String,
        /// The increment delta (can be negative)
        delta: i64,
    },

    /// Create an event-driven trigger
    TriggerCreate {
        /// Trigger name
        name: String,
        /// Event type (e.g., "set", "del", "expire")
        event: String,
        /// Key pattern to match (glob-style)
        pattern: String,
        /// Action to execute (script or command)
        action: String,
    },
}

/// Distance metric for vector similarity search
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DistanceMetric {
    /// Cosine similarity (1 - cosine distance)
    Cosine,
    /// Euclidean (L2) distance
    Euclidean,
    /// Dot product similarity
    DotProduct,
    /// Manhattan (L1) distance
    Manhattan,
}

/// Vector index algorithm type
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IndexType {
    /// Hierarchical Navigable Small World graph
    Hnsw,
    /// Inverted File index
    Ivf,
    /// Flat (brute-force) index
    Flat,
}

/// Typed responses from Ferrite extended commands
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ExtendedResponse {
    /// Simple success acknowledgment
    Ok,

    /// A single string value
    Value(String),

    /// A single integer value
    Integer(i64),

    /// A single float value
    Float(f64),

    /// Binary data
    Data(Bytes),

    /// Null / not found
    Null,

    /// Vector search results: list of (id, score, metadata) tuples
    SearchResults(Vec<SearchResult>),

    /// Key history entries
    HistoryEntries(Vec<HistoryEntry>),

    /// Query result rows
    QueryRows(Vec<HashMap<String, String>>),

    /// Subscription ID for CDC
    SubscriptionId(String),

    /// Error response
    Error(String),
}

/// A single vector search result
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SearchResult {
    /// Vector identifier
    pub id: String,
    /// Similarity score
    pub score: f64,
    /// Optional metadata
    pub metadata: HashMap<String, String>,
}

/// A single history entry for a key
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HistoryEntry {
    /// Unix timestamp in milliseconds
    pub timestamp: u64,
    /// The value at this point in time
    pub value: String,
    /// The operation that produced this entry
    pub operation: String,
}

impl DistanceMetric {
    /// Convert to RESP argument string
    pub fn as_str(&self) -> &'static str {
        match self {
            DistanceMetric::Cosine => "COSINE",
            DistanceMetric::Euclidean => "EUCLIDEAN",
            DistanceMetric::DotProduct => "DOTPRODUCT",
            DistanceMetric::Manhattan => "MANHATTAN",
        }
    }
}

impl IndexType {
    /// Convert to RESP argument string
    pub fn as_str(&self) -> &'static str {
        match self {
            IndexType::Hnsw => "HNSW",
            IndexType::Ivf => "IVF",
            IndexType::Flat => "FLAT",
        }
    }
}

impl ExtendedCommand {
    /// Serialize this command to RESP-compatible argument list.
    ///
    /// Returns a `Vec<Bytes>` where the first element is the command name
    /// and subsequent elements are the arguments, ready to be wrapped in a
    /// RESP array of bulk strings.
    pub fn to_resp_args(&self) -> Vec<Bytes> {
        match self {
            ExtendedCommand::VectorSearch {
                index,
                query_vector,
                top_k,
                filter,
            } => {
                let mut args = vec![
                    Bytes::from_static(b"FERRITE.VSEARCH"),
                    Bytes::from(index.clone()),
                    Bytes::from(top_k.to_string()),
                ];
                for v in query_vector {
                    args.push(Bytes::from(v.to_string()));
                }
                if let Some(f) = filter {
                    args.push(Bytes::from_static(b"FILTER"));
                    args.push(Bytes::from(f.clone()));
                }
                args
            }

            ExtendedCommand::VectorAdd {
                index,
                id,
                vector,
                metadata,
            } => {
                let mut args = vec![
                    Bytes::from_static(b"FERRITE.VADD"),
                    Bytes::from(index.clone()),
                    Bytes::from(id.clone()),
                ];
                for v in vector {
                    args.push(Bytes::from(v.to_string()));
                }
                if let Some(meta) = metadata {
                    args.push(Bytes::from_static(b"META"));
                    for (k, v) in meta {
                        args.push(Bytes::from(k.clone()));
                        args.push(Bytes::from(v.clone()));
                    }
                }
                args
            }

            ExtendedCommand::VectorCreate {
                name,
                dimensions,
                distance_metric,
                index_type,
            } => {
                vec![
                    Bytes::from_static(b"FERRITE.VCREATE"),
                    Bytes::from(name.clone()),
                    Bytes::from(dimensions.to_string()),
                    Bytes::from_static(b"METRIC"),
                    Bytes::from(distance_metric.as_str()),
                    Bytes::from_static(b"TYPE"),
                    Bytes::from(index_type.as_str()),
                ]
            }

            ExtendedCommand::SemanticSet {
                query,
                response,
                ttl,
            } => {
                let mut args = vec![
                    Bytes::from_static(b"FERRITE.SEMSET"),
                    Bytes::from(query.clone()),
                    Bytes::from(response.clone()),
                ];
                if let Some(t) = ttl {
                    args.push(Bytes::from_static(b"TTL"));
                    args.push(Bytes::from(t.to_string()));
                }
                args
            }

            ExtendedCommand::SemanticGet { query, threshold } => {
                vec![
                    Bytes::from_static(b"FERRITE.SEMGET"),
                    Bytes::from(query.clone()),
                    Bytes::from_static(b"THRESHOLD"),
                    Bytes::from(threshold.to_string()),
                ]
            }

            ExtendedCommand::TimeTravel { key, as_of } => {
                vec![
                    Bytes::from_static(b"FERRITE.TIMETRAVEL"),
                    Bytes::from(key.clone()),
                    Bytes::from(as_of.to_string()),
                ]
            }

            ExtendedCommand::History { key, since, limit } => {
                let mut args = vec![
                    Bytes::from_static(b"FERRITE.HISTORY"),
                    Bytes::from(key.clone()),
                ];
                if let Some(s) = since {
                    args.push(Bytes::from_static(b"SINCE"));
                    args.push(Bytes::from(s.to_string()));
                }
                if let Some(l) = limit {
                    args.push(Bytes::from_static(b"LIMIT"));
                    args.push(Bytes::from(l.to_string()));
                }
                args
            }

            ExtendedCommand::FerriteQuery { query_string } => {
                vec![
                    Bytes::from_static(b"FERRITE.QUERY"),
                    Bytes::from(query_string.clone()),
                ]
            }

            ExtendedCommand::CdcSubscribe { pattern, format } => {
                vec![
                    Bytes::from_static(b"FERRITE.CDCSUB"),
                    Bytes::from(pattern.clone()),
                    Bytes::from_static(b"FORMAT"),
                    Bytes::from(format.clone()),
                ]
            }

            ExtendedCommand::CrdtIncr { key, delta } => {
                vec![
                    Bytes::from_static(b"FERRITE.CRDTINCR"),
                    Bytes::from(key.clone()),
                    Bytes::from(delta.to_string()),
                ]
            }

            ExtendedCommand::TriggerCreate {
                name,
                event,
                pattern,
                action,
            } => {
                vec![
                    Bytes::from_static(b"FERRITE.TRIGGER"),
                    Bytes::from(name.clone()),
                    Bytes::from_static(b"ON"),
                    Bytes::from(event.clone()),
                    Bytes::from_static(b"MATCH"),
                    Bytes::from(pattern.clone()),
                    Bytes::from_static(b"DO"),
                    Bytes::from(action.clone()),
                ]
            }
        }
    }

    /// Return the RESP command name for this extended command
    pub fn command_name(&self) -> &'static str {
        match self {
            ExtendedCommand::VectorSearch { .. } => "FERRITE.VSEARCH",
            ExtendedCommand::VectorAdd { .. } => "FERRITE.VADD",
            ExtendedCommand::VectorCreate { .. } => "FERRITE.VCREATE",
            ExtendedCommand::SemanticSet { .. } => "FERRITE.SEMSET",
            ExtendedCommand::SemanticGet { .. } => "FERRITE.SEMGET",
            ExtendedCommand::TimeTravel { .. } => "FERRITE.TIMETRAVEL",
            ExtendedCommand::History { .. } => "FERRITE.HISTORY",
            ExtendedCommand::FerriteQuery { .. } => "FERRITE.QUERY",
            ExtendedCommand::CdcSubscribe { .. } => "FERRITE.CDCSUB",
            ExtendedCommand::CrdtIncr { .. } => "FERRITE.CRDTINCR",
            ExtendedCommand::TriggerCreate { .. } => "FERRITE.TRIGGER",
        }
    }
}

impl ExtendedResponse {
    /// Parse an `ExtendedResponse` from a RESP-style frame representation.
    ///
    /// Accepts a simplified frame model (a list of `Bytes` tokens) as returned
    /// by the server. The first token encodes the response type tag.
    pub fn from_resp_tokens(tokens: &[Bytes]) -> Result<Self, String> {
        if tokens.is_empty() {
            return Err("empty response".to_string());
        }

        let tag = std::str::from_utf8(&tokens[0]).map_err(|e| e.to_string())?;

        match tag {
            "OK" => Ok(ExtendedResponse::Ok),
            "NULL" => Ok(ExtendedResponse::Null),
            "VALUE" => {
                let val = tokens
                    .get(1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .ok_or("missing value")?;
                Ok(ExtendedResponse::Value(val.to_string()))
            }
            "INTEGER" => {
                let n = tokens
                    .get(1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .and_then(|s| s.parse::<i64>().ok())
                    .ok_or("invalid integer")?;
                Ok(ExtendedResponse::Integer(n))
            }
            "FLOAT" => {
                let f = tokens
                    .get(1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .and_then(|s| s.parse::<f64>().ok())
                    .ok_or("invalid float")?;
                Ok(ExtendedResponse::Float(f))
            }
            "ERROR" => {
                let msg = tokens
                    .get(1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .unwrap_or("unknown error");
                Ok(ExtendedResponse::Error(msg.to_string()))
            }
            "SUBSCRIPTION" => {
                let id = tokens
                    .get(1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .ok_or("missing subscription id")?;
                Ok(ExtendedResponse::SubscriptionId(id.to_string()))
            }
            _ => Err(format!("unknown response tag: {}", tag)),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn test_distance_metric_as_str() {
        assert_eq!(DistanceMetric::Cosine.as_str(), "COSINE");
        assert_eq!(DistanceMetric::Euclidean.as_str(), "EUCLIDEAN");
        assert_eq!(DistanceMetric::DotProduct.as_str(), "DOTPRODUCT");
        assert_eq!(DistanceMetric::Manhattan.as_str(), "MANHATTAN");
    }

    #[test]
    fn test_index_type_as_str() {
        assert_eq!(IndexType::Hnsw.as_str(), "HNSW");
        assert_eq!(IndexType::Ivf.as_str(), "IVF");
        assert_eq!(IndexType::Flat.as_str(), "FLAT");
    }

    #[test]
    fn test_vector_search_to_resp() {
        let cmd = ExtendedCommand::VectorSearch {
            index: "idx".to_string(),
            query_vector: vec![0.1, 0.2, 0.3],
            top_k: 5,
            filter: None,
        };
        let args = cmd.to_resp_args();
        assert_eq!(args[0], Bytes::from_static(b"FERRITE.VSEARCH"));
        assert_eq!(args[1], Bytes::from("idx"));
        assert_eq!(args[2], Bytes::from("5"));
        assert_eq!(args.len(), 6); // cmd + index + top_k + 3 floats
    }

    #[test]
    fn test_vector_search_with_filter() {
        let cmd = ExtendedCommand::VectorSearch {
            index: "idx".to_string(),
            query_vector: vec![1.0],
            top_k: 10,
            filter: Some("category = 'news'".to_string()),
        };
        let args = cmd.to_resp_args();
        assert!(args.contains(&Bytes::from_static(b"FILTER")));
        assert!(args.contains(&Bytes::from("category = 'news'")));
    }

    #[test]
    fn test_vector_add_to_resp() {
        let cmd = ExtendedCommand::VectorAdd {
            index: "idx".to_string(),
            id: "vec1".to_string(),
            vector: vec![0.5, 0.6],
            metadata: None,
        };
        let args = cmd.to_resp_args();
        assert_eq!(args[0], Bytes::from_static(b"FERRITE.VADD"));
        assert_eq!(args[1], Bytes::from("idx"));
        assert_eq!(args[2], Bytes::from("vec1"));
        assert_eq!(args.len(), 5); // cmd + index + id + 2 floats
    }

    #[test]
    fn test_vector_add_with_metadata() {
        let mut meta = HashMap::new();
        meta.insert("color".to_string(), "red".to_string());
        let cmd = ExtendedCommand::VectorAdd {
            index: "idx".to_string(),
            id: "v1".to_string(),
            vector: vec![1.0],
            metadata: Some(meta),
        };
        let args = cmd.to_resp_args();
        assert!(args.contains(&Bytes::from_static(b"META")));
        assert!(args.contains(&Bytes::from("color")));
        assert!(args.contains(&Bytes::from("red")));
    }

    #[test]
    fn test_vector_create_to_resp() {
        let cmd = ExtendedCommand::VectorCreate {
            name: "my_index".to_string(),
            dimensions: 128,
            distance_metric: DistanceMetric::Cosine,
            index_type: IndexType::Hnsw,
        };
        let args = cmd.to_resp_args();
        assert_eq!(args[0], Bytes::from_static(b"FERRITE.VCREATE"));
        assert_eq!(args[1], Bytes::from("my_index"));
        assert_eq!(args[2], Bytes::from("128"));
        assert_eq!(args[3], Bytes::from_static(b"METRIC"));
        assert_eq!(args[4], Bytes::from("COSINE"));
        assert_eq!(args[5], Bytes::from_static(b"TYPE"));
        assert_eq!(args[6], Bytes::from("HNSW"));
    }

    #[test]
    fn test_semantic_set_to_resp() {
        let cmd = ExtendedCommand::SemanticSet {
            query: "what is rust?".to_string(),
            response: "A systems language".to_string(),
            ttl: Some(3600),
        };
        let args = cmd.to_resp_args();
        assert_eq!(args[0], Bytes::from_static(b"FERRITE.SEMSET"));
        assert!(args.contains(&Bytes::from_static(b"TTL")));
        assert!(args.contains(&Bytes::from("3600")));
    }

    #[test]
    fn test_semantic_set_no_ttl() {
        let cmd = ExtendedCommand::SemanticSet {
            query: "q".to_string(),
            response: "r".to_string(),
            ttl: None,
        };
        let args = cmd.to_resp_args();
        assert_eq!(args.len(), 3); // cmd + query + response
    }

    #[test]
    fn test_semantic_get_to_resp() {
        let cmd = ExtendedCommand::SemanticGet {
            query: "what is rust?".to_string(),
            threshold: 0.85,
        };
        let args = cmd.to_resp_args();
        assert_eq!(args[0], Bytes::from_static(b"FERRITE.SEMGET"));
        assert_eq!(args[1], Bytes::from("what is rust?"));
        assert_eq!(args[2], Bytes::from_static(b"THRESHOLD"));
        assert_eq!(args[3], Bytes::from("0.85"));
    }

    #[test]
    fn test_time_travel_to_resp() {
        let cmd = ExtendedCommand::TimeTravel {
            key: "mykey".to_string(),
            as_of: 1700000000000,
        };
        let args = cmd.to_resp_args();
        assert_eq!(args[0], Bytes::from_static(b"FERRITE.TIMETRAVEL"));
        assert_eq!(args[1], Bytes::from("mykey"));
        assert_eq!(args[2], Bytes::from("1700000000000"));
    }

    #[test]
    fn test_history_to_resp() {
        let cmd = ExtendedCommand::History {
            key: "mykey".to_string(),
            since: Some(1000),
            limit: Some(50),
        };
        let args = cmd.to_resp_args();
        assert_eq!(args[0], Bytes::from_static(b"FERRITE.HISTORY"));
        assert!(args.contains(&Bytes::from_static(b"SINCE")));
        assert!(args.contains(&Bytes::from("1000")));
        assert!(args.contains(&Bytes::from_static(b"LIMIT")));
        assert!(args.contains(&Bytes::from("50")));
    }

    #[test]
    fn test_history_minimal() {
        let cmd = ExtendedCommand::History {
            key: "k".to_string(),
            since: None,
            limit: None,
        };
        let args = cmd.to_resp_args();
        assert_eq!(args.len(), 2); // cmd + key
    }

    #[test]
    fn test_ferrite_query_to_resp() {
        let cmd = ExtendedCommand::FerriteQuery {
            query_string: "SELECT * FROM keys WHERE ttl > 0".to_string(),
        };
        let args = cmd.to_resp_args();
        assert_eq!(args[0], Bytes::from_static(b"FERRITE.QUERY"));
        assert_eq!(args[1], Bytes::from("SELECT * FROM keys WHERE ttl > 0"));
    }

    #[test]
    fn test_cdc_subscribe_to_resp() {
        let cmd = ExtendedCommand::CdcSubscribe {
            pattern: "user:*".to_string(),
            format: "json".to_string(),
        };
        let args = cmd.to_resp_args();
        assert_eq!(args[0], Bytes::from_static(b"FERRITE.CDCSUB"));
        assert_eq!(args[1], Bytes::from("user:*"));
        assert_eq!(args[2], Bytes::from_static(b"FORMAT"));
        assert_eq!(args[3], Bytes::from("json"));
    }

    #[test]
    fn test_crdt_incr_to_resp() {
        let cmd = ExtendedCommand::CrdtIncr {
            key: "counter".to_string(),
            delta: -5,
        };
        let args = cmd.to_resp_args();
        assert_eq!(args[0], Bytes::from_static(b"FERRITE.CRDTINCR"));
        assert_eq!(args[1], Bytes::from("counter"));
        assert_eq!(args[2], Bytes::from("-5"));
    }

    #[test]
    fn test_trigger_create_to_resp() {
        let cmd = ExtendedCommand::TriggerCreate {
            name: "audit_log".to_string(),
            event: "set".to_string(),
            pattern: "user:*".to_string(),
            action: "LOG".to_string(),
        };
        let args = cmd.to_resp_args();
        assert_eq!(args[0], Bytes::from_static(b"FERRITE.TRIGGER"));
        assert_eq!(args[1], Bytes::from("audit_log"));
        assert_eq!(args[2], Bytes::from_static(b"ON"));
        assert_eq!(args[3], Bytes::from("set"));
        assert_eq!(args[4], Bytes::from_static(b"MATCH"));
        assert_eq!(args[5], Bytes::from("user:*"));
        assert_eq!(args[6], Bytes::from_static(b"DO"));
        assert_eq!(args[7], Bytes::from("LOG"));
    }

    #[test]
    fn test_command_name() {
        let cmd = ExtendedCommand::VectorSearch {
            index: "i".to_string(),
            query_vector: vec![],
            top_k: 1,
            filter: None,
        };
        assert_eq!(cmd.command_name(), "FERRITE.VSEARCH");

        let cmd = ExtendedCommand::CrdtIncr {
            key: "k".to_string(),
            delta: 1,
        };
        assert_eq!(cmd.command_name(), "FERRITE.CRDTINCR");
    }

    #[test]
    fn test_response_from_resp_ok() {
        let tokens = vec![Bytes::from_static(b"OK")];
        let resp = ExtendedResponse::from_resp_tokens(&tokens).unwrap();
        assert_eq!(resp, ExtendedResponse::Ok);
    }

    #[test]
    fn test_response_from_resp_null() {
        let tokens = vec![Bytes::from_static(b"NULL")];
        let resp = ExtendedResponse::from_resp_tokens(&tokens).unwrap();
        assert_eq!(resp, ExtendedResponse::Null);
    }

    #[test]
    fn test_response_from_resp_value() {
        let tokens = vec![Bytes::from("VALUE"), Bytes::from("hello")];
        let resp = ExtendedResponse::from_resp_tokens(&tokens).unwrap();
        assert_eq!(resp, ExtendedResponse::Value("hello".to_string()));
    }

    #[test]
    fn test_response_from_resp_integer() {
        let tokens = vec![Bytes::from("INTEGER"), Bytes::from("42")];
        let resp = ExtendedResponse::from_resp_tokens(&tokens).unwrap();
        assert_eq!(resp, ExtendedResponse::Integer(42));
    }

    #[test]
    fn test_response_from_resp_float() {
        let tokens = vec![Bytes::from("FLOAT"), Bytes::from("2.72")];
        let resp = ExtendedResponse::from_resp_tokens(&tokens).unwrap();
        assert_eq!(resp, ExtendedResponse::Float(2.72));
    }

    #[test]
    fn test_response_from_resp_error() {
        let tokens = vec![Bytes::from("ERROR"), Bytes::from("something broke")];
        let resp = ExtendedResponse::from_resp_tokens(&tokens).unwrap();
        assert_eq!(resp, ExtendedResponse::Error("something broke".to_string()));
    }

    #[test]
    fn test_response_from_resp_subscription() {
        let tokens = vec![Bytes::from("SUBSCRIPTION"), Bytes::from("sub-123")];
        let resp = ExtendedResponse::from_resp_tokens(&tokens).unwrap();
        assert_eq!(
            resp,
            ExtendedResponse::SubscriptionId("sub-123".to_string())
        );
    }

    #[test]
    fn test_response_from_resp_empty() {
        let result = ExtendedResponse::from_resp_tokens(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_response_from_resp_unknown_tag() {
        let tokens = vec![Bytes::from("BOGUS")];
        let result = ExtendedResponse::from_resp_tokens(&tokens);
        assert!(result.is_err());
    }

    #[test]
    fn test_serde_roundtrip_command() {
        let cmd = ExtendedCommand::VectorCreate {
            name: "test".to_string(),
            dimensions: 256,
            distance_metric: DistanceMetric::Euclidean,
            index_type: IndexType::Ivf,
        };
        let json = serde_json::to_string(&cmd).unwrap();
        let deserialized: ExtendedCommand = serde_json::from_str(&json).unwrap();
        assert_eq!(cmd, deserialized);
    }

    #[test]
    fn test_serde_roundtrip_response() {
        let resp = ExtendedResponse::SearchResults(vec![SearchResult {
            id: "v1".to_string(),
            score: 0.95,
            metadata: {
                let mut m = HashMap::new();
                m.insert("tag".to_string(), "important".to_string());
                m
            },
        }]);
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: ExtendedResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(resp, deserialized);
    }

    #[test]
    fn test_serde_roundtrip_distance_metric() {
        for metric in &[
            DistanceMetric::Cosine,
            DistanceMetric::Euclidean,
            DistanceMetric::DotProduct,
            DistanceMetric::Manhattan,
        ] {
            let json = serde_json::to_string(metric).unwrap();
            let deserialized: DistanceMetric = serde_json::from_str(&json).unwrap();
            assert_eq!(*metric, deserialized);
        }
    }

    #[test]
    fn test_serde_roundtrip_index_type() {
        for it in &[IndexType::Hnsw, IndexType::Ivf, IndexType::Flat] {
            let json = serde_json::to_string(it).unwrap();
            let deserialized: IndexType = serde_json::from_str(&json).unwrap();
            assert_eq!(*it, deserialized);
        }
    }
}
