//! GraphQL API layer for Ferrite
//!
//! Provides a GraphQL schema, resolver, and execution engine for querying
//! Ferrite data through a GraphQL endpoint. Supports queries, mutations,
//! and subscriptions over all Ferrite data models (KV, vectors, documents,
//! graphs, time-series).
//!
//! # Architecture
//!
//! This module implements a lightweight, self-contained GraphQL executor
//! without requiring heavy external dependencies. The schema is defined
//! programmatically and can be integrated with any HTTP server.
//!
//! ```text
//! HTTP Request → GraphQL Parser → Resolver → Store Operations → JSON Response
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use ferrite_core::graphql::{GraphQLEngine, GraphQLRequest};
//!
//! let engine = GraphQLEngine::new(store);
//! let request = GraphQLRequest {
//!     query: r#"{ get(key: "hello") { value ttl } }"#.to_string(),
//!     variables: None,
//!     operation_name: None,
//! };
//! let response = engine.execute(request).await;
//! ```

pub mod resolver;
pub mod schema;

pub use resolver::{GraphQLEngine, GraphQLRequest, GraphQLResponse};
pub use schema::{FieldType, GraphQLSchema, SchemaField, SchemaType};

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the GraphQL API endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLConfig {
    /// Whether the GraphQL endpoint is enabled.
    pub enabled: bool,
    /// HTTP path for GraphQL queries (default: /graphql).
    pub path: String,
    /// Enable the GraphiQL interactive IDE.
    pub enable_graphiql: bool,
    /// Maximum query depth to prevent abuse.
    pub max_depth: usize,
    /// Maximum query complexity score.
    pub max_complexity: usize,
    /// Enable introspection queries (__schema, __type).
    pub enable_introspection: bool,
}

impl Default for GraphQLConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: "/graphql".to_string(),
            enable_graphiql: true,
            max_depth: 10,
            max_complexity: 1000,
            enable_introspection: true,
        }
    }
}

/// Errors from the GraphQL layer.
#[derive(Debug, thiserror::Error)]
pub enum GraphQLError {
    /// The query could not be parsed.
    #[error("parse error: {0}")]
    ParseError(String),

    /// A field could not be resolved.
    #[error("resolver error: {0}")]
    ResolverError(String),

    /// The query exceeded depth/complexity limits.
    #[error("query too complex: {0}")]
    ComplexityExceeded(String),

    /// An internal error occurred.
    #[error("internal error: {0}")]
    Internal(String),
}
