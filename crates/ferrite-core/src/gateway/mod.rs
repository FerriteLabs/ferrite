#![forbid(unsafe_code)]
//! Unified Query Gateway — GraphQL, gRPC, and REST access to Ferrite
//!
//! Defines endpoint specifications and routing for multi-protocol access.

pub mod server;

/// Gateway protocol type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GatewayProtocol {
    /// GraphQL protocol
    GraphQL,
    /// gRPC protocol
    Grpc,
    /// REST protocol
    Rest,
}

/// Gateway endpoint configuration
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// Whether the gateway is enabled
    pub enabled: bool,
    /// Address to bind on
    pub bind_address: String,
    /// Port number
    pub port: u16,
    /// Enabled protocols
    pub protocols: Vec<GatewayProtocol>,
    /// Whether CORS is enabled
    pub cors_enabled: bool,
    /// Allowed CORS origins
    pub cors_origins: Vec<String>,
    /// Whether authentication is required
    pub auth_required: bool,
    /// Rate limit in requests per second (None = unlimited)
    pub rate_limit_rps: Option<u32>,
    /// Maximum request body size in bytes
    pub max_request_size_bytes: u64,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_address: "127.0.0.1".to_string(),
            port: 8080,
            protocols: vec![GatewayProtocol::Rest, GatewayProtocol::GraphQL],
            cors_enabled: true,
            cors_origins: vec!["*".to_string()],
            auth_required: false,
            rate_limit_rps: Some(1000),
            max_request_size_bytes: 10 * 1024 * 1024,
        }
    }
}

/// REST endpoint definition
#[derive(Debug, Clone)]
pub struct RestEndpoint {
    /// HTTP method
    pub method: HttpMethod,
    /// URL path pattern
    pub path: String,
    /// Human-readable description
    pub description: String,
    /// JSON schema for request body (if applicable)
    pub request_schema: Option<String>,
    /// JSON schema for response body
    pub response_schema: Option<String>,
}

/// HTTP method
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpMethod {
    /// GET
    Get,
    /// POST
    Post,
    /// PUT
    Put,
    /// DELETE
    Delete,
    /// PATCH
    Patch,
}

/// GraphQL type mapping from Ferrite data types
#[derive(Debug, Clone)]
pub struct GraphQLSchema {
    /// Object type definitions
    pub types: Vec<GraphQLType>,
    /// Root query fields
    pub queries: Vec<GraphQLField>,
    /// Root mutation fields
    pub mutations: Vec<GraphQLField>,
    /// Subscription fields
    pub subscriptions: Vec<GraphQLField>,
}

/// A GraphQL object type
#[derive(Debug, Clone)]
pub struct GraphQLType {
    /// Type name
    pub name: String,
    /// Fields on this type
    pub fields: Vec<GraphQLField>,
}

/// A single GraphQL field
#[derive(Debug, Clone)]
pub struct GraphQLField {
    /// Field name
    pub name: String,
    /// GraphQL type string (e.g. `String!`, `[Int]`)
    pub field_type: String,
    /// Arguments as `(name, type)` pairs
    pub args: Vec<(String, String)>,
    /// Human-readable description
    pub description: String,
}

/// Generate REST API specification from Ferrite's data model.
pub fn generate_rest_endpoints() -> Vec<RestEndpoint> {
    vec![
        RestEndpoint {
            method: HttpMethod::Get,
            path: "/api/v1/keys/{key}".into(),
            description: "Get a key's value".into(),
            request_schema: None,
            response_schema: Some(r#"{"value": "string", "ttl": "integer"}"#.into()),
        },
        RestEndpoint {
            method: HttpMethod::Put,
            path: "/api/v1/keys/{key}".into(),
            description: "Set a key's value".into(),
            request_schema: Some(r#"{"value": "string", "ttl": "integer?"}"#.into()),
            response_schema: Some(r#"{"ok": true}"#.into()),
        },
        RestEndpoint {
            method: HttpMethod::Delete,
            path: "/api/v1/keys/{key}".into(),
            description: "Delete a key".into(),
            request_schema: None,
            response_schema: Some(r#"{"deleted": "integer"}"#.into()),
        },
        RestEndpoint {
            method: HttpMethod::Get,
            path: "/api/v1/keys".into(),
            description: "List keys matching pattern".into(),
            request_schema: None,
            response_schema: Some(r#"{"keys": ["string"]}"#.into()),
        },
        RestEndpoint {
            method: HttpMethod::Post,
            path: "/api/v1/query".into(),
            description: "Execute a FerriteQL query".into(),
            request_schema: Some(r#"{"query": "string"}"#.into()),
            response_schema: Some(r#"{"results": [...]}"#.into()),
        },
        RestEndpoint {
            method: HttpMethod::Post,
            path: "/api/v1/vectors/search".into(),
            description: "Search vectors by similarity".into(),
            request_schema: Some(
                r#"{"index": "string", "vector": [float], "top_k": "integer"}"#.into(),
            ),
            response_schema: Some(r#"{"results": [{"id": "string", "score": "float"}]}"#.into()),
        },
    ]
}

/// Generate GraphQL schema from Ferrite's capabilities.
pub fn generate_graphql_schema() -> GraphQLSchema {
    GraphQLSchema {
        types: vec![GraphQLType {
            name: "KeyValue".into(),
            fields: vec![
                GraphQLField {
                    name: "key".into(),
                    field_type: "String!".into(),
                    args: vec![],
                    description: "Key name".into(),
                },
                GraphQLField {
                    name: "value".into(),
                    field_type: "String".into(),
                    args: vec![],
                    description: "Value".into(),
                },
                GraphQLField {
                    name: "ttl".into(),
                    field_type: "Int".into(),
                    args: vec![],
                    description: "Time to live in seconds".into(),
                },
                GraphQLField {
                    name: "type".into(),
                    field_type: "String!".into(),
                    args: vec![],
                    description: "Data type".into(),
                },
            ],
        }],
        queries: vec![
            GraphQLField {
                name: "get".into(),
                field_type: "KeyValue".into(),
                args: vec![("key".into(), "String!".into())],
                description: "Get a key's value".into(),
            },
            GraphQLField {
                name: "keys".into(),
                field_type: "[String!]!".into(),
                args: vec![("pattern".into(), "String".into())],
                description: "List keys".into(),
            },
        ],
        mutations: vec![
            GraphQLField {
                name: "set".into(),
                field_type: "Boolean!".into(),
                args: vec![
                    ("key".into(), "String!".into()),
                    ("value".into(), "String!".into()),
                    ("ttl".into(), "Int".into()),
                ],
                description: "Set a key".into(),
            },
            GraphQLField {
                name: "del".into(),
                field_type: "Int!".into(),
                args: vec![("keys".into(), "[String!]!".into())],
                description: "Delete keys".into(),
            },
        ],
        subscriptions: vec![GraphQLField {
            name: "keyChanges".into(),
            field_type: "KeyValue!".into(),
            args: vec![("pattern".into(), "String".into())],
            description: "Subscribe to key changes".into(),
        }],
    }
}

/// Gateway runtime statistics
#[derive(Debug, Clone, Default)]
pub struct GatewayStats {
    /// Total requests processed
    pub total_requests: u64,
    /// GraphQL requests
    pub graphql_requests: u64,
    /// gRPC requests
    pub grpc_requests: u64,
    /// REST requests
    pub rest_requests: u64,
    /// Total error count
    pub errors: u64,
    /// Average latency in milliseconds
    pub avg_latency_ms: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = GatewayConfig::default();
        assert_eq!(config.port, 8080);
        assert!(!config.enabled);
    }

    #[test]
    fn test_rest_endpoints() {
        let endpoints = generate_rest_endpoints();
        assert!(endpoints.len() >= 4);
    }

    #[test]
    fn test_graphql_schema() {
        let schema = generate_graphql_schema();
        assert!(!schema.types.is_empty());
        assert!(!schema.queries.is_empty());
        assert!(!schema.mutations.is_empty());
    }
}
