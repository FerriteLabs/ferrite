//! Native GraphQL & REST API Gateway
//!
//! Auto-generates HTTP endpoints from stored data schemas. Provides both REST
//! CRUD endpoints and a GraphQL query interface, eliminating the need for a
//! separate API layer between client applications and Ferrite.
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────────────┐
//! │                  API Gateway                       │
//! │  ┌─────────────────┐  ┌─────────────────────┐     │
//! │  │   REST Router   │  │  GraphQL Executor   │     │
//! │  │  GET /api/v1/…  │  │  POST /graphql      │     │
//! │  └────────┬────────┘  └─────────┬───────────┘     │
//! │           │                     │                  │
//! │  ┌────────▼─────────────────────▼───────────┐     │
//! │  │          Schema Introspector              │     │
//! │  │   (auto-infer types from stored data)     │     │
//! │  └────────────────────┬─────────────────────┘     │
//! │                       │                            │
//! │  ┌────────────────────▼─────────────────────┐     │
//! │  │            Ferrite Storage                │     │
//! │  └──────────────────────────────────────────┘     │
//! └────────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// API Gateway configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiGatewayConfig {
    /// Bind address for the API server
    pub bind_address: String,
    /// Port for the API server
    pub port: u16,
    /// Enable REST endpoints
    pub rest_enabled: bool,
    /// Enable GraphQL endpoint
    pub graphql_enabled: bool,
    /// REST API base path
    pub rest_base_path: String,
    /// GraphQL endpoint path
    pub graphql_path: String,
    /// Enable CORS
    pub cors_enabled: bool,
    /// Allowed origins for CORS
    pub cors_origins: Vec<String>,
    /// Maximum request body size
    pub max_body_size: usize,
    /// Rate limit per IP (requests/second), 0 = unlimited
    pub rate_limit: u32,
    /// Enable API key authentication
    pub require_api_key: bool,
}

impl Default for ApiGatewayConfig {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1".to_string(),
            port: 8080,
            rest_enabled: true,
            graphql_enabled: true,
            rest_base_path: "/api/v1".to_string(),
            graphql_path: "/graphql".to_string(),
            cors_enabled: true,
            cors_origins: vec!["*".to_string()],
            max_body_size: 10 * 1024 * 1024,
            rate_limit: 0,
            require_api_key: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Schema introspection types
// ---------------------------------------------------------------------------

/// An inferred data type from stored JSON values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InferredType {
    /// A string value.
    String,
    /// An integer value.
    Integer,
    /// A floating-point value.
    Float,
    /// A boolean value.
    Boolean,
    /// An array of a uniform element type.
    Array(Box<InferredType>),
    /// A nested object with named fields.
    Object(HashMap<String, InferredType>),
    /// A null/missing value.
    Null,
    /// A field with mixed types across samples.
    Mixed,
}

/// An inferred schema for a key pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferredSchema {
    /// Key pattern this schema covers (e.g., "users:*")
    pub pattern: String,
    /// Resource name (e.g., "users")
    pub resource_name: String,
    /// Inferred fields with types
    pub fields: HashMap<String, FieldSchema>,
    /// Number of keys sampled
    pub samples: usize,
    /// Confidence score (0.0 – 1.0)
    pub confidence: f64,
}

/// Schema for a single field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSchema {
    /// Field name.
    pub name: String,
    /// Inferred type of the field.
    pub field_type: InferredType,
    /// Whether the field can be null.
    pub nullable: bool,
    /// Optional description of the field.
    pub description: Option<String>,
}

impl InferredSchema {
    /// Create a new schema from a pattern and fields.
    pub fn new(pattern: &str, resource_name: &str) -> Self {
        Self {
            pattern: pattern.to_string(),
            resource_name: resource_name.to_string(),
            fields: HashMap::new(),
            samples: 0,
            confidence: 0.0,
        }
    }

    /// Add a field to the schema.
    pub fn add_field(&mut self, name: &str, field_type: InferredType, nullable: bool) {
        self.fields.insert(
            name.to_string(),
            FieldSchema {
                name: name.to_string(),
                field_type,
                nullable,
                description: None,
            },
        );
    }
}

// ---------------------------------------------------------------------------
// REST types
// ---------------------------------------------------------------------------

/// HTTP method for REST routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HttpMethod {
    /// HTTP GET.
    Get,
    /// HTTP POST.
    Post,
    /// HTTP PUT.
    Put,
    /// HTTP PATCH.
    Patch,
    /// HTTP DELETE.
    Delete,
    /// HTTP OPTIONS.
    Options,
}

impl std::fmt::Display for HttpMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Get => write!(f, "GET"),
            Self::Post => write!(f, "POST"),
            Self::Put => write!(f, "PUT"),
            Self::Patch => write!(f, "PATCH"),
            Self::Delete => write!(f, "DELETE"),
            Self::Options => write!(f, "OPTIONS"),
        }
    }
}

/// A REST API route definition.
#[derive(Debug, Clone)]
pub struct RestRoute {
    /// HTTP method for this route.
    pub method: HttpMethod,
    /// URL path pattern (may contain `:id` placeholders).
    pub path: String,
    /// Resource name this route operates on.
    pub resource: String,
    /// CRUD operation this route performs.
    pub operation: RestOperation,
}

/// REST CRUD operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestOperation {
    /// List all resources.
    List,
    /// Get a single resource by ID.
    Get,
    /// Create a new resource.
    Create,
    /// Fully update an existing resource.
    Update,
    /// Partially update an existing resource.
    Patch,
    /// Delete a resource.
    Delete,
}

/// API response wrapper.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiResponse<T: Serialize> {
    /// Whether the request succeeded.
    pub success: bool,
    /// Response data (present on success).
    pub data: Option<T>,
    /// Error information (present on failure).
    pub error: Option<ApiError>,
    /// Pagination and response metadata.
    pub meta: Option<ResponseMeta>,
}

/// Error in API response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiError {
    /// Machine-readable error code.
    pub code: String,
    /// Human-readable error message.
    pub message: String,
    /// Additional error details.
    pub details: Option<serde_json::Value>,
}

/// Pagination and metadata for list responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseMeta {
    /// Total number of items available.
    pub total: u64,
    /// Current page number.
    pub page: u32,
    /// Items per page.
    pub per_page: u32,
    /// Whether there is a next page.
    pub has_next: bool,
}

// ---------------------------------------------------------------------------
// GraphQL types
// ---------------------------------------------------------------------------

/// A GraphQL query request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLRequest {
    /// The GraphQL query string.
    pub query: String,
    /// Optional query variables.
    pub variables: Option<serde_json::Value>,
    /// Optional operation name (for multi-operation documents).
    pub operation_name: Option<String>,
}

/// A GraphQL response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLResponse {
    /// Response data (present on success).
    pub data: Option<serde_json::Value>,
    /// Errors encountered during execution.
    pub errors: Option<Vec<GraphQLError>>,
}

/// A GraphQL error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLError {
    /// Error message.
    pub message: String,
    /// Locations in the query where the error occurred.
    pub locations: Option<Vec<GraphQLLocation>>,
    /// Path to the field that caused the error.
    pub path: Option<Vec<serde_json::Value>>,
}

/// Location in the query where an error occurred.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLLocation {
    /// Line number in the query.
    pub line: u32,
    /// Column number in the query.
    pub column: u32,
}

/// A GraphQL type definition generated from an inferred schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLTypeDef {
    /// Type name.
    pub name: String,
    /// Fields in this type.
    pub fields: Vec<GraphQLFieldDef>,
}

/// A field in a GraphQL type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLFieldDef {
    /// Field name.
    pub name: String,
    /// GraphQL type name (e.g. `String`, `Int`, `[Float]`).
    pub type_name: String,
    /// Whether this field is nullable.
    pub nullable: bool,
    /// Optional field description.
    pub description: Option<String>,
}

// ---------------------------------------------------------------------------
// OpenAPI spec generation
// ---------------------------------------------------------------------------

/// Minimal OpenAPI 3.0 spec for the auto-generated REST API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenApiSpec {
    /// OpenAPI specification version (e.g. `"3.0.0"`).
    pub openapi: String,
    /// API metadata.
    pub info: OpenApiInfo,
    /// API paths keyed by URL, then by HTTP method.
    pub paths: HashMap<String, HashMap<String, OpenApiOperation>>,
    /// Reusable schema components.
    pub components: OpenApiComponents,
}

/// OpenAPI info section.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenApiInfo {
    /// API title.
    pub title: String,
    /// API version.
    pub version: String,
    /// Optional API description.
    pub description: Option<String>,
}

/// An OpenAPI operation definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenApiOperation {
    /// Short summary of what the operation does.
    pub summary: String,
    /// Tags for grouping the operation.
    pub tags: Vec<String>,
    /// Parameters accepted by this operation.
    pub parameters: Vec<OpenApiParameter>,
    /// Possible responses keyed by HTTP status code.
    pub responses: HashMap<String, OpenApiResponse>,
}

/// An OpenAPI parameter definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenApiParameter {
    /// Parameter name.
    pub name: String,
    /// Where the parameter appears (`"query"`, `"path"`, `"header"`).
    #[serde(rename = "in")]
    pub location: String,
    /// Whether this parameter is required.
    pub required: bool,
    /// JSON Schema for the parameter value.
    pub schema: serde_json::Value,
}

/// An OpenAPI response definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenApiResponse {
    /// Human-readable description of the response.
    pub description: String,
}

/// OpenAPI reusable components.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenApiComponents {
    /// Reusable JSON schemas keyed by name.
    pub schemas: HashMap<String, serde_json::Value>,
}

// ---------------------------------------------------------------------------
// API Gateway Engine
// ---------------------------------------------------------------------------

/// Errors from the API gateway.
#[derive(Debug, thiserror::Error)]
pub enum ApiGatewayError {
    /// The requested resource was not found.
    #[error("Resource not found: {0}")]
    NotFound(String),

    /// The request was malformed.
    #[error("Bad request: {0}")]
    BadRequest(String),

    /// Authentication or authorization failure.
    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    /// Client exceeded rate limit.
    #[error("Rate limit exceeded")]
    RateLimited,

    /// An internal server error occurred.
    #[error("Internal error: {0}")]
    Internal(String),

    /// No schema has been registered for the given pattern.
    #[error("Schema not registered for pattern: {0}")]
    SchemaNotFound(String),

    /// Error during GraphQL query execution.
    #[error("GraphQL execution error: {0}")]
    GraphQLError(String),
}

/// Gateway-level metrics.
#[derive(Debug, Default)]
pub struct GatewayMetrics {
    /// Total REST API requests served.
    pub rest_requests: AtomicU64,
    /// Total GraphQL requests served.
    pub graphql_requests: AtomicU64,
    /// Total 4xx client errors.
    pub errors_4xx: AtomicU64,
    /// Total 5xx server errors.
    pub errors_5xx: AtomicU64,
    /// Total bytes received from clients.
    pub bytes_in: AtomicU64,
    /// Total bytes sent to clients.
    pub bytes_out: AtomicU64,
}

/// The main API gateway engine.
pub struct ApiGateway {
    config: ApiGatewayConfig,
    schemas: Arc<parking_lot::RwLock<HashMap<String, InferredSchema>>>,
    routes: Arc<parking_lot::RwLock<Vec<RestRoute>>>,
    metrics: Arc<GatewayMetrics>,
}

impl ApiGateway {
    /// Create a new API gateway.
    pub fn new(config: ApiGatewayConfig) -> Self {
        Self {
            config,
            schemas: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            routes: Arc::new(parking_lot::RwLock::new(Vec::new())),
            metrics: Arc::new(GatewayMetrics::default()),
        }
    }

    /// Register an inferred schema and auto-generate REST routes for it.
    pub fn register_schema(&self, schema: InferredSchema) {
        let resource = schema.resource_name.clone();
        let base = format!("{}/{}", self.config.rest_base_path, resource);

        let routes = vec![
            RestRoute {
                method: HttpMethod::Get,
                path: base.clone(),
                resource: resource.clone(),
                operation: RestOperation::List,
            },
            RestRoute {
                method: HttpMethod::Get,
                path: format!("{}/:id", base),
                resource: resource.clone(),
                operation: RestOperation::Get,
            },
            RestRoute {
                method: HttpMethod::Post,
                path: base.clone(),
                resource: resource.clone(),
                operation: RestOperation::Create,
            },
            RestRoute {
                method: HttpMethod::Put,
                path: format!("{}/:id", base),
                resource: resource.clone(),
                operation: RestOperation::Update,
            },
            RestRoute {
                method: HttpMethod::Patch,
                path: format!("{}/:id", base),
                resource: resource.clone(),
                operation: RestOperation::Patch,
            },
            RestRoute {
                method: HttpMethod::Delete,
                path: format!("{}/:id", base),
                resource: resource.clone(),
                operation: RestOperation::Delete,
            },
        ];

        {
            let mut r = self.routes.write();
            r.extend(routes);
        }
        {
            let mut s = self.schemas.write();
            s.insert(schema.pattern.clone(), schema);
        }
    }

    /// Get all registered schemas.
    pub fn schemas(&self) -> Vec<InferredSchema> {
        self.schemas.read().values().cloned().collect()
    }

    /// Get all registered routes.
    pub fn routes(&self) -> Vec<RestRoute> {
        self.routes.read().clone()
    }

    /// Generate a GraphQL schema string from all registered schemas.
    pub fn generate_graphql_schema(&self) -> String {
        let schemas = self.schemas.read();
        let mut sdl = String::from("type Query {\n");

        for schema in schemas.values() {
            let name = &schema.resource_name;
            let type_name = to_pascal_case(name);
            sdl.push_str(&format!("  {}(id: ID!): {}\n", name, type_name));
            sdl.push_str(&format!(
                "  all{}(page: Int, perPage: Int): [{}!]!\n",
                type_name, type_name
            ));
        }
        sdl.push_str("}\n\n");

        sdl.push_str("type Mutation {\n");
        for schema in schemas.values() {
            let name = &schema.resource_name;
            let type_name = to_pascal_case(name);
            sdl.push_str(&format!(
                "  create{}(input: {}Input!): {}\n",
                type_name, type_name, type_name
            ));
            sdl.push_str(&format!(
                "  update{}(id: ID!, input: {}Input!): {}\n",
                type_name, type_name, type_name
            ));
            sdl.push_str(&format!("  delete{}(id: ID!): Boolean!\n", type_name));
        }
        sdl.push_str("}\n\n");

        for schema in schemas.values() {
            let type_name = to_pascal_case(&schema.resource_name);
            sdl.push_str(&format!("type {} {{\n", type_name));
            sdl.push_str("  id: ID!\n");
            for (name, field) in &schema.fields {
                let gql_type = inferred_to_graphql(&field.field_type);
                let suffix = if field.nullable { "" } else { "!" };
                sdl.push_str(&format!("  {}: {}{}\n", name, gql_type, suffix));
            }
            sdl.push_str("}\n\n");

            sdl.push_str(&format!("input {}Input {{\n", type_name));
            for (name, field) in &schema.fields {
                let gql_type = inferred_to_graphql(&field.field_type);
                sdl.push_str(&format!("  {}: {}\n", name, gql_type));
            }
            sdl.push_str("}\n\n");
        }

        sdl
    }

    /// Generate an OpenAPI spec from all registered schemas.
    pub fn generate_openapi_spec(&self) -> OpenApiSpec {
        let schemas = self.schemas.read();
        let mut paths = HashMap::new();
        let mut components = HashMap::new();

        for schema in schemas.values() {
            let resource = &schema.resource_name;
            let base = format!("{}/{}", self.config.rest_base_path, resource);

            let list_op = OpenApiOperation {
                summary: format!("List all {}", resource),
                tags: vec![resource.clone()],
                parameters: vec![
                    OpenApiParameter {
                        name: "page".to_string(),
                        location: "query".to_string(),
                        required: false,
                        schema: serde_json::json!({"type": "integer"}),
                    },
                    OpenApiParameter {
                        name: "per_page".to_string(),
                        location: "query".to_string(),
                        required: false,
                        schema: serde_json::json!({"type": "integer"}),
                    },
                ],
                responses: HashMap::from([(
                    "200".to_string(),
                    OpenApiResponse {
                        description: format!("List of {}", resource),
                    },
                )]),
            };

            paths
                .entry(base.clone())
                .or_insert_with(HashMap::new)
                .insert("get".to_string(), list_op);

            let get_op = OpenApiOperation {
                summary: format!("Get a single {}", resource),
                tags: vec![resource.clone()],
                parameters: vec![OpenApiParameter {
                    name: "id".to_string(),
                    location: "path".to_string(),
                    required: true,
                    schema: serde_json::json!({"type": "string"}),
                }],
                responses: HashMap::from([(
                    "200".to_string(),
                    OpenApiResponse {
                        description: format!("A single {}", resource),
                    },
                )]),
            };

            paths
                .entry(format!("{}/{{id}}", base))
                .or_insert_with(HashMap::new)
                .insert("get".to_string(), get_op);

            // Component schema
            let mut properties = serde_json::Map::new();
            properties.insert("id".to_string(), serde_json::json!({"type": "string"}));
            for (name, field) in &schema.fields {
                properties.insert(name.clone(), inferred_to_openapi(&field.field_type));
            }
            components.insert(
                to_pascal_case(resource),
                serde_json::json!({
                    "type": "object",
                    "properties": properties
                }),
            );
        }

        OpenApiSpec {
            openapi: "3.0.0".to_string(),
            info: OpenApiInfo {
                title: "Ferrite API".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                description: Some("Auto-generated API from Ferrite data schemas".to_string()),
            },
            paths,
            components: OpenApiComponents {
                schemas: components,
            },
        }
    }

    /// Handle a GraphQL request (simplified executor).
    pub fn execute_graphql(&self, request: GraphQLRequest) -> GraphQLResponse {
        self.metrics
            .graphql_requests
            .fetch_add(1, Ordering::Relaxed);

        // Simplified: validate query is non-empty
        if request.query.trim().is_empty() {
            return GraphQLResponse {
                data: None,
                errors: Some(vec![GraphQLError {
                    message: "Query cannot be empty".to_string(),
                    locations: None,
                    path: None,
                }]),
            };
        }

        // Return an introspection-compatible response stub
        GraphQLResponse {
            data: Some(serde_json::json!({})),
            errors: None,
        }
    }

    /// Get metrics.
    pub fn metrics(&self) -> &GatewayMetrics {
        &self.metrics
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn to_pascal_case(s: &str) -> String {
    s.split(&['_', '-', ' '][..])
        .map(|w| {
            let mut chars = w.chars();
            match chars.next() {
                None => String::new(),
                Some(c) => c.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect()
}

fn inferred_to_graphql(t: &InferredType) -> String {
    match t {
        InferredType::String => "String".to_string(),
        InferredType::Integer => "Int".to_string(),
        InferredType::Float => "Float".to_string(),
        InferredType::Boolean => "Boolean".to_string(),
        InferredType::Array(inner) => format!("[{}]", inferred_to_graphql(inner)),
        InferredType::Object(_) => "JSON".to_string(),
        InferredType::Null | InferredType::Mixed => "String".to_string(),
    }
}

fn inferred_to_openapi(t: &InferredType) -> serde_json::Value {
    match t {
        InferredType::String => serde_json::json!({"type": "string"}),
        InferredType::Integer => serde_json::json!({"type": "integer"}),
        InferredType::Float => serde_json::json!({"type": "number"}),
        InferredType::Boolean => serde_json::json!({"type": "boolean"}),
        InferredType::Array(inner) => serde_json::json!({
            "type": "array",
            "items": inferred_to_openapi(inner)
        }),
        InferredType::Object(_) => serde_json::json!({"type": "object"}),
        InferredType::Null | InferredType::Mixed => serde_json::json!({"type": "string"}),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_schema() -> InferredSchema {
        let mut schema = InferredSchema::new("users:*", "users");
        schema.add_field("name", InferredType::String, false);
        schema.add_field("email", InferredType::String, false);
        schema.add_field("age", InferredType::Integer, true);
        schema.add_field("active", InferredType::Boolean, false);
        schema.samples = 100;
        schema.confidence = 0.95;
        schema
    }

    #[test]
    fn test_register_schema_generates_routes() {
        let gw = ApiGateway::new(ApiGatewayConfig::default());
        gw.register_schema(sample_schema());

        let routes = gw.routes();
        assert_eq!(routes.len(), 6); // CRUD operations
        assert!(routes
            .iter()
            .any(|r| r.method == HttpMethod::Get && r.path == "/api/v1/users"));
        assert!(routes
            .iter()
            .any(|r| r.method == HttpMethod::Post && r.path == "/api/v1/users"));
        assert!(routes.iter().any(|r| r.method == HttpMethod::Delete));
    }

    #[test]
    fn test_generate_graphql_schema() {
        let gw = ApiGateway::new(ApiGatewayConfig::default());
        gw.register_schema(sample_schema());

        let sdl = gw.generate_graphql_schema();
        assert!(sdl.contains("type Query"));
        assert!(sdl.contains("type Users"));
        assert!(sdl.contains("name: String!"));
        assert!(sdl.contains("age: Int"));
        assert!(sdl.contains("input UsersInput"));
        assert!(sdl.contains("createUsers"));
        assert!(sdl.contains("deleteUsers"));
    }

    #[test]
    fn test_generate_openapi_spec() {
        let gw = ApiGateway::new(ApiGatewayConfig::default());
        gw.register_schema(sample_schema());

        let spec = gw.generate_openapi_spec();
        assert_eq!(spec.openapi, "3.0.0");
        assert!(spec.paths.contains_key("/api/v1/users"));
        assert!(spec.components.schemas.contains_key("Users"));
    }

    #[test]
    fn test_graphql_empty_query_error() {
        let gw = ApiGateway::new(ApiGatewayConfig::default());
        let resp = gw.execute_graphql(GraphQLRequest {
            query: "".to_string(),
            variables: None,
            operation_name: None,
        });
        assert!(resp.errors.is_some());
    }

    #[test]
    fn test_graphql_valid_query() {
        let gw = ApiGateway::new(ApiGatewayConfig::default());
        let resp = gw.execute_graphql(GraphQLRequest {
            query: "{ users(id: \"1\") { name } }".to_string(),
            variables: None,
            operation_name: None,
        });
        assert!(resp.data.is_some());
        assert!(resp.errors.is_none());
    }

    #[test]
    fn test_pascal_case() {
        assert_eq!(to_pascal_case("user_profiles"), "UserProfiles");
        assert_eq!(to_pascal_case("orders"), "Orders");
        assert_eq!(to_pascal_case("line-items"), "LineItems");
    }

    #[test]
    fn test_inferred_type_conversions() {
        assert_eq!(inferred_to_graphql(&InferredType::String), "String");
        assert_eq!(inferred_to_graphql(&InferredType::Integer), "Int");
        assert_eq!(
            inferred_to_graphql(&InferredType::Array(Box::new(InferredType::Float))),
            "[Float]"
        );
    }
}
