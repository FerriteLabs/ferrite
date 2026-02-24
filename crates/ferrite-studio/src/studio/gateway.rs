//! REST & GraphQL API Gateway for Ferrite Studio
//!
//! Provides a unified HTTP gateway that routes incoming requests to either
//! REST or GraphQL handlers. The gateway supports:
//!
//! - RESTful CRUD operations on keys, queries, metrics, and server info
//! - GraphQL queries, mutations, and subscription schemas
//! - OpenAPI 3.0 specification generation
//! - GraphQL schema introspection
//! - CORS header management
//! - Rate limiting and authentication flags
//! - Request statistics tracking
//!
//! # Example
//!
//! ```rust
//! use ferrite_studio::studio::gateway::{ApiGateway, GatewayConfig};
//!
//! let config = GatewayConfig::default();
//! let gateway = ApiGateway::new(config);
//! let stats = gateway.get_stats();
//! assert_eq!(stats.total_requests, 0);
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/// Errors that can occur within the API gateway.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GatewayError {
    /// The requested resource was not found.
    NotFound(String),
    /// The request was malformed or invalid.
    BadRequest(String),
    /// Authentication is required but was not provided.
    Unauthorized,
    /// The client has exceeded the rate limit.
    RateLimited,
    /// The GraphQL query exceeds the maximum allowed depth.
    QueryTooComplex,
    /// An unexpected internal error occurred.
    InternalError(String),
}

impl fmt::Display for GatewayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GatewayError::NotFound(msg) => write!(f, "not found: {msg}"),
            GatewayError::BadRequest(msg) => write!(f, "bad request: {msg}"),
            GatewayError::Unauthorized => write!(f, "unauthorized"),
            GatewayError::RateLimited => write!(f, "rate limited"),
            GatewayError::QueryTooComplex => write!(f, "query too complex"),
            GatewayError::InternalError(msg) => write!(f, "internal error: {msg}"),
        }
    }
}

impl std::error::Error for GatewayError {}

impl GatewayError {
    /// Map a gateway error to an HTTP status code.
    pub fn status_code(&self) -> u16 {
        match self {
            GatewayError::NotFound(_) => 404,
            GatewayError::BadRequest(_) => 400,
            GatewayError::Unauthorized => 401,
            GatewayError::RateLimited => 429,
            GatewayError::QueryTooComplex => 422,
            GatewayError::InternalError(_) => 500,
        }
    }
}

// ---------------------------------------------------------------------------
// HTTP method enum
// ---------------------------------------------------------------------------

/// Supported HTTP methods for gateway requests.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HttpMethod {
    /// HTTP GET
    Get,
    /// HTTP POST
    Post,
    /// HTTP PUT
    Put,
    /// HTTP DELETE
    Delete,
    /// HTTP PATCH
    Patch,
    /// HTTP OPTIONS (used for CORS pre-flight)
    Options,
}

impl fmt::Display for HttpMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HttpMethod::Get => write!(f, "GET"),
            HttpMethod::Post => write!(f, "POST"),
            HttpMethod::Put => write!(f, "PUT"),
            HttpMethod::Delete => write!(f, "DELETE"),
            HttpMethod::Patch => write!(f, "PATCH"),
            HttpMethod::Options => write!(f, "OPTIONS"),
        }
    }
}

impl HttpMethod {
    /// Parse an HTTP method string (case-insensitive).
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "GET" => Some(HttpMethod::Get),
            "POST" => Some(HttpMethod::Post),
            "PUT" => Some(HttpMethod::Put),
            "DELETE" => Some(HttpMethod::Delete),
            "PATCH" => Some(HttpMethod::Patch),
            "OPTIONS" => Some(HttpMethod::Options),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the API gateway.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayConfig {
    /// Enable REST API endpoints.
    pub enable_rest: bool,
    /// Enable GraphQL endpoint.
    pub enable_graphql: bool,
    /// Base path prefix for all routes (e.g. `"/api/v1"`).
    pub base_path: String,
    /// Allowed CORS origins. An empty vec disables CORS headers.
    pub cors_origins: Vec<String>,
    /// Maximum number of requests per second (0 = unlimited).
    pub rate_limit_per_second: u32,
    /// Whether authentication is required for all requests.
    pub auth_required: bool,
    /// Maximum allowed depth for GraphQL queries.
    pub max_query_depth: u32,
    /// Whether GraphQL introspection is enabled.
    pub introspection_enabled: bool,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            enable_rest: true,
            enable_graphql: true,
            base_path: "/api/v1".to_string(),
            cors_origins: vec![],
            rate_limit_per_second: 0,
            auth_required: false,
            max_query_depth: 10,
            introspection_enabled: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

/// An incoming gateway request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayRequest {
    /// HTTP method.
    pub method: HttpMethod,
    /// Request path (e.g. `"/api/v1/keys/mykey"`).
    pub path: String,
    /// HTTP headers.
    pub headers: HashMap<String, String>,
    /// Optional request body.
    pub body: Option<String>,
    /// Query string parameters.
    pub query_params: HashMap<String, String>,
}

/// The gateway's HTTP response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayResponse {
    /// HTTP status code.
    pub status_code: u16,
    /// Response headers.
    pub headers: HashMap<String, String>,
    /// Response body.
    pub body: String,
    /// Content-Type header value.
    pub content_type: String,
}

impl GatewayResponse {
    /// Build a JSON success response.
    fn json(status_code: u16, body: &serde_json::Value) -> Self {
        Self {
            status_code,
            headers: HashMap::new(),
            body: serde_json::to_string(body).unwrap_or_default(),
            content_type: "application/json".to_string(),
        }
    }

    /// Build an error response from a [`GatewayError`].
    fn from_error(err: &GatewayError) -> Self {
        let body = serde_json::json!({
            "error": err.to_string(),
        });
        Self {
            status_code: err.status_code(),
            headers: HashMap::new(),
            body: serde_json::to_string(&body).unwrap_or_default(),
            content_type: "application/json".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// GraphQL types
// ---------------------------------------------------------------------------

/// A GraphQL request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLRequest {
    /// The GraphQL query string.
    pub query: String,
    /// Optional variables for the query.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variables: Option<serde_json::Value>,
    /// Optional operation name when multiple operations are present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_name: Option<String>,
}

/// A GraphQL response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLResponse {
    /// The response data (present on success).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    /// A list of errors (present on failure).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub errors: Option<Vec<GraphQLError>>,
}

/// A single GraphQL error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLError {
    /// Human-readable error message.
    pub message: String,
    /// Source locations in the query where the error occurred.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locations: Option<Vec<SourceLocation>>,
    /// Path to the field that caused the error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<Vec<String>>,
    /// Optional vendor-specific extensions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Option<serde_json::Value>,
}

/// A source location within a GraphQL document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceLocation {
    /// Line number (1-indexed).
    pub line: u32,
    /// Column number (1-indexed).
    pub column: u32,
}

// ---------------------------------------------------------------------------
// REST response types
// ---------------------------------------------------------------------------

/// A REST API response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestResponse {
    /// HTTP status code.
    pub status: u16,
    /// Response payload.
    pub data: serde_json::Value,
    /// Optional pagination / timing metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<ResponseMeta>,
}

/// Metadata attached to paginated or timed REST responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseMeta {
    /// Total number of results (for paginated responses).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,
    /// Opaque cursor for the next page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Time taken to process the request in milliseconds.
    pub took_ms: u64,
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Runtime statistics for the gateway.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayStats {
    /// Total number of requests handled.
    pub total_requests: u64,
    /// Number of REST requests handled.
    pub rest_requests: u64,
    /// Number of GraphQL requests handled.
    pub graphql_requests: u64,
    /// Number of error responses returned.
    pub errors: u64,
    /// Average response latency in milliseconds.
    pub avg_latency_ms: f64,
    /// Number of queries executed (REST + GraphQL).
    pub queries_executed: u64,
}

/// Internal atomic counters for thread-safe stat tracking.
struct StatsCounters {
    total_requests: AtomicU64,
    rest_requests: AtomicU64,
    graphql_requests: AtomicU64,
    errors: AtomicU64,
    total_latency_us: AtomicU64,
    queries_executed: AtomicU64,
}

impl StatsCounters {
    fn new() -> Self {
        Self {
            total_requests: AtomicU64::new(0),
            rest_requests: AtomicU64::new(0),
            graphql_requests: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
            queries_executed: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> GatewayStats {
        let total = self.total_requests.load(Ordering::Relaxed);
        let total_latency = self.total_latency_us.load(Ordering::Relaxed);
        let avg_latency_ms = if total > 0 {
            (total_latency as f64) / (total as f64) / 1000.0
        } else {
            0.0
        };
        GatewayStats {
            total_requests: total,
            rest_requests: self.rest_requests.load(Ordering::Relaxed),
            graphql_requests: self.graphql_requests.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            avg_latency_ms,
            queries_executed: self.queries_executed.load(Ordering::Relaxed),
        }
    }

    fn record_latency(&self, start: Instant) {
        let elapsed_us = start.elapsed().as_micros() as u64;
        self.total_latency_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// REST route registry
// ---------------------------------------------------------------------------

/// A registered REST route pattern.
#[derive(Debug, Clone)]
struct RestRoute {
    method: HttpMethod,
    /// Pattern segments – a literal string or `:param` for a path parameter.
    segments: Vec<RouteSegment>,
    /// Human-readable description.
    description: String,
    /// OpenAPI operation ID.
    operation_id: String,
}

#[derive(Debug, Clone)]
enum RouteSegment {
    Literal(String),
    Param(String),
}

impl RestRoute {
    /// Try to match a request path, returning extracted parameters on success.
    fn match_path(&self, path: &str) -> Option<HashMap<String, String>> {
        let path_parts: Vec<&str> = path.trim_matches('/').split('/').collect();
        let route_parts: Vec<&RouteSegment> = self.segments.iter().collect();

        if path_parts.len() != route_parts.len() {
            return None;
        }

        let mut params = HashMap::new();
        for (seg, part) in route_parts.iter().zip(path_parts.iter()) {
            match seg {
                RouteSegment::Literal(lit) => {
                    if lit != part {
                        return None;
                    }
                }
                RouteSegment::Param(name) => {
                    params.insert(name.clone(), (*part).to_string());
                }
            }
        }
        Some(params)
    }
}

/// Parse a route pattern string (e.g. `"/api/v1/keys/:key"`) into segments.
fn parse_route(pattern: &str) -> Vec<RouteSegment> {
    pattern
        .trim_matches('/')
        .split('/')
        .map(|s| {
            if let Some(name) = s.strip_prefix(':') {
                RouteSegment::Param(name.to_string())
            } else {
                RouteSegment::Literal(s.to_string())
            }
        })
        .collect()
}

/// Build the default REST route registry.
fn default_routes() -> Vec<RestRoute> {
    vec![
        RestRoute {
            method: HttpMethod::Get,
            segments: parse_route("/api/v1/keys/:key"),
            description: "Get value of a key".to_string(),
            operation_id: "getKey".to_string(),
        },
        RestRoute {
            method: HttpMethod::Put,
            segments: parse_route("/api/v1/keys/:key"),
            description: "Set value of a key".to_string(),
            operation_id: "setKey".to_string(),
        },
        RestRoute {
            method: HttpMethod::Delete,
            segments: parse_route("/api/v1/keys/:key"),
            description: "Delete a key".to_string(),
            operation_id: "deleteKey".to_string(),
        },
        RestRoute {
            method: HttpMethod::Get,
            segments: parse_route("/api/v1/keys"),
            description: "Scan keys with optional pattern, count, and cursor".to_string(),
            operation_id: "scanKeys".to_string(),
        },
        RestRoute {
            method: HttpMethod::Post,
            segments: parse_route("/api/v1/query"),
            description: "Execute a FerriteQL query".to_string(),
            operation_id: "executeQuery".to_string(),
        },
        RestRoute {
            method: HttpMethod::Get,
            segments: parse_route("/api/v1/info"),
            description: "Get server info".to_string(),
            operation_id: "getInfo".to_string(),
        },
        RestRoute {
            method: HttpMethod::Get,
            segments: parse_route("/api/v1/health"),
            description: "Health check".to_string(),
            operation_id: "healthCheck".to_string(),
        },
        RestRoute {
            method: HttpMethod::Get,
            segments: parse_route("/api/v1/metrics"),
            description: "Prometheus metrics".to_string(),
            operation_id: "getMetrics".to_string(),
        },
        RestRoute {
            method: HttpMethod::Post,
            segments: parse_route("/api/v1/search"),
            description: "Vector / full-text search".to_string(),
            operation_id: "search".to_string(),
        },
    ]
}

// ---------------------------------------------------------------------------
// ApiGateway
// ---------------------------------------------------------------------------

/// Main API gateway manager.
///
/// Routes incoming HTTP requests to the appropriate REST or GraphQL handler,
/// tracks statistics, and generates OpenAPI / GraphQL schema documentation.
pub struct ApiGateway {
    /// Gateway configuration.
    config: GatewayConfig,
    /// Registered REST routes.
    routes: Vec<RestRoute>,
    /// Atomic request counters.
    stats: StatsCounters,
}

impl ApiGateway {
    /// Create a new gateway with the given configuration.
    pub fn new(config: GatewayConfig) -> Self {
        Self {
            config,
            routes: default_routes(),
            stats: StatsCounters::new(),
        }
    }

    // -- Routing -----------------------------------------------------------

    /// Route an incoming [`GatewayRequest`] to the appropriate handler and
    /// return a [`GatewayResponse`].
    pub fn route_request(&self, request: &GatewayRequest) -> GatewayResponse {
        let start = Instant::now();
        self.stats.total_requests.fetch_add(1, Ordering::Relaxed);

        // Handle CORS pre-flight
        if request.method == HttpMethod::Options {
            let mut response = GatewayResponse {
                status_code: 204,
                headers: HashMap::new(),
                body: String::new(),
                content_type: "text/plain".to_string(),
            };
            self.apply_cors_headers(&mut response);
            self.stats.record_latency(start);
            return response;
        }

        // GraphQL endpoint
        if request.path.trim_end_matches('/') == "/api/v1/graphql"
            || request.path.trim_end_matches('/') == "/graphql"
        {
            if !self.config.enable_graphql {
                let err = GatewayError::NotFound("GraphQL endpoint is disabled".to_string());
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                self.stats.record_latency(start);
                return GatewayResponse::from_error(&err);
            }

            let gql_response = match &request.body {
                Some(body) => match serde_json::from_str::<GraphQLRequest>(body) {
                    Ok(gql_req) => self.execute_graphql(&gql_req),
                    Err(e) => GraphQLResponse {
                        data: None,
                        errors: Some(vec![GraphQLError {
                            message: format!("Invalid GraphQL request body: {e}"),
                            locations: None,
                            path: None,
                            extensions: None,
                        }]),
                    },
                },
                None => GraphQLResponse {
                    data: None,
                    errors: Some(vec![GraphQLError {
                        message: "Missing request body".to_string(),
                        locations: None,
                        path: None,
                        extensions: None,
                    }]),
                },
            };

            let body = serde_json::to_string(&gql_response).unwrap_or_default();
            let status_code = if gql_response.errors.is_some() {
                400
            } else {
                200
            };
            let mut response = GatewayResponse {
                status_code,
                headers: HashMap::new(),
                body,
                content_type: "application/json".to_string(),
            };
            self.apply_cors_headers(&mut response);
            self.stats.record_latency(start);
            return response;
        }

        // REST routes
        if !self.config.enable_rest {
            let err = GatewayError::NotFound("REST API is disabled".to_string());
            self.stats.errors.fetch_add(1, Ordering::Relaxed);
            self.stats.record_latency(start);
            return GatewayResponse::from_error(&err);
        }

        let rest_response = self.execute_rest(
            &request.method.to_string(),
            &request.path,
            request.body.as_deref(),
        );

        let mut response = GatewayResponse::json(
            rest_response.status,
            &serde_json::json!({
                "status": rest_response.status,
                "data": rest_response.data,
                "meta": rest_response.meta,
            }),
        );
        response.status_code = rest_response.status;
        self.apply_cors_headers(&mut response);
        self.stats.record_latency(start);
        response
    }

    // -- REST execution ----------------------------------------------------

    /// Execute a REST request by matching the method and path against the
    /// route registry.
    pub fn execute_rest(&self, method: &str, path: &str, body: Option<&str>) -> RestResponse {
        self.stats.rest_requests.fetch_add(1, Ordering::Relaxed);
        let start = Instant::now();

        let http_method = match HttpMethod::from_str(method) {
            Some(m) => m,
            None => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                return RestResponse {
                    status: 405,
                    data: serde_json::json!({"error": "Method not allowed"}),
                    meta: Some(ResponseMeta {
                        total: None,
                        cursor: None,
                        took_ms: start.elapsed().as_millis() as u64,
                    }),
                };
            }
        };

        // Find matching route
        for route in &self.routes {
            if route.method != http_method {
                continue;
            }
            if let Some(params) = route.match_path(path) {
                self.stats.queries_executed.fetch_add(1, Ordering::Relaxed);
                let data = self.dispatch_rest(route, &params, body);
                return RestResponse {
                    status: 200,
                    data,
                    meta: Some(ResponseMeta {
                        total: None,
                        cursor: None,
                        took_ms: start.elapsed().as_millis() as u64,
                    }),
                };
            }
        }

        self.stats.errors.fetch_add(1, Ordering::Relaxed);
        RestResponse {
            status: 404,
            data: serde_json::json!({"error": format!("No route for {method} {path}")}),
            meta: Some(ResponseMeta {
                total: None,
                cursor: None,
                took_ms: start.elapsed().as_millis() as u64,
            }),
        }
    }

    /// Dispatch to a placeholder handler based on the matched route.
    fn dispatch_rest(
        &self,
        route: &RestRoute,
        params: &HashMap<String, String>,
        body: Option<&str>,
    ) -> serde_json::Value {
        match route.operation_id.as_str() {
            "getKey" => {
                let key = params.get("key").cloned().unwrap_or_default();
                serde_json::json!({
                    "key": key,
                    "value": "placeholder",
                    "type": "string",
                    "ttl": -1
                })
            }
            "setKey" => {
                let key = params.get("key").cloned().unwrap_or_default();
                let value = body.unwrap_or("");
                serde_json::json!({
                    "key": key,
                    "value": value,
                    "result": "OK"
                })
            }
            "deleteKey" => {
                let key = params.get("key").cloned().unwrap_or_default();
                serde_json::json!({
                    "key": key,
                    "deleted": true
                })
            }
            "scanKeys" => {
                serde_json::json!({
                    "cursor": "0",
                    "keys": []
                })
            }
            "executeQuery" => {
                let query = body.unwrap_or("");
                serde_json::json!({
                    "query": query,
                    "rows": [],
                    "affected": 0
                })
            }
            "getInfo" => {
                serde_json::json!({
                    "version": env!("CARGO_PKG_VERSION"),
                    "uptime_secs": 0,
                    "mode": "standalone"
                })
            }
            "healthCheck" => {
                serde_json::json!({
                    "status": "ok",
                    "version": env!("CARGO_PKG_VERSION")
                })
            }
            "getMetrics" => {
                serde_json::json!({
                    "ops_per_sec": 0,
                    "connected_clients": 0,
                    "used_memory": 0
                })
            }
            "search" => {
                let query = body.unwrap_or("");
                serde_json::json!({
                    "query": query,
                    "results": [],
                    "total": 0
                })
            }
            _ => serde_json::json!({"error": "unknown operation"}),
        }
    }

    // -- GraphQL execution -------------------------------------------------

    /// Execute a GraphQL request.
    ///
    /// This is a scaffold implementation that validates the query string and
    /// returns placeholder data for known root fields.
    pub fn execute_graphql(&self, query: &GraphQLRequest) -> GraphQLResponse {
        self.stats.graphql_requests.fetch_add(1, Ordering::Relaxed);

        let trimmed = query.query.trim();

        // Reject empty queries
        if trimmed.is_empty() {
            self.stats.errors.fetch_add(1, Ordering::Relaxed);
            return GraphQLResponse {
                data: None,
                errors: Some(vec![GraphQLError {
                    message: "Empty query".to_string(),
                    locations: Some(vec![SourceLocation { line: 1, column: 1 }]),
                    path: None,
                    extensions: None,
                }]),
            };
        }

        // Introspection
        if trimmed.contains("__schema") || trimmed.contains("__type") {
            if !self.config.introspection_enabled {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                return GraphQLResponse {
                    data: None,
                    errors: Some(vec![GraphQLError {
                        message: "Introspection is disabled".to_string(),
                        locations: None,
                        path: None,
                        extensions: None,
                    }]),
                };
            }
            return GraphQLResponse {
                data: Some(serde_json::json!({
                    "__schema": {
                        "queryType": { "name": "Query" },
                        "mutationType": { "name": "Mutation" },
                        "subscriptionType": { "name": "Subscription" }
                    }
                })),
                errors: None,
            };
        }

        // Simple depth check: count nesting braces
        let depth = trimmed
            .chars()
            .fold(0u32, |max_d, c| if c == '{' { max_d + 1 } else { max_d });
        if depth > self.config.max_query_depth {
            self.stats.errors.fetch_add(1, Ordering::Relaxed);
            return GraphQLResponse {
                data: None,
                errors: Some(vec![GraphQLError {
                    message: format!(
                        "Query depth {depth} exceeds maximum allowed depth {}",
                        self.config.max_query_depth
                    ),
                    locations: None,
                    path: None,
                    extensions: None,
                }]),
            };
        }

        self.stats.queries_executed.fetch_add(1, Ordering::Relaxed);

        // Dispatch known root fields (placeholder)
        if trimmed.contains("mutation") {
            return self.execute_graphql_mutation(trimmed);
        }

        self.execute_graphql_query(trimmed)
    }

    /// Handle a GraphQL query operation.
    fn execute_graphql_query(&self, query: &str) -> GraphQLResponse {
        let mut data = serde_json::Map::new();

        if query.contains("key(") || query.contains("key (") {
            data.insert(
                "key".to_string(),
                serde_json::json!({
                    "name": "placeholder",
                    "value": "placeholder",
                    "type": "string",
                    "ttl": -1
                }),
            );
        }

        if query.contains("keys(") || query.contains("keys (") || query.contains("keys{") {
            data.insert("keys".to_string(), serde_json::json!([]));
        }

        if query.contains("query(") || query.contains("query (") {
            data.insert(
                "query".to_string(),
                serde_json::json!({
                    "rows": [],
                    "affected": 0
                }),
            );
        }

        if query.contains("info") {
            data.insert(
                "info".to_string(),
                serde_json::json!({
                    "version": env!("CARGO_PKG_VERSION"),
                    "uptime_secs": 0
                }),
            );
        }

        if data.is_empty() {
            data.insert("__typename".to_string(), serde_json::json!("Query"));
        }

        GraphQLResponse {
            data: Some(serde_json::Value::Object(data)),
            errors: None,
        }
    }

    /// Handle a GraphQL mutation operation.
    fn execute_graphql_mutation(&self, query: &str) -> GraphQLResponse {
        let mut data = serde_json::Map::new();

        if query.contains("set(") || query.contains("set (") {
            data.insert("set".to_string(), serde_json::json!(true));
        }

        if query.contains("delete(") || query.contains("delete (") {
            data.insert("delete".to_string(), serde_json::json!(true));
        }

        if query.contains("query(") || query.contains("query (") {
            data.insert(
                "query".to_string(),
                serde_json::json!({
                    "rows": [],
                    "affected": 0
                }),
            );
        }

        if data.is_empty() {
            data.insert("__typename".to_string(), serde_json::json!("Mutation"));
        }

        GraphQLResponse {
            data: Some(serde_json::Value::Object(data)),
            errors: None,
        }
    }

    // -- Schema / spec generation ------------------------------------------

    /// Generate an OpenAPI 3.0 specification (JSON) for the REST endpoints.
    pub fn get_openapi_spec(&self) -> String {
        let mut paths = serde_json::Map::new();

        for route in &self.routes {
            let path_str = route
                .segments
                .iter()
                .map(|seg| match seg {
                    RouteSegment::Literal(l) => format!("/{l}"),
                    RouteSegment::Param(p) => format!("/{{{p}}}"),
                })
                .collect::<String>();

            let method_lower = route.method.to_string().to_lowercase();
            let operation = serde_json::json!({
                "operationId": route.operation_id,
                "summary": route.description,
                "responses": {
                    "200": {
                        "description": "Successful response",
                        "content": {
                            "application/json": {
                                "schema": { "type": "object" }
                            }
                        }
                    },
                    "400": { "description": "Bad request" },
                    "404": { "description": "Not found" },
                    "500": { "description": "Internal server error" }
                }
            });

            let entry = paths
                .entry(&path_str)
                .or_insert_with(|| serde_json::json!({}));
            if let serde_json::Value::Object(map) = entry {
                map.insert(method_lower, operation);
            }
        }

        let spec = serde_json::json!({
            "openapi": "3.0.3",
            "info": {
                "title": "Ferrite API",
                "version": env!("CARGO_PKG_VERSION"),
                "description": "REST API for Ferrite key-value store"
            },
            "servers": [
                { "url": self.config.base_path }
            ],
            "paths": paths
        });

        serde_json::to_string_pretty(&spec).unwrap_or_default()
    }

    /// Generate the GraphQL schema definition language (SDL).
    pub fn get_graphql_schema(&self) -> String {
        r#"type Query {
  "Get a single key-value pair by name"
  key(name: String!): KeyValue
  "List keys matching an optional pattern"
  keys(pattern: String, limit: Int): [KeySummary!]!
  "Execute a FerriteQL query"
  query(sql: String!): QueryResult
  "Get server information"
  info: ServerInfo
}

type Mutation {
  "Set a key to a value with optional TTL in seconds"
  set(key: String!, value: String!, ttl: Int): Boolean!
  "Delete a key"
  delete(key: String!): Boolean!
  "Execute a FerriteQL mutation query"
  query(sql: String!): QueryResult
}

type Subscription {
  "Subscribe to changes on keys matching a pattern"
  keyChanged(pattern: String!): KeyEvent
}

type KeyValue {
  name: String!
  value: String!
  type: String!
  ttl: Int
}

type KeySummary {
  name: String!
  type: String!
  ttl: Int
  size: Int
}

type QueryResult {
  rows: [JSON]
  affected: Int!
  elapsed_ms: Float
}

type ServerInfo {
  version: String!
  uptime_secs: Int!
  connected_clients: Int!
  total_keys: Int!
  used_memory: Int!
  mode: String!
}

type KeyEvent {
  key: String!
  event: String!
  value: String
  timestamp: String!
}

scalar JSON
"#
        .to_string()
    }

    // -- Stats -------------------------------------------------------------

    /// Return a snapshot of the current gateway statistics.
    pub fn get_stats(&self) -> GatewayStats {
        self.stats.snapshot()
    }

    // -- CORS helpers ------------------------------------------------------

    /// Apply CORS headers to a response based on the gateway configuration.
    fn apply_cors_headers(&self, response: &mut GatewayResponse) {
        if self.config.cors_origins.is_empty() {
            return;
        }

        let origin = if self.config.cors_origins.contains(&"*".to_string()) {
            "*".to_string()
        } else {
            self.config.cors_origins.join(", ")
        };

        response
            .headers
            .insert("Access-Control-Allow-Origin".to_string(), origin);
        response.headers.insert(
            "Access-Control-Allow-Methods".to_string(),
            "GET, POST, PUT, DELETE, PATCH, OPTIONS".to_string(),
        );
        response.headers.insert(
            "Access-Control-Allow-Headers".to_string(),
            "Content-Type, Authorization".to_string(),
        );
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Config defaults ---------------------------------------------------

    #[test]
    fn test_config_defaults() {
        let config = GatewayConfig::default();
        assert!(config.enable_rest);
        assert!(config.enable_graphql);
        assert_eq!(config.base_path, "/api/v1");
        assert!(config.cors_origins.is_empty());
        assert_eq!(config.rate_limit_per_second, 0);
        assert!(!config.auth_required);
        assert_eq!(config.max_query_depth, 10);
        assert!(config.introspection_enabled);
    }

    #[test]
    fn test_config_serialization() {
        let config = GatewayConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: GatewayConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.base_path, config.base_path);
        assert_eq!(deserialized.enable_rest, config.enable_rest);
    }

    // -- REST route matching -----------------------------------------------

    #[test]
    fn test_rest_route_match_literal() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let response = gateway.execute_rest("GET", "/api/v1/health", None);
        assert_eq!(response.status, 200);
        assert!(response.data["status"] == "ok");
    }

    #[test]
    fn test_rest_route_match_with_param() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let response = gateway.execute_rest("GET", "/api/v1/keys/mykey", None);
        assert_eq!(response.status, 200);
        assert_eq!(response.data["key"], "mykey");
    }

    #[test]
    fn test_rest_route_put_key() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let response = gateway.execute_rest("PUT", "/api/v1/keys/foo", Some("bar"));
        assert_eq!(response.status, 200);
        assert_eq!(response.data["key"], "foo");
        assert_eq!(response.data["value"], "bar");
        assert_eq!(response.data["result"], "OK");
    }

    #[test]
    fn test_rest_route_delete_key() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let response = gateway.execute_rest("DELETE", "/api/v1/keys/mykey", None);
        assert_eq!(response.status, 200);
        assert_eq!(response.data["deleted"], true);
    }

    #[test]
    fn test_rest_route_scan_keys() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let response = gateway.execute_rest("GET", "/api/v1/keys", None);
        assert_eq!(response.status, 200);
        assert!(response.data["keys"].is_array());
    }

    #[test]
    fn test_rest_route_execute_query() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let response = gateway.execute_rest("POST", "/api/v1/query", Some("SELECT * FROM keys"));
        assert_eq!(response.status, 200);
        assert!(response.data["rows"].is_array());
    }

    #[test]
    fn test_rest_route_server_info() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let response = gateway.execute_rest("GET", "/api/v1/info", None);
        assert_eq!(response.status, 200);
        assert!(!response.data["version"].as_str().unwrap().is_empty());
    }

    #[test]
    fn test_rest_route_metrics() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let response = gateway.execute_rest("GET", "/api/v1/metrics", None);
        assert_eq!(response.status, 200);
    }

    #[test]
    fn test_rest_route_search() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let response = gateway.execute_rest("POST", "/api/v1/search", Some("hello"));
        assert_eq!(response.status, 200);
        assert_eq!(response.data["total"], 0);
    }

    #[test]
    fn test_rest_route_not_found() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let response = gateway.execute_rest("GET", "/api/v1/nonexistent", None);
        assert_eq!(response.status, 404);
    }

    #[test]
    fn test_rest_route_method_not_allowed() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let response = gateway.execute_rest("INVALID", "/api/v1/keys", None);
        assert_eq!(response.status, 405);
    }

    #[test]
    fn test_rest_response_has_meta() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let response = gateway.execute_rest("GET", "/api/v1/health", None);
        assert!(response.meta.is_some());
        let meta = response.meta.unwrap();
        // took_ms is non-negative (could be 0 for fast operations)
        assert!(meta.took_ms < 10000);
    }

    // -- GraphQL query parsing ---------------------------------------------

    #[test]
    fn test_graphql_empty_query() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let req = GraphQLRequest {
            query: "".to_string(),
            variables: None,
            operation_name: None,
        };
        let resp = gateway.execute_graphql(&req);
        assert!(resp.errors.is_some());
        assert!(resp.data.is_none());
        let errors = resp.errors.unwrap();
        assert_eq!(errors[0].message, "Empty query");
    }

    #[test]
    fn test_graphql_query_key() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let req = GraphQLRequest {
            query: "{ key(name: \"foo\") { name value } }".to_string(),
            variables: None,
            operation_name: None,
        };
        let resp = gateway.execute_graphql(&req);
        assert!(resp.errors.is_none());
        assert!(resp.data.is_some());
        let data = resp.data.unwrap();
        assert!(data["key"].is_object());
    }

    #[test]
    fn test_graphql_query_keys() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let req = GraphQLRequest {
            query: "{ keys(pattern: \"*\") { name type } }".to_string(),
            variables: None,
            operation_name: None,
        };
        let resp = gateway.execute_graphql(&req);
        assert!(resp.errors.is_none());
        assert!(resp.data.is_some());
        let data = resp.data.unwrap();
        assert!(data["keys"].is_array());
    }

    #[test]
    fn test_graphql_query_info() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let req = GraphQLRequest {
            query: "{ info { version uptime_secs } }".to_string(),
            variables: None,
            operation_name: None,
        };
        let resp = gateway.execute_graphql(&req);
        assert!(resp.errors.is_none());
        assert!(resp.data.is_some());
    }

    #[test]
    fn test_graphql_mutation_set() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let req = GraphQLRequest {
            query: "mutation { set(key: \"foo\", value: \"bar\") }".to_string(),
            variables: None,
            operation_name: None,
        };
        let resp = gateway.execute_graphql(&req);
        assert!(resp.errors.is_none());
        assert!(resp.data.is_some());
        let data = resp.data.unwrap();
        assert_eq!(data["set"], true);
    }

    #[test]
    fn test_graphql_mutation_delete() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let req = GraphQLRequest {
            query: "mutation { delete(key: \"foo\") }".to_string(),
            variables: None,
            operation_name: None,
        };
        let resp = gateway.execute_graphql(&req);
        assert!(resp.errors.is_none());
        let data = resp.data.unwrap();
        assert_eq!(data["delete"], true);
    }

    #[test]
    fn test_graphql_introspection() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let req = GraphQLRequest {
            query: "{ __schema { queryType { name } } }".to_string(),
            variables: None,
            operation_name: None,
        };
        let resp = gateway.execute_graphql(&req);
        assert!(resp.errors.is_none());
        assert!(resp.data.is_some());
        let data = resp.data.unwrap();
        assert_eq!(data["__schema"]["queryType"]["name"], "Query");
    }

    #[test]
    fn test_graphql_introspection_disabled() {
        let mut config = GatewayConfig::default();
        config.introspection_enabled = false;
        let gateway = ApiGateway::new(config);
        let req = GraphQLRequest {
            query: "{ __schema { queryType { name } } }".to_string(),
            variables: None,
            operation_name: None,
        };
        let resp = gateway.execute_graphql(&req);
        assert!(resp.errors.is_some());
        assert_eq!(resp.errors.unwrap()[0].message, "Introspection is disabled");
    }

    #[test]
    fn test_graphql_query_too_deep() {
        let mut config = GatewayConfig::default();
        config.max_query_depth = 3;
        let gateway = ApiGateway::new(config);
        let req = GraphQLRequest {
            query: "{ a { b { c { d { e } } } } }".to_string(),
            variables: None,
            operation_name: None,
        };
        let resp = gateway.execute_graphql(&req);
        assert!(resp.errors.is_some());
        let msg = &resp.errors.unwrap()[0].message;
        assert!(msg.contains("exceeds maximum allowed depth"));
    }

    #[test]
    fn test_graphql_request_serialization() {
        let req = GraphQLRequest {
            query: "{ info { version } }".to_string(),
            variables: Some(serde_json::json!({"limit": 10})),
            operation_name: Some("GetInfo".to_string()),
        };
        let json = serde_json::to_string(&req).unwrap();
        let deserialized: GraphQLRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.query, req.query);
        assert_eq!(deserialized.operation_name, req.operation_name);
    }

    // -- OpenAPI spec generation -------------------------------------------

    #[test]
    fn test_openapi_spec_valid_json() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let spec_str = gateway.get_openapi_spec();
        let spec: serde_json::Value =
            serde_json::from_str(&spec_str).expect("OpenAPI spec should be valid JSON");
        assert_eq!(spec["openapi"], "3.0.3");
        assert_eq!(spec["info"]["title"], "Ferrite API");
        assert!(spec["paths"].is_object());
    }

    #[test]
    fn test_openapi_spec_contains_routes() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let spec_str = gateway.get_openapi_spec();
        let spec: serde_json::Value = serde_json::from_str(&spec_str).unwrap();
        let paths = spec["paths"].as_object().unwrap();
        assert!(paths.contains_key("/api/v1/keys/{key}"));
        assert!(paths.contains_key("/api/v1/keys"));
        assert!(paths.contains_key("/api/v1/health"));
        assert!(paths.contains_key("/api/v1/info"));
        assert!(paths.contains_key("/api/v1/metrics"));
        assert!(paths.contains_key("/api/v1/query"));
        assert!(paths.contains_key("/api/v1/search"));
    }

    #[test]
    fn test_openapi_spec_methods() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let spec_str = gateway.get_openapi_spec();
        let spec: serde_json::Value = serde_json::from_str(&spec_str).unwrap();
        let key_path = &spec["paths"]["/api/v1/keys/{key}"];
        assert!(key_path["get"].is_object());
        assert!(key_path["put"].is_object());
        assert!(key_path["delete"].is_object());
    }

    // -- GraphQL schema generation -----------------------------------------

    #[test]
    fn test_graphql_schema_contains_types() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let schema = gateway.get_graphql_schema();
        assert!(schema.contains("type Query"));
        assert!(schema.contains("type Mutation"));
        assert!(schema.contains("type Subscription"));
        assert!(schema.contains("type KeyValue"));
        assert!(schema.contains("type KeySummary"));
        assert!(schema.contains("type QueryResult"));
        assert!(schema.contains("type ServerInfo"));
        assert!(schema.contains("type KeyEvent"));
        assert!(schema.contains("scalar JSON"));
    }

    #[test]
    fn test_graphql_schema_query_fields() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let schema = gateway.get_graphql_schema();
        assert!(schema.contains("key(name: String!): KeyValue"));
        assert!(schema.contains("keys(pattern: String, limit: Int): [KeySummary!]!"));
        assert!(schema.contains("query(sql: String!): QueryResult"));
        assert!(schema.contains("info: ServerInfo"));
    }

    #[test]
    fn test_graphql_schema_mutation_fields() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let schema = gateway.get_graphql_schema();
        assert!(schema.contains("set(key: String!, value: String!, ttl: Int): Boolean!"));
        assert!(schema.contains("delete(key: String!): Boolean!"));
    }

    #[test]
    fn test_graphql_schema_subscription_fields() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let schema = gateway.get_graphql_schema();
        assert!(schema.contains("keyChanged(pattern: String!): KeyEvent"));
    }

    // -- Error responses ---------------------------------------------------

    #[test]
    fn test_gateway_error_status_codes() {
        assert_eq!(GatewayError::NotFound("x".into()).status_code(), 404);
        assert_eq!(GatewayError::BadRequest("x".into()).status_code(), 400);
        assert_eq!(GatewayError::Unauthorized.status_code(), 401);
        assert_eq!(GatewayError::RateLimited.status_code(), 429);
        assert_eq!(GatewayError::QueryTooComplex.status_code(), 422);
        assert_eq!(GatewayError::InternalError("x".into()).status_code(), 500);
    }

    #[test]
    fn test_gateway_error_display() {
        assert_eq!(
            GatewayError::NotFound("key".into()).to_string(),
            "not found: key"
        );
        assert_eq!(GatewayError::Unauthorized.to_string(), "unauthorized");
        assert_eq!(GatewayError::RateLimited.to_string(), "rate limited");
    }

    #[test]
    fn test_gateway_response_from_error() {
        let err = GatewayError::NotFound("missing".into());
        let resp = GatewayResponse::from_error(&err);
        assert_eq!(resp.status_code, 404);
        assert_eq!(resp.content_type, "application/json");
        let body: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
        assert!(body["error"].as_str().unwrap().contains("not found"));
    }

    // -- Stats tracking ----------------------------------------------------

    #[test]
    fn test_stats_initial() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let stats = gateway.get_stats();
        assert_eq!(stats.total_requests, 0);
        assert_eq!(stats.rest_requests, 0);
        assert_eq!(stats.graphql_requests, 0);
        assert_eq!(stats.errors, 0);
        assert_eq!(stats.avg_latency_ms, 0.0);
        assert_eq!(stats.queries_executed, 0);
    }

    #[test]
    fn test_stats_after_rest_requests() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        gateway.execute_rest("GET", "/api/v1/health", None);
        gateway.execute_rest("GET", "/api/v1/info", None);
        gateway.execute_rest("GET", "/api/v1/nonexistent", None);

        let stats = gateway.get_stats();
        assert_eq!(stats.rest_requests, 3);
        assert_eq!(stats.queries_executed, 2); // health + info matched; nonexistent did not
        assert_eq!(stats.errors, 1); // nonexistent route
    }

    #[test]
    fn test_stats_after_graphql_requests() {
        let gateway = ApiGateway::new(GatewayConfig::default());

        let ok_req = GraphQLRequest {
            query: "{ info { version } }".to_string(),
            variables: None,
            operation_name: None,
        };
        gateway.execute_graphql(&ok_req);

        let bad_req = GraphQLRequest {
            query: "".to_string(),
            variables: None,
            operation_name: None,
        };
        gateway.execute_graphql(&bad_req);

        let stats = gateway.get_stats();
        assert_eq!(stats.graphql_requests, 2);
        assert_eq!(stats.errors, 1);
        assert_eq!(stats.queries_executed, 1);
    }

    #[test]
    fn test_stats_serialization() {
        let stats = GatewayStats {
            total_requests: 100,
            rest_requests: 80,
            graphql_requests: 20,
            errors: 5,
            avg_latency_ms: 1.5,
            queries_executed: 95,
        };
        let json = serde_json::to_string(&stats).unwrap();
        let deserialized: GatewayStats = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.total_requests, 100);
        assert_eq!(deserialized.errors, 5);
    }

    // -- CORS header generation --------------------------------------------

    #[test]
    fn test_cors_headers_applied() {
        let mut config = GatewayConfig::default();
        config.cors_origins = vec!["*".to_string()];
        let gateway = ApiGateway::new(config);

        let request = GatewayRequest {
            method: HttpMethod::Get,
            path: "/api/v1/health".to_string(),
            headers: HashMap::new(),
            body: None,
            query_params: HashMap::new(),
        };
        let response = gateway.route_request(&request);
        assert_eq!(
            response.headers.get("Access-Control-Allow-Origin"),
            Some(&"*".to_string())
        );
        assert!(response
            .headers
            .get("Access-Control-Allow-Methods")
            .is_some());
        assert!(response
            .headers
            .get("Access-Control-Allow-Headers")
            .is_some());
    }

    #[test]
    fn test_cors_headers_not_applied_when_empty() {
        let config = GatewayConfig::default();
        let gateway = ApiGateway::new(config);

        let request = GatewayRequest {
            method: HttpMethod::Get,
            path: "/api/v1/health".to_string(),
            headers: HashMap::new(),
            body: None,
            query_params: HashMap::new(),
        };
        let response = gateway.route_request(&request);
        assert!(response
            .headers
            .get("Access-Control-Allow-Origin")
            .is_none());
    }

    #[test]
    fn test_cors_specific_origins() {
        let mut config = GatewayConfig::default();
        config.cors_origins = vec![
            "https://example.com".to_string(),
            "https://app.example.com".to_string(),
        ];
        let gateway = ApiGateway::new(config);

        let request = GatewayRequest {
            method: HttpMethod::Get,
            path: "/api/v1/health".to_string(),
            headers: HashMap::new(),
            body: None,
            query_params: HashMap::new(),
        };
        let response = gateway.route_request(&request);
        let origin = response.headers.get("Access-Control-Allow-Origin").unwrap();
        assert!(origin.contains("https://example.com"));
        assert!(origin.contains("https://app.example.com"));
    }

    #[test]
    fn test_cors_preflight_options() {
        let mut config = GatewayConfig::default();
        config.cors_origins = vec!["*".to_string()];
        let gateway = ApiGateway::new(config);

        let request = GatewayRequest {
            method: HttpMethod::Options,
            path: "/api/v1/keys/foo".to_string(),
            headers: HashMap::new(),
            body: None,
            query_params: HashMap::new(),
        };
        let response = gateway.route_request(&request);
        assert_eq!(response.status_code, 204);
        assert_eq!(
            response.headers.get("Access-Control-Allow-Origin"),
            Some(&"*".to_string())
        );
    }

    // -- Routing integration -----------------------------------------------

    #[test]
    fn test_route_request_rest() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let request = GatewayRequest {
            method: HttpMethod::Get,
            path: "/api/v1/health".to_string(),
            headers: HashMap::new(),
            body: None,
            query_params: HashMap::new(),
        };
        let response = gateway.route_request(&request);
        assert_eq!(response.status_code, 200);
        assert_eq!(response.content_type, "application/json");
    }

    #[test]
    fn test_route_request_graphql() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let gql_body = serde_json::json!({
            "query": "{ info { version } }"
        });
        let request = GatewayRequest {
            method: HttpMethod::Post,
            path: "/api/v1/graphql".to_string(),
            headers: HashMap::new(),
            body: Some(serde_json::to_string(&gql_body).unwrap()),
            query_params: HashMap::new(),
        };
        let response = gateway.route_request(&request);
        assert_eq!(response.status_code, 200);
        let body: serde_json::Value = serde_json::from_str(&response.body).unwrap();
        assert!(body["data"].is_object());
    }

    #[test]
    fn test_route_request_graphql_disabled() {
        let mut config = GatewayConfig::default();
        config.enable_graphql = false;
        let gateway = ApiGateway::new(config);
        let request = GatewayRequest {
            method: HttpMethod::Post,
            path: "/api/v1/graphql".to_string(),
            headers: HashMap::new(),
            body: Some("{\"query\": \"{ info { version } }\"}".to_string()),
            query_params: HashMap::new(),
        };
        let response = gateway.route_request(&request);
        assert_eq!(response.status_code, 404);
    }

    #[test]
    fn test_route_request_rest_disabled() {
        let mut config = GatewayConfig::default();
        config.enable_rest = false;
        let gateway = ApiGateway::new(config);
        let request = GatewayRequest {
            method: HttpMethod::Get,
            path: "/api/v1/health".to_string(),
            headers: HashMap::new(),
            body: None,
            query_params: HashMap::new(),
        };
        let response = gateway.route_request(&request);
        assert_eq!(response.status_code, 404);
    }

    #[test]
    fn test_route_request_graphql_invalid_body() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let request = GatewayRequest {
            method: HttpMethod::Post,
            path: "/api/v1/graphql".to_string(),
            headers: HashMap::new(),
            body: Some("not json".to_string()),
            query_params: HashMap::new(),
        };
        let response = gateway.route_request(&request);
        assert_eq!(response.status_code, 400);
    }

    #[test]
    fn test_route_request_graphql_no_body() {
        let gateway = ApiGateway::new(GatewayConfig::default());
        let request = GatewayRequest {
            method: HttpMethod::Post,
            path: "/graphql".to_string(),
            headers: HashMap::new(),
            body: None,
            query_params: HashMap::new(),
        };
        let response = gateway.route_request(&request);
        assert_eq!(response.status_code, 400);
    }

    // -- HttpMethod --------------------------------------------------------

    #[test]
    fn test_http_method_from_str() {
        assert_eq!(HttpMethod::from_str("GET"), Some(HttpMethod::Get));
        assert_eq!(HttpMethod::from_str("post"), Some(HttpMethod::Post));
        assert_eq!(HttpMethod::from_str("Put"), Some(HttpMethod::Put));
        assert_eq!(HttpMethod::from_str("DELETE"), Some(HttpMethod::Delete));
        assert_eq!(HttpMethod::from_str("PATCH"), Some(HttpMethod::Patch));
        assert_eq!(HttpMethod::from_str("OPTIONS"), Some(HttpMethod::Options));
        assert_eq!(HttpMethod::from_str("UNKNOWN"), None);
    }

    #[test]
    fn test_http_method_display() {
        assert_eq!(HttpMethod::Get.to_string(), "GET");
        assert_eq!(HttpMethod::Post.to_string(), "POST");
        assert_eq!(HttpMethod::Delete.to_string(), "DELETE");
    }

    // -- Type serialization ------------------------------------------------

    #[test]
    fn test_rest_response_serialization() {
        let resp = RestResponse {
            status: 200,
            data: serde_json::json!({"key": "foo"}),
            meta: Some(ResponseMeta {
                total: Some(1),
                cursor: None,
                took_ms: 5,
            }),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: RestResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.status, 200);
        assert_eq!(deserialized.meta.unwrap().total, Some(1));
    }

    #[test]
    fn test_graphql_error_serialization() {
        let err = GraphQLError {
            message: "field not found".to_string(),
            locations: Some(vec![SourceLocation { line: 1, column: 5 }]),
            path: Some(vec!["query".to_string(), "key".to_string()]),
            extensions: None,
        };
        let json = serde_json::to_string(&err).unwrap();
        let deserialized: GraphQLError = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.message, "field not found");
        assert_eq!(deserialized.locations.unwrap()[0].line, 1);
    }

    #[test]
    fn test_gateway_request_serialization() {
        let req = GatewayRequest {
            method: HttpMethod::Post,
            path: "/api/v1/graphql".to_string(),
            headers: HashMap::from([("Content-Type".to_string(), "application/json".to_string())]),
            body: Some("{\"query\":\"{ info }\"}".to_string()),
            query_params: HashMap::new(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let deserialized: GatewayRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.method, HttpMethod::Post);
        assert_eq!(deserialized.path, "/api/v1/graphql");
    }

    #[test]
    fn test_source_location_serialization() {
        let loc = SourceLocation { line: 3, column: 7 };
        let json = serde_json::to_string(&loc).unwrap();
        let deserialized: SourceLocation = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.line, 3);
        assert_eq!(deserialized.column, 7);
    }

    #[test]
    fn test_response_meta_serialization() {
        let meta = ResponseMeta {
            total: Some(42),
            cursor: Some("abc123".to_string()),
            took_ms: 15,
        };
        let json = serde_json::to_string(&meta).unwrap();
        assert!(json.contains("42"));
        assert!(json.contains("abc123"));
    }
}
