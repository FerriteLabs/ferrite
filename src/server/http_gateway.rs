#![allow(dead_code)]
//! HTTP API Gateway Server
//!
//! Serves REST and GraphQL endpoints over HTTP, providing
//! an alternative access method to the RESP protocol.
//!
//! Runs alongside the main RESP server on a configurable port.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

use crate::storage::{Store, Value};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the HTTP API gateway.
#[derive(Debug, Clone)]
pub struct HttpGatewayConfig {
    /// Address to bind on
    pub bind: String,
    /// Port number
    pub port: u16,
    /// Whether REST endpoints are enabled
    pub enable_rest: bool,
    /// Whether GraphQL endpoints are enabled
    pub enable_graphql: bool,
    /// Whether CORS headers are added to responses
    pub enable_cors: bool,
    /// Whether authentication is required
    pub auth_enabled: bool,
    /// Maximum request body size in bytes
    pub max_body_size: usize,
}

impl Default for HttpGatewayConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".to_string(),
            port: 8080,
            enable_rest: true,
            enable_graphql: true,
            enable_cors: true,
            auth_enabled: false,
            max_body_size: 1_048_576, // 1 MB
        }
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur in the HTTP gateway.
#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    /// Failed to bind the TCP listener.
    #[error("Failed to bind to {0}: {1}")]
    BindFailed(String, std::io::Error),

    /// The incoming request is invalid.
    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    /// Authentication failed.
    #[error("Authentication failed: {0}")]
    AuthFailed(String),

    /// An internal error occurred.
    #[error("Internal error: {0}")]
    InternalError(String),

    /// The request body exceeds the configured limit.
    #[error("Request body too large (max {0} bytes)")]
    BodyTooLarge(usize),
}

// ---------------------------------------------------------------------------
// HTTP Gateway
// ---------------------------------------------------------------------------

/// HTTP API gateway server.
///
/// Provides REST and GraphQL access to a Ferrite [`Store`] over HTTP,
/// running alongside the main RESP protocol server.
pub struct HttpGateway {
    config: HttpGatewayConfig,
    store: Arc<Store>,
}

impl HttpGateway {
    /// Create a new HTTP gateway with the given configuration and store.
    pub fn new(config: HttpGatewayConfig, store: Arc<Store>) -> Self {
        Self { config, store }
    }

    /// Start the HTTP gateway server.
    ///
    /// Binds a TCP listener and serves HTTP/1.1 requests until the task
    /// is cancelled.
    pub async fn start(&self) -> Result<(), GatewayError> {
        let addr: SocketAddr = format!("{}:{}", self.config.bind, self.config.port)
            .parse()
            .map_err(|e| {
                GatewayError::BindFailed(
                    format!("{}:{}", self.config.bind, self.config.port),
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, e),
                )
            })?;

        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| GatewayError::BindFailed(addr.to_string(), e))?;

        info!("HTTP gateway listening on {}", addr);

        loop {
            let (stream, remote_addr) = listener
                .accept()
                .await
                .map_err(|e| GatewayError::InternalError(format!("Accept failed: {}", e)))?;

            let io = TokioIo::new(stream);
            let store = Arc::clone(&self.store);
            let config = self.config.clone();

            tokio::spawn(async move {
                let service = service_fn(move |req: Request<Incoming>| {
                    let store = Arc::clone(&store);
                    let config = config.clone();
                    async move {
                        let result = handle_request(req, store, &config).await;
                        match result {
                            Ok(resp) => Ok::<_, std::convert::Infallible>(resp),
                            Err(e) => {
                                error!("Gateway error: {}", e);
                                Ok(error_response(
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    &e.to_string(),
                                ))
                            }
                        }
                    }
                });

                if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                    warn!("HTTP connection error from {}: {}", remote_addr, e);
                }
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Request handling
// ---------------------------------------------------------------------------

/// Route an incoming HTTP request to the appropriate handler.
async fn handle_request(
    req: Request<Incoming>,
    store: Arc<Store>,
    config: &HttpGatewayConfig,
) -> Result<Response<Full<Bytes>>, GatewayError> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();

    // CORS preflight
    if method == Method::OPTIONS {
        let mut resp = json_response(StatusCode::NO_CONTENT, serde_json::json!({}));
        if config.enable_cors {
            cors_headers(&mut resp);
        }
        return Ok(resp);
    }

    let mut response = match (method, path.as_str()) {
        // Health check
        (Method::GET, "/health") => json_response(
            StatusCode::OK,
            serde_json::json!({ "status": "ok", "version": "0.1.0" }),
        ),

        // REST endpoints
        (Method::GET, p) if p.starts_with("/api/v1/string/") && config.enable_rest => {
            handle_get_string(&p, &store)
        }

        (Method::PUT, p) if p.starts_with("/api/v1/string/") && config.enable_rest => {
            let body = read_body(req, config.max_body_size).await?;
            handle_put_string(p, &body, &store)?
        }

        (Method::DELETE, p) if p.starts_with("/api/v1/keys/") && config.enable_rest => {
            handle_delete_key(p, &store)
        }

        (Method::GET, "/api/v1/keys") if config.enable_rest => handle_scan_keys(&query, &store),

        (Method::GET, "/api/v1/info") if config.enable_rest => handle_info(&store),

        (Method::POST, "/api/v1/query") if config.enable_rest => {
            let body = read_body(req, config.max_body_size).await?;
            handle_query(&body, &store)?
        }

        (Method::GET, "/api/docs") if config.enable_rest => handle_openapi_docs(),

        // GraphQL endpoints
        (Method::POST, "/graphql") if config.enable_graphql => {
            let body = read_body(req, config.max_body_size).await?;
            handle_graphql_query(&body)?
        }

        (Method::GET, "/graphql/schema") if config.enable_graphql => handle_graphql_schema(),

        // Not found
        _ => error_response(StatusCode::NOT_FOUND, "Not found"),
    };

    if config.enable_cors {
        cors_headers(&mut response);
    }

    Ok(response)
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

fn handle_get_string(path: &str, store: &Store) -> Response<Full<Bytes>> {
    let key = match extract_path_param(path, "/api/v1/string/") {
        Some(k) => k,
        None => return error_response(StatusCode::BAD_REQUEST, "Missing key parameter"),
    };

    match store.get(0, &Bytes::from(key.clone())) {
        Some(Value::String(v)) => {
            let ttl = store.ttl(0, &Bytes::from(key.clone())).unwrap_or(-1);
            let value_str = String::from_utf8_lossy(&v).to_string();
            json_response(
                StatusCode::OK,
                serde_json::json!({ "key": key, "value": value_str, "ttl": ttl }),
            )
        }
        Some(_) => error_response(
            StatusCode::BAD_REQUEST,
            "Key exists but is not a string type",
        ),
        None => error_response(StatusCode::NOT_FOUND, "Key not found"),
    }
}

fn handle_put_string(
    path: &str,
    body: &[u8],
    store: &Store,
) -> Result<Response<Full<Bytes>>, GatewayError> {
    let key = extract_path_param(path, "/api/v1/string/")
        .ok_or_else(|| GatewayError::InvalidRequest("Missing key parameter".into()))?;

    let parsed: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| GatewayError::InvalidRequest(format!("Invalid JSON: {}", e)))?;

    let value = parsed
        .get("value")
        .and_then(|v| v.as_str())
        .ok_or_else(|| GatewayError::InvalidRequest("Missing 'value' field".into()))?;

    let ttl = parsed.get("ttl").and_then(|v| v.as_i64());

    match ttl {
        Some(seconds) if seconds > 0 => {
            let expires_at =
                std::time::SystemTime::now() + std::time::Duration::from_secs(seconds as u64);
            store.set_with_expiry(
                0,
                Bytes::from(key.clone()),
                Value::String(Bytes::from(value.to_string())),
                expires_at,
            );
        }
        _ => {
            store.set(
                0,
                Bytes::from(key.clone()),
                Value::String(Bytes::from(value.to_string())),
            );
        }
    }

    Ok(json_response(
        StatusCode::OK,
        serde_json::json!({ "status": "OK", "key": key }),
    ))
}

fn handle_delete_key(path: &str, store: &Store) -> Response<Full<Bytes>> {
    let key = match extract_path_param(path, "/api/v1/keys/") {
        Some(k) => k,
        None => return error_response(StatusCode::BAD_REQUEST, "Missing key parameter"),
    };

    let deleted = store.del(0, &[Bytes::from(key.clone())]);
    json_response(
        StatusCode::OK,
        serde_json::json!({ "deleted": deleted, "key": key }),
    )
}

fn handle_scan_keys(query: &str, store: &Store) -> Response<Full<Bytes>> {
    let params = parse_query_params(query);
    let pattern = params.get("pattern").map(|s| s.as_str()).unwrap_or("*");
    let cursor: usize = params
        .get("cursor")
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let count: usize = params
        .get("count")
        .and_then(|c| c.parse().ok())
        .unwrap_or(10);

    let all_keys = store.keys(0);

    // Simple glob-style pattern matching
    let matched: Vec<String> = all_keys
        .iter()
        .map(|k| String::from_utf8_lossy(k).to_string())
        .filter(|k| matches_pattern(k, pattern))
        .collect();

    let total = matched.len();
    let start = cursor.min(total);
    let end = (start + count).min(total);
    let page = &matched[start..end];
    let next_cursor = if end < total { end } else { 0 };

    json_response(
        StatusCode::OK,
        serde_json::json!({
            "cursor": next_cursor,
            "keys": page,
            "total": total,
        }),
    )
}

fn handle_info(store: &Store) -> Response<Full<Bytes>> {
    let key_count = store.key_count(0);
    json_response(
        StatusCode::OK,
        serde_json::json!({
            "server": "ferrite",
            "version": "0.1.0",
            "keys": key_count,
            "backend": format!("{:?}", store.backend_type()),
        }),
    )
}

fn handle_query(body: &[u8], store: &Store) -> Result<Response<Full<Bytes>>, GatewayError> {
    let parsed: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| GatewayError::InvalidRequest(format!("Invalid JSON: {}", e)))?;

    let command = parsed
        .get("command")
        .and_then(|v| v.as_str())
        .ok_or_else(|| GatewayError::InvalidRequest("Missing 'command' field".into()))?
        .to_uppercase();

    let args: Vec<String> = parsed
        .get("args")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let response = match command.as_str() {
        "GET" => {
            if args.is_empty() {
                return Err(GatewayError::InvalidRequest(
                    "GET requires a key argument".into(),
                ));
            }
            match store.get(0, &Bytes::from(args[0].clone())) {
                Some(Value::String(v)) => {
                    serde_json::json!({ "result": String::from_utf8_lossy(&v).to_string() })
                }
                Some(_) => serde_json::json!({ "result": null, "error": "WRONGTYPE" }),
                None => serde_json::json!({ "result": null }),
            }
        }
        "SET" => {
            if args.len() < 2 {
                return Err(GatewayError::InvalidRequest(
                    "SET requires key and value arguments".into(),
                ));
            }
            store.set(
                0,
                Bytes::from(args[0].clone()),
                Value::String(Bytes::from(args[1].clone())),
            );
            serde_json::json!({ "result": "OK" })
        }
        "DEL" => {
            if args.is_empty() {
                return Err(GatewayError::InvalidRequest(
                    "DEL requires at least one key argument".into(),
                ));
            }
            let keys: Vec<Bytes> = args.iter().map(|a| Bytes::from(a.clone())).collect();
            let deleted = store.del(0, &keys);
            serde_json::json!({ "result": deleted })
        }
        "EXISTS" => {
            if args.is_empty() {
                return Err(GatewayError::InvalidRequest(
                    "EXISTS requires at least one key argument".into(),
                ));
            }
            let keys: Vec<Bytes> = args.iter().map(|a| Bytes::from(a.clone())).collect();
            let count = store.exists(0, &keys);
            serde_json::json!({ "result": count })
        }
        _ => {
            return Err(GatewayError::InvalidRequest(format!(
                "Unsupported command: {}",
                command
            )));
        }
    };

    Ok(json_response(StatusCode::OK, response))
}

fn handle_openapi_docs() -> Response<Full<Bytes>> {
    let spec = serde_json::json!({
        "openapi": "3.0.3",
        "info": {
            "title": "Ferrite API",
            "version": "0.1.0",
            "description": "REST API for the Ferrite key-value store"
        },
        "paths": {
            "/health": {
                "get": {
                    "summary": "Health check",
                    "responses": { "200": { "description": "Server is healthy" } }
                }
            },
            "/api/v1/string/{key}": {
                "get": {
                    "summary": "Get a string value by key",
                    "parameters": [{ "name": "key", "in": "path", "required": true, "schema": { "type": "string" } }],
                    "responses": { "200": { "description": "Key value and TTL" } }
                },
                "put": {
                    "summary": "Set a string value",
                    "parameters": [{ "name": "key", "in": "path", "required": true, "schema": { "type": "string" } }],
                    "requestBody": {
                        "content": { "application/json": { "schema": { "type": "object", "properties": { "value": { "type": "string" }, "ttl": { "type": "integer" } }, "required": ["value"] } } }
                    },
                    "responses": { "200": { "description": "Key set successfully" } }
                }
            },
            "/api/v1/keys/{key}": {
                "delete": {
                    "summary": "Delete a key",
                    "parameters": [{ "name": "key", "in": "path", "required": true, "schema": { "type": "string" } }],
                    "responses": { "200": { "description": "Key deleted" } }
                }
            },
            "/api/v1/keys": {
                "get": {
                    "summary": "Scan keys",
                    "parameters": [
                        { "name": "pattern", "in": "query", "schema": { "type": "string", "default": "*" } },
                        { "name": "cursor", "in": "query", "schema": { "type": "integer", "default": 0 } },
                        { "name": "count", "in": "query", "schema": { "type": "integer", "default": 10 } }
                    ],
                    "responses": { "200": { "description": "List of matching keys" } }
                }
            },
            "/api/v1/info": {
                "get": {
                    "summary": "Server information",
                    "responses": { "200": { "description": "Server info" } }
                }
            },
            "/api/v1/query": {
                "post": {
                    "summary": "Execute a generic command",
                    "requestBody": {
                        "content": { "application/json": { "schema": { "type": "object", "properties": { "command": { "type": "string" }, "args": { "type": "array", "items": { "type": "string" } } }, "required": ["command"] } } }
                    },
                    "responses": { "200": { "description": "Command result" } }
                }
            }
        }
    });
    json_response(StatusCode::OK, spec)
}

fn handle_graphql_query(body: &[u8]) -> Result<Response<Full<Bytes>>, GatewayError> {
    let request: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| GatewayError::InvalidRequest(format!("Invalid JSON: {}", e)))?;

    let query = request
        .get("query")
        .and_then(|v| v.as_str())
        .ok_or_else(|| GatewayError::InvalidRequest("Missing 'query' field".into()))?;

    // Introspection-only: return type info for now
    let response = if query.contains("__schema") || query.contains("__type") {
        serde_json::json!({
            "data": {
                "__schema": {
                    "queryType": { "name": "Query" },
                    "types": [
                        { "name": "Query", "kind": "OBJECT" },
                        { "name": "String", "kind": "SCALAR" },
                        { "name": "Int", "kind": "SCALAR" },
                        { "name": "Boolean", "kind": "SCALAR" }
                    ]
                }
            }
        })
    } else {
        serde_json::json!({
            "data": null,
            "errors": [{
                "message": "GraphQL execution engine not yet integrated",
                "locations": [],
                "path": []
            }]
        })
    };

    Ok(json_response(StatusCode::OK, response))
}

fn handle_graphql_schema() -> Response<Full<Bytes>> {
    let sdl = r#"type Query {
  get(key: String!): StringValue
  keys(pattern: String, cursor: Int, count: Int): KeyScan
  info: ServerInfo
}

type Mutation {
  set(key: String!, value: String!, ttl: Int): SetResult
  del(key: String!): DelResult
}

type StringValue {
  key: String!
  value: String
  ttl: Int
}

type KeyScan {
  cursor: Int!
  keys: [String!]!
  total: Int!
}

type ServerInfo {
  server: String!
  version: String!
  keys: Int!
}

type SetResult {
  status: String!
  key: String!
}

type DelResult {
  deleted: Int!
  key: String!
}
"#;
    json_response(StatusCode::OK, serde_json::json!({ "sdl": sdl }))
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

/// Build a JSON response with the given status code and body.
fn json_response(status: StatusCode, body: serde_json::Value) -> Response<Full<Bytes>> {
    let json = serde_json::to_vec(&body).unwrap_or_default();
    let mut resp = Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(json)))
        .unwrap_or_else(|_| {
            Response::new(Full::new(Bytes::from(
                r#"{"error":"Failed to build response"}"#,
            )))
        });
    resp
}

/// Build a JSON error response.
fn error_response(status: StatusCode, message: &str) -> Response<Full<Bytes>> {
    json_response(status, serde_json::json!({ "error": message }))
}

/// Add CORS headers to a response.
fn cors_headers(response: &mut Response<Full<Bytes>>) {
    let headers = response.headers_mut();
    headers.insert(
        "Access-Control-Allow-Origin",
        "*".parse()
            .unwrap_or_else(|_| hyper::header::HeaderValue::from_static("*")),
    );
    headers.insert(
        "Access-Control-Allow-Methods",
        "GET, POST, PUT, DELETE, OPTIONS"
            .parse()
            .unwrap_or_else(|_| {
                hyper::header::HeaderValue::from_static("GET, POST, PUT, DELETE, OPTIONS")
            }),
    );
    headers.insert(
        "Access-Control-Allow-Headers",
        "Content-Type, Authorization, X-API-Key"
            .parse()
            .unwrap_or_else(|_| {
                hyper::header::HeaderValue::from_static("Content-Type, Authorization, X-API-Key")
            }),
    );
    headers.insert(
        "Access-Control-Max-Age",
        "86400"
            .parse()
            .unwrap_or_else(|_| hyper::header::HeaderValue::from_static("86400")),
    );
}

// ---------------------------------------------------------------------------
// Path & query parsing
// ---------------------------------------------------------------------------

/// Extract a path parameter value given a prefix pattern.
///
/// For example, `extract_path_param("/api/v1/string/mykey", "/api/v1/string/")`
/// returns `Some("mykey")`.
fn extract_path_param(path: &str, pattern: &str) -> Option<String> {
    path.strip_prefix(pattern)
        .map(|s| s.trim_end_matches('/'))
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

/// Parse URL query parameters into a map.
fn parse_query_params(query: &str) -> HashMap<String, String> {
    query
        .split('&')
        .filter(|s| !s.is_empty())
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next()?;
            let value = parts.next().unwrap_or("");
            Some((key.to_string(), value.to_string()))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Body reading
// ---------------------------------------------------------------------------

/// Read the full request body, enforcing a size limit.
async fn read_body(req: Request<Incoming>, max_size: usize) -> Result<Vec<u8>, GatewayError> {
    use http_body_util::BodyExt;

    let body = req
        .into_body()
        .collect()
        .await
        .map_err(|e| GatewayError::InternalError(format!("Failed to read body: {}", e)))?
        .to_bytes();

    if body.len() > max_size {
        return Err(GatewayError::BodyTooLarge(max_size));
    }

    Ok(body.to_vec())
}

// ---------------------------------------------------------------------------
// Pattern matching helper
// ---------------------------------------------------------------------------

/// Simple glob-style pattern matching (supports `*` and `?`).
fn matches_pattern(s: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    let mut si = s.chars().peekable();
    let mut pi = pattern.chars().peekable();

    matches_pattern_inner(
        &mut s.chars().collect::<Vec<_>>(),
        &pattern.chars().collect::<Vec<_>>(),
        0,
        0,
    )
}

fn matches_pattern_inner(s: &[char], p: &[char], si: usize, pi: usize) -> bool {
    if pi == p.len() {
        return si == s.len();
    }

    if p[pi] == '*' {
        // Try matching zero or more characters
        for i in si..=s.len() {
            if matches_pattern_inner(s, p, i, pi + 1) {
                return true;
            }
        }
        return false;
    }

    if si < s.len() && (p[pi] == '?' || p[pi] == s[si]) {
        return matches_pattern_inner(s, p, si + 1, pi + 1);
    }

    false
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_path_param() {
        assert_eq!(
            extract_path_param("/api/v1/string/mykey", "/api/v1/string/"),
            Some("mykey".to_string())
        );
        assert_eq!(
            extract_path_param("/api/v1/string/", "/api/v1/string/"),
            None
        );
        assert_eq!(
            extract_path_param("/api/v1/keys/foo", "/api/v1/keys/"),
            Some("foo".to_string())
        );
        assert_eq!(extract_path_param("/other/path", "/api/v1/string/"), None);
    }

    #[test]
    fn test_parse_query_params() {
        let params = parse_query_params("pattern=*&cursor=0&count=10");
        assert_eq!(params.get("pattern"), Some(&"*".to_string()));
        assert_eq!(params.get("cursor"), Some(&"0".to_string()));
        assert_eq!(params.get("count"), Some(&"10".to_string()));
    }

    #[test]
    fn test_parse_query_params_empty() {
        let params = parse_query_params("");
        assert!(params.is_empty());
    }

    #[test]
    fn test_matches_pattern_wildcard() {
        assert!(matches_pattern("hello", "*"));
        assert!(matches_pattern("", "*"));
    }

    #[test]
    fn test_matches_pattern_prefix() {
        assert!(matches_pattern("user:123", "user:*"));
        assert!(!matches_pattern("admin:123", "user:*"));
    }

    #[test]
    fn test_matches_pattern_question_mark() {
        assert!(matches_pattern("abc", "a?c"));
        assert!(!matches_pattern("abbc", "a?c"));
    }

    #[test]
    fn test_matches_pattern_exact() {
        assert!(matches_pattern("hello", "hello"));
        assert!(!matches_pattern("hello", "world"));
    }

    #[test]
    fn test_default_config() {
        let config = HttpGatewayConfig::default();
        assert_eq!(config.bind, "127.0.0.1");
        assert_eq!(config.port, 8080);
        assert!(config.enable_rest);
        assert!(config.enable_graphql);
        assert!(config.enable_cors);
        assert!(!config.auth_enabled);
        assert_eq!(config.max_body_size, 1_048_576);
    }

    #[test]
    fn test_json_response() {
        let resp = json_response(StatusCode::OK, serde_json::json!({"key": "value"}));
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_error_response() {
        let resp = error_response(StatusCode::NOT_FOUND, "Not found");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn test_cors_headers() {
        let mut resp = json_response(StatusCode::OK, serde_json::json!({}));
        cors_headers(&mut resp);
        assert!(resp.headers().contains_key("Access-Control-Allow-Origin"));
        assert!(resp.headers().contains_key("Access-Control-Allow-Methods"));
        assert!(resp.headers().contains_key("Access-Control-Allow-Headers"));
    }
}
