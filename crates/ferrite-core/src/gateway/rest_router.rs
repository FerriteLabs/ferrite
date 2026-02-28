#![allow(dead_code)]
//! REST API router for Ferrite
//!
//! Provides a complete REST interface for all data types with
//! OpenAPI 3.0 spec generation and JSON request/response handling.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Route definitions
// ---------------------------------------------------------------------------

/// HTTP method for REST routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RestMethod {
    /// HTTP GET
    Get,
    /// HTTP POST
    Post,
    /// HTTP PUT
    Put,
    /// HTTP DELETE
    Delete,
}

impl std::fmt::Display for RestMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Get => write!(f, "GET"),
            Self::Post => write!(f, "POST"),
            Self::Put => write!(f, "PUT"),
            Self::Delete => write!(f, "DELETE"),
        }
    }
}

/// A single REST route definition.
#[derive(Debug, Clone)]
pub struct RestRoute {
    /// HTTP method for this route
    pub method: RestMethod,
    /// URL path pattern (e.g. `/api/v1/string/{key}`)
    pub path: String,
    /// Handler function name used for dispatch
    pub handler_fn: String,
    /// Human-readable description
    pub description: String,
    /// JSON Schema for the request body (if applicable)
    pub request_schema: Option<serde_json::Value>,
    /// JSON Schema for the response body
    pub response_schema: Option<serde_json::Value>,
}

/// Incoming REST request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestRequest {
    /// HTTP method
    pub method: RestMethod,
    /// Request path (e.g. `/api/v1/string/mykey`)
    pub path: String,
    /// Query parameters
    pub query_params: HashMap<String, String>,
    /// Request body as JSON
    pub body: serde_json::Value,
    /// HTTP headers
    pub headers: HashMap<String, String>,
}

/// Outgoing REST response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestResponse {
    /// HTTP status code
    pub status_code: u16,
    /// Response body as JSON
    pub body: serde_json::Value,
    /// Response headers
    pub headers: HashMap<String, String>,
}

impl RestResponse {
    /// Create a successful JSON response.
    pub fn ok(body: serde_json::Value) -> Self {
        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        Self {
            status_code: 200,
            body,
            headers,
        }
    }

    /// Create a 201 Created response.
    pub fn created(body: serde_json::Value) -> Self {
        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        Self {
            status_code: 201,
            body,
            headers,
        }
    }

    /// Create a 404 Not Found response.
    pub fn not_found(message: &str) -> Self {
        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        Self {
            status_code: 404,
            body: serde_json::json!({"error": "not_found", "message": message}),
            headers,
        }
    }

    /// Create a 400 Bad Request response.
    pub fn bad_request(message: &str) -> Self {
        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        Self {
            status_code: 400,
            body: serde_json::json!({"error": "bad_request", "message": message}),
            headers,
        }
    }

    /// Create a 405 Method Not Allowed response.
    pub fn method_not_allowed() -> Self {
        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        Self {
            status_code: 405,
            body: serde_json::json!({"error": "method_not_allowed", "message": "Method not allowed"}),
            headers,
        }
    }

    /// Create a 500 Internal Server Error response.
    pub fn internal_error(message: &str) -> Self {
        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        Self {
            status_code: 500,
            body: serde_json::json!({"error": "internal_error", "message": message}),
            headers,
        }
    }
}

// ---------------------------------------------------------------------------
// Matched route result
// ---------------------------------------------------------------------------

/// Result of matching a request to a route.
#[derive(Debug, Clone)]
struct MatchedRoute {
    /// The matched route definition
    route: RestRoute,
    /// Path parameters extracted from the URL (e.g. `{key}` → value)
    path_params: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// REST Router
// ---------------------------------------------------------------------------

/// Production REST API router for Ferrite.
///
/// Contains route definitions for all Ferrite data types (strings, lists,
/// hashes, sets, sorted sets) plus server-level endpoints. Supports route
/// matching with path parameter extraction and OpenAPI 3.0 spec generation.
pub struct RestRouter {
    /// All registered routes keyed by `"METHOD /path"` for display
    routes: Vec<RestRoute>,
}

impl RestRouter {
    /// Create a new router with all built-in Ferrite routes registered.
    pub fn new() -> Self {
        let mut router = Self { routes: Vec::new() };
        router.register_builtin_routes();
        router
    }

    /// Return all registered routes.
    pub fn routes(&self) -> &[RestRoute] {
        &self.routes
    }

    /// Route an incoming request and return the response.
    ///
    /// Matches the request method and path against registered routes,
    /// extracts path parameters, and dispatches to the appropriate handler.
    pub fn route(&self, request: &RestRequest) -> RestResponse {
        match self.match_route(request) {
            Some(matched) => self.dispatch(&matched, request),
            None => RestResponse::not_found(&format!("No route for {} {}", request.method, request.path)),
        }
    }

    /// Generate a full OpenAPI 3.0 specification as JSON.
    pub fn generate_openapi_spec(&self) -> serde_json::Value {
        let mut paths = serde_json::Map::new();

        for route in &self.routes {
            let path_entry = paths
                .entry(route.path.clone())
                .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));

            let method_key = route.method.to_string().to_lowercase();
            let mut operation = serde_json::Map::new();
            operation.insert("summary".to_string(), serde_json::Value::String(route.description.clone()));
            operation.insert("operationId".to_string(), serde_json::Value::String(route.handler_fn.clone()));

            // Extract path parameters from pattern
            let params: Vec<serde_json::Value> = extract_param_names(&route.path)
                .iter()
                .map(|name| {
                    serde_json::json!({
                        "name": name,
                        "in": "path",
                        "required": true,
                        "schema": {"type": "string"}
                    })
                })
                .collect();

            if !params.is_empty() {
                operation.insert("parameters".to_string(), serde_json::Value::Array(params));
            }

            if let Some(ref req_schema) = route.request_schema {
                operation.insert(
                    "requestBody".to_string(),
                    serde_json::json!({
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": req_schema
                            }
                        }
                    }),
                );
            }

            let mut responses = serde_json::Map::new();
            let resp_schema = route.response_schema.clone().unwrap_or(serde_json::json!({"type": "object"}));
            responses.insert(
                "200".to_string(),
                serde_json::json!({
                    "description": "Successful response",
                    "content": {
                        "application/json": {
                            "schema": resp_schema
                        }
                    }
                }),
            );
            responses.insert(
                "400".to_string(),
                serde_json::json!({"description": "Bad request"}),
            );
            responses.insert(
                "404".to_string(),
                serde_json::json!({"description": "Not found"}),
            );
            operation.insert("responses".to_string(), serde_json::Value::Object(responses));

            // Tag from path segment
            let tag = route
                .path
                .split('/')
                .nth(3)
                .unwrap_or("general")
                .to_string();
            operation.insert("tags".to_string(), serde_json::json!([tag]));

            if let serde_json::Value::Object(ref mut map) = path_entry {
                map.insert(method_key, serde_json::Value::Object(operation));
            }
        }

        serde_json::json!({
            "openapi": "3.0.0",
            "info": {
                "title": "Ferrite REST API",
                "version": "1.0.0",
                "description": "REST API for the Ferrite key-value store"
            },
            "paths": paths,
            "components": {
                "schemas": self.component_schemas()
            }
        })
    }

    // -----------------------------------------------------------------------
    // Internal: route registration
    // -----------------------------------------------------------------------

    fn register_builtin_routes(&mut self) {
        // -- Strings --
        self.add_route(RestMethod::Get, "/api/v1/string/{key}", "get_string", "Get a string value by key", None, Some(serde_json::json!({"type": "object", "properties": {"key": {"type": "string"}, "value": {"type": "string"}, "ttl": {"type": "integer"}}})));
        self.add_route(RestMethod::Put, "/api/v1/string/{key}", "put_string", "Set a string value", Some(serde_json::json!({"type": "object", "properties": {"value": {"type": "string"}, "ttl": {"type": "integer"}}, "required": ["value"]})), Some(serde_json::json!({"type": "object", "properties": {"ok": {"type": "boolean"}}})));
        self.add_route(RestMethod::Delete, "/api/v1/string/{key}", "delete_string", "Delete a string key", None, Some(serde_json::json!({"type": "object", "properties": {"deleted": {"type": "integer"}}})));

        // -- Lists --
        self.add_route(RestMethod::Get, "/api/v1/list/{key}", "get_list", "Get list elements (query: start, stop)", None, Some(serde_json::json!({"type": "object", "properties": {"key": {"type": "string"}, "values": {"type": "array", "items": {"type": "string"}}, "length": {"type": "integer"}}})));
        self.add_route(RestMethod::Post, "/api/v1/list/{key}/push", "list_push", "Push values onto a list", Some(serde_json::json!({"type": "object", "properties": {"values": {"type": "array", "items": {"type": "string"}}, "direction": {"type": "string", "enum": ["left", "right"]}}, "required": ["values"]})), Some(serde_json::json!({"type": "object", "properties": {"length": {"type": "integer"}}})));
        self.add_route(RestMethod::Post, "/api/v1/list/{key}/pop", "list_pop", "Pop a value from a list", Some(serde_json::json!({"type": "object", "properties": {"direction": {"type": "string", "enum": ["left", "right"]}, "count": {"type": "integer"}}})), Some(serde_json::json!({"type": "object", "properties": {"values": {"type": "array", "items": {"type": "string"}}}})));

        // -- Hashes --
        self.add_route(RestMethod::Get, "/api/v1/hash/{key}", "get_hash", "Get all fields of a hash", None, Some(serde_json::json!({"type": "object", "properties": {"key": {"type": "string"}, "fields": {"type": "object"}}})));
        self.add_route(RestMethod::Get, "/api/v1/hash/{key}/{field}", "get_hash_field", "Get a single hash field", None, Some(serde_json::json!({"type": "object", "properties": {"key": {"type": "string"}, "field": {"type": "string"}, "value": {"type": "string"}}})));
        self.add_route(RestMethod::Put, "/api/v1/hash/{key}", "put_hash", "Set hash fields", Some(serde_json::json!({"type": "object", "properties": {"fields": {"type": "object"}}, "required": ["fields"]})), Some(serde_json::json!({"type": "object", "properties": {"added": {"type": "integer"}}})));
        self.add_route(RestMethod::Delete, "/api/v1/hash/{key}/{field}", "delete_hash_field", "Delete a hash field", None, Some(serde_json::json!({"type": "object", "properties": {"deleted": {"type": "integer"}}})));

        // -- Sets --
        self.add_route(RestMethod::Get, "/api/v1/set/{key}", "get_set", "Get all members of a set", None, Some(serde_json::json!({"type": "object", "properties": {"key": {"type": "string"}, "members": {"type": "array", "items": {"type": "string"}}, "size": {"type": "integer"}}})));
        self.add_route(RestMethod::Post, "/api/v1/set/{key}/add", "set_add", "Add members to a set", Some(serde_json::json!({"type": "object", "properties": {"members": {"type": "array", "items": {"type": "string"}}}, "required": ["members"]})), Some(serde_json::json!({"type": "object", "properties": {"added": {"type": "integer"}}})));
        self.add_route(RestMethod::Delete, "/api/v1/set/{key}/{member}", "set_remove", "Remove a member from a set", None, Some(serde_json::json!({"type": "object", "properties": {"removed": {"type": "integer"}}})));

        // -- Sorted Sets --
        self.add_route(RestMethod::Get, "/api/v1/zset/{key}", "get_sorted_set", "Get sorted set members (query: min, max, withscores, limit)", None, Some(serde_json::json!({"type": "object", "properties": {"key": {"type": "string"}, "members": {"type": "array", "items": {"type": "object", "properties": {"value": {"type": "string"}, "score": {"type": "number"}}}}}})));
        self.add_route(RestMethod::Post, "/api/v1/zset/{key}/add", "sorted_set_add", "Add members with scores to a sorted set", Some(serde_json::json!({"type": "object", "properties": {"members": {"type": "array", "items": {"type": "object", "properties": {"value": {"type": "string"}, "score": {"type": "number"}}, "required": ["value", "score"]}}}, "required": ["members"]})), Some(serde_json::json!({"type": "object", "properties": {"added": {"type": "integer"}}})));

        // -- Keys --
        self.add_route(RestMethod::Get, "/api/v1/keys", "scan_keys", "Scan keys matching a pattern (query: pattern, cursor, count)", None, Some(serde_json::json!({"type": "object", "properties": {"cursor": {"type": "integer"}, "keys": {"type": "array", "items": {"type": "string"}}}})));
        self.add_route(RestMethod::Delete, "/api/v1/keys/{key}", "delete_key", "Delete a key", None, Some(serde_json::json!({"type": "object", "properties": {"deleted": {"type": "integer"}}})));
        self.add_route(RestMethod::Post, "/api/v1/keys/{key}/expire", "expire_key", "Set TTL on a key", Some(serde_json::json!({"type": "object", "properties": {"ttl": {"type": "integer"}}, "required": ["ttl"]})), Some(serde_json::json!({"type": "object", "properties": {"ok": {"type": "boolean"}}})));

        // -- Server --
        self.add_route(RestMethod::Get, "/api/v1/info", "server_info", "Get server information", None, Some(serde_json::json!({"type": "object", "properties": {"version": {"type": "string"}, "uptime_seconds": {"type": "integer"}, "connected_clients": {"type": "integer"}, "used_memory_bytes": {"type": "integer"}}})));
        self.add_route(RestMethod::Get, "/api/v1/health", "health_check", "Health check endpoint", None, Some(serde_json::json!({"type": "object", "properties": {"status": {"type": "string"}, "version": {"type": "string"}}})));
        self.add_route(RestMethod::Post, "/api/v1/query", "execute_command", "Execute a raw Ferrite command", Some(serde_json::json!({"type": "object", "properties": {"command": {"type": "string"}, "args": {"type": "array", "items": {"type": "string"}}}, "required": ["command"]})), Some(serde_json::json!({"type": "object", "properties": {"result": {}}})));
    }

    fn add_route(
        &mut self,
        method: RestMethod,
        path: &str,
        handler_fn: &str,
        description: &str,
        request_schema: Option<serde_json::Value>,
        response_schema: Option<serde_json::Value>,
    ) {
        self.routes.push(RestRoute {
            method,
            path: path.to_string(),
            handler_fn: handler_fn.to_string(),
            description: description.to_string(),
            request_schema,
            response_schema,
        });
    }

    // -----------------------------------------------------------------------
    // Internal: route matching
    // -----------------------------------------------------------------------

    fn match_route(&self, request: &RestRequest) -> Option<MatchedRoute> {
        for route in &self.routes {
            if route.method != request.method {
                continue;
            }
            if let Some(params) = match_path_pattern(&route.path, &request.path) {
                return Some(MatchedRoute {
                    route: route.clone(),
                    path_params: params,
                });
            }
        }
        None
    }

    // -----------------------------------------------------------------------
    // Internal: dispatch
    // -----------------------------------------------------------------------

    fn dispatch(&self, matched: &MatchedRoute, request: &RestRequest) -> RestResponse {
        match matched.route.handler_fn.as_str() {
            // Strings
            "get_string" => self.handle_get_string(&matched.path_params),
            "put_string" => self.handle_put_string(&matched.path_params, &request.body),
            "delete_string" => self.handle_delete_string(&matched.path_params),
            // Lists
            "get_list" => self.handle_get_list(&matched.path_params, &request.query_params),
            "list_push" => self.handle_list_push(&matched.path_params, &request.body),
            "list_pop" => self.handle_list_pop(&matched.path_params, &request.body),
            // Hashes
            "get_hash" => self.handle_get_hash(&matched.path_params),
            "get_hash_field" => self.handle_get_hash_field(&matched.path_params),
            "put_hash" => self.handle_put_hash(&matched.path_params, &request.body),
            "delete_hash_field" => self.handle_delete_hash_field(&matched.path_params),
            // Sets
            "get_set" => self.handle_get_set(&matched.path_params),
            "set_add" => self.handle_set_add(&matched.path_params, &request.body),
            "set_remove" => self.handle_set_remove(&matched.path_params),
            // Sorted sets
            "get_sorted_set" => self.handle_get_sorted_set(&matched.path_params, &request.query_params),
            "sorted_set_add" => self.handle_sorted_set_add(&matched.path_params, &request.body),
            // Keys
            "scan_keys" => self.handle_scan_keys(&request.query_params),
            "delete_key" => self.handle_delete_key(&matched.path_params),
            "expire_key" => self.handle_expire_key(&matched.path_params, &request.body),
            // Server
            "server_info" => self.handle_server_info(),
            "health_check" => self.handle_health_check(),
            "execute_command" => self.handle_execute_command(&request.body),
            _ => RestResponse::not_found("Unknown handler"),
        }
    }

    // -----------------------------------------------------------------------
    // Handlers — stub implementations (storage integration pending)
    // -----------------------------------------------------------------------

    fn handle_get_string(&self, params: &HashMap<String, String>) -> RestResponse {
        let key = params.get("key").cloned().unwrap_or_default();
        RestResponse::ok(serde_json::json!({
            "key": key,
            "value": null,
            "ttl": -1,
            "note": "Store integration pending"
        }))
    }

    fn handle_put_string(&self, params: &HashMap<String, String>, body: &serde_json::Value) -> RestResponse {
        let key = params.get("key").cloned().unwrap_or_default();
        let value = body.get("value").and_then(|v| v.as_str()).unwrap_or("");
        let _ttl = body.get("ttl").and_then(|v| v.as_i64());
        RestResponse::ok(serde_json::json!({
            "ok": true,
            "key": key,
            "value": value,
            "note": "Store integration pending"
        }))
    }

    fn handle_delete_string(&self, params: &HashMap<String, String>) -> RestResponse {
        let key = params.get("key").cloned().unwrap_or_default();
        RestResponse::ok(serde_json::json!({"deleted": 1, "key": key, "note": "Store integration pending"}))
    }

    fn handle_get_list(&self, params: &HashMap<String, String>, query: &HashMap<String, String>) -> RestResponse {
        let key = params.get("key").cloned().unwrap_or_default();
        let _start: i64 = query.get("start").and_then(|s| s.parse().ok()).unwrap_or(0);
        let _stop: i64 = query.get("stop").and_then(|s| s.parse().ok()).unwrap_or(-1);
        RestResponse::ok(serde_json::json!({"key": key, "values": [], "length": 0, "note": "Store integration pending"}))
    }

    fn handle_list_push(&self, params: &HashMap<String, String>, body: &serde_json::Value) -> RestResponse {
        let key = params.get("key").cloned().unwrap_or_default();
        let values = body.get("values").cloned().unwrap_or(serde_json::json!([]));
        let _direction = body.get("direction").and_then(|v| v.as_str()).unwrap_or("right");
        let count = values.as_array().map(|a| a.len()).unwrap_or(0);
        RestResponse::ok(serde_json::json!({"key": key, "length": count, "note": "Store integration pending"}))
    }

    fn handle_list_pop(&self, params: &HashMap<String, String>, body: &serde_json::Value) -> RestResponse {
        let key = params.get("key").cloned().unwrap_or_default();
        let _direction = body.get("direction").and_then(|v| v.as_str()).unwrap_or("right");
        RestResponse::ok(serde_json::json!({"key": key, "values": [], "note": "Store integration pending"}))
    }

    fn handle_get_hash(&self, params: &HashMap<String, String>) -> RestResponse {
        let key = params.get("key").cloned().unwrap_or_default();
        RestResponse::ok(serde_json::json!({"key": key, "fields": {}, "note": "Store integration pending"}))
    }

    fn handle_get_hash_field(&self, params: &HashMap<String, String>) -> RestResponse {
        let key = params.get("key").cloned().unwrap_or_default();
        let field = params.get("field").cloned().unwrap_or_default();
        RestResponse::ok(serde_json::json!({"key": key, "field": field, "value": null, "note": "Store integration pending"}))
    }

    fn handle_put_hash(&self, params: &HashMap<String, String>, body: &serde_json::Value) -> RestResponse {
        let key = params.get("key").cloned().unwrap_or_default();
        let fields = body.get("fields").cloned().unwrap_or(serde_json::json!({}));
        let count = fields.as_object().map(|o| o.len()).unwrap_or(0);
        RestResponse::ok(serde_json::json!({"key": key, "added": count, "note": "Store integration pending"}))
    }

    fn handle_delete_hash_field(&self, params: &HashMap<String, String>) -> RestResponse {
        let key = params.get("key").cloned().unwrap_or_default();
        let field = params.get("field").cloned().unwrap_or_default();
        RestResponse::ok(serde_json::json!({"key": key, "field": field, "deleted": 1, "note": "Store integration pending"}))
    }

    fn handle_get_set(&self, params: &HashMap<String, String>) -> RestResponse {
        let key = params.get("key").cloned().unwrap_or_default();
        RestResponse::ok(serde_json::json!({"key": key, "members": [], "size": 0, "note": "Store integration pending"}))
    }

    fn handle_set_add(&self, params: &HashMap<String, String>, body: &serde_json::Value) -> RestResponse {
        let key = params.get("key").cloned().unwrap_or_default();
        let members = body.get("members").cloned().unwrap_or(serde_json::json!([]));
        let count = members.as_array().map(|a| a.len()).unwrap_or(0);
        RestResponse::ok(serde_json::json!({"key": key, "added": count, "note": "Store integration pending"}))
    }

    fn handle_set_remove(&self, params: &HashMap<String, String>) -> RestResponse {
        let key = params.get("key").cloned().unwrap_or_default();
        let member = params.get("member").cloned().unwrap_or_default();
        RestResponse::ok(serde_json::json!({"key": key, "member": member, "removed": 1, "note": "Store integration pending"}))
    }

    fn handle_get_sorted_set(&self, params: &HashMap<String, String>, query: &HashMap<String, String>) -> RestResponse {
        let key = params.get("key").cloned().unwrap_or_default();
        let _min: f64 = query.get("min").and_then(|s| s.parse().ok()).unwrap_or(f64::NEG_INFINITY);
        let _max: f64 = query.get("max").and_then(|s| s.parse().ok()).unwrap_or(f64::INFINITY);
        let _withscores = query.get("withscores").map(|s| s == "true").unwrap_or(false);
        let _limit: usize = query.get("limit").and_then(|s| s.parse().ok()).unwrap_or(100);
        RestResponse::ok(serde_json::json!({"key": key, "members": [], "note": "Store integration pending"}))
    }

    fn handle_sorted_set_add(&self, params: &HashMap<String, String>, body: &serde_json::Value) -> RestResponse {
        let key = params.get("key").cloned().unwrap_or_default();
        let members = body.get("members").cloned().unwrap_or(serde_json::json!([]));
        let count = members.as_array().map(|a| a.len()).unwrap_or(0);
        RestResponse::ok(serde_json::json!({"key": key, "added": count, "note": "Store integration pending"}))
    }

    fn handle_scan_keys(&self, query: &HashMap<String, String>) -> RestResponse {
        let _pattern = query.get("pattern").cloned().unwrap_or_else(|| "*".to_string());
        let _cursor: u64 = query.get("cursor").and_then(|s| s.parse().ok()).unwrap_or(0);
        let _count: u64 = query.get("count").and_then(|s| s.parse().ok()).unwrap_or(10);
        RestResponse::ok(serde_json::json!({"cursor": 0, "keys": [], "note": "Store integration pending"}))
    }

    fn handle_delete_key(&self, params: &HashMap<String, String>) -> RestResponse {
        let key = params.get("key").cloned().unwrap_or_default();
        RestResponse::ok(serde_json::json!({"deleted": 1, "key": key, "note": "Store integration pending"}))
    }

    fn handle_expire_key(&self, params: &HashMap<String, String>, body: &serde_json::Value) -> RestResponse {
        let key = params.get("key").cloned().unwrap_or_default();
        let _ttl = body.get("ttl").and_then(|v| v.as_i64()).unwrap_or(0);
        RestResponse::ok(serde_json::json!({"ok": true, "key": key, "note": "Store integration pending"}))
    }

    fn handle_server_info(&self) -> RestResponse {
        RestResponse::ok(serde_json::json!({
            "version": env!("CARGO_PKG_VERSION"),
            "uptime_seconds": 0,
            "connected_clients": 0,
            "used_memory_bytes": 0,
            "note": "Store integration pending"
        }))
    }

    fn handle_health_check(&self) -> RestResponse {
        RestResponse::ok(serde_json::json!({
            "status": "ok",
            "version": env!("CARGO_PKG_VERSION")
        }))
    }

    fn handle_execute_command(&self, body: &serde_json::Value) -> RestResponse {
        let command = body.get("command").and_then(|v| v.as_str()).unwrap_or("");
        let args = body
            .get("args")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        if command.is_empty() {
            return RestResponse::bad_request("Missing 'command' field");
        }

        RestResponse::ok(serde_json::json!({
            "command": command,
            "args": args,
            "result": null,
            "note": "Store integration pending"
        }))
    }

    // -----------------------------------------------------------------------
    // Internal: OpenAPI component schemas
    // -----------------------------------------------------------------------

    fn component_schemas(&self) -> serde_json::Value {
        serde_json::json!({
            "StringValue": {
                "type": "object",
                "properties": {
                    "key": {"type": "string"},
                    "value": {"type": "string"},
                    "ttl": {"type": "integer"}
                }
            },
            "ListValue": {
                "type": "object",
                "properties": {
                    "key": {"type": "string"},
                    "values": {"type": "array", "items": {"type": "string"}},
                    "length": {"type": "integer"}
                }
            },
            "HashValue": {
                "type": "object",
                "properties": {
                    "key": {"type": "string"},
                    "fields": {"type": "object"}
                }
            },
            "SetValue": {
                "type": "object",
                "properties": {
                    "key": {"type": "string"},
                    "members": {"type": "array", "items": {"type": "string"}},
                    "size": {"type": "integer"}
                }
            },
            "SortedSetValue": {
                "type": "object",
                "properties": {
                    "key": {"type": "string"},
                    "members": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "value": {"type": "string"},
                                "score": {"type": "number"}
                            }
                        }
                    }
                }
            },
            "KeyScanResult": {
                "type": "object",
                "properties": {
                    "cursor": {"type": "integer"},
                    "keys": {"type": "array", "items": {"type": "string"}}
                }
            },
            "ServerInfo": {
                "type": "object",
                "properties": {
                    "version": {"type": "string"},
                    "uptime_seconds": {"type": "integer"},
                    "connected_clients": {"type": "integer"},
                    "used_memory_bytes": {"type": "integer"}
                }
            },
            "ErrorResponse": {
                "type": "object",
                "properties": {
                    "error": {"type": "string"},
                    "message": {"type": "string"}
                }
            }
        })
    }
}

impl Default for RestRouter {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Path matching helpers
// ---------------------------------------------------------------------------

/// Match a URL path against a pattern with `{param}` placeholders.
///
/// Returns extracted parameters on match, `None` on mismatch.
fn match_path_pattern(pattern: &str, path: &str) -> Option<HashMap<String, String>> {
    let pattern_segments: Vec<&str> = pattern.split('/').filter(|s| !s.is_empty()).collect();
    let path_segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    if pattern_segments.len() != path_segments.len() {
        return None;
    }

    let mut params = HashMap::new();
    for (pat, val) in pattern_segments.iter().zip(path_segments.iter()) {
        if pat.starts_with('{') && pat.ends_with('}') {
            let name = &pat[1..pat.len() - 1];
            params.insert(name.to_string(), val.to_string());
        } else if pat != val {
            return None;
        }
    }

    Some(params)
}

/// Extract parameter names from a path pattern.
fn extract_param_names(pattern: &str) -> Vec<String> {
    pattern
        .split('/')
        .filter(|s| s.starts_with('{') && s.ends_with('}'))
        .map(|s| s[1..s.len() - 1].to_string())
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_matching_exact() {
        let params = match_path_pattern("/api/v1/health", "/api/v1/health");
        assert!(params.is_some());
        assert!(params.expect("should match").is_empty());
    }

    #[test]
    fn test_path_matching_with_param() {
        let params = match_path_pattern("/api/v1/string/{key}", "/api/v1/string/mykey");
        let params = params.expect("should match");
        assert_eq!(params.get("key").expect("key param"), "mykey");
    }

    #[test]
    fn test_path_matching_multi_param() {
        let params = match_path_pattern("/api/v1/hash/{key}/{field}", "/api/v1/hash/user:1/name");
        let params = params.expect("should match");
        assert_eq!(params.get("key").expect("key param"), "user:1");
        assert_eq!(params.get("field").expect("field param"), "name");
    }

    #[test]
    fn test_path_matching_mismatch() {
        assert!(match_path_pattern("/api/v1/string/{key}", "/api/v1/list/mykey").is_none());
    }

    #[test]
    fn test_path_matching_length_mismatch() {
        assert!(match_path_pattern("/api/v1/string/{key}", "/api/v1/string").is_none());
    }

    #[test]
    fn test_router_creation() {
        let router = RestRouter::new();
        assert!(!router.routes().is_empty());
        // Should have routes for all data types + server endpoints
        assert!(router.routes().len() >= 20);
    }

    #[test]
    fn test_route_get_string() {
        let router = RestRouter::new();
        let request = RestRequest {
            method: RestMethod::Get,
            path: "/api/v1/string/hello".to_string(),
            query_params: HashMap::new(),
            body: serde_json::Value::Null,
            headers: HashMap::new(),
        };
        let response = router.route(&request);
        assert_eq!(response.status_code, 200);
        assert_eq!(response.body["key"], "hello");
    }

    #[test]
    fn test_route_put_string() {
        let router = RestRouter::new();
        let request = RestRequest {
            method: RestMethod::Put,
            path: "/api/v1/string/hello".to_string(),
            query_params: HashMap::new(),
            body: serde_json::json!({"value": "world", "ttl": 60}),
            headers: HashMap::new(),
        };
        let response = router.route(&request);
        assert_eq!(response.status_code, 200);
        assert_eq!(response.body["ok"], true);
    }

    #[test]
    fn test_route_health() {
        let router = RestRouter::new();
        let request = RestRequest {
            method: RestMethod::Get,
            path: "/api/v1/health".to_string(),
            query_params: HashMap::new(),
            body: serde_json::Value::Null,
            headers: HashMap::new(),
        };
        let response = router.route(&request);
        assert_eq!(response.status_code, 200);
        assert_eq!(response.body["status"], "ok");
    }

    #[test]
    fn test_route_not_found() {
        let router = RestRouter::new();
        let request = RestRequest {
            method: RestMethod::Get,
            path: "/api/v1/nonexistent".to_string(),
            query_params: HashMap::new(),
            body: serde_json::Value::Null,
            headers: HashMap::new(),
        };
        let response = router.route(&request);
        assert_eq!(response.status_code, 404);
    }

    #[test]
    fn test_route_hash_field() {
        let router = RestRouter::new();
        let request = RestRequest {
            method: RestMethod::Get,
            path: "/api/v1/hash/user:1/name".to_string(),
            query_params: HashMap::new(),
            body: serde_json::Value::Null,
            headers: HashMap::new(),
        };
        let response = router.route(&request);
        assert_eq!(response.status_code, 200);
        assert_eq!(response.body["key"], "user:1");
        assert_eq!(response.body["field"], "name");
    }

    #[test]
    fn test_route_execute_command() {
        let router = RestRouter::new();
        let request = RestRequest {
            method: RestMethod::Post,
            path: "/api/v1/query".to_string(),
            query_params: HashMap::new(),
            body: serde_json::json!({"command": "GET", "args": ["mykey"]}),
            headers: HashMap::new(),
        };
        let response = router.route(&request);
        assert_eq!(response.status_code, 200);
        assert_eq!(response.body["command"], "GET");
    }

    #[test]
    fn test_route_execute_command_empty() {
        let router = RestRouter::new();
        let request = RestRequest {
            method: RestMethod::Post,
            path: "/api/v1/query".to_string(),
            query_params: HashMap::new(),
            body: serde_json::json!({}),
            headers: HashMap::new(),
        };
        let response = router.route(&request);
        assert_eq!(response.status_code, 400);
    }

    #[test]
    fn test_openapi_spec_generation() {
        let router = RestRouter::new();
        let spec = router.generate_openapi_spec();
        assert_eq!(spec["openapi"], "3.0.0");
        assert_eq!(spec["info"]["title"], "Ferrite REST API");
        assert!(spec["paths"].as_object().expect("paths object").len() > 10);
        assert!(spec["components"]["schemas"].as_object().expect("schemas").contains_key("StringValue"));
        assert!(spec["components"]["schemas"].as_object().expect("schemas").contains_key("ServerInfo"));
    }

    #[test]
    fn test_extract_param_names() {
        let names = extract_param_names("/api/v1/hash/{key}/{field}");
        assert_eq!(names, vec!["key", "field"]);
    }

    #[test]
    fn test_rest_response_helpers() {
        let ok = RestResponse::ok(serde_json::json!({"test": true}));
        assert_eq!(ok.status_code, 200);

        let created = RestResponse::created(serde_json::json!({"id": 1}));
        assert_eq!(created.status_code, 201);

        let not_found = RestResponse::not_found("gone");
        assert_eq!(not_found.status_code, 404);

        let bad = RestResponse::bad_request("invalid");
        assert_eq!(bad.status_code, 400);

        let not_allowed = RestResponse::method_not_allowed();
        assert_eq!(not_allowed.status_code, 405);

        let error = RestResponse::internal_error("boom");
        assert_eq!(error.status_code, 500);
    }

    #[test]
    fn test_rest_method_display() {
        assert_eq!(format!("{}", RestMethod::Get), "GET");
        assert_eq!(format!("{}", RestMethod::Post), "POST");
        assert_eq!(format!("{}", RestMethod::Put), "PUT");
        assert_eq!(format!("{}", RestMethod::Delete), "DELETE");
    }
}
