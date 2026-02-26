//! GraphQL query resolver and execution engine
//!
//! Parses simplified GraphQL queries and resolves them against the Ferrite store.
//! Supports queries, mutations, and produces JSON responses.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::storage::{Store, Value};

use super::GraphQLError;

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

/// A GraphQL request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLRequest {
    /// The GraphQL query string.
    pub query: String,
    /// Optional variables map.
    pub variables: Option<HashMap<String, serde_json::Value>>,
    /// Optional operation name (for multi-operation documents).
    pub operation_name: Option<String>,
}

/// A GraphQL response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLResponse {
    /// Query result data (null on error).
    pub data: Option<serde_json::Value>,
    /// Errors, if any.
    pub errors: Option<Vec<GraphQLResponseError>>,
}

/// A single error in a GraphQL response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLResponseError {
    /// Error message.
    pub message: String,
    /// Optional path to the field that caused the error.
    pub path: Option<Vec<String>>,
}

impl GraphQLResponse {
    /// Create a successful response.
    pub fn ok(data: serde_json::Value) -> Self {
        Self {
            data: Some(data),
            errors: None,
        }
    }

    /// Create an error response.
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            data: None,
            errors: Some(vec![GraphQLResponseError {
                message: message.into(),
                path: None,
            }]),
        }
    }
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

/// The GraphQL execution engine.
///
/// Parses simplified GraphQL operations and resolves them against a [`Store`].
pub struct GraphQLEngine {
    store: Arc<Store>,
    default_db: u8,
}

impl GraphQLEngine {
    /// Create a new engine backed by the given store.
    pub fn new(store: Arc<Store>) -> Self {
        Self {
            store,
            default_db: 0,
        }
    }

    /// Execute a GraphQL request and return a JSON response.
    pub fn execute(&self, request: GraphQLRequest) -> GraphQLResponse {
        let query = request.query.trim();

        // Detect operation type
        if query.starts_with("mutation") || query.starts_with("mutation ") {
            self.execute_mutation(query, &request.variables)
        } else if query.contains("__schema") || query.contains("__type") {
            self.execute_introspection()
        } else if query.starts_with('{') || query.starts_with("query") {
            self.execute_query(query, &request.variables)
        } else {
            GraphQLResponse::error(format!("Unsupported operation: {}", &query[..query.len().min(50)]))
        }
    }

    /// Execute a query operation.
    fn execute_query(
        &self,
        query: &str,
        variables: &Option<HashMap<String, serde_json::Value>>,
    ) -> GraphQLResponse {
        let fields = match parse_fields(query) {
            Ok(f) => f,
            Err(e) => return GraphQLResponse::error(e),
        };

        let mut result = serde_json::Map::new();

        for field in &fields {
            match field.name.as_str() {
                "get" => {
                    if let Some(key) = field.get_arg("key", variables) {
                        let db = field
                            .get_arg("database", variables)
                            .and_then(|v| v.parse::<u8>().ok())
                            .unwrap_or(self.default_db);
                        match self.resolve_get(&key, db) {
                            Ok(val) => {
                                result.insert(field.alias_or_name(), val);
                            }
                            Err(e) => {
                                result.insert(field.alias_or_name(), serde_json::Value::Null);
                                debug!(error = %e, "get resolver error");
                            }
                        }
                    } else {
                        return GraphQLResponse::error("get: missing required argument 'key'");
                    }
                }
                "exists" => {
                    if let Some(key) = field.get_arg("key", variables) {
                        let key_bytes = Bytes::from(key);
                        let exists = self.store.get(self.default_db, &key_bytes).is_some();
                        result.insert(field.alias_or_name(), serde_json::Value::Bool(exists));
                    }
                }
                "serverInfo" => {
                    let info = serde_json::json!({
                        "version": "0.1.0",
                        "connected_clients": 0,
                        "used_memory_bytes": 0,
                    });
                    result.insert(field.alias_or_name(), info);
                }
                "dbsize" => {
                    let size = self.store.key_count(self.default_db);
                    result.insert(
                        field.alias_or_name(),
                        serde_json::Value::Number(serde_json::Number::from(size)),
                    );
                }
                other => {
                    result.insert(
                        other.to_string(),
                        serde_json::Value::String(format!("resolver not implemented: {other}")),
                    );
                }
            }
        }

        GraphQLResponse::ok(serde_json::Value::Object(result))
    }

    /// Execute a mutation operation.
    fn execute_mutation(
        &self,
        query: &str,
        variables: &Option<HashMap<String, serde_json::Value>>,
    ) -> GraphQLResponse {
        let fields = match parse_fields(query) {
            Ok(f) => f,
            Err(e) => return GraphQLResponse::error(e),
        };

        let mut result = serde_json::Map::new();

        for field in &fields {
            match field.name.as_str() {
                "set" => {
                    let key = field.get_arg("key", variables);
                    let value = field.get_arg("value", variables);
                    if let (Some(k), Some(v)) = (key, value) {
                        self.store.set(
                            self.default_db,
                            Bytes::from(k),
                            Value::String(Bytes::from(v)),
                        );
                        result.insert(field.alias_or_name(), serde_json::Value::Bool(true));
                    } else {
                        return GraphQLResponse::error("set: missing 'key' or 'value' argument");
                    }
                }
                "del" => {
                    if let Some(keys_str) = field.get_arg("keys", variables) {
                        let keys: Vec<Bytes> = keys_str
                            .split(',')
                            .map(|s| Bytes::from(s.trim().to_string()))
                            .collect();
                        let deleted = self.store.del(self.default_db, &keys);
                        result.insert(
                            field.alias_or_name(),
                            serde_json::Value::Number(serde_json::Number::from(deleted)),
                        );
                    }
                }
                "flushDb" => {
                    let db = field
                        .get_arg("database", variables)
                        .and_then(|v| v.parse::<u8>().ok())
                        .unwrap_or(self.default_db);
                    self.store.flush_db(db);
                    result.insert(field.alias_or_name(), serde_json::Value::Bool(true));
                }
                other => {
                    result.insert(
                        other.to_string(),
                        serde_json::Value::String(format!("mutation not implemented: {other}")),
                    );
                }
            }
        }

        GraphQLResponse::ok(serde_json::Value::Object(result))
    }

    /// Execute an introspection query (simplified).
    fn execute_introspection(&self) -> GraphQLResponse {
        let schema = super::schema::GraphQLSchema::build();
        let sdl = schema.to_sdl();
        GraphQLResponse::ok(serde_json::json!({
            "__schema": {
                "sdl": sdl,
                "queryType": { "name": "Query" },
                "mutationType": { "name": "Mutation" },
                "subscriptionType": { "name": "Subscription" },
            }
        }))
    }

    /// Resolve a GET operation.
    fn resolve_get(&self, key: &str, db: u8) -> Result<serde_json::Value, GraphQLError> {
        let key_bytes = Bytes::from(key.to_string());
        match self.store.get(db, &key_bytes) {
            Some(value) => {
                let (type_name, val_json) = match &value {
                    Value::String(b) => (
                        "string",
                        serde_json::Value::String(String::from_utf8_lossy(b).to_string()),
                    ),
                    Value::List(items) => (
                        "list",
                        serde_json::Value::Array(
                            items
                                .iter()
                                .map(|b| {
                                    serde_json::Value::String(
                                        String::from_utf8_lossy(b).to_string(),
                                    )
                                })
                                .collect(),
                        ),
                    ),
                    Value::Hash(map) => {
                        let obj: serde_json::Map<String, serde_json::Value> = map
                            .iter()
                            .map(|(k, v)| {
                                (
                                    String::from_utf8_lossy(k).to_string(),
                                    serde_json::Value::String(
                                        String::from_utf8_lossy(v).to_string(),
                                    ),
                                )
                            })
                            .collect();
                        ("hash", serde_json::Value::Object(obj))
                    }
                    Value::Set(members) => (
                        "set",
                        serde_json::Value::Array(
                            members
                                .iter()
                                .map(|b| {
                                    serde_json::Value::String(
                                        String::from_utf8_lossy(b).to_string(),
                                    )
                                })
                                .collect(),
                        ),
                    ),
                    _ => ("unknown", serde_json::Value::Null),
                };

                let ttl = self.store.ttl(db, &key_bytes).unwrap_or(-1);

                Ok(serde_json::json!({
                    "key": key,
                    "value": val_json,
                    "type": type_name,
                    "ttl": ttl,
                }))
            }
            None => Ok(serde_json::Value::Null),
        }
    }
}

// ---------------------------------------------------------------------------
// Simplified GraphQL parser
// ---------------------------------------------------------------------------

/// A parsed field from a GraphQL query.
#[derive(Debug, Clone)]
struct ParsedField {
    name: String,
    alias: Option<String>,
    arguments: HashMap<String, String>,
}

impl ParsedField {
    fn alias_or_name(&self) -> String {
        self.alias.clone().unwrap_or_else(|| self.name.clone())
    }

    fn get_arg(
        &self,
        name: &str,
        variables: &Option<HashMap<String, serde_json::Value>>,
    ) -> Option<String> {
        if let Some(val) = self.arguments.get(name) {
            // Check if it's a variable reference
            if val.starts_with('$') {
                if let Some(vars) = variables {
                    return vars
                        .get(&val[1..])
                        .and_then(|v| v.as_str().map(|s| s.to_string()));
                }
                return None;
            }
            return Some(val.clone());
        }
        None
    }
}

/// Parse top-level fields from a simplified GraphQL query.
fn parse_fields(query: &str) -> Result<Vec<ParsedField>, String> {
    let body = extract_body(query)?;
    let mut fields = Vec::new();
    let mut chars = body.chars().peekable();

    while chars.peek().is_some() {
        skip_whitespace(&mut chars);
        if chars.peek().is_none() {
            break;
        }

        // Read field name
        let name = read_identifier(&mut chars);
        if name.is_empty() {
            // Skip unknown characters
            chars.next();
            continue;
        }

        let mut alias = None;

        // Check for alias (name: actualField)
        skip_whitespace(&mut chars);
        if chars.peek() == Some(&':') {
            chars.next(); // consume ':'
            skip_whitespace(&mut chars);
            alias = Some(name.clone());
            let actual_name = read_identifier(&mut chars);
            fields.push(parse_field_with_args(actual_name, alias, &mut chars));
            continue;
        }

        fields.push(parse_field_with_args(name, alias, &mut chars));
    }

    Ok(fields)
}

fn parse_field_with_args(
    name: String,
    alias: Option<String>,
    chars: &mut std::iter::Peekable<std::str::Chars>,
) -> ParsedField {
    skip_whitespace(chars);
    let arguments = if chars.peek() == Some(&'(') {
        parse_arguments(chars)
    } else {
        HashMap::new()
    };

    // Skip sub-selection { ... } if present
    skip_whitespace(chars);
    if chars.peek() == Some(&'{') {
        skip_braces(chars);
    }

    ParsedField {
        name,
        alias,
        arguments,
    }
}

fn extract_body(query: &str) -> Result<String, String> {
    // Strip "query Name { ... }" or "mutation { ... }" wrapper
    let trimmed = query.trim();
    if let Some(start) = trimmed.find('{') {
        if let Some(end) = trimmed.rfind('}') {
            return Ok(trimmed[start + 1..end].to_string());
        }
    }
    Err("Invalid GraphQL syntax: missing braces".to_string())
}

fn skip_whitespace(chars: &mut std::iter::Peekable<std::str::Chars>) {
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() || c == ',' {
            chars.next();
        } else {
            break;
        }
    }
}

fn read_identifier(chars: &mut std::iter::Peekable<std::str::Chars>) -> String {
    let mut ident = String::new();
    while let Some(&c) = chars.peek() {
        if c.is_alphanumeric() || c == '_' {
            ident.push(c);
            chars.next();
        } else {
            break;
        }
    }
    ident
}

fn parse_arguments(
    chars: &mut std::iter::Peekable<std::str::Chars>,
) -> HashMap<String, String> {
    let mut args = HashMap::new();
    if chars.peek() != Some(&'(') {
        return args;
    }
    chars.next(); // consume '('

    loop {
        skip_whitespace(chars);
        if chars.peek() == Some(&')') {
            chars.next();
            break;
        }
        if chars.peek().is_none() {
            break;
        }

        let key = read_identifier(chars);
        skip_whitespace(chars);

        if chars.peek() == Some(&':') {
            chars.next(); // consume ':'
            skip_whitespace(chars);
            let value = read_argument_value(chars);
            if !key.is_empty() {
                args.insert(key, value);
            }
        }

        skip_whitespace(chars);
        if chars.peek() == Some(&',') {
            chars.next();
        }
    }

    args
}

fn read_argument_value(chars: &mut std::iter::Peekable<std::str::Chars>) -> String {
    match chars.peek() {
        Some(&'"') => {
            chars.next(); // consume opening quote
            let mut val = String::new();
            while let Some(&c) = chars.peek() {
                if c == '"' {
                    chars.next();
                    break;
                }
                if c == '\\' {
                    chars.next();
                    if let Some(&escaped) = chars.peek() {
                        val.push(escaped);
                        chars.next();
                    }
                } else {
                    val.push(c);
                    chars.next();
                }
            }
            val
        }
        Some(&'$') => {
            let mut val = String::new();
            while let Some(&c) = chars.peek() {
                if c.is_alphanumeric() || c == '_' || c == '$' {
                    val.push(c);
                    chars.next();
                } else {
                    break;
                }
            }
            val
        }
        Some(&'[') => {
            // Array literal — read until matching ']'
            let mut depth = 0;
            let mut val = String::new();
            while let Some(&c) = chars.peek() {
                if c == '[' {
                    depth += 1;
                } else if c == ']' {
                    depth -= 1;
                    if depth == 0 {
                        chars.next();
                        break;
                    }
                }
                val.push(c);
                chars.next();
            }
            val
        }
        _ => {
            // Number, boolean, enum
            let mut val = String::new();
            while let Some(&c) = chars.peek() {
                if c == ')' || c == ',' || c.is_whitespace() {
                    break;
                }
                val.push(c);
                chars.next();
            }
            val
        }
    }
}

fn skip_braces(chars: &mut std::iter::Peekable<std::str::Chars>) {
    let mut depth = 0;
    while let Some(&c) = chars.peek() {
        if c == '{' {
            depth += 1;
        } else if c == '}' {
            depth -= 1;
            if depth == 0 {
                chars.next();
                return;
            }
        }
        chars.next();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_store() -> Arc<Store> {
        Arc::new(Store::new(16))
    }

    #[test]
    fn test_execute_get_missing_key() {
        let engine = GraphQLEngine::new(test_store());
        let req = GraphQLRequest {
            query: r#"{ get(key: "nonexistent") { value } }"#.to_string(),
            variables: None,
            operation_name: None,
        };
        let resp = engine.execute(req);
        assert!(resp.errors.is_none());
        let data = resp.data.unwrap();
        assert!(data["get"].is_null());
    }

    #[test]
    fn test_execute_set_and_get() {
        let store = test_store();
        let engine = GraphQLEngine::new(store.clone());

        // Set via mutation
        let set_req = GraphQLRequest {
            query: r#"mutation { set(key: "hello", value: "world") }"#.to_string(),
            variables: None,
            operation_name: None,
        };
        let set_resp = engine.execute(set_req);
        assert!(set_resp.errors.is_none());
        assert_eq!(set_resp.data.unwrap()["set"], true);

        // Get via query
        let get_req = GraphQLRequest {
            query: r#"{ get(key: "hello") { value type } }"#.to_string(),
            variables: None,
            operation_name: None,
        };
        let get_resp = engine.execute(get_req);
        assert!(get_resp.errors.is_none());
        let data = get_resp.data.unwrap();
        assert_eq!(data["get"]["value"], "world");
        assert_eq!(data["get"]["type"], "string");
    }

    #[test]
    fn test_execute_exists() {
        let store = test_store();
        store.set(0, Bytes::from("mykey"), Value::String(Bytes::from("val")));
        let engine = GraphQLEngine::new(store);

        let req = GraphQLRequest {
            query: r#"{ exists(key: "mykey") }"#.to_string(),
            variables: None,
            operation_name: None,
        };
        let resp = engine.execute(req);
        assert_eq!(resp.data.unwrap()["exists"], true);
    }

    #[test]
    fn test_execute_del() {
        let store = test_store();
        store.set(0, Bytes::from("k1"), Value::String(Bytes::from("v1")));
        let engine = GraphQLEngine::new(store);

        let req = GraphQLRequest {
            query: r#"mutation { del(keys: "k1") }"#.to_string(),
            variables: None,
            operation_name: None,
        };
        let resp = engine.execute(req);
        assert_eq!(resp.data.unwrap()["del"], 1);
    }

    #[test]
    fn test_introspection() {
        let engine = GraphQLEngine::new(test_store());
        let req = GraphQLRequest {
            query: r#"{ __schema { sdl } }"#.to_string(),
            variables: None,
            operation_name: None,
        };
        let resp = engine.execute(req);
        assert!(resp.errors.is_none());
        let sdl = resp.data.unwrap()["__schema"]["sdl"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(sdl.contains("type Query"));
    }

    #[test]
    fn test_server_info() {
        let engine = GraphQLEngine::new(test_store());
        let req = GraphQLRequest {
            query: r#"{ serverInfo { version } }"#.to_string(),
            variables: None,
            operation_name: None,
        };
        let resp = engine.execute(req);
        assert!(resp.errors.is_none());
        assert_eq!(resp.data.unwrap()["serverInfo"]["version"], "0.1.0");
    }

    #[test]
    fn test_error_response() {
        let resp = GraphQLResponse::error("test error");
        assert!(resp.data.is_none());
        assert_eq!(resp.errors.unwrap()[0].message, "test error");
    }

    #[test]
    fn test_dbsize_query() {
        let store = test_store();
        store.set(0, Bytes::from("a"), Value::String(Bytes::from("1")));
        store.set(0, Bytes::from("b"), Value::String(Bytes::from("2")));
        let engine = GraphQLEngine::new(store);

        let req = GraphQLRequest {
            query: r#"{ dbsize }"#.to_string(),
            variables: None,
            operation_name: None,
        };
        let resp = engine.execute(req);
        assert_eq!(resp.data.unwrap()["dbsize"], 2);
    }
}
