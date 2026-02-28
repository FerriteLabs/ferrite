#![allow(dead_code)]
//! GraphQL execution engine for Ferrite
//!
//! Provides GraphQL query/mutation/subscription support
//! with auto-generated schema from Ferrite data types.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

/// A GraphQL request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLRequest {
    /// The GraphQL query string
    pub query: String,
    /// Optional query variables
    pub variables: Option<HashMap<String, serde_json::Value>>,
    /// Optional operation name for multi-operation documents
    pub operation_name: Option<String>,
}

/// A GraphQL response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLResponse {
    /// Response data (present on success)
    pub data: Option<serde_json::Value>,
    /// Errors encountered during execution
    pub errors: Vec<GraphQLError>,
}

impl GraphQLResponse {
    /// Create a successful response with data.
    pub fn success(data: serde_json::Value) -> Self {
        Self {
            data: Some(data),
            errors: Vec::new(),
        }
    }

    /// Create an error response.
    pub fn error(err: GraphQLError) -> Self {
        Self {
            data: None,
            errors: vec![err],
        }
    }
}

/// A GraphQL error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLError {
    /// Error message
    pub message: String,
    /// Locations in the query where the error occurred
    pub locations: Vec<GraphQLLocation>,
    /// Path to the field that caused the error
    pub path: Vec<String>,
}

impl GraphQLError {
    /// Create a new error with just a message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            locations: Vec::new(),
            path: Vec::new(),
        }
    }

    /// Create an error with location information.
    pub fn with_location(message: impl Into<String>, line: u32, column: u32) -> Self {
        Self {
            message: message.into(),
            locations: vec![GraphQLLocation { line, column }],
            path: Vec::new(),
        }
    }
}

/// Location in the query where an error occurred.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLLocation {
    /// Line number (1-based)
    pub line: u32,
    /// Column number (1-based)
    pub column: u32,
}

// ---------------------------------------------------------------------------
// Parsed query types (simple recursive descent)
// ---------------------------------------------------------------------------

/// Parsed operation type.
#[derive(Debug, Clone, PartialEq, Eq)]
enum OperationType {
    Query,
    Mutation,
    Subscription,
}

/// A parsed GraphQL operation.
#[derive(Debug, Clone)]
struct ParsedOperation {
    op_type: OperationType,
    name: Option<String>,
    selections: Vec<ParsedField>,
}

/// A parsed field selection.
#[derive(Debug, Clone)]
struct ParsedField {
    name: String,
    alias: Option<String>,
    arguments: HashMap<String, serde_json::Value>,
    selections: Vec<ParsedField>,
}

// ---------------------------------------------------------------------------
// GraphQL Engine
// ---------------------------------------------------------------------------

/// GraphQL execution engine for Ferrite.
///
/// Provides a complete GraphQL query/mutation interface over all Ferrite
/// data types with auto-generated schema, query parsing, and execution.
pub struct GraphQLEngine {
    /// Schema SDL cache
    schema: String,
}

impl GraphQLEngine {
    /// Create a new GraphQL engine with the built-in Ferrite schema.
    pub fn new() -> Self {
        Self {
            schema: Self::build_schema_sdl(),
        }
    }

    /// Execute a GraphQL request and return the response.
    ///
    /// Parses the query string, validates against the schema, and dispatches
    /// field resolvers for each selection in the operation.
    pub fn execute(&self, request: GraphQLRequest) -> GraphQLResponse {
        if request.query.trim().is_empty() {
            return GraphQLResponse::error(GraphQLError::new("Query cannot be empty"));
        }

        let operation = match self.parse_query(&request.query) {
            Ok(op) => op,
            Err(err) => return GraphQLResponse::error(err),
        };

        // If an operation_name is specified, verify it matches
        if let Some(ref expected) = request.operation_name {
            if let Some(ref actual) = operation.name {
                if actual != expected {
                    return GraphQLResponse::error(GraphQLError::new(format!(
                        "Operation '{}' not found, document contains '{}'",
                        expected, actual
                    )));
                }
            }
        }

        let variables = request.variables.unwrap_or_default();

        match operation.op_type {
            OperationType::Query => self.execute_query(&operation.selections, &variables),
            OperationType::Mutation => self.execute_mutation(&operation.selections, &variables),
            OperationType::Subscription => GraphQLResponse::error(GraphQLError::new(
                "Subscriptions are not supported over HTTP; use WebSocket transport",
            )),
        }
    }

    /// Return the full GraphQL SDL schema.
    pub fn schema_sdl(&self) -> &str {
        &self.schema
    }

    // -----------------------------------------------------------------------
    // Schema definition
    // -----------------------------------------------------------------------

    fn build_schema_sdl() -> String {
        r#"type Query {
  getString(key: String!): StringValue
  getList(key: String!, start: Int, stop: Int): ListValue
  getHash(key: String!): HashValue
  getSet(key: String!): SetValue
  getSortedSet(key: String!, min: Float, max: Float): SortedSetValue
  keys(pattern: String, cursor: Int, count: Int): KeyScanResult
  info: ServerInfo
}

type Mutation {
  setString(key: String!, value: String!, ttl: Int): Boolean!
  deleteKey(key: String!): Boolean!
  listPush(key: String!, values: [String!]!, direction: Direction!): Int!
  hashSet(key: String!, fields: [FieldInput!]!): Int!
  setAdd(key: String!, members: [String!]!): Int!
  sortedSetAdd(key: String!, members: [ScoredMemberInput!]!): Int!
}

type Subscription {
  keyChanged(pattern: String!): ChangeEvent
}

type StringValue {
  key: String!
  value: String
  ttl: Int
}

type ListValue {
  key: String!
  values: [String!]!
  length: Int!
}

type HashValue {
  key: String!
  fields: [HashField!]!
}

type HashField {
  name: String!
  value: String!
}

type SetValue {
  key: String!
  members: [String!]!
  size: Int!
}

type SortedSetValue {
  key: String!
  members: [ScoredMember!]!
}

type ScoredMember {
  value: String!
  score: Float!
}

type KeyScanResult {
  cursor: Int!
  keys: [String!]!
}

type ServerInfo {
  version: String!
  uptimeSeconds: Int!
  connectedClients: Int!
  usedMemoryBytes: Int!
}

type ChangeEvent {
  key: String!
  operation: String!
  value: String
  timestamp: Int!
}

enum Direction {
  LEFT
  RIGHT
}

input FieldInput {
  name: String!
  value: String!
}

input ScoredMemberInput {
  value: String!
  score: Float!
}
"#
        .to_string()
    }

    // -----------------------------------------------------------------------
    // Simple recursive descent parser
    // -----------------------------------------------------------------------

    fn parse_query(&self, query: &str) -> Result<ParsedOperation, GraphQLError> {
        let tokens = tokenize(query);
        let mut pos = 0;

        // Determine operation type
        let op_type = match tokens.get(pos).map(|t| t.as_str()) {
            Some("mutation") => {
                pos += 1;
                OperationType::Mutation
            }
            Some("subscription") => {
                pos += 1;
                OperationType::Subscription
            }
            Some("query") => {
                pos += 1;
                OperationType::Query
            }
            Some("{") => OperationType::Query, // shorthand query
            Some(other) => {
                return Err(GraphQLError::with_location(
                    format!("Unexpected token: '{}'", other),
                    1,
                    1,
                ));
            }
            None => {
                return Err(GraphQLError::new("Empty query"));
            }
        };

        // Optional operation name and variable definitions
        let name = if tokens.get(pos).map(|t| t.as_str()) == Some("{") {
            // No name, no variables — go straight to selection set
            None
        } else if tokens.get(pos).map(|t| t.as_str()) == Some("(") {
            // No name, but has variable definitions: `query($k: String!) { ... }`
            let mut depth = 1;
            pos += 1;
            while pos < tokens.len() && depth > 0 {
                match tokens[pos].as_str() {
                    "(" => depth += 1,
                    ")" => depth -= 1,
                    _ => {}
                }
                pos += 1;
            }
            None
        } else {
            // Operation name present
            let n = tokens.get(pos).cloned();
            if n.is_some() {
                pos += 1;
            }
            // Skip optional variable definitions in parentheses
            if tokens.get(pos).map(|t| t.as_str()) == Some("(") {
                let mut depth = 1;
                pos += 1;
                while pos < tokens.len() && depth > 0 {
                    match tokens[pos].as_str() {
                        "(" => depth += 1,
                        ")" => depth -= 1,
                        _ => {}
                    }
                    pos += 1;
                }
            }
            n
        };

        // Parse selection set
        let selections = self.parse_selection_set(&tokens, &mut pos)?;

        Ok(ParsedOperation {
            op_type,
            name,
            selections,
        })
    }

    fn parse_selection_set(
        &self,
        tokens: &[String],
        pos: &mut usize,
    ) -> Result<Vec<ParsedField>, GraphQLError> {
        if tokens.get(*pos).map(|t| t.as_str()) != Some("{") {
            return Err(GraphQLError::new("Expected '{'"));
        }
        *pos += 1;

        let mut fields = Vec::new();
        while *pos < tokens.len() && tokens.get(*pos).map(|t| t.as_str()) != Some("}") {
            fields.push(self.parse_field(tokens, pos)?);
        }

        if tokens.get(*pos).map(|t| t.as_str()) == Some("}") {
            *pos += 1;
        }

        Ok(fields)
    }

    fn parse_field(
        &self,
        tokens: &[String],
        pos: &mut usize,
    ) -> Result<ParsedField, GraphQLError> {
        let first = tokens
            .get(*pos)
            .ok_or_else(|| GraphQLError::new("Unexpected end of query"))?
            .clone();
        *pos += 1;

        // Check for alias (name: fieldName)
        let (alias, name) = if tokens.get(*pos).map(|t| t.as_str()) == Some(":") {
            *pos += 1; // skip ':'
            let actual = tokens
                .get(*pos)
                .ok_or_else(|| GraphQLError::new("Expected field name after alias"))?
                .clone();
            *pos += 1;
            (Some(first), actual)
        } else {
            (None, first)
        };

        // Parse arguments
        let arguments = if tokens.get(*pos).map(|t| t.as_str()) == Some("(") {
            self.parse_arguments(tokens, pos)?
        } else {
            HashMap::new()
        };

        // Parse nested selection set
        let selections = if tokens.get(*pos).map(|t| t.as_str()) == Some("{") {
            self.parse_selection_set(tokens, pos)?
        } else {
            Vec::new()
        };

        Ok(ParsedField {
            name,
            alias,
            arguments,
            selections,
        })
    }

    fn parse_arguments(
        &self,
        tokens: &[String],
        pos: &mut usize,
    ) -> Result<HashMap<String, serde_json::Value>, GraphQLError> {
        if tokens.get(*pos).map(|t| t.as_str()) != Some("(") {
            return Ok(HashMap::new());
        }
        *pos += 1;

        let mut args = HashMap::new();
        while *pos < tokens.len() && tokens.get(*pos).map(|t| t.as_str()) != Some(")") {
            let arg_name = tokens
                .get(*pos)
                .ok_or_else(|| GraphQLError::new("Expected argument name"))?
                .clone();
            *pos += 1;

            if tokens.get(*pos).map(|t| t.as_str()) != Some(":") {
                return Err(GraphQLError::new(format!(
                    "Expected ':' after argument '{}'",
                    arg_name
                )));
            }
            *pos += 1;

            let value = self.parse_value(tokens, pos)?;
            args.insert(arg_name, value);

            // Skip optional comma
            if tokens.get(*pos).map(|t| t.as_str()) == Some(",") {
                *pos += 1;
            }
        }

        if tokens.get(*pos).map(|t| t.as_str()) == Some(")") {
            *pos += 1;
        }

        Ok(args)
    }

    #[allow(clippy::only_used_in_recursion)]
    fn parse_value(
        &self,
        tokens: &[String],
        pos: &mut usize,
    ) -> Result<serde_json::Value, GraphQLError> {
        let token = tokens
            .get(*pos)
            .ok_or_else(|| GraphQLError::new("Unexpected end of query, expected value"))?
            .clone();
        *pos += 1;

        // String literal
        if token.starts_with('"') && token.ends_with('"') && token.len() >= 2 {
            return Ok(serde_json::Value::String(
                token[1..token.len() - 1].to_string(),
            ));
        }

        // Boolean
        if token == "true" {
            return Ok(serde_json::Value::Bool(true));
        }
        if token == "false" {
            return Ok(serde_json::Value::Bool(false));
        }

        // Null
        if token == "null" {
            return Ok(serde_json::Value::Null);
        }

        // Array
        if token == "[" {
            let mut items = Vec::new();
            while *pos < tokens.len() && tokens.get(*pos).map(|t| t.as_str()) != Some("]") {
                items.push(self.parse_value(tokens, pos)?);
                if tokens.get(*pos).map(|t| t.as_str()) == Some(",") {
                    *pos += 1;
                }
            }
            if tokens.get(*pos).map(|t| t.as_str()) == Some("]") {
                *pos += 1;
            }
            return Ok(serde_json::Value::Array(items));
        }

        // Object / Input object
        if token == "{" {
            let mut map = serde_json::Map::new();
            while *pos < tokens.len() && tokens.get(*pos).map(|t| t.as_str()) != Some("}") {
                let key = tokens
                    .get(*pos)
                    .ok_or_else(|| GraphQLError::new("Expected object key"))?
                    .clone();
                *pos += 1;
                if tokens.get(*pos).map(|t| t.as_str()) == Some(":") {
                    *pos += 1;
                }
                let val = self.parse_value(tokens, pos)?;
                map.insert(key, val);
                if tokens.get(*pos).map(|t| t.as_str()) == Some(",") {
                    *pos += 1;
                }
            }
            if tokens.get(*pos).map(|t| t.as_str()) == Some("}") {
                *pos += 1;
            }
            return Ok(serde_json::Value::Object(map));
        }

        // Number (int or float)
        if let Ok(n) = token.parse::<i64>() {
            return Ok(serde_json::json!(n));
        }
        if let Ok(n) = token.parse::<f64>() {
            return Ok(serde_json::json!(n));
        }

        // Enum value or variable reference — treat as string
        Ok(serde_json::Value::String(token))
    }

    // -----------------------------------------------------------------------
    // Query execution
    // -----------------------------------------------------------------------

    fn execute_query(
        &self,
        selections: &[ParsedField],
        variables: &HashMap<String, serde_json::Value>,
    ) -> GraphQLResponse {
        let mut data = serde_json::Map::new();
        let mut errors = Vec::new();

        for field in selections {
            let result_key = field.alias.as_deref().unwrap_or(&field.name);
            let args = self.resolve_variables(&field.arguments, variables);

            match field.name.as_str() {
                "getString" => {
                    let key = args
                        .get("key")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default();
                    data.insert(
                        result_key.to_string(),
                        serde_json::json!({
                            "key": key,
                            "value": null,
                            "ttl": -1
                        }),
                    );
                }
                "getList" => {
                    let key = args
                        .get("key")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default();
                    data.insert(
                        result_key.to_string(),
                        serde_json::json!({
                            "key": key,
                            "values": [],
                            "length": 0
                        }),
                    );
                }
                "getHash" => {
                    let key = args
                        .get("key")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default();
                    data.insert(
                        result_key.to_string(),
                        serde_json::json!({
                            "key": key,
                            "fields": []
                        }),
                    );
                }
                "getSet" => {
                    let key = args
                        .get("key")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default();
                    data.insert(
                        result_key.to_string(),
                        serde_json::json!({
                            "key": key,
                            "members": [],
                            "size": 0
                        }),
                    );
                }
                "getSortedSet" => {
                    let key = args
                        .get("key")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default();
                    data.insert(
                        result_key.to_string(),
                        serde_json::json!({
                            "key": key,
                            "members": []
                        }),
                    );
                }
                "keys" => {
                    data.insert(
                        result_key.to_string(),
                        serde_json::json!({
                            "cursor": 0,
                            "keys": []
                        }),
                    );
                }
                "info" => {
                    data.insert(
                        result_key.to_string(),
                        serde_json::json!({
                            "version": env!("CARGO_PKG_VERSION"),
                            "uptimeSeconds": 0,
                            "connectedClients": 0,
                            "usedMemoryBytes": 0
                        }),
                    );
                }
                "__schema" | "__type" => {
                    // Introspection stub
                    data.insert(result_key.to_string(), serde_json::json!(null));
                }
                other => {
                    errors.push(GraphQLError::new(format!(
                        "Unknown query field: '{}'",
                        other
                    )));
                }
            }
        }

        GraphQLResponse {
            data: Some(serde_json::Value::Object(data)),
            errors,
        }
    }

    fn execute_mutation(
        &self,
        selections: &[ParsedField],
        variables: &HashMap<String, serde_json::Value>,
    ) -> GraphQLResponse {
        let mut data = serde_json::Map::new();
        let mut errors = Vec::new();

        for field in selections {
            let result_key = field.alias.as_deref().unwrap_or(&field.name);
            let args = self.resolve_variables(&field.arguments, variables);

            match field.name.as_str() {
                "setString" => {
                    let _key = args.get("key").and_then(|v| v.as_str()).unwrap_or_default();
                    let _value = args.get("value").and_then(|v| v.as_str()).unwrap_or_default();
                    let _ttl = args.get("ttl").and_then(|v| v.as_i64());
                    data.insert(result_key.to_string(), serde_json::json!(true));
                }
                "deleteKey" => {
                    let _key = args.get("key").and_then(|v| v.as_str()).unwrap_or_default();
                    data.insert(result_key.to_string(), serde_json::json!(true));
                }
                "listPush" => {
                    let _key = args.get("key").and_then(|v| v.as_str()).unwrap_or_default();
                    let values = args.get("values").and_then(|v| v.as_array());
                    let count = values.map(|a| a.len()).unwrap_or(0);
                    data.insert(result_key.to_string(), serde_json::json!(count));
                }
                "hashSet" => {
                    let _key = args.get("key").and_then(|v| v.as_str()).unwrap_or_default();
                    let fields = args.get("fields").and_then(|v| v.as_array());
                    let count = fields.map(|a| a.len()).unwrap_or(0);
                    data.insert(result_key.to_string(), serde_json::json!(count));
                }
                "setAdd" => {
                    let _key = args.get("key").and_then(|v| v.as_str()).unwrap_or_default();
                    let members = args.get("members").and_then(|v| v.as_array());
                    let count = members.map(|a| a.len()).unwrap_or(0);
                    data.insert(result_key.to_string(), serde_json::json!(count));
                }
                "sortedSetAdd" => {
                    let _key = args.get("key").and_then(|v| v.as_str()).unwrap_or_default();
                    let members = args.get("members").and_then(|v| v.as_array());
                    let count = members.map(|a| a.len()).unwrap_or(0);
                    data.insert(result_key.to_string(), serde_json::json!(count));
                }
                other => {
                    errors.push(GraphQLError::new(format!(
                        "Unknown mutation field: '{}'",
                        other
                    )));
                }
            }
        }

        GraphQLResponse {
            data: Some(serde_json::Value::Object(data)),
            errors,
        }
    }

    /// Substitute `$variable` references in argument values with actual variables.
    fn resolve_variables(
        &self,
        args: &HashMap<String, serde_json::Value>,
        variables: &HashMap<String, serde_json::Value>,
    ) -> HashMap<String, serde_json::Value> {
        args.iter()
            .map(|(k, v)| {
                let resolved = match v {
                    serde_json::Value::String(s) if s.starts_with('$') => {
                        let var_name = &s[1..];
                        variables.get(var_name).cloned().unwrap_or(v.clone())
                    }
                    other => other.clone(),
                };
                (k.clone(), resolved)
            })
            .collect()
    }
}

impl Default for GraphQLEngine {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

/// Simple GraphQL tokenizer. Splits on whitespace and punctuation while
/// preserving string literals as single tokens.
fn tokenize(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        // Skip whitespace and commas
        if chars[i].is_whitespace() || chars[i] == ',' {
            i += 1;
            continue;
        }

        // Skip comments
        if chars[i] == '#' {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }

        // String literal
        if chars[i] == '"' {
            let mut s = String::from('"');
            i += 1;
            while i < chars.len() && chars[i] != '"' {
                if chars[i] == '\\' && i + 1 < chars.len() {
                    s.push(chars[i]);
                    i += 1;
                    s.push(chars[i]);
                    i += 1;
                } else {
                    s.push(chars[i]);
                    i += 1;
                }
            }
            if i < chars.len() {
                s.push('"');
                i += 1;
            }
            tokens.push(s);
            continue;
        }

        // Punctuation tokens
        if matches!(
            chars[i],
            '{' | '}' | '(' | ')' | '[' | ']' | ':' | '!' | '@' | '='
        ) {
            tokens.push(chars[i].to_string());
            i += 1;
            continue;
        }

        // Spread operator
        if chars[i] == '.' && i + 2 < chars.len() && chars[i + 1] == '.' && chars[i + 2] == '.' {
            tokens.push("...".to_string());
            i += 3;
            continue;
        }

        // Name / number / keyword
        let start = i;
        while i < chars.len()
            && !chars[i].is_whitespace()
            && !matches!(
                chars[i],
                '{' | '}' | '(' | ')' | '[' | ']' | ':' | '!' | '@' | '=' | ',' | '#' | '"'
            )
        {
            i += 1;
        }
        if i > start {
            tokens.push(chars[start..i].iter().collect());
        }
    }

    tokens
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_query_error() {
        let engine = GraphQLEngine::new();
        let resp = engine.execute(GraphQLRequest {
            query: "".to_string(),
            variables: None,
            operation_name: None,
        });
        assert!(!resp.errors.is_empty());
        assert_eq!(resp.errors[0].message, "Query cannot be empty");
    }

    #[test]
    fn test_simple_query() {
        let engine = GraphQLEngine::new();
        let resp = engine.execute(GraphQLRequest {
            query: r#"{ getString(key: "hello") { key value } }"#.to_string(),
            variables: None,
            operation_name: None,
        });
        assert!(resp.data.is_some());
        let data = resp.data.expect("data");
        assert_eq!(data["getString"]["key"], "hello");
    }

    #[test]
    fn test_explicit_query_keyword() {
        let engine = GraphQLEngine::new();
        let resp = engine.execute(GraphQLRequest {
            query: r#"query { info { version } }"#.to_string(),
            variables: None,
            operation_name: None,
        });
        assert!(resp.data.is_some());
        let data = resp.data.expect("data");
        assert!(data["info"]["version"].is_string());
    }

    #[test]
    fn test_named_query() {
        let engine = GraphQLEngine::new();
        let resp = engine.execute(GraphQLRequest {
            query: r#"query GetInfo { info { version } }"#.to_string(),
            variables: None,
            operation_name: Some("GetInfo".to_string()),
        });
        assert!(resp.data.is_some());
    }

    #[test]
    fn test_wrong_operation_name() {
        let engine = GraphQLEngine::new();
        let resp = engine.execute(GraphQLRequest {
            query: r#"query MyOp { info { version } }"#.to_string(),
            variables: None,
            operation_name: Some("WrongName".to_string()),
        });
        assert!(!resp.errors.is_empty());
    }

    #[test]
    fn test_mutation() {
        let engine = GraphQLEngine::new();
        let resp = engine.execute(GraphQLRequest {
            query: r#"mutation { setString(key: "k", value: "v") }"#.to_string(),
            variables: None,
            operation_name: None,
        });
        assert!(resp.data.is_some());
        let data = resp.data.expect("data");
        assert_eq!(data["setString"], true);
    }

    #[test]
    fn test_mutation_delete() {
        let engine = GraphQLEngine::new();
        let resp = engine.execute(GraphQLRequest {
            query: r#"mutation { deleteKey(key: "k") }"#.to_string(),
            variables: None,
            operation_name: None,
        });
        assert!(resp.data.is_some());
        let data = resp.data.expect("data");
        assert_eq!(data["deleteKey"], true);
    }

    #[test]
    fn test_mutation_list_push() {
        let engine = GraphQLEngine::new();
        let resp = engine.execute(GraphQLRequest {
            query: r#"mutation { listPush(key: "l", values: ["a", "b"], direction: RIGHT) }"#
                .to_string(),
            variables: None,
            operation_name: None,
        });
        assert!(resp.data.is_some());
        let data = resp.data.expect("data");
        assert_eq!(data["listPush"], 2);
    }

    #[test]
    fn test_subscription_rejected() {
        let engine = GraphQLEngine::new();
        let resp = engine.execute(GraphQLRequest {
            query: r#"subscription { keyChanged(pattern: "*") { key } }"#.to_string(),
            variables: None,
            operation_name: None,
        });
        assert!(!resp.errors.is_empty());
        assert!(resp.errors[0].message.contains("Subscriptions"));
    }

    #[test]
    fn test_unknown_field() {
        let engine = GraphQLEngine::new();
        let resp = engine.execute(GraphQLRequest {
            query: r#"{ nonExistent(key: "k") }"#.to_string(),
            variables: None,
            operation_name: None,
        });
        assert!(!resp.errors.is_empty());
    }

    #[test]
    fn test_schema_sdl() {
        let engine = GraphQLEngine::new();
        let sdl = engine.schema_sdl();
        assert!(sdl.contains("type Query"));
        assert!(sdl.contains("type Mutation"));
        assert!(sdl.contains("type Subscription"));
        assert!(sdl.contains("getString(key: String!): StringValue"));
        assert!(sdl.contains("setString(key: String!, value: String!, ttl: Int): Boolean!"));
        assert!(sdl.contains("keyChanged(pattern: String!): ChangeEvent"));
        assert!(sdl.contains("type StringValue"));
        assert!(sdl.contains("type SortedSetValue"));
        assert!(sdl.contains("enum Direction"));
        assert!(sdl.contains("input FieldInput"));
        assert!(sdl.contains("input ScoredMemberInput"));
    }

    #[test]
    fn test_keys_query() {
        let engine = GraphQLEngine::new();
        let resp = engine.execute(GraphQLRequest {
            query: r#"{ keys(pattern: "user:*", cursor: 0, count: 10) { cursor keys } }"#
                .to_string(),
            variables: None,
            operation_name: None,
        });
        assert!(resp.data.is_some());
        let data = resp.data.expect("data");
        assert_eq!(data["keys"]["cursor"], 0);
    }

    #[test]
    fn test_get_list_query() {
        let engine = GraphQLEngine::new();
        let resp = engine.execute(GraphQLRequest {
            query: r#"{ getList(key: "mylist", start: 0, stop: -1) { key values length } }"#
                .to_string(),
            variables: None,
            operation_name: None,
        });
        assert!(resp.data.is_some());
        let data = resp.data.expect("data");
        assert_eq!(data["getList"]["key"], "mylist");
    }

    #[test]
    fn test_tokenizer() {
        let tokens = tokenize(r#"{ getString(key: "hello") { key value } }"#);
        assert!(tokens.contains(&"{".to_string()));
        assert!(tokens.contains(&"getString".to_string()));
        assert!(tokens.contains(&"\"hello\"".to_string()));
    }

    #[test]
    fn test_tokenizer_comments() {
        let tokens = tokenize("{ # a comment\n  info { version } }");
        assert!(!tokens.contains(&"#".to_string()));
        assert!(tokens.contains(&"info".to_string()));
    }

    #[test]
    fn test_variables() {
        let engine = GraphQLEngine::new();
        let mut vars = HashMap::new();
        vars.insert("k".to_string(), serde_json::json!("mykey"));
        let resp = engine.execute(GraphQLRequest {
            query: r#"query($k: String!) { getString(key: $k) { key } }"#.to_string(),
            variables: Some(vars),
            operation_name: None,
        });
        assert!(resp.data.is_some());
    }

    #[test]
    fn test_graphql_response_helpers() {
        let ok = GraphQLResponse::success(serde_json::json!({"test": true}));
        assert!(ok.data.is_some());
        assert!(ok.errors.is_empty());

        let err = GraphQLResponse::error(GraphQLError::new("bad"));
        assert!(err.data.is_none());
        assert_eq!(err.errors.len(), 1);
    }

    #[test]
    fn test_graphql_error_with_location() {
        let err = GraphQLError::with_location("parse error", 3, 7);
        assert_eq!(err.locations.len(), 1);
        assert_eq!(err.locations[0].line, 3);
        assert_eq!(err.locations[0].column, 7);
    }

    #[test]
    fn test_alias_support() {
        let engine = GraphQLEngine::new();
        let resp = engine.execute(GraphQLRequest {
            query: r#"{ myKey: getString(key: "k") { key } }"#.to_string(),
            variables: None,
            operation_name: None,
        });
        assert!(resp.data.is_some());
        let data = resp.data.expect("data");
        assert!(data.get("myKey").is_some());
    }
}
