//! GraphQL schema definition
//!
//! Defines the Ferrite GraphQL schema programmatically: types, fields,
//! queries, mutations, and subscriptions.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Schema types
// ---------------------------------------------------------------------------

/// A scalar or composite type in the GraphQL schema.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FieldType {
    /// String scalar
    String,
    /// Integer scalar
    Int,
    /// Float scalar
    Float,
    /// Boolean scalar
    Boolean,
    /// Byte array (Base64-encoded in JSON)
    Bytes,
    /// A list of another type
    List(Box<FieldType>),
    /// A non-null wrapper
    NonNull(Box<FieldType>),
    /// A reference to a named object type
    Object(String),
}

impl std::fmt::Display for FieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldType::String => write!(f, "String"),
            FieldType::Int => write!(f, "Int"),
            FieldType::Float => write!(f, "Float"),
            FieldType::Boolean => write!(f, "Boolean"),
            FieldType::Bytes => write!(f, "Bytes"),
            FieldType::List(inner) => write!(f, "[{inner}]"),
            FieldType::NonNull(inner) => write!(f, "{inner}!"),
            FieldType::Object(name) => write!(f, "{name}"),
        }
    }
}

/// A field in a GraphQL object type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaField {
    /// Field name
    pub name: String,
    /// Field type
    pub field_type: FieldType,
    /// Human-readable description
    pub description: Option<String>,
    /// Arguments this field accepts (for query/mutation fields)
    pub arguments: Vec<SchemaArgument>,
}

/// An argument to a GraphQL field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaArgument {
    /// Argument name
    pub name: String,
    /// Argument type
    pub arg_type: FieldType,
    /// Default value (as JSON string)
    pub default_value: Option<String>,
    /// Description
    pub description: Option<String>,
}

/// A named object type in the schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaType {
    /// Type name
    pub name: String,
    /// Description
    pub description: Option<String>,
    /// Fields
    pub fields: Vec<SchemaField>,
}

/// The complete GraphQL schema for Ferrite.
#[derive(Debug, Clone)]
pub struct GraphQLSchema {
    /// Query type fields
    pub query_fields: Vec<SchemaField>,
    /// Mutation type fields
    pub mutation_fields: Vec<SchemaField>,
    /// Subscription type fields
    pub subscription_fields: Vec<SchemaField>,
    /// Named object types
    pub types: HashMap<String, SchemaType>,
}

impl GraphQLSchema {
    /// Build the default Ferrite GraphQL schema.
    pub fn build() -> Self {
        let mut schema = Self {
            query_fields: Vec::new(),
            mutation_fields: Vec::new(),
            subscription_fields: Vec::new(),
            types: HashMap::new(),
        };

        schema.register_types();
        schema.register_queries();
        schema.register_mutations();
        schema.register_subscriptions();
        schema
    }

    fn register_types(&mut self) {
        // KeyValue result type
        self.types.insert(
            "KeyValue".to_string(),
            SchemaType {
                name: "KeyValue".to_string(),
                description: Some("A key-value pair with optional metadata".to_string()),
                fields: vec![
                    field("key", FieldType::NonNull(Box::new(FieldType::String))),
                    field("value", FieldType::Bytes),
                    field("ttl", FieldType::Int),
                    field("type", FieldType::String),
                ],
            },
        );

        // VectorSearchResult type
        self.types.insert(
            "VectorSearchResult".to_string(),
            SchemaType {
                name: "VectorSearchResult".to_string(),
                description: Some("A vector similarity search result".to_string()),
                fields: vec![
                    field("id", FieldType::NonNull(Box::new(FieldType::String))),
                    field("score", FieldType::Float),
                    field("metadata", FieldType::String),
                    field("vector", FieldType::List(Box::new(FieldType::Float))),
                ],
            },
        );

        // ServerInfo type
        self.types.insert(
            "ServerInfo".to_string(),
            SchemaType {
                name: "ServerInfo".to_string(),
                description: Some("Server information and statistics".to_string()),
                fields: vec![
                    field("version", FieldType::String),
                    field("uptime_seconds", FieldType::Int),
                    field("connected_clients", FieldType::Int),
                    field("used_memory_bytes", FieldType::Int),
                    field("total_commands_processed", FieldType::Int),
                    field("keyspace_hits", FieldType::Int),
                    field("keyspace_misses", FieldType::Int),
                ],
            },
        );

        // HashField type
        self.types.insert(
            "HashField".to_string(),
            SchemaType {
                name: "HashField".to_string(),
                description: Some("A field-value pair in a hash".to_string()),
                fields: vec![
                    field("field", FieldType::NonNull(Box::new(FieldType::String))),
                    field("value", FieldType::Bytes),
                ],
            },
        );

        // TimeSeriesPoint type
        self.types.insert(
            "TimeSeriesPoint".to_string(),
            SchemaType {
                name: "TimeSeriesPoint".to_string(),
                description: Some("A timestamped data point".to_string()),
                fields: vec![
                    field("timestamp", FieldType::Int),
                    field("value", FieldType::Float),
                    field("labels", FieldType::String),
                ],
            },
        );
    }

    fn register_queries(&mut self) {
        // KV queries
        self.query_fields.push(query_field(
            "get",
            FieldType::Object("KeyValue".to_string()),
            "Retrieve a key-value pair",
            vec![required_arg("key", FieldType::String), arg("database", FieldType::Int, Some("0"))],
        ));
        self.query_fields.push(query_field(
            "mget",
            FieldType::List(Box::new(FieldType::Object("KeyValue".to_string()))),
            "Retrieve multiple keys",
            vec![required_arg("keys", FieldType::List(Box::new(FieldType::String)))],
        ));
        self.query_fields.push(query_field(
            "exists",
            FieldType::Boolean,
            "Check if a key exists",
            vec![required_arg("key", FieldType::String)],
        ));
        self.query_fields.push(query_field(
            "scan",
            FieldType::List(Box::new(FieldType::Object("KeyValue".to_string()))),
            "Scan keys matching a pattern",
            vec![
                arg("pattern", FieldType::String, Some("\"*\"")),
                arg("count", FieldType::Int, Some("100")),
                arg("cursor", FieldType::String, None),
            ],
        ));

        // Hash queries
        self.query_fields.push(query_field(
            "hgetall",
            FieldType::List(Box::new(FieldType::Object("HashField".to_string()))),
            "Get all fields of a hash",
            vec![required_arg("key", FieldType::String)],
        ));

        // Vector search query
        self.query_fields.push(query_field(
            "vectorSearch",
            FieldType::List(Box::new(FieldType::Object("VectorSearchResult".to_string()))),
            "Perform vector similarity search",
            vec![
                required_arg("index", FieldType::String),
                required_arg("vector", FieldType::List(Box::new(FieldType::Float))),
                arg("k", FieldType::Int, Some("10")),
                arg("filter", FieldType::String, None),
            ],
        ));

        // Server info
        self.query_fields.push(query_field(
            "serverInfo",
            FieldType::Object("ServerInfo".to_string()),
            "Get server information",
            vec![],
        ));

        // FerriteQL query
        self.query_fields.push(query_field(
            "query",
            FieldType::String,
            "Execute a FerriteQL query and return JSON results",
            vec![required_arg("fql", FieldType::String)],
        ));
    }

    fn register_mutations(&mut self) {
        self.mutation_fields.push(query_field(
            "set",
            FieldType::Boolean,
            "Set a key to a value",
            vec![
                required_arg("key", FieldType::String),
                required_arg("value", FieldType::String),
                arg("ttl", FieldType::Int, None),
                arg("nx", FieldType::Boolean, Some("false")),
                arg("xx", FieldType::Boolean, Some("false")),
            ],
        ));
        self.mutation_fields.push(query_field(
            "del",
            FieldType::Int,
            "Delete one or more keys",
            vec![required_arg("keys", FieldType::List(Box::new(FieldType::String)))],
        ));
        self.mutation_fields.push(query_field(
            "expire",
            FieldType::Boolean,
            "Set a TTL on a key",
            vec![
                required_arg("key", FieldType::String),
                required_arg("seconds", FieldType::Int),
            ],
        ));
        self.mutation_fields.push(query_field(
            "hset",
            FieldType::Int,
            "Set hash fields",
            vec![
                required_arg("key", FieldType::String),
                required_arg("fields", FieldType::String),
            ],
        ));
        self.mutation_fields.push(query_field(
            "vectorAdd",
            FieldType::Boolean,
            "Add a vector to an index",
            vec![
                required_arg("index", FieldType::String),
                required_arg("id", FieldType::String),
                required_arg("vector", FieldType::List(Box::new(FieldType::Float))),
                arg("metadata", FieldType::String, None),
            ],
        ));
        self.mutation_fields.push(query_field(
            "flushDb",
            FieldType::Boolean,
            "Flush the current database",
            vec![arg("database", FieldType::Int, Some("0"))],
        ));
    }

    fn register_subscriptions(&mut self) {
        self.subscription_fields.push(query_field(
            "keyChanged",
            FieldType::Object("KeyValue".to_string()),
            "Subscribe to changes on keys matching a pattern",
            vec![arg("pattern", FieldType::String, Some("\"*\""))],
        ));
        self.subscription_fields.push(query_field(
            "alerts",
            FieldType::String,
            "Subscribe to anomaly detection alerts",
            vec![arg("minSeverity", FieldType::String, Some("\"WARNING\""))],
        ));
    }

    /// Generate a GraphQL SDL (Schema Definition Language) representation.
    pub fn to_sdl(&self) -> String {
        let mut sdl = String::new();

        // Object types
        for schema_type in self.types.values() {
            if let Some(ref desc) = schema_type.description {
                sdl.push_str(&format!("\"\"\"{desc}\"\"\"\n"));
            }
            sdl.push_str(&format!("type {} {{\n", schema_type.name));
            for f in &schema_type.fields {
                sdl.push_str(&format!("  {}: {}\n", f.name, f.field_type));
            }
            sdl.push_str("}\n\n");
        }

        // Query type
        sdl.push_str("type Query {\n");
        for f in &self.query_fields {
            sdl.push_str(&format!("  {}", format_field_with_args(f)));
        }
        sdl.push_str("}\n\n");

        // Mutation type
        sdl.push_str("type Mutation {\n");
        for f in &self.mutation_fields {
            sdl.push_str(&format!("  {}", format_field_with_args(f)));
        }
        sdl.push_str("}\n\n");

        // Subscription type
        if !self.subscription_fields.is_empty() {
            sdl.push_str("type Subscription {\n");
            for f in &self.subscription_fields {
                sdl.push_str(&format!("  {}", format_field_with_args(f)));
            }
            sdl.push_str("}\n\n");
        }

        sdl.push_str("schema {\n  query: Query\n  mutation: Mutation\n");
        if !self.subscription_fields.is_empty() {
            sdl.push_str("  subscription: Subscription\n");
        }
        sdl.push_str("}\n");

        sdl
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn field(name: &str, field_type: FieldType) -> SchemaField {
    SchemaField {
        name: name.to_string(),
        field_type,
        description: None,
        arguments: Vec::new(),
    }
}

fn query_field(
    name: &str,
    return_type: FieldType,
    description: &str,
    arguments: Vec<SchemaArgument>,
) -> SchemaField {
    SchemaField {
        name: name.to_string(),
        field_type: return_type,
        description: Some(description.to_string()),
        arguments,
    }
}

fn required_arg(name: &str, arg_type: FieldType) -> SchemaArgument {
    SchemaArgument {
        name: name.to_string(),
        arg_type: FieldType::NonNull(Box::new(arg_type)),
        default_value: None,
        description: None,
    }
}

fn arg(name: &str, arg_type: FieldType, default: Option<&str>) -> SchemaArgument {
    SchemaArgument {
        name: name.to_string(),
        arg_type,
        default_value: default.map(|s| s.to_string()),
        description: None,
    }
}

fn format_field_with_args(f: &SchemaField) -> String {
    let mut s = f.name.clone();
    if !f.arguments.is_empty() {
        s.push('(');
        let args: Vec<String> = f
            .arguments
            .iter()
            .map(|a| {
                let mut arg_str = format!("{}: {}", a.name, a.arg_type);
                if let Some(ref default) = a.default_value {
                    arg_str.push_str(&format!(" = {default}"));
                }
                arg_str
            })
            .collect();
        s.push_str(&args.join(", "));
        s.push(')');
    }
    s.push_str(&format!(": {}\n", f.field_type));
    s
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_build() {
        let schema = GraphQLSchema::build();
        assert!(!schema.query_fields.is_empty());
        assert!(!schema.mutation_fields.is_empty());
        assert!(!schema.subscription_fields.is_empty());
        assert!(!schema.types.is_empty());
    }

    #[test]
    fn test_schema_has_vector_search() {
        let schema = GraphQLSchema::build();
        assert!(schema
            .query_fields
            .iter()
            .any(|f| f.name == "vectorSearch"));
    }

    #[test]
    fn test_schema_has_ferriteql() {
        let schema = GraphQLSchema::build();
        assert!(schema.query_fields.iter().any(|f| f.name == "query"));
    }

    #[test]
    fn test_sdl_generation() {
        let schema = GraphQLSchema::build();
        let sdl = schema.to_sdl();
        assert!(sdl.contains("type Query"));
        assert!(sdl.contains("type Mutation"));
        assert!(sdl.contains("type Subscription"));
        assert!(sdl.contains("vectorSearch"));
        assert!(sdl.contains("KeyValue"));
        assert!(sdl.contains("schema {"));
    }

    #[test]
    fn test_field_type_display() {
        assert_eq!(FieldType::String.to_string(), "String");
        assert_eq!(
            FieldType::NonNull(Box::new(FieldType::String)).to_string(),
            "String!"
        );
        assert_eq!(
            FieldType::List(Box::new(FieldType::Int)).to_string(),
            "[Int]"
        );
    }
}
