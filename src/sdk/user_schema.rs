//! User-defined Schema DSL for type-safe SDK generation.
//!
//! Users define their data model in a TOML/YAML file and the SDK generator
//! produces strongly-typed client code for multiple languages.
//!
//! # Example Schema (TOML)
//!
//! ```toml
//! [types.User]
//! key_pattern = "users:{id}"
//! fields.id = "string"
//! fields.name = "string"
//! fields.email = "string"
//! fields.created_at = "timestamp"
//!
//! [types.User.indexes]
//! email = { unique = true }
//!
//! [types.Order]
//! key_pattern = "orders:{id}"
//! fields.id = "string"
//! fields.user_id = "string"
//! fields.total = "float"
//! fields.items = "list<string>"
//! relations.user = { target = "User", foreign_key = "user_id" }
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// A complete user-defined schema describing the data model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserSchema {
    /// Schema version.
    #[serde(default = "default_version")]
    pub version: String,
    /// Schema namespace (used as package/module name).
    #[serde(default)]
    pub namespace: String,
    /// Type definitions.
    #[serde(default)]
    pub types: HashMap<String, TypeDef>,
}

fn default_version() -> String {
    "1.0.0".to_string()
}

impl UserSchema {
    /// Parse a schema from TOML.
    pub fn from_toml(input: &str) -> Result<Self, SchemaError> {
        toml::from_str(input).map_err(|e| SchemaError::Parse(e.to_string()))
    }

    /// Parse a schema from JSON.
    pub fn from_json(input: &str) -> Result<Self, SchemaError> {
        serde_json::from_str(input).map_err(|e| SchemaError::Parse(e.to_string()))
    }

    /// Load from a file (infers format from extension).
    pub fn from_file(path: &Path) -> Result<Self, SchemaError> {
        let content = std::fs::read_to_string(path).map_err(|e| SchemaError::Io(e.to_string()))?;
        match path.extension().and_then(|e| e.to_str()) {
            Some("toml") => Self::from_toml(&content),
            Some("json") => Self::from_json(&content),
            _ => Self::from_toml(&content), // default to TOML
        }
    }

    /// Validate the schema for consistency.
    pub fn validate(&self) -> Result<(), Vec<SchemaError>> {
        let mut errors = Vec::new();

        for (name, typedef) in &self.types {
            if typedef.key_pattern.is_empty() {
                errors.push(SchemaError::Validation(format!(
                    "type '{}' has no key_pattern",
                    name
                )));
            }

            // Check that relation targets exist.
            for (rel_name, rel) in &typedef.relations {
                if !self.types.contains_key(&rel.target) {
                    errors.push(SchemaError::Validation(format!(
                        "type '{}' relation '{}' references unknown type '{}'",
                        name, rel_name, rel.target
                    )));
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Return all type names.
    pub fn type_names(&self) -> Vec<&str> {
        self.types.keys().map(|s| s.as_str()).collect()
    }
}

/// Definition of a user data type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeDef {
    /// Redis key pattern (e.g. "users:{id}").
    pub key_pattern: String,
    /// Field definitions (name → type string).
    #[serde(default)]
    pub fields: HashMap<String, String>,
    /// Index definitions.
    #[serde(default)]
    pub indexes: HashMap<String, IndexDef>,
    /// Relations to other types.
    #[serde(default)]
    pub relations: HashMap<String, RelationDef>,
    /// Documentation string.
    #[serde(default)]
    pub description: String,
}

/// Index definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    /// Whether the index enforces uniqueness.
    #[serde(default)]
    pub unique: bool,
    /// Index type hint.
    #[serde(default)]
    pub index_type: String,
}

/// Relation (foreign key) definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationDef {
    /// Target type name.
    pub target: String,
    /// Foreign key field name.
    pub foreign_key: String,
    /// Cardinality.
    #[serde(default = "default_cardinality")]
    pub cardinality: Cardinality,
}

fn default_cardinality() -> Cardinality {
    Cardinality::ManyToOne
}

/// Relationship cardinality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Cardinality {
    OneToOne,
    OneToMany,
    ManyToOne,
    ManyToMany,
}

/// Errors during schema processing.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SchemaError {
    #[error("parse error: {0}")]
    Parse(String),
    #[error("validation error: {0}")]
    Validation(String),
    #[error("I/O error: {0}")]
    Io(String),
}

/// Map a schema field type string to a target language type.
pub fn map_field_type(field_type: &str, language: &str) -> String {
    let base_type = field_type.trim();

    // Handle generic types like list<string>
    if let Some(inner) = base_type
        .strip_prefix("list<")
        .and_then(|s| s.strip_suffix('>'))
    {
        let inner_mapped = map_field_type(inner, language);
        return match language {
            "typescript" => format!("{}[]", inner_mapped),
            "python" => format!("List[{}]", inner_mapped),
            "go" => format!("[]{}", inner_mapped),
            "rust" => format!("Vec<{}>", inner_mapped),
            "java" => format!("List<{}>", inner_mapped),
            _ => format!("List<{}>", inner_mapped),
        };
    }

    match (base_type, language) {
        ("string", "typescript") => "string".into(),
        ("string", "python") => "str".into(),
        ("string", "go") => "string".into(),
        ("string", "rust") => "String".into(),
        ("string", "java") => "String".into(),

        ("int" | "integer", "typescript") => "number".into(),
        ("int" | "integer", "python") => "int".into(),
        ("int" | "integer", "go") => "int64".into(),
        ("int" | "integer", "rust") => "i64".into(),
        ("int" | "integer", "java") => "long".into(),

        ("float" | "double", "typescript") => "number".into(),
        ("float" | "double", "python") => "float".into(),
        ("float" | "double", "go") => "float64".into(),
        ("float" | "double", "rust") => "f64".into(),
        ("float" | "double", "java") => "double".into(),

        ("bool" | "boolean", "typescript") => "boolean".into(),
        ("bool" | "boolean", "python") => "bool".into(),
        ("bool" | "boolean", "go") => "bool".into(),
        ("bool" | "boolean", "rust") => "bool".into(),
        ("bool" | "boolean", "java") => "boolean".into(),

        ("timestamp", "typescript") => "Date".into(),
        ("timestamp", "python") => "datetime".into(),
        ("timestamp", "go") => "time.Time".into(),
        ("timestamp", "rust") => "chrono::DateTime<chrono::Utc>".into(),
        ("timestamp", "java") => "Instant".into(),

        ("bytes", "typescript") => "Buffer".into(),
        ("bytes", "python") => "bytes".into(),
        ("bytes", "go") => "[]byte".into(),
        ("bytes", "rust") => "Vec<u8>".into(),
        ("bytes", "java") => "byte[]".into(),

        _ => base_type.to_string(),
    }
}

/// Generate a TypeScript interface from a type definition.
pub fn generate_typescript_interface(name: &str, typedef: &TypeDef) -> String {
    let mut out = String::new();
    if !typedef.description.is_empty() {
        out.push_str(&format!("/** {} */\n", typedef.description));
    }
    out.push_str(&format!("export interface {} {{\n", name));
    for (field_name, field_type) in &typedef.fields {
        out.push_str(&format!(
            "  {}: {};\n",
            field_name,
            map_field_type(field_type, "typescript")
        ));
    }
    for (rel_name, rel) in &typedef.relations {
        match rel.cardinality {
            Cardinality::OneToMany | Cardinality::ManyToMany => {
                out.push_str(&format!("  {}?: {}[];\n", rel_name, rel.target));
            }
            _ => {
                out.push_str(&format!("  {}?: {};\n", rel_name, rel.target));
            }
        }
    }
    out.push_str("}\n");
    out
}

/// Generate a Python dataclass from a type definition.
pub fn generate_python_dataclass(name: &str, typedef: &TypeDef) -> String {
    let mut out = String::new();
    out.push_str("@dataclass\n");
    out.push_str(&format!("class {}:\n", name));
    if !typedef.description.is_empty() {
        out.push_str(&format!("    \"\"\"{}\"\"\"\n", typedef.description));
    }
    for (field_name, field_type) in &typedef.fields {
        out.push_str(&format!(
            "    {}: {}\n",
            field_name,
            map_field_type(field_type, "python")
        ));
    }
    if typedef.fields.is_empty() {
        out.push_str("    pass\n");
    }
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn parse_toml_schema() {
        let toml = r#"
version = "1.0.0"
namespace = "myapp"

[types.User]
key_pattern = "users:{id}"
description = "A user account"

[types.User.fields]
id = "string"
name = "string"
email = "string"
created_at = "timestamp"

[types.User.indexes.email]
unique = true

[types.Order]
key_pattern = "orders:{id}"

[types.Order.fields]
id = "string"
user_id = "string"
total = "float"
items = "list<string>"

[types.Order.relations.user]
target = "User"
foreign_key = "user_id"
"#;
        let schema = UserSchema::from_toml(toml).unwrap();
        assert_eq!(schema.types.len(), 2);
        assert!(schema.types.contains_key("User"));
        assert!(schema.types.contains_key("Order"));
        assert!(schema.validate().is_ok());
    }

    #[test]
    fn validation_catches_unknown_relation_target() {
        let toml = r#"
[types.Orphan]
key_pattern = "orphans:{id}"

[types.Orphan.fields]
id = "string"

[types.Orphan.relations.parent]
target = "NonExistent"
foreign_key = "parent_id"
"#;
        let schema = UserSchema::from_toml(toml).unwrap();
        let errors = schema.validate().unwrap_err();
        assert!(!errors.is_empty());
    }

    #[test]
    fn type_mapping() {
        assert_eq!(map_field_type("string", "typescript"), "string");
        assert_eq!(map_field_type("int", "python"), "int");
        assert_eq!(map_field_type("float", "rust"), "f64");
        assert_eq!(map_field_type("list<string>", "typescript"), "string[]");
        assert_eq!(map_field_type("list<int>", "python"), "List[int]");
        assert_eq!(
            map_field_type("timestamp", "rust"),
            "chrono::DateTime<chrono::Utc>"
        );
    }

    #[test]
    fn generate_ts_interface() {
        let typedef = TypeDef {
            key_pattern: "users:{id}".into(),
            fields: vec![
                ("id".into(), "string".into()),
                ("name".into(), "string".into()),
            ]
            .into_iter()
            .collect(),
            indexes: HashMap::new(),
            relations: HashMap::new(),
            description: "A user".into(),
        };
        let ts = generate_typescript_interface("User", &typedef);
        assert!(ts.contains("export interface User {"));
        assert!(ts.contains("id: string;"));
        assert!(ts.contains("/** A user */"));
    }

    #[test]
    fn generate_py_dataclass() {
        let typedef = TypeDef {
            key_pattern: "items:{id}".into(),
            fields: vec![
                ("id".into(), "string".into()),
                ("count".into(), "int".into()),
            ]
            .into_iter()
            .collect(),
            indexes: HashMap::new(),
            relations: HashMap::new(),
            description: "An item".into(),
        };
        let py = generate_python_dataclass("Item", &typedef);
        assert!(py.contains("class Item:"));
        assert!(py.contains("id: str"));
        assert!(py.contains("count: int"));
    }
}
