//! Schema introspection and type-safe client code generation.
//!
//! Provides tools to register schemas for key patterns, infer schemas from
//! sample data, validate queries against schemas, and generate typed client
//! code in TypeScript, Python, and Rust.

use std::collections::HashMap;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors returned by schema introspection and code generation.
#[derive(Debug, Clone, thiserror::Error, Serialize, Deserialize)]
pub enum SchemaError {
    /// A schema with the given name already exists.
    #[error("schema already exists: {0}")]
    AlreadyExists(String),

    /// The requested schema was not found.
    #[error("schema not found: {0}")]
    NotFound(String),

    /// The schema definition is invalid.
    #[error("invalid schema: {0}")]
    InvalidSchema(String),

    /// Validation produced one or more issues.
    #[error("validation failed")]
    ValidationFailed(Vec<ValidationIssue>),
}

// ---------------------------------------------------------------------------
// Field types
// ---------------------------------------------------------------------------

/// Supported field types for schema definitions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    /// UTF-8 string.
    String,
    /// 64-bit signed integer.
    Integer,
    /// 64-bit floating point.
    Float,
    /// Boolean value.
    Boolean,
    /// Raw byte sequence.
    Bytes,
    /// Ordered array of a single element type.
    Array(Box<FieldType>),
    /// Key-value map.
    Map,
    /// Arbitrary JSON value.
    Json,
    /// Unix timestamp (seconds since epoch).
    Timestamp,
}

impl FieldType {
    /// Returns the TypeScript type name for this field type.
    fn to_typescript(&self) -> String {
        match self {
            FieldType::String => "string".to_string(),
            FieldType::Integer | FieldType::Float | FieldType::Timestamp => "number".to_string(),
            FieldType::Boolean => "boolean".to_string(),
            FieldType::Bytes => "Uint8Array".to_string(),
            FieldType::Array(inner) => format!("{}[]", inner.to_typescript()),
            FieldType::Map | FieldType::Json => "Record<string, unknown>".to_string(),
        }
    }

    /// Returns the Python type annotation for this field type.
    fn to_python(&self) -> String {
        match self {
            FieldType::String => "str".to_string(),
            FieldType::Integer | FieldType::Timestamp => "int".to_string(),
            FieldType::Float => "float".to_string(),
            FieldType::Boolean => "bool".to_string(),
            FieldType::Bytes => "bytes".to_string(),
            FieldType::Array(inner) => format!("List[{}]", inner.to_python()),
            FieldType::Map | FieldType::Json => "Dict[str, Any]".to_string(),
        }
    }

    /// Returns the Rust type name for this field type.
    fn to_rust(&self) -> String {
        match self {
            FieldType::String => "String".to_string(),
            FieldType::Integer => "i64".to_string(),
            FieldType::Float => "f64".to_string(),
            FieldType::Boolean => "bool".to_string(),
            FieldType::Bytes => "Vec<u8>".to_string(),
            FieldType::Timestamp => "u64".to_string(),
            FieldType::Array(inner) => format!("Vec<{}>", inner.to_rust()),
            FieldType::Map | FieldType::Json => "serde_json::Value".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Schema types
// ---------------------------------------------------------------------------

/// A single field within a schema definition.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FieldDefinition {
    /// Field name.
    pub name: String,
    /// Data type of the field.
    pub field_type: FieldType,
    /// Whether the field can be null / absent.
    pub nullable: bool,
    /// Optional human-readable description.
    pub description: Option<String>,
    /// Optional default value (serialised as a string).
    pub default_value: Option<String>,
}

/// A complete schema definition for a key pattern.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemaDefinition {
    /// Schema name (unique identifier).
    pub name: String,
    /// Key pattern this schema applies to (e.g. `"users:*"`).
    pub key_pattern: String,
    /// Ordered list of fields.
    pub fields: Vec<FieldDefinition>,
    /// Schema version number.
    pub version: u32,
    /// Optional human-readable description.
    pub description: Option<String>,
    /// Creation timestamp (Unix seconds).
    pub created_at: u64,
}

/// Description of a key pattern produced by introspection.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyPatternDescription {
    /// The pattern that was described.
    pub pattern: String,
    /// Estimated number of matching keys.
    pub estimated_keys: u64,
    /// Field names commonly found across matching keys.
    pub common_fields: Vec<String>,
    /// An example key matching the pattern, if available.
    pub sample_key: Option<String>,
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Severity level for a validation issue.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationSeverity {
    /// Hard error — the query cannot run.
    Error,
    /// Potential problem that may cause unexpected results.
    Warning,
    /// Informational hint.
    Info,
}

/// A single validation issue found during query or schema validation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidationIssue {
    /// How severe the issue is.
    pub severity: ValidationSeverity,
    /// Human-readable message describing the issue.
    pub message: String,
    /// Optional location such as a column name or expression.
    pub location: Option<String>,
}

// ---------------------------------------------------------------------------
// Code generation types
// ---------------------------------------------------------------------------

/// Target language for code generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Language {
    /// TypeScript with interfaces.
    TypeScript,
    /// Python with dataclasses.
    Python,
    /// Rust with structs.
    Rust,
}

/// Configuration options for the code generator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CodeGenConfig {
    /// Generate runtime validation helpers.
    pub include_validation: bool,
    /// Generate builder-pattern constructors.
    pub include_builder_pattern: bool,
    /// Generate serialization helpers.
    pub include_serialization: bool,
    /// Number of spaces per indentation level.
    pub indent_size: usize,
}

impl Default for CodeGenConfig {
    fn default() -> Self {
        Self {
            include_validation: true,
            include_builder_pattern: true,
            include_serialization: true,
            indent_size: 2,
        }
    }
}

/// The output of a code generation run.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GeneratedCode {
    /// Target language.
    pub language: Language,
    /// Generated type / interface / struct definitions.
    pub type_definitions: String,
    /// Generated typed client wrapper code.
    pub client_code: String,
    /// Suggested file name for the output.
    pub file_name: String,
    /// Name of the schema that was generated.
    pub schema_name: String,
}

// ---------------------------------------------------------------------------
// SchemaIntrospector
// ---------------------------------------------------------------------------

/// Manages registered schemas and provides introspection helpers.
pub struct SchemaIntrospector {
    /// Registered schemas keyed by name.
    schemas: RwLock<HashMap<String, SchemaDefinition>>,
}

impl SchemaIntrospector {
    /// Creates a new, empty introspector.
    pub fn new() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
        }
    }

    /// Registers a schema definition.
    ///
    /// Returns `SchemaError::AlreadyExists` if a schema with the same name is
    /// already registered.
    pub fn register_schema(&self, schema: SchemaDefinition) -> Result<(), SchemaError> {
        if schema.name.is_empty() {
            return Err(SchemaError::InvalidSchema(
                "schema name cannot be empty".to_string(),
            ));
        }
        if schema.fields.is_empty() {
            return Err(SchemaError::InvalidSchema(
                "schema must have at least one field".to_string(),
            ));
        }

        let mut schemas = self.schemas.write();
        if schemas.contains_key(&schema.name) {
            return Err(SchemaError::AlreadyExists(schema.name.clone()));
        }
        schemas.insert(schema.name.clone(), schema);
        Ok(())
    }

    /// Returns a clone of the schema with the given name, if it exists.
    pub fn get_schema(&self, name: &str) -> Option<SchemaDefinition> {
        self.schemas.read().get(name).cloned()
    }

    /// Returns all registered schemas.
    pub fn list_schemas(&self) -> Vec<SchemaDefinition> {
        self.schemas.read().values().cloned().collect()
    }

    /// Describes a key pattern by returning metadata about its structure.
    pub fn describe_key_pattern(&self, pattern: &str) -> KeyPatternDescription {
        let schemas = self.schemas.read();
        let mut common_fields = Vec::new();
        let mut sample_key = None;

        for schema in schemas.values() {
            if schema.key_pattern == pattern {
                common_fields = schema.fields.iter().map(|f| f.name.clone()).collect();
                let prefix = pattern.replace('*', "");
                sample_key = Some(format!("{}example_id", prefix));
                break;
            }
        }

        KeyPatternDescription {
            pattern: pattern.to_string(),
            estimated_keys: 0,
            common_fields,
            sample_key,
        }
    }

    /// Infers a `SchemaDefinition` from a set of sample key-value pairs.
    ///
    /// Each value is expected to be a JSON object; fields are merged across
    /// all samples and the most specific type is chosen.
    pub fn infer_schema_from_sample(
        &self,
        keys: &[(String, serde_json::Value)],
    ) -> SchemaDefinition {
        let mut field_types: HashMap<String, FieldType> = HashMap::new();
        let mut field_nullable: HashMap<String, bool> = HashMap::new();
        let mut all_field_names: Vec<String> = Vec::new();

        let key_pattern = if let Some((first_key, _)) = keys.first() {
            infer_pattern(first_key)
        } else {
            "*".to_string()
        };

        let schema_name = key_pattern
            .split(':')
            .next()
            .unwrap_or("inferred")
            .to_string();

        // Collect field information from all samples.
        for (_key, value) in keys {
            if let serde_json::Value::Object(map) = value {
                // Mark fields absent from this sample as nullable.
                for name in &all_field_names {
                    if !map.contains_key(name) {
                        field_nullable.insert(name.clone(), true);
                    }
                }

                for (field_name, field_value) in map {
                    if !field_types.contains_key(field_name) {
                        all_field_names.push(field_name.clone());
                    }
                    let ft = json_value_to_field_type(field_value);
                    field_types.insert(field_name.clone(), ft);
                    field_nullable.entry(field_name.clone()).or_insert(false);
                    if field_value.is_null() {
                        field_nullable.insert(field_name.clone(), true);
                    }
                }
            }
        }

        let fields: Vec<FieldDefinition> = all_field_names
            .iter()
            .map(|name| FieldDefinition {
                name: name.clone(),
                field_type: field_types
                    .get(name)
                    .cloned()
                    .unwrap_or(FieldType::String),
                nullable: field_nullable.get(name).copied().unwrap_or(false),
                description: None,
                default_value: None,
            })
            .collect();

        SchemaDefinition {
            name: schema_name,
            key_pattern,
            fields,
            version: 1,
            description: Some("Auto-inferred from sample data".to_string()),
            created_at: 0,
        }
    }

    /// Validates a query string against a named schema.
    ///
    /// Returns a list of issues found. An empty list means the query is valid
    /// with respect to the schema.
    pub fn validate_query(&self, query: &str, schema_name: &str) -> Vec<ValidationIssue> {
        let mut issues = Vec::new();

        let schemas = self.schemas.read();
        let schema = match schemas.get(schema_name) {
            Some(s) => s,
            None => {
                issues.push(ValidationIssue {
                    severity: ValidationSeverity::Error,
                    message: format!("schema '{}' not found", schema_name),
                    location: None,
                });
                return issues;
            }
        };

        if query.trim().is_empty() {
            issues.push(ValidationIssue {
                severity: ValidationSeverity::Error,
                message: "query is empty".to_string(),
                location: None,
            });
            return issues;
        }

        // Check that referenced columns exist in the schema.
        let upper = query.to_uppercase();
        if upper.contains("SELECT") && upper.contains("FROM") {
            let field_names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();

            // Extract column references from WHERE clause.
            if let Some(where_pos) = upper.find("WHERE") {
                let after_where = &query[where_pos + 5..];
                for field in &field_names {
                    // Nothing to flag — field is known.
                    let _ = field;
                }
                // Look for identifiers that don't match any known field.
                for token in after_where.split_whitespace() {
                    let cleaned = token.trim_matches(|c: char| !c.is_alphanumeric() && c != '_');
                    if cleaned.is_empty() {
                        continue;
                    }
                    // Skip SQL keywords and literal values.
                    let kw = cleaned.to_uppercase();
                    if matches!(
                        kw.as_str(),
                        "AND" | "OR" | "NOT" | "IN" | "IS" | "NULL"
                            | "LIKE" | "BETWEEN" | "TRUE" | "FALSE"
                            | "ORDER" | "BY" | "ASC" | "DESC"
                            | "LIMIT" | "OFFSET" | "GROUP" | "HAVING"
                    ) {
                        continue;
                    }
                    // Skip numeric literals and quoted strings.
                    if cleaned.starts_with('\'')
                        || cleaned.ends_with('\'')
                        || cleaned.parse::<f64>().is_ok()
                    {
                        continue;
                    }
                    // Skip operators.
                    if matches!(cleaned, "=" | "!=" | "<>" | "<" | ">" | "<=" | ">=") {
                        continue;
                    }

                    if !field_names.contains(&cleaned)
                        && !cleaned.contains(':')
                        && !cleaned.contains('*')
                    {
                        issues.push(ValidationIssue {
                            severity: ValidationSeverity::Warning,
                            message: format!(
                                "column '{}' not found in schema '{}'",
                                cleaned, schema_name
                            ),
                            location: Some(cleaned.to_string()),
                        });
                    }
                }
            }

            // Informational: no WHERE clause.
            if !upper.contains("WHERE") {
                issues.push(ValidationIssue {
                    severity: ValidationSeverity::Info,
                    message: "query has no WHERE clause — full scan".to_string(),
                    location: None,
                });
            }
        }

        issues
    }
}

impl Default for SchemaIntrospector {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// CodeGenerator
// ---------------------------------------------------------------------------

/// Generates type-safe client code from schema definitions.
pub struct CodeGenerator {
    /// Code generation configuration.
    pub config: CodeGenConfig,
}

impl CodeGenerator {
    /// Creates a new code generator with the given configuration.
    pub fn new(config: CodeGenConfig) -> Self {
        Self { config }
    }

    /// Generates a TypeScript interface and typed client for the given schema.
    pub fn generate_typescript(&self, schema: &SchemaDefinition) -> GeneratedCode {
        let indent = " ".repeat(self.config.indent_size);
        let name = pascal_case(&schema.name);

        // --- type definitions ---
        let mut td = format!("export interface {} {{\n", name);
        for field in &schema.fields {
            let ts_type = field.field_type.to_typescript();
            let optional = if field.nullable { "?" } else { "" };
            if let Some(desc) = &field.description {
                td.push_str(&format!("{}/** {} */\n", indent, desc));
            }
            td.push_str(&format!(
                "{}{}{}: {};\n",
                indent, field.name, optional, ts_type
            ));
        }
        td.push_str("}\n");

        // --- client code ---
        let mut cc = format!(
            "export class {}Client {{\n{indent}private prefix: string;\n\n",
            name,
            indent = indent
        );
        cc.push_str(&format!(
            "{indent}constructor(prefix: string = \"{}\") {{\n\
             {indent}{indent}this.prefix = prefix;\n\
             {indent}}}\n\n",
            schema.key_pattern.replace('*', ""),
            indent = indent
        ));
        // get
        cc.push_str(&format!(
            "{indent}async get(id: string): Promise<{name} | null> {{\n\
             {indent}{indent}const key = `${{this.prefix}}${{id}}`;\n\
             {indent}{indent}const raw = await this.sendCommand('GET', key);\n\
             {indent}{indent}return raw ? JSON.parse(raw) as {name} : null;\n\
             {indent}}}\n\n",
            indent = indent,
            name = name,
        ));
        // set
        cc.push_str(&format!(
            "{indent}async set(id: string, data: {name}): Promise<void> {{\n\
             {indent}{indent}const key = `${{this.prefix}}${{id}}`;\n\
             {indent}{indent}await this.sendCommand('SET', key, JSON.stringify(data));\n\
             {indent}}}\n\n",
            indent = indent,
            name = name,
        ));
        // find
        cc.push_str(&format!(
            "{indent}async find(query: string): Promise<{name}[]> {{\n\
             {indent}{indent}const raw = await this.sendCommand('QUERY', query);\n\
             {indent}{indent}return raw as unknown as {name}[];\n\
             {indent}}}\n\n",
            indent = indent,
            name = name,
        ));
        // sendCommand stub
        cc.push_str(&format!(
            "{indent}private async sendCommand(..._args: string[]): Promise<string | null> {{\n\
             {indent}{indent}throw new Error('sendCommand not implemented — connect a Ferrite client');\n\
             {indent}}}\n",
            indent = indent,
        ));
        cc.push_str("}\n");

        // --- validation helper ---
        if self.config.include_validation {
            td.push_str(&format!(
                "\nexport function validate{name}(data: unknown): data is {name} {{\n\
                 {indent}if (typeof data !== 'object' || data === null) return false;\n\
                 {indent}return true;\n\
                 }}\n",
                name = name,
                indent = indent,
            ));
        }

        GeneratedCode {
            language: Language::TypeScript,
            type_definitions: td,
            client_code: cc,
            file_name: format!("{}.ts", schema.name),
            schema_name: schema.name.clone(),
        }
    }

    /// Generates a Python dataclass and typed client for the given schema.
    pub fn generate_python(&self, schema: &SchemaDefinition) -> GeneratedCode {
        let indent = " ".repeat(self.config.indent_size.max(4));
        let name = pascal_case(&schema.name);

        // --- type definitions ---
        let mut td = String::new();
        td.push_str("from __future__ import annotations\n\n");
        td.push_str("from dataclasses import dataclass, field\n");
        td.push_str("from typing import Any, Dict, List, Optional\n\n\n");

        td.push_str("@dataclass\n");
        td.push_str(&format!("class {}:\n", name));
        if let Some(desc) = &schema.description {
            td.push_str(&format!("{}\"\"\"{}.\"\"\"\n\n", indent, desc));
        }

        // Required fields first, then optional.
        let mut required: Vec<&FieldDefinition> = Vec::new();
        let mut optional: Vec<&FieldDefinition> = Vec::new();
        for f in &schema.fields {
            if f.nullable || f.default_value.is_some() {
                optional.push(f);
            } else {
                required.push(f);
            }
        }

        for f in &required {
            let py_type = f.field_type.to_python();
            td.push_str(&format!("{}{}: {}\n", indent, f.name, py_type));
        }
        for f in &optional {
            let py_type = f.field_type.to_python();
            let default = f
                .default_value
                .as_deref()
                .unwrap_or("None");
            td.push_str(&format!(
                "{}{}: Optional[{}] = {}\n",
                indent, f.name, py_type, default
            ));
        }
        if required.is_empty() && optional.is_empty() {
            td.push_str(&format!("{}pass\n", indent));
        }

        // --- client code ---
        let mut cc = String::new();
        cc.push_str(&format!("\nclass {}Client:\n", name));
        cc.push_str(&format!(
            "{}def __init__(self, prefix: str = \"{}\"):\n",
            indent,
            schema.key_pattern.replace('*', "")
        ));
        cc.push_str(&format!("{}{}self.prefix = prefix\n\n", indent, indent));

        // get
        cc.push_str(&format!(
            "{}def get(self, id: str) -> Optional[{}]:\n",
            indent, name
        ));
        cc.push_str(&format!(
            "{}{}raise NotImplementedError\n\n",
            indent, indent
        ));
        // set
        cc.push_str(&format!(
            "{}def set(self, id: str, data: {}) -> None:\n",
            indent, name
        ));
        cc.push_str(&format!(
            "{}{}raise NotImplementedError\n\n",
            indent, indent
        ));
        // find
        cc.push_str(&format!(
            "{}def find(self, query: str) -> list[{}]:\n",
            indent, name
        ));
        cc.push_str(&format!(
            "{}{}raise NotImplementedError\n",
            indent, indent
        ));

        GeneratedCode {
            language: Language::Python,
            type_definitions: td,
            client_code: cc,
            file_name: format!("{}.py", schema.name),
            schema_name: schema.name.clone(),
        }
    }

    /// Generates a Rust struct and typed client for the given schema.
    pub fn generate_rust(&self, schema: &SchemaDefinition) -> GeneratedCode {
        let name = pascal_case(&schema.name);

        // --- type definitions ---
        let mut td = String::new();
        td.push_str("use serde::{Deserialize, Serialize};\n\n");
        if let Some(desc) = &schema.description {
            td.push_str(&format!("/// {}\n", desc));
        }
        td.push_str("#[derive(Clone, Debug, Serialize, Deserialize)]\n");
        td.push_str(&format!("pub struct {} {{\n", name));
        for f in &schema.fields {
            let rust_type = if f.nullable {
                format!("Option<{}>", f.field_type.to_rust())
            } else {
                f.field_type.to_rust()
            };
            if let Some(desc) = &f.description {
                td.push_str(&format!("    /// {}\n", desc));
            }
            td.push_str(&format!("    pub {}: {},\n", f.name, rust_type));
        }
        td.push_str("}\n");

        // --- builder ---
        if self.config.include_builder_pattern {
            td.push_str(&format!(
                "\n/// Builder for [`{name}`].\n\
                 #[derive(Default)]\n\
                 pub struct {name}Builder {{\n\
                 {fields}\
                 }}\n\n\
                 impl {name}Builder {{\n\
                 {setters}\
                 }}\n",
                name = name,
                fields = schema
                    .fields
                    .iter()
                    .map(|f| format!(
                        "    {}: Option<{}>,\n",
                        f.name,
                        f.field_type.to_rust()
                    ))
                    .collect::<String>(),
                setters = schema
                    .fields
                    .iter()
                    .map(|f| format!(
                        "    /// Sets the `{}` field.\n    \
                         pub fn {name}(mut self, value: {ty}) -> Self {{\n        \
                         self.{name} = Some(value);\n        \
                         self\n    \
                         }}\n\n",
                        f.name,
                        name = f.name,
                        ty = f.field_type.to_rust()
                    ))
                    .collect::<String>(),
            ));
        }

        // --- client code ---
        let mut cc = String::new();
        cc.push_str(&format!(
            "/// Typed client for [`{name}`] records stored under `{pattern}`.\n\
             pub struct {name}Client {{\n\
             {indent}prefix: String,\n\
             }}\n\n\
             impl {name}Client {{\n\
             {indent}/// Creates a new client with the given key prefix.\n\
             {indent}pub fn new(prefix: &str) -> Self {{\n\
             {indent}{indent}Self {{ prefix: prefix.to_string() }}\n\
             {indent}}}\n\n\
             {indent}/// Retrieves a record by id.\n\
             {indent}pub fn get(&self, _id: &str) -> Option<{name}> {{\n\
             {indent}{indent}unimplemented!(\"connect a Ferrite client\")\n\
             {indent}}}\n\n\
             {indent}/// Stores a record by id.\n\
             {indent}pub fn set(&self, _id: &str, _data: &{name}) {{\n\
             {indent}{indent}unimplemented!(\"connect a Ferrite client\")\n\
             {indent}}}\n\n\
             {indent}/// Finds records matching a query.\n\
             {indent}pub fn find(&self, _query: &str) -> Vec<{name}> {{\n\
             {indent}{indent}unimplemented!(\"connect a Ferrite client\")\n\
             {indent}}}\n\
             }}\n",
            name = name,
            pattern = schema.key_pattern,
            indent = "    ",
        ));

        GeneratedCode {
            language: Language::Rust,
            type_definitions: td,
            client_code: cc,
            file_name: format!("{}.rs", schema.name),
            schema_name: schema.name.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Converts a `serde_json::Value` to the closest `FieldType`.
fn json_value_to_field_type(value: &serde_json::Value) -> FieldType {
    match value {
        serde_json::Value::Null => FieldType::String,
        serde_json::Value::Bool(_) => FieldType::Boolean,
        serde_json::Value::Number(n) => {
            if n.is_f64() && n.as_i64().is_none() {
                FieldType::Float
            } else {
                FieldType::Integer
            }
        }
        serde_json::Value::String(_) => FieldType::String,
        serde_json::Value::Array(arr) => {
            let inner = arr
                .first()
                .map(json_value_to_field_type)
                .unwrap_or(FieldType::String);
            FieldType::Array(Box::new(inner))
        }
        serde_json::Value::Object(_) => FieldType::Json,
    }
}

/// Infers a key pattern from a concrete key (e.g. `"users:123"` → `"users:*"`).
fn infer_pattern(key: &str) -> String {
    if let Some(pos) = key.rfind(':') {
        format!("{}:*", &key[..pos])
    } else {
        format!("{}:*", key)
    }
}

/// Converts a snake_case or kebab-case name to PascalCase.
fn pascal_case(s: &str) -> String {
    s.split(|c: char| c == '_' || c == '-')
        .filter(|seg| !seg.is_empty())
        .map(|seg| {
            let mut chars = seg.chars();
            match chars.next() {
                Some(first) => {
                    let upper: String = first.to_uppercase().collect();
                    format!("{}{}", upper, chars.as_str())
                }
                None => String::new(),
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_schema() -> SchemaDefinition {
        SchemaDefinition {
            name: "user".to_string(),
            key_pattern: "users:*".to_string(),
            fields: vec![
                FieldDefinition {
                    name: "id".to_string(),
                    field_type: FieldType::String,
                    nullable: false,
                    description: None,
                    default_value: None,
                },
                FieldDefinition {
                    name: "name".to_string(),
                    field_type: FieldType::String,
                    nullable: false,
                    description: None,
                    default_value: None,
                },
                FieldDefinition {
                    name: "email".to_string(),
                    field_type: FieldType::String,
                    nullable: false,
                    description: None,
                    default_value: None,
                },
                FieldDefinition {
                    name: "age".to_string(),
                    field_type: FieldType::Integer,
                    nullable: true,
                    description: None,
                    default_value: None,
                },
            ],
            version: 1,
            description: Some("User accounts".to_string()),
            created_at: 1_700_000_000,
        }
    }

    #[test]
    fn test_register_and_retrieve_schema() {
        let introspector = SchemaIntrospector::new();
        let schema = sample_schema();
        introspector
            .register_schema(schema.clone())
            .expect("register should succeed");

        let retrieved = introspector.get_schema("user").expect("should exist");
        assert_eq!(retrieved.name, "user");
        assert_eq!(retrieved.fields.len(), 4);
    }

    #[test]
    fn test_register_duplicate_schema_fails() {
        let introspector = SchemaIntrospector::new();
        introspector
            .register_schema(sample_schema())
            .expect("first register succeeds");

        let err = introspector
            .register_schema(sample_schema())
            .expect_err("duplicate should fail");
        assert!(matches!(err, SchemaError::AlreadyExists(_)));
    }

    #[test]
    fn test_infer_schema_from_sample() {
        let introspector = SchemaIntrospector::new();
        let samples = vec![
            (
                "users:1".to_string(),
                serde_json::json!({"id": "1", "name": "Alice", "active": true}),
            ),
            (
                "users:2".to_string(),
                serde_json::json!({"id": "2", "name": "Bob", "score": 42}),
            ),
        ];

        let schema = introspector.infer_schema_from_sample(&samples);
        assert_eq!(schema.key_pattern, "users:*");
        assert!(!schema.fields.is_empty());

        let names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"name"));
    }

    #[test]
    fn test_generate_typescript() {
        let gen = CodeGenerator::new(CodeGenConfig::default());
        let code = gen.generate_typescript(&sample_schema());

        assert_eq!(code.language, Language::TypeScript);
        assert!(code.type_definitions.contains("export interface User"));
        assert!(code.type_definitions.contains("id: string"));
        assert!(code.type_definitions.contains("age?: number"));
        assert!(code.client_code.contains("class UserClient"));
        assert!(code.client_code.contains("async get("));
    }

    #[test]
    fn test_generate_python() {
        let gen = CodeGenerator::new(CodeGenConfig::default());
        let code = gen.generate_python(&sample_schema());

        assert_eq!(code.language, Language::Python);
        assert!(code.type_definitions.contains("@dataclass"));
        assert!(code.type_definitions.contains("class User:"));
        assert!(code.type_definitions.contains("id: str"));
        assert!(code.type_definitions.contains("Optional[int]"));
        assert!(code.client_code.contains("class UserClient:"));
    }

    #[test]
    fn test_generate_rust() {
        let gen = CodeGenerator::new(CodeGenConfig::default());
        let code = gen.generate_rust(&sample_schema());

        assert_eq!(code.language, Language::Rust);
        assert!(code.type_definitions.contains("pub struct User"));
        assert!(code.type_definitions.contains("pub id: String"));
        assert!(code.type_definitions.contains("pub age: Option<i64>"));
        assert!(code.client_code.contains("struct UserClient"));
    }

    #[test]
    fn test_schema_validation_empty_name() {
        let introspector = SchemaIntrospector::new();
        let mut schema = sample_schema();
        schema.name = String::new();

        let err = introspector
            .register_schema(schema)
            .expect_err("empty name should fail");
        assert!(matches!(err, SchemaError::InvalidSchema(_)));
    }

    #[test]
    fn test_validate_query_against_schema() {
        let introspector = SchemaIntrospector::new();
        introspector
            .register_schema(sample_schema())
            .expect("register ok");

        // Query referencing unknown column.
        let issues = introspector
            .validate_query("SELECT * FROM users:* WHERE nonexistent = 'x'", "user");
        assert!(!issues.is_empty());
        assert!(issues.iter().any(|i| i.severity == ValidationSeverity::Warning));

        // Query with no WHERE — should get Info.
        let issues = introspector.validate_query("SELECT * FROM users:*", "user");
        assert!(issues.iter().any(|i| i.severity == ValidationSeverity::Info));
    }

    #[test]
    fn test_list_schemas() {
        let introspector = SchemaIntrospector::new();
        assert!(introspector.list_schemas().is_empty());

        introspector
            .register_schema(sample_schema())
            .expect("register ok");
        let list = introspector.list_schemas();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].name, "user");
    }
}
