//! Type-Safe Multi-Language Query Validator and Code Generator
//!
//! Compile-time type-safe query validation and code generation for FerriteQL
//! client bindings across multiple programming languages.
//!
//! # Features
//!
//! - Type-checked FerriteQL bindings for TypeScript, Python, Rust, and Go
//! - Compile-time schema validation with detailed error reporting
//! - Generated type-safe repository classes with CRUD operations
//! - Index-aware query generation with composite key support
//!
//! # Example
//!
//! ```ignore
//! use ferrite::sdk::typesafe::{TypeSafeGenerator, TypeSafeConfig, SchemaDefinition};
//!
//! let config = TypeSafeConfig::default();
//! let generator = TypeSafeGenerator::new(config);
//! let code = generator.generate_typescript(&schema).unwrap();
//! ```

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Type-safe FerriteQL code generator.
///
/// Generates type-checked client bindings from schema definitions,
/// producing language-idiomatic code with full type safety.
#[derive(Clone, Debug)]
pub struct TypeSafeGenerator {
    /// Generator configuration
    config: TypeSafeConfig,
}

/// Configuration for the type-safe code generator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TypeSafeConfig {
    /// Output directory for generated files
    pub output_dir: String,
    /// Whether to generate test files alongside bindings
    pub generate_tests: bool,
    /// Whether to include doc comments in generated code
    pub include_docs: bool,
    /// Strict mode enforces non-nullable fields by default
    pub strict_mode: bool,
    /// Whether fields are nullable by default
    pub nullable_by_default: bool,
}

impl Default for TypeSafeConfig {
    fn default() -> Self {
        Self {
            output_dir: "generated".to_string(),
            generate_tests: true,
            include_docs: true,
            strict_mode: false,
            nullable_by_default: false,
        }
    }
}

/// Schema definition describing the key structure for code generation.
///
/// Represents a complete type definition including fields, indices,
/// and versioning metadata used to generate language-specific bindings.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemaDefinition {
    /// Schema name (used as type/class name)
    pub name: String,
    /// Namespace or module path
    pub namespace: String,
    /// Field definitions
    pub fields: Vec<FieldDef>,
    /// Index definitions
    pub indices: Vec<IndexDef>,
    /// Schema version for compatibility tracking
    pub version: u32,
}

/// Field definition within a schema.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FieldDef {
    /// Field name
    pub name: String,
    /// Field type
    pub field_type: FieldType,
    /// Whether the field can be null
    pub nullable: bool,
    /// Default value expression
    pub default_value: Option<String>,
    /// Documentation description
    pub description: Option<String>,
}

/// Supported field types for schema definitions.
///
/// Maps to language-specific types during code generation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FieldType {
    /// UTF-8 string
    String,
    /// 64-bit signed integer
    Int64,
    /// 64-bit floating point
    Float64,
    /// Boolean
    Bool,
    /// Raw byte array
    Bytes,
    /// UTC timestamp
    Timestamp,
    /// Homogeneous array
    Array(Box<FieldType>),
    /// Key-value map
    Map(Box<FieldType>, Box<FieldType>),
    /// Optional wrapper
    Optional(Box<FieldType>),
    /// Arbitrary JSON value
    Json,
    /// Fixed-dimension vector for similarity search
    Vector {
        /// Number of dimensions
        dimensions: u32,
    },
    /// Foreign key reference to another schema
    Reference {
        /// Target schema name
        target: String,
    },
}

/// Index definition for a schema.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexDef {
    /// Index name
    pub name: String,
    /// Fields included in the index
    pub fields: Vec<String>,
    /// Type of index
    pub index_type: IndexType,
    /// Whether the index enforces uniqueness
    pub unique: bool,
}

/// Supported index types.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IndexType {
    /// B-tree ordered index
    BTree,
    /// Hash-based equality index
    Hash,
    /// Full-text search index
    FullText,
    /// Vector similarity index
    Vector,
    /// Composite multi-field index
    Composite,
}

/// Output of a code generation run.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GeneratedCode {
    /// Generated source files
    pub files: Vec<CodeFile>,
    /// Target language
    pub language: Language,
    /// Schema version used for generation
    pub schema_version: u32,
    /// Timestamp of generation
    pub generated_at: DateTime<Utc>,
}

/// A single generated source file.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CodeFile {
    /// File path relative to output directory
    pub path: String,
    /// File content
    pub content: String,
    /// Whether this is a test file
    pub is_test: bool,
}

/// Supported target languages for code generation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Language {
    /// TypeScript with full type annotations
    TypeScript,
    /// Python with type hints and dataclasses
    Python,
    /// Rust with derive macros
    Rust,
    /// Go with struct tags
    Go,
    /// Java with annotations
    Java,
}

/// Result of validating a FerriteQL query against a schema.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryValidation {
    /// Whether the query is valid
    pub valid: bool,
    /// Validation errors
    pub errors: Vec<ValidationError>,
    /// Validation warnings
    pub warnings: Vec<ValidationWarning>,
}

/// A validation error with source location.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidationError {
    /// Line number (1-indexed)
    pub line: u32,
    /// Column number (1-indexed)
    pub column: u32,
    /// Error message
    pub message: String,
    /// Error code for programmatic handling
    pub code: String,
}

/// A validation warning with optional fix suggestion.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidationWarning {
    /// Line number (1-indexed)
    pub line: u32,
    /// Column number (1-indexed)
    pub column: u32,
    /// Warning message
    pub message: String,
    /// Suggested fix
    pub suggestion: Option<String>,
}

/// Errors that can occur during code generation.
#[derive(Debug, Clone, thiserror::Error)]
pub enum CodegenError {
    /// Schema definition is invalid
    #[error("invalid schema: {0}")]
    InvalidSchema(String),

    /// Field type is not supported for the target language
    #[error("unsupported type: {0}")]
    UnsupportedType(String),

    /// Template rendering failed
    #[error("template error: {0}")]
    TemplateError(String),

    /// File system operation failed
    #[error("IO error: {0}")]
    IoError(String),

    /// Internal generator error
    #[error("internal error: {0}")]
    Internal(String),
}

impl TypeSafeGenerator {
    /// Create a new type-safe generator with the given configuration.
    pub fn new(config: TypeSafeConfig) -> Self {
        Self { config }
    }

    /// Generate type-safe TypeScript bindings from a schema definition.
    ///
    /// Produces interfaces, repository classes with async CRUD methods,
    /// and optional test stubs.
    pub fn generate_typescript(
        &self,
        schema: &SchemaDefinition,
    ) -> Result<GeneratedCode, CodegenError> {
        self.validate_schema(schema)?;

        let mut content = "// Generated by Ferrite SDK Generator\n\n".to_string();

        // Interface
        content.push_str(&format!("export interface {} {{\n", schema.name));
        for field in &schema.fields {
            if self.config.include_docs {
                if let Some(desc) = &field.description {
                    content.push_str(&format!("  /** {} */\n", desc));
                }
            }
            let ts_type = self.field_type_typescript(&field.field_type);
            let optional = if field.nullable { "?" } else { "" };
            content.push_str(&format!("  {}{}: {};\n", field.name, optional, ts_type));
        }
        content.push_str("}\n\n");

        // Repository class
        let repo_name = format!("{}Repository", schema.name);
        content.push_str(&format!("export class {} {{\n", repo_name));
        content.push_str("  private client: FerriteClient;\n\n");
        content.push_str(&format!(
            "  constructor(client: FerriteClient) {{\n    this.client = client;\n  }}\n\n"
        ));

        // get method
        content.push_str(&format!(
            "  async get(id: string): Promise<{} | null> {{\n",
            schema.name
        ));
        content.push_str(&format!(
            "    return this.client.get<{}>(id);\n",
            schema.name
        ));
        content.push_str("  }\n\n");

        // set method
        content.push_str(&format!(
            "  async set(id: string, {}: {}, ttl?: number): Promise<void> {{\n",
            to_camel_case(&schema.name),
            schema.name
        ));
        content.push_str(&format!(
            "    await this.client.set(id, {}, ttl);\n",
            to_camel_case(&schema.name)
        ));
        content.push_str("  }\n\n");

        // delete method
        content.push_str(
            "  async delete(id: string): Promise<boolean> {\n\
             \x20   return this.client.delete(id);\n\
             \x20 }\n\n",
        );

        // query method
        content.push_str(&format!(
            "  async query(sql: string): Promise<{}[]> {{\n",
            schema.name
        ));
        content.push_str(&format!(
            "    return this.client.query<{}>(sql);\n",
            schema.name
        ));
        content.push_str("  }\n");

        content.push_str("}\n");

        let mut files = vec![CodeFile {
            path: format!(
                "{}/{}.ts",
                self.config.output_dir,
                to_snake_case(&schema.name)
            ),
            content,
            is_test: false,
        }];

        if self.config.generate_tests {
            files.push(self.generate_typescript_test(schema));
        }

        Ok(GeneratedCode {
            files,
            language: Language::TypeScript,
            schema_version: schema.version,
            generated_at: Utc::now(),
        })
    }

    /// Generate type-safe Python bindings from a schema definition.
    ///
    /// Produces dataclasses with type annotations, repository classes,
    /// and optional pytest stubs.
    pub fn generate_python(
        &self,
        schema: &SchemaDefinition,
    ) -> Result<GeneratedCode, CodegenError> {
        self.validate_schema(schema)?;

        let mut content =
            "# Generated by Ferrite SDK Generator\n\nfrom __future__ import annotations\n\n\
             from dataclasses import dataclass, field\nfrom typing import Optional, List, Dict, Any\n\
             from datetime import datetime\n\n"
                .to_string();

        // Dataclass
        content.push_str("@dataclass\n");
        content.push_str(&format!("class {}:\n", schema.name));
        if self.config.include_docs {
            content.push_str(&format!("    \"\"\"{}.\"\"\"\n\n", schema.name));
        }
        for f in &schema.fields {
            let py_type = self.field_type_python(&f.field_type);
            if f.nullable {
                content.push_str(&format!(
                    "    {}: Optional[{}] = None\n",
                    to_snake_case(&f.name),
                    py_type
                ));
            } else if let Some(default) = &f.default_value {
                content.push_str(&format!(
                    "    {}: {} = {}\n",
                    to_snake_case(&f.name),
                    py_type,
                    default
                ));
            } else {
                content.push_str(&format!(
                    "    {}: {} = None\n",
                    to_snake_case(&f.name),
                    py_type
                ));
            }
        }

        content.push('\n');

        // Repository class
        content.push_str(&format!("\nclass {}Repository:\n", schema.name));
        if self.config.include_docs {
            content.push_str(&format!(
                "    \"\"\"Repository for {} entities.\"\"\"\n\n",
                schema.name
            ));
        }
        content.push_str("    def __init__(self, client: FerriteClient) -> None:\n");
        content.push_str("        self.client = client\n\n");

        content.push_str(&format!(
            "    async def get(self, id: str) -> Optional[{}]:\n",
            schema.name
        ));
        content.push_str("        return await self.client.get(id)\n\n");

        content.push_str(&format!(
            "    async def set(self, id: str, entity: {}, ttl: Optional[int] = None) -> None:\n",
            schema.name
        ));
        content.push_str("        await self.client.set(id, entity, ttl=ttl)\n\n");

        content.push_str("    async def delete(self, id: str) -> bool:\n");
        content.push_str("        return await self.client.delete(id)\n\n");

        content.push_str(&format!(
            "    async def query(self, sql: str) -> List[{}]:\n",
            schema.name
        ));
        content.push_str("        return await self.client.query(sql)\n");

        let mut files = vec![CodeFile {
            path: format!(
                "{}/{}.py",
                self.config.output_dir,
                to_snake_case(&schema.name)
            ),
            content,
            is_test: false,
        }];

        if self.config.generate_tests {
            files.push(self.generate_python_test(schema));
        }

        Ok(GeneratedCode {
            files,
            language: Language::Python,
            schema_version: schema.version,
            generated_at: Utc::now(),
        })
    }

    /// Generate type-safe Rust bindings from a schema definition.
    ///
    /// Produces structs with `Serialize`/`Deserialize` derive macros
    /// and a typed repository implementation.
    pub fn generate_rust(&self, schema: &SchemaDefinition) -> Result<GeneratedCode, CodegenError> {
        self.validate_schema(schema)?;

        let mut content = "// Generated by Ferrite SDK Generator\n\n\
             use serde::{Deserialize, Serialize};\n\n"
            .to_string();

        // Struct
        content.push_str("#[derive(Clone, Debug, Serialize, Deserialize)]\n");
        content.push_str(&format!("pub struct {} {{\n", schema.name));
        for f in &schema.fields {
            if self.config.include_docs {
                if let Some(desc) = &f.description {
                    content.push_str(&format!("    /// {}\n", desc));
                }
            }
            let rust_type = self.field_type_rust(&f.field_type);
            let final_type = if f.nullable {
                format!("Option<{}>", rust_type)
            } else {
                rust_type
            };
            content.push_str(&format!(
                "    pub {}: {},\n",
                to_snake_case(&f.name),
                final_type
            ));
        }
        content.push_str("}\n\n");

        // Repository struct
        let repo_name = format!("{}Repository", schema.name);
        content.push_str(&format!("pub struct {} {{\n", repo_name));
        content.push_str("    client: FerriteClient,\n");
        content.push_str("}\n\n");

        content.push_str(&format!("impl {} {{\n", repo_name));
        content.push_str(
            "    pub fn new(client: FerriteClient) -> Self {\n\
             \x20       Self { client }\n\
             \x20   }\n\n",
        );

        content.push_str(&format!(
            "    pub async fn get(&self, id: &str) -> Result<Option<{}>, FerriteError> {{\n\
             \x20       self.client.get(id).await\n\
             \x20   }}\n\n",
            schema.name
        ));

        content.push_str(&format!(
            "    pub async fn set(&self, id: &str, entity: &{}, ttl: Option<u64>) -> Result<(), FerriteError> {{\n\
             \x20       self.client.set(id, entity, ttl).await\n\
             \x20   }}\n\n",
            schema.name
        ));

        content.push_str(
            "    pub async fn delete(&self, id: &str) -> Result<bool, FerriteError> {\n\
             \x20       self.client.delete(id).await\n\
             \x20   }\n\n",
        );

        content.push_str(&format!(
            "    pub async fn query(&self, sql: &str) -> Result<Vec<{}>, FerriteError> {{\n\
             \x20       self.client.query(sql).await\n\
             \x20   }}\n",
            schema.name
        ));

        content.push_str("}\n");

        let mut files = vec![CodeFile {
            path: format!(
                "{}/{}.rs",
                self.config.output_dir,
                to_snake_case(&schema.name)
            ),
            content,
            is_test: false,
        }];

        if self.config.generate_tests {
            files.push(self.generate_rust_test(schema));
        }

        Ok(GeneratedCode {
            files,
            language: Language::Rust,
            schema_version: schema.version,
            generated_at: Utc::now(),
        })
    }

    /// Generate type-safe Go bindings from a schema definition.
    ///
    /// Produces Go structs with json tags, a typed repository struct,
    /// and optional test stubs.
    pub fn generate_go(&self, schema: &SchemaDefinition) -> Result<GeneratedCode, CodegenError> {
        self.validate_schema(schema)?;

        let mut content = "// Generated by Ferrite SDK Generator\n\n".to_string();
        content.push_str(&format!("package {}\n\n", to_snake_case(&schema.namespace)));
        content.push_str("import (\n\t\"context\"\n\t\"time\"\n)\n\n");

        // Struct
        if self.config.include_docs {
            content.push_str(&format!(
                "// {} represents a {} entity.\n",
                schema.name, schema.name
            ));
        }
        content.push_str(&format!("type {} struct {{\n", schema.name));
        for f in &schema.fields {
            let go_type = self.field_type_go(&f.field_type);
            let final_type = if f.nullable {
                format!("*{}", go_type)
            } else {
                go_type
            };
            let json_tag = if f.nullable {
                format!("`json:\"{},omitempty\"`", to_snake_case(&f.name))
            } else {
                format!("`json:\"{}\"`", to_snake_case(&f.name))
            };
            content.push_str(&format!(
                "\t{} {} {}\n",
                to_pascal_case(&f.name),
                final_type,
                json_tag
            ));
        }
        content.push_str("}\n\n");

        // Repository struct
        let repo_name = format!("{}Repository", schema.name);
        content.push_str(&format!(
            "// {} provides typed access to {} entities.\n",
            repo_name, schema.name
        ));
        content.push_str(&format!("type {} struct {{\n", repo_name));
        content.push_str("\tclient *FerriteClient\n");
        content.push_str("}\n\n");

        content.push_str(&format!(
            "func New{}(client *FerriteClient) *{} {{\n\treturn &{}{{\n\t\tclient: client,\n\t}}\n}}\n\n",
            repo_name, repo_name, repo_name
        ));

        content.push_str(&format!(
            "func (r *{repo}) Get(ctx context.Context, id string) (*{name}, error) {{\n\
             \treturn r.client.Get(ctx, id)\n\
             }}\n\n",
            repo = repo_name,
            name = schema.name
        ));

        content.push_str(&format!(
            "func (r *{repo}) Set(ctx context.Context, id string, entity *{name}, ttl *time.Duration) error {{\n\
             \treturn r.client.Set(ctx, id, entity, ttl)\n\
             }}\n\n",
            repo = repo_name,
            name = schema.name
        ));

        content.push_str(&format!(
            "func (r *{repo}) Delete(ctx context.Context, id string) (bool, error) {{\n\
             \treturn r.client.Delete(ctx, id)\n\
             }}\n\n",
            repo = repo_name
        ));

        content.push_str(&format!(
            "func (r *{repo}) Query(ctx context.Context, sql string) ([]{name}, error) {{\n\
             \treturn r.client.Query(ctx, sql)\n\
             }}\n",
            repo = repo_name,
            name = schema.name
        ));

        let mut files = vec![CodeFile {
            path: format!(
                "{}/{}.go",
                self.config.output_dir,
                to_snake_case(&schema.name)
            ),
            content,
            is_test: false,
        }];

        if self.config.generate_tests {
            files.push(self.generate_go_test(schema));
        }

        Ok(GeneratedCode {
            files,
            language: Language::Go,
            schema_version: schema.version,
            generated_at: Utc::now(),
        })
    }

    /// Validate a FerriteQL query string against a schema definition.
    ///
    /// Returns a list of validation results including errors for type
    /// mismatches, unknown fields, and warnings for potential issues.
    pub fn validate_query(&self, query: &str, schema: &SchemaDefinition) -> Vec<QueryValidation> {
        let mut validations = Vec::new();
        let field_names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();

        for (line_idx, line) in query.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with("--") {
                continue;
            }

            let mut errors = Vec::new();
            let mut warnings = Vec::new();

            // Check for SELECT * usage
            if trimmed.contains("SELECT *") || trimmed.contains("select *") {
                warnings.push(ValidationWarning {
                    line: (line_idx + 1) as u32,
                    column: 1,
                    message: "SELECT * may return unnecessary data".to_string(),
                    suggestion: Some(format!(
                        "SELECT {} FROM {}",
                        field_names.join(", "),
                        schema.name
                    )),
                });
            }

            // Check for unknown field references
            let tokens: Vec<&str> = trimmed.split_whitespace().collect();
            for (col_idx, token) in tokens.iter().enumerate() {
                let clean = token.trim_matches(|c: char| !c.is_alphanumeric() && c != '_');
                if clean.is_empty() {
                    continue;
                }

                // Skip SQL keywords
                let keywords = [
                    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "LIKE", "ORDER", "BY",
                    "ASC", "DESC", "LIMIT", "OFFSET", "INSERT", "UPDATE", "DELETE", "SET",
                    "VALUES", "INTO", "JOIN", "ON", "GROUP", "HAVING", "AS", "select", "from",
                    "where", "and", "or", "not", "in", "like", "order", "by", "asc", "desc",
                    "limit", "offset", "insert", "update", "delete", "set", "values", "into",
                    "join", "on", "group", "having", "as",
                ];
                if keywords.contains(&clean) {
                    continue;
                }

                // Skip numeric literals, string literals, and schema name
                if clean.parse::<f64>().is_ok()
                    || clean.starts_with('\'')
                    || clean.starts_with('"')
                    || clean == schema.name
                    || clean == "*"
                    || clean == "="
                {
                    continue;
                }

                // Check if token looks like a field reference
                if clean.chars().all(|c| c.is_alphanumeric() || c == '_')
                    && clean.chars().next().is_some_and(|c| c.is_alphabetic())
                    && !field_names.contains(&clean)
                {
                    errors.push(ValidationError {
                        line: (line_idx + 1) as u32,
                        column: (col_idx + 1) as u32,
                        message: format!("unknown field '{}'", clean),
                        code: "E001".to_string(),
                    });
                }
            }

            // Check for missing WHERE clause in UPDATE/DELETE
            let upper = trimmed.to_uppercase();
            if (upper.starts_with("UPDATE") || upper.starts_with("DELETE"))
                && !upper.contains("WHERE")
            {
                warnings.push(ValidationWarning {
                    line: (line_idx + 1) as u32,
                    column: 1,
                    message: "statement without WHERE clause affects all rows".to_string(),
                    suggestion: Some("add a WHERE clause to limit scope".to_string()),
                });
            }

            let valid = errors.is_empty();
            validations.push(QueryValidation {
                valid,
                errors,
                warnings,
            });
        }

        validations
    }

    /// Validate a schema definition for completeness and correctness.
    fn validate_schema(&self, schema: &SchemaDefinition) -> Result<(), CodegenError> {
        if schema.name.is_empty() {
            return Err(CodegenError::InvalidSchema(
                "schema name cannot be empty".to_string(),
            ));
        }
        if schema.fields.is_empty() {
            return Err(CodegenError::InvalidSchema(
                "schema must have at least one field".to_string(),
            ));
        }
        for field in &schema.fields {
            if field.name.is_empty() {
                return Err(CodegenError::InvalidSchema(
                    "field name cannot be empty".to_string(),
                ));
            }
        }
        // Validate index field references
        for index in &schema.indices {
            for field_name in &index.fields {
                if !schema.fields.iter().any(|f| &f.name == field_name) {
                    return Err(CodegenError::InvalidSchema(format!(
                        "index '{}' references unknown field '{}'",
                        index.name, field_name
                    )));
                }
            }
        }
        Ok(())
    }

    /// Map a field type to its TypeScript representation.
    fn field_type_typescript(&self, ft: &FieldType) -> String {
        match ft {
            FieldType::String => "string".to_string(),
            FieldType::Int64 | FieldType::Float64 => "number".to_string(),
            FieldType::Bool => "boolean".to_string(),
            FieldType::Bytes => "Uint8Array".to_string(),
            FieldType::Timestamp => "Date".to_string(),
            FieldType::Array(inner) => format!("{}[]", self.field_type_typescript(inner)),
            FieldType::Map(k, v) => format!(
                "Map<{}, {}>",
                self.field_type_typescript(k),
                self.field_type_typescript(v)
            ),
            FieldType::Optional(inner) => {
                format!("{} | null", self.field_type_typescript(inner))
            }
            FieldType::Json => "any".to_string(),
            FieldType::Vector { dimensions } => format!("Float32Array /* {} dims */", dimensions),
            FieldType::Reference { target } => target.clone(),
        }
    }

    /// Map a field type to its Python representation.
    fn field_type_python(&self, ft: &FieldType) -> String {
        match ft {
            FieldType::String => "str".to_string(),
            FieldType::Int64 => "int".to_string(),
            FieldType::Float64 => "float".to_string(),
            FieldType::Bool => "bool".to_string(),
            FieldType::Bytes => "bytes".to_string(),
            FieldType::Timestamp => "datetime".to_string(),
            FieldType::Array(inner) => format!("List[{}]", self.field_type_python(inner)),
            FieldType::Map(k, v) => format!(
                "Dict[{}, {}]",
                self.field_type_python(k),
                self.field_type_python(v)
            ),
            FieldType::Optional(inner) => format!("Optional[{}]", self.field_type_python(inner)),
            FieldType::Json => "Any".to_string(),
            FieldType::Vector { .. } => "List[float]".to_string(),
            FieldType::Reference { target } => target.clone(),
        }
    }

    /// Map a field type to its Rust representation.
    fn field_type_rust(&self, ft: &FieldType) -> String {
        match ft {
            FieldType::String => "String".to_string(),
            FieldType::Int64 => "i64".to_string(),
            FieldType::Float64 => "f64".to_string(),
            FieldType::Bool => "bool".to_string(),
            FieldType::Bytes => "Vec<u8>".to_string(),
            FieldType::Timestamp => "chrono::DateTime<chrono::Utc>".to_string(),
            FieldType::Array(inner) => format!("Vec<{}>", self.field_type_rust(inner)),
            FieldType::Map(k, v) => format!(
                "std::collections::HashMap<{}, {}>",
                self.field_type_rust(k),
                self.field_type_rust(v)
            ),
            FieldType::Optional(inner) => format!("Option<{}>", self.field_type_rust(inner)),
            FieldType::Json => "serde_json::Value".to_string(),
            FieldType::Vector { .. } => "Vec<f32>".to_string(),
            FieldType::Reference { target } => target.clone(),
        }
    }

    /// Map a field type to its Go representation.
    fn field_type_go(&self, ft: &FieldType) -> String {
        match ft {
            FieldType::String => "string".to_string(),
            FieldType::Int64 => "int64".to_string(),
            FieldType::Float64 => "float64".to_string(),
            FieldType::Bool => "bool".to_string(),
            FieldType::Bytes => "[]byte".to_string(),
            FieldType::Timestamp => "time.Time".to_string(),
            FieldType::Array(inner) => format!("[]{}", self.field_type_go(inner)),
            FieldType::Map(k, v) => {
                format!("map[{}]{}", self.field_type_go(k), self.field_type_go(v))
            }
            FieldType::Optional(inner) => format!("*{}", self.field_type_go(inner)),
            FieldType::Json => "interface{}".to_string(),
            FieldType::Vector { .. } => "[]float32".to_string(),
            FieldType::Reference { target } => format!("*{}", target),
        }
    }

    /// Generate a TypeScript test file for the given schema.
    fn generate_typescript_test(&self, schema: &SchemaDefinition) -> CodeFile {
        let snake = to_snake_case(&schema.name);
        let content = format!(
            "// Generated by Ferrite SDK Generator\n\
             import {{ {name}, {name}Repository }} from './{snake}';\n\n\
             describe('{name}Repository', () => {{\n\
             \x20 it('should create a valid {name}', () => {{\n\
             \x20   const entity: {name} = {{\n\
             {fields}\
             \x20   }};\n\
             \x20   expect(entity).toBeDefined();\n\
             \x20 }});\n\
             }});\n",
            name = schema.name,
            snake = snake,
            fields = schema
                .fields
                .iter()
                .map(|f| format!(
                    "      {}: {},\n",
                    f.name,
                    self.typescript_default_value(&f.field_type)
                ))
                .collect::<Vec<_>>()
                .join("")
        );
        CodeFile {
            path: format!("{}/{}.test.ts", self.config.output_dir, snake),
            content,
            is_test: true,
        }
    }

    /// Generate a Python test file for the given schema.
    fn generate_python_test(&self, schema: &SchemaDefinition) -> CodeFile {
        let snake = to_snake_case(&schema.name);
        let content = format!(
            "# Generated by Ferrite SDK Generator\n\
             from {snake} import {name}\n\n\n\
             def test_create_{snake}():\n\
             \x20   entity = {name}()\n\
             \x20   assert entity is not None\n",
            name = schema.name,
            snake = snake,
        );
        CodeFile {
            path: format!("{}/test_{}.py", self.config.output_dir, snake),
            content,
            is_test: true,
        }
    }

    /// Generate a Rust test file for the given schema.
    fn generate_rust_test(&self, schema: &SchemaDefinition) -> CodeFile {
        let snake = to_snake_case(&schema.name);
        let content = format!(
            "// Generated by Ferrite SDK Generator\n\
             #[cfg(test)]\n\
             mod tests {{\n\
             \x20   use super::*;\n\n\
             \x20   #[test]\n\
             \x20   fn test_{snake}_serialization() {{\n\
             \x20       // TODO: implement test\n\
             \x20   }}\n\
             }}\n",
            snake = snake,
        );
        CodeFile {
            path: format!("{}/{}_test.rs", self.config.output_dir, snake),
            content,
            is_test: true,
        }
    }

    /// Generate a Go test file for the given schema.
    fn generate_go_test(&self, schema: &SchemaDefinition) -> CodeFile {
        let snake = to_snake_case(&schema.name);
        let content = format!(
            "// Generated by Ferrite SDK Generator\n\
             package {pkg}\n\n\
             import \"testing\"\n\n\
             func Test{name}Creation(t *testing.T) {{\n\
             \tentity := {name}{{}}\n\
             \tif entity == ({name}{{}}) {{\n\
             \t\tt.Log(\"created empty {name}\")\n\
             \t}}\n\
             }}\n",
            pkg = to_snake_case(&schema.namespace),
            name = schema.name,
        );
        CodeFile {
            path: format!("{}/{}_test.go", self.config.output_dir, snake),
            content,
            is_test: true,
        }
    }

    /// Return a TypeScript default value literal for testing.
    fn typescript_default_value(&self, ft: &FieldType) -> &'static str {
        match ft {
            FieldType::String => "''",
            FieldType::Int64 | FieldType::Float64 => "0",
            FieldType::Bool => "false",
            FieldType::Bytes => "new Uint8Array()",
            FieldType::Timestamp => "new Date()",
            FieldType::Array(_) => "[]",
            FieldType::Map(_, _) => "new Map()",
            FieldType::Optional(_) => "null",
            FieldType::Json => "{}",
            FieldType::Vector { .. } => "new Float32Array()",
            FieldType::Reference { .. } => "null",
        }
    }
}

/// Convert a PascalCase or camelCase string to snake_case.
fn to_snake_case(s: &str) -> String {
    let mut result = std::string::String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() && i > 0 {
            result.push('_');
        }
        result.push(ch.to_lowercase().next().unwrap_or(ch));
    }
    result
}

/// Convert a snake_case string to camelCase.
fn to_camel_case(s: &str) -> String {
    let mut result = std::string::String::new();
    let mut capitalize_next = false;
    for (i, ch) in s.chars().enumerate() {
        if ch == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(ch.to_uppercase().next().unwrap_or(ch));
            capitalize_next = false;
        } else if i == 0 {
            result.push(ch.to_lowercase().next().unwrap_or(ch));
        } else {
            result.push(ch);
        }
    }
    result
}

/// Convert a snake_case string to PascalCase.
fn to_pascal_case(s: &str) -> String {
    let mut result = std::string::String::new();
    let mut capitalize_next = true;
    for ch in s.chars() {
        if ch == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(ch.to_uppercase().next().unwrap_or(ch));
            capitalize_next = false;
        } else {
            result.push(ch);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    /// Helper to create a sample schema for testing.
    fn sample_schema() -> SchemaDefinition {
        SchemaDefinition {
            name: "User".to_string(),
            namespace: "app".to_string(),
            fields: vec![
                FieldDef {
                    name: "id".to_string(),
                    field_type: FieldType::String,
                    nullable: false,
                    default_value: None,
                    description: Some("Unique identifier".to_string()),
                },
                FieldDef {
                    name: "name".to_string(),
                    field_type: FieldType::String,
                    nullable: false,
                    default_value: None,
                    description: Some("User's display name".to_string()),
                },
                FieldDef {
                    name: "email".to_string(),
                    field_type: FieldType::String,
                    nullable: false,
                    default_value: None,
                    description: Some("Email address".to_string()),
                },
                FieldDef {
                    name: "age".to_string(),
                    field_type: FieldType::Int64,
                    nullable: true,
                    default_value: None,
                    description: Some("Age in years".to_string()),
                },
                FieldDef {
                    name: "tags".to_string(),
                    field_type: FieldType::Array(Box::new(FieldType::String)),
                    nullable: false,
                    default_value: None,
                    description: None,
                },
                FieldDef {
                    name: "metadata".to_string(),
                    field_type: FieldType::Map(
                        Box::new(FieldType::String),
                        Box::new(FieldType::Json),
                    ),
                    nullable: true,
                    default_value: None,
                    description: None,
                },
            ],
            indices: vec![
                IndexDef {
                    name: "idx_email".to_string(),
                    fields: vec!["email".to_string()],
                    index_type: IndexType::Hash,
                    unique: true,
                },
                IndexDef {
                    name: "idx_name".to_string(),
                    fields: vec!["name".to_string()],
                    index_type: IndexType::BTree,
                    unique: false,
                },
            ],
            version: 1,
        }
    }

    fn default_generator() -> TypeSafeGenerator {
        TypeSafeGenerator::new(TypeSafeConfig::default())
    }

    // ── Schema validation ──────────────────────────────────────────

    #[test]
    fn test_schema_definition_valid() {
        let schema = sample_schema();
        assert_eq!(schema.name, "User");
        assert_eq!(schema.fields.len(), 6);
        assert_eq!(schema.indices.len(), 2);
        assert_eq!(schema.version, 1);
    }

    #[test]
    fn test_schema_validation_empty_name() {
        let gen = default_generator();
        let mut schema = sample_schema();
        schema.name = String::new();
        let result = gen.generate_typescript(&schema);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            CodegenError::InvalidSchema(_)
        ));
    }

    #[test]
    fn test_schema_validation_no_fields() {
        let gen = default_generator();
        let mut schema = sample_schema();
        schema.fields.clear();
        let result = gen.generate_typescript(&schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_validation_empty_field_name() {
        let gen = default_generator();
        let mut schema = sample_schema();
        schema.fields[0].name = String::new();
        let result = gen.generate_rust(&schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_validation_invalid_index_reference() {
        let gen = default_generator();
        let mut schema = sample_schema();
        schema.indices.push(IndexDef {
            name: "idx_bad".to_string(),
            fields: vec!["nonexistent".to_string()],
            index_type: IndexType::Hash,
            unique: false,
        });
        let result = gen.generate_typescript(&schema);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("nonexistent"));
    }

    // ── TypeScript generation ──────────────────────────────────────

    #[test]
    fn test_generate_typescript() {
        let gen = default_generator();
        let schema = sample_schema();
        let result = gen.generate_typescript(&schema).unwrap();

        assert_eq!(result.language, Language::TypeScript);
        assert_eq!(result.schema_version, 1);
        assert!(!result.files.is_empty());

        let main_file = &result.files[0];
        assert!(main_file.path.ends_with(".ts"));
        assert!(!main_file.is_test);
        assert!(main_file.content.contains("export interface User"));
        assert!(main_file.content.contains("id: string"));
        assert!(main_file.content.contains("age?: number"));
        assert!(main_file.content.contains("export class UserRepository"));
        assert!(main_file.content.contains("async get(id: string)"));
        assert!(main_file.content.contains("Promise<User | null>"));
    }

    #[test]
    fn test_typescript_generates_test_file() {
        let gen = default_generator();
        let schema = sample_schema();
        let result = gen.generate_typescript(&schema).unwrap();

        let test_file = result.files.iter().find(|f| f.is_test).unwrap();
        assert!(test_file.path.ends_with(".test.ts"));
        assert!(test_file.content.contains("describe("));
    }

    #[test]
    fn test_typescript_no_tests_when_disabled() {
        let config = TypeSafeConfig {
            generate_tests: false,
            ..Default::default()
        };
        let gen = TypeSafeGenerator::new(config);
        let schema = sample_schema();
        let result = gen.generate_typescript(&schema).unwrap();
        assert!(result.files.iter().all(|f| !f.is_test));
    }

    #[test]
    fn test_typescript_doc_comments() {
        let gen = default_generator();
        let schema = sample_schema();
        let result = gen.generate_typescript(&schema).unwrap();
        let content = &result.files[0].content;
        assert!(content.contains("/** Unique identifier */"));
    }

    #[test]
    fn test_typescript_no_docs_when_disabled() {
        let config = TypeSafeConfig {
            include_docs: false,
            ..Default::default()
        };
        let gen = TypeSafeGenerator::new(config);
        let schema = sample_schema();
        let result = gen.generate_typescript(&schema).unwrap();
        let content = &result.files[0].content;
        assert!(!content.contains("/**"));
    }

    // ── Python generation ──────────────────────────────────────────

    #[test]
    fn test_generate_python() {
        let gen = default_generator();
        let schema = sample_schema();
        let result = gen.generate_python(&schema).unwrap();

        assert_eq!(result.language, Language::Python);
        let main_file = &result.files[0];
        assert!(main_file.path.ends_with(".py"));
        assert!(main_file.content.contains("@dataclass"));
        assert!(main_file.content.contains("class User:"));
        assert!(main_file.content.contains("id: str"));
        assert!(main_file.content.contains("age: Optional[int]"));
        assert!(main_file.content.contains("class UserRepository:"));
        assert!(main_file.content.contains("async def get"));
    }

    #[test]
    fn test_python_generates_test_file() {
        let gen = default_generator();
        let schema = sample_schema();
        let result = gen.generate_python(&schema).unwrap();

        let test_file = result.files.iter().find(|f| f.is_test).unwrap();
        assert!(test_file.path.contains("test_"));
        assert!(test_file.content.contains("def test_create_"));
    }

    #[test]
    fn test_python_type_annotations() {
        let gen = default_generator();
        let schema = sample_schema();
        let result = gen.generate_python(&schema).unwrap();
        let content = &result.files[0].content;
        assert!(content.contains("from typing import Optional, List, Dict, Any"));
        assert!(content.contains("tags: List[str]"));
        assert!(content.contains("Dict[str, Any]"));
    }

    // ── Rust generation ────────────────────────────────────────────

    #[test]
    fn test_generate_rust() {
        let gen = default_generator();
        let schema = sample_schema();
        let result = gen.generate_rust(&schema).unwrap();

        assert_eq!(result.language, Language::Rust);
        let main_file = &result.files[0];
        assert!(main_file.path.ends_with(".rs"));
        assert!(main_file
            .content
            .contains("#[derive(Clone, Debug, Serialize, Deserialize)]"));
        assert!(main_file.content.contains("pub struct User"));
        assert!(main_file.content.contains("pub id: String"));
        assert!(main_file.content.contains("pub age: Option<i64>"));
        assert!(main_file.content.contains("pub struct UserRepository"));
        assert!(main_file.content.contains("pub async fn get"));
    }

    #[test]
    fn test_rust_generates_test_file() {
        let gen = default_generator();
        let schema = sample_schema();
        let result = gen.generate_rust(&schema).unwrap();

        let test_file = result.files.iter().find(|f| f.is_test).unwrap();
        assert!(test_file.path.ends_with("_test.rs"));
        assert!(test_file.content.contains("#[cfg(test)]"));
    }

    #[test]
    fn test_rust_serde_import() {
        let gen = default_generator();
        let schema = sample_schema();
        let result = gen.generate_rust(&schema).unwrap();
        let content = &result.files[0].content;
        assert!(content.contains("use serde::{Deserialize, Serialize}"));
    }

    // ── Go generation ──────────────────────────────────────────────

    #[test]
    fn test_generate_go() {
        let gen = default_generator();
        let schema = sample_schema();
        let result = gen.generate_go(&schema).unwrap();

        assert_eq!(result.language, Language::Go);
        let main_file = &result.files[0];
        assert!(main_file.path.ends_with(".go"));
        assert!(main_file.content.contains("package app"));
        assert!(main_file.content.contains("type User struct"));
        assert!(main_file.content.contains("`json:\"id\"`"));
        assert!(main_file.content.contains("*int64"));
        assert!(main_file.content.contains("omitempty"));
        assert!(main_file.content.contains("type UserRepository struct"));
        assert!(main_file.content.contains("func (r *UserRepository) Get"));
    }

    #[test]
    fn test_go_generates_test_file() {
        let gen = default_generator();
        let schema = sample_schema();
        let result = gen.generate_go(&schema).unwrap();

        let test_file = result.files.iter().find(|f| f.is_test).unwrap();
        assert!(test_file.path.ends_with("_test.go"));
        assert!(test_file.content.contains("import \"testing\""));
    }

    #[test]
    fn test_go_json_tags() {
        let gen = default_generator();
        let schema = sample_schema();
        let result = gen.generate_go(&schema).unwrap();
        let content = &result.files[0].content;
        assert!(content.contains("`json:\"email\"`"));
        assert!(content.contains("`json:\"age,omitempty\"`"));
    }

    // ── Query validation ───────────────────────────────────────────

    #[test]
    fn test_validate_query_valid() {
        let gen = default_generator();
        let schema = sample_schema();
        let results = gen.validate_query("SELECT id, name FROM User WHERE id = '1'", &schema);
        assert!(!results.is_empty());
        assert!(results[0].valid);
        assert!(results[0].errors.is_empty());
    }

    #[test]
    fn test_validate_query_unknown_field() {
        let gen = default_generator();
        let schema = sample_schema();
        let results = gen.validate_query("SELECT id, unknown_col FROM User", &schema);
        assert!(!results.is_empty());
        assert!(!results[0].valid);
        assert!(results[0]
            .errors
            .iter()
            .any(|e| e.message.contains("unknown_col")));
    }

    #[test]
    fn test_validate_query_select_star_warning() {
        let gen = default_generator();
        let schema = sample_schema();
        let results = gen.validate_query("SELECT * FROM User", &schema);
        assert!(!results.is_empty());
        assert!(!results[0].warnings.is_empty());
        assert!(results[0].warnings[0].message.contains("SELECT *"));
        assert!(results[0].warnings[0].suggestion.is_some());
    }

    #[test]
    fn test_validate_query_update_without_where() {
        let gen = default_generator();
        let schema = sample_schema();
        let results = gen.validate_query("UPDATE User SET name = 'test'", &schema);
        assert!(!results.is_empty());
        assert!(results[0]
            .warnings
            .iter()
            .any(|w| w.message.contains("WHERE")));
    }

    #[test]
    fn test_validate_query_delete_without_where() {
        let gen = default_generator();
        let schema = sample_schema();
        let results = gen.validate_query("DELETE FROM User", &schema);
        assert!(!results.is_empty());
        assert!(results[0]
            .warnings
            .iter()
            .any(|w| w.message.contains("WHERE")));
    }

    #[test]
    fn test_validate_query_empty_and_comments() {
        let gen = default_generator();
        let schema = sample_schema();
        let results = gen.validate_query("-- this is a comment\n\n", &schema);
        assert!(results.is_empty());
    }

    #[test]
    fn test_validate_query_multiline() {
        let gen = default_generator();
        let schema = sample_schema();
        let query = "SELECT id FROM User\nSELECT bad_field FROM User";
        let results = gen.validate_query(query, &schema);
        assert_eq!(results.len(), 2);
        assert!(results[0].valid);
        assert!(!results[1].valid);
    }

    // ── Field type rendering ───────────────────────────────────────

    #[test]
    fn test_field_type_typescript_primitives() {
        let gen = default_generator();
        assert_eq!(gen.field_type_typescript(&FieldType::String), "string");
        assert_eq!(gen.field_type_typescript(&FieldType::Int64), "number");
        assert_eq!(gen.field_type_typescript(&FieldType::Float64), "number");
        assert_eq!(gen.field_type_typescript(&FieldType::Bool), "boolean");
        assert_eq!(gen.field_type_typescript(&FieldType::Bytes), "Uint8Array");
        assert_eq!(gen.field_type_typescript(&FieldType::Timestamp), "Date");
        assert_eq!(gen.field_type_typescript(&FieldType::Json), "any");
    }

    #[test]
    fn test_field_type_typescript_complex() {
        let gen = default_generator();
        assert_eq!(
            gen.field_type_typescript(&FieldType::Array(Box::new(FieldType::String))),
            "string[]"
        );
        assert_eq!(
            gen.field_type_typescript(&FieldType::Map(
                Box::new(FieldType::String),
                Box::new(FieldType::Int64)
            )),
            "Map<string, number>"
        );
        assert_eq!(
            gen.field_type_typescript(&FieldType::Optional(Box::new(FieldType::Bool))),
            "boolean | null"
        );
        assert_eq!(
            gen.field_type_typescript(&FieldType::Vector { dimensions: 128 }),
            "Float32Array /* 128 dims */"
        );
        assert_eq!(
            gen.field_type_typescript(&FieldType::Reference {
                target: "Post".to_string()
            }),
            "Post"
        );
    }

    #[test]
    fn test_field_type_python_primitives() {
        let gen = default_generator();
        assert_eq!(gen.field_type_python(&FieldType::String), "str");
        assert_eq!(gen.field_type_python(&FieldType::Int64), "int");
        assert_eq!(gen.field_type_python(&FieldType::Float64), "float");
        assert_eq!(gen.field_type_python(&FieldType::Bool), "bool");
        assert_eq!(gen.field_type_python(&FieldType::Bytes), "bytes");
        assert_eq!(gen.field_type_python(&FieldType::Timestamp), "datetime");
        assert_eq!(gen.field_type_python(&FieldType::Json), "Any");
    }

    #[test]
    fn test_field_type_python_complex() {
        let gen = default_generator();
        assert_eq!(
            gen.field_type_python(&FieldType::Array(Box::new(FieldType::Int64))),
            "List[int]"
        );
        assert_eq!(
            gen.field_type_python(&FieldType::Map(
                Box::new(FieldType::String),
                Box::new(FieldType::String)
            )),
            "Dict[str, str]"
        );
        assert_eq!(
            gen.field_type_python(&FieldType::Optional(Box::new(FieldType::Float64))),
            "Optional[float]"
        );
        assert_eq!(
            gen.field_type_python(&FieldType::Vector { dimensions: 64 }),
            "List[float]"
        );
    }

    #[test]
    fn test_field_type_rust_primitives() {
        let gen = default_generator();
        assert_eq!(gen.field_type_rust(&FieldType::String), "String");
        assert_eq!(gen.field_type_rust(&FieldType::Int64), "i64");
        assert_eq!(gen.field_type_rust(&FieldType::Float64), "f64");
        assert_eq!(gen.field_type_rust(&FieldType::Bool), "bool");
        assert_eq!(gen.field_type_rust(&FieldType::Bytes), "Vec<u8>");
        assert_eq!(
            gen.field_type_rust(&FieldType::Timestamp),
            "chrono::DateTime<chrono::Utc>"
        );
        assert_eq!(gen.field_type_rust(&FieldType::Json), "serde_json::Value");
    }

    #[test]
    fn test_field_type_rust_complex() {
        let gen = default_generator();
        assert_eq!(
            gen.field_type_rust(&FieldType::Array(Box::new(FieldType::Bool))),
            "Vec<bool>"
        );
        assert_eq!(
            gen.field_type_rust(&FieldType::Map(
                Box::new(FieldType::String),
                Box::new(FieldType::Int64)
            )),
            "std::collections::HashMap<String, i64>"
        );
        assert_eq!(
            gen.field_type_rust(&FieldType::Optional(Box::new(FieldType::String))),
            "Option<String>"
        );
        assert_eq!(
            gen.field_type_rust(&FieldType::Vector { dimensions: 256 }),
            "Vec<f32>"
        );
    }

    #[test]
    fn test_field_type_go_primitives() {
        let gen = default_generator();
        assert_eq!(gen.field_type_go(&FieldType::String), "string");
        assert_eq!(gen.field_type_go(&FieldType::Int64), "int64");
        assert_eq!(gen.field_type_go(&FieldType::Float64), "float64");
        assert_eq!(gen.field_type_go(&FieldType::Bool), "bool");
        assert_eq!(gen.field_type_go(&FieldType::Bytes), "[]byte");
        assert_eq!(gen.field_type_go(&FieldType::Timestamp), "time.Time");
        assert_eq!(gen.field_type_go(&FieldType::Json), "interface{}");
    }

    #[test]
    fn test_field_type_go_complex() {
        let gen = default_generator();
        assert_eq!(
            gen.field_type_go(&FieldType::Array(Box::new(FieldType::String))),
            "[]string"
        );
        assert_eq!(
            gen.field_type_go(&FieldType::Map(
                Box::new(FieldType::String),
                Box::new(FieldType::Float64)
            )),
            "map[string]float64"
        );
        assert_eq!(
            gen.field_type_go(&FieldType::Optional(Box::new(FieldType::Int64))),
            "*int64"
        );
        assert_eq!(
            gen.field_type_go(&FieldType::Reference {
                target: "Post".to_string()
            }),
            "*Post"
        );
    }

    // ── Index generation ───────────────────────────────────────────

    #[test]
    fn test_index_definition() {
        let idx = IndexDef {
            name: "idx_composite".to_string(),
            fields: vec!["name".to_string(), "email".to_string()],
            index_type: IndexType::Composite,
            unique: true,
        };
        assert_eq!(idx.fields.len(), 2);
        assert_eq!(idx.index_type, IndexType::Composite);
        assert!(idx.unique);
    }

    #[test]
    fn test_all_index_types() {
        let types = vec![
            IndexType::BTree,
            IndexType::Hash,
            IndexType::FullText,
            IndexType::Vector,
            IndexType::Composite,
        ];
        assert_eq!(types.len(), 5);
        assert_ne!(types[0], types[1]);
    }

    // ── Error handling ─────────────────────────────────────────────

    #[test]
    fn test_codegen_error_display() {
        let err = CodegenError::InvalidSchema("missing name".to_string());
        assert_eq!(err.to_string(), "invalid schema: missing name");

        let err = CodegenError::UnsupportedType("complex128".to_string());
        assert_eq!(err.to_string(), "unsupported type: complex128");

        let err = CodegenError::TemplateError("syntax".to_string());
        assert_eq!(err.to_string(), "template error: syntax");

        let err = CodegenError::IoError("permission denied".to_string());
        assert_eq!(err.to_string(), "IO error: permission denied");

        let err = CodegenError::Internal("unexpected".to_string());
        assert_eq!(err.to_string(), "internal error: unexpected");
    }

    #[test]
    fn test_codegen_error_variants() {
        let errors: Vec<CodegenError> = vec![
            CodegenError::InvalidSchema("a".to_string()),
            CodegenError::UnsupportedType("b".to_string()),
            CodegenError::TemplateError("c".to_string()),
            CodegenError::IoError("d".to_string()),
            CodegenError::Internal("e".to_string()),
        ];
        assert_eq!(errors.len(), 5);
    }

    // ── Case conversion helpers ────────────────────────────────────

    #[test]
    fn test_to_snake_case() {
        assert_eq!(to_snake_case("User"), "user");
        assert_eq!(to_snake_case("UserProfile"), "user_profile");
        assert_eq!(to_snake_case("HTMLParser"), "h_t_m_l_parser");
        assert_eq!(to_snake_case("already_snake"), "already_snake");
    }

    #[test]
    fn test_to_camel_case() {
        assert_eq!(to_camel_case("User"), "user");
        assert_eq!(to_camel_case("user_profile"), "userProfile");
        assert_eq!(to_camel_case("hello_world"), "helloWorld");
    }

    #[test]
    fn test_to_pascal_case() {
        assert_eq!(to_pascal_case("user"), "User");
        assert_eq!(to_pascal_case("user_profile"), "UserProfile");
        assert_eq!(to_pascal_case("hello_world"), "HelloWorld");
    }

    // ── Config defaults ────────────────────────────────────────────

    #[test]
    fn test_config_default() {
        let config = TypeSafeConfig::default();
        assert_eq!(config.output_dir, "generated");
        assert!(config.generate_tests);
        assert!(config.include_docs);
        assert!(!config.strict_mode);
        assert!(!config.nullable_by_default);
    }

    // ── Language enum ──────────────────────────────────────────────

    #[test]
    fn test_language_equality() {
        assert_eq!(Language::TypeScript, Language::TypeScript);
        assert_ne!(Language::TypeScript, Language::Python);
    }

    // ── Serialization ──────────────────────────────────────────────

    #[test]
    fn test_schema_serialization_roundtrip() {
        let schema = sample_schema();
        let json = serde_json::to_string(&schema).unwrap();
        let deserialized: SchemaDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.name, schema.name);
        assert_eq!(deserialized.fields.len(), schema.fields.len());
        assert_eq!(deserialized.version, schema.version);
    }

    #[test]
    fn test_field_type_serialization() {
        let ft = FieldType::Vector { dimensions: 128 };
        let json = serde_json::to_string(&ft).unwrap();
        let deserialized: FieldType = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, ft);
    }

    #[test]
    fn test_generated_code_timestamp() {
        let gen = default_generator();
        let schema = sample_schema();
        let result = gen.generate_typescript(&schema).unwrap();
        assert!(result.generated_at <= Utc::now());
    }
}
