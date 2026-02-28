//! SDK generation and publishing pipeline.
//!
//! Manages the full lifecycle of multi-language SDK generation from schemas:
//! schema definition → validation → code generation → packaging metadata →
//! version management → publish tracking.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::schema_codegen::{CodeGenConfig, CodeGenerator, FieldDefinition, FieldType, SchemaDefinition};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors returned by the SDK pipeline.
#[derive(Debug, Clone, thiserror::Error)]
pub enum PipelineError {
    /// The requested schema was not found.
    #[error("schema not found: {0}")]
    SchemaNotFound(String),

    /// A schema with the given name already exists.
    #[error("schema already exists: {0}")]
    SchemaAlreadyExists(String),

    /// Schema validation failed.
    #[error("validation failed: {0:?}")]
    ValidationFailed(Vec<String>),

    /// Code generation failed.
    #[error("generation failed: {0}")]
    GenerationFailed(String),

    /// The requested language is not supported.
    #[error("unsupported language: {0}")]
    UnsupportedLanguage(String),

    /// A version conflict was detected.
    #[error("version conflict: {0}")]
    VersionConflict(String),
}

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// Target language for SDK generation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SdkLanguage {
    /// TypeScript with interfaces and ioredis client.
    TypeScript,
    /// Python with dataclasses and redis-py client.
    Python,
    /// Go with structs and go-redis client.
    Go,
    /// Rust with structs and redis-rs client.
    Rust,
    /// Java with classes and Jedis/Lettuce client.
    Java,
    /// C# with classes and StackExchange.Redis client.
    CSharp,
}

impl fmt::Display for SdkLanguage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SdkLanguage::TypeScript => write!(f, "typescript"),
            SdkLanguage::Python => write!(f, "python"),
            SdkLanguage::Go => write!(f, "go"),
            SdkLanguage::Rust => write!(f, "rust"),
            SdkLanguage::Java => write!(f, "java"),
            SdkLanguage::CSharp => write!(f, "csharp"),
        }
    }
}

/// Strategy for version numbering.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum VersionStrategy {
    /// Semantic Versioning (major.minor.patch).
    SemVer,
    /// Calendar Versioning (YYYY.MM.patch).
    CalVer,
    /// Simple sequential numbering.
    Sequential,
}

impl Default for VersionStrategy {
    fn default() -> Self {
        Self::SemVer
    }
}

/// Type of breaking change detected between schema versions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BreakingChangeType {
    /// A field was removed.
    FieldRemoved,
    /// A field's type was changed.
    TypeChanged,
    /// A field was renamed.
    FieldRenamed,
    /// A new non-nullable field was added without a default.
    RequiredFieldAdded,
}

/// Severity level for a validation issue.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IssueSeverity {
    /// Hard error — the schema is invalid.
    Error,
    /// Potential problem worth reviewing.
    Warning,
    /// Informational hint.
    Info,
}

// ---------------------------------------------------------------------------
// Value types
// ---------------------------------------------------------------------------

/// Semantic version for a schema.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaVersion {
    /// Major version — incremented for breaking changes.
    pub major: u32,
    /// Minor version — incremented for backwards-compatible additions.
    pub minor: u32,
    /// Patch version — incremented for backwards-compatible fixes.
    pub patch: u32,
}

impl SchemaVersion {
    /// Creates a new schema version.
    pub fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self { major, minor, patch }
    }

    /// Returns a new version with the major component bumped.
    pub fn bump_major(&self) -> Self {
        Self {
            major: self.major + 1,
            minor: 0,
            patch: 0,
        }
    }

    /// Returns a new version with the minor component bumped.
    pub fn bump_minor(&self) -> Self {
        Self {
            major: self.major,
            minor: self.minor + 1,
            patch: 0,
        }
    }

    /// Returns a new version with the patch component bumped.
    pub fn bump_patch(&self) -> Self {
        Self {
            major: self.major,
            minor: self.minor,
            patch: self.patch + 1,
        }
    }
}

impl fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// A single entry in the schema changelog.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemaChangeEntry {
    /// Version this change was introduced in.
    pub version: SchemaVersion,
    /// Timestamp of the change (Unix seconds).
    pub timestamp: u64,
    /// Human-readable descriptions of what changed.
    pub changes: Vec<String>,
    /// Whether this change is backwards-incompatible.
    pub breaking: bool,
}

/// A schema bundled with version metadata and history.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VersionedSchema {
    /// The underlying schema definition.
    pub schema: SchemaDefinition,
    /// Current version.
    pub version: SchemaVersion,
    /// Creation timestamp (Unix seconds).
    pub created_at: u64,
    /// Last update timestamp (Unix seconds).
    pub updated_at: u64,
    /// Ordered list of changelog entries.
    pub changelog: Vec<SchemaChangeEntry>,
}

/// A generated SDK package for a specific language.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GeneratedSdk {
    /// Target language.
    pub language: SdkLanguage,
    /// Name of the source schema.
    pub schema_name: String,
    /// Schema version used for generation.
    pub schema_version: SchemaVersion,
    /// Generated type definitions source code.
    pub type_definitions: String,
    /// Generated client wrapper source code.
    pub client_code: String,
    /// Generated test code, if configured.
    pub test_code: Option<String>,
    /// Package configuration file contents (package.json, pyproject.toml, etc.).
    pub package_config: String,
    /// Generated README contents.
    pub readme: String,
    /// Timestamp when the SDK was generated (Unix seconds).
    pub generated_at: u64,
    /// Number of files in the generated package.
    pub file_count: usize,
    /// Total lines of generated code.
    pub total_lines: usize,
}

/// A breaking change detected between schema versions.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BreakingChange {
    /// The kind of breaking change.
    pub change_type: BreakingChangeType,
    /// The field affected.
    pub field: String,
    /// Human-readable description.
    pub description: String,
}

/// A validation issue found during schema validation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidationIssue {
    /// Severity of the issue.
    pub severity: IssueSeverity,
    /// Human-readable description.
    pub message: String,
    /// Optional field name related to the issue.
    pub field: Option<String>,
}

/// Manifest describing a package ready for publishing.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PublishManifest {
    /// Name of the source schema.
    pub schema_name: String,
    /// Schema version.
    pub schema_version: SchemaVersion,
    /// Target language.
    pub language: SdkLanguage,
    /// Package name (e.g. "@ferrite/users-client").
    pub package_name: String,
    /// Package version string.
    pub package_version: String,
    /// Target registry (e.g. "npmjs.com", "pypi.org").
    pub registry: String,
    /// List of file paths in the package.
    pub files: Vec<String>,
    /// Hex-encoded checksum of the package contents.
    pub checksum: String,
    /// Timestamp when the manifest was prepared (Unix seconds).
    pub prepared_at: u64,
}

/// Record of a completed publish attempt.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PublishRecord {
    /// The manifest that was published.
    pub manifest: PublishManifest,
    /// Timestamp of the publish attempt (Unix seconds).
    pub published_at: u64,
    /// Whether the publish succeeded.
    pub success: bool,
    /// Error message if the publish failed.
    pub error: Option<String>,
}

/// Snapshot of pipeline statistics.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PipelineStatsSnapshot {
    /// Total schemas registered.
    pub total_schemas: u64,
    /// Total SDKs generated.
    pub total_generated: u64,
    /// Total publish attempts.
    pub total_publishes: u64,
    /// Total failed publish attempts.
    pub failed_publishes: u64,
}

// ---------------------------------------------------------------------------
// Pipeline stats (atomic counters)
// ---------------------------------------------------------------------------

/// Atomic counters for pipeline-level statistics.
pub struct PipelineStats {
    total_schemas: AtomicU64,
    total_generated: AtomicU64,
    total_publishes: AtomicU64,
    failed_publishes: AtomicU64,
}

impl PipelineStats {
    fn new() -> Self {
        Self {
            total_schemas: AtomicU64::new(0),
            total_generated: AtomicU64::new(0),
            total_publishes: AtomicU64::new(0),
            failed_publishes: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> PipelineStatsSnapshot {
        PipelineStatsSnapshot {
            total_schemas: self.total_schemas.load(Ordering::Relaxed),
            total_generated: self.total_generated.load(Ordering::Relaxed),
            total_publishes: self.total_publishes.load(Ordering::Relaxed),
            failed_publishes: self.failed_publishes.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the SDK pipeline.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SdkPipelineConfig {
    /// Languages to generate SDKs for.
    pub languages: Vec<SdkLanguage>,
    /// Base output directory for generated files.
    pub output_dir: String,
    /// Whether to include validation helpers in generated code.
    pub include_validation: bool,
    /// Whether to include test files in generated packages.
    pub include_tests: bool,
    /// Package name prefix (e.g. "ferrite").
    pub package_prefix: String,
    /// Strategy for version numbering.
    pub version_strategy: VersionStrategy,
    /// Whether to detect breaking changes on schema updates.
    pub breaking_change_detection: bool,
}

impl Default for SdkPipelineConfig {
    fn default() -> Self {
        Self {
            languages: vec![
                SdkLanguage::TypeScript,
                SdkLanguage::Python,
                SdkLanguage::Go,
                SdkLanguage::Rust,
            ],
            output_dir: "./sdk/generated".to_string(),
            include_validation: true,
            include_tests: true,
            package_prefix: "ferrite".to_string(),
            version_strategy: VersionStrategy::SemVer,
            breaking_change_detection: true,
        }
    }
}

// ---------------------------------------------------------------------------
// SdkPipeline
// ---------------------------------------------------------------------------

/// Manages the full SDK generation and publishing pipeline.
///
/// Coordinates schema registration, validation, multi-language code generation,
/// package metadata creation, and publish tracking.
pub struct SdkPipeline {
    /// Pipeline configuration.
    config: SdkPipelineConfig,
    /// Registered versioned schemas keyed by name.
    schemas: RwLock<HashMap<String, VersionedSchema>>,
    /// Generated SDKs keyed by schema name.
    generated_sdks: RwLock<HashMap<String, Vec<GeneratedSdk>>>,
    /// Ordered history of publish records (most recent last).
    publish_history: RwLock<VecDeque<PublishRecord>>,
    /// Atomic pipeline statistics.
    stats: PipelineStats,
}

impl SdkPipeline {
    /// Creates a new SDK pipeline with the given configuration.
    pub fn new(config: SdkPipelineConfig) -> Self {
        Self {
            config,
            schemas: RwLock::new(HashMap::new()),
            generated_sdks: RwLock::new(HashMap::new()),
            publish_history: RwLock::new(VecDeque::new()),
            stats: PipelineStats::new(),
        }
    }

    /// Registers a new schema and returns its initial version.
    ///
    /// Returns [`PipelineError::SchemaAlreadyExists`] if a schema with the
    /// same name is already registered, or [`PipelineError::ValidationFailed`]
    /// if the schema has validation errors.
    pub fn register_schema(
        &self,
        schema: SchemaDefinition,
    ) -> Result<SchemaVersion, PipelineError> {
        if self.config.include_validation {
            let issues = self.validate_schema(&schema);
            let errors: Vec<String> = issues
                .iter()
                .filter(|i| i.severity == IssueSeverity::Error)
                .map(|i| i.message.clone())
                .collect();
            if !errors.is_empty() {
                return Err(PipelineError::ValidationFailed(errors));
            }
        }

        let mut schemas = self.schemas.write();
        if schemas.contains_key(&schema.name) {
            return Err(PipelineError::SchemaAlreadyExists(schema.name.clone()));
        }

        let version = SchemaVersion::new(1, 0, 0);
        let now = current_timestamp();
        let versioned = VersionedSchema {
            schema,
            version: version.clone(),
            created_at: now,
            updated_at: now,
            changelog: vec![SchemaChangeEntry {
                version: version.clone(),
                timestamp: now,
                changes: vec!["Initial schema registration".to_string()],
                breaking: false,
            }],
        };
        schemas.insert(versioned.schema.name.clone(), versioned);
        self.stats.total_schemas.fetch_add(1, Ordering::Relaxed);
        Ok(version)
    }

    /// Updates an existing schema and bumps its version.
    ///
    /// If breaking change detection is enabled and breaking changes are found,
    /// the major version is bumped; otherwise the minor version is bumped.
    pub fn update_schema(
        &self,
        name: &str,
        schema: SchemaDefinition,
    ) -> Result<SchemaVersion, PipelineError> {
        if self.config.include_validation {
            let issues = self.validate_schema(&schema);
            let errors: Vec<String> = issues
                .iter()
                .filter(|i| i.severity == IssueSeverity::Error)
                .map(|i| i.message.clone())
                .collect();
            if !errors.is_empty() {
                return Err(PipelineError::ValidationFailed(errors));
            }
        }

        let mut schemas = self.schemas.write();
        let existing = schemas
            .get(name)
            .ok_or_else(|| PipelineError::SchemaNotFound(name.to_string()))?;

        let breaking_changes = if self.config.breaking_change_detection {
            detect_breaking_changes(&existing.schema, &schema)
        } else {
            Vec::new()
        };

        let has_breaking = !breaking_changes.is_empty();
        let new_version = if has_breaking {
            existing.version.bump_major()
        } else {
            existing.version.bump_minor()
        };

        let now = current_timestamp();
        let mut change_descriptions: Vec<String> = breaking_changes
            .iter()
            .map(|bc| bc.description.clone())
            .collect();
        if change_descriptions.is_empty() {
            change_descriptions.push("Schema updated".to_string());
        }

        let mut versioned = existing.clone();
        versioned.schema = schema;
        versioned.version = new_version.clone();
        versioned.updated_at = now;
        versioned.changelog.push(SchemaChangeEntry {
            version: new_version.clone(),
            timestamp: now,
            changes: change_descriptions,
            breaking: has_breaking,
        });

        schemas.insert(name.to_string(), versioned);
        Ok(new_version)
    }

    /// Returns a clone of the versioned schema with the given name.
    pub fn get_schema(&self, name: &str) -> Option<VersionedSchema> {
        self.schemas.read().get(name).cloned()
    }

    /// Returns all registered versioned schemas.
    pub fn list_schemas(&self) -> Vec<VersionedSchema> {
        self.schemas.read().values().cloned().collect()
    }

    /// Generates SDKs for all configured languages for the given schema.
    pub fn generate_all(
        &self,
        schema_name: &str,
    ) -> Result<Vec<GeneratedSdk>, PipelineError> {
        let languages = self.config.languages.clone();
        let mut results = Vec::with_capacity(languages.len());
        for lang in &languages {
            results.push(self.generate_for_language(schema_name, lang.clone())?);
        }

        let mut sdks = self.generated_sdks.write();
        sdks.insert(schema_name.to_string(), results.clone());

        Ok(results)
    }

    /// Generates an SDK for a single language from the given schema.
    pub fn generate_for_language(
        &self,
        schema_name: &str,
        lang: SdkLanguage,
    ) -> Result<GeneratedSdk, PipelineError> {
        let schemas = self.schemas.read();
        let versioned = schemas
            .get(schema_name)
            .ok_or_else(|| PipelineError::SchemaNotFound(schema_name.to_string()))?;

        let schema = &versioned.schema;
        let version = &versioned.version;

        let (type_definitions, client_code, test_code, package_config, readme, file_count) =
            match &lang {
                SdkLanguage::TypeScript => generate_typescript_sdk(
                    schema,
                    version,
                    &self.config,
                ),
                SdkLanguage::Python => generate_python_sdk(
                    schema,
                    version,
                    &self.config,
                ),
                SdkLanguage::Go => generate_go_sdk(
                    schema,
                    version,
                    &self.config,
                ),
                SdkLanguage::Rust => generate_rust_sdk(
                    schema,
                    version,
                    &self.config,
                ),
                SdkLanguage::Java | SdkLanguage::CSharp => {
                    return Err(PipelineError::UnsupportedLanguage(lang.to_string()));
                }
            };

        let total_lines = type_definitions.lines().count()
            + client_code.lines().count()
            + test_code.as_deref().map_or(0, |t| t.lines().count())
            + package_config.lines().count()
            + readme.lines().count();

        self.stats.total_generated.fetch_add(1, Ordering::Relaxed);

        Ok(GeneratedSdk {
            language: lang,
            schema_name: schema_name.to_string(),
            schema_version: version.clone(),
            type_definitions,
            client_code,
            test_code,
            package_config,
            readme,
            generated_at: current_timestamp(),
            file_count,
            total_lines,
        })
    }

    /// Validates a schema definition and returns any issues found.
    pub fn validate_schema(&self, schema: &SchemaDefinition) -> Vec<ValidationIssue> {
        let mut issues = Vec::new();

        if schema.name.is_empty() {
            issues.push(ValidationIssue {
                severity: IssueSeverity::Error,
                message: "schema name cannot be empty".to_string(),
                field: None,
            });
        }

        if schema.fields.is_empty() {
            issues.push(ValidationIssue {
                severity: IssueSeverity::Error,
                message: "schema must have at least one field".to_string(),
                field: None,
            });
        }

        if schema.key_pattern.is_empty() {
            issues.push(ValidationIssue {
                severity: IssueSeverity::Warning,
                message: "key_pattern is empty".to_string(),
                field: None,
            });
        }

        let mut seen_names = std::collections::HashSet::new();
        for f in &schema.fields {
            if f.name.is_empty() {
                issues.push(ValidationIssue {
                    severity: IssueSeverity::Error,
                    message: "field name cannot be empty".to_string(),
                    field: None,
                });
            } else if !seen_names.insert(&f.name) {
                issues.push(ValidationIssue {
                    severity: IssueSeverity::Error,
                    message: format!("duplicate field name: {}", f.name),
                    field: Some(f.name.clone()),
                });
            }
        }

        issues
    }

    /// Checks for breaking changes between the currently registered schema
    /// and a proposed new definition.
    pub fn check_breaking_changes(
        &self,
        name: &str,
        new_schema: &SchemaDefinition,
    ) -> Vec<BreakingChange> {
        let schemas = self.schemas.read();
        match schemas.get(name) {
            Some(existing) => detect_breaking_changes(&existing.schema, new_schema),
            None => Vec::new(),
        }
    }

    /// Prepares a publish manifest for the given schema and language.
    pub fn prepare_publish(
        &self,
        schema_name: &str,
        lang: SdkLanguage,
    ) -> Result<PublishManifest, PipelineError> {
        let schemas = self.schemas.read();
        let versioned = schemas
            .get(schema_name)
            .ok_or_else(|| PipelineError::SchemaNotFound(schema_name.to_string()))?;

        let version = &versioned.version;
        let version_str = version.to_string();

        let (package_name, registry, files) = match &lang {
            SdkLanguage::TypeScript => (
                format!("@{}/{}-client", self.config.package_prefix, schema_name),
                "npmjs.com".to_string(),
                vec![
                    "package.json".to_string(),
                    format!("src/{}.ts", schema_name),
                    "src/index.ts".to_string(),
                    "README.md".to_string(),
                ],
            ),
            SdkLanguage::Python => (
                format!("{}_{}_client", self.config.package_prefix, schema_name),
                "pypi.org".to_string(),
                vec![
                    "pyproject.toml".to_string(),
                    format!("{}_{}_client/__init__.py", self.config.package_prefix, schema_name),
                    format!("{}_{}_client/models.py", self.config.package_prefix, schema_name),
                    "README.md".to_string(),
                ],
            ),
            SdkLanguage::Go => (
                format!("{}-{}-client", self.config.package_prefix, schema_name),
                "pkg.go.dev".to_string(),
                vec![
                    "go.mod".to_string(),
                    format!("{}.go", schema_name),
                    "README.md".to_string(),
                ],
            ),
            SdkLanguage::Rust => (
                format!("{}-{}-client", self.config.package_prefix, schema_name),
                "crates.io".to_string(),
                vec![
                    "Cargo.toml".to_string(),
                    format!("src/{}.rs", schema_name),
                    "src/lib.rs".to_string(),
                    "README.md".to_string(),
                ],
            ),
            other => {
                return Err(PipelineError::UnsupportedLanguage(other.to_string()));
            }
        };

        // Simple checksum from schema name + version + language.
        let checksum = format!("{:016x}", {
            let mut h: u64 = 0xcbf29ce484222325;
            for b in format!("{}{}{}", schema_name, version_str, lang).bytes() {
                h ^= b as u64;
                h = h.wrapping_mul(0x100000001b3);
            }
            h
        });

        Ok(PublishManifest {
            schema_name: schema_name.to_string(),
            schema_version: version.clone(),
            language: lang,
            package_name,
            package_version: version_str,
            registry,
            files,
            checksum,
            prepared_at: current_timestamp(),
        })
    }

    /// Records a completed publish attempt and returns the record.
    pub fn record_publish(&self, manifest: &PublishManifest) -> PublishRecord {
        let record = PublishRecord {
            manifest: manifest.clone(),
            published_at: current_timestamp(),
            success: true,
            error: None,
        };
        self.stats.total_publishes.fetch_add(1, Ordering::Relaxed);
        self.publish_history.write().push_back(record.clone());
        record
    }

    /// Returns the most recent publish records, up to `limit`.
    pub fn publish_history(&self, limit: usize) -> Vec<PublishRecord> {
        let history = self.publish_history.read();
        history.iter().rev().take(limit).cloned().collect()
    }

    /// Returns a snapshot of the pipeline statistics.
    pub fn stats(&self) -> PipelineStatsSnapshot {
        self.stats.snapshot()
    }
}

// ---------------------------------------------------------------------------
// Language-specific code generators
// ---------------------------------------------------------------------------

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

fn generate_typescript_sdk(
    schema: &SchemaDefinition,
    version: &SchemaVersion,
    config: &SdkPipelineConfig,
) -> (String, String, Option<String>, String, String, usize) {
    let gen = CodeGenerator::new(CodeGenConfig {
        include_validation: config.include_validation,
        ..CodeGenConfig::default()
    });
    let code = gen.generate_typescript(schema);

    let package_config = format!(
        r#"{{
  "name": "@{prefix}/{name}-client",
  "version": "{version}",
  "description": "TypeScript client for {name} schema",
  "main": "dist/index.js",
  "types": "dist/index.d.ts",
  "scripts": {{
    "build": "tsc",
    "test": "jest"
  }},
  "dependencies": {{
    "ioredis": "^5.3.0"
  }},
  "devDependencies": {{
    "typescript": "^5.0.0",
    "@types/node": "^20.0.0",
    "jest": "^29.0.0",
    "ts-jest": "^29.0.0"
  }}
}}"#,
        prefix = config.package_prefix,
        name = schema.name,
        version = version,
    );

    let test_code = if config.include_tests {
        let name_pascal = pascal_case(&schema.name);
        Some(format!(
            r#"import {{ {name} }} from './{raw_name}';

describe('{name}', () => {{
  it('should create a valid {name} object', () => {{
    const obj: {name} = {{{fields}
    }};
    expect(obj).toBeDefined();
  }});
}});
"#,
            name = name_pascal,
            raw_name = schema.name,
            fields = schema
                .fields
                .iter()
                .filter(|f| !f.nullable)
                .map(|f| format!(
                    "\n      {}: {}",
                    f.name,
                    ts_default_value(&f.field_type)
                ))
                .collect::<Vec<_>>()
                .join(","),
        ))
    } else {
        None
    };

    let readme = generate_readme(schema, version, &SdkLanguage::TypeScript, config);
    let file_count = if config.include_tests { 5 } else { 4 };

    (
        code.type_definitions,
        code.client_code,
        test_code,
        package_config,
        readme,
        file_count,
    )
}

fn generate_python_sdk(
    schema: &SchemaDefinition,
    version: &SchemaVersion,
    config: &SdkPipelineConfig,
) -> (String, String, Option<String>, String, String, usize) {
    let gen = CodeGenerator::new(CodeGenConfig {
        include_validation: config.include_validation,
        ..CodeGenConfig::default()
    });
    let code = gen.generate_python(schema);

    let package_config = format!(
        r#"[build-system]
requires = ["setuptools>=68.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "{prefix}-{name}-client"
version = "{version}"
description = "Python client for {name} schema"
requires-python = ">=3.9"
dependencies = [
    "redis>=5.0.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0.0",
    "pytest-asyncio>=0.21.0",
]
"#,
        prefix = config.package_prefix,
        name = schema.name,
        version = version,
    );

    let test_code = if config.include_tests {
        let name_pascal = pascal_case(&schema.name);
        Some(format!(
            r#"from {prefix}_{name}_client.models import {pascal}


def test_create_{name}():
    obj = {pascal}({fields})
    assert obj is not None
"#,
            prefix = config.package_prefix,
            name = schema.name,
            pascal = name_pascal,
            fields = schema
                .fields
                .iter()
                .filter(|f| !f.nullable)
                .map(|f| format!("{}={}", f.name, py_default_value(&f.field_type)))
                .collect::<Vec<_>>()
                .join(", "),
        ))
    } else {
        None
    };

    let readme = generate_readme(schema, version, &SdkLanguage::Python, config);
    let file_count = if config.include_tests { 5 } else { 4 };

    (
        code.type_definitions,
        code.client_code,
        test_code,
        package_config,
        readme,
        file_count,
    )
}

fn generate_go_sdk(
    schema: &SchemaDefinition,
    version: &SchemaVersion,
    config: &SdkPipelineConfig,
) -> (String, String, Option<String>, String, String, usize) {
    let name_pascal = pascal_case(&schema.name);
    let pkg_name = schema.name.to_lowercase();

    // --- type definitions ---
    let mut td = format!("package {}\n\n", pkg_name);
    td.push_str(&format!("// {} represents the {} schema.\n", name_pascal, schema.name));
    td.push_str(&format!("type {} struct {{\n", name_pascal));
    for f in &schema.fields {
        let go_type = go_type(&f.field_type, f.nullable);
        let json_tag = if f.nullable {
            format!("`json:\"{},omitempty\"`", f.name)
        } else {
            format!("`json:\"{}\"`", f.name)
        };
        let field_name = pascal_case(&f.name);
        td.push_str(&format!("\t{} {} {}\n", field_name, go_type, json_tag));
    }
    td.push_str("}\n");

    // --- client code ---
    let mut cc = String::new();
    cc.push_str(&format!(
        r#"// {name}Client provides typed access to {raw_name} records.
type {name}Client struct {{
	prefix string
}}

// New{name}Client creates a new client with the given key prefix.
func New{name}Client(prefix string) *{name}Client {{
	return &{name}Client{{prefix: prefix}}
}}

// Get retrieves a {raw_name} by id.
func (c *{name}Client) Get(id string) (*{name}, error) {{
	// TODO: implement with go-redis
	return nil, nil
}}

// Set stores a {raw_name} by id.
func (c *{name}Client) Set(id string, data *{name}) error {{
	// TODO: implement with go-redis
	return nil
}}
"#,
        name = name_pascal,
        raw_name = schema.name,
    ));

    // --- package config (go.mod) ---
    let package_config = format!(
        r#"module github.com/{prefix}/{name}-client

go 1.21

require github.com/redis/go-redis/v9 v9.5.0
"#,
        prefix = config.package_prefix,
        name = schema.name,
    );

    let test_code = if config.include_tests {
        Some(format!(
            r#"package {pkg}

import "testing"

func TestNew{pascal}(t *testing.T) {{
	obj := {pascal}{{}}
	if obj == ({pascal}{{}}) {{
		t.Log("created zero-value {pascal}")
	}}
}}

func TestNew{pascal}Client(t *testing.T) {{
	client := New{pascal}Client("{pattern}")
	if client == nil {{
		t.Fatal("expected non-nil client")
	}}
}}
"#,
            pkg = pkg_name,
            pascal = name_pascal,
            pattern = schema.key_pattern.replace('*', ""),
        ))
    } else {
        None
    };

    let readme = generate_readme(schema, version, &SdkLanguage::Go, config);
    let file_count = if config.include_tests { 4 } else { 3 };

    (td, cc, test_code, package_config, readme, file_count)
}

fn generate_rust_sdk(
    schema: &SchemaDefinition,
    version: &SchemaVersion,
    config: &SdkPipelineConfig,
) -> (String, String, Option<String>, String, String, usize) {
    let gen = CodeGenerator::new(CodeGenConfig {
        include_validation: config.include_validation,
        ..CodeGenConfig::default()
    });
    let code = gen.generate_rust(schema);

    let package_config = format!(
        r#"[package]
name = "{prefix}-{name}-client"
version = "{version}"
edition = "2021"
description = "Rust client for {name} schema"

[dependencies]
redis = "0.25"
serde = {{ version = "1", features = ["derive"] }}
serde_json = "1"
"#,
        prefix = config.package_prefix,
        name = schema.name,
        version = version,
    );

    let test_code = if config.include_tests {
        let name_pascal = pascal_case(&schema.name);
        Some(format!(
            r#"#[cfg(test)]
mod tests {{
    use super::*;

    #[test]
    fn test_create_{name}() {{
        let _obj = {pascal} {{
{fields}
        }};
    }}
}}
"#,
            name = schema.name,
            pascal = name_pascal,
            fields = schema
                .fields
                .iter()
                .map(|f| {
                    let val = rust_default_value(&f.field_type, f.nullable);
                    format!("            {}: {},", f.name, val)
                })
                .collect::<Vec<_>>()
                .join("\n"),
        ))
    } else {
        None
    };

    let readme = generate_readme(schema, version, &SdkLanguage::Rust, config);
    let file_count = if config.include_tests { 5 } else { 4 };

    (
        code.type_definitions,
        code.client_code,
        test_code,
        package_config,
        readme,
        file_count,
    )
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn generate_readme(
    schema: &SchemaDefinition,
    version: &SchemaVersion,
    lang: &SdkLanguage,
    config: &SdkPipelineConfig,
) -> String {
    let desc = schema
        .description
        .as_deref()
        .unwrap_or("Auto-generated SDK");
    format!(
        "# {prefix}-{name}-client ({lang})\n\n\
         {desc}\n\n\
         ## Version\n\n\
         {version}\n\n\
         ## Schema\n\n\
         Key pattern: `{pattern}`\n\n\
         ## Fields\n\n\
         {fields}\n",
        prefix = config.package_prefix,
        name = schema.name,
        lang = lang,
        desc = desc,
        version = version,
        pattern = schema.key_pattern,
        fields = schema
            .fields
            .iter()
            .map(|f| format!(
                "- `{}`: {:?}{}",
                f.name,
                f.field_type,
                if f.nullable { " (optional)" } else { "" }
            ))
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

fn go_type(ft: &FieldType, nullable: bool) -> String {
    let base = match ft {
        FieldType::String => "string".to_string(),
        FieldType::Integer => "int64".to_string(),
        FieldType::Float => "float64".to_string(),
        FieldType::Boolean => "bool".to_string(),
        FieldType::Bytes => "[]byte".to_string(),
        FieldType::Timestamp => "int64".to_string(),
        FieldType::Array(inner) => format!("[]{}", go_type(inner, false)),
        FieldType::Map | FieldType::Json => "map[string]interface{}".to_string(),
    };
    if nullable {
        format!("*{}", base)
    } else {
        base
    }
}

fn ts_default_value(ft: &FieldType) -> &'static str {
    match ft {
        FieldType::String => "'test'",
        FieldType::Integer | FieldType::Float | FieldType::Timestamp => "0",
        FieldType::Boolean => "false",
        FieldType::Bytes => "new Uint8Array()",
        FieldType::Array(_) => "[]",
        FieldType::Map | FieldType::Json => "{}",
    }
}

fn py_default_value(ft: &FieldType) -> &'static str {
    match ft {
        FieldType::String => "'test'",
        FieldType::Integer | FieldType::Timestamp => "0",
        FieldType::Float => "0.0",
        FieldType::Boolean => "False",
        FieldType::Bytes => "b''",
        FieldType::Array(_) => "[]",
        FieldType::Map | FieldType::Json => "{}",
    }
}

fn rust_default_value(ft: &FieldType, nullable: bool) -> String {
    if nullable {
        return "None".to_string();
    }
    match ft {
        FieldType::String => "String::new()".to_string(),
        FieldType::Integer => "0".to_string(),
        FieldType::Float => "0.0".to_string(),
        FieldType::Boolean => "false".to_string(),
        FieldType::Bytes => "Vec::new()".to_string(),
        FieldType::Timestamp => "0".to_string(),
        FieldType::Array(_) => "Vec::new()".to_string(),
        FieldType::Map | FieldType::Json => "serde_json::Value::Null".to_string(),
    }
}

fn detect_breaking_changes(
    old: &SchemaDefinition,
    new: &SchemaDefinition,
) -> Vec<BreakingChange> {
    let mut changes = Vec::new();

    let old_fields: HashMap<&str, &FieldDefinition> =
        old.fields.iter().map(|f| (f.name.as_str(), f)).collect();
    let new_fields: HashMap<&str, &FieldDefinition> =
        new.fields.iter().map(|f| (f.name.as_str(), f)).collect();

    // Check for removed fields.
    for (name, _) in &old_fields {
        if !new_fields.contains_key(name) {
            changes.push(BreakingChange {
                change_type: BreakingChangeType::FieldRemoved,
                field: name.to_string(),
                description: format!("field '{}' was removed", name),
            });
        }
    }

    // Check for type changes.
    for (name, old_field) in &old_fields {
        if let Some(new_field) = new_fields.get(name) {
            if old_field.field_type != new_field.field_type {
                changes.push(BreakingChange {
                    change_type: BreakingChangeType::TypeChanged,
                    field: name.to_string(),
                    description: format!(
                        "field '{}' type changed from {:?} to {:?}",
                        name, old_field.field_type, new_field.field_type
                    ),
                });
            }
        }
    }

    // Check for new required fields.
    for (name, new_field) in &new_fields {
        if !old_fields.contains_key(name) && !new_field.nullable && new_field.default_value.is_none()
        {
            changes.push(BreakingChange {
                change_type: BreakingChangeType::RequiredFieldAdded,
                field: name.to_string(),
                description: format!(
                    "required field '{}' was added without a default",
                    name
                ),
            });
        }
    }

    changes
}

fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
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

    fn make_pipeline() -> SdkPipeline {
        SdkPipeline::new(SdkPipelineConfig::default())
    }

    #[test]
    fn test_register_schema() {
        let pipeline = make_pipeline();
        let version = pipeline.register_schema(sample_schema()).unwrap();
        assert_eq!(version, SchemaVersion::new(1, 0, 0));

        let vs = pipeline.get_schema("user").unwrap();
        assert_eq!(vs.schema.name, "user");
        assert_eq!(vs.version, SchemaVersion::new(1, 0, 0));
    }

    #[test]
    fn test_register_duplicate_fails() {
        let pipeline = make_pipeline();
        pipeline.register_schema(sample_schema()).unwrap();
        let err = pipeline.register_schema(sample_schema()).unwrap_err();
        assert!(matches!(err, PipelineError::SchemaAlreadyExists(_)));
    }

    #[test]
    fn test_update_schema_minor_bump() {
        let pipeline = make_pipeline();
        pipeline.register_schema(sample_schema()).unwrap();

        let mut updated = sample_schema();
        updated.fields.push(FieldDefinition {
            name: "nickname".to_string(),
            field_type: FieldType::String,
            nullable: true,
            description: None,
            default_value: None,
        });

        let version = pipeline.update_schema("user", updated).unwrap();
        assert_eq!(version, SchemaVersion::new(1, 1, 0));
    }

    #[test]
    fn test_update_schema_major_bump_on_breaking() {
        let pipeline = make_pipeline();
        pipeline.register_schema(sample_schema()).unwrap();

        let mut updated = sample_schema();
        // Remove a field → breaking change.
        updated.fields.retain(|f| f.name != "email");

        let version = pipeline.update_schema("user", updated).unwrap();
        assert_eq!(version, SchemaVersion::new(2, 0, 0));
    }

    #[test]
    fn test_update_nonexistent_schema_fails() {
        let pipeline = make_pipeline();
        let err = pipeline
            .update_schema("nonexistent", sample_schema())
            .unwrap_err();
        assert!(matches!(err, PipelineError::SchemaNotFound(_)));
    }

    #[test]
    fn test_generate_typescript_sdk() {
        let pipeline = make_pipeline();
        pipeline.register_schema(sample_schema()).unwrap();

        let sdk = pipeline
            .generate_for_language("user", SdkLanguage::TypeScript)
            .unwrap();
        assert_eq!(sdk.language, SdkLanguage::TypeScript);
        assert!(sdk.type_definitions.contains("interface User"));
        assert!(sdk.client_code.contains("UserClient"));
        assert!(sdk.package_config.contains("ioredis"));
        assert!(sdk.test_code.is_some());
        assert!(sdk.total_lines > 0);
    }

    #[test]
    fn test_generate_python_sdk() {
        let pipeline = make_pipeline();
        pipeline.register_schema(sample_schema()).unwrap();

        let sdk = pipeline
            .generate_for_language("user", SdkLanguage::Python)
            .unwrap();
        assert_eq!(sdk.language, SdkLanguage::Python);
        assert!(sdk.type_definitions.contains("class User"));
        assert!(sdk.client_code.contains("UserClient"));
        assert!(sdk.package_config.contains("redis>=5.0.0"));
        assert!(sdk.test_code.is_some());
    }

    #[test]
    fn test_generate_go_sdk() {
        let pipeline = make_pipeline();
        pipeline.register_schema(sample_schema()).unwrap();

        let sdk = pipeline
            .generate_for_language("user", SdkLanguage::Go)
            .unwrap();
        assert_eq!(sdk.language, SdkLanguage::Go);
        assert!(sdk.type_definitions.contains("type User struct"));
        assert!(sdk.type_definitions.contains("json:\"id\""));
        assert!(sdk.type_definitions.contains("*int64"));
        assert!(sdk.type_definitions.contains("json:\"age,omitempty\""));
        assert!(sdk.client_code.contains("UserClient"));
        assert!(sdk.package_config.contains("go-redis"));
    }

    #[test]
    fn test_generate_rust_sdk() {
        let pipeline = make_pipeline();
        pipeline.register_schema(sample_schema()).unwrap();

        let sdk = pipeline
            .generate_for_language("user", SdkLanguage::Rust)
            .unwrap();
        assert_eq!(sdk.language, SdkLanguage::Rust);
        assert!(sdk.type_definitions.contains("pub struct User"));
        assert!(sdk.client_code.contains("UserClient"));
        assert!(sdk.package_config.contains("redis"));
    }

    #[test]
    fn test_generate_all_languages() {
        let pipeline = make_pipeline();
        pipeline.register_schema(sample_schema()).unwrap();

        let sdks = pipeline.generate_all("user").unwrap();
        assert_eq!(sdks.len(), 4); // TS, Python, Go, Rust
    }

    #[test]
    fn test_unsupported_language() {
        let pipeline = make_pipeline();
        pipeline.register_schema(sample_schema()).unwrap();

        let err = pipeline
            .generate_for_language("user", SdkLanguage::Java)
            .unwrap_err();
        assert!(matches!(err, PipelineError::UnsupportedLanguage(_)));
    }

    #[test]
    fn test_validate_schema() {
        let pipeline = make_pipeline();

        let mut bad = sample_schema();
        bad.name = String::new();
        let issues = pipeline.validate_schema(&bad);
        assert!(issues.iter().any(|i| i.severity == IssueSeverity::Error));

        let mut empty_fields = sample_schema();
        empty_fields.fields.clear();
        let issues = pipeline.validate_schema(&empty_fields);
        assert!(issues
            .iter()
            .any(|i| i.message.contains("at least one field")));
    }

    #[test]
    fn test_check_breaking_changes() {
        let pipeline = make_pipeline();
        pipeline.register_schema(sample_schema()).unwrap();

        // Remove a field.
        let mut new_schema = sample_schema();
        new_schema.fields.retain(|f| f.name != "email");

        let changes = pipeline.check_breaking_changes("user", &new_schema);
        assert!(!changes.is_empty());
        assert!(changes
            .iter()
            .any(|c| c.change_type == BreakingChangeType::FieldRemoved));

        // Change a type.
        let mut type_changed = sample_schema();
        type_changed.fields[3].field_type = FieldType::String; // age: Integer → String
        let changes = pipeline.check_breaking_changes("user", &type_changed);
        assert!(changes
            .iter()
            .any(|c| c.change_type == BreakingChangeType::TypeChanged));
    }

    #[test]
    fn test_prepare_and_record_publish() {
        let pipeline = make_pipeline();
        pipeline.register_schema(sample_schema()).unwrap();

        let manifest = pipeline
            .prepare_publish("user", SdkLanguage::TypeScript)
            .unwrap();
        assert_eq!(manifest.package_name, "@ferrite/user-client");
        assert_eq!(manifest.registry, "npmjs.com");
        assert!(!manifest.checksum.is_empty());

        let record = pipeline.record_publish(&manifest);
        assert!(record.success);

        let history = pipeline.publish_history(10);
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].manifest.package_name, "@ferrite/user-client");
    }

    #[test]
    fn test_pipeline_stats() {
        let pipeline = make_pipeline();
        pipeline.register_schema(sample_schema()).unwrap();
        pipeline
            .generate_for_language("user", SdkLanguage::Go)
            .unwrap();

        let manifest = pipeline
            .prepare_publish("user", SdkLanguage::Go)
            .unwrap();
        pipeline.record_publish(&manifest);

        let stats = pipeline.stats();
        assert_eq!(stats.total_schemas, 1);
        assert_eq!(stats.total_generated, 1);
        assert_eq!(stats.total_publishes, 1);
    }

    #[test]
    fn test_list_schemas() {
        let pipeline = make_pipeline();
        assert!(pipeline.list_schemas().is_empty());

        pipeline.register_schema(sample_schema()).unwrap();
        let list = pipeline.list_schemas();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].schema.name, "user");
    }

    #[test]
    fn test_schema_version_display() {
        let v = SchemaVersion::new(1, 2, 3);
        assert_eq!(v.to_string(), "1.2.3");
        assert_eq!(v.bump_major(), SchemaVersion::new(2, 0, 0));
        assert_eq!(v.bump_minor(), SchemaVersion::new(1, 3, 0));
        assert_eq!(v.bump_patch(), SchemaVersion::new(1, 2, 4));
    }

    #[test]
    fn test_duplicate_field_validation() {
        let pipeline = make_pipeline();
        let mut schema = sample_schema();
        schema.fields.push(FieldDefinition {
            name: "id".to_string(),
            field_type: FieldType::String,
            nullable: false,
            description: None,
            default_value: None,
        });
        let issues = pipeline.validate_schema(&schema);
        assert!(issues.iter().any(|i| i.message.contains("duplicate")));
    }
}
