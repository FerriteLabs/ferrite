//! Data Contract Registry
//!
//! Schema registry for key-value data: define expected shapes per key
//! pattern, enforce on write, track breaking changes, generate reports.
#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the contract registry.
#[derive(Debug, Clone)]
pub struct ContractConfig {
    /// Whether contract enforcement is enabled.
    pub enabled: bool,
    /// How to handle violations.
    pub enforce_mode: EnforceMode,
    /// Maximum number of registered contracts.
    pub max_contracts: usize,
    /// Whether to track individual violations.
    pub track_violations: bool,
    /// Maximum number of violation records to keep.
    pub max_violations: usize,
}

impl Default for ContractConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            enforce_mode: EnforceMode::Warn,
            max_contracts: 1000,
            track_violations: true,
            max_violations: 10000,
        }
    }
}

/// How violations are handled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EnforceMode {
    /// Reject the write operation.
    Reject,
    /// Allow but log a warning.
    Warn,
    /// Allow silently (metrics only).
    Silent,
}

impl EnforceMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Reject => "REJECT",
            Self::Warn => "WARN",
            Self::Silent => "SILENT",
        }
    }

    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "REJECT" => Some(Self::Reject),
            "WARN" => Some(Self::Warn),
            "SILENT" => Some(Self::Silent),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Schema types
// ---------------------------------------------------------------------------

/// A data contract defining the expected shape of values for a key pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataContract {
    /// Unique contract name.
    pub name: String,
    /// Glob pattern for matching keys.
    pub key_pattern: String,
    /// The schema definition.
    pub schema: SchemaDefinition,
    /// Contract version.
    pub version: u32,
    /// Creation timestamp.
    pub created_at: u64,
    /// Last update timestamp.
    pub updated_at: u64,
    /// Human-readable description.
    pub description: String,
    /// Owner team/user.
    pub owner: String,
    /// History of breaking changes.
    pub breaking_changes: Vec<BreakingChange>,
}

/// The schema definition for a contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDefinition {
    /// Top-level type.
    pub schema_type: SchemaType,
    /// Field definitions (for Json / Object types).
    pub fields: Vec<FieldDef>,
    /// Required field names.
    pub required_fields: Vec<String>,
    /// Maximum allowed size in bytes.
    pub max_size_bytes: Option<u64>,
    /// Expected encoding.
    pub encoding: DataEncoding,
}

impl Default for SchemaDefinition {
    fn default() -> Self {
        Self {
            schema_type: SchemaType::Any,
            fields: Vec::new(),
            required_fields: Vec::new(),
            max_size_bytes: None,
            encoding: DataEncoding::Utf8,
        }
    }
}

/// Top-level schema type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaType {
    Json,
    String,
    Integer,
    Float,
    Binary,
    Any,
}

impl SchemaType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Json => "JSON",
            Self::String => "STRING",
            Self::Integer => "INTEGER",
            Self::Float => "FLOAT",
            Self::Binary => "BINARY",
            Self::Any => "ANY",
        }
    }

    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "JSON" => Some(Self::Json),
            "STRING" | "STR" => Some(Self::String),
            "INTEGER" | "INT" => Some(Self::Integer),
            "FLOAT" | "DOUBLE" | "NUMBER" => Some(Self::Float),
            "BINARY" | "BYTES" => Some(Self::Binary),
            "ANY" => Some(Self::Any),
            _ => None,
        }
    }
}

/// Field definition inside a schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDef {
    /// Field name.
    pub name: String,
    /// Expected field type.
    pub field_type: FieldType,
    /// Whether this field is required.
    pub required: bool,
    /// Human-readable description.
    pub description: String,
    /// Constraints on the field value.
    pub constraints: Vec<Constraint>,
}

/// Field types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    String,
    Integer,
    Float,
    Boolean,
    Array,
    Object,
    Null,
    Any,
}

impl FieldType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Integer => "integer",
            Self::Float => "float",
            Self::Boolean => "boolean",
            Self::Array => "array",
            Self::Object => "object",
            Self::Null => "null",
            Self::Any => "any",
        }
    }

    /// Check whether a JSON value matches this type.
    fn matches_json(&self, val: &serde_json::Value) -> bool {
        match self {
            Self::String => val.is_string(),
            Self::Integer => val.is_i64() || val.is_u64(),
            Self::Float => val.is_f64(),
            Self::Boolean => val.is_boolean(),
            Self::Array => val.is_array(),
            Self::Object => val.is_object(),
            Self::Null => val.is_null(),
            Self::Any => true,
        }
    }
}

/// A constraint on a field value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Constraint {
    pub constraint_type: ConstraintType,
}

/// Types of constraints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConstraintType {
    MinLength(usize),
    MaxLength(usize),
    Min(f64),
    Max(f64),
    Pattern(String),
    Enum(Vec<String>),
    Format(String),
}

/// Expected data encoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataEncoding {
    Utf8,
    Json,
    Msgpack,
    Protobuf,
    Raw,
}

impl DataEncoding {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Utf8 => "UTF8",
            Self::Json => "JSON",
            Self::Msgpack => "MSGPACK",
            Self::Protobuf => "PROTOBUF",
            Self::Raw => "RAW",
        }
    }
}

/// Record of a breaking change between versions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BreakingChange {
    pub version: u32,
    pub description: String,
    pub timestamp: u64,
    pub severity: ChangeSeverity,
}

/// Severity of a schema change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeSeverity {
    Minor,
    Major,
    Breaking,
}

impl ChangeSeverity {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Minor => "MINOR",
            Self::Major => "MAJOR",
            Self::Breaking => "BREAKING",
        }
    }
}

// ---------------------------------------------------------------------------
// Validation result types
// ---------------------------------------------------------------------------

/// Result of validating a value against a contract.
#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub valid: bool,
    pub contract_name: Option<String>,
    pub errors: Vec<ValidationError>,
    pub warnings: Vec<String>,
}

/// A single validation error.
#[derive(Debug, Clone)]
pub struct ValidationError {
    pub field: String,
    pub message: String,
    pub constraint: String,
}

/// Result of evolving a contract schema.
#[derive(Debug, Clone)]
pub struct EvolutionResult {
    pub old_version: u32,
    pub new_version: u32,
    pub is_breaking: bool,
    pub changes: Vec<String>,
}

/// A recorded violation.
#[derive(Debug, Clone)]
pub struct Violation {
    pub timestamp: u64,
    pub key: String,
    pub contract: String,
    pub errors: Vec<String>,
}

/// Summary information about a contract.
#[derive(Debug, Clone)]
pub struct ContractInfo {
    pub name: String,
    pub key_pattern: String,
    pub version: u32,
    pub fields_count: usize,
    pub violations: u64,
    pub last_validated: u64,
}

/// Compatibility report for a contract.
#[derive(Debug, Clone)]
pub struct CompatibilityReport {
    pub contract: String,
    pub versions: Vec<u32>,
    pub breaking_changes: Vec<BreakingChange>,
    pub total_validations: u64,
    pub violation_rate: f64,
}

/// Aggregate statistics.
#[derive(Debug, Clone)]
pub struct ContractStats {
    pub contracts: usize,
    pub validations: u64,
    pub violations: u64,
    pub violation_rate: f64,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors returned by the contract registry.
#[derive(Debug, thiserror::Error)]
pub enum ContractError {
    #[error("contract not found: {0}")]
    NotFound(String),
    #[error("contract already exists: {0}")]
    AlreadyExists(String),
    #[error("invalid schema: {0}")]
    InvalidSchema(String),
    #[error("maximum number of contracts reached ({0})")]
    MaxContracts(usize),
    #[error("schema evolution failed: {0}")]
    EvolutionFailed(String),
}

// ---------------------------------------------------------------------------
// Internal per-contract tracking
// ---------------------------------------------------------------------------

struct ContractEntry {
    contract: DataContract,
    validations: AtomicU64,
    violations_count: AtomicU64,
    last_validated: AtomicU64,
}

// ---------------------------------------------------------------------------
// ContractRegistry
// ---------------------------------------------------------------------------

/// Registry for data contracts.
pub struct ContractRegistry {
    config: ContractConfig,
    contracts: RwLock<Vec<ContractEntry>>,
    violations: RwLock<Vec<Violation>>,
    total_validations: AtomicU64,
    total_violations: AtomicU64,
}

impl ContractRegistry {
    /// Create a new registry with the given configuration.
    pub fn new(config: ContractConfig) -> Self {
        Self {
            config,
            contracts: RwLock::new(Vec::new()),
            violations: RwLock::new(Vec::new()),
            total_validations: AtomicU64::new(0),
            total_violations: AtomicU64::new(0),
        }
    }

    /// Register a new contract.
    pub fn register(&self, contract: DataContract) -> Result<(), ContractError> {
        let mut contracts = self.contracts.write();
        if contracts.len() >= self.config.max_contracts {
            return Err(ContractError::MaxContracts(self.config.max_contracts));
        }
        if contracts.iter().any(|e| e.contract.name == contract.name) {
            return Err(ContractError::AlreadyExists(contract.name.clone()));
        }
        contracts.push(ContractEntry {
            contract,
            validations: AtomicU64::new(0),
            violations_count: AtomicU64::new(0),
            last_validated: AtomicU64::new(0),
        });
        Ok(())
    }

    /// Unregister a contract by name.
    pub fn unregister(&self, name: &str) -> Result<(), ContractError> {
        let mut contracts = self.contracts.write();
        let idx = contracts
            .iter()
            .position(|e| e.contract.name == name)
            .ok_or_else(|| ContractError::NotFound(name.to_string()))?;
        contracts.remove(idx);
        Ok(())
    }

    /// Validate a value against the contract matching the key.
    pub fn validate(&self, key: &str, value: &[u8]) -> ValidationResult {
        self.total_validations.fetch_add(1, Ordering::Relaxed);

        let contracts = self.contracts.read();
        let entry = contracts
            .iter()
            .find(|e| glob_match(&e.contract.key_pattern, key));

        let entry = match entry {
            Some(e) => e,
            None => {
                return ValidationResult {
                    valid: true,
                    contract_name: None,
                    errors: vec![],
                    warnings: vec!["no contract matches this key".to_string()],
                }
            }
        };

        entry.validations.fetch_add(1, Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        entry.last_validated.store(now, Ordering::Relaxed);

        let contract = &entry.contract;
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        // Size check.
        if let Some(max) = contract.schema.max_size_bytes {
            if value.len() as u64 > max {
                errors.push(ValidationError {
                    field: "<value>".to_string(),
                    message: format!("value size {} exceeds maximum {}", value.len(), max),
                    constraint: "max_size_bytes".to_string(),
                });
            }
        }

        // For JSON schema type, parse and validate fields.
        if contract.schema.schema_type == SchemaType::Json {
            match std::str::from_utf8(value)
                .ok()
                .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
            {
                Some(json_val) => {
                    validate_json_schema(&contract.schema, &json_val, &mut errors, &mut warnings);
                }
                None => {
                    errors.push(ValidationError {
                        field: "<value>".to_string(),
                        message: "value is not valid JSON".to_string(),
                        constraint: "schema_type".to_string(),
                    });
                }
            }
        }

        let valid = errors.is_empty();
        let contract_name = contract.name.clone();

        if !valid {
            entry.violations_count.fetch_add(1, Ordering::Relaxed);
            self.total_violations.fetch_add(1, Ordering::Relaxed);
            if self.config.track_violations {
                let mut viols = self.violations.write();
                if viols.len() < self.config.max_violations {
                    viols.push(Violation {
                        timestamp: now,
                        key: key.to_string(),
                        contract: contract_name.clone(),
                        errors: errors.iter().map(|e| e.message.clone()).collect(),
                    });
                }
            }
        }

        ValidationResult {
            valid,
            contract_name: Some(contract_name),
            errors,
            warnings,
        }
    }

    /// Evolve a contract's schema to a new version.
    pub fn evolve(
        &self,
        name: &str,
        new_schema: SchemaDefinition,
        description: &str,
    ) -> Result<EvolutionResult, ContractError> {
        let mut contracts = self.contracts.write();
        let entry = contracts
            .iter_mut()
            .find(|e| e.contract.name == name)
            .ok_or_else(|| ContractError::NotFound(name.to_string()))?;

        let old_version = entry.contract.version;
        let mut changes = Vec::new();
        let mut is_breaking = false;

        // Detect field removals (breaking).
        for old_field in &entry.contract.schema.fields {
            if !new_schema.fields.iter().any(|f| f.name == old_field.name) {
                changes.push(format!("removed field '{}'", old_field.name));
                is_breaking = true;
            }
        }
        // Detect field additions.
        for new_field in &new_schema.fields {
            if !entry
                .contract
                .schema
                .fields
                .iter()
                .any(|f| f.name == new_field.name)
            {
                let msg = format!("added field '{}'", new_field.name);
                if new_field.required {
                    is_breaking = true;
                    changes.push(format!("{} (required, breaking)", msg));
                } else {
                    changes.push(msg);
                }
            }
        }
        // Detect type changes.
        for new_field in &new_schema.fields {
            if let Some(old_field) = entry
                .contract
                .schema
                .fields
                .iter()
                .find(|f| f.name == new_field.name)
            {
                if old_field.field_type != new_field.field_type {
                    changes.push(format!(
                        "field '{}' type changed from {} to {}",
                        new_field.name,
                        old_field.field_type.as_str(),
                        new_field.field_type.as_str()
                    ));
                    is_breaking = true;
                }
            }
        }

        if changes.is_empty() {
            changes.push("no detectable changes".to_string());
        }

        let new_version = old_version + 1;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if is_breaking {
            entry.contract.breaking_changes.push(BreakingChange {
                version: new_version,
                description: description.to_string(),
                timestamp: now,
                severity: ChangeSeverity::Breaking,
            });
        }

        entry.contract.schema = new_schema;
        entry.contract.version = new_version;
        entry.contract.updated_at = now;

        Ok(EvolutionResult {
            old_version,
            new_version,
            is_breaking,
            changes,
        })
    }

    /// Get a contract by name.
    pub fn get_contract(&self, name: &str) -> Option<DataContract> {
        let contracts = self.contracts.read();
        contracts
            .iter()
            .find(|e| e.contract.name == name)
            .map(|e| e.contract.clone())
    }

    /// Find the contract that matches a key.
    pub fn find_contract(&self, key: &str) -> Option<DataContract> {
        let contracts = self.contracts.read();
        contracts
            .iter()
            .find(|e| glob_match(&e.contract.key_pattern, key))
            .map(|e| e.contract.clone())
    }

    /// List summary info for all contracts.
    pub fn list_contracts(&self) -> Vec<ContractInfo> {
        let contracts = self.contracts.read();
        contracts
            .iter()
            .map(|e| ContractInfo {
                name: e.contract.name.clone(),
                key_pattern: e.contract.key_pattern.clone(),
                version: e.contract.version,
                fields_count: e.contract.schema.fields.len(),
                violations: e.violations_count.load(Ordering::Relaxed),
                last_validated: e.last_validated.load(Ordering::Relaxed),
            })
            .collect()
    }

    /// Get recent violations.
    pub fn violations(&self, limit: usize) -> Vec<Violation> {
        let viols = self.violations.read();
        viols.iter().rev().take(limit).cloned().collect()
    }

    /// Generate a compatibility report for a contract.
    pub fn compatibility_report(&self, name: &str) -> CompatibilityReport {
        let contracts = self.contracts.read();
        if let Some(entry) = contracts.iter().find(|e| e.contract.name == name) {
            let total_v = entry.validations.load(Ordering::Relaxed);
            let total_viol = entry.violations_count.load(Ordering::Relaxed);
            let versions: Vec<u32> = (1..=entry.contract.version).collect();
            CompatibilityReport {
                contract: name.to_string(),
                versions,
                breaking_changes: entry.contract.breaking_changes.clone(),
                total_validations: total_v,
                violation_rate: if total_v > 0 {
                    total_viol as f64 / total_v as f64
                } else {
                    0.0
                },
            }
        } else {
            CompatibilityReport {
                contract: name.to_string(),
                versions: vec![],
                breaking_changes: vec![],
                total_validations: 0,
                violation_rate: 0.0,
            }
        }
    }

    /// Aggregate statistics.
    pub fn stats(&self) -> ContractStats {
        let contracts = self.contracts.read();
        let total_v = self.total_validations.load(Ordering::Relaxed);
        let total_viol = self.total_violations.load(Ordering::Relaxed);
        ContractStats {
            contracts: contracts.len(),
            validations: total_v,
            violations: total_viol,
            violation_rate: if total_v > 0 {
                total_viol as f64 / total_v as f64
            } else {
                0.0
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Validate JSON value against a schema definition.
fn validate_json_schema(
    schema: &SchemaDefinition,
    val: &serde_json::Value,
    errors: &mut Vec<ValidationError>,
    warnings: &mut Vec<String>,
) {
    let obj = match val.as_object() {
        Some(o) => o,
        None => {
            if !schema.fields.is_empty() {
                errors.push(ValidationError {
                    field: "<root>".to_string(),
                    message: "expected JSON object but got another type".to_string(),
                    constraint: "schema_type".to_string(),
                });
            }
            return;
        }
    };

    // Check required fields.
    for req in &schema.required_fields {
        if !obj.contains_key(req) {
            errors.push(ValidationError {
                field: req.clone(),
                message: format!("required field '{}' is missing", req),
                constraint: "required".to_string(),
            });
        }
    }

    // Validate each defined field.
    for field_def in &schema.fields {
        if let Some(field_val) = obj.get(&field_def.name) {
            // Type check.
            if !field_def.field_type.matches_json(field_val) {
                errors.push(ValidationError {
                    field: field_def.name.clone(),
                    message: format!(
                        "field '{}' expected type {} but got different type",
                        field_def.name,
                        field_def.field_type.as_str()
                    ),
                    constraint: "field_type".to_string(),
                });
                continue;
            }
            // Apply constraints.
            for constraint in &field_def.constraints {
                validate_constraint(
                    &field_def.name,
                    field_val,
                    &constraint.constraint_type,
                    errors,
                );
            }
        } else if field_def.required {
            errors.push(ValidationError {
                field: field_def.name.clone(),
                message: format!("required field '{}' is missing", field_def.name),
                constraint: "required".to_string(),
            });
        }
    }

    // Warn about unknown fields.
    for key in obj.keys() {
        if !schema.fields.iter().any(|f| f.name == *key) && !schema.fields.is_empty() {
            warnings.push(format!("unknown field '{}'", key));
        }
    }
}

/// Validate a single constraint against a JSON value.
fn validate_constraint(
    field_name: &str,
    val: &serde_json::Value,
    ct: &ConstraintType,
    errors: &mut Vec<ValidationError>,
) {
    match ct {
        ConstraintType::MinLength(min) => {
            if let Some(s) = val.as_str() {
                if s.len() < *min {
                    errors.push(ValidationError {
                        field: field_name.to_string(),
                        message: format!("string length {} is less than minimum {}", s.len(), min),
                        constraint: "min_length".to_string(),
                    });
                }
            }
        }
        ConstraintType::MaxLength(max) => {
            if let Some(s) = val.as_str() {
                if s.len() > *max {
                    errors.push(ValidationError {
                        field: field_name.to_string(),
                        message: format!("string length {} exceeds maximum {}", s.len(), max),
                        constraint: "max_length".to_string(),
                    });
                }
            }
        }
        ConstraintType::Min(min) => {
            if let Some(n) = val.as_f64() {
                if n < *min {
                    errors.push(ValidationError {
                        field: field_name.to_string(),
                        message: format!("value {} is less than minimum {}", n, min),
                        constraint: "min".to_string(),
                    });
                }
            }
        }
        ConstraintType::Max(max) => {
            if let Some(n) = val.as_f64() {
                if n > *max {
                    errors.push(ValidationError {
                        field: field_name.to_string(),
                        message: format!("value {} exceeds maximum {}", n, max),
                        constraint: "max".to_string(),
                    });
                }
            }
        }
        ConstraintType::Pattern(pat) => {
            if let Some(s) = val.as_str() {
                if let Ok(re) = regex::Regex::new(pat) {
                    if !re.is_match(s) {
                        errors.push(ValidationError {
                            field: field_name.to_string(),
                            message: format!("value does not match pattern '{}'", pat),
                            constraint: "pattern".to_string(),
                        });
                    }
                }
            }
        }
        ConstraintType::Enum(allowed) => {
            if let Some(s) = val.as_str() {
                if !allowed.contains(&s.to_string()) {
                    errors.push(ValidationError {
                        field: field_name.to_string(),
                        message: format!("value '{}' not in allowed values {:?}", s, allowed),
                        constraint: "enum".to_string(),
                    });
                }
            }
        }
        ConstraintType::Format(_fmt) => {
            // Format validation is advisory only for now.
        }
    }
}

/// Simple glob matching supporting `*` and `?`.
fn glob_match(pattern: &str, text: &str) -> bool {
    glob_match_impl(
        &pattern.chars().collect::<Vec<_>>(),
        &text.chars().collect::<Vec<_>>(),
    )
}

fn glob_match_impl(pattern: &[char], text: &[char]) -> bool {
    let mut px = 0;
    let mut tx = 0;
    let mut star_px = usize::MAX;
    let mut star_tx = 0;

    while tx < text.len() {
        if px < pattern.len() && (pattern[px] == '?' || pattern[px] == text[tx]) {
            px += 1;
            tx += 1;
        } else if px < pattern.len() && pattern[px] == '*' {
            star_px = px;
            star_tx = tx;
            px += 1;
        } else if star_px != usize::MAX {
            px = star_px + 1;
            star_tx += 1;
            tx = star_tx;
        } else {
            return false;
        }
    }
    while px < pattern.len() && pattern[px] == '*' {
        px += 1;
    }
    px == pattern.len()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn now_ts() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn make_contract(name: &str, pattern: &str) -> DataContract {
        DataContract {
            name: name.to_string(),
            key_pattern: pattern.to_string(),
            schema: SchemaDefinition::default(),
            version: 1,
            created_at: now_ts(),
            updated_at: now_ts(),
            description: format!("test contract {}", name),
            owner: "test".to_string(),
            breaking_changes: vec![],
        }
    }

    fn make_json_contract(name: &str, pattern: &str) -> DataContract {
        let mut c = make_contract(name, pattern);
        c.schema.schema_type = SchemaType::Json;
        c.schema.fields = vec![
            FieldDef {
                name: "name".to_string(),
                field_type: FieldType::String,
                required: true,
                description: "user name".to_string(),
                constraints: vec![Constraint {
                    constraint_type: ConstraintType::MinLength(1),
                }],
            },
            FieldDef {
                name: "age".to_string(),
                field_type: FieldType::Integer,
                required: false,
                description: "user age".to_string(),
                constraints: vec![
                    Constraint {
                        constraint_type: ConstraintType::Min(0.0),
                    },
                    Constraint {
                        constraint_type: ConstraintType::Max(200.0),
                    },
                ],
            },
        ];
        c.schema.required_fields = vec!["name".to_string()];
        c
    }

    #[test]
    fn test_register_and_get() {
        let reg = ContractRegistry::new(ContractConfig::default());
        let c = make_contract("test", "user:*");
        reg.register(c).unwrap();
        assert!(reg.get_contract("test").is_some());
        assert!(reg.get_contract("nonexistent").is_none());
    }

    #[test]
    fn test_duplicate_registration() {
        let reg = ContractRegistry::new(ContractConfig::default());
        reg.register(make_contract("dup", "x:*")).unwrap();
        assert!(matches!(
            reg.register(make_contract("dup", "y:*")),
            Err(ContractError::AlreadyExists(_))
        ));
    }

    #[test]
    fn test_unregister() {
        let reg = ContractRegistry::new(ContractConfig::default());
        reg.register(make_contract("rm", "x:*")).unwrap();
        reg.unregister("rm").unwrap();
        assert!(matches!(
            reg.unregister("rm"),
            Err(ContractError::NotFound(_))
        ));
    }

    #[test]
    fn test_find_contract_by_key() {
        let reg = ContractRegistry::new(ContractConfig::default());
        reg.register(make_contract("users", "user:*")).unwrap();
        reg.register(make_contract("sessions", "session:*"))
            .unwrap();

        assert_eq!(
            reg.find_contract("user:123").map(|c| c.name),
            Some("users".to_string())
        );
        assert_eq!(
            reg.find_contract("session:abc").map(|c| c.name),
            Some("sessions".to_string())
        );
        assert!(reg.find_contract("other:key").is_none());
    }

    #[test]
    fn test_validate_json_valid() {
        let reg = ContractRegistry::new(ContractConfig::default());
        reg.register(make_json_contract("users", "user:*")).unwrap();

        let value = br#"{"name": "Alice", "age": 30}"#;
        let result = reg.validate("user:1", value);
        assert!(result.valid);
        assert_eq!(result.contract_name.as_deref(), Some("users"));
    }

    #[test]
    fn test_validate_json_missing_required() {
        let reg = ContractRegistry::new(ContractConfig::default());
        reg.register(make_json_contract("users", "user:*")).unwrap();

        let value = br#"{"age": 30}"#;
        let result = reg.validate("user:1", value);
        assert!(!result.valid);
        assert!(!result.errors.is_empty());
    }

    #[test]
    fn test_validate_json_constraint_violation() {
        let reg = ContractRegistry::new(ContractConfig::default());
        reg.register(make_json_contract("users", "user:*")).unwrap();

        let value = br#"{"name": "", "age": -5}"#;
        let result = reg.validate("user:1", value);
        assert!(!result.valid);
    }

    #[test]
    fn test_validate_no_contract() {
        let reg = ContractRegistry::new(ContractConfig::default());
        let result = reg.validate("unknown:key", b"whatever");
        assert!(result.valid);
        assert!(result.contract_name.is_none());
    }

    #[test]
    fn test_validate_size_limit() {
        let reg = ContractRegistry::new(ContractConfig::default());
        let mut c = make_contract("small", "small:*");
        c.schema.max_size_bytes = Some(10);
        reg.register(c).unwrap();

        let result = reg.validate("small:key", b"this is way too long for the limit");
        assert!(!result.valid);
    }

    #[test]
    fn test_evolve_contract() {
        let reg = ContractRegistry::new(ContractConfig::default());
        reg.register(make_json_contract("users", "user:*")).unwrap();

        let mut new_schema = SchemaDefinition::default();
        new_schema.schema_type = SchemaType::Json;
        new_schema.fields = vec![
            FieldDef {
                name: "name".to_string(),
                field_type: FieldType::String,
                required: true,
                description: "".to_string(),
                constraints: vec![],
            },
            FieldDef {
                name: "email".to_string(),
                field_type: FieldType::String,
                required: true,
                description: "email address".to_string(),
                constraints: vec![],
            },
        ];
        new_schema.required_fields = vec!["name".to_string(), "email".to_string()];

        let result = reg
            .evolve("users", new_schema, "added email, removed age")
            .unwrap();
        assert_eq!(result.old_version, 1);
        assert_eq!(result.new_version, 2);
        assert!(result.is_breaking);
    }

    #[test]
    fn test_list_contracts() {
        let reg = ContractRegistry::new(ContractConfig::default());
        reg.register(make_contract("a", "a:*")).unwrap();
        reg.register(make_contract("b", "b:*")).unwrap();
        assert_eq!(reg.list_contracts().len(), 2);
    }

    #[test]
    fn test_violations_tracking() {
        let reg = ContractRegistry::new(ContractConfig::default());
        reg.register(make_json_contract("users", "user:*")).unwrap();
        reg.validate("user:1", b"not json at all");
        let viols = reg.violations(10);
        assert_eq!(viols.len(), 1);
    }

    #[test]
    fn test_stats() {
        let reg = ContractRegistry::new(ContractConfig::default());
        reg.register(make_json_contract("users", "user:*")).unwrap();
        reg.validate("user:1", br#"{"name":"ok"}"#);
        reg.validate("user:2", b"bad");
        let stats = reg.stats();
        assert_eq!(stats.contracts, 1);
        assert_eq!(stats.validations, 2);
        assert_eq!(stats.violations, 1);
    }

    #[test]
    fn test_compatibility_report() {
        let reg = ContractRegistry::new(ContractConfig::default());
        reg.register(make_json_contract("users", "user:*")).unwrap();
        reg.validate("user:1", br#"{"name":"Alice"}"#);
        let report = reg.compatibility_report("users");
        assert_eq!(report.contract, "users");
        assert_eq!(report.versions, vec![1]);
        assert_eq!(report.total_validations, 1);
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("user:*", "user:123"));
        assert!(glob_match("session:*:data", "session:abc:data"));
        assert!(!glob_match("session:*:data", "session:abc:meta"));
        assert!(glob_match("*", "anything"));
    }
}
