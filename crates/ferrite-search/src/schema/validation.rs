//! Schema validation for enforcing schema constraints

use super::{FieldType, Schema, SchemaField};
use serde::{Deserialize, Serialize};

/// Schema validator for checking data against schemas
pub struct SchemaValidator {
    /// Strict mode - fail on unknown fields
    strict: bool,
    /// Coerce types when possible
    coerce: bool,
    /// Collect all errors vs fail fast
    collect_all: bool,
}

impl SchemaValidator {
    /// Create a new schema validator
    pub fn new() -> Self {
        Self {
            strict: false,
            coerce: false,
            collect_all: true,
        }
    }

    /// Enable strict mode (reject unknown fields)
    pub fn strict(mut self) -> Self {
        self.strict = true;
        self
    }

    /// Enable type coercion
    pub fn with_coercion(mut self) -> Self {
        self.coerce = true;
        self
    }

    /// Fail fast on first error
    pub fn fail_fast(mut self) -> Self {
        self.collect_all = false;
        self
    }

    /// Validate a JSON value against a schema
    pub fn validate(&self, value: &serde_json::Value, schema: &Schema) -> ValidationResult {
        let mut errors = Vec::new();

        let obj = match value {
            serde_json::Value::Object(o) => o,
            _ => {
                return ValidationResult {
                    valid: false,
                    errors: vec![ValidationError::NotAnObject],
                    warnings: vec![],
                    coerced_value: None,
                };
            }
        };

        // Check required fields
        for field in schema.required_fields() {
            if !obj.contains_key(&field.name) {
                errors.push(ValidationError::MissingRequired(field.name.clone()));
                if !self.collect_all {
                    return ValidationResult::invalid(errors);
                }
            }
        }

        // Check each field in the value
        for (key, value) in obj {
            match schema.field(key) {
                Some(field) => {
                    // Validate field type
                    if let Err(e) = self.validate_field(value, field) {
                        errors.push(e);
                        if !self.collect_all {
                            return ValidationResult::invalid(errors);
                        }
                    }
                }
                None if self.strict => {
                    errors.push(ValidationError::UnknownField(key.clone()));
                    if !self.collect_all {
                        return ValidationResult::invalid(errors);
                    }
                }
                None => {
                    // Ignore unknown fields in non-strict mode
                }
            }
        }

        ValidationResult {
            valid: errors.is_empty(),
            errors,
            warnings: vec![],
            coerced_value: None,
        }
    }

    /// Validate a single field value
    fn validate_field(
        &self,
        value: &serde_json::Value,
        field: &SchemaField,
    ) -> Result<(), ValidationError> {
        // Check for null
        if value.is_null() {
            if field.required && field.default.is_none() {
                return Err(ValidationError::NullNotAllowed(field.name.clone()));
            }
            return Ok(());
        }

        // Check type
        if !field.field_type.matches_json(value) {
            if self.coerce {
                // Try coercion
                if can_coerce(value, &field.field_type) {
                    return Ok(());
                }
            }
            return Err(ValidationError::TypeMismatch {
                field: field.name.clone(),
                expected: field.field_type.to_type_string(),
                actual: json_type_name(value),
            });
        }

        // Check constraints
        for constraint in &field.constraints {
            if let Err(e) = constraint.validate(&field.name, value) {
                return Err(ValidationError::ConstraintViolation(e.to_string()));
            }
        }

        Ok(())
    }

    /// Validate and coerce a value, returning the coerced result
    pub fn validate_and_coerce(
        &self,
        value: &serde_json::Value,
        schema: &Schema,
    ) -> ValidationResult {
        let mut result = self.validate(value, schema);

        if self.coerce {
            if let serde_json::Value::Object(obj) = value {
                let coerced = coerce_object(obj, schema);
                if coerced != *value {
                    let revalidation = self.validate(&coerced, schema);
                    if revalidation.valid {
                        result = revalidation;
                        result.coerced_value = Some(coerced);
                    }
                }
            }
        }

        result
    }

    /// Fill in defaults for missing fields
    pub fn apply_defaults(&self, value: &mut serde_json::Value, schema: &Schema) {
        if let serde_json::Value::Object(ref mut obj) = value {
            for field in &schema.fields {
                if !obj.contains_key(&field.name) {
                    if let Some(default) = &field.default {
                        obj.insert(field.name.clone(), default.to_json());
                    }
                }
            }
        }
    }
}

impl Default for SchemaValidator {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of schema validation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Whether validation passed
    pub valid: bool,
    /// Validation errors
    pub errors: Vec<ValidationError>,
    /// Validation warnings
    pub warnings: Vec<String>,
    /// Coerced value if coercion was applied
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coerced_value: Option<serde_json::Value>,
}

impl ValidationResult {
    /// Create an invalid result
    fn invalid(errors: Vec<ValidationError>) -> Self {
        Self {
            valid: false,
            errors,
            warnings: vec![],
            coerced_value: None,
        }
    }

    /// Create a valid result
    pub fn valid() -> Self {
        Self {
            valid: true,
            errors: vec![],
            warnings: vec![],
            coerced_value: None,
        }
    }

    /// Check if result is valid
    pub fn is_valid(&self) -> bool {
        self.valid
    }

    /// Get error messages
    pub fn error_messages(&self) -> Vec<String> {
        self.errors.iter().map(|e| e.to_string()).collect()
    }
}

/// Validation error types
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ValidationError {
    /// Value is not a JSON object
    NotAnObject,
    /// Required field is missing
    MissingRequired(String),
    /// Null value not allowed for field
    NullNotAllowed(String),
    /// Type mismatch
    TypeMismatch {
        field: String,
        expected: String,
        actual: String,
    },
    /// Unknown field in strict mode
    UnknownField(String),
    /// Constraint violation
    ConstraintViolation(String),
    /// Nested validation error
    NestedError {
        field: String,
        error: Box<ValidationError>,
    },
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationError::NotAnObject => write!(f, "Value must be a JSON object"),
            ValidationError::MissingRequired(field) => {
                write!(f, "Missing required field: {}", field)
            }
            ValidationError::NullNotAllowed(field) => {
                write!(f, "Null not allowed for field: {}", field)
            }
            ValidationError::TypeMismatch {
                field,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "Type mismatch for '{}': expected {}, got {}",
                    field, expected, actual
                )
            }
            ValidationError::UnknownField(field) => {
                write!(f, "Unknown field: {}", field)
            }
            ValidationError::ConstraintViolation(msg) => {
                write!(f, "Constraint violation: {}", msg)
            }
            ValidationError::NestedError { field, error } => {
                write!(f, "Error in '{}': {}", field, error)
            }
        }
    }
}

impl std::error::Error for ValidationError {}

/// Get JSON type name
fn json_type_name(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(_) => "boolean".to_string(),
        serde_json::Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                "integer".to_string()
            } else {
                "float".to_string()
            }
        }
        serde_json::Value::String(_) => "string".to_string(),
        serde_json::Value::Array(_) => "array".to_string(),
        serde_json::Value::Object(_) => "object".to_string(),
    }
}

/// Check if a value can be coerced to a type
fn can_coerce(value: &serde_json::Value, target: &FieldType) -> bool {
    match (value, target) {
        // String to number
        (serde_json::Value::String(s), FieldType::Integer) => s.parse::<i64>().is_ok(),
        (serde_json::Value::String(s), FieldType::Float) => s.parse::<f64>().is_ok(),
        // Number to string
        (serde_json::Value::Number(_), FieldType::String) => true,
        // Bool to string
        (serde_json::Value::Bool(_), FieldType::String) => true,
        // String to bool
        (serde_json::Value::String(s), FieldType::Boolean) => {
            matches!(
                s.to_lowercase().as_str(),
                "true" | "false" | "1" | "0" | "yes" | "no"
            )
        }
        // Integer to float
        (serde_json::Value::Number(n), FieldType::Float) if n.is_i64() => true,
        _ => false,
    }
}

/// Coerce an object's fields to match schema types
fn coerce_object(
    obj: &serde_json::Map<String, serde_json::Value>,
    schema: &Schema,
) -> serde_json::Value {
    let mut result = serde_json::Map::new();

    for (key, value) in obj {
        let coerced = match schema.field(key) {
            Some(field) => coerce_value(value, &field.field_type),
            None => value.clone(),
        };
        result.insert(key.clone(), coerced);
    }

    serde_json::Value::Object(result)
}

/// Coerce a single value to a target type
fn coerce_value(value: &serde_json::Value, target: &FieldType) -> serde_json::Value {
    match (value, target) {
        // String to integer
        (serde_json::Value::String(s), FieldType::Integer) => {
            if let Ok(i) = s.parse::<i64>() {
                return serde_json::Value::Number(i.into());
            }
        }
        // String to float
        (serde_json::Value::String(s), FieldType::Float) => {
            if let Ok(f) = s.parse::<f64>() {
                if let Some(n) = serde_json::Number::from_f64(f) {
                    return serde_json::Value::Number(n);
                }
            }
        }
        // Number to string
        (serde_json::Value::Number(n), FieldType::String) => {
            return serde_json::Value::String(n.to_string());
        }
        // Bool to string
        (serde_json::Value::Bool(b), FieldType::String) => {
            return serde_json::Value::String(b.to_string());
        }
        // String to bool
        (serde_json::Value::String(s), FieldType::Boolean) => {
            let lower = s.to_lowercase();
            return serde_json::Value::Bool(matches!(lower.as_str(), "true" | "1" | "yes" | "on"));
        }
        // Integer to float (no precision loss)
        (serde_json::Value::Number(n), FieldType::Float) if n.is_i64() => {
            if let Some(i) = n.as_i64() {
                if let Some(f) = serde_json::Number::from_f64(i as f64) {
                    return serde_json::Value::Number(f);
                }
            }
        }
        _ => {}
    }

    value.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Schema;
    use serde_json::json;

    #[test]
    fn test_validate_simple_schema() {
        let schema = Schema::builder("user")
            .field("id", FieldType::String, true)
            .field("name", FieldType::String, true)
            .field("age", FieldType::Integer, false)
            .build();

        let validator = SchemaValidator::new();

        // Valid
        let valid = json!({"id": "1", "name": "Alice", "age": 30});
        let result = validator.validate(&valid, &schema);
        assert!(result.valid);

        // Missing required
        let missing = json!({"id": "1"});
        let result = validator.validate(&missing, &schema);
        assert!(!result.valid);
        assert!(result
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::MissingRequired(_))));

        // Wrong type
        let wrong_type = json!({"id": "1", "name": "Alice", "age": "thirty"});
        let result = validator.validate(&wrong_type, &schema);
        assert!(!result.valid);
    }

    #[test]
    fn test_strict_mode() {
        let schema = Schema::builder("user")
            .field("id", FieldType::String, true)
            .build();

        let value = json!({"id": "1", "extra": "field"});

        // Non-strict allows unknown fields
        let validator = SchemaValidator::new();
        let result = validator.validate(&value, &schema);
        assert!(result.valid);

        // Strict rejects unknown fields
        let strict_validator = SchemaValidator::new().strict();
        let result = strict_validator.validate(&value, &schema);
        assert!(!result.valid);
        assert!(result
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::UnknownField(_))));
    }

    #[test]
    fn test_type_coercion() {
        let schema = Schema::builder("user")
            .field("age", FieldType::Integer, true)
            .build();

        let value = json!({"age": "25"});

        // Without coercion
        let validator = SchemaValidator::new();
        let result = validator.validate(&value, &schema);
        assert!(!result.valid);

        // With coercion
        let coercing_validator = SchemaValidator::new().with_coercion();
        let result = coercing_validator.validate_and_coerce(&value, &schema);
        assert!(result.valid);
        assert!(result.coerced_value.is_some());
    }

    #[test]
    fn test_apply_defaults() {
        let schema = Schema::builder("user")
            .field("id", FieldType::String, true)
            .field_with_default(
                "status",
                FieldType::String,
                super::super::FieldDefault::String("active".to_string()),
            )
            .build();

        let validator = SchemaValidator::new();
        let mut value = json!({"id": "1"});

        validator.apply_defaults(&mut value, &schema);

        assert_eq!(value.get("status"), Some(&json!("active")));
    }

    #[test]
    fn test_coerce_string_to_int() {
        let coerced = coerce_value(&json!("42"), &FieldType::Integer);
        assert_eq!(coerced, json!(42));
    }

    #[test]
    fn test_coerce_int_to_string() {
        let coerced = coerce_value(&json!(42), &FieldType::String);
        assert_eq!(coerced, json!("42"));
    }

    #[test]
    fn test_coerce_string_to_bool() {
        let coerced = coerce_value(&json!("true"), &FieldType::Boolean);
        assert_eq!(coerced, json!(true));

        let coerced = coerce_value(&json!("false"), &FieldType::Boolean);
        assert_eq!(coerced, json!(false));
    }
}
