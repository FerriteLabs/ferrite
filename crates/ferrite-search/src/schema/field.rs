//! Schema field definitions and constraints

use super::types::FieldType;
use serde::{Deserialize, Serialize};

/// A field in a schema
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemaField {
    /// Field name
    pub name: String,

    /// Field data type
    pub field_type: FieldType,

    /// Whether this field is required
    pub required: bool,

    /// Default value if not provided
    pub default: Option<FieldDefault>,

    /// Human-readable description
    pub description: Option<String>,

    /// Field constraints
    pub constraints: Vec<FieldConstraint>,

    /// Whether this field is deprecated
    pub deprecated: bool,
}

impl SchemaField {
    /// Create a new required field
    pub fn required(name: &str, field_type: FieldType) -> Self {
        Self {
            name: name.to_string(),
            field_type,
            required: true,
            default: None,
            description: None,
            constraints: Vec::new(),
            deprecated: false,
        }
    }

    /// Create a new optional field
    pub fn optional(name: &str, field_type: FieldType) -> Self {
        Self {
            name: name.to_string(),
            field_type,
            required: false,
            default: None,
            description: None,
            constraints: Vec::new(),
            deprecated: false,
        }
    }

    /// Add a default value
    pub fn with_default(mut self, default: FieldDefault) -> Self {
        self.default = Some(default);
        self
    }

    /// Add a description
    pub fn with_description(mut self, desc: &str) -> Self {
        self.description = Some(desc.to_string());
        self
    }

    /// Add a constraint
    pub fn with_constraint(mut self, constraint: FieldConstraint) -> Self {
        self.constraints.push(constraint);
        self
    }

    /// Mark as deprecated
    pub fn deprecate(mut self) -> Self {
        self.deprecated = true;
        self
    }

    /// Validate a JSON value against this field
    pub fn validate(&self, value: Option<&serde_json::Value>) -> Result<(), FieldValidationError> {
        match value {
            None => {
                if self.required && self.default.is_none() {
                    return Err(FieldValidationError::RequiredFieldMissing(
                        self.name.clone(),
                    ));
                }
            }
            Some(v) => {
                // Check type
                if !self.field_type.matches_json(v) {
                    return Err(FieldValidationError::TypeMismatch {
                        field: self.name.clone(),
                        expected: self.field_type.to_type_string(),
                        actual: json_type_name(v),
                    });
                }

                // Check constraints
                for constraint in &self.constraints {
                    constraint.validate(&self.name, v)?;
                }
            }
        }

        Ok(())
    }

    /// Get the default value as JSON
    pub fn default_json(&self) -> Option<serde_json::Value> {
        self.default.as_ref().map(|d| d.to_json())
    }
}

/// Default value for a field
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FieldDefault {
    /// Null value
    Null,
    /// Boolean value
    Bool(bool),
    /// Integer value
    Integer(i64),
    /// Float value
    Float(f64),
    /// String value
    String(String),
    /// JSON value (for complex defaults)
    Json(serde_json::Value),
    /// Expression to evaluate (e.g., "NOW()", "UUID()")
    Expression(String),
}

impl FieldDefault {
    /// Parse a default value from string
    pub fn parse(s: &str, field_type: &FieldType) -> Option<Self> {
        let s = s.trim();

        // Check for special expressions
        if s.eq_ignore_ascii_case("null") || s.eq_ignore_ascii_case("nil") {
            return Some(FieldDefault::Null);
        }
        if s.eq_ignore_ascii_case("now()") || s.eq_ignore_ascii_case("current_timestamp") {
            return Some(FieldDefault::Expression("NOW()".to_string()));
        }
        if s.eq_ignore_ascii_case("uuid()") || s.eq_ignore_ascii_case("gen_uuid") {
            return Some(FieldDefault::Expression("UUID()".to_string()));
        }

        match field_type {
            FieldType::Boolean => {
                if s.eq_ignore_ascii_case("true") {
                    Some(FieldDefault::Bool(true))
                } else if s.eq_ignore_ascii_case("false") {
                    Some(FieldDefault::Bool(false))
                } else {
                    None
                }
            }
            FieldType::Integer => s.parse::<i64>().ok().map(FieldDefault::Integer),
            FieldType::Float => s.parse::<f64>().ok().map(FieldDefault::Float),
            FieldType::String => Some(FieldDefault::String(s.to_string())),
            FieldType::Timestamp => {
                if let Ok(ts) = s.parse::<i64>() {
                    Some(FieldDefault::Integer(ts))
                } else {
                    Some(FieldDefault::String(s.to_string()))
                }
            }
            _ => {
                // Try JSON parsing for complex types
                serde_json::from_str(s).ok().map(FieldDefault::Json)
            }
        }
    }

    /// Convert to JSON value
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            FieldDefault::Null => serde_json::Value::Null,
            FieldDefault::Bool(b) => serde_json::Value::Bool(*b),
            FieldDefault::Integer(i) => serde_json::Value::Number((*i).into()),
            FieldDefault::Float(f) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            FieldDefault::String(s) => serde_json::Value::String(s.clone()),
            FieldDefault::Json(v) => v.clone(),
            FieldDefault::Expression(expr) => {
                // Evaluate expression
                match expr.as_str() {
                    "NOW()" => {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs() as i64)
                            .unwrap_or(0);
                        serde_json::Value::Number(now.into())
                    }
                    "UUID()" => {
                        // Simple UUID generation using random bytes
                        let uuid = format!(
                            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
                            rand_u32(),
                            rand_u16(),
                            rand_u16(),
                            rand_u16(),
                            rand_u64() & 0xFFFFFFFFFFFF
                        );
                        serde_json::Value::String(uuid)
                    }
                    _ => serde_json::Value::Null,
                }
            }
        }
    }
}

// Simple random number generation for UUID
fn rand_u32() -> u32 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let state = RandomState::new();
    let mut hasher = state.build_hasher();
    hasher.write_usize(0);
    hasher.finish() as u32
}

fn rand_u16() -> u16 {
    rand_u32() as u16
}

fn rand_u64() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let state = RandomState::new();
    let mut hasher = state.build_hasher();
    hasher.write_usize(1);
    hasher.finish()
}

/// Field constraint for validation
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FieldConstraint {
    /// Minimum value (for numbers) or length (for strings/arrays)
    Min(f64),
    /// Maximum value (for numbers) or length (for strings/arrays)
    Max(f64),
    /// Exact length (for strings/arrays)
    Length(usize),
    /// Regex pattern (for strings)
    Pattern(String),
    /// Enumeration of allowed values
    Enum(Vec<serde_json::Value>),
    /// Must be unique within the collection
    Unique,
    /// Must reference another key
    Reference(String),
    /// Custom validation expression
    Custom(String),
}

impl FieldConstraint {
    /// Validate a value against this constraint
    pub fn validate(
        &self,
        field_name: &str,
        value: &serde_json::Value,
    ) -> Result<(), FieldValidationError> {
        match self {
            FieldConstraint::Min(min) => {
                let actual = value_to_comparable(value);
                if actual < *min {
                    return Err(FieldValidationError::ConstraintViolation {
                        field: field_name.to_string(),
                        constraint: format!("min({})", min),
                        message: format!("Value {} is less than minimum {}", actual, min),
                    });
                }
            }
            FieldConstraint::Max(max) => {
                let actual = value_to_comparable(value);
                if actual > *max {
                    return Err(FieldValidationError::ConstraintViolation {
                        field: field_name.to_string(),
                        constraint: format!("max({})", max),
                        message: format!("Value {} is greater than maximum {}", actual, max),
                    });
                }
            }
            FieldConstraint::Length(len) => {
                let actual_len = value_length(value);
                if actual_len != *len {
                    return Err(FieldValidationError::ConstraintViolation {
                        field: field_name.to_string(),
                        constraint: format!("length({})", len),
                        message: format!("Length {} does not match required {}", actual_len, len),
                    });
                }
            }
            FieldConstraint::Pattern(pattern) => {
                if let serde_json::Value::String(s) = value {
                    let re = regex::Regex::new(pattern).map_err(|_| {
                        FieldValidationError::ConstraintViolation {
                            field: field_name.to_string(),
                            constraint: format!("pattern({})", pattern),
                            message: "Invalid regex pattern".to_string(),
                        }
                    })?;
                    if !re.is_match(s) {
                        return Err(FieldValidationError::ConstraintViolation {
                            field: field_name.to_string(),
                            constraint: format!("pattern({})", pattern),
                            message: format!("Value does not match pattern {}", pattern),
                        });
                    }
                }
            }
            FieldConstraint::Enum(allowed) => {
                if !allowed.contains(value) {
                    return Err(FieldValidationError::ConstraintViolation {
                        field: field_name.to_string(),
                        constraint: "enum".to_string(),
                        message: format!("Value is not in allowed set: {:?}", allowed),
                    });
                }
            }
            FieldConstraint::Unique
            | FieldConstraint::Reference(_)
            | FieldConstraint::Custom(_) => {
                // These require external context to validate
                // Skip for now, they're validated at a higher level
            }
        }

        Ok(())
    }
}

/// Field validation error
#[derive(Clone, Debug)]
pub enum FieldValidationError {
    /// Required field is missing
    RequiredFieldMissing(String),
    /// Type mismatch
    TypeMismatch {
        field: String,
        expected: String,
        actual: String,
    },
    /// Constraint violation
    ConstraintViolation {
        field: String,
        constraint: String,
        message: String,
    },
}

impl std::fmt::Display for FieldValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldValidationError::RequiredFieldMissing(name) => {
                write!(f, "Required field '{}' is missing", name)
            }
            FieldValidationError::TypeMismatch {
                field,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "Field '{}' type mismatch: expected {}, got {}",
                    field, expected, actual
                )
            }
            FieldValidationError::ConstraintViolation {
                field,
                constraint,
                message,
            } => {
                write!(
                    f,
                    "Field '{}' constraint {} violated: {}",
                    field, constraint, message
                )
            }
        }
    }
}

impl std::error::Error for FieldValidationError {}

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

fn value_to_comparable(value: &serde_json::Value) -> f64 {
    match value {
        serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0),
        serde_json::Value::String(s) => s.len() as f64,
        serde_json::Value::Array(a) => a.len() as f64,
        _ => 0.0,
    }
}

fn value_length(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::String(s) => s.len(),
        serde_json::Value::Array(a) => a.len(),
        serde_json::Value::Object(o) => o.len(),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_schema_field_required() {
        let field = SchemaField::required("id", FieldType::String);
        assert!(field.required);
        assert_eq!(field.name, "id");
    }

    #[test]
    fn test_schema_field_optional() {
        let field = SchemaField::optional("nickname", FieldType::String);
        assert!(!field.required);
    }

    #[test]
    fn test_field_validation_required() {
        let field = SchemaField::required("id", FieldType::String);
        assert!(field.validate(None).is_err());
        assert!(field.validate(Some(&json!("abc"))).is_ok());
    }

    #[test]
    fn test_field_validation_type() {
        let field = SchemaField::required("age", FieldType::Integer);
        assert!(field.validate(Some(&json!(25))).is_ok());
        assert!(field.validate(Some(&json!("25"))).is_err());
    }

    #[test]
    fn test_field_default_parse() {
        assert!(matches!(
            FieldDefault::parse("true", &FieldType::Boolean),
            Some(FieldDefault::Bool(true))
        ));
        assert!(matches!(
            FieldDefault::parse("42", &FieldType::Integer),
            Some(FieldDefault::Integer(42))
        ));
        assert!(matches!(
            FieldDefault::parse("NOW()", &FieldType::Timestamp),
            Some(FieldDefault::Expression(_))
        ));
    }

    #[test]
    fn test_constraint_min_max() {
        let min = FieldConstraint::Min(0.0);
        let max = FieldConstraint::Max(100.0);

        assert!(min.validate("age", &json!(25)).is_ok());
        assert!(min.validate("age", &json!(-5)).is_err());
        assert!(max.validate("age", &json!(50)).is_ok());
        assert!(max.validate("age", &json!(150)).is_err());
    }

    #[test]
    fn test_constraint_enum() {
        let constraint = FieldConstraint::Enum(vec![json!("active"), json!("inactive")]);

        assert!(constraint.validate("status", &json!("active")).is_ok());
        assert!(constraint.validate("status", &json!("deleted")).is_err());
    }
}
