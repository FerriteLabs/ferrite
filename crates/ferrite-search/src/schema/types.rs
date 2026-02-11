//! Schema field types

use serde::{Deserialize, Serialize};

/// Field data type
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    /// Text/string data
    String,
    /// 64-bit signed integer
    Integer,
    /// 64-bit floating point
    Float,
    /// Boolean true/false
    Boolean,
    /// Unix timestamp or ISO8601 datetime
    Timestamp,
    /// Array of values
    Array(Box<FieldType>),
    /// Nested object with its own schema
    Object(Option<String>), // Optional schema name for the object
    /// Raw binary data
    Binary,
    /// Any type (no constraint)
    #[default]
    Any,
    /// Null value
    Null,
    /// Union of multiple types
    Union(Vec<FieldType>),
}

impl FieldType {
    /// Parse a field type from string
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim().to_lowercase();
        match s.as_str() {
            "string" | "str" | "text" => Some(FieldType::String),
            "integer" | "int" | "i64" => Some(FieldType::Integer),
            "float" | "double" | "f64" | "number" => Some(FieldType::Float),
            "boolean" | "bool" => Some(FieldType::Boolean),
            "timestamp" | "datetime" | "date" => Some(FieldType::Timestamp),
            "binary" | "bytes" | "blob" => Some(FieldType::Binary),
            "any" | "*" => Some(FieldType::Any),
            "null" | "nil" => Some(FieldType::Null),
            _ => {
                // Check for array types: array<string>, [string]
                if s.starts_with("array<") && s.ends_with('>') {
                    let inner = &s[6..s.len() - 1];
                    FieldType::parse(inner).map(|t| FieldType::Array(Box::new(t)))
                } else if s.starts_with('[') && s.ends_with(']') {
                    let inner = &s[1..s.len() - 1];
                    FieldType::parse(inner).map(|t| FieldType::Array(Box::new(t)))
                } else if s.starts_with("object") {
                    // object or object<schema_name>
                    if s == "object" {
                        Some(FieldType::Object(None))
                    } else if s.starts_with("object<") && s.ends_with('>') {
                        let schema_name = &s[7..s.len() - 1];
                        Some(FieldType::Object(Some(schema_name.to_string())))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        }
    }

    /// Convert to string representation
    pub fn to_type_string(&self) -> String {
        match self {
            FieldType::String => "string".to_string(),
            FieldType::Integer => "integer".to_string(),
            FieldType::Float => "float".to_string(),
            FieldType::Boolean => "boolean".to_string(),
            FieldType::Timestamp => "timestamp".to_string(),
            FieldType::Array(inner) => format!("array<{}>", inner.to_type_string()),
            FieldType::Object(None) => "object".to_string(),
            FieldType::Object(Some(name)) => format!("object<{}>", name),
            FieldType::Binary => "binary".to_string(),
            FieldType::Any => "any".to_string(),
            FieldType::Null => "null".to_string(),
            FieldType::Union(types) => {
                let parts: Vec<_> = types.iter().map(|t| t.to_type_string()).collect();
                parts.join("|")
            }
        }
    }

    /// Check if this type is compatible with another (for evolution)
    pub fn is_compatible_with(&self, other: &FieldType) -> bool {
        match (self, other) {
            // Same types are always compatible
            (a, b) if a == b => true,

            // Any is compatible with everything
            (FieldType::Any, _) | (_, FieldType::Any) => true,

            // Integer can be widened to Float
            (FieldType::Float, FieldType::Integer) => true,

            // String can accept any type (serialization)
            (FieldType::String, _) => true,

            // Arrays are compatible if element types are compatible
            (FieldType::Array(a), FieldType::Array(b)) => a.is_compatible_with(b),

            // Objects are compatible if both are objects
            (FieldType::Object(_), FieldType::Object(_)) => true,

            // Union types
            (FieldType::Union(types), other) => types.iter().any(|t| t.is_compatible_with(other)),
            (other, FieldType::Union(types)) => types.iter().any(|t| other.is_compatible_with(t)),

            _ => false,
        }
    }

    /// Check if a JSON value matches this type
    pub fn matches_json(&self, value: &serde_json::Value) -> bool {
        match (self, value) {
            (FieldType::String, serde_json::Value::String(_)) => true,
            (FieldType::Integer, serde_json::Value::Number(n)) => n.is_i64() || n.is_u64(),
            (FieldType::Float, serde_json::Value::Number(_)) => true,
            (FieldType::Boolean, serde_json::Value::Bool(_)) => true,
            (FieldType::Timestamp, serde_json::Value::Number(n)) => n.is_i64() || n.is_u64(),
            (FieldType::Timestamp, serde_json::Value::String(_)) => true, // ISO8601
            (FieldType::Array(inner), serde_json::Value::Array(arr)) => {
                arr.iter().all(|v| inner.matches_json(v))
            }
            (FieldType::Object(_), serde_json::Value::Object(_)) => true,
            (FieldType::Binary, serde_json::Value::String(_)) => true, // Base64 encoded
            (FieldType::Any, _) => true,
            (FieldType::Null, serde_json::Value::Null) => true,
            (FieldType::Union(types), value) => types.iter().any(|t| t.matches_json(value)),
            _ => false,
        }
    }

    /// Infer type from a JSON value
    pub fn infer_from_json(value: &serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => FieldType::Null,
            serde_json::Value::Bool(_) => FieldType::Boolean,
            serde_json::Value::Number(n) => {
                if n.is_i64() || n.is_u64() {
                    FieldType::Integer
                } else {
                    FieldType::Float
                }
            }
            serde_json::Value::String(s) => {
                // Try to detect timestamp strings
                if s.contains('T') && (s.contains('Z') || s.contains('+') || s.contains('-')) {
                    // Looks like ISO8601
                    FieldType::Timestamp
                } else {
                    FieldType::String
                }
            }
            serde_json::Value::Array(arr) => {
                if arr.is_empty() {
                    FieldType::Array(Box::new(FieldType::Any))
                } else {
                    // Infer from first element (simplification)
                    let elem_type = FieldType::infer_from_json(&arr[0]);
                    FieldType::Array(Box::new(elem_type))
                }
            }
            serde_json::Value::Object(_) => FieldType::Object(None),
        }
    }
}

impl std::fmt::Display for FieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_type_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_type_parse() {
        assert_eq!(FieldType::parse("string"), Some(FieldType::String));
        assert_eq!(FieldType::parse("integer"), Some(FieldType::Integer));
        assert_eq!(FieldType::parse("int"), Some(FieldType::Integer));
        assert_eq!(FieldType::parse("float"), Some(FieldType::Float));
        assert_eq!(FieldType::parse("boolean"), Some(FieldType::Boolean));
        assert_eq!(FieldType::parse("bool"), Some(FieldType::Boolean));
        assert_eq!(FieldType::parse("any"), Some(FieldType::Any));
    }

    #[test]
    fn test_field_type_parse_array() {
        assert_eq!(
            FieldType::parse("array<string>"),
            Some(FieldType::Array(Box::new(FieldType::String)))
        );
        assert_eq!(
            FieldType::parse("[integer]"),
            Some(FieldType::Array(Box::new(FieldType::Integer)))
        );
    }

    #[test]
    fn test_field_type_compatibility() {
        assert!(FieldType::String.is_compatible_with(&FieldType::String));
        assert!(FieldType::Float.is_compatible_with(&FieldType::Integer));
        assert!(FieldType::String.is_compatible_with(&FieldType::Integer));
        assert!(FieldType::Any.is_compatible_with(&FieldType::String));
        assert!(!FieldType::Integer.is_compatible_with(&FieldType::Boolean));
    }

    #[test]
    fn test_field_type_matches_json() {
        use serde_json::json;

        assert!(FieldType::String.matches_json(&json!("hello")));
        assert!(FieldType::Integer.matches_json(&json!(42)));
        assert!(FieldType::Float.matches_json(&json!(3.14)));
        assert!(FieldType::Boolean.matches_json(&json!(true)));
        assert!(FieldType::Array(Box::new(FieldType::Integer)).matches_json(&json!([1, 2, 3])));
    }

    #[test]
    fn test_field_type_infer() {
        use serde_json::json;

        assert_eq!(
            FieldType::infer_from_json(&json!("hello")),
            FieldType::String
        );
        assert_eq!(FieldType::infer_from_json(&json!(42)), FieldType::Integer);
        assert_eq!(FieldType::infer_from_json(&json!(3.14)), FieldType::Float);
        assert_eq!(FieldType::infer_from_json(&json!(true)), FieldType::Boolean);
        assert_eq!(
            FieldType::infer_from_json(&json!("2024-01-15T10:00:00Z")),
            FieldType::Timestamp
        );
    }
}
