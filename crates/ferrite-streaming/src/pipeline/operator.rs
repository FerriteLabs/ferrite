//! Operator implementations for streaming pipelines.

use serde_json::Value;

/// Result of applying an operator to an event.
#[derive(Debug, Clone)]
pub enum OperatorResult {
    /// Pass the event through (possibly transformed).
    Emit(Value),
    /// Emit multiple events (from flatmap).
    EmitMany(Vec<Value>),
    /// Filter out the event.
    Drop,
    /// Operator error.
    Error(String),
}

/// Applies a simple key-based filter.
pub fn apply_filter(event: &Value, field: &str, expected: &Value) -> OperatorResult {
    match event.get(field) {
        Some(value) if value == expected => OperatorResult::Emit(event.clone()),
        _ => OperatorResult::Drop,
    }
}

/// Applies a field extraction/projection.
pub fn apply_projection(event: &Value, fields: &[&str]) -> OperatorResult {
    let mut result = serde_json::Map::new();
    for field in fields {
        if let Some(value) = event.get(*field) {
            result.insert(field.to_string(), value.clone());
        }
    }
    OperatorResult::Emit(Value::Object(result))
}

/// Extracts a numeric value from an event field.
pub fn extract_number(event: &Value, field: &str) -> Option<f64> {
    event.get(field).and_then(|v| v.as_f64())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_filter_match() {
        let event = json!({"type": "click", "page": "/home"});
        let result = apply_filter(&event, "type", &json!("click"));
        assert!(matches!(result, OperatorResult::Emit(_)));
    }

    #[test]
    fn test_filter_no_match() {
        let event = json!({"type": "view", "page": "/home"});
        let result = apply_filter(&event, "type", &json!("click"));
        assert!(matches!(result, OperatorResult::Drop));
    }

    #[test]
    fn test_projection() {
        let event = json!({"name": "Alice", "age": 30, "email": "alice@example.com"});
        let result = apply_projection(&event, &["name", "email"]);
        if let OperatorResult::Emit(projected) = result {
            assert!(projected.get("name").is_some());
            assert!(projected.get("email").is_some());
            assert!(projected.get("age").is_none());
        } else {
            panic!("Expected Emit");
        }
    }

    #[test]
    fn test_extract_number() {
        let event = json!({"count": 42, "name": "test"});
        assert_eq!(extract_number(&event, "count"), Some(42.0));
        assert_eq!(extract_number(&event, "name"), None);
        assert_eq!(extract_number(&event, "missing"), None);
    }
}
