//! JSON Schema validation (Draft 2020-12 subset)

use std::collections::HashMap;

use serde_json::Value;

use super::DocumentStoreError;

/// A validation error with path context
#[derive(Debug, Clone)]
pub struct ValidationError {
    /// JSON Pointer path to the invalid value
    pub path: String,
    /// Description of the validation failure
    pub message: String,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.path.is_empty() {
            write!(f, "{}", self.message)
        } else {
            write!(f, "at '{}': {}", self.path, self.message)
        }
    }
}

/// Schema validator with support for `$ref` resolution
pub struct SchemaValidator {
    schema: Value,
}

impl SchemaValidator {
    /// Create a new schema validator
    pub fn new(schema: Value) -> Result<Self, DocumentStoreError> {
        if !schema.is_object() && !schema.is_boolean() {
            return Err(DocumentStoreError::ValidationError(
                "Schema must be an object or boolean".into(),
            ));
        }
        Ok(Self { schema })
    }

    /// Validate a document against the schema
    pub fn validate(&self, doc: &Value) -> Result<Vec<ValidationError>, DocumentStoreError> {
        let mut errors = Vec::new();
        validate_value(doc, &self.schema, &self.schema, "", &mut errors);
        Ok(errors)
    }

    /// Check if a document is valid (convenience method)
    pub fn is_valid(&self, doc: &Value) -> Result<bool, DocumentStoreError> {
        Ok(self.validate(doc)?.is_empty())
    }
}

fn validate_value(
    value: &Value,
    schema: &Value,
    root_schema: &Value,
    path: &str,
    errors: &mut Vec<ValidationError>,
) {
    // Boolean schemas
    match schema {
        Value::Bool(true) => return,
        Value::Bool(false) => {
            errors.push(ValidationError {
                path: path.to_string(),
                message: "Value not allowed by schema (false)".into(),
            });
            return;
        }
        _ => {}
    }

    let schema_obj = match schema.as_object() {
        Some(obj) => obj,
        None => return,
    };

    // Handle $ref
    if let Some(ref_val) = schema_obj.get("$ref") {
        if let Some(ref_str) = ref_val.as_str() {
            if let Some(resolved) = resolve_ref(ref_str, root_schema) {
                validate_value(value, resolved, root_schema, path, errors);
                return;
            }
        }
    }

    // Type validation
    if let Some(type_val) = schema_obj.get("type") {
        validate_type(value, type_val, path, errors);
    }

    // Enum validation
    if let Some(Value::Array(enum_vals)) = schema_obj.get("enum") {
        if !enum_vals.contains(value) {
            errors.push(ValidationError {
                path: path.to_string(),
                message: format!("Value must be one of: {:?}", enum_vals),
            });
        }
    }

    // const validation
    if let Some(const_val) = schema_obj.get("const") {
        if value != const_val {
            errors.push(ValidationError {
                path: path.to_string(),
                message: format!("Value must be: {}", const_val),
            });
        }
    }

    // String validations
    if let Value::String(s) = value {
        validate_string(s, schema_obj, path, errors);
    }

    // Number validations
    if let Value::Number(n) = value {
        if let Some(f) = n.as_f64() {
            validate_number(f, schema_obj, path, errors);
        }
    }

    // Array validations
    if let Value::Array(arr) = value {
        validate_array(arr, schema_obj, root_schema, path, errors);
    }

    // Object validations
    if let Value::Object(obj) = value {
        validate_object(obj, schema_obj, root_schema, path, errors);
    }

    // Combinators
    if let Some(Value::Array(all_of)) = schema_obj.get("allOf") {
        for sub in all_of {
            validate_value(value, sub, root_schema, path, errors);
        }
    }

    if let Some(Value::Array(any_of)) = schema_obj.get("anyOf") {
        let any_valid = any_of.iter().any(|sub| {
            let mut sub_errors = Vec::new();
            validate_value(value, sub, root_schema, path, &mut sub_errors);
            sub_errors.is_empty()
        });
        if !any_valid {
            errors.push(ValidationError {
                path: path.to_string(),
                message: "Value does not match any of the 'anyOf' schemas".into(),
            });
        }
    }

    if let Some(Value::Array(one_of)) = schema_obj.get("oneOf") {
        let match_count = one_of
            .iter()
            .filter(|sub| {
                let mut sub_errors = Vec::new();
                validate_value(value, sub, root_schema, path, &mut sub_errors);
                sub_errors.is_empty()
            })
            .count();
        if match_count != 1 {
            errors.push(ValidationError {
                path: path.to_string(),
                message: format!(
                    "Value must match exactly one 'oneOf' schema, matched {}",
                    match_count
                ),
            });
        }
    }

    if let Some(not_schema) = schema_obj.get("not") {
        let mut sub_errors = Vec::new();
        validate_value(value, not_schema, root_schema, path, &mut sub_errors);
        if sub_errors.is_empty() {
            errors.push(ValidationError {
                path: path.to_string(),
                message: "Value must not match the 'not' schema".into(),
            });
        }
    }
}

fn validate_type(value: &Value, type_val: &Value, path: &str, errors: &mut Vec<ValidationError>) {
    let check = |t: &str| -> bool {
        match t {
            "string" => value.is_string(),
            "number" => value.is_number(),
            "integer" => value.is_i64() || value.is_u64(),
            "boolean" => value.is_boolean(),
            "array" => value.is_array(),
            "object" => value.is_object(),
            "null" => value.is_null(),
            _ => true,
        }
    };

    let valid = match type_val {
        Value::String(t) => check(t),
        Value::Array(types) => types
            .iter()
            .any(|t| t.as_str().is_some_and(check)),
        _ => true,
    };

    if !valid {
        errors.push(ValidationError {
            path: path.to_string(),
            message: format!("Expected type {}, got {}", type_val, value_type_name(value)),
        });
    }
}

fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn validate_string(
    s: &str,
    schema: &serde_json::Map<String, Value>,
    path: &str,
    errors: &mut Vec<ValidationError>,
) {
    if let Some(Value::Number(n)) = schema.get("minLength") {
        if let Some(min) = n.as_u64() {
            if (s.len() as u64) < min {
                errors.push(ValidationError {
                    path: path.to_string(),
                    message: format!("String length {} is less than minimum {}", s.len(), min),
                });
            }
        }
    }

    if let Some(Value::Number(n)) = schema.get("maxLength") {
        if let Some(max) = n.as_u64() {
            if (s.len() as u64) > max {
                errors.push(ValidationError {
                    path: path.to_string(),
                    message: format!("String length {} exceeds maximum {}", s.len(), max),
                });
            }
        }
    }

    if let Some(Value::String(pattern)) = schema.get("pattern") {
        if let Ok(re) = regex::Regex::new(pattern) {
            if !re.is_match(s) {
                errors.push(ValidationError {
                    path: path.to_string(),
                    message: format!("String does not match pattern '{}'", pattern),
                });
            }
        }
    }
}

fn validate_number(
    n: f64,
    schema: &serde_json::Map<String, Value>,
    path: &str,
    errors: &mut Vec<ValidationError>,
) {
    if let Some(Value::Number(min)) = schema.get("minimum") {
        if let Some(min_val) = min.as_f64() {
            if n < min_val {
                errors.push(ValidationError {
                    path: path.to_string(),
                    message: format!("{} is less than minimum {}", n, min_val),
                });
            }
        }
    }

    if let Some(Value::Number(max)) = schema.get("maximum") {
        if let Some(max_val) = max.as_f64() {
            if n > max_val {
                errors.push(ValidationError {
                    path: path.to_string(),
                    message: format!("{} exceeds maximum {}", n, max_val),
                });
            }
        }
    }

    if let Some(Value::Number(emin)) = schema.get("exclusiveMinimum") {
        if let Some(min_val) = emin.as_f64() {
            if n <= min_val {
                errors.push(ValidationError {
                    path: path.to_string(),
                    message: format!("{} must be greater than {}", n, min_val),
                });
            }
        }
    }

    if let Some(Value::Number(emax)) = schema.get("exclusiveMaximum") {
        if let Some(max_val) = emax.as_f64() {
            if n >= max_val {
                errors.push(ValidationError {
                    path: path.to_string(),
                    message: format!("{} must be less than {}", n, max_val),
                });
            }
        }
    }

    if let Some(Value::Number(mult)) = schema.get("multipleOf") {
        if let Some(mult_val) = mult.as_f64() {
            if mult_val != 0.0 && (n % mult_val).abs() > f64::EPSILON {
                errors.push(ValidationError {
                    path: path.to_string(),
                    message: format!("{} is not a multiple of {}", n, mult_val),
                });
            }
        }
    }
}

fn validate_array(
    arr: &[Value],
    schema: &serde_json::Map<String, Value>,
    root_schema: &Value,
    path: &str,
    errors: &mut Vec<ValidationError>,
) {
    if let Some(Value::Number(n)) = schema.get("minItems") {
        if let Some(min) = n.as_u64() {
            if (arr.len() as u64) < min {
                errors.push(ValidationError {
                    path: path.to_string(),
                    message: format!("Array has {} items, minimum is {}", arr.len(), min),
                });
            }
        }
    }

    if let Some(Value::Number(n)) = schema.get("maxItems") {
        if let Some(max) = n.as_u64() {
            if (arr.len() as u64) > max {
                errors.push(ValidationError {
                    path: path.to_string(),
                    message: format!("Array has {} items, maximum is {}", arr.len(), max),
                });
            }
        }
    }

    if matches!(schema.get("uniqueItems"), Some(Value::Bool(true))) {
        let mut seen = std::collections::HashSet::new();
        for (i, item) in arr.iter().enumerate() {
            let key = serde_json::to_string(item).unwrap_or_default();
            if !seen.insert(key) {
                errors.push(ValidationError {
                    path: format!("{}/{}", path, i),
                    message: "Duplicate item in array with uniqueItems".into(),
                });
            }
        }
    }

    // Validate items
    if let Some(items_schema) = schema.get("items") {
        for (i, item) in arr.iter().enumerate() {
            let item_path = format!("{}/{}", path, i);
            validate_value(item, items_schema, root_schema, &item_path, errors);
        }
    }

    // Validate prefixItems
    if let Some(Value::Array(prefix)) = schema.get("prefixItems") {
        for (i, item_schema) in prefix.iter().enumerate() {
            if let Some(item) = arr.get(i) {
                let item_path = format!("{}/{}", path, i);
                validate_value(item, item_schema, root_schema, &item_path, errors);
            }
        }
    }
}

fn validate_object(
    obj: &serde_json::Map<String, Value>,
    schema: &serde_json::Map<String, Value>,
    root_schema: &Value,
    path: &str,
    errors: &mut Vec<ValidationError>,
) {
    // Required fields
    if let Some(Value::Array(required)) = schema.get("required") {
        for req in required {
            if let Some(field) = req.as_str() {
                if !obj.contains_key(field) {
                    errors.push(ValidationError {
                        path: path.to_string(),
                        message: format!("Missing required field: {}", field),
                    });
                }
            }
        }
    }

    // minProperties / maxProperties
    if let Some(Value::Number(n)) = schema.get("minProperties") {
        if let Some(min) = n.as_u64() {
            if (obj.len() as u64) < min {
                errors.push(ValidationError {
                    path: path.to_string(),
                    message: format!(
                        "Object has {} properties, minimum is {}",
                        obj.len(),
                        min
                    ),
                });
            }
        }
    }

    if let Some(Value::Number(n)) = schema.get("maxProperties") {
        if let Some(max) = n.as_u64() {
            if (obj.len() as u64) > max {
                errors.push(ValidationError {
                    path: path.to_string(),
                    message: format!(
                        "Object has {} properties, maximum is {}",
                        obj.len(),
                        max
                    ),
                });
            }
        }
    }

    // Properties
    let properties = schema
        .get("properties")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();

    // patternProperties
    let pattern_properties: HashMap<String, Value> = schema
        .get("patternProperties")
        .and_then(|v| v.as_object())
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();

    let additional = schema.get("additionalProperties");

    for (key, value) in obj {
        let prop_path = if path.is_empty() {
            format!("/{}", key)
        } else {
            format!("{}/{}", path, key)
        };

        let mut matched = false;

        // Check defined properties
        if let Some(prop_schema) = properties.get(key) {
            validate_value(value, prop_schema, root_schema, &prop_path, errors);
            matched = true;
        }

        // Check pattern properties
        for (pattern, pat_schema) in &pattern_properties {
            if let Ok(re) = regex::Regex::new(pattern) {
                if re.is_match(key) {
                    validate_value(value, pat_schema, root_schema, &prop_path, errors);
                    matched = true;
                }
            }
        }

        // Check additional properties
        if !matched {
            match additional {
                Some(Value::Bool(false)) => {
                    errors.push(ValidationError {
                        path: prop_path,
                        message: format!("Additional property '{}' not allowed", key),
                    });
                }
                Some(add_schema) if add_schema.is_object() => {
                    validate_value(value, add_schema, root_schema, &prop_path, errors);
                }
                _ => {}
            }
        }
    }
}

/// Resolve a JSON Pointer `$ref` within the root schema
fn resolve_ref<'a>(ref_str: &str, root: &'a Value) -> Option<&'a Value> {
    if !ref_str.starts_with("#/") && ref_str != "#" {
        return None;
    }
    if ref_str == "#" {
        return Some(root);
    }
    let pointer = &ref_str[1..]; // strip leading '#'
    root.pointer(pointer)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_type_validation() {
        let schema = json!({"type": "string"});
        let v = SchemaValidator::new(schema).unwrap();
        assert!(v.validate(&json!("hello")).unwrap().is_empty());
        assert!(!v.validate(&json!(42)).unwrap().is_empty());
    }

    #[test]
    fn test_required_fields() {
        let schema = json!({
            "type": "object",
            "required": ["name", "age"]
        });
        let v = SchemaValidator::new(schema).unwrap();
        assert!(v.validate(&json!({"name": "Alice", "age": 30})).unwrap().is_empty());
        assert!(!v.validate(&json!({"name": "Alice"})).unwrap().is_empty());
    }

    #[test]
    fn test_properties() {
        let schema = json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"}
            }
        });
        let v = SchemaValidator::new(schema).unwrap();
        assert!(v.validate(&json!({"name": "Alice", "age": 30})).unwrap().is_empty());
        assert!(!v.validate(&json!({"name": 42})).unwrap().is_empty());
    }

    #[test]
    fn test_additional_properties_false() {
        let schema = json!({
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "additionalProperties": false
        });
        let v = SchemaValidator::new(schema).unwrap();
        assert!(v.validate(&json!({"name": "Alice"})).unwrap().is_empty());
        assert!(!v.validate(&json!({"name": "Alice", "extra": 1})).unwrap().is_empty());
    }

    #[test]
    fn test_string_min_max_length() {
        let schema = json!({"type": "string", "minLength": 2, "maxLength": 5});
        let v = SchemaValidator::new(schema).unwrap();
        assert!(v.validate(&json!("abc")).unwrap().is_empty());
        assert!(!v.validate(&json!("a")).unwrap().is_empty());
        assert!(!v.validate(&json!("abcdef")).unwrap().is_empty());
    }

    #[test]
    fn test_number_minimum_maximum() {
        let schema = json!({"type": "number", "minimum": 0, "maximum": 100});
        let v = SchemaValidator::new(schema).unwrap();
        assert!(v.validate(&json!(50)).unwrap().is_empty());
        assert!(!v.validate(&json!(-1)).unwrap().is_empty());
        assert!(!v.validate(&json!(101)).unwrap().is_empty());
    }

    #[test]
    fn test_array_min_max_items() {
        let schema = json!({"type": "array", "minItems": 1, "maxItems": 3});
        let v = SchemaValidator::new(schema).unwrap();
        assert!(v.validate(&json!([1, 2])).unwrap().is_empty());
        assert!(!v.validate(&json!([])).unwrap().is_empty());
        assert!(!v.validate(&json!([1, 2, 3, 4])).unwrap().is_empty());
    }

    #[test]
    fn test_array_items() {
        let schema = json!({
            "type": "array",
            "items": {"type": "integer"}
        });
        let v = SchemaValidator::new(schema).unwrap();
        assert!(v.validate(&json!([1, 2, 3])).unwrap().is_empty());
        assert!(!v.validate(&json!([1, "two", 3])).unwrap().is_empty());
    }

    #[test]
    fn test_enum_values() {
        let schema = json!({"enum": ["red", "green", "blue"]});
        let v = SchemaValidator::new(schema).unwrap();
        assert!(v.validate(&json!("red")).unwrap().is_empty());
        assert!(!v.validate(&json!("yellow")).unwrap().is_empty());
    }

    #[test]
    fn test_pattern() {
        let schema = json!({"type": "string", "pattern": "^[a-z]+$"});
        let v = SchemaValidator::new(schema).unwrap();
        assert!(v.validate(&json!("hello")).unwrap().is_empty());
        assert!(!v.validate(&json!("Hello123")).unwrap().is_empty());
    }

    #[test]
    fn test_one_of() {
        let schema = json!({
            "oneOf": [
                {"type": "string"},
                {"type": "integer"}
            ]
        });
        let v = SchemaValidator::new(schema).unwrap();
        assert!(v.validate(&json!("hello")).unwrap().is_empty());
        assert!(v.validate(&json!(42)).unwrap().is_empty());
        assert!(!v.validate(&json!(3.15)).unwrap().is_empty());
    }

    #[test]
    fn test_any_of() {
        let schema = json!({
            "anyOf": [
                {"type": "string", "minLength": 5},
                {"type": "integer", "minimum": 10}
            ]
        });
        let v = SchemaValidator::new(schema).unwrap();
        assert!(v.validate(&json!("hello")).unwrap().is_empty());
        assert!(v.validate(&json!(15)).unwrap().is_empty());
        assert!(!v.validate(&json!(5)).unwrap().is_empty());
    }

    #[test]
    fn test_all_of() {
        let schema = json!({
            "allOf": [
                {"type": "number", "minimum": 0},
                {"maximum": 100}
            ]
        });
        let v = SchemaValidator::new(schema).unwrap();
        assert!(v.validate(&json!(50)).unwrap().is_empty());
        assert!(!v.validate(&json!(-1)).unwrap().is_empty());
        assert!(!v.validate(&json!(101)).unwrap().is_empty());
    }

    #[test]
    fn test_ref() {
        let schema = json!({
            "type": "object",
            "properties": {
                "name": {"$ref": "#/$defs/nameType"}
            },
            "$defs": {
                "nameType": {"type": "string", "minLength": 1}
            }
        });
        let v = SchemaValidator::new(schema).unwrap();
        assert!(v.validate(&json!({"name": "Alice"})).unwrap().is_empty());
        assert!(!v.validate(&json!({"name": ""})).unwrap().is_empty());
    }

    #[test]
    fn test_nested_objects() {
        let schema = json!({
            "type": "object",
            "properties": {
                "address": {
                    "type": "object",
                    "required": ["street"],
                    "properties": {
                        "street": {"type": "string"},
                        "city": {"type": "string"}
                    }
                }
            }
        });
        let v = SchemaValidator::new(schema).unwrap();
        assert!(v
            .validate(&json!({"address": {"street": "123 Main St"}}))
            .unwrap()
            .is_empty());
        assert!(!v.validate(&json!({"address": {}})).unwrap().is_empty());
    }

    #[test]
    fn test_boolean_schema() {
        let v_true = SchemaValidator::new(json!(true)).unwrap();
        assert!(v_true.validate(&json!("anything")).unwrap().is_empty());

        let v_false = SchemaValidator::new(json!(false)).unwrap();
        assert!(!v_false.validate(&json!("anything")).unwrap().is_empty());
    }

    #[test]
    fn test_multiple_types() {
        let schema = json!({"type": ["string", "integer"]});
        let v = SchemaValidator::new(schema).unwrap();
        assert!(v.validate(&json!("hello")).unwrap().is_empty());
        assert!(v.validate(&json!(42)).unwrap().is_empty());
        assert!(!v.validate(&json!(3.15)).unwrap().is_empty());
    }

    #[test]
    fn test_is_valid() {
        let v = SchemaValidator::new(json!({"type": "string"})).unwrap();
        assert!(v.is_valid(&json!("hello")).unwrap());
        assert!(!v.is_valid(&json!(42)).unwrap());
    }

    #[test]
    fn test_exclusive_min_max() {
        let schema = json!({"type": "number", "exclusiveMinimum": 0, "exclusiveMaximum": 10});
        let v = SchemaValidator::new(schema).unwrap();
        assert!(v.validate(&json!(5)).unwrap().is_empty());
        assert!(!v.validate(&json!(0)).unwrap().is_empty());
        assert!(!v.validate(&json!(10)).unwrap().is_empty());
    }
}
