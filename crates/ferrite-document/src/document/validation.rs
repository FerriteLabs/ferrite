//! JSON Schema validation for documents

use std::collections::HashMap;

use super::DocumentStoreError;

/// Validation level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ValidationLevel {
    /// No validation
    Off,
    /// Validate on insert and update
    #[default]
    Strict,
    /// Validate only existing documents
    Moderate,
}

/// JSON Schema for document validation
#[derive(Debug, Clone)]
pub struct JsonSchema {
    /// Schema type
    pub schema_type: SchemaType,
    /// Required fields
    pub required: Vec<String>,
    /// Properties
    pub properties: HashMap<String, PropertySchema>,
    /// Additional properties allowed
    pub additional_properties: bool,
    /// Minimum properties
    pub min_properties: Option<usize>,
    /// Maximum properties
    pub max_properties: Option<usize>,
    /// All of schemas
    pub all_of: Option<Vec<JsonSchema>>,
    /// Any of schemas
    pub any_of: Option<Vec<JsonSchema>>,
    /// One of schemas
    pub one_of: Option<Vec<JsonSchema>>,
    /// Not schema
    pub not: Option<Box<JsonSchema>>,
}

/// Schema types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaType {
    /// Any type
    Any,
    /// Object
    Object,
    /// Array
    Array,
    /// String
    String,
    /// Number
    Number,
    /// Integer
    Integer,
    /// Boolean
    Boolean,
    /// Null
    Null,
    /// Multiple types
    Multiple(Vec<SchemaType>),
}

/// Property schema
#[derive(Debug, Clone)]
pub struct PropertySchema {
    /// Property type
    pub schema_type: SchemaType,
    /// Description
    pub description: Option<String>,
    /// Default value
    pub default: Option<serde_json::Value>,
    /// Enum values
    pub enum_values: Option<Vec<serde_json::Value>>,
    /// Constant value
    pub const_value: Option<serde_json::Value>,

    // String validations
    /// Minimum length
    pub min_length: Option<usize>,
    /// Maximum length
    pub max_length: Option<usize>,
    /// Pattern (regex)
    pub pattern: Option<String>,
    /// Format
    pub format: Option<StringFormat>,

    // Number validations
    /// Minimum value
    pub minimum: Option<f64>,
    /// Maximum value
    pub maximum: Option<f64>,
    /// Exclusive minimum
    pub exclusive_minimum: Option<f64>,
    /// Exclusive maximum
    pub exclusive_maximum: Option<f64>,
    /// Multiple of
    pub multiple_of: Option<f64>,

    // Array validations
    /// Items schema
    pub items: Option<Box<PropertySchema>>,
    /// Minimum items
    pub min_items: Option<usize>,
    /// Maximum items
    pub max_items: Option<usize>,
    /// Unique items
    pub unique_items: bool,

    // Object validations (nested)
    /// Nested properties
    pub properties: HashMap<String, PropertySchema>,
    /// Required nested fields
    pub required: Vec<String>,
    /// Additional properties
    pub additional_properties: bool,
}

/// String formats
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StringFormat {
    /// Email address
    Email,
    /// URI
    Uri,
    /// UUID
    Uuid,
    /// Date (ISO 8601)
    Date,
    /// Time (ISO 8601)
    Time,
    /// Date-time (ISO 8601)
    DateTime,
    /// IPv4 address
    Ipv4,
    /// IPv6 address
    Ipv6,
    /// Hostname
    Hostname,
    /// Phone number
    Phone,
    /// Custom regex pattern
    Regex(String),
}

impl JsonSchema {
    /// Create a new schema for an object type
    pub fn object() -> Self {
        Self {
            schema_type: SchemaType::Object,
            required: Vec::new(),
            properties: HashMap::new(),
            additional_properties: true,
            min_properties: None,
            max_properties: None,
            all_of: None,
            any_of: None,
            one_of: None,
            not: None,
        }
    }

    /// Parse schema from JSON
    pub fn from_json(value: serde_json::Value) -> Result<Self, DocumentStoreError> {
        let obj = value.as_object().ok_or_else(|| {
            DocumentStoreError::ValidationError("Schema must be an object".into())
        })?;

        let schema_type = Self::parse_type(obj.get("type"))?;

        let required = obj
            .get("required")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let properties = obj
            .get("properties")
            .and_then(|v| v.as_object())
            .map(|props| {
                props
                    .iter()
                    .filter_map(|(k, v)| {
                        PropertySchema::from_json(v.clone())
                            .ok()
                            .map(|s| (k.clone(), s))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let additional_properties = obj
            .get("additionalProperties")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        let min_properties = obj
            .get("minProperties")
            .and_then(|v| v.as_u64())
            .map(|n| n as usize);

        let max_properties = obj
            .get("maxProperties")
            .and_then(|v| v.as_u64())
            .map(|n| n as usize);

        Ok(Self {
            schema_type,
            required,
            properties,
            additional_properties,
            min_properties,
            max_properties,
            all_of: None,
            any_of: None,
            one_of: None,
            not: None,
        })
    }

    /// Parse type from JSON
    fn parse_type(value: Option<&serde_json::Value>) -> Result<SchemaType, DocumentStoreError> {
        match value {
            None => Ok(SchemaType::Any),
            Some(serde_json::Value::String(s)) => Self::parse_type_string(s),
            Some(serde_json::Value::Array(arr)) => {
                let types: Result<Vec<_>, _> = arr
                    .iter()
                    .filter_map(|v| v.as_str())
                    .map(Self::parse_type_string)
                    .collect();
                Ok(SchemaType::Multiple(types?))
            }
            _ => Err(DocumentStoreError::ValidationError(
                "type must be a string or array".into(),
            )),
        }
    }

    /// Parse type string
    fn parse_type_string(s: &str) -> Result<SchemaType, DocumentStoreError> {
        match s {
            "object" => Ok(SchemaType::Object),
            "array" => Ok(SchemaType::Array),
            "string" => Ok(SchemaType::String),
            "number" => Ok(SchemaType::Number),
            "integer" => Ok(SchemaType::Integer),
            "boolean" => Ok(SchemaType::Boolean),
            "null" => Ok(SchemaType::Null),
            _ => Err(DocumentStoreError::ValidationError(format!(
                "Unknown type: {}",
                s
            ))),
        }
    }

    /// Add a required field
    pub fn required_field(mut self, field: &str) -> Self {
        self.required.push(field.to_string());
        self
    }

    /// Add a property
    pub fn property(mut self, name: &str, schema: PropertySchema) -> Self {
        self.properties.insert(name.to_string(), schema);
        self
    }

    /// Disallow additional properties
    pub fn no_additional_properties(mut self) -> Self {
        self.additional_properties = false;
        self
    }

    /// Validate a document
    pub fn validate(&self, value: &serde_json::Value) -> Result<(), DocumentStoreError> {
        // Validate type
        self.validate_type(value)?;

        // If it's an object, validate properties
        if let serde_json::Value::Object(obj) = value {
            self.validate_object(obj)?;
        }

        Ok(())
    }

    /// Validate value type
    fn validate_type(&self, value: &serde_json::Value) -> Result<(), DocumentStoreError> {
        let valid = match &self.schema_type {
            SchemaType::Any => true,
            SchemaType::Object => value.is_object(),
            SchemaType::Array => value.is_array(),
            SchemaType::String => value.is_string(),
            SchemaType::Number => value.is_number(),
            SchemaType::Integer => value.as_i64().is_some(),
            SchemaType::Boolean => value.is_boolean(),
            SchemaType::Null => value.is_null(),
            SchemaType::Multiple(types) => types.iter().any(|t| {
                let temp_schema = JsonSchema {
                    schema_type: t.clone(),
                    ..JsonSchema::object()
                };
                temp_schema.validate_type(value).is_ok()
            }),
        };

        if !valid {
            return Err(DocumentStoreError::ValidationError(format!(
                "Expected type {:?}, got {:?}",
                self.schema_type, value
            )));
        }

        Ok(())
    }

    /// Validate object properties
    fn validate_object(
        &self,
        obj: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<(), DocumentStoreError> {
        // Check required fields
        for field in &self.required {
            if !obj.contains_key(field) {
                return Err(DocumentStoreError::ValidationError(format!(
                    "Missing required field: {}",
                    field
                )));
            }
        }

        // Check property count
        if let Some(min) = self.min_properties {
            if obj.len() < min {
                return Err(DocumentStoreError::ValidationError(format!(
                    "Object has {} properties, minimum is {}",
                    obj.len(),
                    min
                )));
            }
        }

        if let Some(max) = self.max_properties {
            if obj.len() > max {
                return Err(DocumentStoreError::ValidationError(format!(
                    "Object has {} properties, maximum is {}",
                    obj.len(),
                    max
                )));
            }
        }

        // Validate each property
        for (key, value) in obj {
            if let Some(prop_schema) = self.properties.get(key) {
                prop_schema.validate(value)?;
            } else if !self.additional_properties {
                return Err(DocumentStoreError::ValidationError(format!(
                    "Additional property not allowed: {}",
                    key
                )));
            }
        }

        Ok(())
    }
}

impl PropertySchema {
    /// Create a string property schema
    pub fn string() -> Self {
        Self {
            schema_type: SchemaType::String,
            description: None,
            default: None,
            enum_values: None,
            const_value: None,
            min_length: None,
            max_length: None,
            pattern: None,
            format: None,
            minimum: None,
            maximum: None,
            exclusive_minimum: None,
            exclusive_maximum: None,
            multiple_of: None,
            items: None,
            min_items: None,
            max_items: None,
            unique_items: false,
            properties: HashMap::new(),
            required: Vec::new(),
            additional_properties: true,
        }
    }

    /// Create a number property schema
    pub fn number() -> Self {
        Self {
            schema_type: SchemaType::Number,
            ..Self::string()
        }
    }

    /// Create an integer property schema
    pub fn integer() -> Self {
        Self {
            schema_type: SchemaType::Integer,
            ..Self::string()
        }
    }

    /// Create a boolean property schema
    pub fn boolean() -> Self {
        Self {
            schema_type: SchemaType::Boolean,
            ..Self::string()
        }
    }

    /// Create an array property schema
    pub fn array() -> Self {
        Self {
            schema_type: SchemaType::Array,
            ..Self::string()
        }
    }

    /// Create an object property schema
    pub fn object() -> Self {
        Self {
            schema_type: SchemaType::Object,
            ..Self::string()
        }
    }

    /// Parse from JSON
    pub fn from_json(value: serde_json::Value) -> Result<Self, DocumentStoreError> {
        let obj = value.as_object().ok_or_else(|| {
            DocumentStoreError::ValidationError("Property schema must be an object".into())
        })?;

        let schema_type = obj
            .get("type")
            .and_then(|v| v.as_str())
            .map(|s| match s {
                "string" => SchemaType::String,
                "number" => SchemaType::Number,
                "integer" => SchemaType::Integer,
                "boolean" => SchemaType::Boolean,
                "array" => SchemaType::Array,
                "object" => SchemaType::Object,
                "null" => SchemaType::Null,
                _ => SchemaType::Any,
            })
            .unwrap_or(SchemaType::Any);

        let description = obj
            .get("description")
            .and_then(|v| v.as_str())
            .map(String::from);

        let default = obj.get("default").cloned();

        let enum_values = obj
            .get("enum")
            .and_then(|v| v.as_array())
            .cloned();

        let const_value = obj.get("const").cloned();

        let min_length = obj
            .get("minLength")
            .and_then(|v| v.as_u64())
            .map(|n| n as usize);
        let max_length = obj
            .get("maxLength")
            .and_then(|v| v.as_u64())
            .map(|n| n as usize);
        let pattern = obj
            .get("pattern")
            .and_then(|v| v.as_str())
            .map(String::from);

        let format = obj.get("format").and_then(|v| v.as_str()).map(|s| match s {
            "email" => StringFormat::Email,
            "uri" => StringFormat::Uri,
            "uuid" => StringFormat::Uuid,
            "date" => StringFormat::Date,
            "time" => StringFormat::Time,
            "date-time" => StringFormat::DateTime,
            "ipv4" => StringFormat::Ipv4,
            "ipv6" => StringFormat::Ipv6,
            "hostname" => StringFormat::Hostname,
            "phone" => StringFormat::Phone,
            _ => StringFormat::Regex(s.to_string()),
        });

        let minimum = obj.get("minimum").and_then(|v| v.as_f64());
        let maximum = obj.get("maximum").and_then(|v| v.as_f64());
        let exclusive_minimum = obj.get("exclusiveMinimum").and_then(|v| v.as_f64());
        let exclusive_maximum = obj.get("exclusiveMaximum").and_then(|v| v.as_f64());
        let multiple_of = obj.get("multipleOf").and_then(|v| v.as_f64());

        let min_items = obj
            .get("minItems")
            .and_then(|v| v.as_u64())
            .map(|n| n as usize);
        let max_items = obj
            .get("maxItems")
            .and_then(|v| v.as_u64())
            .map(|n| n as usize);
        let unique_items = obj
            .get("uniqueItems")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let items = obj
            .get("items")
            .map(|v| PropertySchema::from_json(v.clone()))
            .transpose()?
            .map(Box::new);

        Ok(Self {
            schema_type,
            description,
            default,
            enum_values,
            const_value,
            min_length,
            max_length,
            pattern,
            format,
            minimum,
            maximum,
            exclusive_minimum,
            exclusive_maximum,
            multiple_of,
            items,
            min_items,
            max_items,
            unique_items,
            properties: HashMap::new(),
            required: Vec::new(),
            additional_properties: true,
        })
    }

    /// Set description
    pub fn description(mut self, desc: &str) -> Self {
        self.description = Some(desc.to_string());
        self
    }

    /// Set minimum length (for strings)
    pub fn min_length(mut self, len: usize) -> Self {
        self.min_length = Some(len);
        self
    }

    /// Set maximum length (for strings)
    pub fn max_length(mut self, len: usize) -> Self {
        self.max_length = Some(len);
        self
    }

    /// Set pattern (for strings)
    pub fn pattern(mut self, pattern: &str) -> Self {
        self.pattern = Some(pattern.to_string());
        self
    }

    /// Set format (for strings)
    pub fn format(mut self, format: StringFormat) -> Self {
        self.format = Some(format);
        self
    }

    /// Set minimum value (for numbers)
    pub fn minimum(mut self, min: f64) -> Self {
        self.minimum = Some(min);
        self
    }

    /// Set maximum value (for numbers)
    pub fn maximum(mut self, max: f64) -> Self {
        self.maximum = Some(max);
        self
    }

    /// Set enum values
    pub fn enum_values(mut self, values: Vec<serde_json::Value>) -> Self {
        self.enum_values = Some(values);
        self
    }

    /// Set items schema (for arrays)
    pub fn items(mut self, schema: PropertySchema) -> Self {
        self.items = Some(Box::new(schema));
        self
    }

    /// Set minimum items (for arrays)
    pub fn min_items(mut self, count: usize) -> Self {
        self.min_items = Some(count);
        self
    }

    /// Set maximum items (for arrays)
    pub fn max_items(mut self, count: usize) -> Self {
        self.max_items = Some(count);
        self
    }

    /// Set unique items (for arrays)
    pub fn unique_items(mut self) -> Self {
        self.unique_items = true;
        self
    }

    /// Validate a value against this schema
    pub fn validate(&self, value: &serde_json::Value) -> Result<(), DocumentStoreError> {
        // Validate type
        self.validate_type(value)?;

        // Validate enum
        if let Some(ref enum_values) = self.enum_values {
            if !enum_values.contains(value) {
                return Err(DocumentStoreError::ValidationError(format!(
                    "Value must be one of: {:?}",
                    enum_values
                )));
            }
        }

        // Validate const
        if let Some(ref const_value) = self.const_value {
            if value != const_value {
                return Err(DocumentStoreError::ValidationError(format!(
                    "Value must be: {:?}",
                    const_value
                )));
            }
        }

        // Type-specific validations
        match value {
            serde_json::Value::String(s) => self.validate_string(s)?,
            serde_json::Value::Number(n) => self.validate_number(n)?,
            serde_json::Value::Array(arr) => self.validate_array(arr)?,
            _ => {}
        }

        Ok(())
    }

    /// Validate type
    fn validate_type(&self, value: &serde_json::Value) -> Result<(), DocumentStoreError> {
        let valid = match &self.schema_type {
            SchemaType::Any => true,
            SchemaType::String => value.is_string(),
            SchemaType::Number => value.is_number(),
            SchemaType::Integer => value.as_i64().is_some(),
            SchemaType::Boolean => value.is_boolean(),
            SchemaType::Array => value.is_array(),
            SchemaType::Object => value.is_object(),
            SchemaType::Null => value.is_null(),
            SchemaType::Multiple(types) => types.iter().any(|t| {
                let temp = PropertySchema {
                    schema_type: t.clone(),
                    ..PropertySchema::string()
                };
                temp.validate_type(value).is_ok()
            }),
        };

        if !valid {
            return Err(DocumentStoreError::ValidationError(format!(
                "Expected type {:?}",
                self.schema_type
            )));
        }

        Ok(())
    }

    /// Validate string
    fn validate_string(&self, s: &str) -> Result<(), DocumentStoreError> {
        if let Some(min) = self.min_length {
            if s.len() < min {
                return Err(DocumentStoreError::ValidationError(format!(
                    "String length {} is less than minimum {}",
                    s.len(),
                    min
                )));
            }
        }

        if let Some(max) = self.max_length {
            if s.len() > max {
                return Err(DocumentStoreError::ValidationError(format!(
                    "String length {} exceeds maximum {}",
                    s.len(),
                    max
                )));
            }
        }

        if let Some(ref pattern) = self.pattern {
            let re = regex::Regex::new(pattern).map_err(|_| {
                DocumentStoreError::ValidationError(format!("Invalid pattern: {}", pattern))
            })?;
            if !re.is_match(s) {
                return Err(DocumentStoreError::ValidationError(format!(
                    "String does not match pattern: {}",
                    pattern
                )));
            }
        }

        if let Some(ref format) = self.format {
            self.validate_format(s, format)?;
        }

        Ok(())
    }

    /// Validate string format
    fn validate_format(&self, s: &str, format: &StringFormat) -> Result<(), DocumentStoreError> {
        let valid = match format {
            StringFormat::Email => s.contains('@') && s.contains('.') && s.len() >= 5,
            StringFormat::Uri => {
                s.starts_with("http://") || s.starts_with("https://") || s.starts_with("ftp://")
            }
            StringFormat::Uuid => {
                s.len() == 36
                    && s.chars().enumerate().all(|(i, c)| {
                        if i == 8 || i == 13 || i == 18 || i == 23 {
                            c == '-'
                        } else {
                            c.is_ascii_hexdigit()
                        }
                    })
            }
            StringFormat::Date => {
                // YYYY-MM-DD
                s.len() == 10 && s.chars().nth(4) == Some('-') && s.chars().nth(7) == Some('-')
            }
            StringFormat::Time => {
                // HH:MM:SS
                s.len() >= 8 && s.chars().nth(2) == Some(':') && s.chars().nth(5) == Some(':')
            }
            StringFormat::DateTime => s.len() >= 19 && (s.contains('T') || s.contains(' ')),
            StringFormat::Ipv4 => {
                let parts: Vec<&str> = s.split('.').collect();
                parts.len() == 4 && parts.iter().all(|p| p.parse::<u8>().is_ok())
            }
            StringFormat::Ipv6 => {
                s.contains(':') && s.chars().all(|c| c.is_ascii_hexdigit() || c == ':')
            }
            StringFormat::Hostname => {
                !s.is_empty()
                    && s.chars()
                        .all(|c| c.is_alphanumeric() || c == '-' || c == '.')
            }
            StringFormat::Phone => s.chars().filter(|c| c.is_ascii_digit()).count() >= 10,
            StringFormat::Regex(pattern) => {
                let re = regex::Regex::new(pattern).map_err(|_| {
                    DocumentStoreError::ValidationError(format!("Invalid regex: {}", pattern))
                })?;
                re.is_match(s)
            }
        };

        if !valid {
            return Err(DocumentStoreError::ValidationError(format!(
                "String does not match format: {:?}",
                format
            )));
        }

        Ok(())
    }

    /// Validate number
    fn validate_number(&self, n: &serde_json::Number) -> Result<(), DocumentStoreError> {
        let value = n.as_f64().unwrap_or(0.0);

        if let Some(min) = self.minimum {
            if value < min {
                return Err(DocumentStoreError::ValidationError(format!(
                    "Value {} is less than minimum {}",
                    value, min
                )));
            }
        }

        if let Some(max) = self.maximum {
            if value > max {
                return Err(DocumentStoreError::ValidationError(format!(
                    "Value {} exceeds maximum {}",
                    value, max
                )));
            }
        }

        if let Some(exc_min) = self.exclusive_minimum {
            if value <= exc_min {
                return Err(DocumentStoreError::ValidationError(format!(
                    "Value {} must be greater than {}",
                    value, exc_min
                )));
            }
        }

        if let Some(exc_max) = self.exclusive_maximum {
            if value >= exc_max {
                return Err(DocumentStoreError::ValidationError(format!(
                    "Value {} must be less than {}",
                    value, exc_max
                )));
            }
        }

        if let Some(mult) = self.multiple_of {
            if (value / mult).fract() != 0.0 {
                return Err(DocumentStoreError::ValidationError(format!(
                    "Value {} is not a multiple of {}",
                    value, mult
                )));
            }
        }

        Ok(())
    }

    /// Validate array
    fn validate_array(&self, arr: &[serde_json::Value]) -> Result<(), DocumentStoreError> {
        if let Some(min) = self.min_items {
            if arr.len() < min {
                return Err(DocumentStoreError::ValidationError(format!(
                    "Array has {} items, minimum is {}",
                    arr.len(),
                    min
                )));
            }
        }

        if let Some(max) = self.max_items {
            if arr.len() > max {
                return Err(DocumentStoreError::ValidationError(format!(
                    "Array has {} items, maximum is {}",
                    arr.len(),
                    max
                )));
            }
        }

        if self.unique_items {
            let mut seen = std::collections::HashSet::new();
            for item in arr {
                let key = serde_json::to_string(item).unwrap_or_default();
                if !seen.insert(key) {
                    return Err(DocumentStoreError::ValidationError(
                        "Array contains duplicate items".into(),
                    ));
                }
            }
        }

        if let Some(ref items_schema) = self.items {
            for item in arr {
                items_schema.validate(item)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_required_fields() {
        let schema = JsonSchema::object()
            .required_field("name")
            .required_field("email");

        let valid = json!({ "name": "Alice", "email": "alice@example.com" });
        assert!(schema.validate(&valid).is_ok());

        let invalid = json!({ "name": "Alice" });
        assert!(schema.validate(&invalid).is_err());
    }

    #[test]
    fn test_string_validation() {
        let schema = JsonSchema::object().property(
            "username",
            PropertySchema::string()
                .min_length(3)
                .max_length(20)
                .pattern("^[a-z]+$"),
        );

        let valid = json!({ "username": "alice" });
        assert!(schema.validate(&valid).is_ok());

        let too_short = json!({ "username": "ab" });
        assert!(schema.validate(&too_short).is_err());

        let invalid_pattern = json!({ "username": "Alice123" });
        assert!(schema.validate(&invalid_pattern).is_err());
    }

    #[test]
    fn test_number_validation() {
        let schema = JsonSchema::object()
            .property("age", PropertySchema::integer().minimum(0.0).maximum(150.0));

        let valid = json!({ "age": 30 });
        assert!(schema.validate(&valid).is_ok());

        let invalid = json!({ "age": -5 });
        assert!(schema.validate(&invalid).is_err());
    }

    #[test]
    fn test_array_validation() {
        let schema = JsonSchema::object().property(
            "tags",
            PropertySchema::array()
                .min_items(1)
                .max_items(5)
                .items(PropertySchema::string())
                .unique_items(),
        );

        let valid = json!({ "tags": ["rust", "database"] });
        assert!(schema.validate(&valid).is_ok());

        let empty = json!({ "tags": [] });
        assert!(schema.validate(&empty).is_err());

        let duplicates = json!({ "tags": ["rust", "rust"] });
        assert!(schema.validate(&duplicates).is_err());
    }

    #[test]
    fn test_enum_validation() {
        let schema = JsonSchema::object().property(
            "status",
            PropertySchema::string().enum_values(vec![
                json!("active"),
                json!("inactive"),
                json!("pending"),
            ]),
        );

        let valid = json!({ "status": "active" });
        assert!(schema.validate(&valid).is_ok());

        let invalid = json!({ "status": "unknown" });
        assert!(schema.validate(&invalid).is_err());
    }

    #[test]
    fn test_email_format() {
        let schema = JsonSchema::object().property(
            "email",
            PropertySchema::string().format(StringFormat::Email),
        );

        let valid = json!({ "email": "alice@example.com" });
        assert!(schema.validate(&valid).is_ok());

        let invalid = json!({ "email": "not-an-email" });
        assert!(schema.validate(&invalid).is_err());
    }

    #[test]
    fn test_no_additional_properties() {
        let schema = JsonSchema::object()
            .property("name", PropertySchema::string())
            .no_additional_properties();

        let valid = json!({ "name": "Alice" });
        assert!(schema.validate(&valid).is_ok());

        let invalid = json!({ "name": "Alice", "extra": "field" });
        assert!(schema.validate(&invalid).is_err());
    }

    #[test]
    fn test_from_json() {
        let schema_json = json!({
            "type": "object",
            "required": ["name", "age"],
            "properties": {
                "name": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 100
                },
                "age": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 150
                }
            },
            "additionalProperties": false
        });

        let schema = JsonSchema::from_json(schema_json).unwrap();

        let valid = json!({ "name": "Alice", "age": 30 });
        assert!(schema.validate(&valid).is_ok());

        let missing_required = json!({ "name": "Alice" });
        assert!(schema.validate(&missing_required).is_err());
    }
}
