//! Schema inference from existing data

use super::{FieldType, Schema, SchemaField};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Schema inferrer for detecting schema from data patterns
pub struct SchemaInferrer {
    /// Sample size for inference
    sample_size: usize,
    /// Minimum presence ratio to include field
    min_presence_ratio: f64,
    /// Whether to detect nested objects
    detect_nested: bool,
}

impl SchemaInferrer {
    /// Create a new schema inferrer
    pub fn new() -> Self {
        Self {
            sample_size: 1000,
            min_presence_ratio: 0.1,
            detect_nested: true,
        }
    }

    /// Set sample size
    pub fn with_sample_size(mut self, size: usize) -> Self {
        self.sample_size = size;
        self
    }

    /// Set minimum presence ratio
    pub fn with_min_presence(mut self, ratio: f64) -> Self {
        self.min_presence_ratio = ratio;
        self
    }

    /// Enable/disable nested object detection
    pub fn with_nested_detection(mut self, enabled: bool) -> Self {
        self.detect_nested = enabled;
        self
    }

    /// Infer schema from a collection of JSON values
    pub fn infer_from_values(&self, values: &[serde_json::Value]) -> InferredSchema {
        let total = values.len();
        if total == 0 {
            return InferredSchema::empty();
        }

        let mut field_stats: HashMap<String, FieldStats> = HashMap::new();

        // Analyze each value
        for value in values {
            if let serde_json::Value::Object(obj) = value {
                self.analyze_object(obj, &mut field_stats, "");
            }
        }

        // Convert stats to inferred fields
        let fields: Vec<InferredField> = field_stats
            .into_iter()
            .filter(|(_, stats)| stats.presence_ratio(total) >= self.min_presence_ratio)
            .map(|(name, stats)| stats.into_inferred_field(name, total))
            .collect();

        InferredSchema {
            fields,
            sample_count: total,
        }
    }

    /// Analyze a JSON object and update field stats
    fn analyze_object(
        &self,
        obj: &serde_json::Map<String, serde_json::Value>,
        stats: &mut HashMap<String, FieldStats>,
        prefix: &str,
    ) {
        for (key, value) in obj {
            let field_name = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{}.{}", prefix, key)
            };

            let entry = stats
                .entry(field_name.clone())
                .or_default();
            entry.count += 1;

            let inferred_type = FieldType::infer_from_json(value);
            *entry.type_counts.entry(inferred_type.clone()).or_insert(0) += 1;

            // Track value statistics
            match value {
                serde_json::Value::Null => entry.null_count += 1,
                serde_json::Value::String(s) => {
                    entry.update_string_stats(s);
                }
                serde_json::Value::Number(n) => {
                    if let Some(f) = n.as_f64() {
                        entry.update_number_stats(f);
                    }
                }
                serde_json::Value::Array(arr) => {
                    entry.array_lengths.push(arr.len());
                }
                serde_json::Value::Object(nested) if self.detect_nested => {
                    self.analyze_object(nested, stats, &field_name);
                }
                _ => {}
            }
        }
    }

    /// Infer schema from string key-value data
    pub fn infer_from_strings<'a, I>(&self, data: I) -> InferredSchema
    where
        I: Iterator<Item = &'a str>,
    {
        let values: Vec<serde_json::Value> = data
            .filter_map(|s| serde_json::from_str(s).ok())
            .take(self.sample_size)
            .collect();

        self.infer_from_values(&values)
    }
}

impl Default for SchemaInferrer {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics for a single field
#[derive(Default)]
struct FieldStats {
    /// Number of occurrences
    count: usize,
    /// Count of null values
    null_count: usize,
    /// Count per type
    type_counts: HashMap<FieldType, usize>,
    /// Min string length
    min_string_len: Option<usize>,
    /// Max string length
    max_string_len: Option<usize>,
    /// Sample string values
    sample_strings: Vec<String>,
    /// Min numeric value
    min_number: Option<f64>,
    /// Max numeric value
    max_number: Option<f64>,
    /// Array lengths
    array_lengths: Vec<usize>,
}

impl FieldStats {
    fn new() -> Self {
        Self::default()
    }

    fn presence_ratio(&self, total: usize) -> f64 {
        if total == 0 {
            0.0
        } else {
            self.count as f64 / total as f64
        }
    }

    fn update_string_stats(&mut self, s: &str) {
        let len = s.len();
        self.min_string_len = Some(self.min_string_len.map_or(len, |m| m.min(len)));
        self.max_string_len = Some(self.max_string_len.map_or(len, |m| m.max(len)));

        if self.sample_strings.len() < 5 {
            self.sample_strings.push(s.to_string());
        }
    }

    fn update_number_stats(&mut self, n: f64) {
        self.min_number = Some(self.min_number.map_or(n, |m| m.min(n)));
        self.max_number = Some(self.max_number.map_or(n, |m| m.max(n)));
    }

    fn into_inferred_field(self, name: String, total: usize) -> InferredField {
        // Determine the dominant type
        let dominant_type = self
            .type_counts
            .iter()
            .max_by_key(|(_, count)| *count)
            .map(|(t, _)| t.clone())
            .unwrap_or(FieldType::Any);

        // Check if field should be nullable
        let nullable = self.null_count > 0 || self.count < total;

        // Calculate presence ratio
        let presence_ratio = self.presence_ratio(total);

        // Determine if field appears to be required
        let likely_required = presence_ratio > 0.95 && self.null_count == 0;

        InferredField {
            name,
            field_type: dominant_type,
            nullable,
            presence_ratio,
            likely_required,
            type_distribution: self
                .type_counts
                .into_iter()
                .map(|(t, c)| (t.to_type_string(), c as f64 / self.count as f64))
                .collect(),
            sample_values: self.sample_strings,
            min_value: self.min_number,
            max_value: self.max_number,
            min_length: self.min_string_len,
            max_length: self.max_string_len,
            avg_array_length: if self.array_lengths.is_empty() {
                None
            } else {
                Some(
                    self.array_lengths.iter().sum::<usize>() as f64
                        / self.array_lengths.len() as f64,
                )
            },
        }
    }
}

/// An inferred field from data analysis
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InferredField {
    /// Field name
    pub name: String,
    /// Inferred type
    pub field_type: FieldType,
    /// Whether nulls were observed
    pub nullable: bool,
    /// Ratio of records containing this field (0.0-1.0)
    pub presence_ratio: f64,
    /// Whether this field is likely required
    pub likely_required: bool,
    /// Distribution of types observed
    pub type_distribution: HashMap<String, f64>,
    /// Sample values (for strings)
    pub sample_values: Vec<String>,
    /// Minimum numeric value
    pub min_value: Option<f64>,
    /// Maximum numeric value
    pub max_value: Option<f64>,
    /// Minimum string length
    pub min_length: Option<usize>,
    /// Maximum string length
    pub max_length: Option<usize>,
    /// Average array length
    pub avg_array_length: Option<f64>,
}

impl InferredField {
    /// Convert to a SchemaField
    pub fn to_schema_field(&self) -> SchemaField {
        SchemaField {
            name: self.name.clone(),
            field_type: self.field_type.clone(),
            required: self.likely_required,
            default: None,
            description: Some(self.generate_description()),
            constraints: Vec::new(),
            deprecated: false,
        }
    }

    /// Generate a description based on inferred statistics
    fn generate_description(&self) -> String {
        let mut parts = Vec::new();

        parts.push(format!("{:.0}% present", self.presence_ratio * 100.0));

        if self.nullable {
            parts.push("nullable".to_string());
        }

        if let (Some(min), Some(max)) = (self.min_value, self.max_value) {
            parts.push(format!("range: {:.2} - {:.2}", min, max));
        }

        if let (Some(min), Some(max)) = (self.min_length, self.max_length) {
            if min == max {
                parts.push(format!("length: {}", min));
            } else {
                parts.push(format!("length: {} - {}", min, max));
            }
        }

        parts.join(", ")
    }
}

/// Inferred schema from data analysis
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InferredSchema {
    /// Inferred fields
    pub fields: Vec<InferredField>,
    /// Number of samples analyzed
    pub sample_count: usize,
}

impl InferredSchema {
    /// Create an empty inferred schema
    pub fn empty() -> Self {
        Self {
            fields: Vec::new(),
            sample_count: 0,
        }
    }

    /// Get all fields
    pub fn fields(&self) -> &[InferredField] {
        &self.fields
    }

    /// Get a field by name
    pub fn field(&self, name: &str) -> Option<&InferredField> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Convert to a Schema
    pub fn to_schema(&self, name: &str, version: u32) -> Schema {
        let fields: Vec<SchemaField> = self.fields.iter().map(|f| f.to_schema_field()).collect();

        Schema {
            name: name.to_string(),
            version,
            fields,
            key_pattern: None,
            description: Some(format!("Inferred from {} samples", self.sample_count)),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            updated_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            deprecated: false,
            deprecation_message: None,
        }
    }

    /// Compare with an existing schema
    pub fn diff(&self, schema: &Schema) -> SchemaDiff {
        let mut missing_fields = Vec::new();
        let mut extra_fields = Vec::new();
        let mut type_mismatches = Vec::new();

        // Check for missing fields (in schema but not inferred)
        for field in &schema.fields {
            if self.field(&field.name).is_none() {
                missing_fields.push(field.name.clone());
            }
        }

        // Check for extra fields (inferred but not in schema)
        for inferred in &self.fields {
            match schema.field(&inferred.name) {
                None => extra_fields.push(inferred.name.clone()),
                Some(schema_field) => {
                    // Check type compatibility
                    if !inferred
                        .field_type
                        .is_compatible_with(&schema_field.field_type)
                    {
                        type_mismatches.push(TypeMismatch {
                            field: inferred.name.clone(),
                            schema_type: schema_field.field_type.to_type_string(),
                            inferred_type: inferred.field_type.to_type_string(),
                        });
                    }
                }
            }
        }

        SchemaDiff {
            missing_fields,
            extra_fields,
            type_mismatches,
        }
    }
}

/// Difference between inferred and defined schema
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemaDiff {
    /// Fields in schema but not found in data
    pub missing_fields: Vec<String>,
    /// Fields found in data but not in schema
    pub extra_fields: Vec<String>,
    /// Fields with type mismatches
    pub type_mismatches: Vec<TypeMismatch>,
}

impl SchemaDiff {
    /// Check if schemas match
    pub fn matches(&self) -> bool {
        self.missing_fields.is_empty()
            && self.extra_fields.is_empty()
            && self.type_mismatches.is_empty()
    }
}

/// Type mismatch detail
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TypeMismatch {
    /// Field name
    pub field: String,
    /// Type defined in schema
    pub schema_type: String,
    /// Type inferred from data
    pub inferred_type: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_infer_simple_schema() {
        let values = vec![
            json!({"id": "1", "name": "Alice", "age": 30}),
            json!({"id": "2", "name": "Bob", "age": 25}),
            json!({"id": "3", "name": "Charlie", "age": 35}),
        ];

        let inferrer = SchemaInferrer::new();
        let inferred = inferrer.infer_from_values(&values);

        assert_eq!(inferred.sample_count, 3);
        assert_eq!(inferred.fields.len(), 3);

        let id_field = inferred.field("id").unwrap();
        assert_eq!(id_field.field_type, FieldType::String);
        assert!(id_field.likely_required);

        let age_field = inferred.field("age").unwrap();
        assert_eq!(age_field.field_type, FieldType::Integer);
    }

    #[test]
    fn test_infer_nullable_field() {
        let values = vec![
            json!({"id": "1", "email": "a@test.com"}),
            json!({"id": "2", "email": null}),
            json!({"id": "3"}),
        ];

        let inferrer = SchemaInferrer::new().with_min_presence(0.5);
        let inferred = inferrer.infer_from_values(&values);

        let email_field = inferred.field("email").unwrap();
        assert!(email_field.nullable);
        assert!(!email_field.likely_required);
    }

    #[test]
    fn test_infer_nested_object() {
        let values = vec![
            json!({"user": {"name": "Alice", "age": 30}}),
            json!({"user": {"name": "Bob", "age": 25}}),
        ];

        let inferrer = SchemaInferrer::new().with_nested_detection(true);
        let inferred = inferrer.infer_from_values(&values);

        assert!(inferred.field("user.name").is_some());
        assert!(inferred.field("user.age").is_some());
    }

    #[test]
    fn test_schema_diff() {
        let schema = Schema::builder("user")
            .version(1)
            .field("id", FieldType::String, true)
            .field("name", FieldType::String, true)
            .field("legacy_field", FieldType::String, false)
            .build();

        let values = vec![json!({"id": "1", "name": "Alice", "new_field": "value"})];

        let inferrer = SchemaInferrer::new();
        let inferred = inferrer.infer_from_values(&values);

        let diff = inferred.diff(&schema);
        assert!(diff.missing_fields.contains(&"legacy_field".to_string()));
        assert!(diff.extra_fields.contains(&"new_field".to_string()));
    }

    #[test]
    fn test_convert_to_schema() {
        let values = vec![
            json!({"id": "1", "active": true}),
            json!({"id": "2", "active": false}),
        ];

        let inferrer = SchemaInferrer::new();
        let inferred = inferrer.infer_from_values(&values);
        let schema = inferred.to_schema("user", 1);

        assert_eq!(schema.name, "user");
        assert_eq!(schema.version, 1);
        assert!(schema.has_field("id"));
        assert!(schema.has_field("active"));
    }
}
