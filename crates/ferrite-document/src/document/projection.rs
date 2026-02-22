//! Field projection for document queries

use std::collections::HashMap;

use super::document::Document;
use super::DocumentStoreError;

/// Projection specification
#[derive(Debug, Clone)]
pub struct Projection {
    /// Fields to include/exclude
    fields: HashMap<String, ProjectionValue>,
    /// Mode (include or exclude)
    mode: ProjectionMode,
}

/// Projection mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectionMode {
    /// Include specified fields only
    Include,
    /// Exclude specified fields
    Exclude,
}

/// Projection value
#[derive(Debug, Clone)]
enum ProjectionValue {
    /// Include field
    Include,
    /// Exclude field
    Exclude,
    /// Slice array
    Slice(SliceSpec),
    /// Element match
    ElemMatch(serde_json::Value),
    /// Meta score
    Meta(#[allow(dead_code)] String), // Planned for v0.2 — stored for projection meta scoring
    /// Positional operator
    Positional,
}

/// Slice specification
#[derive(Debug, Clone)]
enum SliceSpec {
    /// Take first/last n elements
    Single(i32),
    /// Skip and take
    Range(i32, i32),
}

impl Projection {
    /// Create an empty projection
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
            mode: ProjectionMode::Include,
        }
    }

    /// Parse projection from JSON
    pub fn from_json(value: serde_json::Value) -> Result<Self, DocumentStoreError> {
        let obj = value.as_object().ok_or_else(|| {
            DocumentStoreError::InvalidQuery("Projection must be an object".into())
        })?;

        let mut projection = Self::new();
        let mut has_include = false;
        let mut has_exclude = false;

        for (field, val) in obj {
            // Skip _id as it can be mixed with either mode
            if field != "_id" {
                match val {
                    serde_json::Value::Number(n) => {
                        if n.as_i64() == Some(0) {
                            has_exclude = true;
                        } else {
                            has_include = true;
                        }
                    }
                    serde_json::Value::Bool(b) => {
                        if *b {
                            has_include = true;
                        } else {
                            has_exclude = true;
                        }
                    }
                    serde_json::Value::Object(_) => {
                        has_include = true;
                    }
                    _ => {}
                }
            }

            let pv = Self::parse_value(val)?;
            projection.fields.insert(field.clone(), pv);
        }

        // Cannot mix include and exclude (except for _id)
        if has_include && has_exclude {
            return Err(DocumentStoreError::InvalidQuery(
                "Cannot mix include and exclude in projection".into(),
            ));
        }

        projection.mode = if has_exclude {
            ProjectionMode::Exclude
        } else {
            ProjectionMode::Include
        };

        Ok(projection)
    }

    /// Parse a projection value
    fn parse_value(value: &serde_json::Value) -> Result<ProjectionValue, DocumentStoreError> {
        match value {
            serde_json::Value::Number(n) => {
                if n.as_i64() == Some(0) {
                    Ok(ProjectionValue::Exclude)
                } else {
                    Ok(ProjectionValue::Include)
                }
            }
            serde_json::Value::Bool(b) => {
                if *b {
                    Ok(ProjectionValue::Include)
                } else {
                    Ok(ProjectionValue::Exclude)
                }
            }
            serde_json::Value::Object(obj) => {
                if let Some(slice) = obj.get("$slice") {
                    let spec = Self::parse_slice(slice)?;
                    return Ok(ProjectionValue::Slice(spec));
                }
                if let Some(elem_match) = obj.get("$elemMatch") {
                    return Ok(ProjectionValue::ElemMatch(elem_match.clone()));
                }
                if let Some(meta) = obj.get("$meta") {
                    let meta_type = meta.as_str().ok_or_else(|| {
                        DocumentStoreError::InvalidQuery("$meta must be a string".into())
                    })?;
                    return Ok(ProjectionValue::Meta(meta_type.to_string()));
                }
                Err(DocumentStoreError::InvalidQuery(
                    "Unknown projection operator".into(),
                ))
            }
            serde_json::Value::String(s) if s == "$" => Ok(ProjectionValue::Positional),
            _ => Err(DocumentStoreError::InvalidQuery(
                "Invalid projection value".into(),
            )),
        }
    }

    /// Parse slice specification
    fn parse_slice(value: &serde_json::Value) -> Result<SliceSpec, DocumentStoreError> {
        match value {
            serde_json::Value::Number(n) => {
                let count = n.as_i64().ok_or_else(|| {
                    DocumentStoreError::InvalidQuery("$slice must be an integer".into())
                })? as i32;
                Ok(SliceSpec::Single(count))
            }
            serde_json::Value::Array(arr) => {
                if arr.len() != 2 {
                    return Err(DocumentStoreError::InvalidQuery(
                        "$slice array must have 2 elements".into(),
                    ));
                }
                let skip = arr[0].as_i64().ok_or_else(|| {
                    DocumentStoreError::InvalidQuery("$slice skip must be an integer".into())
                })? as i32;
                let take = arr[1].as_i64().ok_or_else(|| {
                    DocumentStoreError::InvalidQuery("$slice take must be an integer".into())
                })? as i32;
                Ok(SliceSpec::Range(skip, take))
            }
            _ => Err(DocumentStoreError::InvalidQuery(
                "$slice must be a number or array".into(),
            )),
        }
    }

    /// Apply projection to a document
    pub fn apply(&self, doc: &Document) -> Result<Document, DocumentStoreError> {
        let mut result = serde_json::Map::new();

        match self.mode {
            ProjectionMode::Include => {
                // Include only specified fields
                for (field, pv) in &self.fields {
                    if field == "_id" {
                        match pv {
                            ProjectionValue::Exclude => continue,
                            _ => {
                                result.insert("_id".to_string(), doc.id.to_json());
                                continue;
                            }
                        }
                    }

                    if let Some(value) = self.get_projected_value(doc, field, pv) {
                        result.insert(field.clone(), value);
                    }
                }

                // Always include _id unless explicitly excluded
                if !self.fields.contains_key("_id") {
                    result.insert("_id".to_string(), doc.id.to_json());
                }
            }
            ProjectionMode::Exclude => {
                // Copy all fields except excluded ones
                if let serde_json::Value::Object(obj) = &doc.data {
                    for (field, value) in obj {
                        if let Some(pv) = self.fields.get(field) {
                            match pv {
                                ProjectionValue::Exclude => {},
                                _ => {
                                    if let Some(v) = self.get_projected_value(doc, field, pv) {
                                        result.insert(field.clone(), v);
                                    }
                                }
                            }
                        } else {
                            result.insert(field.clone(), value.clone());
                        }
                    }
                }

                // Handle _id
                if let Some(pv) = self.fields.get("_id") {
                    if !matches!(pv, ProjectionValue::Exclude) {
                        result.insert("_id".to_string(), doc.id.to_json());
                    }
                } else {
                    result.insert("_id".to_string(), doc.id.to_json());
                }
            }
        }

        Ok(Document {
            id: doc.id.clone(),
            data: serde_json::Value::Object(result),
            version: doc.version,
            created_at: doc.created_at,
            updated_at: doc.updated_at,
        })
    }

    /// Get projected value for a field
    fn get_projected_value(
        &self,
        doc: &Document,
        field: &str,
        pv: &ProjectionValue,
    ) -> Option<serde_json::Value> {
        let value = doc.get_field(field)?;

        match pv {
            ProjectionValue::Include => Some(value.clone()),
            ProjectionValue::Exclude => None,
            ProjectionValue::Slice(spec) => {
                if let serde_json::Value::Array(arr) = value {
                    let sliced = self.apply_slice(arr, spec);
                    Some(serde_json::Value::Array(sliced))
                } else {
                    Some(value.clone())
                }
            }
            ProjectionValue::ElemMatch(query) => {
                if let serde_json::Value::Array(arr) = value {
                    // Find first matching element
                    for item in arr {
                        if self.matches_elem_match(item, query) {
                            return Some(serde_json::Value::Array(vec![item.clone()]));
                        }
                    }
                    Some(serde_json::Value::Array(vec![]))
                } else {
                    Some(value.clone())
                }
            }
            ProjectionValue::Meta(_) => {
                // Meta projections (like textScore) not implemented
                None
            }
            ProjectionValue::Positional => {
                // Positional operator requires query context
                Some(value.clone())
            }
        }
    }

    /// Apply slice to array
    fn apply_slice(&self, arr: &[serde_json::Value], spec: &SliceSpec) -> Vec<serde_json::Value> {
        match spec {
            SliceSpec::Single(n) => {
                if *n >= 0 {
                    arr.iter().take(*n as usize).cloned().collect()
                } else {
                    let skip = arr.len().saturating_sub((-n) as usize);
                    arr.iter().skip(skip).cloned().collect()
                }
            }
            SliceSpec::Range(skip, take) => {
                let start = if *skip >= 0 {
                    *skip as usize
                } else {
                    arr.len().saturating_sub((-skip) as usize)
                };
                arr.iter()
                    .skip(start)
                    .take(*take as usize)
                    .cloned()
                    .collect()
            }
        }
    }

    /// Check if element matches $elemMatch query
    fn matches_elem_match(&self, item: &serde_json::Value, query: &serde_json::Value) -> bool {
        // Simplified matching - check if all query fields match
        if let (serde_json::Value::Object(item_obj), serde_json::Value::Object(query_obj)) =
            (item, query)
        {
            for (field, expected) in query_obj {
                if let Some(actual) = item_obj.get(field) {
                    if actual != expected {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            true
        } else {
            false
        }
    }

    /// Check if projection is empty (no fields specified)
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Check if _id is excluded
    pub fn excludes_id(&self) -> bool {
        matches!(self.fields.get("_id"), Some(ProjectionValue::Exclude))
    }
}

impl Default for Projection {
    fn default() -> Self {
        Self::new()
    }
}

/// Projection builder for fluent construction
pub struct ProjectionBuilder {
    fields: HashMap<String, ProjectionValue>,
}

impl ProjectionBuilder {
    /// Create a new projection builder
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    /// Include a field
    pub fn include(mut self, field: &str) -> Self {
        self.fields
            .insert(field.to_string(), ProjectionValue::Include);
        self
    }

    /// Exclude a field
    pub fn exclude(mut self, field: &str) -> Self {
        self.fields
            .insert(field.to_string(), ProjectionValue::Exclude);
        self
    }

    /// Exclude _id
    pub fn exclude_id(self) -> Self {
        self.exclude("_id")
    }

    /// Slice an array field
    pub fn slice(mut self, field: &str, count: i32) -> Self {
        self.fields.insert(
            field.to_string(),
            ProjectionValue::Slice(SliceSpec::Single(count)),
        );
        self
    }

    /// Slice an array field with skip and take
    pub fn slice_range(mut self, field: &str, skip: i32, take: i32) -> Self {
        self.fields.insert(
            field.to_string(),
            ProjectionValue::Slice(SliceSpec::Range(skip, take)),
        );
        self
    }

    /// Build the projection
    pub fn build(self) -> Projection {
        let has_includes = self
            .fields
            .values()
            .any(|v| matches!(v, ProjectionValue::Include));

        Projection {
            fields: self.fields,
            mode: if has_includes {
                ProjectionMode::Include
            } else {
                ProjectionMode::Exclude
            },
        }
    }
}

impl Default for ProjectionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_include_projection() {
        let projection = Projection::from_json(json!({
            "name": 1,
            "age": 1
        }))
        .unwrap();

        let doc = Document::from_json(json!({
            "name": "Alice",
            "age": 30,
            "email": "alice@example.com"
        }))
        .unwrap();

        let result = projection.apply(&doc).unwrap();
        assert!(result.get_field("name").is_some());
        assert!(result.get_field("age").is_some());
        assert!(result.get_field("email").is_none());
    }

    #[test]
    fn test_exclude_projection() {
        let projection = Projection::from_json(json!({
            "password": 0
        }))
        .unwrap();

        let doc = Document::from_json(json!({
            "name": "Alice",
            "password": "secret"
        }))
        .unwrap();

        let result = projection.apply(&doc).unwrap();
        assert!(result.get_field("name").is_some());
        assert!(result.get_field("password").is_none());
    }

    #[test]
    fn test_exclude_id() {
        let projection = Projection::from_json(json!({
            "_id": 0,
            "name": 1
        }))
        .unwrap();

        let doc = Document::from_json(json!({
            "name": "Alice"
        }))
        .unwrap();

        let result = projection.apply(&doc).unwrap();
        assert!(result.get_field("name").is_some());
        // _id should not be in data
        if let serde_json::Value::Object(obj) = &result.data {
            assert!(!obj.contains_key("_id"));
        }
    }

    #[test]
    fn test_slice_projection() {
        let projection = Projection::from_json(json!({
            "tags": { "$slice": 2 }
        }))
        .unwrap();

        let doc = Document::from_json(json!({
            "tags": ["a", "b", "c", "d"]
        }))
        .unwrap();

        let result = projection.apply(&doc).unwrap();
        let tags = result.get_field("tags").unwrap();
        assert_eq!(tags.as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_slice_negative() {
        let projection = Projection::from_json(json!({
            "tags": { "$slice": -2 }
        }))
        .unwrap();

        let doc = Document::from_json(json!({
            "tags": ["a", "b", "c", "d"]
        }))
        .unwrap();

        let result = projection.apply(&doc).unwrap();
        let tags = result.get_field("tags").unwrap();
        let arr = tags.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0], json!("c"));
        assert_eq!(arr[1], json!("d"));
    }

    #[test]
    fn test_builder() {
        let projection = ProjectionBuilder::new()
            .include("name")
            .include("age")
            .exclude_id()
            .build();

        let doc = Document::from_json(json!({
            "name": "Alice",
            "age": 30,
            "email": "alice@example.com"
        }))
        .unwrap();

        let result = projection.apply(&doc).unwrap();
        assert!(result.get_field("name").is_some());
        assert!(result.get_field("age").is_some());
        assert!(result.get_field("email").is_none());
    }

    #[test]
    fn test_mixed_include_exclude_error() {
        let result = Projection::from_json(json!({
            "name": 1,
            "password": 0
        }));

        assert!(result.is_err());
    }
}
