//! Document types for full-text search
//!
//! Provides document and field abstractions.

#[allow(unused_imports)]
use super::*;
use std::collections::HashMap;

/// Unique document identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DocumentId(pub String);

impl DocumentId {
    /// Create a new document ID
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Get the ID as a string reference
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for DocumentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for DocumentId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for DocumentId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// A document to be indexed
#[derive(Debug, Clone)]
pub struct Document {
    /// Document ID
    pub id: DocumentId,
    /// Document fields
    pub fields: HashMap<String, Field>,
    /// Boost factor
    pub boost: f32,
    /// Document metadata
    pub metadata: HashMap<String, String>,
}

impl Document {
    /// Create a new document
    pub fn new(id: impl Into<DocumentId>) -> Self {
        Self {
            id: id.into(),
            fields: HashMap::new(),
            boost: 1.0,
            metadata: HashMap::new(),
        }
    }

    /// Add a text field
    pub fn field(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        let name = name.into();
        self.fields.insert(
            name.clone(),
            Field {
                name,
                value: FieldValue::Text(value.into()),
                field_type: FieldType::Text,
                boost: 1.0,
                stored: true,
                indexed: true,
            },
        );
        self
    }

    /// Add a keyword field (not analyzed)
    pub fn keyword(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        let name = name.into();
        self.fields.insert(
            name.clone(),
            Field {
                name,
                value: FieldValue::Text(value.into()),
                field_type: FieldType::Keyword,
                boost: 1.0,
                stored: true,
                indexed: true,
            },
        );
        self
    }

    /// Add a numeric field
    pub fn number(mut self, name: impl Into<String>, value: f64) -> Self {
        let name = name.into();
        self.fields.insert(
            name.clone(),
            Field {
                name,
                value: FieldValue::Number(value),
                field_type: FieldType::Number,
                boost: 1.0,
                stored: true,
                indexed: true,
            },
        );
        self
    }

    /// Add a boolean field
    pub fn boolean(mut self, name: impl Into<String>, value: bool) -> Self {
        let name = name.into();
        self.fields.insert(
            name.clone(),
            Field {
                name,
                value: FieldValue::Boolean(value),
                field_type: FieldType::Boolean,
                boost: 1.0,
                stored: true,
                indexed: true,
            },
        );
        self
    }

    /// Add a date field
    pub fn date(mut self, name: impl Into<String>, value: i64) -> Self {
        let name = name.into();
        self.fields.insert(
            name.clone(),
            Field {
                name,
                value: FieldValue::Date(value),
                field_type: FieldType::Date,
                boost: 1.0,
                stored: true,
                indexed: true,
            },
        );
        self
    }

    /// Set document boost
    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = boost;
        self
    }

    /// Add metadata
    pub fn meta(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Get a field
    pub fn get(&self, name: &str) -> Option<&Field> {
        self.fields.get(name)
    }

    /// Get text value of a field
    pub fn get_text(&self, name: &str) -> Option<&str> {
        self.fields.get(name).and_then(|f| f.as_text())
    }

    /// Get all field names
    pub fn field_names(&self) -> Vec<&str> {
        self.fields.keys().map(|s| s.as_str()).collect()
    }

    /// Check if field exists
    pub fn has_field(&self, name: &str) -> bool {
        self.fields.contains_key(name)
    }

    /// Get the document ID as string
    pub fn id_str(&self) -> &str {
        self.id.as_str()
    }
}

/// A document field
#[derive(Debug, Clone)]
pub struct Field {
    /// Field name
    pub name: String,
    /// Field value
    pub value: FieldValue,
    /// Field type
    pub field_type: FieldType,
    /// Field boost
    pub boost: f32,
    /// Store the original value
    pub stored: bool,
    /// Index the field for searching
    pub indexed: bool,
}

impl Field {
    /// Create a text field
    pub fn text(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: FieldValue::Text(value.into()),
            field_type: FieldType::Text,
            boost: 1.0,
            stored: true,
            indexed: true,
        }
    }

    /// Create a keyword field
    pub fn keyword(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: FieldValue::Text(value.into()),
            field_type: FieldType::Keyword,
            boost: 1.0,
            stored: true,
            indexed: true,
        }
    }

    /// Set boost
    pub fn with_boost(mut self, boost: f32) -> Self {
        self.boost = boost;
        self
    }

    /// Set stored
    pub fn stored(mut self, stored: bool) -> Self {
        self.stored = stored;
        self
    }

    /// Set indexed
    pub fn indexed(mut self, indexed: bool) -> Self {
        self.indexed = indexed;
        self
    }

    /// Get as text
    pub fn as_text(&self) -> Option<&str> {
        match &self.value {
            FieldValue::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Get as number
    pub fn as_number(&self) -> Option<f64> {
        match &self.value {
            FieldValue::Number(n) => Some(*n),
            _ => None,
        }
    }

    /// Get as boolean
    pub fn as_boolean(&self) -> Option<bool> {
        match &self.value {
            FieldValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// Get as date
    pub fn as_date(&self) -> Option<i64> {
        match &self.value {
            FieldValue::Date(d) => Some(*d),
            _ => None,
        }
    }
}

/// Field value types
#[derive(Debug, Clone)]
pub enum FieldValue {
    /// Text value
    Text(String),
    /// Numeric value
    Number(f64),
    /// Boolean value
    Boolean(bool),
    /// Date value (timestamp)
    Date(i64),
    /// Binary value
    Binary(Vec<u8>),
    /// Array of values
    Array(Vec<FieldValue>),
    /// Null value
    Null,
}

impl FieldValue {
    /// Get string representation
    pub fn to_string_value(&self) -> String {
        match self {
            FieldValue::Text(s) => s.clone(),
            FieldValue::Number(n) => n.to_string(),
            FieldValue::Boolean(b) => b.to_string(),
            FieldValue::Date(d) => d.to_string(),
            FieldValue::Binary(b) => format!("<binary: {} bytes>", b.len()),
            FieldValue::Array(a) => format!("<array: {} items>", a.len()),
            FieldValue::Null => "null".to_string(),
        }
    }

    /// Check if null
    pub fn is_null(&self) -> bool {
        matches!(self, FieldValue::Null)
    }
}

impl std::fmt::Display for FieldValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string_value())
    }
}

/// Field types
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FieldType {
    /// Full-text searchable
    #[default]
    Text,
    /// Exact match only (not analyzed)
    Keyword,
    /// Numeric
    Number,
    /// Boolean
    Boolean,
    /// Date/time
    Date,
    /// Binary data
    Binary,
    /// Geo point
    GeoPoint,
}

impl std::fmt::Display for FieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldType::Text => write!(f, "text"),
            FieldType::Keyword => write!(f, "keyword"),
            FieldType::Number => write!(f, "number"),
            FieldType::Boolean => write!(f, "boolean"),
            FieldType::Date => write!(f, "date"),
            FieldType::Binary => write!(f, "binary"),
            FieldType::GeoPoint => write!(f, "geo_point"),
        }
    }
}

/// Document builder
pub struct DocumentBuilder {
    document: Document,
}

impl DocumentBuilder {
    /// Create a new document builder
    pub fn new(id: impl Into<DocumentId>) -> Self {
        Self {
            document: Document::new(id),
        }
    }

    /// Add a text field
    pub fn text(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.document = self.document.field(name, value);
        self
    }

    /// Add a keyword field
    pub fn keyword(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.document = self.document.keyword(name, value);
        self
    }

    /// Add a number field
    pub fn number(mut self, name: impl Into<String>, value: f64) -> Self {
        self.document = self.document.number(name, value);
        self
    }

    /// Set boost
    pub fn boost(mut self, boost: f32) -> Self {
        self.document.boost = boost;
        self
    }

    /// Build the document
    pub fn build(self) -> Document {
        self.document
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_document_creation() {
        let doc = Document::new("doc1")
            .field("title", "Hello World")
            .field("body", "This is a test");

        assert_eq!(doc.id.as_str(), "doc1");
        assert_eq!(doc.get_text("title"), Some("Hello World"));
        assert_eq!(doc.get_text("body"), Some("This is a test"));
    }

    #[test]
    fn test_document_fields() {
        let doc = Document::new("doc1")
            .field("title", "Test")
            .keyword("category", "tech")
            .number("score", 95.5)
            .boolean("published", true);

        assert!(doc.has_field("title"));
        assert_eq!(doc.get("score").unwrap().as_number(), Some(95.5));
        assert_eq!(doc.get("published").unwrap().as_boolean(), Some(true));
    }

    #[test]
    fn test_document_boost() {
        let doc = Document::new("doc1").boost(2.0);
        assert_eq!(doc.boost, 2.0);
    }

    #[test]
    fn test_document_metadata() {
        let doc = Document::new("doc1")
            .meta("source", "web")
            .meta("author", "john");

        assert_eq!(doc.metadata.get("source"), Some(&"web".to_string()));
    }

    #[test]
    fn test_document_builder() {
        let doc = DocumentBuilder::new("doc1")
            .text("title", "Hello")
            .keyword("tag", "test")
            .number("count", 10.0)
            .boost(1.5)
            .build();

        assert_eq!(doc.id.as_str(), "doc1");
        assert_eq!(doc.boost, 1.5);
        assert!(doc.has_field("title"));
    }

    #[test]
    fn test_field_value() {
        let text = FieldValue::Text("hello".to_string());
        assert_eq!(text.to_string_value(), "hello");

        let num = FieldValue::Number(42.0);
        assert_eq!(num.to_string_value(), "42");

        let null = FieldValue::Null;
        assert!(null.is_null());
    }

    #[test]
    fn test_field_creation() {
        let field = Field::text("title", "Test Title")
            .with_boost(2.0)
            .stored(true)
            .indexed(true);

        assert_eq!(field.name, "title");
        assert_eq!(field.boost, 2.0);
        assert!(field.stored);
        assert!(field.indexed);
    }
}
