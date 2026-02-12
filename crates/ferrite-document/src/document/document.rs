//! Document types and ObjectId implementation

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use super::DocumentStoreError;

/// MongoDB-compatible ObjectId
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectId {
    /// 12-byte identifier
    bytes: [u8; 12],
}

impl ObjectId {
    /// Create a new ObjectId
    pub fn new() -> Self {
        use std::sync::atomic::{AtomicU32, Ordering};

        static COUNTER: AtomicU32 = AtomicU32::new(0);

        let mut bytes = [0u8; 12];

        // 4 bytes: Unix timestamp in seconds
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as u32;
        bytes[0..4].copy_from_slice(&timestamp.to_be_bytes());

        // 5 bytes: Random value (process unique)
        let random: [u8; 5] = rand_bytes();
        bytes[4..9].copy_from_slice(&random);

        // 3 bytes: Counter
        let counter = COUNTER.fetch_add(1, Ordering::SeqCst);
        bytes[9..12].copy_from_slice(&counter.to_be_bytes()[1..4]);

        Self { bytes }
    }

    /// Create ObjectId from hex string
    pub fn from_hex(hex: &str) -> Result<Self, DocumentStoreError> {
        if hex.len() != 24 {
            return Err(DocumentStoreError::InvalidDocument(
                "ObjectId must be 24 hex characters".into(),
            ));
        }

        let mut bytes = [0u8; 12];
        for i in 0..12 {
            bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).map_err(|_| {
                DocumentStoreError::InvalidDocument("Invalid hex in ObjectId".into())
            })?;
        }

        Ok(Self { bytes })
    }

    /// Get hex representation
    pub fn to_hex(&self) -> String {
        self.bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }

    /// Get timestamp from ObjectId
    pub fn timestamp(&self) -> u32 {
        u32::from_be_bytes([self.bytes[0], self.bytes[1], self.bytes[2], self.bytes[3]])
    }

    /// Get the raw bytes
    pub fn bytes(&self) -> &[u8; 12] {
        &self.bytes
    }
}

impl Default for ObjectId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// Generate random bytes for ObjectId
fn rand_bytes() -> [u8; 5] {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    std::process::id().hash(&mut hasher);
    std::thread::current().id().hash(&mut hasher);
    SystemTime::now().hash(&mut hasher);

    let hash = hasher.finish();
    let bytes = hash.to_be_bytes();
    [bytes[0], bytes[1], bytes[2], bytes[3], bytes[4]]
}

/// Document identifier (can be ObjectId or other types)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DocumentId {
    /// ObjectId
    ObjectId(ObjectId),
    /// String ID
    String(String),
    /// Integer ID
    Integer(i64),
}

impl DocumentId {
    /// Create a new ObjectId-based document ID
    pub fn new() -> Self {
        Self::ObjectId(ObjectId::new())
    }

    /// Create from JSON value
    pub fn from_json(value: &serde_json::Value) -> Result<Self, DocumentStoreError> {
        match value {
            serde_json::Value::String(s) => {
                // Try to parse as ObjectId
                if s.len() == 24 && s.chars().all(|c| c.is_ascii_hexdigit()) {
                    Ok(Self::ObjectId(ObjectId::from_hex(s)?))
                } else {
                    Ok(Self::String(s.clone()))
                }
            }
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(Self::Integer(i))
                } else {
                    Err(DocumentStoreError::InvalidDocument(
                        "Document ID must be integer, not float".into(),
                    ))
                }
            }
            serde_json::Value::Object(obj) => {
                // Handle { "$oid": "..." } format
                if let Some(oid) = obj.get("$oid") {
                    if let Some(s) = oid.as_str() {
                        return Ok(Self::ObjectId(ObjectId::from_hex(s)?));
                    }
                }
                Err(DocumentStoreError::InvalidDocument(
                    "Invalid _id format".into(),
                ))
            }
            _ => Err(DocumentStoreError::InvalidDocument(
                "Invalid _id type".into(),
            )),
        }
    }

    /// Convert to JSON value
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Self::ObjectId(oid) => serde_json::json!({ "$oid": oid.to_hex() }),
            Self::String(s) => serde_json::Value::String(s.clone()),
            Self::Integer(i) => serde_json::Value::Number((*i).into()),
        }
    }
}

impl Default for DocumentId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for DocumentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ObjectId(oid) => write!(f, "{}", oid),
            Self::String(s) => write!(f, "{}", s),
            Self::Integer(i) => write!(f, "{}", i),
        }
    }
}

/// A JSON document with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    /// Document ID
    pub id: DocumentId,
    /// Document data
    pub data: serde_json::Value,
    /// Version for optimistic concurrency
    pub version: u64,
    /// Creation timestamp
    pub created_at: u64,
    /// Last update timestamp
    pub updated_at: u64,
}

impl Document {
    /// Create a new document from JSON
    pub fn from_json(mut value: serde_json::Value) -> Result<Self, DocumentStoreError> {
        let obj = value.as_object_mut().ok_or_else(|| {
            DocumentStoreError::InvalidDocument("Document must be an object".into())
        })?;

        // Extract or generate _id
        let id = if let Some(id_value) = obj.remove("_id") {
            DocumentId::from_json(&id_value)?
        } else {
            DocumentId::new()
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Ok(Self {
            id,
            data: value,
            version: 1,
            created_at: now,
            updated_at: now,
        })
    }

    /// Convert document to JSON including _id
    pub fn to_json(&self) -> serde_json::Value {
        let mut obj = self.data.clone();
        if let Some(map) = obj.as_object_mut() {
            map.insert("_id".to_string(), self.id.to_json());
        }
        obj
    }

    /// Get a field value by path (supports dot notation)
    pub fn get_field(&self, path: &str) -> Option<&serde_json::Value> {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = &self.data;

        for part in parts {
            match current {
                serde_json::Value::Object(map) => {
                    current = map.get(part)?;
                }
                serde_json::Value::Array(arr) => {
                    let index: usize = part.parse().ok()?;
                    current = arr.get(index)?;
                }
                _ => return None,
            }
        }

        Some(current)
    }

    /// Set a field value by path (supports dot notation)
    pub fn set_field(
        &mut self,
        path: &str,
        value: serde_json::Value,
    ) -> Result<(), DocumentStoreError> {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = &mut self.data;

        for (i, part) in parts.iter().enumerate() {
            if i == parts.len() - 1 {
                // Last part - set the value
                if let Some(map) = current.as_object_mut() {
                    map.insert(part.to_string(), value);
                    return Ok(());
                }
                return Err(DocumentStoreError::InvalidDocument(
                    "Cannot set field on non-object".into(),
                ));
            }

            // Navigate to next level
            match current {
                serde_json::Value::Object(map) => {
                    if !map.contains_key(*part) {
                        map.insert(part.to_string(), serde_json::json!({}));
                    }
                    current = map.get_mut(*part).ok_or_else(|| {
                        DocumentStoreError::InvalidDocument("Path navigation failed".into())
                    })?;
                }
                _ => {
                    return Err(DocumentStoreError::InvalidDocument(
                        "Path contains non-object".into(),
                    ));
                }
            }
        }

        Ok(())
    }

    /// Remove a field by path
    pub fn unset_field(&mut self, path: &str) -> Option<serde_json::Value> {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = &mut self.data;

        for (i, part) in parts.iter().enumerate() {
            if i == parts.len() - 1 {
                // Last part - remove the value
                if let Some(map) = current.as_object_mut() {
                    return map.remove(*part);
                }
                return None;
            }

            // Navigate to next level
            match current {
                serde_json::Value::Object(map) => {
                    current = map.get_mut(*part)?;
                }
                _ => return None,
            }
        }

        None
    }

    /// Check if document has a field
    pub fn has_field(&self, path: &str) -> bool {
        self.get_field(path).is_some()
    }

    /// Get all field paths in the document
    pub fn field_paths(&self) -> Vec<String> {
        let mut paths = Vec::new();
        Self::collect_paths(&self.data, String::new(), &mut paths);
        paths
    }

    fn collect_paths(value: &serde_json::Value, prefix: String, paths: &mut Vec<String>) {
        match value {
            serde_json::Value::Object(map) => {
                for (key, val) in map {
                    let path = if prefix.is_empty() {
                        key.clone()
                    } else {
                        format!("{}.{}", prefix, key)
                    };
                    paths.push(path.clone());
                    Self::collect_paths(val, path, paths);
                }
            }
            serde_json::Value::Array(arr) => {
                for (i, val) in arr.iter().enumerate() {
                    let path = format!("{}.{}", prefix, i);
                    Self::collect_paths(val, path, paths);
                }
            }
            _ => {}
        }
    }
}

/// Document builder for fluent construction
pub struct DocumentBuilder {
    data: HashMap<String, serde_json::Value>,
    id: Option<DocumentId>,
}

impl DocumentBuilder {
    /// Create a new document builder
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            id: None,
        }
    }

    /// Set the document ID
    pub fn id(mut self, id: DocumentId) -> Self {
        self.id = Some(id);
        self
    }

    /// Set a string field
    pub fn string(mut self, key: &str, value: &str) -> Self {
        self.data.insert(
            key.to_string(),
            serde_json::Value::String(value.to_string()),
        );
        self
    }

    /// Set an integer field
    pub fn int(mut self, key: &str, value: i64) -> Self {
        self.data
            .insert(key.to_string(), serde_json::Value::Number(value.into()));
        self
    }

    /// Set a float field
    pub fn float(mut self, key: &str, value: f64) -> Self {
        self.data.insert(
            key.to_string(),
            serde_json::Number::from_f64(value)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
        );
        self
    }

    /// Set a boolean field
    pub fn bool(mut self, key: &str, value: bool) -> Self {
        self.data
            .insert(key.to_string(), serde_json::Value::Bool(value));
        self
    }

    /// Set a null field
    pub fn null(mut self, key: &str) -> Self {
        self.data.insert(key.to_string(), serde_json::Value::Null);
        self
    }

    /// Set an array field
    pub fn array(mut self, key: &str, value: Vec<serde_json::Value>) -> Self {
        self.data
            .insert(key.to_string(), serde_json::Value::Array(value));
        self
    }

    /// Set an object field
    pub fn object(mut self, key: &str, value: serde_json::Map<String, serde_json::Value>) -> Self {
        self.data
            .insert(key.to_string(), serde_json::Value::Object(value));
        self
    }

    /// Set any JSON value
    pub fn value(mut self, key: &str, value: serde_json::Value) -> Self {
        self.data.insert(key.to_string(), value);
        self
    }

    /// Build the document
    pub fn build(self) -> Document {
        let id = self.id.unwrap_or_default();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let data: serde_json::Map<String, serde_json::Value> = self.data.into_iter().collect();

        Document {
            id,
            data: serde_json::Value::Object(data),
            version: 1,
            created_at: now,
            updated_at: now,
        }
    }
}

impl Default for DocumentBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_object_id_creation() {
        let id1 = ObjectId::new();
        let id2 = ObjectId::new();

        assert_ne!(id1, id2);
        assert_eq!(id1.to_hex().len(), 24);
    }

    #[test]
    fn test_object_id_from_hex() {
        let hex = "507f1f77bcf86cd799439011";
        let id = ObjectId::from_hex(hex).unwrap();
        assert_eq!(id.to_hex(), hex);
    }

    #[test]
    fn test_document_from_json() {
        let json = json!({
            "name": "Alice",
            "age": 30,
            "active": true
        });

        let doc = Document::from_json(json).unwrap();
        assert!(matches!(doc.id, DocumentId::ObjectId(_)));
        assert_eq!(doc.get_field("name"), Some(&json!("Alice")));
        assert_eq!(doc.get_field("age"), Some(&json!(30)));
    }

    #[test]
    fn test_document_with_custom_id() {
        let json = json!({
            "_id": "custom-id",
            "name": "Bob"
        });

        let doc = Document::from_json(json).unwrap();
        assert!(matches!(doc.id, DocumentId::String(ref s) if s == "custom-id"));
    }

    #[test]
    fn test_nested_field_access() {
        let json = json!({
            "user": {
                "profile": {
                    "name": "Alice"
                }
            }
        });

        let doc = Document::from_json(json).unwrap();
        assert_eq!(doc.get_field("user.profile.name"), Some(&json!("Alice")));
    }

    #[test]
    fn test_set_field() {
        let json = json!({
            "name": "Alice"
        });

        let mut doc = Document::from_json(json).unwrap();
        doc.set_field("age", json!(30)).unwrap();
        doc.set_field("address.city", json!("NYC")).unwrap();

        assert_eq!(doc.get_field("age"), Some(&json!(30)));
        assert_eq!(doc.get_field("address.city"), Some(&json!("NYC")));
    }

    #[test]
    fn test_document_builder() {
        let doc = DocumentBuilder::new()
            .string("name", "Alice")
            .int("age", 30)
            .bool("active", true)
            .build();

        assert_eq!(doc.get_field("name"), Some(&json!("Alice")));
        assert_eq!(doc.get_field("age"), Some(&json!(30)));
        assert_eq!(doc.get_field("active"), Some(&json!(true)));
    }
}
