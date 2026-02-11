//! Document Management
//!
//! Handles document storage, metadata, and lifecycle management.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Unique document identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DocumentId(pub String);

impl DocumentId {
    /// Create a new random document ID
    pub fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        Self(format!("doc:{}", id))
    }

    /// Create from a string
    pub fn from_string(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Get the string representation
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for DocumentId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for DocumentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Document metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentMetadata {
    /// Document title
    pub title: Option<String>,
    /// Document source/URL
    pub source: Option<String>,
    /// Document author
    pub author: Option<String>,
    /// Creation timestamp
    pub created_at: SystemTime,
    /// Last modified timestamp
    pub modified_at: SystemTime,
    /// Content type (text/plain, text/html, application/pdf)
    pub content_type: String,
    /// Language (ISO 639-1)
    pub language: Option<String>,
    /// Custom attributes
    pub attributes: HashMap<String, AttributeValue>,
    /// Tags for categorization
    pub tags: Vec<String>,
}

impl Default for DocumentMetadata {
    fn default() -> Self {
        let now = SystemTime::now();
        Self {
            title: None,
            source: None,
            author: None,
            created_at: now,
            modified_at: now,
            content_type: "text/plain".to_string(),
            language: None,
            attributes: HashMap::new(),
            tags: Vec::new(),
        }
    }
}

impl DocumentMetadata {
    /// Create metadata with title
    pub fn with_title(title: impl Into<String>) -> Self {
        Self {
            title: Some(title.into()),
            ..Default::default()
        }
    }

    /// Add an attribute
    pub fn with_attribute(mut self, key: impl Into<String>, value: AttributeValue) -> Self {
        self.attributes.insert(key.into(), value);
        self
    }

    /// Add a tag
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Set source
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }
}

/// Attribute value types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AttributeValue {
    /// String value
    String(String),
    /// Integer value
    Integer(i64),
    /// Float value
    Float(f64),
    /// Boolean value
    Boolean(bool),
    /// List of strings
    StringList(Vec<String>),
}

impl From<String> for AttributeValue {
    fn from(s: String) -> Self {
        AttributeValue::String(s)
    }
}

impl From<&str> for AttributeValue {
    fn from(s: &str) -> Self {
        AttributeValue::String(s.to_string())
    }
}

impl From<i64> for AttributeValue {
    fn from(n: i64) -> Self {
        AttributeValue::Integer(n)
    }
}

impl From<f64> for AttributeValue {
    fn from(n: f64) -> Self {
        AttributeValue::Float(n)
    }
}

impl From<bool> for AttributeValue {
    fn from(b: bool) -> Self {
        AttributeValue::Boolean(b)
    }
}

/// A document in the RAG system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    /// Document ID
    pub id: DocumentId,
    /// Document content
    pub content: String,
    /// Document metadata
    pub metadata: DocumentMetadata,
    /// Content hash for deduplication
    pub content_hash: u64,
    /// Whether the document has been processed
    pub processed: bool,
}

impl Document {
    /// Create a new document from text
    pub fn from_text(content: impl Into<String>) -> Self {
        let content = content.into();
        let content_hash = Self::hash_content(&content);

        Self {
            id: DocumentId::new(),
            content,
            metadata: DocumentMetadata::default(),
            content_hash,
            processed: false,
        }
    }

    /// Create a document with ID
    pub fn with_id(id: DocumentId, content: impl Into<String>) -> Self {
        let content = content.into();
        let content_hash = Self::hash_content(&content);

        Self {
            id,
            content,
            metadata: DocumentMetadata::default(),
            content_hash,
            processed: false,
        }
    }

    /// Set metadata
    pub fn with_metadata(mut self, metadata: DocumentMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Get content length
    pub fn content_length(&self) -> usize {
        self.content.len()
    }

    /// Check if content is empty
    pub fn is_empty(&self) -> bool {
        self.content.is_empty()
    }

    /// Compute content hash
    fn hash_content(content: &str) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        content.hash(&mut hasher);
        hasher.finish()
    }

    /// Mark as processed
    pub fn mark_processed(&mut self) {
        self.processed = true;
        self.metadata.modified_at = SystemTime::now();
    }
}

/// In-memory document store
pub struct DocumentStore {
    /// Documents indexed by ID
    documents: RwLock<HashMap<DocumentId, Document>>,
    /// Content hash to ID for deduplication
    content_index: RwLock<HashMap<u64, DocumentId>>,
    /// Statistics
    stats: DocumentStoreStats,
}

/// Document store statistics
#[derive(Debug, Default)]
pub struct DocumentStoreStats {
    /// Total documents
    pub total_documents: AtomicU64,
    /// Total content bytes
    pub total_bytes: AtomicU64,
    /// Duplicate documents skipped
    pub duplicates_skipped: AtomicU64,
}

impl DocumentStore {
    /// Create a new document store
    pub fn new() -> Self {
        Self {
            documents: RwLock::new(HashMap::new()),
            content_index: RwLock::new(HashMap::new()),
            stats: DocumentStoreStats::default(),
        }
    }

    /// Add a document to the store
    pub fn add(&self, document: Document) -> Result<DocumentId, DocumentError> {
        // Check for duplicates
        {
            let content_index = self.content_index.read();
            if let Some(existing_id) = content_index.get(&document.content_hash) {
                self.stats
                    .duplicates_skipped
                    .fetch_add(1, Ordering::Relaxed);
                return Err(DocumentError::Duplicate(existing_id.clone()));
            }
        }

        let id = document.id.clone();
        let content_len = document.content_length() as u64;
        let content_hash = document.content_hash;

        // Add to stores
        {
            let mut docs = self.documents.write();
            docs.insert(id.clone(), document);
        }
        {
            let mut index = self.content_index.write();
            index.insert(content_hash, id.clone());
        }

        self.stats.total_documents.fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_bytes
            .fetch_add(content_len, Ordering::Relaxed);

        Ok(id)
    }

    /// Get a document by ID
    pub fn get(&self, id: &DocumentId) -> Option<Document> {
        self.documents.read().get(id).cloned()
    }

    /// Remove a document
    pub fn remove(&self, id: &DocumentId) -> Option<Document> {
        let doc = {
            let mut docs = self.documents.write();
            docs.remove(id)
        };

        if let Some(ref doc) = doc {
            let mut index = self.content_index.write();
            index.remove(&doc.content_hash);
            self.stats.total_documents.fetch_sub(1, Ordering::Relaxed);
            self.stats
                .total_bytes
                .fetch_sub(doc.content_length() as u64, Ordering::Relaxed);
        }

        doc
    }

    /// Check if a document exists
    pub fn contains(&self, id: &DocumentId) -> bool {
        self.documents.read().contains_key(id)
    }

    /// Get all document IDs
    pub fn list_ids(&self) -> Vec<DocumentId> {
        self.documents.read().keys().cloned().collect()
    }

    /// Get document count
    pub fn len(&self) -> usize {
        self.documents.read().len()
    }

    /// Check if store is empty
    pub fn is_empty(&self) -> bool {
        self.documents.read().is_empty()
    }

    /// Update a document
    pub fn update(&self, id: &DocumentId, content: String) -> Result<(), DocumentError> {
        let mut docs = self.documents.write();
        if let Some(doc) = docs.get_mut(id) {
            let old_len = doc.content_length() as u64;
            let new_len = content.len() as u64;

            // Update content hash index
            {
                let mut index = self.content_index.write();
                index.remove(&doc.content_hash);
                let new_hash = Document::hash_content(&content);
                doc.content_hash = new_hash;
                index.insert(new_hash, id.clone());
            }

            doc.content = content;
            doc.metadata.modified_at = SystemTime::now();

            // Update byte count
            self.stats.total_bytes.fetch_sub(old_len, Ordering::Relaxed);
            self.stats.total_bytes.fetch_add(new_len, Ordering::Relaxed);

            Ok(())
        } else {
            Err(DocumentError::NotFound(id.clone()))
        }
    }

    /// Search documents by metadata
    pub fn search_by_tag(&self, tag: &str) -> Vec<DocumentId> {
        self.documents
            .read()
            .iter()
            .filter(|(_, doc)| doc.metadata.tags.contains(&tag.to_string()))
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Get statistics
    pub fn stats(&self) -> &DocumentStoreStats {
        &self.stats
    }

    /// Clear all documents
    pub fn clear(&self) {
        let mut docs = self.documents.write();
        let mut index = self.content_index.write();
        docs.clear();
        index.clear();
        self.stats.total_documents.store(0, Ordering::Release);
        self.stats.total_bytes.store(0, Ordering::Release);
    }
}

impl Default for DocumentStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Document errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum DocumentError {
    /// Document not found
    #[error("document not found: {0}")]
    NotFound(DocumentId),

    /// Duplicate document
    #[error("duplicate document detected, existing: {0}")]
    Duplicate(DocumentId),

    /// Invalid content
    #[error("invalid content: {0}")]
    InvalidContent(String),

    /// Storage error
    #[error("storage error: {0}")]
    Storage(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_document_id_generation() {
        let id1 = DocumentId::new();
        let id2 = DocumentId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_document_creation() {
        let doc = Document::from_text("Hello, world!");
        assert!(!doc.is_empty());
        assert_eq!(doc.content_length(), 13);
        assert!(!doc.processed);
    }

    #[test]
    fn test_document_metadata() {
        let metadata = DocumentMetadata::with_title("Test Document")
            .with_source("https://example.com")
            .with_tag("test")
            .with_attribute("category", "example".into());

        assert_eq!(metadata.title, Some("Test Document".to_string()));
        assert_eq!(metadata.tags, vec!["test"]);
    }

    #[test]
    fn test_document_store() {
        let store = DocumentStore::new();

        let doc = Document::from_text("Test content");
        let id = store.add(doc).unwrap();

        assert!(store.contains(&id));
        assert_eq!(store.len(), 1);

        let retrieved = store.get(&id).unwrap();
        assert_eq!(retrieved.content, "Test content");

        store.remove(&id);
        assert!(!store.contains(&id));
    }

    #[test]
    fn test_document_deduplication() {
        let store = DocumentStore::new();

        let doc1 = Document::from_text("Same content");
        let doc2 = Document::from_text("Same content");

        let id1 = store.add(doc1).unwrap();
        let result = store.add(doc2);

        assert!(result.is_err());
        if let Err(DocumentError::Duplicate(dup_id)) = result {
            assert_eq!(dup_id, id1);
        }
    }

    #[test]
    fn test_document_update() {
        let store = DocumentStore::new();

        let doc = Document::from_text("Original");
        let id = store.add(doc).unwrap();

        store.update(&id, "Updated".to_string()).unwrap();

        let updated = store.get(&id).unwrap();
        assert_eq!(updated.content, "Updated");
    }

    #[test]
    fn test_search_by_tag() {
        let store = DocumentStore::new();

        let doc1 = Document::from_text("Doc 1")
            .with_metadata(DocumentMetadata::default().with_tag("important"));
        let doc2 = Document::from_text("Doc 2")
            .with_metadata(DocumentMetadata::default().with_tag("normal"));
        let doc3 = Document::from_text("Doc 3")
            .with_metadata(DocumentMetadata::default().with_tag("important"));

        store.add(doc1).unwrap();
        store.add(doc2).unwrap();
        store.add(doc3).unwrap();

        let important = store.search_by_tag("important");
        assert_eq!(important.len(), 2);
    }

    #[test]
    fn test_attribute_conversions() {
        assert_eq!(
            AttributeValue::from("test"),
            AttributeValue::String("test".to_string())
        );
        assert_eq!(AttributeValue::from(42i64), AttributeValue::Integer(42));
        assert_eq!(AttributeValue::from(3.14f64), AttributeValue::Float(3.14));
        assert_eq!(AttributeValue::from(true), AttributeValue::Boolean(true));
    }
}
