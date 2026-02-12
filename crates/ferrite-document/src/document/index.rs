//! Document indexing for efficient queries

use std::collections::{BTreeMap, HashMap, HashSet};

use super::document::{Document, DocumentId};
use super::query::DocumentQuery;

/// Index type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    /// Single field ascending
    Ascending,
    /// Single field descending
    Descending,
    /// Text index
    Text,
    /// Hashed index
    Hashed,
    /// Geospatial 2d
    Geo2d,
    /// Geospatial 2dsphere
    Geo2dSphere,
}

/// A document index
#[derive(Debug, Clone)]
pub struct DocumentIndex {
    /// Index name
    name: String,
    /// Indexed fields
    fields: Vec<String>,
    /// Unique constraint
    unique: bool,
    /// Sparse index
    sparse: bool,
    /// B-tree index for ordered queries
    btree: BTreeMap<IndexKey, HashSet<DocumentId>>,
    /// Hash index for equality queries
    hash: HashMap<IndexKey, HashSet<DocumentId>>,
    /// Text index for full-text search
    text_index: HashMap<String, HashSet<DocumentId>>,
}

/// Index key (comparable and hashable)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexKey {
    /// Key components
    values: Vec<IndexValue>,
}

/// Individual index value
#[derive(Debug, Clone)]
pub enum IndexValue {
    /// Null value
    Null,
    /// Boolean value
    Bool(bool),
    /// Integer value
    Int(i64),
    /// Float value (stored as bits for ordering)
    Float(u64),
    /// String value
    String(String),
    /// Binary data
    Binary(Vec<u8>),
    /// Array (for multi-key indexes)
    Array(Vec<IndexValue>),
}

impl PartialEq for IndexValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (IndexValue::Null, IndexValue::Null) => true,
            (IndexValue::Bool(a), IndexValue::Bool(b)) => a == b,
            (IndexValue::Int(a), IndexValue::Int(b)) => a == b,
            (IndexValue::Float(a), IndexValue::Float(b)) => a == b,
            (IndexValue::String(a), IndexValue::String(b)) => a == b,
            (IndexValue::Binary(a), IndexValue::Binary(b)) => a == b,
            (IndexValue::Array(a), IndexValue::Array(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for IndexValue {}

impl std::hash::Hash for IndexValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            IndexValue::Null => {}
            IndexValue::Bool(b) => b.hash(state),
            IndexValue::Int(i) => i.hash(state),
            IndexValue::Float(f) => f.hash(state),
            IndexValue::String(s) => s.hash(state),
            IndexValue::Binary(b) => b.hash(state),
            IndexValue::Array(a) => {
                for v in a {
                    v.hash(state);
                }
            }
        }
    }
}

impl PartialOrd for IndexValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndexValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        // Type ordering: Null < Bool < Int/Float < String < Binary < Array
        let type_order = |v: &IndexValue| -> u8 {
            match v {
                IndexValue::Null => 0,
                IndexValue::Bool(_) => 1,
                IndexValue::Int(_) | IndexValue::Float(_) => 2,
                IndexValue::String(_) => 3,
                IndexValue::Binary(_) => 4,
                IndexValue::Array(_) => 5,
            }
        };

        let t1 = type_order(self);
        let t2 = type_order(other);

        if t1 != t2 {
            return t1.cmp(&t2);
        }

        match (self, other) {
            (IndexValue::Null, IndexValue::Null) => Ordering::Equal,
            (IndexValue::Bool(a), IndexValue::Bool(b)) => a.cmp(b),
            (IndexValue::Int(a), IndexValue::Int(b)) => a.cmp(b),
            (IndexValue::Float(a), IndexValue::Float(b)) => {
                // Compare as f64
                let fa = f64::from_bits(*a);
                let fb = f64::from_bits(*b);
                fa.partial_cmp(&fb).unwrap_or(Ordering::Equal)
            }
            (IndexValue::Int(a), IndexValue::Float(b)) => {
                let fa = *a as f64;
                let fb = f64::from_bits(*b);
                fa.partial_cmp(&fb).unwrap_or(Ordering::Equal)
            }
            (IndexValue::Float(a), IndexValue::Int(b)) => {
                let fa = f64::from_bits(*a);
                let fb = *b as f64;
                fa.partial_cmp(&fb).unwrap_or(Ordering::Equal)
            }
            (IndexValue::String(a), IndexValue::String(b)) => a.cmp(b),
            (IndexValue::Binary(a), IndexValue::Binary(b)) => a.cmp(b),
            (IndexValue::Array(a), IndexValue::Array(b)) => a.cmp(b),
            _ => Ordering::Equal,
        }
    }
}

impl PartialOrd for IndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndexKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.values.cmp(&other.values)
    }
}

impl IndexValue {
    /// Create index value from JSON
    fn from_json(value: &serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => IndexValue::Null,
            serde_json::Value::Bool(b) => IndexValue::Bool(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    IndexValue::Int(i)
                } else if let Some(f) = n.as_f64() {
                    IndexValue::Float(f.to_bits())
                } else {
                    IndexValue::Null
                }
            }
            serde_json::Value::String(s) => IndexValue::String(s.clone()),
            serde_json::Value::Array(arr) => {
                IndexValue::Array(arr.iter().map(IndexValue::from_json).collect())
            }
            serde_json::Value::Object(_) => {
                // Objects are serialized to JSON string for indexing
                IndexValue::String(serde_json::to_string(value).unwrap_or_default())
            }
        }
    }
}

impl DocumentIndex {
    /// Create a new index
    pub fn new(name: String, fields: Vec<String>, unique: bool, sparse: bool) -> Self {
        Self {
            name,
            fields,
            unique,
            sparse,
            btree: BTreeMap::new(),
            hash: HashMap::new(),
            text_index: HashMap::new(),
        }
    }

    /// Get index name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get indexed fields
    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    /// Is this a unique index
    pub fn is_unique(&self) -> bool {
        self.unique
    }

    /// Is this a sparse index
    pub fn is_sparse(&self) -> bool {
        self.sparse
    }

    /// Insert a document into the index
    pub fn insert(&mut self, doc: &Document) {
        let key = self.extract_key(doc);

        // Skip null keys for sparse indexes
        if self.sparse && key.values.iter().all(|v| matches!(v, IndexValue::Null)) {
            return;
        }

        // Check for array fields (multi-key index)
        if self.has_array_key(&key) {
            let keys = self.expand_array_keys(&key);
            for k in keys {
                self.btree
                    .entry(k.clone())
                    .or_default()
                    .insert(doc.id.clone());
                self.hash
                    .entry(k)
                    .or_default()
                    .insert(doc.id.clone());
            }
        } else {
            self.btree
                .entry(key.clone())
                .or_default()
                .insert(doc.id.clone());
            self.hash
                .entry(key)
                .or_default()
                .insert(doc.id.clone());
        }

        // Build text index for string fields
        for field in &self.fields {
            if let Some(value) = doc.get_field(field) {
                if let Some(text) = value.as_str() {
                    for word in Self::tokenize(text) {
                        self.text_index
                            .entry(word)
                            .or_default()
                            .insert(doc.id.clone());
                    }
                }
            }
        }
    }

    /// Remove a document from the index
    pub fn remove(&mut self, doc: &Document) {
        let key = self.extract_key(doc);

        // Handle multi-key
        if self.has_array_key(&key) {
            let keys = self.expand_array_keys(&key);
            for k in keys {
                if let Some(set) = self.btree.get_mut(&k) {
                    set.remove(&doc.id);
                    if set.is_empty() {
                        self.btree.remove(&k);
                    }
                }
                if let Some(set) = self.hash.get_mut(&k) {
                    set.remove(&doc.id);
                    if set.is_empty() {
                        self.hash.remove(&k);
                    }
                }
            }
        } else {
            if let Some(set) = self.btree.get_mut(&key) {
                set.remove(&doc.id);
                if set.is_empty() {
                    self.btree.remove(&key);
                }
            }
            if let Some(set) = self.hash.get_mut(&key) {
                set.remove(&doc.id);
                if set.is_empty() {
                    self.hash.remove(&key);
                }
            }
        }

        // Remove from text index
        for field in &self.fields {
            if let Some(value) = doc.get_field(field) {
                if let Some(text) = value.as_str() {
                    for word in Self::tokenize(text) {
                        if let Some(set) = self.text_index.get_mut(&word) {
                            set.remove(&doc.id);
                            if set.is_empty() {
                                self.text_index.remove(&word);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Extract index key from document
    fn extract_key(&self, doc: &Document) -> IndexKey {
        let values: Vec<IndexValue> = self
            .fields
            .iter()
            .map(|field| {
                if field == "_id" {
                    IndexValue::from_json(&doc.id.to_json())
                } else {
                    doc.get_field(field)
                        .map(IndexValue::from_json)
                        .unwrap_or(IndexValue::Null)
                }
            })
            .collect();

        IndexKey { values }
    }

    /// Check if key contains array values
    fn has_array_key(&self, key: &IndexKey) -> bool {
        key.values.iter().any(|v| matches!(v, IndexValue::Array(_)))
    }

    /// Expand array keys for multi-key index
    fn expand_array_keys(&self, key: &IndexKey) -> Vec<IndexKey> {
        let mut results = vec![vec![]];

        for value in &key.values {
            match value {
                IndexValue::Array(arr) => {
                    let mut new_results = Vec::new();
                    for existing in &results {
                        for item in arr {
                            let mut new_key = existing.clone();
                            new_key.push(item.clone());
                            new_results.push(new_key);
                        }
                    }
                    results = new_results;
                }
                _ => {
                    for existing in &mut results {
                        existing.push(value.clone());
                    }
                }
            }
        }

        results
            .into_iter()
            .map(|values| IndexKey { values })
            .collect()
    }

    /// Tokenize text for text index
    fn tokenize(text: &str) -> Vec<String> {
        text.to_lowercase()
            .split_whitespace()
            .filter(|w| w.len() >= 2)
            .map(|w| w.trim_matches(|c: char| !c.is_alphanumeric()).to_string())
            .filter(|w| !w.is_empty())
            .collect()
    }

    /// Find exact match
    pub fn find_exact(&self, doc: &Document) -> Option<DocumentId> {
        let key = self.extract_key(doc);
        self.hash
            .get(&key)
            .and_then(|ids| ids.iter().next().cloned())
    }

    /// Query the index
    pub fn query(&self, _query: &DocumentQuery) -> Vec<DocumentId> {
        // For now, return all indexed documents
        // A full implementation would analyze the query and use the appropriate index scan
        let mut result: HashSet<DocumentId> = HashSet::new();

        for ids in self.hash.values() {
            result.extend(ids.iter().cloned());
        }

        result.into_iter().collect()
    }

    /// Range query on the index
    pub fn range_query(
        &self,
        lower: Option<&IndexKey>,
        upper: Option<&IndexKey>,
        inclusive_lower: bool,
        inclusive_upper: bool,
    ) -> Vec<DocumentId> {
        let mut result = HashSet::new();

        let range = match (lower, upper) {
            (Some(l), Some(u)) => {
                let lower_bound = if inclusive_lower {
                    std::ops::Bound::Included(l)
                } else {
                    std::ops::Bound::Excluded(l)
                };
                let upper_bound = if inclusive_upper {
                    std::ops::Bound::Included(u)
                } else {
                    std::ops::Bound::Excluded(u)
                };
                self.btree.range((lower_bound, upper_bound))
            }
            (Some(l), None) => {
                if inclusive_lower {
                    self.btree.range(l..)
                } else {
                    // Workaround for exclusive lower bound with no upper
                    let iter = self.btree.range(l..);
                    let filtered: BTreeMap<_, _> = iter
                        .filter(|(k, _)| *k != l)
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    return filtered.values().flatten().cloned().collect();
                }
            }
            (None, Some(u)) => {
                let upper_bound = if inclusive_upper {
                    std::ops::Bound::Included(u)
                } else {
                    std::ops::Bound::Excluded(u)
                };
                self.btree.range((std::ops::Bound::Unbounded, upper_bound))
            }
            (None, None) => {
                return self.btree.values().flatten().cloned().collect();
            }
        };

        for (_, ids) in range {
            result.extend(ids.iter().cloned());
        }

        result.into_iter().collect()
    }

    /// Text search on the index
    pub fn text_search(&self, query: &str) -> Vec<DocumentId> {
        let mut result = HashSet::new();
        let words = Self::tokenize(query);

        for word in words {
            if let Some(ids) = self.text_index.get(&word) {
                result.extend(ids.iter().cloned());
            }
        }

        result.into_iter().collect()
    }

    /// Get index statistics
    pub fn stats(&self) -> IndexStats {
        IndexStats {
            name: self.name.clone(),
            fields: self.fields.clone(),
            unique: self.unique,
            sparse: self.sparse,
            key_count: self.btree.len(),
            text_terms: self.text_index.len(),
        }
    }
}

/// Index statistics
#[derive(Debug, Clone)]
pub struct IndexStats {
    /// Index name
    pub name: String,
    /// Indexed fields
    pub fields: Vec<String>,
    /// Unique constraint
    pub unique: bool,
    /// Sparse index
    pub sparse: bool,
    /// Number of distinct keys
    pub key_count: usize,
    /// Number of text terms
    pub text_terms: usize,
}

/// Compound index builder
pub struct CompoundIndexBuilder {
    name: Option<String>,
    fields: Vec<(String, IndexType)>,
    unique: bool,
    sparse: bool,
}

impl CompoundIndexBuilder {
    /// Create a new compound index builder
    pub fn new() -> Self {
        Self {
            name: None,
            fields: Vec::new(),
            unique: false,
            sparse: false,
        }
    }

    /// Set index name
    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    /// Add ascending field
    pub fn ascending(mut self, field: &str) -> Self {
        self.fields.push((field.to_string(), IndexType::Ascending));
        self
    }

    /// Add descending field
    pub fn descending(mut self, field: &str) -> Self {
        self.fields.push((field.to_string(), IndexType::Descending));
        self
    }

    /// Add text field
    pub fn text(mut self, field: &str) -> Self {
        self.fields.push((field.to_string(), IndexType::Text));
        self
    }

    /// Set unique constraint
    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    /// Set sparse index
    pub fn sparse(mut self) -> Self {
        self.sparse = true;
        self
    }

    /// Build the index
    pub fn build(self) -> DocumentIndex {
        let name = self.name.unwrap_or_else(|| {
            self.fields
                .iter()
                .map(|(f, t)| {
                    let suffix = match t {
                        IndexType::Ascending => "1",
                        IndexType::Descending => "-1",
                        IndexType::Text => "text",
                        IndexType::Hashed => "hashed",
                        IndexType::Geo2d => "2d",
                        IndexType::Geo2dSphere => "2dsphere",
                    };
                    format!("{}_{}", f.replace('.', "_"), suffix)
                })
                .collect::<Vec<_>>()
                .join("_")
        });

        let fields: Vec<String> = self.fields.into_iter().map(|(f, _)| f).collect();

        DocumentIndex::new(name, fields, self.unique, self.sparse)
    }
}

impl Default for CompoundIndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_single_field_index() {
        let mut index =
            DocumentIndex::new("name_1".to_string(), vec!["name".to_string()], false, false);

        let doc = Document::from_json(json!({ "name": "Alice", "age": 30 })).unwrap();
        index.insert(&doc);

        assert_eq!(index.stats().key_count, 1);
    }

    #[test]
    fn test_compound_index() {
        let mut index = CompoundIndexBuilder::new()
            .ascending("name")
            .descending("age")
            .build();

        let doc = Document::from_json(json!({ "name": "Alice", "age": 30 })).unwrap();
        index.insert(&doc);

        assert_eq!(index.stats().key_count, 1);
    }

    #[test]
    fn test_unique_index() {
        let index = DocumentIndex::new(
            "email_1".to_string(),
            vec!["email".to_string()],
            true,
            false,
        );

        assert!(index.is_unique());
    }

    #[test]
    fn test_sparse_index() {
        let mut index = DocumentIndex::new(
            "optional_1".to_string(),
            vec!["optional".to_string()],
            false,
            true,
        );

        // Document without the field should not be indexed
        let doc = Document::from_json(json!({ "name": "Alice" })).unwrap();
        index.insert(&doc);

        assert_eq!(index.stats().key_count, 0);

        // Document with the field should be indexed
        let doc2 = Document::from_json(json!({ "optional": "value" })).unwrap();
        index.insert(&doc2);

        assert_eq!(index.stats().key_count, 1);
    }

    #[test]
    fn test_text_index() {
        let mut index = CompoundIndexBuilder::new().text("content").build();

        let doc = Document::from_json(json!({
            "content": "Hello world this is a test"
        }))
        .unwrap();
        index.insert(&doc);

        let results = index.text_search("hello");
        assert_eq!(results.len(), 1);

        let results = index.text_search("world test");
        assert_eq!(results.len(), 1);

        let results = index.text_search("foo");
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_multi_key_index() {
        let mut index =
            DocumentIndex::new("tags_1".to_string(), vec!["tags".to_string()], false, false);

        let doc = Document::from_json(json!({
            "tags": ["rust", "database", "nosql"]
        }))
        .unwrap();
        index.insert(&doc);

        // Multi-key index expands the array
        assert_eq!(index.stats().key_count, 3);
    }

    #[test]
    fn test_range_query() {
        let mut index =
            DocumentIndex::new("age_1".to_string(), vec!["age".to_string()], false, false);

        for age in [20, 25, 30, 35, 40] {
            let doc = Document::from_json(json!({ "age": age })).unwrap();
            index.insert(&doc);
        }

        let lower = IndexKey {
            values: vec![IndexValue::Int(25)],
        };
        let upper = IndexKey {
            values: vec![IndexValue::Int(35)],
        };

        let results = index.range_query(Some(&lower), Some(&upper), true, true);
        assert_eq!(results.len(), 3); // 25, 30, 35
    }
}
