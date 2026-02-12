//! Enhanced document indexes: field, compound, array, and text indexes

use std::collections::{BTreeMap, HashMap, HashSet};

use serde_json::Value;

use super::document::DocumentId;

/// Index entry for a single document
#[derive(Debug, Clone)]
struct IndexEntry {
    doc_id: DocumentId,
}

/// Field index: indexes individual JSON fields for fast equality/range queries
#[derive(Debug, Clone)]
pub struct FieldIndex {
    name: String,
    field: String,
    unique: bool,
    /// Sorted entries for range queries
    entries: BTreeMap<IndexKeyValue, HashSet<DocumentId>>,
}

/// Compound index: indexes multiple fields together
#[derive(Debug, Clone)]
pub struct CompoundFieldIndex {
    name: String,
    fields: Vec<String>,
    unique: bool,
    entries: BTreeMap<Vec<IndexKeyValue>, HashSet<DocumentId>>,
}

/// Array index: indexes elements within array fields
#[derive(Debug, Clone)]
pub struct ArrayIndex {
    name: String,
    field: String,
    entries: HashMap<IndexKeyValue, HashSet<DocumentId>>,
}

/// Text index: basic text search within document fields
#[derive(Debug, Clone)]
pub struct TextIndex {
    name: String,
    fields: Vec<String>,
    /// Inverted index: term -> document IDs
    terms: HashMap<String, HashSet<DocumentId>>,
}

/// Comparable key value for indexes
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IndexKeyValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(u64),
    String(String),
}

impl PartialOrd for IndexKeyValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndexKeyValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        let type_order = |v: &IndexKeyValue| -> u8 {
            match v {
                IndexKeyValue::Null => 0,
                IndexKeyValue::Bool(_) => 1,
                IndexKeyValue::Int(_) | IndexKeyValue::Float(_) => 2,
                IndexKeyValue::String(_) => 3,
            }
        };

        let t1 = type_order(self);
        let t2 = type_order(other);
        if t1 != t2 {
            return t1.cmp(&t2);
        }

        match (self, other) {
            (IndexKeyValue::Null, IndexKeyValue::Null) => Ordering::Equal,
            (IndexKeyValue::Bool(a), IndexKeyValue::Bool(b)) => a.cmp(b),
            (IndexKeyValue::Int(a), IndexKeyValue::Int(b)) => a.cmp(b),
            (IndexKeyValue::Float(a), IndexKeyValue::Float(b)) => {
                f64::from_bits(*a)
                    .partial_cmp(&f64::from_bits(*b))
                    .unwrap_or(Ordering::Equal)
            }
            (IndexKeyValue::Int(a), IndexKeyValue::Float(b)) => {
                (*a as f64)
                    .partial_cmp(&f64::from_bits(*b))
                    .unwrap_or(Ordering::Equal)
            }
            (IndexKeyValue::Float(a), IndexKeyValue::Int(b)) => {
                f64::from_bits(*a)
                    .partial_cmp(&(*b as f64))
                    .unwrap_or(Ordering::Equal)
            }
            (IndexKeyValue::String(a), IndexKeyValue::String(b)) => a.cmp(b),
            _ => Ordering::Equal,
        }
    }
}

impl IndexKeyValue {
    /// Extract a key from a JSON value
    pub fn from_value(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(b) => Self::Bool(*b),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Self::Int(i)
                } else if let Some(f) = n.as_f64() {
                    Self::Float(f.to_bits())
                } else {
                    Self::Null
                }
            }
            Value::String(s) => Self::String(s.clone()),
            _ => Self::String(serde_json::to_string(value).unwrap_or_default()),
        }
    }
}

/// Get a nested field from a JSON value using dot notation
fn get_field<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;
    for part in path.split('.') {
        match current {
            Value::Object(map) => current = map.get(part)?,
            Value::Array(arr) => {
                let idx: usize = part.parse().ok()?;
                current = arr.get(idx)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

// --- FieldIndex ---

impl FieldIndex {
    /// Create a new field index
    pub fn new(name: String, field: String, unique: bool) -> Self {
        Self {
            name,
            field,
            unique,
            entries: BTreeMap::new(),
        }
    }

    /// Index name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Is unique
    pub fn is_unique(&self) -> bool {
        self.unique
    }

    /// Insert a document into the index
    pub fn insert(
        &mut self,
        doc_id: &DocumentId,
        data: &Value,
    ) -> Result<(), super::DocumentStoreError> {
        let key = get_field(data, &self.field)
            .map(IndexKeyValue::from_value)
            .unwrap_or(IndexKeyValue::Null);

        if self.unique {
            if let Some(ids) = self.entries.get(&key) {
                if !ids.is_empty() && !ids.contains(doc_id) {
                    return Err(super::DocumentStoreError::DuplicateKey(format!(
                        "Duplicate key on index '{}'",
                        self.name
                    )));
                }
            }
        }

        self.entries
            .entry(key)
            .or_default()
            .insert(doc_id.clone());
        Ok(())
    }

    /// Remove a document from the index
    pub fn remove(&mut self, doc_id: &DocumentId, data: &Value) {
        let key = get_field(data, &self.field)
            .map(IndexKeyValue::from_value)
            .unwrap_or(IndexKeyValue::Null);

        if let Some(ids) = self.entries.get_mut(&key) {
            ids.remove(doc_id);
            if ids.is_empty() {
                self.entries.remove(&key);
            }
        }
    }

    /// Find documents by exact value
    pub fn find_eq(&self, value: &Value) -> Vec<DocumentId> {
        let key = IndexKeyValue::from_value(value);
        self.entries
            .get(&key)
            .map(|ids| ids.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Range query
    pub fn find_range(
        &self,
        min: Option<&Value>,
        max: Option<&Value>,
        inclusive_min: bool,
        inclusive_max: bool,
    ) -> Vec<DocumentId> {
        let mut result = HashSet::new();

        let lower = min.map(IndexKeyValue::from_value);
        let upper = max.map(IndexKeyValue::from_value);

        let range = match (&lower, &upper) {
            (Some(l), Some(u)) => {
                let lb = if inclusive_min {
                    std::ops::Bound::Included(l)
                } else {
                    std::ops::Bound::Excluded(l)
                };
                let ub = if inclusive_max {
                    std::ops::Bound::Included(u)
                } else {
                    std::ops::Bound::Excluded(u)
                };
                self.entries.range::<IndexKeyValue, _>((lb, ub))
            }
            (Some(l), None) => {
                let lb = if inclusive_min {
                    std::ops::Bound::Included(l)
                } else {
                    std::ops::Bound::Excluded(l)
                };
                self.entries.range::<IndexKeyValue, _>((lb, std::ops::Bound::Unbounded))
            }
            (None, Some(u)) => {
                let ub = if inclusive_max {
                    std::ops::Bound::Included(u)
                } else {
                    std::ops::Bound::Excluded(u)
                };
                self.entries.range::<IndexKeyValue, _>((std::ops::Bound::Unbounded, ub))
            }
            (None, None) => {
                self.entries.range::<IndexKeyValue, _>(..)
            }
        };

        for (_, ids) in range {
            result.extend(ids.iter().cloned());
        }
        result.into_iter().collect()
    }

    /// Get number of distinct keys
    pub fn key_count(&self) -> usize {
        self.entries.len()
    }
}

// --- CompoundFieldIndex ---

impl CompoundFieldIndex {
    /// Create a new compound index
    pub fn new(name: String, fields: Vec<String>, unique: bool) -> Self {
        Self {
            name,
            fields,
            unique,
            entries: BTreeMap::new(),
        }
    }

    /// Index name
    pub fn name(&self) -> &str {
        &self.name
    }

    fn extract_key(&self, data: &Value) -> Vec<IndexKeyValue> {
        self.fields
            .iter()
            .map(|f| {
                get_field(data, f)
                    .map(IndexKeyValue::from_value)
                    .unwrap_or(IndexKeyValue::Null)
            })
            .collect()
    }

    /// Insert a document
    pub fn insert(
        &mut self,
        doc_id: &DocumentId,
        data: &Value,
    ) -> Result<(), super::DocumentStoreError> {
        let key = self.extract_key(data);

        if self.unique {
            if let Some(ids) = self.entries.get(&key) {
                if !ids.is_empty() && !ids.contains(doc_id) {
                    return Err(super::DocumentStoreError::DuplicateKey(format!(
                        "Duplicate compound key on index '{}'",
                        self.name
                    )));
                }
            }
        }

        self.entries.entry(key).or_default().insert(doc_id.clone());
        Ok(())
    }

    /// Remove a document
    pub fn remove(&mut self, doc_id: &DocumentId, data: &Value) {
        let key = self.extract_key(data);
        if let Some(ids) = self.entries.get_mut(&key) {
            ids.remove(doc_id);
            if ids.is_empty() {
                self.entries.remove(&key);
            }
        }
    }

    /// Find by exact compound key
    pub fn find_eq(&self, values: &[Value]) -> Vec<DocumentId> {
        let key: Vec<IndexKeyValue> = values.iter().map(IndexKeyValue::from_value).collect();
        self.entries
            .get(&key)
            .map(|ids| ids.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get number of distinct keys
    pub fn key_count(&self) -> usize {
        self.entries.len()
    }
}

// --- ArrayIndex ---

impl ArrayIndex {
    /// Create a new array index
    pub fn new(name: String, field: String) -> Self {
        Self {
            name,
            field,
            entries: HashMap::new(),
        }
    }

    /// Index name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Insert a document (indexes each array element)
    pub fn insert(&mut self, doc_id: &DocumentId, data: &Value) {
        if let Some(Value::Array(arr)) = get_field(data, &self.field) {
            for item in arr {
                let key = IndexKeyValue::from_value(item);
                self.entries.entry(key).or_default().insert(doc_id.clone());
            }
        }
    }

    /// Remove a document
    pub fn remove(&mut self, doc_id: &DocumentId, data: &Value) {
        if let Some(Value::Array(arr)) = get_field(data, &self.field) {
            for item in arr {
                let key = IndexKeyValue::from_value(item);
                if let Some(ids) = self.entries.get_mut(&key) {
                    ids.remove(doc_id);
                    if ids.is_empty() {
                        self.entries.remove(&key);
                    }
                }
            }
        }
    }

    /// Find documents containing a specific array element
    pub fn find_contains(&self, value: &Value) -> Vec<DocumentId> {
        let key = IndexKeyValue::from_value(value);
        self.entries
            .get(&key)
            .map(|ids| ids.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get number of distinct indexed values
    pub fn key_count(&self) -> usize {
        self.entries.len()
    }
}

// --- TextIndex ---

impl TextIndex {
    /// Create a new text index
    pub fn new(name: String, fields: Vec<String>) -> Self {
        Self {
            name,
            fields,
            terms: HashMap::new(),
        }
    }

    /// Index name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Insert a document
    pub fn insert(&mut self, doc_id: &DocumentId, data: &Value) {
        for field in &self.fields {
            if let Some(Value::String(text)) = get_field(data, field) {
                for term in tokenize(text) {
                    self.terms.entry(term).or_default().insert(doc_id.clone());
                }
            }
        }
    }

    /// Remove a document
    pub fn remove(&mut self, doc_id: &DocumentId, data: &Value) {
        for field in &self.fields {
            if let Some(Value::String(text)) = get_field(data, field) {
                for term in tokenize(text) {
                    if let Some(ids) = self.terms.get_mut(&term) {
                        ids.remove(doc_id);
                        if ids.is_empty() {
                            self.terms.remove(&term);
                        }
                    }
                }
            }
        }
    }

    /// Search for documents matching all terms
    pub fn search(&self, query: &str) -> Vec<DocumentId> {
        let terms = tokenize(query);
        if terms.is_empty() {
            return vec![];
        }

        let mut result: Option<HashSet<DocumentId>> = None;

        for term in &terms {
            let matching = self
                .terms
                .get(term)
                .cloned()
                .unwrap_or_default();

            result = Some(match result {
                Some(existing) => existing.intersection(&matching).cloned().collect(),
                None => matching,
            });
        }

        result.unwrap_or_default().into_iter().collect()
    }

    /// Search for documents matching any term
    pub fn search_any(&self, query: &str) -> Vec<DocumentId> {
        let terms = tokenize(query);
        let mut result = HashSet::new();

        for term in &terms {
            if let Some(ids) = self.terms.get(term) {
                result.extend(ids.iter().cloned());
            }
        }

        result.into_iter().collect()
    }

    /// Get number of distinct terms
    pub fn term_count(&self) -> usize {
        self.terms.len()
    }
}

/// Tokenize text into lowercase terms
fn tokenize(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split_whitespace()
        .filter(|w| w.len() >= 2)
        .map(|w| w.trim_matches(|c: char| !c.is_alphanumeric()).to_string())
        .filter(|w| !w.is_empty())
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::document::document::DocumentId;
    use serde_json::json;

    fn doc_id(s: &str) -> DocumentId {
        DocumentId::String(s.to_string())
    }

    #[test]
    fn test_field_index_insert_find() {
        let mut idx = FieldIndex::new("name_idx".into(), "name".into(), false);
        let data = json!({"name": "Alice", "age": 30});
        idx.insert(&doc_id("1"), &data).unwrap();

        let results = idx.find_eq(&json!("Alice"));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], doc_id("1"));
    }

    #[test]
    fn test_field_index_unique_violation() {
        let mut idx = FieldIndex::new("email_idx".into(), "email".into(), true);
        let data1 = json!({"email": "alice@test.com"});
        let data2 = json!({"email": "alice@test.com"});
        idx.insert(&doc_id("1"), &data1).unwrap();
        assert!(idx.insert(&doc_id("2"), &data2).is_err());
    }

    #[test]
    fn test_field_index_range() {
        let mut idx = FieldIndex::new("age_idx".into(), "age".into(), false);
        for i in 0..10 {
            let data = json!({"age": i});
            idx.insert(&doc_id(&i.to_string()), &data).unwrap();
        }

        let results = idx.find_range(Some(&json!(3)), Some(&json!(7)), true, false);
        assert_eq!(results.len(), 4); // 3, 4, 5, 6
    }

    #[test]
    fn test_field_index_remove() {
        let mut idx = FieldIndex::new("name_idx".into(), "name".into(), false);
        let data = json!({"name": "Alice"});
        idx.insert(&doc_id("1"), &data).unwrap();
        idx.remove(&doc_id("1"), &data);
        assert!(idx.find_eq(&json!("Alice")).is_empty());
    }

    #[test]
    fn test_compound_index() {
        let mut idx = CompoundFieldIndex::new(
            "name_age".into(),
            vec!["name".into(), "age".into()],
            false,
        );
        let data = json!({"name": "Alice", "age": 30});
        idx.insert(&doc_id("1"), &data).unwrap();

        let results = idx.find_eq(&[json!("Alice"), json!(30)]);
        assert_eq!(results.len(), 1);

        let results = idx.find_eq(&[json!("Alice"), json!(25)]);
        assert!(results.is_empty());
    }

    #[test]
    fn test_array_index() {
        let mut idx = ArrayIndex::new("tags_idx".into(), "tags".into());
        let data = json!({"tags": ["rust", "database", "kv"]});
        idx.insert(&doc_id("1"), &data);

        let results = idx.find_contains(&json!("rust"));
        assert_eq!(results.len(), 1);

        let results = idx.find_contains(&json!("python"));
        assert!(results.is_empty());
    }

    #[test]
    fn test_text_index() {
        let mut idx = TextIndex::new("content_text".into(), vec!["title".into(), "body".into()]);

        let data1 = json!({"title": "Hello World", "body": "This is a test document"});
        let data2 = json!({"title": "Goodbye World", "body": "Another test"});
        idx.insert(&doc_id("1"), &data1);
        idx.insert(&doc_id("2"), &data2);

        // Match all terms
        let results = idx.search("hello world");
        assert_eq!(results.len(), 1);

        // Match any term
        let results = idx.search_any("hello goodbye");
        assert_eq!(results.len(), 2);

        // Common term
        let results = idx.search("test");
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_text_index_remove() {
        let mut idx = TextIndex::new("content".into(), vec!["title".into()]);
        let data = json!({"title": "Hello World"});
        idx.insert(&doc_id("1"), &data);
        idx.remove(&doc_id("1"), &data);
        assert!(idx.search("hello").is_empty());
    }

    #[test]
    fn test_nested_field_index() {
        let mut idx = FieldIndex::new("city_idx".into(), "address.city".into(), false);
        let data = json!({"address": {"city": "New York"}});
        idx.insert(&doc_id("1"), &data).unwrap();

        let results = idx.find_eq(&json!("New York"));
        assert_eq!(results.len(), 1);
    }
}
