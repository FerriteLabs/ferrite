//! Collection management for document store

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use super::document::{Document, DocumentId};
use super::index::DocumentIndex;
use super::query::DocumentQuery;
use super::validation::{JsonSchema, ValidationLevel};
use super::{DeleteResult, DocumentStoreError, IndexOptions, UpdateOperation, UpdateResult};

/// Collection options
#[derive(Debug, Clone, Default)]
pub struct CollectionOptions {
    /// Capped collection max documents
    pub capped_max_documents: Option<u64>,
    /// Capped collection max size in bytes
    pub capped_max_size: Option<u64>,
    /// JSON schema for validation
    pub validator: Option<JsonSchema>,
    /// Validation level
    pub validation_level: ValidationLevel,
    /// Validation action
    pub validation_action: ValidationAction,
    /// Enable change streams for this collection
    pub enable_change_streams: bool,
}

/// Validation action
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ValidationAction {
    /// Log warning on validation failure
    Warn,
    /// Reject document on validation failure
    #[default]
    Error,
}

/// A collection of documents
#[derive(Debug, Clone)]
pub struct Collection {
    /// Collection name
    name: String,
    /// Documents by ID
    documents: HashMap<DocumentId, Document>,
    /// Indexes
    indexes: HashMap<String, DocumentIndex>,
    /// Collection options
    options: CollectionOptions,
    /// Document count
    document_count: u64,
    /// Total data size
    data_size: u64,
    /// Creation time
    created_at: u64,
}

impl Collection {
    /// Create a new collection
    pub fn new(name: String, options: CollectionOptions) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut collection = Self {
            name,
            documents: HashMap::new(),
            indexes: HashMap::new(),
            options,
            document_count: 0,
            data_size: 0,
            created_at: now,
        };

        // Create default _id index
        let id_index = DocumentIndex::new("_id_".to_string(), vec!["_id".to_string()], true, false);
        collection.indexes.insert("_id_".to_string(), id_index);

        collection
    }

    /// Get collection name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get document count
    pub fn count_documents(&self) -> u64 {
        self.document_count
    }

    /// Get total data size
    pub fn data_size(&self) -> u64 {
        self.data_size
    }

    /// Insert a document
    pub fn insert(&mut self, doc: Document) -> Result<(), DocumentStoreError> {
        // Validate document if schema is set
        if let Some(ref schema) = self.options.validator {
            if let Err(e) = schema.validate(&doc.data) {
                match self.options.validation_action {
                    ValidationAction::Error => return Err(e),
                    ValidationAction::Warn => {
                        // Log warning but continue
                        eprintln!("Validation warning: {}", e);
                    }
                }
            }
        }

        // Check for duplicate key
        if self.documents.contains_key(&doc.id) {
            return Err(DocumentStoreError::DuplicateKey(format!(
                "Duplicate key error: {}",
                doc.id
            )));
        }

        // Check unique indexes
        for index in self.indexes.values() {
            if index.is_unique() {
                if let Some(existing_id) = index.find_exact(&doc) {
                    if existing_id != doc.id {
                        return Err(DocumentStoreError::DuplicateKey(format!(
                            "Duplicate key error on index {}",
                            index.name()
                        )));
                    }
                }
            }
        }

        // Check capped collection constraints
        if let Some(max_docs) = self.options.capped_max_documents {
            while self.document_count >= max_docs {
                self.remove_oldest_document();
            }
        }

        // Calculate document size
        let doc_size = serde_json::to_vec(&doc.data)
            .map(|v| v.len() as u64)
            .unwrap_or(0);

        if let Some(max_size) = self.options.capped_max_size {
            while self.data_size + doc_size > max_size && self.document_count > 0 {
                self.remove_oldest_document();
            }
        }

        // Update indexes
        for index in self.indexes.values_mut() {
            index.insert(&doc);
        }

        // Insert document
        self.documents.insert(doc.id.clone(), doc);
        self.document_count += 1;
        self.data_size += doc_size;

        Ok(())
    }

    /// Remove oldest document (for capped collections)
    fn remove_oldest_document(&mut self) {
        if let Some(oldest_id) = self
            .documents
            .values()
            .min_by_key(|d| d.created_at)
            .map(|d| d.id.clone())
        {
            if let Some(doc) = self.documents.remove(&oldest_id) {
                // Remove from indexes
                for index in self.indexes.values_mut() {
                    index.remove(&doc);
                }

                let doc_size = serde_json::to_vec(&doc.data)
                    .map(|v| v.len() as u64)
                    .unwrap_or(0);
                self.document_count = self.document_count.saturating_sub(1);
                self.data_size = self.data_size.saturating_sub(doc_size);
            }
        }
    }

    /// Find documents matching a query
    pub fn find(&self, query: &DocumentQuery) -> Result<Vec<Document>, DocumentStoreError> {
        // Try to use index
        if let Some(index_result) = self.try_index_scan(query) {
            return Ok(index_result);
        }

        // Fall back to collection scan
        let results: Vec<Document> = self
            .documents
            .values()
            .filter(|doc| query.matches(doc))
            .cloned()
            .collect();

        Ok(results)
    }

    /// Try to use an index for the query
    fn try_index_scan(&self, query: &DocumentQuery) -> Option<Vec<Document>> {
        // Get fields used in the query
        let query_fields = query.get_fields();

        // Find the best index
        let best_index = self
            .indexes
            .values()
            .filter(|idx| {
                // Index must cover at least one query field as prefix
                idx.fields()
                    .first()
                    .map(|f| query_fields.contains(f))
                    .unwrap_or(false)
            })
            .max_by_key(|idx| {
                // Prefer indexes that cover more query fields
                idx.fields()
                    .iter()
                    .filter(|f| query_fields.contains(*f))
                    .count()
            })?;

        // Use the index to get candidate document IDs
        let candidate_ids = best_index.query(query);

        // Fetch and filter documents
        let results: Vec<Document> = candidate_ids
            .iter()
            .filter_map(|id| self.documents.get(id))
            .filter(|doc| query.matches(doc))
            .cloned()
            .collect();

        Some(results)
    }

    /// Count documents matching a query
    pub fn count(&self, query: &DocumentQuery) -> Result<u64, DocumentStoreError> {
        if query.is_empty() {
            return Ok(self.document_count);
        }

        let count = self.find(query)?.len() as u64;
        Ok(count)
    }

    /// Get distinct values for a field
    pub fn distinct(
        &self,
        field: &str,
        query: Option<&DocumentQuery>,
    ) -> Result<Vec<serde_json::Value>, DocumentStoreError> {
        let mut values = std::collections::HashSet::new();

        for doc in self.documents.values() {
            // Check query if provided
            if let Some(q) = query {
                if !q.matches(doc) {
                    continue;
                }
            }

            if let Some(value) = doc.get_field(field) {
                // Convert to string for HashSet (not ideal but works)
                let key = serde_json::to_string(value).unwrap_or_default();
                if !values.contains(&key) {
                    values.insert(key);
                }
            }
        }

        // Convert back to Values
        let result: Vec<serde_json::Value> = values
            .into_iter()
            .filter_map(|s| serde_json::from_str(&s).ok())
            .collect();

        Ok(result)
    }

    /// Update multiple documents
    pub fn update_many(
        &mut self,
        query: &DocumentQuery,
        update: &UpdateOperation,
    ) -> Result<UpdateResult, DocumentStoreError> {
        let matching_ids: Vec<DocumentId> = self
            .documents
            .values()
            .filter(|doc| query.matches(doc))
            .map(|doc| doc.id.clone())
            .collect();

        let matched_count = matching_ids.len() as u64;
        let mut modified_count = 0u64;

        for id in matching_ids {
            if let Some(doc) = self.documents.get_mut(&id) {
                // Remove from indexes before update
                for index in self.indexes.values_mut() {
                    index.remove(doc);
                }

                // Apply update
                if Self::apply_update(doc, update)? {
                    modified_count += 1;
                }

                // Re-add to indexes after update
                for index in self.indexes.values_mut() {
                    index.insert(doc);
                }
            }
        }

        Ok(UpdateResult {
            matched_count,
            modified_count,
            upserted_id: None,
        })
    }

    /// Update a single document
    pub fn update_one(
        &mut self,
        query: &DocumentQuery,
        update: &UpdateOperation,
    ) -> Result<UpdateResult, DocumentStoreError> {
        let matching_id = self
            .documents
            .values()
            .find(|doc| query.matches(doc))
            .map(|doc| doc.id.clone());

        if let Some(id) = matching_id {
            if let Some(doc) = self.documents.get_mut(&id) {
                // Remove from indexes before update
                for index in self.indexes.values_mut() {
                    index.remove(doc);
                }

                // Apply update
                let modified = Self::apply_update(doc, update)?;

                // Re-add to indexes after update
                for index in self.indexes.values_mut() {
                    index.insert(doc);
                }

                return Ok(UpdateResult {
                    matched_count: 1,
                    modified_count: if modified { 1 } else { 0 },
                    upserted_id: None,
                });
            }
        }

        Ok(UpdateResult {
            matched_count: 0,
            modified_count: 0,
            upserted_id: None,
        })
    }

    /// Apply an update operation to a document
    fn apply_update(
        doc: &mut Document,
        update: &UpdateOperation,
    ) -> Result<bool, DocumentStoreError> {
        let mut modified = false;

        // $set
        if let Some(ref set_fields) = update.set {
            for (field, value) in set_fields {
                doc.set_field(field, value.clone())?;
                modified = true;
            }
        }

        // $unset
        if let Some(ref unset_fields) = update.unset {
            for field in unset_fields {
                if doc.unset_field(field).is_some() {
                    modified = true;
                }
            }
        }

        // $inc
        if let Some(ref inc_fields) = update.inc {
            for (field, inc_value) in inc_fields {
                if let Some(current) = doc.get_field(field) {
                    if let Some(num) = current.as_f64() {
                        let new_value = num + inc_value;
                        doc.set_field(field, serde_json::json!(new_value))?;
                        modified = true;
                    }
                } else {
                    // If field doesn't exist, create it with the increment value
                    doc.set_field(field, serde_json::json!(*inc_value))?;
                    modified = true;
                }
            }
        }

        // $mul
        if let Some(ref mul_fields) = update.mul {
            for (field, mul_value) in mul_fields {
                if let Some(current) = doc.get_field(field) {
                    if let Some(num) = current.as_f64() {
                        let new_value = num * mul_value;
                        doc.set_field(field, serde_json::json!(new_value))?;
                        modified = true;
                    }
                } else {
                    // If field doesn't exist, create it with 0
                    doc.set_field(field, serde_json::json!(0))?;
                    modified = true;
                }
            }
        }

        // $push
        if let Some(ref push_fields) = update.push {
            for (field, value) in push_fields {
                let current = doc.get_field(field).cloned();
                match current {
                    Some(serde_json::Value::Array(mut arr)) => {
                        arr.push(value.clone());
                        doc.set_field(field, serde_json::Value::Array(arr))?;
                        modified = true;
                    }
                    None => {
                        doc.set_field(field, serde_json::json!([value.clone()]))?;
                        modified = true;
                    }
                    _ => {
                        return Err(DocumentStoreError::InvalidUpdate(format!(
                            "Cannot $push to non-array field: {}",
                            field
                        )));
                    }
                }
            }
        }

        // $pull
        if let Some(ref pull_fields) = update.pull {
            for (field, value) in pull_fields {
                if let Some(serde_json::Value::Array(arr)) = doc.get_field(field).cloned() {
                    let new_arr: Vec<serde_json::Value> =
                        arr.into_iter().filter(|v| v != value).collect();
                    doc.set_field(field, serde_json::Value::Array(new_arr))?;
                    modified = true;
                }
            }
        }

        // $addToSet
        if let Some(ref add_to_set_fields) = update.add_to_set {
            for (field, value) in add_to_set_fields {
                let current = doc.get_field(field).cloned();
                match current {
                    Some(serde_json::Value::Array(mut arr)) => {
                        if !arr.contains(value) {
                            arr.push(value.clone());
                            doc.set_field(field, serde_json::Value::Array(arr))?;
                            modified = true;
                        }
                    }
                    None => {
                        doc.set_field(field, serde_json::json!([value.clone()]))?;
                        modified = true;
                    }
                    _ => {
                        return Err(DocumentStoreError::InvalidUpdate(format!(
                            "Cannot $addToSet to non-array field: {}",
                            field
                        )));
                    }
                }
            }
        }

        // $min
        if let Some(ref min_fields) = update.min {
            for (field, value) in min_fields {
                if let Some(current) = doc.get_field(field) {
                    if Self::compare_values(value, current) < 0 {
                        doc.set_field(field, value.clone())?;
                        modified = true;
                    }
                } else {
                    doc.set_field(field, value.clone())?;
                    modified = true;
                }
            }
        }

        // $max
        if let Some(ref max_fields) = update.max {
            for (field, value) in max_fields {
                if let Some(current) = doc.get_field(field) {
                    if Self::compare_values(value, current) > 0 {
                        doc.set_field(field, value.clone())?;
                        modified = true;
                    }
                } else {
                    doc.set_field(field, value.clone())?;
                    modified = true;
                }
            }
        }

        // $rename
        if let Some(ref rename_fields) = update.rename {
            for (old_name, new_name) in rename_fields {
                if let Some(value) = doc.unset_field(old_name) {
                    doc.set_field(new_name, value)?;
                    modified = true;
                }
            }
        }

        // $currentDate
        if let Some(ref current_date_fields) = update.current_date {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            for field in current_date_fields {
                doc.set_field(field, serde_json::json!(now))?;
                modified = true;
            }
        }

        // Update metadata
        if modified {
            doc.version += 1;
            doc.updated_at = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
        }

        Ok(modified)
    }

    /// Compare two JSON values for ordering
    fn compare_values(a: &serde_json::Value, b: &serde_json::Value) -> i32 {
        match (a, b) {
            (serde_json::Value::Number(n1), serde_json::Value::Number(n2)) => {
                let f1 = n1.as_f64().unwrap_or(0.0);
                let f2 = n2.as_f64().unwrap_or(0.0);
                f1.partial_cmp(&f2).map(|o| o as i32).unwrap_or(0)
            }
            (serde_json::Value::String(s1), serde_json::Value::String(s2)) => s1.cmp(s2) as i32,
            _ => 0,
        }
    }

    /// Delete multiple documents
    pub fn delete_many(
        &mut self,
        query: &DocumentQuery,
    ) -> Result<DeleteResult, DocumentStoreError> {
        let matching_ids: Vec<DocumentId> = self
            .documents
            .values()
            .filter(|doc| query.matches(doc))
            .map(|doc| doc.id.clone())
            .collect();

        let deleted_count = matching_ids.len() as u64;

        for id in matching_ids {
            if let Some(doc) = self.documents.remove(&id) {
                // Remove from indexes
                for index in self.indexes.values_mut() {
                    index.remove(&doc);
                }

                let doc_size = serde_json::to_vec(&doc.data)
                    .map(|v| v.len() as u64)
                    .unwrap_or(0);
                self.document_count = self.document_count.saturating_sub(1);
                self.data_size = self.data_size.saturating_sub(doc_size);
            }
        }

        Ok(DeleteResult { deleted_count })
    }

    /// Delete a single document
    pub fn delete_one(
        &mut self,
        query: &DocumentQuery,
    ) -> Result<DeleteResult, DocumentStoreError> {
        let matching_id = self
            .documents
            .values()
            .find(|doc| query.matches(doc))
            .map(|doc| doc.id.clone());

        if let Some(id) = matching_id {
            if let Some(doc) = self.documents.remove(&id) {
                // Remove from indexes
                for index in self.indexes.values_mut() {
                    index.remove(&doc);
                }

                let doc_size = serde_json::to_vec(&doc.data)
                    .map(|v| v.len() as u64)
                    .unwrap_or(0);
                self.document_count = self.document_count.saturating_sub(1);
                self.data_size = self.data_size.saturating_sub(doc_size);

                return Ok(DeleteResult { deleted_count: 1 });
            }
        }

        Ok(DeleteResult { deleted_count: 0 })
    }

    /// Create an index
    pub fn create_index(
        &mut self,
        keys: serde_json::Value,
        options: IndexOptions,
    ) -> Result<String, DocumentStoreError> {
        let fields = Self::parse_index_keys(&keys)?;

        let name = options.name.unwrap_or_else(|| {
            fields
                .iter()
                .map(|f| f.replace('.', "_"))
                .collect::<Vec<_>>()
                .join("_")
                + "_1"
        });

        if self.indexes.contains_key(&name) {
            return Err(DocumentStoreError::IndexError(format!(
                "Index already exists: {}",
                name
            )));
        }

        let mut index = DocumentIndex::new(name.clone(), fields, options.unique, options.sparse);

        // Index existing documents
        for doc in self.documents.values() {
            index.insert(doc);
        }

        self.indexes.insert(name.clone(), index);
        Ok(name)
    }

    /// Parse index key specification
    fn parse_index_keys(keys: &serde_json::Value) -> Result<Vec<String>, DocumentStoreError> {
        let obj = keys
            .as_object()
            .ok_or_else(|| DocumentStoreError::IndexError("Index keys must be an object".into()))?;

        let fields: Vec<String> = obj.keys().cloned().collect();

        if fields.is_empty() {
            return Err(DocumentStoreError::IndexError(
                "Index must have at least one field".into(),
            ));
        }

        Ok(fields)
    }

    /// Drop an index
    pub fn drop_index(&mut self, name: &str) -> Result<(), DocumentStoreError> {
        if name == "_id_" {
            return Err(DocumentStoreError::IndexError(
                "Cannot drop _id index".into(),
            ));
        }

        if self.indexes.remove(name).is_none() {
            return Err(DocumentStoreError::IndexError(format!(
                "Index not found: {}",
                name
            )));
        }

        Ok(())
    }

    /// List indexes
    pub fn list_indexes(&self) -> Vec<IndexInfo> {
        self.indexes
            .values()
            .map(|idx| IndexInfo {
                name: idx.name().to_string(),
                fields: idx.fields().to_vec(),
                unique: idx.is_unique(),
                sparse: idx.is_sparse(),
            })
            .collect()
    }

    /// Get all documents (for iteration)
    pub fn documents(&self) -> impl Iterator<Item = &Document> {
        self.documents.values()
    }
}

/// Index information
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// Index name
    pub name: String,
    /// Indexed fields
    pub fields: Vec<String>,
    /// Unique constraint
    pub unique: bool,
    /// Sparse index
    pub sparse: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_insert_and_find() {
        let mut collection = Collection::new("test".to_string(), CollectionOptions::default());

        let doc = Document::from_json(json!({
            "name": "Alice",
            "age": 30
        }))
        .unwrap();

        collection.insert(doc).unwrap();

        let query = DocumentQuery::from_json(json!({ "name": "Alice" })).unwrap();
        let results = collection.find(&query).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get_field("name"), Some(&json!("Alice")));
    }

    #[test]
    fn test_duplicate_key_error() {
        let mut collection = Collection::new("test".to_string(), CollectionOptions::default());

        let doc1 = Document::from_json(json!({ "_id": "123", "name": "Alice" })).unwrap();
        let doc2 = Document::from_json(json!({ "_id": "123", "name": "Bob" })).unwrap();

        collection.insert(doc1).unwrap();
        let result = collection.insert(doc2);

        assert!(matches!(result, Err(DocumentStoreError::DuplicateKey(_))));
    }

    #[test]
    fn test_update() {
        let mut collection = Collection::new("test".to_string(), CollectionOptions::default());

        let doc = Document::from_json(json!({
            "name": "Alice",
            "age": 30
        }))
        .unwrap();
        collection.insert(doc).unwrap();

        let query = DocumentQuery::from_json(json!({ "name": "Alice" })).unwrap();
        let update = UpdateOperation::from_json(json!({
            "$set": { "age": 31 },
            "$inc": { "visits": 1 }
        }))
        .unwrap();

        let result = collection.update_one(&query, &update).unwrap();
        assert_eq!(result.matched_count, 1);
        assert_eq!(result.modified_count, 1);

        let results = collection.find(&query).unwrap();
        assert_eq!(results[0].get_field("age"), Some(&json!(31)));
        // $inc operation creates/updates numeric fields as floats
        assert_eq!(results[0].get_field("visits"), Some(&json!(1.0)));
    }

    #[test]
    fn test_delete() {
        let mut collection = Collection::new("test".to_string(), CollectionOptions::default());

        for i in 0..5 {
            let doc = Document::from_json(json!({ "num": i })).unwrap();
            collection.insert(doc).unwrap();
        }

        let query = DocumentQuery::from_json(json!({ "num": { "$gte": 3 } })).unwrap();
        let result = collection.delete_many(&query).unwrap();

        assert_eq!(result.deleted_count, 2);
        assert_eq!(collection.count_documents(), 3);
    }

    #[test]
    fn test_capped_collection() {
        let options = CollectionOptions {
            capped_max_documents: Some(3),
            ..Default::default()
        };
        let mut collection = Collection::new("test".to_string(), options);

        for i in 0..5 {
            let doc = Document::from_json(json!({ "num": i })).unwrap();
            collection.insert(doc).unwrap();
        }

        assert_eq!(collection.count_documents(), 3);
    }

    #[test]
    fn test_create_index() {
        let mut collection = Collection::new("test".to_string(), CollectionOptions::default());

        let name = collection
            .create_index(json!({ "name": 1 }), IndexOptions::default())
            .unwrap();

        assert_eq!(name, "name_1");
        assert_eq!(collection.list_indexes().len(), 2); // _id_ + name_1
    }
}
