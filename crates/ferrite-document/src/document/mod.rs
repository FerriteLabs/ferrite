//! JSON Document Store - MongoDB-compatible document database
//!
//! This module provides a full-featured document database supporting:
//! - Schemaless JSON document storage
//! - Rich query language (MongoDB-compatible)
//! - Secondary indexes (single-field, compound, multi-key)
//! - Aggregation pipelines
//! - JSON Schema validation
//! - Change streams
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Document Store                            │
//! ├─────────────────────────────────────────────────────────────┤
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
//! │  │ Collection  │  │ Collection  │  │ Collection  │   ...   │
//! │  │   users     │  │   orders    │  │   products  │         │
//! │  └─────────────┘  └─────────────┘  └─────────────┘         │
//! ├─────────────────────────────────────────────────────────────┤
//! │                    Index Manager                             │
//! │  ┌─────────┐ ┌───────────┐ ┌──────────┐ ┌─────────────┐    │
//! │  │ B-Tree  │ │ Compound  │ │ Multi-Key│ │    Text     │    │
//! │  │ Index   │ │  Index    │ │  Index   │ │   Index     │    │
//! │  └─────────┘ └───────────┘ └──────────┘ └─────────────┘    │
//! ├─────────────────────────────────────────────────────────────┤
//! │              Query Engine & Aggregation                      │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use ferrite::document::{DocumentStore, Document};
//! use serde_json::json;
//!
//! let store = DocumentStore::new(config);
//!
//! // Create collection
//! store.create_collection("users").await?;
//!
//! // Insert document
//! let doc = json!({
//!     "name": "Alice",
//!     "email": "alice@example.com",
//!     "age": 30,
//!     "tags": ["developer", "rust"]
//! });
//! store.insert_one("users", doc).await?;
//!
//! // Query documents
//! let query = json!({
//!     "age": { "$gte": 25 },
//!     "tags": "rust"
//! });
//! let results = store.find("users", query).await?;
//!
//! // Aggregation pipeline
//! let pipeline = vec![
//!     json!({ "$match": { "age": { "$gte": 18 } } }),
//!     json!({ "$group": { "_id": "$city", "count": { "$sum": 1 } } }),
//!     json!({ "$sort": { "count": -1 } })
//! ];
//! let results = store.aggregate("users", pipeline).await?;
//! ```

pub mod aggregation;
pub mod collection;
pub mod doc_indexes;
#[allow(clippy::module_inception)]
pub mod document;
pub mod index;
pub mod jsonpath;
pub mod patch;
pub mod projection;
pub mod query;
pub mod schema;
pub mod validation;

pub use aggregation::{AggregationPipeline, PipelineStage};
pub use collection::{Collection, CollectionOptions};
pub use doc_indexes::{ArrayIndex, CompoundFieldIndex, FieldIndex, TextIndex};
pub use document::{Document, DocumentId, ObjectId};
pub use index::{DocumentIndex, IndexType};
pub use jsonpath::{evaluate as jsonpath_eval, parse as jsonpath_parse, JsonPath};
pub use patch::{apply_merge_patch, apply_patch, PatchOp};
pub use projection::Projection;
pub use query::{DocumentQuery, QueryBuilder, QueryOperator};
pub use schema::{SchemaValidator, ValidationError};
pub use validation::{JsonSchema, ValidationLevel};

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Document store configuration
#[derive(Debug, Clone)]
pub struct DocumentStoreConfig {
    /// Maximum document size in bytes
    pub max_document_size: usize,
    /// Maximum nesting depth for documents
    pub max_nesting_depth: usize,
    /// Enable write concern
    pub write_concern: WriteConcern,
    /// Enable change streams
    pub enable_change_streams: bool,
    /// Default validation level
    pub default_validation_level: ValidationLevel,
}

impl Default for DocumentStoreConfig {
    fn default() -> Self {
        Self {
            max_document_size: 16 * 1024 * 1024, // 16MB like MongoDB
            max_nesting_depth: 100,
            write_concern: WriteConcern::Acknowledged,
            enable_change_streams: true,
            default_validation_level: ValidationLevel::Strict,
        }
    }
}

/// Write concern levels
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteConcern {
    /// No acknowledgment
    Unacknowledged,
    /// Acknowledged by primary
    Acknowledged,
    /// Acknowledged by majority
    Majority,
    /// Acknowledged by specific number of nodes
    Nodes(u32),
}

/// Document store metrics
#[derive(Debug, Default)]
pub struct DocumentStoreMetrics {
    /// Total documents stored
    pub total_documents: u64,
    /// Total collections
    pub total_collections: u64,
    /// Total indexes
    pub total_indexes: u64,
    /// Insert operations
    pub inserts: u64,
    /// Update operations
    pub updates: u64,
    /// Delete operations
    pub deletes: u64,
    /// Query operations
    pub queries: u64,
    /// Aggregation operations
    pub aggregations: u64,
    /// Index scans
    pub index_scans: u64,
    /// Collection scans
    pub collection_scans: u64,
}

/// Main document store engine
pub struct DocumentStore {
    /// Collections by name
    collections: Arc<RwLock<HashMap<String, Collection>>>,
    /// Configuration
    config: DocumentStoreConfig,
    /// Metrics
    metrics: Arc<RwLock<DocumentStoreMetrics>>,
    /// Change stream subscribers
    change_streams: Arc<RwLock<Vec<ChangeStreamSubscriber>>>,
}

impl DocumentStore {
    /// Create a new document store
    pub fn new(config: DocumentStoreConfig) -> Self {
        Self {
            collections: Arc::new(RwLock::new(HashMap::new())),
            config,
            metrics: Arc::new(RwLock::new(DocumentStoreMetrics::default())),
            change_streams: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Create a new collection
    pub async fn create_collection(
        &self,
        name: &str,
        options: Option<CollectionOptions>,
    ) -> Result<(), DocumentStoreError> {
        let mut collections = self.collections.write().await;

        if collections.contains_key(name) {
            return Err(DocumentStoreError::CollectionExists(name.to_string()));
        }

        let collection = Collection::new(name.to_string(), options.unwrap_or_default());
        collections.insert(name.to_string(), collection);

        let mut metrics = self.metrics.write().await;
        metrics.total_collections += 1;

        Ok(())
    }

    /// Drop a collection
    pub async fn drop_collection(&self, name: &str) -> Result<(), DocumentStoreError> {
        let mut collections = self.collections.write().await;

        if collections.remove(name).is_none() {
            return Err(DocumentStoreError::CollectionNotFound(name.to_string()));
        }

        let mut metrics = self.metrics.write().await;
        metrics.total_collections = metrics.total_collections.saturating_sub(1);

        Ok(())
    }

    /// List all collections
    pub async fn list_collections(&self) -> Vec<String> {
        let collections = self.collections.read().await;
        collections.keys().cloned().collect()
    }

    /// Get a collection
    pub async fn get_collection(&self, name: &str) -> Option<Collection> {
        let collections = self.collections.read().await;
        collections.get(name).cloned()
    }

    /// Insert a single document
    pub async fn insert_one(
        &self,
        collection: &str,
        document: serde_json::Value,
    ) -> Result<DocumentId, DocumentStoreError> {
        self.validate_document(&document)?;

        let mut collections = self.collections.write().await;
        let coll = collections
            .get_mut(collection)
            .ok_or_else(|| DocumentStoreError::CollectionNotFound(collection.to_string()))?;

        let doc = Document::from_json(document)?;
        let id = doc.id.clone();

        coll.insert(doc.clone())?;

        // Update metrics
        let mut metrics = self.metrics.write().await;
        metrics.inserts += 1;
        metrics.total_documents += 1;

        // Notify change streams
        if self.config.enable_change_streams {
            self.notify_change(ChangeEvent::Insert {
                collection: collection.to_string(),
                document: doc,
            })
            .await;
        }

        Ok(id)
    }

    /// Insert multiple documents
    pub async fn insert_many(
        &self,
        collection: &str,
        documents: Vec<serde_json::Value>,
    ) -> Result<Vec<DocumentId>, DocumentStoreError> {
        let mut ids = Vec::with_capacity(documents.len());

        for doc in documents {
            let id = self.insert_one(collection, doc).await?;
            ids.push(id);
        }

        Ok(ids)
    }

    /// Find documents matching a query
    pub async fn find(
        &self,
        collection: &str,
        query: serde_json::Value,
    ) -> Result<Vec<Document>, DocumentStoreError> {
        let collections = self.collections.read().await;
        let coll = collections
            .get(collection)
            .ok_or_else(|| DocumentStoreError::CollectionNotFound(collection.to_string()))?;

        let parsed_query = DocumentQuery::from_json(query)?;
        let results = coll.find(&parsed_query)?;

        // Update metrics
        let mut metrics = self.metrics.write().await;
        metrics.queries += 1;

        Ok(results)
    }

    /// Find a single document
    pub async fn find_one(
        &self,
        collection: &str,
        query: serde_json::Value,
    ) -> Result<Option<Document>, DocumentStoreError> {
        let mut results = self.find(collection, query).await?;
        Ok(results.pop())
    }

    /// Update documents matching a query
    pub async fn update_many(
        &self,
        collection: &str,
        query: serde_json::Value,
        update: serde_json::Value,
    ) -> Result<UpdateResult, DocumentStoreError> {
        let mut collections = self.collections.write().await;
        let coll = collections
            .get_mut(collection)
            .ok_or_else(|| DocumentStoreError::CollectionNotFound(collection.to_string()))?;

        let parsed_query = DocumentQuery::from_json(query)?;
        let update_op = UpdateOperation::from_json(update)?;

        let result = coll.update_many(&parsed_query, &update_op)?;

        // Update metrics
        let mut metrics = self.metrics.write().await;
        metrics.updates += result.modified_count;

        Ok(result)
    }

    /// Update a single document
    pub async fn update_one(
        &self,
        collection: &str,
        query: serde_json::Value,
        update: serde_json::Value,
    ) -> Result<UpdateResult, DocumentStoreError> {
        let mut collections = self.collections.write().await;
        let coll = collections
            .get_mut(collection)
            .ok_or_else(|| DocumentStoreError::CollectionNotFound(collection.to_string()))?;

        let parsed_query = DocumentQuery::from_json(query)?;
        let update_op = UpdateOperation::from_json(update)?;

        let result = coll.update_one(&parsed_query, &update_op)?;

        // Update metrics
        let mut metrics = self.metrics.write().await;
        metrics.updates += result.modified_count;

        Ok(result)
    }

    /// Delete documents matching a query
    pub async fn delete_many(
        &self,
        collection: &str,
        query: serde_json::Value,
    ) -> Result<DeleteResult, DocumentStoreError> {
        let mut collections = self.collections.write().await;
        let coll = collections
            .get_mut(collection)
            .ok_or_else(|| DocumentStoreError::CollectionNotFound(collection.to_string()))?;

        let parsed_query = DocumentQuery::from_json(query)?;
        let result = coll.delete_many(&parsed_query)?;

        // Update metrics
        let mut metrics = self.metrics.write().await;
        metrics.deletes += result.deleted_count;
        metrics.total_documents = metrics.total_documents.saturating_sub(result.deleted_count);

        Ok(result)
    }

    /// Delete a single document
    pub async fn delete_one(
        &self,
        collection: &str,
        query: serde_json::Value,
    ) -> Result<DeleteResult, DocumentStoreError> {
        let mut collections = self.collections.write().await;
        let coll = collections
            .get_mut(collection)
            .ok_or_else(|| DocumentStoreError::CollectionNotFound(collection.to_string()))?;

        let parsed_query = DocumentQuery::from_json(query)?;
        let result = coll.delete_one(&parsed_query)?;

        // Update metrics
        let mut metrics = self.metrics.write().await;
        metrics.deletes += result.deleted_count;
        metrics.total_documents = metrics.total_documents.saturating_sub(result.deleted_count);

        Ok(result)
    }

    /// Run an aggregation pipeline
    pub async fn aggregate(
        &self,
        collection: &str,
        pipeline: Vec<serde_json::Value>,
    ) -> Result<Vec<Document>, DocumentStoreError> {
        let collections = self.collections.read().await;
        let coll = collections
            .get(collection)
            .ok_or_else(|| DocumentStoreError::CollectionNotFound(collection.to_string()))?;

        let agg_pipeline = AggregationPipeline::from_json(pipeline)?;
        let results = agg_pipeline.execute(coll)?;

        // Update metrics
        let mut metrics = self.metrics.write().await;
        metrics.aggregations += 1;

        Ok(results)
    }

    /// Create an index on a collection
    pub async fn create_index(
        &self,
        collection: &str,
        keys: serde_json::Value,
        options: Option<IndexOptions>,
    ) -> Result<String, DocumentStoreError> {
        let mut collections = self.collections.write().await;
        let coll = collections
            .get_mut(collection)
            .ok_or_else(|| DocumentStoreError::CollectionNotFound(collection.to_string()))?;

        let index_name = coll.create_index(keys, options.unwrap_or_default())?;

        // Update metrics
        let mut metrics = self.metrics.write().await;
        metrics.total_indexes += 1;

        Ok(index_name)
    }

    /// Drop an index
    pub async fn drop_index(
        &self,
        collection: &str,
        index_name: &str,
    ) -> Result<(), DocumentStoreError> {
        let mut collections = self.collections.write().await;
        let coll = collections
            .get_mut(collection)
            .ok_or_else(|| DocumentStoreError::CollectionNotFound(collection.to_string()))?;

        coll.drop_index(index_name)?;

        // Update metrics
        let mut metrics = self.metrics.write().await;
        metrics.total_indexes = metrics.total_indexes.saturating_sub(1);

        Ok(())
    }

    /// Count documents matching a query
    pub async fn count(
        &self,
        collection: &str,
        query: serde_json::Value,
    ) -> Result<u64, DocumentStoreError> {
        let collections = self.collections.read().await;
        let coll = collections
            .get(collection)
            .ok_or_else(|| DocumentStoreError::CollectionNotFound(collection.to_string()))?;

        let parsed_query = DocumentQuery::from_json(query)?;
        let count = coll.count(&parsed_query)?;

        Ok(count)
    }

    /// Get distinct values for a field
    pub async fn distinct(
        &self,
        collection: &str,
        field: &str,
        query: Option<serde_json::Value>,
    ) -> Result<Vec<serde_json::Value>, DocumentStoreError> {
        let collections = self.collections.read().await;
        let coll = collections
            .get(collection)
            .ok_or_else(|| DocumentStoreError::CollectionNotFound(collection.to_string()))?;

        let parsed_query = query.map(DocumentQuery::from_json).transpose()?;
        let values = coll.distinct(field, parsed_query.as_ref())?;

        Ok(values)
    }

    /// Validate a document against config constraints
    fn validate_document(&self, doc: &serde_json::Value) -> Result<(), DocumentStoreError> {
        let doc_size = serde_json::to_vec(doc)
            .map_err(|e| DocumentStoreError::InvalidDocument(e.to_string()))?
            .len();

        if doc_size > self.config.max_document_size {
            return Err(DocumentStoreError::DocumentTooLarge {
                size: doc_size,
                max: self.config.max_document_size,
            });
        }

        let depth = Self::calculate_depth(doc);
        if depth > self.config.max_nesting_depth {
            return Err(DocumentStoreError::NestingTooDeep {
                depth,
                max: self.config.max_nesting_depth,
            });
        }

        Ok(())
    }

    /// Calculate nesting depth of a JSON value
    fn calculate_depth(value: &serde_json::Value) -> usize {
        match value {
            serde_json::Value::Object(map) => {
                1 + map.values().map(Self::calculate_depth).max().unwrap_or(0)
            }
            serde_json::Value::Array(arr) => {
                1 + arr.iter().map(Self::calculate_depth).max().unwrap_or(0)
            }
            _ => 0,
        }
    }

    /// Subscribe to change stream
    pub async fn watch(&self, collection: Option<&str>) -> ChangeStreamHandle {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);

        let subscriber = ChangeStreamSubscriber {
            collection: collection.map(String::from),
            sender: tx,
        };

        let mut streams = self.change_streams.write().await;
        streams.push(subscriber);

        ChangeStreamHandle { receiver: rx }
    }

    /// Notify change stream subscribers
    async fn notify_change(&self, event: ChangeEvent) {
        let streams = self.change_streams.read().await;

        for subscriber in streams.iter() {
            let should_notify = match &subscriber.collection {
                Some(coll) => match &event {
                    ChangeEvent::Insert { collection, .. } => collection == coll,
                    ChangeEvent::Update { collection, .. } => collection == coll,
                    ChangeEvent::Delete { collection, .. } => collection == coll,
                    ChangeEvent::Replace { collection, .. } => collection == coll,
                },
                None => true, // Watch all collections
            };

            if should_notify {
                let _ = subscriber.sender.send(event.clone()).await;
            }
        }
    }

    /// Get metrics
    pub async fn metrics(&self) -> DocumentStoreMetrics {
        let metrics = self.metrics.read().await;
        DocumentStoreMetrics {
            total_documents: metrics.total_documents,
            total_collections: metrics.total_collections,
            total_indexes: metrics.total_indexes,
            inserts: metrics.inserts,
            updates: metrics.updates,
            deletes: metrics.deletes,
            queries: metrics.queries,
            aggregations: metrics.aggregations,
            index_scans: metrics.index_scans,
            collection_scans: metrics.collection_scans,
        }
    }
}

/// Update result
#[derive(Debug, Clone)]
pub struct UpdateResult {
    /// Number of documents matched
    pub matched_count: u64,
    /// Number of documents modified
    pub modified_count: u64,
    /// Upserted document ID (if any)
    pub upserted_id: Option<DocumentId>,
}

/// Delete result
#[derive(Debug, Clone)]
pub struct DeleteResult {
    /// Number of documents deleted
    pub deleted_count: u64,
}

/// Update operations
#[derive(Debug, Clone)]
pub struct UpdateOperation {
    /// Set fields
    pub set: Option<HashMap<String, serde_json::Value>>,
    /// Unset fields
    pub unset: Option<Vec<String>>,
    /// Increment fields
    pub inc: Option<HashMap<String, f64>>,
    /// Push to arrays
    pub push: Option<HashMap<String, serde_json::Value>>,
    /// Pull from arrays
    pub pull: Option<HashMap<String, serde_json::Value>>,
    /// Add to set
    pub add_to_set: Option<HashMap<String, serde_json::Value>>,
    /// Min values
    pub min: Option<HashMap<String, serde_json::Value>>,
    /// Max values
    pub max: Option<HashMap<String, serde_json::Value>>,
    /// Multiply values
    pub mul: Option<HashMap<String, f64>>,
    /// Rename fields
    pub rename: Option<HashMap<String, String>>,
    /// Current date
    pub current_date: Option<Vec<String>>,
}

impl UpdateOperation {
    /// Parse update operation from JSON
    pub fn from_json(value: serde_json::Value) -> Result<Self, DocumentStoreError> {
        let obj = value
            .as_object()
            .ok_or_else(|| DocumentStoreError::InvalidUpdate("Update must be an object".into()))?;

        let mut op = UpdateOperation {
            set: None,
            unset: None,
            inc: None,
            push: None,
            pull: None,
            add_to_set: None,
            min: None,
            max: None,
            mul: None,
            rename: None,
            current_date: None,
        };

        for (key, value) in obj {
            match key.as_str() {
                "$set" => {
                    let map = value.as_object().ok_or_else(|| {
                        DocumentStoreError::InvalidUpdate("$set must be an object".into())
                    })?;
                    op.set = Some(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
                }
                "$unset" => {
                    let map = value.as_object().ok_or_else(|| {
                        DocumentStoreError::InvalidUpdate("$unset must be an object".into())
                    })?;
                    op.unset = Some(map.keys().cloned().collect());
                }
                "$inc" => {
                    let map = value.as_object().ok_or_else(|| {
                        DocumentStoreError::InvalidUpdate("$inc must be an object".into())
                    })?;
                    let mut inc_map = HashMap::new();
                    for (k, v) in map {
                        let num = v.as_f64().ok_or_else(|| {
                            DocumentStoreError::InvalidUpdate("$inc values must be numbers".into())
                        })?;
                        inc_map.insert(k.clone(), num);
                    }
                    op.inc = Some(inc_map);
                }
                "$push" => {
                    let map = value.as_object().ok_or_else(|| {
                        DocumentStoreError::InvalidUpdate("$push must be an object".into())
                    })?;
                    op.push = Some(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
                }
                "$pull" => {
                    let map = value.as_object().ok_or_else(|| {
                        DocumentStoreError::InvalidUpdate("$pull must be an object".into())
                    })?;
                    op.pull = Some(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
                }
                "$addToSet" => {
                    let map = value.as_object().ok_or_else(|| {
                        DocumentStoreError::InvalidUpdate("$addToSet must be an object".into())
                    })?;
                    op.add_to_set = Some(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
                }
                "$min" => {
                    let map = value.as_object().ok_or_else(|| {
                        DocumentStoreError::InvalidUpdate("$min must be an object".into())
                    })?;
                    op.min = Some(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
                }
                "$max" => {
                    let map = value.as_object().ok_or_else(|| {
                        DocumentStoreError::InvalidUpdate("$max must be an object".into())
                    })?;
                    op.max = Some(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
                }
                "$mul" => {
                    let map = value.as_object().ok_or_else(|| {
                        DocumentStoreError::InvalidUpdate("$mul must be an object".into())
                    })?;
                    let mut mul_map = HashMap::new();
                    for (k, v) in map {
                        let num = v.as_f64().ok_or_else(|| {
                            DocumentStoreError::InvalidUpdate("$mul values must be numbers".into())
                        })?;
                        mul_map.insert(k.clone(), num);
                    }
                    op.mul = Some(mul_map);
                }
                "$rename" => {
                    let map = value.as_object().ok_or_else(|| {
                        DocumentStoreError::InvalidUpdate("$rename must be an object".into())
                    })?;
                    let mut rename_map = HashMap::new();
                    for (k, v) in map {
                        let new_name = v.as_str().ok_or_else(|| {
                            DocumentStoreError::InvalidUpdate(
                                "$rename values must be strings".into(),
                            )
                        })?;
                        rename_map.insert(k.clone(), new_name.to_string());
                    }
                    op.rename = Some(rename_map);
                }
                "$currentDate" => {
                    let map = value.as_object().ok_or_else(|| {
                        DocumentStoreError::InvalidUpdate("$currentDate must be an object".into())
                    })?;
                    op.current_date = Some(map.keys().cloned().collect());
                }
                _ => {
                    return Err(DocumentStoreError::InvalidUpdate(format!(
                        "Unknown update operator: {}",
                        key
                    )));
                }
            }
        }

        Ok(op)
    }
}

/// Index options
#[derive(Debug, Clone, Default)]
pub struct IndexOptions {
    /// Index name
    pub name: Option<String>,
    /// Unique index
    pub unique: bool,
    /// Sparse index (only index documents that have the field)
    pub sparse: bool,
    /// Background index build
    pub background: bool,
    /// Expire after seconds (TTL index)
    pub expire_after_seconds: Option<u64>,
    /// Partial filter expression
    pub partial_filter: Option<serde_json::Value>,
}

/// Change stream subscriber
struct ChangeStreamSubscriber {
    collection: Option<String>,
    sender: tokio::sync::mpsc::Sender<ChangeEvent>,
}

/// Change stream handle
pub struct ChangeStreamHandle {
    receiver: tokio::sync::mpsc::Receiver<ChangeEvent>,
}

impl ChangeStreamHandle {
    /// Receive next change event
    pub async fn next(&mut self) -> Option<ChangeEvent> {
        self.receiver.recv().await
    }
}

/// Change event types
#[derive(Debug, Clone)]
pub enum ChangeEvent {
    /// Document inserted
    Insert {
        /// Collection name
        collection: String,
        /// Inserted document
        document: Document,
    },
    /// Document updated
    Update {
        /// Collection name
        collection: String,
        /// Document ID
        document_id: DocumentId,
        /// Update description
        update_description: UpdateDescription,
    },
    /// Document deleted
    Delete {
        /// Collection name
        collection: String,
        /// Document ID
        document_id: DocumentId,
    },
    /// Document replaced
    Replace {
        /// Collection name
        collection: String,
        /// New document
        document: Document,
    },
}

/// Update description for change streams
#[derive(Debug, Clone)]
pub struct UpdateDescription {
    /// Updated fields
    pub updated_fields: HashMap<String, serde_json::Value>,
    /// Removed fields
    pub removed_fields: Vec<String>,
}

/// Document store errors
#[derive(Debug, thiserror::Error)]
pub enum DocumentStoreError {
    /// Collection already exists
    #[error("Collection already exists: {0}")]
    CollectionExists(String),

    /// Collection not found
    #[error("Collection not found: {0}")]
    CollectionNotFound(String),

    /// Document not found
    #[error("Document not found: {0}")]
    DocumentNotFound(String),

    /// Invalid document
    #[error("Invalid document: {0}")]
    InvalidDocument(String),

    /// Document too large
    #[error("Document too large: {size} bytes (max: {max} bytes)")]
    DocumentTooLarge {
        /// Actual size
        size: usize,
        /// Maximum allowed size
        max: usize,
    },

    /// Nesting too deep
    #[error("Document nesting too deep: {depth} levels (max: {max} levels)")]
    NestingTooDeep {
        /// Actual depth
        depth: usize,
        /// Maximum allowed depth
        max: usize,
    },

    /// Invalid query
    #[error("Invalid query: {0}")]
    InvalidQuery(String),

    /// Invalid update
    #[error("Invalid update: {0}")]
    InvalidUpdate(String),

    /// Invalid aggregation
    #[error("Invalid aggregation: {0}")]
    InvalidAggregation(String),

    /// Index error
    #[error("Index error: {0}")]
    IndexError(String),

    /// Duplicate key
    #[error("Duplicate key error: {0}")]
    DuplicateKey(String),

    /// Validation error
    #[error("Validation error: {0}")]
    ValidationError(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_create_collection() {
        let store = DocumentStore::new(DocumentStoreConfig::default());

        store.create_collection("test", None).await.unwrap();

        let collections = store.list_collections().await;
        assert!(collections.contains(&"test".to_string()));
    }

    #[tokio::test]
    async fn test_insert_and_find() {
        let store = DocumentStore::new(DocumentStoreConfig::default());
        store.create_collection("users", None).await.unwrap();

        let doc = json!({
            "name": "Alice",
            "age": 30
        });

        let id = store.insert_one("users", doc).await.unwrap();

        let query = json!({ "name": "Alice" });
        let results = store.find("users", query).await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, id);
    }

    #[tokio::test]
    async fn test_document_size_limit() {
        let config = DocumentStoreConfig {
            max_document_size: 100,
            ..Default::default()
        };
        let store = DocumentStore::new(config);
        store.create_collection("test", None).await.unwrap();

        // Create a large document
        let large_doc = json!({
            "data": "x".repeat(200)
        });

        let result = store.insert_one("test", large_doc).await;
        assert!(matches!(
            result,
            Err(DocumentStoreError::DocumentTooLarge { .. })
        ));
    }
}
