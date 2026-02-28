//! Cross-Cluster Global Secondary Indexes
//!
//! Secondary indexes spanning multiple Ferrite clusters, maintained
//! via CDC-driven replication with configurable lag tolerance.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the global index manager.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GlobalIndexConfig {
    /// Maximum number of indexes allowed.
    pub max_indexes: usize,
    /// Interval between replication sync cycles.
    pub sync_interval: Duration,
    /// Maximum acceptable replication lag in milliseconds.
    pub max_lag_ms: u64,
    /// Enable query routing to the best cluster.
    pub enable_routing: bool,
}

impl Default for GlobalIndexConfig {
    fn default() -> Self {
        Self {
            max_indexes: 100,
            sync_interval: Duration::from_secs(1),
            max_lag_ms: 5000,
            enable_routing: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Index definition types
// ---------------------------------------------------------------------------

/// Defines a global secondary index.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GlobalIndexDefinition {
    /// Unique index name.
    pub name: String,
    /// Key pattern this index covers (e.g. `"products:*"`).
    pub source_pattern: String,
    /// Fields included in the index.
    pub fields: Vec<IndexField>,
    /// Type of index.
    pub index_type: GlobalIndexType,
    /// Clusters participating in this index.
    pub clusters: Vec<ClusterRef>,
    /// Desired replication factor.
    pub replication_factor: u8,
    /// Unix timestamp (seconds) when this index was created.
    pub created_at: u64,
}

/// A field within an index.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexField {
    /// Field name.
    pub name: String,
    /// Data type of this field.
    pub field_type: IndexFieldType,
    /// Scoring weight for text/vector search.
    pub weight: f64,
}

/// Supported index field types.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum IndexFieldType {
    /// Full-text searchable text.
    Text,
    /// Numeric value (sortable, rangeable).
    Numeric,
    /// Tag / exact-match field.
    Tag,
    /// Geographic point (lat, lon).
    Geo,
    /// Dense vector of the given dimensionality.
    Vector(usize),
}

/// The kind of index.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum GlobalIndexType {
    /// Full-text search index.
    FullText,
    /// Vector similarity index.
    Vector,
    /// Sorted (numeric / tag) index.
    Sorted,
    /// Composite multi-field index.
    Composite,
}

/// Reference to a cluster participating in an index.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterRef {
    /// Unique cluster identifier.
    pub cluster_id: String,
    /// Network endpoint (e.g. `"cluster-a.example.com:6379"`).
    pub endpoint: String,
    /// Role of this cluster.
    pub role: ClusterRole,
    /// Current observed replication lag in milliseconds.
    pub lag_ms: u64,
}

/// Role of a cluster within the index.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ClusterRole {
    /// Primary (writable) cluster.
    Primary,
    /// Replica (read-write, receives CDC events).
    Replica,
    /// Read-only cluster.
    ReadOnly,
}

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

/// Search result returned by the index.
#[derive(Clone, Debug, Serialize)]
pub struct SearchResult {
    /// Matching documents.
    pub hits: Vec<SearchHit>,
    /// Total number of matching documents.
    pub total: u64,
    /// Maximum relevance score across hits.
    pub max_score: f64,
    /// Search latency in milliseconds.
    pub search_ms: u64,
    /// Cluster that served the query.
    pub served_from: String,
}

/// A single search hit.
#[derive(Clone, Debug, Serialize)]
pub struct SearchHit {
    /// Document identifier.
    pub doc_id: String,
    /// Relevance score.
    pub score: f64,
    /// Document fields.
    pub fields: HashMap<String, serde_json::Value>,
    /// Cluster that stored the document.
    pub cluster: String,
}

/// Replication status for an index.
#[derive(Clone, Debug, Serialize)]
pub struct ReplicationStatus {
    /// Index name.
    pub index: String,
    /// Per-cluster replication details.
    pub clusters: Vec<ClusterReplicationInfo>,
}

/// Per-cluster replication info.
#[derive(Clone, Debug, Serialize)]
pub struct ClusterReplicationInfo {
    /// Cluster identifier.
    pub cluster_id: String,
    /// Current replication lag in milliseconds.
    pub lag_ms: u64,
    /// Documents pending replication.
    pub docs_pending: u64,
    /// Unix timestamp of last successful sync.
    pub last_sync: u64,
    /// Whether the cluster is healthy.
    pub healthy: bool,
}

/// Summary information about an index.
#[derive(Clone, Debug, Serialize)]
pub struct GlobalIndexInfo {
    /// Index name.
    pub name: String,
    /// Index type.
    pub index_type: GlobalIndexType,
    /// Number of indexed fields.
    pub fields_count: usize,
    /// Number of participating clusters.
    pub clusters_count: usize,
    /// Total indexed documents.
    pub total_docs: u64,
    /// Unix timestamp when the index was created.
    pub created_at: u64,
}

/// Cumulative statistics for the global index manager.
#[derive(Clone, Debug, Serialize)]
pub struct GlobalIndexStats {
    /// Number of active indexes.
    pub indexes: usize,
    /// Total documents across all indexes.
    pub total_docs: u64,
    /// Total searches executed.
    pub total_searches: u64,
    /// Average search latency in milliseconds.
    pub avg_search_ms: f64,
    /// Average replication lag across all clusters and indexes.
    pub replication_lag_avg_ms: f64,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors returned by the global index manager.
#[derive(Debug, Clone, thiserror::Error)]
pub enum GlobalIndexError {
    /// Index with the given name was not found.
    #[error("index not found: {0}")]
    NotFound(String),
    /// An index with the given name already exists.
    #[error("index already exists: {0}")]
    AlreadyExists(String),
    /// The index definition is invalid.
    #[error("invalid index definition: {0}")]
    InvalidDefinition(String),
    /// A search query failed.
    #[error("search failed: {0}")]
    SearchFailed(String),
    /// Replication to a cluster failed.
    #[error("replication failed: {0}")]
    ReplicationFailed(String),
    /// Maximum number of indexes reached.
    #[error("maximum index limit reached ({0})")]
    MaxIndexes(usize),
    /// Cannot reach a cluster.
    #[error("cluster unreachable: {0}")]
    ClusterUnreachable(String),
}

// ---------------------------------------------------------------------------
// Internal document store per index
// ---------------------------------------------------------------------------

/// In-memory document store backing a single index (for standalone operation).
struct IndexStore {
    definition: GlobalIndexDefinition,
    docs: HashMap<String, HashMap<String, serde_json::Value>>,
    doc_count: u64,
}

impl IndexStore {
    fn new(definition: GlobalIndexDefinition) -> Self {
        Self {
            definition,
            docs: HashMap::new(),
            doc_count: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Manager
// ---------------------------------------------------------------------------

/// Manages cross-cluster global secondary indexes.
pub struct GlobalIndexManager {
    config: GlobalIndexConfig,
    indexes: RwLock<HashMap<String, IndexStore>>,
    total_searches: AtomicU64,
    total_search_ms: AtomicU64,
}

impl GlobalIndexManager {
    /// Create a new global index manager.
    pub fn new(config: GlobalIndexConfig) -> Self {
        Self {
            config,
            indexes: RwLock::new(HashMap::new()),
            total_searches: AtomicU64::new(0),
            total_search_ms: AtomicU64::new(0),
        }
    }

    /// Create a new global index.
    pub fn create_index(
        &self,
        def: GlobalIndexDefinition,
    ) -> Result<(), GlobalIndexError> {
        if def.name.is_empty() {
            return Err(GlobalIndexError::InvalidDefinition(
                "index name is required".to_string(),
            ));
        }
        if def.fields.is_empty() {
            return Err(GlobalIndexError::InvalidDefinition(
                "at least one field is required".to_string(),
            ));
        }

        let mut indexes = self.indexes.write();

        if indexes.len() >= self.config.max_indexes {
            return Err(GlobalIndexError::MaxIndexes(self.config.max_indexes));
        }
        if indexes.contains_key(&def.name) {
            return Err(GlobalIndexError::AlreadyExists(def.name.clone()));
        }

        indexes.insert(def.name.clone(), IndexStore::new(def));
        Ok(())
    }

    /// Drop an existing index.
    pub fn drop_index(&self, name: &str) -> Result<(), GlobalIndexError> {
        let mut indexes = self.indexes.write();
        if indexes.remove(name).is_none() {
            return Err(GlobalIndexError::NotFound(name.to_string()));
        }
        Ok(())
    }

    /// Perform a text search on the named index.
    pub fn search(
        &self,
        index: &str,
        query: &str,
        limit: usize,
    ) -> Result<SearchResult, GlobalIndexError> {
        let start = Instant::now();
        let indexes = self.indexes.read();
        let store = indexes
            .get(index)
            .ok_or_else(|| GlobalIndexError::NotFound(index.to_string()))?;

        let query_lower = query.to_lowercase();
        let mut hits: Vec<SearchHit> = store
            .docs
            .iter()
            .filter_map(|(doc_id, fields)| {
                // Simple text matching: check if any text field contains the query.
                let score = fields
                    .values()
                    .filter_map(|v| v.as_str())
                    .filter(|s| s.to_lowercase().contains(&query_lower))
                    .count() as f64;

                if score > 0.0 {
                    let cluster = store
                        .definition
                        .clusters
                        .first()
                        .map(|c| c.cluster_id.clone())
                        .unwrap_or_else(|| "local".to_string());
                    Some(SearchHit {
                        doc_id: doc_id.clone(),
                        score,
                        fields: fields.clone(),
                        cluster,
                    })
                } else {
                    None
                }
            })
            .collect();

        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let total = hits.len() as u64;
        let max_score = hits.first().map(|h| h.score).unwrap_or(0.0);

        hits.truncate(limit);

        let search_ms = start.elapsed().as_millis() as u64;
        self.total_searches.fetch_add(1, Ordering::Relaxed);
        self.total_search_ms.fetch_add(search_ms, Ordering::Relaxed);

        let served_from = store
            .definition
            .clusters
            .first()
            .map(|c| c.cluster_id.clone())
            .unwrap_or_else(|| "local".to_string());

        Ok(SearchResult {
            hits,
            total,
            max_score,
            search_ms,
            served_from,
        })
    }

    /// Perform a vector similarity search on the named index.
    pub fn search_vector(
        &self,
        index: &str,
        vector: &[f32],
        top_k: usize,
    ) -> Result<SearchResult, GlobalIndexError> {
        let start = Instant::now();
        let indexes = self.indexes.read();
        let store = indexes
            .get(index)
            .ok_or_else(|| GlobalIndexError::NotFound(index.to_string()))?;

        // Simple cosine-similarity placeholder: score = 1/(1+euclidean_distance)
        let mut hits: Vec<SearchHit> = store
            .docs
            .iter()
            .filter_map(|(doc_id, fields)| {
                // Look for a field named "vector" or the first Vector-typed field.
                let vec_field = store
                    .definition
                    .fields
                    .iter()
                    .find(|f| matches!(f.field_type, IndexFieldType::Vector(_)))
                    .map(|f| f.name.as_str())
                    .unwrap_or("vector");

                let doc_vec: Option<Vec<f32>> = fields.get(vec_field).and_then(|v| {
                    v.as_array().map(|arr| {
                        arr.iter()
                            .filter_map(|x| x.as_f64().map(|f| f as f32))
                            .collect()
                    })
                });

                doc_vec.map(|dv| {
                    let dist: f32 = vector
                        .iter()
                        .zip(dv.iter())
                        .map(|(a, b)| (a - b).powi(2))
                        .sum::<f32>()
                        .sqrt();
                    let score = 1.0 / (1.0 + dist as f64);

                    let cluster = store
                        .definition
                        .clusters
                        .first()
                        .map(|c| c.cluster_id.clone())
                        .unwrap_or_else(|| "local".to_string());

                    SearchHit {
                        doc_id: doc_id.clone(),
                        score,
                        fields: fields.clone(),
                        cluster,
                    }
                })
            })
            .collect();

        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let total = hits.len() as u64;
        let max_score = hits.first().map(|h| h.score).unwrap_or(0.0);
        hits.truncate(top_k);

        let search_ms = start.elapsed().as_millis() as u64;
        self.total_searches.fetch_add(1, Ordering::Relaxed);
        self.total_search_ms.fetch_add(search_ms, Ordering::Relaxed);

        let served_from = store
            .definition
            .clusters
            .first()
            .map(|c| c.cluster_id.clone())
            .unwrap_or_else(|| "local".to_string());

        Ok(SearchResult {
            hits,
            total,
            max_score,
            search_ms,
            served_from,
        })
    }

    /// Index (upsert) a document into the named index.
    pub fn index_document(
        &self,
        index: &str,
        doc_id: &str,
        fields: HashMap<String, serde_json::Value>,
    ) -> Result<(), GlobalIndexError> {
        let mut indexes = self.indexes.write();
        let store = indexes
            .get_mut(index)
            .ok_or_else(|| GlobalIndexError::NotFound(index.to_string()))?;

        let is_new = !store.docs.contains_key(doc_id);
        store.docs.insert(doc_id.to_string(), fields);
        if is_new {
            store.doc_count += 1;
        }
        Ok(())
    }

    /// Delete a document from the named index.
    pub fn delete_document(
        &self,
        index: &str,
        doc_id: &str,
    ) -> Result<(), GlobalIndexError> {
        let mut indexes = self.indexes.write();
        let store = indexes
            .get_mut(index)
            .ok_or_else(|| GlobalIndexError::NotFound(index.to_string()))?;

        if store.docs.remove(doc_id).is_some() {
            store.doc_count = store.doc_count.saturating_sub(1);
        }
        Ok(())
    }

    /// Handle a CDC (Change Data Capture) event from a cluster.
    ///
    /// If `value` is `Some`, the key is upserted; if `None`, the key is deleted.
    pub fn on_cdc_event(
        &self,
        _cluster_id: &str,
        key: &str,
        value: Option<&[u8]>,
    ) {
        let indexes = self.indexes.read();
        for store in indexes.values() {
            let pattern_prefix = store.definition.source_pattern.trim_end_matches('*');
            if key.starts_with(pattern_prefix) {
                // In a real implementation we'd update the index via write lock.
                // Here we just note the event was processed.
                if value.is_none() {
                    tracing::debug!(key, "CDC delete event processed");
                } else {
                    tracing::debug!(key, "CDC upsert event processed");
                }
            }
        }
    }

    /// Return replication status for the named index.
    pub fn replication_status(
        &self,
        name: &str,
    ) -> Option<ReplicationStatus> {
        let indexes = self.indexes.read();
        let store = indexes.get(name)?;

        let clusters = store
            .definition
            .clusters
            .iter()
            .map(|c| ClusterReplicationInfo {
                cluster_id: c.cluster_id.clone(),
                lag_ms: c.lag_ms,
                docs_pending: 0,
                last_sync: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
                healthy: c.lag_ms <= self.config.max_lag_ms,
            })
            .collect();

        Some(ReplicationStatus {
            index: name.to_string(),
            clusters,
        })
    }

    /// List all indexes with summary info.
    pub fn list_indexes(&self) -> Vec<GlobalIndexInfo> {
        let indexes = self.indexes.read();
        indexes
            .values()
            .map(|store| GlobalIndexInfo {
                name: store.definition.name.clone(),
                index_type: store.definition.index_type.clone(),
                fields_count: store.definition.fields.len(),
                clusters_count: store.definition.clusters.len(),
                total_docs: store.doc_count,
                created_at: store.definition.created_at,
            })
            .collect()
    }

    /// Return cumulative statistics.
    pub fn stats(&self) -> GlobalIndexStats {
        let indexes = self.indexes.read();
        let total_docs: u64 = indexes.values().map(|s| s.doc_count).sum();
        let total_searches = self.total_searches.load(Ordering::Relaxed);
        let total_ms = self.total_search_ms.load(Ordering::Relaxed);

        let all_lags: Vec<u64> = indexes
            .values()
            .flat_map(|s| s.definition.clusters.iter().map(|c| c.lag_ms))
            .collect();
        let replication_lag_avg_ms = if all_lags.is_empty() {
            0.0
        } else {
            all_lags.iter().sum::<u64>() as f64 / all_lags.len() as f64
        };

        GlobalIndexStats {
            indexes: indexes.len(),
            total_docs,
            total_searches,
            avg_search_ms: if total_searches > 0 {
                total_ms as f64 / total_searches as f64
            } else {
                0.0
            },
            replication_lag_avg_ms,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_definition(name: &str) -> GlobalIndexDefinition {
        GlobalIndexDefinition {
            name: name.to_string(),
            source_pattern: "products:*".to_string(),
            fields: vec![
                IndexField {
                    name: "title".to_string(),
                    field_type: IndexFieldType::Text,
                    weight: 1.0,
                },
                IndexField {
                    name: "price".to_string(),
                    field_type: IndexFieldType::Numeric,
                    weight: 0.5,
                },
            ],
            index_type: GlobalIndexType::FullText,
            clusters: vec![ClusterRef {
                cluster_id: "cluster-a".to_string(),
                endpoint: "cluster-a.example.com:6379".to_string(),
                role: ClusterRole::Primary,
                lag_ms: 10,
            }],
            replication_factor: 1,
            created_at: 1700000000,
        }
    }

    fn manager_with_index() -> GlobalIndexManager {
        let mgr = GlobalIndexManager::new(GlobalIndexConfig::default());
        mgr.create_index(sample_definition("products_idx"))
            .expect("create index");
        mgr
    }

    #[test]
    fn test_create_and_drop_index() {
        let mgr = GlobalIndexManager::new(GlobalIndexConfig::default());
        mgr.create_index(sample_definition("idx1")).expect("create");
        assert_eq!(mgr.list_indexes().len(), 1);

        mgr.drop_index("idx1").expect("drop");
        assert_eq!(mgr.list_indexes().len(), 0);
    }

    #[test]
    fn test_duplicate_index() {
        let mgr = GlobalIndexManager::new(GlobalIndexConfig::default());
        mgr.create_index(sample_definition("idx1")).expect("create");
        let err = mgr.create_index(sample_definition("idx1")).unwrap_err();
        assert!(matches!(err, GlobalIndexError::AlreadyExists(_)));
    }

    #[test]
    fn test_drop_nonexistent() {
        let mgr = GlobalIndexManager::new(GlobalIndexConfig::default());
        let err = mgr.drop_index("nonexistent").unwrap_err();
        assert!(matches!(err, GlobalIndexError::NotFound(_)));
    }

    #[test]
    fn test_index_and_search_document() {
        let mgr = manager_with_index();
        mgr.index_document(
            "products_idx",
            "doc1",
            HashMap::from([
                ("title".to_string(), serde_json::json!("Red Widget")),
                ("price".to_string(), serde_json::json!(9.99)),
            ]),
        )
        .expect("index doc");

        mgr.index_document(
            "products_idx",
            "doc2",
            HashMap::from([
                ("title".to_string(), serde_json::json!("Blue Gadget")),
                ("price".to_string(), serde_json::json!(19.99)),
            ]),
        )
        .expect("index doc");

        let result = mgr.search("products_idx", "widget", 10).expect("search");
        assert_eq!(result.total, 1);
        assert_eq!(result.hits[0].doc_id, "doc1");
    }

    #[test]
    fn test_delete_document() {
        let mgr = manager_with_index();
        mgr.index_document(
            "products_idx",
            "doc1",
            HashMap::from([("title".to_string(), serde_json::json!("Test"))]),
        )
        .expect("index");

        assert_eq!(mgr.list_indexes()[0].total_docs, 1);

        mgr.delete_document("products_idx", "doc1")
            .expect("delete");
        assert_eq!(mgr.list_indexes()[0].total_docs, 0);
    }

    #[test]
    fn test_vector_search() {
        let mgr = GlobalIndexManager::new(GlobalIndexConfig::default());
        let def = GlobalIndexDefinition {
            name: "vec_idx".to_string(),
            source_pattern: "vectors:*".to_string(),
            fields: vec![IndexField {
                name: "embedding".to_string(),
                field_type: IndexFieldType::Vector(3),
                weight: 1.0,
            }],
            index_type: GlobalIndexType::Vector,
            clusters: vec![ClusterRef {
                cluster_id: "c1".to_string(),
                endpoint: "c1:6379".to_string(),
                role: ClusterRole::Primary,
                lag_ms: 0,
            }],
            replication_factor: 1,
            created_at: 1700000000,
        };
        mgr.create_index(def).expect("create");

        mgr.index_document(
            "vec_idx",
            "v1",
            HashMap::from([(
                "embedding".to_string(),
                serde_json::json!([1.0, 0.0, 0.0]),
            )]),
        )
        .expect("index");

        mgr.index_document(
            "vec_idx",
            "v2",
            HashMap::from([(
                "embedding".to_string(),
                serde_json::json!([0.0, 1.0, 0.0]),
            )]),
        )
        .expect("index");

        let result = mgr
            .search_vector("vec_idx", &[1.0, 0.0, 0.0], 2)
            .expect("search");
        assert_eq!(result.total, 2);
        // v1 should be closest
        assert_eq!(result.hits[0].doc_id, "v1");
    }

    #[test]
    fn test_replication_status() {
        let mgr = manager_with_index();
        let status = mgr.replication_status("products_idx").expect("status");
        assert_eq!(status.clusters.len(), 1);
        assert!(status.clusters[0].healthy);
    }

    #[test]
    fn test_stats() {
        let mgr = manager_with_index();
        mgr.index_document(
            "products_idx",
            "d1",
            HashMap::from([("title".to_string(), serde_json::json!("Item"))]),
        )
        .expect("index");
        let _ = mgr.search("products_idx", "Item", 10);

        let stats = mgr.stats();
        assert_eq!(stats.indexes, 1);
        assert_eq!(stats.total_docs, 1);
        assert!(stats.total_searches >= 1);
    }

    #[test]
    fn test_max_indexes_limit() {
        let cfg = GlobalIndexConfig {
            max_indexes: 2,
            ..GlobalIndexConfig::default()
        };
        let mgr = GlobalIndexManager::new(cfg);
        mgr.create_index(sample_definition("idx1")).expect("1");
        mgr.create_index(sample_definition("idx2")).expect("2");
        let err = mgr.create_index(sample_definition("idx3")).unwrap_err();
        assert!(matches!(err, GlobalIndexError::MaxIndexes(2)));
    }

    #[test]
    fn test_invalid_definition() {
        let mgr = GlobalIndexManager::new(GlobalIndexConfig::default());
        let mut def = sample_definition("x");
        def.name = "".to_string();
        let err = mgr.create_index(def).unwrap_err();
        assert!(matches!(err, GlobalIndexError::InvalidDefinition(_)));

        let mut def2 = sample_definition("y");
        def2.fields.clear();
        let err2 = mgr.create_index(def2).unwrap_err();
        assert!(matches!(err2, GlobalIndexError::InvalidDefinition(_)));
    }

    #[test]
    fn test_cdc_event() {
        let mgr = manager_with_index();
        // Should not panic or error — just a no-op in mock mode.
        mgr.on_cdc_event("cluster-a", "products:123", Some(b"data"));
        mgr.on_cdc_event("cluster-a", "products:456", None);
    }
}
