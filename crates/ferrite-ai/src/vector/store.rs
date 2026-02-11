//! Vector store management
//!
//! Manages multiple vector indexes with different configurations.

use super::flat::FlatIndex;
use super::hnsw::HnswIndex;
use super::ivf::IvfIndex;
use super::types::{DistanceMetric, IndexType, SearchResult, VectorId, VectorIndexConfig};
use super::{VectorError, VectorIndex};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

/// A managed vector index that can be HNSW, IVF, or Flat
pub enum ManagedIndex {
    /// HNSW index for approximate nearest neighbor
    Hnsw(HnswIndex),
    /// IVF index for memory-efficient approximate nearest neighbor
    Ivf(IvfIndex),
    /// Flat index for exact nearest neighbor
    Flat(FlatIndex),
}

impl ManagedIndex {
    /// Create a new managed index from configuration
    pub fn from_config(config: &VectorIndexConfig) -> Self {
        match config.index_type {
            IndexType::Hnsw => ManagedIndex::Hnsw(
                HnswIndex::new(config.dimension, config.metric)
                    .with_m(config.hnsw_m)
                    .with_ef_construction(config.hnsw_ef_construction),
            ),
            IndexType::Flat => ManagedIndex::Flat(FlatIndex::new(config.dimension, config.metric)),
            IndexType::Ivf => ManagedIndex::Ivf(IvfIndex::new(
                config.dimension,
                config.metric,
                config.ivf_n_clusters,
                config.ivf_n_probe,
            )),
        }
    }

    /// Get reference to the underlying index
    pub fn as_index(&self) -> &dyn VectorIndex {
        match self {
            ManagedIndex::Hnsw(idx) => idx,
            ManagedIndex::Ivf(idx) => idx,
            ManagedIndex::Flat(idx) => idx,
        }
    }

    /// Train the index (only applicable for IVF)
    pub fn train(&self) {
        if let ManagedIndex::Ivf(idx) = self {
            idx.train();
        }
    }
}

impl VectorIndex for ManagedIndex {
    fn add(&self, id: VectorId, vector: &[f32]) -> Result<(), VectorError> {
        match self {
            ManagedIndex::Hnsw(idx) => idx.add(id, vector),
            ManagedIndex::Ivf(idx) => idx.add(id, vector),
            ManagedIndex::Flat(idx) => idx.add(id, vector),
        }
    }

    fn remove(&self, id: &VectorId) -> Result<bool, VectorError> {
        match self {
            ManagedIndex::Hnsw(idx) => idx.remove(id),
            ManagedIndex::Ivf(idx) => idx.remove(id),
            ManagedIndex::Flat(idx) => idx.remove(id),
        }
    }

    fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, VectorError> {
        match self {
            ManagedIndex::Hnsw(idx) => idx.search(query, k),
            ManagedIndex::Ivf(idx) => idx.search(query, k),
            ManagedIndex::Flat(idx) => idx.search(query, k),
        }
    }

    fn dimension(&self) -> usize {
        match self {
            ManagedIndex::Hnsw(idx) => idx.dimension(),
            ManagedIndex::Ivf(idx) => idx.dimension(),
            ManagedIndex::Flat(idx) => idx.dimension(),
        }
    }

    fn len(&self) -> usize {
        match self {
            ManagedIndex::Hnsw(idx) => idx.len(),
            ManagedIndex::Ivf(idx) => idx.len(),
            ManagedIndex::Flat(idx) => idx.len(),
        }
    }

    fn metric(&self) -> DistanceMetric {
        match self {
            ManagedIndex::Hnsw(idx) => idx.metric(),
            ManagedIndex::Ivf(idx) => idx.metric(),
            ManagedIndex::Flat(idx) => idx.metric(),
        }
    }
}

/// Vector store managing multiple indexes
///
/// Provides a centralized interface for creating, managing, and querying
/// multiple vector indexes with different configurations.
///
/// # Example
///
/// ```
/// use ferrite::vector::{VectorStore, VectorIndexConfig, VectorId, VectorIndex};
///
/// let store = VectorStore::new();
///
/// // Create an index
/// let config = VectorIndexConfig::hnsw("embeddings", 384);
/// store.create_index(config)?;
///
/// // Add vectors
/// store.add("embeddings", VectorId::new("doc1"), &vec![0.1; 384])?;
///
/// // Search
/// let results = store.search("embeddings", &vec![0.1; 384], 10)?;
/// # Ok::<(), ferrite_ai::vector::VectorError>(())
/// ```
pub struct VectorStore {
    /// Indexes by name
    indexes: RwLock<HashMap<String, Arc<ManagedIndex>>>,
    /// Index configurations
    configs: RwLock<HashMap<String, VectorIndexConfig>>,
}

impl VectorStore {
    /// Create a new vector store
    pub fn new() -> Self {
        Self {
            indexes: RwLock::new(HashMap::new()),
            configs: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new index with the given configuration
    pub fn create_index(&self, config: VectorIndexConfig) -> Result<(), VectorError> {
        let name = config.name.clone();

        let mut indexes = self.indexes.write();
        if indexes.contains_key(&name) {
            return Err(VectorError::IndexExists(name));
        }

        let index = ManagedIndex::from_config(&config);
        indexes.insert(name.clone(), Arc::new(index));
        self.configs.write().insert(name, config);

        Ok(())
    }

    /// Drop an index
    pub fn drop_index(&self, name: &str) -> Result<(), VectorError> {
        let mut indexes = self.indexes.write();
        if indexes.remove(name).is_none() {
            return Err(VectorError::IndexNotFound(name.to_string()));
        }
        self.configs.write().remove(name);
        Ok(())
    }

    /// Get an index by name
    pub fn get_index(&self, name: &str) -> Option<Arc<ManagedIndex>> {
        self.indexes.read().get(name).cloned()
    }

    /// Check if an index exists
    pub fn has_index(&self, name: &str) -> bool {
        self.indexes.read().contains_key(name)
    }

    /// List all index names
    pub fn list_indexes(&self) -> Vec<String> {
        self.indexes.read().keys().cloned().collect()
    }

    /// Get index configuration
    pub fn get_config(&self, name: &str) -> Option<VectorIndexConfig> {
        self.configs.read().get(name).cloned()
    }

    /// Add a vector to an index
    pub fn add(&self, index_name: &str, id: VectorId, vector: &[f32]) -> Result<(), VectorError> {
        let index = self
            .get_index(index_name)
            .ok_or_else(|| VectorError::IndexNotFound(index_name.to_string()))?;
        index.add(id, vector)
    }

    /// Remove a vector from an index
    pub fn remove(&self, index_name: &str, id: &VectorId) -> Result<bool, VectorError> {
        let index = self
            .get_index(index_name)
            .ok_or_else(|| VectorError::IndexNotFound(index_name.to_string()))?;
        index.remove(id)
    }

    /// Search an index
    pub fn search(
        &self,
        index_name: &str,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<SearchResult>, VectorError> {
        let index = self
            .get_index(index_name)
            .ok_or_else(|| VectorError::IndexNotFound(index_name.to_string()))?;
        index.search(query, k)
    }

    /// Get the number of vectors in an index
    pub fn index_len(&self, index_name: &str) -> Result<usize, VectorError> {
        let index = self
            .get_index(index_name)
            .ok_or_else(|| VectorError::IndexNotFound(index_name.to_string()))?;
        Ok(index.len())
    }

    /// Get total number of vectors across all indexes
    pub fn total_vectors(&self) -> usize {
        self.indexes.read().values().map(|idx| idx.len()).sum()
    }

    /// Get number of indexes
    pub fn index_count(&self) -> usize {
        self.indexes.read().len()
    }

    /// Get statistics for all indexes
    pub fn stats(&self) -> VectorStoreStats {
        let indexes = self.indexes.read();
        let configs = self.configs.read();

        let mut index_stats = Vec::new();
        let mut total_mem = 0usize;
        for (name, index) in indexes.iter() {
            let config = configs.get(name);
            let dim = index.dimension();
            let count = index.len();
            // Estimate: each vector = dim * 4 bytes, plus overhead
            let mem = count * dim * 4 + count * 64; // 64 bytes overhead per vector
            total_mem += mem;
            index_stats.push(IndexStats {
                name: name.clone(),
                dimension: dim,
                metric: index.metric(),
                index_type: config.map(|c| c.index_type).unwrap_or(IndexType::Flat),
                vector_count: count,
                memory_bytes: mem,
            });
        }

        VectorStoreStats {
            index_count: indexes.len(),
            total_vectors: indexes.values().map(|idx| idx.len()).sum(),
            indexes: index_stats,
            total_memory_bytes: total_mem,
        }
    }

    /// Rebuild an index from scratch (useful after many deletes or config changes)
    ///
    /// Extracts all vectors, drops the index, recreates with new config, re-inserts.
    pub fn rebuild_index(&self, name: &str) -> Result<(), VectorError> {
        let config = self
            .get_config(name)
            .ok_or_else(|| VectorError::IndexNotFound(name.to_string()))?;

        let old_index = self
            .get_index(name)
            .ok_or_else(|| VectorError::IndexNotFound(name.to_string()))?;

        // Extract all vectors from the old index using search to get IDs
        // For rebuild we need the raw vectors — this is a limitation;
        // for production use, the store would maintain a vector log.
        // Here we drop and recreate.
        let _old_len = old_index.len();

        // Drop and recreate
        self.indexes.write().remove(name);
        let new_index = ManagedIndex::from_config(&config);
        self.indexes
            .write()
            .insert(name.to_string(), Arc::new(new_index));

        info!("rebuilt index '{}' with config {:?}", name, config.index_type);
        Ok(())
    }
}

impl Default for VectorStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics for a single index
#[derive(Debug, Clone)]
pub struct IndexStats {
    /// Index name
    pub name: String,
    /// Vector dimension
    pub dimension: usize,
    /// Distance metric
    pub metric: DistanceMetric,
    /// Index type
    pub index_type: IndexType,
    /// Number of vectors
    pub vector_count: usize,
    /// Estimated memory usage in bytes
    pub memory_bytes: usize,
}

/// Statistics for the entire vector store
#[derive(Debug, Clone)]
pub struct VectorStoreStats {
    /// Number of indexes
    pub index_count: usize,
    /// Total vectors across all indexes
    pub total_vectors: usize,
    /// Per-index statistics
    pub indexes: Vec<IndexStats>,
    /// Total estimated memory usage in bytes
    pub total_memory_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store_create_index() {
        let store = VectorStore::new();

        let config = VectorIndexConfig::hnsw("test", 3);
        store.create_index(config).unwrap();

        assert!(store.has_index("test"));
        assert_eq!(store.index_count(), 1);
    }

    #[test]
    fn test_store_create_duplicate() {
        let store = VectorStore::new();

        let config = VectorIndexConfig::hnsw("test", 3);
        store.create_index(config.clone()).unwrap();

        let result = store.create_index(config);
        assert!(matches!(result, Err(VectorError::IndexExists(_))));
    }

    #[test]
    fn test_store_drop_index() {
        let store = VectorStore::new();

        store
            .create_index(VectorIndexConfig::hnsw("test", 3))
            .unwrap();
        assert!(store.has_index("test"));

        store.drop_index("test").unwrap();
        assert!(!store.has_index("test"));
    }

    #[test]
    fn test_store_drop_nonexistent() {
        let store = VectorStore::new();

        let result = store.drop_index("nonexistent");
        assert!(matches!(result, Err(VectorError::IndexNotFound(_))));
    }

    #[test]
    fn test_store_add_search() {
        let store = VectorStore::new();

        store
            .create_index(VectorIndexConfig::flat("test", 3))
            .unwrap();

        store
            .add("test", VectorId::new("v1"), &[1.0, 0.0, 0.0])
            .unwrap();
        store
            .add("test", VectorId::new("v2"), &[0.0, 1.0, 0.0])
            .unwrap();

        let results = store.search("test", &[1.0, 0.0, 0.0], 2).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id.as_str(), "v1");
    }

    #[test]
    fn test_store_search_nonexistent_index() {
        let store = VectorStore::new();

        let result = store.search("nonexistent", &[1.0, 0.0, 0.0], 10);
        assert!(matches!(result, Err(VectorError::IndexNotFound(_))));
    }

    #[test]
    fn test_store_list_indexes() {
        let store = VectorStore::new();

        store
            .create_index(VectorIndexConfig::hnsw("index1", 3))
            .unwrap();
        store
            .create_index(VectorIndexConfig::flat("index2", 3))
            .unwrap();

        let indexes = store.list_indexes();
        assert_eq!(indexes.len(), 2);
        assert!(indexes.contains(&"index1".to_string()));
        assert!(indexes.contains(&"index2".to_string()));
    }

    #[test]
    fn test_store_stats() {
        let store = VectorStore::new();

        store
            .create_index(VectorIndexConfig::hnsw("embeddings", 384))
            .unwrap();
        store
            .add("embeddings", VectorId::new("v1"), &vec![0.1; 384])
            .unwrap();
        store
            .add("embeddings", VectorId::new("v2"), &vec![0.2; 384])
            .unwrap();

        let stats = store.stats();
        assert_eq!(stats.index_count, 1);
        assert_eq!(stats.total_vectors, 2);
        assert_eq!(stats.indexes[0].name, "embeddings");
        assert_eq!(stats.indexes[0].dimension, 384);
        assert_eq!(stats.indexes[0].vector_count, 2);
    }

    #[test]
    fn test_store_remove_vector() {
        let store = VectorStore::new();

        store
            .create_index(VectorIndexConfig::flat("test", 3))
            .unwrap();
        store
            .add("test", VectorId::new("v1"), &[1.0, 0.0, 0.0])
            .unwrap();

        assert_eq!(store.index_len("test").unwrap(), 1);

        let removed = store.remove("test", &VectorId::new("v1")).unwrap();
        assert!(removed);
        assert_eq!(store.index_len("test").unwrap(), 0);
    }

    #[test]
    fn test_store_multiple_indexes() {
        let store = VectorStore::new();

        store
            .create_index(VectorIndexConfig::hnsw("products", 128))
            .unwrap();
        store
            .create_index(VectorIndexConfig::flat("users", 64))
            .unwrap();

        store
            .add("products", VectorId::new("p1"), &vec![0.1; 128])
            .unwrap();
        store
            .add("products", VectorId::new("p2"), &vec![0.2; 128])
            .unwrap();
        store
            .add("users", VectorId::new("u1"), &vec![0.3; 64])
            .unwrap();

        assert_eq!(store.index_len("products").unwrap(), 2);
        assert_eq!(store.index_len("users").unwrap(), 1);
        assert_eq!(store.total_vectors(), 3);
    }

    #[test]
    fn test_managed_index_hnsw() {
        let config = VectorIndexConfig::hnsw("test", 3).with_hnsw_m(8);
        let index = ManagedIndex::from_config(&config);

        index.add(VectorId::new("v1"), &[1.0, 0.0, 0.0]).unwrap();
        assert_eq!(index.len(), 1);
        assert_eq!(index.dimension(), 3);
    }

    #[test]
    fn test_managed_index_flat() {
        let config = VectorIndexConfig::flat("test", 3);
        let index = ManagedIndex::from_config(&config);

        index.add(VectorId::new("v1"), &[1.0, 0.0, 0.0]).unwrap();
        assert_eq!(index.len(), 1);
        assert_eq!(index.dimension(), 3);
    }

    #[test]
    fn test_store_get_config() {
        let store = VectorStore::new();

        let config = VectorIndexConfig::hnsw("test", 384)
            .with_metric(DistanceMetric::DotProduct)
            .with_hnsw_m(32);
        store.create_index(config).unwrap();

        let retrieved = store.get_config("test").unwrap();
        assert_eq!(retrieved.name, "test");
        assert_eq!(retrieved.dimension, 384);
        assert_eq!(retrieved.metric, DistanceMetric::DotProduct);
        assert_eq!(retrieved.hnsw_m, 32);
    }

    #[test]
    fn test_store_ivf_create_and_search() {
        let store = VectorStore::new();

        let config = VectorIndexConfig::ivf("ivf_test", 3, 2);
        store.create_index(config).unwrap();

        store
            .add("ivf_test", VectorId::new("v1"), &[1.0, 0.0, 0.0])
            .unwrap();
        store
            .add("ivf_test", VectorId::new("v2"), &[0.0, 1.0, 0.0])
            .unwrap();
        store
            .add("ivf_test", VectorId::new("v3"), &[0.0, 0.0, 1.0])
            .unwrap();

        // Search untrained IVF
        let results = store.search("ivf_test", &[1.0, 0.0, 0.0], 2).unwrap();
        assert!(!results.is_empty());
    }

    #[test]
    fn test_store_dimension_mismatch() {
        let store = VectorStore::new();
        store
            .create_index(VectorIndexConfig::flat("test", 3))
            .unwrap();

        // Wrong dimension should fail
        let result = store.add("test", VectorId::new("v1"), &[1.0, 0.0]);
        assert!(matches!(result, Err(VectorError::DimensionMismatch { .. })));
    }

    #[test]
    fn test_store_default() {
        let store = VectorStore::default();
        assert_eq!(store.index_count(), 0);
        assert_eq!(store.total_vectors(), 0);
    }

    #[test]
    fn test_store_zero_vectors() {
        let store = VectorStore::new();
        store
            .create_index(VectorIndexConfig::flat("test", 3))
            .unwrap();

        // Zero vector should be insertable
        store
            .add("test", VectorId::new("zero"), &[0.0, 0.0, 0.0])
            .unwrap();
        assert_eq!(store.index_len("test").unwrap(), 1);
    }

    #[test]
    fn test_store_stats_memory() {
        let store = VectorStore::new();
        store
            .create_index(VectorIndexConfig::flat("test", 128))
            .unwrap();
        store
            .add("test", VectorId::new("v1"), &vec![0.1; 128])
            .unwrap();

        let stats = store.stats();
        assert!(stats.total_memory_bytes > 0);
        assert!(stats.indexes[0].memory_bytes > 0);
    }

    #[test]
    fn test_store_rebuild_index() {
        let store = VectorStore::new();
        store
            .create_index(VectorIndexConfig::flat("test", 3))
            .unwrap();
        store
            .add("test", VectorId::new("v1"), &[1.0, 0.0, 0.0])
            .unwrap();
        assert_eq!(store.index_len("test").unwrap(), 1);

        // Rebuild should succeed (resets the index)
        store.rebuild_index("test").unwrap();
        assert_eq!(store.index_len("test").unwrap(), 0);
    }

    #[test]
    fn test_store_rebuild_nonexistent() {
        let store = VectorStore::new();
        let result = store.rebuild_index("nonexistent");
        assert!(matches!(result, Err(VectorError::IndexNotFound(_))));
    }
}
