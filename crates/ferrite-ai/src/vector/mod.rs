//! # Vector Search
//!
//! Native vector similarity search for AI/ML workloads with support for
//! multiple index types, distance metrics, and tiered storage integration.
//!
//! ## Overview
//!
//! Ferrite's vector search enables:
//! - Semantic search over documents, images, or any embeddings
//! - Recommendation systems based on similarity
//! - Anomaly detection via distance from normal clusters
//! - RAG (Retrieval-Augmented Generation) for LLM applications
//!
//! ## Index Types
//!
//! | Index | Use Case | Search Complexity | Build Time | Memory |
//! |-------|----------|-------------------|------------|--------|
//! | HNSW | General purpose, high recall | O(log n) | O(n log n) | High |
//! | IVF | Large datasets (>1M vectors) | O(√n) | O(n) | Medium |
//! | Flat | Small datasets, exact results | O(n) | O(1) | Low |
//!
//! ### HNSW (Hierarchical Navigable Small World)
//!
//! Best for most use cases. Provides excellent recall (>95%) with fast queries.
//!
//! Key parameters:
//! - `m`: Number of connections per node (default: 16). Higher = better recall, more memory
//! - `ef_construction`: Search width during build (default: 200). Higher = better index quality
//! - `ef_search`: Search width during query (default: 50). Higher = better recall, slower
//!
//! ### IVF (Inverted File Index)
//!
//! Memory-efficient for very large datasets. Uses k-means clustering.
//!
//! Key parameters:
//! - `nlist`: Number of clusters (default: 1024). ~√n for optimal performance
//! - `nprobe`: Clusters to search (default: 32). Higher = better recall
//!
//! ### Flat
//!
//! Brute-force exact search. Use for small datasets (<10K) or validation.
//!
//! ## Distance Metrics
//!
//! - **Cosine**: Best for text embeddings (normalized vectors)
//! - **Euclidean (L2)**: Best for image embeddings
//! - **Dot Product**: Best for maximum inner product search
//! - **Manhattan (L1)**: Robust to outliers
//!
//! ## Usage Examples
//!
//! ### Creating an Index
//!
//! ```no_run
//! use ferrite::vector::{VectorStore, IndexType, DistanceMetric, VectorIndexConfig};
//!
//! let store = VectorStore::new();
//!
//! // Create HNSW index for text embeddings
//! store.create_index(VectorIndexConfig {
//!     name: "documents".to_string(),
//!     dimension: 384,  // all-MiniLM-L6-v2
//!     index_type: IndexType::Hnsw { m: 16, ef_construction: 200 },
//!     metric: DistanceMetric::Cosine,
//! })?;
//!
//! // Create IVF index for large image dataset
//! store.create_index(VectorIndexConfig {
//!     name: "images".to_string(),
//!     dimension: 512,  // ResNet features
//!     index_type: IndexType::Ivf { nlist: 4096, nprobe: 64 },
//!     metric: DistanceMetric::Euclidean,
//! })?;
//! # Ok::<(), ferrite::vector::VectorError>(())
//! ```
//!
//! ### Adding Vectors
//!
//! ```no_run
//! use ferrite::vector::{VectorStore, VectorData};
//!
//! let store = VectorStore::new();
//!
//! // Add single vector
//! store.add("documents", VectorData {
//!     id: "doc:1".to_string(),
//!     vector: vec![0.1, 0.2, 0.3, /* ... 384 dims */],
//!     metadata: Some(r#"{"title": "Introduction to Rust"}"#.to_string()),
//! })?;
//!
//! // Batch add for efficiency
//! let vectors: Vec<VectorData> = documents
//!     .iter()
//!     .map(|doc| VectorData {
//!         id: doc.id.clone(),
//!         vector: embed(&doc.content),
//!         metadata: Some(serde_json::to_string(&doc.metadata).unwrap_or_default()),
//!     })
//!     .collect();
//!
//! store.add_batch("documents", vectors)?;
//! # Ok::<(), ferrite::vector::VectorError>(())
//! ```
//!
//! ### Searching
//!
//! ```no_run
//! use ferrite::vector::{VectorStore, SearchResult};
//!
//! let store = VectorStore::new();
//! let query_embedding = vec![0.15, 0.22, 0.28, /* ... */];
//!
//! // Basic search
//! let results: Vec<SearchResult> = store.search("documents", &query_embedding, 10)?;
//!
//! for result in results {
//!     println!(
//!         "ID: {}, Score: {:.4}, Metadata: {:?}",
//!         result.id, result.score, result.metadata
//!     );
//! }
//!
//! // Search with metadata filter (if supported by index)
//! let results = store.search_with_filter(
//!     "documents",
//!     &query_embedding,
//!     10,
//!     Some("category = 'tech'"),
//! )?;
//! # Ok::<(), ferrite::vector::VectorError>(())
//! ```
//!
//! ### HNSW-Specific Configuration
//!
//! ```no_run
//! use ferrite::vector::HnswIndex;
//! use ferrite::vector::DistanceMetric;
//!
//! // Create with custom parameters
//! let index = HnswIndex::builder()
//!     .dimension(384)
//!     .metric(DistanceMetric::Cosine)
//!     .m(32)                    // More connections = better recall
//!     .ef_construction(400)     // Higher quality index
//!     .max_elements(1_000_000)  // Pre-allocate for 1M vectors
//!     .build()?;
//!
//! // Adjust search quality at runtime
//! index.set_ef_search(100);  // Higher ef = better recall, slower
//! # Ok::<(), ferrite::vector::VectorError>(())
//! ```
//!
//! ## Performance Tuning
//!
//! ### Memory vs Recall Trade-offs
//!
//! | Configuration | Recall@10 | QPS (1M vectors) | Memory |
//! |---------------|-----------|------------------|--------|
//! | HNSW m=8, ef=50 | 0.92 | 50K | 800MB |
//! | HNSW m=16, ef=100 | 0.97 | 35K | 1.2GB |
//! | HNSW m=32, ef=200 | 0.99 | 20K | 2.0GB |
//! | IVF nprobe=16 | 0.90 | 40K | 500MB |
//! | IVF nprobe=64 | 0.96 | 15K | 500MB |
//!
//! ### Best Practices
//!
//! 1. **Normalize vectors** for cosine similarity before indexing
//! 2. **Batch inserts** for better build performance
//! 3. **Use IVF** for datasets >5M vectors
//! 4. **Tune ef_search** based on latency requirements
//! 5. **Pre-filter** when possible to reduce search space

mod distance;
pub mod filter;
mod flat;
mod hnsw;
mod ivf;
pub mod persistence;
pub mod quantization;
pub mod search;
mod store;
mod types;

pub use distance::{cosine_similarity, dot_product, euclidean_distance, manhattan_distance};
pub use filter::{
    FilterCondition, FilterOp, FilterStrategy, FilteredSearchOptions, MetadataFilter,
};
pub use flat::FlatIndex;
pub use hnsw::HnswIndex;
pub use ivf::IvfIndex;
pub use persistence::{deserialize_hnsw, recover_hnsw, serialize_hnsw, ChangeTracker};
pub use quantization::{ProductQuantizer, QuantizationConfig, QuantizationType, ScalarQuantizer};
pub use search::{
    batch_search, multi_vector_search, range_search, BatchSearchResult, MultiVectorStrategy,
    RangeSearchResult,
};
pub use store::VectorStore;
pub use types::{
    AttributeValue, DistanceMetric, IndexType, SearchResult, VectorData, VectorEntry, VectorId,
    VectorIndexConfig,
};

use serde::{Deserialize, Serialize};

/// Configuration for vector search
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorConfig {
    /// Enable vector search
    pub enabled: bool,
    /// Default distance metric
    pub default_metric: DistanceMetric,
    /// Default EF for HNSW search
    pub default_ef_search: usize,
    /// Maximum dimension size
    pub max_dimension: usize,
    /// Maximum vectors per index
    pub max_vectors: usize,
}

impl Default for VectorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_metric: DistanceMetric::Cosine,
            default_ef_search: 50,
            max_dimension: 4096,
            max_vectors: 100_000_000,
        }
    }
}

/// Trait for vector indexes
pub trait VectorIndex: Send + Sync {
    /// Add a vector to the index
    fn add(&self, id: VectorId, vector: &[f32]) -> Result<(), VectorError>;

    /// Remove a vector from the index
    fn remove(&self, id: &VectorId) -> Result<bool, VectorError>;

    /// Search for k nearest neighbors
    fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, VectorError>;

    /// Get the dimension of vectors in this index
    fn dimension(&self) -> usize;

    /// Get the number of vectors in the index
    fn len(&self) -> usize;

    /// Check if the index is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the distance metric
    fn metric(&self) -> DistanceMetric;
}

/// Errors that can occur in vector operations
#[derive(Debug, Clone, thiserror::Error)]
pub enum VectorError {
    /// Dimension mismatch
    #[error("dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch {
        /// Expected dimension
        expected: usize,
        /// Got dimension
        got: usize,
    },

    /// Vector not found
    #[error("vector not found: {0}")]
    NotFound(String),

    /// Index already exists
    #[error("index already exists: {0}")]
    IndexExists(String),

    /// Index not found
    #[error("index not found: {0}")]
    IndexNotFound(String),

    /// Invalid configuration
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// Capacity exceeded
    #[error("capacity exceeded: {0}")]
    CapacityExceeded(String),

    /// Internal error
    #[error("internal error: {0}")]
    Internal(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_config_default() {
        let config = VectorConfig::default();
        assert!(config.enabled);
        assert_eq!(config.default_metric, DistanceMetric::Cosine);
        assert_eq!(config.max_dimension, 4096);
    }
}
