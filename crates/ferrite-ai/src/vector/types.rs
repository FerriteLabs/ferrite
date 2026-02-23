//! Vector search types

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;

/// Unique identifier for a vector
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VectorId(pub String);

impl VectorId {
    /// Create a new vector ID
    pub fn new<S: Into<String>>(id: S) -> Self {
        Self(id.into())
    }

    /// Get the ID as a string slice
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for VectorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for VectorId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for VectorId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Distance metrics for vector similarity
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum DistanceMetric {
    /// Cosine similarity (1 - cosine_similarity for distance)
    #[default]
    Cosine,
    /// Euclidean distance (L2)
    Euclidean,
    /// Dot product (negative for distance)
    DotProduct,
    /// Manhattan distance (L1)
    Manhattan,
}

impl fmt::Display for DistanceMetric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DistanceMetric::Cosine => write!(f, "cosine"),
            DistanceMetric::Euclidean => write!(f, "euclidean"),
            DistanceMetric::DotProduct => write!(f, "dot_product"),
            DistanceMetric::Manhattan => write!(f, "manhattan"),
        }
    }
}

/// Vector data storage formats
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum VectorData {
    /// Full precision float32
    F32(Vec<f32>),
    /// Half precision float16 (stored as u16)
    F16(Vec<u16>),
    /// Product quantized (compressed)
    PQ(Vec<u8>),
    /// Binary quantized
    Binary(Vec<u8>),
}

impl VectorData {
    /// Get the dimension of the vector
    pub fn dimension(&self) -> usize {
        match self {
            VectorData::F32(v) => v.len(),
            VectorData::F16(v) => v.len(),
            VectorData::PQ(v) => v.len() * 8, // Approximate
            VectorData::Binary(v) => v.len() * 8,
        }
    }

    /// Convert to f32 slice (only for F32 variant)
    pub fn as_f32(&self) -> Option<&[f32]> {
        match self {
            VectorData::F32(v) => Some(v),
            _ => None,
        }
    }

    /// Create from f32 slice
    pub fn from_f32(data: &[f32]) -> Self {
        VectorData::F32(data.to_vec())
    }
}

/// Attribute value for filtering
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
    /// List of integers
    IntegerList(Vec<i64>),
}

impl AttributeValue {
    /// Get as string
    pub fn as_string(&self) -> Option<&str> {
        match self {
            AttributeValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Get as integer
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            AttributeValue::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Get as float
    pub fn as_float(&self) -> Option<f64> {
        match self {
            AttributeValue::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Get as boolean
    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            AttributeValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }
}

/// A vector entry with metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorEntry {
    /// Unique identifier
    pub id: VectorId,
    /// The vector data
    pub data: VectorData,
    /// Associated Redis key (optional)
    pub redis_key: Option<Bytes>,
    /// Custom attributes for filtering
    pub attributes: HashMap<String, AttributeValue>,
}

impl VectorEntry {
    /// Create a new vector entry
    pub fn new(id: VectorId, data: VectorData) -> Self {
        Self {
            id,
            data,
            redis_key: None,
            attributes: HashMap::new(),
        }
    }

    /// Create from f32 slice
    pub fn from_f32<I: Into<VectorId>>(id: I, vector: &[f32]) -> Self {
        Self {
            id: id.into(),
            data: VectorData::from_f32(vector),
            redis_key: None,
            attributes: HashMap::new(),
        }
    }

    /// Add an attribute
    pub fn with_attribute<K: Into<String>>(mut self, key: K, value: AttributeValue) -> Self {
        self.attributes.insert(key.into(), value);
        self
    }

    /// Set the Redis key
    pub fn with_redis_key(mut self, key: Bytes) -> Self {
        self.redis_key = Some(key);
        self
    }

    /// Get the dimension
    pub fn dimension(&self) -> usize {
        self.data.dimension()
    }
}

/// Search result with score
#[derive(Clone, Debug)]
pub struct SearchResult {
    /// Vector ID
    pub id: VectorId,
    /// Similarity/distance score
    pub score: f32,
    /// Optional vector data
    pub vector: Option<Vec<f32>>,
    /// Optional attributes
    pub attributes: Option<HashMap<String, AttributeValue>>,
}

impl SearchResult {
    /// Create a new search result
    pub fn new(id: VectorId, score: f32) -> Self {
        Self {
            id,
            score,
            vector: None,
            attributes: None,
        }
    }

    /// Add vector data
    pub fn with_vector(mut self, vector: Vec<f32>) -> Self {
        self.vector = Some(vector);
        self
    }

    /// Add attributes
    pub fn with_attributes(mut self, attributes: HashMap<String, AttributeValue>) -> Self {
        self.attributes = Some(attributes);
        self
    }
}

/// Index type
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexType {
    /// HNSW index
    #[default]
    Hnsw,
    /// IVF index
    Ivf,
    /// Flat (brute force) index
    Flat,
}

impl fmt::Display for IndexType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexType::Hnsw => write!(f, "hnsw"),
            IndexType::Ivf => write!(f, "ivf"),
            IndexType::Flat => write!(f, "flat"),
        }
    }
}

/// Configuration for a vector index
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorIndexConfig {
    /// Index name
    pub name: String,
    /// Vector dimension
    pub dimension: usize,
    /// Distance metric
    pub metric: DistanceMetric,
    /// Index type
    pub index_type: IndexType,
    /// HNSW M parameter (max connections)
    pub hnsw_m: usize,
    /// HNSW ef_construction parameter
    pub hnsw_ef_construction: usize,
    /// HNSW ef_search parameter
    pub hnsw_ef_search: usize,
    /// IVF number of clusters
    pub ivf_n_clusters: usize,
    /// IVF number of probes
    pub ivf_n_probe: usize,
}

impl VectorIndexConfig {
    /// Create default HNSW config
    pub fn hnsw(name: &str, dimension: usize) -> Self {
        Self {
            name: name.to_string(),
            dimension,
            metric: DistanceMetric::Cosine,
            index_type: IndexType::Hnsw,
            hnsw_m: 16,
            hnsw_ef_construction: 200,
            hnsw_ef_search: 50,
            ivf_n_clusters: 0,
            ivf_n_probe: 0,
        }
    }

    /// Create default Flat config
    pub fn flat(name: &str, dimension: usize) -> Self {
        Self {
            name: name.to_string(),
            dimension,
            metric: DistanceMetric::Cosine,
            index_type: IndexType::Flat,
            hnsw_m: 0,
            hnsw_ef_construction: 0,
            hnsw_ef_search: 0,
            ivf_n_clusters: 0,
            ivf_n_probe: 0,
        }
    }

    /// Create default IVF config
    ///
    /// # Arguments
    ///
    /// * `name` - Index name
    /// * `dimension` - Vector dimension
    /// * `n_clusters` - Number of clusters (typically sqrt(expected_size))
    pub fn ivf(name: &str, dimension: usize, n_clusters: usize) -> Self {
        Self {
            name: name.to_string(),
            dimension,
            metric: DistanceMetric::Cosine,
            index_type: IndexType::Ivf,
            hnsw_m: 0,
            hnsw_ef_construction: 0,
            hnsw_ef_search: 0,
            ivf_n_clusters: n_clusters,
            ivf_n_probe: (n_clusters as f64).sqrt().ceil() as usize,
        }
    }

    /// Set the distance metric
    pub fn with_metric(mut self, metric: DistanceMetric) -> Self {
        self.metric = metric;
        self
    }

    /// Set HNSW M parameter
    pub fn with_hnsw_m(mut self, m: usize) -> Self {
        self.hnsw_m = m;
        self
    }

    /// Set HNSW ef_construction
    pub fn with_ef_construction(mut self, ef: usize) -> Self {
        self.hnsw_ef_construction = ef;
        self
    }

    /// Set HNSW ef_search
    pub fn with_ef_search(mut self, ef: usize) -> Self {
        self.hnsw_ef_search = ef;
        self
    }

    /// Set IVF n_clusters
    pub fn with_ivf_n_clusters(mut self, n_clusters: usize) -> Self {
        self.ivf_n_clusters = n_clusters;
        self
    }

    /// Set IVF n_probe (number of clusters to search)
    pub fn with_ivf_n_probe(mut self, n_probe: usize) -> Self {
        self.ivf_n_probe = n_probe;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_id() {
        let id = VectorId::new("doc:123");
        assert_eq!(id.as_str(), "doc:123");
        assert_eq!(format!("{}", id), "doc:123");
    }

    #[test]
    fn test_vector_data() {
        let data = VectorData::from_f32(&[1.0, 2.0, 3.0]);
        assert_eq!(data.dimension(), 3);
        assert_eq!(data.as_f32(), Some(&[1.0, 2.0, 3.0][..]));
    }

    #[test]
    fn test_vector_entry() {
        let entry = VectorEntry::from_f32("doc:1", &[1.0, 2.0, 3.0])
            .with_attribute("category", AttributeValue::String("tech".to_string()));

        assert_eq!(entry.id.as_str(), "doc:1");
        assert_eq!(entry.dimension(), 3);
        assert!(entry.attributes.contains_key("category"));
    }

    #[test]
    fn test_distance_metric() {
        assert_eq!(DistanceMetric::default(), DistanceMetric::Cosine);
        assert_eq!(format!("{}", DistanceMetric::Euclidean), "euclidean");
    }

    #[test]
    fn test_index_config() {
        let config = VectorIndexConfig::hnsw("embeddings", 384)
            .with_metric(DistanceMetric::DotProduct)
            .with_hnsw_m(32);

        assert_eq!(config.name, "embeddings");
        assert_eq!(config.dimension, 384);
        assert_eq!(config.metric, DistanceMetric::DotProduct);
        assert_eq!(config.hnsw_m, 32);
    }
}
