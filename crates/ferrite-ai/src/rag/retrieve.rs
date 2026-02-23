//! Vector Retrieval
//!
//! Handles similarity search, hybrid search, and result ranking.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::chunk::{Chunk, ChunkId};
use super::document::DocumentId;

/// Similarity metric for vector comparison
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SimilarityMetric {
    /// Cosine similarity (normalized dot product)
    Cosine,
    /// Euclidean distance (L2)
    Euclidean,
    /// Dot product (for normalized vectors)
    DotProduct,
    /// Manhattan distance (L1)
    Manhattan,
}

impl Default for SimilarityMetric {
    fn default() -> Self {
        Self::Cosine
    }
}

/// Search filter for metadata-based filtering
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SearchFilter {
    /// Filter by document IDs
    pub document_ids: Option<Vec<DocumentId>>,
    /// Filter by tags (any match)
    pub tags: Option<Vec<String>>,
    /// Filter by metadata attributes
    pub attributes: HashMap<String, FilterValue>,
    /// Minimum score threshold
    pub min_score: Option<f32>,
    /// Maximum results
    pub limit: Option<usize>,
}

impl SearchFilter {
    /// Create a new empty filter
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by document IDs
    pub fn with_documents(mut self, ids: Vec<DocumentId>) -> Self {
        self.document_ids = Some(ids);
        self
    }

    /// Filter by tags
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = Some(tags);
        self
    }

    /// Add attribute filter
    pub fn with_attribute(mut self, key: impl Into<String>, value: FilterValue) -> Self {
        self.attributes.insert(key.into(), value);
        self
    }

    /// Set minimum score
    pub fn with_min_score(mut self, score: f32) -> Self {
        self.min_score = Some(score);
        self
    }

    /// Set result limit
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Check if a chunk matches the filter
    pub fn matches(&self, chunk: &Chunk, score: f32) -> bool {
        // Check minimum score
        if let Some(min_score) = self.min_score {
            if score < min_score {
                return false;
            }
        }

        // Check document ID filter
        if let Some(ref doc_ids) = self.document_ids {
            if !doc_ids.contains(&chunk.document_id) {
                return false;
            }
        }

        // Check tag filter
        if let Some(ref filter_tags) = self.tags {
            let has_matching_tag = filter_tags
                .iter()
                .any(|tag| chunk.metadata.tags.contains(tag));
            if !has_matching_tag {
                return false;
            }
        }

        // Check attribute filters
        for (key, filter_value) in &self.attributes {
            if let Some(chunk_value) = chunk.metadata.attributes.get(key) {
                if !filter_value.matches(chunk_value) {
                    return false;
                }
            } else {
                return false;
            }
        }

        true
    }
}

/// Filter value for attribute matching
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterValue {
    /// Exact string match
    Equals(String),
    /// String contains
    Contains(String),
    /// Numeric equals
    NumericEquals(f64),
    /// Numeric range
    NumericRange { min: Option<f64>, max: Option<f64> },
    /// Boolean value
    Boolean(bool),
    /// In list
    In(Vec<String>),
    /// Not in list
    NotIn(Vec<String>),
}

impl FilterValue {
    /// Check if a value matches this filter
    pub fn matches(&self, value: &super::document::AttributeValue) -> bool {
        use super::document::AttributeValue;

        match (self, value) {
            (FilterValue::Equals(pattern), AttributeValue::String(s)) => s == pattern,
            (FilterValue::Contains(pattern), AttributeValue::String(s)) => s.contains(pattern),
            (FilterValue::NumericEquals(n), AttributeValue::Integer(i)) => {
                (*i as f64 - n).abs() < f64::EPSILON
            }
            (FilterValue::NumericEquals(n), AttributeValue::Float(f)) => {
                (f - n).abs() < f64::EPSILON
            }
            (FilterValue::NumericRange { min, max }, AttributeValue::Integer(i)) => {
                let val = *i as f64;
                min.map_or(true, |m| val >= m) && max.map_or(true, |m| val <= m)
            }
            (FilterValue::NumericRange { min, max }, AttributeValue::Float(f)) => {
                min.map_or(true, |m| *f >= m) && max.map_or(true, |m| *f <= m)
            }
            (FilterValue::Boolean(b), AttributeValue::Boolean(v)) => b == v,
            (FilterValue::In(list), AttributeValue::String(s)) => list.contains(s),
            (FilterValue::NotIn(list), AttributeValue::String(s)) => !list.contains(s),
            _ => false,
        }
    }
}

/// A retrieval result with score and metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetrievalResult {
    /// The matched chunk
    pub chunk: Chunk,
    /// Similarity score (0.0 to 1.0)
    pub score: f32,
    /// Rank in results (1-indexed)
    pub rank: usize,
    /// Source of the match (vector, keyword, hybrid)
    pub source: MatchSource,
}

/// Source of a match
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MatchSource {
    /// Vector similarity search
    Vector,
    /// Keyword/BM25 search
    Keyword,
    /// Hybrid (combined) search
    Hybrid,
}

/// Vector index entry
#[derive(Debug, Clone)]
struct VectorEntry {
    /// Chunk ID
    #[allow(dead_code)] // Planned for v0.2 — stored for chunk-level retrieval tracking
    chunk_id: ChunkId,
    /// The chunk data
    chunk: Chunk,
    /// Embedding vector
    embedding: Vec<f32>,
}

/// In-memory vector store for retrieval
pub struct VectorStore {
    /// Stored vectors indexed by chunk ID
    vectors: RwLock<HashMap<ChunkId, VectorEntry>>,
    /// Document to chunks mapping
    doc_chunks: RwLock<HashMap<DocumentId, Vec<ChunkId>>>,
    /// Embedding dimension
    dimension: usize,
    /// Similarity metric
    metric: SimilarityMetric,
    /// Statistics
    stats: VectorStoreStats,
}

/// Vector store statistics
#[derive(Debug, Default)]
pub struct VectorStoreStats {
    /// Total vectors stored
    pub total_vectors: AtomicU64,
    /// Total searches performed
    pub total_searches: AtomicU64,
    /// Average search latency (microseconds)
    pub avg_search_latency_us: AtomicU64,
}

impl VectorStore {
    /// Create a new vector store
    pub fn new(dimension: usize, metric: SimilarityMetric) -> Self {
        Self {
            vectors: RwLock::new(HashMap::new()),
            doc_chunks: RwLock::new(HashMap::new()),
            dimension,
            metric,
            stats: VectorStoreStats::default(),
        }
    }

    /// Add a vector to the store
    pub fn add(&self, chunk: Chunk, embedding: Vec<f32>) -> Result<(), RetrievalError> {
        if embedding.len() != self.dimension {
            return Err(RetrievalError::DimensionMismatch {
                expected: self.dimension,
                got: embedding.len(),
            });
        }

        let chunk_id = chunk.id.clone();
        let doc_id = chunk.document_id.clone();

        let entry = VectorEntry {
            chunk_id: chunk_id.clone(),
            chunk,
            embedding,
        };

        {
            let mut vectors = self.vectors.write();
            vectors.insert(chunk_id.clone(), entry);
        }

        {
            let mut doc_chunks = self.doc_chunks.write();
            doc_chunks.entry(doc_id).or_default().push(chunk_id);
        }

        self.stats.total_vectors.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Remove vectors for a document
    pub fn remove_document(&self, doc_id: &DocumentId) -> usize {
        let chunk_ids = {
            let mut doc_chunks = self.doc_chunks.write();
            doc_chunks.remove(doc_id).unwrap_or_default()
        };

        let count = chunk_ids.len();
        {
            let mut vectors = self.vectors.write();
            for chunk_id in chunk_ids {
                vectors.remove(&chunk_id);
            }
        }

        self.stats
            .total_vectors
            .fetch_sub(count as u64, Ordering::Relaxed);
        count
    }

    /// Search for similar vectors
    pub fn search(
        &self,
        query_embedding: &[f32],
        filter: &SearchFilter,
        k: usize,
    ) -> Result<Vec<RetrievalResult>, RetrievalError> {
        if query_embedding.len() != self.dimension {
            return Err(RetrievalError::DimensionMismatch {
                expected: self.dimension,
                got: query_embedding.len(),
            });
        }

        let start = std::time::Instant::now();
        let vectors = self.vectors.read();

        // Compute similarities
        let mut scored: Vec<(f32, &VectorEntry)> = vectors
            .values()
            .filter_map(|entry| {
                let score = self.compute_similarity(query_embedding, &entry.embedding);
                if filter.matches(&entry.chunk, score) {
                    Some((score, entry))
                } else {
                    None
                }
            })
            .collect();

        // Sort by score (descending)
        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        // Take top k
        let limit = filter.limit.unwrap_or(k).min(k);
        let results: Vec<RetrievalResult> = scored
            .into_iter()
            .take(limit)
            .enumerate()
            .map(|(i, (score, entry))| RetrievalResult {
                chunk: entry.chunk.clone(),
                score,
                rank: i + 1,
                source: MatchSource::Vector,
            })
            .collect();

        let elapsed = start.elapsed().as_micros() as u64;
        self.stats.total_searches.fetch_add(1, Ordering::Relaxed);
        // Simple moving average approximation
        let _ = self.stats.avg_search_latency_us.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |old| Some((old + elapsed) / 2),
        );

        Ok(results)
    }

    /// Compute similarity between two vectors
    fn compute_similarity(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.metric {
            SimilarityMetric::Cosine => cosine_similarity(a, b),
            SimilarityMetric::Euclidean => euclidean_similarity(a, b),
            SimilarityMetric::DotProduct => dot_product(a, b),
            SimilarityMetric::Manhattan => manhattan_similarity(a, b),
        }
    }

    /// Get statistics
    pub fn stats(&self) -> &VectorStoreStats {
        &self.stats
    }

    /// Get vector count
    pub fn len(&self) -> usize {
        self.vectors.read().len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.vectors.read().is_empty()
    }

    /// Clear all vectors
    pub fn clear(&self) {
        self.vectors.write().clear();
        self.doc_chunks.write().clear();
        self.stats.total_vectors.store(0, Ordering::Release);
    }
}

/// Cosine similarity (normalized dot product)
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        0.0
    } else {
        dot / (norm_a * norm_b)
    }
}

/// Euclidean distance converted to similarity
fn euclidean_similarity(a: &[f32], b: &[f32]) -> f32 {
    let distance: f32 = a
        .iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f32>()
        .sqrt();

    // Convert distance to similarity (0 to 1)
    1.0 / (1.0 + distance)
}

/// Dot product similarity
fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

/// Manhattan distance converted to similarity
fn manhattan_similarity(a: &[f32], b: &[f32]) -> f32 {
    let distance: f32 = a.iter().zip(b.iter()).map(|(x, y)| (x - y).abs()).sum();
    1.0 / (1.0 + distance)
}

/// Retriever configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetrieverConfig {
    /// Vector store dimension
    pub dimension: usize,
    /// Similarity metric
    #[serde(default)]
    pub metric: SimilarityMetric,
    /// Default number of results
    #[serde(default = "default_top_k")]
    pub default_top_k: usize,
    /// Enable hybrid search (vector + keyword)
    #[serde(default)]
    pub hybrid_search: bool,
    /// Hybrid search alpha (0 = keyword only, 1 = vector only)
    #[serde(default = "default_hybrid_alpha")]
    pub hybrid_alpha: f32,
}

fn default_top_k() -> usize {
    5
}

fn default_hybrid_alpha() -> f32 {
    0.7
}

impl Default for RetrieverConfig {
    fn default() -> Self {
        Self {
            dimension: 384,
            metric: SimilarityMetric::default(),
            default_top_k: default_top_k(),
            hybrid_search: false,
            hybrid_alpha: default_hybrid_alpha(),
        }
    }
}

/// The retriever handles all search operations
pub struct Retriever {
    /// Vector store
    vector_store: VectorStore,
    /// Configuration
    config: RetrieverConfig,
    /// Keyword index for hybrid search
    keyword_index: RwLock<HashMap<String, Vec<(ChunkId, f32)>>>,
}

impl Retriever {
    /// Create a new retriever
    pub fn new(config: RetrieverConfig) -> Self {
        Self {
            vector_store: VectorStore::new(config.dimension, config.metric),
            config,
            keyword_index: RwLock::new(HashMap::new()),
        }
    }

    /// Index a chunk with its embedding
    pub fn index(&self, chunk: Chunk, embedding: Vec<f32>) -> Result<(), RetrievalError> {
        // Index for vector search
        self.vector_store.add(chunk.clone(), embedding)?;

        // Index for keyword search if hybrid is enabled
        if self.config.hybrid_search {
            self.index_keywords(&chunk);
        }

        Ok(())
    }

    /// Index keywords from a chunk
    fn index_keywords(&self, chunk: &Chunk) {
        let mut index = self.keyword_index.write();

        // Simple tokenization
        for word in chunk.content.split_whitespace() {
            let word = word.to_lowercase();
            let word = word.trim_matches(|c: char| !c.is_alphanumeric());
            if word.len() >= 2 {
                // TF-IDF approximation: just count occurrences for now
                index
                    .entry(word.to_string())
                    .or_default()
                    .push((chunk.id.clone(), 1.0));
            }
        }
    }

    /// Search for relevant chunks
    pub fn search(
        &self,
        query_embedding: &[f32],
        query_text: Option<&str>,
        filter: &SearchFilter,
    ) -> Result<Vec<RetrievalResult>, RetrievalError> {
        let k = filter.limit.unwrap_or(self.config.default_top_k);

        if self.config.hybrid_search && query_text.is_some() {
            self.hybrid_search(query_embedding, query_text.unwrap_or_default(), filter, k)
        } else {
            self.vector_store.search(query_embedding, filter, k)
        }
    }

    /// Perform hybrid search
    fn hybrid_search(
        &self,
        query_embedding: &[f32],
        query_text: &str,
        filter: &SearchFilter,
        k: usize,
    ) -> Result<Vec<RetrievalResult>, RetrievalError> {
        // Get vector results
        let vector_results = self.vector_store.search(query_embedding, filter, k * 2)?;

        // Get keyword results
        let keyword_results = self.keyword_search(query_text, filter, k * 2);

        // Merge results with reciprocal rank fusion
        let merged = self.reciprocal_rank_fusion(vector_results, keyword_results, k);

        Ok(merged)
    }

    /// Simple keyword search using BM25-like scoring
    fn keyword_search(&self, query: &str, filter: &SearchFilter, k: usize) -> Vec<RetrievalResult> {
        let index = self.keyword_index.read();
        let mut chunk_scores: HashMap<ChunkId, f32> = HashMap::new();

        // Score chunks by keyword matches
        for word in query.split_whitespace() {
            let word = word.to_lowercase();
            let word = word.trim_matches(|c: char| !c.is_alphanumeric());

            if let Some(postings) = index.get(word) {
                for (chunk_id, tf) in postings {
                    *chunk_scores.entry(chunk_id.clone()).or_insert(0.0) += tf;
                }
            }
        }

        // Get chunks and sort by score
        let vectors = self.vector_store.vectors.read();
        let mut results: Vec<_> = chunk_scores
            .into_iter()
            .filter_map(|(chunk_id, score)| {
                vectors.get(&chunk_id).and_then(|entry| {
                    if filter.matches(&entry.chunk, score) {
                        Some((entry.chunk.clone(), score))
                    } else {
                        None
                    }
                })
            })
            .collect();

        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        results
            .into_iter()
            .take(k)
            .enumerate()
            .map(|(i, (chunk, score))| RetrievalResult {
                chunk,
                score,
                rank: i + 1,
                source: MatchSource::Keyword,
            })
            .collect()
    }

    /// Reciprocal Rank Fusion for combining vector and keyword results
    fn reciprocal_rank_fusion(
        &self,
        vector_results: Vec<RetrievalResult>,
        keyword_results: Vec<RetrievalResult>,
        k: usize,
    ) -> Vec<RetrievalResult> {
        const RRF_K: f32 = 60.0; // Standard RRF constant

        let mut fused_scores: HashMap<ChunkId, (f32, Option<Chunk>)> = HashMap::new();

        // Add vector results
        for result in &vector_results {
            let rrf_score = self.config.hybrid_alpha / (RRF_K + result.rank as f32);
            fused_scores.insert(
                result.chunk.id.clone(),
                (rrf_score, Some(result.chunk.clone())),
            );
        }

        // Add keyword results
        for result in &keyword_results {
            let rrf_score = (1.0 - self.config.hybrid_alpha) / (RRF_K + result.rank as f32);
            fused_scores
                .entry(result.chunk.id.clone())
                .and_modify(|(score, _)| *score += rrf_score)
                .or_insert((rrf_score, Some(result.chunk.clone())));
        }

        // Sort and take top k
        let mut sorted: Vec<_> = fused_scores.into_iter().collect();
        sorted.sort_by(|a, b| {
            b.1 .0
                .partial_cmp(&a.1 .0)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        sorted
            .into_iter()
            .take(k)
            .enumerate()
            .filter_map(|(i, (_, (score, chunk)))| {
                chunk.map(|c| RetrievalResult {
                    chunk: c,
                    score,
                    rank: i + 1,
                    source: MatchSource::Hybrid,
                })
            })
            .collect()
    }

    /// Remove a document from the index
    pub fn remove_document(&self, doc_id: &DocumentId) -> usize {
        self.vector_store.remove_document(doc_id)
    }

    /// Get retriever statistics
    pub fn stats(&self) -> &VectorStoreStats {
        self.vector_store.stats()
    }

    /// Get indexed chunk count
    pub fn len(&self) -> usize {
        self.vector_store.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.vector_store.is_empty()
    }

    /// Clear all indexed data
    pub fn clear(&self) {
        self.vector_store.clear();
        self.keyword_index.write().clear();
    }
}

/// Retrieval errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum RetrievalError {
    /// Dimension mismatch
    #[error("embedding dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch { expected: usize, got: usize },

    /// Index error
    #[error("index error: {0}")]
    IndexError(String),

    /// Search error
    #[error("search error: {0}")]
    SearchError(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rag::chunk::ChunkMetadata;

    fn create_test_chunk(id: &str, content: &str) -> Chunk {
        Chunk {
            id: ChunkId::from_string(id),
            document_id: DocumentId::from_string("doc:1"),
            content: content.to_string(),
            start_offset: 0,
            end_offset: content.len(),
            index: 0,
            total_chunks: 1,
            metadata: ChunkMetadata::default(),
            embedding: None,
        }
    }

    #[test]
    fn test_cosine_similarity() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        assert!((cosine_similarity(&a, &b) - 1.0).abs() < f32::EPSILON);

        let c = vec![0.0, 1.0, 0.0];
        assert!(cosine_similarity(&a, &c).abs() < f32::EPSILON);
    }

    #[test]
    fn test_vector_store() {
        let store = VectorStore::new(3, SimilarityMetric::Cosine);

        let chunk1 = create_test_chunk("chunk:1", "Hello world");
        let chunk2 = create_test_chunk("chunk:2", "Goodbye world");

        store.add(chunk1, vec![1.0, 0.0, 0.0]).unwrap();
        store.add(chunk2, vec![0.0, 1.0, 0.0]).unwrap();

        assert_eq!(store.len(), 2);

        let results = store
            .search(&[1.0, 0.0, 0.0], &SearchFilter::default(), 2)
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].chunk.id.as_str(), "chunk:1");
        assert!((results[0].score - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn test_search_filter() {
        let store = VectorStore::new(3, SimilarityMetric::Cosine);

        let mut chunk1 = create_test_chunk("chunk:1", "Hello");
        chunk1.metadata.tags.push("important".to_string());

        let chunk2 = create_test_chunk("chunk:2", "World");

        store.add(chunk1, vec![1.0, 0.0, 0.0]).unwrap();
        store.add(chunk2, vec![0.9, 0.1, 0.0]).unwrap();

        let filter = SearchFilter::new().with_tags(vec!["important".to_string()]);

        let results = store.search(&[1.0, 0.0, 0.0], &filter, 10).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].chunk.id.as_str(), "chunk:1");
    }

    #[test]
    fn test_retriever() {
        let config = RetrieverConfig {
            dimension: 3,
            ..Default::default()
        };
        let retriever = Retriever::new(config);

        let chunk = create_test_chunk("chunk:1", "Test content");
        retriever.index(chunk, vec![1.0, 0.0, 0.0]).unwrap();

        let results = retriever
            .search(&[1.0, 0.0, 0.0], None, &SearchFilter::default())
            .unwrap();

        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_dimension_mismatch() {
        let store = VectorStore::new(3, SimilarityMetric::Cosine);

        let chunk = create_test_chunk("chunk:1", "Test");
        let result = store.add(chunk, vec![1.0, 0.0]); // Wrong dimension

        assert!(matches!(
            result,
            Err(RetrievalError::DimensionMismatch { .. })
        ));
    }

    #[test]
    fn test_min_score_filter() {
        let store = VectorStore::new(3, SimilarityMetric::Cosine);

        let chunk1 = create_test_chunk("chunk:1", "Close");
        let chunk2 = create_test_chunk("chunk:2", "Far");

        store.add(chunk1, vec![1.0, 0.0, 0.0]).unwrap();
        store.add(chunk2, vec![0.0, 1.0, 0.0]).unwrap();

        let filter = SearchFilter::new().with_min_score(0.5);

        let results = store.search(&[1.0, 0.0, 0.0], &filter, 10).unwrap();

        assert_eq!(results.len(), 1);
        assert!(results[0].score >= 0.5);
    }

    #[test]
    fn test_remove_document() {
        let store = VectorStore::new(3, SimilarityMetric::Cosine);

        let chunk1 = create_test_chunk("chunk:1", "Doc 1");
        let mut chunk2 = create_test_chunk("chunk:2", "Doc 2");
        chunk2.document_id = DocumentId::from_string("doc:2");

        store.add(chunk1, vec![1.0, 0.0, 0.0]).unwrap();
        store.add(chunk2, vec![0.0, 1.0, 0.0]).unwrap();

        assert_eq!(store.len(), 2);

        let removed = store.remove_document(&DocumentId::from_string("doc:1"));
        assert_eq!(removed, 1);
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_euclidean_similarity() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![0.0, 0.0, 0.0];
        assert!((euclidean_similarity(&a, &b) - 1.0).abs() < f32::EPSILON);

        let c = vec![1.0, 0.0, 0.0];
        assert!(euclidean_similarity(&a, &c) < 1.0);
    }

    #[test]
    fn test_filter_value_matches() {
        use crate::rag::document::AttributeValue;

        let filter = FilterValue::Equals("test".to_string());
        assert!(filter.matches(&AttributeValue::String("test".to_string())));
        assert!(!filter.matches(&AttributeValue::String("other".to_string())));

        let range = FilterValue::NumericRange {
            min: Some(0.0),
            max: Some(10.0),
        };
        assert!(range.matches(&AttributeValue::Integer(5)));
        assert!(!range.matches(&AttributeValue::Integer(15)));
    }
}
