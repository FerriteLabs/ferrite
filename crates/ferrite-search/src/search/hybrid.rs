//! Hybrid Search Module
//!
//! Combines vector similarity search with keyword (BM25) search for
//! improved retrieval quality. Uses various fusion strategies to
//! combine results from different retrieval methods.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Hybrid Search Engine                      │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Query ─────┬──────────────────────────────────────────────│
//! │             │                                                │
//! │     ┌───────▼───────┐           ┌───────────────────┐       │
//! │     │ Vector Search │           │  Keyword Search   │       │
//! │     │   (Embeddings)│           │     (BM25)        │       │
//! │     └───────┬───────┘           └─────────┬─────────┘       │
//! │             │                             │                  │
//! │     ┌───────▼─────────────────────────────▼───────┐         │
//! │     │              Fusion Engine                   │         │
//! │     │  (RRF | Linear | Learned | Weighted)        │         │
//! │     └───────────────────┬─────────────────────────┘         │
//! │                         │                                    │
//! │     ┌───────────────────▼─────────────────────────┐         │
//! │     │            Reranked Results                  │         │
//! │     └─────────────────────────────────────────────┘         │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Fusion Strategies
//!
//! - **RRF (Reciprocal Rank Fusion)**: Combines rankings with `1/(k + rank)`
//! - **Linear Combination**: Weighted sum of normalized scores
//! - **Learned Fusion**: ML-based score combination
//! - **Convex Combination**: Configurable alpha blending

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Fusion strategy for combining search results
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FusionStrategy {
    /// Reciprocal Rank Fusion with parameter k
    /// Score = sum(1 / (k + rank))
    RRF {
        /// RRF constant (typical values: 20-60)
        k: u32,
    },
    /// Linear combination of normalized scores
    /// Score = alpha * vector_score + (1 - alpha) * keyword_score
    Linear {
        /// Weight for vector search results
        alpha: f32,
    },
    /// Weighted combination with custom weights
    Weighted {
        /// Weight for vector scores
        vector_weight: f32,
        /// Weight for keyword scores
        keyword_weight: f32,
    },
    /// Relative Score Fusion
    /// Normalizes scores relative to min/max in each result set
    RelativeScoreFusion,
    /// Distribution-Based Score Fusion
    /// Uses mean and standard deviation for normalization
    DistributionScoreFusion,
    /// Minimum of scores (intersection-like behavior)
    Min,
    /// Maximum of scores (union-like behavior)
    Max,
}

impl Default for FusionStrategy {
    fn default() -> Self {
        FusionStrategy::RRF { k: 60 }
    }
}

impl FusionStrategy {
    /// Create RRF with default k=60
    pub fn rrf() -> Self {
        FusionStrategy::RRF { k: 60 }
    }

    /// Create linear combination with given alpha
    pub fn linear(alpha: f32) -> Self {
        FusionStrategy::Linear {
            alpha: alpha.clamp(0.0, 1.0),
        }
    }

    /// Create weighted combination
    pub fn weighted(vector_weight: f32, keyword_weight: f32) -> Self {
        FusionStrategy::Weighted {
            vector_weight,
            keyword_weight,
        }
    }
}

/// Configuration for hybrid search
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HybridSearchConfig {
    /// Fusion strategy to use
    pub fusion: FusionStrategy,
    /// Maximum results from vector search before fusion
    pub vector_top_k: usize,
    /// Maximum results from keyword search before fusion
    pub keyword_top_k: usize,
    /// Final top-k after fusion
    pub final_top_k: usize,
    /// Minimum score threshold after fusion
    pub min_score: Option<f32>,
    /// Enable score normalization
    pub normalize_scores: bool,
    /// BM25 parameters
    pub bm25_k1: f32,
    /// BM25 parameters
    pub bm25_b: f32,
    /// Enable phrase boosting for keyword search
    pub phrase_boost: bool,
    /// Phrase boost factor
    pub phrase_boost_factor: f32,
    /// Fields to search (for keyword search)
    pub search_fields: Option<Vec<String>>,
}

impl Default for HybridSearchConfig {
    fn default() -> Self {
        Self {
            fusion: FusionStrategy::default(),
            vector_top_k: 50,
            keyword_top_k: 50,
            final_top_k: 10,
            min_score: None,
            normalize_scores: true,
            bm25_k1: 1.2,
            bm25_b: 0.75,
            phrase_boost: true,
            phrase_boost_factor: 2.0,
            search_fields: None,
        }
    }
}

impl HybridSearchConfig {
    /// Create config with RRF fusion
    pub fn with_rrf(k: u32) -> Self {
        Self {
            fusion: FusionStrategy::RRF { k },
            ..Default::default()
        }
    }

    /// Create config with linear fusion
    pub fn with_linear(alpha: f32) -> Self {
        Self {
            fusion: FusionStrategy::linear(alpha),
            ..Default::default()
        }
    }

    /// Set the final top-k
    pub fn top_k(mut self, k: usize) -> Self {
        self.final_top_k = k;
        self
    }

    /// Set minimum score
    pub fn min_score(mut self, score: f32) -> Self {
        self.min_score = Some(score);
        self
    }

    /// Set prefetch sizes
    pub fn prefetch(mut self, vector_k: usize, keyword_k: usize) -> Self {
        self.vector_top_k = vector_k;
        self.keyword_top_k = keyword_k;
        self
    }
}

/// A single result from hybrid search
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HybridSearchResult {
    /// Document/chunk identifier
    pub id: String,
    /// Final fused score
    pub score: f32,
    /// Score from vector search (if present)
    pub vector_score: Option<f32>,
    /// Rank from vector search
    pub vector_rank: Option<usize>,
    /// Score from keyword search (if present)
    pub keyword_score: Option<f32>,
    /// Rank from keyword search
    pub keyword_rank: Option<usize>,
    /// Content snippet
    pub content: String,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl HybridSearchResult {
    /// Check if result came from vector search
    pub fn has_vector_match(&self) -> bool {
        self.vector_score.is_some()
    }

    /// Check if result came from keyword search
    pub fn has_keyword_match(&self) -> bool {
        self.keyword_score.is_some()
    }

    /// Check if result matched both searches
    pub fn is_full_match(&self) -> bool {
        self.has_vector_match() && self.has_keyword_match()
    }
}

/// Results from hybrid search
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HybridSearchResults {
    /// Fused and ranked results
    pub results: Vec<HybridSearchResult>,
    /// Number of vector results before fusion
    pub vector_count: usize,
    /// Number of keyword results before fusion
    pub keyword_count: usize,
    /// Number of results that matched both
    pub overlap_count: usize,
    /// Fusion strategy used
    pub fusion_strategy: String,
    /// Search latency in milliseconds
    pub latency_ms: u64,
    /// Vector search latency
    pub vector_latency_ms: u64,
    /// Keyword search latency
    pub keyword_latency_ms: u64,
    /// Fusion latency
    pub fusion_latency_ms: u64,
}

impl HybridSearchResults {
    /// Get number of results
    pub fn len(&self) -> usize {
        self.results.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }

    /// Get top result
    pub fn top(&self) -> Option<&HybridSearchResult> {
        self.results.first()
    }

    /// Iterate over results
    pub fn iter(&self) -> impl Iterator<Item = &HybridSearchResult> {
        self.results.iter()
    }
}

/// Statistics for hybrid search
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HybridSearchStats {
    /// Total searches performed
    pub total_searches: u64,
    /// Average latency in ms
    pub avg_latency_ms: f64,
    /// Average vector search latency
    pub avg_vector_latency_ms: f64,
    /// Average keyword search latency
    pub avg_keyword_latency_ms: f64,
    /// Average overlap ratio
    pub avg_overlap_ratio: f64,
}

/// Internal state for tracking
struct SearchState {
    total_searches: AtomicU64,
    total_latency_ms: AtomicU64,
    total_vector_latency_ms: AtomicU64,
    total_keyword_latency_ms: AtomicU64,
    total_overlap: AtomicU64,
}

/// The Hybrid Search Engine
///
/// Combines vector and keyword search for improved retrieval quality.
pub struct HybridSearchEngine {
    config: HybridSearchConfig,
    // In a real implementation, these would be actual search indexes
    // For now, we store documents in-memory for demonstration
    documents: RwLock<HashMap<String, (String, Vec<f32>)>>,
    state: SearchState,
}

impl HybridSearchEngine {
    /// Create a new hybrid search engine
    pub fn new(config: HybridSearchConfig) -> Self {
        Self {
            config,
            documents: RwLock::new(HashMap::new()),
            state: SearchState {
                total_searches: AtomicU64::new(0),
                total_latency_ms: AtomicU64::new(0),
                total_vector_latency_ms: AtomicU64::new(0),
                total_keyword_latency_ms: AtomicU64::new(0),
                total_overlap: AtomicU64::new(0),
            },
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(HybridSearchConfig::default())
    }

    /// Index a document with content and embedding
    pub fn index(&self, id: &str, content: &str, embedding: Vec<f32>) {
        let mut docs = self.documents.write();
        docs.insert(id.to_string(), (content.to_string(), embedding));
    }

    /// Perform hybrid search
    pub fn search(&self, query: &str, query_embedding: &[f32]) -> HybridSearchResults {
        let start = Instant::now();

        // Vector search
        let vector_start = Instant::now();
        let vector_results = self.vector_search(query_embedding);
        let vector_latency = vector_start.elapsed().as_millis() as u64;

        // Keyword search
        let keyword_start = Instant::now();
        let keyword_results = self.keyword_search(query);
        let keyword_latency = keyword_start.elapsed().as_millis() as u64;

        // Fusion
        let fusion_start = Instant::now();
        let fused = self.fuse(vector_results.clone(), keyword_results.clone());
        let fusion_latency = fusion_start.elapsed().as_millis() as u64;

        let total_latency = start.elapsed().as_millis() as u64;

        // Calculate overlap
        let vector_ids: HashSet<_> = vector_results.iter().map(|(id, _, _)| id.clone()).collect();
        let keyword_ids: HashSet<_> = keyword_results
            .iter()
            .map(|(id, _, _)| id.clone())
            .collect();
        let overlap_count = vector_ids.intersection(&keyword_ids).count();

        // Update stats
        self.state.total_searches.fetch_add(1, Ordering::Relaxed);
        self.state
            .total_latency_ms
            .fetch_add(total_latency, Ordering::Relaxed);
        self.state
            .total_vector_latency_ms
            .fetch_add(vector_latency, Ordering::Relaxed);
        self.state
            .total_keyword_latency_ms
            .fetch_add(keyword_latency, Ordering::Relaxed);
        self.state
            .total_overlap
            .fetch_add(overlap_count as u64, Ordering::Relaxed);

        HybridSearchResults {
            results: fused,
            vector_count: vector_results.len(),
            keyword_count: keyword_results.len(),
            overlap_count,
            fusion_strategy: format!("{:?}", self.config.fusion),
            latency_ms: total_latency,
            vector_latency_ms: vector_latency,
            keyword_latency_ms: keyword_latency,
            fusion_latency_ms: fusion_latency,
        }
    }

    /// Vector similarity search
    fn vector_search(&self, query_embedding: &[f32]) -> Vec<(String, f32, String)> {
        let docs = self.documents.read();
        let mut results: Vec<_> = docs
            .iter()
            .map(|(id, (content, embedding))| {
                let similarity = cosine_similarity(query_embedding, embedding);
                (id.clone(), similarity, content.clone())
            })
            .collect();

        // Sort by similarity descending
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(self.config.vector_top_k);

        results
    }

    /// Keyword (BM25) search
    fn keyword_search(&self, query: &str) -> Vec<(String, f32, String)> {
        let query_terms: Vec<&str> = query.split_whitespace().collect();
        let docs = self.documents.read();

        // Simple BM25-style scoring
        let avg_doc_len: f32 = docs
            .values()
            .map(|(content, _)| content.split_whitespace().count() as f32)
            .sum::<f32>()
            / docs.len().max(1) as f32;

        let mut results: Vec<_> = docs
            .iter()
            .map(|(id, (content, _))| {
                let score = self.bm25_score(content, &query_terms, avg_doc_len, docs.len());
                (id.clone(), score, content.clone())
            })
            .filter(|(_, score, _)| *score > 0.0)
            .collect();

        // Sort by score descending
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(self.config.keyword_top_k);

        results
    }

    /// BM25 scoring
    fn bm25_score(
        &self,
        document: &str,
        query_terms: &[&str],
        avg_doc_len: f32,
        total_docs: usize,
    ) -> f32 {
        let doc_terms: Vec<&str> = document.split_whitespace().collect();
        let doc_len = doc_terms.len() as f32;

        let k1 = self.config.bm25_k1;
        let b = self.config.bm25_b;

        let mut score = 0.0;
        for term in query_terms {
            let term_lower = term.to_lowercase();
            let tf = doc_terms
                .iter()
                .filter(|t| t.to_lowercase() == term_lower)
                .count() as f32;

            if tf > 0.0 {
                // Simplified IDF (would use actual document frequencies in production)
                let idf = ((total_docs as f32 + 1.0) / (1.0 + 1.0)).ln() + 1.0;

                // BM25 formula
                let numerator = tf * (k1 + 1.0);
                let denominator = tf + k1 * (1.0 - b + b * (doc_len / avg_doc_len.max(1.0)));
                score += idf * numerator / denominator;
            }
        }

        // Phrase boost
        if self.config.phrase_boost {
            let query_phrase = query_terms.join(" ").to_lowercase();
            if document.to_lowercase().contains(&query_phrase) {
                score *= self.config.phrase_boost_factor;
            }
        }

        score
    }

    /// Fuse results from vector and keyword search
    fn fuse(
        &self,
        vector_results: Vec<(String, f32, String)>,
        keyword_results: Vec<(String, f32, String)>,
    ) -> Vec<HybridSearchResult> {
        match &self.config.fusion {
            FusionStrategy::RRF { k } => self.fuse_rrf(vector_results, keyword_results, *k),
            FusionStrategy::Linear { alpha } => {
                self.fuse_linear(vector_results, keyword_results, *alpha)
            }
            FusionStrategy::Weighted {
                vector_weight,
                keyword_weight,
            } => self.fuse_weighted(
                vector_results,
                keyword_results,
                *vector_weight,
                *keyword_weight,
            ),
            FusionStrategy::RelativeScoreFusion => {
                self.fuse_relative(vector_results, keyword_results)
            }
            FusionStrategy::DistributionScoreFusion => {
                self.fuse_distribution(vector_results, keyword_results)
            }
            FusionStrategy::Min => self.fuse_min(vector_results, keyword_results),
            FusionStrategy::Max => self.fuse_max(vector_results, keyword_results),
        }
    }

    /// Reciprocal Rank Fusion
    fn fuse_rrf(
        &self,
        vector_results: Vec<(String, f32, String)>,
        keyword_results: Vec<(String, f32, String)>,
        k: u32,
    ) -> Vec<HybridSearchResult> {
        let mut scores: HashMap<String, HybridSearchResult> = HashMap::new();

        // Process vector results
        for (rank, (id, score, content)) in vector_results.iter().enumerate() {
            let rrf_score = 1.0 / (k as f32 + rank as f32 + 1.0);
            let entry = scores
                .entry(id.clone())
                .or_insert_with(|| HybridSearchResult {
                    id: id.clone(),
                    score: 0.0,
                    vector_score: None,
                    vector_rank: None,
                    keyword_score: None,
                    keyword_rank: None,
                    content: content.clone(),
                    metadata: HashMap::new(),
                });
            entry.vector_score = Some(*score);
            entry.vector_rank = Some(rank + 1);
            entry.score += rrf_score;
        }

        // Process keyword results
        for (rank, (id, score, content)) in keyword_results.iter().enumerate() {
            let rrf_score = 1.0 / (k as f32 + rank as f32 + 1.0);
            let entry = scores
                .entry(id.clone())
                .or_insert_with(|| HybridSearchResult {
                    id: id.clone(),
                    score: 0.0,
                    vector_score: None,
                    vector_rank: None,
                    keyword_score: None,
                    keyword_rank: None,
                    content: content.clone(),
                    metadata: HashMap::new(),
                });
            entry.keyword_score = Some(*score);
            entry.keyword_rank = Some(rank + 1);
            entry.score += rrf_score;
        }

        self.finalize_results(scores)
    }

    /// Linear combination fusion
    fn fuse_linear(
        &self,
        vector_results: Vec<(String, f32, String)>,
        keyword_results: Vec<(String, f32, String)>,
        alpha: f32,
    ) -> Vec<HybridSearchResult> {
        // Normalize scores
        let (vector_normalized, _vector_max) = normalize_scores(&vector_results);
        let (keyword_normalized, _keyword_max) = normalize_scores(&keyword_results);

        let mut scores: HashMap<String, HybridSearchResult> = HashMap::new();

        // Process vector results
        for (rank, ((id, orig_score, content), norm_score)) in vector_results
            .iter()
            .zip(vector_normalized.iter())
            .enumerate()
        {
            let entry = scores
                .entry(id.clone())
                .or_insert_with(|| HybridSearchResult {
                    id: id.clone(),
                    score: 0.0,
                    vector_score: None,
                    vector_rank: None,
                    keyword_score: None,
                    keyword_rank: None,
                    content: content.clone(),
                    metadata: HashMap::new(),
                });
            entry.vector_score = Some(*orig_score);
            entry.vector_rank = Some(rank + 1);
            entry.score += alpha * norm_score;
        }

        // Process keyword results
        for (rank, ((id, orig_score, content), norm_score)) in keyword_results
            .iter()
            .zip(keyword_normalized.iter())
            .enumerate()
        {
            let entry = scores
                .entry(id.clone())
                .or_insert_with(|| HybridSearchResult {
                    id: id.clone(),
                    score: 0.0,
                    vector_score: None,
                    vector_rank: None,
                    keyword_score: None,
                    keyword_rank: None,
                    content: content.clone(),
                    metadata: HashMap::new(),
                });
            entry.keyword_score = Some(*orig_score);
            entry.keyword_rank = Some(rank + 1);
            entry.score += (1.0 - alpha) * norm_score;
        }

        self.finalize_results(scores)
    }

    /// Weighted combination fusion
    fn fuse_weighted(
        &self,
        vector_results: Vec<(String, f32, String)>,
        keyword_results: Vec<(String, f32, String)>,
        vector_weight: f32,
        keyword_weight: f32,
    ) -> Vec<HybridSearchResult> {
        let total = vector_weight + keyword_weight;
        let norm_vector = if total > 0.0 {
            vector_weight / total
        } else {
            0.5
        };
        let norm_keyword = if total > 0.0 {
            keyword_weight / total
        } else {
            0.5
        };

        // Normalize scores
        let (vector_normalized, _) = normalize_scores(&vector_results);
        let (keyword_normalized, _) = normalize_scores(&keyword_results);

        let mut scores: HashMap<String, HybridSearchResult> = HashMap::new();

        for (rank, ((id, orig_score, content), norm_score)) in vector_results
            .iter()
            .zip(vector_normalized.iter())
            .enumerate()
        {
            let entry = scores
                .entry(id.clone())
                .or_insert_with(|| HybridSearchResult {
                    id: id.clone(),
                    score: 0.0,
                    vector_score: None,
                    vector_rank: None,
                    keyword_score: None,
                    keyword_rank: None,
                    content: content.clone(),
                    metadata: HashMap::new(),
                });
            entry.vector_score = Some(*orig_score);
            entry.vector_rank = Some(rank + 1);
            entry.score += norm_vector * norm_score;
        }

        for (rank, ((id, orig_score, content), norm_score)) in keyword_results
            .iter()
            .zip(keyword_normalized.iter())
            .enumerate()
        {
            let entry = scores
                .entry(id.clone())
                .or_insert_with(|| HybridSearchResult {
                    id: id.clone(),
                    score: 0.0,
                    vector_score: None,
                    vector_rank: None,
                    keyword_score: None,
                    keyword_rank: None,
                    content: content.clone(),
                    metadata: HashMap::new(),
                });
            entry.keyword_score = Some(*orig_score);
            entry.keyword_rank = Some(rank + 1);
            entry.score += norm_keyword * norm_score;
        }

        self.finalize_results(scores)
    }

    /// Relative Score Fusion
    fn fuse_relative(
        &self,
        vector_results: Vec<(String, f32, String)>,
        keyword_results: Vec<(String, f32, String)>,
    ) -> Vec<HybridSearchResult> {
        // Use min-max normalization relative to each result set
        self.fuse_linear(vector_results, keyword_results, 0.5)
    }

    /// Distribution-based Score Fusion
    fn fuse_distribution(
        &self,
        vector_results: Vec<(String, f32, String)>,
        keyword_results: Vec<(String, f32, String)>,
    ) -> Vec<HybridSearchResult> {
        // Normalize using z-scores
        let vector_normalized = z_score_normalize(&vector_results);
        let keyword_normalized = z_score_normalize(&keyword_results);

        let mut scores: HashMap<String, HybridSearchResult> = HashMap::new();

        for (rank, ((id, orig_score, content), norm_score)) in vector_results
            .iter()
            .zip(vector_normalized.iter())
            .enumerate()
        {
            let entry = scores
                .entry(id.clone())
                .or_insert_with(|| HybridSearchResult {
                    id: id.clone(),
                    score: 0.0,
                    vector_score: None,
                    vector_rank: None,
                    keyword_score: None,
                    keyword_rank: None,
                    content: content.clone(),
                    metadata: HashMap::new(),
                });
            entry.vector_score = Some(*orig_score);
            entry.vector_rank = Some(rank + 1);
            entry.score += norm_score;
        }

        for (rank, ((id, orig_score, content), norm_score)) in keyword_results
            .iter()
            .zip(keyword_normalized.iter())
            .enumerate()
        {
            let entry = scores
                .entry(id.clone())
                .or_insert_with(|| HybridSearchResult {
                    id: id.clone(),
                    score: 0.0,
                    vector_score: None,
                    vector_rank: None,
                    keyword_score: None,
                    keyword_rank: None,
                    content: content.clone(),
                    metadata: HashMap::new(),
                });
            entry.keyword_score = Some(*orig_score);
            entry.keyword_rank = Some(rank + 1);
            entry.score += norm_score;
        }

        self.finalize_results(scores)
    }

    /// Min fusion (intersection-like)
    fn fuse_min(
        &self,
        vector_results: Vec<(String, f32, String)>,
        keyword_results: Vec<(String, f32, String)>,
    ) -> Vec<HybridSearchResult> {
        let vector_map: HashMap<_, _> = vector_results
            .iter()
            .enumerate()
            .map(|(rank, (id, score, content))| (id.clone(), (rank + 1, *score, content.clone())))
            .collect();

        let keyword_map: HashMap<_, _> = keyword_results
            .iter()
            .enumerate()
            .map(|(rank, (id, score, _))| (id.clone(), (rank + 1, *score)))
            .collect();

        // Only include documents in both sets
        let mut results: Vec<HybridSearchResult> = vector_map
            .iter()
            .filter_map(|(id, (v_rank, v_score, content))| {
                keyword_map
                    .get(id)
                    .map(|(k_rank, k_score)| HybridSearchResult {
                        id: id.clone(),
                        score: v_score.min(*k_score),
                        vector_score: Some(*v_score),
                        vector_rank: Some(*v_rank),
                        keyword_score: Some(*k_score),
                        keyword_rank: Some(*k_rank),
                        content: content.clone(),
                        metadata: HashMap::new(),
                    })
            })
            .collect();

        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(self.config.final_top_k);

        if let Some(min_score) = self.config.min_score {
            results.retain(|r| r.score >= min_score);
        }

        results
    }

    /// Max fusion (union-like)
    fn fuse_max(
        &self,
        vector_results: Vec<(String, f32, String)>,
        keyword_results: Vec<(String, f32, String)>,
    ) -> Vec<HybridSearchResult> {
        let mut scores: HashMap<String, HybridSearchResult> = HashMap::new();

        for (rank, (id, score, content)) in vector_results.iter().enumerate() {
            let entry = scores
                .entry(id.clone())
                .or_insert_with(|| HybridSearchResult {
                    id: id.clone(),
                    score: 0.0,
                    vector_score: None,
                    vector_rank: None,
                    keyword_score: None,
                    keyword_rank: None,
                    content: content.clone(),
                    metadata: HashMap::new(),
                });
            entry.vector_score = Some(*score);
            entry.vector_rank = Some(rank + 1);
            entry.score = entry.score.max(*score);
        }

        for (rank, (id, score, content)) in keyword_results.iter().enumerate() {
            let entry = scores
                .entry(id.clone())
                .or_insert_with(|| HybridSearchResult {
                    id: id.clone(),
                    score: 0.0,
                    vector_score: None,
                    vector_rank: None,
                    keyword_score: None,
                    keyword_rank: None,
                    content: content.clone(),
                    metadata: HashMap::new(),
                });
            entry.keyword_score = Some(*score);
            entry.keyword_rank = Some(rank + 1);
            entry.score = entry.score.max(*score);
        }

        self.finalize_results(scores)
    }

    /// Finalize and sort results
    fn finalize_results(
        &self,
        scores: HashMap<String, HybridSearchResult>,
    ) -> Vec<HybridSearchResult> {
        let mut results: Vec<_> = scores.into_values().collect();
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(self.config.final_top_k);

        if let Some(min_score) = self.config.min_score {
            results.retain(|r| r.score >= min_score);
        }

        results
    }

    /// Get search statistics
    pub fn stats(&self) -> HybridSearchStats {
        let total = self.state.total_searches.load(Ordering::Relaxed);
        let latency = self.state.total_latency_ms.load(Ordering::Relaxed);
        let vector_latency = self.state.total_vector_latency_ms.load(Ordering::Relaxed);
        let keyword_latency = self.state.total_keyword_latency_ms.load(Ordering::Relaxed);
        let overlap = self.state.total_overlap.load(Ordering::Relaxed);

        HybridSearchStats {
            total_searches: total,
            avg_latency_ms: if total > 0 {
                latency as f64 / total as f64
            } else {
                0.0
            },
            avg_vector_latency_ms: if total > 0 {
                vector_latency as f64 / total as f64
            } else {
                0.0
            },
            avg_keyword_latency_ms: if total > 0 {
                keyword_latency as f64 / total as f64
            } else {
                0.0
            },
            avg_overlap_ratio: if total > 0 {
                overlap as f64 / total as f64
            } else {
                0.0
            },
        }
    }

    /// Get current configuration
    pub fn config(&self) -> &HybridSearchConfig {
        &self.config
    }

    /// Clear all indexed documents
    pub fn clear(&self) {
        self.documents.write().clear();
    }

    /// Get document count
    pub fn len(&self) -> usize {
        self.documents.read().len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.documents.read().is_empty()
    }
}

/// Cosine similarity between two vectors
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return 0.0;
    }

    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm_a > 0.0 && norm_b > 0.0 {
        dot / (norm_a * norm_b)
    } else {
        0.0
    }
}

/// Normalize scores to [0, 1] range using min-max
fn normalize_scores(results: &[(String, f32, String)]) -> (Vec<f32>, f32) {
    if results.is_empty() {
        return (vec![], 0.0);
    }

    let min_score = results
        .iter()
        .map(|(_, s, _)| *s)
        .fold(f32::INFINITY, f32::min);
    let max_score = results
        .iter()
        .map(|(_, s, _)| *s)
        .fold(f32::NEG_INFINITY, f32::max);
    let range = max_score - min_score;

    if range > 0.0 {
        let normalized: Vec<f32> = results
            .iter()
            .map(|(_, s, _)| (s - min_score) / range)
            .collect();
        (normalized, max_score)
    } else {
        (vec![1.0; results.len()], max_score)
    }
}

/// Z-score normalization
fn z_score_normalize(results: &[(String, f32, String)]) -> Vec<f32> {
    if results.is_empty() {
        return vec![];
    }

    let scores: Vec<f32> = results.iter().map(|(_, s, _)| *s).collect();
    let n = scores.len() as f32;
    let mean: f32 = scores.iter().sum::<f32>() / n;
    let variance: f32 = scores.iter().map(|s| (s - mean).powi(2)).sum::<f32>() / n;
    let std_dev = variance.sqrt();

    if std_dev > 0.0 {
        scores.iter().map(|s| (s - mean) / std_dev).collect()
    } else {
        vec![0.0; results.len()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_embedding(seed: u8) -> Vec<f32> {
        (0..384)
            .map(|i| ((i as f32 + seed as f32) * 0.01).sin())
            .collect()
    }

    #[test]
    fn test_cosine_similarity() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        assert!((cosine_similarity(&a, &b) - 1.0).abs() < 0.001);

        let c = vec![0.0, 1.0, 0.0];
        assert!(cosine_similarity(&a, &c).abs() < 0.001);
    }

    #[test]
    fn test_hybrid_search_engine_creation() {
        let engine = HybridSearchEngine::with_defaults();
        assert!(engine.is_empty());
    }

    #[test]
    fn test_index_and_search() {
        let engine = HybridSearchEngine::with_defaults();

        engine.index(
            "doc1",
            "Ferrite is a high-performance Redis replacement",
            sample_embedding(1),
        );
        engine.index(
            "doc2",
            "Redis is an in-memory data store",
            sample_embedding(2),
        );
        engine.index(
            "doc3",
            "Databases store and retrieve data efficiently",
            sample_embedding(3),
        );

        assert_eq!(engine.len(), 3);

        let results = engine.search("Redis performance", &sample_embedding(1));
        assert!(!results.is_empty());
    }

    #[test]
    fn test_rrf_fusion() {
        let engine = HybridSearchEngine::new(HybridSearchConfig::with_rrf(60));

        engine.index("doc1", "Ferrite Redis alternative", sample_embedding(1));
        engine.index("doc2", "Redis caching system", sample_embedding(2));

        let results = engine.search("Redis", &sample_embedding(1));
        assert!(results.fusion_strategy.contains("RRF"));
    }

    #[test]
    fn test_linear_fusion() {
        let engine = HybridSearchEngine::new(HybridSearchConfig::with_linear(0.7));

        engine.index("doc1", "test document one", sample_embedding(1));
        engine.index("doc2", "test document two", sample_embedding(2));

        let results = engine.search("test", &sample_embedding(1));
        assert!(results.fusion_strategy.contains("Linear"));
    }

    #[test]
    fn test_stats() {
        let engine = HybridSearchEngine::with_defaults();

        engine.index("doc1", "test", sample_embedding(1));
        engine.search("test", &sample_embedding(1));
        engine.search("test", &sample_embedding(1));

        let stats = engine.stats();
        assert_eq!(stats.total_searches, 2);
    }

    #[test]
    fn test_normalize_scores() {
        let results = vec![
            ("a".to_string(), 0.5, "content".to_string()),
            ("b".to_string(), 1.0, "content".to_string()),
            ("c".to_string(), 0.0, "content".to_string()),
        ];

        let (normalized, max) = normalize_scores(&results);

        assert!((max - 1.0).abs() < 0.001);
        assert!((normalized[0] - 0.5).abs() < 0.001);
        assert!((normalized[1] - 1.0).abs() < 0.001);
        assert!(normalized[2].abs() < 0.001);
    }

    #[test]
    fn test_config_builders() {
        let rrf = HybridSearchConfig::with_rrf(40);
        assert!(matches!(rrf.fusion, FusionStrategy::RRF { k: 40 }));

        let linear = HybridSearchConfig::with_linear(0.6);
        assert!(
            matches!(linear.fusion, FusionStrategy::Linear { alpha } if (alpha - 0.6).abs() < 0.001)
        );

        let custom = HybridSearchConfig::default()
            .top_k(20)
            .min_score(0.5)
            .prefetch(100, 100);

        assert_eq!(custom.final_top_k, 20);
        assert_eq!(custom.min_score, Some(0.5));
        assert_eq!(custom.vector_top_k, 100);
        assert_eq!(custom.keyword_top_k, 100);
    }

    #[test]
    fn test_result_properties() {
        let result = HybridSearchResult {
            id: "doc1".to_string(),
            score: 0.8,
            vector_score: Some(0.9),
            vector_rank: Some(1),
            keyword_score: Some(0.7),
            keyword_rank: Some(2),
            content: "test".to_string(),
            metadata: HashMap::new(),
        };

        assert!(result.has_vector_match());
        assert!(result.has_keyword_match());
        assert!(result.is_full_match());
    }
}
