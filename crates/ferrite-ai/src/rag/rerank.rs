//! Reranking Module for RAG Pipeline
//!
//! Provides reranking capabilities to improve retrieval quality by
//! re-scoring retrieved chunks using a cross-encoder model.
//!
//! # Reranking Strategies
//!
//! - **CrossEncoder**: Use a cross-encoder model to score query-document pairs
//! - **Cohere**: Use Cohere's rerank API
//! - **RecursiveAbstractiveSummarization**: Summarize and re-rank
//! - **None**: Pass through without reranking

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use super::retrieve::RetrievalResult;

/// Reranking strategy
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub enum RerankStrategy {
    /// No reranking - pass through results as-is
    #[default]
    None,
    /// Cross-encoder reranking (local or API-based)
    CrossEncoder {
        /// Model name or path
        model: String,
        /// Maximum sequence length
        max_length: usize,
    },
    /// Cohere Rerank API
    Cohere {
        /// Model name (e.g., "rerank-english-v2.0")
        model: String,
    },
    /// Reciprocal Rank Fusion for combining multiple retrieval results
    RRF {
        /// Constant k for RRF formula (default: 60)
        k: u32,
    },
    /// Linear combination of scores
    LinearCombination {
        /// Weight for semantic score (0.0 - 1.0)
        semantic_weight: f32,
        /// Weight for keyword score (0.0 - 1.0)
        keyword_weight: f32,
    },
    /// Maximum Marginal Relevance for diversity
    MMR {
        /// Lambda parameter (0.0 = diversity, 1.0 = relevance)
        lambda: f32,
    },
}



/// Configuration for reranking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RerankConfig {
    /// Reranking strategy to use
    pub strategy: RerankStrategy,
    /// Maximum number of results to return after reranking
    pub top_k: usize,
    /// Minimum score threshold after reranking
    pub min_score: Option<f32>,
    /// Whether to include original scores in output
    pub include_original_scores: bool,
    /// Batch size for reranking (for efficiency)
    pub batch_size: usize,
    /// Timeout in milliseconds for reranking operations
    pub timeout_ms: u64,
}

impl Default for RerankConfig {
    fn default() -> Self {
        Self {
            strategy: RerankStrategy::None,
            top_k: 10,
            min_score: None,
            include_original_scores: true,
            batch_size: 32,
            timeout_ms: 5000,
        }
    }
}

impl RerankConfig {
    /// Create a config with cross-encoder reranking
    pub fn cross_encoder(model: impl Into<String>) -> Self {
        Self {
            strategy: RerankStrategy::CrossEncoder {
                model: model.into(),
                max_length: 512,
            },
            ..Default::default()
        }
    }

    /// Create a config with Cohere reranking
    pub fn cohere(model: impl Into<String>) -> Self {
        Self {
            strategy: RerankStrategy::Cohere {
                model: model.into(),
            },
            ..Default::default()
        }
    }

    /// Create a config with RRF fusion
    pub fn rrf(k: u32) -> Self {
        Self {
            strategy: RerankStrategy::RRF { k },
            ..Default::default()
        }
    }

    /// Create a config with MMR for diversity
    pub fn mmr(lambda: f32) -> Self {
        Self {
            strategy: RerankStrategy::MMR { lambda },
            ..Default::default()
        }
    }

    /// Set the top-k results to return
    pub fn with_top_k(mut self, k: usize) -> Self {
        self.top_k = k;
        self
    }

    /// Set the minimum score threshold
    pub fn with_min_score(mut self, score: f32) -> Self {
        self.min_score = Some(score);
        self
    }
}

/// Result of reranking operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RerankResult {
    /// Reranked retrieval results
    pub results: Vec<RetrievalResult>,
    /// Reranking latency in milliseconds
    pub latency_ms: u64,
    /// Original result count before reranking
    pub original_count: usize,
    /// Strategy used for reranking
    pub strategy: String,
}

impl RerankResult {
    /// Check if reranking found results
    pub fn has_results(&self) -> bool {
        !self.results.is_empty()
    }

    /// Get the top result
    pub fn top(&self) -> Option<&RetrievalResult> {
        self.results.first()
    }
}

/// Reranker for improving retrieval quality
pub struct Reranker {
    config: RerankConfig,
    // Stats
    total_reranks: AtomicU64,
    total_latency_ms: AtomicU64,
}

impl Reranker {
    /// Create a new reranker with configuration
    pub fn new(config: RerankConfig) -> Self {
        Self {
            config,
            total_reranks: AtomicU64::new(0),
            total_latency_ms: AtomicU64::new(0),
        }
    }

    /// Create a reranker with default configuration (no reranking)
    pub fn passthrough() -> Self {
        Self::new(RerankConfig::default())
    }

    /// Rerank retrieval results
    pub async fn rerank(
        &self,
        query: &str,
        results: Vec<RetrievalResult>,
    ) -> Result<RerankResult, RerankError> {
        let start = Instant::now();
        let original_count = results.len();

        let reranked = match &self.config.strategy {
            RerankStrategy::None => results,
            RerankStrategy::CrossEncoder { model, max_length } => {
                self.rerank_cross_encoder(query, results, model, *max_length)
                    .await?
            }
            RerankStrategy::Cohere { model } => self.rerank_cohere(query, results, model).await?,
            RerankStrategy::RRF { k } => self.rerank_rrf(results, *k),
            RerankStrategy::LinearCombination {
                semantic_weight,
                keyword_weight,
            } => self.rerank_linear(results, *semantic_weight, *keyword_weight),
            RerankStrategy::MMR { lambda } => self.rerank_mmr(query, results, *lambda).await?,
        };

        // Apply top-k limit
        let mut final_results: Vec<RetrievalResult> =
            reranked.into_iter().take(self.config.top_k).collect();

        // Apply minimum score filter
        if let Some(min_score) = self.config.min_score {
            final_results.retain(|r| r.score >= min_score);
        }

        let latency_ms = start.elapsed().as_millis() as u64;

        // Update stats
        self.total_reranks.fetch_add(1, Ordering::Relaxed);
        self.total_latency_ms
            .fetch_add(latency_ms, Ordering::Relaxed);

        Ok(RerankResult {
            results: final_results,
            latency_ms,
            original_count,
            strategy: format!("{:?}", self.config.strategy),
        })
    }

    /// Cross-encoder reranking (simulated - would use actual model in production)
    async fn rerank_cross_encoder(
        &self,
        query: &str,
        mut results: Vec<RetrievalResult>,
        _model: &str,
        _max_length: usize,
    ) -> Result<Vec<RetrievalResult>, RerankError> {
        // In production, this would use a cross-encoder model to score query-document pairs
        // For now, we simulate with a query-relevance heuristic

        for result in &mut results {
            // Simulate cross-encoder scoring based on term overlap and position
            let query_terms: Vec<&str> = query.split_whitespace().collect();
            let doc_lower = result.chunk.content.to_lowercase();

            let mut score_boost: f32 = 0.0;
            for (i, term) in query_terms.iter().enumerate() {
                let term_lower = term.to_lowercase();
                if doc_lower.contains(&term_lower) {
                    // Earlier query terms are more important
                    score_boost += 0.1 / (i as f32 + 1.0);
                }
            }

            // Combine original score with cross-encoder boost
            result.score = result.score * 0.7 + score_boost * 0.3;
        }

        // Sort by new scores
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(results)
    }

    /// Cohere reranking (simulated - would use actual API in production)
    async fn rerank_cohere(
        &self,
        _query: &str,
        mut results: Vec<RetrievalResult>,
        _model: &str,
    ) -> Result<Vec<RetrievalResult>, RerankError> {
        // In production, this would call Cohere's rerank API
        // For now, results pass through with slight score normalization

        let max_score = results.iter().map(|r| r.score).fold(0.0f32, f32::max);
        if max_score > 0.0 {
            for result in &mut results {
                result.score /= max_score;
            }
        }

        Ok(results)
    }

    /// Reciprocal Rank Fusion reranking
    fn rerank_rrf(&self, mut results: Vec<RetrievalResult>, k: u32) -> Vec<RetrievalResult> {
        // RRF formula: score(d) = sum(1 / (k + rank(d)))
        // Here we apply it as a smoothing function on existing ranks

        for (rank, result) in results.iter_mut().enumerate() {
            let rrf_score = 1.0 / (k as f32 + rank as f32 + 1.0);
            // Combine original score with RRF
            result.score = (result.score + rrf_score) / 2.0;
        }

        // Re-sort by combined score
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        results
    }

    /// Linear combination reranking
    fn rerank_linear(
        &self,
        mut results: Vec<RetrievalResult>,
        semantic_weight: f32,
        keyword_weight: f32,
    ) -> Vec<RetrievalResult> {
        // Normalize weights
        let total = semantic_weight + keyword_weight;
        let norm_semantic = if total > 0.0 {
            semantic_weight / total
        } else {
            0.5
        };

        for result in &mut results {
            // Assume original score is semantic, apply weight
            result.score *= norm_semantic;
        }

        // Sort by weighted scores
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        results
    }

    /// Maximum Marginal Relevance reranking for diversity
    async fn rerank_mmr(
        &self,
        _query: &str,
        results: Vec<RetrievalResult>,
        lambda: f32,
    ) -> Result<Vec<RetrievalResult>, RerankError> {
        if results.is_empty() {
            return Ok(results);
        }

        let mut selected: Vec<RetrievalResult> = Vec::new();
        let mut remaining = results.clone();

        // Select first by relevance
        remaining.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        if let Some(first) = remaining.pop() {
            selected.push(first);
        }

        // Greedily select remaining by MMR
        while !remaining.is_empty() && selected.len() < self.config.top_k {
            let mut best_idx = 0;
            let mut best_mmr = f32::NEG_INFINITY;

            for (i, candidate) in remaining.iter().enumerate() {
                // Calculate maximum similarity to already selected documents
                let max_sim = selected
                    .iter()
                    .map(|s| self.text_similarity(&candidate.chunk.content, &s.chunk.content))
                    .fold(0.0f32, f32::max);

                // MMR = lambda * relevance - (1 - lambda) * max_similarity
                let mmr = lambda * candidate.score - (1.0 - lambda) * max_sim;

                if mmr > best_mmr {
                    best_mmr = mmr;
                    best_idx = i;
                }
            }

            let selected_item = remaining.remove(best_idx);
            selected.push(selected_item);
        }

        Ok(selected)
    }

    /// Simple text similarity (Jaccard on words)
    fn text_similarity(&self, text1: &str, text2: &str) -> f32 {
        let words1: std::collections::HashSet<&str> = text1.split_whitespace().collect();
        let words2: std::collections::HashSet<&str> = text2.split_whitespace().collect();

        if words1.is_empty() && words2.is_empty() {
            return 1.0;
        }

        let intersection = words1.intersection(&words2).count() as f32;
        let union = words1.union(&words2).count() as f32;

        if union > 0.0 {
            intersection / union
        } else {
            0.0
        }
    }

    /// Get reranking statistics
    pub fn stats(&self) -> RerankStats {
        let total = self.total_reranks.load(Ordering::Relaxed);
        let latency = self.total_latency_ms.load(Ordering::Relaxed);

        RerankStats {
            total_reranks: total,
            avg_latency_ms: if total > 0 {
                latency as f64 / total as f64
            } else {
                0.0
            },
            strategy: format!("{:?}", self.config.strategy),
        }
    }

    /// Get current configuration
    pub fn config(&self) -> &RerankConfig {
        &self.config
    }
}

/// Reranking statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RerankStats {
    /// Total reranking operations
    pub total_reranks: u64,
    /// Average latency in milliseconds
    pub avg_latency_ms: f64,
    /// Strategy in use
    pub strategy: String,
}

/// Reranking errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum RerankError {
    /// Model loading error
    #[error("failed to load rerank model: {0}")]
    ModelError(String),

    /// API error
    #[error("rerank API error: {0}")]
    ApiError(String),

    /// Timeout error
    #[error("reranking timed out after {0}ms")]
    Timeout(u64),

    /// Invalid configuration
    #[error("invalid rerank configuration: {0}")]
    ConfigError(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rag::chunk::{Chunk, ChunkId, ChunkMetadata};
    use crate::rag::document::DocumentId;
    use crate::rag::retrieve::MatchSource;
    fn make_result(content: &str, score: f32) -> RetrievalResult {
        let document_id = DocumentId::new();
        RetrievalResult {
            chunk: Chunk {
                id: ChunkId::new(&document_id, 0),
                document_id,
                content: content.to_string(),
                start_offset: 0,
                end_offset: content.len(),
                total_chunks: 1,
                metadata: ChunkMetadata::default(),
                index: 0,
                embedding: None,
            },
            score,
            rank: 1,
            source: MatchSource::Vector,
        }
    }

    #[tokio::test]
    async fn test_passthrough_reranking() {
        let reranker = Reranker::passthrough();

        let results = vec![
            make_result("First document", 0.9),
            make_result("Second document", 0.8),
        ];

        let reranked = reranker.rerank("query", results).await.unwrap();

        assert_eq!(reranked.results.len(), 2);
        assert_eq!(reranked.strategy, "None");
    }

    #[tokio::test]
    async fn test_rrf_reranking() {
        let reranker = Reranker::new(RerankConfig::rrf(60));

        let results = vec![
            make_result("First document", 0.9),
            make_result("Second document", 0.8),
            make_result("Third document", 0.7),
        ];

        let reranked = reranker.rerank("query", results).await.unwrap();

        assert_eq!(reranked.results.len(), 3);
    }

    #[tokio::test]
    async fn test_mmr_reranking() {
        let reranker = Reranker::new(RerankConfig::mmr(0.5).with_top_k(2));

        let results = vec![
            make_result("The quick brown fox", 0.9),
            make_result("The quick brown fox jumps", 0.85),
            make_result("A completely different document", 0.7),
        ];

        let reranked = reranker.rerank("fox", results).await.unwrap();

        // Should prefer diversity, so third doc might rank higher than second
        assert_eq!(reranked.results.len(), 2);
    }

    #[tokio::test]
    async fn test_min_score_filter() {
        let config = RerankConfig::default().with_min_score(0.5);
        let reranker = Reranker::new(config);

        let results = vec![
            make_result("High score", 0.9),
            make_result("Low score", 0.3),
        ];

        let reranked = reranker.rerank("query", results).await.unwrap();

        assert_eq!(reranked.results.len(), 1);
        assert!(reranked.results[0].score >= 0.5);
    }

    #[test]
    fn test_text_similarity() {
        let reranker = Reranker::passthrough();

        let sim1 = reranker.text_similarity("hello world", "hello world");
        assert!((sim1 - 1.0).abs() < 0.001);

        let sim2 = reranker.text_similarity("hello world", "goodbye moon");
        assert!(sim2 < 0.5);

        let sim3 = reranker.text_similarity("the quick fox", "quick brown fox");
        assert!(sim3 > 0.3);
    }

    #[test]
    fn test_config_builders() {
        let cross_encoder = RerankConfig::cross_encoder("model.onnx");
        assert!(matches!(
            cross_encoder.strategy,
            RerankStrategy::CrossEncoder { .. }
        ));

        let cohere = RerankConfig::cohere("rerank-english-v2.0");
        assert!(matches!(cohere.strategy, RerankStrategy::Cohere { .. }));

        let rrf = RerankConfig::rrf(60);
        assert!(matches!(rrf.strategy, RerankStrategy::RRF { k: 60 }));

        let mmr = RerankConfig::mmr(0.7);
        assert!(
            matches!(mmr.strategy, RerankStrategy::MMR { lambda } if (lambda - 0.7).abs() < 0.001)
        );
    }
}
