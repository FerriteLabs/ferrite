#![forbid(unsafe_code)]
//! Result fusion algorithms for hybrid retrieval.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A scored result from a single retrieval source.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScoredResult {
    /// Document identifier
    pub doc_id: String,
    /// Score from the retrieval source
    pub score: f64,
}

/// A fused result combining multiple retrieval sources.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FusedResult {
    /// Document identifier
    pub doc_id: String,
    /// Combined score after fusion
    pub fused_score: f64,
    /// Rank in the dense results (None if not present)
    pub dense_rank: Option<usize>,
    /// Rank in the sparse results (None if not present)
    pub sparse_rank: Option<usize>,
}

/// Fusion strategy for combining retrieval results.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FusionStrategy {
    /// Reciprocal Rank Fusion
    RRF,
    /// Linear combination with alpha weight
    Linear,
    /// Use only dense (vector) results
    DenseOnly,
    /// Use only sparse (text) results
    SparseOnly,
}

/// Reciprocal Rank Fusion (RRF) combiner.
pub struct ReciprocalRankFusion;

impl ReciprocalRankFusion {
    /// Fuse dense and sparse results using RRF.
    ///
    /// RRF score = Σ 1 / (k + rank)
    ///
    /// - `k`: smoothing constant (default: 60)
    /// - `top_k`: number of results to return
    pub fn fuse(
        dense_results: &[ScoredResult],
        sparse_results: &[ScoredResult],
        k: u32,
        top_k: usize,
    ) -> Vec<FusedResult> {
        let mut scores: HashMap<String, (f64, Option<usize>, Option<usize>)> = HashMap::new();
        let k_f64 = k as f64;

        for (rank, result) in dense_results.iter().enumerate() {
            let rrf_score = 1.0 / (k_f64 + (rank + 1) as f64);
            let entry = scores.entry(result.doc_id.clone()).or_insert((0.0, None, None));
            entry.0 += rrf_score;
            entry.1 = Some(rank + 1);
        }

        for (rank, result) in sparse_results.iter().enumerate() {
            let rrf_score = 1.0 / (k_f64 + (rank + 1) as f64);
            let entry = scores.entry(result.doc_id.clone()).or_insert((0.0, None, None));
            entry.0 += rrf_score;
            entry.2 = Some(rank + 1);
        }

        let mut results: Vec<FusedResult> = scores
            .into_iter()
            .map(|(doc_id, (fused_score, dense_rank, sparse_rank))| FusedResult {
                doc_id,
                fused_score,
                dense_rank,
                sparse_rank,
            })
            .collect();

        results.sort_by(|a, b| {
            b.fused_score
                .partial_cmp(&a.fused_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(top_k);
        results
    }
}

/// Linear combination fusion: alpha * dense + (1 - alpha) * sparse.
pub struct LinearCombination;

impl LinearCombination {
    /// Fuse results using linear combination of normalized scores.
    ///
    /// - `alpha`: weight for dense scores (0.0–1.0)
    pub fn fuse(
        dense_results: &[ScoredResult],
        sparse_results: &[ScoredResult],
        alpha: f64,
        top_k: usize,
    ) -> Vec<FusedResult> {
        // Normalize scores to [0, 1] via min-max
        let normalize = |results: &[ScoredResult]| -> HashMap<String, f64> {
            if results.is_empty() {
                return HashMap::new();
            }
            let max = results
                .iter()
                .map(|r| r.score)
                .fold(f64::NEG_INFINITY, f64::max);
            let min = results
                .iter()
                .map(|r| r.score)
                .fold(f64::INFINITY, f64::min);
            let range = max - min;
            results
                .iter()
                .map(|r| {
                    let norm = if range > 0.0 {
                        (r.score - min) / range
                    } else {
                        1.0
                    };
                    (r.doc_id.clone(), norm)
                })
                .collect()
        };

        let dense_map = normalize(dense_results);
        let sparse_map = normalize(sparse_results);

        // Build rank maps
        let dense_ranks: HashMap<&str, usize> = dense_results
            .iter()
            .enumerate()
            .map(|(i, r)| (r.doc_id.as_str(), i + 1))
            .collect();
        let sparse_ranks: HashMap<&str, usize> = sparse_results
            .iter()
            .enumerate()
            .map(|(i, r)| (r.doc_id.as_str(), i + 1))
            .collect();

        let mut all_ids: Vec<String> = dense_map.keys().chain(sparse_map.keys()).cloned().collect();
        all_ids.sort();
        all_ids.dedup();

        let mut results: Vec<FusedResult> = all_ids
            .into_iter()
            .map(|doc_id| {
                let dense_score = dense_map.get(&doc_id).copied().unwrap_or(0.0);
                let sparse_score = sparse_map.get(&doc_id).copied().unwrap_or(0.0);
                let fused_score = alpha * dense_score + (1.0 - alpha) * sparse_score;
                FusedResult {
                    dense_rank: dense_ranks.get(doc_id.as_str()).copied(),
                    sparse_rank: sparse_ranks.get(doc_id.as_str()).copied(),
                    doc_id,
                    fused_score,
                }
            })
            .collect();

        results.sort_by(|a, b| {
            b.fused_score
                .partial_cmp(&a.fused_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(top_k);
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_results(ids: &[(&str, f64)]) -> Vec<ScoredResult> {
        ids.iter()
            .map(|(id, score)| ScoredResult {
                doc_id: id.to_string(),
                score: *score,
            })
            .collect()
    }

    #[test]
    fn test_rrf_basic() {
        let dense = make_results(&[("a", 0.9), ("b", 0.8), ("c", 0.7)]);
        let sparse = make_results(&[("b", 5.0), ("c", 4.0), ("d", 3.0)]);

        let fused = ReciprocalRankFusion::fuse(&dense, &sparse, 60, 10);
        assert!(!fused.is_empty());
        // "b" appears in both lists, should have highest fused score
        assert_eq!(fused[0].doc_id, "b");
    }

    #[test]
    fn test_rrf_empty_inputs() {
        let result = ReciprocalRankFusion::fuse(&[], &[], 60, 10);
        assert!(result.is_empty());
    }

    #[test]
    fn test_linear_basic() {
        let dense = make_results(&[("a", 0.9), ("b", 0.5)]);
        let sparse = make_results(&[("b", 5.0), ("c", 3.0)]);

        let fused = LinearCombination::fuse(&dense, &sparse, 0.5, 10);
        assert!(!fused.is_empty());
        // "b" has scores from both sources
        assert!(fused.iter().any(|r| r.doc_id == "b"));
    }

    #[test]
    fn test_linear_alpha_one() {
        let dense = make_results(&[("a", 0.9)]);
        let sparse = make_results(&[("b", 5.0)]);

        let fused = LinearCombination::fuse(&dense, &sparse, 1.0, 10);
        // With alpha=1.0, dense results dominate
        assert_eq!(fused[0].doc_id, "a");
    }
}
