//! Advanced search operations
//!
//! - **Batch search**: Multiple query vectors in one call
//! - **Range search**: Find all vectors within a distance threshold
//! - **Multi-vector queries**: Combine multiple query vectors

use super::types::{SearchResult, VectorId};
use super::{VectorError, VectorIndex};
use std::collections::HashMap;
use tracing::debug;

/// Result of a batch search — one result set per query
#[derive(Debug)]
pub struct BatchSearchResult {
    /// Results for each query, in the same order as the input queries
    pub results: Vec<Vec<SearchResult>>,
}

/// Execute a batch search: multiple queries against the same index
pub fn batch_search(
    index: &dyn VectorIndex,
    queries: &[&[f32]],
    k: usize,
) -> Result<BatchSearchResult, VectorError> {
    let dim = index.dimension();
    let mut results = Vec::with_capacity(queries.len());

    for query in queries {
        if query.len() != dim {
            return Err(VectorError::DimensionMismatch {
                expected: dim,
                got: query.len(),
            });
        }
        results.push(index.search(query, k)?);
    }

    debug!("batch search: {} queries, k={}", queries.len(), k);
    Ok(BatchSearchResult { results })
}

/// Range search result
#[derive(Debug)]
pub struct RangeSearchResult {
    /// All vectors within the distance threshold, sorted by distance
    pub results: Vec<SearchResult>,
    /// The threshold used
    pub threshold: f32,
}

/// Execute a range search: find all vectors within a distance threshold.
///
/// This requires iterating all vectors, so it's most efficient with a flat index
/// or when used as a post-processing step.
pub fn range_search(
    index: &dyn VectorIndex,
    query: &[f32],
    threshold: f32,
    max_results: usize,
) -> Result<RangeSearchResult, VectorError> {
    if query.len() != index.dimension() {
        return Err(VectorError::DimensionMismatch {
            expected: index.dimension(),
            got: query.len(),
        });
    }

    // Over-fetch to find candidates, then filter by threshold
    let candidates = index.search(query, max_results)?;
    let results: Vec<SearchResult> = candidates
        .into_iter()
        .filter(|r| r.score <= threshold)
        .collect();

    debug!(
        "range search: threshold={}, found {} results",
        threshold,
        results.len()
    );
    Ok(RangeSearchResult { results, threshold })
}

/// Multi-vector query combination strategy
#[derive(Clone, Debug)]
pub enum MultiVectorStrategy {
    /// Average the query vectors, then search once
    Average,
    /// Search with each query vector, merge and deduplicate results
    MergeResults,
    /// Search with each query, keep only results that appear in all (intersection)
    Intersection,
    /// Weighted average of query vectors
    WeightedAverage(Vec<f32>),
}

/// Execute a multi-vector query: combine multiple query vectors
pub fn multi_vector_search(
    index: &dyn VectorIndex,
    queries: &[&[f32]],
    k: usize,
    strategy: &MultiVectorStrategy,
) -> Result<Vec<SearchResult>, VectorError> {
    let dim = index.dimension();
    for q in queries {
        if q.len() != dim {
            return Err(VectorError::DimensionMismatch {
                expected: dim,
                got: q.len(),
            });
        }
    }

    if queries.is_empty() {
        return Ok(Vec::new());
    }

    match strategy {
        MultiVectorStrategy::Average => {
            let avg = average_vectors(queries, dim);
            index.search(&avg, k)
        }
        MultiVectorStrategy::WeightedAverage(weights) => {
            if weights.len() != queries.len() {
                return Err(VectorError::InvalidConfig(format!(
                    "weights length {} != queries length {}",
                    weights.len(),
                    queries.len()
                )));
            }
            let avg = weighted_average_vectors(queries, weights, dim);
            index.search(&avg, k)
        }
        MultiVectorStrategy::MergeResults => {
            let mut all_results: HashMap<String, (VectorId, f32)> = HashMap::new();

            for query in queries {
                let results = index.search(query, k)?;
                for r in results {
                    let key = r.id.as_str().to_string();
                    all_results
                        .entry(key)
                        .and_modify(|(_, score)| {
                            if r.score < *score {
                                *score = r.score;
                            }
                        })
                        .or_insert((r.id, r.score));
                }
            }

            let mut results: Vec<SearchResult> = all_results
                .into_values()
                .map(|(id, score)| SearchResult::new(id, score))
                .collect();
            results.sort_by(|a, b| {
                a.score
                    .partial_cmp(&b.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            results.truncate(k);
            Ok(results)
        }
        MultiVectorStrategy::Intersection => {
            // Search each query and intersect results
            let mut result_sets: Vec<HashMap<String, f32>> = Vec::new();

            for query in queries {
                // Over-fetch to increase intersection chance
                let results = index.search(query, k * 5)?;
                let set: HashMap<String, f32> = results
                    .into_iter()
                    .map(|r| (r.id.as_str().to_string(), r.score))
                    .collect();
                result_sets.push(set);
            }

            if result_sets.is_empty() {
                return Ok(Vec::new());
            }

            // Find IDs present in ALL result sets
            let first = &result_sets[0];
            let mut intersection: Vec<(String, f32)> = first
                .iter()
                .filter(|(key, _)| result_sets[1..].iter().all(|set| set.contains_key(*key)))
                .map(|(key, _score)| {
                    // Use average score across all queries
                    let avg_score: f32 = result_sets
                        .iter()
                        .map(|set| set.get(key).copied().unwrap_or(f32::MAX))
                        .sum::<f32>()
                        / result_sets.len() as f32;
                    (key.clone(), avg_score)
                })
                .collect();

            intersection.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
            intersection.truncate(k);

            Ok(intersection
                .into_iter()
                .map(|(id, score)| SearchResult::new(VectorId::new(id), score))
                .collect())
        }
    }
}

/// Compute the element-wise average of multiple vectors
fn average_vectors(vectors: &[&[f32]], dim: usize) -> Vec<f32> {
    let mut result = vec![0.0f32; dim];
    for vec in vectors {
        for (i, &v) in vec.iter().enumerate() {
            result[i] += v;
        }
    }
    let n = vectors.len() as f32;
    for v in &mut result {
        *v /= n;
    }
    result
}

/// Compute a weighted average of multiple vectors
fn weighted_average_vectors(vectors: &[&[f32]], weights: &[f32], dim: usize) -> Vec<f32> {
    let mut result = vec![0.0f32; dim];
    let total_weight: f32 = weights.iter().sum();

    for (vec, &w) in vectors.iter().zip(weights.iter()) {
        for (i, &v) in vec.iter().enumerate() {
            result[i] += v * w;
        }
    }

    if total_weight > 0.0 {
        for v in &mut result {
            *v /= total_weight;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::flat::FlatIndex;
    use crate::vector::types::DistanceMetric;

    fn make_test_index() -> FlatIndex {
        let index = FlatIndex::new(3, DistanceMetric::Euclidean);
        index.add(VectorId::new("v1"), &[1.0, 0.0, 0.0]).unwrap();
        index.add(VectorId::new("v2"), &[0.0, 1.0, 0.0]).unwrap();
        index.add(VectorId::new("v3"), &[0.0, 0.0, 1.0]).unwrap();
        index.add(VectorId::new("v4"), &[0.7, 0.7, 0.0]).unwrap();
        index.add(VectorId::new("v5"), &[0.5, 0.5, 0.5]).unwrap();
        index
    }

    #[test]
    fn test_batch_search() {
        let index = make_test_index();
        let q1 = [1.0f32, 0.0, 0.0];
        let q2 = [0.0f32, 1.0, 0.0];
        let queries: Vec<&[f32]> = vec![&q1, &q2];

        let result = batch_search(&index, &queries, 2).unwrap();
        assert_eq!(result.results.len(), 2);
        assert_eq!(result.results[0][0].id.as_str(), "v1"); // Nearest to [1,0,0]
        assert_eq!(result.results[1][0].id.as_str(), "v2"); // Nearest to [0,1,0]
    }

    #[test]
    fn test_batch_search_empty() {
        let index = make_test_index();
        let queries: Vec<&[f32]> = vec![];
        let result = batch_search(&index, &queries, 2).unwrap();
        assert!(result.results.is_empty());
    }

    #[test]
    fn test_batch_search_dimension_mismatch() {
        let index = make_test_index();
        let q1 = [1.0f32, 0.0]; // Wrong dimension
        let queries: Vec<&[f32]> = vec![&q1];
        let result = batch_search(&index, &queries, 2);
        assert!(matches!(result, Err(VectorError::DimensionMismatch { .. })));
    }

    #[test]
    fn test_range_search() {
        let index = make_test_index();
        let result = range_search(&index, &[1.0, 0.0, 0.0], 0.5, 10).unwrap();
        assert_eq!(result.threshold, 0.5);
        // v1 is at distance 0, should be included
        assert!(result.results.iter().any(|r| r.id.as_str() == "v1"));
        // v3 is at distance sqrt(2) ≈ 1.414, should NOT be included
        assert!(!result.results.iter().any(|r| r.id.as_str() == "v3"));
    }

    #[test]
    fn test_range_search_large_threshold() {
        let index = make_test_index();
        let result = range_search(&index, &[0.0, 0.0, 0.0], 100.0, 100).unwrap();
        assert_eq!(result.results.len(), index.len());
    }

    #[test]
    fn test_range_search_zero_threshold() {
        let index = make_test_index();
        let result = range_search(&index, &[1.0, 0.0, 0.0], 0.0, 100).unwrap();
        // Only exact match
        assert_eq!(result.results.len(), 1);
        assert_eq!(result.results[0].id.as_str(), "v1");
    }

    #[test]
    fn test_multi_vector_average() {
        let index = make_test_index();
        let q1 = [1.0f32, 0.0, 0.0];
        let q2 = [0.0f32, 1.0, 0.0];
        let queries: Vec<&[f32]> = vec![&q1, &q2];

        let results =
            multi_vector_search(&index, &queries, 1, &MultiVectorStrategy::Average).unwrap();
        assert_eq!(results.len(), 1);
        // Average is [0.5, 0.5, 0.0], closest should be v4 [0.7, 0.7, 0.0]
        assert_eq!(results[0].id.as_str(), "v4");
    }

    #[test]
    fn test_multi_vector_weighted_average() {
        let index = make_test_index();
        let q1 = [1.0f32, 0.0, 0.0];
        let q2 = [0.0f32, 0.0, 1.0];
        let queries: Vec<&[f32]> = vec![&q1, &q2];

        let results = multi_vector_search(
            &index,
            &queries,
            1,
            &MultiVectorStrategy::WeightedAverage(vec![0.9, 0.1]),
        )
        .unwrap();
        assert_eq!(results.len(), 1);
        // Heavily weighted toward [1,0,0], so v1 should win
        assert_eq!(results[0].id.as_str(), "v1");
    }

    #[test]
    fn test_multi_vector_merge() {
        let index = make_test_index();
        let q1 = [1.0f32, 0.0, 0.0];
        let q2 = [0.0f32, 1.0, 0.0];
        let queries: Vec<&[f32]> = vec![&q1, &q2];

        let results =
            multi_vector_search(&index, &queries, 3, &MultiVectorStrategy::MergeResults).unwrap();
        assert!(!results.is_empty());
        assert!(results.len() <= 3);
    }

    #[test]
    fn test_multi_vector_intersection() {
        let index = make_test_index();
        let q1 = [1.0f32, 0.0, 0.0];
        let q2 = [0.0f32, 1.0, 0.0];
        let queries: Vec<&[f32]> = vec![&q1, &q2];

        let results =
            multi_vector_search(&index, &queries, 5, &MultiVectorStrategy::Intersection).unwrap();
        // Intersection should contain vectors near both queries
        // v4 [0.7, 0.7, 0.0] and v5 [0.5, 0.5, 0.5] should be in both top-k lists
        assert!(!results.is_empty());
    }

    #[test]
    fn test_multi_vector_empty_queries() {
        let index = make_test_index();
        let queries: Vec<&[f32]> = vec![];
        let results =
            multi_vector_search(&index, &queries, 5, &MultiVectorStrategy::Average).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_multi_vector_wrong_weights() {
        let index = make_test_index();
        let q1 = [1.0f32, 0.0, 0.0];
        let queries: Vec<&[f32]> = vec![&q1];
        let result = multi_vector_search(
            &index,
            &queries,
            5,
            &MultiVectorStrategy::WeightedAverage(vec![0.5, 0.5]), // 2 weights for 1 query
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_average_vectors() {
        let q1 = [2.0f32, 0.0, 0.0];
        let q2 = [0.0f32, 4.0, 0.0];
        let avg = average_vectors(&[&q1, &q2], 3);
        assert!((avg[0] - 1.0).abs() < 1e-5);
        assert!((avg[1] - 2.0).abs() < 1e-5);
        assert!((avg[2] - 0.0).abs() < 1e-5);
    }

    #[test]
    fn test_weighted_average_vectors() {
        let q1 = [1.0f32, 0.0];
        let q2 = [0.0f32, 1.0];
        let avg = weighted_average_vectors(&[&q1, &q2], &[3.0, 1.0], 2);
        // weighted: (3*1+1*0)/4=0.75, (3*0+1*1)/4=0.25
        assert!((avg[0] - 0.75).abs() < 1e-5);
        assert!((avg[1] - 0.25).abs() < 1e-5);
    }
}
