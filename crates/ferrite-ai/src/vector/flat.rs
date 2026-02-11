//! Flat (brute force) vector index
//!
//! Simple linear scan index for exact nearest neighbor search.
//! Best for small datasets (< 100K vectors) where exact results are required.

use super::distance::distance;
use super::types::{DistanceMetric, SearchResult, VectorId};
use super::{VectorError, VectorIndex};
use parking_lot::RwLock;
use std::collections::HashMap;

/// Flat index using brute force linear scan
///
/// This index provides exact results by comparing the query against
/// all vectors in the index. It's best for small datasets.
///
/// # Example
///
/// ```
/// use ferrite::vector::{FlatIndex, DistanceMetric, VectorId, VectorIndex};
///
/// let index = FlatIndex::new(3, DistanceMetric::Cosine);
/// index.add(VectorId::new("v1"), &[1.0, 0.0, 0.0])?;
/// index.add(VectorId::new("v2"), &[0.0, 1.0, 0.0])?;
///
/// let results = index.search(&[1.0, 0.0, 0.0], 1)?;
/// # Ok::<(), ferrite_ai::vector::VectorError>(())
/// assert_eq!(results[0].id.as_str(), "v1");
/// ```
pub struct FlatIndex {
    /// Vector dimension
    dim: usize,
    /// Distance metric
    metric: DistanceMetric,
    /// Vectors stored by ID
    vectors: RwLock<HashMap<VectorId, Vec<f32>>>,
}

impl FlatIndex {
    /// Create a new flat index
    pub fn new(dimension: usize, metric: DistanceMetric) -> Self {
        Self {
            dim: dimension,
            metric,
            vectors: RwLock::new(HashMap::new()),
        }
    }

    /// Get a vector by ID
    pub fn get(&self, id: &VectorId) -> Option<Vec<f32>> {
        self.vectors.read().get(id).cloned()
    }

    /// Check if a vector exists
    pub fn contains(&self, id: &VectorId) -> bool {
        self.vectors.read().contains_key(id)
    }

    /// Get all vector IDs
    pub fn ids(&self) -> Vec<VectorId> {
        self.vectors.read().keys().cloned().collect()
    }

    /// Search within a radius
    pub fn range_search(
        &self,
        query: &[f32],
        radius: f32,
    ) -> Result<Vec<SearchResult>, VectorError> {
        if query.len() != self.dim {
            return Err(VectorError::DimensionMismatch {
                expected: self.dim,
                got: query.len(),
            });
        }

        let vectors = self.vectors.read();
        let mut results = Vec::new();

        for (id, vec) in vectors.iter() {
            let dist = distance(query, vec, self.metric);
            if dist <= radius {
                results.push(SearchResult::new(id.clone(), dist));
            }
        }

        results.sort_by(|a, b| {
            a.score
                .partial_cmp(&b.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok(results)
    }
}

impl VectorIndex for FlatIndex {
    fn add(&self, id: VectorId, vector: &[f32]) -> Result<(), VectorError> {
        if vector.len() != self.dim {
            return Err(VectorError::DimensionMismatch {
                expected: self.dim,
                got: vector.len(),
            });
        }

        self.vectors.write().insert(id, vector.to_vec());
        Ok(())
    }

    fn remove(&self, id: &VectorId) -> Result<bool, VectorError> {
        Ok(self.vectors.write().remove(id).is_some())
    }

    fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, VectorError> {
        if query.len() != self.dim {
            return Err(VectorError::DimensionMismatch {
                expected: self.dim,
                got: query.len(),
            });
        }

        let vectors = self.vectors.read();
        let mut results: Vec<(VectorId, f32)> = vectors
            .iter()
            .map(|(id, vec)| (id.clone(), distance(query, vec, self.metric)))
            .collect();

        // Sort by distance (ascending)
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        // Take top k
        Ok(results
            .into_iter()
            .take(k)
            .map(|(id, score)| SearchResult::new(id, score))
            .collect())
    }

    fn dimension(&self) -> usize {
        self.dim
    }

    fn len(&self) -> usize {
        self.vectors.read().len()
    }

    fn metric(&self) -> DistanceMetric {
        self.metric
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flat_index_basic() {
        let index = FlatIndex::new(3, DistanceMetric::Cosine);

        index.add(VectorId::new("v1"), &[1.0, 0.0, 0.0]).unwrap();
        index.add(VectorId::new("v2"), &[0.0, 1.0, 0.0]).unwrap();
        index.add(VectorId::new("v3"), &[0.0, 0.0, 1.0]).unwrap();

        assert_eq!(index.len(), 3);
        assert!(index.contains(&VectorId::new("v1")));
    }

    #[test]
    fn test_flat_index_search() {
        let index = FlatIndex::new(3, DistanceMetric::Cosine);

        index.add(VectorId::new("v1"), &[1.0, 0.0, 0.0]).unwrap();
        index.add(VectorId::new("v2"), &[0.9, 0.1, 0.0]).unwrap();
        index.add(VectorId::new("v3"), &[0.0, 1.0, 0.0]).unwrap();

        let results = index.search(&[1.0, 0.0, 0.0], 2).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id.as_str(), "v1"); // Exact match
        assert_eq!(results[1].id.as_str(), "v2"); // Similar
    }

    #[test]
    fn test_flat_index_remove() {
        let index = FlatIndex::new(3, DistanceMetric::Cosine);

        index.add(VectorId::new("v1"), &[1.0, 0.0, 0.0]).unwrap();
        assert_eq!(index.len(), 1);

        let removed = index.remove(&VectorId::new("v1")).unwrap();
        assert!(removed);
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn test_flat_index_dimension_mismatch() {
        let index = FlatIndex::new(3, DistanceMetric::Cosine);

        let result = index.add(VectorId::new("v1"), &[1.0, 0.0]);
        assert!(matches!(result, Err(VectorError::DimensionMismatch { .. })));
    }

    #[test]
    fn test_flat_index_euclidean() {
        let index = FlatIndex::new(2, DistanceMetric::Euclidean);

        index.add(VectorId::new("origin"), &[0.0, 0.0]).unwrap();
        index.add(VectorId::new("near"), &[1.0, 0.0]).unwrap();
        index.add(VectorId::new("far"), &[10.0, 10.0]).unwrap();

        let results = index.search(&[0.0, 0.0], 3).unwrap();
        assert_eq!(results[0].id.as_str(), "origin");
        assert_eq!(results[1].id.as_str(), "near");
        assert_eq!(results[2].id.as_str(), "far");
    }

    #[test]
    fn test_flat_index_range_search() {
        let index = FlatIndex::new(2, DistanceMetric::Euclidean);

        index.add(VectorId::new("v1"), &[0.0, 0.0]).unwrap();
        index.add(VectorId::new("v2"), &[1.0, 0.0]).unwrap();
        index.add(VectorId::new("v3"), &[5.0, 0.0]).unwrap();

        let results = index.range_search(&[0.0, 0.0], 2.0).unwrap();
        assert_eq!(results.len(), 2); // v1 and v2 are within radius 2
    }

    #[test]
    fn test_flat_index_empty() {
        let index = FlatIndex::new(3, DistanceMetric::Cosine);
        assert!(index.is_empty());

        let results = index.search(&[1.0, 0.0, 0.0], 10).unwrap();
        assert!(results.is_empty());
    }
}
