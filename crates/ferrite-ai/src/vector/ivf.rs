//! IVF (Inverted File) Vector Index
//!
//! Memory-efficient approximate nearest neighbor search using inverted file index.
//! The index partitions vectors into clusters and only searches the nearest clusters
//! at query time, providing a good trade-off between speed and accuracy.
//!
//! # Algorithm
//!
//! 1. **Training**: Cluster vectors using k-means into n_clusters centroids
//! 2. **Indexing**: Assign each vector to its nearest centroid
//! 3. **Search**: Find nearest centroids to query, then search within those clusters
//!
//! # Parameters
//!
//! - `n_clusters`: Number of clusters (typically sqrt(N) where N is expected dataset size)
//! - `n_probe`: Number of clusters to search at query time (higher = better recall, slower)

use super::distance::distance;
use super::types::{DistanceMetric, SearchResult, VectorId};
use super::{VectorError, VectorIndex};
use parking_lot::RwLock;
use std::collections::HashMap;

/// IVF (Inverted File) Index
///
/// An approximate nearest neighbor index that partitions vectors into clusters.
/// At search time, only the nearest clusters are searched, making it much faster
/// than brute force for large datasets.
///
/// # Example
///
/// ```
/// use ferrite::vector::{IvfIndex, DistanceMetric, VectorId, VectorIndex};
///
/// let mut index = IvfIndex::new(3, DistanceMetric::Cosine, 4, 2);
///
/// // Add vectors
/// index.add(VectorId::new("v1"), &[1.0, 0.0, 0.0])?;
/// index.add(VectorId::new("v2"), &[0.0, 1.0, 0.0])?;
/// index.add(VectorId::new("v3"), &[0.0, 0.0, 1.0])?;
///
/// // Train the index (builds clusters)
/// index.train();
///
/// // Search
/// let results = index.search(&[1.0, 0.0, 0.0], 2)?;
/// # Ok::<(), ferrite_ai::vector::VectorError>(())
/// ```
pub struct IvfIndex {
    /// Vector dimension
    dim: usize,
    /// Distance metric
    metric: DistanceMetric,
    /// Number of clusters
    n_clusters: usize,
    /// Number of clusters to probe during search
    n_probe: usize,
    /// Cluster centroids (computed during training)
    centroids: RwLock<Vec<Vec<f32>>>,
    /// Vectors organized by cluster
    /// cluster_id -> [(vector_id, vector)]
    #[allow(clippy::type_complexity)]
    clusters: RwLock<Vec<Vec<(VectorId, Vec<f32>)>>>,
    /// Index of vector ID to cluster assignment
    vector_cluster: RwLock<HashMap<VectorId, usize>>,
    /// Unassigned vectors (before training)
    unassigned: RwLock<Vec<(VectorId, Vec<f32>)>>,
    /// Whether the index has been trained
    trained: RwLock<bool>,
    /// K-means iteration limit
    kmeans_iterations: usize,
}

impl IvfIndex {
    /// Create a new IVF index
    ///
    /// # Arguments
    ///
    /// * `dimension` - Vector dimension
    /// * `metric` - Distance metric to use
    /// * `n_clusters` - Number of clusters (typically sqrt(expected_size))
    /// * `n_probe` - Number of clusters to search (higher = better recall)
    pub fn new(
        dimension: usize,
        metric: DistanceMetric,
        n_clusters: usize,
        n_probe: usize,
    ) -> Self {
        let n_clusters = n_clusters.max(1);
        let n_probe = n_probe.max(1).min(n_clusters);

        Self {
            dim: dimension,
            metric,
            n_clusters,
            n_probe,
            centroids: RwLock::new(Vec::new()),
            clusters: RwLock::new(vec![Vec::new(); n_clusters]),
            vector_cluster: RwLock::new(HashMap::new()),
            unassigned: RwLock::new(Vec::new()),
            trained: RwLock::new(false),
            kmeans_iterations: 20,
        }
    }

    /// Set the number of probes for search
    pub fn with_n_probe(mut self, n_probe: usize) -> Self {
        self.n_probe = n_probe.max(1).min(self.n_clusters);
        self
    }

    /// Set the number of k-means iterations
    pub fn with_kmeans_iterations(mut self, iterations: usize) -> Self {
        self.kmeans_iterations = iterations.max(1);
        self
    }

    /// Check if the index is trained
    pub fn is_trained(&self) -> bool {
        *self.trained.read()
    }

    /// Train the index using k-means clustering
    ///
    /// This should be called after adding a sufficient number of vectors.
    /// The index will work before training but with reduced efficiency.
    pub fn train(&self) {
        let unassigned = self.unassigned.read();

        if unassigned.is_empty() {
            return;
        }

        // Collect all vectors for training
        let mut all_vectors: Vec<&Vec<f32>> = unassigned.iter().map(|(_, v)| v).collect();

        // Also include already clustered vectors
        let clusters = self.clusters.read();
        for cluster in clusters.iter() {
            for (_, vec) in cluster {
                all_vectors.push(vec);
            }
        }

        if all_vectors.len() < self.n_clusters {
            // Not enough vectors for training, assign to single cluster
            drop(clusters);
            drop(unassigned);
            self.assign_to_single_cluster();
            return;
        }

        // K-means++ initialization
        let centroids = self.kmeans_plusplus_init(&all_vectors);
        drop(clusters);
        drop(unassigned);

        // Run k-means iterations
        let final_centroids = self.run_kmeans(&centroids, self.kmeans_iterations);

        // Update centroids
        *self.centroids.write() = final_centroids;

        // Reassign all vectors to clusters
        self.reassign_all_vectors();

        *self.trained.write() = true;
    }

    /// K-means++ initialization
    fn kmeans_plusplus_init(&self, vectors: &[&Vec<f32>]) -> Vec<Vec<f32>> {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let mut centroids = Vec::with_capacity(self.n_clusters);

        // Choose first centroid randomly
        let first_idx = rng.gen_range(0..vectors.len());
        centroids.push(vectors[first_idx].clone());

        // Choose remaining centroids
        for _ in 1..self.n_clusters {
            // Calculate distances to nearest centroid for each vector
            let distances: Vec<f32> = vectors
                .iter()
                .map(|v| {
                    centroids
                        .iter()
                        .map(|c| distance(v, c, self.metric))
                        .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                        .unwrap_or(f32::MAX)
                })
                .collect();

            // Choose next centroid with probability proportional to distance squared
            let total: f32 = distances.iter().map(|d| d * d).sum();
            if total == 0.0 {
                // All vectors are identical, just pick randomly
                let idx = rng.gen_range(0..vectors.len());
                centroids.push(vectors[idx].clone());
                continue;
            }

            let mut threshold = rng.gen::<f32>() * total;
            let mut chosen_idx = 0;
            for (i, d) in distances.iter().enumerate() {
                threshold -= d * d;
                if threshold <= 0.0 {
                    chosen_idx = i;
                    break;
                }
            }
            centroids.push(vectors[chosen_idx].clone());
        }

        centroids
    }

    /// Run k-means iterations
    fn run_kmeans(&self, initial_centroids: &[Vec<f32>], max_iterations: usize) -> Vec<Vec<f32>> {
        let mut centroids = initial_centroids.to_vec();

        // Collect all vectors (clone to avoid holding locks during iteration)
        let all_vectors: Vec<Vec<f32>> = {
            let unassigned = self.unassigned.read();
            let clusters = self.clusters.read();

            let mut vecs: Vec<Vec<f32>> = unassigned.iter().map(|(_, v)| v.clone()).collect();
            for cluster in clusters.iter() {
                for (_, vec) in cluster {
                    vecs.push(vec.clone());
                }
            }
            vecs
        };

        for _ in 0..max_iterations {
            // Assign vectors to nearest centroid
            let mut cluster_assignments: Vec<Vec<&Vec<f32>>> = vec![Vec::new(); self.n_clusters];

            for vec in &all_vectors {
                let nearest = self.find_nearest_centroid(vec, &centroids);
                cluster_assignments[nearest].push(vec);
            }

            // Update centroids
            let mut converged = true;
            for (i, assignment) in cluster_assignments.iter().enumerate() {
                if assignment.is_empty() {
                    continue;
                }

                let new_centroid = self.compute_centroid(assignment);
                let diff = distance(&centroids[i], &new_centroid, self.metric);
                if diff > 1e-6 {
                    converged = false;
                }
                centroids[i] = new_centroid;
            }

            if converged {
                break;
            }
        }

        centroids
    }

    /// Find the index of the nearest centroid
    fn find_nearest_centroid(&self, vector: &[f32], centroids: &[Vec<f32>]) -> usize {
        centroids
            .iter()
            .enumerate()
            .map(|(i, c)| (i, distance(vector, c, self.metric)))
            .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(i, _)| i)
            .unwrap_or(0)
    }

    /// Compute the centroid (mean) of a set of vectors
    fn compute_centroid(&self, vectors: &[&Vec<f32>]) -> Vec<f32> {
        if vectors.is_empty() {
            return vec![0.0; self.dim];
        }

        let mut centroid = vec![0.0; self.dim];
        for vec in vectors {
            for (i, v) in vec.iter().enumerate() {
                centroid[i] += v;
            }
        }

        let n = vectors.len() as f32;
        for v in &mut centroid {
            *v /= n;
        }

        centroid
    }

    /// Reassign all vectors to clusters based on current centroids
    fn reassign_all_vectors(&self) {
        let centroids = self.centroids.read();
        if centroids.is_empty() {
            return;
        }

        // Collect all vectors
        let mut all_vectors = Vec::new();

        {
            let unassigned = self.unassigned.read();
            all_vectors.extend(unassigned.iter().cloned());
        }

        {
            let clusters = self.clusters.read();
            for cluster in clusters.iter() {
                all_vectors.extend(cluster.iter().cloned());
            }
        }

        // Clear clusters
        {
            let mut clusters = self.clusters.write();
            for cluster in clusters.iter_mut() {
                cluster.clear();
            }
        }

        // Clear unassigned
        self.unassigned.write().clear();

        // Clear vector-cluster mapping
        self.vector_cluster.write().clear();

        // Reassign
        for (id, vec) in all_vectors {
            let cluster_idx = self.find_nearest_centroid(&vec, &centroids);

            self.clusters.write()[cluster_idx].push((id.clone(), vec));
            self.vector_cluster.write().insert(id, cluster_idx);
        }
    }

    /// Assign all vectors to a single cluster (when not enough vectors for training)
    fn assign_to_single_cluster(&self) {
        let unassigned: Vec<_> = self.unassigned.write().drain(..).collect();

        let mut clusters = self.clusters.write();
        let mut vector_cluster = self.vector_cluster.write();

        for (id, vec) in unassigned {
            clusters[0].push((id.clone(), vec));
            vector_cluster.insert(id, 0);
        }

        // Set a simple centroid
        if !clusters[0].is_empty() {
            let vecs: Vec<&Vec<f32>> = clusters[0].iter().map(|(_, v)| v).collect();
            let centroid = self.compute_centroid(&vecs);
            drop(clusters);
            drop(vector_cluster);
            self.centroids.write().push(centroid);
            *self.trained.write() = true;
        }
    }

    /// Find k nearest clusters to a query vector
    fn find_nearest_clusters(&self, query: &[f32], k: usize) -> Vec<usize> {
        let centroids = self.centroids.read();
        if centroids.is_empty() {
            // Not trained, return all clusters
            return (0..self.n_clusters).collect();
        }

        let mut distances: Vec<(usize, f32)> = centroids
            .iter()
            .enumerate()
            .map(|(i, c)| (i, distance(query, c, self.metric)))
            .collect();

        distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        distances.into_iter().take(k).map(|(i, _)| i).collect()
    }

    /// Get the number of clusters
    pub fn num_clusters(&self) -> usize {
        self.n_clusters
    }

    /// Get cluster sizes
    pub fn cluster_sizes(&self) -> Vec<usize> {
        self.clusters.read().iter().map(|c| c.len()).collect()
    }
}

impl VectorIndex for IvfIndex {
    fn add(&self, id: VectorId, vector: &[f32]) -> Result<(), VectorError> {
        if vector.len() != self.dim {
            return Err(VectorError::DimensionMismatch {
                expected: self.dim,
                got: vector.len(),
            });
        }

        let is_trained = *self.trained.read();

        if is_trained {
            // Index is trained, add directly to appropriate cluster
            let centroids = self.centroids.read();
            let cluster_idx = self.find_nearest_centroid(vector, &centroids);
            drop(centroids);

            self.clusters.write()[cluster_idx].push((id.clone(), vector.to_vec()));
            self.vector_cluster.write().insert(id, cluster_idx);
        } else {
            // Add to unassigned for later training
            self.unassigned.write().push((id, vector.to_vec()));
        }

        Ok(())
    }

    fn remove(&self, id: &VectorId) -> Result<bool, VectorError> {
        // Check unassigned first
        {
            let mut unassigned = self.unassigned.write();
            if let Some(pos) = unassigned.iter().position(|(vid, _)| vid == id) {
                unassigned.remove(pos);
                return Ok(true);
            }
        }

        // Check clusters
        let cluster_idx = self.vector_cluster.write().remove(id);
        if let Some(idx) = cluster_idx {
            let mut clusters = self.clusters.write();
            if let Some(pos) = clusters[idx].iter().position(|(vid, _)| vid == id) {
                clusters[idx].remove(pos);
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, VectorError> {
        if query.len() != self.dim {
            return Err(VectorError::DimensionMismatch {
                expected: self.dim,
                got: query.len(),
            });
        }

        let is_trained = *self.trained.read();

        let mut results: Vec<(VectorId, f32)> = Vec::new();

        if is_trained {
            // Search only in nearest clusters
            let nearest_clusters = self.find_nearest_clusters(query, self.n_probe);
            let clusters = self.clusters.read();

            for cluster_idx in nearest_clusters {
                for (id, vec) in &clusters[cluster_idx] {
                    let dist = distance(query, vec, self.metric);
                    results.push((id.clone(), dist));
                }
            }
        } else {
            // Not trained, search all unassigned vectors
            let unassigned = self.unassigned.read();
            for (id, vec) in unassigned.iter() {
                let dist = distance(query, vec, self.metric);
                results.push((id.clone(), dist));
            }

            // Also search all clusters (in case some vectors were assigned)
            let clusters = self.clusters.read();
            for cluster in clusters.iter() {
                for (id, vec) in cluster {
                    let dist = distance(query, vec, self.metric);
                    results.push((id.clone(), dist));
                }
            }
        }

        // Sort by distance and take top k
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

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
        let unassigned_len = self.unassigned.read().len();
        let clustered_len: usize = self.clusters.read().iter().map(|c| c.len()).sum();
        unassigned_len + clustered_len
    }

    fn metric(&self) -> DistanceMetric {
        self.metric
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ivf_index_basic() {
        let index = IvfIndex::new(3, DistanceMetric::Cosine, 4, 2);

        index.add(VectorId::new("v1"), &[1.0, 0.0, 0.0]).unwrap();
        index.add(VectorId::new("v2"), &[0.0, 1.0, 0.0]).unwrap();
        index.add(VectorId::new("v3"), &[0.0, 0.0, 1.0]).unwrap();

        assert_eq!(index.len(), 3);
    }

    #[test]
    fn test_ivf_index_search_untrained() {
        let index = IvfIndex::new(3, DistanceMetric::Euclidean, 4, 2);

        index.add(VectorId::new("v1"), &[1.0, 0.0, 0.0]).unwrap();
        index.add(VectorId::new("v2"), &[0.0, 1.0, 0.0]).unwrap();
        index.add(VectorId::new("v3"), &[0.0, 0.0, 1.0]).unwrap();

        // Search without training (brute force)
        let results = index.search(&[1.0, 0.0, 0.0], 2).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id.as_str(), "v1");
    }

    #[test]
    fn test_ivf_index_train_and_search() {
        let index = IvfIndex::new(3, DistanceMetric::Euclidean, 2, 2);

        // Add enough vectors for training
        index.add(VectorId::new("v1"), &[1.0, 0.0, 0.0]).unwrap();
        index.add(VectorId::new("v2"), &[0.9, 0.1, 0.0]).unwrap();
        index.add(VectorId::new("v3"), &[0.0, 1.0, 0.0]).unwrap();
        index.add(VectorId::new("v4"), &[0.0, 0.9, 0.1]).unwrap();
        index.add(VectorId::new("v5"), &[0.0, 0.0, 1.0]).unwrap();
        index.add(VectorId::new("v6"), &[0.1, 0.0, 0.9]).unwrap();

        // Train
        index.train();
        assert!(index.is_trained());

        // Search should find closest vectors
        let results = index.search(&[1.0, 0.0, 0.0], 2).unwrap();
        assert_eq!(results.len(), 2);
        // v1 should be closest to [1,0,0]
        assert_eq!(results[0].id.as_str(), "v1");
    }

    #[test]
    fn test_ivf_index_remove() {
        let index = IvfIndex::new(3, DistanceMetric::Cosine, 2, 1);

        index.add(VectorId::new("v1"), &[1.0, 0.0, 0.0]).unwrap();
        index.add(VectorId::new("v2"), &[0.0, 1.0, 0.0]).unwrap();

        assert_eq!(index.len(), 2);

        let removed = index.remove(&VectorId::new("v1")).unwrap();
        assert!(removed);
        assert_eq!(index.len(), 1);

        // Remove from trained index
        index.add(VectorId::new("v3"), &[0.5, 0.5, 0.0]).unwrap();
        index.train();

        let removed = index.remove(&VectorId::new("v2")).unwrap();
        assert!(removed);
    }

    #[test]
    fn test_ivf_index_dimension_mismatch() {
        let index = IvfIndex::new(3, DistanceMetric::Cosine, 4, 2);

        let result = index.add(VectorId::new("v1"), &[1.0, 0.0]);
        assert!(matches!(result, Err(VectorError::DimensionMismatch { .. })));
    }

    #[test]
    fn test_ivf_index_cluster_sizes() {
        let index = IvfIndex::new(2, DistanceMetric::Euclidean, 3, 2);

        // Add vectors in distinct groups
        index.add(VectorId::new("a1"), &[0.0, 0.0]).unwrap();
        index.add(VectorId::new("a2"), &[0.1, 0.0]).unwrap();
        index.add(VectorId::new("b1"), &[10.0, 0.0]).unwrap();
        index.add(VectorId::new("b2"), &[10.1, 0.0]).unwrap();
        index.add(VectorId::new("c1"), &[5.0, 10.0]).unwrap();
        index.add(VectorId::new("c2"), &[5.1, 10.0]).unwrap();

        index.train();

        let sizes = index.cluster_sizes();
        assert_eq!(sizes.iter().sum::<usize>(), 6);
    }

    #[test]
    fn test_ivf_index_with_config() {
        let index = IvfIndex::new(3, DistanceMetric::Cosine, 8, 4)
            .with_n_probe(2)
            .with_kmeans_iterations(10);

        assert_eq!(index.n_probe, 2);
        assert_eq!(index.kmeans_iterations, 10);
    }

    #[test]
    fn test_ivf_index_add_after_train() {
        let index = IvfIndex::new(2, DistanceMetric::Euclidean, 2, 1);

        // Add and train
        index.add(VectorId::new("v1"), &[0.0, 0.0]).unwrap();
        index.add(VectorId::new("v2"), &[10.0, 0.0]).unwrap();
        index.train();

        // Add after training
        index.add(VectorId::new("v3"), &[0.1, 0.0]).unwrap();
        index.add(VectorId::new("v4"), &[9.9, 0.0]).unwrap();

        assert_eq!(index.len(), 4);

        // Search should find new vectors
        let results = index.search(&[0.0, 0.0], 2).unwrap();
        assert!(results
            .iter()
            .any(|r| r.id.as_str() == "v1" || r.id.as_str() == "v3"));
    }

    #[test]
    fn test_ivf_index_empty() {
        let index = IvfIndex::new(3, DistanceMetric::Cosine, 4, 2);
        assert!(index.is_empty());
        assert!(!index.is_trained());

        let results = index.search(&[1.0, 0.0, 0.0], 10).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_ivf_index_cosine_similarity() {
        let index = IvfIndex::new(3, DistanceMetric::Cosine, 2, 2);

        index.add(VectorId::new("v1"), &[1.0, 0.0, 0.0]).unwrap();
        index.add(VectorId::new("v2"), &[0.7, 0.7, 0.0]).unwrap();
        index.add(VectorId::new("v3"), &[-1.0, 0.0, 0.0]).unwrap();

        let results = index.search(&[1.0, 0.0, 0.0], 3).unwrap();
        assert_eq!(results[0].id.as_str(), "v1"); // Exact match
        assert_eq!(results[1].id.as_str(), "v2"); // ~45 degrees
        assert_eq!(results[2].id.as_str(), "v3"); // Opposite direction
    }

    #[test]
    fn test_ivf_index_num_clusters() {
        let index = IvfIndex::new(3, DistanceMetric::Cosine, 8, 2);
        assert_eq!(index.num_clusters(), 8);
    }
}
