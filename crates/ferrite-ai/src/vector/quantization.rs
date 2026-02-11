//! Quantization support for memory-efficient vector storage
//!
//! - **Scalar Quantization (SQ8)**: Quantize f32 to i8, ~4x memory reduction
//! - **Product Quantization (PQ)**: Split vector into subspaces, ~32x memory reduction
//!
//! Both support configurable accuracy/memory tradeoffs.

use super::distance;
use super::types::DistanceMetric;
use super::VectorError;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tracing::debug;

/// Quantization method
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuantizationType {
    /// No quantization (full f32)
    #[default]
    None,
    /// Scalar quantization — f32 to i8
    SQ8,
    /// Product quantization with given number of subspaces and centroids per subspace
    PQ {
        /// Number of subspaces (must divide dimension evenly)
        num_subspaces: usize,
        /// Number of centroids per subspace (typically 256 for u8 codes)
        num_centroids: usize,
    },
}

/// Configuration for quantization
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuantizationConfig {
    /// Quantization type
    pub quantization_type: QuantizationType,
    /// Training sample size (for PQ)
    pub training_sample_size: usize,
    /// Number of k-means iterations for PQ training
    pub training_iterations: usize,
}

impl Default for QuantizationConfig {
    fn default() -> Self {
        Self {
            quantization_type: QuantizationType::None,
            training_sample_size: 10_000,
            training_iterations: 20,
        }
    }
}

impl QuantizationConfig {
    /// Create SQ8 config
    pub fn sq8() -> Self {
        Self {
            quantization_type: QuantizationType::SQ8,
            ..Default::default()
        }
    }

    /// Create PQ config
    pub fn pq(num_subspaces: usize) -> Self {
        Self {
            quantization_type: QuantizationType::PQ {
                num_subspaces,
                num_centroids: 256,
            },
            ..Default::default()
        }
    }
}

// ---------------------------------------------------------------------------
// Scalar Quantization (SQ8)
// ---------------------------------------------------------------------------

/// Scalar quantizer — maps each f32 dimension to an i8 value
#[derive(Clone, Debug)]
pub struct ScalarQuantizer {
    /// Per-dimension minimum values
    pub mins: Vec<f32>,
    /// Per-dimension scale factors (max - min) / 255.0
    pub scales: Vec<f32>,
    /// Dimension
    pub dimension: usize,
}

impl ScalarQuantizer {
    /// Train a scalar quantizer from sample vectors
    pub fn train(vectors: &[&[f32]], dimension: usize) -> Result<Self, VectorError> {
        if vectors.is_empty() {
            return Err(VectorError::InvalidConfig(
                "cannot train SQ8 on empty vector set".to_string(),
            ));
        }

        let mut mins = vec![f32::MAX; dimension];
        let mut maxs = vec![f32::MIN; dimension];

        for vec in vectors {
            if vec.len() != dimension {
                return Err(VectorError::DimensionMismatch {
                    expected: dimension,
                    got: vec.len(),
                });
            }
            for (i, &v) in vec.iter().enumerate() {
                if v < mins[i] {
                    mins[i] = v;
                }
                if v > maxs[i] {
                    maxs[i] = v;
                }
            }
        }

        let scales: Vec<f32> = mins
            .iter()
            .zip(maxs.iter())
            .map(|(&min, &max)| {
                let range = max - min;
                if range > 0.0 {
                    range / 255.0
                } else {
                    1.0
                }
            })
            .collect();

        debug!("trained SQ8 quantizer for {} dimensions", dimension);
        Ok(Self {
            mins,
            scales,
            dimension,
        })
    }

    /// Quantize a f32 vector to u8
    pub fn quantize(&self, vector: &[f32]) -> Vec<u8> {
        vector
            .iter()
            .enumerate()
            .map(|(i, &v)| {
                let normalized = (v - self.mins[i]) / self.scales[i];
                normalized.clamp(0.0, 255.0) as u8
            })
            .collect()
    }

    /// Dequantize a u8 vector back to f32 (approximate)
    pub fn dequantize(&self, quantized: &[u8]) -> Vec<f32> {
        quantized
            .iter()
            .enumerate()
            .map(|(i, &q)| self.mins[i] + (q as f32) * self.scales[i])
            .collect()
    }

    /// Compute approximate distance between a query (f32) and a quantized vector
    pub fn distance_to_quantized(
        &self,
        query: &[f32],
        quantized: &[u8],
        metric: DistanceMetric,
    ) -> f32 {
        let dequantized = self.dequantize(quantized);
        distance::distance(query, &dequantized, metric)
    }

    /// Memory usage of a single quantized vector in bytes
    pub fn quantized_size(&self) -> usize {
        self.dimension
    }

    /// Memory usage of the original f32 vector in bytes
    pub fn original_size(&self) -> usize {
        self.dimension * 4
    }

    /// Compression ratio
    pub fn compression_ratio(&self) -> f32 {
        self.original_size() as f32 / self.quantized_size() as f32
    }
}

// ---------------------------------------------------------------------------
// Product Quantization (PQ)
// ---------------------------------------------------------------------------

/// Product quantizer — splits vectors into subspaces and encodes each with a centroid index
#[derive(Clone, Debug)]
pub struct ProductQuantizer {
    /// Vector dimension
    pub dimension: usize,
    /// Number of subspaces
    pub num_subspaces: usize,
    /// Dimensions per subspace
    pub subspace_dim: usize,
    /// Number of centroids per subspace
    pub num_centroids: usize,
    /// Codebooks: [subspace][centroid] -> subvector
    pub codebooks: Vec<Vec<Vec<f32>>>,
}

impl ProductQuantizer {
    /// Train a product quantizer from sample vectors
    pub fn train(
        vectors: &[&[f32]],
        dimension: usize,
        num_subspaces: usize,
        num_centroids: usize,
        iterations: usize,
    ) -> Result<Self, VectorError> {
        if dimension % num_subspaces != 0 {
            return Err(VectorError::InvalidConfig(format!(
                "dimension {} must be divisible by num_subspaces {}",
                dimension, num_subspaces
            )));
        }
        if vectors.is_empty() {
            return Err(VectorError::InvalidConfig(
                "cannot train PQ on empty vector set".to_string(),
            ));
        }
        if num_centroids == 0 || num_centroids > 256 {
            return Err(VectorError::InvalidConfig(format!(
                "num_centroids must be 1..=256, got {}",
                num_centroids
            )));
        }

        let subspace_dim = dimension / num_subspaces;
        let mut codebooks = Vec::with_capacity(num_subspaces);

        for s in 0..num_subspaces {
            let start = s * subspace_dim;
            let end = start + subspace_dim;

            // Extract subvectors
            let subvectors: Vec<Vec<f32>> = vectors
                .iter()
                .map(|v| v[start..end].to_vec())
                .collect();

            // Run k-means on this subspace
            let actual_k = num_centroids.min(subvectors.len());
            let centroids = kmeans(&subvectors, actual_k, iterations);
            codebooks.push(centroids);
        }

        debug!(
            "trained PQ: {} subspaces, {} centroids each, subspace_dim={}",
            num_subspaces, num_centroids, subspace_dim
        );
        Ok(Self {
            dimension,
            num_subspaces,
            subspace_dim,
            num_centroids,
            codebooks,
        })
    }

    /// Encode a f32 vector to PQ codes (one u8 per subspace)
    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        let mut codes = Vec::with_capacity(self.num_subspaces);
        for s in 0..self.num_subspaces {
            let start = s * self.subspace_dim;
            let end = start + self.subspace_dim;
            let subvec = &vector[start..end];

            let mut best_idx = 0u8;
            let mut best_dist = f32::MAX;
            for (i, centroid) in self.codebooks[s].iter().enumerate() {
                let dist = distance::euclidean_distance_squared(subvec, centroid);
                if dist < best_dist {
                    best_dist = dist;
                    best_idx = i as u8;
                }
            }
            codes.push(best_idx);
        }
        codes
    }

    /// Decode PQ codes back to approximate f32 vector
    pub fn decode(&self, codes: &[u8]) -> Vec<f32> {
        let mut vector = Vec::with_capacity(self.dimension);
        for (s, &code) in codes.iter().enumerate() {
            let idx = code as usize;
            if idx < self.codebooks[s].len() {
                vector.extend_from_slice(&self.codebooks[s][idx]);
            } else {
                vector.extend(std::iter::repeat(0.0).take(self.subspace_dim));
            }
        }
        vector
    }

    /// Compute distance using precomputed distance tables (ADC — Asymmetric Distance Computation)
    ///
    /// This is faster than decode+distance for batch queries.
    pub fn compute_distance_table(&self, query: &[f32]) -> Vec<Vec<f32>> {
        let mut table = Vec::with_capacity(self.num_subspaces);
        for s in 0..self.num_subspaces {
            let start = s * self.subspace_dim;
            let end = start + self.subspace_dim;
            let subquery = &query[start..end];

            let distances: Vec<f32> = self.codebooks[s]
                .iter()
                .map(|centroid| distance::euclidean_distance_squared(subquery, centroid))
                .collect();
            table.push(distances);
        }
        table
    }

    /// Compute approximate distance using a precomputed distance table
    pub fn distance_with_table(&self, table: &[Vec<f32>], codes: &[u8]) -> f32 {
        let mut total = 0.0f32;
        for (s, &code) in codes.iter().enumerate() {
            total += table[s][code as usize];
        }
        total.sqrt()
    }

    /// Memory usage of a single PQ-encoded vector in bytes
    pub fn encoded_size(&self) -> usize {
        self.num_subspaces
    }

    /// Memory usage of the original f32 vector in bytes
    pub fn original_size(&self) -> usize {
        self.dimension * 4
    }

    /// Compression ratio
    pub fn compression_ratio(&self) -> f32 {
        self.original_size() as f32 / self.encoded_size() as f32
    }
}

/// Simple k-means clustering for PQ training
fn kmeans(vectors: &[Vec<f32>], k: usize, iterations: usize) -> Vec<Vec<f32>> {
    if vectors.is_empty() || k == 0 {
        return Vec::new();
    }

    let dim = vectors[0].len();
    let k = k.min(vectors.len());

    // K-means++ initialization
    let mut rng = rand::thread_rng();
    let mut centroids: Vec<Vec<f32>> = Vec::with_capacity(k);
    centroids.push(vectors[rng.gen_range(0..vectors.len())].clone());

    for _ in 1..k {
        let distances: Vec<f32> = vectors
            .iter()
            .map(|v| {
                centroids
                    .iter()
                    .map(|c| distance::euclidean_distance_squared(v, c))
                    .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                    .unwrap_or(f32::MAX)
            })
            .collect();
        let total: f32 = distances.iter().sum();
        if total == 0.0 {
            centroids.push(vectors[rng.gen_range(0..vectors.len())].clone());
            continue;
        }
        let mut threshold = rng.gen::<f32>() * total;
        let mut chosen = 0;
        for (i, &d) in distances.iter().enumerate() {
            threshold -= d;
            if threshold <= 0.0 {
                chosen = i;
                break;
            }
        }
        centroids.push(vectors[chosen].clone());
    }

    // K-means iterations
    for _ in 0..iterations {
        let mut assignments: Vec<Vec<usize>> = vec![Vec::new(); k];
        for (i, v) in vectors.iter().enumerate() {
            let mut best = 0;
            let mut best_dist = f32::MAX;
            for (j, c) in centroids.iter().enumerate() {
                let d = distance::euclidean_distance_squared(v, c);
                if d < best_dist {
                    best_dist = d;
                    best = j;
                }
            }
            assignments[best].push(i);
        }

        let mut converged = true;
        for (j, indices) in assignments.iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let mut new_centroid = vec![0.0f32; dim];
            for &idx in indices {
                for (d, &v) in vectors[idx].iter().enumerate() {
                    new_centroid[d] += v;
                }
            }
            let n = indices.len() as f32;
            for v in &mut new_centroid {
                *v /= n;
            }

            let diff = distance::euclidean_distance_squared(&centroids[j], &new_centroid);
            if diff > 1e-8 {
                converged = false;
            }
            centroids[j] = new_centroid;
        }

        if converged {
            break;
        }
    }

    centroids
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sq8_train_and_quantize() {
        let v1 = [0.0, 0.5, 1.0];
        let v2 = [0.1, 0.6, 0.9];
        let v3 = [0.2, 0.4, 0.8];
        let vectors: Vec<&[f32]> = vec![&v1, &v2, &v3];

        let sq = ScalarQuantizer::train(&vectors, 3).unwrap();
        let quantized = sq.quantize(&v1);
        assert_eq!(quantized.len(), 3);

        let dequantized = sq.dequantize(&quantized);
        assert_eq!(dequantized.len(), 3);

        // Should be approximately the same
        for (orig, deq) in v1.iter().zip(dequantized.iter()) {
            assert!((orig - deq).abs() < 0.01, "orig={}, deq={}", orig, deq);
        }
    }

    #[test]
    fn test_sq8_compression_ratio() {
        let v1 = [0.0; 128];
        let vectors: Vec<&[f32]> = vec![&v1];
        let sq = ScalarQuantizer::train(&vectors, 128).unwrap();
        assert!((sq.compression_ratio() - 4.0).abs() < 0.01);
    }

    #[test]
    fn test_sq8_distance() {
        let v1 = [1.0, 0.0, 0.0];
        let v2 = [0.0, 1.0, 0.0];
        let vectors: Vec<&[f32]> = vec![&v1, &v2];

        let sq = ScalarQuantizer::train(&vectors, 3).unwrap();
        let q2 = sq.quantize(&v2);

        let dist = sq.distance_to_quantized(&v1, &q2, DistanceMetric::Euclidean);
        // Should be approximately sqrt(2) ≈ 1.414
        assert!(
            (dist - std::f32::consts::SQRT_2).abs() < 0.1,
            "dist={}",
            dist
        );
    }

    #[test]
    fn test_sq8_empty_vectors() {
        let vectors: Vec<&[f32]> = vec![];
        let result = ScalarQuantizer::train(&vectors, 3);
        assert!(result.is_err());
    }

    #[test]
    fn test_pq_train_and_encode() {
        // 8-dim vectors, 4 subspaces, 4 centroids each
        let vectors: Vec<Vec<f32>> = (0..50)
            .map(|i| (0..8).map(|j| ((i * 7 + j) as f32) / 50.0).collect())
            .collect();
        let refs: Vec<&[f32]> = vectors.iter().map(|v| v.as_slice()).collect();

        let pq = ProductQuantizer::train(&refs, 8, 4, 4, 10).unwrap();

        let codes = pq.encode(&vectors[0]);
        assert_eq!(codes.len(), 4); // One code per subspace

        let decoded = pq.decode(&codes);
        assert_eq!(decoded.len(), 8);

        // Decoded should be roughly similar to original
        let error: f32 = vectors[0]
            .iter()
            .zip(decoded.iter())
            .map(|(a, b)| (a - b) * (a - b))
            .sum::<f32>()
            .sqrt();
        // With only 50 vectors and 4 centroids, quantization error can be significant
        // but should still be finite and bounded
        assert!(error.is_finite(), "error should be finite: {}", error);
        assert!(error < 10.0, "error should be bounded: {}", error);
    }

    #[test]
    fn test_pq_distance_table() {
        let vectors: Vec<Vec<f32>> = (0..20)
            .map(|i| (0..8).map(|j| ((i * 3 + j) as f32) / 20.0).collect())
            .collect();
        let refs: Vec<&[f32]> = vectors.iter().map(|v| v.as_slice()).collect();

        let pq = ProductQuantizer::train(&refs, 8, 4, 4, 10).unwrap();
        let query = &vectors[0];
        let table = pq.compute_distance_table(query);
        assert_eq!(table.len(), 4); // One per subspace

        let codes = pq.encode(&vectors[1]);
        let dist = pq.distance_with_table(&table, &codes);
        assert!(dist >= 0.0);
        assert!(dist.is_finite());
    }

    #[test]
    fn test_pq_compression_ratio() {
        let vectors: Vec<Vec<f32>> = (0..20)
            .map(|i| (0..128).map(|j| ((i + j) as f32) / 100.0).collect())
            .collect();
        let refs: Vec<&[f32]> = vectors.iter().map(|v| v.as_slice()).collect();

        let pq = ProductQuantizer::train(&refs, 128, 16, 256, 5).unwrap();
        // 128 * 4 bytes = 512 bytes original, 16 bytes encoded
        assert!((pq.compression_ratio() - 32.0).abs() < 0.01);
    }

    #[test]
    fn test_pq_dimension_not_divisible() {
        let v = [0.0; 7];
        let vectors: Vec<&[f32]> = vec![&v];
        let result = ProductQuantizer::train(&vectors, 7, 4, 4, 5);
        assert!(result.is_err());
    }

    #[test]
    fn test_pq_empty_vectors() {
        let vectors: Vec<&[f32]> = vec![];
        let result = ProductQuantizer::train(&vectors, 8, 4, 4, 5);
        assert!(result.is_err());
    }

    #[test]
    fn test_quantization_config() {
        let sq = QuantizationConfig::sq8();
        assert_eq!(sq.quantization_type, QuantizationType::SQ8);

        let pq = QuantizationConfig::pq(16);
        assert!(matches!(
            pq.quantization_type,
            QuantizationType::PQ {
                num_subspaces: 16,
                num_centroids: 256
            }
        ));

        let none = QuantizationConfig::default();
        assert_eq!(none.quantization_type, QuantizationType::None);
    }

    #[test]
    fn test_sq8_preserves_ordering() {
        // Quantized distances should preserve relative ordering
        let origin = [0.0, 0.0, 0.0, 0.0];
        let close = [0.1, 0.1, 0.1, 0.1];
        let far = [0.9, 0.9, 0.9, 0.9];
        let vectors: Vec<&[f32]> = vec![&origin, &close, &far];

        let sq = ScalarQuantizer::train(&vectors, 4).unwrap();
        let q_close = sq.quantize(&close);
        let q_far = sq.quantize(&far);

        let d_close = sq.distance_to_quantized(&origin, &q_close, DistanceMetric::Euclidean);
        let d_far = sq.distance_to_quantized(&origin, &q_far, DistanceMetric::Euclidean);
        assert!(d_close < d_far, "d_close={}, d_far={}", d_close, d_far);
    }
}
