//! Distance functions for vector similarity
//!
//! Optimized implementations of common distance metrics.

use super::types::DistanceMetric;

/// Calculate cosine similarity between two vectors
///
/// Returns a value between -1 and 1, where 1 means identical direction.
///
/// # Example
///
/// ```
/// use ferrite::vector::cosine_similarity;
///
/// let a = vec![1.0, 0.0, 0.0];
/// let b = vec![1.0, 0.0, 0.0];
/// assert!((cosine_similarity(&a, &b) - 1.0).abs() < 1e-6);
/// ```
#[inline]
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");

    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;

    // Process in chunks of 4 for better instruction pipelining
    let chunks = a.len() / 4;
    let remainder = a.len() % 4;

    for i in 0..chunks {
        let idx = i * 4;
        let a0 = a[idx];
        let a1 = a[idx + 1];
        let a2 = a[idx + 2];
        let a3 = a[idx + 3];
        let b0 = b[idx];
        let b1 = b[idx + 1];
        let b2 = b[idx + 2];
        let b3 = b[idx + 3];

        dot += a0 * b0 + a1 * b1 + a2 * b2 + a3 * b3;
        norm_a += a0 * a0 + a1 * a1 + a2 * a2 + a3 * a3;
        norm_b += b0 * b0 + b1 * b1 + b2 * b2 + b3 * b3;
    }

    // Handle remainder
    for i in 0..remainder {
        let idx = chunks * 4 + i;
        dot += a[idx] * b[idx];
        norm_a += a[idx] * a[idx];
        norm_b += b[idx] * b[idx];
    }

    let norm = (norm_a * norm_b).sqrt();
    if norm > 0.0 {
        dot / norm
    } else {
        0.0
    }
}

/// Calculate cosine distance (1 - similarity)
#[inline]
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    1.0 - cosine_similarity(a, b)
}

/// Calculate dot product between two vectors
///
/// # Example
///
/// ```
/// use ferrite::vector::dot_product;
///
/// let a = vec![1.0, 2.0, 3.0];
/// let b = vec![4.0, 5.0, 6.0];
/// assert!((dot_product(&a, &b) - 32.0).abs() < 1e-6);
/// ```
#[inline]
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");

    let mut sum = 0.0f32;

    // Process in chunks of 4
    let chunks = a.len() / 4;
    let remainder = a.len() % 4;

    for i in 0..chunks {
        let idx = i * 4;
        sum += a[idx] * b[idx]
            + a[idx + 1] * b[idx + 1]
            + a[idx + 2] * b[idx + 2]
            + a[idx + 3] * b[idx + 3];
    }

    for i in 0..remainder {
        let idx = chunks * 4 + i;
        sum += a[idx] * b[idx];
    }

    sum
}

/// Calculate Euclidean distance (L2) between two vectors
///
/// # Example
///
/// ```
/// use ferrite::vector::euclidean_distance;
///
/// let a = vec![0.0, 0.0, 0.0];
/// let b = vec![3.0, 4.0, 0.0];
/// assert!((euclidean_distance(&a, &b) - 5.0).abs() < 1e-6);
/// ```
#[inline]
pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    euclidean_distance_squared(a, b).sqrt()
}

/// Calculate squared Euclidean distance (faster, no sqrt)
#[inline]
pub fn euclidean_distance_squared(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");

    let mut sum = 0.0f32;

    // Process in chunks of 4
    let chunks = a.len() / 4;
    let remainder = a.len() % 4;

    for i in 0..chunks {
        let idx = i * 4;
        let d0 = a[idx] - b[idx];
        let d1 = a[idx + 1] - b[idx + 1];
        let d2 = a[idx + 2] - b[idx + 2];
        let d3 = a[idx + 3] - b[idx + 3];

        sum += d0 * d0 + d1 * d1 + d2 * d2 + d3 * d3;
    }

    for i in 0..remainder {
        let idx = chunks * 4 + i;
        let d = a[idx] - b[idx];
        sum += d * d;
    }

    sum
}

/// Calculate Manhattan distance (L1) between two vectors
///
/// # Example
///
/// ```
/// use ferrite::vector::manhattan_distance;
///
/// let a = vec![0.0, 0.0];
/// let b = vec![3.0, 4.0];
/// assert!((manhattan_distance(&a, &b) - 7.0).abs() < 1e-6);
/// ```
#[inline]
pub fn manhattan_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");

    let mut sum = 0.0f32;

    for i in 0..a.len() {
        sum += (a[i] - b[i]).abs();
    }

    sum
}

/// Calculate distance based on metric type
#[inline]
pub fn distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::Cosine => cosine_distance(a, b),
        DistanceMetric::Euclidean => euclidean_distance(a, b),
        DistanceMetric::DotProduct => -dot_product(a, b), // Negative for distance (higher = better)
        DistanceMetric::Manhattan => manhattan_distance(a, b),
    }
}

/// Calculate similarity based on metric type (higher is better)
#[inline]
#[allow(dead_code)] // Planned for v0.2 — generic similarity dispatch for vector queries
pub fn similarity(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::Cosine => cosine_similarity(a, b),
        DistanceMetric::Euclidean => 1.0 / (1.0 + euclidean_distance(a, b)),
        DistanceMetric::DotProduct => dot_product(a, b),
        DistanceMetric::Manhattan => 1.0 / (1.0 + manhattan_distance(a, b)),
    }
}

/// Normalize a vector to unit length
#[allow(dead_code)] // Planned for v0.2 — vector normalization for cosine similarity preprocessing
pub fn normalize(vector: &mut [f32]) {
    let norm: f32 = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in vector.iter_mut() {
            *x /= norm;
        }
    }
}

/// Get the L2 norm of a vector
#[inline]
#[allow(dead_code)] // Planned for v0.2 — norm computation for vector validation
pub fn l2_norm(vector: &[f32]) -> f32 {
    vector.iter().map(|x| x * x).sum::<f32>().sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_similarity_identical() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![1.0, 2.0, 3.0];
        assert!((cosine_similarity(&a, &b) - 1.0).abs() < 1e-5);
    }

    #[test]
    fn test_cosine_similarity_orthogonal() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        assert!(cosine_similarity(&a, &b).abs() < 1e-5);
    }

    #[test]
    fn test_cosine_similarity_opposite() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![-1.0, 0.0, 0.0];
        assert!((cosine_similarity(&a, &b) + 1.0).abs() < 1e-5);
    }

    #[test]
    fn test_dot_product() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        // 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
        assert!((dot_product(&a, &b) - 32.0).abs() < 1e-5);
    }

    #[test]
    fn test_euclidean_distance() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![3.0, 4.0, 0.0];
        assert!((euclidean_distance(&a, &b) - 5.0).abs() < 1e-5);
    }

    #[test]
    fn test_euclidean_distance_same() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![1.0, 2.0, 3.0];
        assert!(euclidean_distance(&a, &b).abs() < 1e-5);
    }

    #[test]
    fn test_manhattan_distance() {
        let a = vec![0.0, 0.0];
        let b = vec![3.0, 4.0];
        assert!((manhattan_distance(&a, &b) - 7.0).abs() < 1e-5);
    }

    #[test]
    fn test_normalize() {
        let mut v = vec![3.0, 4.0, 0.0];
        normalize(&mut v);
        assert!((v[0] - 0.6).abs() < 1e-5);
        assert!((v[1] - 0.8).abs() < 1e-5);
        assert!(v[2].abs() < 1e-5);
    }

    #[test]
    fn test_l2_norm() {
        let v = vec![3.0, 4.0, 0.0];
        assert!((l2_norm(&v) - 5.0).abs() < 1e-5);
    }

    #[test]
    fn test_distance_metrics() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];

        let d_cosine = distance(&a, &b, DistanceMetric::Cosine);
        assert!((d_cosine - 1.0).abs() < 1e-5); // Orthogonal = distance 1

        let d_euclidean = distance(&a, &b, DistanceMetric::Euclidean);
        assert!((d_euclidean - std::f32::consts::SQRT_2).abs() < 1e-5);
    }

    #[test]
    fn test_high_dimension() {
        let dim = 384;
        let a: Vec<f32> = (0..dim).map(|i| (i as f32) / (dim as f32)).collect();
        let b: Vec<f32> = (0..dim)
            .map(|i| ((dim - i) as f32) / (dim as f32))
            .collect();

        // Just verify it doesn't crash and returns a valid number
        let sim = cosine_similarity(&a, &b);
        assert!(sim.is_finite());
        assert!(sim >= -1.0 && sim <= 1.0);
    }
}
