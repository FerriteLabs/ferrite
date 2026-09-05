//! Mnemo hybrid retrieval scorer.
//!
//! Combines four signals into a single score in `[0.0, 1.0]`:
//!
//! - **Semantic similarity** — cosine similarity between query and record embeddings.
//! - **Importance** — record's caller-supplied `importance` field.
//! - **Recency** — exponential decay of `now - last_accessed`.
//! - **Frequency** — log-scaled `access_count`.
//!
//! The default weights are documented in
//! [`docs/phases/m1-mnemo-roadmap.md`](../../../docs/phases/m1-mnemo-roadmap.md).
//! Future ADR-018 amendments may tune them per-tenant.

use crate::schema::MemoryRecord;

// ---------------------------------------------------------------------------
// Similarity function selection
// ---------------------------------------------------------------------------

/// Selects the vector similarity function used for the semantic component.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum SimilarityFn {
    #[default]
    Cosine,
    DotProduct,
    Euclidean,
}

// ---------------------------------------------------------------------------
// Weights
// ---------------------------------------------------------------------------

/// Tunable weights for the hybrid scorer.  Must sum to 1.0; if not, scoring
/// still works but absolute values lose interpretability.
#[derive(Debug, Clone, Copy)]
pub struct ScorerWeights {
    pub semantic: f32,
    pub importance: f32,
    pub recency: f32,
    pub frequency: f32,
    /// Half-life of recency decay in milliseconds.  Default 24h.
    pub recency_half_life_ms: u64,
}

impl Default for ScorerWeights {
    fn default() -> Self {
        Self {
            semantic: 0.55,
            importance: 0.20,
            recency: 0.15,
            frequency: 0.10,
            recency_half_life_ms: 24 * 60 * 60 * 1_000,
        }
    }
}

// ---------------------------------------------------------------------------
// HybridScorer — stateful, configurable wrapper
// ---------------------------------------------------------------------------

/// Configurable hybrid scorer that combines semantic similarity, importance,
/// recency, and frequency signals.
#[derive(Debug, Clone, Default)]
pub struct HybridScorer {
    pub weights: ScorerWeights,
}

impl HybridScorer {
    pub fn new(weights: ScorerWeights) -> Self {
        Self { weights }
    }

    /// Score a record against a query embedding with the full hybrid formula.
    pub fn score(
        &self,
        query_embedding: Option<&[f32]>,
        record: &MemoryRecord,
        now_ms: u64,
        similarity_fn: SimilarityFn,
    ) -> f32 {
        let semantic = match (query_embedding, record.embedding.as_ref()) {
            (Some(q), Some(r)) => self.compute_similarity(q, r, &similarity_fn),
            _ => 0.0,
        };
        let importance = record.importance.clamp(0.0, 1.0);
        let recency = self.recency_score(record.last_accessed, now_ms);
        let frequency = self.frequency_score(record.access_count);

        self.weights.semantic * semantic
            + self.weights.importance * importance
            + self.weights.recency * recency
            + self.weights.frequency * frequency
    }

    /// Dispatch to the selected similarity function and clamp to [0, 1].
    fn compute_similarity(&self, a: &[f32], b: &[f32], sim_fn: &SimilarityFn) -> f32 {
        match sim_fn {
            SimilarityFn::Cosine => cosine_similarity(a, b).clamp(0.0, 1.0),
            SimilarityFn::DotProduct => dot_product(a, b).clamp(0.0, 1.0),
            SimilarityFn::Euclidean => euclidean_similarity(a, b).clamp(0.0, 1.0),
        }
    }

    fn recency_score(&self, last_accessed_ms: u64, now_ms: u64) -> f32 {
        recency_score(last_accessed_ms, now_ms, self.weights.recency_half_life_ms)
    }

    fn frequency_score(&self, access_count: u64) -> f32 {
        frequency_score(access_count)
    }

    /// Score and sort a batch of records.
    pub fn score_records(
        &self,
        records: &[MemoryRecord],
        query_embedding: Option<&[f32]>,
        now_ms: u64,
        similarity_fn: SimilarityFn,
    ) -> Vec<Scored> {
        let mut out: Vec<Scored> = records
            .iter()
            .map(|r| Scored {
                score: self.score(query_embedding, r, now_ms, similarity_fn),
                record: r.clone(),
            })
            .collect();
        out.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        out
    }
}

/// A scored record.  Returned by [`score_records`] sorted descending.
#[derive(Debug, Clone)]
pub struct Scored {
    pub record: MemoryRecord,
    pub score: f32,
}

// ---------------------------------------------------------------------------
// Free-function API (backwards compatible)
// ---------------------------------------------------------------------------

/// Score records against an optional query embedding.
///
/// Returns the records sorted by descending hybrid score.  When the query
/// has no embedding the semantic component is treated as zero (the call
/// falls back to importance + recency + frequency).
pub fn score_records(
    records: &[MemoryRecord],
    query_embedding: Option<&[f32]>,
    now_ms: u64,
    weights: &ScorerWeights,
) -> Vec<Scored> {
    let mut out: Vec<Scored> = records
        .iter()
        .map(|r| Scored {
            score: hybrid_score(r, query_embedding, now_ms, weights),
            record: r.clone(),
        })
        .collect();
    out.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    out
}

/// Score a single record.  Pure function — no I/O, deterministic given
/// the same inputs (modulo NaN in embeddings, which is treated as zero).
pub fn hybrid_score(
    record: &MemoryRecord,
    query_embedding: Option<&[f32]>,
    now_ms: u64,
    w: &ScorerWeights,
) -> f32 {
    let semantic = match (query_embedding, record.embedding.as_ref()) {
        (Some(q), Some(r)) => cosine_similarity(q, r).clamp(0.0, 1.0),
        _ => 0.0,
    };
    let importance = record.importance.clamp(0.0, 1.0);
    let recency = recency_score(record.last_accessed, now_ms, w.recency_half_life_ms);
    let frequency = frequency_score(record.access_count);
    w.semantic * semantic
        + w.importance * importance
        + w.recency * recency
        + w.frequency * frequency
}

// ---------------------------------------------------------------------------
// Similarity functions
// ---------------------------------------------------------------------------

/// Standard cosine similarity.  Returns 0.0 if either vector is zero, has
/// mismatched length, or contains NaN.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.is_empty() || a.len() != b.len() {
        return 0.0;
    }
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        if x.is_nan() || y.is_nan() {
            return 0.0;
        }
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

/// Raw dot product.  Returns 0.0 on mismatched length, empty, or NaN.
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    if a.is_empty() || a.len() != b.len() {
        return 0.0;
    }
    let mut sum = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        if x.is_nan() || y.is_nan() {
            return 0.0;
        }
        sum += x * y;
    }
    sum
}

/// Euclidean distance between two vectors.  Returns 0.0 on edge cases.
pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    if a.is_empty() || a.len() != b.len() {
        return 0.0;
    }
    let mut sum = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        if x.is_nan() || y.is_nan() {
            return 0.0;
        }
        let d = x - y;
        sum += d * d;
    }
    sum.sqrt()
}

/// Convert euclidean distance to a similarity score in [0, 1].
/// Uses `1 / (1 + distance)` so identical vectors → 1, far vectors → 0.
pub fn euclidean_similarity(a: &[f32], b: &[f32]) -> f32 {
    1.0 / (1.0 + euclidean_distance(a, b))
}

fn recency_score(last_accessed_ms: u64, now_ms: u64, half_life_ms: u64) -> f32 {
    if half_life_ms == 0 || now_ms <= last_accessed_ms {
        return 1.0;
    }
    let age_ms = now_ms - last_accessed_ms;
    // 2^(-age/half_life)
    let ratio = age_ms as f64 / half_life_ms as f64;
    (-(ratio * std::f64::consts::LN_2)).exp() as f32
}

fn frequency_score(access_count: u64) -> f32 {
    // log1p saturates: 0→0, 10→0.4, 100→0.67, 1k→0.79.  Capped at 1.
    let v = (access_count as f64).ln_1p() / (1e6f64).ln();
    (v as f32).clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{MemoryKind, MemoryRecordBuilder};

    fn rec_with(
        embedding: Option<Vec<f32>>,
        importance: f32,
        last: u64,
        count: u64,
    ) -> MemoryRecord {
        let mut r = MemoryRecordBuilder::new()
            .id("r")
            .tenant("t")
            .agent("a")
            .kind(MemoryKind::Semantic)
            .content("c")
            .importance(importance)
            .created_at(0)
            .build()
            .unwrap();
        r.embedding = embedding;
        r.last_accessed = last;
        r.access_count = count;
        r
    }

    // -----------------------------------------------------------------------
    // Cosine similarity
    // -----------------------------------------------------------------------

    #[test]
    fn cosine_identical_vectors_is_one() {
        let v = vec![1.0, 2.0, 3.0];
        assert!((cosine_similarity(&v, &v) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_orthogonal_vectors_is_zero() {
        assert!(cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]).abs() < 1e-6);
    }

    #[test]
    fn cosine_basic() {
        assert!((cosine_similarity(&[1.0, 0.0], &[1.0, 0.0]) - 1.0).abs() < 1e-6);
        assert_eq!(cosine_similarity(&[], &[]), 0.0);
        assert_eq!(cosine_similarity(&[1.0], &[1.0, 2.0]), 0.0);
        assert_eq!(cosine_similarity(&[0.0, 0.0], &[1.0, 1.0]), 0.0);
        assert_eq!(cosine_similarity(&[f32::NAN, 1.0], &[1.0, 1.0]), 0.0);
    }

    // -----------------------------------------------------------------------
    // Dot product
    // -----------------------------------------------------------------------

    #[test]
    fn dot_product_basic() {
        assert!((dot_product(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]) - 32.0).abs() < 1e-6);
        assert_eq!(dot_product(&[], &[]), 0.0);
        assert_eq!(dot_product(&[1.0], &[1.0, 2.0]), 0.0);
        assert_eq!(dot_product(&[f32::NAN], &[1.0]), 0.0);
    }

    #[test]
    fn dot_product_orthogonal_is_zero() {
        assert!(dot_product(&[1.0, 0.0], &[0.0, 1.0]).abs() < 1e-6);
    }

    // -----------------------------------------------------------------------
    // Euclidean distance / similarity
    // -----------------------------------------------------------------------

    #[test]
    fn euclidean_identical_vectors_zero_distance() {
        let v = vec![1.0, 2.0, 3.0];
        assert!(euclidean_distance(&v, &v).abs() < 1e-6);
        assert!((euclidean_similarity(&v, &v) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn euclidean_distance_basic() {
        // distance between (0,0) and (3,4) = 5
        assert!((euclidean_distance(&[0.0, 0.0], &[3.0, 4.0]) - 5.0).abs() < 1e-5);
        assert_eq!(euclidean_distance(&[], &[]), 0.0);
        assert_eq!(euclidean_distance(&[1.0], &[1.0, 2.0]), 0.0);
        assert_eq!(euclidean_distance(&[f32::NAN], &[1.0]), 0.0);
    }

    #[test]
    fn euclidean_similarity_decreases_with_distance() {
        let origin = [0.0, 0.0];
        let near = [1.0, 0.0];
        let far = [10.0, 0.0];
        let s_near = euclidean_similarity(&origin, &near);
        let s_far = euclidean_similarity(&origin, &far);
        assert!(s_near > s_far, "near={s_near} should > far={s_far}");
    }

    // -----------------------------------------------------------------------
    // Recency & frequency
    // -----------------------------------------------------------------------

    #[test]
    fn recency_decays_over_time() {
        let s_now = recency_score(1000, 1000, 1000);
        let s_half = recency_score(0, 1000, 1000);
        let s_old = recency_score(0, 10_000, 1000);
        assert!((s_now - 1.0).abs() < 1e-6);
        assert!((s_half - 0.5).abs() < 1e-3);
        assert!(s_old < s_half, "older should score lower");
    }

    #[test]
    fn recency_decays_to_half_at_half_life() {
        let s = recency_score(0, 1000, 1000);
        assert!((s - 0.5).abs() < 1e-3, "got {s}");
        let s2 = recency_score(0, 0, 1000);
        assert!((s2 - 1.0).abs() < 1e-6);
    }

    #[test]
    fn frequency_increases_with_access_count() {
        let s0 = frequency_score(0);
        let s1 = frequency_score(1);
        let s10 = frequency_score(10);
        let s100 = frequency_score(100);
        assert!(s0 < s1, "0 < 1 access");
        assert!(s1 < s10, "1 < 10 accesses");
        assert!(s10 < s100, "10 < 100 accesses");
    }

    #[test]
    fn frequency_monotone() {
        let s0 = frequency_score(0);
        let s10 = frequency_score(10);
        let s100 = frequency_score(100);
        assert!(s0 < s10 && s10 < s100 && s100 < 1.0);
    }

    // -----------------------------------------------------------------------
    // HybridScorer
    // -----------------------------------------------------------------------

    #[test]
    fn hybrid_scorer_full_computation() {
        let scorer = HybridScorer::default();
        let rec = rec_with(Some(vec![1.0, 0.0]), 0.8, 500, 50);
        let query = vec![1.0, 0.0];
        let score = scorer.score(Some(&query), &rec, 1000, SimilarityFn::Cosine);
        // semantic=1.0, importance=0.8, recency and frequency contribute
        assert!(score > 0.0);
        assert!(score <= 1.0, "score {score} should be ≤ 1.0");
    }

    #[test]
    fn hybrid_scorer_no_embedding_falls_back() {
        let scorer = HybridScorer::default();
        let rec = rec_with(None, 0.9, 0, 10);
        let score = scorer.score(None, &rec, 0, SimilarityFn::Cosine);
        // No semantic component, but importance+recency+frequency still contribute
        assert!(score > 0.0);
    }

    #[test]
    fn hybrid_scorer_dot_product_mode() {
        let scorer = HybridScorer::default();
        let rec = rec_with(Some(vec![0.5, 0.5]), 0.5, 0, 0);
        let query = vec![0.5, 0.5];
        let score_cos = scorer.score(Some(&query), &rec, 0, SimilarityFn::Cosine);
        let score_dot = scorer.score(Some(&query), &rec, 0, SimilarityFn::DotProduct);
        // Both should produce a positive score
        assert!(score_cos > 0.0);
        assert!(score_dot > 0.0);
    }

    #[test]
    fn hybrid_scorer_euclidean_mode() {
        let scorer = HybridScorer::default();
        let rec = rec_with(Some(vec![1.0, 0.0]), 0.5, 0, 0);
        let query = vec![1.0, 0.0];
        let score = scorer.score(Some(&query), &rec, 0, SimilarityFn::Euclidean);
        // Identical vectors → euclidean_similarity = 1.0
        assert!(
            score > 0.5,
            "identical embeddings should score high, got {score}"
        );
    }

    #[test]
    fn hybrid_scorer_score_records_orders_correctly() {
        let scorer = HybridScorer::default();
        let q = vec![1.0, 0.0];
        let near = rec_with(Some(vec![1.0, 0.0]), 0.5, 0, 0);
        let far = rec_with(Some(vec![0.0, 1.0]), 0.9, 0, 0);
        let scored = scorer.score_records(&[far, near], Some(&q), 0, SimilarityFn::Cosine);
        assert_eq!(
            scored[0].record.embedding.as_ref().unwrap(),
            &vec![1.0, 0.0]
        );
    }

    // -----------------------------------------------------------------------
    // Legacy free-function tests
    // -----------------------------------------------------------------------

    #[test]
    fn score_records_orders_semantic_high_first() {
        let q = vec![1.0, 0.0];
        let near = rec_with(Some(vec![1.0, 0.0]), 0.5, 0, 0);
        let far = rec_with(Some(vec![0.0, 1.0]), 0.9, 0, 0);
        let scored = score_records(&[far, near], Some(&q), 0, &ScorerWeights::default());
        assert_eq!(
            scored[0].record.embedding.as_ref().unwrap(),
            &vec![1.0, 0.0]
        );
    }

    #[test]
    fn score_falls_back_when_no_query_embedding() {
        let high_imp = rec_with(None, 0.9, 0, 0);
        let low_imp = rec_with(None, 0.1, 0, 0);
        let scored = score_records(&[low_imp, high_imp], None, 0, &ScorerWeights::default());
        assert!(scored[0].score > scored[1].score);
        assert!(scored[0].record.importance > scored[1].record.importance);
    }

    #[test]
    fn weights_default_sum_to_one() {
        let w = ScorerWeights::default();
        let sum = w.semantic + w.importance + w.recency + w.frequency;
        assert!((sum - 1.0).abs() < 1e-6);
    }
}
