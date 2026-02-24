#![forbid(unsafe_code)]
//! BM25 scoring algorithm.

use serde::{Deserialize, Serialize};

/// Standard BM25 scorer with configurable parameters.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Bm25Scorer {
    /// Term frequency saturation parameter (default: 1.2)
    pub k1: f64,
    /// Length normalization parameter (default: 0.75)
    pub b: f64,
}

impl Default for Bm25Scorer {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

impl Bm25Scorer {
    /// Create a new scorer with custom parameters.
    pub fn new(k1: f64, b: f64) -> Self {
        Self { k1, b }
    }

    /// Compute BM25 score for a single term in a document.
    ///
    /// - `tf`: term frequency in the document
    /// - `df`: document frequency (number of docs containing the term)
    /// - `doc_len`: length of the document (in tokens)
    /// - `avg_doc_len`: average document length across the corpus
    /// - `total_docs`: total number of documents in the corpus
    pub fn score(
        &self,
        tf: f64,
        df: u64,
        doc_len: u64,
        avg_doc_len: f64,
        total_docs: u64,
    ) -> f64 {
        if total_docs == 0 || df == 0 {
            return 0.0;
        }

        // IDF component: log((N - n + 0.5) / (n + 0.5) + 1)
        let n = df as f64;
        let big_n = total_docs as f64;
        let idf = ((big_n - n + 0.5) / (n + 0.5) + 1.0).ln();

        // TF component with length normalization
        let dl = doc_len as f64;
        let numerator = tf * (self.k1 + 1.0);
        let denominator = tf + self.k1 * (1.0 - self.b + self.b * dl / avg_doc_len);

        idf * (numerator / denominator)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_params() {
        let scorer = Bm25Scorer::default();
        assert!((scorer.k1 - 1.2).abs() < f64::EPSILON);
        assert!((scorer.b - 0.75).abs() < f64::EPSILON);
    }

    #[test]
    fn test_score_basic() {
        let scorer = Bm25Scorer::default();
        let score = scorer.score(2.0, 5, 100, 120.0, 1000);
        assert!(score > 0.0);
    }

    #[test]
    fn test_score_zero_docs() {
        let scorer = Bm25Scorer::default();
        assert_eq!(scorer.score(1.0, 1, 10, 10.0, 0), 0.0);
    }

    #[test]
    fn test_score_zero_df() {
        let scorer = Bm25Scorer::default();
        assert_eq!(scorer.score(1.0, 0, 10, 10.0, 100), 0.0);
    }

    #[test]
    fn test_higher_tf_higher_score() {
        let scorer = Bm25Scorer::default();
        let s1 = scorer.score(1.0, 10, 100, 100.0, 1000);
        let s2 = scorer.score(5.0, 10, 100, 100.0, 1000);
        assert!(s2 > s1);
    }

    #[test]
    fn test_rarer_term_higher_score() {
        let scorer = Bm25Scorer::default();
        let common = scorer.score(1.0, 500, 100, 100.0, 1000);
        let rare = scorer.score(1.0, 5, 100, 100.0, 1000);
        assert!(rare > common);
    }
}
