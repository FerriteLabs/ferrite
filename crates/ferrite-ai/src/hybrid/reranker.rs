#![forbid(unsafe_code)]
//! Cross-encoder re-ranking for hybrid retrieval.

use serde::{Deserialize, Serialize};

/// A document to be re-ranked.
#[derive(Clone, Debug)]
pub struct Document {
    /// Document identifier
    pub id: String,
    /// Document text content
    pub text: String,
    /// Original score from first-stage retrieval
    pub original_score: f64,
}

/// A re-ranked document with updated score.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RankedDoc {
    /// Document identifier
    pub doc_id: String,
    /// Score from the first-stage retrieval
    pub original_score: f64,
    /// Score after re-ranking
    pub reranked_score: f64,
    /// Final rank (1-based)
    pub rank: usize,
}

/// Trait for re-ranking documents given a query.
pub trait Reranker: Send + Sync {
    /// Re-rank documents and return the top_k results.
    fn rerank(&self, query: &str, documents: &[Document], top_k: usize) -> Vec<RankedDoc>;
}

/// Lightweight re-ranker using keyword overlap scoring.
pub struct SimpleReranker;

impl SimpleReranker {
    /// Tokenize text into lowercase terms.
    fn tokenize(text: &str) -> Vec<String> {
        text.split_whitespace()
            .map(|t| t.to_lowercase())
            .collect()
    }

    /// Compute keyword overlap score between query and document.
    fn overlap_score(query_terms: &[String], doc_terms: &[String]) -> f64 {
        if query_terms.is_empty() || doc_terms.is_empty() {
            return 0.0;
        }

        let mut matches = 0usize;
        for qt in query_terms {
            if doc_terms.contains(qt) {
                matches += 1;
            }
        }

        // Normalized overlap: matches / query_terms
        matches as f64 / query_terms.len() as f64
    }
}

impl Reranker for SimpleReranker {
    fn rerank(&self, query: &str, documents: &[Document], top_k: usize) -> Vec<RankedDoc> {
        let query_terms = Self::tokenize(query);

        let mut scored: Vec<(usize, f64, f64)> = documents
            .iter()
            .enumerate()
            .map(|(i, doc)| {
                let doc_terms = Self::tokenize(&doc.text);
                let overlap = Self::overlap_score(&query_terms, &doc_terms);
                // Combine original score with overlap (weighted)
                let reranked = 0.4 * doc.original_score + 0.6 * overlap;
                (i, doc.original_score, reranked)
            })
            .collect();

        scored.sort_by(|a, b| {
            b.2.partial_cmp(&a.2)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        scored.truncate(top_k);

        scored
            .into_iter()
            .enumerate()
            .map(|(rank, (idx, original_score, reranked_score))| RankedDoc {
                doc_id: documents[idx].id.clone(),
                original_score,
                reranked_score,
                rank: rank + 1,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_reranker() {
        let reranker = SimpleReranker;
        let docs = vec![
            Document {
                id: "d1".to_string(),
                text: "the quick brown fox".to_string(),
                original_score: 0.8,
            },
            Document {
                id: "d2".to_string(),
                text: "a lazy brown dog".to_string(),
                original_score: 0.9,
            },
            Document {
                id: "d3".to_string(),
                text: "quick fox jumps high".to_string(),
                original_score: 0.7,
            },
        ];

        let results = reranker.rerank("quick fox", &docs, 3);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].rank, 1);
        // d1 has both "quick" and "fox", d3 also has both
        assert!(results[0].doc_id == "d1" || results[0].doc_id == "d3");
    }

    #[test]
    fn test_reranker_empty() {
        let reranker = SimpleReranker;
        let results = reranker.rerank("query", &[], 10);
        assert!(results.is_empty());
    }

    #[test]
    fn test_reranker_top_k() {
        let reranker = SimpleReranker;
        let docs: Vec<Document> = (0..10)
            .map(|i| Document {
                id: format!("d{}", i),
                text: format!("document number {}", i),
                original_score: 0.5,
            })
            .collect();

        let results = reranker.rerank("document", &docs, 3);
        assert_eq!(results.len(), 3);
    }
}
