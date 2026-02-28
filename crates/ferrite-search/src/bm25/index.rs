#![forbid(unsafe_code)]
//! BM25 inverted index for sparse retrieval.

use std::collections::HashMap;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use super::scorer::Bm25Scorer;

/// A BM25-scored search result.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScoredDoc {
    /// Document identifier
    pub doc_id: String,
    /// BM25 score
    pub score: f64,
}

/// Term-level posting: document ID and term frequency.
#[derive(Clone, Debug)]
struct Posting {
    doc_id: String,
    tf: u32,
}

/// Per-document metadata.
#[derive(Clone, Debug)]
struct DocInfo {
    /// Number of tokens in the document
    len: u64,
    /// Terms present in this document (for removal)
    terms: Vec<String>,
}

/// Thread-safe BM25 inverted index.
pub struct Bm25Index {
    /// Inverted index: term → list of postings
    postings: DashMap<String, Vec<Posting>>,
    /// Document metadata
    docs: DashMap<String, DocInfo>,
    /// Total token count across all documents (for avg_doc_len)
    total_tokens: std::sync::atomic::AtomicU64,
    /// BM25 scorer
    scorer: Bm25Scorer,
}

impl Default for Bm25Index {
    fn default() -> Self {
        Self::new(Bm25Scorer::default())
    }
}

impl Bm25Index {
    /// Create a new index with the given scorer.
    pub fn new(scorer: Bm25Scorer) -> Self {
        Self {
            postings: DashMap::new(),
            docs: DashMap::new(),
            total_tokens: std::sync::atomic::AtomicU64::new(0),
            scorer,
        }
    }

    /// Tokenize text into lowercase terms.
    fn tokenize(text: &str) -> Vec<String> {
        text.split_whitespace().map(|t| t.to_lowercase()).collect()
    }

    /// Add a document to the index.
    pub fn add_document(&self, doc_id: &str, text: &str) {
        // Remove existing document if present
        if self.docs.contains_key(doc_id) {
            self.remove_document(doc_id);
        }

        let tokens = Self::tokenize(text);
        let doc_len = tokens.len() as u64;

        // Count term frequencies
        let mut tf_map: HashMap<String, u32> = HashMap::new();
        for token in &tokens {
            *tf_map.entry(token.clone()).or_insert(0) += 1;
        }

        let terms: Vec<String> = tf_map.keys().cloned().collect();

        // Insert postings
        for (term, tf) in &tf_map {
            self.postings
                .entry(term.clone())
                .or_default()
                .push(Posting {
                    doc_id: doc_id.to_string(),
                    tf: *tf,
                });
        }

        // Store doc info
        self.docs.insert(
            doc_id.to_string(),
            DocInfo {
                len: doc_len,
                terms,
            },
        );

        self.total_tokens
            .fetch_add(doc_len, std::sync::atomic::Ordering::Relaxed);
    }

    /// Remove a document from the index.
    pub fn remove_document(&self, doc_id: &str) {
        if let Some((_, info)) = self.docs.remove(doc_id) {
            for term in &info.terms {
                if let Some(mut postings) = self.postings.get_mut(term) {
                    postings.retain(|p| p.doc_id != doc_id);
                }
            }
            self.total_tokens
                .fetch_sub(info.len, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Search the index and return BM25-ranked results.
    pub fn search(&self, query: &str, top_k: usize) -> Vec<ScoredDoc> {
        let query_terms = Self::tokenize(query);
        let total_docs = self.docs.len() as u64;
        let total_tokens = self.total_tokens.load(std::sync::atomic::Ordering::Relaxed);
        let avg_doc_len = if total_docs > 0 {
            total_tokens as f64 / total_docs as f64
        } else {
            1.0
        };

        // Accumulate scores per document
        let mut scores: HashMap<String, f64> = HashMap::new();

        for term in &query_terms {
            if let Some(postings) = self.postings.get(term) {
                let df = postings.len() as u64;
                for posting in postings.iter() {
                    let doc_len = self.docs.get(&posting.doc_id).map(|d| d.len).unwrap_or(1);

                    let term_score =
                        self.scorer
                            .score(posting.tf as f64, df, doc_len, avg_doc_len, total_docs);

                    *scores.entry(posting.doc_id.clone()).or_insert(0.0) += term_score;
                }
            }
        }

        // Sort by score descending and take top_k
        let mut results: Vec<ScoredDoc> = scores
            .into_iter()
            .map(|(doc_id, score)| ScoredDoc { doc_id, score })
            .collect();

        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(top_k);
        results
    }

    /// Get the number of indexed documents.
    pub fn len(&self) -> usize {
        self.docs.len()
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_and_search() {
        let index = Bm25Index::default();
        index.add_document("doc1", "the quick brown fox jumps over the lazy dog");
        index.add_document("doc2", "the quick brown cat sits on the mat");
        index.add_document("doc3", "a fox runs through the forest");

        let results = index.search("quick fox", 10);
        assert!(!results.is_empty());
        // doc1 has both "quick" and "fox", should rank highest
        assert_eq!(results[0].doc_id, "doc1");
    }

    #[test]
    fn test_remove_document() {
        let index = Bm25Index::default();
        index.add_document("doc1", "hello world");
        assert_eq!(index.len(), 1);

        index.remove_document("doc1");
        assert_eq!(index.len(), 0);

        let results = index.search("hello", 10);
        assert!(results.is_empty());
    }

    #[test]
    fn test_empty_search() {
        let index = Bm25Index::default();
        let results = index.search("anything", 10);
        assert!(results.is_empty());
    }

    #[test]
    fn test_duplicate_add() {
        let index = Bm25Index::default();
        index.add_document("doc1", "hello world");
        index.add_document("doc1", "goodbye world");
        assert_eq!(index.len(), 1);
    }
}
