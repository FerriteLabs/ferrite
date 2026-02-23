#![forbid(unsafe_code)]
//! Multi-vector document support for hybrid retrieval.

use std::collections::HashMap;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

/// Strategy for combining scores across multiple vector spaces.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MultiVectorStrategy {
    /// Maximum similarity across all vector spaces
    MaxSim,
    /// Average similarity across all vector spaces
    AvgSim,
    /// Weighted sum of similarities per vector space
    WeightedSum(HashMap<String, f64>),
}

/// A scored result from multi-vector search.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScoredResult {
    /// Document identifier
    pub doc_id: String,
    /// Combined similarity score
    pub score: f64,
}

/// A document with multiple named vectors.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MultiVectorDoc {
    /// Document identifier
    pub doc_id: String,
    /// Named vectors (e.g., "title", "body", "image")
    pub vectors: HashMap<String, Vec<f32>>,
    /// Arbitrary metadata
    pub metadata: HashMap<String, String>,
}

/// Index that stores multiple named vectors per document.
pub struct MultiVectorIndex {
    /// Documents indexed by ID
    docs: DashMap<String, MultiVectorDoc>,
}

impl Default for MultiVectorIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl MultiVectorIndex {
    /// Create a new empty multi-vector index.
    pub fn new() -> Self {
        Self {
            docs: DashMap::new(),
        }
    }

    /// Add a document to the index.
    pub fn add(&self, doc: MultiVectorDoc) {
        self.docs.insert(doc.doc_id.clone(), doc);
    }

    /// Remove a document from the index.
    pub fn remove(&self, doc_id: &str) -> bool {
        self.docs.remove(doc_id).is_some()
    }

    /// Get the number of documents in the index.
    pub fn len(&self) -> usize {
        self.docs.len()
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }

    /// Search with multiple query vectors using the given strategy.
    pub fn search(
        &self,
        query_vectors: &HashMap<String, Vec<f32>>,
        strategy: &MultiVectorStrategy,
        top_k: usize,
    ) -> Vec<ScoredResult> {
        let mut results: Vec<ScoredResult> = self
            .docs
            .iter()
            .map(|entry| {
                let doc = entry.value();
                let score = self.compute_score(query_vectors, doc, strategy);
                ScoredResult {
                    doc_id: doc.doc_id.clone(),
                    score,
                }
            })
            .collect();

        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(top_k);
        results
    }

    /// Compute cosine similarity between two vectors.
    fn cosine_similarity(a: &[f32], b: &[f32]) -> f64 {
        if a.len() != b.len() || a.is_empty() {
            return 0.0;
        }

        let mut dot = 0.0f64;
        let mut norm_a = 0.0f64;
        let mut norm_b = 0.0f64;

        for (x, y) in a.iter().zip(b.iter()) {
            let xf = *x as f64;
            let yf = *y as f64;
            dot += xf * yf;
            norm_a += xf * xf;
            norm_b += yf * yf;
        }

        let denom = norm_a.sqrt() * norm_b.sqrt();
        if denom > 0.0 {
            dot / denom
        } else {
            0.0
        }
    }

    /// Compute combined score for a document given query vectors and strategy.
    fn compute_score(
        &self,
        query_vectors: &HashMap<String, Vec<f32>>,
        doc: &MultiVectorDoc,
        strategy: &MultiVectorStrategy,
    ) -> f64 {
        // Compute per-space similarities
        let similarities: Vec<(String, f64)> = query_vectors
            .iter()
            .filter_map(|(name, query_vec)| {
                doc.vectors
                    .get(name)
                    .map(|doc_vec| (name.clone(), Self::cosine_similarity(query_vec, doc_vec)))
            })
            .collect();

        if similarities.is_empty() {
            return 0.0;
        }

        match strategy {
            MultiVectorStrategy::MaxSim => similarities
                .iter()
                .map(|(_, s)| *s)
                .fold(f64::NEG_INFINITY, f64::max),
            MultiVectorStrategy::AvgSim => {
                let sum: f64 = similarities.iter().map(|(_, s)| *s).sum();
                sum / similarities.len() as f64
            }
            MultiVectorStrategy::WeightedSum(weights) => {
                let mut total = 0.0;
                let mut weight_sum = 0.0;
                for (name, sim) in &similarities {
                    let w = weights.get(name).copied().unwrap_or(1.0);
                    total += w * sim;
                    weight_sum += w;
                }
                if weight_sum > 0.0 {
                    total / weight_sum
                } else {
                    0.0
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_and_search() {
        let index = MultiVectorIndex::new();

        let doc = MultiVectorDoc {
            doc_id: "d1".to_string(),
            vectors: HashMap::from([
                ("title".to_string(), vec![1.0, 0.0, 0.0]),
                ("body".to_string(), vec![0.0, 1.0, 0.0]),
            ]),
            metadata: HashMap::new(),
        };
        index.add(doc);

        let query = HashMap::from([("title".to_string(), vec![1.0, 0.0, 0.0])]);
        let results = index.search(&query, &MultiVectorStrategy::MaxSim, 10);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, "d1");
        assert!((results[0].score - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_avg_sim() {
        let index = MultiVectorIndex::new();

        let doc = MultiVectorDoc {
            doc_id: "d1".to_string(),
            vectors: HashMap::from([
                ("a".to_string(), vec![1.0, 0.0]),
                ("b".to_string(), vec![0.0, 1.0]),
            ]),
            metadata: HashMap::new(),
        };
        index.add(doc);

        let query = HashMap::from([
            ("a".to_string(), vec![1.0, 0.0]),
            ("b".to_string(), vec![1.0, 0.0]),
        ]);
        let results = index.search(&query, &MultiVectorStrategy::AvgSim, 10);

        assert_eq!(results.len(), 1);
        // a: cosine(1,0 vs 1,0) = 1.0, b: cosine(1,0 vs 0,1) = 0.0 → avg = 0.5
        assert!((results[0].score - 0.5).abs() < 1e-6);
    }

    #[test]
    fn test_remove() {
        let index = MultiVectorIndex::new();
        index.add(MultiVectorDoc {
            doc_id: "d1".to_string(),
            vectors: HashMap::new(),
            metadata: HashMap::new(),
        });
        assert_eq!(index.len(), 1);
        assert!(index.remove("d1"));
        assert!(index.is_empty());
    }
}
