//! Embedding-based semantic knowledge retrieval.
//!
//! Stores knowledge entries with vector embeddings for similarity search,
//! supporting categorized storage and confidence-based ranking.

use std::time::SystemTime;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

/// A knowledge entry with an associated embedding vector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticEntry {
    /// Unique identifier.
    pub id: String,
    /// The knowledge content.
    pub content: String,
    /// Embedding vector for similarity search.
    pub embedding: Vec<f32>,
    /// Category (e.g., "fact", "preference", "concept").
    pub category: String,
    /// Confidence in this knowledge (0.0 - 1.0).
    pub confidence: f64,
    /// Where this knowledge originated.
    pub source: String,
    /// When this entry was created.
    pub created_at: SystemTime,
    /// Number of times this knowledge has been reinforced.
    pub reinforcement_count: u64,
}

/// Result of a semantic similarity search.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticSearchResult {
    /// The matching entry.
    pub entry: SemanticEntry,
    /// Cosine similarity score (0.0 - 1.0).
    pub similarity: f64,
}

// ---------------------------------------------------------------------------
// Cosine similarity
// ---------------------------------------------------------------------------

/// Computes cosine similarity between two vectors.
/// Returns 0.0 if either vector has zero magnitude.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }

    let mut dot = 0.0f64;
    let mut mag_a = 0.0f64;
    let mut mag_b = 0.0f64;

    for (x, y) in a.iter().zip(b.iter()) {
        let x = *x as f64;
        let y = *y as f64;
        dot += x * y;
        mag_a += x * x;
        mag_b += y * y;
    }

    let denom = mag_a.sqrt() * mag_b.sqrt();
    if denom == 0.0 {
        0.0
    } else {
        (dot / denom).clamp(-1.0, 1.0)
    }
}

// ---------------------------------------------------------------------------
// SemanticMemoryStore
// ---------------------------------------------------------------------------

/// Embedding-based semantic memory store.
pub struct SemanticMemoryStore {
    entries: RwLock<Vec<SemanticEntry>>,
    embedding_dim: usize,
}

impl SemanticMemoryStore {
    /// Creates a new semantic memory store with the given embedding dimensionality.
    pub fn new(embedding_dim: usize) -> Self {
        Self {
            entries: RwLock::new(Vec::new()),
            embedding_dim,
        }
    }

    /// Stores a new knowledge entry. Returns the generated ID.
    pub fn store(
        &self,
        content: &str,
        embedding: Vec<f32>,
        category: &str,
        confidence: f64,
    ) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        let entry = SemanticEntry {
            id: id.clone(),
            content: content.to_string(),
            embedding,
            category: category.to_string(),
            confidence: confidence.clamp(0.0, 1.0),
            source: String::new(),
            created_at: SystemTime::now(),
            reinforcement_count: 0,
        };

        self.entries.write().push(entry);
        id
    }

    /// Searches for entries similar to the query embedding.
    /// Returns results with similarity >= threshold, sorted by similarity descending.
    pub fn search(
        &self,
        query_embedding: &[f32],
        limit: usize,
        threshold: f64,
    ) -> Vec<SemanticSearchResult> {
        let entries = self.entries.read();

        let mut results: Vec<SemanticSearchResult> = entries
            .iter()
            .map(|entry| {
                let similarity = cosine_similarity(query_embedding, &entry.embedding);
                SemanticSearchResult {
                    entry: entry.clone(),
                    similarity,
                }
            })
            .filter(|r| r.similarity >= threshold)
            .collect();

        results.sort_by(|a, b| {
            b.similarity
                .partial_cmp(&a.similarity)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(limit);
        results
    }

    /// Returns entries in a given category, sorted by confidence descending.
    pub fn search_by_category(&self, category: &str, limit: usize) -> Vec<SemanticEntry> {
        let entries = self.entries.read();

        let mut results: Vec<SemanticEntry> = entries
            .iter()
            .filter(|e| e.category == category)
            .cloned()
            .collect();

        results.sort_by(|a, b| {
            b.confidence
                .partial_cmp(&a.confidence)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(limit);
        results
    }

    /// Updates the confidence of an entry. Returns `true` if found.
    pub fn update_confidence(&self, id: &str, new_confidence: f64) -> bool {
        let mut entries = self.entries.write();
        if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
            entry.confidence = new_confidence.clamp(0.0, 1.0);
            entry.reinforcement_count += 1;
            true
        } else {
            false
        }
    }

    /// Removes an entry by ID. Returns `true` if found and removed.
    pub fn remove(&self, id: &str) -> bool {
        let mut entries = self.entries.write();
        let before = entries.len();
        entries.retain(|e| e.id != id);
        entries.len() < before
    }

    /// Returns the total number of entries.
    pub fn count(&self) -> usize {
        self.entries.read().len()
    }

    /// Returns the embedding dimensionality.
    pub fn embedding_dim(&self) -> usize {
        self.embedding_dim
    }

    /// Returns categories with their entry counts, sorted by count descending.
    pub fn categories(&self) -> Vec<(String, usize)> {
        let entries = self.entries.read();
        let mut counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

        for entry in entries.iter() {
            *counts.entry(entry.category.clone()).or_insert(0) += 1;
        }

        let mut result: Vec<(String, usize)> = counts.into_iter().collect();
        result.sort_by(|a, b| b.1.cmp(&a.1));
        result
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_embedding(values: &[f32]) -> Vec<f32> {
        values.to_vec()
    }

    #[test]
    fn test_cosine_similarity_identical() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        let sim = cosine_similarity(&a, &b);
        assert!((sim - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_orthogonal() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        let sim = cosine_similarity(&a, &b);
        assert!(sim.abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_opposite() {
        let a = vec![1.0, 0.0];
        let b = vec![-1.0, 0.0];
        let sim = cosine_similarity(&a, &b);
        assert!((sim - (-1.0)).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_empty() {
        assert_eq!(cosine_similarity(&[], &[]), 0.0);
    }

    #[test]
    fn test_cosine_similarity_mismatched_length() {
        let a = vec![1.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        assert_eq!(cosine_similarity(&a, &b), 0.0);
    }

    #[test]
    fn test_store_and_search() {
        let store = SemanticMemoryStore::new(3);

        store.store(
            "Rust is fast",
            make_embedding(&[1.0, 0.0, 0.0]),
            "fact",
            0.9,
        );
        store.store(
            "Python is easy",
            make_embedding(&[0.0, 1.0, 0.0]),
            "fact",
            0.8,
        );
        store.store(
            "Rust has ownership",
            make_embedding(&[0.9, 0.1, 0.0]),
            "fact",
            0.85,
        );

        // Search for something close to "Rust is fast"
        let results = store.search(&[1.0, 0.0, 0.0], 2, 0.5);
        assert_eq!(results.len(), 2);
        assert!(results[0].entry.content.contains("Rust"));
        assert!(results[0].similarity > results[1].similarity);
    }

    #[test]
    fn test_search_with_threshold() {
        let store = SemanticMemoryStore::new(2);

        store.store("close", make_embedding(&[1.0, 0.0]), "fact", 0.9);
        store.store("far", make_embedding(&[0.0, 1.0]), "fact", 0.5);

        let results = store.search(&[1.0, 0.0], 10, 0.9);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].entry.content, "close");
    }

    #[test]
    fn test_search_by_category() {
        let store = SemanticMemoryStore::new(3);

        store.store("fact 1", make_embedding(&[1.0, 0.0, 0.0]), "fact", 0.9);
        store.store(
            "pref 1",
            make_embedding(&[0.0, 1.0, 0.0]),
            "preference",
            0.8,
        );
        store.store("fact 2", make_embedding(&[0.0, 0.0, 1.0]), "fact", 0.7);

        let facts = store.search_by_category("fact", 10);
        assert_eq!(facts.len(), 2);
        // Should be sorted by confidence desc
        assert!(facts[0].confidence >= facts[1].confidence);
    }

    #[test]
    fn test_update_confidence() {
        let store = SemanticMemoryStore::new(3);
        let id = store.store("test", make_embedding(&[1.0, 0.0, 0.0]), "fact", 0.5);

        assert!(store.update_confidence(&id, 0.9));
        let results = store.search_by_category("fact", 1);
        assert!((results[0].confidence - 0.9).abs() < 1e-6);
        assert_eq!(results[0].reinforcement_count, 1);

        assert!(!store.update_confidence("nonexistent", 0.5));
    }

    #[test]
    fn test_remove() {
        let store = SemanticMemoryStore::new(3);
        let id = store.store("to remove", make_embedding(&[1.0, 0.0, 0.0]), "fact", 0.5);

        assert_eq!(store.count(), 1);
        assert!(store.remove(&id));
        assert_eq!(store.count(), 0);
        assert!(!store.remove(&id)); // Already removed
    }

    #[test]
    fn test_categories() {
        let store = SemanticMemoryStore::new(3);
        store.store("f1", make_embedding(&[1.0, 0.0, 0.0]), "fact", 0.9);
        store.store("f2", make_embedding(&[0.0, 1.0, 0.0]), "fact", 0.8);
        store.store("p1", make_embedding(&[0.0, 0.0, 1.0]), "preference", 0.7);

        let cats = store.categories();
        assert_eq!(cats.len(), 2);
        assert_eq!(cats[0].0, "fact");
        assert_eq!(cats[0].1, 2);
    }

    #[test]
    fn test_confidence_clamping() {
        let store = SemanticMemoryStore::new(3);
        let id = store.store("test", make_embedding(&[1.0, 0.0, 0.0]), "fact", 1.5);

        let entries = store.search_by_category("fact", 1);
        assert!((entries[0].confidence - 1.0).abs() < 1e-6);

        store.update_confidence(&id, -0.5);
        let entries = store.search_by_category("fact", 1);
        assert!((entries[0].confidence - 0.0).abs() < 1e-6);
    }
}
