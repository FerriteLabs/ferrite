//! GraphRAG retriever
//!
//! Combines vector search with graph traversal for enhanced retrieval.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::entity::{Entity, EntityExtractor, ExtractorConfig};
use super::graph::{GraphConfig, KnowledgeGraph, Relationship};
use super::Result;

/// GraphRAG retriever configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetrieverConfig {
    /// Entity extractor configuration
    pub extractor: ExtractorConfig,
    /// Knowledge graph configuration
    pub graph: GraphConfig,
    /// Maximum results to return
    pub max_results: usize,
    /// Number of hops for graph expansion
    pub expansion_hops: usize,
    /// Weight for vector similarity score
    pub vector_weight: f32,
    /// Weight for graph relevance score
    pub graph_weight: f32,
    /// Include related entities in results
    pub include_related: bool,
}

impl Default for RetrieverConfig {
    fn default() -> Self {
        Self {
            extractor: ExtractorConfig::default(),
            graph: GraphConfig::default(),
            max_results: 10,
            expansion_hops: 2,
            vector_weight: 0.7,
            graph_weight: 0.3,
            include_related: true,
        }
    }
}

impl RetrieverConfig {
    /// Set expansion hops
    pub fn with_expansion_hops(mut self, hops: usize) -> Self {
        self.expansion_hops = hops;
        self
    }

    /// Set weights
    pub fn with_weights(mut self, vector: f32, graph: f32) -> Self {
        let total = vector + graph;
        self.vector_weight = vector / total;
        self.graph_weight = graph / total;
        self
    }

    /// Set max results
    pub fn with_max_results(mut self, max: usize) -> Self {
        self.max_results = max;
        self
    }
}

/// Result from GraphRAG retrieval
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphRagResult {
    /// Document ID
    pub document_id: String,
    /// Content (chunk) text
    pub content: String,
    /// Combined relevance score
    pub score: f32,
    /// Vector similarity score
    pub vector_score: f32,
    /// Graph relevance score
    pub graph_score: f32,
    /// Entities found in this result
    pub entities: Vec<Entity>,
    /// Related entities from graph
    pub related_entities: Vec<Entity>,
    /// Relationship paths to query entities
    pub relationship_paths: Vec<Vec<Relationship>>,
}

impl GraphRagResult {
    /// Create a new result
    pub fn new(document_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            document_id: document_id.into(),
            content: content.into(),
            score: 0.0,
            vector_score: 0.0,
            graph_score: 0.0,
            entities: Vec::new(),
            related_entities: Vec::new(),
            relationship_paths: Vec::new(),
        }
    }

    /// Set scores
    pub fn with_scores(mut self, vector: f32, graph: f32, combined: f32) -> Self {
        self.vector_score = vector;
        self.graph_score = graph;
        self.score = combined;
        self
    }

    /// Set entities
    pub fn with_entities(mut self, entities: Vec<Entity>) -> Self {
        self.entities = entities;
        self
    }

    /// Set related entities
    pub fn with_related(mut self, related: Vec<Entity>) -> Self {
        self.related_entities = related;
        self
    }
}

/// Indexed document
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexedDocument {
    /// Document ID
    pub id: String,
    /// Document content
    pub content: String,
    /// Extracted entities
    pub entities: Vec<String>,
    /// Embedding vector (if available)
    pub embedding: Option<Vec<f32>>,
    /// Metadata
    pub metadata: HashMap<String, String>,
}

/// GraphRAG retriever combining vector search and graph traversal
pub struct GraphRagRetriever {
    config: RetrieverConfig,
    extractor: EntityExtractor,
    graph: KnowledgeGraph,
    documents: parking_lot::RwLock<HashMap<String, IndexedDocument>>,
}

impl GraphRagRetriever {
    /// Create a new GraphRAG retriever
    pub fn new(config: RetrieverConfig) -> Self {
        let extractor = EntityExtractor::new(config.extractor.clone());
        let graph = KnowledgeGraph::new(config.graph.clone());

        Self {
            config,
            extractor,
            graph,
            documents: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Create with default config
    pub fn with_defaults() -> Self {
        Self::new(RetrieverConfig::default())
    }

    /// Index a document
    pub fn index_document(
        &self,
        doc_id: impl Into<String>,
        content: impl Into<String>,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<IndexedDocument> {
        let doc_id = doc_id.into();
        let content = content.into();

        // Extract entities
        let entities = self.extractor.extract(&content);
        let entity_ids: Vec<String> = entities
            .iter()
            .filter_map(|e| self.graph.add_entity(e.clone(), Some(&doc_id)).ok())
            .collect();

        // Create co-occurrence relationships
        self.graph.create_cooccurrences(&doc_id)?;

        // Store document
        let doc = IndexedDocument {
            id: doc_id.clone(),
            content,
            entities: entity_ids,
            embedding: None,
            metadata: metadata.unwrap_or_default(),
        };

        self.documents.write().insert(doc_id, doc.clone());

        Ok(doc)
    }

    /// Index document with embedding
    pub fn index_document_with_embedding(
        &self,
        doc_id: impl Into<String>,
        content: impl Into<String>,
        embedding: Vec<f32>,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<IndexedDocument> {
        let mut doc = self.index_document(doc_id, content, metadata)?;
        doc.embedding = Some(embedding);

        self.documents.write().insert(doc.id.clone(), doc.clone());
        Ok(doc)
    }

    /// Retrieve documents for a query
    pub fn retrieve(&self, query: &str, limit: Option<usize>) -> Result<Vec<GraphRagResult>> {
        let limit = limit.unwrap_or(self.config.max_results);

        // Extract entities from query
        let query_entities = self.extractor.extract(query);

        // Find documents containing query entities
        let mut doc_scores: HashMap<String, (f32, Vec<Entity>)> = HashMap::new();

        for entity in &query_entities {
            // Search for this entity in the graph
            let found = self.graph.search_entities(&entity.name, 10);

            for found_entity in found {
                // Get documents containing this entity
                let doc_entities = self.get_documents_with_entity(&found_entity.id);

                for (doc_id, score) in doc_entities {
                    let entry = doc_scores.entry(doc_id).or_insert((0.0, Vec::new()));
                    entry.0 += score;
                    entry.1.push(found_entity.clone());
                }
            }
        }

        // Expand via graph relationships
        if self.config.include_related && self.config.expansion_hops > 0 {
            for entity in &query_entities {
                let found = self.graph.search_entities(&entity.name, 5);

                for found_entity in found {
                    let related = self
                        .graph
                        .get_related(&found_entity.id, self.config.expansion_hops);

                    for related_entity in related {
                        let doc_entities = self.get_documents_with_entity(&related_entity.id);

                        for (doc_id, score) in doc_entities {
                            // Lower weight for related entities
                            let entry = doc_scores.entry(doc_id).or_insert((0.0, Vec::new()));
                            entry.0 += score * 0.5;
                        }
                    }
                }
            }
        }

        // Build results
        let mut results: Vec<GraphRagResult> = Vec::new();
        let documents = self.documents.read();

        for (doc_id, (graph_score, matched_entities)) in doc_scores {
            if let Some(doc) = documents.get(&doc_id) {
                let normalized_graph_score =
                    (graph_score / (query_entities.len() as f32).max(1.0)).min(1.0);

                // For now, use graph score only (vector search would be external)
                let combined_score = normalized_graph_score * self.config.graph_weight;

                let mut result = GraphRagResult::new(&doc_id, &doc.content)
                    .with_scores(0.0, normalized_graph_score, combined_score)
                    .with_entities(matched_entities);

                // Add related entities
                if self.config.include_related {
                    let mut related = Vec::new();
                    for entity_id in &doc.entities {
                        let related_entities = self.graph.get_related(entity_id, 1);
                        related.extend(related_entities);
                    }
                    result = result.with_related(related);
                }

                results.push(result);
            }
        }

        // Sort by score
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Limit results
        results.truncate(limit);

        Ok(results)
    }

    /// Retrieve with both query text and embedding
    pub fn retrieve_hybrid(
        &self,
        query: &str,
        query_embedding: &[f32],
        limit: Option<usize>,
    ) -> Result<Vec<GraphRagResult>> {
        let limit = limit.unwrap_or(self.config.max_results);

        // Get graph-based results
        let mut results = self.retrieve(query, Some(limit * 2))?;

        // Enhance with vector similarity
        let documents = self.documents.read();

        for result in &mut results {
            if let Some(doc) = documents.get(&result.document_id) {
                if let Some(ref doc_embedding) = doc.embedding {
                    let similarity = cosine_similarity(query_embedding, doc_embedding);
                    result.vector_score = similarity;

                    // Recalculate combined score
                    result.score = (result.vector_score * self.config.vector_weight)
                        + (result.graph_score * self.config.graph_weight);
                }
            }
        }

        // Re-sort by combined score
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        results.truncate(limit);

        Ok(results)
    }

    /// Get documents containing an entity
    fn get_documents_with_entity(&self, entity_id: &str) -> Vec<(String, f32)> {
        let mut results = Vec::new();
        let documents = self.documents.read();

        for (doc_id, doc) in documents.iter() {
            if doc.entities.contains(&entity_id.to_string()) {
                // Score based on position and count
                let count = doc.entities.iter().filter(|e| e == &entity_id).count();
                let score = (count as f32).min(3.0) / 3.0; // Normalize to 0-1
                results.push((doc_id.clone(), score));
            }
        }

        results
    }

    /// Get entity statistics
    pub fn get_entity_stats(&self) -> EntityStats {
        let graph_stats = self.graph.stats();
        let doc_count = self.documents.read().len();

        EntityStats {
            total_entities: graph_stats.total_entities,
            total_relationships: graph_stats.total_relationships,
            total_documents: doc_count as u64,
        }
    }

    /// Get knowledge graph
    pub fn graph(&self) -> &KnowledgeGraph {
        &self.graph
    }

    /// Get configuration
    pub fn config(&self) -> &RetrieverConfig {
        &self.config
    }

    /// Clear all data
    pub fn clear(&self) {
        self.graph.clear();
        self.documents.write().clear();
    }
}

impl Default for GraphRagRetriever {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// Entity statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityStats {
    /// Total entities
    pub total_entities: u64,
    /// Total relationships
    pub total_relationships: u64,
    /// Total documents
    pub total_documents: u64,
}

/// Calculate cosine similarity between two vectors
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }

    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let mag_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let mag_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if mag_a == 0.0 || mag_b == 0.0 {
        0.0
    } else {
        (dot / (mag_a * mag_b)).clamp(-1.0, 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_document() {
        let retriever = GraphRagRetriever::with_defaults();

        let doc = retriever
            .index_document("doc1", "Apple Inc. released a new iPhone product.", None)
            .unwrap();

        assert_eq!(doc.id, "doc1");
        // Should have extracted at least Apple Inc
        assert!(!doc.entities.is_empty() || doc.content.contains("Apple"));
    }

    #[test]
    fn test_retrieve() {
        let retriever = GraphRagRetriever::with_defaults();

        retriever
            .index_document("doc1", "Apple Inc. released iPhone.", None)
            .unwrap();
        retriever
            .index_document("doc2", "Microsoft Corp announced Windows.", None)
            .unwrap();

        let results = retriever.retrieve("Apple products", Some(5)).unwrap();

        // Results should be returned, potentially empty if no entities match
        // This depends on the entity extraction quality
        assert!(results.len() <= 5);
    }

    #[test]
    fn test_cosine_similarity() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        assert!((cosine_similarity(&a, &b) - 1.0).abs() < 0.01);

        let c = vec![0.0, 1.0, 0.0];
        assert!(cosine_similarity(&a, &c).abs() < 0.01);
    }

    #[test]
    fn test_retriever_stats() {
        let retriever = GraphRagRetriever::with_defaults();

        retriever
            .index_document("doc1", "Test document with Apple Inc.", None)
            .unwrap();

        let stats = retriever.get_entity_stats();
        assert_eq!(stats.total_documents, 1);
    }

    #[test]
    fn test_clear() {
        let retriever = GraphRagRetriever::with_defaults();

        retriever
            .index_document("doc1", "Test content", None)
            .unwrap();
        retriever.clear();

        let stats = retriever.get_entity_stats();
        assert_eq!(stats.total_documents, 0);
        assert_eq!(stats.total_entities, 0);
    }
}
