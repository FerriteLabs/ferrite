//! GraphRAG Integration
//!
//! Combines graph traversals with vector search for enhanced RAG retrieval.
//! Uses knowledge graphs to improve context and relationships in retrieved documents.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                     GraphRAG Pipeline                        │
//! ├─────────────────────────────────────────────────────────────┤
//! │  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐ │
//! │  │ Vector   │   │  Graph   │   │  Entity  │   │ Context  │ │
//! │  │ Search   │──▶│ Traverse │──▶│  Link    │──▶│ Builder  │ │
//! │  └──────────┘   └──────────┘   └──────────┘   └──────────┘ │
//! │       │              │              │              │        │
//! │       ▼              ▼              ▼              ▼        │
//! │  Similarity    Relationships   Entity Graph   Rich Context │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Features
//!
//! - **Entity Extraction**: Extract entities from documents
//! - **Knowledge Graph**: Build graph of entity relationships
//! - **Hybrid Retrieval**: Combine vector similarity with graph traversal
//! - **Context Enrichment**: Add related entities to context
//!
//! # Example
//!
//! ```ignore
//! use ferrite::graphrag::{GraphRag, GraphRagConfig};
//!
//! let graphrag = GraphRag::new(GraphRagConfig::default());
//!
//! // Index a document with entity extraction
//! graphrag.index_document("doc1", "Apple Inc. released a new iPhone.")?;
//!
//! // Query with graph-enhanced retrieval
//! let results = graphrag.query("What products does Apple make?", 10)?;
//! ```

mod entity;
mod graph;
mod retriever;

pub use entity::{Entity, EntityExtractor, EntityType, ExtractorConfig};
pub use graph::{GraphConfig, KnowledgeGraph, RelationType, Relationship};
pub use retriever::{GraphRagResult, GraphRagRetriever, RetrieverConfig};

/// Error types for GraphRAG operations
#[derive(Debug, Clone, thiserror::Error)]
pub enum GraphRagError {
    /// Entity extraction failed
    #[error("entity extraction failed: {0}")]
    ExtractionError(String),

    /// Graph operation failed
    #[error("graph error: {0}")]
    GraphError(String),

    /// Vector search failed
    #[error("vector search error: {0}")]
    VectorError(String),

    /// Document not found
    #[error("document not found: {0}")]
    DocumentNotFound(String),

    /// Invalid configuration
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
}

/// Result type for GraphRAG operations
pub type Result<T> = std::result::Result<T, GraphRagError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = GraphRagError::DocumentNotFound("doc-123".to_string());
        assert!(err.to_string().contains("doc-123"));
    }
}
