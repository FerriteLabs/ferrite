//! RAG (Retrieval-Augmented Generation) Pipeline
//!
//! Built-in RAG pipeline for AI-powered applications. Provides document
//! ingestion, chunking, embedding, retrieval, and context management.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                        RAG Pipeline                              │
//! ├─────────────────────────────────────────────────────────────────┤
//! │                                                                  │
//! │  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐      │
//! │  │ Document │──▶│ Chunker  │──▶│ Embedder │──▶│ Vector   │      │
//! │  │ Ingester │   │          │   │          │   │ Store    │      │
//! │  └──────────┘   └──────────┘   └──────────┘   └──────────┘      │
//! │                                                    │             │
//! │  ┌──────────┐   ┌──────────┐   ┌──────────┐       ▼             │
//! │  │ Response │◀──│ Reranker │◀──│ Retriever│◀─────────           │
//! │  │ Builder  │   │ (opt)    │   │          │                     │
//! │  └──────────┘   └──────────┘   └──────────┘                     │
//! │                                                                  │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Features
//!
//! - **Multiple Chunking Strategies**: Fixed-size, sentence, semantic
//! - **Embedding Providers**: OpenAI, local transformers, custom
//! - **Hybrid Search**: Combine vector similarity with keyword matching
//! - **Context Window Management**: Smart context assembly for LLMs
//! - **Metadata Filtering**: Filter results by document attributes
//!
//! # Example
//!
//! ```ignore
//! use ferrite::rag::{RagPipeline, Document, ChunkingStrategy};
//!
//! let pipeline = RagPipeline::new(config);
//!
//! // Ingest documents
//! pipeline.ingest(&Document::from_text("my content")).await?;
//!
//! // Query with RAG
//! let context = pipeline.retrieve("What is...?", 5).await?;
//! let response = pipeline.generate_response("What is...?", &context).await?;
//! ```

mod chunk;
mod context;
mod document;
mod embed;
mod orchestrate;
mod pipeline;
mod rerank;
mod retrieve;

pub use chunk::{Chunk, ChunkId, ChunkMetadata, Chunker, ChunkingConfig, ChunkingStrategy};
pub use context::{ContextAssembler, ContextConfig, RetrievalContext};
pub use document::{Document, DocumentId, DocumentMetadata, DocumentStore};
pub use embed::{Embedder, EmbeddingConfig, EmbeddingProviderType, EmbeddingResult};
pub use orchestrate::{
    IngestSummary, OrchestrationMode, OrchestrationResult, OrchestrationTiming,
    OrchestratorBuilder, OrchestratorConfig, OrchestratorStats, RagOrchestrator,
};
pub use pipeline::{RagConfig, RagPipeline, RagResult};
pub use rerank::{RerankConfig, RerankError, RerankResult, RerankStats, RerankStrategy, Reranker};
pub use retrieve::{RetrievalResult, Retriever, SearchFilter};

use serde::{Deserialize, Serialize};

/// RAG pipeline statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RagStats {
    /// Total documents ingested
    pub documents_ingested: u64,
    /// Total chunks created
    pub chunks_created: u64,
    /// Total embeddings generated
    pub embeddings_generated: u64,
    /// Total queries processed
    pub queries_processed: u64,
    /// Average retrieval latency (ms)
    pub avg_retrieval_latency_ms: f64,
    /// Average embedding latency (ms)
    pub avg_embedding_latency_ms: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rag_stats_default() {
        let stats = RagStats::default();
        assert_eq!(stats.documents_ingested, 0);
        assert_eq!(stats.chunks_created, 0);
    }
}
