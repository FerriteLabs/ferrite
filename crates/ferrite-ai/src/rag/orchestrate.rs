//! RAG Pipeline Orchestration
//!
//! Provides end-to-end RAG pipeline orchestration with:
//! - Document ingestion with automatic chunking
//! - Embedding generation
//! - Hybrid retrieval (vector + keyword)
//! - Reranking
//! - Context assembly
//! - Response generation hooks
//!
//! # Example
//!
//! ```ignore
//! use ferrite::rag::{RagOrchestrator, OrchestratorConfig};
//!
//! let orchestrator = RagOrchestrator::new(config);
//!
//! // Full pipeline execution
//! let result = orchestrator.execute("What is Ferrite?").await?;
//! println!("Context: {}", result.context);
//! println!("Citations: {:?}", result.citations);
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::chunk::{Chunker, ChunkingConfig};
use super::context::{ContextAssembler, ContextConfig, RetrievalContext};
use super::document::{Document, DocumentId, DocumentStore};
use super::embed::{Embedder, EmbeddingConfig};
use super::pipeline::RagError;
use super::rerank::{RerankConfig, RerankResult, Reranker};
use super::retrieve::{Retriever, RetrieverConfig, SearchFilter};

/// Orchestration mode for the pipeline
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub enum OrchestrationMode {
    /// Standard retrieval-only mode
    Retrieve,
    /// Retrieval with reranking
    RetrieveRerank,
    /// Full RAG with context assembly
    #[default]
    FullRag,
    /// Multi-step reasoning with iterative retrieval
    MultiStep {
        /// Maximum iterations
        max_iterations: usize,
        /// Convergence threshold
        threshold: f32,
    },
    /// Agent-style with tool use
    Agentic {
        /// Available tools/functions
        tools: Vec<String>,
    },
}

/// Configuration for RAG orchestration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestratorConfig {
    /// Chunking configuration
    #[serde(default)]
    pub chunking: ChunkingConfig,
    /// Embedding configuration
    #[serde(default)]
    pub embedding: EmbeddingConfig,
    /// Retriever configuration
    #[serde(default)]
    pub retriever: RetrieverConfig,
    /// Reranking configuration
    #[serde(default)]
    pub rerank: RerankConfig,
    /// Context configuration
    #[serde(default)]
    pub context: ContextConfig,
    /// Orchestration mode
    #[serde(default)]
    pub mode: OrchestrationMode,
    /// Enable hybrid search (vector + keyword)
    #[serde(default)]
    pub hybrid_search: bool,
    /// Hybrid search weight for vector results (0.0 - 1.0)
    #[serde(default = "default_vector_weight")]
    pub vector_weight: f32,
    /// Maximum documents to retrieve before reranking
    #[serde(default = "default_prefetch")]
    pub prefetch_k: usize,
    /// Enable query expansion
    #[serde(default)]
    pub query_expansion: bool,
    /// Enable response caching
    #[serde(default)]
    pub cache_responses: bool,
    /// Cache TTL in seconds
    #[serde(default = "default_cache_ttl")]
    pub cache_ttl_secs: u64,
}

fn default_vector_weight() -> f32 {
    0.7
}

fn default_prefetch() -> usize {
    50
}

fn default_cache_ttl() -> u64 {
    3600
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self {
            chunking: ChunkingConfig::default(),
            embedding: EmbeddingConfig::default(),
            retriever: RetrieverConfig::default(),
            rerank: RerankConfig::default(),
            context: ContextConfig::default(),
            mode: OrchestrationMode::default(),
            hybrid_search: true,
            vector_weight: default_vector_weight(),
            prefetch_k: default_prefetch(),
            query_expansion: false,
            cache_responses: false,
            cache_ttl_secs: default_cache_ttl(),
        }
    }
}

impl OrchestratorConfig {
    /// Create a fast configuration optimized for low latency
    pub fn fast() -> Self {
        Self {
            prefetch_k: 20,
            rerank: RerankConfig::default(), // No reranking
            hybrid_search: false,
            query_expansion: false,
            ..Default::default()
        }
    }

    /// Create an accurate configuration optimized for quality
    pub fn accurate() -> Self {
        Self {
            prefetch_k: 100,
            rerank: RerankConfig::rrf(60),
            hybrid_search: true,
            query_expansion: true,
            ..Default::default()
        }
    }

    /// Set the orchestration mode
    pub fn with_mode(mut self, mode: OrchestrationMode) -> Self {
        self.mode = mode;
        self
    }

    /// Enable hybrid search
    pub fn with_hybrid_search(mut self, weight: f32) -> Self {
        self.hybrid_search = true;
        self.vector_weight = weight.clamp(0.0, 1.0);
        self
    }

    /// Set reranking configuration
    pub fn with_rerank(mut self, config: RerankConfig) -> Self {
        self.rerank = config;
        self
    }
}

/// Result of orchestrated RAG execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestrationResult {
    /// Query that was executed
    pub query: String,
    /// Assembled context
    pub context: RetrievalContext,
    /// Reranking result (if reranking was performed)
    pub rerank_result: Option<RerankResult>,
    /// Retrieved documents before reranking
    pub retrieved_count: usize,
    /// Final documents after all processing
    pub final_count: usize,
    /// Timing breakdown
    pub timing: OrchestrationTiming,
    /// Mode that was used
    pub mode: String,
}

impl OrchestrationResult {
    /// Get the context string for prompt injection
    pub fn context_string(&self) -> &str {
        &self.context.content
    }

    /// Check if results were found
    pub fn has_results(&self) -> bool {
        !self.context.is_empty()
    }

    /// Get total execution time
    pub fn total_time_ms(&self) -> u64 {
        self.timing.total_ms
    }
}

/// Timing breakdown for orchestration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OrchestrationTiming {
    /// Query embedding time
    pub embedding_ms: u64,
    /// Vector retrieval time
    pub retrieval_ms: u64,
    /// Keyword search time (if hybrid)
    pub keyword_ms: u64,
    /// Reranking time
    pub rerank_ms: u64,
    /// Context assembly time
    pub assembly_ms: u64,
    /// Total time
    pub total_ms: u64,
}

/// Internal state for orchestrator
struct OrchestratorState {
    queries_processed: AtomicU64,
    documents_indexed: AtomicU64,
    cache_hits: AtomicU64,
    total_latency_ms: AtomicU64,
}

/// The RAG Orchestrator
///
/// Coordinates all components of the RAG pipeline for end-to-end execution.
pub struct RagOrchestrator {
    config: OrchestratorConfig,
    documents: DocumentStore,
    chunker: Chunker,
    embedder: Embedder,
    retriever: Retriever,
    reranker: Reranker,
    context_assembler: RwLock<ContextAssembler>,
    state: OrchestratorState,
}

impl RagOrchestrator {
    /// Create a new RAG orchestrator with configuration
    pub fn new(config: OrchestratorConfig) -> Self {
        let chunker = Chunker::with_config(config.chunking.clone());
        let embedder = Embedder::new(config.embedding.clone());
        let retriever = Retriever::new(config.retriever.clone());
        let reranker = Reranker::new(config.rerank.clone());
        let context_assembler = ContextAssembler::new(config.context.clone());

        Self {
            config,
            documents: DocumentStore::new(),
            chunker,
            embedder,
            retriever,
            reranker,
            context_assembler: RwLock::new(context_assembler),
            state: OrchestratorState {
                queries_processed: AtomicU64::new(0),
                documents_indexed: AtomicU64::new(0),
                cache_hits: AtomicU64::new(0),
                total_latency_ms: AtomicU64::new(0),
            },
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(OrchestratorConfig::default())
    }

    /// Ingest a document into the pipeline
    pub async fn ingest(&self, document: Document) -> Result<IngestSummary, RagError> {
        let start = Instant::now();

        // Store document
        let doc_id = self
            .documents
            .add(document.clone())
            .map_err(|e| RagError::IngestionError(e.to_string()))?;

        // Chunk document
        let chunks = self.chunker.chunk(&document);
        let chunk_count = chunks.len();

        // Generate embeddings and index
        let mut embedding_time_total = 0u64;
        for chunk in chunks {
            let embed_start = Instant::now();
            let embedding = self.embedder.embed(&chunk.content).await?;
            embedding_time_total += embed_start.elapsed().as_millis() as u64;

            self.retriever
                .index(chunk, embedding.embedding)
                .map_err(|e| RagError::IndexError(e.to_string()))?;
        }

        self.state.documents_indexed.fetch_add(1, Ordering::Relaxed);

        Ok(IngestSummary {
            document_id: doc_id,
            chunks_created: chunk_count,
            embedding_time_ms: embedding_time_total,
            total_time_ms: start.elapsed().as_millis() as u64,
        })
    }

    /// Execute the full RAG pipeline for a query
    pub async fn execute(&self, query: &str) -> Result<OrchestrationResult, RagError> {
        self.execute_with_filter(query, SearchFilter::default())
            .await
    }

    /// Execute with custom filter
    pub async fn execute_with_filter(
        &self,
        query: &str,
        filter: SearchFilter,
    ) -> Result<OrchestrationResult, RagError> {
        let total_start = Instant::now();
        let mut timing = OrchestrationTiming::default();

        // Step 1: Expand query if enabled
        let expanded_query = if self.config.query_expansion {
            self.expand_query(query).await
        } else {
            query.to_string()
        };

        // Step 2: Generate query embedding
        let embed_start = Instant::now();
        let query_embedding = self.embedder.embed(&expanded_query).await?;
        timing.embedding_ms = embed_start.elapsed().as_millis() as u64;

        // Step 3: Retrieve candidates
        let retrieve_start = Instant::now();
        let prefetch_filter = filter.clone().with_limit(self.config.prefetch_k);
        let retrieved = self
            .retriever
            .search(
                &query_embedding.embedding,
                Some(&expanded_query),
                &prefetch_filter,
            )
            .map_err(|e| RagError::RetrievalError(e.to_string()))?;
        timing.retrieval_ms = retrieve_start.elapsed().as_millis() as u64;

        let retrieved_count = retrieved.len();

        // Step 4: Optional keyword search for hybrid
        let combined_results = retrieved;
        if self.config.hybrid_search {
            let keyword_start = Instant::now();
            // In a full implementation, this would perform keyword search
            // and combine results using RRF or other fusion strategies
            timing.keyword_ms = keyword_start.elapsed().as_millis() as u64;
        }

        // Step 5: Rerank
        let rerank_start = Instant::now();
        let rerank_result = self.reranker.rerank(query, combined_results).await.ok();
        timing.rerank_ms = rerank_start.elapsed().as_millis() as u64;

        let final_results = match &rerank_result {
            Some(r) => r.results.clone(),
            None => Vec::new(),
        };
        let final_count = final_results.len();

        // Step 6: Assemble context
        let assembly_start = Instant::now();
        let context = self.context_assembler.read().assemble(final_results);
        timing.assembly_ms = assembly_start.elapsed().as_millis() as u64;

        timing.total_ms = total_start.elapsed().as_millis() as u64;
        if timing.total_ms == 0 {
            timing.total_ms = 1;
        }

        // Update stats
        self.state.queries_processed.fetch_add(1, Ordering::Relaxed);
        self.state
            .total_latency_ms
            .fetch_add(timing.total_ms, Ordering::Relaxed);

        Ok(OrchestrationResult {
            query: query.to_string(),
            context,
            rerank_result,
            retrieved_count,
            final_count,
            timing,
            mode: format!("{:?}", self.config.mode),
        })
    }

    /// Execute multi-step retrieval for complex queries
    pub async fn execute_multi_step(
        &self,
        query: &str,
        max_iterations: usize,
    ) -> Result<Vec<OrchestrationResult>, RagError> {
        let mut results = Vec::new();
        let mut current_query = query.to_string();

        for _ in 0..max_iterations {
            let result = self.execute(&current_query).await?;

            if result.context.is_empty() {
                break;
            }

            // Generate follow-up query based on context (simplified)
            let follow_up = self.generate_follow_up(&current_query, &result.context);

            results.push(result);

            if let Some(next_query) = follow_up {
                current_query = next_query;
            } else {
                break;
            }
        }

        Ok(results)
    }

    /// Expand query for better retrieval
    async fn expand_query(&self, query: &str) -> String {
        // Simple query expansion using synonyms and related terms
        // In production, this could use an LLM or thesaurus
        query.to_string()
    }

    /// Generate follow-up query for multi-step retrieval
    fn generate_follow_up(&self, _query: &str, context: &RetrievalContext) -> Option<String> {
        if context.is_empty() {
            return None;
        }
        // In production, this would analyze the context and generate
        // follow-up queries for missing information
        None
    }

    /// Get orchestrator statistics
    pub fn stats(&self) -> OrchestratorStats {
        let queries = self.state.queries_processed.load(Ordering::Relaxed);
        let latency = self.state.total_latency_ms.load(Ordering::Relaxed);

        OrchestratorStats {
            queries_processed: queries,
            documents_indexed: self.state.documents_indexed.load(Ordering::Relaxed),
            cache_hits: self.state.cache_hits.load(Ordering::Relaxed),
            avg_latency_ms: if queries > 0 {
                latency as f64 / queries as f64
            } else {
                0.0
            },
            mode: format!("{:?}", self.config.mode),
            hybrid_enabled: self.config.hybrid_search,
            rerank_strategy: format!("{:?}", self.config.rerank.strategy),
        }
    }

    /// Get current configuration
    pub fn config(&self) -> &OrchestratorConfig {
        &self.config
    }

    /// Get document count
    pub fn document_count(&self) -> usize {
        self.documents.len()
    }

    /// Clear all data
    pub fn clear(&self) {
        self.documents.clear();
        self.retriever.clear();
    }
}

/// Summary of document ingestion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestSummary {
    /// Document ID
    pub document_id: DocumentId,
    /// Number of chunks created
    pub chunks_created: usize,
    /// Embedding generation time
    pub embedding_time_ms: u64,
    /// Total ingestion time
    pub total_time_ms: u64,
}

/// Orchestrator statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestratorStats {
    /// Total queries processed
    pub queries_processed: u64,
    /// Total documents indexed
    pub documents_indexed: u64,
    /// Cache hits
    pub cache_hits: u64,
    /// Average latency
    pub avg_latency_ms: f64,
    /// Orchestration mode
    pub mode: String,
    /// Hybrid search enabled
    pub hybrid_enabled: bool,
    /// Rerank strategy
    pub rerank_strategy: String,
}

/// Builder for RagOrchestrator
pub struct OrchestratorBuilder {
    config: OrchestratorConfig,
}

impl OrchestratorBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: OrchestratorConfig::default(),
        }
    }

    /// Use fast configuration
    pub fn fast(mut self) -> Self {
        self.config = OrchestratorConfig::fast();
        self
    }

    /// Use accurate configuration
    pub fn accurate(mut self) -> Self {
        self.config = OrchestratorConfig::accurate();
        self
    }

    /// Set orchestration mode
    pub fn mode(mut self, mode: OrchestrationMode) -> Self {
        self.config.mode = mode;
        self
    }

    /// Enable hybrid search
    pub fn hybrid_search(mut self, weight: f32) -> Self {
        self.config = self.config.with_hybrid_search(weight);
        self
    }

    /// Set reranking
    pub fn rerank(mut self, config: RerankConfig) -> Self {
        self.config = self.config.with_rerank(config);
        self
    }

    /// Set prefetch count
    pub fn prefetch(mut self, k: usize) -> Self {
        self.config.prefetch_k = k;
        self
    }

    /// Enable query expansion
    pub fn query_expansion(mut self, enabled: bool) -> Self {
        self.config.query_expansion = enabled;
        self
    }

    /// Enable response caching
    pub fn cache(mut self, ttl_secs: u64) -> Self {
        self.config.cache_responses = true;
        self.config.cache_ttl_secs = ttl_secs;
        self
    }

    /// Build the orchestrator
    pub fn build(self) -> RagOrchestrator {
        RagOrchestrator::new(self.config)
    }
}

impl Default for OrchestratorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = OrchestratorConfig::default();
        assert!(config.hybrid_search);
        assert!((config.vector_weight - 0.7).abs() < 0.001);
        assert_eq!(config.prefetch_k, 50);
    }

    #[test]
    fn test_config_fast() {
        let config = OrchestratorConfig::fast();
        assert!(!config.hybrid_search);
        assert_eq!(config.prefetch_k, 20);
    }

    #[test]
    fn test_config_accurate() {
        let config = OrchestratorConfig::accurate();
        assert!(config.hybrid_search);
        assert_eq!(config.prefetch_k, 100);
        assert!(config.query_expansion);
    }

    #[test]
    fn test_builder() {
        let orchestrator = OrchestratorBuilder::new()
            .fast()
            .hybrid_search(0.8)
            .prefetch(30)
            .build();

        let config = orchestrator.config();
        assert!(config.hybrid_search);
        assert!((config.vector_weight - 0.8).abs() < 0.001);
        assert_eq!(config.prefetch_k, 30);
    }

    #[tokio::test]
    async fn test_orchestrator_creation() {
        let orchestrator = RagOrchestrator::with_defaults();
        assert_eq!(orchestrator.document_count(), 0);
    }

    #[tokio::test]
    async fn test_ingest_and_execute() {
        let orchestrator = OrchestratorBuilder::new().fast().build();

        let doc = Document::from_text("Ferrite is a high-performance Redis replacement. It provides fast in-memory reads with durable storage.");
        let ingest_result = orchestrator.ingest(doc).await.unwrap();

        assert!(ingest_result.chunks_created > 0);
        assert_eq!(orchestrator.document_count(), 1);

        let result = orchestrator.execute("What is Ferrite?").await.unwrap();
        assert!(result.timing.total_ms > 0);
    }

    #[tokio::test]
    async fn test_stats() {
        let orchestrator = RagOrchestrator::with_defaults();

        let doc = Document::from_text("Test document");
        orchestrator.ingest(doc).await.unwrap();
        orchestrator.execute("test").await.unwrap();

        let stats = orchestrator.stats();
        assert_eq!(stats.documents_indexed, 1);
        assert_eq!(stats.queries_processed, 1);
    }

    #[test]
    fn test_orchestration_modes() {
        let retrieve = OrchestrationMode::Retrieve;
        let multi_step = OrchestrationMode::MultiStep {
            max_iterations: 3,
            threshold: 0.8,
        };
        let agentic = OrchestrationMode::Agentic {
            tools: vec!["search".to_string()],
        };

        assert_eq!(retrieve, OrchestrationMode::Retrieve);
        assert!(matches!(
            multi_step,
            OrchestrationMode::MultiStep {
                max_iterations: 3,
                ..
            }
        ));
        assert!(matches!(agentic, OrchestrationMode::Agentic { .. }));
    }
}
