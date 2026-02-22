//! RAG Pipeline
//!
//! Orchestrates the complete RAG workflow: ingest, embed, retrieve, and assemble context.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::chunk::{Chunker, ChunkingConfig, ChunkingStrategy};
use super::context::{ContextAssembler, ContextConfig, ContextFormat, RetrievalContext};
use super::document::{Document, DocumentId, DocumentStore};
use super::embed::{Embedder, EmbeddingConfig, EmbeddingProviderType};
use super::retrieve::{
    RetrievalResult, Retriever, RetrieverConfig, SearchFilter, SimilarityMetric,
};
use super::RagStats;

/// RAG pipeline configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RagConfig {
    /// Chunking configuration
    #[serde(default)]
    pub chunking: ChunkingConfig,
    /// Embedding configuration
    #[serde(default)]
    pub embedding: EmbeddingConfig,
    /// Retriever configuration
    #[serde(default)]
    pub retriever: RetrieverConfig,
    /// Context configuration
    #[serde(default)]
    pub context: ContextConfig,
    /// Enable async processing
    #[serde(default)]
    pub async_processing: bool,
    /// Batch size for embedding
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_batch_size() -> usize {
    32
}

impl Default for RagConfig {
    fn default() -> Self {
        Self {
            chunking: ChunkingConfig::default(),
            embedding: EmbeddingConfig::default(),
            retriever: RetrieverConfig::default(),
            context: ContextConfig::default(),
            async_processing: false,
            batch_size: default_batch_size(),
        }
    }
}

impl RagConfig {
    /// Create a config optimized for speed
    pub fn fast() -> Self {
        Self {
            chunking: ChunkingConfig {
                strategy: ChunkingStrategy::FixedSize {
                    size: 512,
                    overlap: 50,
                },
                ..Default::default()
            },
            retriever: RetrieverConfig {
                default_top_k: 3,
                ..Default::default()
            },
            context: ContextConfig {
                max_chunks: 5,
                ..Default::default()
            },
            batch_size: 64,
            ..Default::default()
        }
    }

    /// Create a config optimized for accuracy
    pub fn accurate() -> Self {
        Self {
            chunking: ChunkingConfig {
                strategy: ChunkingStrategy::Recursive {
                    target_size: 256,
                    separators: vec![
                        "\n\n".to_string(),
                        "\n".to_string(),
                        ". ".to_string(),
                        " ".to_string(),
                    ],
                },
                ..Default::default()
            },
            retriever: RetrieverConfig {
                default_top_k: 10,
                hybrid_search: true,
                ..Default::default()
            },
            context: ContextConfig {
                max_chunks: 10,
                dedup_threshold: 0.85,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Set the embedding dimension
    pub fn with_dimension(mut self, dimension: usize) -> Self {
        self.embedding.dimension = dimension;
        self.retriever.dimension = dimension;
        self
    }

    /// Set the embedding provider type
    pub fn with_provider(mut self, provider: EmbeddingProviderType) -> Self {
        self.embedding.provider = provider;
        self
    }
}

/// Result of a RAG query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RagResult {
    /// Retrieved context
    pub context: RetrievalContext,
    /// Query processing time (ms)
    pub query_time_ms: u64,
    /// Embedding time (ms)
    pub embedding_time_ms: u64,
    /// Retrieval time (ms)
    pub retrieval_time_ms: u64,
    /// Total time (ms)
    pub total_time_ms: u64,
}

impl RagResult {
    /// Check if results were found
    pub fn has_results(&self) -> bool {
        !self.context.is_empty()
    }

    /// Get the assembled context string
    pub fn context_string(&self) -> &str {
        &self.context.content
    }
}

/// Internal pipeline state (thread-safe counters)
struct PipelineState {
    /// Documents ingested
    documents_ingested: AtomicU64,
    /// Chunks created
    chunks_created: AtomicU64,
    /// Embeddings generated
    embeddings_generated: AtomicU64,
    /// Queries processed
    queries_processed: AtomicU64,
    /// Total embedding latency for averaging
    total_embedding_latency: AtomicU64,
    /// Total retrieval latency for averaging
    total_retrieval_latency: AtomicU64,
}

/// The main RAG pipeline
pub struct RagPipeline {
    /// Configuration
    config: RagConfig,
    /// Document store
    documents: Arc<DocumentStore>,
    /// Document chunker
    chunker: Chunker,
    /// Embedding generator
    embedder: Embedder,
    /// Vector retriever
    retriever: Retriever,
    /// Context assembler
    context_assembler: RwLock<ContextAssembler>,
    /// Internal state
    state: PipelineState,
}

impl RagPipeline {
    /// Create a new RAG pipeline
    pub fn new(config: RagConfig) -> Self {
        let chunker = Chunker::with_config(config.chunking.clone());
        let embedder = Embedder::new(config.embedding.clone());
        let retriever = Retriever::new(config.retriever.clone());
        let context_assembler = ContextAssembler::new(config.context.clone());

        Self {
            config,
            documents: Arc::new(DocumentStore::new()),
            chunker,
            embedder,
            retriever,
            context_assembler: RwLock::new(context_assembler),
            state: PipelineState {
                documents_ingested: AtomicU64::new(0),
                chunks_created: AtomicU64::new(0),
                embeddings_generated: AtomicU64::new(0),
                queries_processed: AtomicU64::new(0),
                total_embedding_latency: AtomicU64::new(0),
                total_retrieval_latency: AtomicU64::new(0),
            },
        }
    }

    /// Create with default configuration
    pub fn default_config() -> Self {
        Self::new(RagConfig::default())
    }

    /// Ingest a document into the pipeline
    pub async fn ingest(&self, document: Document) -> Result<IngestResult, RagError> {
        let start = Instant::now();

        // Store document
        let doc_id = self
            .documents
            .add(document.clone())
            .map_err(|e| RagError::IngestionError(format!("Failed to store document: {}", e)))?;

        // Chunk document
        let chunks = self.chunker.chunk(&document);
        let chunk_count = chunks.len();

        // Generate embeddings and index
        let mut embed_time_total = 0u64;
        for chunk in chunks {
            let embed_start = Instant::now();
            let embedding = self.embedder.embed(&chunk.content).await?;
            embed_time_total += embed_start.elapsed().as_millis() as u64;

            self.retriever
                .index(chunk, embedding.embedding)
                .map_err(|e| RagError::IndexError(format!("Failed to index chunk: {}", e)))?;
        }

        // Update stats
        self.state
            .documents_ingested
            .fetch_add(1, Ordering::Relaxed);
        self.state
            .chunks_created
            .fetch_add(chunk_count as u64, Ordering::Relaxed);
        self.state
            .embeddings_generated
            .fetch_add(chunk_count as u64, Ordering::Relaxed);

        let total_time = start.elapsed().as_millis() as u64;

        Ok(IngestResult {
            document_id: doc_id,
            chunks_created: chunk_count,
            embedding_time_ms: embed_time_total,
            total_time_ms: total_time,
        })
    }

    /// Ingest multiple documents
    pub async fn ingest_batch(
        &self,
        documents: Vec<Document>,
    ) -> Vec<Result<IngestResult, RagError>> {
        let mut results = Vec::with_capacity(documents.len());
        for doc in documents {
            results.push(self.ingest(doc).await);
        }
        results
    }

    /// Query the RAG pipeline
    pub async fn query(&self, query: &str) -> Result<RagResult, RagError> {
        self.query_with_filter(query, SearchFilter::default()).await
    }

    /// Query with custom filter
    pub async fn query_with_filter(
        &self,
        query: &str,
        filter: SearchFilter,
    ) -> Result<RagResult, RagError> {
        let start = Instant::now();

        // Generate query embedding
        let embed_start = Instant::now();
        let query_embedding = self.embedder.embed(query).await?;
        let embedding_time_ms = embed_start.elapsed().as_millis() as u64;

        // Retrieve relevant chunks
        let retrieve_start = Instant::now();
        let results = self
            .retriever
            .search(&query_embedding.embedding, Some(query), &filter)
            .map_err(|e| RagError::RetrievalError(format!("Search failed: {}", e)))?;
        let retrieval_time_ms = retrieve_start.elapsed().as_millis() as u64;

        // Assemble context
        let context = self.context_assembler.read().assemble(results);

        let total_time_ms = start.elapsed().as_millis() as u64;

        // Update stats
        self.state.queries_processed.fetch_add(1, Ordering::Relaxed);
        self.state
            .total_embedding_latency
            .fetch_add(embedding_time_ms, Ordering::Relaxed);
        self.state
            .total_retrieval_latency
            .fetch_add(retrieval_time_ms, Ordering::Relaxed);

        Ok(RagResult {
            context,
            query_time_ms: embedding_time_ms + retrieval_time_ms,
            embedding_time_ms,
            retrieval_time_ms,
            total_time_ms,
        })
    }

    /// Retrieve chunks without context assembly
    pub async fn retrieve(&self, query: &str, k: usize) -> Result<Vec<RetrievalResult>, RagError> {
        let query_embedding = self.embedder.embed(query).await?;
        let filter = SearchFilter::new().with_limit(k);

        self.retriever
            .search(&query_embedding.embedding, Some(query), &filter)
            .map_err(|e| RagError::RetrievalError(format!("Search failed: {}", e)))
    }

    /// Remove a document and its chunks
    pub fn remove_document(&self, doc_id: &DocumentId) -> bool {
        let removed_chunks = self.retriever.remove_document(doc_id);
        let removed_doc = self.documents.remove(doc_id).is_some();
        removed_doc || removed_chunks > 0
    }

    /// Get document by ID
    pub fn get_document(&self, doc_id: &DocumentId) -> Option<Document> {
        self.documents.get(doc_id)
    }

    /// List all document IDs
    pub fn list_documents(&self) -> Vec<DocumentId> {
        self.documents.list_ids()
    }

    /// Get pipeline statistics
    pub fn stats(&self) -> RagStats {
        let queries = self.state.queries_processed.load(Ordering::Relaxed);
        let avg_embed = if queries > 0 {
            self.state.total_embedding_latency.load(Ordering::Relaxed) as f64 / queries as f64
        } else {
            0.0
        };
        let avg_retrieve = if queries > 0 {
            self.state.total_retrieval_latency.load(Ordering::Relaxed) as f64 / queries as f64
        } else {
            0.0
        };

        RagStats {
            documents_ingested: self.state.documents_ingested.load(Ordering::Relaxed),
            chunks_created: self.state.chunks_created.load(Ordering::Relaxed),
            embeddings_generated: self.state.embeddings_generated.load(Ordering::Relaxed),
            queries_processed: queries,
            avg_embedding_latency_ms: avg_embed,
            avg_retrieval_latency_ms: avg_retrieve,
        }
    }

    /// Get current configuration
    pub fn config(&self) -> &RagConfig {
        &self.config
    }

    /// Update context configuration
    pub fn set_context_config(&self, config: ContextConfig) {
        self.context_assembler.write().set_config(config);
    }

    /// Get document count
    pub fn document_count(&self) -> usize {
        self.documents.len()
    }

    /// Get indexed chunk count
    pub fn chunk_count(&self) -> usize {
        self.retriever.len()
    }

    /// Clear all data
    pub fn clear(&self) {
        self.documents.clear();
        self.retriever.clear();
    }
}

/// Result of document ingestion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestResult {
    /// Document ID
    pub document_id: DocumentId,
    /// Number of chunks created
    pub chunks_created: usize,
    /// Embedding generation time (ms)
    pub embedding_time_ms: u64,
    /// Total ingestion time (ms)
    pub total_time_ms: u64,
}

/// RAG pipeline errors
#[derive(Debug, Clone, thiserror::Error)]
#[allow(clippy::enum_variant_names)]
pub enum RagError {
    /// Ingestion error
    #[error("ingestion error: {0}")]
    IngestionError(String),

    /// Embedding error
    #[error("embedding error: {0}")]
    EmbeddingError(String),

    /// Retrieval error
    #[error("retrieval error: {0}")]
    RetrievalError(String),

    /// Index error
    #[error("index error: {0}")]
    IndexError(String),

    /// Configuration error
    #[error("configuration error: {0}")]
    ConfigError(String),
}

impl From<super::embed::EmbeddingError> for RagError {
    fn from(e: super::embed::EmbeddingError) -> Self {
        RagError::EmbeddingError(e.to_string())
    }
}

/// Builder for creating a RAG pipeline
#[allow(dead_code)] // Planned for v0.2 — builder pattern for RAG pipeline configuration
pub struct RagPipelineBuilder {
    config: RagConfig,
}

impl RagPipelineBuilder {
    /// Create a new builder
    #[allow(dead_code)] // Planned for v0.2 — builder entry point
    pub fn new() -> Self {
        Self {
            config: RagConfig::default(),
        }
    }

    /// Use fast configuration
    #[allow(dead_code)] // Planned for v0.2 — builder preset for fast mode
    pub fn fast(mut self) -> Self {
        self.config = RagConfig::fast();
        self
    }

    /// Use accurate configuration
    #[allow(dead_code)] // Planned for v0.2 — builder preset for accurate mode
    pub fn accurate(mut self) -> Self {
        self.config = RagConfig::accurate();
        self
    }

    /// Set embedding dimension
    #[allow(dead_code)] // Planned for v0.2 — builder method for embedding dimension
    pub fn dimension(mut self, dim: usize) -> Self {
        self.config = self.config.with_dimension(dim);
        self
    }

    /// Set embedding provider type
    #[allow(dead_code)] // Planned for v0.2 — builder method for provider selection
    pub fn provider(mut self, provider: EmbeddingProviderType) -> Self {
        self.config = self.config.with_provider(provider);
        self
    }

    /// Set chunking strategy
    #[allow(dead_code)] // Planned for v0.2 — builder method for chunking strategy
    pub fn chunking(mut self, strategy: ChunkingStrategy) -> Self {
        self.config.chunking.strategy = strategy;
        self
    }

    /// Set chunk size
    #[allow(dead_code)] // Planned for v0.2 — builder method for chunk size
    pub fn chunk_size(mut self, size: usize) -> Self {
        self.config.chunking.strategy = ChunkingStrategy::FixedSize {
            size,
            overlap: size / 10,
        };
        self
    }

    /// Set similarity metric
    #[allow(dead_code)] // Planned for v0.2 — builder method for similarity metric
    pub fn similarity(mut self, metric: SimilarityMetric) -> Self {
        self.config.retriever.metric = metric;
        self
    }

    /// Set context format
    #[allow(dead_code)] // Planned for v0.2 — builder method for context format
    pub fn context_format(mut self, format: ContextFormat) -> Self {
        self.config.context.format = format;
        self
    }

    /// Set max tokens for context
    #[allow(dead_code)] // Planned for v0.2 — builder method for max context tokens
    pub fn max_tokens(mut self, tokens: usize) -> Self {
        self.config.context.max_tokens = tokens;
        self
    }

    /// Enable hybrid search
    #[allow(dead_code)] // Planned for v0.2 — builder method for hybrid search toggle
    pub fn hybrid_search(mut self, enabled: bool) -> Self {
        self.config.retriever.hybrid_search = enabled;
        self
    }

    /// Set default top-k results
    #[allow(dead_code)] // Planned for v0.2 — builder method for top-k results
    pub fn top_k(mut self, k: usize) -> Self {
        self.config.retriever.default_top_k = k;
        self
    }

    /// Set minimum chunk size
    #[allow(dead_code)] // Planned for v0.2 — builder method for minimum chunk size
    pub fn min_chunk_size(mut self, size: usize) -> Self {
        self.config.chunking.min_chunk_size = size;
        self
    }

    /// Build the pipeline
    #[allow(dead_code)] // Planned for v0.2 — builder finalizer
    pub fn build(self) -> RagPipeline {
        RagPipeline::new(self.config)
    }
}

impl Default for RagPipelineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rag::document::DocumentMetadata;

    #[test]
    fn test_pipeline_creation() {
        let pipeline = RagPipeline::default_config();
        assert_eq!(pipeline.document_count(), 0);
        assert_eq!(pipeline.chunk_count(), 0);
    }

    #[tokio::test]
    async fn test_document_ingestion() {
        let pipeline = RagPipelineBuilder::new()
            .dimension(384)
            .chunk_size(100)
            .build();

        let doc = Document::from_text(
            "This is a test document with some content that will be chunked and embedded.",
        );

        let result = pipeline.ingest(doc).await.unwrap();
        assert!(result.chunks_created > 0);
        assert!(pipeline.document_count() > 0);
        assert!(pipeline.chunk_count() > 0);
    }

    #[tokio::test]
    async fn test_query() {
        let pipeline = RagPipelineBuilder::new()
            .dimension(384)
            .chunk_size(50)
            .build();

        // Ingest a document
        let doc = Document::from_text("Ferrite is a high-performance Redis-compatible database.");
        pipeline.ingest(doc).await.unwrap();

        // Query
        let result = pipeline.query("What is Ferrite?").await.unwrap();
        assert!(result.has_results());
        assert!(!result.context_string().is_empty());
    }

    #[tokio::test]
    async fn test_batch_ingestion() {
        let pipeline = RagPipeline::default_config();

        let docs = vec![
            Document::from_text("First document content"),
            Document::from_text("Second document content"),
            Document::from_text("Third document content"),
        ];

        let results = pipeline.ingest_batch(docs).await;
        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r.is_ok()));
        assert_eq!(pipeline.document_count(), 3);
    }

    #[tokio::test]
    async fn test_document_removal() {
        let pipeline = RagPipeline::default_config();

        let doc = Document::from_text("Test content");
        let result = pipeline.ingest(doc).await.unwrap();

        assert_eq!(pipeline.document_count(), 1);

        let removed = pipeline.remove_document(&result.document_id);
        assert!(removed);
        assert_eq!(pipeline.document_count(), 0);
    }

    #[tokio::test]
    async fn test_retrieve() {
        let pipeline = RagPipelineBuilder::new()
            .dimension(384)
            .top_k(5)
            .min_chunk_size(10)
            .build();

        let doc = Document::from_text(
            "The quick brown fox jumps over the lazy dog. This is a test document for retrieval.",
        );
        pipeline.ingest(doc).await.unwrap();

        let results = pipeline.retrieve("fox jumping", 3).await.unwrap();
        assert!(!results.is_empty());
    }

    #[tokio::test]
    async fn test_statistics() {
        let pipeline = RagPipelineBuilder::new().min_chunk_size(10).build();

        let doc = Document::from_text(
            "Test content for statistics that is long enough to be chunked properly.",
        );
        pipeline.ingest(doc).await.unwrap();
        pipeline.query("test").await.unwrap();

        let stats = pipeline.stats();
        assert_eq!(stats.documents_ingested, 1);
        assert!(stats.chunks_created > 0);
        assert_eq!(stats.queries_processed, 1);
    }

    #[test]
    fn test_builder_configurations() {
        let fast = RagPipelineBuilder::new().fast().build();
        let accurate = RagPipelineBuilder::new().accurate().build();

        // Fast config should have smaller default top_k
        assert!(fast.config().retriever.default_top_k < accurate.config().retriever.default_top_k);
    }

    #[tokio::test]
    async fn test_clear() {
        let pipeline = RagPipelineBuilder::new().min_chunk_size(10).build();

        let doc = Document::from_text(
            "Test content that is long enough for chunking to work properly with default settings.",
        );
        pipeline.ingest(doc).await.unwrap();

        assert!(pipeline.document_count() > 0);
        assert!(pipeline.chunk_count() > 0);

        pipeline.clear();

        assert_eq!(pipeline.document_count(), 0);
        assert_eq!(pipeline.chunk_count(), 0);
    }

    #[tokio::test]
    async fn test_query_with_filter() {
        let pipeline = RagPipeline::default_config();

        let mut doc1 = Document::from_text("Important document about Rust");
        doc1 = doc1.with_metadata(DocumentMetadata::default().with_tag("important"));

        let doc2 = Document::from_text("Regular document about Python");

        pipeline.ingest(doc1).await.unwrap();
        pipeline.ingest(doc2).await.unwrap();

        // Query with tag filter
        let filter = SearchFilter::new().with_tags(vec!["important".to_string()]);
        let result = pipeline
            .query_with_filter("programming", filter)
            .await
            .unwrap();

        // Should only get results from tagged document
        if result.has_results() {
            for citation in &result.context.citations {
                // All results should be from the important document
                assert!(pipeline
                    .get_document(&citation.document_id)
                    .map(|d| d.metadata.tags.contains(&"important".to_string()))
                    .unwrap_or(false));
            }
        }
    }

    #[test]
    fn test_config_presets() {
        let fast = RagConfig::fast();
        let accurate = RagConfig::accurate();

        // Fast should have larger chunk size
        match &fast.chunking.strategy {
            ChunkingStrategy::FixedSize { size, .. } => {
                assert!(*size >= 512);
            }
            _ => {}
        }

        // Accurate should enable hybrid search
        assert!(accurate.retriever.hybrid_search);
    }

    #[tokio::test]
    async fn test_list_documents() {
        let pipeline = RagPipeline::default_config();

        let doc1 = Document::from_text("First");
        let doc2 = Document::from_text("Second");

        pipeline.ingest(doc1).await.unwrap();
        pipeline.ingest(doc2).await.unwrap();

        let ids = pipeline.list_documents();
        assert_eq!(ids.len(), 2);
    }

    #[tokio::test]
    async fn test_get_document() {
        let pipeline = RagPipeline::default_config();

        let doc = Document::from_text("Test content");
        let result = pipeline.ingest(doc).await.unwrap();

        let retrieved = pipeline.get_document(&result.document_id);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().content, "Test content");
    }

    #[tokio::test]
    async fn test_context_config_update() {
        let pipeline = RagPipeline::default_config();

        let new_config = ContextConfig {
            max_tokens: 2048,
            format: ContextFormat::Markdown,
            ..Default::default()
        };

        pipeline.set_context_config(new_config);

        // Verify the update took effect (would be visible in query results)
        let doc = Document::from_text("Test");
        pipeline.ingest(doc).await.unwrap();
        let result = pipeline.query("test").await.unwrap();

        // Markdown format includes ### headers
        if result.has_results() {
            assert!(result.context_string().contains("###"));
        }
    }
}
