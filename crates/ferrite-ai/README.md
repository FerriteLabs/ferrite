# Ferrite AI

AI/ML features for Ferrite — vector similarity search, semantic caching, RAG pipelines, and embedding generation.

Part of the [Ferrite](https://github.com/ferritelabs/ferrite) workspace.

## Features

- **Vector similarity search** — HNSW, IVF, and flat indexes with cosine, Euclidean, dot-product, and Manhattan distance metrics
- **Semantic caching** — Cache LLM responses by meaning with cost tracking, circuit-breaker resilience, and genetic-algorithm optimization
- **RAG pipelines** — End-to-end retrieval-augmented generation with document chunking, embedding, retrieval, and re-ranking
- **Graph-enhanced RAG** — Knowledge-graph construction with entity extraction, relationship mapping, and graph-aware retrieval
- **Embedding generation** — ONNX runtime and HTTP-based model support with a model registry for versioning and A/B testing
- **ML inference** — Run models at the data layer with automatic batching and result caching
- **Agent memory** — Episodic, semantic, procedural, and working memory stores for AI agents with multi-agent sharing
- **Conversation store** — Session management with sliding/token-based context windows and automatic summarization

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
ferrite-ai = { git = "https://github.com/ferritelabs/ferrite" }
```

### Creating a vector index and searching

```rust,ignore
use ferrite_ai::vector::{VectorStore, VectorIndexConfig, VectorData, DistanceMetric};

fn main() {
    // Create a vector store with HNSW indexing
    let config = VectorIndexConfig {
        dimensions: 384,
        distance_metric: DistanceMetric::Cosine,
        ..Default::default()
    };
    let store = VectorStore::new(config);

    // Add vectors with metadata
    let doc_embedding = VectorData {
        id: "doc:1".into(),
        values: vec![0.1, 0.25, 0.8, /* ... 384 dims */],
        metadata: serde_json::json!({ "title": "Ferrite architecture" }),
    };
    store.upsert(doc_embedding).expect("insert failed");

    // Search for similar vectors
    let query = vec![0.12, 0.24, 0.79, /* ... */];
    let results = store.search(&query, 10).expect("search failed");

    for result in results {
        println!("id={}, score={:.4}", result.id, result.score);
    }
}
```

### Using the semantic cache

```rust,ignore
use ferrite_ai::semantic::{SemanticCacheBuilder};

#[tokio::main]
async fn main() {
    let cache = SemanticCacheBuilder::new()
        .similarity_threshold(0.92)
        .build()
        .expect("cache init failed");

    // Cache an LLM response
    cache.put("What is Ferrite?", "A high-performance key-value store.").await;

    // Retrieve by semantic similarity
    if let Some(hit) = cache.get("Tell me about Ferrite").await {
        println!("Cache hit: {}", hit);
    }
}
```

## Module Overview

| Module | Description |
|--------|-------------|
| `vector` | `VectorStore`, `HnswIndex`, `IvfIndex`, `FlatIndex`, `DistanceMetric`, `FilterCondition` — Vector similarity search with multiple index backends |
| `semantic` | `SemanticCache`, `SemanticCacheBuilder`, `LlmCache`, `FunctionCache`, `ResilientEmbedder` — Meaning-aware caching with cost tracking |
| `rag` | `RagPipeline`, `RagOrchestrator`, `Document`, `Chunk`, `Retriever`, `Reranker` — Retrieval-augmented generation orchestration |
| `graphrag` | `KnowledgeGraph`, `GraphRagRetriever`, `Entity`, `Relationship` — Graph-enhanced RAG with entity extraction |
| `embedding` | `ModelRegistry`, `CustomEmbeddingModel`, `HttpEmbeddingModel`, `ModelManager` — Embedding model management with A/B testing |
| `inference` | `InferenceEngine`, `ModelRegistry`, `BatchManager`, `InferenceCache` — ML model serving with batching and caching |
| `agent_memory` | `AgentMemoryStore`, `MemoryEntry`, `MemoryType`, `MemorySharingHub` — Episodic/semantic/procedural memory for AI agents |
| `conversation` | `ConversationStore`, `Message`, `Conversation`, `ContextWindow`, `Summarizer` — Chat session management and summarization |

## License

Apache-2.0 — see [LICENSE](../../LICENSE) for details.
