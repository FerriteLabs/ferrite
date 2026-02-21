# Semantic Cache

Cache by meaning, not just exact keys — reduce LLM API costs by 40-60%.

> 🔬 **Status: Experimental** — Requires `--features cloud` build.

## Overview

Traditional caches require exact key matches. Ferrite's semantic cache understands that "What is the capital of France?" and "France's capital city?" are semantically equivalent.

## How It Works

```
User Query ──▶ Embed Query ──▶ Vector Search ──▶ Hit? ──▶ Return cached
                                                    │
                                                    ▼ Miss
                                              Call LLM API
                                                    │
                                                    ▼
                                              Cache Response
```

1. **Query arrives** — user asks a question
2. **Embed** — convert to vector using configured model
3. **Search** — find similar cached queries (cosine similarity via HNSW index)
4. **Hit/Miss** — if similarity > threshold, return cached response
5. **Cache miss** — forward to LLM, cache the response for future queries

## Quick Start

```bash
# Configure semantic caching
SEMANTIC.CONFIG SET threshold 0.85
SEMANTIC.CONFIG SET embedding_model "all-MiniLM-L6-v2"
SEMANTIC.CONFIG SET auto_embed true

# Cache an LLM response
SEMANTIC.SET "What is the capital of France?" "Paris is the capital of France. It is located in the north-central part of the country along the Seine River."

# Query with similar phrasing — returns cached result!
SEMANTIC.GET "France's capital city?" 0.85
# → "Paris is the capital of France..."

# Check cache stats
SEMANTIC.STATS
# entries: 150
# hits: 87
# misses: 63
# hit_rate: 0.58
# sets: 150
# evictions: 0
```

## Configuration

```toml
[semantic]
enabled = true
threshold = 0.85              # Similarity threshold (0.0 - 1.0)
embedding_dim = 384           # Embedding dimensions
max_cache_size = 100000       # Maximum cached entries
default_ttl = 3600            # Default TTL in seconds

[semantic.embedding]
model = "all-MiniLM-L6-v2"   # Local ONNX model
# Or use OpenAI:
# provider = "openai"
# api_key = "${OPENAI_API_KEY}"
# model = "text-embedding-3-small"
```

### Rust Configuration

```rust,ignore
use ferrite::semantic::{SemanticConfig, EmbeddingModelConfig, EmbeddingModelType, IndexType, DistanceMetric};

let config = SemanticConfig {
    enabled: true,
    default_threshold: 0.85,
    max_entries: 100_000,
    embedding_dim: 384,
    default_ttl_secs: 3600,
    index_type: IndexType::Hnsw,
    distance_metric: DistanceMetric::Cosine,
    auto_embed: false,
    embedding_model: EmbeddingModelConfig {
        model_type: EmbeddingModelType::Onnx,
        model_path: Some("./models/all-MiniLM-L6-v2.onnx".to_string()),
        batch_size: 32,
        cache_embeddings: true,
        ..Default::default()
    },
};
```

## Commands

| Command | Description |
|---------|------------|
| `SEMANTIC.SET <query> <response> [TTL sec]` | Cache a query-response pair with embedding |
| `SEMANTIC.GET <embedding> [threshold] [COUNT n]` | Find cached response by embedding similarity |
| `SEMANTIC.GETTEXT <query> [threshold]` | Get response by text (requires `auto_embed`) |
| `SEMANTIC.DEL <id>` | Delete a cached entry by ID |
| `SEMANTIC.CLEAR` | Clear all cached entries |
| `SEMANTIC.INFO` | Cache configuration info |
| `SEMANTIC.STATS` | Cache performance statistics |
| `SEMANTIC.CONFIG GET [param]` | Get configuration parameter(s) |
| `SEMANTIC.CONFIG SET <param> <value>` | Set configuration parameter |

### Function Cache Commands

Specialized cache for deterministic function/API call results:

| Command | Description |
|---------|------------|
| `FCACHE.SET <fn> <args_json> <result_json> [NAMESPACE ns] [TTL sec]` | Cache a function call result |
| `FCACHE.GET <fn> <args_json> [NAMESPACE ns] [THRESHOLD t]` | Retrieve cached result |
| `FCACHE.INVALIDATE <fn_name \| ID id>` | Invalidate by function name or cache ID |
| `FCACHE.STATS` | Function cache statistics |
| `FCACHE.INFO` | Function cache configuration |
| `FCACHE.CLEAR` | Clear all function cache entries |
| `FCACHE.THRESHOLD <fn> <threshold>` | Set per-function similarity threshold |
| `FCACHE.TTL <fn> <ttl_secs>` | Set per-function TTL |

## Usage Patterns

### Pre-computed Embeddings

```rust,ignore
use ferrite::semantic::{SemanticCache, SemanticConfig};

let cache = SemanticCache::new(SemanticConfig::default())?;

// Store with pre-computed embedding
let question = "What is the capital of France?";
let answer = "Paris is the capital of France.";
let embedding = compute_embedding(question);  // Your embedding function
cache.set(question, answer, &embedding, None)?;

// Query with embedding
let query_embedding = compute_embedding("France's capital city?");
let result = cache.get(&query_embedding, 0.85)?;  // Threshold: 85% similarity

if let Some(hit) = result {
    println!("Cache hit! Answer: {}", hit.value);
    println!("Similarity: {:.2}%", hit.score * 100.0);
}
```

### Automatic Embedding (Local ONNX)

```rust,ignore
use ferrite::semantic::{SemanticCache, SemanticConfig, EmbeddingModelConfig, EmbeddingModelType};

let config = SemanticConfig {
    auto_embed: true,
    embedding_model: EmbeddingModelConfig {
        model_type: EmbeddingModelType::Onnx,
        model_path: Some("./models/all-MiniLM-L6-v2.onnx".to_string()),
        ..Default::default()
    },
    ..Default::default()
};

let cache = SemanticCache::new(config)?;

// Embedding generated automatically
cache.set_auto("What is Rust?", "Rust is a systems programming language...", None)?;
let result = cache.get_auto("Tell me about Rust programming", 0.85)?;
```

### LLM Response Caching with Cost Tracking

```rust,ignore
use ferrite::semantic::llm_cache::{LlmCache, LlmCacheConfig, CostTrackingConfig};

let cache = LlmCache::new(LlmCacheConfig {
    similarity_threshold: 0.88,
    max_entries: 100_000,
    cost_tracking: CostTrackingConfig {
        enabled: true,
        cost_per_1k_tokens: 0.002,  // GPT-4 pricing
        avg_tokens_per_query: 500,
    },
    ..Default::default()
})?;

// Check cache before calling LLM
let query = "Explain quantum computing";
match cache.get(query).await? {
    Some(cached) => {
        println!("Cache hit! Saved ${:.4}", cached.estimated_savings);
    }
    None => {
        let response = call_llm(query).await?;
        cache.set(query, &response).await?;
    }
}

// Check cost savings
let stats = cache.stats();
println!("Total saved: ${:.2}", stats.total_cost_saved);
println!("Hit rate: {:.1}%", stats.hit_rate * 100.0);
```

### Resilient Embedding with Circuit Breaker

Handle embedding API failures gracefully with automatic fallback:

```rust,ignore
use ferrite::semantic::resilience::{ResilientEmbedder, CircuitBreaker, RetryPolicy, FallbackStrategy};

let embedder = ResilientEmbedder::builder()
    .primary(openai_embedder)
    .fallback(FallbackStrategy::Secondary(onnx_embedder))
    .circuit_breaker(CircuitBreaker::new(
        5,       // failures before open
        60,      // seconds before half-open
    ))
    .retry_policy(RetryPolicy::exponential(3, 100))  // 3 retries, 100ms base
    .build();

// Automatically retries, falls back, and handles circuit breaker
let embedding = embedder.embed("Hello world").await?;
```

## Integration with LLM Frameworks

### LangChain

```python
from langchain.cache import FerriteSemanticCache
from langchain.llms import OpenAI

# Use Ferrite as LLM cache
import langchain
langchain.llm_cache = FerriteSemanticCache(
    redis_url="redis://localhost:6379",
    similarity_threshold=0.85,
)

llm = OpenAI(model_name="gpt-4")
# First call: hits OpenAI API
result = llm("What is the capital of France?")
# Second call (similar query): returns from cache!
result = llm("Tell me France's capital")
```

### LlamaIndex

```python
from llama_index.core import Settings
from ferrite import FerriteSemanticCache

Settings.llm_cache = FerriteSemanticCache(
    host="localhost", port=6379,
    threshold=0.85,
)
```

## Threshold Tuning

The similarity threshold controls cache hit sensitivity:

| Threshold | Hit Rate | Quality | Use Case |
|-----------|----------|---------|----------|
| 0.95+ | Low | Very High | Critical applications |
| 0.90 | Medium | High | Production default |
| 0.85 | High | Good | Most LLM caching |
| 0.80 | Very High | Moderate | Cost-sensitive apps |

**Recommendation**: Start with 0.85, monitor quality, adjust as needed.

## Embedding Model Options

| Model Type | Latency | Dimensions | Cost | Privacy |
|-----------|---------|------------|------|---------|
| ONNX (local `all-MiniLM-L6-v2`) | ~2ms | 384 | Free | Full |
| OpenAI `text-embedding-3-small` | ~50ms | 1536 | $0.02/1M tokens | API |
| Cohere `embed-english-v3.0` | ~50ms | 1024 | $0.10/1M tokens | API |
| Custom HTTP endpoint | Varies | Configurable | Varies | Varies |

## Cost Savings Calculator

| Monthly LLM Calls | Hit Rate | Est. Savings |
|-------------------|----------|-------------|
| 10,000 | 40% | $80-120 |
| 100,000 | 50% | $1,000-1,500 |
| 1,000,000 | 60% | $12,000-18,000 |

*Based on average GPT-4 pricing of $0.03/1K input tokens + $0.06/1K output tokens*

## Performance

| Operation | Latency | Throughput |
|-----------|---------|------------|
| Cache hit (pre-embedded) | 20-50μs | 40K/s |
| Cache hit (ONNX embed) | 0.5-2ms | 1K/s |
| Cache hit (OpenAI embed) | 5-20ms | 100/s |
| Cache set | 30-80μs | 25K/s |
| Memory per entry | ~1.5KB (384-dim vectors) | — |

## Submodule Reference

| Module | Purpose |
|--------|---------|
| `ferrite-ai::semantic::cache` | Core `SemanticCache` with HNSW-backed similarity search |
| `ferrite-ai::semantic::hnsw` | HNSW index implementation for approximate nearest neighbors |
| `ferrite-ai::semantic::embeddings` | Embedding model abstraction and providers |
| `ferrite-ai::semantic::async_embeddings` | Async embedding pool and batch processor |
| `ferrite-ai::semantic::llm_cache` | LLM-specific cache with cost tracking |
| `ferrite-ai::semantic::function_cache` | Function/API call result caching |
| `ferrite-ai::semantic::resilience` | Circuit breaker, retry, and fallback for embedding APIs |
| `ferrite-ai::semantic::streaming` | Streaming embedding pipeline |
| `ferrite-ai::semantic::metrics` | Cache performance metrics and timers |
| `ferrite-ai::semantic::ga_cache` | Persistence-enabled semantic cache variant |
| `commands::handlers::semantic` | Redis protocol command handlers (SEMANTIC.*, FCACHE.*) |
