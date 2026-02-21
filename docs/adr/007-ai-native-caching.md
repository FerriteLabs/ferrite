# ADR-007: AI-Native Caching Niche

## Status

**Proposed** — February 2026

## Context

The AI/ML application stack creates a new caching tier that Redis doesn't serve well:
- **LLM API calls** are expensive ($0.01-0.06 per call) and often redundant
- **Embedding computations** are compute-intensive and highly cacheable
- **RAG pipelines** need vector similarity + metadata filtering + caching in one system
- **Agent memory** requires structured working memory with TTL and semantic retrieval

Ferrite already has `ferrite-ai` (semantic caching, vector search, RAG) and `ferrite-search`. Making these production-quality first positions Ferrite as THE cache for AI applications.

## Decision

Focus the `ferrite-ai` crate as a first-class AI caching layer, not just a feature bolt-on.

### Target User Persona

**AI Application Developer** building with:
- OpenAI / Anthropic / Cohere APIs
- LangChain / LlamaIndex / Semantic Kernel frameworks
- RAG pipelines with vector databases
- Multi-agent systems

### Core AI Caching Features (Priority Order)

#### 1. Semantic Cache (Production Priority 🎯)

Cache LLM responses by meaning, not just exact match:

```bash
# Store a response with automatic embedding
SEMANTIC.SET "What is the capital of France?" "Paris is the capital of France" \
  MODEL text-embedding-3-small THRESHOLD 0.85

# Query by meaning — returns cached response if similarity > threshold
SEMANTIC.GET "France's capital city?" MODEL text-embedding-3-small
# → "Paris is the capital of France" (similarity: 0.92)

# With metadata for cost tracking
SEMANTIC.SET "..." "..." META '{"model":"gpt-4","tokens":150,"cost":0.0045}'
SEMANTIC.STATS  # → cache hit rate, estimated savings
```

**Key differentiator**: Built-in cost savings tracking. Show users exactly how much money caching saves them.

#### 2. Embedding Cache

Cache expensive embedding computations:

```bash
# Cache embeddings with automatic invalidation
EMBEDDING.CACHE "document text here" MODEL text-embedding-3-small
# Returns cached embedding if text was seen before, otherwise stores new

# Batch embedding cache
EMBEDDING.CACHE_BATCH doc1 doc2 doc3 MODEL text-embedding-3-small
```

#### 3. RAG Pipeline Cache

Cache entire RAG pipeline stages:

```bash
# Cache retrieval results
RAG.CACHE.RETRIEVAL "user query" RESULTS '[{"doc_id":"d1","score":0.95}]' TTL 3600

# Cache augmented context
RAG.CACHE.CONTEXT "user query" CONTEXT "Retrieved context..." TTL 1800

# Cache final generation
RAG.CACHE.GENERATION "user query" RESPONSE "Final answer..." TTL 7200
```

#### 4. Agent Working Memory

Structured memory for AI agents with semantic retrieval:

```bash
# Store agent memory
AGENT.MEMORY.SET agent:1 "User prefers dark mode" TYPE preference
AGENT.MEMORY.SET agent:1 "User asked about pricing" TYPE interaction

# Semantic recall
AGENT.MEMORY.RECALL agent:1 "What does the user like?" TOP 5
```

### Integration Strategy

#### Framework Integrations (Priority)

| Framework | Integration | Effort |
|-----------|------------|--------|
| LangChain (Python) | Custom `CacheBackend` implementation | 1 week |
| LlamaIndex | Custom `LLMCache` implementation | 1 week |
| Semantic Kernel (.NET) | `ISemanticTextMemory` implementation | 2 weeks |
| OpenAI SDK | Proxy mode (intercept completions API) | 2 weeks |

#### LangChain Integration Example

```python
from langchain.cache import FerriteSemantCache
from ferrite import FerriteClient

client = FerriteClient("localhost", 6379)
cache = FerriteSemanticCache(
    client=client,
    embedding_model="text-embedding-3-small",
    similarity_threshold=0.85,
)

# LangChain automatically uses Ferrite for caching
import langchain
langchain.llm_cache = cache
```

### Competitive Positioning

| Feature | Redis | GPTCache | Ferrite |
|---------|-------|----------|---------|
| Exact-match caching | ✅ | ✅ | ✅ |
| Semantic similarity cache | ❌ | ✅ | ✅ |
| Vector search | ✅ (module) | ❌ | ✅ (native) |
| Cost tracking | ❌ | ❌ | ✅ |
| Framework integrations | ✅ | ✅ | 🧪 |
| Redis protocol compat | ✅ | ❌ | ✅ |
| All-in-one (cache + vector + semantic) | ❌ | ❌ | ✅ |

**Unique value proposition**: Ferrite is the only system that combines Redis-compatible caching, native vector search, semantic caching, AND cost tracking in a single binary.

### Implementation Phases

1. **Phase 1** (3 weeks): Production-harden SEMANTIC.SET/GET with proper embedding support
2. **Phase 2** (2 weeks): Cost tracking dashboard (SEMANTIC.STATS, Grafana dashboard)
3. **Phase 3** (2 weeks): LangChain + LlamaIndex integrations (Python packages)
4. **Phase 4** (2 weeks): Blog post + benchmark vs GPTCache + Redis

## Consequences

### Positive
- Captures a growing market before competitors (no Redis-compatible AI cache exists)
- Clear ROI story: "Save 40-60% on LLM API costs"
- Framework integrations create sticky adoption
- Positions Ferrite for the AI infrastructure wave

### Negative
- Requires embedding model integration (API keys, model hosting)
- Similarity thresholds need careful tuning per use case
- AI hype cycle risk — feature may not find PMF

### Metrics for Success
- Users reporting >30% cache hit rate on semantic queries
- LangChain integration stars/downloads
- Documentation page views for AI features vs core features
