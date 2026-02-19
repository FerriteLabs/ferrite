# ferrite-ai

Semantic caching SDKs for LLM frameworks, backed by [Ferrite](https://github.com/ferritelabs/ferrite).

Cache LLM responses by **meaning** rather than exact key match — reduce API costs by 40–60 % while keeping response quality high.

## Installation

```bash
pip install ferrite-ai

# With framework extras
pip install ferrite-ai[langchain]
pip install ferrite-ai[openai]
pip install ferrite-ai[all]
```

## Quick Start

### Basic Semantic Caching

```python
from ferrite_ai import FerriteClient

client = FerriteClient(host="127.0.0.1", port=6380)

# Store a response
client.semantic_set(
    "What is the capital of France?",
    "Paris is the capital of France.",
    ttl=3600,
)

# Retrieve by meaning (not exact match)
result = client.semantic_get("France's capital city?", threshold=0.85)
if result:
    print(result.response)   # "Paris is the capital of France."
    print(result.similarity)  # 0.92
```

### LangChain Integration

```python
from langchain.globals import set_llm_cache
from langchain_openai import ChatOpenAI
from ferrite_ai.langchain import FerriteSemanticCache

set_llm_cache(FerriteSemanticCache(
    similarity_threshold=0.85,
    ttl=3600,
))

llm = ChatOpenAI(model="gpt-4o-mini")
# First call hits the API; semantically similar calls hit the cache
response = llm.invoke("What is the capital of France?")
```

### LlamaIndex Integration

```python
from ferrite_ai.llamaindex import FerriteSemanticCache

cache = FerriteSemanticCache(similarity_threshold=0.85)
cache.put("What is Ferrite?", "Ferrite is a high-performance KV store.")

result = cache.get("Tell me about Ferrite")
print(result)  # "Ferrite is a high-performance KV store."
```

### OpenAI Caching Wrapper

```python
from ferrite_ai.openai_wrapper import cached_completion

response = cached_completion(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": "Explain quantum computing"}],
    threshold=0.85,
    ttl=3600,
)
print(response["choices"][0]["message"]["content"])
print(response["_cache_hit"])  # True on subsequent similar queries
```

## API Reference

### `FerriteClient`

| Method | Description |
|--------|-------------|
| `semantic_set(query, response, metadata?, ttl?)` | Store a query/response pair |
| `semantic_get(query, threshold=0.85)` | Retrieve by semantic similarity |
| `semantic_delete(query)` | Remove a cached entry |
| `semantic_stats()` | Get hit rate, entry count, avg similarity |

### `FerriteSemanticCache` (LangChain)

Implements `langchain_core.caches.BaseCache` with `lookup()` and `update()`.

### `FerriteSemanticCache` (LlamaIndex)

Provides `get()` and `put()` methods compatible with LlamaIndex caching.

### `cached_completion` (OpenAI)

Drop-in replacement for `openai.chat.completions.create` with transparent caching.

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `host` | `127.0.0.1` | Ferrite server host |
| `port` | `6380` | Ferrite server port |
| `similarity_threshold` | `0.85` | Minimum cosine similarity for a hit |
| `ttl` | `None` | Cache entry TTL (seconds) |
| `namespace` | varies | Key prefix for cache isolation |

## Requirements

- Python 3.9+
- Ferrite server running (or any Redis-compatible server with semantic commands)
- `redis>=5.0.0`

## License

Apache-2.0
