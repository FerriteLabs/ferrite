# @ferrite/ai

Semantic caching SDKs for LLM frameworks, backed by [Ferrite](https://github.com/ferritelabs/ferrite).

Cache LLM responses by **meaning** rather than exact key match — reduce API costs by 40–60 % while keeping response quality high.

## Installation

```bash
npm install @ferrite/ai

# Peer dependencies (install the ones you need)
npm install @langchain/core   # for LangChain.js integration
npm install openai             # for OpenAI wrapper
```

## Quick Start

### Basic Semantic Caching

```typescript
import { FerriteClient } from "@ferrite/ai";

const client = new FerriteClient({ host: "127.0.0.1", port: 6380 });

// Store a response
await client.semanticSet(
  "What is the capital of France?",
  "Paris is the capital of France.",
  undefined,
  3600,
);

// Retrieve by meaning (not exact match)
const result = await client.semanticGet("France's capital city?", 0.85);
if (result) {
  console.log(result.response);   // "Paris is the capital of France."
  console.log(result.similarity);  // 0.92
}

await client.disconnect();
```

### LangChain.js Integration

```typescript
import { FerriteSemanticCache } from "@ferrite/ai";

const cache = new FerriteSemanticCache({
  similarityThreshold: 0.85,
  ttl: 3600,
});

// Use with LangChain — implements lookup() and update()
const cached = await cache.lookup("What is France's capital?", "gpt-4o-mini");
if (!cached) {
  await cache.update("What is France's capital?", "gpt-4o-mini", [
    { text: "Paris is the capital of France." },
  ]);
}
```

### OpenAI Caching Wrapper

```typescript
import { cachedCompletion } from "@ferrite/ai";

const response = await cachedCompletion({
  model: "gpt-4o-mini",
  messages: [{ role: "user", content: "Explain quantum computing" }],
  threshold: 0.85,
  ttl: 3600,
});

console.log(response.choices[0].message.content);
console.log(response._cacheHit); // true on subsequent similar queries
```

## API Reference

### `FerriteClient`

| Method | Description |
|--------|-------------|
| `semanticSet(query, response, metadata?, ttl?)` | Store a query/response pair |
| `semanticGet(query, threshold?)` | Retrieve by semantic similarity |
| `semanticDelete(query)` | Remove a cached entry |
| `semanticStats()` | Get hit rate, entry count, avg similarity |

### `FerriteSemanticCache` (LangChain.js)

| Method | Description |
|--------|-------------|
| `lookup(prompt, llmString)` | Look up cached generations |
| `update(prompt, llmString, generations)` | Store generations |
| `clear()` | Flush cache entries |

### `cachedCompletion` (OpenAI)

Drop-in wrapper around `openai.chat.completions.create` with transparent caching.

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `host` | `127.0.0.1` | Ferrite server host |
| `port` | `6380` | Ferrite server port |
| `similarityThreshold` | `0.85` | Minimum cosine similarity for a hit |
| `ttl` | `undefined` | Cache entry TTL (seconds) |
| `namespace` | varies | Key prefix for cache isolation |

## Requirements

- Node.js 18+
- Ferrite server running (or any Redis-compatible server with semantic commands)
- `ioredis>=5.3.0`

## License

Apache-2.0
