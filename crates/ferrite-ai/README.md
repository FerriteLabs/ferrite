# Ferrite AI

AI/ML features for Ferrite — vector similarity search, semantic caching, RAG pipelines, and embedding generation.

Part of the [Ferrite](https://github.com/ferritelabs/ferrite) workspace.

## Modules

- `vector` — Vector similarity search (HNSW/IVF indexes)
- `semantic` — Semantic caching for LLM responses
- `rag` — Retrieval-augmented generation pipelines
- `graphrag` — Graph-enhanced RAG
- `embedding` — Embedding generation (ONNX/API)
- `inference` — ML model inference at the data layer
- `agent_memory` — AI agent memory backend
- `conversation` — Conversation history store

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
ferrite-ai = { git = "https://github.com/ferritelabs/ferrite" }
```

## License

Apache-2.0 — see [LICENSE](../../LICENSE) for details.
