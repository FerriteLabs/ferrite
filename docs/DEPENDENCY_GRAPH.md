# Workspace Dependency Graph

This document describes the dependency relationships between Ferrite workspace crates.

## Crate Overview

| Crate | Description | Type |
|-------|-------------|------|
| `ferrite` | Top-level binary — server, CLI, integration layer | Binary + Library |
| `ferrite-core` | Core engine — storage, protocol, persistence, auth, cluster | Library |
| `ferrite-ai` | AI/ML — vector search, semantic caching, RAG, embeddings | Library |
| `ferrite-search` | Full-text search, query routing, schema, auto-indexing | Library |
| `ferrite-graph` | Graph data model and traversal | Library |
| `ferrite-timeseries` | Time-series ingestion and downsampling | Library |
| `ferrite-document` | JSON document store | Library |
| `ferrite-streaming` | Event streaming, CDC, pipelines | Library |
| `ferrite-cloud` | Cloud storage (S3/GCS/Azure), serverless, edge | Library (optional) |
| `ferrite-k8s` | Kubernetes operator | Library (optional) |
| `ferrite-enterprise` | Multi-tenancy, governance, federation | Library (optional) |
| `ferrite-plugins` | Plugin system, WASM runtime, CRDTs | Library |
| `ferrite-studio` | Web management UI and playground | Library (optional) |

## Dependency DAG

```
                          ┌──────────────────────────┐
                          │     ferrite (top-level)   │
                          │  commands/ server/ repl/  │
                          └─────────────┬────────────┘
                                        │
                 ┌──────────────────────┼──────────────────────┐
                 │ depends on           │                      │
        ┌────────┴────────┐   ┌────────┴────────┐   ┌────────┴────────┐
        │  ferrite-core   │   │  Extension       │   │  Optional       │
        │  (always)       │   │  Crates          │   │  Crates         │
        └─────────────────┘   │  (always)        │   │  (feature-gated)│
                              └────────┬─────────┘   └────────┬────────┘
                                       │                      │
                         ┌─────────────┼──────┐      ┌────────┼─────────┐
                         │             │      │      │        │         │
                    ferrite-ai   ferrite-  ferrite-  ferrite- ferrite-  ferrite-
                    ferrite-     search    graph     cloud    k8s      enterprise
                    plugins      ferrite-  ferrite-  ferrite-
                    ferrite-     document  timeseries studio
                    streaming
```

## Key Design Rules

1. **Extension crates are self-contained** — they do NOT depend on `ferrite-core` or each other
2. **`ferrite` (top-level) is the integration layer** — it imports all crates and wires them together
3. **Optional crates** (`ferrite-cloud`, `ferrite-k8s`, `ferrite-enterprise`, `ferrite-studio`) are feature-gated
4. **No circular dependencies** — the DAG flows strictly upward

## Feature Flag Propagation

```
ferrite (top-level features)          → Sub-crate features
─────────────────────────────────────────────────────────
cloud                                 → ferrite-cloud + ferrite-core/cloud
wasm                                  → wasmtime + ferrite-plugins/wasm
onnx                                  → ort + ndarray + ferrite-ai/onnx
crypto                                → ferrite-core/crypto
io-uring                              → ferrite-core/io-uring
otel                                  → opentelemetry stack
tls                                   → rustls + tokio-rustls
scripting                             → mlua
experimental                          → ferrite-k8s + ferrite-enterprise + ferrite-studio
```

## External Dependencies (Key)

| Category | Dependencies |
|----------|-------------|
| Async Runtime | `tokio`, `futures`, `async-trait` |
| Data Structures | `dashmap`, `crossbeam`, `parking_lot`, `bytes`, `ordered-float` |
| Serialization | `serde`, `serde_json`, `toml`, `bincode` |
| Observability | `tracing`, `tracing-subscriber`, `metrics`, `metrics-exporter-prometheus` |
| HTTP | `hyper`, `hyper-util`, `http-body-util`, `reqwest` |
| Crypto/TLS | `rustls`, `tokio-rustls`, `argon2`, `chacha20poly1305` |
| Storage | `memmap2`, `lz4_flex`, `flate2`, `crc32fast` |
| CLI | `clap`, `rustyline`, `colored`, `ratatui` |

---

*Generated from workspace Cargo.toml. Run `cargo tree --workspace` for the full transitive dependency tree.*
