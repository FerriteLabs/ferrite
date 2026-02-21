# Feature Maturity

> **Maturity key**: ✅ Stable — 🧪 Beta — 🔬 Experimental

This document tracks the maturity level of all Ferrite features. Features graduate through maturity levels based on testing, stability, and real-world usage.

## Tier 1: Stable (✅)

Production-ready features with comprehensive tests and stable APIs.

| Feature | Description | How to Enable |
|---------|-------------|---------------|
| Core Redis Commands | GET, SET, DEL, EXPIRE, TTL, KEYS, SCAN, etc. | Default build |
| String Operations | INCR, DECR, APPEND, STRLEN, GETRANGE, SETRANGE | Default build |
| List Operations | LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, etc. | Default build |
| Hash Operations | HSET, HGET, HMSET, HMGET, HDEL, HGETALL, etc. | Default build |
| Set Operations | SADD, SREM, SMEMBERS, SISMEMBER, SUNION, etc. | Default build |
| Sorted Set Operations | ZADD, ZREM, ZSCORE, ZRANGE, ZRANGEBYSCORE, etc. | Default build |
| HyperLogLog | PFADD, PFCOUNT, PFMERGE | Default build |
| Pub/Sub | SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUBLISH | Default build |
| Transactions | MULTI, EXEC, DISCARD, WATCH | Default build |
| Lua Scripting | EVAL, EVALSHA, SCRIPT | `--features scripting` (default) |
| RESP2/RESP3 Protocol | Full wire compatibility | Default build |
| AOF Persistence | Write-ahead logging | `persistence.aof_enabled = true` |
| Prometheus Metrics | `/metrics` endpoint | `metrics.enabled = true` |
| Embedded Mode | Library usage without server | Use as dependency |
| TLS | TLS 1.2/1.3 connections | `--features tls` (default) |
| ACLs | Fine-grained access control | `auth.enabled = true` |

## Tier 2: Beta (🧪)

Feature-complete but still undergoing testing. APIs may change.

| Feature | Description | How to Enable |
|---------|-------------|---------------|
| HybridLog Storage | Three-tier storage engine | `storage.backend = "hybridlog"` |
| Vector Search | HNSW/IVF/Flat indexes | Default build |
| CDC / Event Streaming | Change data capture | Default build |
| CRDTs | Conflict-free replicated types | Default build |
| Cluster Mode | Hash slot sharding | `cluster.enabled = true` |
| Replication | Primary-replica sync | Config-driven |
| OpenTelemetry | Distributed tracing | `--features otel` |
| TUI Dashboard | Terminal monitoring UI | `--features tui` |
| io_uring I/O | Linux async I/O | `--features io-uring` (Linux 5.11+) |
| Full-Text Search | Faceted search with highlighting | Default build |
| Time Series | Time-series ingestion/query | Default build |

## Tier 3: Experimental (🔬)

Early implementations. APIs will change. Not recommended for production.

| Feature | Description | How to Enable |
|---------|-------------|---------------|
| FerriteQL | SQL-like query language | Default build |
| Time-Travel Queries | `GET key AS OF -1h` | Default build |
| Semantic Caching | Cache by meaning | Default build |
| Programmable Triggers | Event-driven functions | Default build |
| WASM User Functions | Custom WASM modules | `--features wasm` |
| ONNX Embeddings | Local ML inference | `--features onnx` |
| Multi-Tenancy | Tenant isolation | `--features experimental` |
| Kubernetes Operator | CRD-based management | `--features experimental` |
| Ferrite Studio | Web management UI | `--features experimental` |
| Document Store | MongoDB-compatible JSON | `--features experimental` |
| Graph Database | Property graphs | `--features experimental` |

## Cargo Feature Mapping

Each maturity tier maps to specific Cargo feature flags in the top-level `Cargo.toml`:

| Cargo Feature | Included Crates / Capabilities | Tier |
|---------------|-------------------------------|------|
| *(default)* | `scripting`, `tls`, `cloud`, `crypto`, `cli` | Tier 1 |
| `scripting` | Lua scripting (`mlua`) | Tier 1 |
| `tls` | TLS support (`rustls`, `tokio-rustls`) | Tier 1 |
| `cloud` | `ferrite-cloud`, cloud storage backends | Tier 2 |
| `crypto` | Cryptographic utilities | Tier 1 |
| `cli` | CLI client (`rustyline`, `clap_complete`) | Tier 1 |
| `otel` | OpenTelemetry tracing | Tier 2 |
| `tui` | Terminal dashboard (`ratatui`) | Tier 2 |
| `io-uring` | Linux io_uring I/O | Tier 2 |
| `wasm` | WASM user functions (`wasmtime`) | Tier 3 |
| `onnx` | ONNX ML inference (`ort`) | Tier 3 |
| `experimental` | `ferrite-k8s`, `ferrite-enterprise`, `ferrite-studio` | Tier 3 |
| `all` | Everything above | — |

### Extension crates gated by `experimental`

The following optional crates are pulled in only when `--features experimental` is enabled:

- **`ferrite-k8s`** — Kubernetes operator / CRD management
- **`ferrite-enterprise`** — Multi-tenancy, cost optimiser, policy engine
- **`ferrite-studio`** — Web-based management UI

### Handler-level feature gates (`src/commands/handlers/mod.rs`)

| Gate | Handlers |
|------|----------|
| *(always compiled)* | `admin`, `cluster`, `crdt`, `keys`, `server`, `temporal`, `timeseries`, `transaction`, `trigger`, `vector`, `wasm` |
| `#[cfg(feature = "experimental")]` | `autoindex`, `conversation`, `costoptimizer`, `document`, `graph`, `inference`, `multicloud`, `policy`, `s3`, `schema`, `slots` |
| `#[cfg(feature = "cloud")]` | `rag`, `semantic` |

## Graduation Criteria

To graduate from one tier to the next, a feature must meet:

**🔬 → 🧪 (Experimental → Beta):**
- [ ] Core functionality complete
- [ ] Unit tests with >70% coverage
- [ ] Integration tests for happy paths
- [ ] API documentation
- [ ] No known critical bugs

**🧪 → ✅ (Beta → Stable):**
- [ ] API frozen (breaking changes require major version bump)
- [ ] Unit tests with >90% coverage
- [ ] Integration and property-based tests
- [ ] Performance benchmarks
- [ ] Production usage by at least one deployment
- [ ] Migration guide from previous API versions
