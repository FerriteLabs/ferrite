# Ferrite Features Catalog

This page catalogues every feature in Ferrite — what it does, its maturity, and how to enable it. For an at-a-glance maturity matrix see [`FEATURE_MATURITY.md`](FEATURE_MATURITY.md).

> **Maturity key**: ✅ Stable — 🧪 Beta — 🔬 Experimental
>
> Features marked 🧪 have library implementations and partial server wiring; expect API changes.
> Features marked 🔬 represent **planned APIs** and require `--features experimental`. Always check the [Feature Status table](#feature-status--gating) before relying on these in production.

## Feature Status & Gating

| Area | Status | How to enable |
|------|--------|---------------|
| Core Redis commands | ✅ | Default build |
| Persistence (AOF + checkpoints) | ✅ | `persistence.aof_enabled = true` |
| Metrics | ✅ | `metrics.enabled = true` |
| Embedded mode | ✅ | Use `ferrite` as a library dependency |
| HybridLog backend | 🧪 | `storage.backend = "hybridlog"` |
| OpenTelemetry export | 🧪 | Build with `--features otel`, configure `[otel]` |
| TUI dashboard | 🧪 | Build with `--features tui`, run `ferrite-tui` |
| io_uring I/O | 🧪 | Linux 5.11+, build with `--features io-uring` |
| WASM functions | 🔬 | Build with `--features wasm` (not yet wired to server) |
| ONNX embeddings | 🔬 | Build with `--features onnx` |

## Feature Comparison vs. Alternatives

| Feature | Redis | Dragonfly | KeyDB | Garnet | Valkey | **Ferrite** |
|---------|-------|-----------|-------|--------|--------|-------------|
| Multi-threaded | - | + | + | + | - | ✅ |
| Tiered Storage | - | - | - | + | - | 🧪 |
| Vector Search | + | - | - | - | + | 🧪 |
| CRDT Replication | - | - | - | - | - | 🧪 |
| Time-Travel Queries | - | - | - | - | - | 🔬 |
| Semantic Caching | - | - | - | - | - | 🔬 |
| SQL-like Queries | - | - | - | - | - | 🔬 |
| Data Triggers | - | - | - | - | - | 🔬 |
| Multi-Tenancy | - | - | - | + | - | 🔬 |
| Embedded Mode | - | - | - | - | - | ✅ |

## Core Redis Compatibility ✅

Full wire-compatible RESP2/RESP3 protocol support with all major data types:

- **Strings**: GET, SET, MGET, MSET, INCR, DECR, APPEND, STRLEN, GETRANGE, SETRANGE
- **Lists**: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX, LSET, LREM, LINSERT
- **Hashes**: HSET, HGET, HMSET, HMGET, HDEL, HEXISTS, HGETALL, HKEYS, HVALS, HLEN
- **Sets**: SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SUNION, SINTER, SDIFF
- **Sorted Sets**: ZADD, ZREM, ZSCORE, ZCARD, ZCOUNT, ZRANK, ZRANGE, ZRANGEBYSCORE
- **Keys**: DEL, EXISTS, EXPIRE, TTL, PTTL, KEYS, SCAN
- **Server**: PING, ECHO, INFO, SELECT, DBSIZE
- **Pub/Sub**: SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUBLISH
- **Transactions**: MULTI, EXEC, DISCARD, WATCH
- **HyperLogLog**: PFADD, PFCOUNT, PFMERGE
- **Lua Scripting**: EVAL, EVALSHA, SCRIPT

See [`REDIS_COMPATIBILITY.md`](REDIS_COMPATIBILITY.md) for the full command-by-command matrix.

## Next-Generation Features 🧪🔬

### New Command Families 🧪

Eight new command families extending Ferrite beyond Redis:

| Family | Purpose |
|--------|---------|
| `AUTOINDEX` | Automatic secondary index management |
| `CONV` | Data format conversion (JSON ↔ RESP, encoding transforms) |
| `COST` | Infrastructure cost estimation and optimization |
| `MULTICLOUD` | Multi-cloud replication and failover |
| `POLICY` | Data lifecycle policy management (TTL, tiering, eviction) |
| `S3` | Direct S3-compatible object storage operations |
| `SLOT` | Advanced hash slot inspection and management |
| `VECTOR.INGEST` | Bulk vector ingestion with batched indexing |

SDK support: Rust, Python, Go, Node.js, Java, and .NET clients cover all 8 families.

### Vector Search 🧪
Native vector similarity search for AI/ML workloads with HNSW, IVF, and flat indexes:
```bash
VECTOR.CREATE myindex DIM 384 DISTANCE cosine
VECTOR.ADD myindex doc1 [0.1, 0.2, ...] '{"title": "Hello"}'
VECTOR.SEARCH myindex [0.1, 0.2, ...] TOP 10
```

### Semantic Caching 🔬
Cache by meaning, not just exact keys — reduces LLM API costs by 40–60%:
```bash
SEMANTIC.SET "What is the capital of France?" "Paris is the capital..."
SEMANTIC.GET "France's capital city?" 0.85  # Returns cached answer if similarity > 85%
```

### Time-Travel Queries 🔬
Query data at any point in time for debugging, auditing, and recovery:
```bash
GET mykey AS OF -1h           # Value from 1 hour ago
HISTORY mykey SINCE -24h      # All changes in last 24 hours
```

### Change Data Capture (CDC) 🧪
First-class event streaming for real-time data pipelines:
```bash
CDC.SUBSCRIBE users:* --format json --output kafka://localhost:9092
```

### CRDTs for Multi-Region 🧪
Built-in conflict-free replicated data types for geo-distributed deployments:
```bash
CRDT.GCOUNTER mycounter INCR 5    # Grow-only counter
CRDT.LWWREGISTER mykey SET value  # Last-writer-wins register
CRDT.ORSET myset ADD item         # Observed-remove set
```

### FerriteQL Query Language 🔬
SQL-like queries with joins, aggregations, and materialized views:
```sql
QUERY FROM users:* WHERE $.active = true
      JOIN orders:* ON $.user_id = users.id
      SELECT users.name, COUNT(orders.*) as order_count
      GROUP BY users.id ORDER BY order_count DESC LIMIT 10
```

### Programmable Triggers 🔬
Event-driven functions that execute on data mutations:
```bash
TRIGGER.CREATE order_notify ON SET orders:* DO
  PUBLISH order_updates $KEY
  HTTP.POST "https://api.example.com/webhook" $VALUE
END
```

### WebAssembly User Functions 🔬
Run custom WASM modules at the data layer with near-native performance:
```bash
WASM.LOAD validate_user /path/to/validate.wasm
TRIGGER.CREATE validate ON SET users:* WASM validate_user
```

## Multi-Model Database 🔬

> **Not production-ready**: Multi-model capabilities are in early development. APIs below are aspirational and subject to significant changes.

Beyond key-value, Ferrite supports multiple data models:

- **Document Store** — MongoDB-compatible JSON documents with secondary indexes and aggregation pipelines
- **Graph Database** — Property graphs with traversal algorithms, pattern matching, and PageRank
- **Time Series** — Optimized storage and querying for time-stamped data
- **Full-Text Search** — Faceted search with highlighting and fuzzy matching

## Enterprise Features 🧪

> **Partially implemented**: Items marked ✅ below are functional; others are in progress. Build with `--features experimental` to access them. Do not use in production without testing.

- **Replication** — Primary-replica replication with partial resync ✅
- **TLS Support** — Secure connections with optional mTLS (TLS 1.2/1.3) ✅
- **ACLs** — Fine-grained access control with Argon2 password hashing ✅
- **Cluster Mode** — Hash slot-based sharding (16384 slots) with automatic failover 🧪
- **Cloud Storage Tiering** — Automatic cold data migration to S3/GCS/Azure 🧪
- **Backup & Restore** — Full and incremental backups with point-in-time recovery 🧪
- **Multi-Tenancy** — First-class tenant isolation with per-tenant resource limits 🔬
- **Kubernetes Operator** — Full CRD support for automated cluster management 🔬

## Performance

Performance figures are based on internal benchmarks; run `cargo bench` to measure on your hardware.

- **Throughput** — 11.8M+ GET ops/sec, 2.6M+ SET ops/sec (single-threaded)
- **Latency** — P99 < 250ns, P99.9 < 20µs
- **Memory Efficient** — Epoch-based reclamation, zero-copy operations
- **Tiered Storage** — Hot (memory) → Warm (mmap) → Cold (disk/cloud)
- **Adaptive Tuning** — ML-based auto-tuning that adapts to workload patterns

See [`BENCHMARKS.md`](BENCHMARKS.md) for the full performance methodology and competitive comparison.

## Cargo Feature Flags

| Feature | Description |
|---------|-------------|
| `io-uring` | Enable io_uring for Linux (requires kernel 5.11+) |
| `tui` | Build the terminal dashboard (`ferrite-tui`) |
| `wasm` | Enable WebAssembly user functions |
| `onnx` | Enable local ONNX embeddings for semantic search |
| `otel` | Enable OpenTelemetry tracing |
| `scripting` | Lua scripting (default) |
| `tls` | TLS 1.2/1.3 support (default) |
| `cloud` | Cloud-storage tiering backends (S3/GCS/Azure) |

```bash
# Build with specific features
cargo build --release --features "io-uring,wasm,otel"

# Build with all features
cargo build --release --all-features
```
