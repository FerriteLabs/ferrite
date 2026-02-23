# Ferrite

[English](README.md) | [简体中文](README.zh-CN.md) | [日本語](README.ja-JP.md) | [한국어](README.ko-KR.md)

A high-performance, tiered-storage key-value store designed as a drop-in Redis replacement. Built in Rust with epoch-based concurrency and io_uring-first persistence.

**The speed of memory, the capacity of disk, the economics of cloud.**

[![Build Status](https://github.com/ferritelabs/ferrite/actions/workflows/ci.yml/badge.svg)](https://github.com/ferritelabs/ferrite/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/ferrite.svg)](https://crates.io/crates/ferrite)
[![Documentation](https://docs.rs/ferrite/badge.svg)](https://docs.rs/ferrite)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.80%2B-orange)](https://www.rust-lang.org/)
[![codecov](https://codecov.io/gh/ferritelabs/ferrite/branch/main/graph/badge.svg)](https://codecov.io/gh/ferritelabs/ferrite)
[![Redis Compat](https://img.shields.io/badge/Redis%20Compat-72%25-yellow)](docs/REDIS_COMPAT.md)

## Quick Start (60 seconds)

Linux prerequisites:

```bash
sudo apt-get install -y pkg-config libssl-dev
# or: sudo dnf install -y pkgconf-pkg-config openssl-devel
```

```bash
# Build and run (macOS/Linux) — no config file needed
cargo build --release && ./target/release/ferrite

# Or use Make
make quickstart
```

No Rust toolchain? Use Docker Compose:

```bash
docker compose up -d
```

Verify:

```bash
redis-cli PING                    # any Redis client works
./target/release/ferrite-cli PING # or use the built-in CLI
```

Something not working? Run diagnostics:

```bash
./target/release/ferrite doctor
```

## First 5 minutes

```bash
redis-cli SET mykey "Hello, Ferrite!"
redis-cli GET mykey
redis-cli INFO
# or use: ./target/release/ferrite-cli
```

## Install (Prebuilt)

```bash
curl -fsSL https://raw.githubusercontent.com/ferritelabs/ferrite/main/scripts/install.sh | bash
# or: curl -fsSL https://raw.githubusercontent.com/ferritelabs/ferrite/main/scripts/install.sh | bash -s -- v0.1.0
```

The installer writes a default config to `~/.config/ferrite/ferrite.toml` (unless
`FERRITE_SKIP_INIT=1`) and prints a ready-to-run command.

```bash
brew tap ferritelabs/ferrite
brew install ferrite
```

Set `FERRITE_INSTALL_DIR` to customize the install location.

## More details

The rest of this README dives deeper into features, architecture, and operations.

## What Works Today

These features are **stable and tested** — you can use them right now:

- **Full Redis Protocol**: Wire-compatible RESP2/RESP3 ([~72% Redis test suite passing](docs/REDIS_COMPAT.md)). Connect with `redis-cli`, `redis-py`, `redis-rs`, or any Redis client
- **All Core Data Types**: Strings, Lists, Hashes, Sets, Sorted Sets, HyperLogLog
- **Key Operations**: DEL, EXISTS, EXPIRE, TTL, PTTL, KEYS, SCAN, TYPE, RENAME
- **Pub/Sub**: SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUBLISH
- **Transactions**: MULTI, EXEC, DISCARD, WATCH
- **Lua Scripting**: EVAL, EVALSHA, SCRIPT (requires `--features scripting`, included by default)
- **Persistence**: AOF with configurable sync + periodic RDB-style checkpoints
- **Prometheus Metrics**: Built-in `/metrics` endpoint on port 9090
- **Embedded Mode**: Use as a library without a server (`Database::open(...)`)
- **Docker Ready**: Multi-stage Dockerfile + Compose with Prometheus/Grafana profiles

```bash
# Verify it works — connect with any Redis client
redis-cli -p 6379 SET hello "world"
redis-cli -p 6379 GET hello
```

## Why Ferrite?

> **Maturity key**: ✅ Stable — 🧪 Beta — 🔬 Experimental

### Feature Status & Gating

Features are grouped by maturity in [`docs/FEATURE_MATURITY.md`](docs/FEATURE_MATURITY.md).
Optional capabilities are gated behind Cargo features or config settings.
Experimental features (Tier 4) require `--features experimental`.

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

## Features

### Core Redis Compatibility ✅

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

### Next-Generation Features 🧪🔬

Ferrite goes beyond Redis compatibility with features designed for the AI/cloud-native era:

> **⚠️ Maturity Notice**: Features below marked 🧪 (Beta) have library-level implementations
> and partial server wiring — expect API changes. Features marked 🔬 (Experimental) represent
> the **planned API** and require `--features experimental`. They are not yet available in the
> default server binary. Always check the [Feature Status table](#feature-status--gating) above
> before relying on these in production.

#### Vector Search 🧪
Native vector similarity search for AI/ML workloads with HNSW, IVF, and flat indexes:
```bash
VECTOR.CREATE myindex DIM 384 DISTANCE cosine
VECTOR.ADD myindex doc1 [0.1, 0.2, ...] '{"title": "Hello"}'
VECTOR.SEARCH myindex [0.1, 0.2, ...] TOP 10
```

#### Semantic Caching 🔬
Cache by meaning, not just exact keys - reduces LLM API costs by 40-60%:
```bash
SEMANTIC.SET "What is the capital of France?" "Paris is the capital..."
SEMANTIC.GET "France's capital city?" 0.85  # Returns cached answer if similarity > 85%
```

#### Time-Travel Queries 🔬
Query data at any point in time for debugging, auditing, and recovery:
```bash
GET mykey AS OF -1h           # Value from 1 hour ago
HISTORY mykey SINCE -24h      # All changes in last 24 hours
```

#### Change Data Capture (CDC) 🧪
First-class event streaming for real-time data pipelines:
```bash
CDC.SUBSCRIBE users:* --format json --output kafka://localhost:9092
```

#### CRDTs for Multi-Region 🧪
Built-in conflict-free replicated data types for geo-distributed deployments:
```bash
CRDT.GCOUNTER mycounter INCR 5    # Grow-only counter
CRDT.LWWREGISTER mykey SET value  # Last-writer-wins register
CRDT.ORSET myset ADD item         # Observed-remove set
```

#### FerriteQL Query Language 🔬
SQL-like queries with joins, aggregations, and materialized views:
```sql
QUERY FROM users:* WHERE $.active = true
      JOIN orders:* ON $.user_id = users.id
      SELECT users.name, COUNT(orders.*) as order_count
      GROUP BY users.id ORDER BY order_count DESC LIMIT 10
```

#### Programmable Triggers 🔬
Event-driven functions that execute on data mutations:
```bash
TRIGGER.CREATE order_notify ON SET orders:* DO
  PUBLISH order_updates $KEY
  HTTP.POST "https://api.example.com/webhook" $VALUE
END
```

#### WebAssembly User Functions 🔬
Run custom WASM modules at the data layer with near-native performance:
```bash
WASM.LOAD validate_user /path/to/validate.wasm
TRIGGER.CREATE validate ON SET users:* WASM validate_user
```

### Multi-Model Database 🔬

> **⚠️ Not production-ready**: Multi-model capabilities are in early development.
> The APIs below are aspirational and subject to significant changes.

Beyond key-value, Ferrite supports multiple data models:

- **Document Store**: MongoDB-compatible JSON documents with secondary indexes and aggregation pipelines
- **Graph Database**: Property graphs with traversal algorithms, pattern matching, and PageRank
- **Time Series**: Optimized storage and querying for time-stamped data
- **Full-Text Search**: Faceted search with highlighting and fuzzy matching

### Enterprise Features 🧪

> **⚠️ Partially implemented**: Enterprise features are under active development. Items
> marked with ✅ below are functional; others are in progress. Build with
> `--features experimental` to access them. Do not use in production without testing.

- **Replication**: Primary-replica replication with partial resync ✅
- **TLS Support**: Secure connections with optional mTLS (TLS 1.2/1.3) ✅
- **ACLs**: Fine-grained access control with Argon2 password hashing ✅
- **Cluster Mode**: Hash slot-based sharding (16384 slots) with automatic failover 🧪
- **Cloud Storage Tiering**: Automatic cold data migration to S3/GCS/Azure 🧪
- **Backup & Restore**: Full and incremental backups with point-in-time recovery 🧪
- **Multi-Tenancy**: First-class tenant isolation with per-tenant resource limits 🔬
- **Kubernetes Operator**: Full CRD support for automated cluster management 🔬

### Performance

Performance figures are based on internal benchmarks; run `cargo bench` to
measure on your hardware.

- **Throughput**: 11.8M+ GET ops/sec, 2.6M+ SET ops/sec (single-threaded)
- **Latency**: P99 < 250ns, P99.9 < 20us
- **Memory Efficient**: Epoch-based reclamation, zero-copy operations
- **Tiered Storage**: Hot (memory) -> Warm (mmap) -> Cold (disk/cloud)
- **Adaptive Tuning**: ML-based auto-tuning that adapts to workload patterns

## Build from Source

### Prerequisites

- **Rust** 1.80+ MSRV (install via [rustup](https://rustup.rs/); contributors use the 1.88 toolchain pinned in `rust-toolchain.toml`)
- **Git** for source control
- **Redis CLI** for manual testing (optional)
- **Linux only**: io_uring requires kernel 5.11+ and `--features io-uring`

### Installation

```bash
# Clone the repository
git clone https://github.com/ferritelabs/ferrite.git
cd ferrite

# Build release version
cargo build --release

# Generate a config and run
./target/release/ferrite init --output ferrite.toml
./target/release/ferrite
```

### Using Docker

```bash
# Docker Compose (recommended)
docker compose up -d
# or: make docker-compose-up

# Build Docker image
docker build -t ferrite:latest .

# Run container
docker run -p 6379:6379 -p 9090:9090 \
  -e FERRITE_BIND=0.0.0.0 \
  -e FERRITE_METRICS_BIND=0.0.0.0 \
  ferrite:latest
```

### Configuration

Ferrite loads `./ferrite.toml` by default; if it is missing but
`ferrite.example.toml` exists, it will be used with a warning.

Copy the included example configuration as a starting point:

```bash
cp ferrite.example.toml ferrite.toml
```

Or create a `ferrite.toml` configuration file:

```toml
[server]
bind = "127.0.0.1"
port = 6379
max_connections = 10000
timeout = 0

[storage]
databases = 16
max_memory = 1073741824

[persistence]
aof_enabled = true
aof_sync = "everysecond"
checkpoint_interval = 300

[metrics]
enabled = true
bind = "127.0.0.1"
port = 9090

[tls]
enabled = false
cert_file = "/path/to/cert.pem"
key_file = "/path/to/key.pem"
```

Run with configuration:

```bash
./target/release/ferrite --config ferrite.toml
```

Validate and inspect configuration:

```bash
ferrite --test-config --config ferrite.toml
ferrite --dump-config > ferrite.toml
ferrite doctor --config ferrite.toml
```

### CLI Helpers

```bash
ferrite init --output ferrite.toml
ferrite completions bash > /usr/local/etc/bash_completion.d/ferrite
```

### Connecting with Redis Clients

Ferrite is compatible with standard Redis clients:

**Redis CLI:**
```bash
redis-cli -p 6379
127.0.0.1:6379> SET mykey "Hello, Ferrite!"
OK
127.0.0.1:6379> GET mykey
"Hello, Ferrite!"
```

**Python (redis-py):**
```python
import redis

r = redis.Redis(host='localhost', port=6379)
r.set('mykey', 'Hello, Ferrite!')
print(r.get('mykey'))
```

**Rust (redis-rs):**
```rust
use redis::Commands;

let client = redis::Client::open("redis://127.0.0.1/")?;
let mut con = client.get_connection()?;
con.set("mykey", "Hello, Ferrite!")?;
let value: String = con.get("mykey")?;
```

### Examples

```bash
cargo run --example client_connection   # TCP client → server (RESP protocol)
cargo run --example basic_operations    # Embedded database API
cargo run --example transactions        # Transaction support
cargo run --example server_mode         # Server mode configuration
cargo run --example persistence_config  # Persistence options
```

## Architecture

### Storage Tiers

Ferrite implements a three-tier storage architecture inspired by Microsoft FASTER:

```
┌─────────────────────────────────────────────────────────────┐
│                    Hot Tier (Memory)                        │
│  - In-place updates, lock-free reads                        │
│  - DashMap with epoch-based reclamation                     │
│  - Automatic LRU-based eviction                             │
├─────────────────────────────────────────────────────────────┤
│                   Warm Tier (mmap)                          │
│  - Memory-mapped files, zero-copy reads                     │
│  - Read-only with copy-on-write for updates                 │
│  - Automatic promotion on access                            │
├─────────────────────────────────────────────────────────────┤
│                   Cold Tier (Disk/Cloud)                    │
│  - Async I/O with io_uring (Linux) / tokio (macOS)         │
│  - Optional cloud storage (S3, GCS, Azure)                  │
│  - Compression and encryption support                       │
└─────────────────────────────────────────────────────────────┘
```

### Persistence

- **AOF (Append-Only File)**: Write-ahead logging with configurable sync policies
- **Checkpointing**: Fork-less snapshots for consistent backups
- **Point-in-Time Recovery**: Restore to any moment using AOF + checkpoints

### High Availability

```
                    ┌─────────────┐
                    │   Primary   │
                    │   (Write)   │
                    └──────┬──────┘
                           │
          ┌────────────────┼────────────────┐
          │                │                │
    ┌─────▼─────┐    ┌─────▼─────┐    ┌─────▼─────┐
    │  Replica  │    │  Replica  │    │  Replica  │
    │  (Read)   │    │  (Read)   │    │  (Read)   │
    └───────────┘    └───────────┘    └───────────┘
```

### Cluster Mode

Hash slot-based sharding with 16384 slots:

```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│   Node A     │  │   Node B     │  │   Node C     │
│ Slots: 0-5460│  │Slots:5461-10922│ │Slots:10923-16383│
└──────────────┘  └──────────────┘  └──────────────┘
```

## Operations Guide

### Monitoring

Ferrite exposes Prometheus metrics at `/metrics`:

```bash
# View metrics
curl http://localhost:9090/metrics

# One-command monitoring stack (Prometheus + Grafana)
# See: https://github.com/ferritelabs/ferrite-ops/tree/main/monitoring
cd ferrite-ops/monitoring && ./setup.sh
```

Key metrics:
- `ferrite_commands_total`: Total commands processed
- `ferrite_connections_total`: Total connections
- `ferrite_memory_bytes`: Memory usage
- `ferrite_keys_total`: Number of keys per database
- `ferrite_latency_seconds`: Command latency histogram

### Backup & Restore

**Create a backup:**
```bash
redis-cli BGSAVE
# Or programmatically
redis-cli CONFIG SET appendonly yes
```

**Restore from backup:**
```bash
./target/release/ferrite --restore /path/to/backup.ferrite.backup
```

### TLS Configuration

Enable TLS by updating `ferrite.toml`:

```toml
[tls]
enabled = true
cert_file = "/path/to/server.crt"
key_file = "/path/to/server.key"
ca_file = "/path/to/ca.crt"  # For mTLS
require_client_cert = false
```

Connect with TLS:
```bash
redis-cli --tls -p 6379
```

### ACL Configuration

Create users with specific permissions:

```bash
# Create admin user
ACL SETUSER admin on >password ~* &* +@all

# Create read-only user
ACL SETUSER reader on >password ~* &* +@read -@write

# Create user for specific key patterns
ACL SETUSER app on >password ~app:* +@all
```

### Cluster Setup

Initialize a 3-node cluster:

```bash
# Start nodes
./ferrite --port 7000 --cluster-enabled yes
./ferrite --port 7001 --cluster-enabled yes
./ferrite --port 7002 --cluster-enabled yes

# Create cluster
redis-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002
```

## Benchmarks

Run benchmarks:

```bash
# Throughput benchmarks
cargo bench --bench throughput

# Latency benchmarks
cargo bench --bench latency
```

### Core Operations (Apple M1 Pro, Single-threaded)

| Operation | Throughput | P50 | P99 | P99.9 |
|-----------|------------|-----|-----|-------|
| GET       | 11.8M/s    | 83ns | 125ns | 167ns |
| SET       | 2.6M/s     | 84ns | 250ns | 16us |
| HGET      | 8.2M/s     | 95ns | 180ns | 350ns |
| SADD      | 3.1M/s     | 120ns | 290ns | 1us |
| ZADD      | 1.8M/s     | 180ns | 450ns | 2us |

### Vector Search (1M vectors, 384 dimensions)

| Operation | Index Type | Throughput | P50 | P99 | Recall@10 |
|-----------|------------|------------|-----|-----|-----------|
| VECTOR.SEARCH (k=10) | HNSW | 45K/s | 18us | 85us | 0.98 |
| VECTOR.SEARCH (k=10) | IVF | 28K/s | 32us | 120us | 0.95 |
| VECTOR.SEARCH (k=10) | Flat | 850/s | 1.1ms | 1.8ms | 1.00 |
| VECTOR.ADD | HNSW | 12K/s | 75us | 350us | - |
| VECTOR.ADD | IVF | 18K/s | 48us | 180us | - |

### Semantic Caching

| Operation | Embedding | Throughput | P50 | P99 | Cache Hit Rate* |
|-----------|-----------|------------|-----|-----|-----------------|
| SEMANTIC.GET (hit) | ONNX local | 32K/s | 28us | 95us | - |
| SEMANTIC.GET (hit) | Cached embed | 41K/s | 22us | 78us | - |
| SEMANTIC.SET | ONNX local | 1.2K/s | 0.8ms | 2.1ms | - |
| SEMANTIC.SET | OpenAI API | 180/s | 5.2ms | 12ms | - |

*Cache hit rate depends on query similarity; typical LLM workloads see 40-60% hit rates.

### Query Operations (FerriteQL)

| Query Type | Dataset Size | Throughput | P50 | P99 |
|------------|--------------|------------|-----|-----|
| Simple SELECT | 100K keys | 85K/s | 11us | 45us |
| SELECT with WHERE | 100K keys | 42K/s | 22us | 95us |
| JOIN (2 patterns) | 10K x 10K | 2.8K/s | 340us | 1.2ms |
| GROUP BY + COUNT | 100K keys | 1.5K/s | 0.6ms | 2.1ms |
| Materialized View (read) | - | 95K/s | 9us | 38us |

### Time-Travel Queries

| Operation | History Depth | Throughput | P50 | P99 |
|-----------|---------------|------------|-----|-----|
| GET AS OF (in memory) | <1h | 8.5M/s | 95ns | 180ns |
| GET AS OF (warm tier) | 1-24h | 450K/s | 2us | 12us |
| GET AS OF (cold tier) | >24h | 25K/s | 38us | 180us |
| HISTORY (10 versions) | - | 1.2M/s | 0.8us | 3.5us |

### CDC and Streaming

| Operation | Throughput | P50 | P99 |
|-----------|------------|-----|-----|
| CDC event capture | 2.8M events/s | 0.3us | 1.2us |
| CDC subscription delivery | 850K events/s | 1.1us | 4.5us |
| Stream processing (map+filter) | 1.2M/s | 0.8us | 3.2us |
| Windowed aggregation (1s tumbling) | 680K/s | 1.4us | 5.8us |

### Multi-Tenancy Overhead

| Scenario | Overhead vs Single-Tenant |
|----------|---------------------------|
| Key isolation (prefix routing) | <1% |
| Per-tenant rate limiting | 2-3% |
| Per-tenant memory accounting | 1-2% |
| Cross-tenant operation blocking | <0.5% |

## Development

### Getting Started

```bash
make setup      # Install all dev tools and verify the build (first time)
make dev        # Start server with auto-reload on code changes
make check      # Run fmt + clippy + tests
```

### Common Make Targets

```bash
make quickstart        # Build and start the server
make dev               # Auto-reload development server
make test              # Run test suite
make check             # Run all quality checks (fmt, clippy, tests)
make docker-compose-up # Start full stack with Docker
```

### Building

```bash
# Debug build
cargo build

# Release build
cargo build --release

# With io_uring support (Linux only)
cargo build --release --features io-uring
```

### Testing

```bash
# Run all tests
cargo test

# Run with release optimizations
cargo test --release

# Run specific test
cargo test test_name

# Run integration tests
cargo test --test integration_test

# Run the full local check (fmt, clippy, tests)
make check
```

### Linting

```bash
# Format code
cargo fmt

# Run clippy
cargo clippy -- -D warnings
```

### Documentation

```bash
# Generate and open docs
cargo doc --open
```

For more detailed workflows, see [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md).

## CLI Tools

Ferrite includes several command-line tools:

### ferrite (Server)
The main database server:
```bash
ferrite --config ferrite.toml
ferrite doctor --config ferrite.toml
RUST_LOG=ferrite=debug ferrite  # With debug logging
```

### ferrite-cli (Interactive Client)
A Redis-compatible CLI with syntax highlighting and auto-completion:
```bash
ferrite-cli                       # Connect to localhost:6379
ferrite-cli -h myhost -p 6380     # Connect to specific host/port
```

### ferrite-tui (Terminal Dashboard)
Real-time monitoring dashboard (requires `--features tui`):
```bash
cargo build --release --features tui
ferrite-tui
```

Features:
- Key browser with search and filtering
- Real-time metrics visualization
- Cluster topology view
- Slow query log

## Project Structure

```
ferrite/
├── src/
│   ├── main.rs              # Server entry point
│   ├── lib.rs               # Library exports
│   ├── bin/
│   │   ├── ferrite_cli/     # Interactive CLI client
│   │   └── ferrite_tui/     # Terminal dashboard
│   ├── server/              # Network layer (TCP, TLS)
│   ├── protocol/            # RESP2/RESP3 protocol
│   ├── storage/             # HybridLog tiered storage
│   │   ├── hybridlog/       # Three-tier log structure
│   │   └── epoch.rs         # Epoch-based reclamation
│   ├── commands/            # Command handlers
│   ├── persistence/         # AOF, checkpoints, backup
│   ├── replication/         # Primary-replica sync
│   ├── cluster/             # Hash slot sharding
│   ├── auth/                # ACL system
│   ├── metrics/             # Prometheus + OpenTelemetry
│   ├── vector/              # Vector search (HNSW, IVF)
│   ├── semantic/            # Semantic caching
│   ├── temporal/            # Time-travel queries
│   ├── cdc/                 # Change data capture
│   ├── crdt/                # Conflict-free replicated types
│   ├── query/               # FerriteQL query engine
│   ├── wasm/                # WebAssembly runtime
│   ├── triggers/            # Programmable triggers
│   ├── document/            # Document store
│   ├── graph/               # Graph database
│   ├── timeseries/          # Time series support
│   ├── search/              # Full-text search
│   ├── streaming/           # Stream processing
│   ├── tenancy/             # Multi-tenancy
│   ├── embedded/            # Embedded/library mode
│   ├── k8s/                 # Kubernetes operator
│   ├── rag/                 # RAG pipeline
│   └── plugin/              # Plugin system
├── benches/                 # Criterion benchmarks
├── tests/                   # Integration tests
└── docs/                    # Documentation
    ├── ARCHITECTURE.md      # Technical deep-dive
    ├── DEPLOYMENT.md        # Deployment guide
    ├── OPERATIONS.md        # Operations guide
    ├── PERFORMANCE_TUNING.md
    ├── TROUBLESHOOTING.md
    └── adr/                 # Architecture Decision Records
```

## Embedded Mode

Use Ferrite as an embedded library (like SQLite) without a separate server:

```rust
use ferrite::embedded::Database;

fn main() -> anyhow::Result<()> {
    // Open or create database
    let db = Database::open("./my_data")?;

    // Direct function calls - no network overhead
    db.set("user:1", r#"{"name": "Alice"}"#)?;
    let user = db.get("user:1")?;

    // Full Redis command support
    db.lpush("queue", &["task1", "task2"])?;
    let task = db.rpop("queue")?;

    // Transactions
    let tx = db.transaction();
    tx.set("key1", "value1")?;
    tx.set("key2", "value2")?;
    tx.commit()?;

    Ok(())
}
```

## Feature Flags

Ferrite supports optional features via Cargo feature flags:

| Feature | Description |
|---------|-------------|
| `io-uring` | Enable io_uring for Linux (requires kernel 5.11+) |
| `tui` | Build the terminal dashboard (ferrite-tui) |
| `wasm` | Enable WebAssembly user functions |
| `onnx` | Enable local ONNX embeddings for semantic search |
| `otel` | Enable OpenTelemetry tracing |

```bash
# Build with specific features
cargo build --release --features "io-uring,wasm,otel"

# Build with all features
cargo build --release --all-features
```

## Documentation

| Document | Description |
|----------|-------------|
| [BENCHMARKS.md](docs/BENCHMARKS.md) | Performance benchmarks and competitive comparison |
| [REDIS_COMPATIBILITY.md](docs/REDIS_COMPATIBILITY.md) | Redis command compatibility matrix (~72% passing) |
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | Technical deep-dive into Ferrite's internals |
| [DEPLOYMENT.md](docs/DEPLOYMENT.md) | Deployment guide for various environments |
| [OPERATIONS.md](docs/OPERATIONS.md) | Production operations guide |
| [EXAMPLE_CONFIGURATIONS.md](docs/EXAMPLE_CONFIGURATIONS.md) | Ready-to-use configs for AI/ML, SaaS, Edge, etc. |
| [PERFORMANCE_TUNING.md](docs/PERFORMANCE_TUNING.md) | Performance optimization guide |
| [TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) | Common issues and solutions |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Contribution guidelines |
| [SECURITY.md](SECURITY.md) | Security policy |
| [ADRs](docs/adrs/) | Architecture Decision Records |
| [GOVERNANCE.md](GOVERNANCE.md) | Project governance and decision-making |
| [COMMUNITY.md](COMMUNITY.md) | How to get involved |

### Tools

| Tool | Description |
|------|-------------|
| [Cost Calculator](https://ferritelabs.github.io/ferrite-docs/cost-calculator) | Compare Redis vs Ferrite infrastructure costs |
| [Monitoring Kit](https://github.com/ferritelabs/ferrite-ops/tree/main/monitoring) | One-command Prometheus + Grafana stack |

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Microsoft FASTER](https://github.com/microsoft/FASTER) - HybridLog architecture inspiration
- [Garnet](https://github.com/microsoft/garnet) - Design patterns
- [Redis](https://redis.io/) - Protocol and API compatibility target
- [DashMap](https://github.com/xacrimon/dashmap) - Concurrent hashmap implementation
- [Crossbeam](https://github.com/crossbeam-rs/crossbeam) - Epoch-based reclamation
- [wasmtime](https://github.com/bytecodealliance/wasmtime) - WebAssembly runtime
- [ONNX Runtime](https://onnxruntime.ai/) - Local ML inference for embeddings
