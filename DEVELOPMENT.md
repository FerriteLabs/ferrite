# Development Guide

Detailed development workflows, debugging tips, and testing guidance for Ferrite contributors.

> **Quick start?** See [CONTRIBUTING_QUICKSTART.md](CONTRIBUTING_QUICKSTART.md) for a 5-minute setup.

## Local Development Loop

### Fast Iteration (Recommended)

```bash
# One-time setup
make setup

# Fast inner loop (~6 seconds)
make test-fast       # Unit tests only
make fmt && make lint # Format + clippy

# Full quality gate (~45-60 seconds)
make check           # fmt-check + lint + test + cargo check
```

### Auto-Reload Development

```bash
make dev             # Auto-reload server with debug logging
make dev-test        # Watch mode — re-runs tests on file change
```

## Running Tests

### Test Tiers

| Command | Scope | Time | When to Use |
|---------|-------|------|-------------|
| `make test-fast` | Unit tests (`--lib`) | ~6s | Every save |
| `make test` | All tests | ~30s | Before commit |
| `make test-release` | Release mode tests | ~60s | Before PR |
| `cargo test -p ferrite-core` | Single crate | ~10s | Focused work |
| `cargo test test_name` | Single test | <1s | Debugging |

### Running Integration Tests

Integration tests start an in-process server and communicate over TCP:

```bash
cargo test --test server_e2e --release
cargo test --test redis_compatibility --release
```

### Running Jepsen/Consistency Tests

Requires Docker for the multi-node environment:

```bash
# Start 3-node cluster with toxiproxy
docker compose -f tests/jepsen/docker-compose.jepsen.yml up -d

# Run consistency tests
cargo test --test jepsen_integration --release

# Tear down
docker compose -f tests/jepsen/docker-compose.jepsen.yml down
```

### Running Property-Based Tests

```bash
cargo test proptest           # All proptest files
cargo test --test proptest_redis   # Redis compatibility properties
cargo test --test proptest_crdt    # CRDT convergence properties
```

### Running Fuzz Tests

Fuzzing requires nightly Rust and `cargo-fuzz`:

```bash
# Install
rustup install nightly
cargo +nightly install cargo-fuzz

# List targets
cargo +nightly fuzz list

# Run a specific target (runs until stopped with Ctrl+C)
cargo +nightly fuzz run fuzz_resp_parser
cargo +nightly fuzz run fuzz_command_parser
cargo +nightly fuzz run fuzz_config_parser

# Run with a time limit
cargo +nightly fuzz run fuzz_resp_parser -- -max_total_time=300

# Available targets:
#   fuzz_resp_parser      — RESP protocol parsing robustness
#   fuzz_resp_roundtrip   — RESP encode→decode round-trip integrity
#   fuzz_command_parser   — Command parsing edge cases
#   fuzz_config_parser    — TOML config parsing safety
#   fuzz_gossip_message   — Cluster gossip protocol messages
#   fuzz_wal_record       — WAL record parsing reliability
```

### Running Benchmarks

```bash
make bench                     # All criterion benchmarks
make bench-throughput          # GET/SET throughput
make bench-latency             # P50/P99 latency

# Compare against Redis (requires Docker)
cd ../ferrite-bench
docker compose -f docker-compose.benchmark.yml up -d
./run_memtier_comparison.sh
```

## Test Coverage

```bash
make coverage                  # Generate coverage report (requires cargo-tarpaulin)
```

**Targets** (from `codecov.yml`):
- Global: 50% minimum
- `ferrite-core`: 60% minimum
- Patch coverage: 80% minimum

## Debugging

### Logging

```bash
RUST_LOG=ferrite=debug cargo run              # Debug logging
RUST_LOG=ferrite=trace cargo run              # Trace logging
RUST_LOG=ferrite::server=trace cargo run      # Module-specific
```

### Async Debugging

```bash
# Install tokio-console for async debugging
cargo install tokio-console
RUSTFLAGS="--cfg tokio_unstable" cargo run    # Enable tokio instrumentation
tokio-console                                  # In another terminal
```

### CPU Profiling

```bash
cargo install flamegraph
cargo flamegraph --release -- --port 6379
# Load test in another terminal, then view flamegraph.svg
```

### Memory Profiling (Linux)

```bash
heaptrack ./target/release/ferrite
heaptrack_print heaptrack.ferrite.*.zst
```

## CI Pipeline

When you open a PR, these checks run automatically:

| Check | Workflow | What It Does |
|-------|----------|-------------|
| **Build & Test** | `ci.yml` | `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test --workspace` |
| **Redis Compatibility** | `redis-compat.yml` | Runs Redis TCL test suite against Ferrite |
| **Benchmarks** | `benchmarks.yml` | Criterion benchmarks, posts delta comment on PR |
| **Fuzz** | `fuzz.yml` | Short fuzz run on 6 targets |
| **Links** | `links.yml` | Dead link detection in docs |
| **Security** | `scorecard.yml` | OpenSSF Scorecard + dependency audit |
| **SDK Tests** | `sdk-*.yml` | Language-specific SDK builds and tests |

**Required checks for merge**: Build & Test, Redis Compatibility.

### Running CI Locally

```bash
# Reproduce the full CI check locally
make check                    # fmt + clippy + test

# Redis compatibility specifically
cargo test --test redis_compatibility --release

# Security audit
make audit                    # cargo audit + cargo deny
```

## Common Issues

### Build Fails with `openssl` Error
```bash
# macOS
brew install openssl@3
export OPENSSL_DIR=$(brew --prefix openssl@3)

# Ubuntu/Debian
sudo apt-get install libssl-dev pkg-config
```

### `io_uring` Not Available
io_uring requires Linux 5.11+. On macOS, Ferrite automatically falls back to `tokio::fs`.

### Test Flakiness
Some tests use random ports. If you see `AddrInUse`, just re-run. The `start_test_server()` helper in `tests/common/mod.rs` handles port allocation.

## Architecture Quick Reference

```
ferrite/
├── src/                    # Top-level binary crate (integration layer)
│   ├── commands/           # Redis command implementations
│   ├── server/             # Network layer (TCP, TLS, HTTP, gRPC)
│   └── replication/        # Primary/replica replication
├── crates/
│   ├── ferrite-core/       # Foundation: storage, protocol, config, auth
│   ├── ferrite-search/     # Full-text search (BM25)
│   ├── ferrite-ai/         # Vector search, semantic caching, RAG
│   ├── ferrite-graph/      # Property graph (Cypher)
│   ├── ferrite-timeseries/ # Time-series ingestion
│   ├── ferrite-document/   # JSON document store
│   ├── ferrite-streaming/  # CDC, event streaming, Kafka compat
│   └── ...                 # See Cargo.toml for full list
├── tests/                  # Integration tests
├── benches/                # Criterion benchmarks
├── fuzz/                   # Fuzz testing targets
└── examples/               # Usage examples
```

**Key rule**: Extension crates are self-contained — they don't depend on `ferrite-core` or each other.
