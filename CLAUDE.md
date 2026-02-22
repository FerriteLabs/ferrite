# Ferrite - Claude Code Project Guide

## Project Overview

Ferrite is a high-performance, tiered-storage key-value store designed as a drop-in Redis replacement. Built in Rust with epoch-based concurrency and io_uring-first persistence.

**Tagline**: *The speed of memory, the capacity of disk, the economics of cloud.*

## Quick Reference

```bash
# Build
cargo build --release

# Build a single crate
cargo build -p ferrite-core
cargo build -p ferrite-ai

# Test
cargo test --workspace
cargo test -p ferrite-core       # Test single crate
cargo test --release             # Run with optimizations

# Benchmarks
cargo bench

# Lint & Format
cargo fmt --all --check
cargo clippy --workspace -- -D warnings

# Run server
cargo run --release -- --config ferrite.toml

# Run with debug logging
RUST_LOG=ferrite=debug cargo run

# Generate docs
cargo doc --workspace --open

# Check a single crate compiles
cargo check -p ferrite-search
```

## Architecture Overview

This is a **Cargo workspace** with 12 crates under `crates/` plus the top-level binary crate.

```
ferrite/                          ← Workspace root + top-level binary crate
├── Cargo.toml                    # Workspace config + [workspace.dependencies]
├── src/
│   ├── main.rs                   # Entry point, CLI parsing
│   ├── lib.rs                    # Re-exports from all workspace crates
│   ├── commands/                 # Redis command implementations (integration layer)
│   │   ├── executor.rs           # Command dispatch — imports from all extension crates
│   │   ├── parser.rs             # RESP command parsing
│   │   ├── handlers/             # Per-feature command handlers
│   │   ├── strings.rs, lists.rs, hashes.rs, sets.rs, ...
│   │   └── scripting.rs          # Lua scripting (optional)
│   ├── server/                   # Network layer (TCP listener, TLS, request routing)
│   ├── replication/              # Primary/replica replication, geo-replication
│   ├── bin/                      # Additional binaries (ferrite-cli, ferrite-tui, etc.)
│   ├── sdk/                      # Client SDK helpers
│   └── migration/                # Data migration tools
├── crates/
│   ├── ferrite-core/             # Core engine (Tier 1+2: storage, protocol, persistence, auth, cluster, ...)
│   ├── ferrite-search/           # Full-text search, query routing, schema, auto-indexing
│   ├── ferrite-ai/               # Vector search, semantic caching, RAG, embeddings
│   ├── ferrite-graph/            # Graph data model and traversal
│   ├── ferrite-timeseries/       # Time-series ingestion and downsampling
│   ├── ferrite-document/         # JSON document store
│   ├── ferrite-streaming/        # Event streaming, CDC, pipelines
│   ├── ferrite-cloud/            # Cloud storage (S3/GCS/Azure), serverless, edge
│   ├── ferrite-k8s/              # Kubernetes operator
│   ├── ferrite-enterprise/       # Multi-tenancy, governance, federation
│   ├── ferrite-plugins/          # Plugin system, WASM, CRDTs
│   └── ferrite-studio/           # Web management UI and playground
├── benches/                      # Criterion benchmarks
├── tests/                        # Integration tests
└── examples/                     # Usage examples
```

### Dependency DAG (no circular deps)

```
ferrite-core             (foundation: storage, protocol, config, error, auth, cluster, ...)
    ↑ (no deps on core)
ferrite-search, ferrite-ai, ferrite-graph, ...  (self-contained extension crates)
    ↑
ferrite (top-level)      (commands/, server/, replication/ — depends on all crates)
```

### Key design rules
- **Extension crates are self-contained** — they don't depend on ferrite-core or each other
- **commands/ and server/ are in the top-level crate** because they're the integration layer that imports from all extensions
- **Shared dependency versions** are defined in `[workspace.dependencies]` in the root Cargo.toml
- All crates inherit `version`, `edition`, `authors`, `license`, `lints` from the workspace

## Core Technical Concepts

### HybridLog (from Microsoft FASTER)
The storage engine uses a three-tier log structure:
- **Mutable Region** (Memory): Hot data, in-place updates, lock-free reads
- **Read-Only Region** (mmap): Warm data, zero-copy reads, copy-on-write
- **Head/Disk Region** (io_uring): Cold data, async sequential I/O

### Epoch-Based Reclamation (EBR)
Thread-safe memory reclamation without garbage collection:
- Threads "pin" an epoch when accessing shared data
- Memory is reclaimed only when all threads have moved past that epoch
- Use `crossbeam-epoch` crate for implementation

### Thread-Per-Core Architecture
- Each CPU core has a dedicated thread
- Shards are assigned to threads (no cross-thread locks)
- Each thread has its own io_uring instance
- Key routing: `shard = hash(key) % num_shards`

## Coding Conventions

### Rust Style
- Follow Rust API Guidelines: https://rust-lang.github.io/api-guidelines/
- Use `rustfmt` with default settings
- All public items must have documentation
- Prefer `Result<T, E>` over panics
- Use `thiserror` for library errors, `anyhow` for application errors

### Naming
- Types: `PascalCase` (e.g., `HybridLog`, `CommandParser`)
- Functions/methods: `snake_case` (e.g., `parse_command`, `get_value`)
- Constants: `SCREAMING_SNAKE_CASE` (e.g., `MAX_KEY_SIZE`)
- Modules: `snake_case` (e.g., `hybrid_log`, `command_parser`)

### Error Handling
```rust
// Define errors with thiserror
#[derive(Debug, thiserror::Error)]
pub enum FerriteError {
    #[error("key not found: {0}")]
    KeyNotFound(String),

    #[error("invalid command: {0}")]
    InvalidCommand(String),

    #[error("storage error: {0}")]
    Storage(#[from] std::io::Error),
}

// Use Result alias
pub type Result<T> = std::result::Result<T, FerriteError>;
```

### Async Patterns
```rust
// Use tokio for networking
use tokio::net::TcpListener;

// Use tokio-uring for file I/O (Linux only)
#[cfg(target_os = "linux")]
use tokio_uring::fs::File;

// Graceful fallback for non-Linux
#[cfg(not(target_os = "linux"))]
use tokio::fs::File;
```

### Unsafe Code Guidelines
- **Minimize**: Only use `unsafe` when absolutely necessary for performance
- **Document**: Every `unsafe` block must have a `// SAFETY:` comment explaining:
  - What invariants must be upheld
  - Why those invariants are satisfied
  - What could go wrong if they're violated
- **Isolate**: Wrap unsafe operations in safe abstractions
- **Test**: All unsafe code must have corresponding tests

```rust
// SAFETY: We have exclusive access to this memory region because:
// 1. The epoch system guarantees no other thread holds a reference
// 2. The address was returned from our allocator in this epoch
// 3. The buffer is properly aligned for T
unsafe {
    std::ptr::write(addr as *mut T, value);
}
```

### Performance-Critical Code
- Use `#[inline]` for small, hot functions
- Use `#[cold]` for error paths
- Prefer stack allocation for small buffers
- Use `bytes::Bytes` for zero-copy buffer sharing
- Profile before optimizing (use `cargo flamegraph`)

## Key Dependencies

| Crate | Purpose | Notes |
|-------|---------|-------|
| `tokio` | Async runtime | Use `features = ["full"]` |
| `tokio-uring` | io_uring I/O | Linux 5.11+ required |
| `dashmap` | Concurrent HashMap | For in-memory index |
| `crossbeam-epoch` | Epoch-based reclamation | For lock-free reads |
| `bytes` | Buffer management | Zero-copy operations |
| `memmap2` | Memory-mapped files | For warm tier |
| `tracing` | Structured logging | With `tracing-subscriber` |
| `metrics` | Prometheus metrics | With `metrics-exporter-prometheus` |
| `serde` | Serialization | With `serde_json` for config |
| `toml` | Config file parsing | |
| `thiserror` | Error definitions | |
| `criterion` | Benchmarking | Dev dependency |

## RESP Protocol Quick Reference

```
// Simple String: +OK\r\n
// Error: -ERR message\r\n
// Integer: :1000\r\n
// Bulk String: $6\r\nfoobar\r\n
// Array: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
// Null: $-1\r\n (bulk) or *-1\r\n (array)
```

## Testing Strategy

### Unit Tests
- Test individual components in isolation
- Use `#[cfg(test)]` modules
- Mock external dependencies

### Integration Tests
- Test full request/response cycles
- Use actual network connections
- Test persistence and recovery

### Redis Compatibility Tests
- Run official Redis test suite where applicable
- Test with multiple Redis client libraries
- Verify behavior matches Redis documentation

### Performance Tests
- Benchmark with `criterion`
- Compare against Redis using `memtier_benchmark`
- Track P50, P99, P99.9 latencies

## Common Tasks

### Adding a New Redis Command
1. Add command variant to `src/commands/parser.rs`
2. Add parser logic to `src/commands/parser.rs`
3. Implement handler in appropriate `src/commands/*.rs` or `src/commands/handlers/*.rs` module
4. If the command uses an extension crate, add the import from the crate (e.g., `use ferrite_ai::vector::...`)
5. Add tests in both unit and integration test files
6. Update command coverage documentation

### Adding a New Extension Module
1. Decide which crate it belongs to (or create a new one under `crates/`)
2. Add the module to the crate's `src/lib.rs`
3. If the module needs core types (Store, Config, etc.), it should go in `ferrite-core`
4. If it's self-contained, add it to the appropriate extension crate
5. Wire up command handlers in `src/commands/handlers/` if needed

### Adding a New Dependency
1. Add it to `[workspace.dependencies]` in the root `Cargo.toml` (single source of truth)
2. In the crate that needs it: `my-dep.workspace = true`
3. Never specify versions in individual crate Cargo.toml files

### Adding a New Metric
1. Define metric in `crates/ferrite-core/src/metrics/mod.rs`
2. Use `metrics` crate macros: `counter!`, `gauge!`, `histogram!`
3. Add to `/metrics` endpoint documentation

### Multi-Repo Organization
This repo is part of the [Ferrite Labs](https://github.com/ferritelabs) organization:
- **ferrite** (this repo) — Core database engine (Cargo workspace)
- **ferrite-docs** — Documentation website and markdown docs
- **ferrite-ops** — Docker, Helm, Grafana, packaging, scripts
- **vscode-ferrite** — VS Code extension
- **jetbrains-ferrite** — JetBrains IDE plugin
- **homebrew-tap** — Homebrew formula
- **ferrite-bench** — External performance benchmarks

### Debugging Tips
- Enable trace logging: `RUST_LOG=ferrite=trace`
- Use `tokio-console` for async debugging
- Use `perf` + `flamegraph` for CPU profiling
- Use `heaptrack` for memory profiling

## Performance Targets

| Metric | Target |
|--------|--------|
| GET throughput | >500K ops/sec/core |
| SET throughput | >400K ops/sec/core |
| P50 latency | <0.3ms |
| P99 latency | <1ms |
| P99.9 latency | <2ms |
| Memory overhead | <20% vs data size |

## References

- [FASTER Paper (Microsoft Research)](https://www.microsoft.com/en-us/research/uploads/prod/2018/03/faster-sigmod18.pdf)
- [Garnet (Microsoft)](https://github.com/microsoft/garnet)
- [Redis Protocol Specification](https://redis.io/docs/reference/protocol-spec/)
- [io_uring Documentation](https://kernel.dk/io_uring.pdf)
- [Epoch-Based Reclamation](https://aturon.github.io/blog/2015/08/27/epoch/)

## Development Philosophy

1. **Correctness First**: Get it right before making it fast
2. **Predictable Performance**: P99.9 matters more than P50
3. **No Magic**: Prefer explicit over implicit
4. **Test Everything**: Especially edge cases and error paths
5. **Document Why**: Code shows what, comments explain why
