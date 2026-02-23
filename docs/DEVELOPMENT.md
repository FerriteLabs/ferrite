# Development Guide

This guide helps you understand Ferrite's internals and start contributing effectively.

## Quick Setup

```bash
make setup      # Install tools, verify build (first time only)
make dev        # Start server with auto-reload
make check      # Run fmt + clippy + tests
```

## Architecture Overview

Ferrite is organized as a Cargo workspace with a layered dependency structure:

```
┌─────────────────────────────────────────────────┐
│  ferrite (top-level binary)                     │
│  src/commands/ src/server/ src/replication/      │
├─────────────────────────────────────────────────┤
│  Extension Crates (self-contained, no cross-deps)│
│  ferrite-ai   ferrite-search   ferrite-graph    │
│  ferrite-streaming  ferrite-plugins  ...        │
├─────────────────────────────────────────────────┤
│  ferrite-core (foundation)                      │
│  storage/  protocol/  config/  auth/  cluster/  │
└─────────────────────────────────────────────────┘
```

**Key principles:**
- Extension crates never depend on each other or on the top-level crate
- The top-level crate is the integration layer — it wires extensions into RESP commands
- All dependency versions live in the workspace root `Cargo.toml`

### Directory Map

| Path | Purpose |
|------|---------|
| `src/main.rs` | CLI entry point (clap subcommands) |
| `src/commands/` | RESP command parsing and dispatch |
| `src/commands/handlers/` | Per-feature command handlers |
| `src/server/` | TCP listener, connection, TLS, handler |
| `src/replication/` | Primary-replica replication |
| `crates/ferrite-core/src/storage/` | Storage backends (memory, HybridLog) |
| `crates/ferrite-core/src/protocol/` | RESP parser and encoder |
| `crates/ferrite-core/src/config.rs` | All configuration structs |
| `crates/ferrite-core/src/error.rs` | Centralized error types |
| `tests/` | Integration tests |
| `benches/` | Criterion benchmarks |
| `fuzz/` | libfuzzer targets |

### Request Flow

A client command like `SET mykey myvalue` flows through:

1. **`server/listener.rs`** — accepts TCP connection
2. **`server/connection.rs`** — reads bytes, calls RESP parser
3. **`protocol/parser.rs`** — parses RESP frame into `Frame` enum
4. **`commands/parser.rs`** — converts `Frame` into `Command` enum
5. **`commands/executor.rs`** — dispatches to the correct handler
6. **`commands/strings.rs`** — executes SET against the `Store`
7. **`server/connection.rs`** — encodes response and writes back

### Storage Architecture

```
Hot Tier:   MutableRegion (memory, append-only, lock-free reads)
                │ promotion when full
Warm Tier:  ReadOnlyRegion (mmap, read-only, compactable)
                │ promotion when full
Cold Tier:  DiskRegion (file I/O or io_uring, sequential reads)
                │ optional cloud tiering
Cloud Tier: S3/GCS/Azure (via object_store crate)
```

Each tier uses epoch-based reclamation for thread-safe memory management.

## Development Workflow

### Inner Loop (Fast Feedback)

```bash
make dev-test   # Watch for changes, run unit tests continuously
make test-fast  # Run unit tests only (~6 seconds)
```

### Full Validation

```bash
make check      # Format check + clippy + all tests
make bench      # Run performance benchmarks
```

### Working on a Single Crate

```bash
cargo test -p ferrite-core       # Test one crate
cargo clippy -p ferrite-search   # Lint one crate
cargo check -p ferrite-ai        # Type check one crate
```

## Adding New Features

### Adding a New Redis Command

1. Add command variant to `src/commands/parser.rs`
2. Add handler in `src/commands/handlers/` (or existing handler module)
3. Wire into `src/commands/executor.rs`
4. Add unit tests and integration tests
5. Update command coverage documentation

**Example:** See `src/commands/strings.rs` for a complete handler implementation.

### Adding a New Crate

1. Create `crates/ferrite-myfeature/`
2. Add to `[workspace.members]` in root `Cargo.toml`
3. Add workspace dependency: `ferrite-myfeature = { path = "crates/ferrite-myfeature" }`
4. Wire command handlers in `src/commands/handlers/`

### Adding a New Dependency

1. Add to `[workspace.dependencies]` in root `Cargo.toml` (single source of truth)
2. In the crate: `my-dep.workspace = true`
3. Never specify versions in individual crate `Cargo.toml` files

## Code Quality

```bash
cargo fmt --all           # Format code
cargo clippy --workspace  # Lint
cargo audit               # Security audit
cargo deny check          # License and advisory checks
cargo outdated            # Check for outdated deps
make coverage             # Generate coverage report
```

### Conventions

- Use `thiserror` for library errors, `anyhow` for application errors
- Every `unsafe` block must have a `// SAFETY:` comment
- All `#[allow(dead_code)]` must have a `// Planned for v0.2 — reason` justification
- Use `Result<T>` return types; avoid `unwrap()` in production code paths
- Run `make check` before every PR

## Debugging

```bash
RUST_LOG=ferrite=debug cargo run                    # Debug logging
RUST_LOG=ferrite=trace cargo run                    # Trace logging
RUST_BACKTRACE=1 cargo test -- --nocapture          # Backtrace on failures
```

## Profiling

```bash
cargo flamegraph                                    # CPU flamegraph
cargo build --release && perf record ./target/release/ferrite  # Linux perf
```

## Feature Flags

Build with specific features to reduce compile time during development:

```bash
cargo build                              # Default features (scripting, tls, cloud, crypto, cli)
cargo build --no-default-features        # Minimal build (fastest compile)
cargo build --features experimental      # Include K8s, enterprise, studio
cargo build --all-features               # Everything enabled
```
