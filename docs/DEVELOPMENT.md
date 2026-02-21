# Development Guide

## Quick Setup

```bash
make setup      # Install tools, verify build (first time only)
make dev        # Start server with auto-reload
make check      # Run fmt + clippy + tests
```

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
