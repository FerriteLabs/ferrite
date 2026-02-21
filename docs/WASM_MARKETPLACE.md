# WASM UDF Marketplace

Ferrite's WASM marketplace enables community-driven extensions without recompilation.

> 🔬 **Status: Experimental** — The marketplace API is under active development.

## Overview

User-Defined Functions (UDFs) run in sandboxed WebAssembly runtimes, providing:
- **Safe execution** — Memory and CPU limits enforced per function
- **Hot reload** — Deploy new functions without server restart
- **Language agnostic** — Write UDFs in Rust, Go, C, AssemblyScript, or any WASM-targeting language
- **Marketplace** — Discover, install, and share UDFs

## Quick Start

```bash
# Search available UDFs
WASM.MARKETPLACE.SEARCH "rate"

# Get details
WASM.MARKETPLACE.INFO rate-limiter

# Install
WASM.MARKETPLACE.INSTALL rate-limiter

# List installed
WASM.MARKETPLACE.LIST

# Use the UDF
WASM.EXEC rate-limiter '{"key": "api:user:1", "limit": 100, "window": 60}'
```

## Available UDFs

| Name | Description | Version | Source |
|------|------------|---------|--------|
| `rate-limiter` | Token bucket rate limiting | 1.0.0 | Official |
| `bloom-filter` | Probabilistic set membership | 1.0.0 | Official |
| `json-transform` | JSONPath transformations | 1.0.0 | Official |
| `geo-fence` | Geographic boundary checking | 0.9.0 | Community |
| `dedup` | Stream deduplication | 1.0.0 | Official |
| `aggregator` | Sliding window aggregation | 0.8.0 | Community |
| `validator` | Schema validation for values | 1.0.0 | Official |
| `encryption` | Field-level encryption | 1.0.0 | Official |

## Writing Custom UDFs

### Rust

```rust
// my_udf/src/lib.rs
use ferrite_wasm_sdk::*;

#[ferrite_udf]
fn my_function(input: &str) -> Result<String, Error> {
    let parsed: serde_json::Value = serde_json::from_str(input)?;
    // Your logic here
    Ok(result.to_string())
}
```

### Building

```bash
cargo build --target wasm32-wasi --release
# Output: target/wasm32-wasi/release/my_udf.wasm
```

### Publishing

```bash
WASM.LOAD my_udf /path/to/my_udf.wasm
```

## Command Reference

| Command | Description |
|---------|------------|
| `WASM.MARKETPLACE.SEARCH <query>` | Search for available UDFs |
| `WASM.MARKETPLACE.INFO <name>` | Get UDF details |
| `WASM.MARKETPLACE.INSTALL <name>` | Install a UDF from marketplace |
| `WASM.MARKETPLACE.LIST` | List installed UDFs |
| `WASM.LOAD <name> <path>` | Load a local WASM file |
| `WASM.EXEC <name> <args>` | Execute a loaded UDF |
| `WASM.UNLOAD <name>` | Unload a UDF |
| `WASM.LIST` | List all loaded functions |
