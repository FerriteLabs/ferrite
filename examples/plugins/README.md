# Example WASM Plugin Manifests

These demonstrate the `plugin.toml` format used by Ferrite's WASM marketplace.

## Quick Start

```bash
# Install a plugin from the marketplace
redis-cli PLUGIN INSTALL rate-limiter

# List installed plugins
redis-cli PLUGIN LIST

# Remove a plugin
redis-cli PLUGIN REMOVE rate-limiter

# Get help
redis-cli PLUGIN HELP
```

## Directory Layout

A plugin directory should contain:

```
my-plugin/
├── plugin.toml       # Manifest (required)
├── plugin.wasm       # Compiled WASM module (required)
└── README.md         # Usage documentation (optional)
```

## Available Examples

1. **rate-limiter.toml** — Sliding window rate limiter command
2. **bloom-filter.toml** — Space-efficient probabilistic set membership
3. **json-path.toml** — JSONPath query support for JSON values

## Building a Plugin

### Prerequisites

- Rust toolchain with `wasm32-wasi` target: `rustup target add wasm32-wasi`
- The Ferrite Plugin SDK (coming soon via crates.io)

### Steps

```bash
# 1. Create a new Rust library
cargo new --lib my-plugin
cd my-plugin

# 2. Add wasm32-wasi target to Cargo.toml
# [lib]
# crate-type = ["cdylib"]

# 3. Build as WASM
cargo build --target wasm32-wasi --release

# 4. The output is at target/wasm32-wasi/release/my_plugin.wasm
cp target/wasm32-wasi/release/my_plugin.wasm plugin.wasm

# 5. Create plugin.toml (see examples in this directory)

# 6. Test locally
redis-cli PLUGIN INSTALL ./my-plugin
```

### Plugin Manifest Reference

```toml
[plugin]
name = "my-plugin"              # Unique identifier
display_name = "My Plugin"      # Human-readable name
version = "1.0.0"               # Semantic version
description = "What it does"    # Short description
author = "Your Name"
license = "Apache-2.0"          # SPDX identifier
min_ferrite_version = "0.2.0"   # Minimum compatible Ferrite version
sdk_version = "1.0.0"           # Plugin SDK version
type = "Command"                # Command | DataType | StorageBackend | Connector | Extension

[plugin.commands]
"MY.CMD" = { arity = 2, description = "Description of the command" }

[plugin.permissions]
read_keys = true                # Can read Ferrite keys
write_keys = true               # Can write Ferrite keys
network = false                 # Can make network calls
filesystem = false              # Can access filesystem

[plugin.resources]
max_memory_mb = 16              # WASM memory limit
max_cpu_ms_per_call = 10        # CPU time limit per invocation
max_fuel = 100000               # Wasmtime fuel limit
```
