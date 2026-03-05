# WASM User-Defined Functions (UDF) Examples

Example WASM modules for Ferrite's plugin system.

## Examples

| Directory | Description |
|-----------|-------------|
| `validate_email/` | Email validation UDF — validates email format server-side |
| `json_transform/` | JSON transformation UDF — transform JSON values on read/write |

## Building

```bash
# Install WASM target
rustup target add wasm32-wasi

# Build a specific UDF
cd validate_email
cargo build --target wasm32-wasi --release

# The .wasm file will be in target/wasm32-wasi/release/
```

## Loading into Ferrite

```bash
# Load a WASM module
redis-cli FUNCTION LOAD ./target/wasm32-wasi/release/validate_email.wasm

# Call the function
redis-cli FCALL validate_email 1 user@example.com
```

## Creating Your Own UDF

1. Create a new Cargo project with `wasm32-wasi` target
2. Implement the Ferrite UDF interface (see `src/` for the shared types)
3. Build with `cargo build --target wasm32-wasi --release`
4. Load the `.wasm` file into a running Ferrite instance

See the [WASM Functions guide](../../docs/WASM_MARKETPLACE.md) for full documentation.
