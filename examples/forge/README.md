# Ferrite Forge — Example Modules

This directory contains example [Forge](../../crates/ferrite-forge/) WASM component
modules that demonstrate how to extend Ferrite with user-defined functions.

Every module is compiled against the **`ferrite:fn@0.1.0`** WIT world defined in
[`crates/ferrite-forge/wit/ferrite.wit`](../../crates/ferrite-forge/wit/ferrite.wit).

## Prerequisites

```bash
# Install the cargo-component toolchain
cargo install cargo-component
```

## Example Modules

| Module | Description |
|--------|-------------|
| [`rate_limit`](rate_limit/) | Token-bucket rate limiter per key |
| [`jwt_verify`](jwt_verify/) | RFC 7519 JWT verification with KV-cached JWKS |
| [`json_patch`](json_patch/) | RFC 6902 JSON Patch on KV-stored JSON values |
| [`custom_merge`](custom_merge/) | Last-write-wins-by-source-rank merge for Chronicle |
| [`hot_keys`](hot_keys/) | Streaming top-K hot key detector (space-saving) |

## Quick Start

```bash
# Build any module
cd rate_limit
cargo component build --release

# Load into Ferrite
redis-cli FN.LOAD rate_limit $(cat target/wasm32-wasip2/release/ferrite_fn_rate_limit.wasm | base64)

# Invoke
redis-cli FN.CALL rate_limit user:42 '{"capacity":10,"refill_rate":1}'
```

## Writing Your Own Module

1. Copy any example directory as a template.
2. Implement the `process(input) -> result<output, error>` export.
3. Use the `kv`, `time`, and `log` host imports as needed.
4. Build with `cargo component build --release`.
5. Deploy with `FN.LOAD <name> <bytes>`.
