# validate_email — Ferrite WASM UDF

A simple email validation user-defined function compiled to WebAssembly for
use with Ferrite.

## What It Does

Validates that a string is a plausible email address by checking:

- Presence of exactly one `@` symbol
- Non-empty local and domain parts
- Domain contains at least one `.`
- Top-level domain is ≥ 2 characters
- No consecutive dots or leading/trailing special characters

## Building

```bash
# Install the WASM target (one-time setup)
rustup target add wasm32-unknown-unknown

# Build the module
cd examples/wasm_udf/validate_email
cargo build --target wasm32-unknown-unknown --release
```

The resulting `.wasm` file will be at:

```
target/wasm32-unknown-unknown/release/ferrite_udf_validate_email.wasm
```

## Deploying to Ferrite

```
FUNCTION.LOAD validate_email ./ferrite_udf_validate_email.wasm
```

## Usage

```
FCALL validate_email 0 "alice@example.com"
# => 1  (valid)

FCALL validate_email 0 "not-an-email"
# => 0  (invalid)
```

## Manifest

See [`manifest.toml`](manifest.toml) for the `ferrite-plugin.toml` format
used by the Ferrite WASM marketplace.
