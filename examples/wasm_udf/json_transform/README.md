# json_transform — Ferrite WASM UDF

Lightweight JSON transformation functions compiled to WebAssembly for use
with Ferrite.

## What It Does

Provides three exported functions:

| Function | Description |
|----------|-------------|
| `count_keys` | Count top-level keys in a JSON object |
| `has_field` | Check whether a field name exists in a JSON object |
| `extract_string_field` | Extract a string value for a given key |

All functions operate on UTF-8 JSON text passed through the shared memory
buffer without requiring a full JSON parser library, keeping the `.wasm`
binary small.

## Building

```bash
# Install the WASM target (one-time setup)
rustup target add wasm32-unknown-unknown

# Build the module
cd examples/wasm_udf/json_transform
cargo build --target wasm32-unknown-unknown --release
```

The resulting `.wasm` file will be at:

```
target/wasm32-unknown-unknown/release/ferrite_udf_json_transform.wasm
```

## Deploying to Ferrite

```
FUNCTION.LOAD json_transform ./ferrite_udf_json_transform.wasm
```

## Usage

```
# Count keys — writes JSON to buffer, then calls count_keys
FCALL count_keys 0 '{"a":1,"b":2,"c":3}'
# => 3

# Check if field exists
FCALL has_field 0 '{"name":"Alice"}' "name"
# => 1

# Extract a string field
FCALL extract_string_field 0 '{"city":"Berlin"}' "city"
# => "Berlin"
```

## Manifest

See [`manifest.toml`](manifest.toml) for the `ferrite-plugin.toml` format
used by the Ferrite WASM marketplace.
