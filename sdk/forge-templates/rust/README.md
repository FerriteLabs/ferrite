# Ferrite Forge — Rust Function Template

This is a [cargo-generate](https://github.com/cargo-generate/cargo-generate) template for
creating Ferrite Forge serverless functions in Rust.

## Prerequisites

```bash
# Install cargo-component (builds WASI P2 components)
cargo install cargo-component

# Ensure the wasm32-wasip2 target is available
rustup target add wasm32-wasip2
```

## Create a New Function

```bash
# Using cargo-generate
cargo generate --path sdk/forge-templates/rust --name my-function

# Or with the ferrite-fn CLI
ferrite-fn new my-function
```

## Project Structure

```
my-function/
├── Cargo.toml          # wit-bindgen dependency, cdylib output
└── src/
    └── lib.rs          # Implements the `function` world Guest trait
```

## Build

```bash
cd my-function
cargo component build --release
```

The compiled WASM component will be at:
```
target/wasm32-wasip2/release/my_function.wasm
```

## Deploy

```bash
# Load into a running Ferrite instance
ferrite-cli fn load my-function target/wasm32-wasip2/release/my_function.wasm

# Or via the Redis protocol
redis-cli -p 6379 FN.LOAD my-function /path/to/my_function.wasm
```

## Host Imports

Inside your `process` function you can call the host-provided imports:

| Import          | Description                      |
|-----------------|----------------------------------|
| `kv::get(key)`  | Read a key from the Ferrite store |
| `kv::set(k, v)` | Write a key to the Ferrite store  |
| `time::now_ms`  | Current wall-clock time (ms)     |
| `log::info(msg)`| Emit an info-level log line      |

## Testing Locally

You can unit-test your function logic with standard `cargo test`.
The WIT bindings are only used at component build time, so pure-logic
tests work without a Ferrite instance.
