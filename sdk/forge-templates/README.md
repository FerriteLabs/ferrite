# Ferrite Forge — Function Templates

Project templates for building [Ferrite Forge](../../crates/ferrite-forge/) serverless
functions in multiple languages. Each function compiles to a WASI P2 component
that the Forge runtime loads and executes inside a sandboxed environment.

## Available Templates

| Language   | Directory | Toolchain             | Build command                              |
|------------|-----------|-----------------------|--------------------------------------------|
| **Rust**   | `rust/`   | cargo-component       | `cargo component build --release`          |
| **Go**     | `go/`     | TinyGo ≥ 0.33        | `tinygo build -o fn.wasm -target=wasip2`   |
| **TypeScript** | `ts/` | jco + componentize-js | `npm run build`                            |

## Quick Start

The fastest way to scaffold a new function is the `ferrite-fn` CLI:

```bash
# Rust (default)
ferrite-fn new my-function

# Go
ferrite-fn new my-function --lang go

# TypeScript
ferrite-fn new my-function --lang ts
```

Or use the templates directly:

```bash
# Rust — via cargo-generate
cargo generate --path sdk/forge-templates/rust --name my-function

# Go — copy and rename
cp -r sdk/forge-templates/go my-function

# TypeScript — copy, rename, and install
cp -r sdk/forge-templates/ts my-function && cd my-function && npm install
```

## Architecture

Every function implements the **`function`** WIT world defined in
`crates/ferrite-forge/wit/`. The world exports a single `process` function
and imports host capabilities:

```
┌──────────────────────────────────────┐
│         Your Function (WASM)         │
│  export: process(bytes) → bytes      │
│  imports: kv, time, log              │
└──────────┬───────────────────────────┘
           │  WASI P2 component model
┌──────────▼───────────────────────────┐
│      Ferrite Forge Runtime           │
│  (wasmtime, resource limits, ABI)    │
└──────────┬───────────────────────────┘
           │
┌──────────▼───────────────────────────┐
│        Ferrite KV Store              │
└──────────────────────────────────────┘
```

## Development Workflow

1. **Scaffold** — `ferrite-fn new <name> [--lang rust|go|ts]`
2. **Develop** — Edit the `process` function; write unit tests
3. **Build** — Compile to a `.wasm` component
4. **Deploy** — `ferrite-fn deploy localhost:6379 <name> [path]`
5. **Invoke** — `redis-cli FN.CALL <name> <arg>`

See each template's `README.md` for language-specific details.
