# ADR-006: Wasmtime as the WASM Plugin Runtime

## Status

Accepted

## Context

Ferrite supports user-defined functions and plugins via WebAssembly. Several WASM runtimes exist in the Rust ecosystem:

- **Wasmtime** — Bytecode Alliance reference runtime, focus on security and correctness
- **Wasmer** — Focus on ease of use and broad language support
- **WasmEdge** — Focus on edge/cloud-native, WASI-NN support
- **Lunatic** — Actor-based, Erlang-inspired, not a general runtime

Key requirements for Ferrite's plugin system:

1. **Security** — Plugins run untrusted code; sandboxing must be airtight
2. **Performance** — Low overhead for hot-path plugin calls (e.g., triggers, custom commands)
3. **WASI support** — Plugins need filesystem and networking access (scoped)
4. **Fuel metering** — Ability to limit CPU time per plugin invocation
5. **Rust ecosystem integration** — First-class Rust API, `async` support
6. **Long-term maintenance** — Backed by a credible organization

## Decision

We use **Wasmtime** as the WASM plugin runtime.

Wasmtime provides:

- **Cranelift JIT/AOT compilation** with competitive performance
- **Fuel-based execution metering** for CPU limiting
- **Component Model support** for typed plugin interfaces
- **Epoch-based interruption** that aligns with Ferrite's own epoch model
- **WASI preview 2** support for structured capabilities
- **Bytecode Alliance backing** (Mozilla, Fastly, Intel, Microsoft)

## Consequences

### Positive

- Strong security guarantees from the reference implementation
- Fuel metering enables per-request CPU budgets for plugins
- Component Model enables typed, versioned plugin APIs
- Active maintenance with regular releases and CVE handling

### Negative

- Wasmtime has larger binary size than Wasmer (~5MB overhead)
- Slightly slower cold-start than Wasmer's Singlepass compiler
- Component Model is still evolving; API may change between major versions

### Mitigations

- WASM support is behind the `wasm` feature flag, so binary size is opt-in
- Instance pooling and pre-compilation amortize cold-start costs
- `ferrite-plugins` abstracts the runtime, allowing future migration if needed
