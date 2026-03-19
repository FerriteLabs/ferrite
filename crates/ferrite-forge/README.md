# ferrite-forge

Forge — WASM in-database functions.  See [ADR-019](../../docs/adrs/adr-019-forge-wasm-functions.md)
and the [phase roadmap](../../docs/phases/m2-forge-roadmap.md).

This crate owns:
- The **registry** of loaded WASM modules (per ADR-019 module storage layout).
- The **engine factory** producing one `wasmtime::Engine` per worker thread.
- The **resource budget** type (fuel + memory + wall time) attached to each call.

The actual host-API binding set, the `FN.LOAD` / `FN.CALL` command handlers,
and replication of module state live in the top-level `ferrite` crate so they
can call into ferrite-core.  Forge stays storage- and protocol-agnostic.

Status: P0 spike — registry + budget types only.  The Wasmtime engine factory
is gated behind the `runtime` cargo feature so the workspace builds with no
wasmtime cost when only schema types are needed.
