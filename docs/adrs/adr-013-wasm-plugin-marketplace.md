# ADR-013: WASM Plugin Marketplace

## Status
Proposed

## Context

Ferrite already supports WebAssembly user-defined functions (UDFs) via wasmtime.
Users can load custom WASM modules that execute at the data layer with near-native
performance. However, the current model requires users to:

1. Write their own WASM modules in Rust (or C/Go/AssemblyScript)
2. Compile to `wasm32-unknown-unknown`
3. Manually load via `FUNCTION.LOAD`

This creates a high barrier to entry. A **plugin marketplace** would allow:
- Community-contributed data transformations
- Pre-built modules for common patterns (validation, enrichment, routing)
- Curated, security-reviewed plugins with versioning

## Decision

### Architecture

```
┌─────────────────────────────────────────────────────┐
│                 Ferrite Plugin Registry              │
│  (GitHub-hosted, static JSON index + WASM binaries)  │
├─────────────────────────────────────────────────────┤
│                                                     │
│  registry.json                                      │
│  ├── validate_email@1.0.0.wasm                      │
│  ├── json_transform@1.0.0.wasm                      │
│  ├── rate_limiter@1.0.0.wasm                        │
│  └── geo_fence@1.0.0.wasm                           │
│                                                     │
└─────────────────────────────────────────────────────┘
         ▲
         │ HTTPS fetch
         │
┌────────┴────────────────────────────────────────────┐
│              Ferrite Server                          │
│                                                     │
│  PLUGIN.INSTALL validate_email                      │
│  PLUGIN.LIST                                        │
│  PLUGIN.INFO validate_email                         │
│  PLUGIN.REMOVE validate_email                       │
│                                                     │
└─────────────────────────────────────────────────────┘
```

### Plugin Specification

Each plugin declares metadata in a `plugin.toml`:

```toml
[plugin]
name = "validate_email"
version = "1.0.0"
description = "RFC 5322 email address validation"
author = "ferritelabs"
license = "Apache-2.0"
min_ferrite_version = "0.2.0"

[exports]
functions = ["validate_email"]

[permissions]
# No key access needed (pure function)
keys = false
network = false
```

### Security Model

- All marketplace plugins are WASM-sandboxed (no filesystem, no network by default)
- Permission escalation requires explicit user approval (`PLUGIN.INSTALL ... --allow-keys`)
- Fuel-limited execution (CPU budget per invocation)
- Memory-limited (configurable per-plugin max heap)
- Registry plugins are signed with a maintainer key

### Implementation Phases

1. **Phase 1**: Static GitHub-hosted registry with `PLUGIN.INSTALL <url>` command
2. **Phase 2**: `PLUGIN.SEARCH` / `PLUGIN.LIST` commands querying the registry
3. **Phase 3**: Community submission process (PR-based, with automated security scanning)
4. **Phase 4**: Plugin dependency resolution and version pinning

## Consequences

### Positive
- Dramatically lowers the barrier to WASM UDF adoption
- Creates a community ecosystem around Ferrite
- Pre-built solutions for common patterns reduce boilerplate
- Security model prevents supply-chain attacks

### Negative
- Registry maintenance overhead
- Plugin compatibility across Ferrite versions
- Trust model for community plugins needs careful design
- Review process could become a bottleneck

## References
- [wasmtime security model](https://docs.wasmtime.dev/security.html)
- [Extism (universal plugin system)](https://extism.org/)
- [Redis Modules](https://redis.io/docs/modules/)
