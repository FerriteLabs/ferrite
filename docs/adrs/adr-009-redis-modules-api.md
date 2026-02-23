# ADR-009: Redis Modules API Compatibility

## Status

**Proposed** — February 2026

## Context

Redis Modules (introduced in Redis 4.0) are dynamically loaded shared libraries that extend Redis with new commands, data types, and hooks. The ecosystem includes production-grade modules like RedisJSON, RediSearch, RedisGraph (deprecated), and RedisTimeSeries. If Ferrite could load existing Redis modules, it would instantly gain access to a mature ecosystem without reimplementing everything.

## Decision

Investigate a phased approach to Redis Modules API compatibility.

### Phase 1: Module API Shim (FFI Layer)

Implement a subset of the Redis Modules API (`redismodule.h`) as a Rust FFI layer:

```
┌─────────────────────┐
│  Redis Module (.so)  │  ← Existing compiled module
│  (C ABI)             │
└──────────┬──────────┘
           │ FFI calls
           ▼
┌─────────────────────┐
│  Ferrite Module API  │  ← Rust implementation of redismodule.h
│  (C ABI shim)        │
└──────────┬──────────┘
           │ Safe Rust
           ▼
┌─────────────────────┐
│  Ferrite Core        │
│  (Store, Commands)   │
└─────────────────────┘
```

**Critical API surface to implement first:**
- `RedisModule_Init` — Module registration
- `RedisModule_CreateCommand` — Register new commands
- `RedisModule_ReplyWith*` — Response encoding
- `RedisModule_OpenKey`, `RedisModule_KeyType`, `RedisModule_StringDMA` — Key access
- `RedisModule_Call` — Call other Redis commands

**Estimated effort:** 4-8 weeks for basic command registration + key access

### Phase 2: Native Module SDK (Rust-first)

Provide a Rust-native module SDK that's ergonomic and safe:

```rust
use ferrite_module::prelude::*;

#[ferrite_module]
fn my_module(ctx: &ModuleContext) -> ModuleResult {
    ctx.create_command("MYMOD.HELLO", hello_handler, "readonly")?;
    Ok(())
}

fn hello_handler(ctx: &CommandContext, args: &[FerryArg]) -> CommandResult {
    ctx.reply_string("Hello from my module!")
}
```

### Risks

- **ABI compatibility**: Redis modules assume specific memory layout and threading model
- **Thread safety**: Redis is single-threaded; Ferrite is multi-threaded. Modules may not be thread-safe
- **Version drift**: Redis Modules API evolves across versions (4.0, 6.0, 7.0, 7.2)
- **Legal**: Some modules are source-available (SSPL/RSALv2), not open source

### Recommendation

Start with the **Rust-native SDK** (Phase 2) as it's safer and more aligned with Ferrite's architecture. Defer the C FFI shim until there's clear user demand for loading specific existing modules.

## Consequences

- Rust-native SDK is lower risk and produces better modules
- C FFI shim opens the Redis module ecosystem but carries safety/compatibility risks
- Either approach makes Ferrite a platform, not just a database
