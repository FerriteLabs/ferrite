# Redis Module API Compatibility Layer

Ferrite provides a **Redis Module API compatibility layer** that enables existing Redis module *concepts* — commands, custom data types, and lifecycle management — to be expressed and executed within Ferrite's plugin system.

> **Note:** This layer does **not** load native Redis `.so` modules via `dlopen`. It provides a Rust-native API that mirrors the key abstractions of the Redis Module API so that module logic can be ported to, or written directly for, Ferrite.

---

## Supported API Surface

### Context (`RedisModuleCtx`)

| Function | Status | Notes |
|----------|--------|-------|
| `RedisModule_OpenKey` | ✅ Supported | Opens a key for read or write |
| `RedisModule_CloseKey` | ✅ Supported | Closes a key handle |
| `RedisModule_ReplyWithSimpleString` | ✅ Supported | |
| `RedisModule_ReplyWithError` | ✅ Supported | |
| `RedisModule_ReplyWithLongLong` | ✅ Supported | |
| `RedisModule_ReplyWithString` | ✅ Supported | |
| `RedisModule_ReplyWithStringBuffer` | ✅ Supported | |
| `RedisModule_ReplyWithNull` | ✅ Supported | |
| `RedisModule_ReplyWithArray` | ✅ Supported | |
| `RedisModule_CreateString` | ✅ Supported | Via `ctx.create_string()` |

### Key Operations (`RedisModuleKey`)

| Function | Status | Notes |
|----------|--------|-------|
| `RedisModule_KeyType` | ✅ Supported | Returns key type enum |
| `RedisModule_StringSet` | ✅ Supported | Set string value |
| `RedisModule_StringDMA` | ✅ Supported | Direct memory access (read) |
| `RedisModule_StringTruncate` | ✅ Supported | Truncate string value |
| `RedisModule_DeleteKey` | ✅ Supported | Delete the key |
| `RedisModule_ValueLength` | ✅ Supported | Length in bytes |

### Command Registration

| Function | Status | Notes |
|----------|--------|-------|
| `RedisModule_CreateCommand` | ✅ Supported | Register a module command |
| Command flags (write, readonly, admin, fast, etc.) | ✅ Supported | Via `CommandFlag` enum |
| Key spec (first, last, step) | ✅ Supported | Via `with_keys()` |

### Custom Data Types (`RedisModuleType`)

| Function | Status | Notes |
|----------|--------|-------|
| `RedisModule_CreateDataType` | ✅ Supported | Via `ModuleTypeRegistry` |
| `rdb_save` | ✅ Supported | Serialise for persistence |
| `rdb_load` | ✅ Supported | Deserialise from persistence |
| `aof_rewrite` | ✅ Supported | Optional AOF callback |
| `mem_usage` | ✅ Supported | Report memory consumption |
| `digest` | ✅ Supported | Debug digest |
| `free` | ✅ Supported | Cleanup callback |

---

## How to Use

### Registering a Module

```rust
use ferrite_plugins::redis_module::{
    RedisModule, ModuleInfo,
    api::{RedisModuleCtx, RedisModuleString, Status, REDISMODULE_API_VERSION},
    commands::{ModuleCommand, CommandFlag},
};

// Define a command callback
fn my_set(ctx: &mut RedisModuleCtx, args: &[RedisModuleString]) -> Status {
    if args.len() < 2 {
        ctx.reply_with_error("ERR wrong number of arguments");
        return Status::Err;
    }
    let key = ctx.open_key(&args[0].to_string_lossy(), api::KeyMode::Write);
    key.string_set(&args[1]);
    ctx.reply_with_simple_string("OK");
    Status::Ok
}

// Create and populate the module
let mut module = RedisModule::new(ModuleInfo {
    name: "mymodule".into(),
    version: 1,
    api_version: REDISMODULE_API_VERSION,
});

module.register_command(
    ModuleCommand::new("MYMOD.SET", my_set, vec![CommandFlag::Write])
        .with_keys(1, 1, 1),
);

module.mark_loaded();
```

### Using the Module Manager

```rust
use ferrite_plugins::redis_module::{RedisModuleManager, ModuleInfo};

let manager = RedisModuleManager::new();
manager.load(ModuleInfo {
    name: "mymodule".into(),
    version: 1,
    api_version: 1,
}).unwrap();

// List loaded modules
for info in manager.list() {
    println!("{} v{}", info.name, info.version);
}

// Unload
manager.unload("mymodule").unwrap();
```

### Registering Custom Data Types

```rust
use ferrite_plugins::redis_module::types::{
    RedisModuleTypeDef, TypeMethods, ModuleTypeValue, ModuleTypeError,
};
use std::sync::Arc;

let type_def = RedisModuleTypeDef::new("mytype", 1)
    .with_methods(TypeMethods {
        rdb_save: Some(Arc::new(|val| val.rdb_save())),
        ..TypeMethods::default()
    });

let module = RedisModule::new(/* ... */);
module.register_type(type_def).unwrap();
```

---

## Limitations and Differences from Redis

1. **No native `.so` loading** — Ferrite does not `dlopen` compiled Redis modules. Module logic must be written in Rust using this compatibility API.

2. **No C ABI** — The Redis Module API is a C ABI with function pointers. This layer exposes Rust-native types and traits instead. A C FFI bridge is planned for a future release.

3. **Async execution model** — Redis modules execute synchronously on the main thread. Ferrite commands run on an async executor. Module command callbacks are currently synchronous but are invoked within Ferrite's async dispatch.

4. **Key access is in-memory only** — `RedisModuleKey` currently operates on an in-memory buffer. Integration with Ferrite's tiered HybridLog storage (mutable → read-only → disk) is on the roadmap.

5. **Blocking commands** — `RedisModule_BlockClient` and related APIs are not yet supported. Modules cannot block clients.

6. **Cluster API** — `RedisModule_SendClusterMessage` and cluster-aware APIs are not yet implemented.

7. **Server events / keyspace notifications** — Module-level keyspace notifications (`RedisModule_SubscribeToKeyspaceEvents`) are planned but not yet available.

8. **Thread-safe contexts** — `RedisModule_GetThreadSafeContext` is not needed; Ferrite's types are `Send + Sync` by design.

---

## Roadmap

| Phase | Target | Description |
|-------|--------|-------------|
| **Phase 1** (current) | v0.1 | Core types, command registration, custom data types, in-memory key access |
| **Phase 2** | v0.2 | Integration with Ferrite's HybridLog storage engine for real key access |
| **Phase 3** | v0.3 | Blocking client support, keyspace notifications |
| **Phase 4** | v0.4 | C FFI bridge for loading existing compiled Redis modules |
| **Phase 5** | v0.5 | Cluster API, cross-node module communication |

---

## Architecture

```
┌──────────────────────────────────────────────────┐
│                  RedisModuleManager              │
│  ┌─────────────┐  ┌─────────────┐               │
│  │ RedisModule  │  │ RedisModule  │  ...         │
│  │  (mymod)     │  │  (other)     │              │
│  └──────┬───────┘  └──────┬───────┘              │
│         │                  │                      │
│   ┌─────┴─────┐     ┌─────┴─────┐               │
│   │ Commands  │     │  Types    │                │
│   │ Registry  │     │ Registry  │                │
│   └───────────┘     └───────────┘                │
└──────────────────────────────────────────────────┘
         │                    │
         ▼                    ▼
┌─────────────────┐  ┌────────────────┐
│ Ferrite Command │  │ Ferrite Storage│
│   Executor      │  │   Engine       │
└─────────────────┘  └────────────────┘
```

Module commands are registered in the `ModuleCommandRegistry` and dispatched through Ferrite's command executor. Custom data types register persistence callbacks that will be invoked during RDB save/load cycles.
