# Ferrite Embedded SDK

Use Ferrite as a library directly in your application — no separate server process needed.

## Overview

The embedded SDK provides:
- **In-process database** — Zero network overhead
- **Thread-safe** — Safe concurrent access from multiple threads
- **Full API** — Same data types as the server (strings, lists, hashes, sets, sorted sets)
- **Optional persistence** — AOF with configurable sync
- **Memory management** — Configurable limits with 8 eviction policies
- **Vector search** — HNSW/IVF/Flat indexes for AI/ML workloads

## Available SDKs

| Language | Package | Status |
|----------|---------|--------|
| Rust | `ferrite` (library mode) | ✅ Stable |
| Python | `ferrite-py` | 🧪 Beta |
| Node.js | `@ferritelabs/ferrite` | 🧪 Beta |
| Go | `github.com/ferritelabs/ferrite-go` | 🔬 Planned |
| Java/JVM | via JNI | 🔬 Planned |
| C/C++ | via FFI | 🧪 Beta |

## Architecture

```
┌─────────────────────┐
│  Your Application   │
│  (Rust/Python/Node) │
├─────────────────────┤
│   Ferrite SDK       │
│   (language bindings)│
├─────────────────────┤
│   ferrite-core      │
│   (storage engine)  │
├─────────────────────┤
│   HybridLog / DashMap│
│   (data storage)    │
└─────────────────────┘
```

## Rust (Native)

```rust
use ferrite::embedded::{Ferrite, EmbeddedConfig, EvictionPolicy};

let config = EmbeddedConfig::builder()
    .memory_limit("256mb")
    .eviction_policy(EvictionPolicy::AllKeysLru)
    .build();

let db = Ferrite::open(config)?;
db.set("key", "value")?;
let val = db.get("key")?;
```

See [sdk/rust/](../sdk/rust/) for full API reference.

## Python

```python
from ferrite import Ferrite
db = Ferrite(memory_limit="256mb")
db.set("key", "value")
```

See [sdk/python/](../sdk/python/) for full documentation.

## Node.js

```javascript
const { Ferrite } = require('@ferritelabs/ferrite');
const db = new Ferrite({ memoryLimit: '256mb' });
await db.set('key', 'value');
```

See [sdk/nodejs/](../sdk/nodejs/) for full documentation.
