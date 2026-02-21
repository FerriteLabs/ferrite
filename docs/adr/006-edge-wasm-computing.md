# ADR-006: Embedded WASM for Edge Computing

## Status

**Proposed** — February 2026

## Context

Edge computing (Cloudflare Workers, Deno Deploy, Fastly Compute, embedded IoT) needs lightweight, fast data stores. Currently SQLite (via libsql/Turso) dominates this space. Ferrite's `embedded` mode + existing `wasm` feature flag position it to compete here.

## Decision

Develop a WASM-compiled embedded Ferrite for edge/IoT deployments.

### Target Environments

| Platform | Runtime | Use Case |
|----------|---------|----------|
| Cloudflare Workers | V8 + WASM | Edge caching, session storage |
| Deno Deploy | V8 + WASM | Server-side caching |
| Fastly Compute | Wasmtime | CDN-layer caching |
| IoT / Embedded | WASI | Local device cache |
| Browser | Web WASM | Client-side state (playground) |

### Architecture

```
┌──────────────────────────────┐
│  Application Code (JS/Rust)   │
│                               │
│  const db = new Ferrite();    │
│  await db.set("key", "val");  │
│  await db.get("key");         │
└──────────┬───────────────────┘
           │ wasm-bindgen / WASI
           ▼
┌──────────────────────────────┐
│  ferrite-embedded.wasm        │
│  ┌────────────────────────┐  │
│  │ ferrite-core (lite)     │  │
│  │ - Memory-only store     │  │
│  │ - RESP parser           │  │
│  │ - Command executor      │  │
│  │ - No io_uring/mmap/TLS  │  │
│  └────────────────────────┘  │
│  Size target: <5MB gzipped   │
└──────────────────────────────┘
```

### Build Strategy

```bash
# Build with minimal features for small WASM binary
cargo build --target wasm32-wasi \
  --features lite \
  --no-default-features \
  -p ferrite
```

**Feature gating for WASM:**
- Disable: `io-uring`, `tls`, `cloud`, `otel`, `tui`, `scripting`
- Enable: `lite` (memory-only, no persistence)
- Strip: All CLI, config file parsing, metrics HTTP server

### JavaScript Bindings

```javascript
import { Ferrite } from '@ferritelabs/ferrite-wasm';

const db = new Ferrite({ maxMemory: '64mb' });

// Standard Redis API
await db.set('user:1', JSON.stringify({ name: 'Alice' }));
const user = JSON.parse(await db.get('user:1'));

// Vector search (if AI features compiled in)
await db.execute('VECTOR.CREATE', 'idx', 'DIM', '128', 'DISTANCE', 'cosine');
```

### NPM Package: `@ferritelabs/ferrite-wasm`

Publish to npm with pre-built WASM binaries for:
- `ferrite-wasm-web` (browser, wasm-bindgen)
- `ferrite-wasm-node` (Node.js, WASI)
- `ferrite-wasm-cloudflare` (Workers-compatible)

### Size Budget

| Component | Estimated Size |
|-----------|---------------|
| Core store + commands | ~2MB |
| RESP parser | ~100KB |
| Vector search (optional) | ~1.5MB |
| **Total (gzipped)** | **~2-4MB** |

### Implementation Phases

1. **Phase 1** (2 weeks): Compile `ferrite-core` to `wasm32-wasi` with `--features lite`
2. **Phase 2** (2 weeks): JavaScript bindings with `wasm-bindgen`, npm package
3. **Phase 3** (2 weeks): Cloudflare Workers adapter, Deno module
4. **Phase 4** (ongoing): Size optimization, streaming support

## Consequences

### Positive
- Opens entirely new deployment surface (edge, browser, IoT)
- "SQLite for caching" positioning with Redis API familiarity
- Natural fit for the playground (ADR-004 Phase 2)

### Negative
- WASM binary size must be managed aggressively
- No persistence in WASM (memory-only)
- Testing matrix expands significantly

### Risks
- Some Rust dependencies may not compile to WASM (e.g., `parking_lot`, `dashmap`)
- Performance overhead of WASM vs native (~10-30% slower)
