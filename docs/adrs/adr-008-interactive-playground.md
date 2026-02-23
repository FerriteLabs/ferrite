# ADR-008: Interactive Playground (try.ferrite.dev)

## Status

**Proposed** — February 2026

## Context

Ferrite needs to lower the barrier to trying the database. Currently, users must install Rust 1.80+, build from source or pull a Docker image, and run a server. An interactive web playground at `try.ferrite.dev` would let users experiment with Ferrite commands in their browser — similar to [try.redis.io](https://try.redis.io/) and the [Dragonfly Playground](https://www.dragonflydb.io/docs/getting-started).

## Decision

We will build a web-based interactive REPL for Ferrite, accessible at `try.ferrite.dev`.

### Architecture Options Evaluated

| Option | Pros | Cons | Decision |
|--------|------|------|----------|
| **A. Sandboxed backend** | Full compatibility, real Ferrite server | Infrastructure cost, security surface, session management | **Chosen (Phase 1)** |
| **B. WASM in-browser** | Zero infrastructure, instant load | Limited features (no io_uring, no persistence), WASM binary size | **Chosen (Phase 2)** |
| **C. Static documentation only** | Zero cost | Not interactive | Rejected |

### Phase 1: Sandboxed Backend (MVP)

**Architecture:**
```
┌─────────────────────┐     ┌───────────────────────────┐
│  Browser (SPA)      │────▶│  API Gateway (Cloudflare)  │
│  - Terminal emulator │     │  - Rate limiting            │
│  - Syntax highlight  │     │  - Session management       │
│  - Tutorial sidebar  │     └────────────┬──────────────┘
└─────────────────────┘                   │
                                          ▼
                              ┌──────────────────────┐
                              │  Ferrite Sandbox Pool  │
                              │  - Ephemeral containers │
                              │  - 64MB RAM limit       │
                              │  - 60s session timeout   │
                              │  - Read/write, no SAVE   │
                              │  - No network access     │
                              └──────────────────────┘
```

**Components:**
1. **Frontend**: React SPA with xterm.js terminal emulator, hosted on Cloudflare Pages
2. **API Gateway**: Cloudflare Workers for rate limiting (10 req/s per IP), session routing, and WebSocket proxying
3. **Sandbox Pool**: Pre-warmed pool of Ferrite containers (Fly.io Machines or AWS Lambda containers)
   - Each session gets an isolated Ferrite instance
   - 64MB memory limit, 60-second idle timeout
   - Dangerous commands disabled: `SAVE`, `BGSAVE`, `CONFIG SET`, `SHUTDOWN`, `DEBUG`, `FLUSHALL`
   - Pre-loaded with sample data (demo:users, demo:products, demo:vectors)

**Session Flow:**
1. User opens try.ferrite.dev → SPA loads
2. User types first command → API allocates a sandbox from the pool
3. Commands sent via WebSocket → API forwards to sandbox Ferrite via RESP
4. Session expires after 60s idle → sandbox recycled

**Cost Estimate:** ~$20-50/month for light usage (Fly.io Machines with scale-to-zero)

### Phase 2: WASM In-Browser (Future)

Once `ferrite` compiles to WASM (the `wasm` feature flag exists), embed it directly:
- Compile `ferrite` with `--target wasm32-wasi` using the `lite` feature set
- Use `wasm-bindgen` to expose the command executor to JavaScript
- Terminal emulator sends commands directly to in-browser Ferrite
- Zero backend cost, instant load, works offline
- Limitation: No persistence, no io_uring, memory-only mode

### Tutorial System

The playground includes guided tutorials:
1. **Getting Started** (5 min): SET/GET, data types, TTL
2. **Data Structures** (10 min): Lists, hashes, sets, sorted sets
3. **Pub/Sub** (5 min): SUBSCRIBE/PUBLISH with split pane
4. **Vector Search** (10 min): VECTOR.CREATE, VECTOR.ADD, VECTOR.SEARCH
5. **Transactions** (5 min): MULTI/EXEC, WATCH

Each tutorial has pre-loaded data and step-by-step instructions in a sidebar.

## Consequences

### Positive
- Dramatically lower barrier to trying Ferrite (zero-install)
- Tutorial system drives feature discovery
- Demonstrates real performance characteristics (Phase 1)
- Marketing asset ("Try it now" button on homepage)

### Negative
- Phase 1 requires ongoing infrastructure ($20-50/month)
- Security hardening needed for public-facing sandboxes
- WASM binary size may be large (~10-20MB compressed)

### Risks
- Sandbox escape if container isolation fails → mitigated by Fly.io machine isolation + disabled commands
- Abuse for crypto mining → mitigated by 64MB RAM limit + 60s timeout + rate limiting

## Implementation Plan

1. **Week 1-2**: Frontend SPA with xterm.js + mock responses
2. **Week 2-3**: Sandbox API (Fly.io Machines) + WebSocket proxy
3. **Week 3-4**: Tutorial content + pre-loaded demo data
4. **Week 4**: Security audit, rate limiting, monitoring
5. **Future**: WASM compilation + in-browser fallback

## References

- [try.redis.io](https://try.redis.io/) — Redis playground
- [Fly.io Machines API](https://fly.io/docs/machines/) — Ephemeral container hosting
- [xterm.js](https://xtermjs.org/) — Terminal emulator for web
- [WASM Feature Flag](../Cargo.toml) — `wasm` feature in Ferrite
