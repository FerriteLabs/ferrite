# ADR-019: Forge — WASM In-DB Functions

- **Date:** 2026-04-17
- **Status:** Accepted (P5 — versioning & signed modules implemented)
- **Related:** Moonshot M2; supersedes scope of ADR-006 (plugin runtime)
- **Supersedes:** ADR-019 (Spike)

## Context

ADR-006 established Wasmtime as Ferrite's plugin runtime, but no public command surface
exists yet for users to load and invoke their own functions. Lua scripting (`EVAL`) is
the only current extension path; it is single-language, weakly sandboxed, and slow on
modern workloads.

Forge ships a deterministic, sandboxed user-defined function (UDF) layer using Wasmtime
+ WASI Preview 2 (Component Model). Three driving use cases:

1. **Custom merge functions** for Chronicle (M4) — required dependency.
2. **Inline policy / rate-limit / auth checks** without round-tripping to the app.
3. **Server-side transformations** (JSON-patch, format conversion) that today require
   a network hop.

## Decision

We will integrate Wasmtime per-thread (matching Ferrite's thread-per-core model), expose
a `FN.*` command family, and adopt the Component Model so functions can be authored in
any language with WIT bindings (Rust, Go, TS, Python via Componentize-Py).

## Phase-0 acceptance bar

A spike binary that loads a tiny Wasm component and invokes its exported `process` function
with an input bytes blob, returning output bytes. Measured warm-call p99 must be
**< 50 µs** on a Linux x86_64 dev box (matching the budget in the strategy brief).

The bench is **scoped explicitly** so the result is meaningful:

- Module: a single component pre-compiled via `wasmtime::Module::serialize` and loaded with `Module::deserialize`.
- Store: drawn from a per-thread pre-instantiated pool (see Execution model below).
- Host imports: KV `get`/`set`/`scan` linked but **not invoked** by the bench (we measure runtime overhead, not host-call cost; host-call overhead is benched separately).
- WASI: Component Model + WASI Preview 2 enabled, but no capabilities granted.
- Epoch interruption enabled at 1 ms ticks; fuel metering disabled in this bench.
- Cold-load time and host-call overhead each have their own bench (see `MOONSHOT_HARNESS.md` § Forge) — they are NOT bundled into the warm-call number.

If the warm-call result misses the 50 µs budget by >2× on the named hardware, we
re-evaluate (precompiled artefacts, stricter pre-instantiation, alternate runtimes).

## APIs (Phase-0 contract)

```
FN.LOAD     <name> <wasm-bytes> [DECLARED_KEYSPACE <prefix>+]   -> OK
FN.LIST                                                          -> array of <name, version, hash>
FN.CALL     <name> [KEYS k1 [k2 ...]] [ARGV ...] [TIMEOUT ms]    -> <output-bytes>
FN.CALL_RO  <name> [KEYS k1 [k2 ...]] [ARGV ...] [TIMEOUT ms]    -> <output-bytes>   ; read-only, can fan to replicas
FN.DROP     <name>                                               -> OK
FN.STATS    <name>                                               -> { calls, p50, p99, errors, mem_bytes }
```

### P5 versioning & signing extensions

```
FN.SHOW     <name>                                               -> { name, version, sha256, size, call_count, signed_by }
FN.PROMOTE  <name> <version>                                     -> OK
FN.VERSIONS <name>                                               -> array of { version, is_default, loaded_at_ms, call_count, signed_by }
```

Module signing uses HMAC-SHA256 (`SignedEnvelope` in `ferrite-forge::signing`),
with `SigningPolicy` controlling whether signatures are required at `FN.LOAD` time.
The signing envelope is a placeholder for a future ML-DSA COSE_Sign1 upgrade.

`KEYS` / `ARGV` deliberately mirror Redis `FCALL` / `EVAL` so the existing router can
hash-route to a single shard/core *before* invoking the function. Functions may only
touch keys passed in `KEYS` plus their `DECLARED_KEYSPACE` static prefixes (granted at
`FN.LOAD`); host bindings reject any access outside that set.

Functions are scoped per-database; cluster-wide registry deferred to Phase 5. ACL
checks run pre-execution against the union of `KEYS` and `DECLARED_KEYSPACE`, exactly
matching how `EVAL`/`FCALL` are handled today (see
`src/commands/executor/mod.rs:1672` and the FCALL_RO handling around it).

## WIT interface (host → guest)

```wit
package ferrite:fn@0.1.0;

interface kv {
  get: func(key: list<u8>) -> option<list<u8>>;
  set: func(key: list<u8>, value: list<u8>) -> result<_, string>;
  scan: func(prefix: list<u8>, limit: u32) -> list<tuple<list<u8>, list<u8>>>;
}

world function {
  import kv;
  export process: func(input: list<u8>) -> result<list<u8>, string>;
}
```

## Execution & isolation model

Forge introduces an explicit per-call execution model. The current
`src/wasm/host.rs` bindings mutate the backend immediately (lines 243-252, 286-295,
466-475); Forge **changes that** for `FN.CALL` (read-write) and **forbids it** for
`FN.CALL_RO`:

| Aspect | Semantics |
|---|---|
| Isolation level | Snapshot at call-start + read-your-own-write within the call |
| Reads | First check the per-call write buffer; fall through to the snapshot |
| `scan` | Returns a deterministic merge of snapshot + buffered writes |
| Commit point | After the guest returns `Ok`; abort discards the buffer entirely |
| Replication / AOF | Emitted **only** at commit, as a single batched op containing all buffered writes — replicas replay the batch atomically |
| Read-only mode | `FN.CALL_RO`: writes return `WriteForbidden` from the host; can fan out to replicas |

The existing `src/wasm/host.rs` immediate-mutation bindings remain as a separate
"unmanaged" runtime for legacy plugins; Forge is a new, layered binding set. ADR-006
remains accepted for the runtime choice; ADR-019 supersedes its execution-semantics
implications for user-defined functions.

## Sandboxing & resource limits

| Resource | Default | Configurable | Wasmtime mechanism |
|---|---|---|---|
| CPU | 10 ms wall | per-call `TIMEOUT` | `Engine::increment_epoch` driven by per-thread timer; `Store::epoch_deadline_async_yield_and_update` for cooperative cancellation |
| Memory | 16 MiB | `FN.LOAD ... LIMIT MEM <bytes>` | `StoreLimits` + `ResourceLimiter` |
| Stack | 1 MiB | not user-configurable | `Config::max_wasm_stack` |
| Fuel (optional belt-and-braces) | off by default | `mnemo.forge.fuel` | existing `src/wasm/runtime.rs:150-153` |
| Network/FS | denied | not user-configurable in v1 | no WASI capabilities granted |
| KV access | union of `KEYS` + `DECLARED_KEYSPACE` | per-call + per-load | host binding bounds-checks every `get`/`set`/`scan` |

Pre-instantiation: Forge maintains a per-thread pool of pre-instantiated `Store`s per
loaded module to keep warm-call latency low (current code path creates a fresh `Store`
per call at `src/wasm/runtime.rs:246-311` — that path is replaced for Forge).

## Component-Model rationale

WASI Preview 2 + Component Model lets us:
- Accept modules from any language without language-specific shims.
- Version interfaces (`ferrite:fn@0.1.0` → `0.2.0`) without breaking older modules.
- Share interfaces with future moonshots (Chronicle merge-fn, Mnemo retriever-fn).

## Module storage, replication, and replica hydration

Module bytes live in-DB at `__ferrite:fn:<name>:wasm` (replicated as ordinary KV
data). Per-thread caches hold the *compiled* `Module` (Wasmtime serialised AOT
artefact) keyed by `(name, sha256(bytes))`.

Lifecycle:

| Event | Behaviour |
|---|---|
| `FN.LOAD` (primary) | Compile + serialise once; store bytes + serialised AOT under `__ferrite:fn:<name>:{wasm,aot,hash}`; broadcast a `FN.LOAD` op via the replication path |
| `FN.LOAD` replay (replica) | Each thread lazily compiles the module on the first `FN.CALL` referencing it; AOT artefact is reused if hash matches and Wasmtime version matches |
| Server restart | On startup, scan `__ferrite:fn:*` and pre-warm the per-thread cache for the top-N most-called modules (telemetry from `FN.STATS`); rest are lazy |
| `FN.DROP` | Deletes all `__ferrite:fn:<name>:*` keys; broadcasts `FN.DROP`; per-thread caches invalidate on next access via a generation counter bump |
| Wasmtime upgrade | `aot` artefacts are versioned by `wasmtime_version`; on mismatch they are recompiled lazily and rewritten |

This explicitly fixes the gap noted in `src/commands/handlers/wasm.rs:31-45` where
the legacy handler stored only names — Forge stores the bytes themselves so a fresh
replica can hydrate from replication alone.

## Phase-0 deliverables

- [x] This ADR.
- [ ] (Phase 0) Spike crate `crates/ferrite-forge-spike/` with a single bench:
      `cargo bench -p ferrite-forge-spike --bench warm_call`.
- [ ] (Phase 0) Bench result published to `ferrite-bench/results/forge-spike.md`.
- [ ] (Phase 1) Production crate `crates/ferrite-forge/` + `src/commands/fn.rs`.

## Open questions

1. ~~Per-thread vs shared `Engine`?~~ **Resolved:** per-thread.
2. ~~Module storage location?~~ **Resolved:** in-DB, see Module storage section.
3. ~~Signed modules in v1 or v2?~~ **Resolved:** Implemented in P5 — HMAC-SHA256 signing with `SigningPolicy` and `SignedEnvelope` in `ferrite-forge::signing`. ML-DSA COSE_Sign1 upgrade deferred to post-v1.
4. How is fairness enforced when many `FN.CALL`s share one core? *(Open — needs answer before P3 alpha; candidates: per-tenant token bucket, cooperative yield via epoch ticks.)*

## Exit criteria for Phase 0

- ADR merged ✅
- Spike bench shows p99 < 50 µs warm call (or documented re-evaluation if missed)
- WIT interface reviewed by maintainer
- Decision recorded on per-thread `Engine` and module storage location
