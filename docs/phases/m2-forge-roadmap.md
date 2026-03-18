# Forge (M2) — Implementation Roadmap

> Source of truth: ADR-019 (`docs/adrs/adr-019-forge-wasm-functions.md`).
> Phase-by-phase execution plan for WASM-based UDFs.

## Phase index

| ID | Phase | Effort | Status | Exit gate |
|---|---|---|---|---|
| m2-p0-spike | Spike + bench | 2 wk | done (ADR-019) | bench shows < 50 µs warm-call p99 |
| m2-p1-runtime | Runtime integration | 4 wk | planned | FN.LOAD/CALL working with sandbox tests |
| m2-p2-hostapi | Host API | 3 wk | planned | 5 example modules + WIT shipped |
| m2-p3-toolchain | Toolchain | 2 wk | planned | `cargo ferrite-fn` template published |
| m2-p4-alpha | Alpha | 3 wk | planned | chaos test: malicious modules can't break Ferrite |
| m2-p5-ga | Beta → GA | 4 wk | planned | function registry + signed modules + GA |

---

## Phase 0 — Spike (already chartered in ADR-019)

A standalone bench validating the warm-call latency target. Code lives in
`crates/ferrite-forge-spike/` (delete-after-spike crate).

```
crates/ferrite-forge-spike/
├── Cargo.toml          # depends on wasmtime 25+, criterion
├── benches/
│   └── warm_call.rs    # criterion bench: pre-instantiated component, no host calls
├── modules/
│   └── echo.wat        # the smallest valid component
└── README.md           # how to reproduce + last-measured numbers
```

Bench expectation: p99 < 50 µs warm call on AMD EPYC 9554P, Linux 6.8.
Result published to `ferrite-bench/results/forge-spike-<date>.md`.

**Decision rule**: if p99 between 50 and 100 µs, proceed but flag in P1; if > 100 µs,
halt and re-evaluate (alternative runtimes, AOT pipeline).

---

## Phase 1 — Runtime integration

### Files to create

```
crates/ferrite-forge/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── engine.rs         # per-thread Wasmtime Engine + module cache
│   ├── store.rs          # per-call snapshot + write buffer
│   ├── limits.rs         # ResourceLimiter impl (memory cap, table size cap)
│   ├── exec.rs           # call orchestration (deadline, abort, commit)
│   └── error.rs
├── tests/
│   ├── load_call.rs
│   ├── timeout.rs
│   ├── memory_limit.rs
│   ├── stack_limit.rs
│   ├── rollback_on_abort.rs
│   └── replication.rs
src/commands/handlers/forge.rs    # FN.LOAD / FN.CALL / FN.CALL_RO / FN.LIST / FN.DROP / FN.STATS
src/commands/handlers/mod.rs      # register FN.* family
src/server/handler.rs             # add FN.LOAD, FN.DROP, FN.CALL (write) to replicated-command list
```

### Engine lifecycle

```rust
// crates/ferrite-forge/src/engine.rs
thread_local! {
    static ENGINE: Engine = build_engine();      // per-thread, never shared
    static CACHE: RefCell<HashMap<(SmolStr, [u8;32]), Module>> = ...;
}
fn build_engine() -> Engine {
    let mut cfg = Config::new();
    cfg.epoch_interruption(true);
    cfg.wasm_component_model(true);
    cfg.async_support(false);
    cfg.max_wasm_stack(1 * 1024 * 1024);
    Engine::new(&cfg).unwrap()
}
```

### Per-call execution

```rust
// crates/ferrite-forge/src/exec.rs
pub fn call(name: &str, keys: &[Bytes], argv: &[Bytes], deadline: Duration)
    -> Result<Bytes, ForgeError>
{
    let module = cache_get_or_load(name)?;
    let mut store = Store::new(&engine(), CallState::new(keys, argv));
    store.limiter(|s| &mut s.limits);
    store.set_epoch_deadline(epoch_ticks_for(deadline));

    let instance = INSTANCE_POOL.with(|p| p.borrow_mut().acquire(&module, &mut store))?;
    let process = instance.get_typed_func::<(Bytes,), Bytes>(&mut store, "process")?;

    match process.call(&mut store, (input,)) {
        Ok(out) => {
            commit_writes(store.into_data())?;   // batched, replicated, AOF-emitted
            Ok(out)
        }
        Err(e) if is_trap(&e) => {
            // discard write buffer; nothing to replicate
            Err(ForgeError::Aborted(e))
        }
    }
}
```

### Tests (must pass)

| Test | Expectation |
|---|---|
| `load_call` | LOAD a module, CALL it, get expected output |
| `timeout` | infinite-loop module aborts within 10ms |
| `memory_limit` | module that grows beyond 16 MiB traps |
| `stack_limit` | recursive WASM hits stack limit cleanly |
| `rollback_on_abort` | trap mid-call leaves no writes in store |
| `replication` | LOAD on primary replicates to replica; CALL on replica works |
| `acl_keyspace` | guest accessing key outside KEYS+DECLARED_KEYSPACE traps |

### Acceptance criteria (P1 → P2 gate)

- All 7 tests above green.
- Sandbox demonstrably blocks: file syscalls, network, stack overflow, memory blowup, infinite loops.
- Per-thread cache hit rate > 99% in steady state under repeated FN.CALL load.

---

## Phase 2 — Host API

### WIT (`crates/ferrite-forge/wit/ferrite.wit`)

```wit
package ferrite:fn@0.1.0;

interface kv {
    get: func(key: list<u8>) -> option<list<u8>>;
    set: func(key: list<u8>, value: list<u8>) -> result<_, string>;
    del: func(key: list<u8>) -> bool;
    scan: func(prefix: list<u8>, limit: u32) -> list<tuple<list<u8>, list<u8>>>;
}
interface time {
    now-ms: func() -> u64;
}
interface log {
    info: func(msg: string);
    warn: func(msg: string);
}
world function {
    import kv;
    import time;
    import log;
    export process: func(input: list<u8>) -> result<list<u8>, string>;
}
```

### Five example modules

| Path | Purpose |
|---|---|
| `examples/forge/rate_limit/` | token bucket per key |
| `examples/forge/jwt_verify/` | RFC 7519 verification with KV-cached JWKS |
| `examples/forge/json_patch/` | RFC 6902 patch on a JSON value |
| `examples/forge/custom_merge/` | last-write-wins-by-source-rank merge for Chronicle |
| `examples/forge/hot_keys/` | streaming top-K detector |

### Acceptance criteria (P2 → P3 gate)

- WIT published; `wit-deps` distribution working.
- Each example builds via `cargo component build --release` and runs against the dev cluster.
- KV access enforcement test: any key outside grant traps and is recorded in `forge.fn_call_errors_total{reason="acl"}`.

---

## Phase 3 — Toolchain

### Templates

```
sdk/forge-templates/
├── rust/                # cargo generate template
├── go/                  # tinygo wasi-p2 example
└── ts/                  # jco-componentize example
```

### CLI

`cargo ferrite-fn new <name>` — wraps `cargo generate` with the Rust template.
`cargo ferrite-fn build` — invokes `cargo component build --release`.
`cargo ferrite-fn deploy <addr> <name>` — issues `FN.LOAD`.

### Acceptance criteria (P3 → P4 gate)

- Templates published as a `ferritelabs/forge-templates` repo + `crates.io` tarball.
- Quickstart in docs gets a developer to a running custom function in < 10 min.

---

## Phase 4 — Alpha

### Hardening

- [ ] OTel metrics: forge.fn_calls_total, fn_call_latency_seconds, fn_call_errors_total{reason}, fn_modules_loaded, fn_compile_seconds.
- [ ] Tracing: span per FN.CALL with attrs (fn_name, fn_hash, deadline_ms, abort_reason).
- [ ] Chaos suite (must all be no-op for Ferrite stability):
  - module that allocates until OOM
  - module that recurses to stack limit
  - module that calls `set` in tight loop forever
  - module that returns malformed component types
  - module signed with wrong key (after P5: rejected)
- [ ] Resource accounting: per-tenant fn-call budget enforced (token bucket).

### Acceptance criteria (P4 → P5 gate)

- Chaos suite green; Ferrite process never crashes regardless of guest behaviour.
- ≥ 2 design partners running internal logic on Forge.

---

## Phase 5 — Beta → GA

### Function registry (per-DB)

- `FN.SHOW <name>` returns name, hash, size, declared_keyspace, calls, signed_by.
- Versioning: `FN.LOAD foo:v2` keeps `foo:v1` callable; `FN.PROMOTE foo v2` flips default.

### Signed modules

- Module bytes prefixed with a detached COSE_Sign1 envelope.
- Verifier configured per-DB: `forge.signers = [{key_id, public_key_pem}]`.
- `forge.require_signing = true` rejects unsigned LOADs.
- ML-DSA (FIPS 204) signature curve, matching Lucidity's PQ choice.

### Acceptance criteria (GA gate)

- Registry surfaces stable for 4+ weeks across versions.
- Signed-module flow has ≥ 1 production user.
- ADR-019 status flipped to `Accepted (GA)`.
- ≥ 1 community-published function in a public registry.

---

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Wasmtime warm-call regresses below 50 µs | pin Wasmtime version; bench in CI; track `wasmtime` releases |
| Component Model API churn pre-1.0 | track WASI Preview 2 freeze; pin `wit-bindgen` major |
| Per-tenant fairness becomes a bottleneck | open question 4 in ADR-019 must be resolved before P3 |
| Signed-module key rotation | document rotation procedure; support multiple active keys |
