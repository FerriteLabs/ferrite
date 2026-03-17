# ADR-018: Mnemo — Agent Memory OS

- **Date:** 2026-04-17
- **Status:** Accepted (Beta)
- **Related:** Moonshot M1
- **Supersedes:** ADR-018 (Spike)

## Context

LLM agent frameworks (LangGraph, LlamaIndex, Letta, AutoGen) all reinvent the same
memory primitives — episodic event logs, semantic vector recall, summarisation, and
"forgetting." Today these are stitched together by the application from a vector DB
(Pinecone, Weaviate), a KV store (Redis), and bespoke summarisation code. The result is
high latency (multiple network hops), no transactional guarantees across stores, and no
unified retention/forgetting policy.

Ferrite already ships the building blocks in `crates/ferrite-ai/src/`:

- `agent_memory/` — episodic event log primitive
- `vector/` — HNSW vector index
- `embedding/` — embedding pipeline
- `graphrag/` — graph-structured retrieval
- `semantic/` — semantic-cache style recall
- `rag/` — retrieval-augmented generation glue

These are not yet exposed as a coherent product surface. Mnemo unifies them behind a
small `MEMORY.*` RESP command family addressable from any Redis-compatible client.

## Decision

We will ship Mnemo as a **new command family** layered on top of the existing
`ferrite-ai` primitives, **without rewriting them**. The Phase-0 spike commits to:

1. A unified data model — see §Data model below — extending what
   `crates/ferrite-ai/src/agent_memory/store.rs` already tracks (access count,
   importance, expiry) rather than redefining it.
2. Five core commands defined in this ADR (signatures in §APIs below).
3. A composition strategy: each `MEMORY.WRITE` produces **one authoritative record**
   on a hash-routed primary key plus **derived secondary indexes** (vector, graph)
   maintained asynchronously. The primary key is what gets replicated; secondary
   indexes are rebuilt on a replica from the replicated record. This matches the
   existing per-key/per-core ownership model (ADR-003) and the explicit
   replicated-command list in `src/server/handler.rs` (no command "replicates for
   free" — Mnemo's primary write is added to that list explicitly).
4. **Tenancy is enforced server-side**, not by name alone — see §Tenancy & isolation.
5. Co-location: all keys for a given `(db, tenant, agent_id, session_id)` are
   computed via a fixed prefix hash so they land on the same shard/core. Cross-shard
   recall is supported via scatter-gather, but the common single-shard path is the
   optimised one.

Code lives in `crates/ferrite-ai/src/mnemo/` (facade, no new crate, no cross-extension
deps — ADR-005 unchanged) with RESP wiring in the top-level `ferrite` crate at
`src/commands/handlers/memory.rs`.

## Data model

`MemoryRecord` extends the existing struct in
`crates/ferrite-ai/src/agent_memory/store.rs:120-129`. Mnemo does not redefine these
fields — it requires they be exposed via the facade:

| Field | Type | Source |
|---|---|---|
| `id` | `RecordId` (ULID) | new |
| `tenant` | `TenantId` | new — see Tenancy |
| `agent_id`, `session_id` | `String` | new |
| `kind` | `episodic \| semantic \| procedural \| summary` | new |
| `content` | `bytes` | existing |
| `embedding` | `Option<Vec<f32>>` | existing (`vector/`) |
| `created_at`, `last_accessed`, `access_count` | derived | existing |
| `importance` | `f32 [0,1]` | existing |
| `expires_at` | `Option<u64>` | existing |
| `summary_of` | `Option<Vec<RecordId>>` | new — lineage for summaries |
| `meta` | `Map<String,String>` | new (bounded ≤16 KiB) |

Tombstones are first-class records with `kind = tombstone`; they replicate so replicas
can drop secondary-index entries.

## APIs (Phase-0 contract)

```
MEMORY.WRITE     <agent_id> <session_id> <kind> <content> [META k v ...]
                 -> <record_id>
MEMORY.RECALL    <agent_id> <session_id> <query> [TOP <n>] [KIND <kind>]
                 -> array of <record_id, content, score>
MEMORY.FORGET    <agent_id> <session_id> [ID <record_id> | OLDER <duration>]
                 -> <count_forgotten>
MEMORY.SUMMARIZE <agent_id> <session_id> [WINDOW <n>] [SYNC]
                 -> <job_handle>            ; default async; SYNC returns <summary_id>
MEMORY.STATS     <agent_id> [<session_id>]
                 -> { records, bytes, last_write_ts, ... }
```

`<kind>` is one of `episodic | semantic | procedural | summary` (extensible).
`MEMORY.SUMMARIZE` returns a job handle by default (resolvable via a forthcoming
`JOBS.STATUS` command — out of scope here). The `SYNC` flag opts into inline
summarisation that blocks until completion and returns the created summary's
`<record_id>` directly.

## Tenancy & isolation

Mnemo enforces the namespace `(db, tenant, agent_id, session_id)` server-side:

- `db` — the active Ferrite logical DB (existing).
- `tenant` — derived from the authenticated ACL user (`src/commands/executor/mod.rs`
  already enforces ACL pre-execution); rejected if anonymous unless explicitly
  configured via `mnemo.allow_anonymous = true`.
- `agent_id` / `session_id` — caller-supplied but namespaced under tenant.

Cross-tenant access is denied unconditionally. Per-tenant quotas (configurable):

| Quota | Default | Enforcement point |
|---|---|---|
| Records per session | 100 000 | `MEMORY.WRITE` |
| Bytes per tenant | 10 GiB | `MEMORY.WRITE` |
| Vector dims | 1536 | `MEMORY.WRITE` |
| Recall ops/sec | 1000 | token bucket in handler |
| Summarise ops/min | 10 | token bucket in handler |

Quota state is persisted under `__ferrite:mnemo:quota:<tenant>` (ADR-016 convention).

## Composition diagram

```
        client (RESP / SDK)
                │
                ▼
        commands/memory.rs       <-- new
                │
        ┌───────┼────────┐
        ▼       ▼        ▼
   agent_memory vector  graphrag
   (log)        (HNSW)  (KG)
        │       │        │
        └───────┴────────┘
                │
           HybridLog storage
```

## Hybrid retrieval scoring (initial)

```
score = w_v * cosine(q, e)        // semantic similarity
      + w_r * exp(-Δt / τ)        // recency decay
      + w_f * log(1 + freq)       // access frequency
```

Initial weights `w_v=0.7, w_r=0.2, w_f=0.1`; `τ=24h`. Tunable per-call via `OPTS`.

## Phase-0 deliverables

- [x] This ADR.
- [ ] (Phase 1) `crates/ferrite-ai/src/mnemo/` facade module.
- [ ] (Phase 1) `src/commands/memory.rs` handler with the 5 commands above.
- [ ] (Phase 1) Demo notebook in `ferrite-docs/website/docs/examples/agent-memory.md`.

## Eval plan (Phase 2)

A reproducible harness lives at `ferrite-bench/moonshots/mnemo/eval/` (per
`MOONSHOT_HARNESS.md`) and pins:

| Knob | Value |
|---|---|
| Embedding model | `bge-large-en-v1.5` @ commit pinned in `eval/models.lock` |
| Inference model | `gpt-4o-mini` (or local Llama-3.1-8B-Instruct via vLLM) |
| Hardware baseline | AMD EPYC 9554P, 256 GiB DRAM, NVMe Gen4 |
| Random seed | `FERRITE_BENCH_SEED=42` |
| Scoring | provided by each benchmark's official harness, version-pinned |

Targets:

- **LongMemEval** (https://arxiv.org/abs/2410.10813, harness: github.com/xiaowu0162/LongMemEval @ pinned commit): ≥75% on `longmemeval_s` subset.
- **LoCoMo** (https://arxiv.org/abs/2402.17753, harness pinned): ≥70% F1 on `qa-conv`.
- **Mem0 reproduction**: rerun the Mem0 paper's evaluation script against Mnemo using their published configs; results published with raw numbers, not just deltas.

Pre-launch, the harness must be runnable end-to-end by a contributor with one command
(`bash run.sh longmemeval`) and produce a JSON result in the schema defined by the
benchmark harness doc. Eval missing or unreproducible blocks the Beta gate.

## Consequences

### Positive
- Unifies five existing crates into a coherent product story.
- Leverages Ferrite's existing storage, persistence, and replication for free.
- New command family is additive — no breaking changes.

### Negative
- Couples `ferrite-ai` crates more tightly via the facade; needs a clean trait boundary
  to avoid violating the "no cross-extension-crate dependencies" rule of ADR-005.
  Mitigation: facade lives in `ferrite-ai` itself (the only crate already importing
  the others is the top-level integration crate), keeping the rule intact.
- Hybrid retrieval scoring is heuristic; will need eval-driven tuning.

## Open questions

1. ~~Should `MEMORY.SUMMARIZE` be sync or async?~~ **Resolved:** async by default, `SYNC` flag for inline.
2. How are embeddings sourced — server-side or client-supplied? **Pragmatic default:** both supported; client-supplied wins if present. *(Confirm before P1.)*
3. GDPR forget — is a logical tombstone enough, or do we need cryptographic proof of forgetting? **Defer to M3 (Lucidity).**
4. How do we evolve `kind` and `meta` schemas without breaking replicated records? *(Open — needs answer before P3 alpha.)*

## Exit criteria for Phase 0

- ADR merged ✅
- API signatures reviewed by maintainer
- One ferrite-ai crate owner signs off on the facade plan
- Open questions 1 and 2 answered (Q3 deferred)

## P5 deliverables

- Python SDK published (`ferrite-mnemo` on PyPI)
- Migration notes for upgrading from `ferrite-ai` agent_memory to Mnemo commands
- Changelog entry documenting MEM.PUT/GET/RECALL/FORGET/STATS/SUMMARIZE commands
