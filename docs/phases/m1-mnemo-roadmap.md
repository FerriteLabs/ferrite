# Mnemo (M1) — Implementation Roadmap

> Source of truth: ADR-018 (`docs/adrs/adr-018-mnemo-agent-memory-os.md`).
> This document expands ADR-018 into a phase-by-phase execution plan.
> Every phase must hit its acceptance criteria before the next opens.

## Phase index

| ID | Phase | Effort | Status | Exit gate |
|---|---|---|---|---|
| m1-p0-spike | RFC + design | 2 wk | done (ADR-018) | ADR merged |
| m1-p1-core | Core API | 4 wk | planned | 5 commands handle round-trip with tests |
| m1-p2-retrieval | Hybrid retrieval + eval | 3 wk | planned | ≥75% on LongMemEval `_s` |
| m1-p3-adapters | LangGraph/LlamaIndex/Letta adapters | 3 wk | planned | 3 working examples |
| m1-p4-alpha | Alpha hardening | 4 wk | planned | 1 internal + 3 external partners |
| m1-p5-ga | Beta → GA | 6 wk | planned | GA blog + integrations index entry |

---

## Phase 1 — Core API

### Files to create

```
crates/ferrite-ai/src/mnemo/
├── mod.rs                 # facade entry
├── record.rs              # MemoryRecord + serde
├── tenancy.rs             # TenantId, namespace derivation, quota state
├── retrieval.rs           # placeholder for P2 — hybrid scorer
├── summarise.rs           # async job stub
└── tests/                 # unit tests live alongside
src/commands/handlers/memory.rs   # MEMORY.* dispatch
src/commands/handlers/mod.rs      # register MEMORY family
src/server/handler.rs             # add MEMORY.WRITE, MEMORY.FORGET, MEMORY.SUMMARIZE to replicated-command list
```

### Schema

```rust
pub struct MemoryRecord {
    pub id: RecordId,                 // ULID
    pub tenant: TenantId,
    pub agent_id: SmolStr,
    pub session_id: SmolStr,
    pub kind: Kind,                   // Episodic | Semantic | Procedural | Summary | Tombstone
    pub content: Bytes,
    pub embedding: Option<Vec<f32>>,
    pub created_at: u64,
    pub last_accessed: AtomicU64,
    pub access_count: AtomicU64,
    pub importance: f32,              // [0.0, 1.0]
    pub expires_at: Option<u64>,
    pub summary_of: Option<SmallVec<[RecordId; 8]>>,
    pub meta: BoundedMap,             // ≤16 KiB total
}
```

### Storage layout

| Key | Value | Replicated |
|---|---|---|
| `__ferrite:mnemo:r:<tenant>:<agent>:<session>:<id>` | record bytes | yes (primary) |
| `__ferrite:mnemo:vec:<tenant>:<agent>:<session>` | HNSW handle metadata | rebuilt on replica |
| `__ferrite:mnemo:idx:<tenant>:<agent>:<session>:<kind>` | sorted-set of (ts, id) | rebuilt on replica |
| `__ferrite:mnemo:quota:<tenant>` | quota counters | yes |
| `__ferrite:mnemo:job:<id>` | summarise job state | yes |

Hash-route prefix: `<tenant>:<agent>:<session>` so all per-session keys land on one shard.

### Command handlers — pseudocode

```rust
// src/commands/handlers/memory.rs
async fn handle_memory_write(ctx: &Ctx, args: &[Bytes]) -> Result<Reply> {
    let (agent, session, kind, content, meta) = parse_write_args(args)?;
    let tenant = ctx.tenant_or_err()?;                // ACL-derived
    ctx.quota.check_write(&tenant, content.len())?;   // pre-check
    let rec = MemoryRecord::new(tenant, agent, session, kind, content, meta);
    let primary_key = rec.primary_key();
    ctx.store.set_replicated(primary_key, rec.encode()).await?;
    ctx.mnemo.index_async(rec.clone());               // fire-and-forget secondary indexes
    Ok(Reply::Bulk(rec.id.to_string()))
}
```

### Tests (must exist, must pass)

| File | Tests |
|---|---|
| `crates/ferrite-ai/src/mnemo/tests/round_trip.rs` | write→recall returns the same record |
| `tests/mnemo_replication.rs` | record written on primary appears on replica via standard repl path |
| `tests/mnemo_tenancy.rs` | cross-tenant access denied; quota enforcement |
| `tests/mnemo_routing.rs` | all keys for a `(tenant, agent, session)` hash to one shard |
| `tests/mnemo_forget.rs` | tombstone replicates and removes secondary indexes on replica |

### Acceptance criteria (P1 → P2 gate)

- All 5 commands round-trip with `redis-cli` against a single-node Ferrite.
- Replication test green on a 2-node setup (`tests/mnemo_replication.rs`).
- Quota test denies a write that would exceed `bytes_per_tenant`.
- Demo notebook in `ferrite-docs/website/docs/examples/agent-memory.md` runs end-to-end.

---

## Phase 2 — Hybrid retrieval + eval

### Retrieval implementation

```rust
// crates/ferrite-ai/src/mnemo/retrieval.rs
pub struct HybridScorer {
    pub w_v: f32,    // 0.7
    pub w_r: f32,    // 0.2
    pub w_f: f32,    // 0.1
    pub tau_secs: f32, // 86400
}
impl HybridScorer {
    pub fn score(&self, q_emb: &[f32], rec: &MemoryRecord, now: u64) -> f32 {
        let cos = cosine(q_emb, rec.embedding.as_ref().unwrap_or(&[]));
        let dt = now.saturating_sub(rec.last_accessed.load(Ordering::Relaxed)) as f32;
        let recency = (-dt / self.tau_secs).exp();
        let freq = (1.0 + rec.access_count.load(Ordering::Relaxed) as f32).ln();
        self.w_v * cos + self.w_r * recency + self.w_f * freq
    }
}
```

Vector search delegates to existing `ferrite-ai/vector/` HNSW; graph re-ranking via
`graphrag/` is a feature flag (`mnemo.graph_rerank = true`).

### Eval harness

```
ferrite-bench/moonshots/mnemo/eval/
├── README.md
├── run.sh              # bash run.sh longmemeval | locomo | mem0
├── headline-metrics.toml
├── models.lock         # bge-large-en-v1.5 SHA, gpt-4o-mini config
└── scripts/
    ├── longmemeval.py
    ├── locomo.py
    └── mem0.py
```

### Acceptance criteria (P2 → P3 gate)

- LongMemEval `longmemeval_s` ≥ 75% (recorded in `ferrite-bench/results/mnemo-eval-<date>.json`).
- LoCoMo `qa-conv` F1 ≥ 70%.
- Mem0 reproduction within ±5% of paper numbers.
- Eval reproducible by a contributor with `bash run.sh longmemeval`.

---

## Phase 3 — Framework adapters

### Adapters

| Path | Language | Surfaces |
|---|---|---|
| `sdk/python/ferrite_mnemo/langgraph/` | Python | LangGraph `BaseMemory` impl |
| `sdk/python/ferrite_mnemo/llamaindex/` | Python | LlamaIndex `BaseMemory` impl |
| `sdk/python/ferrite_mnemo/letta/` | Python | Letta-compatible shim (passthrough) |
| `sdk/node/ferrite-mnemo/` | TS | LangChain.js `BaseMemory` |

### Examples

| Path | What it shows |
|---|---|
| `ferrite-docs/examples/agent-memory/langgraph_chatbot.py` | 50-line agent with persistent memory |
| `ferrite-docs/examples/agent-memory/llamaindex_rag.py` | RAG over MEMORY.* |
| `ferrite-docs/examples/agent-memory/letta_passthrough.py` | Drop-in for Letta users |

### Acceptance criteria (P3 → P4 gate)

- Each example runs from a fresh checkout (CI smoke test).
- Adapters published as pre-release packages (`pip install --pre ferrite-mnemo`).

---

## Phase 4 — Alpha

### Hardening checklist

- [ ] OTel metrics + spans per `docs/OBSERVABILITY.md` (mnemo.requests_total, recall_latency_seconds, records_resident, summarise_job_duration_seconds, etc.)
- [ ] Per-tenant quota enforcement covered by chaos tests (overload + recover)
- [ ] Multi-tenant safety review (no cross-tenant data leakage in any code path)
- [ ] `MEMORY.STATS` accurate vs storage layer truth
- [ ] Docs page (`ferrite-docs/.../moonshots/mnemo.md`) covers all 6 sections from `MOONSHOT_DOCS_PIPELINE.md`
- [ ] Grafana dashboard `ferrite-ops/grafana/dashboards/mnemo.json` committed

### Design partners

- Recruit per `DESIGN_PARTNER_PROGRAM.md` (1 internal + 3 external).
- Each partner runs alpha for ≥4 weeks, signs LOI, opts into telemetry.

### Acceptance criteria (P4 → P5 gate)

- 4 partners in production-ish use, ≥1 case study draft.
- Zero P0/P1 bugs open >7 days.
- Telemetry confirms p99 recall < 50 ms at partner scale.

---

## Phase 5 — Beta → GA

### Hardening

- [ ] Python SDK + Node SDK exposed via official package channels.
- [ ] Pricing-tier docs (which tier includes Mnemo) merged.
- [ ] Migration notes from Mem0 / Letta / Pinecone-as-memory documented.
- [ ] LangChain integrations index PR merged (https://python.langchain.com/docs/integrations/memory/).

### GA launch checklist

- [ ] GA blog post on `ferrite-docs/website/blog/`.
- [ ] HN / Lobsters launch.
- [ ] Reference customers public-launch ready.
- [ ] ADR-018 status flipped to `Accepted (GA)`.

### North-star metric

% of new Ferrite installs that exercise a `MEMORY.*` command in their first 7 days.
Target at GA+90d: **≥ 25%**.

---

## Risks & mitigations (summary)

| Risk | Mitigation |
|---|---|
| Eval scores fall short of 75% target | P2 budget includes 1 wk for retrieval tuning; if still short, ship at 70% with honest comparison |
| Cross-tenant data leak | Independent security review at Beta gate; chaos tests in P4 |
| Embedding model drift between client/server | Server-side default with explicit override; document drift behaviour |
| Adapters fragment as upstream APIs change | Adapters pinned to upstream major versions; quarterly review |
