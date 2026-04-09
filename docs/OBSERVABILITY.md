# Observability Conventions

This document defines telemetry conventions every Ferrite component, including all
moonshots (M1–M6, see `plan.md`), MUST follow.

## Three signals

Ferrite emits **metrics**, **logs**, and **traces** via OpenTelemetry. The `otel`
feature flag enables exporters; without it, signals are emitted to the in-process
collector that backs the existing Prometheus endpoint.

## Naming

### Metrics

`<area>.<noun>_<unit>` — snake_case, dotted area prefix.

Areas: `ferrite` (core), `mnemo`, `forge`, `lucidity`, `chronicle`, `concord`, `pangea`.

Units suffix the metric name and follow OTel semantic conventions (`_total`, `_seconds`,
`_bytes`, `_count`).

Examples:

| Metric | Type | Description |
|---|---|---|
| `mnemo.requests_total` | counter | Count of MEMORY.* commands processed |
| `mnemo.recall_latency_seconds` | histogram | Latency of MEMORY.RECALL |
| `mnemo.records_resident` | gauge | Records currently held per (agent_id) |
| `forge.fn_call_latency_seconds` | histogram | FN.CALL warm-path latency |
| `forge.fn_call_errors_total{reason}` | counter | FN.CALL errors by category |
| `lucidity.proof_generation_seconds` | histogram | AUDIT.PROVE latency |
| `chronicle.branch_count` | gauge | Active branches |

### Logs

JSON lines with required fields:

```json
{
  "ts": "2026-04-17T10:00:00.123Z",
  "level": "info",
  "target": "ferrite_forge::runtime",
  "msg": "fn loaded",
  "fn_name": "rate_limit",
  "fn_hash": "sha256:...",
  "size_bytes": 12480
}
```

Forbidden in logs: key contents, value contents, embeddings, PII. Field names
documented per moonshot in their ADR.

### Traces

Spans use `<area>.<operation>` naming. Required attributes per span:

- `cmd` — RESP command name (if applicable)
- `agent_id`, `session_id` — for Mnemo
- `fn_name` — for Forge
- `branch_id` — for Chronicle
- `region` — for Concord

Spans MUST set status to `error` on failure with an `error.type` attribute.

## Required cardinality discipline

- Labels with unbounded cardinality (user IDs, key contents, query strings) are
  **forbidden** on metrics.
- Bounded labels (cmd name, error category, region) are encouraged.
- High-cardinality dimensions belong in logs/traces, not metrics.

## Per-moonshot dashboard

Each moonshot ships a Grafana dashboard at `ferrite-ops/grafana/dashboards/<name>.json`
showing:

1. Request rate (`requests_total` rate over 5m), grouped by command.
2. Latency p50 / p95 / p99 (from histogram quantile).
3. Error rate (errors_total / requests_total).
4. A moonshot-specific "health" panel (resident records for Mnemo, branch count for
   Chronicle, conflict rate for Concord, etc.).
5. Resource usage (CPU, memory, disk) attributable to the moonshot.

## Exporting

`ferrite.toml`:

```toml
[observability]
otel_endpoint = "http://otel-collector:4317"
otel_service_name = "ferrite"
log_format = "json"     # or "human" for dev
log_level = "info"
```

## Per-phase observability gates

| Phase | Required |
|---|---|
| **Spike (P0)** | At least one log line per code path |
| **Prototype (P1)** | Counter + latency histogram for the main op |
| **Alpha (P2/P3)** | Full metric set per table above + traces |
| **Beta (P4)** | Grafana dashboard committed |
| **GA (P5)** | Dashboard reviewed by ops, alerting rules suggested |

## Existing prior art

See `crates/ferrite-core/src/metrics/` for current metric registration patterns and
`src/server/` for trace propagation through the command pipeline.

---

## Moonshot metric catalogue

Each moonshot (M1–M6) exports the metrics listed below. All names follow the
`<area>_<noun>_<unit>` convention defined above and carry only bounded labels.

### Forge (M1 — Stored Functions)

| Metric | Type | Labels | Description |
|---|---|---|---|
| `forge_fn_calls_total` | counter | `fn_name`, `status` | Total stored-function invocations |
| `forge_fn_call_latency_seconds` | histogram | `fn_name` | Invocation latency (p50, p95, p99) |
| `forge_fn_call_errors_total` | counter | `fn_name`, `reason` | Errors by category — `reason` ∈ {`acl`, `timeout`, `oom`, `trap`, `budget`} |
| `forge_fn_modules_loaded` | gauge | — | Currently loaded WASM/Lua modules |
| `forge_fn_compile_seconds` | histogram | — | Module compilation / JIT time |

### Mnemo (M2 — Agentic Memory)

| Metric | Type | Labels | Description |
|---|---|---|---|
| `mnemo_requests_total` | counter | `command`, `tenant` | Total MEM.* commands processed |
| `mnemo_recall_latency_seconds` | histogram | — | MEM.RECALL end-to-end latency |
| `mnemo_records_resident` | gauge | `tenant`, `kind` | Records currently held in memory |
| `mnemo_storage_bytes` | gauge | `tenant` | On-disk storage consumed per tenant |
| `mnemo_summarise_job_duration_seconds` | histogram | — | Background summarisation job latency |

### Lucidity (M3 — Verifiable Audit Log)

| Metric | Type | Labels | Description |
|---|---|---|---|
| `lucidity_appends_total` | counter | — | Total entries appended to the log |
| `lucidity_proof_latency_seconds` | histogram | — | Merkle proof generation latency |
| `lucidity_log_size` | gauge | — | Current number of entries in the audit log |
| `lucidity_checkpoint_latency_seconds` | histogram | — | Checkpoint / snapshot latency |

### Chronicle (M4 — Branching & Time-Travel)

| Metric | Type | Labels | Description |
|---|---|---|---|
| `chronicle_branches_total` | gauge | — | Number of active branches |
| `chronicle_branch_create_seconds` | histogram | — | Branch creation latency |
| `chronicle_merge_seconds` | histogram | — | Branch merge latency |
| `chronicle_overlay_keys` | gauge | `branch` | Keys in the overlay for a given branch |

### Concord (M5 — CRDT Mesh)

| Metric | Type | Labels | Description |
|---|---|---|---|
| `concord_operations_total` | counter | `type` | CRDT operations by type (inc, dec, set, add, rem, merge) |
| `concord_convergence_seconds` | histogram | — | Time for a CRDT update to converge across replicas |
| `concord_keys_total` | gauge | — | Total CRDT-managed keys |
| `concord_metadata_bytes` | gauge | — | Bytes consumed by CRDT metadata overhead |

### Pangea (M6 — Unified Storage)

| Metric | Type | Labels | Description |
|---|---|---|---|
| `pangea_allocations_total` | counter | — | Total allocation requests |
| `pangea_free_bytes` | gauge | `node` | Free bytes available per storage node |
| `pangea_read_latency_seconds` | histogram | `tier` | Read latency by storage tier (memory, ssd, s3) |
| `pangea_migrations_total` | counter | `direction` | Data migrations (promote / demote) between tiers |
