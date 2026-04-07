# custom_merge — LWW-by-Source-Rank Merge for Chronicle

A custom CRDT merge strategy for Ferrite Chronicle that resolves concurrent
writes using last-write-wins semantics with source-rank tie-breaking.

## What it does

When Chronicle detects conflicting writes from multiple replicas, it invokes
this module to decide the winning value. The strategy:

1. Compare timestamps — the later write wins.
2. On timestamp tie, compare source ranks (lower rank = higher priority).
3. On both tie, compare values lexicographically for determinism.

This gives you **last-write-wins** with deterministic tie-breaking, which is
useful for multi-region deployments where clock skew is bounded.

## Build

```bash
cargo component build --release
```

## Deploy

```bash
redis-cli FN.LOAD custom_merge $(cat target/wasm32-wasip2/release/ferrite_fn_custom_merge.wasm | base64)

# Register as the merge function for a key prefix
redis-cli CHRONICLE.MERGE_FN "user:*" custom_merge
```

## Use

```bash
# The module is called automatically by Chronicle during replication.
# You can also test it manually:
redis-cli FN.CALL custom_merge merge:test '{"local":{"value":"A","ts":100,"source_rank":1},"remote":{"value":"B","ts":100,"source_rank":2}}'
# => {"winner":"local","value":"A","reason":"lower source rank"}
```

### Input JSON

| Field | Type | Description |
|-------|------|-------------|
| `local.value` | `string` | Local replica value (base64 if binary) |
| `local.ts` | `u64` | Local write timestamp (ms) |
| `local.source_rank` | `u32` | Local source rank (lower = higher priority) |
| `remote.value` | `string` | Remote replica value |
| `remote.ts` | `u64` | Remote write timestamp (ms) |
| `remote.source_rank` | `u32` | Remote source rank |

### Output JSON

| Field | Type | Description |
|-------|------|-------------|
| `winner` | `string` | `"local"` or `"remote"` |
| `value` | `string` | The winning value |
| `reason` | `string` | Why this value won |
