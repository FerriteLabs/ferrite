# ADR-003: Workspace Crate Structure

## Status
Accepted

## Context
As Ferrite grew beyond core Redis compatibility to include vector search, graph databases, time series, streaming, and more, the codebase needed modularization. A monolithic crate would lead to:
- Long compile times for any change
- Difficulty reasoning about dependencies
- Feature flag complexity explosion

## Decision
Structure the project as a Cargo workspace with isolated extension crates:

```
ferrite-core                  ← Foundation (storage, protocol, config, auth, cluster)
    ↑ (no deps on core)
ferrite-{search,ai,graph,...} ← Self-contained extensions
    ↑
ferrite (top-level)           ← Integration layer (commands/, server/)
```

Key rules:
1. Extension crates are **self-contained** — they don't depend on ferrite-core or each other
2. The top-level crate is the **integration layer** that imports from all extensions
3. All dependency versions are defined in `[workspace.dependencies]` (single source of truth)
4. Crates inherit `version`, `edition`, `authors`, `license`, `lints` from workspace

## Consequences
**Positive:**
- Incremental compilation: changing ferrite-ai doesn't recompile ferrite-core
- Clear dependency boundaries prevent accidental coupling
- Extensions can be developed and tested independently
- Smart CI: only test changed crates on PRs

**Negative:**
- More initial boilerplate (12 Cargo.toml files)
- Command dispatch lives in the top-level crate (large executor.rs)
- Need to coordinate workspace-wide releases
