# ADR-005: Extension Crate Architecture

- **Date:** 2026-02-20
- **Status:** Accepted

## Context

Ferrite offers features beyond basic key-value storage — graph database operations, document
store queries, AI/ML vector search, time-series, streaming, and scripting. These features
have distinct dependencies, release cadences, and testing requirements. Bundling everything
into a single crate would create a monolithic build with slow compile times, tangled
dependencies, and all-or-nothing feature inclusion.

## Decision

We organize the codebase as a Cargo workspace with self-contained extension crates under
`crates/`:

- **Extension crates** (`ferrite-search`, `ferrite-ai`, `ferrite-graph`, `ferrite-timeseries`,
  etc.) are self-contained and **do not depend on each other or on `ferrite-core` internals**.
- **`ferrite-core`** provides foundational types: storage engine, protocol parsing,
  configuration, authentication, cluster management, and metrics.
- **The top-level `ferrite` crate** serves as the integration layer, housing `commands/`,
  `server/`, and `replication/` modules that wire everything together.
- Feature flags control which extension crates are compiled into the final binary
  (e.g., `lite`, `experimental`, `all`).

Dependency DAG:
```
ferrite-core  ←  ferrite-{search,ai,graph,...}  ←  ferrite (top-level)
```

## Consequences

### Positive

- Clean, acyclic dependency graph — easy to reason about and enforce in CI
- Each extension crate can be tested, versioned, and released independently
- Compile times improve since unchanged crates are cached
- Feature flags let users build minimal or full-featured binaries

### Negative

- Requires careful API design at integration boundaries (top-level ↔ extensions)
- Shared types must live in `ferrite-core`, which can become a "kitchen sink" if not curated
- Cross-crate refactoring is more involved than single-crate changes
- New contributors must understand the workspace layout before contributing
