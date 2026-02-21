# Architecture Decision Records (ADRs)

This directory contains the Architecture Decision Records (ADRs) for the Ferrite project.

ADRs document significant architectural decisions made during the development of Ferrite,
a high-performance tiered-storage key-value store designed as a drop-in Redis replacement.

## Index

| ADR | Title | Status | Date |
|-----|-------|--------|------|
| [ADR-001](adr-001-hybridlog-tiered-storage.md) | HybridLog Tiered Storage Engine | Accepted | 2026-02-20 |
| [ADR-002](adr-002-epoch-based-memory-reclamation.md) | Epoch-Based Memory Reclamation | Accepted | 2026-02-20 |
| [ADR-003](adr-003-thread-per-core-architecture.md) | Thread-Per-Core Architecture | Accepted | 2026-02-20 |
| [ADR-004](adr-004-resp-protocol-compatibility.md) | RESP2/RESP3 Protocol Compatibility | Accepted | 2026-02-20 |
| [ADR-005](adr-005-extension-crate-architecture.md) | Extension Crate Architecture | Accepted | 2026-02-20 |

## Format

Each ADR follows the standard format:

- **Title** — Short noun phrase
- **Status** — Proposed | Accepted | Deprecated | Superseded
- **Context** — Forces at play
- **Decision** — What we decided
- **Consequences** — Trade-offs and implications
