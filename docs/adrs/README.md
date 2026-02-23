# Architecture Decision Records (ADRs)

This directory contains the Architecture Decision Records (ADRs) for the Ferrite project.

ADRs document significant architectural decisions made during the development of Ferrite,
a high-performance tiered-storage key-value store designed as a drop-in Redis replacement.

## Index

### Accepted

| ADR | Title | Status | Date |
|-----|-------|--------|------|
| [ADR-001](adr-001-hybridlog-tiered-storage.md) | HybridLog Tiered Storage Engine | Accepted | 2026-02-20 |
| [ADR-002](adr-002-epoch-based-memory-reclamation.md) | Epoch-Based Memory Reclamation | Accepted | 2026-02-20 |
| [ADR-003](adr-003-thread-per-core-architecture.md) | Thread-Per-Core Architecture | Accepted | 2026-02-20 |
| [ADR-004](adr-004-resp-protocol-compatibility.md) | RESP2/RESP3 Protocol Compatibility | Accepted | 2026-02-20 |
| [ADR-005](adr-005-extension-crate-architecture.md) | Extension Crate Architecture | Accepted | 2026-02-20 |
| [ADR-006](adr-006-wasmtime-plugin-runtime.md) | Wasmtime as the WASM Plugin Runtime | Accepted | 2026-02-20 |
| [ADR-007](adr-007-argon2-password-hashing.md) | Argon2id for Password Hashing | Accepted | 2026-02-20 |

### Proposed

| ADR | Title | Status | Date |
|-----|-------|--------|------|
| [ADR-008](adr-008-interactive-playground.md) | Interactive Playground (try.ferrite.dev) | Proposed | 2026-02-20 |
| [ADR-009](adr-009-redis-modules-api.md) | Redis Modules API Compatibility | Proposed | 2026-02-20 |
| [ADR-010](adr-010-ai-native-caching.md) | AI-Native Caching Niche | Proposed | 2026-02-20 |
| [ADR-011](adr-011-kubernetes-sidecar.md) | Kubernetes Sidecar Mode | Proposed | 2026-02-20 |
| [ADR-012](adr-012-ebpf-observability.md) | eBPF-Based Observability | Proposed | 2026-02-20 |
| [ADR-013](adr-013-wasm-plugin-marketplace.md) | WASM Plugin Marketplace | Proposed | 2026-02-20 |
| [ADR-014](adr-014-redis-cluster-protocol.md) | Redis Cluster Protocol Compatibility | Proposed | 2026-02-20 |
| [ADR-015](adr-015-raft-consensus.md) | Raft Consensus for Cluster Mode | Proposed | 2026-02-20 |

## Format

Each ADR follows the standard format:

- **Title** — Short noun phrase
- **Status** — Proposed | Accepted | Deprecated | Superseded
- **Context** — Forces at play
- **Decision** — What we decided
- **Consequences** — Trade-offs and implications
