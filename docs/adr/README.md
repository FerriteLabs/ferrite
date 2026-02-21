# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for the Ferrite project.

## Index

| ADR | Title | Status | Date |
|-----|-------|--------|------|
| [001](001-hybridlog-storage-engine.md) | HybridLog Storage Engine | Accepted | 2025-01-01 |
| [002](002-epoch-based-reclamation.md) | Epoch-Based Memory Reclamation | Accepted | 2025-01-01 |
| [003](003-workspace-crate-structure.md) | Workspace Crate Structure | Accepted | 2025-01-15 |
| [004](004-interactive-playground.md) | Interactive Playground (try.ferrite.dev) | Proposed | 2026-02-18 |
| [005](005-redis-modules-api.md) | Redis Modules API Compatibility | Proposed | 2026-02-18 |
| [006](006-edge-wasm-computing.md) | Embedded WASM for Edge Computing | Proposed | 2026-02-18 |
| [007](007-ai-native-caching.md) | AI-Native Caching Niche | Proposed | 2026-02-18 |
| [008](008-kubernetes-sidecar.md) | Kubernetes Sidecar Mode | Proposed | 2026-02-18 |
| [009](009-ebpf-observability.md) | eBPF-Based Observability | Proposed | 2026-02-19 |
| [010](010-wasm-plugin-marketplace.md) | WASM Plugin Marketplace | Proposed | 2026-02-19 |
| [011](011-redis-cluster-protocol-compat.md) | Redis Cluster Protocol Compatibility | Proposed | 2026-02-19 |
| [012](012-raft-consensus.md) | Raft Consensus for Cluster Mode | Proposed | 2026-02-19 |

## Template

Use this template for new ADRs:

```markdown
# ADR-NNN: Title

## Status
Proposed | Accepted | Deprecated | Superseded by ADR-NNN

## Context
What is the issue that we're seeing that is motivating this decision?

## Decision
What is the change that we're proposing and/or doing?

## Consequences
What becomes easier or more difficult because of this change?
```
