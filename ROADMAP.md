# Roadmap

This document outlines Ferrite's development trajectory and feature maturity targets.
For the live feature status of every capability, see [Feature Maturity](docs/FEATURE_MATURITY.md).

## Vision

**Ferrite** aims to be the go-to Redis replacement for teams that need the speed of in-memory caching, the capacity of disk-backed storage, and the economics of cloud tiering — all in a single binary.

## Release Timeline

### v0.2.0 — Core Hardening

Focus: production-readiness of the foundational feature set.

- [ ] Enforce `clippy::unwrap_used = "deny"` across the workspace (eliminate ~1,300 production unwraps)
- [ ] Increase unit test coverage for `ferrite-core` (target: >70%)
- [ ] Graduate **HybridLog Storage** from 🧪 Beta → ✅ Stable
- [ ] Graduate **Replication** (primary-replica with PSYNC2) from 🧪 Beta → ✅ Stable
- [ ] Publish Redis compatibility percentage from TCL test suite
- [ ] First `crates.io` release (`cargo install ferrite`)
- [ ] Publish benchmark comparison report (vs Redis, Dragonfly, KeyDB)

### v0.3.0 — Enterprise & Observability

Focus: features required for production deployments at scale.

- [ ] Graduate **Cluster Mode** from 🧪 Beta → ✅ Stable (automated failover, Jepsen-style testing)
- [ ] Graduate **Vector Search** from 🧪 Beta → ✅ Stable (end-to-end server wiring + benchmarks)
- [ ] Graduate **OpenTelemetry** from 🧪 Beta → ✅ Stable
- [ ] Graduate **CDC / Event Streaming** from 🧪 Beta → ✅ Stable
- [ ] Graduate **Full-Text Search** from 🧪 Beta → ✅ Stable
- [ ] Expand integration tests for extension crates (search, AI, streaming, cloud)
- [ ] Kubernetes Operator CRD scaffolding and basic reconciliation loop

### v0.4.0 — Advanced Data Models

Focus: multi-model capabilities and query language.

- [ ] Graduate **FerriteQL** from 🔬 Experimental → 🧪 Beta
- [ ] Graduate **Time-Travel Queries** from 🔬 Experimental → 🧪 Beta
- [ ] Graduate **Semantic Caching** from 🔬 Experimental → 🧪 Beta
- [ ] Graduate **Document Store** from 🔬 Experimental → 🧪 Beta
- [ ] Graduate **Graph Database** from 🔬 Experimental → 🧪 Beta
- [ ] Graduate **Multi-Tenancy** from 🔬 Experimental → 🧪 Beta

### v1.0.0 — Production-Grade Release

Focus: API stability, migration tooling, and ecosystem maturity.

- [ ] Freeze all Tier 1 and Tier 2 APIs (breaking changes require major version bump)
- [ ] >90% test coverage for all stable features
- [ ] Complete migration guide from Redis
- [ ] Redis Cluster protocol compatibility (auto-migration for existing deployments)
- [ ] Published performance benchmarks with reproducible methodology
- [ ] Ferrite Studio web UI for monitoring and management
- [ ] Kubernetes Operator with full CRD lifecycle management

## Experimental Explorations

These are research-stage ideas that may or may not land in a release:

| Exploration | Status | Description |
|-------------|--------|-------------|
| eBPF Observability | 📋 Design | Zero-overhead production tracing without code changes |
| WASM Plugin Marketplace | 📋 Design | Community-contributed data transformations |
| Raft Consensus | 📋 Design | Alternative to gossip for production-grade cluster consensus |
| Redis Cluster Protocol | 📋 Design | Wire-compatible cluster protocol for zero-migration adoption |

## Graduation Criteria

Features move between tiers based on concrete quality gates:

**🔬 Experimental → 🧪 Beta:**
- Core functionality complete
- Unit tests with >70% coverage
- Integration tests for happy paths
- API documentation

**🧪 Beta → ✅ Stable:**
- API frozen (breaking changes require major version bump)
- Unit tests with >90% coverage
- Integration and property-based tests
- Performance benchmarks
- Production usage by at least one deployment
- Migration guide from previous API versions

## Contributing

We welcome contributions at every level! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.
Feature work is tracked in [GitHub Issues](https://github.com/ferritelabs/ferrite/issues).
