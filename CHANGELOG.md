# Changelog

All notable changes to Ferrite will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Delta sync protocol for edge replication, enabling bandwidth-efficient incremental updates between edge nodes and cloud hub
- ROADMAP.md with versioned milestones and feature graduation targets
- `detect-secrets` pre-commit hook for secret scanning
- `gitleaks` secret scanning in CI for all organization repositories
- `missing_docs = "warn"` lint for `ferrite-core` public API
- Shared test utilities module (`tests/common/`) for integration tests
- Enhanced SAFETY comments on WASM example unsafe blocks

### Changed
- Clarified MSRV vs contributor toolchain in README (1.80 MSRV, 1.88 dev toolchain)

## [0.3.0] — Planned

*Target: Enterprise & Observability — see [ROADMAP.md](ROADMAP.md) for details.*

### Added
- Cluster Mode graduation to Stable (automated failover testing)
- Vector Search end-to-end server wiring and benchmarks
- OpenTelemetry graduation to Stable
- CDC / Event Streaming graduation to Stable
- Full-Text Search graduation to Stable
- Kubernetes Operator CRD scaffolding
- Integration tests for extension crates (search, AI, streaming, cloud)

## [0.2.0] — Planned

*Target: Core Hardening — see [ROADMAP.md](ROADMAP.md) for details.*

### Added
- HybridLog Storage graduation from Beta to Stable
- Replication (PSYNC2) graduation from Beta to Stable
- Redis compatibility test suite with published results
- Benchmark comparison report (vs Redis, Dragonfly, KeyDB)
- First `crates.io` release

### Changed
- Enforced `clippy::unwrap_used = "deny"` across the workspace
- Increased unit test coverage for `ferrite-core` (target: >70%)

## [0.1.0] - 2025-01-23

Initial release targeting feature parity with Redis core functionality.

### Added
- Full RESP2/RESP3 protocol compatibility
- All core data types: Strings, Lists, Hashes, Sets, Sorted Sets, HyperLogLog
- Key operations: DEL, EXISTS, EXPIRE, TTL, PTTL, KEYS, SCAN, TYPE, RENAME
- Pub/Sub: SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUBLISH
- Transactions: MULTI, EXEC, DISCARD, WATCH
- Lua Scripting: EVAL, EVALSHA, SCRIPT
- Persistence: AOF with configurable sync + periodic checkpoints
- Prometheus metrics endpoint on port 9090
- Embedded mode: library usage without server (`Database::open(...)`)
- Docker multi-stage build + Compose with Prometheus/Grafana profiles
- HybridLog three-tier storage engine (Mutable/ReadOnly/Disk) 🧪
- io_uring-first I/O with automatic tokio::fs fallback 🧪
- Redis Cluster hash slot support (16384 slots) 🧪
- Distributed transactions with 2PC and MVCC 🧪
- WASM-based user-defined functions via wasmtime 🔬
- Embedded vector search with HNSW/IVF indexes 🧪
- ACL-based permission system with Argon2 password hashing
- Programmable triggers (FerriteFunctions) 🔬
- Time-series data support 🧪
- Multi-model database features (document, graph, search) 🔬
- Ferrite Studio web UI for monitoring and management 🔬

### Security
- Redis-compatible ACL system
- TLS 1.2/1.3 support for encrypted connections
- Secure password hashing with Argon2

[Unreleased]: https://github.com/ferritelabs/ferrite/compare/v0.1.0...HEAD
[0.3.0]: https://github.com/ferritelabs/ferrite/milestone/3
[0.2.0]: https://github.com/ferritelabs/ferrite/milestone/2
[0.1.0]: https://github.com/ferritelabs/ferrite/releases/tag/v0.1.0
