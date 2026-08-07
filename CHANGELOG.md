# Changelog

All notable changes to Ferrite will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Raised the core workspace MSRV to Rust 1.88 to match the committed dependency graph and security-supported dependency releases.

## [0.4.0] - 2026-04-20

*Moonshot Extensions — six new experimental crates, four new command families, and AI agent SDK integrations.*

### Added

- **ferrite-mnemo** (M1): Agent memory OS crate — schema, key layout, scoring, summarization, and telemetry for persistent AI agent state (`MEM.*` commands)
- **ferrite-forge** (M2): WASM in-DB functions crate — execution engine, module signing, rate limiting, host interface, and WIT interface definition (`FN.*` commands)
- **ferrite-lucidity** (M3): Verifiable audit log crate — binary Merkle accumulator, ZK disclosure circuits, ed25519 signing, post-quantum support, and key rotation (`LUC.*` commands stub)
- **ferrite-chronicle** (M4): Branchable state crate — HAMT-backed branch registry, overlay reader, and GC (`CHR.*` commands stub)
- **ferrite-concord** (M5): Multi-master CRDT crate — G-counter, PN-counter, OR-Set, LWW/MV registers, gossip protocol, anti-entropy, delta sync, DVV causality, and TLA+ formal specs (`CON.*` commands)
- **ferrite-pangea** (M6): CXL tier-0 memory management crate — NUMA-aware allocator, policy engine, cache eviction, and topology management (`PNG.*` commands)
- **ferrite-spike-bridge**: Cross-moonshot integration crate with chronicle+lucidity, mnemo+forge, and pangea+concord integration test suites
- `MEM.*` command family: PUT, GET, RECALL, FORGET, SUMMARIZE, STATS, SAVE, LOAD, HELP
- `FN.*` command family: LOAD, DROP, CALL, CALL_RO, LIST, STATS, SHOW, VERSIONS, PROMOTE, BUDGET, SAVE, LOAD_FROM_STORE, HELP
- `CON.*` command family: GINC, GVAL, GMERGE, PNINC, PNVAL, PNMERGE, SADD, SREM, SMEMBERS, SMERGE, LWWSET, LWWGET, LWWMERGE, MVSET, MVGET, MVMERGE, DVV, CLOCK, PEERS, SYNC, ENTROPY, ROUTE, ADDRULE, RULES
- `PNG.*` command family: ALLOC, FREE, MIGRATE, POLICY, TIERPOLICY, STATS, TOPOLOGY, HELP
- Replication support for all mutating moonshot subcommands via `spike_replication_raw`
- `ferrite-fn` binary for standalone Forge WASM function runtime
- Node.js Mnemo client SDK (`sdk/node/ferrite-mnemo`) with LangChain memory adapter
- Python Mnemo client SDK (`sdk/python/ferrite_mnemo`) with LangChain, LlamaIndex, and Letta bindings
- Multi-language Forge function starter templates (Rust, Go, TypeScript)
- Agent-memory integration examples for LangGraph, LlamaIndex, and Letta
- Forge function examples: `jwt_verify`, `rate_limit`, `json_patch`, `hot_keys`, `custom_merge`
- Moonshot integration test harness (`tests/harness/`) with cross-crate smoke tests
- ADR-017 through ADR-023 covering all moonshot architecture decisions
- Moonshot phase roadmaps M1–M6 with wave gate criteria (`docs/phases/`)
- Design partner program documentation and LOI/report templates
- CXL cloud provider integration guides (AWS, Azure, GCP)
- AI Act compliance mapping
- Observability guide and moonshot docs pipeline specification
- Moonshot CI workflow (`.github/workflows/moonshot-ci.yml`)
- Docs gate action (`.github/workflows/docs-gate.yml`)
- `forge-runtime` feature flag for opt-in Forge WASM runtime

### Changed

- `publish.yml` updated to include all 7 new moonshot crates in dependency order
- `CommandExecutor` now routes `Concord`, `Forge`, `Pangea`, and `Mnemo` command variants
- `CommandMeta` extended with entries for all moonshot command families
- Command parser extended to dispatch moonshot command families
- Handler module exposes shared `bulk()` helper and `should_persist()` debounce utility
- Workspace version bumped to 0.4.0

## [0.3.0] - 2026-03-09

*Observability, Operations & AI Integrations — see [ROADMAP.md](ROADMAP.md) for details.*

### Added
- 5 new Grafana dashboards: Memory Tiers, Query Performance, Cluster & Replication, CDC & Streaming, Vector Search & AI
- 7 new Prometheus alert rules: vector index stalled, CDC consumer lag, tier promotion spike, disk I/O latency, memory fragmentation, split-brain detection, backup overdue
- 6 operational runbooks: high-memory, high-latency, replication-lag, cluster-failure, backup-failure, disk-full
- Grafana provisioning configs for auto-loading dashboards and datasources
- CI/CD workflows for Rust SDK (crates.io) and Node.js SDK (npm)
- SDK integration test harness with Docker Compose and cross-language test runner
- Docker Hub publishing support in release workflow with smoke testing
- Tiered storage benchmark script for hot/warm/cold tier performance measurement
- Persistence impact benchmark script (AOF-always vs AOF-everysec vs no-persist)
- Hardware attestation script for benchmark reproducibility
- Zipfian and batch-ops scenarios in benchmark harness
- LangChain integration guide with LLM caching, vector store, and RAG pipeline examples
- LlamaIndex integration guide with vector store and query engine examples
- "Ferrite for AI" landing page on documentation site
- Redis-to-Ferrite migration playbook (8-phase operational guide)
- Redis Cluster migration guide for existing cluster deployments
- Jepsen Docker test environment with toxiproxy fault injection
- DEVELOPMENT.md with local dev workflow, debugging tips, and test guidance
- Local fuzzing guide in CONTRIBUTING.md
- CI pipeline documentation for contributors

### Changed
- Redis compatibility documentation updated from ~72% to ~92% (corrected 40+ stale entries)
- REDIS_COMPAT.md summary updated to reflect actual implementation status
- README.md badge updated from 72% to 92% compatibility
- Configuration reference enhanced with hot-reload and restart-required matrix
- Error reference enhanced with complete FerriteError→RESP mapping (27 variants)
- Monitoring docs updated with dashboard descriptions and alert rule summaries
- HybridLog internals docs updated with Mermaid architecture diagram
- Added experimental/beta feature banners to 11 documentation pages
- Homebrew formula: fixed broken depends_on line, enabled Linux x86_64 bottle, cleaned TODOs
- Benchmark docker-compose: pinned Redis image to 7.4.2-alpine
- Clarified duplicate compat scripts with descriptive headers

### Fixed
- Homebrew formula broken `depends_on` line (concatenated with comment)
- COMMUNITY.md removed dead Discord "Coming soon" placeholder
- SECURITY.md replaced vague email guidance with explicit security@ferritelabs.dev

## [0.2.0] - 2026-02-28

*Core Hardening — see [ROADMAP.md](ROADMAP.md) for details.*

### Added
- Delta sync protocol for edge replication, enabling bandwidth-efficient incremental updates between edge nodes and cloud hub
- ROADMAP.md with versioned milestones and feature graduation targets
- `detect-secrets` pre-commit hook for secret scanning
- `gitleaks` secret scanning in CI for all organization repositories
- `missing_docs = "warn"` lint for `ferrite-core` public API
- Shared test utilities module (`tests/common/`) for integration tests
- Enhanced SAFETY comments on WASM example unsafe blocks
- HybridLog Storage graduation from Beta to Stable
- Replication (PSYNC2) graduation from Beta to Stable
- Redis compatibility test suite with published results
- Benchmark comparison report (vs Redis, Dragonfly, KeyDB)
- First `crates.io` release
- Configurable compaction threshold for tiered storage
- Property tests for HybridLog mutable region
- Unified observer tracing for storage ops
- Redis TCL test suite compatibility commands

### Changed
- Clarified MSRV vs contributor toolchain in README (1.80 MSRV, 1.88 dev toolchain)
- Enforced `clippy::unwrap_used = "deny"` across the workspace
- Increased unit test coverage for `ferrite-core` (target: >70%)
- Reduced allocation overhead in RESP protocol parser
- Simplified epoch-based reclamation in storage engine
- Optimized query parser token lookahead
- Reorganized storage tier module hierarchy

### Fixed
- Resolved race condition in cluster node discovery
- Resolved compatibility tracker initialization race
- Fixed edge case in RESP3 parser for nested arrays

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

[Unreleased]: https://github.com/ferritelabs/ferrite/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/ferritelabs/ferrite/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/ferritelabs/ferrite/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/ferritelabs/ferrite/releases/tag/v0.1.0
