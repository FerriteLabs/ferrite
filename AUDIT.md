# Single-Responsibility Refactor Audit

## Summary

- BASE-001, BASE-002, and BASE-003 resolve the verification blockers that previously prevented reliable workspace-wide formatting, linting, and test gates.
- The highest-leverage split is SRP-01: separating `advanced_ops.rs` by complete command family removes the largest concentration of unrelated command actors while preserving `CommandExecutor` dispatch.
- SRP-02 and SRP-06 isolate state-owning runtime actors, reducing synchronization and lifecycle reasoning from broad orchestrator modules.
- SRP-03 and SRP-04 protect compatibility-sensitive byte formats, command frames, and parser errors with characterization tests before structural moves.
- SRP-05 keeps the public configuration router stable while moving parsing, validation, and mutation decisions to the configuration sections that own the affected state.

## Findings

| ID | location | category | severity P0/P1/P2 | actors-in-conflict | cost | size S/M/L | behavior risk |
| --- | --- | --- | --- | --- | --- | --- | --- |
| BASE-001 | commit `225bc27` | resolved verification blocker | P0 | workspace verification vs. pre-existing failing tests | Resolved: full verification baseline restored | S | Low; verification-only repair is already committed |
| BASE-002 | commit `755c63b` | resolved verification blocker | P0 | deterministic verification vs. unstable tests | Resolved: workspace verification stabilized | S | Low; test stabilization is already committed |
| BASE-003 | commit `ec484bb` | resolved verification blocker | P0 | plugin marketplace tests vs. shared filesystem state | Resolved: marketplace filesystem state isolated | S | Low; test isolation is already committed |
| SRP-01 | `src/commands/executor/advanced_ops.rs:1-5526` | command execution responsibilities | P1 | tiering, CDC, temporal, Redis streams, geo, search/semantic, CRDT, trigger, streaming, federation, and region command families | High navigation and change-coupling cost in a single inherent impl | L | High; frames, feature gates, metrics, and async behavior must remain exact |
| SRP-02 | `src/commands/blocking.rs:1-3049` | blocking coordination responsibilities | P1 | list waiters, stream readers/groups, and sorted-set pop coordination | High concurrency review cost and broad test surface | L | High; wake ordering, timeout, cancellation, and mutation ordering are observable |
| SRP-03 | `crates/ferrite-core/src/persistence/backup/manager.rs:213-1250` | backup codec vs. lifecycle policy | P1 | serialization/deserialization, storage I/O, retention, compression, and incremental-chain policy | High compatibility and maintenance cost | M | High; existing backup bytes, tags, versions, and malformed-input errors must remain exact |
| SRP-04 | `src/commands/parser/parsers/advanced.rs:1-1636`; `src/commands/parser/parsers/cluster.rs:1-363` | command parsing responsibilities | P1 | unrelated command-family grammars and error paths | Medium navigation and merge-conflict cost | L | High; `Frame -> Command`, arity errors, coercions, and ignored arguments are compatibility surfaces |
| SRP-05 | `crates/ferrite-core/src/config.rs:488-751` | configuration ownership | P2 | routing vs. leaf parsing, validation, and state mutation | Medium extension cost whenever a mutable setting is added | M | Medium; exact values, errors, mutation order, and restart requirements must remain stable |
| SRP-06 | `crates/ferrite-core/src/observability/unified_observer.rs:600-1000` | observability actor ownership | P1 | session telemetry orchestration, alert rule state, cooldown decisions, and probe lifecycle | High synchronization and lifecycle reasoning cost | M | High; alert thresholds, replacement behavior, cooldowns, and probe restrictions are observable |

## Ordered Refactor Sequence

1. **SRP-01 — Advanced command execution families:** add characterization coverage where seams are missing, then move complete families into private sibling inherent-impl modules without changing dispatch or behavior.
2. **SRP-02 — Blocking managers:** characterize wake, timeout, cancellation, stream group, and sorted-set behavior, then move each state-owning manager and its cohesive support to `blocking/{list,stream,sorted_set}.rs`.
3. **SRP-03 — Backup codec:** add byte-level golden and malformed-input tests, then extract a private `BackupCodec` while leaving lifecycle, I/O, retention, compression, and chain policy in `BackupManager`.
4. **SRP-04 — Advanced and cluster parsers:** add representative public-path `Frame -> Command` tests, then split complete parser families into private modules re-exported through the current parents.
5. **SRP-05 — Runtime configuration mutation:** add table-driven characterization tests, retain `Config::set_param` as the router, and move parsing/validation/mutation to private methods on existing leaf configuration owners.
6. **SRP-06 — Unified observer actors:** add alert/probe characterization tests, then extract private `AlertManager { rules }` and `ProbeRegistry { probes }`, keeping telemetry and session orchestration in `UnifiedObserver`.

## Out of Scope

- `src/commands/executor/mod.rs:878-2581` remains out of scope because it is a long but cohesive central dispatch unit; splitting it in this pass would mix routing changes with the command-family extraction and materially increase frame/metric regression risk.
- `src/commands/executor/meta.rs` remains out of scope because its command implementations form a cohesive metadata and administration surface whose internal split requires a separate compatibility audit.
- `crates/ferrite-core/src/query/parser.rs` remains out of scope because it is a cohesive grammar/parser implementation; size alone is not an SRP violation, and restructuring it would add parser risk unrelated to the requested command parser split.
- `crates/ferrite-core/src/cluster/raft.rs` remains out of scope because consensus state transitions and persistence are intentionally co-located for invariant review; decomposing them requires a dedicated distributed-systems design pass.
- `src/migration/rdb_parser.rs` remains out of scope because byte-level RDB parsing is a cohesive compatibility unit whose format and corruption handling need a dedicated golden corpus before structural changes.


## Completion Status

All six SRP refactoring items completed and committed:

| ID | Commit | Summary |
| --- | --- | --- |
| SRP-01 | `48d030f` | Split advanced_ops.rs into 8 command-family modules + tiering consolidation |
| SRP-02 | `d103011` | Split blocking.rs into blocking/{list,stream,sorted_set}.rs |
| SRP-03 | `b382ae0` | Extract backup codec.rs for serialization/deserialization |
| SRP-04 | `557ac05` | Extract 6 parser submodules from advanced.rs + 1 from cluster.rs |
| SRP-05 | `1faf2b0` | Delegate Config::set_param to leaf config struct owners |
| SRP-06 | `d957625` | Extract AlertManager and ProbeRegistry from UnifiedObserver |

All commits pass: `cargo fmt --all --check`, `cargo clippy --workspace --all-features -- -D warnings`, `cargo test --workspace --all-features --quiet`.

### Assumptions
- SRP-01: WASM, timeseries, document, graph, RAG, JSON, bloom, query, advisor, FaaS, view, migrate, studio, gateway, budget methods left in advanced_ops.rs as they were not in the listed families
- SRP-01: Kafka streaming handlers grouped with Redis stream commands in stream_ops.rs
- SRP-02: Tests kept in blocking/mod.rs with pub(crate) visibility for test-accessed internals
- SRP-03: Codec extracted as additional impl BackupManager block (unchanged move); BackupCodec struct deferred
- SRP-04: crdt + wasm parsers combined into crdt_wasm_parsers.rs to avoid thin modules
- SRP-05: AuditConfig and EncryptionConfig single-arm handling kept inline in Config::set_param (too thin for own method)
- SRP-06: check_alerts receives global_stats and sessions as parameters since AlertManager doesn't own them
