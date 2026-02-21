# Good First Issues for FerriteLabs/ferrite

These issues are tagged as `good first issue` to help new contributors get started.
Create each as a GitHub issue with labels: `good first issue`, `help wanted`, and the appropriate area label.

---

## Issue 1: Pin model checksums in embedding catalog
**Labels:** `good first issue`, `ferrite-ai`
**File:** `crates/ferrite-ai/src/embedding/catalog.rs:72`
**Description:** The model catalog has `None` for checksums with a TODO to pin them once models are verified. Add SHA256 checksums for each model in the catalog to ensure download integrity.

## Issue 2: Implement ONNX inference in RAG embedder
**Labels:** `good first issue`, `ferrite-ai`, `enhancement`
**File:** `crates/ferrite-ai/src/rag/embed.rs:773`
**Description:** Replace the placeholder with full ONNX inference support using the `ort` 2.0 API for the RAG embedding pipeline.

## Issue 3: Track pub/sub state in connection handler
**Labels:** `good first issue`, `enhancement`
**File:** `src/server/handler.rs:1028`
**Description:** The RESET command notes that pub/sub mode exit needs pub/sub state tracking in the handler. Implement pub/sub state tracking so RESET can properly clean up subscriptions.

## Issue 4: Implement document sort in DocumentStore
**Labels:** `good first issue`, `ferrite-document`, `enhancement`
**File:** `src/commands/handlers/document.rs:280`
**Description:** The document query handler has a TODO to implement sort when DocumentStore supports it. Add sorting capability to document queries.

## Issue 5: Add SDK generator tests for all languages
**Labels:** `good first issue`, `sdk`, `testing`
**Files:** `src/sdk/generator.rs:433,572`
**Description:** The SDK generator produces placeholder test and example files (`"// TODO: Add tests"`, `"// TODO: Add examples"`). Implement proper test and example generation for each SDK language target.

## Issue 6: Document dead_code modules with stabilization criteria
**Labels:** `good first issue`, `documentation`
**Description:** 40+ modules have `#![allow(dead_code)]` for in-development features. Create a tracking table in `docs/` listing each module, its current status (experimental/beta/stable), and the criteria needed to remove the dead_code annotation.

## Issue 7: Add property-based tests for HybridLog storage
**Labels:** `good first issue`, `testing`, `ferrite-core`
**Description:** The HybridLog storage engine (Mutable → Read-Only → Disk) lacks property-based tests. Add `proptest` tests in `crates/ferrite-core/` to verify: insert/read roundtrips, concurrent reads/writes, tier promotion correctness.

## Issue 8: Add property-based tests for RESP parser
**Labels:** `good first issue`, `testing`, `ferrite-core`
**Description:** Add `proptest` or `quickcheck` tests for the RESP protocol parser to verify roundtrip encoding/decoding for all frame types (Simple String, Error, Integer, Bulk String, Array, Null).

## Issue 9: Add fuzz target for ACL rule parsing
**Labels:** `good first issue`, `security`, `testing`
**Description:** The `fuzz/` directory has targets for RESP parsing, command parsing, config, gossip, and WAL. Add a fuzz target for ACL rule parsing (`crates/ferrite-core/src/auth/acl.rs`) since this is a security-critical surface.

## Issue 10: Improve error messages in FerriteQL parser
**Labels:** `good first issue`, `ferrite-core`, `dx`
**File:** `crates/ferrite-core/src/query/parser.rs`
**Description:** The FerriteQL parser could provide better error messages with line/column information and suggestions for common mistakes. Improve the parser error output for better developer experience.

## Issue 11: Add shell completion scripts
**Labels:** `good first issue`, `cli`, `dx`
**Description:** Generate shell completion scripts (bash, zsh, fish) for `ferrite-cli` using `clap_complete`. Add installation instructions to the CLI documentation.

## Issue 12: Add Redis COMMAND DOCS compatibility
**Labels:** `good first issue`, `compatibility`
**Description:** Implement the `COMMAND DOCS` subcommand (Redis 7.0+) to return documentation for each command. This helps Redis client libraries auto-discover command metadata.

## Issue 13: Add benchmarks for persistence operations
**Labels:** `good first issue`, `performance`, `testing`
**Description:** The `benches/` directory has latency, throughput, tiered_storage, and vector benchmarks but no persistence benchmarks. Add criterion benchmarks for AOF write/fsync, RDB snapshot creation, and checkpoint operations.

## Issue 14: Create a migration guide from Redis
**Labels:** `good first issue`, `documentation`
**Description:** Create `docs/migration-from-redis.md` covering: compatibility matrix (what works, what doesn't), configuration mapping (redis.conf → ferrite.toml), deployment migration steps, and known behavioral differences.

## Issue 15: Add integration test for IDE extension against live server
**Labels:** `good first issue`, `testing`, `vscode-ferrite`
**Description:** The VS Code extension has build/lint CI but no integration tests against a live Ferrite server. Add a CI workflow that starts a Ferrite server in Docker and runs basic extension operations (connect, SET/GET, key browser).
