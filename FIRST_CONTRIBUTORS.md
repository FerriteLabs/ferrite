# First-Time Contributor Guide

Welcome to **Ferrite** — a high-performance tiered-storage key-value store and drop-in Redis replacement, built in Rust. This guide will get you from zero to your first pull request.

> Already comfortable with the codebase? Check [CONTRIBUTING.md](CONTRIBUTING.md) for the full contributor reference, or [CONTRIBUTING_QUICKSTART.md](CONTRIBUTING_QUICKSTART.md) for a condensed setup guide.

---

## 1. Quick Setup (< 60 seconds)

**Prerequisites:**

- Rust 1.80+ (`rustup update stable`)
- Git
- Linux recommended for full io_uring support; macOS/Windows work with fallback I/O

**Clone and build:**

```bash
git clone https://github.com/FerriteLabs/ferrite.git
cd ferrite
cargo build && cargo test --lib
```

Or if a Makefile target is available:

```bash
make setup
```

**Verify everything works:**

```bash
cargo test --lib
```

This runs 5,000+ unit tests and should complete in ~7 seconds. If it passes, you're ready to contribute.

**Useful dev commands:**

```bash
cargo fmt --all --check       # Check formatting
cargo clippy --workspace -- -D warnings  # Lint
RUST_LOG=ferrite=debug cargo run          # Run server with debug logs
```

---

## 2. Architecture Overview

Ferrite is organized as a Cargo workspace with 12 crates under `crates/` plus a top-level binary crate. The key design rule is a strict dependency DAG — no circular dependencies, and extension crates never depend on each other.

### Crate Dependency Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│  ferrite (top-level binary crate)                                │
│                                                                  │
│  src/commands/     ← Command handlers (61 files)                 │
│  src/server/       ← TCP listener + RESP protocol handler        │
│  src/replication/  ← Primary/replica synchronization             │
│  src/migration/    ← Redis migration tools                       │
│                                                                  │
│  This is the integration layer. Commands, server, and            │
│  replication logic live here, wiring together everything below.  │
└──────────────────────┬───────────────────────────────────────────┘
                       │ depends on
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│  ferrite-core (foundation crate)                                 │
│                                                                  │
│  storage/        ← HybridLog tiered engine (FASTER-inspired)    │
│  protocol/       ← RESP2/RESP3 parser                           │
│  persistence/    ← AOF + RDB snapshots                          │
│  auth/           ← ACL system                                   │
│  cluster/        ← Slot sharding + Raft consensus               │
│  query/          ← FerriteQL engine                              │
│  tiering/        ← Hot/warm/cold data management                │
│  transaction/    ← MULTI/EXEC + distributed transactions        │
│  metrics/        ← Prometheus + OpenTelemetry                   │
│  observability/  ← Tracing, profiling, eBPF                     │
│  runtime/        ← Shutdown, config, error recovery              │
│  embedded/       ← Library mode + WASM                          │
└──────────────────────┬───────────────────────────────────────────┘
                       │ depended on by
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│  Extension crates (self-contained, no cross-dependencies)        │
│                                                                  │
│  ferrite-ai          ← Vector search, RAG, semantic caching      │
│  ferrite-search      ← Full-text search + indexing               │
│  ferrite-graph       ← Graph database + Cypher queries           │
│  ferrite-timeseries  ← Time-series data                          │
│  ferrite-document    ← JSON document store                       │
│  ferrite-streaming   ← CDC + event pipelines                     │
│  ferrite-cloud       ← S3/cloud storage integration              │
│  ferrite-plugins     ← WASM plugins + CRDTs                     │
│  ferrite-k8s         ← Kubernetes operator                       │
│  ferrite-enterprise  ← Multi-tenancy + compliance                │
│  ferrite-studio      ← Web UI + dashboard                        │
└──────────────────────────────────────────────────────────────────┘
```

### How Data Flows

```
Client (redis-cli, ioredis, etc.)
  │
  ▼
TCP Listener  (src/server/)
  │
  ▼
RESP Parser   (ferrite-core/protocol/)
  │
  ▼
Command Router (src/commands/parser/)
  │
  ▼
Command Handler (src/commands/handlers/)
  │
  ▼
Storage Engine (ferrite-core/storage/)
  │
  ├─► Mutable Region   (in-memory, hot data)
  ├─► Read-Only Region  (mmap, warm data)
  └─► Disk Region       (io_uring, cold data)
```

The storage engine uses a three-tier **HybridLog** design inspired by Microsoft FASTER:
- **Mutable Region** — in-memory writes with epoch-based concurrency
- **Read-Only Region** — memory-mapped pages for reads
- **Disk Region** — persisted via io_uring (Linux) or tokio::fs (fallback)

### Key Directories at a Glance

| Path | What Lives There |
|------|-----------------|
| `src/commands/parser/` | Command parsing — turn RESP frames into typed enums |
| `src/commands/handlers/` | Command execution — business logic per command |
| `src/commands/executor/` | Dispatch + ACL metadata |
| `src/server/` | TCP accept loop, connection state, RESP framing |
| `crates/ferrite-core/src/` | All foundational subsystems |
| `crates/ferrite-*/src/` | Extension crate implementations |
| `tests/` | Integration tests |
| `benches/` | Criterion benchmarks |

---

## 3. How to Add a New Redis Command

This is the most common type of contribution. Here's the step-by-step process:

### Step 1: Add the command variant

Open `src/commands/parser/mod.rs` and add a new variant to the command enum:

```rust
pub enum Command {
    // ...existing variants...
    ObjectEncoding { key: String },
}
```

### Step 2: Add the parser

Create or edit a file in `src/commands/parser/parsers/`. Parse the raw RESP frames into your new command variant:

```rust
// In the appropriate parser file
"OBJECT" => match subcommand {
    "ENCODING" => {
        let key = parse_string(&frames, 2)?;
        Ok(Command::ObjectEncoding { key })
    }
    // ...
}
```

### Step 3: Add the executor match arm

In `src/commands/executor/mod.rs`, add a match arm that routes to your handler:

```rust
Command::ObjectEncoding { key } => handlers::keys::object_encoding(store, &key),
```

### Step 4: Implement the handler

Write the actual logic in `src/commands/handlers/` or a relevant `src/commands/*.rs` file:

```rust
pub fn object_encoding(store: &Store, key: &str) -> Frame {
    match store.get(key) {
        Some(value) => Frame::Bulk(value.encoding_name().into()),
        None => Frame::Null,
    }
}
```

### Step 5: Add ACL metadata

In `src/commands/executor/meta.rs`, register the command's ACL category, arity, and key positions:

```rust
"OBJECT ENCODING" => CommandMeta {
    arity: 3,
    flags: &["readonly", "fast"],
    first_key: 2,
    last_key: 2,
    step: 1,
},
```

### Step 6: Add tests

Add tests in the same handler file or in `tests/`:

```rust
#[cfg(test)]
mod tests {
    #[test]
    fn test_object_encoding_string() {
        let store = setup_test_store();
        store.set("mykey", "hello");
        let result = object_encoding(&store, "mykey");
        assert_eq!(result, Frame::Bulk("embstr".into()));
    }
}
```

### Step 7: Update compatibility tracker

If there's a compatibility matrix (check `docs/` or the project wiki), mark the command as implemented.

---

## 4. How to Add a Module to an Extension Crate

Extension crates (`crates/ferrite-*`) are self-contained. Adding a new module is straightforward:

1. **Create your module file:**
   ```bash
   touch crates/ferrite-search/src/your_module.rs
   ```

2. **Register it in `lib.rs`:**
   ```rust
   // In crates/ferrite-search/src/lib.rs
   pub mod your_module;
   ```

3. **Follow existing patterns:**
   - Use `thiserror` for error types
   - Derive `serde::Serialize`/`Deserialize` where appropriate
   - Place tests in `#[cfg(test)] mod tests` at the bottom

4. **Verify it compiles:**
   ```bash
   cargo check -p ferrite-search
   cargo test -p ferrite-search --lib
   ```

5. **Wire it into the top-level crate** (if it exposes a new command):
   - Follow the "How to Add a New Redis Command" steps above
   - Import from the extension crate in the top-level `Cargo.toml` and handler

---

## 5. Good First Issues

These issues are specifically chosen for first-time contributors. Each one is scoped, self-contained, and has clear guidance on where to make changes.

Difficulty legend: 🟢 Easy (1-3 hours) · 🟡 Medium (3-6 hours)

---

### 🟢 GFI-01: Implement OBJECT ENCODING command

**Difficulty:** Easy | **Crate:** ferrite (top-level) | **Est. time:** 2-4 hours

**What:** Add the `OBJECT ENCODING <key>` command that returns the internal encoding used to store the value associated with a key.

**Where:** `src/commands/handlers/keys.rs`

**How:** Match on the value's internal type and return the encoding name string (`embstr`, `int`, `ziplist`, `hashtable`, `skiplist`, `quicklist`, etc.).

**Test:** Add a test verifying correct encoding strings for different value types (string, list, set, hash, sorted set).

**Reference:** https://redis.io/commands/object-encoding

---

### 🟢 GFI-02: Implement OBJECT IDLETIME command

**Difficulty:** Easy | **Crate:** ferrite (top-level) | **Est. time:** 2-3 hours

**What:** Add `OBJECT IDLETIME <key>` which returns the number of seconds since the key was last accessed (read or write).

**Where:** `src/commands/handlers/keys.rs`

**How:** Read the key's last-access timestamp from metadata, subtract from current time, return as integer.

**Test:** Set a key, wait briefly, then verify idletime is >= 0 and increases over time.

**Reference:** https://redis.io/commands/object-idletime

---

### 🟢 GFI-03: Implement OBJECT REFCOUNT command

**Difficulty:** Easy | **Crate:** ferrite (top-level) | **Est. time:** 1-2 hours

**What:** Add `OBJECT REFCOUNT <key>` which returns the reference count of the value stored at key. In Ferrite, this can return 1 for existing keys (since Rust ownership means single-owner by default).

**Where:** `src/commands/handlers/keys.rs`

**How:** Return `1` for existing keys and null for missing keys — this matches Redis behavior for most practical cases.

**Test:** Verify returns 1 for an existing key and null for a non-existent key.

**Reference:** https://redis.io/commands/object-refcount

---

### 🟢 GFI-04: Implement OBJECT HELP command

**Difficulty:** Easy | **Crate:** ferrite (top-level) | **Est. time:** 1-2 hours

**What:** Add `OBJECT HELP` which returns a list of help strings describing the OBJECT subcommands.

**Where:** `src/commands/handlers/keys.rs`

**How:** Return a static array of bulk strings describing each OBJECT subcommand. No storage access needed — it's purely informational.

**Test:** Verify the response contains entries for ENCODING, IDLETIME, REFCOUNT, FREQ, and HELP.

**Reference:** https://redis.io/commands/object-help

---

### 🟢 GFI-05: Implement RANDOMKEY command

**Difficulty:** Easy | **Crate:** ferrite (top-level) | **Est. time:** 2-3 hours

**What:** Add `RANDOMKEY` which returns a random key from the currently selected database.

**Where:** `src/commands/handlers/keys.rs`

**How:** Use `rand` to pick a random entry from the key-space iterator. Return null if the database is empty.

**Test:** Insert multiple keys, call RANDOMKEY, verify the result is one of the inserted keys.

**Reference:** https://redis.io/commands/randomkey

---

### 🟡 GFI-06: Implement DUMP and RESTORE commands

**Difficulty:** Medium | **Crate:** ferrite (top-level) | **Est. time:** 4-6 hours

**What:** `DUMP <key>` serializes a key's value into a Redis-compatible binary format. `RESTORE <key> <ttl> <serialized>` deserializes it back.

**Where:** `src/commands/handlers/keys.rs` and `crates/ferrite-core/src/persistence/`

**How:** Implement a serialization format (can start with a simplified version). DUMP produces bytes, RESTORE parses them and inserts into the store with optional TTL.

**Test:** Round-trip test: SET key, DUMP it, DELETE key, RESTORE it, verify GET returns original value.

**Reference:** https://redis.io/commands/dump / https://redis.io/commands/restore

---

### 🟡 GFI-07: Implement WAIT command

**Difficulty:** Medium | **Crate:** ferrite (top-level) | **Est. time:** 4-6 hours

**What:** `WAIT <numreplicas> <timeout>` blocks until the specified number of replicas have acknowledged previous writes, or until the timeout.

**Where:** `src/commands/handlers/server.rs` and `src/replication/`

**How:** Query the replication subsystem for the current replica acknowledgement state. Block the client connection (with timeout) until enough replicas report in.

**Test:** Unit test the timeout behavior; integration test requires multi-node setup (can be mocked).

**Reference:** https://redis.io/commands/wait

---

### 🟡 GFI-08: Implement CLIENT PAUSE and CLIENT UNPAUSE

**Difficulty:** Medium | **Crate:** ferrite (top-level) | **Est. time:** 3-5 hours

**What:** `CLIENT PAUSE <timeout> [WRITE|ALL]` suspends client processing. `CLIENT UNPAUSE` resumes.

**Where:** `src/commands/handlers/client.rs` and `src/server/`

**How:** Set a server-wide pause flag with a timeout. The connection handler should check this flag before processing commands. UNPAUSE clears it early.

**Test:** Verify that commands are delayed during a pause and resume after unpause or timeout.

**Reference:** https://redis.io/commands/client-pause

---

### 🟡 GFI-09: Implement DEBUG OBJECT command

**Difficulty:** Medium | **Crate:** ferrite (top-level) | **Est. time:** 3-4 hours

**What:** `DEBUG OBJECT <key>` returns internal debugging information about a key (encoding, serialized length, refcount, LRU info).

**Where:** `src/commands/handlers/debug.rs`

**How:** Gather internal metadata about the key's storage representation and format it as a single debug string matching Redis's format.

**Test:** Verify output contains expected fields (`encoding`, `serializedlength`, `lru_seconds_idle`).

**Reference:** https://redis.io/commands/debug-object

---

### 🟢 GFI-10: Implement COMMAND COUNT and COMMAND INFO

**Difficulty:** Easy | **Crate:** ferrite (top-level) | **Est. time:** 2-3 hours

**What:** `COMMAND COUNT` returns total number of supported commands. `COMMAND INFO <cmd>` returns metadata about a specific command.

**Where:** `src/commands/handlers/server.rs` and `src/commands/executor/meta.rs`

**How:** COUNT iterates the command metadata table and returns its length. INFO looks up a specific command and returns its arity, flags, and key positions.

**Test:** Verify COUNT returns a positive integer; verify INFO returns correct metadata for known commands like GET and SET.

**Reference:** https://redis.io/commands/command-count

---

### 🟢 GFI-11: Implement LPOS command

**Difficulty:** Easy | **Crate:** ferrite (top-level) | **Est. time:** 2-3 hours

**What:** `LPOS <key> <element> [RANK rank] [COUNT count] [MAXLEN maxlen]` returns the position(s) of an element in a list.

**Where:** `src/commands/handlers/list.rs`

**How:** Iterate the list, find matching elements, respect RANK (skip N matches), COUNT (return N positions), and MAXLEN (limit scan length).

**Test:** Test with duplicate elements, RANK to skip first match, COUNT to get all positions.

**Reference:** https://redis.io/commands/lpos

---

### 🟢 GFI-12: Implement SINTERCARD command

**Difficulty:** Easy | **Crate:** ferrite (top-level) | **Est. time:** 2-3 hours

**What:** `SINTERCARD <numkeys> <key> [key ...] [LIMIT limit]` returns the cardinality of the intersection of multiple sets.

**Where:** `src/commands/handlers/set.rs`

**How:** Compute the intersection of the specified sets but only count members (don't build the full result set). Stop early if LIMIT is reached.

**Test:** Test with overlapping sets, disjoint sets, and with/without LIMIT.

**Reference:** https://redis.io/commands/sintercard

---

### 🟡 GFI-13: Implement ZRANGESTORE command

**Difficulty:** Medium | **Crate:** ferrite (top-level) | **Est. time:** 3-5 hours

**What:** `ZRANGESTORE <dst> <src> <min> <max> [BYSCORE|BYLEX] [REV] [LIMIT offset count]` stores the result of a sorted set range query into a new key.

**Where:** `src/commands/handlers/sorted_set.rs`

**How:** Reuse existing ZRANGE logic to select elements, then store the result into the destination key. Return the number of elements stored.

**Test:** Test BYSCORE, BYLEX, REV, and LIMIT options. Verify destination key contains the correct subset.

**Reference:** https://redis.io/commands/zrangestore

---

### 🟡 GFI-14: Implement COPY command

**Difficulty:** Medium | **Crate:** ferrite (top-level) | **Est. time:** 3-4 hours

**What:** `COPY <source> <destination> [DB db] [REPLACE]` copies the value of a key to a new key.

**Where:** `src/commands/handlers/keys.rs`

**How:** Deep-clone the value (and TTL) from source to destination. If REPLACE is set, overwrite existing destination. If DB is set, copy across databases.

**Test:** Test copy with and without REPLACE, verify TTL is preserved, verify different data types copy correctly.

**Reference:** https://redis.io/commands/copy

---

### 🟢 GFI-15: Implement OBJECT FREQ command

**Difficulty:** Easy | **Crate:** ferrite (top-level) | **Est. time:** 1-2 hours

**What:** `OBJECT FREQ <key>` returns the access frequency counter of a key (used by the LFU eviction policy).

**Where:** `src/commands/handlers/keys.rs`

**How:** Read the key's LFU counter from metadata. If LFU is not enabled, return 0 or an error matching Redis behavior.

**Test:** Verify returns a non-negative integer for existing keys and null for missing keys.

**Reference:** https://redis.io/commands/object-freq

---

### 🟢 GFI-16: Fix PERSIST command implementation

**Difficulty:** Easy | **Crate:** ferrite (top-level) | **Est. time:** 1-2 hours

**What:** Audit and fix the `PERSIST` command to ensure it correctly removes TTL from a key and returns the right status codes.

**Where:** `src/commands/handlers/keys.rs`

**How:** Verify that PERSIST returns `1` when a timeout was removed, `0` when the key exists but has no timeout, and `0` when the key doesn't exist. Fix any edge cases.

**Test:** Test all three cases: key with TTL, key without TTL, and non-existent key.

**Reference:** https://redis.io/commands/persist

---

### 🟢 GFI-17: Implement CONFIG RESETSTAT command

**Difficulty:** Easy | **Crate:** ferrite (top-level) | **Est. time:** 1-2 hours

**What:** `CONFIG RESETSTAT` resets the statistics counters (connected clients peak, keyspace hits/misses, etc.).

**Where:** `src/commands/handlers/config.rs` and `crates/ferrite-core/src/metrics/`

**How:** Zero out the relevant counters in the metrics subsystem. Return `OK`.

**Test:** Increment some counters, call RESETSTAT, verify counters are back to zero.

**Reference:** https://redis.io/commands/config-resetstat

---

### 🟡 GFI-18: Implement LATENCY HISTORY and LATENCY LATEST

**Difficulty:** Medium | **Crate:** ferrite (top-level) | **Est. time:** 4-6 hours

**What:** `LATENCY HISTORY <event>` returns latency samples for a given event. `LATENCY LATEST` returns the latest sample for all tracked events.

**Where:** `src/commands/handlers/server.rs` and `crates/ferrite-core/src/observability/`

**How:** Implement a latency tracking ring buffer that records timestamps and durations for key events (fork, aof-rewrite, etc.). HISTORY returns the buffer contents; LATEST returns the most recent entry per event.

**Test:** Record synthetic latency events, verify HISTORY returns them in order, verify LATEST returns the most recent.

**Reference:** https://redis.io/commands/latency-history

---

### 🟡 GFI-19: Implement MEMORY USAGE command

**Difficulty:** Medium | **Crate:** ferrite (top-level) | **Est. time:** 3-5 hours

**What:** `MEMORY USAGE <key> [SAMPLES count]` returns the number of bytes a key and its value occupy in memory.

**Where:** `src/commands/handlers/server.rs`

**How:** Calculate memory usage by summing: key string size + value size (using `std::mem::size_of_val` or a custom trait) + metadata overhead. For aggregate types, SAMPLES controls how many elements to sample for estimation.

**Test:** Verify small string returns a reasonable byte count; verify list with many elements returns more than a single-element list.

**Reference:** https://redis.io/commands/memory-usage

---

### 🟢 GFI-20: Add SWAPDB correctness tests

**Difficulty:** Easy | **Crate:** ferrite (top-level) | **Est. time:** 2-3 hours

**What:** Write comprehensive tests for the existing `SWAPDB` command to ensure correctness across edge cases.

**Where:** `tests/` or `src/commands/handlers/server.rs` (test module)

**How:** Test these scenarios:
- Swap two populated databases and verify keys moved correctly
- Swap a populated database with an empty one
- Swap a database with itself (no-op)
- Verify TTLs are preserved after swap
- Verify watchers/subscribers are notified (if applicable)

**Test:** All tests should be self-contained and not depend on external services.

**Reference:** https://redis.io/commands/swapdb

---

## 6. Code Style Quick Reference

**Error handling:**
- Use `thiserror` for typed errors in library crates (`ferrite-core`, extensions)
- Use `anyhow` for application-level errors in the top-level crate
- Propagate errors with `?` — avoid `.unwrap()` in production code

**Safety:**
- Every `unsafe` block must have a `// SAFETY:` comment explaining the invariant
- Prefer safe abstractions; use `unsafe` only when necessary for performance

**Testing:**
- Place unit tests in `#[cfg(test)] mod tests` at the bottom of each file
- Use descriptive test names: `test_object_encoding_returns_embstr_for_short_strings`
- Integration tests go in the `tests/` directory

**Data types:**
- Use `bytes::Bytes` for binary data (zero-copy where possible)
- Use the `Frame` type for RESP protocol responses
- Prefer `&str` over `String` in function signatures when ownership isn't needed

**Formatting:**
```bash
cargo fmt --all           # Auto-format all code
cargo fmt --all --check   # Check without modifying (used in CI)
```

**Linting:**
```bash
cargo clippy --all-targets --all-features -- -D warnings
```

---

## 7. PR Checklist

Before submitting your pull request, verify each item:

- [ ] **Format:** `cargo fmt --all --check` passes with no warnings
- [ ] **Lint:** `cargo clippy --workspace -- -D warnings` passes cleanly
- [ ] **Tests:** `cargo test --lib` passes (all 5,000+ tests)
- [ ] **New tests:** Any new functionality includes corresponding tests
- [ ] **Doc comments:** Public functions, structs, and traits have `///` doc comments
- [ ] **No unwrap:** Production code doesn't use `.unwrap()` or `.expect()` without justification
- [ ] **Commit messages:** Clear, descriptive, and in imperative mood ("Add OBJECT ENCODING command")
- [ ] **Single concern:** Each PR addresses one issue or feature

**PR title format:**

```
feat: implement OBJECT ENCODING command
fix: correct PERSIST return value for keys without TTL
test: add SWAPDB correctness tests
docs: update command compatibility matrix
```

---

## 8. Getting Help

**Before you ask:**
- Search existing [GitHub Issues](https://github.com/FerriteLabs/ferrite/issues) and [Discussions](https://github.com/FerriteLabs/ferrite/discussions)
- Check the [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines
- Read the [GOVERNANCE.md](GOVERNANCE.md) for project decision-making processes

**Where to ask:**
- **GitHub Discussions** — Questions, ideas, and show-and-tell. Best for design questions and "how does X work?" inquiries.
- **GitHub Issues** — Bug reports and feature requests. Use the issue templates.
- **Pull Request comments** — Ask for help directly on your PR. Maintainers are happy to guide you through reviews.

**Community standards:**
- We follow the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/)
- Be kind, be patient, be constructive
- Every contribution matters — documentation fixes and test improvements are just as valued as new features

---

## 9. Your First Contribution Workflow

Here's the typical workflow from start to finish:

```bash
# 1. Fork and clone
gh repo fork FerriteLabs/ferrite --clone
cd ferrite

# 2. Create a feature branch
git checkout -b feat/object-encoding

# 3. Make your changes (see sections 3-4 above)

# 4. Run the checks
cargo fmt --all
cargo clippy --workspace -- -D warnings
cargo test --lib

# 5. Commit and push
git add -A
git commit -m "feat: implement OBJECT ENCODING command"
git push origin feat/object-encoding

# 6. Open a PR
gh pr create --title "feat: implement OBJECT ENCODING command" \
             --body "Implements OBJECT ENCODING. Closes #123."
```

**What happens next:**
1. CI runs format, lint, and test checks automatically
2. A maintainer reviews your PR (usually within a few days)
3. You may get feedback — this is normal and expected
4. Once approved, your PR gets merged

Welcome aboard — we're glad you're here.
