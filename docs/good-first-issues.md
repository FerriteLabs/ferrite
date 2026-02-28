# Good First Issues

Welcome, new contributor! 🦀 Here are carefully curated tasks designed to help you make your first contribution to Ferrite.

## How to Get Started

1. Pick an issue below that interests you
2. Comment on the GitHub issue to claim it
3. Fork the repo, create a branch, make your changes
4. Run `cargo fmt --all --check && cargo clippy && cargo test --lib`
5. Submit a PR referencing the issue

---

## Issue 1: Add OBJECT ENCODING command

**Difficulty:** Easy
**Labels:** `good first issue`, `compatibility`
**File:** `src/commands/object_commands.rs`

**Description:** Implement the `OBJECT ENCODING <key>` command to return the internal encoding type of a key's value (e.g., `embstr`, `int`, `hashtable`, `listpack`, `skiplist`, `raw`). This is used by Redis clients and monitoring tools to inspect memory efficiency.

**Step-by-step:**
1. Add a new handler function `cmd_object_encoding` in `src/commands/object_commands.rs`
2. Look up the key in the store and determine its encoding based on the value type (String → `embstr`/`int`/`raw`, List → `listpack`/`linkedlist`, Hash → `listpack`/`hashtable`, Set → `listpack`/`hashtable`, ZSet → `listpack`/`skiplist`)
3. Return the encoding as a bulk string, or `nil` if the key does not exist
4. Register the subcommand in the OBJECT command dispatcher

**How to test:**
```bash
cargo test -p ferrite -- object_encoding
```
Write a unit test that sets a string key and verifies `OBJECT ENCODING` returns `embstr`.

**Expected behavior:** Matches [Redis OBJECT ENCODING](https://redis.io/commands/object-encoding/) output.

---

## Issue 2: Add RANDOMKEY command

**Difficulty:** Easy
**Labels:** `good first issue`, `compatibility`
**File:** `src/commands/executor/keys.rs`

**Description:** Implement `RANDOMKEY` to return a random key from the current database. Returns `nil` if the database is empty.

**Step-by-step:**
1. Add a `cmd_randomkey` function in `src/commands/executor/keys.rs`
2. Access the key store and pick a random entry (use `rand::thread_rng()` — `rand` is already a workspace dependency)
3. Return the key as a bulk string, or nil for an empty database
4. Register the command in the command table

**How to test:**
```bash
cargo test -p ferrite -- randomkey
```
Write tests for: empty database returns nil, database with keys returns one of them.

**Expected behavior:** Matches [Redis RANDOMKEY](https://redis.io/commands/randomkey/).

---

## Issue 3: Add CLIENT GETNAME/SETNAME

**Difficulty:** Easy
**Labels:** `good first issue`, `compatibility`
**File:** `src/commands/client_commands.rs`

**Description:** Implement `CLIENT SETNAME <name>` and `CLIENT GETNAME` to allow connections to be named for debugging and monitoring purposes.

**Step-by-step:**
1. Add a `name: Option<String>` field to the connection state struct
2. Implement `CLIENT SETNAME <name>` to set the name (validate: no spaces allowed)
3. Implement `CLIENT GETNAME` to return the current name or nil
4. Wire into the existing CLIENT command dispatcher

**How to test:**
```bash
cargo test -p ferrite -- client_name
```
Test: set a name, get it back, set empty string to clear, verify no-spaces validation.

**Expected behavior:** Matches [Redis CLIENT SETNAME](https://redis.io/commands/client-setname/).

---

## Issue 4: Add DEBUG SET-ACTIVE-EXPIRE

**Difficulty:** Easy
**Labels:** `good first issue`, `enhancement`
**File:** `src/commands/debug_commands.rs`

**Description:** Implement `DEBUG SET-ACTIVE-EXPIRE <0|1>` to toggle active key expiration for testing. When disabled (`0`), keys only expire on access (lazy expiration). When enabled (`1`, default), the background expiration task runs normally.

**Step-by-step:**
1. Add a global or server-level `active_expire_enabled: AtomicBool` flag
2. Add a `cmd_debug_set_active_expire` handler that sets the flag
3. Check the flag in the active expiration background task
4. Return `+OK\r\n` on success

**How to test:**
```bash
cargo test -p ferrite -- debug_set_active_expire
```
Test: disable active expire, set a key with TTL, verify it is not actively expired.

---

## Issue 5: Add OBJECT REFCOUNT

**Difficulty:** Easy
**Labels:** `good first issue`, `compatibility`
**File:** `src/commands/object_commands.rs`

**Description:** Implement `OBJECT REFCOUNT <key>` to return the reference count of a key's value object. Since Ferrite does not use shared objects like Redis, this always returns `1` for existing keys and an error for non-existent keys.

**Step-by-step:**
1. Add `cmd_object_refcount` in `src/commands/object_commands.rs`
2. If the key exists, return `:1\r\n` (integer 1)
3. If the key does not exist, return an error
4. Register in the OBJECT command dispatcher

**How to test:**
```bash
cargo test -p ferrite -- object_refcount
```
Test: existing key returns 1, missing key returns error.

**Expected behavior:** Matches [Redis OBJECT REFCOUNT](https://redis.io/commands/object-refcount/).

---

## Issue 6: Improve CLUSTER INFO output

**Difficulty:** Medium
**Labels:** `good first issue`, `compatibility`, `cluster`
**File:** `src/commands/cluster_commands.rs`

**Description:** The current `CLUSTER INFO` output is missing several fields from the Redis 7.x specification. Add the missing fields to improve client compatibility.

**Step-by-step:**
1. Compare current output against [Redis CLUSTER INFO spec](https://redis.io/commands/cluster-info/)
2. Add missing fields: `cluster_my_epoch`, `cluster_stats_messages_sent`, `cluster_stats_messages_received`, `total_cluster_links_buffer_limit_exceeded`
3. Source values from cluster state or use sensible defaults (0) for unimplemented metrics
4. Maintain the `key:value\r\n` output format

**How to test:**
```bash
cargo test -p ferrite -- cluster_info
```
Test: verify output contains all required fields and parses correctly.

---

## Issue 7: Add WAIT command

**Difficulty:** Medium
**Labels:** `good first issue`, `compatibility`, `replication`
**File:** `src/commands/replication_commands.rs`

**Description:** Implement `WAIT <numreplicas> <timeout>` as a stub that returns `0` (no replicas acknowledged). This unblocks client libraries that require the WAIT command to exist.

**Step-by-step:**
1. Add `cmd_wait` handler that parses `numreplicas` and `timeout` arguments
2. If timeout is 0, return immediately with `:0\r\n`
3. Otherwise, sleep for the timeout duration, then return `:0\r\n`
4. Register the command in the command table

**How to test:**
```bash
cargo test -p ferrite -- cmd_wait
```
Test: `WAIT 1 0` returns 0 immediately, `WAIT 1 100` returns 0 after ~100ms.

**Expected behavior:** Matches [Redis WAIT](https://redis.io/commands/wait/) semantics for a single-node deployment.

---

## Issue 8: Add MEMORY USAGE command

**Difficulty:** Medium
**Labels:** `good first issue`, `compatibility`, `enhancement`
**File:** `src/commands/memory_commands.rs`

**Description:** Implement `MEMORY USAGE <key> [SAMPLES <count>]` to return the approximate memory consumption of a key and its value in bytes.

**Step-by-step:**
1. Add `cmd_memory_usage` handler
2. For a given key, estimate memory as: key length + value serialized size + per-key overhead (~80 bytes)
3. For collection types (lists, hashes, sets), sample `SAMPLES` entries (default: 5) and extrapolate
4. Return the estimate as an integer, or nil if the key doesn't exist

**How to test:**
```bash
cargo test -p ferrite -- memory_usage
```
Test: string key returns reasonable estimate, missing key returns nil, collection type works with sampling.

**Expected behavior:** Matches [Redis MEMORY USAGE](https://redis.io/commands/memory-usage/).

---

## Issue 9: Add CONFIG REWRITE

**Difficulty:** Medium
**Labels:** `good first issue`, `compatibility`
**File:** `src/commands/config_commands.rs`

**Description:** Implement `CONFIG REWRITE` to persist the current server configuration to disk. This writes the in-memory configuration (as modified by `CONFIG SET`) back to the configuration file.

**Step-by-step:**
1. Add `cmd_config_rewrite` handler
2. Serialize current config state to TOML format (Ferrite uses `ferrite.toml`)
3. Write atomically: write to a temp file, then rename over the original
4. Return `+OK\r\n` on success, or an error if no config file path is set

**How to test:**
```bash
cargo test -p ferrite -- config_rewrite
```
Test: modify a config value with CONFIG SET, call CONFIG REWRITE, read the file and verify.

**Expected behavior:** Matches [Redis CONFIG REWRITE](https://redis.io/commands/config-rewrite/) semantics adapted for TOML.

---

## Issue 10: Add COMMAND COUNT

**Difficulty:** Easy
**Labels:** `good first issue`, `compatibility`
**File:** `src/commands/command_commands.rs`

**Description:** Implement `COMMAND COUNT` to return the total number of commands supported by the server as an integer.

**Step-by-step:**
1. Add `cmd_command_count` handler
2. Access the command registry/table and return its length as an integer reply
3. Register the subcommand in the COMMAND dispatcher

**How to test:**
```bash
cargo test -p ferrite -- command_count
```
Test: verify the returned count is a positive integer matching the number of registered commands.

**Expected behavior:** Matches [Redis COMMAND COUNT](https://redis.io/commands/command-count/).
