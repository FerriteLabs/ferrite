#!/usr/bin/env bash
# =============================================================================
# Create Good First Issues from FIRST_CONTRIBUTORS.md
# =============================================================================
# Uses the GitHub CLI (gh) to create 20 good-first-issues.
#
# Usage:
#   ./scripts/create-issues.sh           # Create all issues
#   ./scripts/create-issues.sh --dry-run # Preview only
#
# Prerequisites: gh auth login

set -euo pipefail

DRY_RUN=false
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=true

REPO="ferritelabs/ferrite"
LABELS="good first issue,help wanted"

create_issue() {
    local title="$1"
    local body="$2"
    local difficulty="$3"
    local est="$4"
    
    local full_body="## Good First Issue

**Difficulty**: ${difficulty}
**Estimated Time**: ${est}

${body}

## How to Get Started

1. Fork and clone the repository
2. Run \`cargo build && cargo test --lib\` to verify setup
3. Find the file mentioned above and implement the change
4. Add tests in the same file
5. Run \`cargo fmt --all && cargo clippy --workspace\`
6. Submit a PR referencing this issue

## Resources

- [FIRST_CONTRIBUTORS.md](../blob/main/FIRST_CONTRIBUTORS.md) — Full contributor guide
- [CONTRIBUTING.md](../blob/main/CONTRIBUTING.md) — Contribution guidelines
- [Redis command reference](https://redis.io/commands/) — Command specifications"

    if $DRY_RUN; then
        echo "[DRY RUN] Would create: $title ($difficulty, $est)"
    else
        gh issue create --repo "$REPO" \
            --title "[GFI] $title" \
            --body "$full_body" \
            --label "$LABELS" 2>/dev/null && echo "✓ Created: $title" || echo "✗ Failed: $title"
    fi
}

echo "═══════════════════════════════════════════════════════════════"
echo "  Creating 20 Good First Issues"
echo "═══════════════════════════════════════════════════════════════"
echo ""

create_issue \
    "Implement OBJECT ENCODING command" \
    "Add the \`OBJECT ENCODING <key>\` command that returns the internal encoding of a value (embstr, int, ziplist, hashtable, skiplist, etc.).

**Where**: \`src/commands/handlers/keys.rs\`
**How**: Match on the value type stored in the Store and return the appropriate encoding string.
**Test**: Verify correct encoding strings for each data type.
**Reference**: https://redis.io/commands/object-encoding" \
    "Easy" "2-4 hours"

create_issue \
    "Implement OBJECT IDLETIME command" \
    "Add the \`OBJECT IDLETIME <key>\` command that returns seconds since the key was last accessed.

**Where**: \`src/commands/handlers/keys.rs\`
**How**: Track last access time per key and compute elapsed seconds.
**Reference**: https://redis.io/commands/object-idletime" \
    "Easy" "2-4 hours"

create_issue \
    "Implement OBJECT REFCOUNT command" \
    "Add the \`OBJECT REFCOUNT <key>\` command that returns the reference count of the value.

**Where**: \`src/commands/handlers/keys.rs\`
**How**: Return 1 for all existing keys (Ferrite doesn't share objects).
**Reference**: https://redis.io/commands/object-refcount" \
    "Easy" "1-2 hours"

create_issue \
    "Implement OBJECT HELP command" \
    "Add the \`OBJECT HELP\` subcommand that returns help text listing available OBJECT subcommands.

**Where**: \`src/commands/handlers/keys.rs\`
**How**: Return a Frame::Array with help strings for each subcommand.
**Reference**: https://redis.io/commands/object-help" \
    "Easy" "1-2 hours"

create_issue \
    "Implement RANDOMKEY command" \
    "Add the \`RANDOMKEY\` command that returns a random key from the current database.

**Where**: \`src/commands/handlers/keys.rs\`
**How**: Get all keys, pick one at random using \`rand\` crate. Return Null if empty.
**Reference**: https://redis.io/commands/randomkey" \
    "Easy" "2-3 hours"

create_issue \
    "Implement DUMP command" \
    "Add the \`DUMP <key>\` command that serializes a key's value in Redis-compatible RDB format.

**Where**: \`src/commands/handlers/keys.rs\`
**How**: Serialize the value using the existing persistence serialization, add CRC64 checksum.
**Reference**: https://redis.io/commands/dump" \
    "Medium" "4-6 hours"

create_issue \
    "Implement RESTORE command" \
    "Add the \`RESTORE <key> <ttl> <serialized>\` command that deserializes a DUMP payload.

**Where**: \`src/commands/handlers/keys.rs\`
**How**: Deserialize the RDB-format payload, verify CRC, store with optional TTL.
**Reference**: https://redis.io/commands/restore" \
    "Medium" "4-6 hours"

create_issue \
    "Implement WAIT command" \
    "Add the \`WAIT <numreplicas> <timeout>\` command that blocks until writes are acknowledged by replicas.

**Where**: \`src/commands/handlers/server.rs\`
**How**: Check replication offset against connected replicas, block until count met or timeout.
**Reference**: https://redis.io/commands/wait" \
    "Medium" "4-6 hours"

create_issue \
    "Implement CLIENT PAUSE/UNPAUSE" \
    "Add \`CLIENT PAUSE <timeout> [WRITE|ALL]\` and \`CLIENT UNPAUSE\` commands.

**Where**: \`src/commands/handlers/server.rs\`
**How**: Set a pause flag on the client registry, delay command processing during pause.
**Reference**: https://redis.io/commands/client-pause" \
    "Medium" "4-6 hours"

create_issue \
    "Implement DEBUG OBJECT command" \
    "Add \`DEBUG OBJECT <key>\` that returns internal debugging information about a key.

**Where**: \`src/commands/handlers/server.rs\`
**How**: Return value type, encoding, serialized length, LRU info, refcount.
**Reference**: https://redis.io/commands/debug-object" \
    "Medium" "3-5 hours"

create_issue \
    "Implement COMMAND COUNT" \
    "Add \`COMMAND COUNT\` that returns the total number of commands supported by the server.

**Where**: \`src/commands/handlers/server.rs\`
**How**: Count the COMMAND_INFO entries in executor. Return as integer.
**Reference**: https://redis.io/commands/command-count" \
    "Easy" "1-2 hours"

create_issue \
    "Implement LPOS command" \
    "Add \`LPOS <key> <element> [RANK rank] [COUNT count] [MAXLEN len]\` for finding element positions in lists.

**Where**: \`src/commands/lists.rs\`
**How**: Iterate list elements, match target, respect RANK/COUNT/MAXLEN options.
**Reference**: https://redis.io/commands/lpos" \
    "Easy" "3-4 hours"

create_issue \
    "Implement SINTERCARD command" \
    "Add \`SINTERCARD <numkeys> <key> [key ...] [LIMIT limit]\` for counting set intersection cardinality.

**Where**: \`src/commands/sets.rs\`
**How**: Compute intersection of all sets, return count (optionally capped by LIMIT).
**Reference**: https://redis.io/commands/sintercard" \
    "Easy" "2-4 hours"

create_issue \
    "Implement ZRANGESTORE command" \
    "Add \`ZRANGESTORE <dst> <src> <min> <max> [BYSCORE|BYLEX] [REV] [LIMIT offset count]\`.

**Where**: \`src/commands/sorted_sets.rs\`
**How**: Query range from source sorted set, store results in destination key.
**Reference**: https://redis.io/commands/zrangestore" \
    "Medium" "4-6 hours"

create_issue \
    "Implement COPY command" \
    "Add \`COPY <source> <destination> [DB db] [REPLACE]\` that copies a key's value.

**Where**: \`src/commands/handlers/keys.rs\`
**How**: Clone the value from source, store at destination. Handle REPLACE flag and cross-DB copy.
**Reference**: https://redis.io/commands/copy" \
    "Medium" "3-5 hours"

create_issue \
    "Implement OBJECT FREQ command" \
    "Add \`OBJECT FREQ <key>\` that returns the LFU access frequency counter.

**Where**: \`src/commands/handlers/keys.rs\`
**How**: Return the access frequency from the key's metadata (0 if LFU not enabled).
**Reference**: https://redis.io/commands/object-freq" \
    "Easy" "1-2 hours"

create_issue \
    "Implement CONFIG RESETSTAT" \
    "Add \`CONFIG RESETSTAT\` command that resets server statistics (ops/sec, hit rate, etc.).

**Where**: \`src/commands/handlers/server.rs\`
**How**: Reset the metrics counters in MetricsRegistry to zero.
**Reference**: https://redis.io/commands/config-resetstat" \
    "Easy" "2-3 hours"

create_issue \
    "Implement LATENCY HISTORY/LATEST commands" \
    "Add \`LATENCY HISTORY <event>\` and \`LATENCY LATEST\` diagnostic commands.

**Where**: \`src/commands/handlers/server.rs\`
**How**: Use the DiagnosticsEngine's latency tracking to return historical latency data.
**Reference**: https://redis.io/commands/latency-history" \
    "Medium" "4-6 hours"

create_issue \
    "Implement MEMORY USAGE command" \
    "Add \`MEMORY USAGE <key> [SAMPLES count]\` that estimates key memory consumption.

**Where**: \`src/commands/handlers/server.rs\`
**How**: Use MemoryManager::get_key_memory() to compute overhead + value size.
**Reference**: https://redis.io/commands/memory-usage" \
    "Medium" "3-5 hours"

create_issue \
    "Add SWAPDB correctness tests" \
    "Add comprehensive tests for the existing SWAPDB command covering edge cases.

**Where**: \`tests/integration_harness.rs\` or \`src/commands/executor/tests.rs\`
**How**: Test SWAPDB between databases with keys, verify keys swap correctly, test empty DBs.
**Reference**: https://redis.io/commands/swapdb" \
    "Easy" "2-3 hours"

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Done! Created 20 good-first-issues."
echo "═══════════════════════════════════════════════════════════════"
