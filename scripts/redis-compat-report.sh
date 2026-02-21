#!/usr/bin/env bash
# Generate a Markdown Redis-compatibility report for Ferrite
#
# Starts a Ferrite server, tests individual Redis commands across every major
# category, and produces a Markdown table summarising pass/fail results.
#
# Prerequisites:
#   - Ferrite binary built: cargo build --release
#   - redis-cli installed:  apt install redis-tools / brew install redis
#
# Environment variables:
#   FERRITE_BIN   Path to Ferrite binary        (default: ./target/release/ferrite)
#   FERRITE_PORT  Port for the test server      (default: 6381)
#   REPORT_FILE   Output path for the report    (default: stdout)
#
# Usage:
#   ./scripts/redis-compat-report.sh                         # Print to stdout
#   REPORT_FILE=compat.md ./scripts/redis-compat-report.sh   # Write to file
#   ./scripts/redis-compat-report.sh --help

set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────

FERRITE_BIN="${FERRITE_BIN:-./target/release/ferrite}"
FERRITE_PORT="${FERRITE_PORT:-6381}"
REPORT_FILE="${REPORT_FILE:-}"
FERRITE_PID=""

# Counters
TOTAL_PASS=0
TOTAL_FAIL=0
TOTAL_SKIP=0

usage() {
    cat <<'EOF'
Usage: redis-compat-report.sh [OPTIONS]

Test Redis command compatibility and produce a Markdown report.

Options:
  --help    Show this help message and exit

Environment variables:
  FERRITE_BIN   Path to Ferrite binary        (default: ./target/release/ferrite)
  FERRITE_PORT  Port for the test server      (default: 6381)
  REPORT_FILE   Output path for the report    (default: stdout)

Examples:
  ./scripts/redis-compat-report.sh
  REPORT_FILE=compatibility.md ./scripts/redis-compat-report.sh
  FERRITE_PORT=7777 ./scripts/redis-compat-report.sh
EOF
    exit 0
}

for arg in "$@"; do
    case "$arg" in
        --help|-h) usage ;;
        *) echo "Unknown option: $arg"; usage ;;
    esac
done

# ── Helpers ───────────────────────────────────────────────────────────────────

info()  { printf '\033[0;34m[INFO]\033[0m  %s\n' "$*" >&2; }
ok()    { printf '\033[0;32m[PASS]\033[0m  %s\n' "$*" >&2; }
fail()  { printf '\033[0;31m[FAIL]\033[0m  %s\n' "$*" >&2; }

cleanup() {
    if [[ -n "${FERRITE_PID}" ]]; then
        info "Stopping Ferrite (PID $FERRITE_PID)..."
        kill "$FERRITE_PID" 2>/dev/null || true
        wait "$FERRITE_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

# Accumulator for report lines — we build the full Markdown in memory so it can
# be written atomically at the end.
REPORT_LINES=""

emit() {
    REPORT_LINES+="$1
"
}

# ── Prerequisites ─────────────────────────────────────────────────────────────

check_prereqs() {
    local missing=0
    if [[ ! -x "$FERRITE_BIN" ]]; then
        fail "Ferrite binary not found at $FERRITE_BIN"
        echo "  Build with: cargo build --release" >&2
        missing=1
    fi
    if ! command -v redis-cli &>/dev/null; then
        fail "redis-cli not found"
        echo "  Linux: sudo apt install redis-tools" >&2
        echo "  macOS: brew install redis" >&2
        missing=1
    fi
    if [[ $missing -ne 0 ]]; then
        exit 1
    fi
}

# ── Start / stop Ferrite ──────────────────────────────────────────────────────

start_ferrite() {
    info "Starting Ferrite on port $FERRITE_PORT..."
    "$FERRITE_BIN" --port "$FERRITE_PORT" &
    FERRITE_PID=$!

    local retries=30
    for (( i=1; i<=retries; i++ )); do
        if redis-cli -p "$FERRITE_PORT" PING 2>/dev/null | grep -q PONG; then
            ok "Ferrite started (PID $FERRITE_PID)"
            return
        fi
        sleep 0.5
    done
    fail "Ferrite failed to start within 15 seconds"
    exit 1
}

# ── Command tester ────────────────────────────────────────────────────────────

CLI="redis-cli -p ${FERRITE_PORT}"

# test_cmd <command_name> <test_body>
#   test_body is eval'd; return 0 = pass, nonzero = fail.
#   Results are tracked and a table row is emitted.
CATEGORY_PASS=0
CATEGORY_FAIL=0
CATEGORY_SKIP=0

begin_category() {
    local cat="$1"
    CATEGORY_PASS=0
    CATEGORY_FAIL=0
    CATEGORY_SKIP=0
    emit ""
    emit "### $cat"
    emit ""
    emit "| Command | Status | Notes |"
    emit "|---------|--------|-------|"
}

end_category() {
    local cat="$1"
    emit ""
    emit "_${cat} summary: ${CATEGORY_PASS} passed, ${CATEGORY_FAIL} failed, ${CATEGORY_SKIP} skipped_"
}

test_cmd() {
    local cmd="$1"
    local body="$2"
    local notes="${3:-}"

    if eval "$body" >/dev/null 2>&1; then
        emit "| \`$cmd\` | ✅ Pass | $notes |"
        CATEGORY_PASS=$((CATEGORY_PASS + 1))
        TOTAL_PASS=$((TOTAL_PASS + 1))
    else
        emit "| \`$cmd\` | ❌ Fail | $notes |"
        CATEGORY_FAIL=$((CATEGORY_FAIL + 1))
        TOTAL_FAIL=$((TOTAL_FAIL + 1))
    fi
}

skip_cmd() {
    local cmd="$1"
    local reason="${2:-Not yet implemented}"
    emit "| \`$cmd\` | ⏭ Skip | $reason |"
    CATEGORY_SKIP=$((CATEGORY_SKIP + 1))
    TOTAL_SKIP=$((TOTAL_SKIP + 1))
}

# Helper: flush all keys created during a test run
flush_test_keys() {
    $CLI FLUSHDB >/dev/null 2>&1 || true
}

# ── Test categories ───────────────────────────────────────────────────────────

test_strings() {
    begin_category "Strings"

    test_cmd "SET"       "$CLI SET compat_s hello"
    test_cmd "GET"       "$CLI GET compat_s 2>/dev/null | grep -q hello"
    test_cmd "SETNX"     "$CLI SETNX compat_nx first && $CLI GET compat_nx 2>/dev/null | grep -q first"
    test_cmd "SETEX"     "$CLI SETEX compat_ex 100 val && $CLI TTL compat_ex 2>/dev/null | grep -qv '\-1'"
    test_cmd "PSETEX"    "$CLI PSETEX compat_pex 100000 val && $CLI PTTL compat_pex 2>/dev/null | grep -qv '\-1'"
    test_cmd "MSET"      "$CLI MSET compat_a 1 compat_b 2"
    test_cmd "MGET"      "$CLI MGET compat_a compat_b"
    test_cmd "MSETNX"    "$CLI MSETNX compat_nx2 x compat_nx3 y"
    test_cmd "APPEND"    "$CLI APPEND compat_s ' world'"
    test_cmd "STRLEN"    "$CLI STRLEN compat_s 2>/dev/null | grep -q 11"
    test_cmd "INCR"      "$CLI SET compat_i 0 && $CLI INCR compat_i 2>/dev/null | grep -q 1"
    test_cmd "INCRBY"    "$CLI INCRBY compat_i 5 2>/dev/null | grep -q 6"
    test_cmd "INCRBYFLOAT" "$CLI SET compat_f 1.0 && $CLI INCRBYFLOAT compat_f 0.5"
    test_cmd "DECR"      "$CLI DECR compat_i"
    test_cmd "DECRBY"    "$CLI DECRBY compat_i 2"
    test_cmd "GETRANGE"  "$CLI GETRANGE compat_s 0 4"
    test_cmd "SETRANGE"  "$CLI SETRANGE compat_s 0 HELLO"
    test_cmd "GETSET"    "$CLI GETSET compat_s newval"
    test_cmd "GETDEL"    "$CLI SET compat_gd v && $CLI GETDEL compat_gd"

    flush_test_keys
    end_category "Strings"
}

test_lists() {
    begin_category "Lists"

    test_cmd "RPUSH"     "$CLI RPUSH compat_l a b c"
    test_cmd "LPUSH"     "$CLI LPUSH compat_l z"
    test_cmd "LLEN"      "$CLI LLEN compat_l 2>/dev/null | grep -q 4"
    test_cmd "LRANGE"    "$CLI LRANGE compat_l 0 -1"
    test_cmd "LINDEX"    "$CLI LINDEX compat_l 0"
    test_cmd "LSET"      "$CLI LSET compat_l 0 x"
    test_cmd "LPOP"      "$CLI LPOP compat_l"
    test_cmd "RPOP"      "$CLI RPOP compat_l"
    test_cmd "LINSERT"   "$CLI RPUSH compat_li a c && $CLI LINSERT compat_li BEFORE c b"
    test_cmd "LREM"      "$CLI RPUSH compat_lr a a b && $CLI LREM compat_lr 1 a"
    test_cmd "LTRIM"     "$CLI RPUSH compat_lt a b c d && $CLI LTRIM compat_lt 1 2"
    test_cmd "LPOS"      "$CLI RPUSH compat_lp a b c && $CLI LPOS compat_lp b"
    test_cmd "RPOPLPUSH" "$CLI RPUSH compat_src x y && $CLI RPOPLPUSH compat_src compat_dst"
    test_cmd "LMOVE"     "$CLI RPUSH compat_lm1 a b && $CLI LMOVE compat_lm1 compat_lm2 LEFT RIGHT"

    flush_test_keys
    end_category "Lists"
}

test_hashes() {
    begin_category "Hashes"

    test_cmd "HSET"      "$CLI HSET compat_h f1 v1 f2 v2"
    test_cmd "HGET"      "$CLI HGET compat_h f1 2>/dev/null | grep -q v1"
    test_cmd "HLEN"      "$CLI HLEN compat_h 2>/dev/null | grep -q 2"
    test_cmd "HDEL"      "$CLI HDEL compat_h f2"
    test_cmd "HEXISTS"   "$CLI HEXISTS compat_h f1 2>/dev/null | grep -q 1"
    test_cmd "HGETALL"   "$CLI HGETALL compat_h"
    test_cmd "HKEYS"     "$CLI HKEYS compat_h"
    test_cmd "HVALS"     "$CLI HVALS compat_h"
    test_cmd "HMSET"     "$CLI HMSET compat_h f3 v3 f4 v4"
    test_cmd "HMGET"     "$CLI HMGET compat_h f1 f3"
    test_cmd "HINCRBY"   "$CLI HSET compat_h num 0 && $CLI HINCRBY compat_h num 5 2>/dev/null | grep -q 5"
    test_cmd "HINCRBYFLOAT" "$CLI HINCRBYFLOAT compat_h fnum 1.5"
    test_cmd "HSETNX"    "$CLI HSETNX compat_h newfield newval"
    test_cmd "HRANDFIELD" "$CLI HRANDFIELD compat_h"
    test_cmd "HSCAN"     "$CLI HSCAN compat_h 0 COUNT 100"

    flush_test_keys
    end_category "Hashes"
}

test_sets() {
    begin_category "Sets"

    test_cmd "SADD"        "$CLI SADD compat_set a b c"
    test_cmd "SCARD"       "$CLI SCARD compat_set 2>/dev/null | grep -q 3"
    test_cmd "SISMEMBER"   "$CLI SISMEMBER compat_set a 2>/dev/null | grep -q 1"
    test_cmd "SMISMEMBER"  "$CLI SMISMEMBER compat_set a z"
    test_cmd "SMEMBERS"    "$CLI SMEMBERS compat_set"
    test_cmd "SRANDMEMBER" "$CLI SRANDMEMBER compat_set"
    test_cmd "SPOP"        "$CLI SPOP compat_set"
    test_cmd "SREM"        "$CLI SREM compat_set a"
    test_cmd "SMOVE"       "$CLI SADD compat_set2 x && $CLI SMOVE compat_set2 compat_set x"
    test_cmd "SUNION"      "$CLI SADD compat_su1 a && $CLI SADD compat_su2 b && $CLI SUNION compat_su1 compat_su2"
    test_cmd "SUNIONSTORE" "$CLI SUNIONSTORE compat_sud compat_su1 compat_su2"
    test_cmd "SINTER"      "$CLI SADD compat_si1 a b && $CLI SADD compat_si2 b c && $CLI SINTER compat_si1 compat_si2"
    test_cmd "SINTERSTORE" "$CLI SINTERSTORE compat_sid compat_si1 compat_si2"
    test_cmd "SDIFF"       "$CLI SDIFF compat_si1 compat_si2"
    test_cmd "SDIFFSTORE"  "$CLI SDIFFSTORE compat_sdd compat_si1 compat_si2"
    test_cmd "SSCAN"       "$CLI SSCAN compat_set 0 COUNT 100"

    flush_test_keys
    end_category "Sets"
}

test_sorted_sets() {
    begin_category "Sorted Sets"

    test_cmd "ZADD"           "$CLI ZADD compat_z 1 a 2 b 3 c"
    test_cmd "ZCARD"          "$CLI ZCARD compat_z 2>/dev/null | grep -q 3"
    test_cmd "ZSCORE"         "$CLI ZSCORE compat_z b 2>/dev/null | grep -q 2"
    test_cmd "ZRANK"          "$CLI ZRANK compat_z a 2>/dev/null | grep -q 0"
    test_cmd "ZREVRANK"       "$CLI ZREVRANK compat_z a 2>/dev/null | grep -q 2"
    test_cmd "ZRANGE"         "$CLI ZRANGE compat_z 0 -1"
    test_cmd "ZREVRANGE"      "$CLI ZREVRANGE compat_z 0 -1"
    test_cmd "ZRANGEBYSCORE"  "$CLI ZRANGEBYSCORE compat_z 1 2"
    test_cmd "ZREVRANGEBYSCORE" "$CLI ZREVRANGEBYSCORE compat_z 3 1"
    test_cmd "ZINCRBY"        "$CLI ZINCRBY compat_z 10 a"
    test_cmd "ZREM"           "$CLI ZREM compat_z a"
    test_cmd "ZCOUNT"         "$CLI ZCOUNT compat_z 1 3 2>/dev/null | grep -q 2"
    test_cmd "ZLEXCOUNT"      "$CLI ZADD compat_zl 0 a 0 b 0 c && $CLI ZLEXCOUNT compat_zl '[a' '[c'"
    test_cmd "ZRANGEBYLEX"    "$CLI ZRANGEBYLEX compat_zl '[a' '[c'"
    test_cmd "ZPOPMIN"        "$CLI ZPOPMIN compat_z"
    test_cmd "ZPOPMAX"        "$CLI ZPOPMAX compat_z"
    test_cmd "ZRANDMEMBER"    "$CLI ZRANDMEMBER compat_z"
    test_cmd "ZMSCORE"        "$CLI ZADD compat_zm 1 a 2 b && $CLI ZMSCORE compat_zm a b"
    test_cmd "ZUNIONSTORE"    "$CLI ZADD compat_zu1 1 a && $CLI ZADD compat_zu2 2 b && $CLI ZUNIONSTORE compat_zud 2 compat_zu1 compat_zu2"
    test_cmd "ZINTERSTORE"    "$CLI ZADD compat_zi1 1 a 2 b && $CLI ZADD compat_zi2 3 b 4 c && $CLI ZINTERSTORE compat_zid 2 compat_zi1 compat_zi2"
    test_cmd "ZSCAN"          "$CLI ZSCAN compat_z 0 COUNT 100"

    flush_test_keys
    end_category "Sorted Sets"
}

test_keys() {
    begin_category "Keys"

    test_cmd "DEL"        "$CLI SET compat_dk v && $CLI DEL compat_dk 2>/dev/null | grep -q 1"
    test_cmd "UNLINK"     "$CLI SET compat_uk v && $CLI UNLINK compat_uk"
    test_cmd "EXISTS"     "$CLI SET compat_ek v && $CLI EXISTS compat_ek 2>/dev/null | grep -q 1"
    test_cmd "TYPE"       "$CLI SET compat_tk v && $CLI TYPE compat_tk 2>/dev/null | grep -q string"
    test_cmd "RENAME"     "$CLI SET compat_rn v && $CLI RENAME compat_rn compat_rn2"
    test_cmd "RENAMENX"   "$CLI SET compat_rnx v && $CLI RENAMENX compat_rnx compat_rnx2"
    test_cmd "KEYS"       "$CLI SET compat_keys_a 1 && $CLI KEYS 'compat_keys_*'"
    test_cmd "SCAN"       "$CLI SCAN 0 COUNT 100"
    test_cmd "RANDOMKEY"  "$CLI SET compat_rand v && $CLI RANDOMKEY"
    test_cmd "EXPIRE"     "$CLI SET compat_exp v && $CLI EXPIRE compat_exp 100"
    test_cmd "EXPIREAT"   "$CLI EXPIREAT compat_exp $(($(date +%s) + 3600))"
    test_cmd "PEXPIRE"    "$CLI SET compat_pexp v && $CLI PEXPIRE compat_pexp 100000"
    test_cmd "PEXPIREAT"  "$CLI PEXPIREAT compat_pexp $(( $(date +%s) * 1000 + 3600000 ))"
    test_cmd "TTL"        "$CLI TTL compat_exp"
    test_cmd "PTTL"       "$CLI PTTL compat_pexp"
    test_cmd "PERSIST"    "$CLI PERSIST compat_exp"
    test_cmd "OBJECT ENCODING" "$CLI SET compat_oe v && $CLI OBJECT ENCODING compat_oe"
    test_cmd "OBJECT REFCOUNT" "$CLI OBJECT REFCOUNT compat_oe"
    test_cmd "DUMP"       "$CLI DUMP compat_oe"
    test_cmd "SORT"       "$CLI RPUSH compat_sort 3 1 2 && $CLI SORT compat_sort"
    test_cmd "TOUCH"      "$CLI TOUCH compat_oe"
    test_cmd "OBJECT HELP" "$CLI OBJECT HELP"

    flush_test_keys
    end_category "Keys"
}

test_server() {
    begin_category "Server"

    test_cmd "PING"       "$CLI PING 2>/dev/null | grep -q PONG"
    test_cmd "ECHO"       "$CLI ECHO hello 2>/dev/null | grep -q hello"
    test_cmd "DBSIZE"     "$CLI DBSIZE"
    test_cmd "INFO"       "$CLI INFO"
    test_cmd "INFO server" "$CLI INFO server 2>/dev/null | head -1 | grep -qi 'server\|redis\|ferrite'"
    test_cmd "CONFIG GET" "$CLI CONFIG GET save"
    test_cmd "CONFIG SET" "$CLI CONFIG SET hz 10"
    test_cmd "FLUSHDB"    "$CLI FLUSHDB"
    test_cmd "FLUSHALL"   "$CLI FLUSHALL"
    test_cmd "SELECT"     "$CLI SELECT 0"
    test_cmd "SWAPDB"     "$CLI SWAPDB 0 1"
    test_cmd "TIME"       "$CLI TIME"
    test_cmd "COMMAND"    "$CLI COMMAND"
    test_cmd "COMMAND COUNT" "$CLI COMMAND COUNT"
    test_cmd "CLIENT ID"  "$CLI CLIENT ID"
    test_cmd "CLIENT LIST" "$CLI CLIENT LIST"
    test_cmd "CLIENT SETNAME" "$CLI CLIENT SETNAME compat-test"
    test_cmd "CLIENT GETNAME" "$CLI CLIENT GETNAME 2>/dev/null | grep -q compat-test"
    test_cmd "MULTI"      "printf 'MULTI\r\nSET txk txv\r\nEXEC\r\n' | $CLI && $CLI GET txk 2>/dev/null | grep -q txv && $CLI DEL txk"
    test_cmd "DISCARD"    "printf 'MULTI\r\nSET dk dv\r\nDISCARD\r\n' | $CLI"
    test_cmd "WATCH"      "$CLI WATCH compat_w"
    test_cmd "UNWATCH"    "$CLI UNWATCH"
    test_cmd "WAIT"       "$CLI WAIT 0 0"
    test_cmd "DEBUG SLEEP" "$CLI DEBUG SLEEP 0"

    flush_test_keys
    end_category "Server"
}

test_pubsub() {
    begin_category "Pub/Sub"

    test_cmd "PUBSUB CHANNELS"  "$CLI PUBSUB CHANNELS"
    test_cmd "PUBSUB NUMSUB"    "$CLI PUBSUB NUMSUB"
    test_cmd "PUBSUB NUMPAT"    "$CLI PUBSUB NUMPAT"
    test_cmd "PUBLISH"          "$CLI PUBLISH compat_chan hello"

    # SUBSCRIBE / PSUBSCRIBE are blocking — just verify the command is accepted
    skip_cmd "SUBSCRIBE"  "Blocking command; tested via integration tests"
    skip_cmd "PSUBSCRIBE" "Blocking command; tested via integration tests"
    skip_cmd "UNSUBSCRIBE" "Requires active subscription"
    skip_cmd "PUNSUBSCRIBE" "Requires active subscription"

    end_category "Pub/Sub"
}

test_scripting() {
    begin_category "Scripting"

    test_cmd "EVAL"       "$CLI EVAL 'return 1' 0 2>/dev/null | grep -q 1"
    test_cmd "EVALSHA"    "local sha=\$($CLI SCRIPT LOAD 'return 1' 2>/dev/null) && $CLI EVALSHA \"\$sha\" 0 2>/dev/null | grep -q 1"
    test_cmd "SCRIPT LOAD" "$CLI SCRIPT LOAD 'return 1'"
    test_cmd "SCRIPT EXISTS" "local sha=\$($CLI SCRIPT LOAD 'return 2' 2>/dev/null) && $CLI SCRIPT EXISTS \"\$sha\""
    test_cmd "SCRIPT FLUSH" "$CLI SCRIPT FLUSH"

    end_category "Scripting"
}

test_bitops() {
    begin_category "Bitmaps & HyperLogLog"

    test_cmd "SETBIT"     "$CLI SETBIT compat_bit 7 1"
    test_cmd "GETBIT"     "$CLI GETBIT compat_bit 7 2>/dev/null | grep -q 1"
    test_cmd "BITCOUNT"   "$CLI BITCOUNT compat_bit 2>/dev/null | grep -q 1"
    test_cmd "BITOP"      "$CLI SET compat_b1 a && $CLI SET compat_b2 b && $CLI BITOP AND compat_bd compat_b1 compat_b2"
    test_cmd "BITPOS"     "$CLI BITPOS compat_bit 1"
    test_cmd "PFADD"      "$CLI PFADD compat_hll a b c d"
    test_cmd "PFCOUNT"    "$CLI PFCOUNT compat_hll"
    test_cmd "PFMERGE"    "$CLI PFADD compat_hll2 e f && $CLI PFMERGE compat_hlld compat_hll compat_hll2"

    flush_test_keys
    end_category "Bitmaps & HyperLogLog"
}

# ── Generate report ───────────────────────────────────────────────────────────

generate_report() {
    emit "# Ferrite — Redis Compatibility Report"
    emit ""
    emit "_Generated on $(date -u '+%Y-%m-%d %H:%M:%S UTC')_"
    emit ""
    emit "Server: **Ferrite** on port \`${FERRITE_PORT}\`"
    emit ""

    test_strings
    test_lists
    test_hashes
    test_sets
    test_sorted_sets
    test_keys
    test_server
    test_pubsub
    test_scripting
    test_bitops

    local total=$((TOTAL_PASS + TOTAL_FAIL + TOTAL_SKIP))
    local pct=0
    if [[ $((TOTAL_PASS + TOTAL_FAIL)) -gt 0 ]]; then
        pct=$(( (TOTAL_PASS * 100) / (TOTAL_PASS + TOTAL_FAIL) ))
    fi

    emit ""
    emit "---"
    emit ""
    emit "## Summary"
    emit ""
    emit "| Metric | Count |"
    emit "|--------|------:|"
    emit "| ✅ Passed  | $TOTAL_PASS |"
    emit "| ❌ Failed  | $TOTAL_FAIL |"
    emit "| ⏭ Skipped | $TOTAL_SKIP |"
    emit "| **Total**  | **$total** |"
    emit ""
    emit "**Compatibility: ${pct}%** (${TOTAL_PASS}/${TOTAL_PASS + TOTAL_FAIL} tested commands passing)"
    emit ""

    # Output report
    if [[ -n "$REPORT_FILE" ]]; then
        printf '%s' "$REPORT_LINES" > "$REPORT_FILE"
        info "Report written to $REPORT_FILE"
    else
        printf '%s' "$REPORT_LINES"
    fi
}

# ── Main ──────────────────────────────────────────────────────────────────────

main() {
    info "Ferrite Redis compatibility report generator"
    info "Binary: $FERRITE_BIN | Port: $FERRITE_PORT"
    echo "" >&2

    check_prereqs
    start_ferrite
    generate_report

    info "Done — $TOTAL_PASS passed, $TOTAL_FAIL failed, $TOTAL_SKIP skipped"
}

main
