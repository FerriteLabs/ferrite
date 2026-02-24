#!/usr/bin/env bash
# =============================================================================
# Ferrite Redis TCL Test Runner
# =============================================================================
# Runs the official Redis TCL test suite against a Ferrite server and generates
# a compatibility report with per-command pass/fail status.
#
# Usage:
#   ./scripts/tcl-compat.sh [OPTIONS]
#
# Options:
#   --ferrite-bin PATH    Path to ferrite binary (default: ./target/release/ferrite)
#   --redis-tests PATH   Path to Redis tests/ directory (default: ./redis-tests/)
#   --port PORT           Port to run Ferrite on (default: 6399)
#   --output DIR          Output directory for reports (default: ./tcl-results/)
#   --tags TAGS           Only run tests with these tags (comma-separated)
#   --skip PATTERNS       Skip tests matching patterns (comma-separated)
#   --help                Show this help
#
# Prerequisites:
#   - Tcl 8.6+: apt-get install tcl
#   - Redis source: git clone https://github.com/redis/redis.git redis-tests
#   - Ferrite binary: cargo build --release

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────

FERRITE_BIN="./target/release/ferrite"
REDIS_TESTS="./redis-tests"
PORT=6399
OUTPUT_DIR="./tcl-results"
TAGS=""
SKIP=""
FERRITE_PID=""

# ── Color output ──────────────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log()  { echo -e "${BLUE}[TCL]${NC} $*"; }
ok()   { echo -e "${GREEN}[PASS]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }

# ── Argument parsing ──────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
    case $1 in
        --ferrite-bin) FERRITE_BIN="$2"; shift 2 ;;
        --redis-tests) REDIS_TESTS="$2"; shift 2 ;;
        --port) PORT="$2"; shift 2 ;;
        --output) OUTPUT_DIR="$2"; shift 2 ;;
        --tags) TAGS="$2"; shift 2 ;;
        --skip) SKIP="$2"; shift 2 ;;
        --help)
            sed -n '2,/^$/p' "$0" | sed 's/^# //' | sed 's/^#//'
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ── Validation ────────────────────────────────────────────────────────────────

if [[ ! -x "$FERRITE_BIN" ]]; then
    echo "Error: Ferrite binary not found at $FERRITE_BIN"
    echo "Build with: cargo build --release"
    exit 1
fi

if [[ ! -d "$REDIS_TESTS/tests" ]]; then
    echo "Error: Redis tests directory not found at $REDIS_TESTS/tests"
    echo "Clone Redis: git clone --depth 1 https://github.com/redis/redis.git $REDIS_TESTS"
    exit 1
fi

if ! command -v tclsh &>/dev/null; then
    echo "Error: tclsh not found. Install Tcl: apt-get install tcl"
    exit 1
fi

mkdir -p "$OUTPUT_DIR"

# ── Cleanup ───────────────────────────────────────────────────────────────────

cleanup() {
    if [[ -n "$FERRITE_PID" ]] && kill -0 "$FERRITE_PID" 2>/dev/null; then
        log "Stopping Ferrite (PID $FERRITE_PID)"
        kill "$FERRITE_PID" 2>/dev/null || true
        wait "$FERRITE_PID" 2>/dev/null || true
    fi
    rm -f "$OUTPUT_DIR/.ferrite.pid"
}
trap cleanup EXIT

# ── Start Ferrite ─────────────────────────────────────────────────────────────

log "Starting Ferrite on port $PORT..."
"$FERRITE_BIN" --port "$PORT" --databases 16 --data-dir /tmp/ferrite-tcl-test &
FERRITE_PID=$!
echo "$FERRITE_PID" > "$OUTPUT_DIR/.ferrite.pid"

# Wait for server to be ready
for i in $(seq 1 30); do
    if redis-cli -p "$PORT" PING 2>/dev/null | grep -q PONG; then
        log "Ferrite is ready (PID $FERRITE_PID)"
        break
    fi
    if [[ $i -eq 30 ]]; then
        fail "Ferrite failed to start within 30 seconds"
        exit 1
    fi
    sleep 1
done

# ── Discover tests ────────────────────────────────────────────────────────────

log "Discovering TCL test files..."
TEST_FILES=()
while IFS= read -r -d '' f; do
    TEST_FILES+=("$f")
done < <(find "$REDIS_TESTS/tests/unit" -name '*.tcl' -print0 | sort -z)

TOTAL=${#TEST_FILES[@]}
log "Found $TOTAL test files"

# ── Run tests ─────────────────────────────────────────────────────────────────

PASSED=0
FAILED=0
ERRORS=0
SKIPPED=0
RESULTS_CSV="$OUTPUT_DIR/results.csv"
RESULTS_MD="$OUTPUT_DIR/compatibility-matrix.md"

echo "file,test_name,status,duration_ms,error" > "$RESULTS_CSV"

for test_file in "${TEST_FILES[@]}"; do
    basename=$(basename "$test_file" .tcl)

    # Skip if matches skip pattern
    if [[ -n "$SKIP" ]]; then
        IFS=',' read -ra SKIP_PATTERNS <<< "$SKIP"
        skip_this=false
        for pattern in "${SKIP_PATTERNS[@]}"; do
            if [[ "$basename" == *"$pattern"* ]]; then
                skip_this=true
                break
            fi
        done
        if $skip_this; then
            warn "Skipping $basename"
            echo "$basename,SKIPPED,skipped,0,skipped" >> "$RESULTS_CSV"
            SKIPPED=$((SKIPPED + 1))
            continue
        fi
    fi

    log "Running: $basename"
    start_time=$(date +%s%N)

    # Run the TCL test with Ferrite's port
    set +e
    output=$(cd "$REDIS_TESTS" && tclsh tests/unit/"$basename".tcl \
        --host 127.0.0.1 --port "$PORT" \
        --single "tests/unit/$basename.tcl" \
        --timeout 30 2>&1)
    exit_code=$?
    set -e

    end_time=$(date +%s%N)
    duration_ms=$(( (end_time - start_time) / 1000000 ))

    if [[ $exit_code -eq 0 ]]; then
        ok "$basename ($duration_ms ms)"
        echo "$basename,$basename,PASS,$duration_ms," >> "$RESULTS_CSV"
        PASSED=$((PASSED + 1))
    else
        # Extract error message (last 3 lines)
        error_msg=$(echo "$output" | tail -3 | tr '\n' ' ' | sed 's/,/ /g' | cut -c1-200)
        fail "$basename ($duration_ms ms): $error_msg"
        echo "$basename,$basename,FAIL,$duration_ms,$error_msg" >> "$RESULTS_CSV"
        FAILED=$((FAILED + 1))
    fi
done

# ── Generate Markdown Report ─────────────────────────────────────────────────

TOTAL_RUN=$((PASSED + FAILED))
if [[ $TOTAL_RUN -gt 0 ]]; then
    PASS_RATE=$(echo "scale=1; $PASSED * 100 / $TOTAL_RUN" | bc)
else
    PASS_RATE="0.0"
fi

cat > "$RESULTS_MD" << EOF
# Ferrite Redis Compatibility Matrix

**Generated**: $(date -u '+%Y-%m-%d %H:%M:%S UTC')
**Ferrite Version**: $(${FERRITE_BIN} --version 2>/dev/null || echo "dev")
**Redis Tests Source**: $(cd "$REDIS_TESTS" && git log -1 --format='%h %s' 2>/dev/null || echo "unknown")

## Summary

| Metric | Count |
|--------|-------|
| **Total Test Files** | $TOTAL |
| **Passed** | $PASSED |
| **Failed** | $FAILED |
| **Skipped** | $SKIPPED |
| **Pass Rate** | ${PASS_RATE}% |

## Results by Test File

| Test File | Status | Duration (ms) | Error |
|-----------|--------|---------------|-------|
EOF

# Generate table from CSV
tail -n +2 "$RESULTS_CSV" | while IFS=',' read -r file name status duration error; do
    if [[ "$status" == "PASS" ]]; then
        icon="✅"
    elif [[ "$status" == "FAIL" ]]; then
        icon="❌"
    else
        icon="⏭️"
    fi
    echo "| $file | $icon $status | $duration | ${error:-—} |" >> "$RESULTS_MD"
done

cat >> "$RESULTS_MD" << EOF

## How to Reproduce

\`\`\`bash
# Clone Redis for tests
git clone --depth 1 https://github.com/redis/redis.git redis-tests

# Build Ferrite
cargo build --release

# Run compatibility tests
./scripts/tcl-compat.sh --ferrite-bin ./target/release/ferrite --redis-tests ./redis-tests
\`\`\`

## Known Deviations

Commands that intentionally differ from Redis behavior:

| Command | Deviation | Reason |
|---------|-----------|--------|
| INFO | Additional sections (tiering, vector, query) | Multi-model features |
| CONFIG | Additional parameters (tiering-*, cluster-*) | Ferrite-specific config |
| MEMORY | Additional stats (tier breakdown) | Tiered storage |
EOF

# ── Summary ───────────────────────────────────────────────────────────────────

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  Redis TCL Compatibility Results"
echo "════════════════════════════════════════════════════════════════"
echo "  Total:   $TOTAL test files"
echo "  Passed:  $PASSED"
echo "  Failed:  $FAILED"
echo "  Skipped: $SKIPPED"
echo "  Rate:    ${PASS_RATE}%"
echo ""
echo "  CSV:     $RESULTS_CSV"
echo "  Report:  $RESULTS_MD"
echo "════════════════════════════════════════════════════════════════"
