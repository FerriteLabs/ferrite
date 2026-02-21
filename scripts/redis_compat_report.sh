#!/usr/bin/env bash
# Parse Rust test output from the Redis compatibility test suite and generate
# a Markdown compatibility report with pass/fail/skip percentages.
#
# This script runs `cargo test --test redis_compatibility` and parses the
# standard test harness output to produce a summary grouped by command category.
#
# Usage:
#   ./scripts/redis_compat_report.sh                          # Print to stdout
#   REPORT_FILE=compat.md ./scripts/redis_compat_report.sh    # Write to file

set -euo pipefail

REPORT_FILE="${REPORT_FILE:-}"
CARGO_ARGS="${CARGO_ARGS:---release}"

# ── Helpers ───────────────────────────────────────────────────────────────────

info() { printf '\033[0;34m[INFO]\033[0m  %s\n' "$*" >&2; }
ok()   { printf '\033[0;32m[PASS]\033[0m  %s\n' "$*" >&2; }
fail() { printf '\033[0;31m[FAIL]\033[0m  %s\n' "$*" >&2; }

# ── Run tests ─────────────────────────────────────────────────────────────────

info "Running Redis compatibility tests..."

TEST_OUTPUT=$(cargo test --test redis_compatibility ${CARGO_ARGS} -- --format terse 2>&1) || true

# ── Parse results ─────────────────────────────────────────────────────────────

# Count test results from cargo test output
PASSED=$(echo "$TEST_OUTPUT" | grep -cE '^\s*test .+ \.\.\. ok$' || echo 0)
FAILED=$(echo "$TEST_OUTPUT" | grep -cE '^\s*test .+ \.\.\. FAILED$' || echo 0)
IGNORED=$(echo "$TEST_OUTPUT" | grep -cE '^\s*test .+ \.\.\. ignored$' || echo 0)

# Also try the summary line as fallback
if [[ "$PASSED" -eq 0 && "$FAILED" -eq 0 ]]; then
    SUMMARY_LINE=$(echo "$TEST_OUTPUT" | grep -E '^test result:' | tail -1 || echo "")
    if [[ -n "$SUMMARY_LINE" ]]; then
        PASSED=$(echo "$SUMMARY_LINE" | grep -oE '[0-9]+ passed' | grep -oE '[0-9]+' || echo 0)
        FAILED=$(echo "$SUMMARY_LINE" | grep -oE '[0-9]+ failed' | grep -oE '[0-9]+' || echo 0)
        IGNORED=$(echo "$SUMMARY_LINE" | grep -oE '[0-9]+ ignored' | grep -oE '[0-9]+' || echo 0)
    fi
fi

TOTAL=$((PASSED + FAILED + IGNORED))
TESTED=$((PASSED + FAILED))

if [[ "$TESTED" -gt 0 ]]; then
    PCT=$(( (PASSED * 100) / TESTED ))
else
    PCT=0
fi

# ── Categorise tests ─────────────────────────────────────────────────────────

declare -A CAT_PASS CAT_FAIL CAT_IGNORE

categorise() {
    local status="$1" name="$2"
    local cat="Other"

    case "$name" in
        *string*)       cat="String" ;;
        *list*)         cat="List" ;;
        *hash*)         cat="Hash" ;;
        *set_s*|*set_no*|*set_sadd*|*set_srem*|*set_smembers*|*set_sismember*|*set_scard*|*set_sunion*|*set_sinter*|*set_sdiff*|*set_spop*|*set_srand*)
                        cat="Set" ;;
        *zset*|*sorted*) cat="Sorted Set" ;;
        *key_*|*unlink*|*rename*|*scan*|*random*|*touch*)
                        cat="Key" ;;
        *server*|*database*|*ping*|*echo*|*dbsize*|*flush*|*select*|*info*)
                        cat="Server" ;;
        *pubsub*|*subscribe*|*publish*)
                        cat="Pub/Sub" ;;
        *transaction*|*multi*|*exec*|*discard*|*watch*)
                        cat="Transaction" ;;
        *hyperlog*|*pfadd*|*pfcount*|*pfmerge*)
                        cat="HyperLogLog" ;;
        *resp*)         cat="Protocol" ;;
        *concurrent*)   cat="Concurrency" ;;
        *expir*|*ttl*|*persist*)
                        cat="Expiration" ;;
        *type*)         cat="Key" ;;
        *matrix*)       cat="Report" ;;
    esac

    case "$status" in
        ok)      CAT_PASS[$cat]=$(( ${CAT_PASS[$cat]:-0} + 1 )) ;;
        FAILED)  CAT_FAIL[$cat]=$(( ${CAT_FAIL[$cat]:-0} + 1 )) ;;
        ignored) CAT_IGNORE[$cat]=$(( ${CAT_IGNORE[$cat]:-0} + 1 )) ;;
    esac
}

while IFS= read -r line; do
    if [[ "$line" =~ ^[[:space:]]*test[[:space:]]+(compat_[a-z0-9_]+)[[:space:]]+\.\.\.[[:space:]]+(ok|FAILED|ignored) ]]; then
        categorise "${BASH_REMATCH[2]}" "${BASH_REMATCH[1]}"
    fi
done <<< "$TEST_OUTPUT"

# ── Generate report ──────────────────────────────────────────────────────────

REPORT=""
emit() { REPORT+="$1
"; }

emit "# Ferrite — Redis Compatibility Test Report"
emit ""
emit "_Generated on $(date -u '+%Y-%m-%d %H:%M:%S UTC')_"
emit ""
emit "## Overall Compatibility"
emit ""
emit "| Metric | Value |"
emit "|--------|------:|"
emit "| ✅ Passed  | $PASSED |"
emit "| ❌ Failed  | $FAILED |"
emit "| ⏭ Ignored | $IGNORED |"
emit "| **Total**  | **$TOTAL** |"
emit "| **Pass Rate** | **${PCT}%** |"
emit ""
emit "---"
emit ""
emit "## Results by Category"
emit ""
emit "| Category | Passed | Failed | Ignored | Pass Rate |"
emit "|----------|-------:|-------:|--------:|----------:|"

ALL_CATS=$(echo "${!CAT_PASS[@]} ${!CAT_FAIL[@]} ${!CAT_IGNORE[@]}" | tr ' ' '\n' | sort -u)

for cat in $ALL_CATS; do
    p=${CAT_PASS[$cat]:-0}
    f=${CAT_FAIL[$cat]:-0}
    i=${CAT_IGNORE[$cat]:-0}
    tested=$((p + f))
    if [[ "$tested" -gt 0 ]]; then
        cpct=$(( (p * 100) / tested ))
    else
        cpct=100
    fi
    emit "| $cat | $p | $f | $i | ${cpct}% |"
done

emit ""
emit "---"
emit ""

# List failures if any
if [[ "$FAILED" -gt 0 ]]; then
    emit "## Failed Tests"
    emit ""
    echo "$TEST_OUTPUT" | grep -E '^\s*test .+ \.\.\. FAILED$' | while IFS= read -r line; do
        name=$(echo "$line" | sed 's/.*test \(.*\) \.\.\. FAILED/\1/' | xargs)
        emit "- \`$name\`"
    done
    emit ""
fi

# Compilation errors
if echo "$TEST_OUTPUT" | grep -q 'could not compile'; then
    emit "## ⚠️ Compilation Errors"
    emit ""
    emit "The test suite did not compile successfully. Fix compilation errors first."
    emit ""
    emit '```'
    echo "$TEST_OUTPUT" | grep -E '^error' | head -10 | while IFS= read -r line; do
        emit "$line"
    done
    emit '```'
    emit ""
fi

emit "---"
emit ""
emit "_Run locally: \`cargo test --test redis_compatibility\`_"

# ── Output ───────────────────────────────────────────────────────────────────

if [[ -n "$REPORT_FILE" ]]; then
    printf '%s' "$REPORT" > "$REPORT_FILE"
    info "Report written to $REPORT_FILE"
else
    printf '%s' "$REPORT"
fi

# ── Summary ──────────────────────────────────────────────────────────────────

info "Compatibility: ${PCT}% (${PASSED}/${TESTED} tests passing)"
if [[ "$FAILED" -gt 0 ]]; then
    fail "$FAILED tests failed"
    exit 1
else
    ok "All $PASSED tests passed"
fi
