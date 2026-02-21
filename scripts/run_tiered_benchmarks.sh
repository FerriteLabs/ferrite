#!/usr/bin/env bash
# Run Ferrite tiered storage benchmarks and generate reports.
#
# Usage:
#   ./scripts/run_tiered_benchmarks.sh            # Run all groups
#   ./scripts/run_tiered_benchmarks.sh hot_tier    # Run a single group
#
# Output:
#   target/criterion/          — Criterion HTML reports
#   target/tiered_bench_summary.md — Key numbers extracted into markdown

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

FILTER="${1:-}"
SUMMARY_FILE="target/tiered_bench_summary.md"

echo "=== Ferrite Tiered Storage Benchmarks ==="
echo "Repo root: $REPO_ROOT"
echo ""

# ── 1. Run Criterion benchmarks ──────────────────────────────────────────────

if [ -n "$FILTER" ]; then
    echo "Running benchmarks matching: $FILTER"
    cargo bench --bench tiered_storage -- "$FILTER"
else
    echo "Running all tiered storage benchmarks…"
    cargo bench --bench tiered_storage
fi

# ── 2. Generate summary markdown ─────────────────────────────────────────────

mkdir -p target

{
    echo "# Tiered Storage Benchmark Summary"
    echo ""
    echo "_Generated on $(date -u +"%Y-%m-%d %H:%M:%S UTC")_"
    echo ""
    echo "| Benchmark | Time (mean) | Throughput |"
    echo "|-----------|-------------|------------|"

    # Parse Criterion's JSON estimates if available
    for est_file in target/criterion/*/new/estimates.json; do
        [ -f "$est_file" ] || continue
        bench_name=$(echo "$est_file" | sed 's|target/criterion/||; s|/new/estimates.json||')
        # Extract mean point estimate (nanoseconds)
        if command -v python3 &>/dev/null; then
            mean_ns=$(python3 -c "
import json, sys
with open('$est_file') as f:
    data = json.load(f)
print(f\"{data['mean']['point_estimate']:.1f}\")
" 2>/dev/null || echo "N/A")
        else
            mean_ns="N/A"
        fi

        if [ "$mean_ns" != "N/A" ]; then
            # Convert to human-readable
            human=$(python3 -c "
ns = $mean_ns
if ns < 1000:
    print(f'{ns:.1f} ns')
elif ns < 1_000_000:
    print(f'{ns/1000:.2f} µs')
else:
    print(f'{ns/1_000_000:.2f} ms')
" 2>/dev/null || echo "${mean_ns} ns")

            ops=$(python3 -c "
ns = $mean_ns
if ns > 0:
    ops = 1_000_000_000 / ns
    if ops > 1_000_000:
        print(f'{ops/1_000_000:.2f}M ops/s')
    elif ops > 1_000:
        print(f'{ops/1_000:.1f}K ops/s')
    else:
        print(f'{ops:.0f} ops/s')
else:
    print('N/A')
" 2>/dev/null || echo "N/A")

            echo "| $bench_name | $human | $ops |"
        else
            echo "| $bench_name | N/A | N/A |"
        fi
    done
} > "$SUMMARY_FILE"

echo ""
echo "=== Results ==="
echo ""
cat "$SUMMARY_FILE"
echo ""
echo "HTML reports: target/criterion/report/index.html"
echo "Summary:      $SUMMARY_FILE"
