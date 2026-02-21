#!/usr/bin/env bash
# Check for missing documentation across workspace crates.
# Runs `cargo doc` with warnings enabled and reports undocumented public items.
#
# Usage:
#   ./scripts/check-docs.sh              # Check all crates
#   ./scripts/check-docs.sh ferrite-core # Check single crate

set -euo pipefail

CRATE="${1:-}"
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
NC='\033[0m'
BOLD='\033[1m'

if [[ -n "$CRATE" ]]; then
    crates=("$CRATE")
else
    crates=(
        ferrite-core
        ferrite-search
        ferrite-ai
        ferrite-graph
        ferrite-timeseries
        ferrite-document
        ferrite-streaming
        ferrite-cloud
        ferrite-k8s
        ferrite-enterprise
        ferrite-plugins
        ferrite-studio
    )
fi

echo ""
echo -e "${BOLD}Documentation Coverage Report${NC}"
echo "════════════════════════════════════════════════════"
echo ""

total_warnings=0

for crate in "${crates[@]}"; do
    count=$(RUSTDOCFLAGS="-W missing_docs" cargo doc -p "$crate" --no-deps 2>&1 | grep -c "warning: missing documentation" || true)
    total_warnings=$((total_warnings + count))

    if [[ "$count" -eq 0 ]]; then
        echo -e "  ${GREEN}✓${NC} ${crate}: fully documented"
    elif [[ "$count" -lt 10 ]]; then
        echo -e "  ${YELLOW}△${NC} ${crate}: ${count} missing docs"
    else
        echo -e "  ${RED}✗${NC} ${crate}: ${count} missing docs"
    fi
done

echo ""
echo "────────────────────────────────────────────────────"
echo -e "  Total missing docs: ${BOLD}${total_warnings}${NC}"
echo ""

if [[ "$total_warnings" -eq 0 ]]; then
    echo -e "${GREEN}${BOLD}All public items are documented!${NC}"
else
    echo -e "${YELLOW}Run with a crate name to see specific warnings:${NC}"
    echo -e "  ${CYAN}RUSTDOCFLAGS=\"-W missing_docs\" cargo doc -p ferrite-core --no-deps 2>&1 | head -50${NC}"
fi
