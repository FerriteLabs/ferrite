#!/usr/bin/env bash
# =============================================================================
# Ferrite Ship Verification
# =============================================================================
# Checks all prerequisites before shipping v0.2.0
#
# Usage: ./scripts/verify-ship.sh

set -euo pipefail

PASS=0
FAIL=0
WARN=0

check() {
    local name="$1"
    local result="$2"
    if [[ "$result" == "pass" ]]; then
        echo "  ✅ $name"
        PASS=$((PASS + 1))
    elif [[ "$result" == "warn" ]]; then
        echo "  ⚠️  $name"
        WARN=$((WARN + 1))
    else
        echo "  ❌ $name"
        FAIL=$((FAIL + 1))
    fi
}

cd "$(git rev-parse --show-toplevel)"

echo "═══════════════════════════════════════════════════════════════"
echo "  Ferrite v0.2.0 Ship Verification"
echo "═══════════════════════════════════════════════════════════════"
echo ""

echo "Build & Quality:"
# Build
cargo check --quiet 2>/dev/null && check "cargo check passes" "pass" || check "cargo check passes" "fail"

# Warnings
WARNS=$(cargo check 2>&1 | grep '^warning:' | grep -v 'generated\|Finished' | wc -l | tr -d ' ')
[[ "$WARNS" -eq 0 ]] && check "0 compiler warnings" "pass" || check "$WARNS compiler warnings" "fail"

# Format
cargo fmt --all --check --quiet 2>/dev/null && check "cargo fmt clean" "pass" || check "cargo fmt clean" "fail"

# Clippy
CLIPPY=$(cargo clippy --workspace --quiet 2>&1 | grep -c '^warning\[' 2>/dev/null || echo 0)
[[ "$CLIPPY" -eq 0 ]] && check "0 clippy warnings" "pass" || check "$CLIPPY clippy warnings" "fail"

echo ""
echo "Tests:"
# Unit tests
TESTS=$(cargo test --workspace --lib --quiet 2>&1 | grep 'test result' | grep -c 'ok' 2>/dev/null || echo 0)
[[ "$TESTS" -gt 10 ]] && check "Unit tests pass ($TESTS crates)" "pass" || check "Unit tests" "fail"

# Integration tests
cargo test --test integration_harness --quiet 2>/dev/null && check "Integration tests pass" "pass" || check "Integration tests" "fail"

echo ""
echo "Documentation:"
[[ -f README.md ]] && check "README.md exists" "pass" || check "README.md" "fail"
[[ -f CHANGELOG.md ]] && check "CHANGELOG.md exists" "pass" || check "CHANGELOG.md" "fail"
[[ -f LICENSE ]] && check "LICENSE exists" "pass" || check "LICENSE" "fail"
[[ -f CONTRIBUTING.md ]] && check "CONTRIBUTING.md exists" "pass" || check "CONTRIBUTING.md" "fail"
[[ -f CODE_OF_CONDUCT.md ]] && check "CODE_OF_CONDUCT.md exists" "pass" || check "CODE_OF_CONDUCT.md" "fail"
[[ -f SECURITY.md ]] && check "SECURITY.md exists" "pass" || check "SECURITY.md" "fail"
[[ -f FIRST_CONTRIBUTORS.md ]] && check "FIRST_CONTRIBUTORS.md exists" "pass" || check "FIRST_CONTRIBUTORS.md" "fail"
[[ -f RELEASE_CHECKLIST.md ]] && check "RELEASE_CHECKLIST.md exists" "pass" || check "RELEASE_CHECKLIST.md" "fail"

echo ""
echo "CI/CD:"
[[ -f .github/workflows/ci.yml ]] && check "CI workflow exists" "pass" || check "CI workflow" "fail"
[[ -f .github/workflows/release-full.yml ]] && check "Release workflow exists" "pass" || check "Release workflow" "fail"
[[ -f .github/workflows/compat-dashboard.yml ]] && check "Compat dashboard workflow exists" "pass" || check "Compat dashboard" "fail"

echo ""
echo "Security:"
[[ -f deny.toml ]] && check "deny.toml configured" "pass" || check "deny.toml" "fail"
[[ -f .pre-commit-config.yaml ]] && check "Pre-commit hooks configured" "pass" || check "Pre-commit hooks" "fail"
[[ -f scripts/security-audit.sh ]] && check "Security audit script exists" "pass" || check "Security audit" "fail"

echo ""
echo "Ecosystem:"
[[ -f scripts/tcl-compat.sh ]] && check "TCL compat runner exists" "pass" || check "TCL compat runner" "fail"
[[ -f scripts/create-issues.sh ]] && check "Issue creation script exists" "pass" || check "Issue creation" "fail"
[[ -f scripts/release.sh ]] && check "Release script exists" "pass" || check "Release script" "fail"
[[ -f scripts/atomic-commit.sh ]] && check "Atomic commit script exists" "pass" || check "Atomic commit" "fail"

echo ""
echo "Version:"
VERSION=$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)".*/\1/')
check "Current version: $VERSION" "pass"

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Results: $PASS passed, $WARN warnings, $FAIL failed"
if [[ $FAIL -eq 0 ]]; then
    echo "  ✅ SHIP IT!"
else
    echo "  ❌ Fix $FAIL issue(s) before shipping."
fi
echo "═══════════════════════════════════════════════════════════════"

exit $FAIL
