#!/usr/bin/env bash
# =============================================================================
# Ferrite Security Audit Script
# =============================================================================
# Runs a comprehensive security audit of the Ferrite codebase:
#   1. cargo-audit: known vulnerability scan
#   2. cargo-deny: license and advisory check
#   3. unsafe block audit: find and verify all unsafe usage
#   4. Secret scan: check for hardcoded credentials
#   5. Dependency review: check for unmaintained/yanked crates
#
# Usage: ./scripts/security-audit.sh [--fix] [--verbose]

set -euo pipefail

FIX=false
VERBOSE=false
ISSUES=0

for arg in "$@"; do
    case $arg in
        --fix) FIX=true ;;
        --verbose) VERBOSE=true ;;
    esac
done

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

section() { echo -e "\n${BLUE}══════════════════════════════════════════${NC}"; echo -e "${BLUE}  $1${NC}"; echo -e "${BLUE}══════════════════════════════════════════${NC}"; }
ok()      { echo -e "  ${GREEN}✓${NC} $*"; }
warn()    { echo -e "  ${YELLOW}⚠${NC} $*"; ISSUES=$((ISSUES + 1)); }
fail()    { echo -e "  ${RED}✗${NC} $*"; ISSUES=$((ISSUES + 1)); }

# ── 1. Cargo Audit ────────────────────────────────────────────────────────────

section "1/5 — Vulnerability Scan (cargo-audit)"

if command -v cargo-audit &>/dev/null; then
    if cargo audit 2>/dev/null; then
        ok "No known vulnerabilities found"
    else
        fail "Vulnerabilities detected — run 'cargo audit' for details"
    fi
else
    warn "cargo-audit not installed. Install: cargo install cargo-audit"
fi

# ── 2. Cargo Deny ────────────────────────────────────────────────────────────

section "2/5 — License & Advisory Check (cargo-deny)"

if command -v cargo-deny &>/dev/null; then
    if cargo deny check 2>/dev/null; then
        ok "All dependency licenses and advisories clear"
    else
        fail "cargo-deny found issues — run 'cargo deny check' for details"
    fi
else
    warn "cargo-deny not installed. Install: cargo install cargo-deny"
fi

# ── 3. Unsafe Block Audit ────────────────────────────────────────────────────

section "3/5 — Unsafe Code Audit"

UNSAFE_FILES=$(grep -rl 'unsafe\s*{' --include='*.rs' | grep -v target | grep -v test || true)
UNSAFE_COUNT=$(echo "$UNSAFE_FILES" | grep -c '.' 2>/dev/null || echo 0)

echo "  Found $UNSAFE_COUNT files with unsafe blocks"

if [[ $UNSAFE_COUNT -gt 0 ]]; then
    MISSING_SAFETY=0
    while IFS= read -r file; do
        [[ -z "$file" ]] && continue
        # Check each unsafe block has a SAFETY comment
        unsafe_lines=$(grep -n 'unsafe\s*{' "$file" | grep -v '//.*unsafe' || true)
        while IFS= read -r line; do
            [[ -z "$line" ]] && continue
            line_num=$(echo "$line" | cut -d: -f1)
            prev_line=$((line_num - 1))
            has_safety=$(sed -n "${prev_line}p" "$file" | grep -ci 'SAFETY' || true)
            if [[ $has_safety -eq 0 ]]; then
                # Check 2 lines above too
                prev_line2=$((line_num - 2))
                has_safety2=$(sed -n "${prev_line2}p" "$file" | grep -ci 'SAFETY' || true)
                if [[ $has_safety2 -eq 0 ]]; then
                    warn "Missing // SAFETY: comment at $file:$line_num"
                    MISSING_SAFETY=$((MISSING_SAFETY + 1))
                fi
            fi
        done <<< "$unsafe_lines"
    done <<< "$UNSAFE_FILES"

    if [[ $MISSING_SAFETY -eq 0 ]]; then
        ok "All unsafe blocks have SAFETY comments"
    else
        fail "$MISSING_SAFETY unsafe blocks missing SAFETY comments"
    fi
else
    ok "No unsafe blocks found"
fi

# ── 4. Secret Scan ───────────────────────────────────────────────────────────

section "4/5 — Secret & Credential Scan"

SECRET_PATTERNS=(
    'password\s*=\s*"[^"]*[a-zA-Z0-9]'
    'api[_-]?key\s*=\s*"[^"]*[a-zA-Z0-9]'
    'secret[_-]?key\s*=\s*"[^"]*[a-zA-Z0-9]'
    'token\s*=\s*"[^"]*[a-zA-Z0-9]'
    'BEGIN (RSA |EC )?PRIVATE KEY'
    'AKIA[0-9A-Z]{16}'
)

SECRETS_FOUND=0
for pattern in "${SECRET_PATTERNS[@]}"; do
    matches=$(grep -rn "$pattern" --include='*.rs' --include='*.toml' --include='*.yml' \
              --include='*.yaml' --include='*.json' \
              | grep -v target | grep -v test | grep -v example | grep -v mock | grep -v placeholder || true)
    if [[ -n "$matches" ]]; then
        fail "Potential secret found matching: $pattern"
        if $VERBOSE; then echo "$matches" | head -3; fi
        SECRETS_FOUND=$((SECRETS_FOUND + 1))
    fi
done

if [[ $SECRETS_FOUND -eq 0 ]]; then
    ok "No hardcoded secrets detected"
fi

# ── 5. Dependency Health ─────────────────────────────────────────────────────

section "5/5 — Dependency Health Check"

# Check for wildcard dependencies
WILDCARD=$(grep -rn '"*"' Cargo.toml crates/*/Cargo.toml 2>/dev/null | grep -v '#' || true)
if [[ -n "$WILDCARD" ]]; then
    fail "Wildcard (*) dependency versions found"
    if $VERBOSE; then echo "$WILDCARD"; fi
else
    ok "No wildcard dependency versions"
fi

# Check Cargo.lock exists (for reproducible builds)
if [[ -f Cargo.lock ]]; then
    ok "Cargo.lock present (reproducible builds)"
else
    fail "Cargo.lock missing"
fi

# Check for git dependencies (should use crates.io for releases)
GIT_DEPS=$(grep -c 'git\s*=' Cargo.toml crates/*/Cargo.toml 2>/dev/null || echo 0)
if [[ $GIT_DEPS -gt 0 ]]; then
    warn "$GIT_DEPS git dependencies found (prefer crates.io for releases)"
else
    ok "No git dependencies (all from crates.io)"
fi

# ── Summary ──────────────────────────────────────────────────────────────────

echo ""
echo "════════════════════════════════════════════"
echo "  Security Audit Summary"
echo "════════════════════════════════════════════"
if [[ $ISSUES -eq 0 ]]; then
    echo -e "  ${GREEN}✓ All checks passed — 0 issues${NC}"
    exit 0
else
    echo -e "  ${YELLOW}⚠ $ISSUES issue(s) found${NC}"
    echo "  Run with --verbose for details"
    echo "  Run with --fix to auto-fix where possible"
    exit 1
fi
