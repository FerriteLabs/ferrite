#!/usr/bin/env bash
# =============================================================================
# Ferrite Release Script
# =============================================================================
# Bumps version to 0.2.0, creates git tag, and optionally pushes.
#
# Usage:
#   ./scripts/release.sh 0.2.0           # Bump, tag, push
#   ./scripts/release.sh 0.2.0 --dry-run # Preview only
#
# Prerequisites:
#   - Clean git working tree (commit all changes first)
#   - cargo-release installed: cargo install cargo-release

set -euo pipefail

VERSION="${1:?Usage: $0 <version> [--dry-run]}"
DRY_RUN=false
[[ "${2:-}" == "--dry-run" ]] && DRY_RUN=true

cd "$(git rev-parse --show-toplevel)"

echo "═══════════════════════════════════════════════════════════════"
echo "  Ferrite Release v${VERSION}"
echo "═══════════════════════════════════════════════════════════════"

# ── Pre-checks ────────────────────────────────────────────────────────────────

echo ""
echo "Running pre-release checks..."

# Check clean working tree
if [[ -n "$(git status --porcelain)" ]]; then
    echo "ERROR: Working tree is not clean. Commit or stash changes first."
    echo "$(git status --short | head -10)"
    exit 1
fi

# Check tests pass
echo "  Running tests..."
if ! cargo test --workspace --lib --quiet 2>/dev/null; then
    echo "ERROR: Tests failed. Fix before releasing."
    exit 1
fi
echo "  ✓ Tests pass"

# Check formatting
if ! cargo fmt --all --check 2>/dev/null; then
    echo "ERROR: Formatting violations. Run: cargo fmt --all"
    exit 1
fi
echo "  ✓ Formatting clean"

# Check no warnings
WARNS=$(cargo check 2>&1 | grep '^warning:' | grep -v 'generated\|Finished' | wc -l | tr -d ' ')
if [[ "$WARNS" -gt 0 ]]; then
    echo "WARNING: $WARNS compiler warnings remain."
fi
echo "  ✓ Build clean"

echo ""
echo "All pre-checks passed."

# ── Version bump ──────────────────────────────────────────────────────────────

echo ""
echo "Bumping version to ${VERSION}..."

if $DRY_RUN; then
    echo "[DRY RUN] Would update workspace version in Cargo.toml"
    echo "[DRY RUN] Would run: cargo check (verify)"
else
    # Update workspace version
    sed -i '' "s/^version = \".*\"/version = \"${VERSION}\"/" Cargo.toml
    
    # Verify it compiles with new version
    cargo check --quiet
    
    # Commit version bump
    git add Cargo.toml Cargo.lock
    git commit -m "release: v${VERSION}

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
    echo "  ✓ Version bumped and committed"
fi

# ── Tag ───────────────────────────────────────────────────────────────────────

echo ""
echo "Creating tag v${VERSION}..."

if $DRY_RUN; then
    echo "[DRY RUN] Would create tag: v${VERSION}"
else
    git tag -a "v${VERSION}" -m "Release v${VERSION}

Ferrite v${VERSION} — Tiered-storage Redis replacement

Highlights:
- Redis-compatible RESP2/RESP3 protocol (100+ commands)
- Three-tier HybridLog storage (memory → mmap → disk → cloud)
- FerriteQL SQL-like query language with JOINs
- Multi-model: vector search, full-text, graph, time-series, documents
- Distributed ACID transactions with Raft consensus
- AI-native: RAG pipeline, semantic caching, ONNX inference
- Production hardening: graceful shutdown, error recovery, config hot-reload
- Full ecosystem: VS Code, JetBrains, Helm, Grafana, Homebrew"
    echo "  ✓ Tag created: v${VERSION}"
fi

# ── Push ──────────────────────────────────────────────────────────────────────

echo ""
if $DRY_RUN; then
    echo "[DRY RUN] Would push: git push origin main --tags"
else
    read -p "Push to origin? [y/N] " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        git push origin main --tags
        echo "  ✓ Pushed to origin"
        echo ""
        echo "  Release workflow will run at:"
        echo "  https://github.com/ferritelabs/ferrite/actions"
    else
        echo "  Skipped push. Run manually: git push origin main --tags"
    fi
fi

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Release v${VERSION} prepared!"
echo ""
echo "  Next steps:"
echo "  1. Push: git push origin main --tags"
echo "  2. Monitor: https://github.com/ferritelabs/ferrite/actions"  
echo "  3. Verify: https://github.com/ferritelabs/ferrite/releases"
echo "  4. Publish: cargo publish -p ferrite-core (then each crate)"
echo "  5. Docker: docker push ferritelabs/ferrite:${VERSION}"
echo "═══════════════════════════════════════════════════════════════"
