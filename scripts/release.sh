#!/usr/bin/env bash
# =============================================================================
# Ferrite Release Script
# =============================================================================
# Updates release metadata, creates the release tag, and optionally pushes.
#
# Usage:
#   ./scripts/release.sh 0.5.0           # Bump, tag, push
#   ./scripts/release.sh 0.5.0 --dry-run # Preview only
#
# Prerequisites:
#   - Clean git working tree (commit all changes first)
#   - cargo-release installed: cargo install cargo-release
#   - Successful publish.yml dry-run for the release commit, as required by RELEASE_CHECKLIST.md

set -euo pipefail

VERSION="${1:?Usage: $0 <version> [--dry-run]}"
DRY_RUN=false
[[ "${2:-}" == "--dry-run" ]] && DRY_RUN=true

cd "$(git rev-parse --show-toplevel)"
CURRENT_VERSION="$(awk -F'"' '/^version = / {print $2; exit}' Cargo.toml)"

if [[ ! "$VERSION" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z]+([.-][0-9A-Za-z]+)*)?$ ]]; then
    echo "ERROR: Version must be strict SemVer without a leading v: ${VERSION}"
    exit 1
fi

echo "═══════════════════════════════════════════════════════════════"
echo "  Ferrite Release v${VERSION}"
echo "═══════════════════════════════════════════════════════════════"

# ── Pre-checks ────────────────────────────────────────────────────────────────

echo ""
echo "Running pre-release checks..."

# Tags and release commits must be created from main. Dry-runs remain available
# on release branches for pre-merge validation.
if ! $DRY_RUN && [[ "$(git branch --show-current)" != "main" ]]; then
    echo "ERROR: Non-dry-run releases must run from main."
    exit 1
fi

# Check clean working tree
if [[ -n "$(git status --porcelain)" ]]; then
    echo "ERROR: Working tree is not clean. Commit or stash changes first."
    git status --short | head -10
    exit 1
fi

# Check tag collisions locally and remotely
if git rev-parse --verify --quiet "refs/tags/v${VERSION}" >/dev/null; then
    echo "ERROR: Local tag v${VERSION} already exists."
    exit 1
fi
remote_tag_status=0
git ls-remote --exit-code --tags origin "refs/tags/v${VERSION}" >/dev/null 2>&1 ||
    remote_tag_status=$?
case "$remote_tag_status" in
    0)
        echo "ERROR: Remote tag v${VERSION} already exists."
        exit 1
        ;;
    2) ;;
    *)
        echo "ERROR: Could not verify whether remote tag v${VERSION} exists."
        exit 1
        ;;
esac

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

# Check build and report warnings without letting a zero-match grep abort the script
if ! CHECK_OUTPUT=$(cargo check --workspace 2>&1); then
    printf '%s\n' "$CHECK_OUTPUT"
    echo "ERROR: cargo check failed."
    exit 1
fi
WARNS=$(printf '%s\n' "$CHECK_OUTPUT" | grep -c '^warning:' || true)
if [[ "$WARNS" -gt 0 ]]; then
    echo "WARNING: $WARNS compiler warnings remain."
else
    echo "  ✓ Build clean"
fi

echo ""
echo "All pre-checks passed."

# ── Version bump ──────────────────────────────────────────────────────────────

echo ""
echo "Bumping version to ${VERSION}..."

if [[ "$CURRENT_VERSION" == "$VERSION" ]]; then
    python3 scripts/check_release_metadata.py "$VERSION"
    echo "  ✓ Workspace metadata already uses ${VERSION}"
else
    if ! cargo release --version >/dev/null 2>&1; then
        echo "ERROR: cargo-release is required to change versions. Install it with: cargo install cargo-release"
        exit 1
    fi

    if $DRY_RUN; then
        cargo release version "$VERSION" --workspace --no-confirm
        echo "[DRY RUN] cargo-release validated the synchronized workspace version update."
        echo "[DRY RUN] Would refresh Cargo.lock with Cargo metadata and validate all Ferrite package/dependency versions."
    else
        cargo release version "$VERSION" --workspace --execute --no-confirm
        cargo metadata --format-version 1 >/dev/null
        python3 scripts/check_release_metadata.py "$VERSION"

        git add Cargo.toml Cargo.lock crates/*/Cargo.toml
        git commit -m "release: v${VERSION}"
        echo "  ✓ Version bumped and committed"
    fi
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
echo "  3. Require the tag release's Crates publication preflight and artifact jobs to pass"
echo "  4. Re-run the protected dry-run if needed: gh workflow run publish.yml --ref \"v${VERSION}\" -f dry_run=true -f expected_version=\"${VERSION}\""
echo "  5. Publish explicitly through the protected workflow: gh workflow run publish.yml --ref \"v${VERSION}\" -f dry_run=false -f expected_version=\"${VERSION}\""
echo "═══════════════════════════════════════════════════════════════"
