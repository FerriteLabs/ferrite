#!/usr/bin/env bash
# =============================================================================
# Ferrite Atomic Commit Script
# =============================================================================
# Commits the accumulated changes in logical, reviewable atomic chunks.
# Each commit follows Conventional Commits format.
#
# Usage: ./scripts/atomic-commit.sh [--dry-run]
#
# Run with --dry-run first to preview what will be committed.

set -euo pipefail

DRY_RUN=false
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=true

commit() {
    local msg="$1"
    shift
    if $DRY_RUN; then
        echo "[DRY RUN] Would commit: $msg"
        echo "  Files: $*"
        echo ""
    else
        git add "$@"
        git commit -m "$msg

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
        echo "✓ $msg"
    fi
}

cd "$(git rev-parse --show-toplevel)"

echo "═══════════════════════════════════════════════════════════════"
echo "  Ferrite Atomic Commit Script"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# ── Commit 1: Project quality files ──────────────────────────────────────────
commit "docs: add CODE_OF_CONDUCT, REFACTORING guide, and RELEASE_CHECKLIST" \
    CODE_OF_CONDUCT.md REFACTORING.md RELEASE_CHECKLIST.md

# ── Commit 2: Community & contributor onboarding ─────────────────────────────
commit "docs: add first-contributor guide with 20 good-first-issues" \
    FIRST_CONTRIBUTORS.md \
    .github/ISSUE_TEMPLATE/good_first_issue.md

# ── Commit 3: CI/CD workflows ───────────────────────────────────────────────
commit "ci: add release pipeline, compat dashboard, and security audit" \
    .github/workflows/release-full.yml \
    .github/workflows/compat-dashboard.yml \
    scripts/security-audit.sh \
    scripts/tcl-compat.sh

# ── Commit 4: README & crate docs improvements ──────────────────────────────
commit "docs: add architecture diagram to README, enhance crate READMEs" \
    README.md \
    crates/ferrite-core/README.md \
    crates/ferrite-ai/README.md \
    crates/ferrite-search/README.md \
    crates/ferrite-plugins/README.md

# ── Commit 5: Gitignore update ──────────────────────────────────────────────
commit "chore: update .gitignore to prevent binary artifacts" \
    .gitignore

# ── Commit 6: Round 1 — FerriteQL enhancements ──────────────────────────────
commit "feat(query): add real-time subscriptions and cross-model data sources" \
    crates/ferrite-core/src/query/subscription.rs \
    crates/ferrite-core/src/query/datasource.rs

# ── Commit 7: Round 1 — Tiering & transactions ──────────────────────────────
commit "feat(core): add tiering advisor, distributed lock manager" \
    crates/ferrite-core/src/tiering/advisor.rs \
    crates/ferrite-core/src/transaction/distributed.rs

# ── Commit 8: Round 1 — Triggers, observability, embedded ───────────────────
commit "feat(core): add FerriteFunctions runtime, eBPF probes, WASM adapter, mobile SDK" \
    crates/ferrite-core/src/triggers/runtime.rs \
    crates/ferrite-core/src/observability/ebpf.rs \
    crates/ferrite-core/src/embedded/wasm_adapter.rs \
    crates/ferrite-core/src/embedded/mobile_sdk.rs

# ── Commit 9: Round 1 — Extension crate features ────────────────────────────
commit "feat: add RAG source sync, key browser, cluster viz, K8s backup/scaling" \
    crates/ferrite-ai/src/rag/source_sync.rs \
    crates/ferrite-studio/src/studio/key_browser.rs \
    crates/ferrite-studio/src/studio/cluster_viz.rs \
    crates/ferrite-k8s/src/k8s/predictive_scaling.rs \
    crates/ferrite-k8s/src/k8s/backup.rs

# ── Commit 10: Round 1 — Migration tool ─────────────────────────────────────
commit "feat(migration): add Redis RDB file parser for snapshot import" \
    src/migration/rdb_parser.rs

# ── Commit 11: Round 2 — Cluster protocol & Raft ────────────────────────────
commit "feat(cluster): add cluster bus protocol and Raft log replication" \
    crates/ferrite-core/src/cluster/cluster_bus.rs \
    crates/ferrite-core/src/cluster/raft_log.rs

# ── Commit 12: Round 2 — Infrastructure features ────────────────────────────
commit "feat: add TCL compat tracker, cache warming, plugin SDK, CRDT geo-replication" \
    crates/ferrite-core/src/compatibility/tcl_compat.rs \
    crates/ferrite-core/src/tiering/warming.rs \
    crates/ferrite-plugins/src/sdk.rs \
    crates/ferrite-plugins/src/crdt/geo_replication.rs

# ── Commit 13: Round 2 — Gateway, serverless, compliance ────────────────────
commit "feat: add GraphQL/REST gateway, serverless connection pool, compliance framework" \
    crates/ferrite-studio/src/studio/gateway.rs \
    crates/ferrite-cloud/src/serverless/connection_pool.rs \
    crates/ferrite-enterprise/src/governance/compliance_framework.rs

# ── Commit 14: Round 3 — Platform features ──────────────────────────────────
commit "feat: add type-safe SDK gen, web playground, streaming views, schema registry" \
    src/sdk/typesafe.rs \
    crates/ferrite-studio/src/playground/web_server.rs \
    crates/ferrite-core/src/query/streaming_views.rs \
    crates/ferrite-core/src/query/schema_registry.rs

# ── Commit 15: Round 3 — Dashboard, lifecycle, federation ───────────────────
commit "feat: add observability dashboard, lifecycle manager, federated executor, query advisor" \
    crates/ferrite-studio/src/studio/dashboard.rs \
    crates/ferrite-core/src/tiering/lifecycle.rs \
    crates/ferrite-core/src/query/federated_executor.rs \
    crates/ferrite-core/src/query/advisor.rs

# ── Commit 16: Round 3 — Chaos & CDC connectors ─────────────────────────────
commit "feat: add chaos engineering toolkit and CDC connectors" \
    crates/ferrite-core/src/observability/chaos_engine.rs \
    crates/ferrite-streaming/src/cdc/connectors.rs

# ── Commit 17: Round 4 — Production hardening ───────────────────────────────
commit "feat(runtime): add metrics pipeline, graceful shutdown, error recovery, config hot-reload" \
    crates/ferrite-core/src/metrics/instrumentation.rs \
    crates/ferrite-core/src/runtime/shutdown.rs \
    crates/ferrite-core/src/runtime/error_recovery.rs \
    crates/ferrite-core/src/runtime/config_manager.rs

# ── Commit 18: Round 4 — Memory, diagnostics, client protocol ───────────────
commit "feat(core): add memory manager, diagnostics engine, client protocol negotiation" \
    crates/ferrite-core/src/storage/memory_manager.rs \
    crates/ferrite-core/src/observability/diagnostics.rs \
    crates/ferrite-core/src/protocol/client_negotiation.rs

# ── Commit 19: Round 4 — Integration tests ──────────────────────────────────
commit "test: add end-to-end integration test harness with 24 test cases" \
    tests/integration_harness.rs

# ── Commit 20: Module wiring (all mod.rs changes) ───────────────────────────
MOD_FILES=$(git diff --name-only | grep -E 'mod\.rs$|lib\.rs$' || true)
if [[ -n "$MOD_FILES" ]]; then
    # shellcheck disable=SC2086
    commit "refactor: wire all new modules into crate module trees" $MOD_FILES
fi

# ── Commit 21: Cargo fix cleanups (narrowed allows, removed unused) ─────────
REMAINING=$(git diff --name-only || true)
if [[ -n "$REMAINING" ]]; then
    # shellcheck disable=SC2086
    commit "refactor: narrow blanket #![allow] directives, fix unused imports, add missing docs" $REMAINING
fi

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Done! Created up to 21 atomic commits."
echo "═══════════════════════════════════════════════════════════════"
