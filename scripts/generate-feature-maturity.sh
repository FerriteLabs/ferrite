#!/usr/bin/env bash
# generate-feature-maturity.sh — Validate and report on feature maturity data.
#
# Usage:
#   ./scripts/generate-feature-maturity.sh           # Validate and summarize
#   ./scripts/generate-feature-maturity.sh --check    # CI mode: exit 1 if issues found
#
# Reads: docs/feature-maturity.toml
# This script requires Python 3.6+ (uses tomllib or toml package).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${SCRIPT_DIR}/.."
DATA_FILE="${REPO_ROOT}/docs/feature-maturity.toml"
MD_FILE="${REPO_ROOT}/docs/FEATURE_MATURITY.md"
CHECK_MODE="${1:-}"

if [ ! -f "$DATA_FILE" ]; then
    echo "ERROR: $DATA_FILE not found"
    exit 1
fi

python3 - "$DATA_FILE" "$MD_FILE" "$CHECK_MODE" << 'PYTHON_SCRIPT'
import sys
import os

data_file = sys.argv[1]
md_file = sys.argv[2]
check_mode = sys.argv[3] if len(sys.argv) > 3 else ""

# Parse TOML
try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib
    except ImportError:
        # Fallback: minimal TOML parser for simple [[features]] arrays
        print("WARNING: tomllib/tomli not available. Install with: pip install tomli")
        print("Falling back to basic parsing...")

        features = []
        current = {}
        with open(data_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line == '[[features]]':
                    if current:
                        features.append(current)
                    current = {}
                elif '=' in line and not line.startswith('#'):
                    key, _, value = line.partition('=')
                    key = key.strip()
                    value = value.strip().strip('"')
                    current[key] = value
            if current:
                features.append(current)

        data = {'features': features}
        tomllib = None

if tomllib:
    with open(data_file, 'rb') as f:
        data = tomllib.load(f)

features = data.get('features', [])
if not features:
    print("ERROR: No features found in data file")
    sys.exit(1)

# Classify
stable = [f for f in features if f.get('tier') == 'stable']
beta = [f for f in features if f.get('tier') == 'beta']
experimental = [f for f in features if f.get('tier') == 'experimental']

# Validate
errors = []
warnings = []
valid_tiers = {'stable', 'beta', 'experimental'}
valid_crates = {
    'ferrite', 'ferrite-core', 'ferrite-search', 'ferrite-ai', 'ferrite-graph',
    'ferrite-timeseries', 'ferrite-document', 'ferrite-streaming', 'ferrite-cloud',
    'ferrite-k8s', 'ferrite-enterprise', 'ferrite-plugins', 'ferrite-studio'
}

for f in features:
    name = f.get('name', '<unnamed>')
    if not f.get('name'):
        errors.append(f"Feature missing 'name' field")
    if not f.get('description'):
        errors.append(f"Feature '{name}' missing 'description'")
    if f.get('tier') not in valid_tiers:
        errors.append(f"Feature '{name}' has invalid tier: {f.get('tier')}")
    if not f.get('enable'):
        errors.append(f"Feature '{name}' missing 'enable' field")
    if f.get('crate') and f['crate'] not in valid_crates:
        warnings.append(f"Feature '{name}' references unknown crate: {f['crate']}")
    if f.get('tier') == 'stable' and not f.get('graduated'):
        warnings.append(f"Stable feature '{name}' missing 'graduated' version")

# Report
print("╔════════════════════════════════════════════════════════╗")
print("║        Feature Maturity Report                        ║")
print("╠════════════════════════════════════════════════════════╣")
print(f"║  ✅ Stable:       {len(stable):>3} features                       ║")
print(f"║  🧪 Beta:         {len(beta):>3} features                       ║")
print(f"║  🔬 Experimental: {len(experimental):>3} features                       ║")
print(f"║  ── Total:        {len(features):>3} features                       ║")
print("╚════════════════════════════════════════════════════════╝")
print()

# Crate distribution
crate_counts = {}
for f in features:
    c = f.get('crate', 'unknown')
    crate_counts[c] = crate_counts.get(c, 0) + 1

print("Features by crate:")
for crate, count in sorted(crate_counts.items(), key=lambda x: -x[1]):
    print(f"  {crate}: {count}")
print()

# Beta features targeting graduation
graduating = [f for f in beta if f.get('target_graduation')]
if graduating:
    print("Beta features targeting graduation:")
    for f in graduating:
        print(f"  → {f['name']} → v{f['target_graduation']}")
    print()

if errors:
    print(f"ERRORS ({len(errors)}):")
    for e in errors:
        print(f"  ❌ {e}")
    print()

if warnings:
    print(f"WARNINGS ({len(warnings)}):")
    for w in warnings:
        print(f"  ⚠️  {w}")
    print()

if check_mode == '--check':
    if errors:
        print("FAILED: Fix errors above before merging.")
        sys.exit(1)
    else:
        print("PASSED: Feature maturity data is valid.")
else:
    if not errors:
        print("✓ All features valid.")

PYTHON_SCRIPT
