#!/usr/bin/env bash
# Run all SDK integration tests against a live Ferrite server.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SDK_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
FERRITE_PORT="${FERRITE_PORT:-6399}"

echo "=== SDK Integration Tests ==="
echo "Ferrite: localhost:${FERRITE_PORT}"
echo ""

# Start server if not running
if ! redis-cli -p "$FERRITE_PORT" PING 2>/dev/null | grep -q PONG; then
  echo "Starting Ferrite via Docker..."
  docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d --wait
  trap 'docker compose -f "$SCRIPT_DIR/docker-compose.yml" down' EXIT
fi

PASS=0
FAIL=0
SKIP=0

run_sdk_test() {
  local name="$1" dir="$2" cmd="$3"
  echo "--- $name ---"
  if [[ ! -d "$dir" ]]; then
    echo "  SKIP: directory not found"
    ((SKIP++)) || true
    return
  fi
  if (cd "$dir" && eval "$cmd" 2>&1); then
    echo "  PASS"
    ((PASS++)) || true
  else
    echo "  FAIL"
    ((FAIL++)) || true
  fi
  # Flush between SDKs
  redis-cli -p "$FERRITE_PORT" FLUSHALL >/dev/null 2>&1 || true
  echo ""
}

# Rust SDK
run_sdk_test "Rust SDK (ferrite-rs)" "$SDK_DIR/ferrite-rs" \
  "FERRITE_TEST_PORT=$FERRITE_PORT cargo test 2>&1 | tail -5"

# Python SDK (ferrite-py)
run_sdk_test "Python SDK (ferrite-py)" "$SDK_DIR/ferrite-py" \
  "FERRITE_PORT=$FERRITE_PORT python -m pytest tests/ -q 2>&1 | tail -5"

# Python SDK (client + AI)
run_sdk_test "Python SDK (client)" "$SDK_DIR/python" \
  "FERRITE_PORT=$FERRITE_PORT python -m pytest tests/ -q 2>&1 | tail -5"

# Node.js SDK
run_sdk_test "Node.js SDK" "$SDK_DIR/nodejs" \
  "FERRITE_PORT=$FERRITE_PORT npm test 2>&1 | tail -10"

# TypeScript AI SDK
run_sdk_test "TypeScript AI SDK" "$SDK_DIR/typescript" \
  "FERRITE_PORT=$FERRITE_PORT npm test 2>&1 | tail -10"

# Go SDK
run_sdk_test "Go SDK" "$SDK_DIR/go" \
  "FERRITE_PORT=$FERRITE_PORT go test ./... 2>&1 | tail -5"

# Java SDK
run_sdk_test "Java SDK" "$SDK_DIR/java" \
  "FERRITE_PORT=$FERRITE_PORT mvn test -q 2>&1 | tail -5"

# .NET SDK
run_sdk_test ".NET SDK" "$SDK_DIR/dotnet" \
  "FERRITE_PORT=$FERRITE_PORT dotnet test --verbosity quiet 2>&1 | tail -5"

echo "==============================="
echo "Results: ${PASS} passed, ${FAIL} failed, ${SKIP} skipped"
echo "==============================="
exit "$FAIL"
