#!/usr/bin/env bash
# =============================================================================
# Deploy Ferrite Web Playground
# =============================================================================
# Builds and deploys play.ferrite.dev
#
# Usage:
#   ./scripts/deploy-playground.sh              # Build and run locally
#   ./scripts/deploy-playground.sh --push       # Build and push to registry
#   ./scripts/deploy-playground.sh --cloud      # Deploy to Fly.io

set -euo pipefail

ACTION="${1:-local}"
IMAGE="ferritelabs/playground:latest"
FERRITE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OPS_DIR="$(cd "$FERRITE_DIR/../ferrite-ops" && pwd)"

echo "═══════════════════════════════════════════════════════════════"
echo "  Ferrite Playground Deployment"
echo "═══════════════════════════════════════════════════════════════"

case "$ACTION" in
    --local|local)
        echo "Building playground Docker image..."
        docker build -t "$IMAGE" \
            -f "$OPS_DIR/Dockerfile.playground" \
            "$FERRITE_DIR"
        echo ""
        echo "Starting playground..."
        docker run -d --name ferrite-playground \
            -p 8080:8080 -p 6379:6379 \
            "$IMAGE"
        echo ""
        echo "✓ Playground running!"
        echo "  Web UI:  http://localhost:8080"
        echo "  RESP:    redis-cli -p 6379"
        echo "  Stop:    docker stop ferrite-playground && docker rm ferrite-playground"
        ;;

    --push)
        echo "Building and pushing playground image..."
        docker buildx build --platform linux/amd64,linux/arm64 \
            -t "$IMAGE" \
            -f "$OPS_DIR/Dockerfile.playground" \
            --push \
            "$FERRITE_DIR"
        echo "✓ Pushed $IMAGE"
        ;;

    --cloud)
        echo "Deploying to Fly.io..."
        if ! command -v fly &>/dev/null; then
            echo "Install Fly CLI: curl -L https://fly.io/install.sh | sh"
            exit 1
        fi

        # Create fly.toml if it doesn't exist
        if [[ ! -f "$OPS_DIR/fly.playground.toml" ]]; then
            cat > "$OPS_DIR/fly.playground.toml" << 'EOF'
app = "ferrite-playground"
primary_region = "iad"

[build]
  dockerfile = "Dockerfile.playground"

[http_service]
  internal_port = 8080
  force_https = true
  auto_stop_machines = true
  auto_start_machines = true
  min_machines_running = 1

[[services]]
  internal_port = 6379
  protocol = "tcp"
  [[services.ports]]
    port = 6379

[vm]
  cpu_kind = "shared"
  cpus = 1
  memory_mb = 512
EOF
            echo "Created fly.playground.toml"
        fi

        cd "$OPS_DIR"
        fly deploy --config fly.playground.toml --dockerfile Dockerfile.playground
        echo ""
        echo "✓ Deployed to Fly.io!"
        echo "  URL: https://ferrite-playground.fly.dev"
        ;;

    *)
        echo "Usage: $0 [--local|--push|--cloud]"
        exit 1
        ;;
esac
