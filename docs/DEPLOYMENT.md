# Deployment Guide

## Quick Start

```bash
# Build and run (no config needed)
cargo build --release && ./target/release/ferrite

# Or use Docker
docker compose up -d
```

## Docker

### Docker Compose (Recommended)

```bash
# Clone the ops repo alongside ferrite
git clone https://github.com/ferritelabs/ferrite-ops.git

# Start Ferrite
docker compose -f ferrite-ops/docker-compose.yml up -d

# Start with monitoring (Prometheus + Grafana)
docker compose -f ferrite-ops/docker-compose.yml --profile monitoring up -d

# Start with Redis for comparison testing
docker compose -f ferrite-ops/docker-compose.yml --profile redis up -d
```

### Standalone Docker

```bash
# Build
docker build -t ferrite:latest -f ferrite-ops/Dockerfile .

# Run
docker run -d --name ferrite \
  -p 6379:6379 -p 9090:9090 \
  -e FERRITE_BIND=0.0.0.0 \
  -e FERRITE_METRICS_BIND=0.0.0.0 \
  -v ferrite-data:/var/lib/ferrite/data \
  ferrite:latest
```

## Kubernetes

### Helm Chart

```bash
# Install via Helm (from ferrite-ops)
helm install ferrite ferrite-ops/charts/ferrite

# With custom values
helm install ferrite ferrite-ops/charts/ferrite \
  --set persistence.enabled=true \
  --set persistence.size=10Gi \
  --set resources.requests.memory=2Gi
```

See [ferrite-ops/charts/ferrite/values.yaml](https://github.com/ferritelabs/ferrite-ops/tree/main/charts/ferrite) for all configuration options.

## Bare Metal

### Install from Source

```bash
cargo install --git https://github.com/ferritelabs/ferrite.git
```

### Install via Homebrew

```bash
brew tap ferritelabs/ferrite
brew install ferrite
brew services start ferrite
```

### Install via Script

```bash
curl -fsSL https://raw.githubusercontent.com/ferritelabs/ferrite/main/scripts/install.sh | bash
```

## Configuration

Generate a default configuration:

```bash
ferrite init --output ferrite.toml
```

See [EXAMPLE_CONFIGURATIONS.md](EXAMPLE_CONFIGURATIONS.md) for production-ready configs.

## Production Checklist

- [ ] Enable TLS for all connections
- [ ] Enable authentication (ACLs)
- [ ] Configure persistence (AOF + checkpoints)
- [ ] Set memory limits
- [ ] Configure monitoring (Prometheus metrics)
- [ ] Set up log rotation
- [ ] Configure firewall rules
- [ ] Enable audit logging
- [ ] Test backup and restore procedures
- [ ] Set up alerting rules

See [OPERATIONS.md](OPERATIONS.md) for detailed production guidance.
