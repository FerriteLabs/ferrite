# Operations Guide

## Monitoring

### Prometheus Metrics

Ferrite exposes metrics at `http://localhost:9090/metrics`:

```bash
curl http://localhost:9090/metrics
```

**Key Metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `ferrite_commands_total` | Counter | Total commands processed |
| `ferrite_connections_total` | Counter | Total connections accepted |
| `ferrite_memory_bytes` | Gauge | Memory usage by category |
| `ferrite_keys_total` | Gauge | Number of keys per database |
| `ferrite_latency_seconds` | Histogram | Command latency distribution |
| `ferrite_auth_failures_total` | Counter | Failed authentication attempts |
| `ferrite_persistence_last_save` | Gauge | Unix timestamp of last save |

### Grafana Dashboards

Import the included dashboard from `ferrite-ops/grafana/ferrite-dashboard.json`.

## Backup & Restore

### Create a Backup

```bash
redis-cli BGSAVE
# or
ferrite-cli BGSAVE
```

### Restore from Backup

```bash
./target/release/ferrite --restore /path/to/backup.ferrite.backup
```

### Scheduled Backups

Configure automatic checkpoints in `ferrite.toml`:

```toml
[persistence]
checkpoint_enabled = true
checkpoint_interval = 3600  # Every hour
```

## Scaling

### Vertical Scaling

Ferrite scales well vertically thanks to its thread-per-core architecture. Increase CPU cores for more throughput.

### Horizontal Scaling (Cluster Mode)

```bash
# Start 3 nodes
./ferrite --port 7000 --cluster-enabled yes
./ferrite --port 7001 --cluster-enabled yes
./ferrite --port 7002 --cluster-enabled yes

# Create cluster
redis-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002
```

## Log Management

Configure logging level:

```bash
RUST_LOG=ferrite=info ./ferrite     # Production
RUST_LOG=ferrite=debug ./ferrite    # Debugging
RUST_LOG=ferrite=trace ./ferrite    # Full tracing
```

## Health Checks

```bash
# CLI health check
ferrite-cli PING

# HTTP health (via metrics endpoint)
curl -f http://localhost:9090/metrics || exit 1

# Built-in diagnostics
ferrite doctor --config ferrite.toml
```
