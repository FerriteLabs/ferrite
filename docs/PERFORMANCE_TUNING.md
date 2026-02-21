# Performance Tuning Guide

## System-Level Tuning

### Linux Kernel Parameters

```bash
# Increase max open files
echo "fs.file-max = 100000" >> /etc/sysctl.conf

# Increase TCP backlog
echo "net.core.somaxconn = 65535" >> /etc/sysctl.conf

# Disable THP (Transparent Huge Pages) — recommended for latency-sensitive workloads
echo never > /sys/kernel/mm/transparent_hugepage/enabled

# Apply changes
sysctl -p
```

### io_uring Setup (Linux 5.11+)

Build with io_uring support for optimal disk I/O:

```bash
cargo build --release --features io-uring
```

## Ferrite Configuration Tuning

### Memory-Optimized (High Throughput)

```toml
[storage]
max_memory = 8589934592  # 8GB
databases = 1            # Single DB reduces overhead

[persistence]
aof_enabled = true
aof_sync = "everysec"    # Balance durability vs throughput

[server]
max_connections = 10000
timeout = 300
```

### Latency-Optimized (P99 < 1ms)

```toml
[storage]
max_memory = 4294967296  # 4GB — keep working set in memory

[persistence]
aof_enabled = false      # Disable for lowest latency
checkpoint_interval = 0  # Disable checkpoints

[server]
tcp_keepalive = 60
```

## Benchmarking

### Built-in Benchmarks

```bash
cargo bench --bench throughput    # Ops/sec measurement
cargo bench --bench latency       # Latency histogram
```

### External Tools

```bash
# redis-benchmark (works with Ferrite)
redis-benchmark -h 127.0.0.1 -p 6379 -t get,set -n 1000000 -c 50

# memtier_benchmark
memtier_benchmark -s 127.0.0.1 -p 6379 --ratio=1:1 -n 100000
```

## Performance Targets

| Metric | Target | How to Verify |
|--------|--------|---------------|
| GET throughput | >500K ops/sec/core | `cargo bench --bench throughput` |
| SET throughput | >400K ops/sec/core | `cargo bench --bench throughput` |
| P50 latency | <0.3ms | `cargo bench --bench latency` |
| P99 latency | <1ms | `cargo bench --bench latency` |
| P99.9 latency | <2ms | `cargo bench --bench latency` |
| Memory overhead | <20% vs data size | Monitor `ferrite_memory_bytes` |
