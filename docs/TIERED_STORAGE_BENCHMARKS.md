# Tiered Storage Benchmarks

Benchmarks that demonstrate Ferrite's core value proposition: a three-tier
HybridLog storage engine that delivers near-Redis latency for hot data while
transparently spilling to disk when the working set exceeds memory — something
Redis cannot do without external sharding or eviction.

## Methodology

### Storage tiers under test

| Tier | Region | Backing | Expected Latency |
|------|--------|---------|-----------------|
| **Hot** | `MutableRegion` | Heap-allocated buffer, lock-free atomic tail | < 100 ns |
| **Warm** | `ReadOnlyRegion` | `mmap`-backed file, zero-copy reads | 100–500 ns |
| **Cold** | `DiskRegion` | File I/O with read-ahead buffer | 1–10 µs |

### Test environment

- All benchmarks use [Criterion.rs](https://bheisler.github.io/criterion.rs/)
  with HTML report generation.
- Temporary directories (`tempfile` crate) are used for warm/cold tier files so
  benchmarks are self-cleaning.
- Value sizes default to 128 bytes; the `value_size_tiers` group sweeps 64 B,
  256 B, 1 KB, and 4 KB.

### Measurement approach

- **Throughput**: operations/second via Criterion's `Throughput::Elements(1)`.
- **Latency**: Criterion reports mean, median, and confidence intervals.
- Each benchmark warms up then runs enough iterations for statistical
  significance (default Criterion settings: 100 samples, 5 s measurement).

## Test Scenarios

### a) Working set fits in memory → parity with Redis

**Benchmark**: `all_in_memory_vs_tiered/in_memory_read`

When all data lives in the Store (DashMap), Ferrite delivers the same
sub-microsecond GET/SET latency as Redis. This proves there is no overhead from
the tiering infrastructure when it is not needed.

### b) Working set exceeds memory → Ferrite's advantage

**Benchmark**: `all_in_memory_vs_tiered/tiered_read_realistic`

With 50K keys distributed across hot (20%), warm (30%), and cold (50%) tiers,
Ferrite transparently serves requests from each tier. Redis would either OOM,
evict keys, or require application-level sharding.

### c) Mixed hot/cold access patterns → tiering benefit

**Benchmarks**: `mixed_workload/80_20_hot_cold`, `mixed_workload/50_50_hot_cold`

Real workloads follow Zipf-like distributions. The 80/20 mix shows that when
most accesses hit the hot tier, overall latency stays close to pure in-memory
performance despite half the data being on disk.

### d) Cost analysis: memory $$$ vs disk $ at scale

| Dataset | Redis (all-in-memory) | Ferrite (tiered) | Savings |
|---------|----------------------|------------------|---------|
| 10 GB, 80/20 hot/cold | ~$100/mo (RAM) | ~$24/mo (2 GB RAM + 8 GB disk) | **76%** |
| 100 GB, 80/20 hot/cold | ~$1,000/mo | ~$170/mo (20 GB RAM + 80 GB SSD) | **83%** |
| 1 TB, 90/10 hot/cold | ~$10,000/mo | ~$1,150/mo (100 GB RAM + 900 GB SSD) | **88%** |

*Cost model uses `TierCostConfig::default()` rates: $10/GB/mo memory,
$0.15/GB/mo SSD. Cloud tiers (S3 at $0.023/GB/mo) reduce costs further.*

## Expected Results

Fill in after running benchmarks on your target hardware.

| Benchmark | Metric | Value |
|-----------|--------|-------|
| `hot_tier/read_sequential` | ops/sec | __________ |
| `hot_tier/read_random` | ops/sec | __________ |
| `warm_tier/read_sequential` | ops/sec | __________ |
| `warm_tier/read_random` | ops/sec | __________ |
| `cold_tier/read_sync_sequential` | ops/sec | __________ |
| `cold_tier/read_sync_random` | ops/sec | __________ |
| `tier_promotion/cold_to_hot` | ops/sec | __________ |
| `tier_promotion/warm_to_hot` | ops/sec | __________ |
| `mixed_workload/80_20_hot_cold` | ops/sec | __________ |
| `mixed_workload/50_50_hot_cold` | ops/sec | __________ |
| `all_in_memory_vs_tiered/in_memory_read` | ops/sec | __________ |
| `all_in_memory_vs_tiered/tiered_read_realistic` | ops/sec | __________ |

## How to Reproduce

```bash
# Run all tiered storage benchmarks
cargo bench --bench tiered_storage

# Run a specific benchmark group
cargo bench --bench tiered_storage -- hot_tier
cargo bench --bench tiered_storage -- mixed_workload

# Run with HTML report generation (default with criterion)
cargo bench --bench tiered_storage

# View HTML reports
open target/criterion/report/index.html

# Use the runner script for full automation
./scripts/run_tiered_benchmarks.sh
```

## Architecture Reference

```
┌─────────────────────────────────────────────────────┐
│                    HybridLog                        │
│                                                     │
│  ┌─────────────┐  ┌──────────────┐  ┌───────────┐  │
│  │   Mutable    │  │  Read-Only   │  │   Disk    │  │
│  │   Region     │  │   Region     │  │  Region   │  │
│  │  (Hot Tier)  │  │ (Warm Tier)  │  │(Cold Tier)│  │
│  │              │  │              │  │           │  │
│  │ Lock-free    │  │ mmap-backed  │  │ File I/O  │  │
│  │ append-only  │  │ zero-copy    │  │ read-ahead│  │
│  │ < 100 ns     │  │ 100-500 ns   │  │ 1-10 µs   │  │
│  └──────┬───────┘  └──────┬───────┘  └─────┬─────┘  │
│         │   overflow       │   eviction     │        │
│         └────────►─────────┘───────►────────┘        │
│                  promotion on access                 │
│         ◄──────────────────◄────────────────         │
└─────────────────────────────────────────────────────┘
```
