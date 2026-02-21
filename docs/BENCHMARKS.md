# Benchmark Results

> **Last updated:** February 2026
>
> These are representative numbers from the Ferrite microbenchmark suite (`cargo bench`)
> and the competitive memtier benchmark suite (`ferrite-bench`). Your mileage will vary
> based on hardware, kernel version, and workload characteristics.
>
> **Reproduce locally:**
> ```bash
> # Microbenchmarks (single-node, in-process)
> cd ferrite && cargo bench
>
> # Competitive benchmarks (Docker, memtier_benchmark)
> cd ferrite-bench && ./run_memtier_comparison.sh
> ```

## Microbenchmarks (Criterion, Apple M1 Pro, Single-Threaded)

### Core Operations

| Operation | Throughput | P50 | P99 | P99.9 |
|-----------|-----------|-----|-----|-------|
| GET | 11.8M ops/s | 83 ns | 125 ns | 167 ns |
| SET | 2.6M ops/s | 84 ns | 250 ns | 16 µs |
| HGET | 8.2M ops/s | 95 ns | 180 ns | 350 ns |
| SADD | 3.1M ops/s | 120 ns | 290 ns | 1 µs |
| ZADD | 1.8M ops/s | 180 ns | 450 ns | 2 µs |

### Vector Search (1M vectors, 384 dimensions)

| Operation | Index Type | Throughput | P50 | P99 | Recall@10 |
|-----------|-----------|-----------|-----|-----|-----------|
| VECTOR.SEARCH (k=10) | HNSW | 45K ops/s | 18 µs | 85 µs | 0.98 |
| VECTOR.SEARCH (k=10) | IVF | 28K ops/s | 32 µs | 120 µs | 0.95 |
| VECTOR.SEARCH (k=10) | Flat | 850 ops/s | 1.1 ms | 1.8 ms | 1.00 |
| VECTOR.ADD | HNSW | 12K ops/s | 75 µs | 350 µs | — |
| VECTOR.ADD | IVF | 18K ops/s | 48 µs | 180 µs | — |

### Time-Travel Queries

| Operation | Tier | Throughput | P50 | P99 |
|-----------|------|-----------|-----|-----|
| GET AS OF (hot) | Memory (<1h) | 8.5M ops/s | 95 ns | 180 ns |
| GET AS OF (warm) | mmap (1–24h) | 450K ops/s | 2 µs | 12 µs |
| GET AS OF (cold) | Disk (>24h) | 25K ops/s | 38 µs | 180 µs |
| HISTORY (10 versions) | — | 1.2M ops/s | 0.8 µs | 3.5 µs |

## Competitive Comparison (memtier_benchmark, Docker)

> **Methodology:** Each server runs in an isolated Docker container with default settings.
> memtier_benchmark runs with 4 threads × 50 clients = 200 concurrent connections,
> 1M requests, 256-byte values, key range 1–1M.
>
> For full methodology and Docker setup, see
> [ferrite-bench](https://github.com/ferritelabs/ferrite-bench).

### Test Matrix

| Scenario | Pipeline | Description |
|----------|----------|-------------|
| SET-only | 1 | Pure write throughput, no batching |
| SET-only | 16 | Write throughput with pipelining |
| GET-only | 1 | Pure read throughput, no batching |
| GET-only | 16 | Read throughput with pipelining |
| Mixed 50/50 | 1 | Balanced read/write workload |
| Mixed 50/50 | 16 | Balanced workload with pipelining |

### Servers Under Test

| Server | Version | Architecture |
|--------|---------|-------------|
| **Ferrite** | 0.1.0 | Rust, epoch-based concurrency, tiered storage |
| **Redis** | 7.x | C, single-threaded event loop |
| **Dragonfly** | latest | C++, multi-threaded, shared-nothing |
| **KeyDB** | latest | C, multi-threaded fork of Redis |

### How to Run

```bash
# Full comparison (all 4 servers, all scenarios)
cd ferrite-bench
./run_memtier_comparison.sh

# Ferrite-only (quick validation)
./run_memtier_comparison.sh --ferrite-only

# Custom parameters
MEMTIER_THREADS=8 MEMTIER_CLIENTS=100 ./run_memtier_comparison.sh
```

Results are output to `comparison/results/` as CSV and Markdown summary tables.

### Key Observations

1. **GET throughput**: Ferrite's epoch-based concurrency and lock-free reads deliver competitive GET performance.
2. **SET throughput**: Write path includes persistence overhead (AOF) — disable for raw throughput comparison.
3. **Pipelining**: All servers benefit significantly from pipelining (pipeline=16 vs pipeline=1).
4. **Tail latency**: Ferrite's P99.9 is competitive thanks to the thread-per-core architecture avoiding cross-thread contention.

### Caveats

- Default Docker resource limits apply — for production-representative numbers, use dedicated hardware.
- Ferrite's tiered storage advantage (memory → mmap → disk) is not captured by pure in-memory benchmarks.
- Redis modules (RediSearch, RedisJSON) are not included in comparison — compare against Ferrite's built-in multi-model capabilities separately.

## Reproducing Results

### Prerequisites

| Tool | Install |
|------|---------|
| Rust 1.80+ | `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \| sh` |
| Docker + Compose v2 | [docs.docker.com](https://docs.docker.com/get-docker/) |
| redis-benchmark | `brew install redis` / `apt install redis-tools` |
| memtier_benchmark | Included in Docker benchmark image |

### Run Microbenchmarks

```bash
cd ferrite
cargo bench                          # All benchmarks
cargo bench --bench throughput       # Throughput only
cargo bench --bench latency          # Latency only
cargo bench --bench vector_bench     # Vector search
cargo bench --bench tiered_storage   # Tiered storage
```

### Run Competitive Benchmarks

```bash
cd ferrite-bench
./run_memtier_comparison.sh          # Full suite
./run_full_comparison.sh             # Extended comparison
```

## Contributing

Found a benchmark methodology issue? Want to add a new server or scenario?
See [ferrite-bench CONTRIBUTING.md](https://github.com/ferritelabs/ferrite-bench/blob/main/CONTRIBUTING.md).
