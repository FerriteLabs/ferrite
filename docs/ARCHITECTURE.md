# Architecture

## Overview

Ferrite is a high-performance, tiered-storage key-value store designed as a drop-in Redis replacement. Built in Rust with epoch-based concurrency and io_uring-first persistence.

## Storage Engine: HybridLog

Inspired by Microsoft's [FASTER](https://www.microsoft.com/en-us/research/uploads/prod/2018/03/faster-sigmod18.pdf), Ferrite implements a three-tier log-structured storage engine:

```
┌─────────────────────────────────────────────────────────────┐
│                    Hot Tier (Memory)                         │
│  - In-place updates, lock-free reads via DashMap             │
│  - Epoch-based reclamation (crossbeam-epoch)                 │
│  - Automatic LRU-based eviction to warm tier                 │
├─────────────────────────────────────────────────────────────┤
│                   Warm Tier (mmap)                           │
│  - Memory-mapped files via memmap2                           │
│  - Zero-copy reads, copy-on-write for updates                │
│  - Automatic promotion on access                             │
├─────────────────────────────────────────────────────────────┤
│                   Cold Tier (Disk/Cloud)                     │
│  - Async I/O with io_uring (Linux) / tokio::fs (macOS)      │
│  - Optional cloud storage (S3, GCS, Azure via object_store)  │
│  - Compression (lz4, flate2) and encryption support          │
└─────────────────────────────────────────────────────────────┘
```

## Concurrency Model

### Thread-Per-Core Architecture
- Each CPU core has a dedicated thread with its own io_uring instance
- Shards are assigned to threads: `shard = hash(key) % num_shards`
- No cross-thread locks on the hot path

### Epoch-Based Reclamation (EBR)
- Threads "pin" an epoch when accessing shared data
- Memory is reclaimed only when all threads have moved past that epoch
- Implemented via `crossbeam-epoch`

## Workspace Structure

```
ferrite/
├── src/                    ← Top-level binary crate (integration layer)
│   ├── commands/           ← Redis command dispatch and handlers
│   ├── server/             ← TCP listener, TLS, connection handling
│   └── replication/        ← Primary-replica sync, geo-replication
├── crates/
│   ├── ferrite-core/       ← Foundation: storage, protocol, config, auth, cluster
│   ├── ferrite-search/     ← Full-text search, query routing
│   ├── ferrite-ai/         ← Vector search, semantic caching, RAG
│   ├── ferrite-graph/      ← Property graph model and traversal
│   ├── ferrite-timeseries/ ← Time-series ingestion and downsampling
│   ├── ferrite-document/   ← JSON document store
│   ├── ferrite-streaming/  ← Event streaming, CDC, pipelines
│   ├── ferrite-cloud/      ← Cloud storage tiering (S3/GCS/Azure)
│   ├── ferrite-k8s/        ← Kubernetes operator
│   ├── ferrite-enterprise/ ← Multi-tenancy, governance, federation
│   ├── ferrite-plugins/    ← Plugin system, WASM, CRDTs
│   └── ferrite-studio/     ← Web management UI
```

### Dependency DAG

```
ferrite-core                  ← Foundation (no external crate deps)
    ↑
ferrite-{search,ai,graph,...} ← Self-contained extensions (don't depend on core or each other)
    ↑
ferrite (top-level)           ← Integration layer (depends on all crates)
```

## Protocol

Full RESP2/RESP3 wire compatibility. See the [Redis Protocol Specification](https://redis.io/docs/reference/protocol-spec/).

## Persistence

- **AOF (Append-Only File)**: Write-ahead logging with configurable sync (`always`, `everysec`, `no`)
- **Checkpointing**: Fork-less snapshots for consistent backups
- **Point-in-Time Recovery**: Restore to any moment using AOF + checkpoints

## References

- [FASTER Paper (Microsoft Research)](https://www.microsoft.com/en-us/research/uploads/prod/2018/03/faster-sigmod18.pdf)
- [Garnet (Microsoft)](https://github.com/microsoft/garnet)
- [Redis Protocol Specification](https://redis.io/docs/reference/protocol-spec/)
- [io_uring Documentation](https://kernel.dk/io_uring.pdf)
- [Epoch-Based Reclamation](https://aturon.github.io/blog/2015/08/27/epoch/)
