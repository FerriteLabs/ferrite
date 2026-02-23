# Ferrite Core

Core database engine — storage, protocol, commands, clustering, and persistence. This is the foundation that all other Ferrite crates build upon.

Part of the [Ferrite](https://github.com/ferritelabs/ferrite) workspace.

## Features

- **Three-tier HybridLog storage** — Hot (mutable memory), Warm (read-only mmap), and Cold (disk via io_uring) regions with epoch-based memory reclamation
- **RESP2/RESP3 protocol** — Full Redis Serialization Protocol support with client-side caching and HELLO negotiation
- **Persistence** — Append-Only File (AOF) with replay, RDB snapshots, checkpoint/recovery, and cloud-capable backup
- **Access control** — Redis-compatible ACL system with user management, command permissions, and key-pattern authorization
- **FerriteQL query engine** — SQL-like query language with JOINs, aggregations, prepared statements, and materialized views
- **Cluster coordination** — Raft consensus, gossip protocol, and hash-slot routing for horizontal scaling
- **Embedded mode** — Use Ferrite as a library without a server, with optional FFI bindings for C/Python
- **Observability** — Prometheus metrics exposition, OpenTelemetry integration, and audit logging

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
ferrite-core = { git = "https://github.com/ferritelabs/ferrite" }
```

### Creating a store and working with data

```rust,ignore
use ferrite_core::storage::Store;

#[tokio::main]
async fn main() {
    // Create an in-memory store (database index 0)
    let store = Store::new();

    // Set a key-value pair
    store.set("user:1:name", "Alice".into());
    store.set("user:1:email", "alice@example.com".into());

    // Retrieve a value
    if let Some(value) = store.get("user:1:name") {
        println!("Name: {value}");
    }

    // Use the embedded database builder for persistent storage
    use ferrite_core::embedded::{Database, DatabaseBuilder};

    let db = DatabaseBuilder::new()
        .path("/tmp/ferrite-data")
        .build()
        .expect("failed to open database");

    let tx = db.begin_transaction();
    tx.set("counter", "42");
    tx.commit().expect("commit failed");
}
```

## Module Overview

| Module | Description |
|--------|-------------|
| `storage` | `Store`, `Database`, `HybridLog`, `Value`, `Entry` — DashMap backend with three-tier tiered storage and stream support |
| `protocol` | `Frame`, `parse_frame()`, `encode_frame()` — RESP2/RESP3 parser, encoder, and client negotiation |
| `persistence` | `AofManager`, `RdbReader`, `RdbWriter`, `BackupManager`, `CheckpointManager` — AOF replay, RDB snapshots, and backup/recovery |
| `auth` | `User`, `Acl`, `SharedAcl`, `CommandPermission`, `KeyPattern` — Redis-compatible ACL authorization |
| `cluster` | Raft consensus, gossip protocol, hash-slot routing for distributed deployments |
| `query` | `QueryEngine`, `QueryParser`, `QueryPlanner`, `ResultSet` — FerriteQL with JOINs, aggregations, and materialized views |
| `config` | `Config` — TOML-based configuration parsing and validation |
| `embedded` | `Database`, `DatabaseBuilder`, `Transaction` — Library mode with FFI bindings and edge/IoT support |
| `io` | I/O abstractions and buffer management |
| `metrics` | Prometheus metrics exposition |
| `runtime` | Tokio runtime configuration and tuning |
| `network` | Networking layer and connection handling |
| `tiering` | Data tiering policies and migration between storage tiers |
| `transaction` | Multi-key transaction support |
| `observability` | OpenTelemetry integration and distributed tracing |
| `temporal` | Time-series and temporal data operations |
| `triggers` | Event-driven trigger system |
| `audit` | Audit logging for compliance and security monitoring |
| `grpc` | gRPC service definitions and transport |
| `compatibility` | Redis command compatibility layer |
| `telemetry` | Telemetry collection and reporting |
| `crypto` | Cryptographic utilities |

## License

Apache-2.0 — see [LICENSE](../../LICENSE) for details.
