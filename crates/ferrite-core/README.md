# Ferrite Core

Core database engine — storage, protocol, commands, clustering, and persistence. This is the foundation that all other Ferrite crates build upon.

Part of the [Ferrite](https://github.com/ferritelabs/ferrite) workspace.

## Modules

- `storage` — DashMap in-memory backend + HybridLog tiered storage
- `persistence` — AOF, RDB snapshots, checkpoint/recovery
- `protocol` — RESP2/RESP3 parser and encoder
- `auth` — ACL, password hashing, user management
- `cluster` — Raft consensus, gossip protocol, hash slots
- `query` — FerriteQL query engine and optimizer
- `io` — I/O abstractions and buffer management
- `metrics` — Prometheus metrics exposition
- `runtime` — Tokio runtime configuration
- `config` — TOML configuration parsing
- `embedded` — Library mode (use Ferrite without a server)
- And more: network, tiering, observability, compatibility, transaction, temporal, triggers, audit, grpc

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
ferrite-core = { git = "https://github.com/ferritelabs/ferrite" }
```

## License

Apache-2.0 — see [LICENSE](../../LICENSE) for details.
