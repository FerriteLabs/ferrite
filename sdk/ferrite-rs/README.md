# ferrite-rs

Official Rust client SDK for [Ferrite](https://github.com/ferritelabs/ferrite) — a high-performance, tiered-storage key-value store designed as a drop-in Redis replacement.

Also fully compatible with standard Redis servers.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
ferrite-rs = { path = "sdk/ferrite-rs" }
tokio = { version = "1", features = ["full"] }
```

For TLS support:

```toml
[dependencies]
ferrite-rs = { path = "sdk/ferrite-rs", features = ["tls"] }
```

## Quick Start

### Async (recommended)

```rust
use ferrite_rs::{AsyncClient, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let client = AsyncClient::connect("127.0.0.1", 6379).await?;

    // SET with builder pattern
    client.set("key", "value").ex(3600).nx().execute().await?;

    // GET
    let val = client.get("key").await?;
    println!("{}", val);

    Ok(())
}
```

### Blocking

```rust
use ferrite_rs::{Client, Result};

fn main() -> Result<()> {
    let client = Client::connect("127.0.0.1", 6379)?;

    client.set("key", "value")?;
    let val = client.get("key")?;
    println!("{}", val);

    Ok(())
}
```

## Features

| Feature | Default | Description |
|---------|---------|-------------|
| `tls` | No | TLS support via `tokio-rustls` |

## Connection Pooling

```rust
use ferrite_rs::{AsyncClient, ConnectionConfig, PoolConfig};

let config = PoolConfig {
    connection: ConnectionConfig {
        host: "127.0.0.1".into(),
        port: 6379,
        password: Some("secret".into()),
        database: 0,
        ..Default::default()
    },
    max_size: 16,
};

let client = AsyncClient::connect_pooled(config).await?;
```

## Supported Commands

### Strings
`SET` (with EX/PX/NX/XX/KEEPTTL/GET), `GET`, `DEL`, `EXISTS`, `INCR`, `INCRBY`, `DECR`, `DECRBY`, `MGET`, `MSET`, `APPEND`, `STRLEN`, `GETRANGE`, `SETNX`, `SETEX`, `TTL`, `EXPIRE`, `PERSIST`, `TYPE`

### Lists
`LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`, `LSET`, `LREM`, `LTRIM`

### Hashes
`HSET`, `HGET`, `HDEL`, `HGETALL`, `HMGET`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS`, `HINCRBY`, `HSETNX`

### Sets
`SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`, `SUNION`, `SINTER`, `SDIFF`, `SPOP`, `SRANDMEMBER`

### Sorted Sets
`ZADD`, `ZREM`, `ZRANGE`, `ZREVRANGE`, `ZSCORE`, `ZCARD`, `ZCOUNT`, `ZINCRBY`, `ZRANK`, `ZRANGEBYSCORE`

### Server
`PING`, `ECHO`, `INFO`, `DBSIZE`, `FLUSHDB`, `FLUSHALL`, `SELECT`, `CLIENT SETNAME`, `CLIENT GETNAME`, `CONFIG GET`, `CONFIG SET`, `KEYS`, `SCAN`

### Raw Commands

For commands not yet in the typed API:

```rust
let val = client.execute(&["CLIENT", "ID"]).await?;
```

## Architecture

```
ferrite-rs/
├── src/
│   ├── lib.rs          — Public API re-exports
│   ├── async_client.rs — Async client (primary API)
│   ├── client.rs       — Blocking client (sync wrapper)
│   ├── connection.rs   — TCP/TLS connection management
│   ├── pool.rs         — Connection pooling
│   ├── resp.rs         — RESP2 protocol codec
│   ├── error.rs        — Error types
│   ├── types.rs        — Value types and ToArg trait
│   └── commands/       — Command implementations
│       ├── strings.rs
│       ├── lists.rs
│       ├── hashes.rs
│       ├── sets.rs
│       ├── sorted_sets.rs
│       └── server.rs
└── examples/
    └── basic.rs        — Usage example
```

## License

Apache-2.0 — same as Ferrite.
