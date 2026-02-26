# ferrite-rs

Official Rust client SDK for [Ferrite](https://github.com/ferritelabs/ferrite) — a high-performance, tiered-storage key-value store designed as a drop-in Redis replacement.

Also fully compatible with standard Redis servers.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
ferrite-rs = "0.1"
tokio = { version = "1", features = ["full"] }
```

For TLS support:

```toml
[dependencies]
ferrite-rs = { version = "0.1", features = ["tls"] }
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

### Ferrite-Specific: Vector Search
`VECTOR.CREATE`, `VECTOR.ADD`, `VECTOR.SEARCH`, `VECTOR.DELETE`, `VECTOR.INFO`

```rust
// Create an HNSW index for 384-dim embeddings
client.vector_create("docs", 384)
    .distance("cosine")
    .index_type("hnsw")
    .m(16)
    .ef_construction(200)
    .execute().await?;

// Add a vector with metadata
client.vector_add("docs", "doc:1", &[0.1, 0.2, 0.3, /* ... */])
    .metadata(r#"{"title": "Hello"}"#)
    .execute().await?;

// Search for nearest neighbors
let results = client.vector_search("docs", &query_embedding)
    .top(10)
    .filter("category = 'tech'")
    .execute().await?;
```

### Ferrite-Specific: Semantic Caching
`SEMANTIC.SET`, `SEMANTIC.GET`, `SEMANTIC.DEL`, `SEMANTIC.STATS`

```rust
// Cache a response by semantic meaning (reduces LLM API costs)
client.semantic_set("What is Rust?", "Rust is a systems programming language...")
    .ttl(3600)
    .execute().await?;

// Retrieve by meaning (not exact key match)
let cached = client.semantic_get("Tell me about Rust programming")
    .threshold(0.85)
    .execute().await?;
```

### Ferrite-Specific: FerriteQL
`QUERY`, `VIEW.CREATE`, `VIEW.QUERY`, `VIEW.DROP`

```rust
// Execute a FerriteQL query
let results = client.query("FROM users:* WHERE $.active = true SELECT $.name")
    .limit(100)
    .execute().await?;

// Create a materialized view
client.create_view("active_users")
    .query("FROM users:* WHERE $.active = true")
    .refresh_interval(60)
    .execute().await?;
```

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
│       ├── strings.rs    — GET, SET, INCR, etc.
│       ├── lists.rs      — LPUSH, RPOP, LRANGE, etc.
│       ├── hashes.rs     — HSET, HGET, HGETALL, etc.
│       ├── sets.rs       — SADD, SMEMBERS, etc.
│       ├── sorted_sets.rs — ZADD, ZRANGE, etc.
│       ├── server.rs     — PING, INFO, etc.
│       ├── vector.rs     — VECTOR.CREATE/ADD/SEARCH (Ferrite-specific)
│       ├── semantic.rs   — SEMANTIC.SET/GET (Ferrite-specific)
│       └── ferriteql.rs  — QUERY, VIEW.* (Ferrite-specific)
└── examples/
    └── basic.rs        — Usage example
```

## License

Apache-2.0 — same as Ferrite.
