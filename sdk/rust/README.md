# ferrite-client

Official Rust client SDK for [Ferrite](https://github.com/ferritelabs/ferrite), a high-performance tiered-storage key-value store that is wire-compatible with Redis.

## Installation

Add `ferrite-client` to your `Cargo.toml`:

```toml
[dependencies]
ferrite-client = "0.1"
tokio = { version = "1", features = ["full"] }
```

## Quick Start

```rust
use ferrite_client::{FerriteClient, FerriteClientBuilder, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Connect with the builder
    let mut client = FerriteClientBuilder::new()
        .host("localhost")
        .port(6379)
        .build()
        .await?;

    // All standard Redis commands work via Deref
    redis::AsyncCommands::set(&mut *client, "key", "value").await?;
    let val: String = redis::AsyncCommands::get(&mut *client, "key").await?;
    println!("{}", val);

    Ok(())
}
```

Or connect with a URL:

```rust
let mut client = FerriteClient::connect("redis://localhost:6379").await?;
```

## TLS

```rust
let client = FerriteClientBuilder::new()
    .host("db.example.com")
    .port(6380)
    .password("secret")
    .tls(true)
    .build()
    .await?;
```

## Ferrite Extensions

All Ferrite-specific commands are available through the `FerriteCommands` trait:

```rust
use ferrite_client::commands::{FerriteCommands, VectorSearchOptions};
```

### Vector Search

```rust
// Create an index
client.vector_create_index("embeddings", 384, "cosine", "hnsw").await?;

// Add a vector
let embedding: Vec<f32> = vec![0.1; 384];
client.vector_set("embeddings", "doc:1", &embedding, None).await?;

// Search
let results = client.vector_search(
    "embeddings",
    &embedding,
    &VectorSearchOptions::new().top_k(10).filter("category == 'docs'"),
).await?;
```

### Semantic Caching

```rust
client.semantic_set("What is the capital of France?", "Paris is the capital...").await?;
let results = client.semantic_search("France's capital city?", 0.85).await?;
```

### Time Series

```rust
client.ts_add("temperature:room1", "*", 23.5).await?;
let samples = client.ts_range("temperature:room1", "-", "+").await?;
let latest = client.ts_get("temperature:room1").await?;
```

### Document Store

```rust
use serde_json::json;

let doc = json!({"title": "Hello", "author": "Alice"});
client.doc_set("articles", "article:1", &doc).await?;

let query = json!({"author": "Alice"});
let results = client.doc_search("articles", &query).await?;
```

### Graph Queries

```rust
let result = client.graph_query(
    "social",
    "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name = 'Alice' RETURN b",
).await?;
```

### FerriteQL

```rust
let result = client.query(
    "FROM users:* WHERE $.active = true SELECT $.name LIMIT 10",
).await?;
```

### Time-Travel Queries

```rust
// Read a key's value from 1 hour ago
let old_value = client.time_travel("mykey", "-1h").await?;

// Get the full history of changes in the last 24 hours
let history = client.history("mykey", "-24h").await?;
```

## Raw Commands

For Ferrite commands not yet covered by typed helpers:

```rust
let raw = client.execute_raw("CRDT.GCOUNTER", &["mycounter", "INCR", "5"]).await?;
```

## Error Handling

All methods return `ferrite_client::Result<T>`, which uses the `FerriteError` enum:

```rust
use ferrite_client::FerriteError;

match client.ping().await {
    Ok(pong) => println!("Connected: {}", pong),
    Err(FerriteError::Connection(msg)) => eprintln!("Connection failed: {}", msg),
    Err(FerriteError::Timeout(ms)) => eprintln!("Timed out after {}ms", ms),
    Err(e) => eprintln!("Error: {}", e),
}
```

## Embedded Mode

Ferrite can also be used as an in-process embedded database with no network overhead:

```rust
use ferrite::embedded::{Ferrite, EmbeddedConfig, EvictionPolicy};

let config = EmbeddedConfig::builder()
    .memory_limit("256mb")
    .eviction_policy(EvictionPolicy::AllKeysLru)
    .build();

let db = Ferrite::open(config)?;
db.set("key", "value")?;
let val = db.get("key")?;
```

See the [`examples/`](examples/) directory for more usage patterns.

## License

Apache-2.0
