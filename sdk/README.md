# Ferrite SDK Client Libraries

Official client SDKs for [Ferrite](https://github.com/ferritelabs/ferrite), a high-performance tiered-storage key-value store designed as a drop-in Redis replacement.

Each SDK wraps the canonical Redis client for its language and adds typed helpers for every Ferrite extension command.

## Available SDKs

| Language | Package | Directory | Underlying Driver |
|----------|---------|-----------|-------------------|
| **Rust** | `ferrite-client` | [`rust/`](rust/) | [redis-rs](https://crates.io/crates/redis) |
| **Python** | `ferrite-py` | [`python/`](python/) | [redis-py](https://pypi.org/project/redis/) |
| **Node.js / TypeScript** | `@ferrite/client` | [`nodejs/`](nodejs/) | [ioredis](https://www.npmjs.com/package/ioredis) |

### AI / LLM Caching SDKs

| Language | Package | Directory | Integrations |
|----------|---------|-----------|--------------|
| **Python** | `ferrite-ai` | [`python/ferrite_ai/`](python/ferrite_ai/) | LangChain, LlamaIndex, OpenAI |
| **TypeScript** | `@ferrite/ai` | [`typescript/`](typescript/) | LangChain.js, OpenAI |

## Supported Ferrite Extensions

Every SDK provides typed methods for the following Ferrite-specific features:

| Feature | Commands | Status |
|---------|----------|--------|
| **Vector Search** | `VECTOR.INDEX.CREATE`, `VECTOR.ADD`, `VECTOR.SEARCH` | Beta |
| **Semantic Caching** | `SEMANTIC.SET`, `SEMANTIC.GET` | Experimental |
| **Time Series** | `TS.ADD`, `TS.RANGE`, `TS.GET` | Beta |
| **Document Store** | `DOC.INSERT`, `DOC.FIND` | Experimental |
| **Graph Queries** | `GRAPH.QUERY` | Experimental |
| **FerriteQL** | `QUERY` | Experimental |
| **Time-Travel** | `GET ... AS OF`, `HISTORY` | Experimental |

All standard Redis commands are also available through each SDK's underlying driver.

## Quick Start

### Rust

```rust
use ferrite_client::{FerriteClientBuilder, Result};
use ferrite_client::commands::FerriteCommands;

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = FerriteClientBuilder::new()
        .host("localhost")
        .port(6379)
        .build()
        .await?;

    redis::AsyncCommands::set(&mut *client, "key", "value").await?;
    let val: String = redis::AsyncCommands::get(&mut *client, "key").await?;
    println!("{}", val);
    Ok(())
}
```

### Python

```python
from ferrite import FerriteClient

client = FerriteClient(host="localhost", port=6379)
client.set("key", "value")
print(client.get("key"))
client.close()
```

### Node.js / TypeScript

```typescript
import { FerriteClient } from "@ferrite/client";

const client = new FerriteClient({ host: "localhost", port: 6379 });
await client.set("key", "value");
console.log(await client.get("key"));
await client.disconnect();
```

## Architecture

Each SDK follows the same design principles:

1. **Thin wrapper** -- The SDK wraps the most popular Redis client for its language rather than implementing the RESP protocol from scratch. This gives you battle-tested connection pooling, TLS, cluster support, and encoding out of the box.

2. **Extension commands as typed methods** -- Ferrite-specific commands (`VECTOR.SEARCH`, `SEMANTIC.GET`, `TS.RANGE`, `GRAPH.QUERY`, `QUERY`, `HISTORY`, etc.) are exposed as typed methods with structured result types.

3. **Full standard Redis access** -- Every SDK exposes the underlying Redis client so you can use any standard Redis command directly.

4. **Raw command escape hatch** -- An `execute_raw` / `executeCommand` method lets you send any arbitrary command for extensions not yet covered by typed helpers.

## Building

### Rust

```bash
cd rust
cargo build
```

### Python

```bash
cd python
pip install -e ".[test]"
pytest tests/
```

### Node.js

```bash
cd nodejs
npm install
npm run build
npm test
```

## Embedded Mode

Ferrite also supports an **embedded mode** where the database runs in-process — no server needed. See [`docs/EMBEDDED_SDK.md`](../docs/EMBEDDED_SDK.md) for details.

| Language | Embedded Package | Status |
|----------|-----------------|--------|
| **Rust** | `ferrite` (library mode) | ✅ Stable |
| **Python** | `ferrite-py` (embedded) | 🧪 Beta |
| **Node.js** | `@ferritelabs/ferrite` (embedded) | 🧪 Beta |

## License

Apache-2.0
