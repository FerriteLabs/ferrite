# ferrite-py

Official Python client SDK for [Ferrite](https://github.com/ferritelabs/ferrite), a high-performance tiered-storage key-value store that is wire-compatible with Redis.

## Installation

```bash
pip install ferrite-py

# With async support and hiredis parser
pip install ferrite-py[async]
```

## Quick Start

### Synchronous

```python
from ferrite import FerriteClient

client = FerriteClient(host="localhost", port=6379)

client.set("key", "value")
print(client.get("key"))  # "value"

client.close()
```

### Asynchronous

```python
import asyncio
from ferrite import AsyncFerriteClient

async def main():
    async with AsyncFerriteClient(host="localhost", port=6379) as client:
        await client.set("key", "value")
        print(await client.get("key"))

asyncio.run(main())
```

### TLS

```python
client = FerriteClient(
    host="db.example.com",
    port=6380,
    password="secret",
    ssl=True,
    ssl_ca_certs="/path/to/ca.crt",
)
```

## Ferrite Extensions

### Vector Search

```python
client.vector_create_index("embeddings", dimensions=384, distance="cosine")

embedding = [0.1] * 384
client.vector_set("embeddings", "doc:1", embedding, metadata={"tag": "docs"})

results = client.vector_search("embeddings", embedding, top_k=10)
for r in results:
    print(f"{r.id}: {r.score}")
```

### Semantic Caching

```python
client.semantic_set("What is the capital of France?", "Paris is the capital...")
results = client.semantic_search("France's capital city?", threshold=0.85)
```

### Time Series

```python
client.ts_add("temperature:room1", "*", 23.5)
samples = client.ts_range("temperature:room1", "-", "+")
latest = client.ts_get("temperature:room1")
```

### Document Store

```python
doc = {"title": "Hello", "author": "Alice", "views": 100}
client.doc_set("articles", "article:1", doc)

results = client.doc_search("articles", {"author": "Alice"})
```

### Graph Queries

```python
result = client.graph_query(
    "social",
    "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name = 'Alice' RETURN b",
)
```

### FerriteQL

```python
result = client.query(
    "FROM users:* WHERE $.active = true SELECT $.name LIMIT 10"
)
```

### Time-Travel Queries

```python
old_value = client.time_travel("mykey", "-1h")
history = client.history("mykey", "-24h")
```

## Standard Redis Commands

All standard Redis operations are available through the `redis` property:

```python
client.redis.lpush("queue", "task1", "task2")
client.redis.hset("user:1", mapping={"name": "Alice", "age": "30"})
```

## Error Handling

```python
from ferrite.exceptions import (
    FerriteConnectionError,
    FerriteTimeoutError,
    FerriteCommandError,
)

try:
    client.ping()
except FerriteConnectionError:
    print("Cannot reach the server")
```

## Embedded Mode

Ferrite also supports an embedded mode — use it as an in-process database without running a separate server:

```python
from ferrite import Ferrite, EvictionPolicy

# Create an in-memory database
db = Ferrite(memory_limit="256mb")

# Basic operations
db.set("key", "value")
value = db.get("key")  # "value"

# With TTL (seconds)
db.set("session", "data", ttl=3600)

# With persistence
db = Ferrite(
    data_dir="/var/lib/ferrite",
    persistence=True,
    memory_limit="1gb",
    eviction_policy=EvictionPolicy.ALL_KEYS_LRU,
)
```

### Data Types

```python
# Strings
db.set("name", "ferrite")
db.incr("counter")
db.append("name", "-db")

# Lists
db.lpush("queue", "item1", "item2")
items = db.lrange("queue", 0, -1)

# Hashes
db.hset("user:1", {"name": "Alice", "age": "30"})
name = db.hget("user:1", "name")

# Sets
db.sadd("tags", "python", "database", "fast")
members = db.smembers("tags")

# Sorted Sets
db.zadd("leaderboard", {"alice": 100, "bob": 85})
top = db.zrange("leaderboard", 0, 9, withscores=True)
```

### Vector Search (Embedded)

```python
from ferrite import Ferrite, VectorIndex

db = Ferrite(memory_limit="512mb")

# Create a vector index
db.vector_create("embeddings", dim=384, distance="cosine")

# Add vectors
db.vector_add("embeddings", "doc1", [0.1, 0.2, ...], metadata={"title": "Hello"})

# Search
results = db.vector_search("embeddings", [0.1, 0.2, ...], top_k=10)
```

### Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `memory_limit` | `"256mb"` | Maximum memory usage |
| `data_dir` | `None` | Directory for persistence (None = memory-only) |
| `persistence` | `False` | Enable AOF persistence |
| `eviction_policy` | `NO_EVICTION` | Eviction policy when memory limit reached |
| `compression` | `False` | Enable LZ4 compression |

### Thread Safety

The Ferrite Python SDK is thread-safe. The underlying Rust engine uses `Arc<RwLock>` for concurrent access.

```python
import threading

db = Ferrite(memory_limit="256mb")

def worker(thread_id):
    for i in range(1000):
        db.set(f"key:{thread_id}:{i}", f"value:{i}")

threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
for t in threads:
    t.start()
for t in threads:
    t.join()
```

## Running Tests

```bash
pip install ferrite-py[test]
pytest tests/
```

## License

Apache-2.0
