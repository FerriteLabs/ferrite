# @ferrite/client

Official Node.js / TypeScript client SDK for [Ferrite](https://github.com/ferritelabs/ferrite), a high-performance tiered-storage key-value store that is wire-compatible with Redis.

## Installation

```bash
npm install @ferrite/client
# or
yarn add @ferrite/client
# or
pnpm add @ferrite/client
```

## Quick Start

```typescript
import { FerriteClient } from "@ferrite/client";

const client = new FerriteClient({ host: "localhost", port: 6379 });

await client.set("key", "value");
console.log(await client.get("key")); // "value"

await client.disconnect();
```

### TLS

```typescript
const client = new FerriteClient({
  host: "db.example.com",
  port: 6380,
  password: "secret",
  tls: {
    ca: fs.readFileSync("/path/to/ca.crt"),
  },
});
```

## Ferrite Extensions

All Ferrite-specific commands are available as typed methods on the client.

### Vector Search

```typescript
await client.vectorCreateIndex("embeddings", 384, "cosine", "hnsw");

const embedding = new Array(384).fill(0.1);
await client.vectorSet("embeddings", "doc:1", embedding, { tag: "docs" });

const results = await client.vectorSearch("embeddings", embedding, {
  topK: 10,
  filter: "tag == 'docs'",
});
for (const r of results) {
  console.log(`${r.id}: ${r.score}`);
}
```

### Semantic Caching

```typescript
await client.semanticSet("What is the capital of France?", "Paris is the capital...");
const results = await client.semanticSearch("France's capital city?", 0.85);
```

### Time Series

```typescript
await client.tsAdd("temperature:room1", "*", 23.5);
const samples = await client.tsRange("temperature:room1", "-", "+");
const latest = await client.tsGet("temperature:room1");
```

### Document Store

```typescript
await client.docSet("articles", "article:1", {
  title: "Hello",
  author: "Alice",
  views: 100,
});

const results = await client.docSearch("articles", { author: "Alice" });
```

### Graph Queries

```typescript
const result = await client.graphQuery(
  "social",
  "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name = 'Alice' RETURN b",
);
```

### FerriteQL

```typescript
const result = await client.query(
  "FROM users:* WHERE $.active = true SELECT $.name LIMIT 10",
);
```

### Time-Travel Queries

```typescript
const oldValue = await client.timeTravel("mykey", "-1h");
const history = await client.history("mykey", "-24h");
```

## Standard Redis Commands

Access the underlying `ioredis` instance for any standard Redis command:

```typescript
const redis = client.ioRedis;
await redis.lpush("queue", "task1", "task2");
await redis.hset("user:1", "name", "Alice", "age", "30");
```

## Raw Commands

For Ferrite commands not yet covered by typed helpers:

```typescript
const result = await client.executeCommand("CRDT.GCOUNTER", "mycounter", "INCR", "5");
```

## Error Handling

```typescript
import { FerriteConnectionError, FerriteCommandError } from "@ferrite/client";

try {
  await client.ping();
} catch (err) {
  if (err instanceof FerriteConnectionError) {
    console.error("Cannot reach the server");
  }
}
```

## Embedded Mode

Ferrite also supports an embedded mode — use it as an in-process database in your Node.js application:

```javascript
const { Ferrite } = require('@ferritelabs/ferrite');

// Create an in-memory database
const db = new Ferrite({ memoryLimit: '256mb' });

// Basic operations
await db.set('key', 'value');
const value = await db.get('key'); // 'value'

// With TTL (seconds)
await db.set('session', 'data', { ttl: 3600 });

// With persistence
const persistentDb = new Ferrite({
  dataDir: '/var/lib/ferrite',
  persistence: true,
  memoryLimit: '1gb',
});
```

### Data Types (Embedded)

```javascript
// Strings
await db.set('name', 'ferrite');
await db.incr('counter');

// Lists
await db.lpush('queue', 'item1', 'item2');
const items = await db.lrange('queue', 0, -1);

// Hashes
await db.hset('user:1', { name: 'Alice', age: '30' });
const name = await db.hget('user:1', 'name');

// Sets
await db.sadd('tags', 'nodejs', 'database', 'fast');
const members = await db.smembers('tags');

// Sorted Sets
await db.zadd('leaderboard', { alice: 100, bob: 85 });
const top = await db.zrange('leaderboard', 0, 9, { withScores: true });
```

### TypeScript Support

Full TypeScript definitions included:

```typescript
import { Ferrite, FerriteConfig, EvictionPolicy } from '@ferritelabs/ferrite';

const config: FerriteConfig = {
  memoryLimit: '512mb',
  evictionPolicy: EvictionPolicy.AllKeysLru,
};

const db = new Ferrite(config);
const value: string | null = await db.get('key');
```

### Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `memoryLimit` | `'256mb'` | Maximum memory usage |
| `dataDir` | `undefined` | Path for persistence |
| `persistence` | `false` | Enable AOF persistence |
| `evictionPolicy` | `'noeviction'` | Eviction policy |
| `compression` | `false` | Enable LZ4 compression |

## Running Tests

```bash
npm install
npm test
```

## License

Apache-2.0
