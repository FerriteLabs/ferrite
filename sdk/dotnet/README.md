# Ferrite .NET SDK

Official .NET client for [Ferrite](https://github.com/ferritelabs/ferrite) — a high-performance Redis-compatible key-value store with vector search, semantic caching, and FerriteQL.

## Features

- Fully async API with `CancellationToken` support
- RESP2 protocol implementation (no external dependencies)
- Connection pooling with configurable pool size
- All standard Redis data structures (strings, lists, hashes, sets, sorted sets)
- Ferrite-specific extensions: vector search, FerriteQL
- SSL/TLS support
- Retry with configurable backoff
- `IAsyncDisposable` for proper resource cleanup

## Requirements

- .NET 8.0+
- No external dependencies

## Installation

```bash
dotnet add package Ferrite.Client --version 0.1.0
```

## Quick Start

```csharp
using Ferrite.Client;

await using var client = await FerriteClient.ConnectAsync("localhost", 6379);

// String operations
await client.SetAsync("key", "value");
var val = await client.GetAsync("key");

// With TTL
await client.SetAsync("temp", "data", TimeSpan.FromSeconds(60));

// Hash operations
await client.HSetAsync("user:1", default, "name", "Alice", "age", "30");
var name = await client.HGetAsync("user:1", "name");
var user = await client.HGetAllAsync("user:1");

// List operations
await client.RPushAsync("queue", default, "a", "b", "c");
var items = await client.LRangeAsync("queue", 0, -1);
```

## Configuration

```csharp
var options = new FerriteOptions
{
    Password = "secret",
    Database = 1,
    Ssl = true,
    PoolSize = 20,
    Timeout = TimeSpan.FromSeconds(10),
    MaxRetries = 3,
    RetryBackoff = TimeSpan.FromMilliseconds(200),
};

await using var client = await FerriteClient.ConnectAsync("localhost", 6379, options);
```

## Vector Search

```csharp
var vectors = new VectorClient(client);

// Create an index
await vectors.CreateIndexAsync("embeddings", 384, "cosine");

// Add vectors
var vec = new float[] { 0.1f, 0.2f, 0.3f };
await vectors.AddVectorAsync("embeddings", "doc1", vec,
    new Dictionary<string, object> { ["text"] = "hello world" });

// Search
var query = new float[] { 0.1f, 0.2f, 0.3f };
var results = await vectors.SearchAsync("embeddings", query, 10);
foreach (var r in results)
{
    Console.WriteLine($"ID: {r.Id}, Distance: {r.Distance}");
}

// Delete
await vectors.DeleteAsync("embeddings", "doc1");
```

## FerriteQL

```csharp
var queryClient = new QueryClient(client);
var result = await queryClient.ExecuteAsync("SELECT * FROM users WHERE age > 25");
Console.WriteLine($"Found {result.Count} rows in {result.Duration.TotalMilliseconds:F2} ms");
foreach (var row in result.Rows)
{
    Console.WriteLine(string.Join(", ", row.Select(kv => $"{kv.Key}={kv.Value}")));
}
```

## Data Structures

### Lists

```csharp
await client.LPushAsync("list", default, "a", "b");
await client.RPushAsync("list", default, "c");
var first = await client.LPopAsync("list");
var length = await client.LLenAsync("list");
```

### Sets

```csharp
await client.SAddAsync("tags", default, "csharp", "redis", "ferrite");
var members = await client.SMembersAsync("tags");
var count = await client.SCardAsync("tags");
await client.SRemAsync("tags", default, "redis");
```

### Sorted Sets

```csharp
await client.ZAddAsync("leaderboard", 100.0, "alice");
await client.ZAddAsync("leaderboard", 200.0, "bob");
var top = await client.ZRangeAsync("leaderboard", 0, -1);
var score = await client.ZScoreAsync("leaderboard", "alice");
```

## License

Apache-2.0
