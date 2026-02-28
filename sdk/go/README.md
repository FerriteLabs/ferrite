# Ferrite Go SDK

Official Go client for [Ferrite](https://github.com/ferritelabs/ferrite) — a high-performance Redis-compatible key-value store with vector search, semantic caching, and FerriteQL.

## Features

- RESP2 protocol implementation (no external dependencies)
- Connection pooling with configurable pool size
- All standard Redis data structures (strings, lists, hashes, sets, sorted sets)
- Ferrite-specific extensions: vector search, FerriteQL
- TLS support
- Retry with configurable backoff
- Context-aware operations with deadlines and cancellation

## Installation

```bash
go get github.com/ferritelabs/ferrite-go
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "time"

    ferrite "github.com/ferritelabs/ferrite-go"
)

func main() {
    client, err := ferrite.NewClient("localhost:6379")
    if err != nil {
        panic(err)
    }
    defer client.Close()

    ctx := context.Background()

    // String operations
    _ = client.Set(ctx, "key", "value", 10*time.Second)
    val, _ := client.Get(ctx, "key")
    fmt.Println(val) // "value"

    // Hash operations
    client.HSet(ctx, "user:1", "name", "Alice", "age", "30")
    name, _ := client.HGet(ctx, "user:1", "name")
    fmt.Println(name) // "Alice"

    // List operations
    client.RPush(ctx, "queue", "a", "b", "c")
    items, _ := client.LRange(ctx, "queue", 0, -1)
    fmt.Println(items) // ["a", "b", "c"]
}
```

## Connection Options

```go
client, err := ferrite.NewClient("localhost:6379",
    ferrite.WithPassword("secret"),
    ferrite.WithDB(1),
    ferrite.WithPoolSize(20),
    ferrite.WithTimeout(10 * time.Second),
    ferrite.WithRetry(3, 200*time.Millisecond),
    ferrite.WithTLS(&tls.Config{
        InsecureSkipVerify: false,
    }),
)
```

## Vector Search

```go
// Create an index
err := client.VectorCreate(ctx, "embeddings", 384, "cosine")

// Add vectors
vector := []float32{0.1, 0.2, 0.3, /* ... */}
metadata := map[string]interface{}{"text": "hello world"}
err = client.VectorAdd(ctx, "embeddings", "doc1", vector, metadata)

// Search
query := []float32{0.1, 0.2, 0.3, /* ... */}
results, err := client.VectorSearch(ctx, "embeddings", query, 10)
for _, r := range results {
    fmt.Printf("ID: %s, Distance: %f\n", r.ID, r.Distance)
}

// Delete
err = client.VectorDelete(ctx, "embeddings", "doc1")
```

## FerriteQL

```go
result, err := client.Query(ctx, "SELECT * FROM users WHERE age > 25")
if err != nil {
    panic(err)
}
fmt.Printf("Found %d rows in %v\n", result.Count, result.Duration)
for _, row := range result.Rows {
    fmt.Println(row)
}
```

## Data Structures

### Lists

```go
client.LPush(ctx, "list", "a", "b")
client.RPush(ctx, "list", "c")
val, _ := client.LPop(ctx, "list")
length, _ := client.LLen(ctx, "list")
```

### Sets

```go
client.SAdd(ctx, "tags", "go", "redis", "ferrite")
members, _ := client.SMembers(ctx, "tags")
count, _ := client.SCard(ctx, "tags")
client.SRem(ctx, "tags", "redis")
```

### Sorted Sets

```go
client.ZAdd(ctx, "leaderboard", 100.0, "alice")
client.ZAdd(ctx, "leaderboard", 200.0, "bob")
top, _ := client.ZRange(ctx, "leaderboard", 0, -1)
score, _ := client.ZScore(ctx, "leaderboard", "alice")
```

## License

Apache-2.0
