# Ferrite Java SDK

Official Java client for [Ferrite](https://github.com/ferritelabs/ferrite) — a high-performance Redis-compatible key-value store with vector search, semantic caching, and FerriteQL.

## Features

- RESP2 protocol implementation (no external dependencies, JDK 11+ only)
- Connection pooling with configurable pool size
- All standard Redis data structures (strings, lists, hashes, sets, sorted sets)
- Ferrite-specific extensions: vector search, FerriteQL
- SSL/TLS support
- Retry with configurable backoff
- Builder pattern configuration

## Requirements

- Java 11+
- No external dependencies

## Installation

### Maven

```xml
<dependency>
    <groupId>io.ferritelabs</groupId>
    <artifactId>ferrite-java</artifactId>
    <version>0.1.0</version>
</dependency>
```

### Gradle

```groovy
implementation 'io.ferritelabs:ferrite-java:0.1.0'
```

## Quick Start

```java
import io.ferritelabs.ferrite.*;

FerriteConfig config = FerriteConfig.builder()
    .host("localhost")
    .port(6379)
    .build();

try (FerriteClient client = FerriteClient.connect(config)) {
    // String operations
    client.set("key", "value");
    String val = client.get("key");

    // With TTL
    client.set("temp", "data", Duration.ofSeconds(60));

    // Hash operations
    client.hset("user:1", "name", "Alice", "age", "30");
    String name = client.hget("user:1", "name");
    Map<String, String> user = client.hgetAll("user:1");

    // List operations
    client.rpush("queue", "a", "b", "c");
    List<String> items = client.lrange("queue", 0, -1);
}
```

## Configuration

```java
FerriteConfig config = FerriteConfig.builder()
    .host("localhost")
    .port(6379)
    .password("secret")
    .database(1)
    .ssl(true)
    .poolSize(20)
    .timeout(Duration.ofSeconds(10))
    .maxRetries(3)
    .retryBackoff(Duration.ofMillis(200))
    .build();
```

## Vector Search

```java
VectorClient vectors = new VectorClient(client);

// Create an index
vectors.createIndex("embeddings", 384, "cosine");

// Add vectors
float[] vec = new float[]{0.1f, 0.2f, 0.3f};
vectors.addVector("embeddings", "doc1", vec, Map.of("text", "hello world"));

// Search
float[] query = new float[]{0.1f, 0.2f, 0.3f};
List<VectorClient.VectorResult> results = vectors.search("embeddings", query, 10);
for (VectorClient.VectorResult r : results) {
    System.out.printf("ID: %s, Distance: %f%n", r.getId(), r.getDistance());
}

// Delete
vectors.deleteVector("embeddings", "doc1");
```

## FerriteQL

```java
QueryClient queryClient = new QueryClient(client);
QueryClient.QueryResult result = queryClient.execute("SELECT * FROM users WHERE age > 25");
System.out.printf("Found %d rows in %.2f ms%n", result.getCount(), result.getDurationMillis());
for (Map<String, Object> row : result.getRows()) {
    System.out.println(row);
}
```

## Data Structures

### Lists

```java
client.lpush("list", "a", "b");
client.rpush("list", "c");
String first = client.lpop("list");
long length = client.llen("list");
```

### Sets

```java
client.sadd("tags", "java", "redis", "ferrite");
Set<String> members = client.smembers("tags");
long count = client.scard("tags");
client.srem("tags", "redis");
```

### Sorted Sets

```java
client.zadd("leaderboard", 100.0, "alice");
client.zadd("leaderboard", 200.0, "bob");
List<String> top = client.zrange("leaderboard", 0, -1);
Double score = client.zscore("leaderboard", "alice");
```

## Building

```bash
mvn clean package
```

## License

Apache-2.0
