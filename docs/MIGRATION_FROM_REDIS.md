# Migrating from Redis to Ferrite

This guide covers the practical steps to migrate an existing Redis deployment to Ferrite.

## Compatibility Overview

Ferrite passes **~72% of the Redis TCL test suite** and supports all core data types and operations. See [REDIS_COMPAT.md](REDIS_COMPAT.md) for the detailed command-by-command matrix.

### What Works Out of the Box

| Category | Compatibility | Notes |
|----------|--------------|-------|
| **Strings** | 100% | GET, SET, MGET, MSET, INCR, APPEND, etc. |
| **Lists** | 100% | LPUSH, RPUSH, LPOP, RPOP, LRANGE, etc. |
| **Hashes** | 100% | HSET, HGET, HMSET, HGETALL, etc. |
| **Sets** | 100% | SADD, SMEMBERS, SINTER, SUNION, etc. |
| **Sorted Sets** | 100% | ZADD, ZRANGE, ZRANGEBYSCORE, etc. |
| **HyperLogLog** | 100% | PFADD, PFCOUNT, PFMERGE |
| **Pub/Sub** | 100% | SUBSCRIBE, PUBLISH, PSUBSCRIBE |
| **Transactions** | 100% | MULTI, EXEC, DISCARD, WATCH |
| **Lua Scripting** | 95% | EVAL, EVALSHA (Lua 5.4) |
| **Streams** | ~60% | XADD, XREAD, XRANGE (consumer groups partial) |
| **Cluster** | ~40% | Hash slots, MOVED/ASK (resharding pending) |
| **ACL** | ~50% | User management, command permissions |

### Not Yet Supported

- Redis Modules (`MODULE LOAD`) — use Ferrite's WASM plugin system instead
- Redis Functions (`FUNCTION LOAD`) — planned for v0.3
- `WAIT` with full replication acknowledgement semantics
- Redis Cluster automatic resharding

## Step 1: Configuration Mapping

| Redis (`redis.conf`) | Ferrite (`ferrite.toml`) | Notes |
|---------------------|------------------------|-------|
| `bind 127.0.0.1` | `server.bind = "127.0.0.1"` | Same |
| `port 6379` | `server.port = 6379` | Same |
| `maxclients 10000` | `server.max_connections = 10000` | Same |
| `timeout 0` | `server.timeout = 0` | Same |
| `tcp-keepalive 300` | `server.tcp_keepalive = 300` | Same |
| `maxmemory 2gb` | `storage.max_memory = 2147483648` | Bytes in Ferrite |
| `maxmemory-policy allkeys-lru` | *(built-in eviction)* | Automatic tiering instead |
| `appendonly yes` | `persistence.aof_enabled = true` | Same concept |
| `appendfsync everysec` | `persistence.aof_sync = "everysecond"` | Slightly different name |
| `save 60 1000` | `persistence.checkpoint_enabled = true` | RDB-style checkpoints |
| `requirepass secret` | ACL system | Use `ferrite-cli ACL SETUSER` |
| `dir /data` | `storage.data_dir = "/data"` | Same |
| `loglevel notice` | `logging.level = "info"` | Standard log levels |
| *(no equivalent)* | `storage.backend = "hybridlog"` | **Ferrite-specific**: tiered storage |
| *(no equivalent)* | `server.proto_max_bulk_len` | Protocol DoS limits |

## Step 2: Client Library Compatibility

Ferrite speaks the standard RESP2/RESP3 protocol. **No client library changes needed.**

```python
# Python — works identically
import redis
r = redis.Redis(host='localhost', port=6379)
r.set('key', 'value')
r.get('key')  # b'value'
```

```javascript
// Node.js — works identically
const Redis = require('ioredis');
const redis = new Redis(6379, 'localhost');
await redis.set('key', 'value');
await redis.get('key'); // 'value'
```

```rust
// Rust — works identically
let client = redis::Client::open("redis://127.0.0.1/")?;
let mut con = client.get_connection()?;
redis::cmd("SET").arg("key").arg("value").execute(&mut con);
```

## Step 3: Data Migration

### Option A: Dump and Restore (recommended for <10GB)

```bash
# On Redis
redis-cli BGSAVE
# Wait for save to complete
redis-cli LASTSAVE

# Copy the RDB file
cp /var/lib/redis/dump.rdb /tmp/dump.rdb

# On Ferrite (import RDB)
ferrite-cli --rdb-import /tmp/dump.rdb
```

### Option B: Live Replication (recommended for >10GB or zero-downtime)

```bash
# Configure Ferrite as a replica of Redis
ferrite-cli REPLICAOF redis-host 6379

# Wait for sync to complete
ferrite-cli INFO replication
# Look for: master_sync_in_progress:0

# Promote Ferrite to primary
ferrite-cli REPLICAOF NO ONE

# Point your application to Ferrite
```

### Option C: Dual-Write (safest for production)

1. Configure your application to write to both Redis and Ferrite
2. Read from Redis, verify with Ferrite
3. Once validated, switch reads to Ferrite
4. Remove Redis writes

## Step 4: Verify

```bash
# Compare key counts
redis-cli DBSIZE
ferrite-cli DBSIZE

# Run the compatibility test suite
cargo test --test redis_compatibility

# Monitor for errors
RUST_LOG=ferrite=warn ferrite --config ferrite.toml
```

## Step 5: Leverage Ferrite-Specific Features

After migration, you can enable Ferrite-exclusive features:

```toml
# Enable tiered storage (reduce memory costs by 80%+)
[storage]
backend = "hybridlog"

# Enable vector search
# Use FT.CREATE, FT.SEARCH commands

# Enable WASM plugins
# Use PLUGIN INSTALL, PLUGIN LIST commands
```

## Behavioral Differences

| Behavior | Redis | Ferrite |
|----------|-------|---------|
| Memory limit exceeded | Eviction or OOM error | Automatic tiering to disk |
| Persistence format | RDB + AOF | AOF + checkpoints (RDB import supported) |
| Cluster gossip | Redis Cluster protocol | Compatible (partial) |
| Module system | C shared libraries | WASM sandboxed plugins |
| Scripting | Lua 5.1 | Lua 5.4 |
| TLS implementation | OpenSSL | rustls (memory-safe) |
| Default auth | `requirepass` | ACL system |

## Rollback Plan

If you need to revert:

```bash
# Ferrite supports BGSAVE which produces a compatible snapshot
ferrite-cli BGSAVE

# Stop Ferrite, start Redis with the snapshot
redis-server --dbfilename dump.rdb --dir /var/lib/ferrite/data
```

## Getting Help

- [Troubleshooting Guide](TROUBLESHOOTING.md)
- [GitHub Issues](https://github.com/ferritelabs/ferrite/issues)
- [Community](https://github.com/ferritelabs/ferrite/blob/main/COMMUNITY.md)
