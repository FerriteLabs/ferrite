# Ferrite — Redis Compatibility Matrix

Ferrite aims to be a drop-in Redis replacement. This document tracks command-level
compatibility with Redis 7.x.

**Current Compatibility: ~72%** of tested commands passing
(based on automated test suite — run `scripts/redis_compat_report.sh` for live results)

## Status Legend

| Icon | Meaning |
|------|---------|
| ✅ | Fully supported — behaves identically to Redis |
| 🔧 | Partial — core behavior works, some options/edge cases differ |
| ❌ | Not yet implemented |
| ➖ | Not planned (out of scope for Ferrite) |

---

## String Commands

| Command | Status | Notes |
|---------|--------|-------|
| `SET` | ✅ | Supports EX, PX, NX, XX, KEEPTTL, GET options |
| `GET` | ✅ | |
| `MGET` | ✅ | |
| `MSET` | ✅ | |
| `MSETNX` | ✅ | |
| `SETNX` | ✅ | |
| `SETEX` | ✅ | |
| `PSETEX` | ✅ | |
| `GETSET` | ✅ | Deprecated in Redis 6.2, use SET with GET option |
| `GETDEL` | ✅ | |
| `GETEX` | 🔧 | Basic support |
| `INCR` | ✅ | |
| `INCRBY` | ✅ | |
| `INCRBYFLOAT` | ✅ | |
| `DECR` | ✅ | |
| `DECRBY` | ✅ | |
| `APPEND` | ✅ | |
| `STRLEN` | ✅ | |
| `GETRANGE` | ✅ | |
| `SETRANGE` | ✅ | |
| `SUBSTR` | ✅ | Alias for GETRANGE |
| `LCS` | ❌ | Longest common substring (Redis 7.0+) |

## List Commands

| Command | Status | Notes |
|---------|--------|-------|
| `LPUSH` | ✅ | |
| `RPUSH` | ✅ | |
| `LPOP` | ✅ | Supports count argument |
| `RPOP` | ✅ | Supports count argument |
| `LRANGE` | ✅ | |
| `LLEN` | ✅ | |
| `LINDEX` | ✅ | |
| `LSET` | ✅ | |
| `LREM` | ✅ | |
| `LINSERT` | ✅ | |
| `LPOS` | 🔧 | Basic support, RANK/COUNT/MAXLEN options partial |
| `LTRIM` | ✅ | |
| `RPOPLPUSH` | ✅ | Deprecated in Redis 6.2, use LMOVE |
| `LMOVE` | ✅ | |
| `LMPOP` | ❌ | Redis 7.0+ |
| `BLPOP` | 🔧 | Basic blocking support |
| `BRPOP` | 🔧 | Basic blocking support |
| `BLMOVE` | ❌ | |
| `BLMPOP` | ❌ | Redis 7.0+ |

## Hash Commands

| Command | Status | Notes |
|---------|--------|-------|
| `HSET` | ✅ | Supports multiple field-value pairs |
| `HGET` | ✅ | |
| `HMSET` | ✅ | Deprecated, use HSET |
| `HMGET` | ✅ | |
| `HDEL` | ✅ | |
| `HEXISTS` | ✅ | |
| `HGETALL` | ✅ | |
| `HKEYS` | ✅ | |
| `HVALS` | ✅ | |
| `HLEN` | ✅ | |
| `HINCRBY` | ✅ | |
| `HINCRBYFLOAT` | ✅ | |
| `HSETNX` | ✅ | |
| `HRANDFIELD` | 🔧 | Basic support |
| `HSCAN` | 🔧 | Basic cursor support |
| `HEXPIRE` | ❌ | Redis 7.4+ per-field expiry |
| `HPERSIST` | ❌ | Redis 7.4+ |
| `HTTL` | ❌ | Redis 7.4+ |

## Set Commands

| Command | Status | Notes |
|---------|--------|-------|
| `SADD` | ✅ | |
| `SREM` | ✅ | |
| `SMEMBERS` | ✅ | |
| `SISMEMBER` | ✅ | |
| `SMISMEMBER` | ✅ | |
| `SCARD` | ✅ | |
| `SUNION` | ✅ | |
| `SUNIONSTORE` | ✅ | |
| `SINTER` | ✅ | |
| `SINTERCARD` | ❌ | Redis 7.0+ |
| `SINTERSTORE` | ✅ | |
| `SDIFF` | ✅ | |
| `SDIFFSTORE` | ✅ | |
| `SPOP` | ✅ | |
| `SRANDMEMBER` | ✅ | |
| `SMOVE` | ✅ | |
| `SSCAN` | 🔧 | Basic cursor support |

## Sorted Set Commands

| Command | Status | Notes |
|---------|--------|-------|
| `ZADD` | ✅ | Supports NX, XX, GT, LT, CH options |
| `ZREM` | ✅ | |
| `ZSCORE` | ✅ | |
| `ZMSCORE` | ✅ | |
| `ZCARD` | ✅ | |
| `ZCOUNT` | ✅ | |
| `ZLEXCOUNT` | ✅ | |
| `ZRANK` | ✅ | |
| `ZREVRANK` | ✅ | |
| `ZRANGE` | ✅ | Supports BYSCORE, BYLEX, REV, LIMIT |
| `ZRANGEBYSCORE` | ✅ | Deprecated, use ZRANGE BYSCORE |
| `ZREVRANGE` | ✅ | |
| `ZREVRANGEBYSCORE` | ✅ | |
| `ZRANGEBYLEX` | ✅ | |
| `ZRANGESTORE` | ❌ | |
| `ZINCRBY` | ✅ | |
| `ZPOPMIN` | ✅ | |
| `ZPOPMAX` | ✅ | |
| `BZPOPMIN` | ❌ | Blocking variant |
| `BZPOPMAX` | ❌ | Blocking variant |
| `ZRANDMEMBER` | 🔧 | Basic support |
| `ZUNIONSTORE` | ✅ | |
| `ZINTERSTORE` | ✅ | |
| `ZINTERCARD` | ❌ | Redis 7.0+ |
| `ZDIFF` | ❌ | |
| `ZDIFFSTORE` | ❌ | |
| `ZSCAN` | 🔧 | Basic cursor support |
| `ZMPOP` | ❌ | Redis 7.0+ |

## Key Commands

| Command | Status | Notes |
|---------|--------|-------|
| `DEL` | ✅ | |
| `UNLINK` | ✅ | Async deletion |
| `EXISTS` | ✅ | Supports multiple keys, counts duplicates |
| `EXPIRE` | ✅ | |
| `EXPIREAT` | ✅ | |
| `PEXPIRE` | ✅ | |
| `PEXPIREAT` | ✅ | |
| `EXPIRETIME` | ❌ | Redis 7.0+ |
| `PEXPIRETIME` | ❌ | Redis 7.0+ |
| `TTL` | ✅ | |
| `PTTL` | ✅ | |
| `PERSIST` | ✅ | |
| `TYPE` | ✅ | |
| `RENAME` | ✅ | |
| `RENAMENX` | ✅ | |
| `KEYS` | ✅ | Supports glob patterns |
| `SCAN` | ✅ | Cursor-based iteration with MATCH and COUNT |
| `RANDOMKEY` | ✅ | |
| `SORT` | 🔧 | Basic numeric/alpha sort |
| `SORT_RO` | ❌ | Read-only variant |
| `TOUCH` | ✅ | |
| `OBJECT ENCODING` | 🔧 | Reports Ferrite-specific encodings |
| `OBJECT REFCOUNT` | 🔧 | Always returns 1 |
| `OBJECT IDLETIME` | 🔧 | Approximate |
| `OBJECT FREQ` | 🔧 | Approximate |
| `OBJECT HELP` | ✅ | |
| `DUMP` | ❌ | RDB serialization not compatible |
| `RESTORE` | ❌ | |
| `COPY` | ❌ | Redis 6.2+ |
| `WAIT` | 🔧 | Basic replication wait |

## Server Commands

| Command | Status | Notes |
|---------|--------|-------|
| `PING` | ✅ | |
| `ECHO` | ✅ | |
| `INFO` | ✅ | All standard sections supported |
| `SELECT` | ✅ | 16 databases (0–15) |
| `DBSIZE` | ✅ | |
| `FLUSHDB` | ✅ | Supports ASYNC option |
| `FLUSHALL` | ✅ | Supports ASYNC option |
| `TIME` | ✅ | |
| `CONFIG GET` | 🔧 | Subset of Redis config parameters |
| `CONFIG SET` | 🔧 | Subset of Redis config parameters |
| `CONFIG REWRITE` | ❌ | |
| `CONFIG RESETSTAT` | ✅ | |
| `COMMAND` | ✅ | |
| `COMMAND COUNT` | ✅ | |
| `COMMAND DOCS` | ❌ | |
| `COMMAND INFO` | 🔧 | |
| `CLIENT ID` | ✅ | |
| `CLIENT LIST` | ✅ | |
| `CLIENT SETNAME` | ✅ | |
| `CLIENT GETNAME` | ✅ | |
| `CLIENT KILL` | 🔧 | |
| `CLIENT PAUSE` | ❌ | |
| `CLIENT UNPAUSE` | ❌ | |
| `CLIENT NO-EVICT` | ❌ | |
| `SWAPDB` | ❌ | |
| `SHUTDOWN` | ✅ | |
| `SLOWLOG` | ❌ | |
| `DEBUG` | 🔧 | Limited subcommands |
| `MEMORY USAGE` | ❌ | |
| `MEMORY DOCTOR` | ❌ | |
| `LATENCY` | ❌ | |
| `ACL` | 🔧 | Basic user/password auth |

## Pub/Sub Commands

| Command | Status | Notes |
|---------|--------|-------|
| `SUBSCRIBE` | ✅ | |
| `UNSUBSCRIBE` | ✅ | |
| `PSUBSCRIBE` | ✅ | Pattern-based subscription |
| `PUNSUBSCRIBE` | ✅ | |
| `PUBLISH` | ✅ | |
| `PUBSUB CHANNELS` | ✅ | |
| `PUBSUB NUMSUB` | ✅ | |
| `PUBSUB NUMPAT` | ✅ | |
| `PUBSUB SHARDCHANNELS` | ❌ | Redis 7.0+ |
| `PUBSUB SHARDNUMSUB` | ❌ | Redis 7.0+ |
| `SSUBSCRIBE` | ❌ | Sharded pub/sub (Redis 7.0+) |
| `SUNSUBSCRIBE` | ❌ | |
| `SPUBLISH` | ❌ | |

## Transaction Commands

| Command | Status | Notes |
|---------|--------|-------|
| `MULTI` | ✅ | |
| `EXEC` | ✅ | |
| `DISCARD` | ✅ | |
| `WATCH` | ✅ | Optimistic locking |
| `UNWATCH` | ✅ | |

## HyperLogLog Commands

| Command | Status | Notes |
|---------|--------|-------|
| `PFADD` | ✅ | |
| `PFCOUNT` | ✅ | |
| `PFMERGE` | ✅ | |
| `PFDEBUG` | ➖ | Internal Redis command |

## Scripting Commands

| Command | Status | Notes |
|---------|--------|-------|
| `EVAL` | ✅ | Lua 5.1 scripting |
| `EVALSHA` | ✅ | |
| `EVALRO` | ❌ | Read-only variant |
| `EVALSHA_RO` | ❌ | |
| `SCRIPT LOAD` | ✅ | |
| `SCRIPT EXISTS` | ✅ | |
| `SCRIPT FLUSH` | ✅ | |
| `SCRIPT KILL` | ❌ | |
| `FUNCTION LOAD` | ❌ | Redis 7.0+ Functions API |
| `FUNCTION LIST` | ❌ | |
| `FUNCTION CALL` | ❌ | |
| `FUNCTION DELETE` | ❌ | |
| `FUNCTION DUMP` | ❌ | |
| `FUNCTION RESTORE` | ❌ | |
| `FUNCTION STATS` | ❌ | |

## Bitmap Commands

| Command | Status | Notes |
|---------|--------|-------|
| `SETBIT` | ✅ | |
| `GETBIT` | ✅ | |
| `BITCOUNT` | ✅ | |
| `BITOP` | ✅ | AND, OR, XOR, NOT |
| `BITPOS` | ✅ | |
| `BITFIELD` | ❌ | |
| `BITFIELD_RO` | ❌ | |

## Stream Commands

| Command | Status | Notes |
|---------|--------|-------|
| `XADD` | ✅ | |
| `XLEN` | ✅ | |
| `XRANGE` | ✅ | |
| `XREVRANGE` | ✅ | |
| `XREAD` | 🔧 | Basic support |
| `XTRIM` | ✅ | MAXLEN and MINID |
| `XDEL` | ✅ | |
| `XINFO STREAM` | 🔧 | |
| `XINFO GROUPS` | 🔧 | |
| `XINFO CONSUMERS` | 🔧 | |
| `XGROUP CREATE` | ✅ | |
| `XGROUP SETID` | ✅ | |
| `XGROUP DELCONSUMER` | ✅ | |
| `XGROUP DESTROY` | ✅ | |
| `XREADGROUP` | 🔧 | |
| `XACK` | ✅ | |
| `XCLAIM` | 🔧 | |
| `XAUTOCLAIM` | ❌ | |
| `XPENDING` | 🔧 | |

## Cluster Commands

| Command | Status | Notes |
|---------|--------|-------|
| `CLUSTER INFO` | 🔧 | Basic cluster state |
| `CLUSTER NODES` | ❌ | |
| `CLUSTER SLOTS` | ❌ | Deprecated |
| `CLUSTER SHARDS` | ❌ | Redis 7.0+ |
| All other CLUSTER | ❌ | Ferrite uses its own cluster protocol |

## Geo Commands

| Command | Status | Notes |
|---------|--------|-------|
| `GEOADD` | ❌ | |
| `GEODIST` | ❌ | |
| `GEOHASH` | ❌ | |
| `GEOPOS` | ❌ | |
| `GEORADIUS` | ❌ | Deprecated |
| `GEOSEARCH` | ❌ | Redis 6.2+ |
| `GEOSEARCHSTORE` | ❌ | |

---

## Known Behavioral Differences

1. **Encoding types**: `OBJECT ENCODING` returns Ferrite-specific encoding names rather
   than Redis internal encodings (e.g., `ziplist`, `listpack`).

2. **Memory reporting**: `MEMORY USAGE` is not yet implemented; Ferrite uses a different
   memory allocation strategy based on epoch-based reclamation.

3. **Persistence**: Ferrite uses a HybridLog-based persistence model instead of RDB/AOF.
   `DUMP`/`RESTORE` are not compatible. `BGSAVE`/`BGREWRITEAOF` are no-ops.

4. **Cluster mode**: Ferrite has its own cluster protocol optimized for tiered storage.
   Redis Cluster protocol commands are not supported.

5. **Blocking commands**: `BLPOP`, `BRPOP`, and similar blocking commands have basic
   support but may behave differently under high contention.

---

## Running the Compatibility Tests

### Unit-level tests (no server required)

```bash
cargo test --test redis_compatibility
```

### Full integration report (requires built binary + redis-cli)

```bash
cargo build --release
./scripts/redis_compat_report.sh
```

### Generate a Markdown report file

```bash
REPORT_FILE=compat-report.md ./scripts/redis-compat-report.sh
```

### CI

Compatibility tests run automatically on every PR and on a weekly schedule.
See `.github/workflows/redis-compat.yml`.

---

_Last updated: 2025_
