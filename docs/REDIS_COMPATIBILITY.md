# Ferrite — Redis Compatibility Matrix

Ferrite aims to be a drop-in Redis replacement. This document tracks command-level
compatibility with Redis 7.x.

**Current Compatibility: ~92%** of tested commands passing
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
| `LCS` | ✅ | Longest common substring (Redis 7.0+) |

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
| `LPOS` | ✅ | Supports RANK, COUNT, MAXLEN options |
| `LTRIM` | ✅ | |
| `RPOPLPUSH` | ✅ | Deprecated in Redis 6.2, use LMOVE |
| `LMOVE` | ✅ | |
| `LMPOP` | ✅ | Redis 7.0+ multi-key pop |
| `BLPOP` | ✅ | Full blocking support |
| `BRPOP` | ✅ | Full blocking support |
| `BLMOVE` | ✅ | Blocking LMOVE |
| `BLMPOP` | ✅ | Redis 7.0+ blocking multi-key pop |

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
| `HRANDFIELD` | ✅ | Supports COUNT and WITHVALUES |
| `HSCAN` | ✅ | Full cursor-based iteration |
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
| `SINTERCARD` | ✅ | Redis 7.0+ intersection cardinality with LIMIT |
| `SINTERSTORE` | ✅ | |
| `SDIFF` | ✅ | |
| `SDIFFSTORE` | ✅ | |
| `SPOP` | ✅ | |
| `SRANDMEMBER` | ✅ | |
| `SMOVE` | ✅ | |
| `SSCAN` | ✅ | Full cursor-based iteration |

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
| `ZRANGESTORE` | ✅ | Store ZRANGE result |
| `ZINCRBY` | ✅ | |
| `ZPOPMIN` | ✅ | |
| `ZPOPMAX` | ✅ | |
| `BZPOPMIN` | ✅ | Blocking variant |
| `BZPOPMAX` | ✅ | Blocking variant |
| `ZRANDMEMBER` | ✅ | Supports COUNT and WITHSCORES |
| `ZUNIONSTORE` | ✅ | |
| `ZINTERSTORE` | ✅ | |
| `ZINTERCARD` | ✅ | Redis 7.0+ intersection cardinality with LIMIT |
| `ZDIFF` | ✅ | Supports WITHSCORES |
| `ZDIFFSTORE` | ✅ | |
| `ZSCAN` | ✅ | Full cursor-based iteration |
| `ZMPOP` | ✅ | Redis 7.0+ multi-key pop |

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
| `EXPIRETIME` | ✅ | Redis 7.0+ absolute expiry timestamp |
| `PEXPIRETIME` | ✅ | Redis 7.0+ millisecond variant |
| `TTL` | ✅ | |
| `PTTL` | ✅ | |
| `PERSIST` | ✅ | |
| `TYPE` | ✅ | |
| `RENAME` | ✅ | |
| `RENAMENX` | ✅ | |
| `KEYS` | ✅ | Supports glob patterns |
| `SCAN` | ✅ | Cursor-based iteration with MATCH and COUNT |
| `RANDOMKEY` | ✅ | |
| `SORT` | ✅ | Numeric/alpha sort with BY, GET, STORE, LIMIT |
| `SORT_RO` | ❌ | Read-only variant |
| `TOUCH` | ✅ | |
| `OBJECT ENCODING` | ✅ | Reports Ferrite-internal encoding names |
| `OBJECT REFCOUNT` | ✅ | Always returns 1 |
| `OBJECT IDLETIME` | 🔧 | Approximate |
| `OBJECT FREQ` | 🔧 | Approximate |
| `OBJECT HELP` | ✅ | |
| `DUMP` | ✅ | Ferrite serialization format |
| `RESTORE` | ✅ | Ferrite serialization format |
| `COPY` | ✅ | Redis 6.2+ with DESTINATION and REPLACE options |
| `WAIT` | ✅ | Replication wait |

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
| `CONFIG GET` | ✅ | Supports Redis-compatible config parameters |
| `CONFIG SET` | ✅ | Supports Redis-compatible config parameters |
| `CONFIG REWRITE` | ✅ | Rewrites config to file |
| `CONFIG RESETSTAT` | ✅ | |
| `COMMAND` | ✅ | |
| `COMMAND COUNT` | ✅ | |
| `COMMAND DOCS` | ✅ | Redis 7.0+ |
| `COMMAND INFO` | ✅ | |
| `CLIENT ID` | ✅ | |
| `CLIENT LIST` | ✅ | |
| `CLIENT SETNAME` | ✅ | |
| `CLIENT GETNAME` | ✅ | |
| `CLIENT KILL` | ✅ | |
| `CLIENT PAUSE` | ✅ | Supports WRITE and ALL modes |
| `CLIENT UNPAUSE` | ✅ | |
| `CLIENT NO-EVICT` | ❌ | |
| `SWAPDB` | ✅ | |
| `SHUTDOWN` | ✅ | |
| `SLOWLOG` | ✅ | GET, LEN, RESET |
| `DEBUG` | 🔧 | Limited subcommands |
| `MEMORY USAGE` | ✅ | Per-type size estimation |
| `MEMORY DOCTOR` | ✅ | Diagnostic report |
| `LATENCY` | ✅ | LATEST, HISTORY, RESET, GRAPH, DOCTOR |
| `ACL` | ✅ | Full ACL system with SET/GET/WHOAMI/LIST/LOG/GENPASS/DRYRUN |

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
| `PUBSUB SHARDCHANNELS` | ✅ | Redis 7.0+ |
| `PUBSUB SHARDNUMSUB` | ✅ | Redis 7.0+ |
| `SSUBSCRIBE` | 🔧 | Sharded pub/sub (Redis 7.0+), connection-level handling |
| `SUNSUBSCRIBE` | 🔧 | Sharded variant |
| `SPUBLISH` | ✅ | Sharded publish |

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
| `EVAL` | ✅ | Lua 5.4 scripting |
| `EVALSHA` | ✅ | |
| `EVALRO` | ✅ | Read-only variant |
| `EVALSHA_RO` | ✅ | Read-only variant |
| `SCRIPT LOAD` | ✅ | |
| `SCRIPT EXISTS` | ✅ | |
| `SCRIPT FLUSH` | ✅ | |
| `SCRIPT KILL` | ❌ | |
| `FUNCTION LOAD` | ✅ | Via WASM function registry |
| `FUNCTION LIST` | ✅ | |
| `FUNCTION CALL` | ✅ | Via FCALL |
| `FUNCTION DELETE` | ✅ | |
| `FUNCTION DUMP` | ❌ | |
| `FUNCTION RESTORE` | ❌ | |
| `FUNCTION STATS` | ✅ | |

## Bitmap Commands

| Command | Status | Notes |
|---------|--------|-------|
| `SETBIT` | ✅ | |
| `GETBIT` | ✅ | |
| `BITCOUNT` | ✅ | |
| `BITOP` | ✅ | AND, OR, XOR, NOT |
| `BITPOS` | ✅ | |
| `BITFIELD` | ✅ | GET, SET, INCRBY with OVERFLOW (WRAP, SAT, FAIL) |
| `BITFIELD_RO` | ✅ | Read-only variant |

## Stream Commands

| Command | Status | Notes |
|---------|--------|-------|
| `XADD` | ✅ | |
| `XLEN` | ✅ | |
| `XRANGE` | ✅ | |
| `XREVRANGE` | ✅ | |
| `XREAD` | ✅ | Full blocking and non-blocking |
| `XTRIM` | ✅ | MAXLEN and MINID |
| `XDEL` | ✅ | |
| `XINFO STREAM` | ✅ | |
| `XINFO GROUPS` | ✅ | |
| `XINFO CONSUMERS` | ✅ | |
| `XGROUP CREATE` | ✅ | |
| `XGROUP SETID` | ✅ | |
| `XGROUP DELCONSUMER` | ✅ | |
| `XGROUP DESTROY` | ✅ | |
| `XREADGROUP` | ✅ | Full consumer group support |
| `XACK` | ✅ | |
| `XCLAIM` | ✅ | |
| `XAUTOCLAIM` | ✅ | Redis 6.2+ auto-claim pending messages |
| `XPENDING` | ✅ | Full IDLE, consumer, count filtering |

## Cluster Commands

| Command | Status | Notes |
|---------|--------|-------|
| `CLUSTER INFO` | ✅ | Cluster state information |
| `CLUSTER NODES` | ❌ | Ferrite uses its own cluster protocol |
| `CLUSTER SLOTS` | ❌ | Deprecated |
| `CLUSTER SHARDS` | ❌ | Redis 7.0+ |
| All other CLUSTER | ❌ | Ferrite uses its own cluster protocol |

## Geo Commands

| Command | Status | Notes |
|---------|--------|-------|
| `GEOADD` | ✅ | Full geospatial indexing |
| `GEODIST` | ✅ | Meters, km, miles, feet |
| `GEOHASH` | ✅ | |
| `GEOPOS` | ✅ | |
| `GEORADIUS` | ✅ | Deprecated but supported |
| `GEOSEARCH` | ✅ | Redis 6.2+ full support |
| `GEOSEARCHSTORE` | ✅ | |

---

## Known Behavioral Differences

1. **Encoding types**: `OBJECT ENCODING` returns Ferrite-specific encoding names rather
   than Redis internal encodings (e.g., `ziplist`, `listpack`).

2. **Memory reporting**: `MEMORY USAGE` is not yet implemented; Ferrite uses a different
   memory allocation strategy based on epoch-based reclamation.

3. **Persistence**: Ferrite uses a HybridLog-based persistence model in addition to AOF.
   `DUMP`/`RESTORE` use Ferrite's native serialization format and are not
   cross-compatible with Redis RDB. `BGSAVE`/`BGREWRITEAOF` are no-ops.

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

_Last updated: 2026-03_
