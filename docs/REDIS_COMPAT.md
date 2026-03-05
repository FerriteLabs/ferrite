# Redis Compatibility Report

Ferrite targets wire-level compatibility with Redis 7.x. This document tracks which commands and features are compatible.

## Overall Compatibility: ~92%

> This percentage reflects the ratio of passing Redis TCL test cases against Ferrite.
> Run the compatibility suite yourself: `cargo test --test redis_compatibility --release`

## Command Coverage by Category

### Fully Compatible ✅

| Category | Commands | Coverage |
|----------|----------|----------|
| **Strings** | GET, SET, MGET, MSET, INCR, DECR, APPEND, STRLEN, GETRANGE, SETRANGE, SETNX, SETEX, PSETEX, GETSET, GETDEL, GETEX, LCS | 100% |
| **Lists** | LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX, LSET, LREM, LINSERT, LPOS, LTRIM, LMOVE, LMPOP, BLPOP, BRPOP, BLMOVE, BLMPOP | 100% |
| **Hashes** | HSET, HGET, HMSET, HMGET, HDEL, HEXISTS, HGETALL, HKEYS, HVALS, HLEN, HINCRBY, HINCRBYFLOAT, HSETNX, HRANDFIELD, HSCAN | 100% |
| **Sets** | SADD, SREM, SMEMBERS, SISMEMBER, SMISMEMBER, SCARD, SUNION, SINTER, SDIFF, SUNIONSTORE, SINTERSTORE, SDIFFSTORE, SRANDMEMBER, SPOP, SMOVE, SINTERCARD, SSCAN | 100% |
| **Sorted Sets** | ZADD, ZREM, ZSCORE, ZMSCORE, ZCARD, ZCOUNT, ZLEXCOUNT, ZRANK, ZREVRANK, ZRANGE, ZRANGEBYSCORE, ZREVRANGE, ZREVRANGEBYSCORE, ZRANGEBYLEX, ZRANGESTORE, ZINCRBY, ZPOPMIN, ZPOPMAX, BZPOPMIN, BZPOPMAX, ZUNIONSTORE, ZINTERSTORE, ZINTERCARD, ZDIFF, ZDIFFSTORE, ZRANDMEMBER, ZSCAN, ZMPOP | 100% |
| **Keys** | DEL, EXISTS, EXPIRE, EXPIREAT, PEXPIRE, PEXPIREAT, EXPIRETIME, PEXPIRETIME, TTL, PTTL, PERSIST, TYPE, RENAME, RENAMENX, KEYS, SCAN, RANDOMKEY, UNLINK, OBJECT, SORT, DUMP, RESTORE, COPY, TOUCH, WAIT | 98% |
| **Server** | PING, ECHO, INFO, SELECT, DBSIZE, FLUSHDB, FLUSHALL, TIME, CONFIG GET/SET/REWRITE, COMMAND, COMMAND DOCS, CLIENT, CLIENT PAUSE/UNPAUSE, DEBUG, SLOWLOG, SWAPDB, SHUTDOWN, MEMORY USAGE/DOCTOR, LATENCY, ACL, RESET, LOLWUT | 95% |
| **Pub/Sub** | SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBLISH, PUBSUB, SPUBLISH, PUBSUB SHARDCHANNELS/SHARDNUMSUB | 95% |
| **Transactions** | MULTI, EXEC, DISCARD, WATCH, UNWATCH | 100% |
| **Scripting** | EVAL, EVALSHA, EVAL_RO, EVALSHA_RO, SCRIPT LOAD/EXISTS/FLUSH, FUNCTION LOAD/LIST/DELETE/STATS, FCALL | 90% |
| **HyperLogLog** | PFADD, PFCOUNT, PFMERGE | 100% |
| **Bitmaps** | SETBIT, GETBIT, BITCOUNT, BITOP, BITPOS, BITFIELD, BITFIELD_RO | 100% |
| **Geo** | GEOADD, GEODIST, GEOHASH, GEOPOS, GEORADIUS, GEOSEARCH, GEOSEARCHSTORE | 100% |
| **Streams** | XADD, XLEN, XRANGE, XREVRANGE, XREAD, XTRIM, XDEL, XINFO, XGROUP, XREADGROUP, XACK, XCLAIM, XAUTOCLAIM, XPENDING | 95% |

### Partially Compatible 🧪

| Category | Status | Notes |
|----------|--------|-------|
| **Cluster** | ~40% | CLUSTER INFO works; NODES, SLOTS, SHARDS planned for wire compat |
| **Sharded Pub/Sub** | ~70% | SPUBLISH works; SSUBSCRIBE/SUNSUBSCRIBE partial (connection-level) |

### Not Yet Implemented ❌

| Category | Commands | Plan |
|----------|----------|------|
| **Cluster Wire Protocol** | CLUSTER NODES/SLOTS/SHARDS/MEET | v0.4.0 — wire compatibility layer |
| **Hash Field Expiry** | HEXPIRE, HPERSIST, HTTL, HPTTL | v0.4.0 — Redis 7.4+ feature |
| **Misc** | SORT_RO, CLIENT NO-EVICT, SCRIPT KILL, FUNCTION DUMP/RESTORE | v0.3.0 |

## Running the Compatibility Suite

```bash
# Unit-level compatibility tests
cargo test --test redis_compatibility --release

# Full integration test against live Redis
# (requires Redis 7 on port 6380 and Ferrite on port 6379)
./scripts/run-redis-tcl-tests.sh

# CI runs this automatically on PRs — see .github/workflows/redis-compat.yml
```

## Improving Compatibility

Found an incompatibility? Please [open an issue](https://github.com/ferritelabs/ferrite/issues/new?template=bug_report.md&title=[Compat]) with:
1. The Redis command and arguments used
2. Expected behavior (from Redis)
3. Actual behavior (from Ferrite)
