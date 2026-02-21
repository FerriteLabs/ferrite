# Redis Compatibility Report

Ferrite targets wire-level compatibility with Redis 7.x. This document tracks which commands and features are compatible.

## Overall Compatibility: ~72%

> This percentage reflects the ratio of passing Redis TCL test cases against Ferrite.
> Run the compatibility suite yourself: `cargo test --test redis_compatibility --release`

## Command Coverage by Category

### Fully Compatible ✅

| Category | Commands | Coverage |
|----------|----------|----------|
| **Strings** | GET, SET, MGET, MSET, INCR, DECR, APPEND, STRLEN, GETRANGE, SETRANGE, SETNX, SETEX, PSETEX, GETSET, GETDEL, GETEX | 100% |
| **Lists** | LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX, LSET, LREM, LINSERT, LPOS, LTRIM | 100% |
| **Hashes** | HSET, HGET, HMSET, HMGET, HDEL, HEXISTS, HGETALL, HKEYS, HVALS, HLEN, HINCRBY, HINCRBYFLOAT, HSETNX, HRANDFIELD | 100% |
| **Sets** | SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SUNION, SINTER, SDIFF, SUNIONSTORE, SINTERSTORE, SDIFFSTORE, SRANDMEMBER, SPOP, SMOVE, SINTERCARD | 100% |
| **Sorted Sets** | ZADD, ZREM, ZSCORE, ZCARD, ZCOUNT, ZRANK, ZRANGE, ZRANGEBYSCORE, ZREVRANGE, ZREVRANGEBYSCORE, ZINCRBY, ZUNIONSTORE, ZINTERSTORE, ZRANDMEMBER, ZPOPMIN, ZPOPMAX, ZRANGESTORE, ZMSCORE, ZLEXCOUNT | 100% |
| **Keys** | DEL, EXISTS, EXPIRE, TTL, PTTL, PERSIST, TYPE, RENAME, RENAMENX, KEYS, SCAN, RANDOMKEY, UNLINK, OBJECT, SORT, DUMP, RESTORE, COPY, TOUCH, WAIT | 95% |
| **Server** | PING, ECHO, INFO, SELECT, DBSIZE, FLUSHDB, FLUSHALL, TIME, CONFIG GET/SET, COMMAND, CLIENT, DEBUG, SLOWLOG, SWAPDB, RESET, LOLWUT | 90% |
| **Pub/Sub** | SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBLISH, PUBSUB | 100% |
| **Transactions** | MULTI, EXEC, DISCARD, WATCH, UNWATCH | 100% |
| **Scripting** | EVAL, EVALSHA, SCRIPT LOAD/EXISTS/FLUSH | 90% |
| **HyperLogLog** | PFADD, PFCOUNT, PFMERGE | 100% |

### Partially Compatible 🧪

| Category | Status | Notes |
|----------|--------|-------|
| **Streams** | ~60% | XADD, XREAD, XRANGE work; XGROUP partially implemented |
| **Cluster** | ~40% | CLUSTER INFO, NODES, SLOTS work; full gossip protocol in beta |
| **ACL** | ~50% | Basic ACL SET/GET/WHOAMI; missing full ACL LOG, GENPASS |
| **Geo** | ~70% | GEOADD, GEODIST, GEOSEARCH work; GEORADIUS deprecated path missing |

### Not Yet Implemented ❌

| Category | Commands | Plan |
|----------|----------|------|
| **Modules** | MODULE LOAD/UNLOAD/LIST | v0.3.0 via WASM compatibility layer |
| **Functions** | FUNCTION LOAD/DUMP/DELETE/LIST | v0.4.0 |
| **Object** | OBJECT FREQ/IDLETIME/HELP | v0.2.0 |
| **Latency** | LATENCY LATEST/HISTORY/RESET | v0.2.0 |

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
