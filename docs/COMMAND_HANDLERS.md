# Command Handlers Reference

This document catalogs all command handlers in the Ferrite codebase, organized by category.

## Core Redis Commands

These handlers implement standard Redis-compatible commands and live in `src/commands/`.

| Module | Commands | Description |
|--------|----------|-------------|
| `strings.rs` | GET, SET, MGET, MSET, APPEND, INCR, DECR, etc. | String data type operations |
| `lists.rs` | LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, etc. | List data type operations |
| `hashes.rs` | HSET, HGET, HMSET, HMGET, HDEL, HGETALL, etc. | Hash data type operations |
| `sets.rs` | SADD, SREM, SMEMBERS, SINTER, SUNION, SDIFF, etc. | Set data type operations |
| `sorted_sets.rs` | ZADD, ZREM, ZRANGE, ZRANGEBYSCORE, ZRANK, etc. | Sorted set operations |
| `streams.rs` | XADD, XREAD, XRANGE, XLEN, XGROUP, XACK, etc. | Stream data type operations |
| `streams_extended.rs` | XCLAIM, XAUTOCLAIM, XPENDING, XTRIM, etc. | Extended stream operations |
| `bitmap.rs` | SETBIT, GETBIT, BITCOUNT, BITOP, BITFIELD, etc. | Bitmap operations |
| `geo.rs` | GEOADD, GEODIST, GEOHASH, GEOPOS, GEOSEARCH, etc. | Geospatial operations |
| `hyperloglog.rs` | PFADD, PFCOUNT, PFMERGE | Probabilistic counting |
| `pubsub.rs` | PUBLISH, SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, etc. | Pub/Sub messaging |
| `blocking.rs` | BLPOP, BRPOP, BLMOVE, BZPOPMIN, BZPOPMAX, etc. | Blocking operations |
| `scripting.rs` | EVAL, EVALSHA, SCRIPT LOAD/EXISTS/FLUSH | Lua scripting |
| `scripting_stub.rs` | — | Stub when scripting feature is disabled |
| `scan.rs` | SCAN, SSCAN, HSCAN, ZSCAN | Cursor-based iteration |
| `keys.rs` | DEL, EXISTS, EXPIRE, TTL, TYPE, RENAME, PERSIST, etc. | Key management |
| `latency.rs` | LATENCY LATEST, HISTORY, RESET | Latency monitoring |
| `acl_commands.rs` | ACL SETUSER, GETUSER, DELUSER, LIST, etc. | Access control |
| `client_commands.rs` | CLIENT ID, INFO, SETNAME, KILL, LIST, etc. | Client management |
| `config_commands.rs` | CONFIG GET, SET, RESETSTAT, REWRITE | Server configuration |
| `object_commands.rs` | OBJECT ENCODING, FREQ, IDLETIME, HELP | Object introspection |

## Command Parser & Executor

| Module | Description |
|--------|-------------|
| `parser/` | RESP protocol command parsing and argument extraction |
| `executor/` | Command dispatch, routing, and pipeline execution |

## Extension Handlers

These handlers implement Ferrite's advanced capabilities and live in `src/commands/handlers/`.

### Data Model Extensions

| Module | Commands | Description |
|--------|----------|-------------|
| `vector.rs` | FT.VSEARCH, FT.VADD, FT.VDEL, etc. | Vector similarity search (HNSW, IVF) |
| `vector_ingest.rs` | FT.VINGEST, FT.VBATCH | Batch vector ingestion |
| `semantic.rs` | FT.SEMCACHE, FT.SEMGET, FT.SEMPUT | Semantic caching for LLM workloads |
| `graph.rs` | GRAPH.QUERY, GRAPH.ADD, GRAPH.DELETE | Graph data model and Cypher queries |
| `timeseries.rs` | TS.ADD, TS.RANGE, TS.MRANGE, TS.DOWNSAMPLE | Time-series operations |
| `document.rs` | DOC.INSERT, DOC.FIND, DOC.UPDATE, DOC.DELETE | JSON document store |
| `crdt.rs` | CRDT.COUNTER, CRDT.SET, CRDT.MAP, CRDT.MERGE | Conflict-free replicated data types |
| `temporal.rs` | TEMPORAL.GET, TEMPORAL.RANGE, TEMPORAL.SNAPSHOT | Time-travel queries |

### Search & Query

| Module | Commands | Description |
|--------|----------|-------------|
| `query.rs` | FQL.EXEC, FQL.EXPLAIN, FQL.PREPARE | FerriteQL query execution |
| `autoindex.rs` | FT.AUTOINDEX, FT.INDEXRECOMMEND | ML-based automatic index recommendations |
| `views.rs` | VIEW.CREATE, VIEW.REFRESH, VIEW.DROP | Materialized views |
| `global_index.rs` | INDEX.CREATE, INDEX.SEARCH, INDEX.DROP | Global secondary indexes |

### AI & Machine Learning

| Module | Commands | Description |
|--------|----------|-------------|
| `inference.rs` | AI.INFER, AI.MODELLOAD, AI.MODELDEL | Model inference (ONNX) |
| `rag.rs` | RAG.SEARCH, RAG.INDEX, RAG.CONFIG | Retrieval-augmented generation |
| `conversation.rs` | CONV.ADD, CONV.GET, CONV.SUMMARIZE | Conversational memory |
| `agent.rs` | AGENT.MEMORY, AGENT.RECALL, AGENT.STORE | Agent memory management |
| `feature_store.rs` | FS.SET, FS.GET, FS.BATCH | ML feature store operations |
| `classify.rs` | AI.CLASSIFY, AI.TRAIN | Classification and training |

### Streaming & Events

| Module | Commands | Description |
|--------|----------|-------------|
| `pipeline.rs` | PIPELINE.CREATE, PIPELINE.RUN, PIPELINE.STATUS | Data pipeline management |
| `trigger.rs` | TRIGGER.CREATE, TRIGGER.DELETE, TRIGGER.LIST | Event-driven triggers |

### Cluster & Replication

| Module | Commands | Description |
|--------|----------|-------------|
| `cluster.rs` | CLUSTER INFO, NODES, MEET, FORGET, etc. | Cluster management |
| `replication_cmd.rs` | REPLCONF, PSYNC, WAIT | Replication protocol |
| `consensus.rs` | RAFT.ADD, RAFT.REMOVE, RAFT.STATUS | Raft consensus operations |
| `slots.rs` | CLUSTER ADDSLOTS, DELSLOTS, SETSLOT | Hash slot management |
| `scaling.rs` | SCALE.OUT, SCALE.IN, SCALE.STATUS | Online scaling operations |
| `federation.rs` | FED.QUERY, FED.JOIN, FED.CONFIG | Cross-cluster federation |

### Cloud & Multi-Region

| Module | Commands | Description |
|--------|----------|-------------|
| `cloud.rs` | CLOUD.STATUS, CLOUD.PROVISION, CLOUD.CONFIG | Cloud provider management |
| `multicloud.rs` | MCLOUD.REPLICATE, MCLOUD.SYNC, MCLOUD.STATUS | Multi-cloud replication |
| `s3.rs` | S3.GET, S3.PUT, S3.LIST, S3.DELETE | S3-compatible object storage |
| `edge.rs` | EDGE.SYNC, EDGE.STATUS, EDGE.CONFIG | Edge computing operations |
| `costoptimizer.rs` | COST.ANALYZE, COST.OPTIMIZE, COST.BUDGET | Cost optimization |

### Operations & Observability

| Module | Commands | Description |
|--------|----------|-------------|
| `server.rs` | INFO, PING, DBSIZE, FLUSHDB, FLUSHALL, etc. | Server management |
| `admin.rs` | ADMIN.BACKUP, ADMIN.RESTORE, ADMIN.STATUS | Administrative operations |
| `observe.rs` | OBS.TRACE, OBS.METRICS, OBS.SPAN | Observability commands |
| `analytics.rs` | ANALYTICS.QUERY, ANALYTICS.REPORT | Analytics operations |
| `audit.rs` | AUDIT.LOG, AUDIT.SEARCH, AUDIT.CONFIG | Audit trail management |
| `chaos.rs` | CHAOS.INJECT, CHAOS.STOP, CHAOS.STATUS | Chaos engineering |
| `locks.rs` | LOCK.ACQUIRE, LOCK.RELEASE, LOCK.EXTEND | Distributed locks |

### Enterprise & Governance

| Module | Commands | Description |
|--------|----------|-------------|
| `policy.rs` | POLICY.SET, POLICY.GET, POLICY.EVAL | Policy management |
| `policy_engine.rs` | PENG.EXEC, PENG.VALIDATE, PENG.AUDIT | Policy engine execution |
| `schema.rs` | SCHEMA.CREATE, SCHEMA.VALIDATE, SCHEMA.EVOLVE | Schema management |
| `contract.rs` | CONTRACT.DEFINE, CONTRACT.VERIFY | Data contracts |
| `lineage.rs` | LINEAGE.TRACE, LINEAGE.GRAPH | Data lineage tracking |

### Extensibility

| Module | Commands | Description |
|--------|----------|-------------|
| `wasm.rs` | WASM.LOAD, WASM.EXEC, WASM.UNLOAD | WebAssembly functions |
| `functions.rs` | FUNCTION LOAD, DELETE, LIST, CALL | Server-side functions |
| `marketplace.rs` | MARKET.INSTALL, MARKET.LIST, MARKET.SEARCH | Plugin marketplace |

### Networking & Protocol

| Module | Commands | Description |
|--------|----------|-------------|
| `protocol.rs` | HELLO, AUTH, SELECT | Protocol negotiation |
| `transaction.rs` | MULTI, EXEC, DISCARD, WATCH, UNWATCH | Transaction support |
| `gateway.rs` | GW.ROUTE, GW.CONFIG | API gateway routing |
| `mesh.rs` | MESH.QUERY, MESH.JOIN, MESH.STATUS | Data mesh operations |
| `smart_proxy_cmd.rs` | PROXY.CONFIG, PROXY.STATUS | Smart proxy commands |
| `optimizer.rs` | OPT.ANALYZE, OPT.SUGGEST | Query optimization |
| `version.rs` | VERSION | Server version information |
| `ebpf.rs` | EBPF.ATTACH, EBPF.STATUS | eBPF observability (experimental) |

## Adding a New Command

1. Add command variant to `src/commands/parser/`
2. Implement handler in `src/commands/handlers/` (extension) or `src/commands/` (core)
3. Register in `src/commands/executor/`
4. Add tests
5. Update this document
