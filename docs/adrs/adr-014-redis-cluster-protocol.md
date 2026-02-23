# ADR-014: Redis Cluster Protocol Compatibility

## Status
Proposed

## Context

Ferrite supports its own cluster mode with hash slot-based sharding (16384 slots),
but does **not** currently implement the Redis Cluster protocol wire format. This means:

- Existing Redis Cluster deployments cannot migrate to Ferrite without client changes
- Redis Cluster-aware clients (redis-py cluster, JedisCluster, ioredis cluster) won't work
- Cluster management tools (redis-cli --cluster) are incompatible

For Ferrite to be a true **drop-in replacement**, cluster protocol compatibility is essential
for organizations running Redis Cluster in production.

## Decision

Implement Redis Cluster protocol compatibility in phases:

### Phase 1: CLUSTER Command Responses (Already Partial)
Ensure all `CLUSTER` subcommands return Redis-compatible responses:
- `CLUSTER INFO` — already implemented
- `CLUSTER NODES` — needs Redis-format node strings
- `CLUSTER SLOTS` — needs exact Redis response format
- `CLUSTER MYID` — node ID in Redis format
- `CLUSTER SHARDS` — Redis 7.0+ format

### Phase 2: `-MOVED` and `-ASK` Redirections
When a client sends a command to the wrong node:
- Return `-MOVED <slot> <ip>:<port>` for permanent slot ownership
- Return `-ASK <slot> <ip>:<port>` during slot migration
- Handle `ASKING` command for migration-in-progress reads

This enables Redis Cluster-aware clients to work without modification.

### Phase 3: Cluster Bus Protocol
Implement the Redis Cluster binary bus protocol (port + 10000):
- Gossip-based node discovery (PING/PONG with cluster state)
- Failure detection and voting
- Slot migration coordination
- Config epoch management

### Phase 4: Migration Tooling
- `ferrite-migrate --from-redis-cluster` tool for live migration
- Dual-write proxy for gradual cutover
- Slot-by-slot migration with progress tracking

### Compatibility Target
- Redis Cluster protocol version 1 (Redis 3.0+)
- Redis 7.0+ CLUSTER SHARDS response format
- Support for cluster-aware clients: redis-py, ioredis, JedisCluster, Lettuce, redis-rs

## Consequences

### Positive
- Zero-migration adoption for Redis Cluster users
- All cluster-aware clients work without modification
- Standard Redis cluster management tools (redis-cli --cluster) work
- Significant competitive advantage over alternatives without cluster compat

### Negative
- Significant implementation effort (cluster bus protocol is complex)
- Must maintain compatibility as Redis evolves the protocol
- Testing matrix explodes (N clients × M Redis versions × cluster topologies)
- May constrain Ferrite's own cluster innovations

### Risks
- Redis Cluster protocol has undocumented behaviors that only surface under edge cases
- Performance overhead of protocol translation layer
- Split-brain handling differences between Redis gossip and Ferrite's approach

## References
- [Redis Cluster Specification](https://redis.io/docs/reference/cluster-spec/)
- [Redis Cluster Tutorial](https://redis.io/docs/management/scaling/)
- [Garnet Cluster Support](https://microsoft.github.io/garnet/docs/cluster/)
