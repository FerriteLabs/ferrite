# Redis Cluster Compatibility

Ferrite implements the Redis Cluster protocol for horizontal scaling. This document tracks compatibility with Redis Cluster commands and behaviors.

## Supported Cluster Commands

| Command | Status | Notes |
|---------|--------|-------|
| CLUSTER INFO | ✅ | Full cluster state reporting |
| CLUSTER NODES | ✅ | Node list with slots and flags |
| CLUSTER SLOTS | ✅ | Slot-to-node mapping |
| CLUSTER SHARDS | ✅ | Redis 7.0+ shard information |
| CLUSTER KEYSLOT | ✅ | CRC16 hash slot calculation |
| CLUSTER COUNTKEYSINSLOT | ✅ | Count keys in a slot |
| CLUSTER GETKEYSINSLOT | ✅ | List keys in a slot |
| CLUSTER MEET | ✅ | Add node to cluster |
| CLUSTER ADDSLOTS | ✅ | Assign slots to node |
| CLUSTER DELSLOTS | ✅ | Remove slots from node |
| CLUSTER FLUSHSLOTS | ✅ | Remove all slots |
| CLUSTER SETSLOT | ✅ | IMPORTING/MIGRATING/STABLE/NODE |
| CLUSTER FAILOVER | ✅ | FORCE/TAKEOVER support |
| CLUSTER MYID | ✅ | Node unique identifier |
| CLUSTER RESET | ✅ | SOFT/HARD reset |
| CLUSTER REPLICATE | ✅ | Configure as replica |
| CLUSTER SAVECONFIG | ✅ | Persist cluster config |
| CLUSTER SET-CONFIG-EPOCH | ✅ | Set config epoch |
| CLUSTER COUNT-FAILURE-REPORTS | ✅ | Failure report count |
| CLUSTER LINKS | ✅ | Cluster bus connections |

## Cluster Behaviors

| Behavior | Status | Notes |
|----------|--------|-------|
| Hash slot routing | ✅ | CRC16 mod 16384 |
| MOVED redirections | 🧪 | Beta — works for known slot owners |
| ASK redirections | 🧪 | Beta — during slot migration |
| Cluster bus (gossip) | 🧪 | Beta — heartbeat and failure detection |
| Automatic failover | 🧪 | Beta — Raft-based leader election |
| Slot migration | 🧪 | Beta — online slot movement |
| Read replicas | 🧪 | Beta — READONLY mode |
| Cross-slot multi-key | ❌ | Planned for v0.3.0 |

## Migration from Redis Cluster

To migrate an existing Redis Cluster to Ferrite:

1. Deploy Ferrite nodes alongside Redis nodes
2. Use `CLUSTER MEET` to add Ferrite nodes to the cluster
3. Migrate slots using `CLUSTER SETSLOT` and `MIGRATE`
4. Remove Redis nodes once migration is complete

> ⚠️ Full cluster migration tooling is planned for v1.0.0
