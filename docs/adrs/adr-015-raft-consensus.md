# ADR-015: Raft Consensus for Cluster Mode

## Status
Proposed

## Context

Ferrite's cluster mode currently uses a gossip-based protocol for:
- Node discovery and membership
- Failure detection
- Slot ownership propagation
- Leader election

While gossip is lightweight and eventually consistent, it has well-known limitations
for a distributed database:

1. **Split-brain risk**: Gossip cannot guarantee consensus during network partitions
2. **Convergence time**: Gossip propagation is O(log N) but non-deterministic
3. **No linearizable operations**: Slot migration and failover lack strong consistency
4. **Difficult to reason about**: No formal proof of correctness

Raft consensus provides strong consistency guarantees that are essential for:
- Leader election during failover (exactly one primary per shard)
- Slot migration coordination (atomic slot ownership transfer)
- Configuration changes (adding/removing nodes safely)

## Decision

### Hybrid Approach: Gossip + Raft

Rather than replacing gossip entirely, we propose a **hybrid architecture**:

```
┌──────────────────────────────────────────────────────┐
│                   Raft Consensus                     │
│  (Leader election, slot ownership, config changes)   │
│                                                      │
│  raft_group_1: [shard_0_primary, shard_0_replicas]   │
│  raft_group_2: [shard_1_primary, shard_1_replicas]   │
│  meta_group:   [all nodes — cluster metadata]        │
└──────────────────────────────────────────────────────┘
                        ▲
                        │ Strong consensus for critical ops
                        │
┌───────────────────────┴──────────────────────────────┐
│                   Gossip Protocol                    │
│  (Health monitoring, metrics propagation, discovery)  │
│                                                      │
│  Lightweight, best-effort, eventually consistent     │
└──────────────────────────────────────────────────────┘
```

### What Uses Raft
- **Leader election**: Exactly one primary per shard, no split-brain
- **Slot ownership**: Atomic slot transfers during rebalancing
- **Membership changes**: Adding/removing nodes with joint consensus
- **Configuration**: Cluster-wide config propagation

### What Stays Gossip
- **Health monitoring**: Heartbeats and failure suspicion
- **Metrics propagation**: Non-critical cluster statistics
- **Node discovery**: Initial cluster formation
- **Soft state**: Connection counts, memory usage, load metrics

### Implementation Plan

1. **Phase 1**: Integrate `openraft` crate as the Raft implementation
2. **Phase 2**: Raft-based leader election for each shard group
3. **Phase 3**: Raft-coordinated slot migration
4. **Phase 4**: Jepsen testing for correctness verification
5. **Phase 5**: Performance optimization (batched log entries, pipelining)

### Raft Group Topology

- **Meta group**: All cluster nodes participate (manages slot map, membership)
- **Shard groups**: Primary + replicas per shard (manages replication log)
- **Learner nodes**: Read replicas that receive log entries but don't vote

### Log Storage

Raft log entries stored in the same persistence layer (AOF/checkpoint):
- Reuse existing WAL infrastructure
- Shared compaction with data persistence
- Feature-gated: `--features raft` (not in default build)

## Consequences

### Positive
- **Correctness**: Formally proven consensus algorithm (no split-brain by construction)
- **Deterministic failover**: Leader election completes in bounded time
- **Linearizable slot migration**: No data loss during rebalancing
- **Testable**: Can use Jepsen/Maelstrom for correctness verification
- **Industry standard**: Raft is well-understood, documented, and battle-tested

### Negative
- **Latency**: Raft commits require majority ACK (adds ~1 RTT to writes in consensus path)
- **Complexity**: Multi-group Raft is significantly more complex than gossip
- **Quorum requirements**: Minimum 3 nodes for fault tolerance (gossip works with 2)
- **Network sensitivity**: Raft is sensitive to network latency variance

### Migration Path
- Gossip remains the default for backward compatibility
- Raft enabled via `cluster.consensus = "raft"` config
- Gradual rollout: gossip for discovery, Raft for consensus operations
- No data migration needed (Raft manages metadata, not data)

## References
- [Raft Paper](https://raft.github.io/raft.pdf)
- [openraft (Rust)](https://github.com/datafuselabs/openraft)
- [TiKV Raft Implementation](https://github.com/tikv/raft-rs)
- [Jepsen Testing](https://jepsen.io/)
- [Redis Cluster Gossip Protocol](https://redis.io/docs/reference/cluster-spec/#failure-detection)
