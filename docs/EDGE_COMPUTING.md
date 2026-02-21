# Edge Computing Mode

Run Ferrite on edge devices with local-first data, automatic sync, and resource-aware operation.

> 🔬 **Status: Experimental** — Edge computing features are under active development.

## Overview

Ferrite's edge mode is designed for:
- **IoT gateways** — Collect and process sensor data locally
- **Retail/POS systems** — Operate during network outages
- **Mobile backends** — Local-first with cloud sync
- **CDN edge nodes** — Cache and process at the edge

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ Edge Node 1 │────▶│ Cloud Hub   │◀────│ Edge Node 2 │
│ (local DB)  │     │ (primary)   │     │ (local DB)  │
└─────────────┘     └─────────────┘     └─────────────┘
       │                   │                    │
  Local reads         Sync engine          Local reads
  Local writes     Conflict resolution     Local writes
  Offline-capable   CRDT merging         Offline-capable
```

The internal edge node architecture uses four cooperating components:

```
┌────────────────────────────────┐     ┌─────────────────────┐
│       Edge Node                │     │   Cloud Ferrite      │
│  ┌──────────┐ ┌────────────┐  │     │                     │
│  │ Ferrite  │ │  Sync      │◄─┼────►│  Sync Endpoint      │
│  │ Embedded │ │  Engine    │  │     │                     │
│  └──────────┘ └────────────┘  │     └─────────────────────┘
│  ┌──────────┐ ┌────────────┐  │
│  │ Offline  │ │  Resource  │  │
│  │ Queue    │ │  Monitor   │  │
│  └──────────┘ └────────────┘  │
└────────────────────────────────┘
```

## Quick Start

```toml
# ferrite-edge.toml
[server]
port = 6379

[edge]
enabled = true
mode = "edge"                    # "edge" or "hub"
hub_url = "redis://hub:6379"    # Cloud hub address
sync_interval = "30s"           # Sync frequency
offline_queue_size = 10000      # Max queued operations
conflict_resolution = "lww"     # last-writer-wins

[edge.resources]
memory_limit = "128mb"          # Constrained for edge
storage_limit = "1gb"
cpu_budget = "low"              # low/medium/high
```

```bash
# Start in edge mode
ferrite --config ferrite-edge.toml
```

### Programmatic Usage (Rust)

```rust,ignore
use ferrite::edge::{EdgeNode, EdgeConfig, SyncPolicy};

let node = EdgeNode::new(EdgeConfig {
    node_id: "edge-001".to_string(),
    max_memory: 50 * 1024 * 1024, // 50MB
    sync_policy: SyncPolicy::Opportunistic,
    ..Default::default()
});

// Works fully offline
node.set("sensor:temp", "22.5")?;
let temp = node.get("sensor:temp")?;

// Syncs when connectivity is available
node.sync().await?;
```

## Features

### Offline-First Operation

All reads and writes work without network connectivity. Operations are queued in an offline queue and synced when connectivity returns. The queue is backed by a persistent journal (configurable via `queue_path`) to survive restarts.

- Queue capacity is configurable (`max_offline_queue`, default 10,000 ops)
- When the queue is full, the oldest operations are dropped to make room
- Automatic retry with exponential backoff on sync failure

### Conflict Resolution

| Strategy | Enum Value | Description |
|----------|-----------|------------|
| Last-Writer-Wins | `LastWriteWins` | Timestamp-based — most recent write wins |
| CRDT Merge | `CrdtMerge` | Conflict-free replicated data types (preferred) |
| Cloud Wins | `CloudWins` | Remote/cloud value always takes precedence |
| Edge Wins | `EdgeWins` | Local/edge value always takes precedence |

Conflicts are detected using **vector clocks** for causal ordering. When two operations on the same key are concurrent (neither causally before the other), the configured strategy resolves the conflict.

### Resource Monitoring

Edge mode actively monitors device resources and takes adaptive action:

| Resource Level | Memory | Disk | Action |
|---------------|--------|------|--------|
| Normal | <80% | <70% | No action |
| Warning | 80-90% | 70-85% | Sync & offload / compact |
| High | 90-95% | 85-95% | Evict LRU / compress data |
| Critical | >95% | >95% | Reject new writes |

Resource thresholds trigger automatic responses:
- **Memory pressure** → triggers LRU eviction
- **Disk usage** → prunes old sync logs, compacts storage
- **Network quality** → adjusts sync frequency
- **Battery level** → reduces background sync (when available)

### Sync Protocol

The sync protocol uses vector clocks and delta synchronization:

1. Edge node accumulates write operations in the offline queue
2. On sync interval (or manual trigger):
   - Builds a `SyncMessage` with all pending operations and the current vector clock
   - Sends batch to hub as a `DeltaSync` message
   - Hub resolves conflicts using vector clock comparison and responds with merged state
   - Edge applies remote changes and merges vector clocks
3. Acknowledged operations are pruned from the queue

Sync message types:
- **FullSync** — Complete state transfer (initial sync or recovery)
- **DeltaSync** — Incremental changes only (normal operation)
- **Ack** — Acknowledgement of received operations
- **ConflictReport** — Notification of detected conflicts

### Compact Storage

Edge mode uses memory-optimized value representations:

| Value Size | Storage | Overhead |
|-----------|---------|----------|
| ≤23 bytes | Inline (stack-allocated) | 0 bytes heap |
| 24+ bytes | Heap-allocated (`Bytes`) | Normal |
| 64+ bytes | LZ4 compressed (if enabled) | Compression metadata |

Configuration presets for different device classes:

| Preset | Memory | Max Keys | Compression | Use Case |
|--------|--------|----------|-------------|----------|
| `minimal()` | 4 MB | 10,000 | Off | Microcontrollers |
| `iot()` | 2 MB | 1,000 | Max (level 9) | Constrained IoT sensors |
| `mobile()` | 32 MB | 50,000 | On (level 6) | Mobile/tablet apps |
| `standard()` | 16 MB | 100,000 | Off | Default edge |
| `full()` | 64 MB | 1,000,000 | On (level 3) | Powerful edge gateways |

## Commands

| Command | Description |
|---------|------------|
| `EDGE.STATUS` | Current edge mode status and sync state |
| `EDGE.SYNC` | Trigger immediate sync to hub |
| `EDGE.QUEUE` | Show pending sync queue size |
| `EDGE.CONFLICTS` | List unresolved conflicts |
| `EDGE.RESOURCES` | Current resource usage |

## IoT Sensor Ingestion

Edge mode includes a specialized sensor data pipeline with time-series optimizations:

```rust,ignore
use ferrite::embedded::iot::*;

let ingestion = SensorIngestion::new(SensorConfig {
    max_readings_per_sensor: 1000,
    auto_aggregate: true,
    ..Default::default()
});

// Ingest a sensor reading
ingestion.ingest(SensorReading {
    sensor_id: "temp-001".into(),
    value: 22.5,
    unit: "celsius".into(),
    timestamp: None, // auto-filled
    metadata: Default::default(),
});

// Get running aggregates (count, min, max, avg, sum)
let stats = ingestion.aggregate("temp-001");

// Batch ingestion (single lock acquisition)
ingestion.ingest_batch(batch_of_readings);
```

### Edge Aggregation

Pre-aggregate sensor data into time-bucketed summaries before syncing to cloud, reducing bandwidth:

```rust,ignore
use ferrite::embedded::iot::EdgeAggregator;

let aggregator = EdgeAggregator::new(
    Duration::from_secs(60),  // 1-minute buckets
    100,                       // max pending buckets
);

aggregator.add_reading("temp", 22.5, timestamp);

// Flush and upload completed buckets
aggregator.flush();
let buckets = aggregator.drain_pending();
```

## Use Cases

### IoT Gateway

```bash
# Collect sensor readings locally
SET sensor:temp:001 "23.5" EX 86400
LPUSH sensor:log:001 '{"temp":23.5,"ts":1708300000}'

# Sync to cloud hub periodically
EDGE.SYNC
```

### Retail POS

```bash
# Process transactions offline
HINCRBY store:42:daily_revenue 2025-02-19 4999
LPUSH store:42:transactions '{"id":"tx-001","amount":49.99}'

# When network returns, sync automatically
EDGE.STATUS
# sync_status: syncing
# pending_ops: 142
# last_sync: 2025-02-19T10:30:00Z
```

## Crate Structure

Edge computing spans two crates:

| Crate | Module | Purpose |
|-------|--------|---------|
| `ferrite-cloud` | `edge::mod` | Edge node, config, sync policy, offline queue |
| `ferrite-cloud` | `edge::offline_sync` | Offline-first sync engine with vector clocks |
| `ferrite-cloud` | `edge::sync_protocol` | CRDT sync messages, vector clock implementation |
| `ferrite-cloud` | `edge::resource_monitor` | Resource monitoring and adaptive actions |
| `ferrite-core` | `embedded::edge` | Compact storage engine, inline/compressed values |
| `ferrite-core` | `embedded::iot` | Sensor ingestion, aggregation, IoT sync manager |
