# Refactoring Guide

This document tracks recommended refactoring tasks that require careful
manual review before implementing.

## Large File Splits

These files exceed 2,500 lines and should be split into focused modules:

### 1. `src/commands/executor/advanced_ops.rs` (2,823 lines)

**Current contents** (mixed concerns):
- Tiering operations (lines 19–436)
- CDC operations (lines 437–892)
- Temporal/time-travel operations (lines 893–1124)
- Stream operations (lines 1125–1290)
- Geo operations (lines 1289+)

**Recommended split**:
```
src/commands/executor/
├── advanced_ops.rs      → keep as re-export hub
├── tiering_ops.rs       → tiering operations
├── cdc_ops.rs           → CDC operations
├── temporal_ops.rs      → time-travel queries
├── stream_ops.rs        → XADD, XLEN, XRANGE, etc.
└── geo_ops.rs           → GEOADD, GEODIST, etc.
```

### 2. `src/commands/blocking.rs` (3,049 lines)

**Current contents**:
- BlockingListManager + BLPOP/BRPOP/BLMOVE/BLMPOP
- BlockingSortedSetManager + BZPOPMIN/BZPOPMAX/BZMPOP
- BlockingStreamManager + XREAD BLOCK/XREADGROUP BLOCK

**Recommended split**:
```
src/commands/
├── blocking/
│   ├── mod.rs            → re-exports
│   ├── lists.rs          → BLPOP, BRPOP, BLMOVE, BLMPOP
│   ├── sorted_sets.rs    → BZPOPMIN, BZPOPMAX, BZMPOP
│   └── streams.rs        → XREAD BLOCK, XREADGROUP BLOCK
```

### 3. `crates/ferrite-core/src/persistence/backup/manager.rs` (2,596 lines)

**Current contents**:
- BackupManager configuration and lifecycle
- Incremental backup logic
- Restore logic
- Verification and validation
- Cloud storage integration

**Recommended split**:
```
crates/ferrite-core/src/persistence/backup/
├── manager.rs            → core lifecycle only (~800 lines)
├── incremental.rs        → incremental backup logic
├── restore.rs            → restore operations
└── verification.rs       → checksum and validation
```

## Blanket Allow Audit

The following modules use `#![allow(dead_code)]`. Each should be audited
to determine which items are genuinely staged for future use vs. truly dead:

- `cluster/mod.rs`
- `transaction/mod.rs`
- `auth/mod.rs`
- `observability/mod.rs`
- `embedded/mod.rs`
- `compatibility/mod.rs`

## Executor Dispatch

`src/commands/executor/mod.rs` (2,074 lines) uses a 286-arm `match`
statement. When approaching 400+ commands, consider migrating to a
HashMap-based dispatch table:

```rust
type CommandHandler = Box<dyn Fn(&CommandExecutor, Command, u8) -> BoxFuture<Frame>>;
lazy_static! {
    static ref DISPATCH: HashMap<&'static str, CommandHandler> = { ... };
}
```

This would improve compilation speed and make command registration dynamic
(useful for the plugin system).
