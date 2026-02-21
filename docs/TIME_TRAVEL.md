# Time-Travel Queries

Query data at any point in time for debugging, auditing, and recovery.

> 🔬 **Status: Experimental** — Requires HybridLog storage backend.

## Overview

Ferrite's time-travel capability allows you to:
- **View history** — See all changes to a key over time
- **Point-in-time reads** — Read the value of a key as it was at a specific moment
- **Diff** — Compare values at two different points in time
- **Restore** — Revert a key to a previous value

## Quick Start

```bash
# Enable time-travel (requires HybridLog backend)
CONFIG SET storage.backend hybridlog
CONFIG SET temporal.enabled true
CONFIG SET temporal.retention 7d

# Write some data
SET user:1:name "Alice"
SET user:1:name "Alice Smith"
SET user:1:name "Alice Johnson"

# View history
HISTORY user:1:name
# 1) timestamp:1708300000 value:"Alice"
# 2) timestamp:1708300100 value:"Alice Smith"
# 3) timestamp:1708300200 value:"Alice Johnson"

# Get value from 1 hour ago
HISTORY.FIRST user:1:name

# Compare two points in time
DIFF user:1:name 1708300000 1708300200

# Restore to previous value
RESTORE.FROM user:1:name 1708300000
GET user:1:name  # "Alice"
```

## Time Format

Commands accept multiple time formats:

| Format | Example | Description |
|--------|---------|-------------|
| Relative | `-1h`, `-30m`, `-7d` | Offset from now |
| Unix timestamp | `1708300000` | Seconds since epoch |
| Unix millis | `1708300000000` | Milliseconds since epoch |

### Relative Units

| Unit | Meaning |
|------|---------|
| `s` | Seconds |
| `m` | Minutes |
| `h` | Hours |
| `d` | Days |
| `w` | Weeks |

## Command Reference

| Command | Description |
|---------|------------|
| `HISTORY <key> [SINCE ts] [UNTIL ts] [COUNT n]` | List versions of a key |
| `HISTORY.COUNT <key> [SINCE ts] [UNTIL ts]` | Count versions |
| `HISTORY.FIRST <key>` | Oldest known version |
| `HISTORY.LAST <key>` | Most recent version |
| `DIFF <key> <ts1> <ts2>` | Compare values at two times |
| `RESTORE.FROM <key> <ts>` | Restore to value at time |
| `TEMPORAL CONFIG GET <opt>` | Get temporal config |
| `TEMPORAL CONFIG SET <opt> <val>` | Set temporal config |
| `TEMPORAL HELP` | Show command help |

## Configuration

```toml
[temporal]
enabled = true
retention = "7d"          # How long to keep history
max_versions_per_key = 1000  # Max versions to retain
```

## Use Cases

### Debugging
Find when a value changed unexpectedly:
```bash
HISTORY user:1:email SINCE -24h
```

### Auditing
Track all modifications for compliance:
```bash
HISTORY.COUNT sensitive:data SINCE -30d
```

### Recovery
Restore accidentally overwritten data:
```bash
RESTORE.FROM production:config -1h
```
