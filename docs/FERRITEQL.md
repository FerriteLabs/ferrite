# FerriteQL Reference

FerriteQL is Ferrite's SQL-like query language for advanced data operations.

> 🔬 **Status: Experimental** — FerriteQL is under active development. Syntax and features may change.

## Quick Start

```bash
# Query keys matching a pattern
QUERY "SELECT * FROM keys WHERE pattern = 'user:*'"

# Count keys by type
QUERY "SELECT type, COUNT(*) FROM keys GROUP BY type"

# Get keys with TTL info
QUERY "SELECT key, ttl FROM keys WHERE ttl > 0 ORDER BY ttl ASC LIMIT 10"

# Delete expired keys
QUERY "DELETE FROM keys WHERE ttl = -2"
```

## Syntax Reference

### SELECT

```sql
SELECT [columns] FROM [source]
  [WHERE condition]
  [ORDER BY column [ASC|DESC]]
  [LIMIT n]
  [OFFSET n]
```

**Sources:**
- `keys` — All keys in the current database
- `keys:<pattern>` — Keys matching a glob pattern

**Columns:**
- `*` — All available fields
- `key` — The key name
- `value` — The stored value
- `type` — Data type (string, list, hash, set, zset)
- `ttl` — Time-to-live in seconds (-1 = no expiry, -2 = expired)
- `size` — Approximate memory usage in bytes
- `encoding` — Internal encoding type

### SET

```sql
SET key = 'value' [TTL seconds] [IF NOT EXISTS]
```

### DELETE

```sql
DELETE FROM keys WHERE key = 'mykey'
DELETE FROM keys WHERE pattern = 'temp:*'
```

### Aggregate Functions

| Function | Description |
|----------|------------|
| `COUNT(*)` | Count matching rows |
| `COUNT(DISTINCT column)` | Count unique values |
| `SUM(column)` | Sum numeric values |
| `AVG(column)` | Average of numeric values |
| `MIN(column)` | Minimum value |
| `MAX(column)` | Maximum value |

### WHERE Operators

| Operator | Example |
|----------|---------|
| `=` | `WHERE key = 'user:1'` |
| `!=` | `WHERE type != 'string'` |
| `>`, `<`, `>=`, `<=` | `WHERE ttl > 3600` |
| `LIKE` | `WHERE key LIKE 'user:%'` |
| `IN` | `WHERE type IN ('list', 'set')` |
| `BETWEEN` | `WHERE ttl BETWEEN 0 AND 3600` |
| `IS NULL` | `WHERE value IS NULL` |
| `AND`, `OR` | `WHERE type = 'string' AND ttl > 0` |

## Examples

### Find large keys
```sql
QUERY "SELECT key, type, size FROM keys ORDER BY size DESC LIMIT 20"
```

### Monitor expiring keys
```sql
QUERY "SELECT key, ttl FROM keys WHERE ttl BETWEEN 0 AND 60 ORDER BY ttl ASC"
```

### Key type distribution
```sql
QUERY "SELECT type, COUNT(*) as count FROM keys GROUP BY type ORDER BY count DESC"
```

### Subqueries (planned)
```sql
QUERY "SELECT * FROM keys WHERE size > (SELECT AVG(size) FROM keys)"
```

## QUERY Command Reference

| Command | Description |
|---------|------------|
| `QUERY "<sql>"` | Execute a FerriteQL query |
| `QUERY.JSON "<sql>"` | Execute and return JSON results |
| `QUERY.EXPLAIN "<sql>"` | Show query execution plan |
| `QUERY.PREPARE <name> "<sql>"` | Create a prepared statement |
| `QUERY.EXEC <name> [params...]` | Execute a prepared statement |
| `QUERY.HELP` | Show available FerriteQL commands |
| `QUERY.VERSION` | Show FerriteQL version |
