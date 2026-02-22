---
name: Performance Regression
about: Report a performance degradation or regression
title: "[Perf] "
labels: performance
---

## Description
Describe the performance regression you've observed.

## Affected Operation
Which operations are affected?
- [ ] GET / read operations
- [ ] SET / write operations
- [ ] Vector search (VECTOR.SEARCH)
- [ ] Persistence (AOF / checkpoints)
- [ ] Replication
- [ ] Network / connection handling
- [ ] Other: ___

## Benchmark Results

**Before** (version/commit):
```
Operation: 
Throughput: 
P50 latency: 
P99 latency: 
```

**After** (version/commit):
```
Operation: 
Throughput: 
P50 latency: 
P99 latency: 
```

## Steps to Reproduce
1. Build with: `cargo build --release ...`
2. Run benchmark: `cargo bench --bench ...`
3. Or use external tool: `redis-benchmark -t ... -n ...`

## Environment
- Ferrite version (or commit SHA):
- OS:
- CPU:
- Memory:
- Rust version:
- Build flags / features:

## Additional Context
Any additional context, profiling data (flamegraphs, perf output), or configuration details.
