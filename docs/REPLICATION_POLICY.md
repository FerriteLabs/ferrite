# Moonshot Command Replication Policy

This document is the single source of truth for which moonshot commands
are replicated from primary to replica nodes.

## Replication Rules

| Family | Replicated Subcommands | Local-Only |
|--------|----------------------|------------|
| FN.* | LOAD, DROP, CALL | CALL_RO, LIST, STATS, SHOW, VERSIONS, HELP, SAVE, LOAD_FROM_STORE |
| MEM.* | PUT, FORGET, SUMMARIZE | GET, RECALL, STATS, HELP, SAVE, LOAD |
| CHR.* | SET, DEL, BRANCH, SNAPSHOT, ROLLBACK, MERGE, GC, RETENTION | GET, STATS, DIFF, BRANCHES, HISTORY, ASOF, KEYHISTORY, CONFIG, HELP, SAVE, LOAD |
| LUC.* | APPEND, DEL, FORGET, CHECKPOINT | LEN, HEAD, PROOF, VERIFY, CONSISTENCY, LEAVES, SIGNER, ROTATE, HELP, SAVE, LOAD |
| CON.* | GINC, GMERGE, PNINC, PNMERGE, SADD, SREM, SMERGE, LWWSET, LWWMERGE, MVSET, MVMERGE, ADDRULE | GVAL, PNVAL, SMEMBERS, LWWGET, MVGET, DVV, CLOCK, PEERS, SYNC, ENTROPY, ROUTE, RULES, HELP, SAVE, LOAD |
| PNG.* | ALLOC, FREE, MIGRATE | READ, STATS, TOPOLOGY, KEYS, NODE, POLICY, DETECT, BENCH, TIER, EVALUATE, TIERPOLICY, SIZING, HELP, SAVE, LOAD |

## Semantics

- **SAVE** is always local-only (operator-controlled persistence checkpoint)
- **LOAD** is always local-only (rehydrate from Store on this node)
- **HELP** is always local-only
- Write commands auto-persist to Store after successful execution
- Replicas re-execute replicated commands through the same handler dispatch
