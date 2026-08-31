# Take-Home Exercise — Distributed Systems Specialist

> **Time box: 4 hours.** We mean it. A focused 4-hour solution beats a polished weekend
> project. If you run out of time, leave TODOs explaining what you'd do next.

---

## Task: PN-Counter with δ-Propagation

Implement a **PN-counter** (positive-negative counter) CRDT with **δ-state propagation**:

1. **PN-counter.** A convergent replicated counter that supports both increment and decrement operations. Each replica maintains its own state and can compute the global counter value from its local state.

2. **δ-state propagation.** Instead of shipping the full state on every sync, replicas exchange only the *delta* (state changes since last sync). Implement:
   - `delta_increment(replica_id, amount)` — returns the δ-mutator for an increment.
   - `delta_decrement(replica_id, amount)` — returns the δ-mutator for a decrement.
   - `merge_delta(delta)` — applies a received δ-state to the local replica.
   - `merge_full(remote_state)` — full-state merge as a fallback.

3. **Anti-entropy.** Implement a simple anti-entropy mechanism: each replica tracks what it has sent to each peer (e.g., via a version vector or sequence number) so that only unsent deltas are transmitted.

4. **Convergence.** After all deltas are exchanged (in any order, with any duplication), all replicas must agree on the same counter value.

### Language

Use any language you're most productive in. Rust is preferred but not required — we're evaluating distributed-systems thinking, not language proficiency.

### Deliverables

| Artifact | Required |
|---|---|
| PN-counter implementation with δ-mutators | ✅ |
| Anti-entropy tracking (version vectors or equivalent) | ✅ |
| Convergence tests: reorder, duplicate, and drop deltas | ✅ |
| A brief convergence argument or proof sketch | ✅ |
| `README.md` with design notes and usage instructions | ✅ |
| Performance observations (throughput, delta size growth) | Optional |

### Evaluation Criteria

We review submissions on four axes, equally weighted:

1. **Convergence proof.** Can you articulate *why* replicas converge? A formal proof is ideal; a clear informal argument with identified invariants is acceptable. We want to see that you reason about correctness before writing code.

2. **Test coverage.** Tests should exercise:
   - Basic increment/decrement across replicas.
   - Out-of-order delta application.
   - Duplicate delta application (idempotence).
   - Network partition simulation (delayed deltas).
   - At least 3 replicas with concurrent operations.

3. **Performance awareness.** You don't need to optimise, but demonstrate awareness of:
   - Delta size growth and garbage collection of causal metadata.
   - Trade-offs between δ-state and full-state sync frequency.
   - Memory overhead per replica as the number of peers grows.

4. **Documentation.** Can a reviewer understand your design from the README and code comments alone? Are CRDT-specific choices (join-semilattice structure, δ-group decomposition) explained?

### What We Don't Care About

- Network transport implementation (simulate message passing in-process).
- Polished CLI or UI.
- Production hardening — focus on correctness and clarity.

### References

These are for context, not required reading:

- Almeida, Shoker, Baquero — "Delta State Replicated Data Types" (2018)
- Shapiro et al. — "A comprehensive study of Convergent and Commutative Replicated Data Types" (2011)
- Ferrite's Concord moonshot design notes (shared during the pairing interview)

---

## Submission

- **GitHub repository** (preferred): public or private (add `ferrite-hiring` as a collaborator).
- **Alternative submission**: if you cannot use GitHub, open a [GitHub Discussion](https://github.com/ferritelabs/ferrite/discussions) before starting to arrange another submission method; do not post submission materials publicly.

Include a `TIME_LOG.md` noting roughly how you spent your 4 hours (e.g., "1h research,
1h design, 1.5h implementation, 0.5h tests"). This helps us calibrate, not judge.

---

## Questions?

If anything is unclear, open a [GitHub Discussion](https://github.com/ferritelabs/ferrite/discussions). We'd rather answer a question than have you spend time guessing our intent. Report suspected vulnerabilities only through [GitHub private vulnerability reporting](https://github.com/ferritelabs/ferrite/security/advisories/new).
