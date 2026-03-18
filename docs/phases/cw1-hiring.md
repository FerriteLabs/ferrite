# Hiring Plan (CW1)

> The portfolio assumes ≥ 1 senior Rust engineer added before Wave 2 and an additional
> distributed-systems specialist before Wave 3 Concord. This doc is the public-facing
> brief used to recruit them.

## Roles

### Senior Rust Engineer — Storage / Runtime

**Goal**: take primary ownership of one of Mnemo, Forge, or Lucidity through GA.

Required:
- Strong Rust (3+ yrs production), comfortable with `unsafe`, lock-free programming.
- Experience with epoch-based or hazard-pointer reclamation, or willingness to learn quickly.
- Familiarity with at least one of: Wasmtime, embedded vector indexes, Merkle/audit logs.
- Public OSS contributions (commits to Tokio, Wasmtime, FoundationDB, Sled, redb, etc.).

Nice-to-have:
- io_uring experience.
- Cryptography exposure (Halo2, ML-DSA, COSE).

### Distributed Systems Specialist — Concord

**Goal**: lead Concord (M5) from spike through GA.

Required:
- Production CRDT, Paxos, or Raft experience.
- TLA+ or P (formal methods) for at least one prior protocol.
- Comfort with gossip, anti-entropy, vector clocks.

Nice-to-have:
- Multi-region operations experience (Riak, ScyllaDB, Cosmos DB, FoundationDB).

## Compensation philosophy

Public salary band, options grant aligned with the open-source-friendly default
(non-restrictive, employee-friendly exercise window). Both roles are remote-first.

## Process

1. Async take-home (Rust): implement a small but real component matching the role
   (e.g. WIT-bound function host for the Rust role; PN-counter + δ-propagation for
   the dist-sys role). Time-boxed at 4 hours.
2. Pairing interview on the take-home.
3. System-design interview tied to the relevant moonshot's architecture.
4. References + offer.

Total candidate time investment ≤ 8 hours. Total elapsed ≤ 2 weeks.

## Funding gate

Both hires require either:
- Operating budget runway ≥ 12 months at current burn.
- A signed enterprise contract or grant covering ≥ 50% of fully-loaded cost.

If neither is true at the relevant gate (`m1-p4-alpha` triggers Wave-2 hiring decision),
substitute paid contractors for fixed-scope phases instead of full-time hires.

## Public-facing artifact

Job posts published at `ferrite-docs/website/jobs/` plus standard channels
(LinkedIn, This Week in Rust, hn:hiring). Template lives in `RECRUITMENT_TEMPLATES.md`.
