# Moonshot Phase Roadmaps

Per-moonshot, per-phase implementation plans. Each roadmap is the engineering team's
source of truth for executing one moonshot from spike through GA.

## Index

| Doc | Moonshot | Status |
|---|---|---|
| [m1-mnemo-roadmap.md](./m1-mnemo-roadmap.md) | Mnemo — Agent Memory OS | Roadmap published; ADR-018 done |
| [m2-forge-roadmap.md](./m2-forge-roadmap.md) | Forge — WASM In-DB Functions | Roadmap published; ADR-019 done |
| [m3-lucidity-roadmap.md](./m3-lucidity-roadmap.md) | Lucidity — Verifiable Audit Plane | Roadmap published; ADR-020 (P0) pending |
| [m4-chronicle-roadmap.md](./m4-chronicle-roadmap.md) | Chronicle — Branchable State | Roadmap published; ADR-021 (P0) pending |
| [m5-concord-roadmap.md](./m5-concord-roadmap.md) | Concord — Multi-Master CRDTs | Roadmap published; ADR-022 (P0) pending |
| [m6-pangea-roadmap.md](./m6-pangea-roadmap.md) | Pangea — CXL Tier-0 | Roadmap published; ADR-023 (P0) pending |
| [wave-1-gate.md](./wave-1-gate.md) | Wave 1 exit gate | Criteria locked |
| [wave-2-gate.md](./wave-2-gate.md) | Wave 2 exit gate | Criteria locked |
| [cw1-hiring.md](./cw1-hiring.md) | Cross-cutting: hiring | Roles defined |

## Reading order for a new engineer

1. Top-level plan: `~/.copilot/session-state/.../plan.md` (3-wave portfolio).
2. The moonshot ADR for the area you'll work on (`docs/adrs/adr-01[8-9]-*.md`, etc.).
3. The corresponding roadmap in this directory.
4. `MOONSHOT_DOCS_PIPELINE.md`, `OBSERVABILITY.md`, `MOONSHOT_HARNESS.md` for cross-cutting contracts.
5. `DESIGN_PARTNER_PROGRAM.md` once you start recruiting alpha users.

## Conventions

- Each phase has explicit acceptance criteria. No phase advances without them green.
- Each phase that ships code names the exact files to create or modify.
- Each moonshot reuses cross-moonshot infrastructure when possible (e.g. Lucidity's
  Merkle accumulator is reused by Concord; Chronicle's custom merge is a Forge function).
- ADRs precede code; roadmaps precede ADRs only for spike phases.
