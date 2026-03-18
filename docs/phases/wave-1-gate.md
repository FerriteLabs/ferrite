# Wave 1 Exit Gate — Mnemo + Forge

> Source: `~/.copilot/session-state/.../plan.md`. Wave 1 closes only when **all** the
> criteria below are green. Wave 2 (Lucidity + Chronicle) does not start otherwise.

## Hard requirements

### Mnemo (M1)

- [ ] At Beta or later (per `m1-p4-alpha` exit criteria).
- [ ] ≥ 3 external design partners running production-ish workloads for ≥ 4 weeks.
- [ ] LongMemEval `_s` ≥ 75% in published results.
- [ ] No P0/P1 bugs open more than 7 days.

### Forge (M2)

- [ ] At GA (per `m2-p5-ga` exit criteria).
- [ ] Function registry stable for 4+ weeks across two patch releases.
- [ ] ≥ 1 community-published function discoverable.

### Org capacity

- [ ] At least one of:
  - 1+ senior Rust engineer added to the Mnemo or Forge team, OR
  - external community contributor velocity > 5 PRs/month sustained on the new crates.
- [ ] Named owner for the Design Partner Program (`cw2-partners`).

### Cross-cutting

- [ ] Benchmark harness (`MOONSHOT_HARNESS.md`) operational with at least one published baseline per Wave-1 moonshot.
- [ ] Docs pipeline (`MOONSHOT_DOCS_PIPELINE.md`) enforced in CI for crates touched in Wave 1.
- [ ] Observability conventions (`OBSERVABILITY.md`) implemented for all Wave-1 metrics.

## Soft signals (informational, do not block)

- Public traction: HN/Lobsters/Reddit launch coverage at GA.
- ≥ 1 public talk submitted (KubeCon, RedisConf-equivalent, AI Engineer Summit).
- LangChain integrations index PR merged for Mnemo.

## Decision protocol at gate review

A maintainer-led gate review meeting:

1. Walk every checkbox above; record link to evidence per item.
2. Surface unresolved risks from M1/M2 alpha telemetry.
3. Vote (maintainers only) on opening Wave 2.

If gate fails: produce a written remediation plan with new dates, do not open Wave 2.
If gate partially passes (e.g. Forge GA, Mnemo only Beta): open only the dependent
Wave-2 work that needs the passed component.

## Dependency unlocks

| Wave 2 task | Unlocked by |
|---|---|
| `m3-p0-spike` (Lucidity spike) | Wave 1 gate |
| `m4-p0-spike` (Chronicle spike) | Wave 1 gate |

Wave 2 tasks remain `pending` in the SQL todo list until this gate is recorded as passed.
