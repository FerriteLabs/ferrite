# Ferrite Design Partner Program

The Design Partner Program is how Ferrite ships moonshot features (M1–M6, see
`plan.md`). Design partners get pre-GA access in exchange for production-grade feedback,
shared telemetry, and a public reference once the feature graduates.

## Who is a fit

- Operating Ferrite (or a Redis-compatible store) in production today.
- A specific workload that maps to one of the active moonshots:
  - **Mnemo** — agent applications using LangGraph / LlamaIndex / Letta.
  - **Forge** — custom server-side logic currently implemented as Lua, sidecars, or
    application middleware.
  - **Lucidity** — regulated industries (finance, health, EU) needing audit trails.
  - **Chronicle** — data teams running test/staging branches of production data.
  - **Concord** — multi-region active-active with manual conflict reconciliation today.
  - **Pangea** — workloads bottlenecked on RAM cost at >256 GiB working set.
- Willingness to run pre-release builds for ≥ 8 weeks.
- A named technical contact reachable on a shared channel (Slack / Discord).

## What partners get

- Direct access to the moonshot lead engineer.
- Pre-release builds with private patches as needed.
- A monthly 30-min sync.
- Public co-launch slot at GA (case study, blog, conference talk).
- Influence on the API before it is locked.

## What partners give

- Honest feedback on the API and behaviour, in writing.
- Telemetry opt-in (metrics + structured logs, no payload data) — see Telemetry Schema below.
- Permission to publish a case study at GA, with names redactable on request.
- Bug reports filed in the Ferrite issue tracker, not via private channels.

## Telemetry schema (opt-in)

Sent over OTLP (see `docs/OBSERVABILITY.md` once published) to a partner-specific
endpoint controlled by Ferrite Labs.

| Signal | Fields | Frequency |
|---|---|---|
| Counter | `mnemo.requests_total{cmd,status}` etc. per moonshot | 60s |
| Histogram | `*.latency_ns{cmd}` p50/p99 | 60s |
| Gauge | `*.records_total`, `*.bytes_resident` | 60s |
| Log | structured errors + WARN with cmd context, no values | on event |

**No payload bytes, no key contents, no embeddings, no PII.** Only counts, sizes, and
error signatures.

## LOI template

A short Letter of Intent — not a contract. Designed to fit on one page.

```
                    FERRITE DESIGN PARTNER — LETTER OF INTENT

Partner organisation: ____________________________________________
Primary technical contact (name + email + handle): _______________
Primary moonshot of interest:  [ ] Mnemo  [ ] Forge  [ ] Lucidity
                               [ ] Chronicle  [ ] Concord  [ ] Pangea
Workload description (3–5 sentences): ____________________________

We commit to:
  [ ] Run pre-release builds for at least 8 weeks
  [ ] One 30-min monthly sync with the moonshot lead
  [ ] Opt-in telemetry per the Ferrite Design Partner Program doc
  [ ] Public reference at GA (logo + 1-paragraph quote, drafts approved by us)

Ferrite Labs commits to:
  [ ] Direct access to the moonshot lead engineer
  [ ] Pre-release builds and private patches as needed
  [ ] Reasonable response time on partner-reported issues (next business day)
  [ ] Co-launch slot at GA

Either party may exit this LOI at any time with 7 days notice.

Partner signature: _____________________  Date: __________
Ferrite Labs:      _____________________  Date: __________
```

## Process

1. Inbound interest → triage call (30 min) → fit decision within 1 week.
2. LOI signed → moonshot lead onboards partner; private channel created.
3. Monthly sync; written feedback after each release.
4. At GA: case study published, partner moves to standard support tier.

## Owner

Owner: **TBD** — must be named before Wave 1 exit gate (`w1-gate` in plan.md).
Until then, every moonshot lead handles their own partner relationships.

## Tracking

Partner pipeline is tracked in `docs/design-partners.md` (gitignored — contains names).
Public-facing summary is published as part of each moonshot's release notes.
