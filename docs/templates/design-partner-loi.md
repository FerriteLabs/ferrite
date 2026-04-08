# Ferrite Design Partner — Letter of Intent

> This is a statement of mutual interest, not a binding contract.
> Either party may exit at any time with 7 days written notice.

---

## 1. Partner Information

| Field | Value |
|---|---|
| **Company name** | |
| **Primary technical contact** (name) | |
| **Email** | |
| **GitHub / Discord handle** | |
| **Company size** (approx. engineers) | |

## 2. Use Case Description

_Describe the workload you plan to run on Ferrite in 3–5 sentences. Include current
stack (e.g. Redis 7, KeyDB, DragonflyDB), approximate data size, and throughput._

> _Your description here._

## 3. Moonshot Selection

Select the moonshot(s) you intend to evaluate:

- [ ] **Mnemo** (M1) — Agentic memory with LangGraph / LlamaIndex / Letta workloads
- [ ] **Forge** (M2) — WebAssembly server-side functions replacing Lua scripts or sidecars
- [ ] **Lucidity** (M3) — Cryptographic audit trails for regulated industries
- [ ] **Chronicle** (M4) — Time-travel branching for test/staging environments
- [ ] **Concord** (M5) — Multi-region active-active with CRDT conflict resolution
- [ ] **Pangea** (M6) — Tiered storage for working sets exceeding 256 GiB RAM

## 4. Duration Commitment

- **Minimum evaluation period:** 4 weeks (recommended: 8 weeks for GA-track feedback).
- **Start date (target):** _______________
- **End date (target):** _______________

The evaluation period may be extended by mutual agreement.

## 5. Telemetry Consent (Opt-In)

Telemetry is **opt-in** and governed by the
[Design Partner Program](../DESIGN_PARTNER_PROGRAM.md#telemetry-schema-opt-in) doc.

**What is collected:**

| Signal | Examples | Frequency |
|---|---|---|
| Counters | `mnemo.requests_total{cmd,status}` | 60 s |
| Histograms | `*.latency_ns{cmd}` p50/p99 | 60 s |
| Gauges | `*.records_total`, `*.bytes_resident` | 60 s |
| Structured logs | Errors + WARN with command context | On event |

**What is never collected:** payload bytes, key contents, embeddings, PII.

- [ ] **We opt in** to the telemetry schema described above.
- [ ] **We opt out** — we will provide equivalent metrics manually in monthly reports.

Data is sent via OTLP to a partner-specific endpoint controlled by Ferrite Labs and
retained for the duration of the evaluation plus 90 days.

## 6. Feedback Cadence

- **Monthly sync:** 30-minute video call with the moonshot lead engineer.
- **Async channel:** Shared Slack or Discord channel; Ferrite Labs targets next-business-day
  response for partner-reported issues.
- **Bug reports:** Filed in the Ferrite issue tracker (GitHub), not via private channels.
- **Written feedback:** A short written summary after each pre-release build.

## 7. Partner Commitments

- [ ] Run pre-release builds for the agreed evaluation period.
- [ ] Participate in monthly sync calls with the moonshot lead.
- [ ] Provide honest written feedback on API and behaviour.
- [ ] File bugs in the public issue tracker.

## 8. Ferrite Labs Commitments

- [ ] Direct access to the moonshot lead engineer.
- [ ] Pre-release builds and private patches as needed.
- [ ] Next-business-day response on partner-reported issues.
- [ ] Co-launch slot at GA (case study, blog post, or conference talk).
- [ ] Draft approval on any public reference material before publication.

## 9. Public Reference at GA

- [ ] **Yes** — we are willing to be a public reference at GA (logo + quote; drafts
  approved by us before publication).
- [ ] **Maybe** — we will decide closer to GA.
- [ ] **No** — we prefer to remain anonymous; Ferrite Labs may reference an anonymised
  summary only.

## 10. Exit Terms

Either party may exit this LOI at any time with **7 days written notice** to the
primary contact on the other side. Upon exit:

- Telemetry collection stops immediately.
- Collected telemetry is deleted within 30 days upon request.
- No further obligations remain for either party.

---

## Signatures

| | Name | Date |
|---|---|---|
| **Partner** | | |
| **Ferrite Labs** | | |
