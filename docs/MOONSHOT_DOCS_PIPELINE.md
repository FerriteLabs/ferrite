# Moonshot Documentation Pipeline

Every moonshot (M1–M6, see `plan.md`) MUST satisfy this docs contract before its **Beta**
gate. Missing any item blocks the Beta → GA transition.

## Per-moonshot required artifacts

For each moonshot `<name>`:

### 1. ADR (Architecture Decision Record)

- Path: `docs/adrs/adr-NNN-<name>-*.md`
- Covers: context, decision, APIs, sandboxing/limits, consequences, open questions, exit criteria.
- Style: matches existing ADR-001..ADR-019.
- Status field tracks lifecycle: `Proposed (Spike) → Accepted → GA`.

### 2. User-facing docs page

- Path: `ferrite-docs/website/docs/moonshots/<name>.md`
- Sections (required, in this order):
  1. **What it is** (1 paragraph, no jargon)
  2. **When to use it** (3–5 bullet decision points)
  3. **Quick start** (10-line runnable example)
  4. **Command reference** (one subsection per command, with synopsis, args, return, example)
  5. **Concepts** (data model, lifecycle, semantics)
  6. **Operational guidance** (telemetry, limits, failure modes)
  7. **Migration / interop** (if it replaces or coexists with an existing feature)
  8. **Status** (Alpha / Beta / GA + maturity caveats)

### 3. Runnable example

- Path: `ferrite/examples/<name>/`
- Contains: `Cargo.toml` (or language-appropriate manifest), `README.md`, source.
- MUST run from a fresh clone via `cargo run -p ferrite-example-<name>` (or the
  language equivalent) and produce expected output.
- MUST be exercised by CI in a smoke-test job.

### 4. Changelog entry

- Path: `CHANGELOG.md` (top-level)
- Section: `## [Unreleased]` → subsection `### Added` (or `### Changed`).
- Format: `- **<Name>** ([#PR]): one-sentence what + link to docs page.`

### 5. Migration / upgrade notes (if breaking)

- Path: `docs/MIGRATION_FROM_REDIS.md` (append section if relevant)
- For internal-breaking changes: `CHANGELOG.md` `### Breaking` subsection.

### 6. Telemetry schema documentation

- Path: `docs/OBSERVABILITY.md` (create if missing)
- Document every new metric/log emitted by the moonshot.

## Per-phase docs gates

| Phase | Required docs artifacts |
|---|---|
| **Spike (P0)** | ADR with status `Proposed (Spike)` |
| **Prototype (P1)** | ADR updated; placeholder docs page with "Status: Pre-alpha — APIs may change" warning |
| **Alpha (P2/P3)** | All sections of docs page draft; runnable example committed |
| **Beta (P4)** | All 6 artifacts above complete; docs reviewed by ≥1 contributor outside the implementer |
| **GA (P5)** | ADR status updated to `Accepted (GA)`; example exercised by CI; changelog entry merged |

## Style conventions

- Code blocks use language tags (` ```rust `, ` ```bash `, ` ```ferrite ` for RESP).
- Examples are copy-pasteable — no `<placeholder>` markers in runnable code.
- Each command reference includes at least one example with realistic input.
- No marketing language in docs pages — that lives on the website landing page.

## CI enforcement

- A check in `.github/workflows/docs.yml` (to be added) runs on PRs that touch any
  `crates/ferrite-<moonshot>/` directory and fails if:
  - the corresponding `ferrite-docs/.../moonshots/<name>.md` was not also updated,
  - the example in `examples/<name>/` no longer compiles, or
  - the changelog `[Unreleased]` section is empty.

## Owner

Each moonshot lead owns their docs. The Ferrite Labs documentation maintainer reviews
all moonshot docs PRs before Beta gate.
