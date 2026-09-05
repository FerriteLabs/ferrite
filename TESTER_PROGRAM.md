# Ferrite External Tester Program

This program recruits developers to spend **60–90 minutes** evaluating a
candidate Ferrite build in a disposable environment. This document is
version-neutral: it does not itself name a release or claim any image is
currently available. The canonical current Ferrite version is documented in
[CHANGELOG.md](CHANGELOG.md); it is tracked separately from this program.

Candidate campaign builds are for hardening and validation only: **never use
production data, credentials, hosts, or workloads you cannot safely lose**.

## Launch gate

Interest can be registered before a campaign has artifacts — see
[Register interest](#register-interest) below. **Hands-on testing does not
start until the campaign owner has published both of the following, and you
have verified them on a clean machine:**

1. `CAMPAIGN_OPS_COMMIT` — the full 40-character lowercase
   [ferrite-ops](https://github.com/ferritelabs/ferrite-ops) commit SHA that
   the campaign was validated against. Only an exact commit SHA is accepted:
   a tag, `main`, any other branch, and an abbreviated SHA are all rejected,
   because only a commit SHA is immutable and unambiguous.
2. `FERRITE_TEST_IMAGE` — the complete repository-qualified sha256 digest
   reference for the candidate build (e.g.
   `ghcr.io/ferritelabs/ferrite@sha256:<CAMPAIGN_DIGEST>`); never a tag. The
   initial cohort is Docker/Docker Compose only; alternative installation
   cohorts (Homebrew, source build, Kubernetes) are deferred until
   maintained tooling exists for them.

Do not begin a session against `main`/`latest` or any artifact you assembled
yourself. If either reference is missing, unverifiable, or does not check
out/pull cleanly, stop and wait for the campaign owner to fix or reissue it.
The core path below checks the commit out in detached HEAD state and verifies
`git rev-parse HEAD` matches `CAMPAIGN_OPS_COMMIT` exactly before running any
tooling; if that comparison fails, stop.

## Campaign artifact rule

Use only the exact immutable reference the campaign owner supplies:

- **Docker image** — the complete repository-qualified digest reference,
  for example `ghcr.io/ferritelabs/ferrite@sha256:<CAMPAIGN_DIGEST>`. A tag
  (pinned or otherwise) is never accepted, only an exact sha256 digest.
  **Never use `latest`** or a locally built image when reporting a result.

The initial cohort is Docker/Docker Compose only. Alternative installation
cohorts (Homebrew, source build, Kubernetes) are deferred until maintained
tooling exists for them. Record the exact reference you used in your report.

## Journey and tracks

Every tester runs the core path: deploy the candidate build, run the smoke
checks, exercise one realistic client workflow, collect safe diagnostics, and
submit a report. Choose any optional track if time permits:

1. **Redis/client compatibility** — exercise an application or Redis client.
2. **Durability/restart** — verify a value survives a controlled restart.
   Only run this track if the campaign owner has explicitly enabled it (see
   [Core path](#core-path)); it is not part of the required core journey.
3. **Operations/metrics** — inspect health, `INFO`, logs, and metrics.
4. **Performance comparison** — compare a small, disclosed, non-production
   workload without making general benchmark claims.
5. **IDE tooling** — connect the VS Code or JetBrains Ferrite tooling to the
   running Docker Compose instance.

Docker with Docker Compose is the only supported starting environment for the
initial cohort. Alternative installation cohorts (Homebrew, a source build,
Kubernetes) are deferred until maintained tooling exists for them.
Participation does not imply that Ferrite or any deployment method is
production-ready; see
[feature maturity and known limitations](docs/FEATURE_MATURITY.md).

## Entry checklist

- You can reserve 60–90 uninterrupted minutes.
- Docker Engine and Docker Compose v2 are available (`docker compose version`).
  Linux hosts require Docker Engine 28 or newer because older engines can
  expose loopback-published ports to the local network.
- Python 3 is available for the bounded host-side RESP and metrics probes.
- Ports `6379` and `9090` are free, or you have chosen overrides.
- The campaign owner has published both `CAMPAIGN_OPS_COMMIT` (a full
  40-character lowercase commit SHA) and `FERRITE_TEST_IMAGE` (see
  [Launch gate](#launch-gate)) and you have the exact values.
- You will use synthetic data and can delete the tester volume afterward.
- You have chosen a track and a client to record in your report.

### Register interest

First-time testers can register interest before a campaign is scheduled using
the [Tester Interest form](https://github.com/ferritelabs/ferrite/issues/new?template=tester_interest.yml).
Do not put email addresses, credentials, customer data, or other sensitive
information in a public issue.

## Core path

Clone [ferrite-ops](https://github.com/ferritelabs/ferrite-ops), check out the
exact campaign commit in detached HEAD state, and verify the checkout before
running its isolated tester tooling:

```bash
git clone https://github.com/ferritelabs/ferrite-ops.git
cd ferrite-ops
git checkout --detach <CAMPAIGN_OPS_COMMIT>
test "$(git rev-parse HEAD)" = "<CAMPAIGN_OPS_COMMIT>" || {
  echo "HEAD is not <CAMPAIGN_OPS_COMMIT>; stop and re-request the campaign commit" >&2
  exit 1
}
test -x scripts/tester.sh && ./scripts/tester.sh --help >/dev/null || {
  echo "scripts/tester.sh is missing or not runnable at <CAMPAIGN_OPS_COMMIT>" >&2
  exit 1
}
export FERRITE_TEST_IMAGE='ghcr.io/ferritelabs/ferrite@sha256:<CAMPAIGN_DIGEST>' # complete repository-qualified digest the owner supplied; never latest or a tag
./scripts/tester.sh start
./scripts/tester.sh smoke
./scripts/tester.sh diagnostics
./scripts/tester.sh stop
```

Both placeholders above (`<CAMPAIGN_OPS_COMMIT>` and `<CAMPAIGN_DIGEST>`) must
be replaced with the values the campaign owner publishes; there is no default.
`<CAMPAIGN_OPS_COMMIT>` must be the full 40-character lowercase commit SHA —
substituting a tag or branch name makes the `git rev-parse HEAD` comparison
above fail, which is intended: record the commit, not a movable label.
`FERRITE_TEST_IMAGE` has no fallback — `tester.sh` fails fast with an
actionable error before touching Docker if it is unset, floating (`latest`),
a tag, or not the complete repository-qualified sha256 digest reference.

Only run `./scripts/tester.sh durability` if the campaign owner has explicitly
enabled that track (they will state `FERRITE_TEST_ENABLE_DURABILITY=1`); it is
an optional, campaign-specific diagnostic, not part of the required core path.

Expected outcomes:

- `start` reports a healthy candidate container within the configured timeout.
- `smoke` passes PING, string, hash, list, sorted-set, and TTL checks and removes
  its temporary keys.
- `durability` (when enabled) confirms its unique value survives a restart and
  then removes it.
- `diagnostics` prints the path to a timestamped `.tar.gz` for you to review.

Spend the remaining time on one realistic client workflow or optional track.
Record exact commands and results; a pass is useful feedback too.

## Exit and cleanup

Preserve the tester volume when you may need follow-up investigation:

```bash
./scripts/tester.sh stop
```

After diagnostics are reviewed and no follow-up is needed, explicitly delete
the isolated tester volume:

```bash
./scripts/tester.sh reset
```

## Diagnostics, privacy, and reporting

The diagnostics command intentionally excludes environment variables, secrets,
full configuration, and database contents. Logs can still contain keys, client
addresses, or values. **Open the archive and redact sensitive or identifying
data before sharing it.** For a security vulnerability, do not file a public
tester report; report it privately using
[GitHub private vulnerability reporting](https://github.com/ferritelabs/ferrite/security/advisories/new)
as described in the [Security Policy](SECURITY.md#reporting-a-vulnerability).

Every report must record the exact provenance of the session: the
`ops_commit` field is the full 40-character lowercase `CAMPAIGN_OPS_COMMIT`
you verified with `git rev-parse HEAD`, and the image field is the complete
repository-qualified sha256 digest reference you used. Copy both verbatim; a
tag, a branch name, an abbreviated SHA, or "same as published" is not a
usable provenance record.

Use these definitions in the
[Tester Report form](https://github.com/ferritelabs/ferrite/issues/new?template=tester_report.yml):

| Severity | Definition |
|---|---|
| Critical | Data loss/corruption, remote security impact, or service unusable with no workaround. |
| High | Core path blocked, crash, or major compatibility failure with no practical workaround. |
| Medium | Important behavior is wrong or degraded, but a workaround exists. |
| Low | Minor defect, confusing output, or documentation/tooling issue. |
| No issues observed | Session passed; no defect found. |

| Reproducibility | Definition |
|---|---|
| Always | Reproduced on every attempt. |
| Intermittent | Reproduced more than once but not every time. |
| Once | Observed once after retrying. |
| Not retried | No safe opportunity to retry. |

Expect an acknowledgment or triage update within **three business days**. This
is a community testing target, not a guaranteed support SLA. Maintainers may
request a redacted diagnostic archive or a narrower reproduction.

## What success looks like

The first cohort is complete when:

- 8–12 testers are recruited and at least 80% complete the core path;
- at least three client libraries and three host environments are represented;
- every optional track receives at least one report;
- all Critical findings are resolved or the campaign is stopped, and every High
  finding has an owner and documented disposition;
- recurring setup friction and known limitations are reflected in the canonical
  docs before the next cohort.

Program questions belong on the
[Tester Interest form](https://github.com/ferritelabs/ferrite/issues/new?template=tester_interest.yml);
completed sessions belong in the Tester Report form above.
