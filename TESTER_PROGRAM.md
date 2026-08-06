# Ferrite v0.4 External Tester Program

This program is for developers willing to spend **60–90 minutes** evaluating
Ferrite v0.4 in a disposable environment. Ferrite and campaign builds are
pre-release software: **never use production data, credentials, hosts, or
workloads you cannot safely lose**.

## Campaign artifact rule

Use the exact immutable artifact reference supplied by the campaign owner: a
release tag governed as immutable or, preferably for an RC, a digest. The v0.4
baseline is `ghcr.io/ferritelabs/ferrite:0.4.0`, but an RC campaign may provide
an override such as `ghcr.io/ferritelabs/ferrite@sha256:...`. Copy it exactly;
**never use `latest`** or substitute a locally built image when reporting a
campaign result.

## Journey and tracks

Every tester runs the core path: deploy Ferrite, run the smoke checks, exercise
one realistic client workflow, collect safe diagnostics, and submit a report.
Choose any optional track if time permits:

1. **Redis/client compatibility** — exercise an application or Redis client.
2. **Durability/restart** — verify a value survives a controlled restart.
3. **Operations/metrics** — inspect health, `INFO`, logs, and metrics.
4. **Performance comparison** — compare a small, disclosed, non-production
   workload without making general benchmark claims.
5. **IDE tooling** — try the VS Code or JetBrains Ferrite tooling.

Docker with Docker Compose is the primary supported starting environment.
Homebrew, a source build, and Kubernetes are advanced starting points for
testers already comfortable debugging them. Participation does not imply that
Ferrite or any deployment method is production-ready; see
[feature maturity and known limitations](docs/FEATURE_MATURITY.md).

## Entry checklist

- You can reserve 60–90 uninterrupted minutes.
- Docker Engine and Docker Compose v2 are available (`docker compose version`).
- Ports `6379` and `9090` are free, or you have chosen overrides.
- You have the campaign owner's exact image tag/digest.
- You will use synthetic data and can delete the tester volume afterward.
- You have chosen a track and a client to record in your report.

First-time testers can submit the
[Tester Interest form](https://github.com/ferritelabs/ferrite/issues/new?template=tester_interest.yml).
Do not put email addresses, credentials, customer data, or other sensitive
information in a public issue.

## Core path

Clone or update [ferrite-ops](https://github.com/ferritelabs/ferrite-ops), then
run its isolated tester tooling:

```bash
git clone https://github.com/ferritelabs/ferrite-ops.git
cd ferrite-ops
export FERRITE_TEST_IMAGE='ghcr.io/ferritelabs/ferrite:0.4.0' # or exact campaign digest
./scripts/tester.sh start
./scripts/tester.sh smoke
./scripts/tester.sh durability
./scripts/tester.sh diagnostics
```

Expected outcomes:

- `start` reports a healthy Ferrite container within the configured timeout.
- `smoke` passes PING, string, hash, list, sorted-set, and TTL checks and removes
  its temporary keys.
- `durability` confirms its unique value survives a restart and then removes it.
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
tester report; use the
[private security advisory](https://github.com/ferritelabs/ferrite/security/advisories/new).

Use these definitions in the
[Tester Report form](https://github.com/ferritelabs/ferrite/issues/new?template=tester_report.yml):

| Severity | Definition |
|---|---|
| Critical | Data loss/corruption, remote security impact, or service unusable with no workaround. |
| High | Core path blocked, crash, or major compatibility failure with no practical workaround. |
| Medium | Important behavior is wrong or degraded, but a workaround exists. |
| Low | Minor defect, confusing output, or documentation/tooling issue. |
| None | Session passed; no defect found. |

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

Program questions belong in
[GitHub Discussions](https://github.com/ferritelabs/ferrite/discussions);
completed sessions belong in the Tester Report form above.
