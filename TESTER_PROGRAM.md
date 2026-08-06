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

1. `CAMPAIGN_OPS_REF` — an immutable [ferrite-ops](https://github.com/ferritelabs/ferrite-ops)
   reference (a tag or a full commit SHA; never `main` or a floating branch).
2. `FERRITE_TEST_IMAGE` — an exact, immutable artifact reference for the
   candidate build (a Docker image digest is preferred; a Homebrew formula
   version/revision or a source commit SHA are acceptable alternatives when a
   Docker image is not the artifact under test).

Do not begin a session against `main`/`latest` or any artifact you assembled
yourself. If either reference is missing, unverifiable, or does not check
out/pull cleanly, stop and wait for the campaign owner to fix or reissue it.

## Campaign artifact rule

Use only the exact immutable reference the campaign owner supplies:

- **Docker image (primary)** — an exact digest such as
  `ghcr.io/ferritelabs/ferrite@<CAMPAIGN_IMAGE_DIGEST>`. A pinned tag is
  acceptable only if the owner states it is immutable for the campaign.
  **Never use `latest`** or a locally built image when reporting a result.
- **Homebrew formula** — the exact formula version/revision the owner names,
  installed fresh (`brew uninstall`/`brew install`, not `brew upgrade` from an
  older cache).
- **Source commit** — the exact commit SHA the owner names, built clean from
  that checkout.

Record which artifact type and exact reference you used in your report.

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
- The campaign owner has published both `CAMPAIGN_OPS_REF` and
  `FERRITE_TEST_IMAGE` (see [Launch gate](#launch-gate)) and you have the
  exact values.
- You will use synthetic data and can delete the tester volume afterward.
- You have chosen a track and a client to record in your report.

### Register interest

First-time testers can register interest before a campaign is scheduled using
the [Tester Interest form](https://github.com/ferritelabs/ferrite/issues/new?template=tester_interest.yml).
This is a temporary public intake; it will be replaced by a dedicated channel
once one exists. Do not put email addresses, credentials, customer data, or
other sensitive information in a public issue.

## Core path

Clone [ferrite-ops](https://github.com/ferritelabs/ferrite-ops) and check out
the exact campaign reference before running its isolated tester tooling:

```bash
git clone https://github.com/ferritelabs/ferrite-ops.git
cd ferrite-ops
git checkout <CAMPAIGN_OPS_REF>
test -x scripts/tester.sh && ./scripts/tester.sh --help >/dev/null || {
  echo "scripts/tester.sh is missing or not runnable at <CAMPAIGN_OPS_REF>" >&2
  exit 1
}
export FERRITE_TEST_IMAGE='<CAMPAIGN_IMAGE_DIGEST>' # exact digest/tag the owner supplied; never latest
./scripts/tester.sh start
./scripts/tester.sh smoke
./scripts/tester.sh diagnostics
./scripts/tester.sh stop
```

Both placeholders above (`<CAMPAIGN_OPS_REF>` and `<CAMPAIGN_IMAGE_DIGEST>`)
must be replaced with the values the campaign owner publishes; there is no
default. `FERRITE_TEST_IMAGE` has no fallback — `tester.sh` fails fast with an
actionable error before touching Docker if it is unset, floating (`latest` or
an implicit tag-less reference), or malformed.

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
tester report; report it privately following the
[Security Policy](SECURITY.md#reporting-a-vulnerability) (email
**security@ferritelabs.dev**).

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

Program questions belong on the
[Tester Interest form](https://github.com/ferritelabs/ferrite/issues/new?template=tester_interest.yml)
until a dedicated community channel is available; completed sessions belong in
the Tester Report form above.
