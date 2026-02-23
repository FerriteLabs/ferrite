# Governance

This document describes the governance model for the Ferrite project.

## Overview

Ferrite is currently maintained by a small core team. As the project matures toward v1.0, we intend to formalize roles and expand the maintainer base. This document will evolve alongside the community.

## Roles

### Maintainers

Maintainers have full write access to one or more repositories in the FerriteLabs organization. They are responsible for:

- Reviewing and merging pull requests
- Triaging issues and managing milestones
- Making release decisions
- Upholding code quality, security, and architectural standards

Current maintainers are listed in the [CODEOWNERS](.github/CODEOWNERS) file.

### Contributors

Anyone who submits a pull request, files an issue, or participates in discussions is a contributor. Contributors are recognized in [CONTRIBUTORS.md](CONTRIBUTORS.md) and in release notes.

### Committers

Committers are trusted contributors who have been granted triage access (label issues, request reviews) but not merge rights. Committer status is granted by maintainers after sustained, high-quality contributions.

## Decision-Making Process

### Day-to-Day Decisions

Routine decisions (bug fixes, documentation improvements, minor refactors) are made by any maintainer through the standard PR review process. A single approving review from a maintainer is sufficient to merge.

### Significant Changes

Changes that affect public APIs, architecture, performance characteristics, or cross-crate boundaries require:

1. **An issue or discussion** describing the motivation and proposed approach
2. **Approval from at least one maintainer** familiar with the affected area
3. **A reasonable review period** (minimum 48 hours for non-trivial changes) to allow asynchronous feedback

### Breaking Changes

Breaking changes to stable APIs require:

1. **An RFC-style proposal** posted as a GitHub Discussion (category: "RFC")
2. **A minimum 7-day comment period** for community feedback
3. **Explicit approval from at least two maintainers** (or all maintainers if fewer than three)
4. **A migration guide** included in the PR or linked documentation
5. **A CHANGELOG entry** clearly marked as a breaking change

### Feature Graduation

Features progress through maturity tiers as defined in the [ROADMAP](ROADMAP.md):

| Tier | Label | Criteria to Graduate |
|------|-------|---------------------|
| Experimental | `🔬 Experimental` | Initial implementation, may change without notice |
| Beta | `🧪 Beta` | API stabilizing, >70% test coverage, no known critical bugs |
| Stable | `✅ Stable` | API frozen, >90% test coverage, production usage confirmed |

Graduation from Beta to Stable requires a maintainer-approved PR that updates the feature's tier designation and confirms the criteria are met.

## Releases

- **Patch releases** (0.x.Y): Bug fixes and security patches. Any maintainer can initiate.
- **Minor releases** (0.X.0): New features and non-breaking improvements. Require maintainer consensus on included features.
- **Major releases** (X.0.0): Breaking changes and milestone features. Require full maintainer agreement and a published migration guide.

All releases follow [Semantic Versioning 2.0.0](https://semver.org/) and the [Keep a Changelog](https://keepachangelog.com/) format.

## Conflict Resolution

If maintainers disagree on a decision:

1. Discuss in the relevant issue or PR to find consensus
2. If no consensus after 7 days, the proposal author may call for a vote
3. Each maintainer gets one vote; simple majority wins
4. In the event of a tie, the project lead (as listed first in CODEOWNERS) makes the final call

## Adding Maintainers

New maintainers are nominated by existing maintainers based on:

- Sustained, high-quality contributions over at least 3 months
- Demonstrated understanding of the project's architecture and goals
- Positive interactions with the community
- Willingness to commit time to reviews and maintenance

Nomination requires agreement from all existing maintainers.

## Changes to Governance

Changes to this document follow the same process as breaking changes: an RFC discussion with a 7-day comment period and maintainer approval.
