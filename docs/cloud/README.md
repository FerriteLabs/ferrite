# Cloud Deployment Guides

This directory contains CXL integration guides for running Ferrite with
Pangea Tier-0 storage on major cloud providers.

| Guide | Provider | Instance Family | CXL Version |
|-------|----------|-----------------|-------------|
| [cxl-aws.md](cxl-aws.md) | AWS | r7i.metal | CXL 1.1 |
| [cxl-azure.md](cxl-azure.md) | Azure | Mv3-series | CXL 1.1+ |
| [cxl-gcp.md](cxl-gcp.md) | GCP | c4-standard | CXL 2.0 (roadmap) |

## Prerequisites

- Ferrite built with `pangea` feature enabled
- `ferrite-cli` available on the instance
- Kernel ≥ 6.2 with CXL subsystem enabled

## Quick Start

1. Choose a bare-metal or CXL-capable instance from one of the guides above.
2. Verify CXL detection: `ferrite-cli PNG.DETECT`
3. Enable Pangea tier: set `pangea.enabled = true` in `ferrite.toml`.
4. Monitor with: `ferrite-cli PNG.STATS`
5. Estimate savings: `ferrite-cli PNG.SIZING <working_set_gib>`
