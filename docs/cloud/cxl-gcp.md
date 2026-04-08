# Ferrite + CXL on GCP

## Instance Types
- c4-standard (bare-metal) — Intel Emerald Rapids, CXL 2.0 roadmap
- Status: Roadmap (no GA date announced)

## Setup
1. Provision a c4-standard bare-metal instance with Container-Optimized OS or Ubuntu 22.04
2. Verify CXL detection: `ferrite-cli PNG.DETECT`
3. Enable Pangea tier: set `pangea.enabled = true` in ferrite.toml
4. Monitor with: `ferrite-cli PNG.STATS`

## Performance Expectations
- CXL read latency: ~120-180ns expected with CXL 2.0 (vs ~80ns DRAM)
- Effective capacity: 2-3x DRAM at ≤5% p99 regression
- Working set ratio: optimal at 1.5-2x DRAM-only capacity

## Known Limitations
- CXL 2.0 support is on the GCP roadmap; not yet generally available
- Local SSD persistence tier recommended for Tier-2 alongside CXL
- Sole-tenant nodes may be required for bare-metal CXL access
