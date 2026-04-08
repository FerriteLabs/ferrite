# Ferrite + CXL on Azure

## Instance Types
- Mv3-series — Intel Sapphire Rapids with CXL 1.1+ support
- Status: Preview (enrol via Azure Portal → Preview Features)

## Setup
1. Deploy an Mv3-series VM with Ubuntu 22.04 or CBL-Mariner 2.0
2. Verify CXL detection: `ferrite-cli PNG.DETECT`
3. Enable Pangea tier: set `pangea.enabled = true` in ferrite.toml
4. Monitor with: `ferrite-cli PNG.STATS`

## Performance Expectations
- CXL read latency: ~150-200ns (vs ~80ns DRAM)
- Effective capacity: 2x DRAM at ≤5% p99 regression
- Working set ratio: optimal at 1.5-2x DRAM-only capacity

## Known Limitations
- CXL 1.1+ does not support hot-plug on current Mv3 SKUs
- Accelerated Networking must be enabled for lowest latency
- Premium SSD v2 recommended for Tier-2 persistence alongside CXL
