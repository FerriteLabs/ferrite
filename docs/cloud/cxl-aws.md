# Ferrite + CXL on AWS

## Instance Types
- r7i.metal — Intel Sapphire Rapids with CXL 1.1 support
- Status: Preview (request access via AWS Support)

## Setup
1. Launch an r7i.metal instance with Amazon Linux 2023
2. Verify CXL detection: `ferrite-cli PNG.DETECT`
3. Enable Pangea tier: set `pangea.enabled = true` in ferrite.toml
4. Monitor with: `ferrite-cli PNG.STATS`

## Performance Expectations
- CXL read latency: ~150-200ns (vs ~80ns DRAM)
- Effective capacity: 2x DRAM at ≤5% p99 regression
- Working set ratio: optimal at 1.5-2x DRAM-only capacity

## Known Limitations
- CXL 1.1 does not support hot-plug
- Cross-socket CXL latency is higher
- EBS-backed instances cannot use CXL for persistence
