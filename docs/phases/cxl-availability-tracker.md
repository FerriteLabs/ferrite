# CXL Hardware Availability Tracker

Hardware availability across major cloud providers and bare-metal vendors
for CXL-based memory tiering (Pangea P1+ targets).

## Cloud & Bare-Metal Availability

| Provider | Instance Type | CXL Version | Status | Notes |
|----------|--------------|-------------|--------|-------|
| AWS | r7i.metal | CXL 1.1 | Preview | Request access via AWS support |
| Azure | Mv3 | CXL 2.0 | Private preview | NDA required |
| GCP | c4-standard | CXL 2.0 | Roadmap | No date announced |
| Bare metal | Marvell Structera S | CXL 3.1 | Available | Requires procurement |

## Kernel Requirements

| Kernel | CXL Support | Notes |
|--------|------------|-------|
| Linux 6.3+ | Basic CXL 2.0 | `/sys/bus/cxl/` enumeration |
| Linux 6.5+ | CXL region management | Dynamic region creation |
| Linux 6.8+ | CXL 3.0 hotplug | Fabric manager support |

## Detection Methods (used by `ferrite_pangea::feature::detect()`)

1. **SysBusCxl** — enumerate `/sys/bus/cxl/devices/` (Linux 6.x+)
2. **NumaHeuristic** — identify NUMA nodes with anomalously high latency
3. **AcpiCedt** — parse ACPI CEDT table for CXL host bridges
4. **Simulated** — software-only mode for development/testing

## References

- [CXL Consortium Specification](https://www.computeexpresslink.org/spec-landing)
- [Linux CXL Documentation](https://docs.kernel.org/driver-api/cxl/)
- [QEMU CXL Emulation](https://www.qemu.org/docs/master/system/devices/cxl.html)

Last updated: 2025-07-17
