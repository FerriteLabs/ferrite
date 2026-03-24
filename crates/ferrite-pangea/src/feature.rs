//! CXL hardware feature detection.
//!
//! Detects CXL availability at runtime to enable/disable the Pangea tier.
//! On systems without CXL, gracefully falls back to DRAM-only mode.

/// CXL hardware capability information.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CxlCapability {
    /// Whether CXL memory is detected.
    pub available: bool,
    /// CXL protocol version (e.g., "3.1").
    pub version: Option<String>,
    /// Type of CXL device (Type 1/2/3).
    pub device_type: Option<CxlDeviceType>,
    /// Total CXL memory in bytes.
    pub capacity_bytes: Option<u64>,
    /// Number of CXL-attached NUMA nodes.
    pub numa_nodes: usize,
    /// Detection method used.
    pub detection_method: DetectionMethod,
}

/// CXL device type classification per CXL specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CxlDeviceType {
    /// CXL.cache
    Type1,
    /// CXL.cache + CXL.mem (accelerator)
    Type2,
    /// CXL.mem only (memory expander) — our target
    Type3,
}

/// How CXL was detected on the system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DetectionMethod {
    /// Detected via /sys/bus/cxl (Linux 6.x+)
    SysBusCxl,
    /// Detected via NUMA topology heuristics
    NumaHeuristic,
    /// Detected via ACPI CEDT table
    AcpiCedt,
    /// No detection available (simulated)
    Simulated,
    /// Not available
    NotAvailable,
}

/// Detect CXL capabilities on the current system.
pub fn detect() -> CxlCapability {
    // Try Linux sysfs detection first
    #[cfg(target_os = "linux")]
    if let Some(cap) = detect_linux_sysfs() {
        return cap;
    }

    // Fall back to NUMA heuristic
    #[cfg(target_os = "linux")]
    if let Some(cap) = detect_numa_heuristic() {
        return cap;
    }

    // No CXL detected — return not-available for non-Linux or missing hardware
    CxlCapability {
        available: false,
        version: None,
        device_type: None,
        capacity_bytes: None,
        numa_nodes: 0,
        detection_method: DetectionMethod::NotAvailable,
    }
}

#[cfg(target_os = "linux")]
fn detect_linux_sysfs() -> Option<CxlCapability> {
    let cxl_path = std::path::Path::new("/sys/bus/cxl/devices");
    if !cxl_path.exists() {
        return None;
    }
    // Enumerate devices — placeholder for real hardware enumeration in P1.
    None
}

#[cfg(target_os = "linux")]
fn detect_numa_heuristic() -> Option<CxlCapability> {
    // Check if there are NUMA nodes with significantly higher latency
    // (indicating CXL-attached memory) — placeholder for P1.
    None
}

/// Create a simulated CXL capability for testing.
pub fn simulated(capacity_bytes: u64, numa_nodes: usize) -> CxlCapability {
    CxlCapability {
        available: true,
        version: Some("3.1".to_string()),
        device_type: Some(CxlDeviceType::Type3),
        capacity_bytes: Some(capacity_bytes),
        numa_nodes,
        detection_method: DetectionMethod::Simulated,
    }
}

impl std::fmt::Display for CxlDeviceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CxlDeviceType::Type1 => write!(f, "Type1"),
            CxlDeviceType::Type2 => write!(f, "Type2"),
            CxlDeviceType::Type3 => write!(f, "Type3"),
        }
    }
}

impl std::fmt::Display for DetectionMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DetectionMethod::SysBusCxl => write!(f, "SysBusCxl"),
            DetectionMethod::NumaHeuristic => write!(f, "NumaHeuristic"),
            DetectionMethod::AcpiCedt => write!(f, "AcpiCedt"),
            DetectionMethod::Simulated => write!(f, "Simulated"),
            DetectionMethod::NotAvailable => write!(f, "NotAvailable"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_returns_not_available_on_non_cxl_systems() {
        let cap = detect();
        // CI / dev machines won't have CXL hardware
        assert!(!cap.available || cap.detection_method == DetectionMethod::Simulated);
        if !cap.available {
            assert_eq!(cap.detection_method, DetectionMethod::NotAvailable);
            assert!(cap.version.is_none());
            assert!(cap.device_type.is_none());
            assert!(cap.capacity_bytes.is_none());
            assert_eq!(cap.numa_nodes, 0);
        }
    }

    #[test]
    fn simulated_returns_available() {
        let cap = simulated(1024 * 1024 * 1024, 2);
        assert!(cap.available);
        assert_eq!(cap.version.as_deref(), Some("3.1"));
        assert_eq!(cap.device_type, Some(CxlDeviceType::Type3));
        assert_eq!(cap.capacity_bytes, Some(1024 * 1024 * 1024));
        assert_eq!(cap.numa_nodes, 2);
        assert_eq!(cap.detection_method, DetectionMethod::Simulated);
    }

    #[test]
    fn serialization_round_trip() {
        let cap = simulated(4096, 1);
        let json = serde_json::to_string(&cap).expect("serialize");
        let back: CxlCapability = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.available, cap.available);
        assert_eq!(back.version, cap.version);
        assert_eq!(back.device_type, cap.device_type);
        assert_eq!(back.capacity_bytes, cap.capacity_bytes);
        assert_eq!(back.numa_nodes, cap.numa_nodes);
        assert_eq!(back.detection_method, cap.detection_method);
    }

    #[test]
    fn not_available_serialization_round_trip() {
        let cap = detect();
        let json = serde_json::to_string(&cap).expect("serialize");
        let back: CxlCapability = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.available, cap.available);
        assert_eq!(back.detection_method, cap.detection_method);
    }
}
