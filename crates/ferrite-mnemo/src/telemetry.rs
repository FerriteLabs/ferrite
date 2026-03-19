//! Mnemo telemetry — metric recording for MEM.* operations.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Global counters for Mnemo operations (production would use OTel/metrics crate).
pub static MNEMO_REQUESTS_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static MNEMO_RECALL_COUNT: AtomicU64 = AtomicU64::new(0);
pub static MNEMO_RECORDS_RESIDENT: AtomicU64 = AtomicU64::new(0);

/// Record any MEM.* request.
pub fn record_request(command: &str) {
    MNEMO_REQUESTS_TOTAL.fetch_add(1, Ordering::Relaxed);
    tracing::debug!(command, "mnemo.request");
}

/// Record a MEM.RECALL with latency and result count.
pub fn record_recall(latency: Duration, result_count: usize) {
    MNEMO_RECALL_COUNT.fetch_add(1, Ordering::Relaxed);
    tracing::debug!(
        latency_ms = latency.as_millis() as u64,
        result_count,
        "mnemo.recall"
    );
}

/// Snapshot of current Mnemo telemetry counters.
pub struct MnemoTelemetrySnapshot {
    pub requests_total: u64,
    pub recall_count: u64,
    pub records_resident: u64,
}

impl MnemoTelemetrySnapshot {
    pub fn capture() -> Self {
        Self {
            requests_total: MNEMO_REQUESTS_TOTAL.load(Ordering::Relaxed),
            recall_count: MNEMO_RECALL_COUNT.load(Ordering::Relaxed),
            records_resident: MNEMO_RECORDS_RESIDENT.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn reset_counters() {
        MNEMO_REQUESTS_TOTAL.store(0, Ordering::Relaxed);
        MNEMO_RECALL_COUNT.store(0, Ordering::Relaxed);
        MNEMO_RECORDS_RESIDENT.store(0, Ordering::Relaxed);
    }

    #[test]
    fn snapshot_captures_current_values() {
        reset_counters();
        MNEMO_REQUESTS_TOTAL.store(42, Ordering::Relaxed);
        MNEMO_RECALL_COUNT.store(7, Ordering::Relaxed);
        MNEMO_RECORDS_RESIDENT.store(100, Ordering::Relaxed);

        let snap = MnemoTelemetrySnapshot::capture();
        assert_eq!(snap.requests_total, 42);
        assert_eq!(snap.recall_count, 7);
        assert_eq!(snap.records_resident, 100);
    }

    #[test]
    fn record_request_increments_counter() {
        reset_counters();
        record_request("MEM.PUT");
        record_request("MEM.GET");
        record_recall(Duration::from_millis(5), 3);
        assert_eq!(MNEMO_REQUESTS_TOTAL.load(Ordering::Relaxed), 2);
        assert_eq!(MNEMO_RECALL_COUNT.load(Ordering::Relaxed), 1);
    }
}
