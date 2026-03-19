//! Forge telemetry — metric recording for FN.* operations.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Global counters for Forge operations (production would use OTel/metrics crate).
pub static FN_CALLS_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static FN_CALL_ERRORS_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static FN_MODULES_LOADED: AtomicU64 = AtomicU64::new(0);

/// Record a successful FN.CALL.
pub fn record_call(fn_name: &str, duration: Duration) {
    FN_CALLS_TOTAL.fetch_add(1, Ordering::Relaxed);
    tracing::debug!(
        fn_name,
        duration_ms = duration.as_millis() as u64,
        "forge.fn_call"
    );
}

/// Record a failed FN.CALL.
pub fn record_call_error(fn_name: &str, reason: &str) {
    FN_CALL_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
    tracing::warn!(fn_name, reason, "forge.fn_call_error");
}

/// Record module load/unload.
pub fn record_module_loaded() {
    FN_MODULES_LOADED.fetch_add(1, Ordering::Relaxed);
}

pub fn record_module_unloaded() {
    FN_MODULES_LOADED.fetch_sub(1, Ordering::Relaxed);
}

/// Snapshot of current telemetry counters.
pub struct TelemetrySnapshot {
    pub calls_total: u64,
    pub errors_total: u64,
    pub modules_loaded: u64,
}

impl TelemetrySnapshot {
    pub fn capture() -> Self {
        Self {
            calls_total: FN_CALLS_TOTAL.load(Ordering::Relaxed),
            errors_total: FN_CALL_ERRORS_TOTAL.load(Ordering::Relaxed),
            modules_loaded: FN_MODULES_LOADED.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn reset_counters() {
        FN_CALLS_TOTAL.store(0, Ordering::Relaxed);
        FN_CALL_ERRORS_TOTAL.store(0, Ordering::Relaxed);
        FN_MODULES_LOADED.store(0, Ordering::Relaxed);
    }

    #[test]
    fn snapshot_captures_current_values() {
        reset_counters();
        FN_CALLS_TOTAL.store(10, Ordering::Relaxed);
        FN_CALL_ERRORS_TOTAL.store(3, Ordering::Relaxed);
        FN_MODULES_LOADED.store(2, Ordering::Relaxed);

        let snap = TelemetrySnapshot::capture();
        assert_eq!(snap.calls_total, 10);
        assert_eq!(snap.errors_total, 3);
        assert_eq!(snap.modules_loaded, 2);
    }

    #[test]
    fn record_call_increments_counter() {
        reset_counters();
        record_call("my_fn", Duration::from_millis(5));
        record_call("my_fn", Duration::from_millis(10));
        assert_eq!(FN_CALLS_TOTAL.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn record_call_error_increments_error_counter() {
        reset_counters();
        record_call_error("bad_fn", "timeout");
        assert_eq!(FN_CALL_ERRORS_TOTAL.load(Ordering::Relaxed), 1);
    }
}
