use std::time::{SystemTime, UNIX_EPOCH};

/// Converts a [`SystemTime`] to milliseconds since the Unix epoch.
///
/// Returns `None` if the given time is before the Unix epoch (1970-01-01).
pub(crate) fn systemtime_to_epoch_ms(expiry: SystemTime) -> Option<u64> {
    expiry
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|d| d.as_millis() as u64)
}
