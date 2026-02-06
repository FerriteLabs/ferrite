use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn systemtime_to_epoch_ms(expiry: SystemTime) -> Option<u64> {
    expiry
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|d| d.as_millis() as u64)
}
