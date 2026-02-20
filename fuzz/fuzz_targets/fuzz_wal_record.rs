#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Fuzz the WAL log entry decoder with arbitrary bytes.
    // WAL records are read from disk and must handle corruption gracefully.
    if data.is_empty() {
        return;
    }
    let type_code = data[0];
    let _ = ferrite_core::transaction::wal::LogEntryType::from_bytes(type_code, &data[1..]);
});
