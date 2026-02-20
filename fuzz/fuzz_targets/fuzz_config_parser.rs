#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Fuzz the TOML configuration parser with arbitrary bytes.
    // Config parsing must handle malformed input without panicking.
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = ferrite_core::config::Config::from_str(s);
    }
});
