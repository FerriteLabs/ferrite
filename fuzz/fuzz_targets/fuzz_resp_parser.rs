#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Fuzz the RESP protocol parser with arbitrary bytes.
    // The parser should never panic — it should return Ok or Err for any input.
    let mut buf = BytesMut::from(data);
    let _ = ferrite_core::protocol::parse_frame(&mut buf);
});
