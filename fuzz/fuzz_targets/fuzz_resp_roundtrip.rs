#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Fuzz the RESP parser and verify encode→parse roundtrip consistency.
    // If parsing succeeds, re-encoding and re-parsing should yield the same frame.
    let mut buf = BytesMut::from(data);
    if let Ok(Some(frame)) = ferrite_core::protocol::parse_frame(&mut buf) {
        let mut encoded = BytesMut::new();
        ferrite_core::protocol::encode_frame(&frame, &mut encoded);

        if let Ok(Some(reparsed)) = ferrite_core::protocol::parse_frame(&mut encoded) {
            assert_eq!(
                format!("{:?}", frame),
                format!("{:?}", reparsed),
                "Roundtrip mismatch"
            );
        }
    }
});
