#![no_main]

use bytes::BytesMut;
use ferrite_core::protocol::Frame;
use libfuzzer_sys::fuzz_target;

fn frames_equivalent(left: &Frame, right: &Frame) -> bool {
    match (left, right) {
        (Frame::Double(left), Frame::Double(right)) => {
            (left.is_nan() && right.is_nan()) || left == right
        }
        (Frame::Array(left), Frame::Array(right)) => match (left, right) {
            (Some(left), Some(right)) => {
                left.len() == right.len()
                    && left
                        .iter()
                        .zip(right)
                        .all(|(left, right)| frames_equivalent(left, right))
            }
            (None, None) => true,
            _ => false,
        },
        (Frame::Map(left), Frame::Map(right)) => {
            left.len() == right.len()
                && left.iter().all(|(key, value)| {
                    right
                        .get(key)
                        .is_some_and(|other| frames_equivalent(value, other))
                })
        }
        (Frame::Set(left), Frame::Set(right)) | (Frame::Push(left), Frame::Push(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| frames_equivalent(left, right))
        }
        _ => left == right,
    }
}

fuzz_target!(|data: &[u8]| {
    // Fuzz the RESP parser and verify encode→parse roundtrip consistency.
    // If parsing succeeds, re-encoding and re-parsing should yield the same frame.
    let mut buf = BytesMut::from(data);
    if let Ok(Some(frame)) = ferrite_core::protocol::parse_frame(&mut buf) {
        let mut encoded = BytesMut::new();
        ferrite_core::protocol::encode_frame(&frame, &mut encoded);

        if let Ok(Some(reparsed)) = ferrite_core::protocol::parse_frame(&mut encoded) {
            assert!(frames_equivalent(&frame, &reparsed), "Roundtrip mismatch");
        }
    }
});
