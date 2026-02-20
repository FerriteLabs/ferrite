#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Fuzz the command parser by first parsing bytes as a RESP frame,
    // then feeding the frame into Command::from_frame.
    // Both stages must handle arbitrary input without panicking.
    let mut buf = BytesMut::from(data);
    if let Ok(Some(frame)) = ferrite_core::protocol::parse_frame(&mut buf) {
        let _ = ferrite::commands::Command::from_frame(frame);
    }
});
