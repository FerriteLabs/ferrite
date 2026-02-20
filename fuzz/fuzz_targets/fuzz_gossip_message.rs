#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Fuzz the cluster gossip message parser with arbitrary bytes.
    // This is network-facing and must never panic on malformed input.
    let mut buf = Bytes::copy_from_slice(data);
    let _ = ferrite_core::cluster::gossip::GossipMessage::from_bytes(&mut buf);
});
