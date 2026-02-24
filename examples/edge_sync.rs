#![allow(clippy::unwrap_used)]
//! Edge-to-Cloud Synchronisation Example
//!
//! Demonstrates how an edge device can:
//! 1. Collect sensor data locally with bounded memory
//! 2. Aggregate readings into time-bucketed summaries
//! 3. Queue operations for offline resilience
//! 4. Sync aggregated data to a central Ferrite server
//!
//! **Note**: This example requires the `ferrite::embedded::iot` module which
//! is not yet implemented. It serves as a design sketch for the future IoT API.
//!
//! Run with: `cargo run --example edge_sync`

fn main() {
    eprintln!(
        "edge_sync example is disabled: ferrite::embedded::iot module is not yet implemented"
    );
}
