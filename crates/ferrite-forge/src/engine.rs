//! Per-worker `wasmtime::Engine` factory (ADR-019 §Execution & isolation).
//!
//! Gated behind the `runtime` feature so workspace builds that don't need
//! Wasmtime stay light.  In production every worker thread holds one
//! `Engine`; `Module` instances are compiled once per Engine and stored in a
//! per-thread cache (not implemented in the spike).

use wasmtime::{Config, Engine};

/// Build a `wasmtime::Engine` configured per ADR-019:
/// fuel metering on, epoch interruption on, async support off (we want
/// deterministic latency and run wasm on the calling worker).
pub struct EngineFactory;

impl EngineFactory {
    pub fn build() -> Result<Engine, wasmtime::Error> {
        let mut cfg = Config::new();
        cfg.consume_fuel(true);
        cfg.epoch_interruption(true);
        cfg.async_support(false);
        cfg.cranelift_nan_canonicalization(true);
        Engine::new(&cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_builds_with_fuel_and_epoch() {
        let engine = EngineFactory::build().expect("engine");
        // Smoke: bumping the epoch on a fresh engine is a no-op but must not panic.
        engine.increment_epoch();
    }
}
