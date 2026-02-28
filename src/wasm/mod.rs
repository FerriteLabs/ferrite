//! # WASM User-Defined Functions Runtime
//!
//! This module provides the top-level integration layer for Ferrite's WASM UDF
//! subsystem. It wraps the runtime components from `ferrite-plugins` and exposes
//! them to the command executor for FUNCTION/FCALL command handling.
//!
//! ## Architecture
//!
//! ```text
//! Commands (FCALL, FUNCTION LOAD, WASM.*)
//!        |
//!        v
//! src/wasm/  <-- this module (integration layer)
//!   - runtime.rs   WasmRuntime: Engine + Linker + Module cache
//!   - host.rs      Host function bindings (ferrite_get, ferrite_set, ...)
//!   - registry.rs  UdfRegistry: maps function names to WASM modules
//!        |
//!        v
//! crates/ferrite-plugins/src/wasm/  (core types, executor, permissions)
//! ```

pub mod playground;

#[cfg(feature = "wasm")]
pub mod runtime;

#[cfg(feature = "wasm")]
pub mod host;

#[cfg(feature = "wasm")]
pub mod registry;

#[cfg(feature = "wasm")]
pub use registry::UdfRegistry;

#[cfg(feature = "wasm")]
pub use runtime::WasmRuntime;
