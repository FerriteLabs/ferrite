//! Forge — WASM in-DB functions.
//!
//! See ADR-019 (`docs/adrs/adr-019-forge-wasm-functions.md`).
//!
//! Public types:
//! - [`ResourceBudget`]: fuel + memory + wall-time caps per call.
//! - [`Module`]: a registered WASM module with metadata and ACL.
//! - [`ModuleRegistry`]: thread-safe registry indexed by name.
//! - [`engine::EngineFactory`] (feature `runtime`): per-worker `wasmtime::Engine` builder.
//!
//! # Quick start (no runtime feature — ACL + registry only)
//!
//! ```
//! use ferrite_forge::{Module, ModuleAcl, ModuleRegistry};
//!
//! let registry = ModuleRegistry::default();
//! let acl = ModuleAcl {
//!     read_keys: vec!["user:*".into()],
//!     write_keys: vec!["scratch:*".into()],
//! };
//! let module = Module::new("hello", b"\0asm\x01\0\0\0".to_vec(), acl);
//! registry.insert(module);
//! assert!(registry.get("hello").is_some());
//!
//! // Replicate to another node by encoding + decoding the envelope.
//! let bytes = registry.get("hello").unwrap().encode().unwrap();
//! let restored = Module::decode(&bytes).unwrap();
//! assert_eq!(restored.meta.name, "hello");
//! ```

#![forbid(unsafe_code)]
#![allow(missing_docs)] // P0 spike — public docs land in P1 alongside FN.* handlers.
#![cfg_attr(
    test,
    allow(clippy::unwrap_used, clippy::float_cmp, clippy::expect_used)
)]

pub mod budget;
pub mod module;
pub mod rate_limiter;
pub mod registry;
pub mod signing;
pub mod telemetry;

pub mod host;

#[cfg(feature = "runtime")]
pub mod engine;
#[cfg(feature = "runtime")]
pub mod exec;

pub use budget::{BudgetError, ResourceBudget};
pub use host::{AclHostContext, HostContext, HostError, InMemoryHostContext};
pub use module::{CodecError, Module, ModuleAcl, ModuleEnvelope, ModuleMeta};
pub use registry::{ModuleRegistry, ModuleVersion, RegistryError};
pub use signing::{SignedEnvelope, SignerKey, SigningError, SigningPolicy};

#[cfg(feature = "runtime")]
pub use exec::{ExecError, Executor};
#[cfg(feature = "runtime")]
pub use host::{link_host_api, HostState};
