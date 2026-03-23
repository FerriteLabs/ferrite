//! Chronicle — Branchable state.
//!
//! See ADR-021 (`docs/adrs/adr-021-chronicle-branchable-state.md`).
//!
//! P0 spike: in-memory branch registry + overlay reader + a generic
//! `BranchedKv<S>` adapter that wraps any `BaseKv` with branch-aware
//! reads/writes.  No replication wiring yet.
//!
//! # Quick start
//!
//! ```
//! use ferrite_chronicle::{BranchedKv, BranchRegistry, InMemoryKv, BaseKv};
//!
//! let base = InMemoryKv::default();
//! base.set("user:1", b"alice".to_vec());
//! let registry = BranchRegistry::new();
//! let mut branched = BranchedKv::new(base, registry);
//! let b = branched.create_branch(None, "tenant").unwrap();
//! branched.use_branch(Some(b.clone()));
//! branched.set("user:1", b"bob".to_vec()); // overlay
//! assert_eq!(branched.get("user:1"), Some(b"bob".to_vec()));
//! branched.use_branch(None);
//! assert_eq!(branched.get("user:1"), Some(b"alice".to_vec()));
//! ```

#![forbid(unsafe_code)]
#![allow(missing_docs)] // P0 spike — public docs land in P1 alongside CHR.* handlers.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod branched;
pub mod gc;
pub mod hamt;
pub mod registry;

pub use branched::{
    BaseKv, BranchStats, BranchedKv, DiffEntry, DiffOp, HistoryDump, InMemoryKv, MergeStrategy,
    OverlayDump, OverlayDumpWithStats, OverlayEntry, RetentionPolicy, SnapshotStackDump,
    TimestampedEntry,
};
pub use gc::{GcPolicy, GcResult};
pub use registry::{BranchId, BranchMeta, BranchRegistry, RegistryError};

/// HAMT-backed branched KV (for production use — O(1) branching).
/// Preparatory alias — actual HAMT integration with HybridLog comes later.
pub type HamtBranchedStore = BranchedKv<InMemoryKv>;
