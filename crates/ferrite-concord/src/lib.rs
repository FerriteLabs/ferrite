//! Concord — Multi-master CRDTs.
//!
//! See ADR-022 (`docs/adrs/adr-022-concord-multi-master-crdts.md`).
//!
//! P0 spike: pure-Rust state-based CRDT primitives with property tests for
//! commutativity, associativity, and idempotence of `merge`.  No
//! replication wiring yet — that lands in P1 against the real transport.
//!
//! # Quick start
//!
//! ```
//! use ferrite_concord::{Crdt, GCounter};
//!
//! let mut a = GCounter::default();
//! let mut b = GCounter::default();
//! a.increment("node-a", 5);
//! b.increment("node-b", 3);
//! a.merge(&b);
//! b.merge(&a);
//! assert_eq!(a.value(), 8);
//! assert_eq!(b.value(), 8); // strong eventual consistency
//! ```

#![forbid(unsafe_code)]
#![allow(missing_docs)] // P0 spike — public docs land in P1 alongside CON.* handlers.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod antientropy;
pub mod chaos;
pub mod delta;
pub mod dvv;
pub mod g_counter;
pub mod gossip;
pub mod lww_register;
pub mod mv_register;
pub mod or_set;
pub mod pn_counter;
pub mod routing;

pub use antientropy::AntiEntropyTree;
pub use delta::Delta;
pub use dvv::{DottedVersionVector, VersionVector};
pub use g_counter::GCounter;
pub use gossip::{CrdtType, GossipCluster, GossipConfig, GossipMessage, GossipNode};
pub use lww_register::{LwwRegister, LwwWrite};
pub use mv_register::{MvRegister, MvWrite, VectorClock};
pub use or_set::OrSet;
pub use pn_counter::PnCounter;
pub use routing::{RoutingRule, SovereigntyRouter};

/// Common surface every state-based CRDT in this crate implements.
pub trait Crdt {
    /// Merge `other` into self.  Must be commutative, associative, and idempotent.
    fn merge(&mut self, other: &Self);
}
