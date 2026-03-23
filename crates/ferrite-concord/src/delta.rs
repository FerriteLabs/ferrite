//! Delta-state CRDTs.
//!
//! `Delta` extends [`Crdt`] with a per-mutation "delta" type that
//! captures only what changed.  Replicas ship deltas (small) instead
//! of full state (big), trading bandwidth for a slightly more complex
//! API.  See Almeida, Shoker & Baquero, "Delta state replicated data
//! types" (JPDC 2018).

use crate::Crdt;

/// CRDT with a delta channel.
///
/// `mutate` returns the delta produced by the mutation; `merge_delta`
/// applies a delta from another replica.  The combination of
/// `merge_delta` calls must converge to the same state as `merge` of
/// the corresponding full states (delta-state convergence theorem).
pub trait Delta: Crdt {
    type Mutation;
    type DeltaState: Clone + std::fmt::Debug;

    /// Apply a local mutation and return the delta to ship.
    fn mutate(&mut self, mutation: Self::Mutation) -> Self::DeltaState;

    /// Merge a delta received from another replica.
    fn merge_delta(&mut self, delta: &Self::DeltaState);
}
