//! PangeaCrdtStore: stores serialised CRDT state in a NumaTopology.
//!
//! Also provides standalone helpers for serializing / deserializing
//! a [`GCounter`] and allocating it directly via a [`NumaTopology`].

use ferrite_concord::{Crdt, GCounter};
use ferrite_pangea::{AllocError, CxlAllocator, Locator, NumaTopology};

/// Serialize a [`GCounter`] to JSON bytes for CXL tier storage.
pub fn serialize_gcounter(counter: &GCounter) -> Vec<u8> {
    serde_json::to_vec(counter).expect("gcounter encode")
}

/// Deserialize a [`GCounter`] from bytes previously produced by
/// [`serialize_gcounter`]. Returns `None` on malformed input.
pub fn deserialize_gcounter(bytes: &[u8]) -> Option<GCounter> {
    serde_json::from_slice(bytes).ok()
}

/// Allocate a serialized [`GCounter`] in the given topology under `key`.
///
/// Any previous value stored under `key` is freed first so the
/// topology always holds the latest state.
pub fn allocate_crdt<A: CxlAllocator>(
    topo: &NumaTopology<A>,
    key: &str,
    counter: &GCounter,
) -> Result<Locator, AllocError> {
    let bytes = serialize_gcounter(counter);
    topo.free(key);
    topo.allocate(key, &bytes)
}

pub struct PangeaCrdtStore<A: CxlAllocator> {
    topo: NumaTopology<A>,
}

impl<A: CxlAllocator> PangeaCrdtStore<A> {
    pub fn new(topo: NumaTopology<A>) -> Self {
        Self { topo }
    }

    pub fn put(&self, key: &str, counter: &GCounter) {
        let bytes = serialize_gcounter(counter);
        self.topo.free(key);
        self.topo.allocate(key, &bytes).expect("allocate");
    }

    pub fn get(&self, key: &str) -> Option<GCounter> {
        self.topo.read(key).and_then(|b| deserialize_gcounter(&b))
    }

    /// Read-merge-write: applies `delta` to the stored counter, returning the new value.
    pub fn merge_into(&self, key: &str, delta: &GCounter) -> GCounter {
        let mut current = self.get(key).unwrap_or_default();
        current.merge(delta);
        self.put(key, &current);
        current
    }

    pub fn topology(&self) -> &NumaTopology<A> {
        &self.topo
    }
}
