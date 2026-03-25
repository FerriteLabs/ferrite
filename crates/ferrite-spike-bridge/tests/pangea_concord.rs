//! Pangea + Concord: CRDT state lives in a NumaTopology, replicas
//! converge after merge regardless of which node holds the bytes.
//!
//! Standalone helpers (`serialize_gcounter`, `deserialize_gcounter`,
//! `allocate_crdt`) provide a lower-level interface for direct
//! topology interaction.

use ferrite_concord::GCounter;
use ferrite_pangea::{InMemoryCxlAllocator, NumaTopology, RoutingPolicy};
use ferrite_spike_bridge::{
    allocate_crdt, deserialize_gcounter, serialize_gcounter, PangeaCrdtStore,
};
use std::sync::Arc;

fn topo(nodes: usize) -> NumaTopology<InMemoryCxlAllocator> {
    let allocs: Vec<Arc<InMemoryCxlAllocator>> = (0..nodes)
        .map(|_| InMemoryCxlAllocator::shared(64 * 1024, 256))
        .collect();
    NumaTopology::new(allocs, RoutingPolicy::HashMod)
}

#[test]
fn two_replicas_converge_via_pangea_store() {
    let store_a = PangeaCrdtStore::new(topo(2));
    let store_b = PangeaCrdtStore::new(topo(2));

    let mut local_a = GCounter::new();
    local_a.increment("a", 5);
    store_a.put("hits", &local_a);

    let mut local_b = GCounter::new();
    local_b.increment("b", 7);
    store_b.put("hits", &local_b);

    // Cross-pollinate: A merges B's state, B merges A's state.
    let merged_a = store_a.merge_into("hits", &local_b);
    let merged_b = store_b.merge_into("hits", &local_a);

    assert_eq!(merged_a.value(), 12);
    assert_eq!(merged_b.value(), 12);

    // Idempotent re-merge: applying the same delta again is a no-op.
    let again = store_a.merge_into("hits", &local_b);
    assert_eq!(again.value(), 12);

    // Bytes survived a full encode → NUMA write → NUMA read → decode round-trip.
    let reread = store_a.get("hits").expect("present");
    assert_eq!(reread.slot("a"), 5);
    assert_eq!(reread.slot("b"), 7);
}

#[test]
fn gcounter_serialization_round_trip() {
    let mut counter = GCounter::new();
    counter.increment("node-1", 10);
    counter.increment("node-2", 20);

    let bytes = serialize_gcounter(&counter);
    assert!(!bytes.is_empty());

    let decoded = deserialize_gcounter(&bytes).expect("valid gcounter");
    assert_eq!(decoded.value(), 30);
    assert_eq!(decoded.slot("node-1"), 10);
    assert_eq!(decoded.slot("node-2"), 20);

    // Malformed bytes return None instead of panicking.
    assert!(deserialize_gcounter(b"not-json").is_none());
    assert!(deserialize_gcounter(&[]).is_none());
}

#[test]
fn allocate_crdt_stores_and_reads_back_from_topology() {
    let topology = topo(3);

    let mut counter = GCounter::new();
    counter.increment("west", 42);
    counter.increment("east", 58);

    // Allocate directly into the topology.
    let locator = allocate_crdt(&topology, "pageviews", &counter).expect("allocate");
    assert!(locator.node < 3);

    // Read raw bytes back and deserialize.
    let raw = topology.read("pageviews").expect("present");
    let restored = deserialize_gcounter(&raw).expect("valid gcounter");
    assert_eq!(restored.value(), 100);
    assert_eq!(restored.slot("west"), 42);
    assert_eq!(restored.slot("east"), 58);
}

#[test]
fn allocate_crdt_overwrites_previous_value() {
    let topology = topo(2);

    let mut v1 = GCounter::new();
    v1.increment("r1", 1);
    allocate_crdt(&topology, "counter:x", &v1).expect("first alloc");

    // Overwrite with a larger counter.
    let mut v2 = GCounter::new();
    v2.increment("r1", 1);
    v2.increment("r2", 9);
    allocate_crdt(&topology, "counter:x", &v2).expect("second alloc");

    let raw = topology.read("counter:x").expect("present");
    let restored = deserialize_gcounter(&raw).expect("valid");
    assert_eq!(restored.value(), 10);
    assert_eq!(restored.slot("r1"), 1);
    assert_eq!(restored.slot("r2"), 9);
}

#[test]
fn store_and_standalone_helpers_interoperate() {
    let topology = topo(2);
    let store = PangeaCrdtStore::new(topo(2));

    // Write via store, read via standalone helpers on a separate topology.
    let mut c = GCounter::new();
    c.increment("a", 3);
    store.put("shared", &c);

    // Serialize the same counter and write to the other topology.
    allocate_crdt(&topology, "shared", &c).expect("alloc");
    let raw = topology.read("shared").expect("present");
    let decoded = deserialize_gcounter(&raw).expect("valid");
    assert_eq!(decoded.value(), 3);
    assert_eq!(decoded.slot("a"), 3);

    // The store can also read its own data back.
    let from_store = store.get("shared").expect("present");
    assert_eq!(from_store.value(), decoded.value());
}
