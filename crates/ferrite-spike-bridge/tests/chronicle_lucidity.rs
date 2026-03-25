//! Chronicle + Lucidity: every KV mutation produces a verifiable
//! audit leaf, and a witness records each STH without seeing forks.

use ferrite_chronicle::InMemoryKv;
use ferrite_lucidity::{verify_consistency, verify_inclusion, InMemoryWitness, MockSigner};
use ferrite_spike_bridge::{audit_chronicle_op, verify_chronicle_audit, ChronicleAuditedKv};

#[test]
fn writes_emit_inclusion_proofs_and_witness_advances() {
    let kv = ChronicleAuditedKv::new(InMemoryKv::new(), Box::new(MockSigner::new("k1")));
    let witness = InMemoryWitness::new();

    // Three writes on main → audit log has 3 leaves.
    let i0 = kv.set("user:1", b"alice".to_vec());
    let i1 = kv.set("user:2", b"bob".to_vec());
    let i2 = kv.set("user:1", b"alice2".to_vec());
    assert_eq!((i0, i1, i2), (0, 1, 2));

    let audit = kv.audit();
    assert_eq!(audit.len(), 3);

    // Witness records the current STH — no fork, no regression.
    let sth = audit.signed_tree_head();
    assert_eq!(sth.size, 3);
    witness.record(&sth).expect("first record");
    assert_eq!(witness.highest(), 3);

    // An inclusion proof for index 1 verifies against the current root.
    let proof = audit.inclusion_proof(1).expect("inclusion");
    assert!(verify_inclusion(&proof, &sth.root));

    // Branch + write only mutates the branch overlay, but the audit log
    // grows globally — branch state and audit history are orthogonal.
    let b = kv.create_branch(None, "tenant-a");
    kv.use_branch(Some(b));
    kv.set("user:3", b"carol".to_vec());
    assert_eq!(audit.len(), 4);

    let sth2 = audit.signed_tree_head();
    witness.record(&sth2).expect("monotonic advance");
    assert_eq!(witness.highest(), 4);
}

#[test]
fn standalone_audit_leaf_and_verification() {
    let signer = Box::new(MockSigner::new("standalone"));
    let log = ferrite_lucidity::AuditLog::new(signer);

    // Produce leaves via the standalone helper for set / del / forget ops.
    let leaf_set = audit_chronicle_op("set", "key:a", Some(b"val-a"), 100);
    let leaf_del = audit_chronicle_op("del", "key:b", None, 101);
    let leaf_forget = audit_chronicle_op("forget", "key:c", None, 102);

    log.append(leaf_set);
    log.append(leaf_del);
    log.append(leaf_forget);
    assert_eq!(log.len(), 3);

    // Every leaf should be independently verifiable via inclusion proof.
    for idx in 0..3 {
        assert!(
            verify_chronicle_audit(&log, idx),
            "inclusion proof failed for index {idx}"
        );
    }

    // Out-of-range index returns false, not a panic.
    assert!(!verify_chronicle_audit(&log, 99));
}

#[test]
fn delete_produces_audit_trail_and_consistency_holds() {
    let kv = ChronicleAuditedKv::new(InMemoryKv::new(), Box::new(MockSigner::new("k2")));

    kv.set("session:1", b"active".to_vec());
    kv.set("session:2", b"active".to_vec());

    let audit = kv.audit();
    let sth_before = audit.signed_tree_head();
    assert_eq!(sth_before.size, 2);

    // Delete emits an audit leaf; the value is gone but the proof stays.
    let (existed, del_idx) = kv.del("session:1");
    assert!(existed);
    assert_eq!(del_idx, 2);
    assert!(kv.get("session:1").is_none());

    let sth_after = audit.signed_tree_head();
    assert_eq!(sth_after.size, 3);

    // Consistency proof: the tree at size 2 is a prefix of the tree at size 3.
    let cp = audit
        .consistency_proof(sth_before.size)
        .expect("consistency");
    assert!(verify_consistency(&cp, &sth_before.root, &sth_after.root));

    // The delete leaf itself is verifiable.
    assert!(verify_chronicle_audit(&audit, del_idx));
}
