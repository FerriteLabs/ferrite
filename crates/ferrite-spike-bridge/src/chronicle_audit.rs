//! ChronicleAuditedKv: every mutation emits an audit leaf.
//!
//! Also provides standalone helpers for producing and verifying
//! audit leaves outside the `ChronicleAuditedKv` wrapper.

use ferrite_chronicle::{BaseKv, BranchId, BranchRegistry, BranchedKv};
use ferrite_lucidity::{verify_inclusion, AuditLog, Leaf, Signer};
use std::sync::Arc;

/// Produce an audit [`Leaf`] for a Chronicle key-value operation.
///
/// `op` must be one of `"set"`, `"del"`, or `"forget"`.
/// For `"set"` operations `value` should be `Some`; for deletes/forgets it is ignored.
pub fn audit_chronicle_op(op: &str, key: &str, value: Option<&[u8]>, ts: u64) -> Leaf {
    match op {
        "set" => Leaf::for_set(key.as_bytes(), value.unwrap_or(&[]), ts),
        "del" => Leaf::for_del(key.as_bytes(), ts),
        "forget" => Leaf::for_forget(key.as_bytes(), ts),
        other => panic!("unknown chronicle op: {other}"),
    }
}

/// Verify that the leaf at `index` in `log` has a valid inclusion proof
/// against the log's current root.
pub fn verify_chronicle_audit(log: &AuditLog, index: usize) -> bool {
    let sth = log.signed_tree_head();
    match log.inclusion_proof(index) {
        Ok(proof) => verify_inclusion(&proof, &sth.root),
        Err(_) => false,
    }
}

pub struct ChronicleAuditedKv<S: BaseKv> {
    kv: BranchedKv<S>,
    audit: Arc<AuditLog>,
    seq: parking_lot::Mutex<u64>,
}

impl<S: BaseKv> ChronicleAuditedKv<S> {
    pub fn new(base: S, signer: Box<dyn Signer>) -> Self {
        Self {
            kv: BranchedKv::new(base, BranchRegistry::new()),
            audit: Arc::new(AuditLog::new(signer)),
            seq: parking_lot::Mutex::new(0),
        }
    }

    fn next_ts(&self) -> u64 {
        let mut g = self.seq.lock();
        *g += 1;
        *g
    }

    pub fn audit(&self) -> Arc<AuditLog> {
        self.audit.clone()
    }
    pub fn registry(&self) -> &BranchRegistry {
        self.kv.registry()
    }

    pub fn create_branch(&self, parent: Option<BranchId>, tenant: &str) -> BranchId {
        self.kv
            .create_branch(parent, tenant)
            .expect("branch create")
    }

    pub fn use_branch(&self, b: Option<BranchId>) {
        self.kv.use_branch(b);
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.kv.get(key)
    }

    pub fn set(&self, key: &str, value: Vec<u8>) -> usize {
        let leaf = Leaf::for_set(key.as_bytes(), &value, self.next_ts());
        self.kv.set(key, value);
        self.audit.append(leaf)
    }

    pub fn del(&self, key: &str) -> (bool, usize) {
        let leaf = Leaf::for_del(key.as_bytes(), self.next_ts());
        let existed = self.kv.del(key);
        let idx = self.audit.append(leaf);
        (existed, idx)
    }
}
