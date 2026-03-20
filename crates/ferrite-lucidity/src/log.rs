//! AuditLog: append leaves, snapshot signed tree heads, produce proofs.

use crate::leaf::Leaf;
use crate::merkle::{
    consistency_proof, inclusion_proof, root, ConsistencyProof, Hash, InclusionProof,
};
use crate::signer::{SignedTreeHead, Signer};
use parking_lot::RwLock;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum AuditError {
    #[error("leaf index {0} out of range")]
    OutOfRange(usize),
    #[error("audit log is empty")]
    Empty,
}

pub struct AuditLog {
    leaves: Arc<RwLock<Vec<Leaf>>>,
    signer: Box<dyn Signer>,
}

impl AuditLog {
    pub fn new(signer: Box<dyn Signer>) -> Self {
        Self {
            leaves: Arc::default(),
            signer,
        }
    }

    pub fn append(&self, leaf: Leaf) -> usize {
        let mut g = self.leaves.write();
        g.push(leaf);
        g.len() - 1
    }

    pub fn len(&self) -> usize {
        self.leaves.read().len()
    }
    pub fn is_empty(&self) -> bool {
        self.leaves.read().is_empty()
    }

    pub fn root(&self) -> Hash {
        let g = self.leaves.read();
        let hashes: Vec<Hash> = g.iter().map(Leaf::merkle_hash).collect();
        root(&hashes)
    }

    pub fn signed_tree_head(&self) -> SignedTreeHead {
        let size = self.len();
        let root = self.root();
        let ts_ms = 0; // P0 stub — real impl pulls from a clock source.
        let signature = self.signer.sign_sth(size, &root, ts_ms);
        SignedTreeHead {
            size,
            root,
            ts_ms,
            signer_id: self.signer.id().to_string(),
            signature,
        }
    }

    pub fn inclusion_proof(&self, index: usize) -> Result<InclusionProof, AuditError> {
        let g = self.leaves.read();
        let hashes: Vec<Hash> = g.iter().map(Leaf::merkle_hash).collect();
        inclusion_proof(&hashes, index).ok_or(AuditError::OutOfRange(index))
    }

    pub fn consistency_proof(&self, old_size: usize) -> Result<ConsistencyProof, AuditError> {
        if self.is_empty() {
            return Err(AuditError::Empty);
        }
        let g = self.leaves.read();
        let hashes: Vec<Hash> = g.iter().map(Leaf::merkle_hash).collect();
        consistency_proof(&hashes, old_size).ok_or(AuditError::OutOfRange(old_size))
    }

    /// Snapshot all leaves for persistence.  The signer is not exported — it
    /// must be supplied again at restore via [`AuditLog::from_leaves`].
    pub fn snapshot_leaves(&self) -> Vec<Leaf> {
        self.leaves.read().clone()
    }

    /// Reconstruct an `AuditLog` from a previously-snapshot vector of leaves.
    pub fn from_leaves(signer: Box<dyn Signer>, leaves: Vec<Leaf>) -> Self {
        Self {
            leaves: Arc::new(RwLock::new(leaves)),
            signer,
        }
    }

    /// Replace the active signer (used during key rotation).
    pub fn set_signer(&mut self, signer: Box<dyn Signer>) {
        self.signer = signer;
    }

    /// Get the current signer's ID.
    pub fn signer_id(&self) -> &str {
        self.signer.id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::leaf::Op;
    use crate::merkle::{verify_consistency, verify_inclusion};
    use crate::signer::MockSigner;

    fn fresh_log() -> AuditLog {
        AuditLog::new(Box::new(MockSigner::new("test")))
    }

    #[test]
    fn append_advances_length() {
        let log = fresh_log();
        let i = log.append(Leaf::for_set(b"k", b"v", 1));
        assert_eq!(i, 0);
        assert_eq!(log.len(), 1);
    }

    #[test]
    fn signed_tree_head_round_trips() {
        let log = fresh_log();
        log.append(Leaf::for_set(b"k", b"v", 1));
        log.append(Leaf::for_set(b"k2", b"v2", 2));
        let sth = log.signed_tree_head();
        assert_eq!(sth.size, 2);
        let signer = MockSigner::new("test");
        assert!(signer.verify(&sth));
    }

    #[test]
    fn inclusion_proof_for_appended_leaf_verifies() {
        let log = fresh_log();
        for i in 0..5 {
            log.append(Leaf::for_set(b"k", b"v", i));
        }
        let sth = log.signed_tree_head();
        for idx in 0..5 {
            let p = log.inclusion_proof(idx).unwrap();
            assert!(verify_inclusion(&p, &sth.root), "idx {idx}");
        }
    }

    #[test]
    fn consistency_proof_links_old_and_new() {
        let log = fresh_log();
        for i in 0..3 {
            log.append(Leaf::for_set(b"k", b"v", i));
        }
        let sth_old = log.signed_tree_head();
        for i in 3..6 {
            log.append(Leaf::for_set(b"k", b"v", i));
        }
        let sth_new = log.signed_tree_head();
        let proof = log.consistency_proof(sth_old.size).unwrap();
        assert!(verify_consistency(&proof, &sth_old.root, &sth_new.root));
    }

    #[test]
    fn forget_leaf_kept_in_tree_but_value_unrecoverable() {
        let log = fresh_log();
        log.append(Leaf::for_set(b"k", b"sensitive", 1));
        log.append(Leaf::for_forget(b"k", 2));
        let g = log.leaves.read();
        assert_eq!(g[1].op, Op::Forget);
        assert_eq!(g[1].value_hash, [0u8; 32]);
        assert_eq!(g.len(), 2);
    }

    #[test]
    fn snapshot_and_restore_round_trip() {
        let log = fresh_log();
        log.append(Leaf::for_set(b"k", b"v", 1));
        log.append(Leaf::for_set(b"k", b"v2", 2));
        let leaves = log.snapshot_leaves();
        let json = serde_json::to_string(&leaves).unwrap();
        let restored: Vec<Leaf> = serde_json::from_str(&json).unwrap();
        let log2 = AuditLog::from_leaves(Box::new(MockSigner::new("test")), restored);
        assert_eq!(log2.len(), 2);
        assert_eq!(log2.signed_tree_head().root, log.signed_tree_head().root);
    }
}
