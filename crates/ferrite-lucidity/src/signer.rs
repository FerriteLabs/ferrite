//! Signer trait + Mock implementation.

use crate::merkle::Hash;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedTreeHead {
    pub size: usize,
    pub root: Hash,
    pub ts_ms: u64,
    pub signer_id: String,
    pub signature: Vec<u8>,
}

pub trait Signer: Send + Sync {
    fn id(&self) -> &str;
    /// Sign `(size || root || ts_ms)`.  Real impl uses ML-DSA-65; the
    /// mock impl returns a deterministic byte string so tests can assert
    /// round-trips without picking a crypto crate at the spike stage.
    fn sign_sth(&self, size: usize, root: &Hash, ts_ms: u64) -> Vec<u8>;

    /// Verify a signature produced by this signer.  Mock impl recomputes
    /// the deterministic byte string and compares.
    fn verify(&self, sth: &SignedTreeHead) -> bool;
}

/// Deterministic placeholder signer used in tests and the eval harness.
/// **Not cryptographically secure** — it concatenates the inputs and
/// hashes them with SHA-256 prefixed by the signer id.
pub struct MockSigner {
    id: String,
}

impl MockSigner {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }

    fn payload(&self, size: usize, root: &Hash, ts_ms: u64) -> Vec<u8> {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(self.id.as_bytes());
        h.update(size.to_be_bytes());
        h.update(root);
        h.update(ts_ms.to_be_bytes());
        h.finalize().to_vec()
    }
}

impl Signer for MockSigner {
    fn id(&self) -> &str {
        &self.id
    }

    fn sign_sth(&self, size: usize, root: &Hash, ts_ms: u64) -> Vec<u8> {
        self.payload(size, root, ts_ms)
    }

    fn verify(&self, sth: &SignedTreeHead) -> bool {
        sth.signer_id == self.id && sth.signature == self.payload(sth.size, &sth.root, sth.ts_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mock_signer_verifies_its_own_signature() {
        let s = MockSigner::new("sig1");
        let root = [9u8; 32];
        let sig = s.sign_sth(10, &root, 1234);
        let sth = SignedTreeHead {
            size: 10,
            root,
            ts_ms: 1234,
            signer_id: "sig1".into(),
            signature: sig,
        };
        assert!(s.verify(&sth));
    }

    #[test]
    fn mock_signer_rejects_tampered_signature() {
        let s = MockSigner::new("sig1");
        let root = [9u8; 32];
        let mut sig = s.sign_sth(10, &root, 1234);
        sig[0] = sig[0].wrapping_add(1);
        let sth = SignedTreeHead {
            size: 10,
            root,
            ts_ms: 1234,
            signer_id: "sig1".into(),
            signature: sig,
        };
        assert!(!s.verify(&sth));
    }

    #[test]
    fn mock_signer_rejects_wrong_signer_id() {
        let s = MockSigner::new("sig1");
        let root = [9u8; 32];
        let sig = s.sign_sth(10, &root, 1234);
        let sth = SignedTreeHead {
            size: 10,
            root,
            ts_ms: 1234,
            signer_id: "other".into(),
            signature: sig,
        };
        assert!(!s.verify(&sth));
    }
}
