//! Proof of forgetting — tombstone records with unreachability proofs.

use crate::signer::Signer;
use sha2::{Digest, Sha256};

/// A GDPR forgetting receipt — proves that a key was tombstoned at a
/// specific epoch with a specific Merkle root and a cryptographic
/// signature binding the three together.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ForgetReceipt {
    pub tenant: String,
    pub key_hash: [u8; 32],
    pub tombstone_epoch: u64,
    pub merkle_root: [u8; 32],
    pub signature: Vec<u8>,
    pub signer_id: String,
    pub issued_at_ms: u64,
}

impl ForgetReceipt {
    /// Create a forgetting receipt by hashing the key and signing the
    /// `(key_hash, epoch, merkle_root)` tuple.
    pub fn create(
        tenant: &str,
        key: &[u8],
        epoch: u64,
        merkle_root: [u8; 32],
        signer: &dyn Signer,
        now_ms: u64,
    ) -> Self {
        let key_hash = Self::hash_key(key);
        let payload = Self::sign_payload(&key_hash, epoch, &merkle_root);
        let signature = signer.sign_sth(payload.len(), &payload, now_ms);

        Self {
            tenant: tenant.to_string(),
            key_hash,
            tombstone_epoch: epoch,
            merkle_root,
            signature,
            signer_id: signer.id().to_string(),
            issued_at_ms: now_ms,
        }
    }

    /// Verify a receipt offline by re-signing and comparing.
    pub fn verify(&self, signer: &dyn Signer) -> bool {
        if signer.id() != self.signer_id {
            return false;
        }
        let payload = Self::sign_payload(&self.key_hash, self.tombstone_epoch, &self.merkle_root);
        let expected = signer.sign_sth(payload.len(), &payload, self.issued_at_ms);
        self.signature == expected
    }

    fn hash_key(key: &[u8]) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(key);
        h.finalize().into()
    }

    fn sign_payload(key_hash: &[u8; 32], epoch: u64, merkle_root: &[u8; 32]) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(key_hash);
        h.update(epoch.to_be_bytes());
        h.update(merkle_root);
        h.finalize().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signer::MockSigner;

    fn test_signer() -> MockSigner {
        MockSigner::new("test-forget-signer")
    }

    #[test]
    fn create_and_verify_receipt() {
        let signer = test_signer();
        let receipt =
            ForgetReceipt::create("acme", b"user:42:pii", 10, [0xAA; 32], &signer, 1234567890);
        assert_eq!(receipt.tenant, "acme");
        assert_eq!(receipt.tombstone_epoch, 10);
        assert!(receipt.verify(&signer));
    }

    #[test]
    fn verify_succeeds_for_valid_receipt() {
        let signer = test_signer();
        let receipt = ForgetReceipt::create("t1", b"k", 1, [0xBB; 32], &signer, 999);
        assert!(receipt.verify(&signer), "valid receipt must verify");
    }

    #[test]
    fn tampered_receipt_rejected() {
        let signer = test_signer();
        let mut receipt = ForgetReceipt::create("t1", b"k", 1, [0xCC; 32], &signer, 999);
        // Tamper with the epoch
        receipt.tombstone_epoch = 999;
        assert!(!receipt.verify(&signer), "tampered receipt must not verify");
    }
}
