//! Post-Quantum signing placeholder (ML-DSA-65 API surface).
//!
//! Production: replace with `pqcrypto-mldsa` or `pqcrypto-dilithium`.
//! This implementation uses HMAC-SHA256 as a stand-in to validate the API
//! surface and key rotation logic without requiring the PQ crypto dependency.

use crate::merkle::Hash;
use crate::signer::{SignedTreeHead, Signer};
use sha2::{Digest, Sha256};

/// ML-DSA security level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MlDsaLevel {
    /// NIST Level 2
    MlDsa44,
    /// NIST Level 3 (default)
    MlDsa65,
    /// NIST Level 5
    MlDsa87,
}

/// Post-quantum signer using ML-DSA API (HMAC-SHA256 placeholder).
pub struct PqSigner {
    id: String,
    secret: [u8; 32],
    level: MlDsaLevel,
}

const BLOCK_SIZE: usize = 64;
const IPAD: u8 = 0x36;
const OPAD: u8 = 0x5c;

/// HMAC-SHA256(key, data) using the standard two-pass construction.
fn hmac_sha256(key: &[u8; 32], data: &[u8]) -> Vec<u8> {
    // Pad key to block size (already ≤64, so just zero-pad).
    let mut padded_key = [0u8; BLOCK_SIZE];
    padded_key[..32].copy_from_slice(key);

    // Inner hash: H((key ^ ipad) || data)
    let mut inner = Sha256::new();
    let mut ipad_key = [0u8; BLOCK_SIZE];
    for (i, b) in padded_key.iter().enumerate() {
        ipad_key[i] = b ^ IPAD;
    }
    inner.update(&ipad_key);
    inner.update(data);
    let inner_hash = inner.finalize();

    // Outer hash: H((key ^ opad) || inner_hash)
    let mut outer = Sha256::new();
    let mut opad_key = [0u8; BLOCK_SIZE];
    for (i, b) in padded_key.iter().enumerate() {
        opad_key[i] = b ^ OPAD;
    }
    outer.update(&opad_key);
    outer.update(&inner_hash);
    outer.finalize().to_vec()
}

impl PqSigner {
    pub fn new(id: &str, secret: [u8; 32], level: MlDsaLevel) -> Self {
        Self {
            id: id.to_string(),
            secret,
            level,
        }
    }

    pub fn level(&self) -> MlDsaLevel {
        self.level
    }

    fn payload(size: usize, root: &Hash, ts_ms: u64) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + 32 + 8);
        buf.extend_from_slice(&size.to_be_bytes());
        buf.extend_from_slice(root);
        buf.extend_from_slice(&ts_ms.to_be_bytes());
        buf
    }
}

impl Signer for PqSigner {
    fn id(&self) -> &str {
        &self.id
    }

    fn sign_sth(&self, size: usize, root: &Hash, ts_ms: u64) -> Vec<u8> {
        let data = Self::payload(size, root, ts_ms);
        hmac_sha256(&self.secret, &data)
    }

    fn verify(&self, sth: &SignedTreeHead) -> bool {
        if sth.signer_id != self.id {
            return false;
        }
        let data = Self::payload(sth.size, &sth.root, sth.ts_ms);
        hmac_sha256(&self.secret, &data) == sth.signature
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_and_verify_round_trip() {
        let signer = PqSigner::new("pq1", [42u8; 32], MlDsaLevel::MlDsa65);
        let root = [7u8; 32];
        let sig = signer.sign_sth(10, &root, 1234);
        let sth = SignedTreeHead {
            size: 10,
            root,
            ts_ms: 1234,
            signer_id: "pq1".into(),
            signature: sig,
        };
        assert!(signer.verify(&sth));
    }

    #[test]
    fn different_data_produces_different_signatures() {
        let signer = PqSigner::new("pq1", [42u8; 32], MlDsaLevel::MlDsa65);
        let root_a = [1u8; 32];
        let root_b = [2u8; 32];
        let sig_a = signer.sign_sth(1, &root_a, 100);
        let sig_b = signer.sign_sth(1, &root_b, 100);
        assert_ne!(sig_a, sig_b);
    }

    #[test]
    fn wrong_secret_fails_verification() {
        let signer_a = PqSigner::new("pq1", [1u8; 32], MlDsaLevel::MlDsa65);
        let signer_b = PqSigner::new("pq1", [2u8; 32], MlDsaLevel::MlDsa65);
        let root = [7u8; 32];
        let sig = signer_a.sign_sth(5, &root, 500);
        let sth = SignedTreeHead {
            size: 5,
            root,
            ts_ms: 500,
            signer_id: "pq1".into(),
            signature: sig,
        };
        assert!(!signer_b.verify(&sth));
    }

    #[test]
    fn all_levels_construct_without_error() {
        let levels = [
            MlDsaLevel::MlDsa44,
            MlDsaLevel::MlDsa65,
            MlDsaLevel::MlDsa87,
        ];
        for level in levels {
            let s = PqSigner::new("test", [0u8; 32], level);
            assert_eq!(s.level(), level);
            let sig = s.sign_sth(1, &[0u8; 32], 0);
            assert_eq!(sig.len(), 32); // HMAC-SHA256 output
        }
    }

    #[test]
    fn rejects_tampered_signature() {
        let signer = PqSigner::new("pq1", [42u8; 32], MlDsaLevel::MlDsa65);
        let root = [7u8; 32];
        let mut sig = signer.sign_sth(10, &root, 1234);
        sig[0] = sig[0].wrapping_add(1);
        let sth = SignedTreeHead {
            size: 10,
            root,
            ts_ms: 1234,
            signer_id: "pq1".into(),
            signature: sig,
        };
        assert!(!signer.verify(&sth));
    }

    #[test]
    fn rejects_wrong_signer_id() {
        let signer = PqSigner::new("pq1", [42u8; 32], MlDsaLevel::MlDsa65);
        let root = [7u8; 32];
        let sig = signer.sign_sth(10, &root, 1234);
        let sth = SignedTreeHead {
            size: 10,
            root,
            ts_ms: 1234,
            signer_id: "other".into(),
            signature: sig,
        };
        assert!(!signer.verify(&sth));
    }

    #[test]
    fn deterministic_from_same_seed() {
        let a = PqSigner::new("x", [99u8; 32], MlDsaLevel::MlDsa65);
        let b = PqSigner::new("x", [99u8; 32], MlDsaLevel::MlDsa65);
        let root = [0u8; 32];
        assert_eq!(a.sign_sth(1, &root, 1), b.sign_sth(1, &root, 1));
    }
}
