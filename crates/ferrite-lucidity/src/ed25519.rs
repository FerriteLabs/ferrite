//! Real Ed25519 signer using `ed25519-dalek` v2.
//!
//! Holds a `SigningKey` in process memory.  Production deployments wrap
//! this with an HSM/KMS adapter behind the same [`Signer`] trait.

use crate::merkle::Hash;
use crate::signer::{SignedTreeHead, Signer};
use ed25519_dalek::{Signature, SigningKey, Verifier, VerifyingKey, SECRET_KEY_LENGTH};

pub struct Ed25519Signer {
    id: String,
    key: SigningKey,
}

impl Ed25519Signer {
    /// Construct from an existing 32-byte secret seed.
    pub fn from_secret(id: impl Into<String>, secret: [u8; SECRET_KEY_LENGTH]) -> Self {
        Self {
            id: id.into(),
            key: SigningKey::from_bytes(&secret),
        }
    }

    /// Generate a fresh keypair using the OS RNG.
    pub fn generate(id: impl Into<String>) -> Self {
        use rand::RngCore;
        let mut seed = [0u8; SECRET_KEY_LENGTH];
        rand::thread_rng().fill_bytes(&mut seed);
        Self::from_secret(id, seed)
    }

    pub fn verifying_key(&self) -> VerifyingKey {
        self.key.verifying_key()
    }

    fn payload(size: usize, root: &Hash, ts_ms: u64) -> [u8; 8 + 32 + 8] {
        let mut buf = [0u8; 8 + 32 + 8];
        buf[..8].copy_from_slice(&size.to_be_bytes());
        buf[8..40].copy_from_slice(root);
        buf[40..].copy_from_slice(&ts_ms.to_be_bytes());
        buf
    }
}

impl Signer for Ed25519Signer {
    fn id(&self) -> &str {
        &self.id
    }

    fn sign_sth(&self, size: usize, root: &Hash, ts_ms: u64) -> Vec<u8> {
        use ed25519_dalek::Signer as _;
        let payload = Self::payload(size, root, ts_ms);
        self.key.sign(&payload).to_bytes().to_vec()
    }

    fn verify(&self, sth: &SignedTreeHead) -> bool {
        if sth.signer_id != self.id {
            return false;
        }
        let Ok(sig_bytes) = <[u8; 64]>::try_from(sth.signature.as_slice()) else {
            return false;
        };
        let sig = Signature::from_bytes(&sig_bytes);
        let payload = Self::payload(sth.size, &sth.root, sth.ts_ms);
        self.key.verifying_key().verify(&payload, &sig).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ed25519_round_trip() {
        let s = Ed25519Signer::generate("prod");
        let root = [7u8; 32];
        let sig = s.sign_sth(42, &root, 1234);
        let sth = SignedTreeHead {
            size: 42,
            root,
            ts_ms: 1234,
            signer_id: "prod".into(),
            signature: sig,
        };
        assert!(s.verify(&sth));
    }

    #[test]
    fn ed25519_rejects_tampered_payload() {
        let s = Ed25519Signer::generate("prod");
        let root = [7u8; 32];
        let sig = s.sign_sth(42, &root, 1234);
        let sth = SignedTreeHead {
            size: 42,
            root: [9u8; 32], // tampered
            ts_ms: 1234,
            signer_id: "prod".into(),
            signature: sig,
        };
        assert!(!s.verify(&sth));
    }

    #[test]
    fn ed25519_rejects_signature_from_other_key() {
        let a = Ed25519Signer::generate("a");
        let b = Ed25519Signer::generate("a"); // same id, different key
        let root = [7u8; 32];
        let sig = a.sign_sth(1, &root, 1);
        let sth = SignedTreeHead {
            size: 1,
            root,
            ts_ms: 1,
            signer_id: "a".into(),
            signature: sig,
        };
        assert!(!b.verify(&sth));
    }

    #[test]
    fn deterministic_from_seed() {
        let seed = [1u8; 32];
        let a = Ed25519Signer::from_secret("x", seed);
        let b = Ed25519Signer::from_secret("x", seed);
        let root = [0u8; 32];
        assert_eq!(a.sign_sth(1, &root, 1), b.sign_sth(1, &root, 1));
    }
}
