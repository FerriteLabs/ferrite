//! Module signing — COSE_Sign1-style envelope (HMAC-SHA256 placeholder for ML-DSA).

use sha2::{Digest, Sha256};

/// A signer identity for module verification.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SignerKey {
    pub key_id: String,
    /// In production this would be a public key PEM; for now it is a shared secret.
    pub secret: Vec<u8>,
}

/// A signed module envelope.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SignedEnvelope {
    pub signer_key_id: String,
    pub signature: Vec<u8>,
    pub payload: Vec<u8>,
}

impl SignedEnvelope {
    /// Sign module bytes with a secret key.
    pub fn sign(key: &SignerKey, payload: Vec<u8>) -> Self {
        let mut mac = Sha256::new();
        mac.update(&key.secret);
        mac.update(&payload);
        let signature = mac.finalize().to_vec();
        Self {
            signer_key_id: key.key_id.clone(),
            signature,
            payload,
        }
    }

    /// Verify the envelope against a set of trusted signer keys.
    pub fn verify(&self, trusted_keys: &[SignerKey]) -> Result<(), SigningError> {
        let key = trusted_keys
            .iter()
            .find(|k| k.key_id == self.signer_key_id)
            .ok_or(SigningError::UntrustedSigner(self.signer_key_id.clone()))?;
        let mut mac = Sha256::new();
        mac.update(&key.secret);
        mac.update(&self.payload);
        let expected = mac.finalize().to_vec();
        if self.signature == expected {
            Ok(())
        } else {
            Err(SigningError::InvalidSignature)
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SigningError {
    #[error("untrusted signer: {0}")]
    UntrustedSigner(String),
    #[error("invalid signature")]
    InvalidSignature,
    #[error("signing required but module is unsigned")]
    UnsignedModule,
}

/// Signing policy configuration.
#[derive(Debug, Clone, Default)]
pub struct SigningPolicy {
    /// Trusted signer keys.
    pub signers: Vec<SignerKey>,
    /// Whether signing is required for FN.LOAD.
    pub require_signing: bool,
}

impl SigningPolicy {
    /// Validate a module against the policy.
    pub fn validate(&self, envelope: Option<&SignedEnvelope>) -> Result<(), SigningError> {
        match (self.require_signing, envelope) {
            (true, None) => Err(SigningError::UnsignedModule),
            (true, Some(env)) => env.verify(&self.signers),
            (false, Some(env)) => env.verify(&self.signers),
            (false, None) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> SignerKey {
        SignerKey {
            key_id: "test-signer".into(),
            secret: b"super-secret-key".to_vec(),
        }
    }

    fn other_key() -> SignerKey {
        SignerKey {
            key_id: "other-signer".into(),
            secret: b"other-secret".to_vec(),
        }
    }

    #[test]
    fn sign_and_verify_roundtrip() {
        let key = test_key();
        let payload = b"hello wasm module bytes".to_vec();
        let env = SignedEnvelope::sign(&key, payload.clone());
        assert_eq!(env.signer_key_id, "test-signer");
        assert_eq!(env.payload, payload);
        assert!(!env.signature.is_empty());
        env.verify(&[key]).expect("verification should succeed");
    }

    #[test]
    fn invalid_signature_rejected() {
        let key = test_key();
        let payload = b"hello wasm module bytes".to_vec();
        let mut env = SignedEnvelope::sign(&key, payload);
        // Tamper with the signature.
        env.signature[0] = env.signature[0].wrapping_add(1);
        let err = env.verify(&[key]).unwrap_err();
        assert!(matches!(err, SigningError::InvalidSignature));
    }

    #[test]
    fn untrusted_signer_rejected() {
        let key = test_key();
        let payload = b"hello wasm module bytes".to_vec();
        let env = SignedEnvelope::sign(&key, payload);
        // Verify against a different set of trusted keys.
        let err = env.verify(&[other_key()]).unwrap_err();
        assert!(matches!(err, SigningError::UntrustedSigner(_)));
    }

    #[test]
    fn require_signing_rejects_unsigned() {
        let policy = SigningPolicy {
            signers: vec![test_key()],
            require_signing: true,
        };
        let err = policy.validate(None).unwrap_err();
        assert!(matches!(err, SigningError::UnsignedModule));
    }

    #[test]
    fn require_signing_accepts_valid_signed() {
        let key = test_key();
        let policy = SigningPolicy {
            signers: vec![key.clone()],
            require_signing: true,
        };
        let env = SignedEnvelope::sign(&key, b"payload".to_vec());
        policy
            .validate(Some(&env))
            .expect("should accept valid signed module");
    }

    #[test]
    fn no_require_signing_accepts_unsigned() {
        let policy = SigningPolicy {
            signers: vec![test_key()],
            require_signing: false,
        };
        policy
            .validate(None)
            .expect("should accept unsigned when not required");
    }

    #[test]
    fn no_require_signing_still_verifies_if_provided() {
        let key = test_key();
        let policy = SigningPolicy {
            signers: vec![key.clone()],
            require_signing: false,
        };
        let env = SignedEnvelope::sign(&key, b"payload".to_vec());
        policy
            .validate(Some(&env))
            .expect("should verify valid signature");

        let mut bad_env = SignedEnvelope::sign(&key, b"payload".to_vec());
        bad_env.signature[0] = bad_env.signature[0].wrapping_add(1);
        let err = policy.validate(Some(&bad_env)).unwrap_err();
        assert!(matches!(err, SigningError::InvalidSignature));
    }
}
