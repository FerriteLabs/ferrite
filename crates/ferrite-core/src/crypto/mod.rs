//! Encryption at rest module
//!
//! This module provides encryption utilities for securing data at rest,
//! including AOF entries, RDB snapshots, and checkpoints.

mod key;

pub use key::{EncryptionKey, KeyManager};

use chacha20poly1305::{
    aead::{Aead, KeyInit, OsRng},
    ChaCha20Poly1305, Nonce,
};
use rand::RngCore;
use std::sync::Arc;

use crate::config::EncryptionAlgorithm;
use crate::error::{FerriteError, Result};

/// Size of the encryption nonce in bytes
pub const NONCE_SIZE: usize = 12;

/// Size of the authentication tag in bytes
pub const TAG_SIZE: usize = 16;

/// Magic bytes to identify encrypted data
pub const ENCRYPTED_MAGIC: &[u8; 8] = b"FERR_ENC";

/// Shared encryption context
pub type SharedEncryption = Arc<Encryption>;

/// Encryption context for data at rest
pub struct Encryption {
    /// The encryption key
    key: EncryptionKey,
    /// Algorithm to use
    algorithm: EncryptionAlgorithm,
}

impl Encryption {
    /// Create a new encryption context
    pub fn new(key: EncryptionKey, algorithm: EncryptionAlgorithm) -> Self {
        Self { key, algorithm }
    }

    /// Encrypt data
    ///
    /// Returns encrypted data with format:
    /// - 8 bytes: magic header "FERR_ENC"
    /// - 1 byte: algorithm identifier (0 = ChaCha20, 1 = AES)
    /// - 12 bytes: nonce
    /// - N bytes: ciphertext with authentication tag
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        // Generate random nonce
        let mut nonce_bytes = [0u8; NONCE_SIZE];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        // Encrypt based on algorithm
        let ciphertext = match self.algorithm {
            EncryptionAlgorithm::ChaCha20Poly1305 => {
                let cipher = ChaCha20Poly1305::new(self.key.as_bytes().into());
                cipher.encrypt(nonce, plaintext).map_err(|e| {
                    FerriteError::Encryption(format!("ChaCha20 encryption failed: {}", e))
                })?
            }
            EncryptionAlgorithm::Aes256Gcm => {
                // For now, fall back to ChaCha20 - AES-GCM would require additional dependency
                // In production, you'd add aes-gcm crate and implement properly
                let cipher = ChaCha20Poly1305::new(self.key.as_bytes().into());
                cipher
                    .encrypt(nonce, plaintext)
                    .map_err(|e| FerriteError::Encryption(format!("Encryption failed: {}", e)))?
            }
        };

        // Build output: magic + algorithm + nonce + ciphertext
        let mut output = Vec::with_capacity(8 + 1 + NONCE_SIZE + ciphertext.len());
        output.extend_from_slice(ENCRYPTED_MAGIC);
        output.push(self.algorithm as u8);
        output.extend_from_slice(&nonce_bytes);
        output.extend_from_slice(&ciphertext);

        Ok(output)
    }

    /// Decrypt data
    ///
    /// Expects encrypted data with the format produced by encrypt()
    pub fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        // Check minimum size
        let header_size = 8 + 1 + NONCE_SIZE;
        if ciphertext.len() < header_size + TAG_SIZE {
            return Err(FerriteError::Encryption(
                "Encrypted data too short".to_string(),
            ));
        }

        // Verify magic header
        if &ciphertext[..8] != ENCRYPTED_MAGIC {
            return Err(FerriteError::Encryption(
                "Invalid encrypted data header".to_string(),
            ));
        }

        // Read algorithm (we ignore it and use configured algorithm)
        let _stored_algorithm = ciphertext[8];

        // Extract nonce
        let nonce = Nonce::from_slice(&ciphertext[9..9 + NONCE_SIZE]);

        // Extract ciphertext
        let encrypted_data = &ciphertext[header_size..];

        // Decrypt based on algorithm
        let plaintext = match self.algorithm {
            EncryptionAlgorithm::ChaCha20Poly1305 => {
                let cipher = ChaCha20Poly1305::new(self.key.as_bytes().into());
                cipher.decrypt(nonce, encrypted_data).map_err(|e| {
                    FerriteError::Encryption(format!(
                        "ChaCha20 decryption failed (wrong key or corrupted data): {}",
                        e
                    ))
                })?
            }
            EncryptionAlgorithm::Aes256Gcm => {
                let cipher = ChaCha20Poly1305::new(self.key.as_bytes().into());
                cipher.decrypt(nonce, encrypted_data).map_err(|e| {
                    FerriteError::Encryption(format!(
                        "Decryption failed (wrong key or corrupted data): {}",
                        e
                    ))
                })?
            }
        };

        Ok(plaintext)
    }

    /// Check if data is encrypted (has magic header)
    pub fn is_encrypted(data: &[u8]) -> bool {
        data.len() >= 8 && &data[..8] == ENCRYPTED_MAGIC
    }

    /// Get the algorithm being used
    pub fn algorithm(&self) -> EncryptionAlgorithm {
        self.algorithm
    }
}

/// No-op encryption for when encryption is disabled
pub struct NoopEncryption;

impl NoopEncryption {
    /// "Encrypt" data (just returns the data as-is)
    pub fn encrypt(data: &[u8]) -> Vec<u8> {
        data.to_vec()
    }

    /// "Decrypt" data (just returns the data as-is)
    pub fn decrypt(data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> EncryptionKey {
        let key_bytes = [0x42u8; 32];
        EncryptionKey::from_bytes(key_bytes)
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let encryption = Encryption::new(test_key(), EncryptionAlgorithm::ChaCha20Poly1305);

        let plaintext = b"Hello, World! This is a test message.";
        let ciphertext = encryption.encrypt(plaintext).unwrap();

        // Verify it's not plaintext
        assert_ne!(&ciphertext[..], plaintext);

        // Verify it starts with magic header
        assert!(Encryption::is_encrypted(&ciphertext));

        // Decrypt and verify
        let decrypted = encryption.decrypt(&ciphertext).unwrap();
        assert_eq!(&decrypted[..], plaintext);
    }

    #[test]
    fn test_encrypt_empty_data() {
        let encryption = Encryption::new(test_key(), EncryptionAlgorithm::ChaCha20Poly1305);

        let plaintext = b"";
        let ciphertext = encryption.encrypt(plaintext).unwrap();
        let decrypted = encryption.decrypt(&ciphertext).unwrap();
        assert_eq!(&decrypted[..], plaintext);
    }

    #[test]
    fn test_encrypt_large_data() {
        let encryption = Encryption::new(test_key(), EncryptionAlgorithm::ChaCha20Poly1305);

        // 1MB of data
        let plaintext: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();
        let ciphertext = encryption.encrypt(&plaintext).unwrap();
        let decrypted = encryption.decrypt(&ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_different_keys_fail() {
        let encryption1 = Encryption::new(test_key(), EncryptionAlgorithm::ChaCha20Poly1305);
        let encryption2 = Encryption::new(
            EncryptionKey::from_bytes([0x11u8; 32]),
            EncryptionAlgorithm::ChaCha20Poly1305,
        );

        let plaintext = b"Secret data";
        let ciphertext = encryption1.encrypt(plaintext).unwrap();

        // Decrypting with wrong key should fail
        let result = encryption2.decrypt(&ciphertext);
        assert!(result.is_err());
    }

    #[test]
    fn test_corrupted_data_fails() {
        let encryption = Encryption::new(test_key(), EncryptionAlgorithm::ChaCha20Poly1305);

        let plaintext = b"Secret data";
        let mut ciphertext = encryption.encrypt(plaintext).unwrap();

        // Corrupt some data
        if let Some(byte) = ciphertext.last_mut() {
            *byte ^= 0xFF;
        }

        // Decryption should fail
        let result = encryption.decrypt(&ciphertext);
        assert!(result.is_err());
    }

    #[test]
    fn test_is_encrypted() {
        let encryption = Encryption::new(test_key(), EncryptionAlgorithm::ChaCha20Poly1305);

        let plaintext = b"Not encrypted";
        assert!(!Encryption::is_encrypted(plaintext));

        let ciphertext = encryption.encrypt(plaintext).unwrap();
        assert!(Encryption::is_encrypted(&ciphertext));
    }

    #[test]
    fn test_noop_encryption() {
        let data = b"Some data";
        let encrypted = NoopEncryption::encrypt(data);
        assert_eq!(&encrypted[..], data);

        let decrypted = NoopEncryption::decrypt(data).unwrap();
        assert_eq!(&decrypted[..], data);
    }

    #[test]
    fn test_decrypt_too_short() {
        let encryption = Encryption::new(test_key(), EncryptionAlgorithm::ChaCha20Poly1305);

        // Too short to be valid encrypted data
        let result = encryption.decrypt(b"short");
        assert!(result.is_err());
    }

    #[test]
    fn test_decrypt_invalid_magic() {
        let encryption = Encryption::new(test_key(), EncryptionAlgorithm::ChaCha20Poly1305);

        // Valid length but wrong magic
        let fake_encrypted = vec![0u8; 100];
        let result = encryption.decrypt(&fake_encrypted);
        assert!(result.is_err());
    }
}
