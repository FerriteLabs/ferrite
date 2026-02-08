//! Encryption key management
//!
//! This module handles loading and managing encryption keys.

use std::path::Path;

use chacha20poly1305::aead::OsRng;
use rand::RngCore;
use tracing::{debug, info};

use crate::config::EncryptionConfig;
use crate::error::{FerriteError, Result};

/// Size of the encryption key in bytes (256 bits)
pub const KEY_SIZE: usize = 32;

/// An encryption key
#[derive(Clone)]
pub struct EncryptionKey {
    bytes: [u8; KEY_SIZE],
}

impl EncryptionKey {
    /// Create a new key from raw bytes
    pub fn from_bytes(bytes: [u8; KEY_SIZE]) -> Self {
        Self { bytes }
    }

    /// Generate a new random key
    pub fn generate() -> Self {
        let mut bytes = [0u8; KEY_SIZE];
        OsRng.fill_bytes(&mut bytes);
        Self { bytes }
    }

    /// Get the key bytes
    pub fn as_bytes(&self) -> &[u8; KEY_SIZE] {
        &self.bytes
    }

    /// Load key from a file
    ///
    /// Supports two formats:
    /// - Raw binary: exactly 32 bytes
    /// - Base64: 44 characters (32 bytes encoded)
    pub fn from_file(path: &Path) -> Result<Self> {
        let contents = std::fs::read(path).map_err(|e| {
            FerriteError::Encryption(format!("Failed to read key file {:?}: {}", path, e))
        })?;

        Self::from_data(&contents)
    }

    /// Load key from raw data (file contents)
    pub fn from_data(data: &[u8]) -> Result<Self> {
        // Try raw binary first
        if data.len() == KEY_SIZE {
            let mut bytes = [0u8; KEY_SIZE];
            bytes.copy_from_slice(data);
            return Ok(Self { bytes });
        }

        // Try base64 (strip whitespace first)
        let trimmed: Vec<u8> = data
            .iter()
            .copied()
            .filter(|b| !b.is_ascii_whitespace())
            .collect();

        if let Ok(decoded) = base64_decode(&trimmed) {
            if decoded.len() == KEY_SIZE {
                let mut bytes = [0u8; KEY_SIZE];
                bytes.copy_from_slice(&decoded);
                return Ok(Self { bytes });
            }
        }

        // Try hex encoding
        if let Ok(decoded) = hex_decode(&trimmed) {
            if decoded.len() == KEY_SIZE {
                let mut bytes = [0u8; KEY_SIZE];
                bytes.copy_from_slice(&decoded);
                return Ok(Self { bytes });
            }
        }

        Err(FerriteError::Encryption(format!(
            "Invalid key format: expected {} bytes raw, 44 bytes base64, or 64 bytes hex",
            KEY_SIZE
        )))
    }

    /// Export key as base64 string
    pub fn to_base64(&self) -> String {
        base64_encode(&self.bytes)
    }

    /// Export key as hex string
    pub fn to_hex(&self) -> String {
        hex_encode(&self.bytes)
    }

    /// Save key to a file in base64 format
    pub fn to_file(&self, path: &Path) -> Result<()> {
        let base64 = self.to_base64();
        std::fs::write(path, base64.as_bytes()).map_err(|e| {
            FerriteError::Encryption(format!("Failed to write key file {:?}: {}", path, e))
        })?;
        Ok(())
    }
}

// Secure zeroing on drop
impl Drop for EncryptionKey {
    fn drop(&mut self) {
        // Zero out the key bytes
        self.bytes.iter_mut().for_each(|b| *b = 0);
    }
}

impl std::fmt::Debug for EncryptionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Don't reveal key contents in debug output
        write!(f, "EncryptionKey([REDACTED])")
    }
}

/// Key manager for handling encryption keys
pub struct KeyManager;

impl KeyManager {
    /// Load or generate an encryption key based on config
    pub fn load_key(config: &EncryptionConfig) -> Result<Option<EncryptionKey>> {
        if !config.enabled {
            return Ok(None);
        }

        let key_file = config.key_file.as_ref().ok_or_else(|| {
            FerriteError::Encryption("Encryption enabled but no key file specified".to_string())
        })?;

        if key_file.exists() {
            info!("Loading encryption key from {:?}", key_file);
            let key = EncryptionKey::from_file(key_file)?;
            debug!("Encryption key loaded successfully");
            Ok(Some(key))
        } else {
            Err(FerriteError::Encryption(format!(
                "Key file {:?} does not exist. Generate one with: ferrite --generate-key > {:?}",
                key_file, key_file
            )))
        }
    }

    /// Generate a new key and save it to a file
    pub fn generate_key_file(path: &Path) -> Result<()> {
        let key = EncryptionKey::generate();
        key.to_file(path)?;
        info!("Generated new encryption key at {:?}", path);
        Ok(())
    }

    /// Generate a new key and return it as base64
    pub fn generate_key_base64() -> String {
        let key = EncryptionKey::generate();
        key.to_base64()
    }
}

/// Simple base64 encoding (standard alphabet, no padding)
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut result = String::with_capacity(data.len().div_ceil(3) * 4);

    for chunk in data.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;

        result.push(ALPHABET[b0 >> 2] as char);
        result.push(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)] as char);

        if chunk.len() > 1 {
            result.push(ALPHABET[((b1 & 0x0f) << 2) | (b2 >> 6)] as char);
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            result.push(ALPHABET[b2 & 0x3f] as char);
        } else {
            result.push('=');
        }
    }

    result
}

/// Simple base64 decoding
fn base64_decode(data: &[u8]) -> Result<Vec<u8>> {
    const DECODE: [i8; 128] = [
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1,
        -1, 63, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2, 3, 4,
        5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1,
        -1, -1, -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45,
        46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1,
    ];

    let mut result = Vec::with_capacity(data.len() * 3 / 4);
    let mut buf = 0u32;
    let mut buf_len = 0;

    for &byte in data {
        if byte == b'=' {
            break;
        }
        if byte >= 128 {
            return Err(FerriteError::Encryption(
                "Invalid base64 character".to_string(),
            ));
        }
        let val = DECODE[byte as usize];
        if val < 0 {
            return Err(FerriteError::Encryption(
                "Invalid base64 character".to_string(),
            ));
        }

        buf = (buf << 6) | (val as u32);
        buf_len += 6;

        if buf_len >= 8 {
            buf_len -= 8;
            result.push((buf >> buf_len) as u8);
            buf &= (1 << buf_len) - 1;
        }
    }

    Ok(result)
}

/// Simple hex encoding
fn hex_encode(data: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(data.len() * 2);
    for &byte in data {
        result.push(HEX[(byte >> 4) as usize] as char);
        result.push(HEX[(byte & 0x0f) as usize] as char);
    }
    result
}

/// Simple hex decoding
fn hex_decode(data: &[u8]) -> Result<Vec<u8>> {
    if data.len() % 2 != 0 {
        return Err(FerriteError::Encryption("Invalid hex length".to_string()));
    }

    let mut result = Vec::with_capacity(data.len() / 2);
    for chunk in data.chunks(2) {
        let hi = hex_char_to_val(chunk[0])?;
        let lo = hex_char_to_val(chunk[1])?;
        result.push((hi << 4) | lo);
    }
    Ok(result)
}

fn hex_char_to_val(c: u8) -> Result<u8> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(FerriteError::Encryption(
            "Invalid hex character".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_key_generate() {
        let key1 = EncryptionKey::generate();
        let key2 = EncryptionKey::generate();

        // Keys should be different
        assert_ne!(key1.as_bytes(), key2.as_bytes());

        // Keys should be the right size
        assert_eq!(key1.as_bytes().len(), KEY_SIZE);
    }

    #[test]
    fn test_key_from_bytes() {
        let bytes = [0x42u8; KEY_SIZE];
        let key = EncryptionKey::from_bytes(bytes);
        assert_eq!(key.as_bytes(), &bytes);
    }

    #[test]
    fn test_key_file_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.key");

        let original = EncryptionKey::generate();
        original.to_file(&path).unwrap();

        let loaded = EncryptionKey::from_file(&path).unwrap();
        assert_eq!(original.as_bytes(), loaded.as_bytes());
    }

    #[test]
    fn test_key_from_raw_bytes() {
        let raw = [0xAAu8; KEY_SIZE];
        let key = EncryptionKey::from_data(&raw).unwrap();
        assert_eq!(key.as_bytes(), &raw);
    }

    #[test]
    fn test_key_from_base64() {
        // 32 bytes encoded as base64
        let key = EncryptionKey::generate();
        let base64 = key.to_base64();
        let loaded = EncryptionKey::from_data(base64.as_bytes()).unwrap();
        assert_eq!(key.as_bytes(), loaded.as_bytes());
    }

    #[test]
    fn test_key_from_hex() {
        let key = EncryptionKey::generate();
        let hex = key.to_hex();
        let loaded = EncryptionKey::from_data(hex.as_bytes()).unwrap();
        assert_eq!(key.as_bytes(), loaded.as_bytes());
    }

    #[test]
    fn test_key_invalid_length() {
        let short = [0u8; 16];
        let result = EncryptionKey::from_data(&short);
        assert!(result.is_err());
    }

    #[test]
    fn test_base64_roundtrip() {
        let data = b"Hello, World!";
        let encoded = base64_encode(data);
        let decoded = base64_decode(encoded.as_bytes()).unwrap();
        assert_eq!(&decoded[..], data);
    }

    #[test]
    fn test_hex_roundtrip() {
        let data = b"Hello, World!";
        let encoded = hex_encode(data);
        let decoded = hex_decode(encoded.as_bytes()).unwrap();
        assert_eq!(&decoded[..], data);
    }

    #[test]
    fn test_key_debug_redacted() {
        let key = EncryptionKey::generate();
        let debug_str = format!("{:?}", key);
        assert!(debug_str.contains("REDACTED"));
        assert!(!debug_str.contains("42")); // Shouldn't contain key bytes
    }

    #[test]
    fn test_generate_key_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("generated.key");

        KeyManager::generate_key_file(&path).unwrap();
        assert!(path.exists());

        // Should be loadable
        let key = EncryptionKey::from_file(&path).unwrap();
        assert_eq!(key.as_bytes().len(), KEY_SIZE);
    }

    #[test]
    fn test_generate_key_base64() {
        let base64 = KeyManager::generate_key_base64();
        // Base64 encoded 32 bytes = 44 characters (with padding)
        assert_eq!(base64.len(), 44);

        // Should be decodable
        let key = EncryptionKey::from_data(base64.as_bytes()).unwrap();
        assert_eq!(key.as_bytes().len(), KEY_SIZE);
    }
}
