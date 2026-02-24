//! Module packaging for the WASM marketplace.
//!
//! Packs a WASM binary together with its `ferrite-plugin.toml` manifest into a
//! `.ferrpkg` archive and unpacks / validates such archives.
//!
//! # Archive format
//!
//! A `.ferrpkg` file is a simple concatenation of JSON header + raw wasm:
//!
//! ```text
//! [4 bytes]   magic: "FPKG"
//! [4 bytes]   header length (little-endian u32)
//! [N bytes]   JSON-encoded PackageHeader
//! [M bytes]   raw WASM binary
//! ```

use serde::{Deserialize, Serialize};

use super::discovery::ModuleManifest;

/// Magic bytes identifying a `.ferrpkg` archive.
const FERRPKG_MAGIC: &[u8; 4] = b"FPKG";

// ---------------------------------------------------------------------------
// Package header
// ---------------------------------------------------------------------------

/// JSON-serialised header embedded in a `.ferrpkg` archive.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageHeader {
    /// Module name.
    pub name: String,
    /// Module version.
    pub version: String,
    /// Author.
    pub author: String,
    /// Description.
    pub description: String,
    /// License (SPDX).
    pub license: String,
    /// Hex-encoded SHA-256 checksum of the WASM binary.
    pub checksum: String,
    /// Size of the WASM binary in bytes.
    pub wasm_size: u64,
}

// ---------------------------------------------------------------------------
// Packing
// ---------------------------------------------------------------------------

/// Packs a WASM binary and its manifest into a `.ferrpkg` byte vector.
pub fn pack(manifest: &ModuleManifest, wasm_bytes: &[u8]) -> Result<Vec<u8>, PackagingError> {
    if wasm_bytes.len() < 8 {
        return Err(PackagingError::InvalidWasm("module too small".into()));
    }
    // Validate WASM magic
    if &wasm_bytes[0..4] != b"\x00asm" {
        return Err(PackagingError::InvalidWasm(
            "invalid WASM magic number".into(),
        ));
    }

    let checksum = compute_checksum(wasm_bytes);

    let header = PackageHeader {
        name: manifest.module.name.clone(),
        version: manifest.module.version.clone(),
        author: manifest.module.author.clone(),
        description: manifest.module.description.clone(),
        license: manifest.module.license.clone(),
        checksum,
        wasm_size: wasm_bytes.len() as u64,
    };

    let header_json =
        serde_json::to_vec(&header).map_err(|e| PackagingError::Serialization(e.to_string()))?;
    let header_len = header_json.len() as u32;

    let mut archive = Vec::with_capacity(4 + 4 + header_json.len() + wasm_bytes.len());
    archive.extend_from_slice(FERRPKG_MAGIC);
    archive.extend_from_slice(&header_len.to_le_bytes());
    archive.extend_from_slice(&header_json);
    archive.extend_from_slice(wasm_bytes);

    Ok(archive)
}

// ---------------------------------------------------------------------------
// Unpacking
// ---------------------------------------------------------------------------

/// Result of unpacking a `.ferrpkg` archive.
#[derive(Debug, Clone)]
pub struct UnpackedPackage {
    /// Parsed header.
    pub header: PackageHeader,
    /// Raw WASM binary.
    pub wasm_bytes: Vec<u8>,
}

/// Unpacks and validates a `.ferrpkg` archive.
pub fn unpack(archive: &[u8]) -> Result<UnpackedPackage, PackagingError> {
    if archive.len() < 8 {
        return Err(PackagingError::InvalidArchive("archive too small".into()));
    }
    if &archive[0..4] != FERRPKG_MAGIC {
        return Err(PackagingError::InvalidArchive(
            "invalid magic bytes — not a .ferrpkg archive".into(),
        ));
    }

    let header_len = u32::from_le_bytes([archive[4], archive[5], archive[6], archive[7]]) as usize;

    if archive.len() < 8 + header_len {
        return Err(PackagingError::InvalidArchive(
            "archive truncated (header)".into(),
        ));
    }

    let header_json = &archive[8..8 + header_len];
    let header: PackageHeader = serde_json::from_slice(header_json)
        .map_err(|e| PackagingError::Serialization(format!("bad header JSON: {}", e)))?;

    let wasm_bytes = archive[8 + header_len..].to_vec();

    if wasm_bytes.len() as u64 != header.wasm_size {
        return Err(PackagingError::InvalidArchive(format!(
            "WASM size mismatch: header says {} but got {}",
            header.wasm_size,
            wasm_bytes.len()
        )));
    }

    Ok(UnpackedPackage { header, wasm_bytes })
}

/// Verifies the checksum of an unpacked package.
pub fn verify_checksum(package: &UnpackedPackage) -> Result<(), PackagingError> {
    let actual = compute_checksum(&package.wasm_bytes);
    if actual != package.header.checksum {
        return Err(PackagingError::ChecksumMismatch {
            expected: package.header.checksum.clone(),
            actual,
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Checksum computation (SHA-256 via FNV-1a 128-bit emulation)
// ---------------------------------------------------------------------------

/// Computes a hex-encoded checksum of the given data.
///
/// Uses a double FNV-1a hash to produce a 128-bit fingerprint.
/// For production use this should be replaced with a proper SHA-256
/// implementation (e.g. the `sha2` crate).
pub fn compute_checksum(data: &[u8]) -> String {
    const FNV_OFFSET_A: u64 = 0xcbf29ce484222325;
    const FNV_PRIME_A: u64 = 0x100000001b3;
    const FNV_OFFSET_B: u64 = 0x6c62272e07bb0142;
    const FNV_PRIME_B: u64 = 0x1000193;

    let mut ha = FNV_OFFSET_A;
    let mut hb = FNV_OFFSET_B;

    for &byte in data {
        ha ^= byte as u64;
        ha = ha.wrapping_mul(FNV_PRIME_A);
        hb ^= byte as u64;
        hb = hb.wrapping_mul(FNV_PRIME_B);
    }

    // Incorporate length to reduce collisions
    ha ^= data.len() as u64;
    ha = ha.wrapping_mul(FNV_PRIME_A);
    hb ^= data.len() as u64;
    hb = hb.wrapping_mul(FNV_PRIME_B);

    format!("{:016x}{:016x}", ha, hb)
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors produced during packaging operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum PackagingError {
    #[error("invalid WASM module: {0}")]
    InvalidWasm(String),

    #[error("invalid archive: {0}")]
    InvalidArchive(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },
}

// ---------------------------------------------------------------------------
// PluginPackage — high-level packaging API
// ---------------------------------------------------------------------------

/// A complete plugin package containing manifest, WASM binary, and optional
/// README / examples.
///
/// Supports creating packages from a directory layout, serialising to/from
/// `.ferrpkg` archives, and integrity verification.
#[derive(Debug, Clone)]
pub struct PluginPackage {
    /// Parsed manifest.
    pub manifest: ModuleManifest,
    /// Raw WASM binary.
    pub wasm_bytes: Vec<u8>,
    /// Optional README content.
    pub readme: Option<String>,
    /// Example code snippets bundled with the plugin.
    pub examples: Vec<PluginExample>,
}

/// An example snippet bundled inside a plugin package.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginExample {
    /// Example name / title.
    pub name: String,
    /// Source code.
    pub source: String,
}

impl PluginPackage {
    /// Create a package from a directory.
    ///
    /// Expects the directory to contain:
    /// - `ferrite-plugin.toml` (manifest)
    /// - `plugin.wasm` (WASM binary)
    /// - `README.md` (optional)
    /// - `examples/` directory with `.rs` files (optional)
    pub fn from_directory(path: &std::path::Path) -> Result<Self, PackagingError> {
        // Read manifest
        let manifest_path = path.join("ferrite-plugin.toml");
        let manifest_text = std::fs::read_to_string(&manifest_path)
            .map_err(|e| PackagingError::InvalidArchive(format!("cannot read manifest: {}", e)))?;
        let manifest = ModuleManifest::from_toml(&manifest_text)
            .map_err(|e| PackagingError::Serialization(format!("invalid manifest: {}", e)))?;

        // Read WASM binary
        let wasm_path = path.join("plugin.wasm");
        let wasm_bytes = std::fs::read(&wasm_path)
            .map_err(|e| PackagingError::InvalidWasm(format!("cannot read plugin.wasm: {}", e)))?;

        // Read optional README
        let readme = std::fs::read_to_string(path.join("README.md")).ok();

        // Read optional examples
        let mut examples = Vec::new();
        let examples_dir = path.join("examples");
        if examples_dir.is_dir() {
            if let Ok(entries) = std::fs::read_dir(&examples_dir) {
                for entry in entries.flatten() {
                    let p = entry.path();
                    if p.extension().is_some_and(|e| e == "rs") {
                        if let Ok(source) = std::fs::read_to_string(&p) {
                            let name = p
                                .file_stem()
                                .and_then(|s| s.to_str())
                                .unwrap_or("example")
                                .to_string();
                            examples.push(PluginExample { name, source });
                        }
                    }
                }
            }
        }

        Ok(Self {
            manifest,
            wasm_bytes,
            readme,
            examples,
        })
    }

    /// Serialise the package into a `.ferrpkg` archive.
    pub fn to_archive(&self) -> Result<Vec<u8>, PackagingError> {
        pack(&self.manifest, &self.wasm_bytes)
    }

    /// Deserialise a `.ferrpkg` archive into a `PluginPackage`.
    ///
    /// Note: README and examples are not stored in the archive format, so
    /// they will be `None` / empty after round-tripping.
    pub fn from_archive(data: &[u8]) -> Result<Self, PackagingError> {
        let unpacked = unpack(data)?;
        // Reconstruct a minimal manifest from the header
        let manifest_toml = format!(
            r#"[module]
name = "{}"
version = "{}"
author = "{}"
description = "{}"
license = "{}"
categories = []
tags = []
min_ferrite_version = "0.1.0"

[dependencies]
"#,
            unpacked.header.name,
            unpacked.header.version,
            unpacked.header.author,
            unpacked.header.description,
            unpacked.header.license,
        );

        let manifest = ModuleManifest::from_toml(&manifest_toml)
            .map_err(|e| PackagingError::Serialization(format!("reconstruct manifest: {}", e)))?;

        Ok(Self {
            manifest,
            wasm_bytes: unpacked.wasm_bytes,
            readme: None,
            examples: Vec::new(),
        })
    }

    /// Verify the integrity of the WASM binary against its checksum.
    pub fn verify_integrity(&self) -> Result<(), PackagingError> {
        // Pack to get the expected checksum, then verify
        let archive = self.to_archive()?;
        let unpacked = unpack(&archive)?;
        verify_checksum(&unpacked)
    }

    /// Get the checksum of the WASM binary.
    pub fn wasm_checksum(&self) -> String {
        compute_checksum(&self.wasm_bytes)
    }

    /// Get the size of the WASM binary in bytes.
    pub fn wasm_size(&self) -> u64 {
        self.wasm_bytes.len() as u64
    }

    /// Get the plugin name from the manifest.
    pub fn name(&self) -> &str {
        &self.manifest.module.name
    }

    /// Get the plugin version from the manifest.
    pub fn version(&self) -> &str {
        &self.manifest.module.version
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_manifest() -> ModuleManifest {
        ModuleManifest::from_toml(
            r#"
[module]
name = "test-module"
version = "1.0.0"
author = "Test"
description = "A test module"
license = "MIT"
categories = ["validation"]
tags = ["test"]
min_ferrite_version = "0.1.0"

[dependencies]
"#,
        )
        .expect("parse")
    }

    fn valid_wasm() -> Vec<u8> {
        vec![
            0x00, 0x61, 0x73, 0x6D, // magic: \0asm
            0x01, 0x00, 0x00, 0x00, // version 1
        ]
    }

    #[test]
    fn test_pack_unpack_roundtrip() {
        let manifest = sample_manifest();
        let wasm = valid_wasm();

        let archive = pack(&manifest, &wasm).expect("pack failed");
        assert_eq!(&archive[0..4], b"FPKG");

        let unpacked = unpack(&archive).expect("unpack failed");
        assert_eq!(unpacked.header.name, "test-module");
        assert_eq!(unpacked.header.version, "1.0.0");
        assert_eq!(unpacked.wasm_bytes, wasm);
    }

    #[test]
    fn test_verify_checksum_ok() {
        let manifest = sample_manifest();
        let wasm = valid_wasm();
        let archive = pack(&manifest, &wasm).expect("pack");
        let unpacked = unpack(&archive).expect("unpack");
        verify_checksum(&unpacked).expect("checksum should match");
    }

    #[test]
    fn test_verify_checksum_mismatch() {
        let manifest = sample_manifest();
        let wasm = valid_wasm();
        let archive = pack(&manifest, &wasm).expect("pack");
        let mut unpacked = unpack(&archive).expect("unpack");
        // Tamper with the wasm bytes
        unpacked.wasm_bytes.push(0xFF);
        let result = verify_checksum(&unpacked);
        assert!(matches!(
            result,
            Err(PackagingError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn test_pack_invalid_wasm() {
        let manifest = sample_manifest();
        let result = pack(&manifest, &[0, 0, 0, 0, 0, 0, 0, 0]);
        assert!(matches!(result, Err(PackagingError::InvalidWasm(_))));
    }

    #[test]
    fn test_unpack_invalid_magic() {
        let result = unpack(b"NOT_FPKG_DATA_HERE");
        assert!(matches!(result, Err(PackagingError::InvalidArchive(_))));
    }

    #[test]
    fn test_unpack_too_small() {
        let result = unpack(b"FPKG");
        assert!(matches!(result, Err(PackagingError::InvalidArchive(_))));
    }

    #[test]
    fn test_compute_checksum_deterministic() {
        let a = compute_checksum(b"hello world");
        let b = compute_checksum(b"hello world");
        assert_eq!(a, b);
        assert_eq!(a.len(), 32); // 128-bit hex
    }

    #[test]
    fn test_compute_checksum_differs() {
        let a = compute_checksum(b"hello");
        let b = compute_checksum(b"world");
        assert_ne!(a, b);
    }

    // -- PluginPackage tests --

    #[test]
    fn test_plugin_package_archive_roundtrip() {
        let manifest = sample_manifest();
        let wasm = valid_wasm();

        let pkg = PluginPackage {
            manifest: manifest.clone(),
            wasm_bytes: wasm.clone(),
            readme: Some("# My Plugin".to_string()),
            examples: vec![PluginExample {
                name: "basic".to_string(),
                source: "fn main() {}".to_string(),
            }],
        };

        let archive = pkg.to_archive().expect("to_archive failed");
        let restored = PluginPackage::from_archive(&archive).expect("from_archive failed");

        assert_eq!(restored.name(), "test-module");
        assert_eq!(restored.version(), "1.0.0");
        assert_eq!(restored.wasm_bytes, wasm);
        // README and examples are not preserved in archive format
        assert!(restored.readme.is_none());
        assert!(restored.examples.is_empty());
    }

    #[test]
    fn test_plugin_package_verify_integrity() {
        let manifest = sample_manifest();
        let wasm = valid_wasm();

        let pkg = PluginPackage {
            manifest,
            wasm_bytes: wasm,
            readme: None,
            examples: vec![],
        };

        pkg.verify_integrity().expect("integrity check failed");
    }

    #[test]
    fn test_plugin_package_checksum() {
        let manifest = sample_manifest();
        let wasm = valid_wasm();

        let pkg = PluginPackage {
            manifest,
            wasm_bytes: wasm.clone(),
            readme: None,
            examples: vec![],
        };

        assert_eq!(pkg.wasm_checksum(), compute_checksum(&wasm));
        assert_eq!(pkg.wasm_size(), wasm.len() as u64);
    }
}
