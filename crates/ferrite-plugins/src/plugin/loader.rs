//! Plugin Loader
//!
//! Handles loading and validating plugins.

use super::manifest::{PluginManifest, Version};
use super::registry::LoadedPlugin;
use super::sandbox::SandboxConfig;
use super::PluginError;

/// Plugin loader
pub struct PluginLoader {
    /// Sandbox configuration
    sandbox_config: SandboxConfig,
}

impl PluginLoader {
    /// Create a new loader
    pub fn new(sandbox_config: SandboxConfig) -> Self {
        Self { sandbox_config }
    }

    /// Load a plugin from bytes
    pub fn load(
        &self,
        name: &str,
        bytes: Vec<u8>,
        manifest: Option<PluginManifest>,
    ) -> Result<LoadedPlugin, PluginError> {
        // Validate WASM
        self.validate_wasm(&bytes)?;

        // Parse or create manifest
        let manifest = match manifest {
            Some(m) => {
                m.validate()
                    .map_err(|e| PluginError::InvalidManifest(e.to_string()))?;
                m
            }
            None => self.create_default_manifest(name),
        };

        // Calculate source hash
        let source_hash = self.calculate_hash(&bytes);

        // Create loaded plugin
        Ok(LoadedPlugin::new(
            name.to_string(),
            manifest,
            bytes,
            source_hash,
        ))
    }

    /// Load from plugin source
    pub fn load_from_source(&self, source: PluginSource) -> Result<LoadedPlugin, PluginError> {
        match source {
            PluginSource::Bytes {
                name,
                bytes,
                manifest,
            } => self.load(&name, bytes, manifest),
            PluginSource::File { path } => {
                let bytes = std::fs::read(&path)
                    .map_err(|e| PluginError::LoadError(format!("failed to read file: {}", e)))?;

                let name = std::path::Path::new(&path)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("unknown")
                    .to_string();

                // Try loading manifest from adjacent file
                let manifest_path = format!("{}.toml", path.trim_end_matches(".wasm"));
                let manifest = std::fs::read_to_string(&manifest_path)
                    .ok()
                    .and_then(|s| toml::from_str(&s).ok());

                self.load(&name, bytes, manifest)
            }
            PluginSource::Base64 {
                name,
                data,
                manifest,
            } => {
                use base64::Engine;
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(&data)
                    .map_err(|e| PluginError::LoadError(format!("invalid base64: {}", e)))?;

                self.load(&name, bytes, manifest)
            }
        }
    }

    /// Validate WASM bytecode
    fn validate_wasm(&self, bytes: &[u8]) -> Result<(), PluginError> {
        // Check minimum size
        if bytes.len() < 8 {
            return Err(PluginError::LoadError("WASM too small".to_string()));
        }

        // Check magic number
        if &bytes[0..4] != b"\0asm" {
            return Err(PluginError::LoadError(
                "invalid WASM magic number".to_string(),
            ));
        }

        // Check version (1)
        let version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        if version != 1 {
            return Err(PluginError::LoadError(format!(
                "unsupported WASM version: {}",
                version
            )));
        }

        // Check size limits
        if bytes.len() > self.sandbox_config.limits.max_wasm_size {
            return Err(PluginError::LoadError(format!(
                "WASM size {} exceeds limit {}",
                bytes.len(),
                self.sandbox_config.limits.max_wasm_size
            )));
        }

        Ok(())
    }

    /// Calculate hash of bytes
    fn calculate_hash(&self, data: &[u8]) -> String {
        // FNV-1a 64-bit hash
        const FNV_OFFSET: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;

        let mut hash = FNV_OFFSET;
        for byte in data {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash ^= data.len() as u64;
        hash = hash.wrapping_mul(FNV_PRIME);

        format!("{:016x}", hash)
    }

    /// Create a default manifest for a plugin
    fn create_default_manifest(&self, name: &str) -> PluginManifest {
        PluginManifest::new(name, Version::new(0, 0, 0)).with_description("Auto-generated manifest")
    }

    /// Parse manifest from TOML
    pub fn parse_manifest(&self, toml_str: &str) -> Result<PluginManifest, PluginError> {
        toml::from_str(toml_str)
            .map_err(|e| PluginError::InvalidManifest(format!("TOML parse error: {}", e)))
    }

    /// Parse manifest from embedded custom section
    pub fn parse_embedded_manifest(
        &self,
        bytes: &[u8],
    ) -> Result<Option<PluginManifest>, PluginError> {
        // Look for custom section named "ferrite-manifest"
        // Custom sections start after the header (8 bytes)
        if bytes.len() < 8 {
            return Ok(None);
        }

        let mut pos = 8;
        while pos < bytes.len() {
            // Read section id
            let section_id = bytes.get(pos).copied().unwrap_or(0);
            pos += 1;

            if pos >= bytes.len() {
                break;
            }

            // Read section size (LEB128)
            let (section_size, bytes_read) = read_leb128(&bytes[pos..]);
            pos += bytes_read;

            if section_id == 0 {
                // Custom section - check name
                if pos >= bytes.len() {
                    break;
                }

                let (name_len, name_bytes_read) = read_leb128(&bytes[pos..]);
                pos += name_bytes_read;

                if pos + name_len > bytes.len() {
                    break;
                }

                let name = std::str::from_utf8(&bytes[pos..pos + name_len]).unwrap_or("");
                pos += name_len;

                if name == "ferrite-manifest" {
                    // Found manifest section
                    let content_size = section_size - name_bytes_read - name_len;
                    if pos + content_size <= bytes.len() {
                        let manifest_bytes = &bytes[pos..pos + content_size];
                        if let Ok(manifest_str) = std::str::from_utf8(manifest_bytes) {
                            return self.parse_manifest(manifest_str).map(Some);
                        }
                    }
                }
            }

            // Skip to next section
            pos += section_size.saturating_sub(pos - (pos - 1));
        }

        Ok(None)
    }
}

impl Default for PluginLoader {
    fn default() -> Self {
        Self::new(SandboxConfig::default())
    }
}

/// Plugin source for loading
pub enum PluginSource {
    /// Load from raw bytes
    Bytes {
        /// Plugin name
        name: String,
        /// WASM bytes
        bytes: Vec<u8>,
        /// Optional manifest
        manifest: Option<PluginManifest>,
    },
    /// Load from file
    File {
        /// File path
        path: String,
    },
    /// Load from base64 encoded string
    Base64 {
        /// Plugin name
        name: String,
        /// Base64 encoded WASM
        data: String,
        /// Optional manifest
        manifest: Option<PluginManifest>,
    },
}

impl PluginSource {
    /// Create from bytes
    pub fn from_bytes(name: &str, bytes: Vec<u8>) -> Self {
        PluginSource::Bytes {
            name: name.to_string(),
            bytes,
            manifest: None,
        }
    }

    /// Create from file path
    pub fn from_file(path: &str) -> Self {
        PluginSource::File {
            path: path.to_string(),
        }
    }

    /// Create from base64
    pub fn from_base64(name: &str, data: &str) -> Self {
        PluginSource::Base64 {
            name: name.to_string(),
            data: data.to_string(),
            manifest: None,
        }
    }

    /// Add manifest to source
    pub fn with_manifest(self, manifest: PluginManifest) -> Self {
        match self {
            PluginSource::Bytes { name, bytes, .. } => PluginSource::Bytes {
                name,
                bytes,
                manifest: Some(manifest),
            },
            PluginSource::File { path } => PluginSource::File { path },
            PluginSource::Base64 { name, data, .. } => PluginSource::Base64 {
                name,
                data,
                manifest: Some(manifest),
            },
        }
    }
}

/// Read LEB128 encoded unsigned integer
fn read_leb128(bytes: &[u8]) -> (usize, usize) {
    let mut result = 0usize;
    let mut shift = 0;
    let mut bytes_read = 0;

    for byte in bytes {
        bytes_read += 1;
        result |= ((byte & 0x7f) as usize) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift > 28 {
            break; // Prevent overflow
        }
    }

    (result, bytes_read)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_wasm_bytes() -> Vec<u8> {
        vec![
            0x00, 0x61, 0x73, 0x6D, // Magic: \0asm
            0x01, 0x00, 0x00, 0x00, // Version: 1
        ]
    }

    #[test]
    fn test_loader_new() {
        let loader = PluginLoader::new(SandboxConfig::default());
        // Just verify it constructs
        let _ = loader;
    }

    #[test]
    fn test_loader_load() {
        let loader = PluginLoader::default();
        let plugin = loader.load("test", valid_wasm_bytes(), None).unwrap();

        assert_eq!(plugin.name, "test");
        assert!(!plugin.source_hash.is_empty());
    }

    #[test]
    fn test_loader_load_with_manifest() {
        let loader = PluginLoader::default();
        let manifest =
            PluginManifest::new("test", Version::new(1, 2, 3)).with_description("Test plugin");

        let plugin = loader
            .load("test", valid_wasm_bytes(), Some(manifest))
            .unwrap();

        assert_eq!(plugin.name, "test");
        assert_eq!(plugin.manifest.version, Version::new(1, 2, 3));
        assert_eq!(plugin.manifest.description, "Test plugin");
    }

    #[test]
    fn test_loader_validate_wasm_invalid_magic() {
        let loader = PluginLoader::default();
        let invalid = vec![0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00];

        let result = loader.load("test", invalid, None);
        assert!(matches!(result, Err(PluginError::LoadError(_))));
    }

    #[test]
    fn test_loader_validate_wasm_too_small() {
        let loader = PluginLoader::default();
        let too_small = vec![0x00, 0x61, 0x73];

        let result = loader.load("test", too_small, None);
        assert!(matches!(result, Err(PluginError::LoadError(_))));
    }

    #[test]
    fn test_loader_validate_wasm_wrong_version() {
        let loader = PluginLoader::default();
        let wrong_version = vec![
            0x00, 0x61, 0x73, 0x6D, 0x02, 0x00, 0x00, 0x00, // Version 2
        ];

        let result = loader.load("test", wrong_version, None);
        assert!(matches!(result, Err(PluginError::LoadError(_))));
    }

    #[test]
    fn test_loader_calculate_hash() {
        let loader = PluginLoader::default();

        let hash1 = loader.calculate_hash(b"hello");
        let hash2 = loader.calculate_hash(b"hello");
        let hash3 = loader.calculate_hash(b"world");

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
        assert_eq!(hash1.len(), 16);
    }

    #[test]
    fn test_loader_parse_manifest() {
        let loader = PluginLoader::default();
        let toml = r#"
            name = "test-plugin"
            description = "A test plugin"

            [version]
            major = 1
            minor = 2
            patch = 3
        "#;

        let manifest = loader.parse_manifest(toml).unwrap();
        assert_eq!(manifest.name, "test-plugin");
        assert_eq!(manifest.version, Version::new(1, 2, 3));
    }

    #[test]
    fn test_plugin_source_from_bytes() {
        let source = PluginSource::from_bytes("test", valid_wasm_bytes());
        match source {
            PluginSource::Bytes {
                name,
                bytes,
                manifest,
            } => {
                assert_eq!(name, "test");
                assert_eq!(bytes.len(), 8);
                assert!(manifest.is_none());
            }
            _ => panic!("expected Bytes variant"),
        }
    }

    #[test]
    fn test_plugin_source_from_file() {
        let source = PluginSource::from_file("/path/to/plugin.wasm");
        match source {
            PluginSource::File { path } => {
                assert_eq!(path, "/path/to/plugin.wasm");
            }
            _ => panic!("expected File variant"),
        }
    }

    #[test]
    fn test_plugin_source_with_manifest() {
        let source = PluginSource::from_bytes("test", valid_wasm_bytes());
        let manifest = PluginManifest::new("test", Version::new(1, 0, 0));
        let source = source.with_manifest(manifest);

        match source {
            PluginSource::Bytes { manifest, .. } => {
                assert!(manifest.is_some());
            }
            _ => panic!("expected Bytes variant"),
        }
    }

    #[test]
    fn test_read_leb128() {
        // Single byte
        assert_eq!(read_leb128(&[0x05]), (5, 1));
        assert_eq!(read_leb128(&[0x7F]), (127, 1));

        // Multi-byte
        assert_eq!(read_leb128(&[0x80, 0x01]), (128, 2));
        assert_eq!(read_leb128(&[0xE5, 0x8E, 0x26]), (624485, 3));
    }

    #[test]
    fn test_loader_load_from_source_bytes() {
        let loader = PluginLoader::default();
        let source = PluginSource::from_bytes("test", valid_wasm_bytes());

        let plugin = loader.load_from_source(source).unwrap();
        assert_eq!(plugin.name, "test");
    }

    #[test]
    fn test_loader_load_from_source_base64() {
        let loader = PluginLoader::default();

        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode(valid_wasm_bytes());

        let source = PluginSource::from_base64("test", &encoded);
        let plugin = loader.load_from_source(source).unwrap();

        assert_eq!(plugin.name, "test");
    }
}
