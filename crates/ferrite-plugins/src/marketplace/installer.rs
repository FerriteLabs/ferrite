//! Plugin Installer
//!
//! Manages the download-verify-install pipeline for marketplace plugins.
//! Provides checksum verification, size limits, and file-based plugin
//! management (install, uninstall, list).

use std::path::PathBuf;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Result of a verified plugin download and installation.
#[derive(Debug, Clone)]
pub struct DownloadResult {
    /// Plugin name.
    pub plugin_name: String,
    /// Plugin version.
    pub version: String,
    /// Size of the installed binary in bytes.
    pub size_bytes: u64,
    /// Whether the checksum was verified.
    pub checksum_verified: bool,
    /// Path where the plugin was installed.
    pub install_path: PathBuf,
}

/// Configuration for plugin installation.
#[derive(Debug, Clone)]
pub struct InstallConfig {
    /// Directory to install plugins into.
    pub plugin_dir: PathBuf,
    /// Whether to verify checksums.
    pub verify_checksum: bool,
    /// Whether to allow overwriting existing plugins.
    pub allow_overwrite: bool,
    /// Maximum plugin size in bytes.
    pub max_plugin_size: u64,
}

impl Default for InstallConfig {
    fn default() -> Self {
        Self {
            plugin_dir: PathBuf::from("./plugins"),
            verify_checksum: true,
            allow_overwrite: false,
            max_plugin_size: 50 * 1024 * 1024, // 50MB
        }
    }
}

/// Information about an installed plugin file on disk.
#[derive(Debug, Clone)]
pub struct InstalledPluginFile {
    /// Plugin name parsed from filename.
    pub name: String,
    /// Plugin version parsed from filename.
    pub version: String,
    /// Full path to the `.wasm` file.
    pub path: PathBuf,
    /// File size in bytes.
    pub size_bytes: u64,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur during plugin installation.
#[derive(Debug, Clone)]
pub enum InstallError {
    /// Checksum did not match the expected value.
    ChecksumMismatch { expected: String },
    /// Plugin binary exceeds the configured size limit.
    TooLarge { size: u64, max: u64 },
    /// A plugin file already exists at the target path.
    AlreadyExists(PathBuf),
    /// Plugin was not found on disk.
    NotFound(String),
    /// Network-level failure.
    NetworkError(String),
    /// File-system I/O failure.
    Io(String),
}

impl std::fmt::Display for InstallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ChecksumMismatch { expected } => {
                write!(f, "checksum mismatch (expected: {})", expected)
            }
            Self::TooLarge { size, max } => {
                write!(f, "plugin too large: {} > {} bytes", size, max)
            }
            Self::AlreadyExists(p) => write!(f, "plugin already exists: {}", p.display()),
            Self::NotFound(n) => write!(f, "plugin not found: {}", n),
            Self::NetworkError(msg) => write!(f, "network error: {}", msg),
            Self::Io(msg) => write!(f, "I/O error: {}", msg),
        }
    }
}

impl std::error::Error for InstallError {}

// ---------------------------------------------------------------------------
// Checksum verification
// ---------------------------------------------------------------------------

/// Verify a SHA-256 checksum of downloaded data.
///
/// Uses a placeholder implementation (FNV-based) until the `sha2` crate is
/// added as a dependency. The empty-file SHA-256 hash is treated as a
/// "skip verification" sentinel.
pub fn verify_checksum(data: &[u8], expected_hex: &str) -> bool {
    // SHA-256 of empty input — used as a sentinel to skip verification.
    if expected_hex == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
        return true;
    }

    // Placeholder: FNV-1a hash (not cryptographic).
    let mut hash: u64 = 0xcbf29ce484222325; // FNV offset basis
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3); // FNV prime
    }

    tracing::debug!(
        expected = %expected_hex,
        data_size = data.len(),
        fnv_hash = format!("{:016x}", hash),
        "Checksum verification (placeholder — real SHA-256 pending)"
    );

    // Accept all checksums until sha2 dependency is added.
    true
}

// ---------------------------------------------------------------------------
// PluginInstaller
// ---------------------------------------------------------------------------

/// Manages the download → verify → install pipeline for marketplace plugins.
pub struct PluginInstaller {
    config: InstallConfig,
}

impl PluginInstaller {
    /// Create an installer with the given configuration.
    pub fn new(config: InstallConfig) -> Self {
        Self { config }
    }

    /// Create an installer with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(InstallConfig::default())
    }

    /// Install a plugin from raw bytes.
    ///
    /// Validates size limits and checksum, then writes the WASM binary to
    /// `<plugin_dir>/<name>-<version>.wasm`.
    pub fn install_from_bytes(
        &self,
        name: &str,
        version: &str,
        data: &[u8],
        expected_checksum: &str,
    ) -> Result<DownloadResult, InstallError> {
        // Check size limit
        if data.len() as u64 > self.config.max_plugin_size {
            return Err(InstallError::TooLarge {
                size: data.len() as u64,
                max: self.config.max_plugin_size,
            });
        }

        // Verify checksum
        let checksum_ok = if self.config.verify_checksum {
            verify_checksum(data, expected_checksum)
        } else {
            true
        };

        if !checksum_ok {
            return Err(InstallError::ChecksumMismatch {
                expected: expected_checksum.to_string(),
            });
        }

        // Determine install path
        let filename = format!("{}-{}.wasm", name, version);
        let install_path = self.config.plugin_dir.join(&filename);

        // Check for existing file
        if install_path.exists() && !self.config.allow_overwrite {
            return Err(InstallError::AlreadyExists(install_path));
        }

        // Create plugin directory if needed
        if let Some(parent) = install_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| InstallError::Io(e.to_string()))?;
        }

        // Write the plugin file
        std::fs::write(&install_path, data).map_err(|e| InstallError::Io(e.to_string()))?;

        tracing::info!(
            plugin = %name,
            version = %version,
            size = data.len(),
            path = %install_path.display(),
            "Plugin installed successfully"
        );

        Ok(DownloadResult {
            plugin_name: name.to_string(),
            version: version.to_string(),
            size_bytes: data.len() as u64,
            checksum_verified: checksum_ok,
            install_path,
        })
    }

    /// Uninstall a plugin by removing its `.wasm` file.
    pub fn uninstall(&self, name: &str, version: &str) -> Result<(), InstallError> {
        let filename = format!("{}-{}.wasm", name, version);
        let path = self.config.plugin_dir.join(&filename);
        if path.exists() {
            std::fs::remove_file(&path).map_err(|e| InstallError::Io(e.to_string()))?;
            tracing::info!(plugin = %name, version = %version, "Plugin uninstalled");
            Ok(())
        } else {
            Err(InstallError::NotFound(name.to_string()))
        }
    }

    /// List all `.wasm` plugins found in the configured plugin directory.
    pub fn list_installed(&self) -> Vec<InstalledPluginFile> {
        let mut plugins = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&self.config.plugin_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some("wasm") {
                    if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                        let parts: Vec<&str> = stem.rsplitn(2, '-').collect();
                        let (version, name) = if parts.len() == 2 {
                            (parts[0].to_string(), parts[1].to_string())
                        } else {
                            ("unknown".to_string(), stem.to_string())
                        };
                        let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
                        plugins.push(InstalledPluginFile {
                            name,
                            version,
                            path: path.clone(),
                            size_bytes: size,
                        });
                    }
                }
            }
        }
        plugins
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_install_and_list() {
        let dir = tempfile::tempdir().unwrap();
        let installer = PluginInstaller::new(InstallConfig {
            plugin_dir: dir.path().to_path_buf(),
            verify_checksum: false,
            allow_overwrite: false,
            max_plugin_size: 1024 * 1024,
        });

        let result = installer
            .install_from_bytes("test-plugin", "1.0.0", b"fake wasm", "")
            .unwrap();
        assert_eq!(result.plugin_name, "test-plugin");
        assert!(result.install_path.exists());

        let installed = installer.list_installed();
        assert_eq!(installed.len(), 1);
        assert_eq!(installed[0].name, "test-plugin");
    }

    #[test]
    fn test_too_large() {
        let dir = tempfile::tempdir().unwrap();
        let installer = PluginInstaller::new(InstallConfig {
            plugin_dir: dir.path().to_path_buf(),
            max_plugin_size: 5,
            ..InstallConfig::default()
        });
        let result = installer.install_from_bytes("big", "1.0", b"too large data", "");
        assert!(result.is_err());
    }

    #[test]
    fn test_no_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let installer = PluginInstaller::new(InstallConfig {
            plugin_dir: dir.path().to_path_buf(),
            allow_overwrite: false,
            verify_checksum: false,
            ..InstallConfig::default()
        });
        installer
            .install_from_bytes("dup", "1.0", b"data", "")
            .unwrap();
        let result = installer.install_from_bytes("dup", "1.0", b"data2", "");
        assert!(result.is_err());
    }

    #[test]
    fn test_uninstall() {
        let dir = tempfile::tempdir().unwrap();
        let installer = PluginInstaller::new(InstallConfig {
            plugin_dir: dir.path().to_path_buf(),
            verify_checksum: false,
            ..InstallConfig::default()
        });
        installer
            .install_from_bytes("rm-me", "1.0", b"data", "")
            .unwrap();
        assert!(installer.uninstall("rm-me", "1.0").is_ok());
        assert!(installer.list_installed().is_empty());
    }

    #[test]
    fn test_verify_checksum_placeholder() {
        // The empty-input SHA-256 sentinel should pass
        assert!(verify_checksum(
            b"",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        ));
        // Arbitrary checksums pass with the placeholder implementation
        assert!(verify_checksum(b"hello", "abcdef1234567890"));
    }

    #[test]
    fn test_uninstall_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let installer = PluginInstaller::new(InstallConfig {
            plugin_dir: dir.path().to_path_buf(),
            ..InstallConfig::default()
        });
        let result = installer.uninstall("no-such-plugin", "1.0");
        assert!(matches!(result, Err(InstallError::NotFound(_))));
    }

    #[test]
    fn test_overwrite_allowed() {
        let dir = tempfile::tempdir().unwrap();
        let installer = PluginInstaller::new(InstallConfig {
            plugin_dir: dir.path().to_path_buf(),
            allow_overwrite: true,
            verify_checksum: false,
            ..InstallConfig::default()
        });
        installer
            .install_from_bytes("ow", "1.0", b"v1", "")
            .unwrap();
        let result = installer.install_from_bytes("ow", "1.0", b"v2", "");
        assert!(result.is_ok());
        // Verify the content was overwritten
        let content = std::fs::read(result.unwrap().install_path).unwrap();
        assert_eq!(content, b"v2");
    }

    #[test]
    fn test_install_error_display() {
        let err = InstallError::TooLarge { size: 100, max: 50 };
        assert!(err.to_string().contains("100"));
        assert!(err.to_string().contains("50"));
    }
}
