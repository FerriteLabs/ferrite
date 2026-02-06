//! Actionable startup error messages
//!
//! This module provides user-friendly error messages with actionable suggestions
//! for common startup issues. Each error has a machine-readable error code for
//! programmatic handling by SDKs and monitoring tools.
//!
//! ## Error Code Format
//!
//! Error codes follow the format `FERR{category}{number}`:
//! - `FERR1xxx` - Network/binding errors
//! - `FERR2xxx` - File system/permission errors
//! - `FERR3xxx` - Configuration errors
//! - `FERR4xxx` - Resource errors (disk, memory)
//! - `FERR5xxx` - Security/TLS errors
//! - `FERR9xxx` - Other/unknown errors
//!
//! ## Example
//!
//! ```rust,ignore
//! use ferrite::startup_errors::{StartupError, ErrorCode};
//!
//! let err = StartupError::port_in_use(6379);
//! println!("Code: {}", err.error_code()); // FERR1001
//! println!("Message: {}", err);
//! ```

use std::fmt;
use std::io;
use std::path::Path;

/// Machine-readable error code for programmatic handling
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ErrorCode {
    code: &'static str,
    category: ErrorCategory,
}

impl ErrorCode {
    const fn new(code: &'static str, category: ErrorCategory) -> Self {
        Self { code, category }
    }

    /// Get the error code string (e.g., "FERR1001")
    pub fn code(&self) -> &'static str {
        self.code
    }

    /// Get the error category
    pub fn category(&self) -> ErrorCategory {
        self.category
    }

    /// Check if this is a retryable error
    pub fn is_retryable(&self) -> bool {
        matches!(
            self.category,
            ErrorCategory::Network | ErrorCategory::Resource
        )
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.code)
    }
}

/// Error category for grouping related errors
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCategory {
    /// Network and binding errors (FERR1xxx)
    Network,
    /// File system and permission errors (FERR2xxx)
    FileSystem,
    /// Configuration errors (FERR3xxx)
    Configuration,
    /// Resource errors - disk, memory (FERR4xxx)
    Resource,
    /// Security and TLS errors (FERR5xxx)
    Security,
    /// Other/unknown errors (FERR9xxx)
    Other,
}

impl fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorCategory::Network => write!(f, "Network"),
            ErrorCategory::FileSystem => write!(f, "FileSystem"),
            ErrorCategory::Configuration => write!(f, "Configuration"),
            ErrorCategory::Resource => write!(f, "Resource"),
            ErrorCategory::Security => write!(f, "Security"),
            ErrorCategory::Other => write!(f, "Other"),
        }
    }
}

// Network errors (FERR1xxx)
/// Port already in use
pub const FERR1001: ErrorCode = ErrorCode::new("FERR1001", ErrorCategory::Network);
/// Cannot bind to address
pub const FERR1002: ErrorCode = ErrorCode::new("FERR1002", ErrorCategory::Network);

// File system errors (FERR2xxx)
/// Permission denied
pub const FERR2001: ErrorCode = ErrorCode::new("FERR2001", ErrorCategory::FileSystem);
/// Directory not found
pub const FERR2002: ErrorCode = ErrorCode::new("FERR2002", ErrorCategory::FileSystem);
/// Lock file exists
pub const FERR2003: ErrorCode = ErrorCode::new("FERR2003", ErrorCategory::FileSystem);
/// General I/O error
pub const FERR2004: ErrorCode = ErrorCode::new("FERR2004", ErrorCategory::FileSystem);

// Configuration errors (FERR3xxx)
/// Configuration file not found
pub const FERR3001: ErrorCode = ErrorCode::new("FERR3001", ErrorCategory::Configuration);
/// Configuration parse error
pub const FERR3002: ErrorCode = ErrorCode::new("FERR3002", ErrorCategory::Configuration);
/// Invalid configuration value
pub const FERR3003: ErrorCode = ErrorCode::new("FERR3003", ErrorCategory::Configuration);

// Resource errors (FERR4xxx)
/// Insufficient disk space
pub const FERR4001: ErrorCode = ErrorCode::new("FERR4001", ErrorCategory::Resource);

// Security errors (FERR5xxx)
/// TLS certificate error
pub const FERR5001: ErrorCode = ErrorCode::new("FERR5001", ErrorCategory::Security);

// Other errors (FERR9xxx)
/// Other/unknown error
pub const FERR9001: ErrorCode = ErrorCode::new("FERR9001", ErrorCategory::Other);

/// Format a startup error with actionable suggestions
///
/// The formatted message includes:
/// - Machine-readable error code (e.g., FERR1001)
/// - Human-readable error description
/// - Actionable suggestions for fixing the issue
/// - Platform-specific commands where applicable
pub fn format_startup_error(error: &StartupError) -> String {
    let code = error.error_code();
    match error {
        StartupError::PortInUse { port, process } => {
            let mut msg = format!("[{}] Error: Port {} is already in use", code, port);
            if let Some(proc) = process {
                msg.push_str(&format!(" by {}", proc));
            }
            msg.push_str("\n\nTo fix this, try one of the following:");
            msg.push_str(&format!("\n  1. Stop the process using port {}", port));
            #[cfg(unix)]
            {
                msg.push_str(&format!("\n     Run: lsof -i :{} | grep LISTEN", port));
                msg.push_str("\n     Then: kill <PID>");
            }
            #[cfg(windows)]
            {
                msg.push_str(&format!("\n     Run: netstat -ano | findstr :{}", port));
                msg.push_str("\n     Then: taskkill /PID <PID> /F");
            }
            if *port == 6379 {
                msg.push_str("\n  2. Is Redis already running? Stop it first:");
                msg.push_str("\n     redis-cli shutdown");
            }
            msg.push_str(&format!(
                "\n  {}. Use a different port:",
                if *port == 6379 { 3 } else { 2 }
            ));
            msg.push_str(&format!("\n     ferrite --port {}", port + 1));
            msg.push_str(&format!("\n     or: export FERRITE_PORT={}", port + 1));
            msg.push_str("\n\nRun 'ferrite doctor' for full diagnostics.");
            msg
        }

        StartupError::PermissionDenied { path, operation } => {
            let mut msg = format!(
                "[{}] Error: Permission denied {} '{}'",
                code,
                operation,
                path.display()
            );
            msg.push_str("\n\nTo fix this, try one of the following:");
            #[cfg(unix)]
            {
                msg.push_str("\n  1. Check file permissions:");
                msg.push_str(&format!("\n     ls -la {}", path.display()));
                msg.push_str("\n  2. Fix ownership:");
                msg.push_str(&format!("\n     sudo chown $USER {}", path.display()));
                msg.push_str("\n  3. Use a different directory:");
                msg.push_str("\n     ferrite --data-dir /tmp/ferrite");
            }
            #[cfg(windows)]
            {
                msg.push_str("\n  1. Run as Administrator");
                msg.push_str(&format!("\n  2. Check file permissions in Properties"));
                msg.push_str("\n  3. Use a different directory:");
                msg.push_str("\n     ferrite --data-dir C:\\Temp\\ferrite");
            }
            msg
        }

        StartupError::DirectoryNotFound { path } => {
            let mut msg = format!(
                "[{}] Error: Directory not found: '{}'",
                code,
                path.display()
            );
            msg.push_str("\n\nTo fix this:");
            msg.push_str("\n  1. Create the directory:");
            #[cfg(unix)]
            msg.push_str(&format!("\n     mkdir -p {}", path.display()));
            #[cfg(windows)]
            msg.push_str(&format!("\n     mkdir {}", path.display()));
            msg.push_str("\n  2. Or run 'ferrite init' to set up a new configuration");
            msg.push_str("\n  3. Or use a different path:");
            msg.push_str("\n     ferrite --data-dir ./data");
            msg
        }

        StartupError::ConfigNotFound { path } => {
            let mut msg = format!(
                "[{}] Error: Configuration file not found: '{}'",
                code,
                path.display()
            );
            msg.push_str("\n\nTo fix this:");
            msg.push_str("\n  1. Run 'ferrite init' to create a new configuration");
            msg.push_str(&format!(
                "\n  2. Or check the path is correct: {}",
                path.display()
            ));
            msg.push_str("\n  3. Or run without a config (Ferrite uses sensible defaults):");
            msg.push_str("\n     ferrite");
            msg
        }

        StartupError::ConfigParseError {
            path,
            line,
            column,
            message,
        } => {
            let mut msg = format!("[{}] Error: Failed to parse configuration file", code);
            if let Some(p) = path {
                msg.push_str(&format!(": '{}'", p.display()));
            }
            if let (Some(l), Some(c)) = (line, column) {
                msg.push_str(&format!(" at line {}, column {}", l, c));
            } else if let Some(l) = line {
                msg.push_str(&format!(" at line {}", l));
            }
            msg.push_str(&format!("\n\nDetails: {}", message));
            msg.push_str("\n\nTo fix this:");
            msg.push_str("\n  1. Check the TOML syntax in your configuration file");
            msg.push_str("\n  2. Validate with: ferrite --test-config --config ferrite.toml");
            msg.push_str("\n  3. Or regenerate: ferrite init --force");
            msg.push_str("\n\nCommon issues:");
            msg.push_str("\n  - Missing quotes around string values");
            msg.push_str("\n  - Unclosed brackets or braces");
            msg.push_str("\n  - Invalid key names (typos)");
            msg.push_str("\n\nRun 'ferrite doctor' for full diagnostics.");
            msg
        }

        StartupError::InvalidConfigValue {
            key,
            value,
            expected,
        } => {
            let mut msg = format!(
                "[{}] Error: Invalid value for configuration key '{}'",
                code, key
            );
            if let Some(v) = value {
                msg.push_str(&format!(": got '{}'", v));
            }
            msg.push_str(&format!(", expected {}", expected));
            msg.push_str("\n\nTo fix this:");
            msg.push_str(&format!(
                "\n  1. Check the value of '{}' in your config file",
                key
            ));
            msg.push_str(&format!("\n  2. Expected format: {}", expected));

            // Provide specific suggestions based on the key
            if key.contains("port") {
                msg.push_str("\n  3. Example: port = 6379 (must be 1-65535)");
            } else if key.contains("memory") || key.contains("size") {
                msg.push_str("\n  3. Example: max_memory = 1073741824 (1GB in bytes)");
            } else if key.contains("level") {
                msg.push_str("\n  3. Valid levels: trace, debug, info, warn, error");
            }
            msg
        }

        StartupError::TlsCertificateError { path, message } => {
            let mut msg = format!("[{}] Error: TLS certificate error", code);
            if let Some(p) = path {
                msg.push_str(&format!(": '{}'", p.display()));
            }
            msg.push_str(&format!("\n\nDetails: {}", message));
            msg.push_str("\n\nTo fix this:");
            msg.push_str("\n  1. Verify the certificate file exists and is readable");
            msg.push_str("\n  2. Check the certificate format (PEM or DER)");
            msg.push_str("\n  3. Verify the certificate is not expired:");
            #[cfg(unix)]
            msg.push_str("\n     openssl x509 -in cert.pem -text -noout");
            msg.push_str("\n  4. For testing, disable TLS:");
            msg.push_str("\n     [tls]");
            msg.push_str("\n     enabled = false");
            msg
        }

        StartupError::BindAddressError { address, message } => {
            let mut msg = format!("[{}] Error: Cannot bind to address '{}'", code, address);
            msg.push_str(&format!("\n\nDetails: {}", message));
            msg.push_str("\n\nTo fix this:");
            msg.push_str("\n  1. Check if the address is valid");
            msg.push_str("\n  2. Try binding to 0.0.0.0 (all interfaces):");
            msg.push_str("\n     ferrite --bind 0.0.0.0");
            msg.push_str("\n  3. Or try 127.0.0.1 (localhost only):");
            msg.push_str("\n     ferrite --bind 127.0.0.1");
            #[cfg(unix)]
            {
                msg.push_str("\n  4. Check available network interfaces:");
                msg.push_str("\n     ip addr show");
            }
            msg
        }

        StartupError::DiskSpaceError {
            path,
            available,
            required,
        } => {
            let mut msg = format!(
                "[{}] Error: Insufficient disk space at '{}'",
                code,
                path.display()
            );
            msg.push_str(&format!("\n\nAvailable: {} MB", available / (1024 * 1024)));
            msg.push_str(&format!(", Required: {} MB", required / (1024 * 1024)));
            msg.push_str("\n\nTo fix this:");
            msg.push_str("\n  1. Free up disk space");
            #[cfg(unix)]
            {
                msg.push_str(&format!("\n     df -h {}", path.display()));
                msg.push_str("\n     du -sh /* | sort -h");
            }
            msg.push_str("\n  2. Use a different data directory:");
            msg.push_str("\n     ferrite --data-dir /path/with/more/space");
            msg.push_str("\n  3. Reduce max memory setting in configuration");
            msg
        }

        StartupError::LockFileExists { path, pid } => {
            let mut msg = format!("[{}] Error: Lock file exists at '{}'", code, path.display());
            if let Some(p) = pid {
                msg.push_str(&format!(
                    "\n\nAnother Ferrite instance may be running (PID: {})",
                    p
                ));
            }
            msg.push_str("\n\nTo fix this:");
            msg.push_str("\n  1. Check if another Ferrite instance is running:");
            #[cfg(unix)]
            {
                msg.push_str("\n     pgrep -a ferrite");
                if let Some(p) = pid {
                    msg.push_str(&format!("\n     kill {}", p));
                }
            }
            msg.push_str("\n  2. If no instance is running, remove the stale lock file:");
            msg.push_str(&format!("\n     rm {}", path.display()));
            msg.push_str("\n  3. Or use a different data directory:");
            msg.push_str("\n     ferrite --data-dir ./new-data");
            msg
        }

        StartupError::IoError {
            operation,
            path,
            source,
        } => {
            let mut msg = format!("[{}] Error: I/O error during {}", code, operation);
            if let Some(p) = path {
                msg.push_str(&format!(" on '{}'", p.display()));
            }
            msg.push_str(&format!("\n\nDetails: {}", source));

            // Provide suggestions based on error kind
            match source.kind() {
                io::ErrorKind::NotFound => {
                    msg.push_str("\n\nTo fix this:");
                    msg.push_str("\n  1. Verify the path exists");
                    if let Some(p) = path {
                        msg.push_str(&format!("\n  2. Create it: mkdir -p {}", p.display()));
                    }
                }
                io::ErrorKind::PermissionDenied => {
                    msg.push_str("\n\nTo fix this:");
                    msg.push_str("\n  1. Check file/directory permissions");
                    #[cfg(unix)]
                    if let Some(p) = path {
                        msg.push_str(&format!(
                            "\n  2. Fix ownership: sudo chown $USER {}",
                            p.display()
                        ));
                    }
                }
                io::ErrorKind::AlreadyExists => {
                    msg.push_str("\n\nTo fix this:");
                    msg.push_str("\n  1. Remove the existing file/directory");
                    msg.push_str("\n  2. Or use --force to overwrite");
                }
                _ => {
                    msg.push_str("\n\nGeneral suggestions:");
                    msg.push_str("\n  1. Check disk health and available space");
                    msg.push_str("\n  2. Verify the path is accessible");
                    msg.push_str("\n  3. Check system logs for more details");
                }
            }
            msg
        }

        StartupError::Other {
            message,
            suggestion,
        } => {
            let mut msg = format!("[{}] Error: {}", code, message);
            if let Some(s) = suggestion {
                msg.push_str(&format!("\n\n{}", s));
            }
            msg
        }
    }
}

/// Startup error types with context for actionable messages
#[derive(Debug)]
pub enum StartupError {
    /// Port is already in use
    PortInUse {
        /// The port number that is occupied.
        port: u16,
        /// Name of the process occupying the port, if known.
        process: Option<String>,
    },

    /// Permission denied accessing a path
    PermissionDenied {
        /// Filesystem path where permission was denied.
        path: std::path::PathBuf,
        /// Description of the operation that failed (e.g., "reading", "writing").
        operation: String,
    },

    /// Directory does not exist
    DirectoryNotFound {
        /// Path to the missing directory.
        path: std::path::PathBuf,
    },

    /// Configuration file not found
    ConfigNotFound {
        /// Path to the expected configuration file.
        path: std::path::PathBuf,
    },

    /// Configuration parse error
    ConfigParseError {
        /// Path to the configuration file, if available.
        path: Option<std::path::PathBuf>,
        /// Line number where the parse error occurred.
        line: Option<usize>,
        /// Column number where the parse error occurred.
        column: Option<usize>,
        /// Human-readable description of the parse error.
        message: String,
    },

    /// Invalid configuration value
    InvalidConfigValue {
        /// Dotted config key name (e.g., `"server.port"`).
        key: String,
        /// The invalid value that was provided, if available.
        value: Option<String>,
        /// Description of the expected value format.
        expected: String,
    },

    /// TLS certificate error
    TlsCertificateError {
        /// Path to the certificate file, if available.
        path: Option<std::path::PathBuf>,
        /// Human-readable description of the TLS error.
        message: String,
    },

    /// Cannot bind to address
    BindAddressError {
        /// The address that could not be bound.
        address: String,
        /// Human-readable description of the bind failure.
        message: String,
    },

    /// Insufficient disk space
    DiskSpaceError {
        /// Path to the directory with insufficient space.
        path: std::path::PathBuf,
        /// Available disk space in bytes.
        available: u64,
        /// Required disk space in bytes.
        required: u64,
    },

    /// Lock file exists (another instance running)
    LockFileExists {
        /// Path to the existing lock file.
        path: std::path::PathBuf,
        /// PID of the process holding the lock, if known.
        pid: Option<u32>,
    },

    /// General I/O error with context
    IoError {
        /// Description of the operation that failed.
        operation: String,
        /// Filesystem path involved, if applicable.
        path: Option<std::path::PathBuf>,
        /// The underlying I/O error.
        source: io::Error,
    },

    /// Other error with optional suggestion
    Other {
        /// Human-readable error message.
        message: String,
        /// Optional actionable suggestion for the user.
        suggestion: Option<String>,
    },
}

impl StartupError {
    /// Create a port in use error
    pub fn port_in_use(port: u16) -> Self {
        StartupError::PortInUse {
            port,
            process: None,
        }
    }

    /// Create a permission denied error
    pub fn permission_denied(path: impl AsRef<Path>, operation: &str) -> Self {
        StartupError::PermissionDenied {
            path: path.as_ref().to_path_buf(),
            operation: operation.to_string(),
        }
    }

    /// Create a config not found error
    pub fn config_not_found(path: impl AsRef<Path>) -> Self {
        StartupError::ConfigNotFound {
            path: path.as_ref().to_path_buf(),
        }
    }

    /// Create a directory not found error
    pub fn directory_not_found(path: impl AsRef<Path>) -> Self {
        StartupError::DirectoryNotFound {
            path: path.as_ref().to_path_buf(),
        }
    }

    /// Create a config parse error
    pub fn config_parse_error(path: Option<&Path>, message: &str) -> Self {
        StartupError::ConfigParseError {
            path: path.map(|p| p.to_path_buf()),
            line: None,
            column: None,
            message: message.to_string(),
        }
    }

    /// Create an I/O error with context
    pub fn io_error(operation: &str, path: Option<&Path>, source: io::Error) -> Self {
        StartupError::IoError {
            operation: operation.to_string(),
            path: path.map(|p| p.to_path_buf()),
            source,
        }
    }

    /// Check if an I/O error is a port-in-use error and convert it
    pub fn from_bind_error(err: io::Error, port: u16) -> Self {
        if err.kind() == io::ErrorKind::AddrInUse {
            StartupError::port_in_use(port)
        } else {
            StartupError::IoError {
                operation: "binding to port".to_string(),
                path: None,
                source: err,
            }
        }
    }

    /// Get the machine-readable error code for this error
    ///
    /// Error codes follow the format `FERR{category}{number}`:
    /// - `FERR1xxx` - Network/binding errors
    /// - `FERR2xxx` - File system/permission errors
    /// - `FERR3xxx` - Configuration errors
    /// - `FERR4xxx` - Resource errors
    /// - `FERR5xxx` - Security/TLS errors
    /// - `FERR9xxx` - Other/unknown errors
    pub fn error_code(&self) -> ErrorCode {
        match self {
            StartupError::PortInUse { .. } => FERR1001,
            StartupError::BindAddressError { .. } => FERR1002,
            StartupError::PermissionDenied { .. } => FERR2001,
            StartupError::DirectoryNotFound { .. } => FERR2002,
            StartupError::LockFileExists { .. } => FERR2003,
            StartupError::IoError { .. } => FERR2004,
            StartupError::ConfigNotFound { .. } => FERR3001,
            StartupError::ConfigParseError { .. } => FERR3002,
            StartupError::InvalidConfigValue { .. } => FERR3003,
            StartupError::DiskSpaceError { .. } => FERR4001,
            StartupError::TlsCertificateError { .. } => FERR5001,
            StartupError::Other { .. } => FERR9001,
        }
    }
}

impl std::fmt::Display for StartupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format_startup_error(self))
    }
}

impl std::error::Error for StartupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StartupError::IoError { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_port_in_use_message() {
        let err = StartupError::port_in_use(6379);
        let msg = format_startup_error(&err);

        assert!(msg.contains("[FERR1001]"));
        assert!(msg.contains("Port 6379 is already in use"));
        assert!(msg.contains("lsof") || msg.contains("netstat"));
        assert!(msg.contains("--port 6380"));
    }

    #[test]
    fn test_permission_denied_message() {
        let err = StartupError::permission_denied("/var/lib/ferrite", "accessing");
        let msg = format_startup_error(&err);

        assert!(msg.contains("[FERR2001]"));
        assert!(msg.contains("Permission denied"));
        assert!(msg.contains("/var/lib/ferrite"));
        assert!(msg.contains("chown") || msg.contains("Administrator"));
    }

    #[test]
    fn test_config_not_found_message() {
        let err = StartupError::config_not_found("/etc/ferrite/ferrite.toml");
        let msg = format_startup_error(&err);

        assert!(msg.contains("[FERR3001]"));
        assert!(msg.contains("Configuration file not found"));
        assert!(msg.contains("ferrite init"));
    }

    #[test]
    fn test_config_parse_error_message() {
        let err = StartupError::ConfigParseError {
            path: Some("/etc/ferrite/ferrite.toml".into()),
            line: Some(10),
            column: Some(5),
            message: "unexpected token".to_string(),
        };
        let msg = format_startup_error(&err);

        assert!(msg.contains("[FERR3002]"));
        assert!(msg.contains("line 10"));
        assert!(msg.contains("column 5"));
        assert!(msg.contains("unexpected token"));
        assert!(msg.contains("--test-config"));
    }

    #[test]
    fn test_error_codes() {
        // Network errors
        assert_eq!(StartupError::port_in_use(6379).error_code(), FERR1001);
        assert_eq!(
            StartupError::BindAddressError {
                address: "0.0.0.0".to_string(),
                message: "test".to_string(),
            }
            .error_code(),
            FERR1002
        );

        // File system errors
        assert_eq!(
            StartupError::permission_denied("/test", "reading").error_code(),
            FERR2001
        );
        assert_eq!(
            StartupError::DirectoryNotFound {
                path: "/test".into(),
            }
            .error_code(),
            FERR2002
        );
        assert_eq!(
            StartupError::LockFileExists {
                path: "/test".into(),
                pid: None,
            }
            .error_code(),
            FERR2003
        );

        // Configuration errors
        assert_eq!(
            StartupError::config_not_found("/test").error_code(),
            FERR3001
        );
        assert_eq!(
            StartupError::config_parse_error(None, "test").error_code(),
            FERR3002
        );
        assert_eq!(
            StartupError::InvalidConfigValue {
                key: "port".to_string(),
                value: None,
                expected: "integer".to_string(),
            }
            .error_code(),
            FERR3003
        );

        // Resource errors
        assert_eq!(
            StartupError::DiskSpaceError {
                path: "/test".into(),
                available: 100,
                required: 1000,
            }
            .error_code(),
            FERR4001
        );

        // Security errors
        assert_eq!(
            StartupError::TlsCertificateError {
                path: None,
                message: "test".to_string(),
            }
            .error_code(),
            FERR5001
        );

        // Other errors
        assert_eq!(
            StartupError::Other {
                message: "test".to_string(),
                suggestion: None,
            }
            .error_code(),
            FERR9001
        );
    }

    #[test]
    fn test_error_code_categories() {
        assert_eq!(FERR1001.category(), ErrorCategory::Network);
        assert_eq!(FERR2001.category(), ErrorCategory::FileSystem);
        assert_eq!(FERR3001.category(), ErrorCategory::Configuration);
        assert_eq!(FERR4001.category(), ErrorCategory::Resource);
        assert_eq!(FERR5001.category(), ErrorCategory::Security);
        assert_eq!(FERR9001.category(), ErrorCategory::Other);
    }

    #[test]
    fn test_error_code_retryable() {
        // Network and resource errors should be retryable
        assert!(FERR1001.is_retryable());
        assert!(FERR4001.is_retryable());

        // Configuration and other errors should not be retryable
        assert!(!FERR3001.is_retryable());
        assert!(!FERR5001.is_retryable());
        assert!(!FERR9001.is_retryable());
    }

    #[test]
    fn test_error_code_display() {
        assert_eq!(format!("{}", FERR1001), "FERR1001");
        assert_eq!(FERR1001.code(), "FERR1001");
    }
}
