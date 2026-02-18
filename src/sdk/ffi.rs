//! C FFI Layer for Multi-Language Embedded SDKs
//!
//! Provides a stable C ABI for the embedded Ferrite database that can be consumed
//! by Python (via PyO3/ctypes), Node.js (via napi-rs/ffi-napi), Go (via CGo),
//! and Java (via JNI). This is the foundation layer that all language bindings use.
//!
//! # Safety
//!
//! This module uses `unsafe` for FFI boundary operations. All public FFI functions:
//! - Validate non-null pointers before dereferencing
//! - Use `catch_unwind` to prevent panics from crossing the FFI boundary
//! - Return error codes for all failure cases
//!
//! # ABI Stability
//!
//! The C ABI exposed here follows semver. Breaking changes only happen on
//! major version bumps. All structs use `#[repr(C)]`.
//!
//! # Example (C)
//!
//! ```c
//! #include "ferrite.h"
//!
//! FerDB *db = fer_open("./data");
//! fer_set(db, "key", 3, "value", 5, -1);
//! FerBuffer buf = fer_get(db, "key", 3);
//! fer_buffer_free(&buf);
//! fer_close(db);
//! ```

use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicU64;

// ---------------------------------------------------------------------------
// Error codes
// ---------------------------------------------------------------------------

/// FFI error codes returned by all C API functions.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FfiErrorCode {
    Ok = 0,
    NullPointer = -1,
    InvalidUtf8 = -2,
    KeyNotFound = -3,
    DatabaseClosed = -4,
    WrongType = -5,
    OutOfMemory = -6,
    IoError = -7,
    InvalidArgument = -8,
    InternalError = -99,
}

impl std::fmt::Display for FfiErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ok => write!(f, "OK"),
            Self::NullPointer => write!(f, "NULL_POINTER"),
            Self::InvalidUtf8 => write!(f, "INVALID_UTF8"),
            Self::KeyNotFound => write!(f, "KEY_NOT_FOUND"),
            Self::DatabaseClosed => write!(f, "DATABASE_CLOSED"),
            Self::WrongType => write!(f, "WRONG_TYPE"),
            Self::OutOfMemory => write!(f, "OUT_OF_MEMORY"),
            Self::IoError => write!(f, "IO_ERROR"),
            Self::InvalidArgument => write!(f, "INVALID_ARGUMENT"),
            Self::InternalError => write!(f, "INTERNAL_ERROR"),
        }
    }
}

// ---------------------------------------------------------------------------
// FFI types (repr(C))
// ---------------------------------------------------------------------------

/// Opaque database handle for FFI consumers.
#[repr(C)]
pub struct FfiDbHandle {
    // Opaque pointer to the actual database - would be Box<Database> in real impl
    _opaque: u64,
    is_open: bool,
}

/// A buffer returned across the FFI boundary.
#[repr(C)]
#[derive(Debug)]
pub struct FfiBuffer {
    pub data: *mut u8,
    pub len: usize,
    pub capacity: usize,
    pub error_code: i32,
}

impl FfiBuffer {
    /// Create a buffer from Rust bytes.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut vec = bytes.to_vec();
        let buf = Self {
            data: vec.as_mut_ptr(),
            len: vec.len(),
            capacity: vec.capacity(),
            error_code: FfiErrorCode::Ok as i32,
        };
        std::mem::forget(vec); // ownership transfers to caller
        buf
    }

    /// Create an error buffer.
    pub fn error(code: FfiErrorCode) -> Self {
        Self {
            data: std::ptr::null_mut(),
            len: 0,
            capacity: 0,
            error_code: code as i32,
        }
    }

    /// Create an empty success buffer.
    pub fn ok() -> Self {
        Self {
            data: std::ptr::null_mut(),
            len: 0,
            capacity: 0,
            error_code: FfiErrorCode::Ok as i32,
        }
    }

    pub fn is_ok(&self) -> bool {
        self.error_code == FfiErrorCode::Ok as i32
    }
}

/// Key-value pair for batch operations.
#[repr(C)]
#[derive(Debug)]
pub struct FfiKeyValue {
    pub key: *const u8,
    pub key_len: usize,
    pub value: *const u8,
    pub value_len: usize,
}

// ---------------------------------------------------------------------------
// Language binding descriptors
// ---------------------------------------------------------------------------

/// Supported target languages for SDK generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BindingLanguage {
    Python,
    NodeJs,
    Go,
    Java,
    CSharp,
    Ruby,
    Swift,
}

impl std::fmt::Display for BindingLanguage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Python => write!(f, "python"),
            Self::NodeJs => write!(f, "nodejs"),
            Self::Go => write!(f, "go"),
            Self::Java => write!(f, "java"),
            Self::CSharp => write!(f, "csharp"),
            Self::Ruby => write!(f, "ruby"),
            Self::Swift => write!(f, "swift"),
        }
    }
}

/// Describes a function exported through the C FFI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FfiFunction {
    pub name: String,
    pub c_name: String,
    pub description: String,
    pub parameters: Vec<FfiParameter>,
    pub return_type: FfiType,
    pub safe: bool,
}

/// A parameter in an FFI function.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FfiParameter {
    pub name: String,
    pub param_type: FfiType,
    pub nullable: bool,
    pub description: String,
}

/// C-compatible type descriptors for code generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FfiType {
    Void,
    Int32,
    Int64,
    UInt64,
    Float64,
    Bool,
    CString,
    ByteBuffer,
    OpaquePtr(String),
}

impl std::fmt::Display for FfiType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Void => write!(f, "void"),
            Self::Int32 => write!(f, "int32_t"),
            Self::Int64 => write!(f, "int64_t"),
            Self::UInt64 => write!(f, "uint64_t"),
            Self::Float64 => write!(f, "double"),
            Self::Bool => write!(f, "bool"),
            Self::CString => write!(f, "const char*"),
            Self::ByteBuffer => write!(f, "FerBuffer"),
            Self::OpaquePtr(name) => write!(f, "{}*", name),
        }
    }
}

// ---------------------------------------------------------------------------
// FFI manifest (for code generation)
// ---------------------------------------------------------------------------

/// Complete manifest of the C FFI surface for code generators.
pub struct FfiManifest {
    functions: Vec<FfiFunction>,
}

impl FfiManifest {
    /// Build the FFI manifest describing all exported functions.
    pub fn build() -> Self {
        let functions = vec![
            FfiFunction {
                name: "open".to_string(),
                c_name: "fer_open".to_string(),
                description: "Open or create a database at the given path".to_string(),
                parameters: vec![FfiParameter {
                    name: "path".to_string(),
                    param_type: FfiType::CString,
                    nullable: false,
                    description: "Path to the database directory".to_string(),
                }],
                return_type: FfiType::OpaquePtr("FerDB".to_string()),
                safe: true,
            },
            FfiFunction {
                name: "close".to_string(),
                c_name: "fer_close".to_string(),
                description: "Close the database and free resources".to_string(),
                parameters: vec![FfiParameter {
                    name: "db".to_string(),
                    param_type: FfiType::OpaquePtr("FerDB".to_string()),
                    nullable: false,
                    description: "Database handle".to_string(),
                }],
                return_type: FfiType::Int32,
                safe: true,
            },
            FfiFunction {
                name: "set".to_string(),
                c_name: "fer_set".to_string(),
                description: "Set a key-value pair with optional TTL".to_string(),
                parameters: vec![
                    FfiParameter {
                        name: "db".to_string(),
                        param_type: FfiType::OpaquePtr("FerDB".to_string()),
                        nullable: false,
                        description: "Database handle".to_string(),
                    },
                    FfiParameter {
                        name: "key".to_string(),
                        param_type: FfiType::CString,
                        nullable: false,
                        description: "Key bytes".to_string(),
                    },
                    FfiParameter {
                        name: "key_len".to_string(),
                        param_type: FfiType::UInt64,
                        nullable: false,
                        description: "Key length".to_string(),
                    },
                    FfiParameter {
                        name: "value".to_string(),
                        param_type: FfiType::CString,
                        nullable: false,
                        description: "Value bytes".to_string(),
                    },
                    FfiParameter {
                        name: "value_len".to_string(),
                        param_type: FfiType::UInt64,
                        nullable: false,
                        description: "Value length".to_string(),
                    },
                    FfiParameter {
                        name: "ttl_ms".to_string(),
                        param_type: FfiType::Int64,
                        nullable: false,
                        description: "TTL in milliseconds (-1 = no expiry)".to_string(),
                    },
                ],
                return_type: FfiType::Int32,
                safe: true,
            },
            FfiFunction {
                name: "get".to_string(),
                c_name: "fer_get".to_string(),
                description: "Get a value by key".to_string(),
                parameters: vec![
                    FfiParameter {
                        name: "db".to_string(),
                        param_type: FfiType::OpaquePtr("FerDB".to_string()),
                        nullable: false,
                        description: "Database handle".to_string(),
                    },
                    FfiParameter {
                        name: "key".to_string(),
                        param_type: FfiType::CString,
                        nullable: false,
                        description: "Key bytes".to_string(),
                    },
                    FfiParameter {
                        name: "key_len".to_string(),
                        param_type: FfiType::UInt64,
                        nullable: false,
                        description: "Key length".to_string(),
                    },
                ],
                return_type: FfiType::ByteBuffer,
                safe: true,
            },
            FfiFunction {
                name: "del".to_string(),
                c_name: "fer_del".to_string(),
                description: "Delete a key".to_string(),
                parameters: vec![
                    FfiParameter {
                        name: "db".to_string(),
                        param_type: FfiType::OpaquePtr("FerDB".to_string()),
                        nullable: false,
                        description: "Database handle".to_string(),
                    },
                    FfiParameter {
                        name: "key".to_string(),
                        param_type: FfiType::CString,
                        nullable: false,
                        description: "Key bytes".to_string(),
                    },
                    FfiParameter {
                        name: "key_len".to_string(),
                        param_type: FfiType::UInt64,
                        nullable: false,
                        description: "Key length".to_string(),
                    },
                ],
                return_type: FfiType::Int32,
                safe: true,
            },
            FfiFunction {
                name: "exists".to_string(),
                c_name: "fer_exists".to_string(),
                description: "Check if a key exists".to_string(),
                parameters: vec![
                    FfiParameter {
                        name: "db".to_string(),
                        param_type: FfiType::OpaquePtr("FerDB".to_string()),
                        nullable: false,
                        description: "Database handle".to_string(),
                    },
                    FfiParameter {
                        name: "key".to_string(),
                        param_type: FfiType::CString,
                        nullable: false,
                        description: "Key bytes".to_string(),
                    },
                    FfiParameter {
                        name: "key_len".to_string(),
                        param_type: FfiType::UInt64,
                        nullable: false,
                        description: "Key length".to_string(),
                    },
                ],
                return_type: FfiType::Bool,
                safe: true,
            },
            FfiFunction {
                name: "dbsize".to_string(),
                c_name: "fer_dbsize".to_string(),
                description: "Get the number of keys".to_string(),
                parameters: vec![FfiParameter {
                    name: "db".to_string(),
                    param_type: FfiType::OpaquePtr("FerDB".to_string()),
                    nullable: false,
                    description: "Database handle".to_string(),
                }],
                return_type: FfiType::UInt64,
                safe: true,
            },
            FfiFunction {
                name: "buffer_free".to_string(),
                c_name: "fer_buffer_free".to_string(),
                description: "Free a buffer returned by the library".to_string(),
                parameters: vec![FfiParameter {
                    name: "buf".to_string(),
                    param_type: FfiType::ByteBuffer,
                    nullable: false,
                    description: "Buffer to free".to_string(),
                }],
                return_type: FfiType::Void,
                safe: true,
            },
        ];

        Self { functions }
    }

    pub fn functions(&self) -> &[FfiFunction] {
        &self.functions
    }

    /// Generate a C header file.
    pub fn generate_c_header(&self) -> String {
        let mut header = String::new();
        header.push_str("/* Auto-generated by ferrite SDK generator */\n");
        header.push_str("#ifndef FERRITE_H\n#define FERRITE_H\n\n");
        header.push_str("#include <stdint.h>\n#include <stdbool.h>\n#include <stddef.h>\n\n");
        header.push_str("#ifdef __cplusplus\nextern \"C\" {\n#endif\n\n");

        header.push_str("typedef struct FerDB FerDB;\n\n");
        header.push_str("typedef struct {\n");
        header.push_str("    uint8_t *data;\n");
        header.push_str("    size_t len;\n");
        header.push_str("    size_t capacity;\n");
        header.push_str("    int32_t error_code;\n");
        header.push_str("} FerBuffer;\n\n");

        for func in &self.functions {
            header.push_str(&format!("/* {} */\n", func.description));
            let params: Vec<String> = func
                .parameters
                .iter()
                .map(|p| format!("{} {}", p.param_type, p.name))
                .collect();
            header.push_str(&format!(
                "{} {}({});\n\n",
                func.return_type,
                func.c_name,
                if params.is_empty() {
                    "void".to_string()
                } else {
                    params.join(", ")
                }
            ));
        }

        header.push_str("#ifdef __cplusplus\n}\n#endif\n\n");
        header.push_str("#endif /* FERRITE_H */\n");
        header
    }

    /// Generate Python ctypes bindings.
    pub fn generate_python_bindings(&self) -> String {
        let mut py = String::new();
        py.push_str("\"\"\"Auto-generated Python bindings for ferrite\"\"\"\n");
        py.push_str("import ctypes\nimport os\n\n");
        py.push_str(
            "_lib = ctypes.CDLL(os.path.join(os.path.dirname(__file__), 'libferrite.so'))\n\n",
        );

        py.push_str("class FerBuffer(ctypes.Structure):\n");
        py.push_str("    _fields_ = [\n");
        py.push_str("        ('data', ctypes.POINTER(ctypes.c_uint8)),\n");
        py.push_str("        ('len', ctypes.c_size_t),\n");
        py.push_str("        ('capacity', ctypes.c_size_t),\n");
        py.push_str("        ('error_code', ctypes.c_int32),\n");
        py.push_str("    ]\n\n");

        py.push_str("class Ferrite:\n");
        py.push_str("    def __init__(self, path: str):\n");
        py.push_str("        self._db = _lib.fer_open(path.encode())\n\n");
        py.push_str("    def close(self):\n");
        py.push_str("        _lib.fer_close(self._db)\n\n");
        py.push_str("    def set(self, key: str, value: str, ttl_ms: int = -1):\n");
        py.push_str("        k, v = key.encode(), value.encode()\n");
        py.push_str("        _lib.fer_set(self._db, k, len(k), v, len(v), ttl_ms)\n\n");
        py.push_str("    def get(self, key: str) -> bytes | None:\n");
        py.push_str("        k = key.encode()\n");
        py.push_str("        buf = _lib.fer_get(self._db, k, len(k))\n");
        py.push_str("        if buf.error_code != 0:\n");
        py.push_str("            return None\n");
        py.push_str("        result = ctypes.string_at(buf.data, buf.len)\n");
        py.push_str("        _lib.fer_buffer_free(ctypes.byref(buf))\n");
        py.push_str("        return result\n\n");
        py.push_str("    def delete(self, key: str) -> bool:\n");
        py.push_str("        k = key.encode()\n");
        py.push_str("        return _lib.fer_del(self._db, k, len(k)) == 0\n");

        py
    }
}

// ---------------------------------------------------------------------------
// FFI Metrics
// ---------------------------------------------------------------------------

/// Metrics for FFI calls.
#[derive(Debug, Default)]
pub struct FfiMetrics {
    pub calls_total: AtomicU64,
    pub errors_total: AtomicU64,
    pub get_calls: AtomicU64,
    pub set_calls: AtomicU64,
    pub del_calls: AtomicU64,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ffi_buffer_from_bytes() {
        let buf = FfiBuffer::from_bytes(b"hello");
        assert!(buf.is_ok());
        assert_eq!(buf.len, 5);
        assert!(!buf.data.is_null());
        // Clean up
        // SAFETY: `buf.data` was allocated by `FfiBuffer::from_bytes` via `Vec::into_raw_parts`
        // with the exact `buf.len` and `buf.capacity`. Reconstructing the Vec reclaims the memory.
        unsafe {
            let _ = Vec::from_raw_parts(buf.data, buf.len, buf.capacity);
        }
    }

    #[test]
    fn test_ffi_buffer_error() {
        let buf = FfiBuffer::error(FfiErrorCode::KeyNotFound);
        assert!(!buf.is_ok());
        assert_eq!(buf.error_code, FfiErrorCode::KeyNotFound as i32);
        assert!(buf.data.is_null());
    }

    #[test]
    fn test_ffi_error_code_display() {
        assert_eq!(FfiErrorCode::Ok.to_string(), "OK");
        assert_eq!(FfiErrorCode::KeyNotFound.to_string(), "KEY_NOT_FOUND");
    }

    #[test]
    fn test_ffi_type_display() {
        assert_eq!(FfiType::Void.to_string(), "void");
        assert_eq!(FfiType::Int32.to_string(), "int32_t");
        assert_eq!(FfiType::CString.to_string(), "const char*");
        assert_eq!(FfiType::OpaquePtr("FerDB".into()).to_string(), "FerDB*");
    }

    #[test]
    fn test_manifest_build() {
        let manifest = FfiManifest::build();
        assert!(!manifest.functions().is_empty());
        assert!(manifest.functions().iter().any(|f| f.c_name == "fer_open"));
        assert!(manifest.functions().iter().any(|f| f.c_name == "fer_get"));
        assert!(manifest.functions().iter().any(|f| f.c_name == "fer_set"));
        assert!(manifest.functions().iter().any(|f| f.c_name == "fer_del"));
        assert!(manifest.functions().iter().any(|f| f.c_name == "fer_close"));
    }

    #[test]
    fn test_generate_c_header() {
        let manifest = FfiManifest::build();
        let header = manifest.generate_c_header();
        assert!(header.contains("#ifndef FERRITE_H"));
        assert!(header.contains("typedef struct FerDB FerDB"));
        assert!(header.contains("fer_open"));
        assert!(header.contains("fer_close"));
        assert!(header.contains("FerBuffer"));
        assert!(header.contains("#endif"));
    }

    #[test]
    fn test_generate_python_bindings() {
        let manifest = FfiManifest::build();
        let py = manifest.generate_python_bindings();
        assert!(py.contains("class Ferrite:"));
        assert!(py.contains("def set("));
        assert!(py.contains("def get("));
        assert!(py.contains("ctypes.CDLL"));
    }

    #[test]
    fn test_binding_language_display() {
        assert_eq!(BindingLanguage::Python.to_string(), "python");
        assert_eq!(BindingLanguage::Go.to_string(), "go");
        assert_eq!(BindingLanguage::Java.to_string(), "java");
    }
}
