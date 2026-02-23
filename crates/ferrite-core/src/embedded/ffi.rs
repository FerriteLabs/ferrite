//! Foreign Function Interface (FFI) for Cross-Platform Integration
//!
//! C-compatible API for embedding Ferrite in applications written in
//! other languages (C, C++, Python, Go, etc.) and WebAssembly.
//!
//! # C API
//!
//! ```c
//! #include "ferrite.h"
//!
//! // Open database
//! FerriteDb* db = ferrite_open("./myapp.db");
//!
//! // Set a key
//! ferrite_set(db, "key", 3, "value", 5, 0);
//!
//! // Get a key
//! size_t len;
//! const char* value = ferrite_get(db, "key", 3, &len);
//!
//! // Free and close
//! ferrite_free(value);
//! ferrite_close(db);
//! ```
//!
//! # Safety
//!
//! All FFI functions follow these safety rules:
//! - Null pointers are checked before dereferencing
//! - Lengths are bounds-checked
//! - Errors are returned as status codes
//! - Memory ownership is explicit

use super::edge::{EdgeConfig, EdgeError, EdgeStore};
use std::os::raw::{c_char, c_int};
use std::ptr;
use std::slice;
use std::sync::Arc;

/// Opaque handle to a Ferrite database
pub struct FerriteDb {
    store: Arc<EdgeStore>,
}

/// Error codes returned by FFI functions
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FerriteError {
    /// Success (no error)
    Ok = 0,
    /// Null pointer passed
    NullPointer = 1,
    /// Invalid UTF-8 string
    InvalidUtf8 = 2,
    /// Key not found
    NotFound = 3,
    /// Memory limit exceeded
    MemoryLimit = 4,
    /// Key limit exceeded
    KeyLimit = 5,
    /// Value too large
    ValueTooLarge = 6,
    /// Key too large
    KeyTooLarge = 7,
    /// Internal error
    Internal = 8,
    /// Invalid argument
    InvalidArgument = 9,
}

impl From<EdgeError> for FerriteError {
    fn from(err: EdgeError) -> Self {
        match err {
            EdgeError::MemoryLimitExceeded { .. } => FerriteError::MemoryLimit,
            EdgeError::KeyLimitExceeded { .. } => FerriteError::KeyLimit,
            EdgeError::ValueTooLarge { .. } => FerriteError::ValueTooLarge,
            EdgeError::KeyTooLarge { .. } => FerriteError::KeyTooLarge,
            EdgeError::KeyNotFound(_) => FerriteError::NotFound,
            _ => FerriteError::Internal,
        }
    }
}

/// Result type for FFI operations with value
#[repr(C)]
pub struct FerriteResult {
    /// Error code (0 = success)
    pub error: FerriteError,
    /// Value data (owned, must be freed with ferrite_free)
    pub data: *mut u8,
    /// Value length
    pub len: usize,
}

impl Default for FerriteResult {
    fn default() -> Self {
        Self {
            error: FerriteError::Ok,
            data: ptr::null_mut(),
            len: 0,
        }
    }
}

/// Statistics returned by ferrite_stats
#[repr(C)]
pub struct FerriteStats {
    /// Number of keys
    pub keys: u64,
    /// Memory used in bytes
    pub memory_used: u64,
    /// Memory limit in bytes
    pub memory_limit: u64,
    /// Total get operations
    pub gets: u64,
    /// Total set operations
    pub sets: u64,
    /// Cache hits
    pub hits: u64,
    /// Cache misses
    pub misses: u64,
    /// Hit rate (0.0 to 1.0)
    pub hit_rate: f64,
}

/// Configuration for opening a database
#[repr(C)]
pub struct FerriteConfig {
    /// Maximum memory in bytes (0 = default 16MB)
    pub max_memory: usize,
    /// Maximum keys (0 = default 100K)
    pub max_keys: usize,
    /// Enable compression (1 = true, 0 = false)
    pub compression: c_int,
    /// Compression level (1-9)
    pub compression_level: u8,
    /// Maximum value size (0 = default 1MB)
    pub max_value_size: usize,
    /// Maximum key size (0 = default 1KB)
    pub max_key_size: usize,
}

impl Default for FerriteConfig {
    fn default() -> Self {
        Self {
            max_memory: 0,
            max_keys: 0,
            compression: 0,
            compression_level: 1,
            max_value_size: 0,
            max_key_size: 0,
        }
    }
}

impl From<FerriteConfig> for EdgeConfig {
    fn from(config: FerriteConfig) -> Self {
        let default = EdgeConfig::default();
        EdgeConfig {
            max_memory: if config.max_memory > 0 {
                config.max_memory
            } else {
                default.max_memory
            },
            max_keys: if config.max_keys > 0 {
                config.max_keys
            } else {
                default.max_keys
            },
            compression: config.compression != 0,
            compression_level: if config.compression_level > 0 {
                config.compression_level
            } else {
                default.compression_level
            },
            max_value_size: if config.max_value_size > 0 {
                config.max_value_size
            } else {
                default.max_value_size
            },
            max_key_size: if config.max_key_size > 0 {
                config.max_key_size
            } else {
                default.max_key_size
            },
            ..default
        }
    }
}

// === Database Lifecycle ===

/// Open a new Ferrite database with default configuration.
///
/// # Safety
///
/// Returns a pointer to a FerriteDb that must be freed with `ferrite_close`.
/// Returns NULL on error.
// SAFETY: No pointer arguments to validate. Returns a heap-allocated FerriteDb via
// Box::into_raw. Caller must free with `ferrite_close` to avoid a memory leak.
#[no_mangle]
pub unsafe extern "C" fn ferrite_open() -> *mut FerriteDb {
    ferrite_open_with_config(FerriteConfig::default())
}

/// Open a Ferrite database with custom configuration.
///
/// # Safety
///
/// Returns a pointer to a FerriteDb that must be freed with `ferrite_close`.
/// Returns NULL on error.
// SAFETY: `config` is passed by value (#[repr(C)]) so no pointer validity concerns.
// Returns a heap-allocated FerriteDb via Box::into_raw. Caller must free with `ferrite_close`.
#[no_mangle]
pub unsafe extern "C" fn ferrite_open_with_config(config: FerriteConfig) -> *mut FerriteDb {
    let edge_config: EdgeConfig = config.into();
    let store = Arc::new(EdgeStore::new(edge_config));

    let db = Box::new(FerriteDb { store });
    Box::into_raw(db)
}

/// Close and free a Ferrite database.
///
/// # Safety
///
/// The db pointer must be valid and not used after this call.
// SAFETY: `db` must be a pointer returned by `ferrite_open`/`ferrite_open_with_config` and
// not previously freed. Null is checked. `Box::from_raw` reclaims ownership and drops.
// Using `db` after this call is use-after-free.
#[no_mangle]
pub unsafe extern "C" fn ferrite_close(db: *mut FerriteDb) {
    if !db.is_null() {
        drop(Box::from_raw(db));
    }
}

// === Key-Value Operations ===

/// Set a key-value pair.
///
/// # Arguments
///
/// * `db` - Database handle
/// * `key` - Key data
/// * `key_len` - Key length
/// * `value` - Value data
/// * `value_len` - Value length
/// * `ttl_secs` - TTL in seconds (0 = no expiry)
///
/// # Returns
///
/// FerriteError code (0 = success)
///
/// # Safety
///
/// All pointers must be valid for the specified lengths.
// SAFETY: `db` must point to a live FerriteDb. `key` must be valid for `key_len` bytes.
// `value` must be valid for `value_len` bytes (or may be null if `value_len` is 0).
// Null checks are performed before dereferencing. `slice::from_raw_parts` relies on caller
// providing correct lengths; incorrect lengths would read out-of-bounds memory.
#[no_mangle]
pub unsafe extern "C" fn ferrite_set(
    db: *mut FerriteDb,
    key: *const c_char,
    key_len: usize,
    value: *const u8,
    value_len: usize,
    ttl_secs: u64,
) -> FerriteError {
    if db.is_null() || key.is_null() || (value_len > 0 && value.is_null()) {
        return FerriteError::NullPointer;
    }

    let db = &*db;

    // Convert key to string
    let key_bytes = slice::from_raw_parts(key as *const u8, key_len);
    let key_str = match std::str::from_utf8(key_bytes) {
        Ok(s) => s,
        Err(_) => return FerriteError::InvalidUtf8,
    };

    // Get value slice
    let value_bytes = if value_len > 0 {
        slice::from_raw_parts(value, value_len)
    } else {
        &[]
    };

    // Set with optional TTL
    let ttl = if ttl_secs > 0 { Some(ttl_secs) } else { None };

    match db.store.set(key_str, value_bytes, ttl) {
        Ok(()) => FerriteError::Ok,
        Err(e) => e.into(),
    }
}

/// Get a value by key.
///
/// # Arguments
///
/// * `db` - Database handle
/// * `key` - Key data
/// * `key_len` - Key length
///
/// # Returns
///
/// FerriteResult with data and length. Data must be freed with `ferrite_free`.
///
/// # Safety
///
/// All pointers must be valid. Result data must be freed by caller.
// SAFETY: `db` must point to a live FerriteDb. `key` must be valid for `key_len` bytes.
// Null checks guard all dereferences. Returned data pointer is heap-allocated via
// Box::into_raw and must be freed with `ferrite_free` to avoid a leak.
#[no_mangle]
pub unsafe extern "C" fn ferrite_get(
    db: *mut FerriteDb,
    key: *const c_char,
    key_len: usize,
) -> FerriteResult {
    if db.is_null() || key.is_null() {
        return FerriteResult {
            error: FerriteError::NullPointer,
            ..Default::default()
        };
    }

    let db = &*db;

    // Convert key to string
    let key_bytes = slice::from_raw_parts(key as *const u8, key_len);
    let key_str = match std::str::from_utf8(key_bytes) {
        Ok(s) => s,
        Err(_) => {
            return FerriteResult {
                error: FerriteError::InvalidUtf8,
                ..Default::default()
            }
        }
    };

    match db.store.get(key_str) {
        Ok(Some(value)) => {
            let len = value.len();
            let data = Box::into_raw(value.to_vec().into_boxed_slice()) as *mut u8;
            FerriteResult {
                error: FerriteError::Ok,
                data,
                len,
            }
        }
        Ok(None) => FerriteResult {
            error: FerriteError::NotFound,
            ..Default::default()
        },
        Err(e) => FerriteResult {
            error: e.into(),
            ..Default::default()
        },
    }
}

/// Delete a key.
///
/// # Returns
///
/// 1 if key was deleted, 0 if not found
///
/// # Safety
///
/// All pointers must be valid.
// SAFETY: `db` must point to a live FerriteDb. `key` must be valid for `key_len` bytes.
// Null checks are performed before any dereference. `slice::from_raw_parts` requires
// the caller to provide a correct `key_len`.
#[no_mangle]
pub unsafe extern "C" fn ferrite_delete(
    db: *mut FerriteDb,
    key: *const c_char,
    key_len: usize,
) -> c_int {
    if db.is_null() || key.is_null() {
        return -1;
    }

    let db = &*db;

    let key_bytes = slice::from_raw_parts(key as *const u8, key_len);
    let key_str = match std::str::from_utf8(key_bytes) {
        Ok(s) => s,
        Err(_) => return -1,
    };

    if db.store.delete(key_str) {
        1
    } else {
        0
    }
}

/// Check if a key exists.
///
/// # Returns
///
/// 1 if exists, 0 if not
///
/// # Safety
///
/// All pointers must be valid.
// SAFETY: `db` must point to a live FerriteDb. `key` must be valid for `key_len` bytes.
// Null checks are performed before any dereference. `slice::from_raw_parts` requires
// the caller to provide a correct `key_len`.
#[no_mangle]
pub unsafe extern "C" fn ferrite_exists(
    db: *mut FerriteDb,
    key: *const c_char,
    key_len: usize,
) -> c_int {
    if db.is_null() || key.is_null() {
        return -1;
    }

    let db = &*db;

    let key_bytes = slice::from_raw_parts(key as *const u8, key_len);
    let key_str = match std::str::from_utf8(key_bytes) {
        Ok(s) => s,
        Err(_) => return -1,
    };

    if db.store.exists(key_str) {
        1
    } else {
        0
    }
}

// === Utility Functions ===

/// Free memory returned by ferrite_get.
///
/// # Safety
///
/// ptr must be from a previous ferrite_get call or NULL.
// SAFETY: `ptr` must have been allocated by `ferrite_get` via `Box::into_raw` with the exact
// `len`. Null is checked. `Box::from_raw` reclaims the slice and drops it. Passing a pointer
// from a different allocator or with wrong `len` is undefined behavior.
#[no_mangle]
pub unsafe extern "C" fn ferrite_free(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
        // Reconstruct the boxed slice and drop it
        let slice = slice::from_raw_parts_mut(ptr, len);
        drop(Box::from_raw(slice as *mut [u8]));
    }
}

/// Get the number of keys in the database.
///
/// # Safety
///
/// db must be valid.
// SAFETY: `db` must point to a live FerriteDb from `ferrite_open`. Null is checked.
// The dereference `&*db` is safe because the pointer was created via `Box::into_raw`.
#[no_mangle]
pub unsafe extern "C" fn ferrite_len(db: *mut FerriteDb) -> usize {
    if db.is_null() {
        return 0;
    }

    let db = &*db;
    db.store.len()
}

/// Clear all keys from the database.
///
/// # Safety
///
/// db must be valid.
// SAFETY: `db` must point to a live FerriteDb from `ferrite_open`. Null is checked.
// The dereference `&*db` is safe because the pointer was created via `Box::into_raw`.
#[no_mangle]
pub unsafe extern "C" fn ferrite_clear(db: *mut FerriteDb) {
    if db.is_null() {
        return;
    }

    let db = &*db;
    db.store.clear();
}

/// Get database statistics.
///
/// # Safety
///
/// db must be valid.
// SAFETY: `db` must point to a live FerriteDb from `ferrite_open`. Null is checked.
// The dereference `&*db` is safe because the pointer was created via `Box::into_raw`.
#[no_mangle]
pub unsafe extern "C" fn ferrite_stats(db: *mut FerriteDb) -> FerriteStats {
    if db.is_null() {
        return FerriteStats {
            keys: 0,
            memory_used: 0,
            memory_limit: 0,
            gets: 0,
            sets: 0,
            hits: 0,
            misses: 0,
            hit_rate: 0.0,
        };
    }

    let db = &*db;
    let stats = db.store.stats();

    FerriteStats {
        keys: stats.keys as u64,
        memory_used: stats.memory_used as u64,
        memory_limit: stats.memory_limit as u64,
        gets: stats.gets,
        sets: stats.sets,
        hits: stats.hits,
        misses: stats.misses,
        hit_rate: stats.hit_rate,
    }
}

/// Get memory usage in bytes.
///
/// # Safety
///
/// db must be valid.
// SAFETY: `db` must point to a live FerriteDb from `ferrite_open`. Null is checked.
// The dereference `&*db` is safe because the pointer was created via `Box::into_raw`.
#[no_mangle]
pub unsafe extern "C" fn ferrite_memory_usage(db: *mut FerriteDb) -> usize {
    if db.is_null() {
        return 0;
    }

    let db = &*db;
    db.store.memory_usage()
}

/// Run cleanup of expired keys.
///
/// # Returns
///
/// Number of keys cleaned up.
///
/// # Safety
///
/// db must be valid.
// SAFETY: `db` must point to a live FerriteDb from `ferrite_open`. Null is checked.
// The dereference `&*db` is safe because the pointer was created via `Box::into_raw`.
#[no_mangle]
pub unsafe extern "C" fn ferrite_cleanup(db: *mut FerriteDb) -> usize {
    if db.is_null() {
        return 0;
    }

    let db = &*db;
    db.store.cleanup_expired()
}

// === Version Information ===

/// Get the library version string.
///
/// # Safety
///
/// Returns a static string that should not be modified or freed.
// SAFETY: Returns a pointer to a static byte string with a NUL terminator.
// The pointer is valid for the lifetime of the program and must not be freed.
#[no_mangle]
pub unsafe extern "C" fn ferrite_version() -> *const c_char {
    static VERSION: &[u8] = b"0.1.0\0";
    VERSION.as_ptr() as *const c_char
}

// === C Header Generation ===

/// Generate C header content
pub fn generate_c_header() -> String {
    r#"/* Ferrite - Embedded Key-Value Store
 * C API Header
 *
 * Usage:
 *   #include "ferrite.h"
 *
 *   FerriteDb* db = ferrite_open();
 *   ferrite_set(db, "key", 3, "value", 5, 0);
 *   FerriteResult result = ferrite_get(db, "key", 3);
 *   if (result.error == FERRITE_OK) {
 *       // Use result.data, result.len
 *       ferrite_free(result.data, result.len);
 *   }
 *   ferrite_close(db);
 */

#ifndef FERRITE_H
#define FERRITE_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque database handle */
typedef struct FerriteDb FerriteDb;

/* Error codes */
typedef enum {
    FERRITE_OK = 0,
    FERRITE_NULL_POINTER = 1,
    FERRITE_INVALID_UTF8 = 2,
    FERRITE_NOT_FOUND = 3,
    FERRITE_MEMORY_LIMIT = 4,
    FERRITE_KEY_LIMIT = 5,
    FERRITE_VALUE_TOO_LARGE = 6,
    FERRITE_KEY_TOO_LARGE = 7,
    FERRITE_INTERNAL = 8,
    FERRITE_INVALID_ARGUMENT = 9
} FerriteError;

/* Result type for get operations */
typedef struct {
    FerriteError error;
    uint8_t* data;
    size_t len;
} FerriteResult;

/* Statistics */
typedef struct {
    uint64_t keys;
    uint64_t memory_used;
    uint64_t memory_limit;
    uint64_t gets;
    uint64_t sets;
    uint64_t hits;
    uint64_t misses;
    double hit_rate;
} FerriteStats;

/* Configuration */
typedef struct {
    size_t max_memory;      /* 0 = default 16MB */
    size_t max_keys;        /* 0 = default 100K */
    int compression;        /* 0 = off, 1 = on */
    uint8_t compression_level; /* 1-9 */
    size_t max_value_size;  /* 0 = default 1MB */
    size_t max_key_size;    /* 0 = default 1KB */
} FerriteConfig;

/* Database lifecycle */
FerriteDb* ferrite_open(void);
FerriteDb* ferrite_open_with_config(FerriteConfig config);
void ferrite_close(FerriteDb* db);

/* Key-value operations */
FerriteError ferrite_set(
    FerriteDb* db,
    const char* key,
    size_t key_len,
    const uint8_t* value,
    size_t value_len,
    uint64_t ttl_secs
);

FerriteResult ferrite_get(
    FerriteDb* db,
    const char* key,
    size_t key_len
);

int ferrite_delete(
    FerriteDb* db,
    const char* key,
    size_t key_len
);

int ferrite_exists(
    FerriteDb* db,
    const char* key,
    size_t key_len
);

/* Utility functions */
void ferrite_free(uint8_t* ptr, size_t len);
size_t ferrite_len(FerriteDb* db);
void ferrite_clear(FerriteDb* db);
FerriteStats ferrite_stats(FerriteDb* db);
size_t ferrite_memory_usage(FerriteDb* db);
size_t ferrite_cleanup(FerriteDb* db);

/* Version info */
const char* ferrite_version(void);

#ifdef __cplusplus
}
#endif

#endif /* FERRITE_H */
"#
    .to_string()
}

// === WASM Support ===

/// WASM-compatible API (when compiled for wasm32 target)
#[cfg(target_arch = "wasm32")]
pub mod wasm {
    use super::*;
    use wasm_bindgen::prelude::*;

    #[wasm_bindgen]
    pub struct WasmDb {
        store: Arc<EdgeStore>,
    }

    #[wasm_bindgen]
    impl WasmDb {
        #[wasm_bindgen(constructor)]
        pub fn new() -> Self {
            Self {
                store: Arc::new(EdgeStore::with_defaults()),
            }
        }

        #[wasm_bindgen]
        pub fn set(&self, key: &str, value: &[u8]) -> bool {
            self.store.set(key, value, None).is_ok()
        }

        #[wasm_bindgen]
        pub fn set_with_ttl(&self, key: &str, value: &[u8], ttl_secs: u64) -> bool {
            self.store.set(key, value, Some(ttl_secs)).is_ok()
        }

        #[wasm_bindgen]
        pub fn get(&self, key: &str) -> Option<Vec<u8>> {
            self.store.get(key).ok().flatten().map(|b| b.to_vec())
        }

        #[wasm_bindgen]
        pub fn delete(&self, key: &str) -> bool {
            self.store.delete(key)
        }

        #[wasm_bindgen]
        pub fn exists(&self, key: &str) -> bool {
            self.store.exists(key)
        }

        #[wasm_bindgen]
        pub fn len(&self) -> usize {
            self.store.len()
        }

        #[wasm_bindgen]
        pub fn clear(&self) {
            self.store.clear()
        }

        #[wasm_bindgen]
        pub fn memory_usage(&self) -> usize {
            self.store.memory_usage()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;

    #[test]
    fn test_ffi_lifecycle() {
        // SAFETY: Calling FFI functions in a controlled test; db pointer is obtained from
        // ferrite_open and passed to ferrite_close exactly once.
        unsafe {
            let db = ferrite_open();
            assert!(!db.is_null());

            ferrite_close(db);
        }
    }

    #[test]
    fn test_ffi_set_get() {
        // SAFETY: FFI test — db pointer from ferrite_open is valid for the duration,
        // key/value pointers reference stack-local byte arrays with correct lengths.
        unsafe {
            let db = ferrite_open();

            let key = b"test_key";
            let value = b"test_value";

            let err = ferrite_set(
                db,
                key.as_ptr() as *const c_char,
                key.len(),
                value.as_ptr(),
                value.len(),
                0,
            );
            assert_eq!(err, FerriteError::Ok);

            let result = ferrite_get(db, key.as_ptr() as *const c_char, key.len());
            assert_eq!(result.error, FerriteError::Ok);
            assert_eq!(result.len, value.len());

            let retrieved = slice::from_raw_parts(result.data, result.len);
            assert_eq!(retrieved, value);

            ferrite_free(result.data, result.len);
            ferrite_close(db);
        }
    }

    #[test]
    fn test_ffi_delete() {
        // SAFETY: FFI test — db pointer from ferrite_open is valid for the duration,
        // key/value pointers reference stack-local byte arrays with correct lengths.
        unsafe {
            let db = ferrite_open();

            let key = b"key_to_delete";
            let value = b"value";

            ferrite_set(
                db,
                key.as_ptr() as *const c_char,
                key.len(),
                value.as_ptr(),
                value.len(),
                0,
            );

            assert_eq!(
                ferrite_exists(db, key.as_ptr() as *const c_char, key.len()),
                1
            );
            assert_eq!(
                ferrite_delete(db, key.as_ptr() as *const c_char, key.len()),
                1
            );
            assert_eq!(
                ferrite_exists(db, key.as_ptr() as *const c_char, key.len()),
                0
            );

            ferrite_close(db);
        }
    }

    #[test]
    fn test_ffi_stats() {
        // SAFETY: FFI test — db pointer from ferrite_open is valid for the duration,
        // key/value pointers reference stack-local byte arrays with correct lengths.
        unsafe {
            let db = ferrite_open();

            let key = b"key";
            let value = b"value";

            ferrite_set(
                db,
                key.as_ptr() as *const c_char,
                key.len(),
                value.as_ptr(),
                value.len(),
                0,
            );

            let stats = ferrite_stats(db);
            assert_eq!(stats.keys, 1);
            assert_eq!(stats.sets, 1);

            ferrite_close(db);
        }
    }

    #[test]
    fn test_ffi_version() {
        // SAFETY: ferrite_version returns a pointer to a static string literal
        // that is valid for the lifetime of the program.
        unsafe {
            let version = ferrite_version();
            assert!(!version.is_null());

            let version_str = CStr::from_ptr(version).to_str().unwrap();
            assert_eq!(version_str, "0.1.0");
        }
    }

    #[test]
    fn test_generate_c_header() {
        let header = generate_c_header();
        assert!(header.contains("FERRITE_H"));
        assert!(header.contains("ferrite_open"));
        assert!(header.contains("ferrite_set"));
        assert!(header.contains("ferrite_get"));
    }
}
