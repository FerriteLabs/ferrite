//! Host function bindings (WASM -> Ferrite bridge)
//!
//! This module registers host functions into a `wasmtime::Linker` so that
//! WASM modules can call back into Ferrite to read/write keys, log messages,
//! and query the current time.
//!
//! All host functions are registered under the `"ferrite"` module namespace,
//! so a WASM module imports them as e.g. `(import "ferrite" "get" ...)`.
//!
//! ## Memory Model
//!
//! For functions that exchange variable-length data (strings, byte arrays),
//! we use a shared `HostMemory` buffer inside `HostState`. The WASM module
//! writes key/value data into its own linear memory, then passes pointers
//! and lengths to the host functions. Results are written into a result
//! buffer that the WASM module can read back.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use wasmtime::Linker;

use ferrite_plugins::wasm::StoreBackend;
use ferrite_plugins::wasm::{FunctionPermissions, WasmError};

/// State stored in each wasmtime `Store<HostState>`.
///
/// Each execution gets its own `HostState` with the appropriate
/// database index, permissions, and optional store backend.
pub struct HostState {
    /// Current database index (0-15).
    pub db: u8,
    /// Permissions for this execution.
    pub permissions: FunctionPermissions,
    /// Memory limit in bytes.
    pub memory_limit: usize,
    /// Store backend for data operations.
    pub store_backend: Option<Arc<dyn StoreBackend>>,
    /// Log buffer for messages emitted by the WASM module.
    pub log_buffer: Vec<String>,
    /// Error from the last host function call.
    pub last_error: Option<String>,
    /// Number of host function calls made.
    pub host_call_count: u64,
}

impl HostState {
    /// Create a new host state without a store backend.
    pub fn new(db: u8, permissions: FunctionPermissions, memory_limit: usize) -> Self {
        Self {
            db,
            permissions,
            memory_limit,
            store_backend: None,
            log_buffer: Vec::new(),
            last_error: None,
            host_call_count: 0,
        }
    }

    /// Create a new host state with a store backend for real data access.
    pub fn with_store(
        db: u8,
        permissions: FunctionPermissions,
        memory_limit: usize,
        store: Arc<dyn StoreBackend>,
    ) -> Self {
        Self {
            db,
            permissions,
            memory_limit,
            store_backend: Some(store),
            log_buffer: Vec::new(),
            last_error: None,
            host_call_count: 0,
        }
    }
}

/// Register all Ferrite host functions into a wasmtime Linker.
///
/// The functions are registered under the `"ferrite"` namespace so WASM
/// modules import them as `(import "ferrite" "log" ...)`.
///
/// Functions that need to exchange variable-length data use the WASM
/// module's linear memory (via `Memory` export named `"memory"`).
pub fn register_host_functions(linker: &mut Linker<HostState>) -> Result<(), WasmError> {
    // ── ferrite_log(level: i32, ptr: i32, len: i32) ──────────────────
    linker
        .func_wrap(
            "ferrite",
            "log",
            |mut caller: wasmtime::Caller<'_, HostState>, level: i32, ptr: i32, len: i32| {
                caller.data_mut().host_call_count += 1;

                let memory = match caller.get_export("memory") {
                    Some(wasmtime::Extern::Memory(mem)) => mem,
                    _ => return,
                };

                let data = memory.data(&caller);
                let start = ptr as usize;
                let end = start + len as usize;
                if end > data.len() {
                    return;
                }

                let msg = String::from_utf8_lossy(&data[start..end]).to_string();

                match level {
                    0 => tracing::trace!(target: "wasm_udf", "{}", msg),
                    1 => tracing::debug!(target: "wasm_udf", "{}", msg),
                    2 => tracing::info!(target: "wasm_udf", "{}", msg),
                    3 => tracing::warn!(target: "wasm_udf", "{}", msg),
                    _ => tracing::error!(target: "wasm_udf", "{}", msg),
                }

                caller.data_mut().log_buffer.push(msg);
            },
        )
        .map_err(|e| WasmError::Internal(format!("failed to register ferrite.log: {}", e)))?;

    // ── ferrite_time() -> i64 ────────────────────────────────────────
    linker
        .func_wrap(
            "ferrite",
            "time",
            |mut caller: wasmtime::Caller<'_, HostState>| -> i64 {
                caller.data_mut().host_call_count += 1;
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0)
            },
        )
        .map_err(|e| WasmError::Internal(format!("failed to register ferrite.time: {}", e)))?;

    // ── ferrite_get(key_ptr: i32, key_len: i32, buf_ptr: i32, buf_cap: i32) -> i32
    //    Returns: bytes written (>= 0) on success, -1 if not found, -2 on error
    linker
        .func_wrap(
            "ferrite",
            "get",
            |mut caller: wasmtime::Caller<'_, HostState>,
             key_ptr: i32,
             key_len: i32,
             buf_ptr: i32,
             buf_cap: i32|
             -> i32 {
                caller.data_mut().host_call_count += 1;
                let db = caller.data().db;
                let store = caller.data().store_backend.clone();

                let memory = match caller.get_export("memory") {
                    Some(wasmtime::Extern::Memory(mem)) => mem,
                    _ => return -2,
                };

                // Read key from WASM memory
                let data = memory.data(&caller);
                let ks = key_ptr as usize;
                let ke = ks + key_len as usize;
                if ke > data.len() {
                    return -2;
                }
                let key = data[ks..ke].to_vec();

                // Check permission
                let key_str = String::from_utf8_lossy(&key);
                if !caller.data().permissions.is_key_allowed(&key_str) {
                    caller.data_mut().last_error =
                        Some(format!("permission denied for key: {}", key_str));
                    return -2;
                }

                match store {
                    Some(backend) => match backend.get(db, &key) {
                        Ok(Some(value)) => {
                            let write_len = value.len().min(buf_cap as usize);
                            let data_mut = memory.data_mut(&mut caller);
                            let bs = buf_ptr as usize;
                            let be = bs + write_len;
                            if be > data_mut.len() {
                                return -2;
                            }
                            data_mut[bs..be].copy_from_slice(&value[..write_len]);
                            write_len as i32
                        }
                        Ok(None) => -1,
                        Err(e) => {
                            caller.data_mut().last_error = Some(e);
                            -2
                        }
                    },
                    None => -1, // No store backend
                }
            },
        )
        .map_err(|e| WasmError::Internal(format!("failed to register ferrite.get: {}", e)))?;

    // ── ferrite_set(key_ptr, key_len, val_ptr, val_len) -> i32
    //    Returns: 0 on success, -1 on error
    linker
        .func_wrap(
            "ferrite",
            "set",
            |mut caller: wasmtime::Caller<'_, HostState>,
             key_ptr: i32,
             key_len: i32,
             val_ptr: i32,
             val_len: i32|
             -> i32 {
                caller.data_mut().host_call_count += 1;
                let db = caller.data().db;
                let store = caller.data().store_backend.clone();

                if !caller.data().permissions.allow_write {
                    caller.data_mut().last_error =
                        Some("permission denied: write not allowed".to_string());
                    return -1;
                }

                let memory = match caller.get_export("memory") {
                    Some(wasmtime::Extern::Memory(mem)) => mem,
                    _ => return -1,
                };

                let data = memory.data(&caller);
                let ks = key_ptr as usize;
                let ke = ks + key_len as usize;
                let vs = val_ptr as usize;
                let ve = vs + val_len as usize;
                if ke > data.len() || ve > data.len() {
                    return -1;
                }
                let key = data[ks..ke].to_vec();
                let value = data[vs..ve].to_vec();

                let key_str = String::from_utf8_lossy(&key);
                if !caller.data().permissions.is_key_allowed(&key_str) {
                    caller.data_mut().last_error =
                        Some(format!("permission denied for key: {}", key_str));
                    return -1;
                }

                match store {
                    Some(backend) => match backend.set(db, &key, &value, None) {
                        Ok(()) => 0,
                        Err(e) => {
                            caller.data_mut().last_error = Some(e);
                            -1
                        }
                    },
                    None => 0, // No store backend, simulate success
                }
            },
        )
        .map_err(|e| WasmError::Internal(format!("failed to register ferrite.set: {}", e)))?;

    // ── ferrite_del(key_ptr, key_len) -> i32
    //    Returns: 1 if deleted, 0 if not found, -1 on error
    linker
        .func_wrap(
            "ferrite",
            "del",
            |mut caller: wasmtime::Caller<'_, HostState>, key_ptr: i32, key_len: i32| -> i32 {
                caller.data_mut().host_call_count += 1;
                let db = caller.data().db;
                let store = caller.data().store_backend.clone();

                if !caller.data().permissions.allow_write {
                    caller.data_mut().last_error =
                        Some("permission denied: write not allowed".to_string());
                    return -1;
                }

                let memory = match caller.get_export("memory") {
                    Some(wasmtime::Extern::Memory(mem)) => mem,
                    _ => return -1,
                };

                let data = memory.data(&caller);
                let ks = key_ptr as usize;
                let ke = ks + key_len as usize;
                if ke > data.len() {
                    return -1;
                }
                let key = data[ks..ke].to_vec();

                match store {
                    Some(backend) => match backend.del(db, &key) {
                        Ok(true) => 1,
                        Ok(false) => 0,
                        Err(e) => {
                            caller.data_mut().last_error = Some(e);
                            -1
                        }
                    },
                    None => 0,
                }
            },
        )
        .map_err(|e| WasmError::Internal(format!("failed to register ferrite.del: {}", e)))?;

    // ── ferrite_exists(key_ptr, key_len) -> i32
    //    Returns: 1 if exists, 0 if not
    linker
        .func_wrap(
            "ferrite",
            "exists",
            |mut caller: wasmtime::Caller<'_, HostState>, key_ptr: i32, key_len: i32| -> i32 {
                caller.data_mut().host_call_count += 1;
                let db = caller.data().db;
                let store = caller.data().store_backend.clone();

                let memory = match caller.get_export("memory") {
                    Some(wasmtime::Extern::Memory(mem)) => mem,
                    _ => return -1,
                };

                let data = memory.data(&caller);
                let ks = key_ptr as usize;
                let ke = ks + key_len as usize;
                if ke > data.len() {
                    return -1;
                }
                let key = data[ks..ke].to_vec();

                match store {
                    Some(backend) => backend.exists(db, &key).unwrap_or(-1),
                    None => 0,
                }
            },
        )
        .map_err(|e| WasmError::Internal(format!("failed to register ferrite.exists: {}", e)))?;

    // ── ferrite_incr(key_ptr, key_len) -> i64
    //    Returns: new value on success, i64::MIN on error
    linker
        .func_wrap(
            "ferrite",
            "incr",
            |mut caller: wasmtime::Caller<'_, HostState>, key_ptr: i32, key_len: i32| -> i64 {
                caller.data_mut().host_call_count += 1;
                let db = caller.data().db;
                let store = caller.data().store_backend.clone();

                if !caller.data().permissions.allow_write {
                    return i64::MIN;
                }

                let memory = match caller.get_export("memory") {
                    Some(wasmtime::Extern::Memory(mem)) => mem,
                    _ => return i64::MIN,
                };

                let data = memory.data(&caller);
                let ks = key_ptr as usize;
                let ke = ks + key_len as usize;
                if ke > data.len() {
                    return i64::MIN;
                }
                let key = data[ks..ke].to_vec();

                match store {
                    Some(backend) => match backend.incr(db, &key) {
                        Ok(n) => n,
                        Err(_) => i64::MIN,
                    },
                    None => 1,
                }
            },
        )
        .map_err(|e| WasmError::Internal(format!("failed to register ferrite.incr: {}", e)))?;

    // ── ferrite_hget(key_ptr, key_len, field_ptr, field_len, buf_ptr, buf_cap) -> i32
    linker
        .func_wrap(
            "ferrite",
            "hget",
            |mut caller: wasmtime::Caller<'_, HostState>,
             key_ptr: i32,
             key_len: i32,
             field_ptr: i32,
             field_len: i32,
             buf_ptr: i32,
             buf_cap: i32|
             -> i32 {
                caller.data_mut().host_call_count += 1;
                let db = caller.data().db;
                let store = caller.data().store_backend.clone();

                let memory = match caller.get_export("memory") {
                    Some(wasmtime::Extern::Memory(mem)) => mem,
                    _ => return -2,
                };

                let data = memory.data(&caller);
                let ks = key_ptr as usize;
                let ke = ks + key_len as usize;
                let fs = field_ptr as usize;
                let fe = fs + field_len as usize;
                if ke > data.len() || fe > data.len() {
                    return -2;
                }
                let key = data[ks..ke].to_vec();
                let field = data[fs..fe].to_vec();

                match store {
                    Some(backend) => match backend.hget(db, &key, &field) {
                        Ok(Some(value)) => {
                            let write_len = value.len().min(buf_cap as usize);
                            let data_mut = memory.data_mut(&mut caller);
                            let bs = buf_ptr as usize;
                            let be = bs + write_len;
                            if be > data_mut.len() {
                                return -2;
                            }
                            data_mut[bs..be].copy_from_slice(&value[..write_len]);
                            write_len as i32
                        }
                        Ok(None) => -1,
                        Err(e) => {
                            caller.data_mut().last_error = Some(e);
                            -2
                        }
                    },
                    None => -1,
                }
            },
        )
        .map_err(|e| WasmError::Internal(format!("failed to register ferrite.hget: {}", e)))?;

    // ── ferrite_hset(key_ptr, key_len, field_ptr, field_len, val_ptr, val_len) -> i32
    linker
        .func_wrap(
            "ferrite",
            "hset",
            |mut caller: wasmtime::Caller<'_, HostState>,
             key_ptr: i32,
             key_len: i32,
             field_ptr: i32,
             field_len: i32,
             val_ptr: i32,
             val_len: i32|
             -> i32 {
                caller.data_mut().host_call_count += 1;
                let db = caller.data().db;
                let store = caller.data().store_backend.clone();

                if !caller.data().permissions.allow_write {
                    return -1;
                }

                let memory = match caller.get_export("memory") {
                    Some(wasmtime::Extern::Memory(mem)) => mem,
                    _ => return -1,
                };

                let data = memory.data(&caller);
                let ks = key_ptr as usize;
                let ke = ks + key_len as usize;
                let fs = field_ptr as usize;
                let fe = fs + field_len as usize;
                let vs = val_ptr as usize;
                let ve = vs + val_len as usize;
                if ke > data.len() || fe > data.len() || ve > data.len() {
                    return -1;
                }
                let key = data[ks..ke].to_vec();
                let field = data[fs..fe].to_vec();
                let value = data[vs..ve].to_vec();

                match store {
                    Some(backend) => match backend.hset(db, &key, &field, &value) {
                        Ok(n) => n,
                        Err(e) => {
                            caller.data_mut().last_error = Some(e);
                            -1
                        }
                    },
                    None => 1,
                }
            },
        )
        .map_err(|e| WasmError::Internal(format!("failed to register ferrite.hset: {}", e)))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasmtime::Engine;

    #[test]
    fn test_host_state_creation() {
        let state = HostState::new(0, FunctionPermissions::default(), 16 * 1024 * 1024);
        assert_eq!(state.db, 0);
        assert!(state.store_backend.is_none());
        assert!(state.log_buffer.is_empty());
        assert_eq!(state.host_call_count, 0);
    }

    #[test]
    fn test_register_host_functions_succeeds() {
        let mut config = wasmtime::Config::new();
        config.consume_fuel(true);
        let engine = Engine::new(&config).expect("engine creation failed");
        let mut linker = Linker::new(&engine);
        let result = register_host_functions(&mut linker);
        assert!(result.is_ok());
    }
}
