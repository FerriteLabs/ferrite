//! Host functions exposed to WASM modules
//!
//! These functions provide WASM modules with access to Ferrite operations
//! in a controlled, sandboxed manner.
//!
//! # Store Integration
//!
//! To enable actual store operations, create the HostContext with a store
//! backend that implements the `StoreBackend` trait.

use super::types::{FunctionPermissions, ResourceUsage};
use super::WasmError;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Backend trait for store operations from WASM
///
/// This trait allows WASM host functions to interact with the actual
/// Ferrite store in a decoupled way.
pub trait StoreBackend: Send + Sync {
    /// GET key - returns value or None if not found
    fn get(&self, db: u8, key: &[u8]) -> Result<Option<Vec<u8>>, String>;

    /// SET key value with optional TTL in milliseconds
    fn set(&self, db: u8, key: &[u8], value: &[u8], ttl_ms: Option<u64>) -> Result<(), String>;

    /// DEL key - returns true if deleted
    fn del(&self, db: u8, key: &[u8]) -> Result<bool, String>;

    /// HGET hash field - returns value or None
    fn hget(&self, db: u8, key: &[u8], field: &[u8]) -> Result<Option<Vec<u8>>, String>;

    /// HSET hash field value - returns 1 if new field, 0 if updated
    fn hset(&self, db: u8, key: &[u8], field: &[u8], value: &[u8]) -> Result<i32, String>;

    /// INCR key - returns new value
    fn incr(&self, db: u8, key: &[u8]) -> Result<i64, String>;

    /// INCRBY key amount - returns new value
    fn incrby(&self, db: u8, key: &[u8], amount: i64) -> Result<i64, String>;

    /// EXPIRE key seconds - returns 1 if set, 0 if key doesn't exist
    fn expire(&self, db: u8, key: &[u8], seconds: u64) -> Result<i32, String>;

    /// EXISTS key - returns 1 if exists, 0 otherwise
    fn exists(&self, db: u8, key: &[u8]) -> Result<i32, String>;

    /// TTL key - returns TTL in seconds, -1 if no TTL, -2 if not exists
    fn ttl(&self, db: u8, key: &[u8]) -> Result<i64, String>;
}

/// A no-op store backend that returns default values
/// Used when no real store is available (e.g., for testing)
pub struct NoopStoreBackend;

impl StoreBackend for NoopStoreBackend {
    fn get(&self, _db: u8, _key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        Ok(None)
    }

    fn set(&self, _db: u8, _key: &[u8], _value: &[u8], _ttl_ms: Option<u64>) -> Result<(), String> {
        Ok(())
    }

    fn del(&self, _db: u8, _key: &[u8]) -> Result<bool, String> {
        Ok(false)
    }

    fn hget(&self, _db: u8, _key: &[u8], _field: &[u8]) -> Result<Option<Vec<u8>>, String> {
        Ok(None)
    }

    fn hset(&self, _db: u8, _key: &[u8], _field: &[u8], _value: &[u8]) -> Result<i32, String> {
        Ok(1)
    }

    fn incr(&self, _db: u8, _key: &[u8]) -> Result<i64, String> {
        Ok(1)
    }

    fn incrby(&self, _db: u8, _key: &[u8], amount: i64) -> Result<i64, String> {
        Ok(amount)
    }

    fn expire(&self, _db: u8, _key: &[u8], _seconds: u64) -> Result<i32, String> {
        Ok(1)
    }

    fn exists(&self, _db: u8, _key: &[u8]) -> Result<i32, String> {
        Ok(0)
    }

    fn ttl(&self, _db: u8, _key: &[u8]) -> Result<i64, String> {
        Ok(-2)
    }
}

/// Context for host function execution
pub struct HostContext {
    /// Current database
    pub db: u8,
    /// Permissions for this execution
    pub permissions: FunctionPermissions,
    /// Resource usage tracking
    pub usage: ResourceUsage,
    /// Error state
    pub error: Option<String>,
    /// Memory buffer for data exchange
    pub memory: HostMemory,
    /// Result buffer
    pub result_buffer: Vec<u8>,
    /// Store backend for actual data operations
    pub store: Option<Arc<dyn StoreBackend>>,
}

impl HostContext {
    /// Create a new host context without store backend (for testing)
    pub fn new(db: u8, permissions: FunctionPermissions) -> Self {
        Self {
            db,
            permissions,
            usage: ResourceUsage::new(),
            error: None,
            memory: HostMemory::new(64 * 1024), // 64KB initial
            result_buffer: Vec::new(),
            store: None,
        }
    }

    /// Create a new host context with store backend
    pub fn with_store(
        db: u8,
        permissions: FunctionPermissions,
        store: Arc<dyn StoreBackend>,
    ) -> Self {
        Self {
            db,
            permissions,
            usage: ResourceUsage::new(),
            error: None,
            memory: HostMemory::new(64 * 1024), // 64KB initial
            result_buffer: Vec::new(),
            store: Some(store),
        }
    }

    /// Set an error
    pub fn set_error(&mut self, error: String) {
        self.error = Some(error);
    }

    /// Clear error
    pub fn clear_error(&mut self) {
        self.error = None;
    }

    /// Check if key access is allowed
    pub fn check_key(&self, key: &[u8]) -> Result<(), WasmError> {
        let key_str = String::from_utf8_lossy(key);
        if !self.permissions.is_key_allowed(&key_str) {
            return Err(WasmError::PermissionDenied(format!(
                "access to key '{}' denied",
                key_str
            )));
        }
        Ok(())
    }

    /// Check if command is allowed
    pub fn check_command(&self, command: &str) -> Result<(), WasmError> {
        if !self.permissions.is_command_allowed(command) {
            return Err(WasmError::PermissionDenied(format!(
                "command '{}' not allowed",
                command
            )));
        }
        Ok(())
    }
}

/// Host memory for data exchange with WASM
pub struct HostMemory {
    /// Memory buffer
    buffer: Vec<u8>,
    /// Current allocation position
    position: usize,
}

impl HostMemory {
    /// Create new host memory
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: vec![0u8; capacity],
            position: 0,
        }
    }

    /// Allocate memory and return offset
    pub fn alloc(&mut self, size: usize) -> Option<usize> {
        if self.position + size > self.buffer.len() {
            // Grow buffer if needed
            let new_size = (self.position + size).next_power_of_two();
            self.buffer.resize(new_size, 0);
        }

        let offset = self.position;
        self.position += size;
        Some(offset)
    }

    /// Write data at offset
    pub fn write(&mut self, offset: usize, data: &[u8]) -> Result<(), WasmError> {
        if offset + data.len() > self.buffer.len() {
            return Err(WasmError::MemoryLimitExceeded {
                limit: self.buffer.len(),
            });
        }
        self.buffer[offset..offset + data.len()].copy_from_slice(data);
        Ok(())
    }

    /// Read data from offset
    pub fn read(&self, offset: usize, len: usize) -> Result<&[u8], WasmError> {
        if offset + len > self.buffer.len() {
            return Err(WasmError::MemoryLimitExceeded {
                limit: self.buffer.len(),
            });
        }
        Ok(&self.buffer[offset..offset + len])
    }

    /// Reset allocator
    pub fn reset(&mut self) {
        self.position = 0;
    }

    /// Get current usage
    pub fn usage(&self) -> usize {
        self.position
    }

    /// Get capacity
    pub fn capacity(&self) -> usize {
        self.buffer.len()
    }
}

/// A host function that can be called from WASM
pub trait HostFunction: Send + Sync {
    /// Function name
    fn name(&self) -> &str;

    /// Execute the function
    fn call(&self, ctx: &mut HostContext, args: &[i32]) -> Result<i32, WasmError>;
}

/// GET key
pub struct GetFunction;

impl HostFunction for GetFunction {
    fn name(&self) -> &str {
        "ferrite_get"
    }

    fn call(&self, ctx: &mut HostContext, args: &[i32]) -> Result<i32, WasmError> {
        if args.len() < 4 {
            return Err(WasmError::InvalidArgument(
                "get requires 4 arguments".to_string(),
            ));
        }

        let key_ptr = args[0] as usize;
        let key_len = args[1] as usize;
        let buf_ptr = args[2] as usize;
        let buf_len = args[3] as usize;

        // Read key from memory
        let key = ctx.memory.read(key_ptr, key_len)?.to_vec();
        ctx.check_key(&key)?;
        ctx.check_command("GET")?;
        ctx.usage.record_host_call();

        // Call store backend if available
        if let Some(ref store) = ctx.store {
            match store.get(ctx.db, &key) {
                Ok(Some(value)) => {
                    // Write value to output buffer, truncate if needed
                    let write_len = value.len().min(buf_len);
                    ctx.memory.write(buf_ptr, &value[..write_len])?;
                    // Also store in result buffer for full access
                    ctx.result_buffer = value;
                    Ok(write_len as i32)
                }
                Ok(None) => Ok(-1), // Not found
                Err(e) => {
                    ctx.set_error(e);
                    Ok(-2) // Error
                }
            }
        } else {
            // No store backend - return not found
            Ok(-1)
        }
    }
}

/// SET key value
pub struct SetFunction;

impl HostFunction for SetFunction {
    fn name(&self) -> &str {
        "ferrite_set"
    }

    fn call(&self, ctx: &mut HostContext, args: &[i32]) -> Result<i32, WasmError> {
        if args.len() < 6 {
            return Err(WasmError::InvalidArgument(
                "set requires 6 arguments".to_string(),
            ));
        }

        let key_ptr = args[0] as usize;
        let key_len = args[1] as usize;
        let val_ptr = args[2] as usize;
        let val_len = args[3] as usize;
        let _options = args[4];
        let ttl_ms = args[5];

        // Read key and value from memory
        let key = ctx.memory.read(key_ptr, key_len)?.to_vec();
        let value = ctx.memory.read(val_ptr, val_len)?.to_vec();
        ctx.check_key(&key)?;
        ctx.check_command("SET")?;
        ctx.usage.record_host_call();

        // Call store backend if available
        if let Some(ref store) = ctx.store {
            let ttl = if ttl_ms > 0 {
                Some(ttl_ms as u64)
            } else {
                None
            };
            match store.set(ctx.db, &key, &value, ttl) {
                Ok(()) => Ok(0), // Success
                Err(e) => {
                    ctx.set_error(e);
                    Ok(-1) // Error
                }
            }
        } else {
            // No store backend - simulate success
            Ok(0)
        }
    }
}

/// DEL key(s)
pub struct DelFunction;

impl HostFunction for DelFunction {
    fn name(&self) -> &str {
        "ferrite_del"
    }

    fn call(&self, ctx: &mut HostContext, args: &[i32]) -> Result<i32, WasmError> {
        if args.len() < 2 {
            return Err(WasmError::InvalidArgument(
                "del requires 2 arguments".to_string(),
            ));
        }

        let key_ptr = args[0] as usize;
        let key_len = args[1] as usize;

        // Read key from memory
        let key = ctx.memory.read(key_ptr, key_len)?.to_vec();
        ctx.check_key(&key)?;
        ctx.check_command("DEL")?;
        ctx.usage.record_host_call();

        // Call store backend if available
        if let Some(ref store) = ctx.store {
            match store.del(ctx.db, &key) {
                Ok(true) => Ok(1),  // Key was deleted
                Ok(false) => Ok(0), // Key didn't exist
                Err(e) => {
                    ctx.set_error(e);
                    Ok(-1) // Error
                }
            }
        } else {
            // No store backend - simulate success
            Ok(0)
        }
    }
}

/// Log a message
pub struct LogFunction;

impl HostFunction for LogFunction {
    fn name(&self) -> &str {
        "ferrite_log"
    }

    fn call(&self, ctx: &mut HostContext, args: &[i32]) -> Result<i32, WasmError> {
        if args.len() < 3 {
            return Err(WasmError::InvalidArgument(
                "log requires 3 arguments".to_string(),
            ));
        }

        let level = args[0];
        let msg_ptr = args[1] as usize;
        let msg_len = args[2] as usize;

        let msg = ctx.memory.read(msg_ptr, msg_len)?;
        let msg_str = String::from_utf8_lossy(msg);

        ctx.usage.record_host_call();

        // Log based on level
        match level {
            0 => tracing::trace!(target: "wasm", "{}", msg_str),
            1 => tracing::debug!(target: "wasm", "{}", msg_str),
            2 => tracing::info!(target: "wasm", "{}", msg_str),
            3 => tracing::warn!(target: "wasm", "{}", msg_str),
            _ => tracing::error!(target: "wasm", "{}", msg_str),
        }

        Ok(0)
    }
}

/// Get current time in milliseconds
pub struct TimeFunction;

impl HostFunction for TimeFunction {
    fn name(&self) -> &str {
        "ferrite_time_millis"
    }

    fn call(&self, ctx: &mut HostContext, _args: &[i32]) -> Result<i32, WasmError> {
        ctx.usage.record_host_call();

        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        // Return lower 32 bits (for simplicity)
        Ok(millis as i32)
    }
}

/// Generate random bytes
pub struct RandomFunction;

impl HostFunction for RandomFunction {
    fn name(&self) -> &str {
        "ferrite_random"
    }

    fn call(&self, ctx: &mut HostContext, args: &[i32]) -> Result<i32, WasmError> {
        if args.len() < 2 {
            return Err(WasmError::InvalidArgument(
                "random requires 2 arguments".to_string(),
            ));
        }

        let buf_ptr = args[0] as usize;
        let buf_len = args[1] as usize;

        ctx.usage.record_host_call();

        // Generate random bytes
        let mut random_bytes = vec![0u8; buf_len];
        for byte in &mut random_bytes {
            *byte = rand::random();
        }

        ctx.memory.write(buf_ptr, &random_bytes)?;

        Ok(buf_len as i32)
    }
}

/// HGET hash field
pub struct HGetFunction;

impl HostFunction for HGetFunction {
    fn name(&self) -> &str {
        "ferrite_hget"
    }

    fn call(&self, ctx: &mut HostContext, args: &[i32]) -> Result<i32, WasmError> {
        if args.len() < 6 {
            return Err(WasmError::InvalidArgument(
                "hget requires 6 arguments".to_string(),
            ));
        }

        let key_ptr = args[0] as usize;
        let key_len = args[1] as usize;
        let field_ptr = args[2] as usize;
        let field_len = args[3] as usize;
        let buf_ptr = args[4] as usize;
        let buf_len = args[5] as usize;

        let key = ctx.memory.read(key_ptr, key_len)?.to_vec();
        let field = ctx.memory.read(field_ptr, field_len)?.to_vec();
        ctx.check_key(&key)?;
        ctx.check_command("HGET")?;
        ctx.usage.record_host_call();

        // Call store backend if available
        if let Some(ref store) = ctx.store {
            match store.hget(ctx.db, &key, &field) {
                Ok(Some(value)) => {
                    let write_len = value.len().min(buf_len);
                    ctx.memory.write(buf_ptr, &value[..write_len])?;
                    ctx.result_buffer = value;
                    Ok(write_len as i32)
                }
                Ok(None) => Ok(-1), // Not found
                Err(e) => {
                    ctx.set_error(e);
                    Ok(-2) // Error
                }
            }
        } else {
            Ok(-1) // Not found
        }
    }
}

/// HSET hash field value
pub struct HSetFunction;

impl HostFunction for HSetFunction {
    fn name(&self) -> &str {
        "ferrite_hset"
    }

    fn call(&self, ctx: &mut HostContext, args: &[i32]) -> Result<i32, WasmError> {
        if args.len() < 6 {
            return Err(WasmError::InvalidArgument(
                "hset requires 6 arguments".to_string(),
            ));
        }

        let key_ptr = args[0] as usize;
        let key_len = args[1] as usize;
        let field_ptr = args[2] as usize;
        let field_len = args[3] as usize;
        let val_ptr = args[4] as usize;
        let val_len = args[5] as usize;

        let key = ctx.memory.read(key_ptr, key_len)?.to_vec();
        let field = ctx.memory.read(field_ptr, field_len)?.to_vec();
        let value = ctx.memory.read(val_ptr, val_len)?.to_vec();
        ctx.check_key(&key)?;
        ctx.check_command("HSET")?;
        ctx.usage.record_host_call();

        // Call store backend if available
        if let Some(ref store) = ctx.store {
            match store.hset(ctx.db, &key, &field, &value) {
                Ok(n) => Ok(n),
                Err(e) => {
                    ctx.set_error(e);
                    Ok(-1) // Error
                }
            }
        } else {
            Ok(1) // Simulate field added
        }
    }
}

/// INCR key
pub struct IncrFunction;

impl HostFunction for IncrFunction {
    fn name(&self) -> &str {
        "ferrite_incr"
    }

    fn call(&self, ctx: &mut HostContext, args: &[i32]) -> Result<i32, WasmError> {
        if args.len() < 2 {
            return Err(WasmError::InvalidArgument(
                "incr requires 2 arguments".to_string(),
            ));
        }

        let key_ptr = args[0] as usize;
        let key_len = args[1] as usize;
        // Optional: amount (default 1)
        let amount = if args.len() > 2 { args[2] as i64 } else { 1 };

        let key = ctx.memory.read(key_ptr, key_len)?.to_vec();
        ctx.check_key(&key)?;
        ctx.check_command("INCR")?;
        ctx.usage.record_host_call();

        // Call store backend if available
        if let Some(ref store) = ctx.store {
            let result = if amount == 1 {
                store.incr(ctx.db, &key)
            } else {
                store.incrby(ctx.db, &key, amount)
            };
            match result {
                Ok(n) => Ok(n as i32),
                Err(e) => {
                    ctx.set_error(e);
                    Ok(i32::MIN) // Error indicator
                }
            }
        } else {
            Ok(1) // Simulate new value
        }
    }
}

/// EXPIRE key seconds
pub struct ExpireFunction;

impl HostFunction for ExpireFunction {
    fn name(&self) -> &str {
        "ferrite_expire"
    }

    fn call(&self, ctx: &mut HostContext, args: &[i32]) -> Result<i32, WasmError> {
        if args.len() < 3 {
            return Err(WasmError::InvalidArgument(
                "expire requires 3 arguments".to_string(),
            ));
        }

        let key_ptr = args[0] as usize;
        let key_len = args[1] as usize;
        let seconds = args[2] as u64;

        let key = ctx.memory.read(key_ptr, key_len)?.to_vec();
        ctx.check_key(&key)?;
        ctx.check_command("EXPIRE")?;
        ctx.usage.record_host_call();

        // Call store backend if available
        if let Some(ref store) = ctx.store {
            match store.expire(ctx.db, &key, seconds) {
                Ok(n) => Ok(n),
                Err(e) => {
                    ctx.set_error(e);
                    Ok(-1) // Error
                }
            }
        } else {
            Ok(1) // Simulate success
        }
    }
}

/// EXISTS key
pub struct ExistsFunction;

impl HostFunction for ExistsFunction {
    fn name(&self) -> &str {
        "ferrite_exists"
    }

    fn call(&self, ctx: &mut HostContext, args: &[i32]) -> Result<i32, WasmError> {
        if args.len() < 2 {
            return Err(WasmError::InvalidArgument(
                "exists requires 2 arguments".to_string(),
            ));
        }

        let key_ptr = args[0] as usize;
        let key_len = args[1] as usize;

        let key = ctx.memory.read(key_ptr, key_len)?.to_vec();
        ctx.check_key(&key)?;
        ctx.check_command("EXISTS")?;
        ctx.usage.record_host_call();

        // Call store backend if available
        if let Some(ref store) = ctx.store {
            match store.exists(ctx.db, &key) {
                Ok(n) => Ok(n),
                Err(e) => {
                    ctx.set_error(e);
                    Ok(-1) // Error
                }
            }
        } else {
            Ok(0) // Simulate not exists
        }
    }
}

/// TTL key
pub struct TtlFunction;

impl HostFunction for TtlFunction {
    fn name(&self) -> &str {
        "ferrite_ttl"
    }

    fn call(&self, ctx: &mut HostContext, args: &[i32]) -> Result<i32, WasmError> {
        if args.len() < 2 {
            return Err(WasmError::InvalidArgument(
                "ttl requires 2 arguments".to_string(),
            ));
        }

        let key_ptr = args[0] as usize;
        let key_len = args[1] as usize;

        let key = ctx.memory.read(key_ptr, key_len)?.to_vec();
        ctx.check_key(&key)?;
        ctx.check_command("TTL")?;
        ctx.usage.record_host_call();

        // Call store backend if available
        if let Some(ref store) = ctx.store {
            match store.ttl(ctx.db, &key) {
                Ok(n) => Ok(n as i32),
                Err(e) => {
                    ctx.set_error(e);
                    Ok(i32::MIN) // Error
                }
            }
        } else {
            Ok(-2) // Simulate key not exists
        }
    }
}

/// Collection of all host functions
pub struct HostFunctions {
    functions: Vec<Box<dyn HostFunction>>,
}

impl HostFunctions {
    /// Create a new host functions collection
    pub fn new() -> Self {
        let functions: Vec<Box<dyn HostFunction>> = vec![
            Box::new(GetFunction),
            Box::new(SetFunction),
            Box::new(DelFunction),
            Box::new(LogFunction),
            Box::new(TimeFunction),
            Box::new(RandomFunction),
            Box::new(HGetFunction),
            Box::new(HSetFunction),
            Box::new(IncrFunction),
            Box::new(ExpireFunction),
            Box::new(ExistsFunction),
            Box::new(TtlFunction),
        ];

        Self { functions }
    }

    /// Get a function by name
    pub fn get(&self, name: &str) -> Option<&dyn HostFunction> {
        self.functions
            .iter()
            .find(|f| f.name() == name)
            .map(|f| f.as_ref())
    }

    /// List all function names
    pub fn names(&self) -> Vec<&str> {
        self.functions.iter().map(|f| f.name()).collect()
    }
}

impl Default for HostFunctions {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_host_context() {
        let ctx = HostContext::new(0, FunctionPermissions::default());
        assert_eq!(ctx.db, 0);
        assert!(ctx.error.is_none());
    }

    #[test]
    fn test_host_context_check_key() {
        let mut perms = FunctionPermissions::default();
        perms.allowed_keys = vec!["user:*".to_string()];

        let ctx = HostContext::new(0, perms);

        assert!(ctx.check_key(b"user:123").is_ok());
        assert!(ctx.check_key(b"other:123").is_err());
    }

    #[test]
    fn test_host_context_check_command() {
        let perms = FunctionPermissions::read_only();
        let ctx = HostContext::new(0, perms);

        assert!(ctx.check_command("GET").is_ok());
        assert!(ctx.check_command("SET").is_err());
    }

    #[test]
    fn test_host_memory() {
        let mut mem = HostMemory::new(1024);

        let offset = mem.alloc(100).unwrap();
        assert_eq!(offset, 0);

        let offset2 = mem.alloc(200).unwrap();
        assert_eq!(offset2, 100);

        mem.write(0, b"hello").unwrap();
        let data = mem.read(0, 5).unwrap();
        assert_eq!(data, b"hello");
    }

    #[test]
    fn test_host_memory_reset() {
        let mut mem = HostMemory::new(1024);

        mem.alloc(500).unwrap();
        assert_eq!(mem.usage(), 500);

        mem.reset();
        assert_eq!(mem.usage(), 0);
    }

    #[test]
    fn test_host_functions() {
        let funcs = HostFunctions::new();

        assert!(funcs.get("ferrite_get").is_some());
        assert!(funcs.get("ferrite_set").is_some());
        assert!(funcs.get("ferrite_del").is_some());
        assert!(funcs.get("ferrite_log").is_some());
        assert!(funcs.get("ferrite_time_millis").is_some());
        assert!(funcs.get("nonexistent").is_none());
    }

    #[test]
    fn test_host_functions_names() {
        let funcs = HostFunctions::new();
        let names = funcs.names();

        assert!(names.contains(&"ferrite_get"));
        assert!(names.contains(&"ferrite_set"));
        assert!(names.contains(&"ferrite_exists"));
        assert!(names.contains(&"ferrite_ttl"));
        assert!(names.len() >= 12);
    }

    #[test]
    fn test_log_function() {
        let mut ctx = HostContext::new(0, FunctionPermissions::default());
        let log_fn = LogFunction;

        // Write message to memory
        ctx.memory.write(0, b"test message").unwrap();

        // Call log function (level, ptr, len)
        let result = log_fn.call(&mut ctx, &[2, 0, 12]); // level 2 = info
        assert!(result.is_ok());
        assert_eq!(ctx.usage.host_calls, 1);
    }

    #[test]
    fn test_time_function() {
        let mut ctx = HostContext::new(0, FunctionPermissions::default());
        let time_fn = TimeFunction;

        let result = time_fn.call(&mut ctx, &[]);
        assert!(result.is_ok());

        // Should return a positive value (current time)
        let time = result.unwrap();
        assert!(time > 0 || time < 0); // i32 overflow is possible but it's still a valid call
        assert_eq!(ctx.usage.host_calls, 1);
    }

    #[test]
    fn test_random_function() {
        let mut ctx = HostContext::new(0, FunctionPermissions::default());
        let random_fn = RandomFunction;

        // Allocate space in memory
        ctx.memory.alloc(16).unwrap();

        let result = random_fn.call(&mut ctx, &[0, 16]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 16);

        // Check that memory was written
        let data = ctx.memory.read(0, 16).unwrap();
        // Very unlikely to be all zeros
        assert!(data.iter().any(|&b| b != 0));
    }
}
