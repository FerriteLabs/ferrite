//! Custom data type registration for Redis Module API compatibility
//!
//! Provides `RedisModuleType` and associated method callbacks so that
//! modules can register custom data types that participate in persistence,
//! replication, and memory management.


use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use parking_lot::RwLock;

/// Opaque value stored by a module data type.
///
/// Module authors store arbitrary data behind this trait. Ferrite invokes
/// the registered callbacks (save, load, free, etc.) through it.
pub trait ModuleTypeValue: Send + Sync + fmt::Debug {
    /// Serialise the value for RDB persistence.
    fn rdb_save(&self) -> Vec<u8>;
    /// Create the value from an RDB-persisted byte stream.
    fn rdb_load(data: &[u8]) -> Result<Box<dyn ModuleTypeValue>, ModuleTypeError>
    where
        Self: Sized;
    /// Produce an AOF rewrite representation (optional).
    fn aof_rewrite(&self, _key: &str) -> Option<String> {
        None
    }
    /// Report the memory usage of this value in bytes.
    fn mem_usage(&self) -> usize {
        0
    }
    /// Produce a digest for DEBUG DIGEST (optional).
    fn digest(&self) -> Option<Vec<u8>> {
        None
    }
}

/// Descriptor for a module-registered custom data type (`RedisModuleType`).
///
/// This is the Ferrite equivalent of `RedisModuleTypeMethods` and the
/// opaque `RedisModuleType` pointer in the Redis Module API.
pub struct RedisModuleTypeDef {
    /// Unique type name (max 9 ASCII characters in Redis, relaxed here)
    pub name: String,
    /// Encoding version used for RDB serialisation
    pub encoding_version: u32,
    /// Type method callbacks
    pub methods: TypeMethods,
}

impl RedisModuleTypeDef {
    /// Create a new type definition with default (no-op) methods.
    pub fn new(name: &str, encoding_version: u32) -> Self {
        Self {
            name: name.to_string(),
            encoding_version,
            methods: TypeMethods::default(),
        }
    }

    /// Builder: set all type methods at once.
    pub fn with_methods(mut self, methods: TypeMethods) -> Self {
        self.methods = methods;
        self
    }
}

impl fmt::Debug for RedisModuleTypeDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisModuleTypeDef")
            .field("name", &self.name)
            .field("encoding_version", &self.encoding_version)
            .finish()
    }
}

/// Callback signatures used by the type system.
///
/// Each field corresponds to one of the `RedisModuleTypeMethods` callbacks
/// in the Redis Module API.
#[derive(Default)]
pub struct TypeMethods {
    /// Serialise a value to bytes (RDB save)
    pub rdb_save: Option<RdbSaveFn>,
    /// Deserialise a value from bytes (RDB load)
    pub rdb_load: Option<RdbLoadFn>,
    /// Emit AOF rewrite commands
    pub aof_rewrite: Option<AofRewriteFn>,
    /// Report memory usage
    pub mem_usage: Option<MemUsageFn>,
    /// Produce a digest
    pub digest: Option<DigestFn>,
    /// Free / drop the value
    pub free: Option<FreeFn>,
}

/// RDB save callback: serialise a value to bytes.
pub type RdbSaveFn = Arc<dyn Fn(&dyn ModuleTypeValue) -> Vec<u8> + Send + Sync>;
/// RDB load callback: deserialise a value from bytes.
pub type RdbLoadFn =
    Arc<dyn Fn(&[u8]) -> Result<Box<dyn ModuleTypeValue>, ModuleTypeError> + Send + Sync>;
/// AOF rewrite callback.
pub type AofRewriteFn = Arc<dyn Fn(&dyn ModuleTypeValue, &str) -> Option<String> + Send + Sync>;
/// Memory usage callback.
pub type MemUsageFn = Arc<dyn Fn(&dyn ModuleTypeValue) -> usize + Send + Sync>;
/// Digest callback.
pub type DigestFn = Arc<dyn Fn(&dyn ModuleTypeValue) -> Option<Vec<u8>> + Send + Sync>;
/// Free callback.
pub type FreeFn = Arc<dyn Fn(Box<dyn ModuleTypeValue>) + Send + Sync>;

impl fmt::Debug for TypeMethods {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TypeMethods")
            .field("rdb_save", &self.rdb_save.is_some())
            .field("rdb_load", &self.rdb_load.is_some())
            .field("aof_rewrite", &self.aof_rewrite.is_some())
            .field("mem_usage", &self.mem_usage.is_some())
            .field("digest", &self.digest.is_some())
            .field("free", &self.free.is_some())
            .finish()
    }
}

/// Registry of custom module data types.
pub struct ModuleTypeRegistry {
    types: RwLock<HashMap<String, Arc<RedisModuleTypeDef>>>,
}

impl ModuleTypeRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            types: RwLock::new(HashMap::new()),
        }
    }

    /// Register a custom data type.
    pub fn register(&self, type_def: RedisModuleTypeDef) -> Result<(), ModuleTypeError> {
        let mut types = self.types.write();
        if types.contains_key(&type_def.name) {
            return Err(ModuleTypeError::AlreadyRegistered(type_def.name.clone()));
        }
        types.insert(type_def.name.clone(), Arc::new(type_def));
        Ok(())
    }

    /// Unregister a type by name.
    pub fn unregister(&self, name: &str) -> Result<(), ModuleTypeError> {
        let mut types = self.types.write();
        if types.remove(name).is_none() {
            return Err(ModuleTypeError::NotFound(name.to_string()));
        }
        Ok(())
    }

    /// Look up a registered type definition.
    pub fn get(&self, name: &str) -> Option<Arc<RedisModuleTypeDef>> {
        self.types.read().get(name).cloned()
    }

    /// Return the number of registered types.
    pub fn len(&self) -> usize {
        self.types.read().len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.types.read().is_empty()
    }

    /// List all registered type names.
    pub fn type_names(&self) -> Vec<String> {
        self.types.read().keys().cloned().collect()
    }
}

impl Default for ModuleTypeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors related to the module type system.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ModuleTypeError {
    /// Type already registered
    #[error("module type already registered: {0}")]
    AlreadyRegistered(String),
    /// Type not found
    #[error("module type not found: {0}")]
    NotFound(String),
    /// RDB load failed
    #[error("rdb load failed: {0}")]
    RdbLoadFailed(String),
    /// Serialisation error
    #[error("serialization error: {0}")]
    SerializationError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestValue {
        data: Vec<u8>,
    }

    impl ModuleTypeValue for TestValue {
        fn rdb_save(&self) -> Vec<u8> {
            self.data.clone()
        }

        fn rdb_load(data: &[u8]) -> Result<Box<dyn ModuleTypeValue>, ModuleTypeError> {
            Ok(Box::new(TestValue {
                data: data.to_vec(),
            }))
        }

        fn mem_usage(&self) -> usize {
            self.data.len()
        }
    }

    #[test]
    fn test_register_type() {
        let registry = ModuleTypeRegistry::new();
        let td = RedisModuleTypeDef::new("mytype", 1);
        assert!(registry.register(td).is_ok());
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_register_duplicate() {
        let registry = ModuleTypeRegistry::new();
        registry
            .register(RedisModuleTypeDef::new("mytype", 1))
            .unwrap();
        let result = registry.register(RedisModuleTypeDef::new("mytype", 1));
        assert!(matches!(result, Err(ModuleTypeError::AlreadyRegistered(_))));
    }

    #[test]
    fn test_unregister_type() {
        let registry = ModuleTypeRegistry::new();
        registry
            .register(RedisModuleTypeDef::new("mytype", 1))
            .unwrap();
        assert!(registry.unregister("mytype").is_ok());
        assert!(registry.is_empty());
    }

    #[test]
    fn test_unregister_not_found() {
        let registry = ModuleTypeRegistry::new();
        assert!(matches!(
            registry.unregister("nope"),
            Err(ModuleTypeError::NotFound(_))
        ));
    }

    #[test]
    fn test_type_names() {
        let registry = ModuleTypeRegistry::new();
        registry
            .register(RedisModuleTypeDef::new("type_a", 1))
            .unwrap();
        registry
            .register(RedisModuleTypeDef::new("type_b", 1))
            .unwrap();
        let mut names = registry.type_names();
        names.sort();
        assert_eq!(names, vec!["type_a", "type_b"]);
    }

    #[test]
    fn test_module_type_value_trait() {
        let val = TestValue {
            data: vec![1, 2, 3],
        };
        assert_eq!(val.rdb_save(), vec![1, 2, 3]);
        assert_eq!(val.mem_usage(), 3);
        assert!(val.aof_rewrite("key").is_none());
        assert!(val.digest().is_none());

        let loaded = TestValue::rdb_load(&[4, 5]).unwrap();
        assert_eq!(loaded.rdb_save(), vec![4, 5]);
    }

    #[test]
    fn test_type_methods_default() {
        let methods = TypeMethods::default();
        assert!(methods.rdb_save.is_none());
        assert!(methods.rdb_load.is_none());
        assert!(methods.aof_rewrite.is_none());
        assert!(methods.mem_usage.is_none());
        assert!(methods.digest.is_none());
        assert!(methods.free.is_none());
    }

    #[test]
    fn test_type_def_with_methods() {
        let td = RedisModuleTypeDef::new("test", 1).with_methods(TypeMethods {
            rdb_save: Some(Arc::new(|v| v.rdb_save())),
            ..TypeMethods::default()
        });
        assert!(td.methods.rdb_save.is_some());
        assert!(td.methods.rdb_load.is_none());
    }
}
