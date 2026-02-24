//! Schema Definitions
//!
//! Type schemas for SDK generation.

use super::CommandCategory;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Complete API schema
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApiSchema {
    /// API version
    pub version: String,
    /// API title
    pub title: String,
    /// Description
    pub description: String,
    /// Commands grouped by category
    pub commands: HashMap<CommandCategory, Vec<CommandSchema>>,
    /// Type definitions
    pub types: HashMap<String, TypeSchema>,
}

impl ApiSchema {
    /// Create a new API schema
    pub fn new(version: impl Into<String>, title: impl Into<String>) -> Self {
        Self {
            version: version.into(),
            title: title.into(),
            description: String::new(),
            commands: HashMap::new(),
            types: HashMap::new(),
        }
    }

    /// Add a command
    pub fn add_command(&mut self, category: CommandCategory, command: CommandSchema) {
        self.commands.entry(category).or_default().push(command);
    }

    /// Add a type
    pub fn add_type(&mut self, name: impl Into<String>, type_schema: TypeSchema) {
        self.types.insert(name.into(), type_schema);
    }

    /// Get all commands flat
    pub fn all_commands(&self) -> Vec<&CommandSchema> {
        self.commands.values().flatten().collect()
    }

    /// Build the default Ferrite API schema
    pub fn ferrite_default() -> Self {
        let mut schema = Self::new("1.0.0", "Ferrite API");
        schema.description = "Ferrite Key-Value Store API".to_string();

        // Add string commands
        schema.add_command(CommandCategory::Strings, CommandSchema::get());
        schema.add_command(CommandCategory::Strings, CommandSchema::set());
        schema.add_command(CommandCategory::Strings, CommandSchema::mget());
        schema.add_command(CommandCategory::Strings, CommandSchema::mset());
        schema.add_command(CommandCategory::Strings, CommandSchema::incr());
        schema.add_command(CommandCategory::Strings, CommandSchema::decr());
        schema.add_command(CommandCategory::Strings, CommandSchema::append());

        // Add key commands
        schema.add_command(CommandCategory::Keys, CommandSchema::del());
        schema.add_command(CommandCategory::Keys, CommandSchema::exists());
        schema.add_command(CommandCategory::Keys, CommandSchema::expire());
        schema.add_command(CommandCategory::Keys, CommandSchema::ttl());
        schema.add_command(CommandCategory::Keys, CommandSchema::keys());
        schema.add_command(CommandCategory::Keys, CommandSchema::scan());

        // Add list commands
        schema.add_command(CommandCategory::Lists, CommandSchema::lpush());
        schema.add_command(CommandCategory::Lists, CommandSchema::rpush());
        schema.add_command(CommandCategory::Lists, CommandSchema::lpop());
        schema.add_command(CommandCategory::Lists, CommandSchema::rpop());
        schema.add_command(CommandCategory::Lists, CommandSchema::lrange());
        schema.add_command(CommandCategory::Lists, CommandSchema::llen());

        // Add hash commands
        schema.add_command(CommandCategory::Hashes, CommandSchema::hset());
        schema.add_command(CommandCategory::Hashes, CommandSchema::hget());
        schema.add_command(CommandCategory::Hashes, CommandSchema::hmset());
        schema.add_command(CommandCategory::Hashes, CommandSchema::hgetall());
        schema.add_command(CommandCategory::Hashes, CommandSchema::hdel());

        // Add set commands
        schema.add_command(CommandCategory::Sets, CommandSchema::sadd());
        schema.add_command(CommandCategory::Sets, CommandSchema::srem());
        schema.add_command(CommandCategory::Sets, CommandSchema::smembers());
        schema.add_command(CommandCategory::Sets, CommandSchema::sismember());

        // Add sorted set commands
        schema.add_command(CommandCategory::SortedSets, CommandSchema::zadd());
        schema.add_command(CommandCategory::SortedSets, CommandSchema::zrange());
        schema.add_command(CommandCategory::SortedSets, CommandSchema::zscore());

        // Add server commands
        schema.add_command(CommandCategory::Server, CommandSchema::ping());
        schema.add_command(CommandCategory::Server, CommandSchema::info());
        schema.add_command(CommandCategory::Server, CommandSchema::dbsize());

        // Add vector commands (Ferrite extension)
        schema.add_command(CommandCategory::Vector, CommandSchema::vector_add());
        schema.add_command(CommandCategory::Vector, CommandSchema::vector_search());

        // Add common types
        schema.add_type("String", TypeSchema::string());
        schema.add_type("Integer", TypeSchema::integer());
        schema.add_type("Float", TypeSchema::float());
        schema.add_type("Boolean", TypeSchema::boolean());
        schema.add_type("Array", TypeSchema::array(TypeSchema::any()));
        schema.add_type("Object", TypeSchema::object());
        schema.add_type("Null", TypeSchema::null());

        schema
    }
}

impl Default for ApiSchema {
    fn default() -> Self {
        Self::ferrite_default()
    }
}

/// Command schema definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandSchema {
    /// Command name (e.g., "GET", "SET")
    pub name: String,
    /// Method name in generated code (e.g., "get", "set")
    pub method_name: String,
    /// Description
    pub description: String,
    /// Parameters
    pub parameters: Vec<ParameterSchema>,
    /// Return type
    pub return_type: TypeSchema,
    /// Whether this is an async command
    pub is_async: bool,
    /// Complexity (O notation)
    pub complexity: Option<String>,
    /// Since version
    pub since: Option<String>,
    /// Deprecated flag
    pub deprecated: bool,
    /// Deprecation message
    pub deprecation_message: Option<String>,
    /// Example usage
    pub examples: Vec<CommandExample>,
}

impl CommandSchema {
    /// Create a new command schema
    pub fn new(name: impl Into<String>, method_name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            method_name: method_name.into(),
            description: String::new(),
            parameters: Vec::new(),
            return_type: TypeSchema::any(),
            is_async: true,
            complexity: None,
            since: None,
            deprecated: false,
            deprecation_message: None,
            examples: Vec::new(),
        }
    }

    /// Builder: set description
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = desc.into();
        self
    }

    /// Builder: add parameter
    pub fn with_param(mut self, param: ParameterSchema) -> Self {
        self.parameters.push(param);
        self
    }

    /// Builder: set return type
    pub fn with_return_type(mut self, rt: TypeSchema) -> Self {
        self.return_type = rt;
        self
    }

    /// Builder: set complexity
    pub fn with_complexity(mut self, complexity: impl Into<String>) -> Self {
        self.complexity = Some(complexity.into());
        self
    }

    // Standard Redis commands

    /// GET command schema
    pub fn get() -> Self {
        Self::new("GET", "get")
            .with_description("Get the value of a key")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_return_type(TypeSchema::nullable(TypeSchema::string()))
            .with_complexity("O(1)")
    }

    /// SET command schema
    pub fn set() -> Self {
        Self::new("SET", "set")
            .with_description("Set the value of a key")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_param(ParameterSchema::required("value", TypeSchema::string()))
            .with_param(
                ParameterSchema::optional("ex", TypeSchema::integer())
                    .with_description("Expiry in seconds"),
            )
            .with_param(
                ParameterSchema::optional("px", TypeSchema::integer())
                    .with_description("Expiry in milliseconds"),
            )
            .with_param(
                ParameterSchema::optional("nx", TypeSchema::boolean())
                    .with_description("Only set if not exists"),
            )
            .with_param(
                ParameterSchema::optional("xx", TypeSchema::boolean())
                    .with_description("Only set if exists"),
            )
            .with_return_type(TypeSchema::nullable(TypeSchema::string()))
            .with_complexity("O(1)")
    }

    /// MGET command schema
    pub fn mget() -> Self {
        Self::new("MGET", "mget")
            .with_description("Get the values of multiple keys")
            .with_param(ParameterSchema::variadic("keys", TypeSchema::string()))
            .with_return_type(TypeSchema::array(
                TypeSchema::nullable(TypeSchema::string()),
            ))
            .with_complexity("O(N)")
    }

    /// MSET command schema
    pub fn mset() -> Self {
        Self::new("MSET", "mset")
            .with_description("Set multiple keys to multiple values")
            .with_param(ParameterSchema::variadic(
                "key_values",
                TypeSchema::string(),
            ))
            .with_return_type(TypeSchema::string())
            .with_complexity("O(N)")
    }

    /// INCR command schema
    pub fn incr() -> Self {
        Self::new("INCR", "incr")
            .with_description("Increment the integer value of a key by one")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_return_type(TypeSchema::integer())
            .with_complexity("O(1)")
    }

    /// DECR command schema
    pub fn decr() -> Self {
        Self::new("DECR", "decr")
            .with_description("Decrement the integer value of a key by one")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_return_type(TypeSchema::integer())
            .with_complexity("O(1)")
    }

    /// APPEND command schema
    pub fn append() -> Self {
        Self::new("APPEND", "append")
            .with_description("Append a value to a key")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_param(ParameterSchema::required("value", TypeSchema::string()))
            .with_return_type(TypeSchema::integer())
            .with_complexity("O(1)")
    }

    /// DEL command schema
    pub fn del() -> Self {
        Self::new("DEL", "del")
            .with_description("Delete one or more keys")
            .with_param(ParameterSchema::variadic("keys", TypeSchema::string()))
            .with_return_type(TypeSchema::integer())
            .with_complexity("O(N)")
    }

    /// EXISTS command schema
    pub fn exists() -> Self {
        Self::new("EXISTS", "exists")
            .with_description("Check if keys exist")
            .with_param(ParameterSchema::variadic("keys", TypeSchema::string()))
            .with_return_type(TypeSchema::integer())
            .with_complexity("O(N)")
    }

    /// EXPIRE command schema
    pub fn expire() -> Self {
        Self::new("EXPIRE", "expire")
            .with_description("Set a key's time to live in seconds")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_param(ParameterSchema::required("seconds", TypeSchema::integer()))
            .with_return_type(TypeSchema::integer())
            .with_complexity("O(1)")
    }

    /// TTL command schema
    pub fn ttl() -> Self {
        Self::new("TTL", "ttl")
            .with_description("Get the time to live for a key in seconds")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_return_type(TypeSchema::integer())
            .with_complexity("O(1)")
    }

    /// KEYS command schema
    pub fn keys() -> Self {
        Self::new("KEYS", "keys")
            .with_description("Find all keys matching the given pattern")
            .with_param(ParameterSchema::required("pattern", TypeSchema::string()))
            .with_return_type(TypeSchema::array(TypeSchema::string()))
            .with_complexity("O(N)")
    }

    /// SCAN command schema
    pub fn scan() -> Self {
        Self::new("SCAN", "scan")
            .with_description("Incrementally iterate the keys space")
            .with_param(ParameterSchema::required("cursor", TypeSchema::integer()))
            .with_param(ParameterSchema::optional("match", TypeSchema::string()))
            .with_param(ParameterSchema::optional("count", TypeSchema::integer()))
            .with_return_type(TypeSchema::array(TypeSchema::any()))
            .with_complexity("O(1) per call")
    }

    /// LPUSH command schema
    pub fn lpush() -> Self {
        Self::new("LPUSH", "lpush")
            .with_description("Prepend values to a list")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_param(ParameterSchema::variadic("values", TypeSchema::string()))
            .with_return_type(TypeSchema::integer())
            .with_complexity("O(N)")
    }

    /// RPUSH command schema
    pub fn rpush() -> Self {
        Self::new("RPUSH", "rpush")
            .with_description("Append values to a list")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_param(ParameterSchema::variadic("values", TypeSchema::string()))
            .with_return_type(TypeSchema::integer())
            .with_complexity("O(N)")
    }

    /// LPOP command schema
    pub fn lpop() -> Self {
        Self::new("LPOP", "lpop")
            .with_description("Remove and get the first element in a list")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_param(ParameterSchema::optional("count", TypeSchema::integer()))
            .with_return_type(TypeSchema::nullable(TypeSchema::string()))
            .with_complexity("O(N)")
    }

    /// RPOP command schema
    pub fn rpop() -> Self {
        Self::new("RPOP", "rpop")
            .with_description("Remove and get the last element in a list")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_param(ParameterSchema::optional("count", TypeSchema::integer()))
            .with_return_type(TypeSchema::nullable(TypeSchema::string()))
            .with_complexity("O(N)")
    }

    /// LRANGE command schema
    pub fn lrange() -> Self {
        Self::new("LRANGE", "lrange")
            .with_description("Get a range of elements from a list")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_param(ParameterSchema::required("start", TypeSchema::integer()))
            .with_param(ParameterSchema::required("stop", TypeSchema::integer()))
            .with_return_type(TypeSchema::array(TypeSchema::string()))
            .with_complexity("O(S+N)")
    }

    /// LLEN command schema
    pub fn llen() -> Self {
        Self::new("LLEN", "llen")
            .with_description("Get the length of a list")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_return_type(TypeSchema::integer())
            .with_complexity("O(1)")
    }

    /// HSET command schema
    pub fn hset() -> Self {
        Self::new("HSET", "hset")
            .with_description("Set field(s) in a hash")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_param(ParameterSchema::variadic(
                "field_values",
                TypeSchema::string(),
            ))
            .with_return_type(TypeSchema::integer())
            .with_complexity("O(N)")
    }

    /// HGET command schema
    pub fn hget() -> Self {
        Self::new("HGET", "hget")
            .with_description("Get the value of a hash field")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_param(ParameterSchema::required("field", TypeSchema::string()))
            .with_return_type(TypeSchema::nullable(TypeSchema::string()))
            .with_complexity("O(1)")
    }

    /// HMSET command schema
    pub fn hmset() -> Self {
        Self::new("HMSET", "hmset")
            .with_description("Set multiple hash fields")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_param(ParameterSchema::variadic(
                "field_values",
                TypeSchema::string(),
            ))
            .with_return_type(TypeSchema::string())
            .with_complexity("O(N)")
    }

    /// HGETALL command schema
    pub fn hgetall() -> Self {
        Self::new("HGETALL", "hgetall")
            .with_description("Get all fields and values in a hash")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_return_type(TypeSchema::object())
            .with_complexity("O(N)")
    }

    /// HDEL command schema
    pub fn hdel() -> Self {
        Self::new("HDEL", "hdel")
            .with_description("Delete one or more hash fields")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_param(ParameterSchema::variadic("fields", TypeSchema::string()))
            .with_return_type(TypeSchema::integer())
            .with_complexity("O(N)")
    }

    /// SADD command schema
    pub fn sadd() -> Self {
        Self::new("SADD", "sadd")
            .with_description("Add members to a set")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_param(ParameterSchema::variadic("members", TypeSchema::string()))
            .with_return_type(TypeSchema::integer())
            .with_complexity("O(N)")
    }

    /// SREM command schema
    pub fn srem() -> Self {
        Self::new("SREM", "srem")
            .with_description("Remove members from a set")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_param(ParameterSchema::variadic("members", TypeSchema::string()))
            .with_return_type(TypeSchema::integer())
            .with_complexity("O(N)")
    }

    /// SMEMBERS command schema
    pub fn smembers() -> Self {
        Self::new("SMEMBERS", "smembers")
            .with_description("Get all members in a set")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_return_type(TypeSchema::array(TypeSchema::string()))
            .with_complexity("O(N)")
    }

    /// SISMEMBER command schema
    pub fn sismember() -> Self {
        Self::new("SISMEMBER", "sismember")
            .with_description("Check if a member is in a set")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_param(ParameterSchema::required("member", TypeSchema::string()))
            .with_return_type(TypeSchema::integer())
            .with_complexity("O(1)")
    }

    /// ZADD command schema
    pub fn zadd() -> Self {
        Self::new("ZADD", "zadd")
            .with_description("Add members to a sorted set")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_param(ParameterSchema::variadic(
                "score_members",
                TypeSchema::any(),
            ))
            .with_return_type(TypeSchema::integer())
            .with_complexity("O(log(N))")
    }

    /// ZRANGE command schema
    pub fn zrange() -> Self {
        Self::new("ZRANGE", "zrange")
            .with_description("Return a range of members in a sorted set")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_param(ParameterSchema::required("start", TypeSchema::integer()))
            .with_param(ParameterSchema::required("stop", TypeSchema::integer()))
            .with_param(ParameterSchema::optional(
                "withscores",
                TypeSchema::boolean(),
            ))
            .with_return_type(TypeSchema::array(TypeSchema::string()))
            .with_complexity("O(log(N)+M)")
    }

    /// ZSCORE command schema
    pub fn zscore() -> Self {
        Self::new("ZSCORE", "zscore")
            .with_description("Get the score of a member in a sorted set")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_param(ParameterSchema::required("member", TypeSchema::string()))
            .with_return_type(TypeSchema::nullable(TypeSchema::float()))
            .with_complexity("O(1)")
    }

    /// PING command schema
    pub fn ping() -> Self {
        Self::new("PING", "ping")
            .with_description("Ping the server")
            .with_param(ParameterSchema::optional("message", TypeSchema::string()))
            .with_return_type(TypeSchema::string())
            .with_complexity("O(1)")
    }

    /// INFO command schema
    pub fn info() -> Self {
        Self::new("INFO", "info")
            .with_description("Get server information")
            .with_param(ParameterSchema::optional("section", TypeSchema::string()))
            .with_return_type(TypeSchema::string())
            .with_complexity("O(1)")
    }

    /// DBSIZE command schema
    pub fn dbsize() -> Self {
        Self::new("DBSIZE", "dbsize")
            .with_description("Get the number of keys in the database")
            .with_return_type(TypeSchema::integer())
            .with_complexity("O(1)")
    }

    // Ferrite extension commands

    /// VECTOR.ADD command schema
    pub fn vector_add() -> Self {
        Self::new("VECTOR.ADD", "vectorAdd")
            .with_description("Add a vector to an index")
            .with_param(ParameterSchema::required("index", TypeSchema::string()))
            .with_param(ParameterSchema::required("id", TypeSchema::string()))
            .with_param(ParameterSchema::required(
                "vector",
                TypeSchema::array(TypeSchema::float()),
            ))
            .with_param(ParameterSchema::optional("metadata", TypeSchema::object()))
            .with_return_type(TypeSchema::string())
            .with_complexity("O(log(N))")
    }

    /// VECTOR.SEARCH command schema
    pub fn vector_search() -> Self {
        Self::new("VECTOR.SEARCH", "vectorSearch")
            .with_description("Search for similar vectors")
            .with_param(ParameterSchema::required("index", TypeSchema::string()))
            .with_param(ParameterSchema::required(
                "vector",
                TypeSchema::array(TypeSchema::float()),
            ))
            .with_param(ParameterSchema::optional("k", TypeSchema::integer()))
            .with_param(ParameterSchema::optional("filter", TypeSchema::string()))
            .with_return_type(TypeSchema::array(TypeSchema::object()))
            .with_complexity("O(log(N))")
    }
}

/// Parameter schema
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParameterSchema {
    /// Parameter name
    pub name: String,
    /// Parameter type
    pub param_type: TypeSchema,
    /// Description
    pub description: Option<String>,
    /// Whether required
    pub required: bool,
    /// Whether variadic
    pub variadic: bool,
    /// Default value (if optional)
    pub default: Option<String>,
}

impl ParameterSchema {
    /// Create a required parameter
    pub fn required(name: impl Into<String>, param_type: TypeSchema) -> Self {
        Self {
            name: name.into(),
            param_type,
            description: None,
            required: true,
            variadic: false,
            default: None,
        }
    }

    /// Create an optional parameter
    pub fn optional(name: impl Into<String>, param_type: TypeSchema) -> Self {
        Self {
            name: name.into(),
            param_type,
            description: None,
            required: false,
            variadic: false,
            default: None,
        }
    }

    /// Create a variadic parameter
    pub fn variadic(name: impl Into<String>, param_type: TypeSchema) -> Self {
        Self {
            name: name.into(),
            param_type,
            description: None,
            required: true,
            variadic: true,
            default: None,
        }
    }

    /// Add description
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Add default value
    pub fn with_default(mut self, default: impl Into<String>) -> Self {
        self.default = Some(default.into());
        self
    }
}

/// Type schema
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TypeSchema {
    /// Type kind
    pub kind: TypeKind,
    /// Whether nullable
    pub nullable: bool,
    /// Inner type (for arrays, optionals)
    pub inner: Option<Box<TypeSchema>>,
    /// Properties (for objects)
    pub properties: Option<HashMap<String, TypeSchema>>,
}

impl TypeSchema {
    /// String type
    pub fn string() -> Self {
        Self {
            kind: TypeKind::String,
            nullable: false,
            inner: None,
            properties: None,
        }
    }

    /// Integer type
    pub fn integer() -> Self {
        Self {
            kind: TypeKind::Integer,
            nullable: false,
            inner: None,
            properties: None,
        }
    }

    /// Float type
    pub fn float() -> Self {
        Self {
            kind: TypeKind::Float,
            nullable: false,
            inner: None,
            properties: None,
        }
    }

    /// Boolean type
    pub fn boolean() -> Self {
        Self {
            kind: TypeKind::Boolean,
            nullable: false,
            inner: None,
            properties: None,
        }
    }

    /// Array type
    pub fn array(inner: TypeSchema) -> Self {
        Self {
            kind: TypeKind::Array,
            nullable: false,
            inner: Some(Box::new(inner)),
            properties: None,
        }
    }

    /// Object type
    pub fn object() -> Self {
        Self {
            kind: TypeKind::Object,
            nullable: false,
            inner: None,
            properties: Some(HashMap::new()),
        }
    }

    /// Null type
    pub fn null() -> Self {
        Self {
            kind: TypeKind::Null,
            nullable: true,
            inner: None,
            properties: None,
        }
    }

    /// Any type
    pub fn any() -> Self {
        Self {
            kind: TypeKind::Any,
            nullable: true,
            inner: None,
            properties: None,
        }
    }

    /// Make nullable
    pub fn nullable(mut inner: TypeSchema) -> Self {
        inner.nullable = true;
        inner
    }
}

/// Type kind
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TypeKind {
    /// String
    String,
    /// Integer
    Integer,
    /// Float
    Float,
    /// Boolean
    Boolean,
    /// Array
    Array,
    /// Object/Map
    Object,
    /// Null
    Null,
    /// Any/Dynamic
    Any,
    /// Custom type reference
    Reference(String),
}

/// Command example
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandExample {
    /// Example description
    pub description: String,
    /// Code snippet
    pub code: String,
    /// Expected result
    pub result: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_schema_default() {
        let schema = ApiSchema::default();
        assert!(!schema.commands.is_empty());
        assert!(!schema.types.is_empty());
    }

    #[test]
    fn test_command_schema_builder() {
        let cmd = CommandSchema::new("TEST", "test")
            .with_description("Test command")
            .with_param(ParameterSchema::required("key", TypeSchema::string()))
            .with_return_type(TypeSchema::string());

        assert_eq!(cmd.name, "TEST");
        assert_eq!(cmd.parameters.len(), 1);
    }

    #[test]
    fn test_type_schema() {
        let array_type = TypeSchema::array(TypeSchema::string());
        assert!(matches!(array_type.kind, TypeKind::Array));
        assert!(array_type.inner.is_some());
    }
}
