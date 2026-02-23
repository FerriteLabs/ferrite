//! Mobile SDK Binding Generator
//!
//! Generates Swift, Kotlin, and C header bindings for embedding Ferrite in
//! mobile applications. The generator produces idiomatic code for each target
//! language with Foundation types (Swift) and standard Kotlin types.
//!
//! # Example
//!
//! ```no_run
//! use ferrite_core::embedded::mobile_sdk::{MobileSdkGenerator, SdkConfig};
//!
//! let generator = MobileSdkGenerator::new(SdkConfig::default());
//! let swift = generator.generate_swift().unwrap();
//! println!("{}", swift.files[0].content);
//! ```

use std::fmt::Write;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur during SDK generation.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SdkError {
    /// The provided configuration is invalid.
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    /// Code generation failed.
    #[error("generation failed: {0}")]
    GenerationFailed(String),

    /// No API methods were specified or available.
    #[error("no API methods to generate")]
    NoMethods,

    /// An internal error occurred.
    #[error("internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Describes a data type in the generated API.
#[derive(Clone, Debug, PartialEq)]
pub enum ApiType {
    /// UTF-8 string.
    String,
    /// Raw byte buffer.
    Bytes,
    /// 64-bit signed integer.
    Int64,
    /// 64-bit floating-point number.
    Float64,
    /// Boolean.
    Bool,
    /// No return value.
    Void,
    /// Optional wrapper.
    Optional(Box<ApiType>),
    /// Array / list.
    Array(Box<ApiType>),
    /// Dictionary / map.
    Map(Box<ApiType>, Box<ApiType>),
}

impl ApiType {
    /// Swift type representation.
    fn to_swift(&self) -> String {
        match self {
            ApiType::String => "String".into(),
            ApiType::Bytes => "Data".into(),
            ApiType::Int64 => "Int64".into(),
            ApiType::Float64 => "Double".into(),
            ApiType::Bool => "Bool".into(),
            ApiType::Void => "Void".into(),
            ApiType::Optional(inner) => format!("{}?", inner.to_swift()),
            ApiType::Array(inner) => format!("[{}]", inner.to_swift()),
            ApiType::Map(k, v) => format!("[{}: {}]", k.to_swift(), v.to_swift()),
        }
    }

    /// Kotlin type representation.
    fn to_kotlin(&self) -> String {
        match self {
            ApiType::String => "String".into(),
            ApiType::Bytes => "ByteArray".into(),
            ApiType::Int64 => "Long".into(),
            ApiType::Float64 => "Double".into(),
            ApiType::Bool => "Boolean".into(),
            ApiType::Void => "Unit".into(),
            ApiType::Optional(inner) => format!("{}?", inner.to_kotlin()),
            ApiType::Array(inner) => format!("List<{}>", inner.to_kotlin()),
            ApiType::Map(k, v) => format!("Map<{}, {}>", k.to_kotlin(), v.to_kotlin()),
        }
    }

    /// C type representation.
    fn to_c(&self) -> String {
        match self {
            ApiType::String => "const char*".into(),
            ApiType::Bytes => "const uint8_t*".into(),
            ApiType::Int64 => "int64_t".into(),
            ApiType::Float64 => "double".into(),
            ApiType::Bool => "bool".into(),
            ApiType::Void => "void".into(),
            ApiType::Optional(inner) => inner.to_c(),
            ApiType::Array(_) => "ferrite_array_t*".into(),
            ApiType::Map(_, _) => "ferrite_map_t*".into(),
        }
    }
}

// ---------------------------------------------------------------------------
// ApiParam / ApiMethod
// ---------------------------------------------------------------------------

/// Describes a parameter for an API method.
#[derive(Clone, Debug)]
pub struct ApiParam {
    /// Parameter name.
    pub name: String,
    /// Parameter type.
    pub param_type: ApiType,
    /// Whether the parameter is optional.
    pub optional: bool,
}

/// Describes an API method to expose in the generated SDK.
#[derive(Clone, Debug)]
pub struct ApiMethod {
    /// Method name (snake_case by convention).
    pub name: String,
    /// Human-readable description.
    pub description: String,
    /// Method parameters.
    pub params: Vec<ApiParam>,
    /// Return type.
    pub return_type: ApiType,
    /// Whether the method should have an async variant.
    pub is_async: bool,
}

// ---------------------------------------------------------------------------
// Output types
// ---------------------------------------------------------------------------

/// Target language for generated code.
#[derive(Clone, Debug, PartialEq)]
pub enum SdkLanguage {
    /// Apple Swift.
    Swift,
    /// JetBrains Kotlin.
    Kotlin,
    /// C header file.
    CHeader,
}

/// A single generated source file.
#[derive(Clone, Debug)]
pub struct GeneratedFile {
    /// File name (e.g., `Ferrite.swift`).
    pub filename: String,
    /// File content.
    pub content: String,
    /// Whether this is the primary entry-point file.
    pub is_primary: bool,
}

/// Output of the SDK generation process.
#[derive(Clone, Debug)]
pub struct SdkOutput {
    /// Target language.
    pub language: SdkLanguage,
    /// Generated files.
    pub files: Vec<GeneratedFile>,
    /// Number of API methods included.
    pub api_methods: usize,
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the SDK generator.
#[derive(Clone, Debug)]
pub struct SdkConfig {
    /// Package / module name (e.g., `com.example.ferrite`).
    pub package_name: String,
    /// SDK version string.
    pub version: String,
    /// API methods to expose. If empty, [`DEFAULT_API_METHODS`] is used.
    pub api_methods: Vec<ApiMethod>,
    /// Generate async method variants.
    pub include_async: bool,
    /// Include TTL-related methods.
    pub include_ttl: bool,
}

impl Default for SdkConfig {
    fn default() -> Self {
        Self {
            package_name: "Ferrite".into(),
            version: "0.1.0".into(),
            api_methods: Vec::new(),
            include_async: true,
            include_ttl: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Default API methods
// ---------------------------------------------------------------------------

/// Default set of API methods exposed by the SDK.
pub fn default_api_methods() -> Vec<ApiMethod> {
    vec![
        ApiMethod {
            name: "get".into(),
            description: "Retrieve a value by key.".into(),
            params: vec![ApiParam {
                name: "key".into(),
                param_type: ApiType::String,
                optional: false,
            }],
            return_type: ApiType::Optional(Box::new(ApiType::Bytes)),
            is_async: true,
        },
        ApiMethod {
            name: "set".into(),
            description: "Store a key-value pair.".into(),
            params: vec![
                ApiParam {
                    name: "key".into(),
                    param_type: ApiType::String,
                    optional: false,
                },
                ApiParam {
                    name: "value".into(),
                    param_type: ApiType::Bytes,
                    optional: false,
                },
            ],
            return_type: ApiType::Bool,
            is_async: true,
        },
        ApiMethod {
            name: "set_with_ttl".into(),
            description: "Store a key-value pair with a TTL in milliseconds.".into(),
            params: vec![
                ApiParam {
                    name: "key".into(),
                    param_type: ApiType::String,
                    optional: false,
                },
                ApiParam {
                    name: "value".into(),
                    param_type: ApiType::Bytes,
                    optional: false,
                },
                ApiParam {
                    name: "ttl_ms".into(),
                    param_type: ApiType::Int64,
                    optional: false,
                },
            ],
            return_type: ApiType::Bool,
            is_async: true,
        },
        ApiMethod {
            name: "delete".into(),
            description: "Delete a key.".into(),
            params: vec![ApiParam {
                name: "key".into(),
                param_type: ApiType::String,
                optional: false,
            }],
            return_type: ApiType::Bool,
            is_async: true,
        },
        ApiMethod {
            name: "exists".into(),
            description: "Check whether a key exists.".into(),
            params: vec![ApiParam {
                name: "key".into(),
                param_type: ApiType::String,
                optional: false,
            }],
            return_type: ApiType::Bool,
            is_async: true,
        },
        ApiMethod {
            name: "keys".into(),
            description: "Return keys matching a glob pattern.".into(),
            params: vec![ApiParam {
                name: "pattern".into(),
                param_type: ApiType::String,
                optional: false,
            }],
            return_type: ApiType::Array(Box::new(ApiType::String)),
            is_async: true,
        },
        ApiMethod {
            name: "flush".into(),
            description: "Remove all keys.".into(),
            params: vec![],
            return_type: ApiType::Void,
            is_async: true,
        },
        ApiMethod {
            name: "stats".into(),
            description: "Retrieve store statistics.".into(),
            params: vec![],
            return_type: ApiType::Map(Box::new(ApiType::String), Box::new(ApiType::Int64)),
            is_async: false,
        },
    ]
}

/// Constant-like accessor for the default API methods.
pub const DEFAULT_API_METHODS: &[&str] = &[
    "get",
    "set",
    "set_with_ttl",
    "delete",
    "exists",
    "keys",
    "flush",
    "stats",
];

// ---------------------------------------------------------------------------
// Generator
// ---------------------------------------------------------------------------

/// Mobile SDK binding generator.
///
/// Produces Swift, Kotlin, and C header code from a list of [`ApiMethod`]
/// definitions.
pub struct MobileSdkGenerator {
    config: SdkConfig,
    methods: Vec<ApiMethod>,
}

impl MobileSdkGenerator {
    /// Create a new generator. If `config.api_methods` is empty the
    /// [`default_api_methods`] set is used.
    pub fn new(config: SdkConfig) -> Self {
        let methods = if config.api_methods.is_empty() {
            let mut m = default_api_methods();
            if !config.include_ttl {
                m.retain(|method| method.name != "set_with_ttl");
            }
            m
        } else {
            config.api_methods.clone()
        };
        Self { config, methods }
    }

    /// Return the list of API methods that will be generated.
    pub fn list_api_methods(&self) -> Vec<ApiMethod> {
        self.methods.clone()
    }

    // -- Swift ------------------------------------------------------------

    /// Generate Swift SDK bindings.
    pub fn generate_swift(&self) -> Result<SdkOutput, SdkError> {
        if self.methods.is_empty() {
            return Err(SdkError::NoMethods);
        }

        let mut code = String::new();
        writeln!(
            code,
            "// Auto-generated by Ferrite SDK Generator v{}",
            self.config.version
        )
        .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "// Do not edit manually.\n")
            .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "import Foundation\n").map_err(|e| SdkError::Internal(e.to_string()))?;

        // Class header
        writeln!(code, "/// Ferrite embedded database client.")
            .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "public class {} {{", self.config.package_name)
            .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "    private let handle: OpaquePointer\n")
            .map_err(|e| SdkError::Internal(e.to_string()))?;

        // Initializer
        writeln!(code, "    public init() {{").map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "        self.handle = ferrite_open(nil)")
            .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "    }}\n").map_err(|e| SdkError::Internal(e.to_string()))?;

        // Deinit
        writeln!(code, "    deinit {{").map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "        ferrite_close(handle)")
            .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "    }}\n").map_err(|e| SdkError::Internal(e.to_string()))?;

        for method in &self.methods {
            self.write_swift_method(&mut code, method)
                .map_err(|e| SdkError::GenerationFailed(e.to_string()))?;
        }

        writeln!(code, "}}").map_err(|e| SdkError::Internal(e.to_string()))?;

        let filename = format!("{}.swift", self.config.package_name);
        Ok(SdkOutput {
            language: SdkLanguage::Swift,
            files: vec![GeneratedFile {
                filename,
                content: code,
                is_primary: true,
            }],
            api_methods: self.methods.len(),
        })
    }

    fn write_swift_method(&self, code: &mut String, method: &ApiMethod) -> std::fmt::Result {
        let swift_name = to_camel_case(&method.name);
        let ret = method.return_type.to_swift();

        // Doc comment
        writeln!(code, "    /// {}", method.description)?;

        // Signature
        let params: Vec<String> = method
            .params
            .iter()
            .map(|p| {
                let t = if p.optional {
                    format!("{}?", p.param_type.to_swift())
                } else {
                    p.param_type.to_swift()
                };
                format!("{}: {}", p.name, t)
            })
            .collect();
        let params_str = params.join(", ");

        if self.config.include_async && method.is_async {
            writeln!(
                code,
                "    public func {swift_name}({params_str}) async -> {ret} {{"
            )?;
            writeln!(code, "        // TODO: FFI bridge implementation")?;
            self.write_swift_return_default(code, &method.return_type)?;
            writeln!(code, "    }}\n")?;
        } else {
            writeln!(
                code,
                "    public func {swift_name}({params_str}) -> {ret} {{"
            )?;
            writeln!(code, "        // TODO: FFI bridge implementation")?;
            self.write_swift_return_default(code, &method.return_type)?;
            writeln!(code, "    }}\n")?;
        }
        Ok(())
    }

    fn write_swift_return_default(
        &self,
        code: &mut String,
        return_type: &ApiType,
    ) -> std::fmt::Result {
        match return_type {
            ApiType::Void => {}
            ApiType::Bool => writeln!(code, "        return false")?,
            ApiType::Int64 => writeln!(code, "        return 0")?,
            ApiType::Float64 => writeln!(code, "        return 0.0")?,
            ApiType::String => writeln!(code, "        return \"\"")?,
            ApiType::Bytes => writeln!(code, "        return Data()")?,
            ApiType::Optional(_) => writeln!(code, "        return nil")?,
            ApiType::Array(_) => writeln!(code, "        return []")?,
            ApiType::Map(_, _) => writeln!(code, "        return [:]")?,
        }
        Ok(())
    }

    // -- Kotlin -----------------------------------------------------------

    /// Generate Kotlin SDK bindings.
    pub fn generate_kotlin(&self) -> Result<SdkOutput, SdkError> {
        if self.methods.is_empty() {
            return Err(SdkError::NoMethods);
        }

        let mut code = String::new();
        writeln!(
            code,
            "// Auto-generated by Ferrite SDK Generator v{}",
            self.config.version
        )
        .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "// Do not edit manually.\n")
            .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(
            code,
            "package {}\n",
            self.config.package_name.to_lowercase()
        )
        .map_err(|e| SdkError::Internal(e.to_string()))?;

        // Class header
        writeln!(code, "/**").map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, " * Ferrite embedded database client.")
            .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, " */").map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(
            code,
            "class {} : AutoCloseable {{",
            self.config.package_name
        )
        .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "    private val handle: Long\n")
            .map_err(|e| SdkError::Internal(e.to_string()))?;

        // Constructor
        writeln!(code, "    init {{").map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "        System.loadLibrary(\"ferrite\")")
            .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "        handle = nativeOpen()")
            .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "    }}\n").map_err(|e| SdkError::Internal(e.to_string()))?;

        // close
        writeln!(code, "    override fun close() {{")
            .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "        nativeClose(handle)")
            .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "    }}\n").map_err(|e| SdkError::Internal(e.to_string()))?;

        for method in &self.methods {
            self.write_kotlin_method(&mut code, method)
                .map_err(|e| SdkError::GenerationFailed(e.to_string()))?;
        }

        // Native JNI declarations
        writeln!(code, "    // -- JNI native methods --")
            .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "    private external fun nativeOpen(): Long")
            .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "    private external fun nativeClose(handle: Long)")
            .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "}}").map_err(|e| SdkError::Internal(e.to_string()))?;

        let filename = format!("{}.kt", self.config.package_name);
        Ok(SdkOutput {
            language: SdkLanguage::Kotlin,
            files: vec![GeneratedFile {
                filename,
                content: code,
                is_primary: true,
            }],
            api_methods: self.methods.len(),
        })
    }

    fn write_kotlin_method(&self, code: &mut String, method: &ApiMethod) -> std::fmt::Result {
        let kotlin_name = to_camel_case(&method.name);
        let ret = method.return_type.to_kotlin();

        // Doc comment
        writeln!(code, "    /**")?;
        writeln!(code, "     * {}", method.description)?;
        writeln!(code, "     */")?;

        let params: Vec<String> = method
            .params
            .iter()
            .map(|p| {
                let t = if p.optional {
                    format!("{}?", p.param_type.to_kotlin())
                } else {
                    p.param_type.to_kotlin()
                };
                format!("{}: {}", p.name, t)
            })
            .collect();
        let params_str = params.join(", ");

        if self.config.include_async && method.is_async {
            writeln!(
                code,
                "    suspend fun {kotlin_name}({params_str}): {ret} {{"
            )?;
        } else {
            writeln!(code, "    fun {kotlin_name}({params_str}): {ret} {{")?;
        }
        writeln!(code, "        // TODO: JNI bridge implementation")?;
        self.write_kotlin_return_default(code, &method.return_type)?;
        writeln!(code, "    }}\n")?;
        Ok(())
    }

    fn write_kotlin_return_default(
        &self,
        code: &mut String,
        return_type: &ApiType,
    ) -> std::fmt::Result {
        match return_type {
            ApiType::Void => {}
            ApiType::Bool => writeln!(code, "        return false")?,
            ApiType::Int64 => writeln!(code, "        return 0L")?,
            ApiType::Float64 => writeln!(code, "        return 0.0")?,
            ApiType::String => writeln!(code, "        return \"\"")?,
            ApiType::Bytes => writeln!(code, "        return ByteArray(0)")?,
            ApiType::Optional(_) => writeln!(code, "        return null")?,
            ApiType::Array(_) => writeln!(code, "        return emptyList()")?,
            ApiType::Map(_, _) => writeln!(code, "        return emptyMap()")?,
        }
        Ok(())
    }

    // -- C Header ---------------------------------------------------------

    /// Generate a C header file defining the FFI interface.
    pub fn generate_c_header(&self) -> Result<SdkOutput, SdkError> {
        if self.methods.is_empty() {
            return Err(SdkError::NoMethods);
        }

        let guard = format!("{}_H", self.config.package_name.to_uppercase());

        let mut code = String::new();
        writeln!(
            code,
            "// Auto-generated by Ferrite SDK Generator v{}",
            self.config.version
        )
        .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "// Do not edit manually.\n")
            .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "#ifndef {guard}").map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "#define {guard}\n").map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "#include <stdint.h>").map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "#include <stdbool.h>").map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "#include <stddef.h>\n").map_err(|e| SdkError::Internal(e.to_string()))?;

        writeln!(code, "#ifdef __cplusplus").map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "extern \"C\" {{").map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "#endif\n").map_err(|e| SdkError::Internal(e.to_string()))?;

        // Opaque handle
        writeln!(code, "/// Opaque database handle.")
            .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "typedef struct ferrite_db ferrite_db_t;\n")
            .map_err(|e| SdkError::Internal(e.to_string()))?;

        // Helper types
        writeln!(code, "/// Byte buffer returned by the library.")
            .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "typedef struct {{").map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "    const uint8_t* data;")
            .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "    size_t len;").map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "}} ferrite_bytes_t;\n").map_err(|e| SdkError::Internal(e.to_string()))?;

        writeln!(
            code,
            "typedef struct {{ void* _ptr; size_t _len; }} ferrite_array_t;"
        )
        .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(
            code,
            "typedef struct {{ void* _ptr; size_t _len; }} ferrite_map_t;\n"
        )
        .map_err(|e| SdkError::Internal(e.to_string()))?;

        // Lifecycle
        writeln!(code, "/// Open a new database handle.")
            .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "ferrite_db_t* ferrite_open(const char* path);")
            .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "/// Close a database handle.")
            .map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "void ferrite_close(ferrite_db_t* db);\n")
            .map_err(|e| SdkError::Internal(e.to_string()))?;

        // Methods
        for method in &self.methods {
            self.write_c_method(&mut code, method)
                .map_err(|e| SdkError::GenerationFailed(e.to_string()))?;
        }

        writeln!(code, "#ifdef __cplusplus").map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "}}").map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "#endif\n").map_err(|e| SdkError::Internal(e.to_string()))?;
        writeln!(code, "#endif // {guard}").map_err(|e| SdkError::Internal(e.to_string()))?;

        let filename = format!("{}.h", self.config.package_name.to_lowercase());
        Ok(SdkOutput {
            language: SdkLanguage::CHeader,
            files: vec![GeneratedFile {
                filename,
                content: code,
                is_primary: true,
            }],
            api_methods: self.methods.len(),
        })
    }

    fn write_c_method(&self, code: &mut String, method: &ApiMethod) -> std::fmt::Result {
        let c_name = format!("ferrite_{}", method.name);
        let ret = method.return_type.to_c();

        writeln!(code, "/// {}", method.description)?;

        let mut params = vec!["ferrite_db_t* db".to_string()];
        for p in &method.params {
            let c_type = p.param_type.to_c();
            if matches!(p.param_type, ApiType::Bytes) {
                params.push(format!("{} {}", c_type, p.name));
                params.push(format!("size_t {}_len", p.name));
            } else {
                params.push(format!("{} {}", c_type, p.name));
            }
        }

        // For methods returning bytes, use out-params
        if matches!(method.return_type, ApiType::Optional(ref inner) if matches!(**inner, ApiType::Bytes))
        {
            params.push("ferrite_bytes_t* out".to_string());
            writeln!(code, "bool {c_name}({});", params.join(", "))?;
        } else {
            writeln!(code, "{ret} {c_name}({});", params.join(", "))?;
        }
        writeln!(code)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert `snake_case` to `camelCase`.
fn to_camel_case(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut capitalize_next = false;
    for ch in s.chars() {
        if ch == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.extend(ch.to_uppercase());
            capitalize_next = false;
        } else {
            result.push(ch);
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- default API methods ----------------------------------------------

    #[test]
    fn test_default_api_methods_count() {
        let methods = default_api_methods();
        assert_eq!(methods.len(), DEFAULT_API_METHODS.len());
    }

    #[test]
    fn test_default_api_methods_names() {
        let methods = default_api_methods();
        let names: Vec<&str> = methods.iter().map(|m| m.name.as_str()).collect();
        for expected in DEFAULT_API_METHODS {
            assert!(
                names.contains(expected),
                "missing default method: {expected}"
            );
        }
    }

    // -- Swift generation -------------------------------------------------

    #[test]
    fn test_generate_swift_default() {
        let gen = MobileSdkGenerator::new(SdkConfig::default());
        let output = gen.generate_swift().unwrap();

        assert_eq!(output.language, SdkLanguage::Swift);
        assert_eq!(output.files.len(), 1);
        assert!(output.files[0].is_primary);
        assert!(output.files[0].filename.ends_with(".swift"));
        assert_eq!(output.api_methods, default_api_methods().len());

        let code = &output.files[0].content;
        assert!(code.contains("import Foundation"));
        assert!(code.contains("public class Ferrite"));
        assert!(code.contains("func get("));
        assert!(code.contains("func set("));
        assert!(code.contains("func delete("));
        assert!(code.contains("async"));
    }

    #[test]
    fn test_generate_swift_no_async() {
        let config = SdkConfig {
            include_async: false,
            ..SdkConfig::default()
        };
        let gen = MobileSdkGenerator::new(config);
        let output = gen.generate_swift().unwrap();
        let code = &output.files[0].content;
        assert!(!code.contains("async"));
    }

    #[test]
    fn test_generate_swift_no_ttl() {
        let config = SdkConfig {
            include_ttl: false,
            ..SdkConfig::default()
        };
        let gen = MobileSdkGenerator::new(config);
        let output = gen.generate_swift().unwrap();
        let code = &output.files[0].content;
        assert!(!code.contains("setWithTtl"));
    }

    // -- Kotlin generation ------------------------------------------------

    #[test]
    fn test_generate_kotlin_default() {
        let gen = MobileSdkGenerator::new(SdkConfig::default());
        let output = gen.generate_kotlin().unwrap();

        assert_eq!(output.language, SdkLanguage::Kotlin);
        assert_eq!(output.files.len(), 1);
        assert!(output.files[0].is_primary);
        assert!(output.files[0].filename.ends_with(".kt"));
        assert_eq!(output.api_methods, default_api_methods().len());

        let code = &output.files[0].content;
        assert!(code.contains("package ferrite"));
        assert!(code.contains("class Ferrite"));
        assert!(code.contains("fun get("));
        assert!(code.contains("fun set("));
        assert!(code.contains("fun delete("));
        assert!(code.contains("suspend"));
        assert!(code.contains("AutoCloseable"));
    }

    #[test]
    fn test_generate_kotlin_no_async() {
        let config = SdkConfig {
            include_async: false,
            ..SdkConfig::default()
        };
        let gen = MobileSdkGenerator::new(config);
        let output = gen.generate_kotlin().unwrap();
        let code = &output.files[0].content;
        assert!(!code.contains("suspend"));
    }

    // -- C header generation ----------------------------------------------

    #[test]
    fn test_generate_c_header_default() {
        let gen = MobileSdkGenerator::new(SdkConfig::default());
        let output = gen.generate_c_header().unwrap();

        assert_eq!(output.language, SdkLanguage::CHeader);
        assert_eq!(output.files.len(), 1);
        assert!(output.files[0].is_primary);
        assert!(output.files[0].filename.ends_with(".h"));

        let code = &output.files[0].content;
        assert!(code.contains("#ifndef FERRITE_H"));
        assert!(code.contains("#define FERRITE_H"));
        assert!(code.contains("#endif"));
        assert!(code.contains("ferrite_db_t"));
        assert!(code.contains("ferrite_open"));
        assert!(code.contains("ferrite_close"));
        assert!(code.contains("ferrite_get"));
        assert!(code.contains("ferrite_set"));
        assert!(code.contains("ferrite_delete"));
        assert!(code.contains("stdint.h"));
        assert!(code.contains("stdbool.h"));
    }

    // -- error cases ------------------------------------------------------

    #[test]
    fn test_no_methods_error() {
        let config = SdkConfig {
            api_methods: vec![],
            include_ttl: true,
            ..SdkConfig::default()
        };
        // Override methods to empty by providing an explicit empty + custom flag
        let gen = MobileSdkGenerator {
            config: config.clone(),
            methods: vec![],
        };
        assert!(matches!(gen.generate_swift(), Err(SdkError::NoMethods)));
        assert!(matches!(gen.generate_kotlin(), Err(SdkError::NoMethods)));
        assert!(matches!(gen.generate_c_header(), Err(SdkError::NoMethods)));
    }

    // -- custom config ----------------------------------------------------

    #[test]
    fn test_custom_config() {
        let config = SdkConfig {
            package_name: "MyApp".into(),
            version: "2.0.0".into(),
            api_methods: vec![ApiMethod {
                name: "ping".into(),
                description: "Health check.".into(),
                params: vec![],
                return_type: ApiType::Bool,
                is_async: false,
            }],
            include_async: false,
            include_ttl: false,
        };
        let gen = MobileSdkGenerator::new(config);
        assert_eq!(gen.list_api_methods().len(), 1);
        assert_eq!(gen.list_api_methods()[0].name, "ping");

        let swift = gen.generate_swift().unwrap();
        assert!(swift.files[0].content.contains("class MyApp"));
        assert!(swift.files[0].content.contains("v2.0.0"));
        assert_eq!(swift.api_methods, 1);

        let kotlin = gen.generate_kotlin().unwrap();
        assert!(kotlin.files[0].content.contains("class MyApp"));

        let c = gen.generate_c_header().unwrap();
        assert!(c.files[0].content.contains("MYAPP_H"));
    }

    // -- camelCase helper -------------------------------------------------

    #[test]
    fn test_to_camel_case() {
        assert_eq!(to_camel_case("get"), "get");
        assert_eq!(to_camel_case("set_with_ttl"), "setWithTtl");
        assert_eq!(to_camel_case("a_b_c"), "aBC");
    }

    // -- ApiType rendering ------------------------------------------------

    #[test]
    fn test_api_type_swift() {
        assert_eq!(ApiType::String.to_swift(), "String");
        assert_eq!(ApiType::Bytes.to_swift(), "Data");
        assert_eq!(
            ApiType::Optional(Box::new(ApiType::String)).to_swift(),
            "String?"
        );
        assert_eq!(
            ApiType::Array(Box::new(ApiType::Int64)).to_swift(),
            "[Int64]"
        );
        assert_eq!(
            ApiType::Map(Box::new(ApiType::String), Box::new(ApiType::Bool)).to_swift(),
            "[String: Bool]"
        );
    }

    #[test]
    fn test_api_type_kotlin() {
        assert_eq!(ApiType::String.to_kotlin(), "String");
        assert_eq!(ApiType::Bytes.to_kotlin(), "ByteArray");
        assert_eq!(
            ApiType::Optional(Box::new(ApiType::String)).to_kotlin(),
            "String?"
        );
        assert_eq!(
            ApiType::Array(Box::new(ApiType::Int64)).to_kotlin(),
            "List<Long>"
        );
    }

    #[test]
    fn test_api_type_c() {
        assert_eq!(ApiType::String.to_c(), "const char*");
        assert_eq!(ApiType::Bytes.to_c(), "const uint8_t*");
        assert_eq!(ApiType::Int64.to_c(), "int64_t");
        assert_eq!(ApiType::Bool.to_c(), "bool");
        assert_eq!(ApiType::Void.to_c(), "void");
    }
}
