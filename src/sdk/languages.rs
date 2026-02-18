//! Language Support
//!
//! Language-specific code generation support.

use super::schema::{CommandSchema, TypeKind, TypeSchema};
use serde::{Deserialize, Serialize};

/// Supported programming languages for SDK generation
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Language {
    /// TypeScript/JavaScript
    TypeScript,
    /// Python
    Python,
    /// Go
    Go,
    /// Rust
    Rust,
    /// Java
    Java,
    /// C#
    CSharp,
    /// PHP
    Php,
    /// Ruby
    Ruby,
}

impl Language {
    /// Get all supported languages
    pub fn all() -> Vec<Language> {
        vec![
            Language::TypeScript,
            Language::Python,
            Language::Go,
            Language::Rust,
            Language::Java,
            Language::CSharp,
            Language::Php,
            Language::Ruby,
        ]
    }

    /// Get language file extension
    pub fn extension(&self) -> &'static str {
        match self {
            Language::TypeScript => "ts",
            Language::Python => "py",
            Language::Go => "go",
            Language::Rust => "rs",
            Language::Java => "java",
            Language::CSharp => "cs",
            Language::Php => "php",
            Language::Ruby => "rb",
        }
    }

    /// Get language name
    pub fn name(&self) -> &'static str {
        match self {
            Language::TypeScript => "TypeScript",
            Language::Python => "Python",
            Language::Go => "Go",
            Language::Rust => "Rust",
            Language::Java => "Java",
            Language::CSharp => "C#",
            Language::Php => "PHP",
            Language::Ruby => "Ruby",
        }
    }

    /// Get package manager command
    pub fn package_manager(&self) -> &'static str {
        match self {
            Language::TypeScript => "npm",
            Language::Python => "pip",
            Language::Go => "go mod",
            Language::Rust => "cargo",
            Language::Java => "maven",
            Language::CSharp => "nuget",
            Language::Php => "composer",
            Language::Ruby => "gem",
        }
    }

    /// Get type mapper for this language
    pub fn type_mapper(&self) -> Box<dyn TypeMapper> {
        match self {
            Language::TypeScript => Box::new(TypeScriptMapper),
            Language::Python => Box::new(PythonMapper),
            Language::Go => Box::new(GoMapper),
            Language::Rust => Box::new(RustMapper),
            Language::Java => Box::new(JavaMapper),
            Language::CSharp => Box::new(CSharpMapper),
            Language::Php => Box::new(PhpMapper),
            Language::Ruby => Box::new(RubyMapper),
        }
    }
}

/// Trait for mapping types to language-specific representations
pub trait TypeMapper: Send + Sync {
    /// Map a type schema to language-specific type string
    fn map_type(&self, schema: &TypeSchema) -> String;

    /// Map a parameter name to language-specific name
    fn map_param_name(&self, name: &str) -> String;

    /// Map a method name to language-specific name
    fn map_method_name(&self, name: &str) -> String;

    /// Generate method signature
    fn generate_signature(&self, command: &CommandSchema) -> String;

    /// Generate doc comment
    fn generate_doc(&self, description: &str) -> String;

    /// Get null/none literal
    fn null_literal(&self) -> &'static str;

    /// Get true literal
    fn true_literal(&self) -> &'static str;

    /// Get false literal
    fn false_literal(&self) -> &'static str;
}

/// TypeScript type mapper
pub struct TypeScriptMapper;

impl TypeMapper for TypeScriptMapper {
    fn map_type(&self, schema: &TypeSchema) -> String {
        let base = match &schema.kind {
            TypeKind::String => "string".to_string(),
            TypeKind::Integer => "number".to_string(),
            TypeKind::Float => "number".to_string(),
            TypeKind::Boolean => "boolean".to_string(),
            TypeKind::Array => {
                if let Some(inner) = &schema.inner {
                    format!("{}[]", self.map_type(inner))
                } else {
                    "any[]".to_string()
                }
            }
            TypeKind::Object => "Record<string, any>".to_string(),
            TypeKind::Null => "null".to_string(),
            TypeKind::Any => "any".to_string(),
            TypeKind::Reference(name) => name.clone(),
        };

        if schema.nullable && !matches!(schema.kind, TypeKind::Null | TypeKind::Any) {
            format!("{} | null", base)
        } else {
            base
        }
    }

    fn map_param_name(&self, name: &str) -> String {
        to_camel_case(name)
    }

    fn map_method_name(&self, name: &str) -> String {
        to_camel_case(name)
    }

    fn generate_signature(&self, command: &CommandSchema) -> String {
        let params: Vec<String> = command
            .parameters
            .iter()
            .map(|p| {
                let param_name = self.map_param_name(&p.name);
                let param_type = self.map_type(&p.param_type);
                if p.required {
                    if p.variadic {
                        format!("...{}: {}[]", param_name, param_type)
                    } else {
                        format!("{}: {}", param_name, param_type)
                    }
                } else {
                    format!("{}?: {}", param_name, param_type)
                }
            })
            .collect();

        let return_type = self.map_type(&command.return_type);
        format!(
            "async {}({}): Promise<{}>",
            self.map_method_name(&command.method_name),
            params.join(", "),
            return_type
        )
    }

    fn generate_doc(&self, description: &str) -> String {
        format!("/** {} */", description)
    }

    fn null_literal(&self) -> &'static str {
        "null"
    }
    fn true_literal(&self) -> &'static str {
        "true"
    }
    fn false_literal(&self) -> &'static str {
        "false"
    }
}

/// Python type mapper
pub struct PythonMapper;

impl TypeMapper for PythonMapper {
    fn map_type(&self, schema: &TypeSchema) -> String {
        let base = match &schema.kind {
            TypeKind::String => "str".to_string(),
            TypeKind::Integer => "int".to_string(),
            TypeKind::Float => "float".to_string(),
            TypeKind::Boolean => "bool".to_string(),
            TypeKind::Array => {
                if let Some(inner) = &schema.inner {
                    format!("List[{}]", self.map_type(inner))
                } else {
                    "List[Any]".to_string()
                }
            }
            TypeKind::Object => "Dict[str, Any]".to_string(),
            TypeKind::Null => "None".to_string(),
            TypeKind::Any => "Any".to_string(),
            TypeKind::Reference(name) => name.clone(),
        };

        if schema.nullable && !matches!(schema.kind, TypeKind::Null | TypeKind::Any) {
            format!("Optional[{}]", base)
        } else {
            base
        }
    }

    fn map_param_name(&self, name: &str) -> String {
        to_snake_case(name)
    }

    fn map_method_name(&self, name: &str) -> String {
        to_snake_case(name)
    }

    fn generate_signature(&self, command: &CommandSchema) -> String {
        let params: Vec<String> = command
            .parameters
            .iter()
            .map(|p| {
                let param_name = self.map_param_name(&p.name);
                let param_type = self.map_type(&p.param_type);
                if p.required {
                    if p.variadic {
                        format!("*{}: {}", param_name, param_type)
                    } else {
                        format!("{}: {}", param_name, param_type)
                    }
                } else {
                    format!("{}: {} = None", param_name, param_type)
                }
            })
            .collect();

        let return_type = self.map_type(&command.return_type);
        format!(
            "async def {}(self, {}) -> {}:",
            self.map_method_name(&command.method_name),
            params.join(", "),
            return_type
        )
    }

    fn generate_doc(&self, description: &str) -> String {
        format!("\"\"\"{}\"\"\"", description)
    }

    fn null_literal(&self) -> &'static str {
        "None"
    }
    fn true_literal(&self) -> &'static str {
        "True"
    }
    fn false_literal(&self) -> &'static str {
        "False"
    }
}

/// Go type mapper
pub struct GoMapper;

impl TypeMapper for GoMapper {
    fn map_type(&self, schema: &TypeSchema) -> String {
        let base = match &schema.kind {
            TypeKind::String => "string".to_string(),
            TypeKind::Integer => "int64".to_string(),
            TypeKind::Float => "float64".to_string(),
            TypeKind::Boolean => "bool".to_string(),
            TypeKind::Array => {
                if let Some(inner) = &schema.inner {
                    format!("[]{}", self.map_type(inner))
                } else {
                    "[]interface{}".to_string()
                }
            }
            TypeKind::Object => "map[string]interface{}".to_string(),
            TypeKind::Null => "nil".to_string(),
            TypeKind::Any => "interface{}".to_string(),
            TypeKind::Reference(name) => name.clone(),
        };

        if schema.nullable
            && !matches!(
                schema.kind,
                TypeKind::Null | TypeKind::Any | TypeKind::Object | TypeKind::Array
            )
        {
            format!("*{}", base)
        } else {
            base
        }
    }

    fn map_param_name(&self, name: &str) -> String {
        to_camel_case(name)
    }

    fn map_method_name(&self, name: &str) -> String {
        to_pascal_case(name)
    }

    fn generate_signature(&self, command: &CommandSchema) -> String {
        let params: Vec<String> = command
            .parameters
            .iter()
            .map(|p| {
                let param_name = self.map_param_name(&p.name);
                let param_type = self.map_type(&p.param_type);
                if p.variadic {
                    format!("{} ...{}", param_name, param_type)
                } else {
                    format!("{} {}", param_name, param_type)
                }
            })
            .collect();

        let return_type = self.map_type(&command.return_type);
        format!(
            "func (c *Client) {}(ctx context.Context, {}) ({}, error)",
            self.map_method_name(&command.method_name),
            params.join(", "),
            return_type
        )
    }

    fn generate_doc(&self, description: &str) -> String {
        format!("// {}", description)
    }

    fn null_literal(&self) -> &'static str {
        "nil"
    }
    fn true_literal(&self) -> &'static str {
        "true"
    }
    fn false_literal(&self) -> &'static str {
        "false"
    }
}

/// Rust type mapper
pub struct RustMapper;

impl TypeMapper for RustMapper {
    fn map_type(&self, schema: &TypeSchema) -> String {
        let base = match &schema.kind {
            TypeKind::String => "String".to_string(),
            TypeKind::Integer => "i64".to_string(),
            TypeKind::Float => "f64".to_string(),
            TypeKind::Boolean => "bool".to_string(),
            TypeKind::Array => {
                if let Some(inner) = &schema.inner {
                    format!("Vec<{}>", self.map_type(inner))
                } else {
                    "Vec<Value>".to_string()
                }
            }
            TypeKind::Object => "HashMap<String, Value>".to_string(),
            TypeKind::Null => "()".to_string(),
            TypeKind::Any => "Value".to_string(),
            TypeKind::Reference(name) => name.clone(),
        };

        if schema.nullable && !matches!(schema.kind, TypeKind::Null | TypeKind::Any) {
            format!("Option<{}>", base)
        } else {
            base
        }
    }

    fn map_param_name(&self, name: &str) -> String {
        to_snake_case(name)
    }

    fn map_method_name(&self, name: &str) -> String {
        to_snake_case(name)
    }

    fn generate_signature(&self, command: &CommandSchema) -> String {
        let params: Vec<String> = command
            .parameters
            .iter()
            .map(|p| {
                let param_name = self.map_param_name(&p.name);
                let param_type = self.map_type(&p.param_type);
                if p.variadic {
                    format!("{}: impl IntoIterator<Item = {}>", param_name, param_type)
                } else if p.required {
                    format!("{}: {}", param_name, param_type)
                } else {
                    format!("{}: Option<{}>", param_name, param_type)
                }
            })
            .collect();

        let return_type = self.map_type(&command.return_type);
        format!(
            "pub async fn {}(&self, {}) -> Result<{}>",
            self.map_method_name(&command.method_name),
            params.join(", "),
            return_type
        )
    }

    fn generate_doc(&self, description: &str) -> String {
        format!("/// {}", description)
    }

    fn null_literal(&self) -> &'static str {
        "None"
    }
    fn true_literal(&self) -> &'static str {
        "true"
    }
    fn false_literal(&self) -> &'static str {
        "false"
    }
}

/// Java type mapper
pub struct JavaMapper;

impl TypeMapper for JavaMapper {
    fn map_type(&self, schema: &TypeSchema) -> String {
        let base = match &schema.kind {
            TypeKind::String => "String".to_string(),
            TypeKind::Integer => "Long".to_string(),
            TypeKind::Float => "Double".to_string(),
            TypeKind::Boolean => "Boolean".to_string(),
            TypeKind::Array => {
                if let Some(inner) = &schema.inner {
                    format!("List<{}>", self.map_type(inner))
                } else {
                    "List<Object>".to_string()
                }
            }
            TypeKind::Object => "Map<String, Object>".to_string(),
            TypeKind::Null => "Void".to_string(),
            TypeKind::Any => "Object".to_string(),
            TypeKind::Reference(name) => name.clone(),
        };

        base
    }

    fn map_param_name(&self, name: &str) -> String {
        to_camel_case(name)
    }

    fn map_method_name(&self, name: &str) -> String {
        to_camel_case(name)
    }

    fn generate_signature(&self, command: &CommandSchema) -> String {
        let params: Vec<String> = command
            .parameters
            .iter()
            .map(|p| {
                let param_name = self.map_param_name(&p.name);
                let param_type = self.map_type(&p.param_type);
                if p.variadic {
                    format!("{}... {}", param_type, param_name)
                } else {
                    format!("{} {}", param_type, param_name)
                }
            })
            .collect();

        let return_type = self.map_type(&command.return_type);
        format!(
            "public CompletableFuture<{}> {}({})",
            return_type,
            self.map_method_name(&command.method_name),
            params.join(", ")
        )
    }

    fn generate_doc(&self, description: &str) -> String {
        format!("/** {} */", description)
    }

    fn null_literal(&self) -> &'static str {
        "null"
    }
    fn true_literal(&self) -> &'static str {
        "true"
    }
    fn false_literal(&self) -> &'static str {
        "false"
    }
}

/// C# type mapper
pub struct CSharpMapper;

impl TypeMapper for CSharpMapper {
    fn map_type(&self, schema: &TypeSchema) -> String {
        let base = match &schema.kind {
            TypeKind::String => "string".to_string(),
            TypeKind::Integer => "long".to_string(),
            TypeKind::Float => "double".to_string(),
            TypeKind::Boolean => "bool".to_string(),
            TypeKind::Array => {
                if let Some(inner) = &schema.inner {
                    format!("List<{}>", self.map_type(inner))
                } else {
                    "List<object>".to_string()
                }
            }
            TypeKind::Object => "Dictionary<string, object>".to_string(),
            TypeKind::Null => "void".to_string(),
            TypeKind::Any => "object".to_string(),
            TypeKind::Reference(name) => name.clone(),
        };

        if schema.nullable
            && !matches!(
                schema.kind,
                TypeKind::Null | TypeKind::Any | TypeKind::Object | TypeKind::Array
            )
        {
            format!("{}?", base)
        } else {
            base
        }
    }

    fn map_param_name(&self, name: &str) -> String {
        to_camel_case(name)
    }

    fn map_method_name(&self, name: &str) -> String {
        to_pascal_case(name)
    }

    fn generate_signature(&self, command: &CommandSchema) -> String {
        let params: Vec<String> = command
            .parameters
            .iter()
            .map(|p| {
                let param_name = self.map_param_name(&p.name);
                let param_type = self.map_type(&p.param_type);
                if p.variadic {
                    format!("params {}[] {}", param_type, param_name)
                } else {
                    format!("{} {}", param_type, param_name)
                }
            })
            .collect();

        let return_type = self.map_type(&command.return_type);
        format!(
            "public async Task<{}> {}Async({})",
            return_type,
            self.map_method_name(&command.method_name),
            params.join(", ")
        )
    }

    fn generate_doc(&self, description: &str) -> String {
        format!("/// <summary>{}</summary>", description)
    }

    fn null_literal(&self) -> &'static str {
        "null"
    }
    fn true_literal(&self) -> &'static str {
        "true"
    }
    fn false_literal(&self) -> &'static str {
        "false"
    }
}

/// PHP type mapper
pub struct PhpMapper;

impl TypeMapper for PhpMapper {
    fn map_type(&self, schema: &TypeSchema) -> String {
        match &schema.kind {
            TypeKind::String => "string".to_string(),
            TypeKind::Integer => "int".to_string(),
            TypeKind::Float => "float".to_string(),
            TypeKind::Boolean => "bool".to_string(),
            TypeKind::Array => "array".to_string(),
            TypeKind::Object => "array".to_string(),
            TypeKind::Null => "null".to_string(),
            TypeKind::Any => "mixed".to_string(),
            TypeKind::Reference(name) => name.clone(),
        }
    }

    fn map_param_name(&self, name: &str) -> String {
        format!("${}", to_camel_case(name))
    }

    fn map_method_name(&self, name: &str) -> String {
        to_camel_case(name)
    }

    fn generate_signature(&self, command: &CommandSchema) -> String {
        let params: Vec<String> = command
            .parameters
            .iter()
            .map(|p| {
                let param_name = self.map_param_name(&p.name);
                let param_type = self.map_type(&p.param_type);
                if p.variadic {
                    format!("{} ...{}", param_type, param_name)
                } else if p.required {
                    format!("{} {}", param_type, param_name)
                } else {
                    format!("?{} {} = null", param_type, param_name)
                }
            })
            .collect();

        let return_type = self.map_type(&command.return_type);
        format!(
            "public function {}({}): {}",
            self.map_method_name(&command.method_name),
            params.join(", "),
            return_type
        )
    }

    fn generate_doc(&self, description: &str) -> String {
        format!("/** {} */", description)
    }

    fn null_literal(&self) -> &'static str {
        "null"
    }
    fn true_literal(&self) -> &'static str {
        "true"
    }
    fn false_literal(&self) -> &'static str {
        "false"
    }
}

/// Ruby type mapper
pub struct RubyMapper;

impl TypeMapper for RubyMapper {
    fn map_type(&self, _schema: &TypeSchema) -> String {
        // Ruby is dynamically typed, no type annotations needed
        String::new()
    }

    fn map_param_name(&self, name: &str) -> String {
        to_snake_case(name)
    }

    fn map_method_name(&self, name: &str) -> String {
        to_snake_case(name)
    }

    fn generate_signature(&self, command: &CommandSchema) -> String {
        let params: Vec<String> = command
            .parameters
            .iter()
            .map(|p| {
                let param_name = self.map_param_name(&p.name);
                if p.variadic {
                    format!("*{}", param_name)
                } else if p.required {
                    param_name
                } else {
                    format!("{}: nil", param_name)
                }
            })
            .collect();

        format!(
            "def {}({})",
            self.map_method_name(&command.method_name),
            params.join(", ")
        )
    }

    fn generate_doc(&self, description: &str) -> String {
        format!("# {}", description)
    }

    fn null_literal(&self) -> &'static str {
        "nil"
    }
    fn true_literal(&self) -> &'static str {
        "true"
    }
    fn false_literal(&self) -> &'static str {
        "false"
    }
}

// Helper functions for case conversion

/// Convert to camelCase
fn to_camel_case(s: &str) -> String {
    let mut result = String::new();
    let mut capitalize_next = false;

    for (i, c) in s.chars().enumerate() {
        if c == '_' || c == '-' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(c.to_ascii_uppercase());
            capitalize_next = false;
        } else if i == 0 {
            result.push(c.to_ascii_lowercase());
        } else {
            result.push(c);
        }
    }

    result
}

/// Convert to PascalCase
fn to_pascal_case(s: &str) -> String {
    let mut result = String::new();
    let mut capitalize_next = true;

    for c in s.chars() {
        if c == '_' || c == '-' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(c.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(c);
        }
    }

    result
}

/// Convert to snake_case
fn to_snake_case(s: &str) -> String {
    let mut result = String::new();

    for (i, c) in s.chars().enumerate() {
        if c == '-' {
            result.push('_');
        } else if c.is_ascii_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(c.to_ascii_lowercase());
        } else {
            result.push(c);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_case_conversion() {
        assert_eq!(to_camel_case("hello_world"), "helloWorld");
        assert_eq!(to_pascal_case("hello_world"), "HelloWorld");
        assert_eq!(to_snake_case("helloWorld"), "hello_world");
    }

    #[test]
    fn test_typescript_mapper() {
        let mapper = TypeScriptMapper;

        assert_eq!(mapper.map_type(&TypeSchema::string()), "string");
        assert_eq!(mapper.map_type(&TypeSchema::integer()), "number");
        assert_eq!(
            mapper.map_type(&TypeSchema::array(TypeSchema::string())),
            "string[]"
        );
    }

    #[test]
    fn test_python_mapper() {
        let mapper = PythonMapper;

        assert_eq!(mapper.map_type(&TypeSchema::string()), "str");
        assert_eq!(mapper.map_type(&TypeSchema::integer()), "int");
        assert_eq!(
            mapper.map_type(&TypeSchema::nullable(TypeSchema::string())),
            "Optional[str]"
        );
    }

    #[test]
    fn test_rust_mapper() {
        let mapper = RustMapper;

        assert_eq!(mapper.map_type(&TypeSchema::string()), "String");
        assert_eq!(mapper.map_type(&TypeSchema::integer()), "i64");
        assert_eq!(
            mapper.map_type(&TypeSchema::array(TypeSchema::string())),
            "Vec<String>"
        );
    }

    #[test]
    fn test_language_all() {
        let languages = Language::all();
        assert!(languages.len() >= 8);
    }
}
