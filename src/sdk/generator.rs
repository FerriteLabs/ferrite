//! SDK Generator
//!
//! Main generator for creating client SDKs.

use super::{
    schema::ApiSchema, templates::TemplateEngine, FileType, GeneratedFile, GeneratedSdk, Language,
    Result, SdkError, SdkMetadata,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// SDK generator configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GeneratorConfig {
    /// SDK metadata
    pub metadata: SdkMetadata,
    /// Languages to generate
    pub languages: Vec<Language>,
    /// Output base directory
    pub output_dir: PathBuf,
    /// Generate tests
    pub generate_tests: bool,
    /// Generate documentation
    pub generate_docs: bool,
    /// Include examples
    pub include_examples: bool,
    /// Custom templates directory
    pub templates_dir: Option<PathBuf>,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            metadata: SdkMetadata::default(),
            languages: vec![Language::TypeScript, Language::Python],
            output_dir: PathBuf::from("./sdk"),
            generate_tests: true,
            generate_docs: true,
            include_examples: true,
            templates_dir: None,
        }
    }
}

/// SDK generator
pub struct SdkGenerator {
    /// Configuration
    config: GeneratorConfig,
    /// API schema
    schema: ApiSchema,
}

impl SdkGenerator {
    /// Create a new SDK generator with default schema
    pub fn new() -> Self {
        Self {
            config: GeneratorConfig::default(),
            schema: ApiSchema::default(),
        }
    }

    /// Create with custom configuration
    pub fn with_config(config: GeneratorConfig) -> Self {
        Self {
            config,
            schema: ApiSchema::default(),
        }
    }

    /// Create with custom schema
    pub fn with_schema(schema: ApiSchema) -> Self {
        Self {
            config: GeneratorConfig::default(),
            schema,
        }
    }

    /// Set configuration
    pub fn config(mut self, config: GeneratorConfig) -> Self {
        self.config = config;
        self
    }

    /// Set schema
    pub fn schema(mut self, schema: ApiSchema) -> Self {
        self.schema = schema;
        self
    }

    /// Generate SDK for a specific language
    pub fn generate(&self, language: Language) -> Result<GeneratedSdk> {
        let output_dir = self.config.output_dir.join(language.name().to_lowercase());
        let template_engine = TemplateEngine::new(language.clone());

        let mut files = Vec::new();

        // Generate main client file
        let client_code = template_engine.generate_client(&self.schema);
        let client_filename = format!("client.{}", language.extension());
        files.push(GeneratedFile {
            path: PathBuf::from(&client_filename),
            content: client_code,
            file_type: FileType::Source,
        });

        // Generate package manifest
        let manifest_content =
            template_engine.generate_manifest(&self.schema, &self.config.metadata);
        let manifest_filename = self.manifest_filename(&language);
        files.push(GeneratedFile {
            path: PathBuf::from(manifest_filename),
            content: manifest_content,
            file_type: FileType::Config,
        });

        // Generate README
        if self.config.generate_docs {
            let readme = self.generate_readme(&language);
            files.push(GeneratedFile {
                path: PathBuf::from("README.md"),
                content: readme,
                file_type: FileType::Documentation,
            });
        }

        // Generate tests
        if self.config.generate_tests {
            let tests = self.generate_tests(&language);
            let test_filename = format!("test_client.{}", language.extension());
            files.push(GeneratedFile {
                path: PathBuf::from(test_filename),
                content: tests,
                file_type: FileType::Test,
            });
        }

        // Generate examples
        if self.config.include_examples {
            let examples = self.generate_examples(&language);
            let example_filename = format!("example.{}", language.extension());
            files.push(GeneratedFile {
                path: PathBuf::from(example_filename),
                content: examples,
                file_type: FileType::Source,
            });
        }

        Ok(GeneratedSdk {
            language,
            output_dir,
            files,
            metadata: self.config.metadata.clone(),
        })
    }

    /// Generate SDKs for all configured languages
    pub fn generate_all(&self) -> Result<Vec<GeneratedSdk>> {
        let mut results = Vec::new();
        for language in &self.config.languages {
            results.push(self.generate(language.clone())?);
        }
        Ok(results)
    }

    /// Write generated SDK to disk
    pub fn write(&self, sdk: &GeneratedSdk) -> Result<()> {
        std::fs::create_dir_all(&sdk.output_dir).map_err(|e| SdkError::Io(e.to_string()))?;

        for file in &sdk.files {
            let file_path = sdk.output_dir.join(&file.path);

            // Create parent directories if needed
            if let Some(parent) = file_path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| SdkError::Io(e.to_string()))?;
            }

            std::fs::write(&file_path, &file.content).map_err(|e| SdkError::Io(e.to_string()))?;
        }

        Ok(())
    }

    /// Generate and write all SDKs
    pub fn generate_and_write(&self) -> Result<Vec<GeneratedSdk>> {
        let sdks = self.generate_all()?;
        for sdk in &sdks {
            self.write(sdk)?;
        }
        Ok(sdks)
    }

    /// Get manifest filename for language
    fn manifest_filename(&self, language: &Language) -> &'static str {
        match language {
            Language::TypeScript => "package.json",
            Language::Python => "pyproject.toml",
            Language::Go => "go.mod",
            Language::Rust => "Cargo.toml",
            Language::Java => "pom.xml",
            Language::CSharp => "Ferrite.Client.csproj",
            Language::Php => "composer.json",
            Language::Ruby => "ferrite.gemspec",
        }
    }

    /// Generate README for language
    fn generate_readme(&self, language: &Language) -> String {
        let mut readme = String::new();

        readme.push_str(&format!("# Ferrite {} Client\n\n", language.name()));
        readme.push_str(&format!("{}\n\n", self.config.metadata.description));

        readme.push_str("## Installation\n\n");
        match language {
            Language::TypeScript => {
                readme.push_str("```bash\nnpm install ferrite-client\n```\n\n");
            }
            Language::Python => {
                readme.push_str("```bash\npip install ferrite-client\n```\n\n");
            }
            Language::Go => {
                readme.push_str("```bash\ngo get github.com/ferrite/ferrite-client\n```\n\n");
            }
            Language::Rust => {
                readme.push_str("```toml\n[dependencies]\nferrite-client = \"0.1\"\n```\n\n");
            }
            Language::Java => {
                readme.push_str("```xml\n<dependency>\n  <groupId>io.ferrite</groupId>\n  <artifactId>ferrite-client</artifactId>\n</dependency>\n```\n\n");
            }
            _ => {
                readme.push_str("See documentation for installation instructions.\n\n");
            }
        }

        readme.push_str("## Quick Start\n\n");
        readme.push_str(&self.generate_quick_start(language));

        readme.push_str("\n## Available Commands\n\n");
        for (category, commands) in &self.schema.commands {
            readme.push_str(&format!("### {}\n\n", category.name()));
            for cmd in commands {
                readme.push_str(&format!("- `{}` - {}\n", cmd.method_name, cmd.description));
            }
            readme.push('\n');
        }

        readme.push_str("## License\n\n");
        readme.push_str(&format!("{}\n", self.config.metadata.license));

        readme
    }

    /// Generate quick start code for language
    fn generate_quick_start(&self, language: &Language) -> String {
        match language {
            Language::TypeScript => r#"```typescript
import { FerriteClient } from 'ferrite-client';

const client = new FerriteClient({ host: 'localhost', port: 6379 });
await client.connect();

// Set a value
await client.set('key', 'value');

// Get a value
const value = await client.get('key');
console.log(value); // 'value'

await client.disconnect();
```
"#
            .to_string(),
            Language::Python => r#"```python
from ferrite import FerriteClient

client = FerriteClient(host='localhost', port=6379)
await client.connect()

# Set a value
await client.set('key', 'value')

# Get a value
value = await client.get('key')
print(value)  # 'value'

await client.close()
```
"#
            .to_string(),
            Language::Go => r#"```go
package main

import (
    "context"
    "fmt"
    "github.com/ferrite/ferrite-client"
)

func main() {
    client := ferrite.NewClient(ferrite.Options{
        Addr: "localhost:6379",
    })
    defer client.Close()

    ctx := context.Background()

    // Set a value
    client.Set(ctx, "key", "value")

    // Get a value
    value, _ := client.Get(ctx, "key")
    fmt.Println(value) // "value"
}
```
"#
            .to_string(),
            Language::Rust => r#"```rust
use ferrite_client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect("redis://localhost:6379").await?;

    // Set a value
    client.set("key", "value").await?;

    // Get a value
    let value: Option<String> = client.get("key").await?;
    println!("{:?}", value); // Some("value")

    Ok(())
}
```
"#
            .to_string(),
            _ => "See the examples directory for usage.\n".to_string(),
        }
    }

    /// Generate tests for language
    fn generate_tests(&self, language: &Language) -> String {
        match language {
            Language::TypeScript => r#"import { FerriteClient } from './client';

describe('FerriteClient', () => {
  let client: FerriteClient;

  beforeAll(async () => {
    client = new FerriteClient({ host: 'localhost', port: 6379 });
    await client.connect();
  });

  afterAll(async () => {
    await client.disconnect();
  });

  test('ping', async () => {
    const result = await client.ping();
    expect(result).toBe('PONG');
  });

  test('set and get', async () => {
    await client.set('test:key', 'test:value');
    const value = await client.get('test:key');
    expect(value).toBe('test:value');
  });

  test('del', async () => {
    await client.set('test:del', 'value');
    const deleted = await client.del('test:del');
    expect(deleted).toBe(1);
  });
});
"#
            .to_string(),
            Language::Python => r#"import pytest
from client import FerriteClient

@pytest.fixture
async def client():
    client = FerriteClient(host='localhost', port=6379)
    await client.connect()
    yield client
    await client.close()

@pytest.mark.asyncio
async def test_ping(client):
    result = await client.ping()
    assert result == 'PONG'

@pytest.mark.asyncio
async def test_set_and_get(client):
    await client.set('test:key', 'test:value')
    value = await client.get('test:key')
    assert value == 'test:value'

@pytest.mark.asyncio
async def test_del(client):
    await client.set('test:del', 'value')
    deleted = await client.del('test:del')
    assert deleted == 1
"#
            .to_string(),
            Language::Rust => r#"use super::*;

#[tokio::test]
async fn test_ping() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect("redis://localhost:6379").await?;
    let result: String = client.ping(None).await?;
    assert_eq!(result, "PONG");
    Ok(())
}

#[tokio::test]
async fn test_set_and_get() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect("redis://localhost:6379").await?;
    client.set("test:key", "test:value").await?;
    let value: Option<String> = client.get("test:key").await?;
    assert_eq!(value, Some("test:value".to_string()));
    Ok(())
}

#[tokio::test]
async fn test_del() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect("redis://localhost:6379").await?;
    client.set("test:del", "value").await?;
    let deleted: i64 = client.del("test:del").await?;
    assert_eq!(deleted, 1);
    Ok(())
}
"#
            .to_string(),
            _ => "// TODO: Add tests\n".to_string(),
        }
    }

    /// Generate examples for language
    fn generate_examples(&self, language: &Language) -> String {
        match language {
            Language::TypeScript => r#"/**
 * Ferrite TypeScript Client Example
 */

import { FerriteClient } from './client';

async function main() {
  const client = new FerriteClient({
    host: 'localhost',
    port: 6379,
  });

  await client.connect();

  try {
    // String operations
    console.log('=== String Operations ===');
    await client.set('greeting', 'Hello, Ferrite!');
    const greeting = await client.get('greeting');
    console.log(`Greeting: ${greeting}`);

    // Counter operations
    console.log('\n=== Counter Operations ===');
    await client.set('counter', '0');
    await client.incr('counter');
    await client.incr('counter');
    const count = await client.get('counter');
    console.log(`Counter: ${count}`);

    // List operations
    console.log('\n=== List Operations ===');
    await client.lpush('mylist', 'world');
    await client.lpush('mylist', 'hello');
    const list = await client.lrange('mylist', 0, -1);
    console.log(`List: ${list}`);

    // Hash operations
    console.log('\n=== Hash Operations ===');
    await client.hset('user:1', 'name', 'Alice', 'age', '30');
    const user = await client.hgetall('user:1');
    console.log(`User: ${JSON.stringify(user)}`);

    // Clean up
    await client.del('greeting', 'counter', 'mylist', 'user:1');

  } finally {
    await client.disconnect();
  }
}

main().catch(console.error);
"#
            .to_string(),
            Language::Python => r#"""
Ferrite Python Client Example
"""

import asyncio
from client import FerriteClient

async def main():
    client = FerriteClient(host='localhost', port=6379)
    await client.connect()

    try:
        # String operations
        print('=== String Operations ===')
        await client.set('greeting', 'Hello, Ferrite!')
        greeting = await client.get('greeting')
        print(f'Greeting: {greeting}')

        # Counter operations
        print('\n=== Counter Operations ===')
        await client.set('counter', '0')
        await client.incr('counter')
        await client.incr('counter')
        count = await client.get('counter')
        print(f'Counter: {count}')

        # List operations
        print('\n=== List Operations ===')
        await client.lpush('mylist', 'world')
        await client.lpush('mylist', 'hello')
        items = await client.lrange('mylist', 0, -1)
        print(f'List: {items}')

        # Hash operations
        print('\n=== Hash Operations ===')
        await client.hset('user:1', 'name', 'Alice', 'age', '30')
        user = await client.hgetall('user:1')
        print(f'User: {user}')

        # Clean up
        await client.del('greeting', 'counter', 'mylist', 'user:1')

    finally:
        await client.close()

if __name__ == '__main__':
    asyncio.run(main())
"#
            .to_string(),
            Language::Rust => r#"//! Ferrite Rust Client Example

use ferrite_client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect("redis://localhost:6379").await?;

    // String operations
    println!("=== String Operations ===");
    client.set("greeting", "Hello, Ferrite!").await?;
    let greeting: Option<String> = client.get("greeting").await?;
    println!("Greeting: {:?}", greeting);

    // Counter operations
    println!("\n=== Counter Operations ===");
    client.set("counter", "0").await?;
    client.incr("counter").await?;
    client.incr("counter").await?;
    let count: Option<String> = client.get("counter").await?;
    println!("Counter: {:?}", count);

    // Clean up
    client.del("greeting").await?;
    client.del("counter").await?;

    Ok(())
}
"#
            .to_string(),
            _ => "// TODO: Add examples\n".to_string(),
        }
    }

    /// Export OpenAPI spec
    pub fn export_openapi(&self) -> Result<String> {
        let mut spec = serde_json::json!({
            "openapi": "3.0.0",
            "info": {
                "title": self.schema.title,
                "version": self.schema.version,
                "description": self.schema.description
            },
            "servers": [
                {
                    "url": "http://localhost:6379",
                    "description": "Local Ferrite server"
                }
            ],
            "paths": {}
        });

        // Add paths for each command
        let paths = spec["paths"]
            .as_object_mut()
            .ok_or_else(|| SdkError::Schema("paths field missing from OpenAPI spec".into()))?;
        for (category, commands) in &self.schema.commands {
            for cmd in commands {
                let path = format!("/{}", cmd.method_name);
                paths.insert(
                    path,
                    serde_json::json!({
                        "post": {
                            "summary": cmd.description,
                            "tags": [category.name()],
                            "operationId": cmd.method_name,
                            "responses": {
                                "200": {
                                    "description": "Successful response"
                                }
                            }
                        }
                    }),
                );
            }
        }

        serde_json::to_string_pretty(&spec).map_err(|e| SdkError::Schema(e.to_string()))
    }
}

impl Default for SdkGenerator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn test_generator_creation() {
        let generator = SdkGenerator::new();
        assert!(!generator.schema.commands.is_empty());
    }

    #[test]
    fn test_generate_typescript() {
        let generator = SdkGenerator::new();
        let sdk = generator.generate(Language::TypeScript).unwrap();

        assert_eq!(sdk.language, Language::TypeScript);
        assert!(!sdk.files.is_empty());

        // Should have client file
        let has_client = sdk
            .files
            .iter()
            .any(|f| f.path.to_str().unwrap().contains("client"));
        assert!(has_client);
    }

    #[test]
    fn test_generate_all() {
        let config = GeneratorConfig {
            languages: vec![Language::TypeScript, Language::Python],
            ..Default::default()
        };
        let generator = SdkGenerator::with_config(config);
        let sdks = generator.generate_all().unwrap();

        assert_eq!(sdks.len(), 2);
    }

    #[test]
    fn test_openapi_export() {
        let generator = SdkGenerator::new();
        let spec = generator.export_openapi().unwrap();

        assert!(spec.contains("openapi"));
        assert!(spec.contains("3.0.0"));
    }
}
