//! Code Templates
//!
//! Language-specific code templates for SDK generation.

use super::schema::{ApiSchema, CommandSchema};
use super::Language;

/// Template engine for code generation
pub struct TemplateEngine {
    /// Language being generated
    language: Language,
}

impl TemplateEngine {
    /// Create a new template engine
    pub fn new(language: Language) -> Self {
        Self { language }
    }

    /// Generate client class code
    pub fn generate_client(&self, schema: &ApiSchema) -> String {
        match self.language {
            Language::TypeScript => self.generate_typescript_client(schema),
            Language::Python => self.generate_python_client(schema),
            Language::Go => self.generate_go_client(schema),
            Language::Rust => self.generate_rust_client(schema),
            Language::Java => self.generate_java_client(schema),
            Language::CSharp => self.generate_csharp_client(schema),
            Language::Php => self.generate_php_client(schema),
            Language::Ruby => self.generate_ruby_client(schema),
        }
    }

    /// Generate TypeScript client
    fn generate_typescript_client(&self, schema: &ApiSchema) -> String {
        let mapper = self.language.type_mapper();
        let mut code = String::new();

        // Header
        code.push_str("/**\n");
        code.push_str(&format!(
            " * {} - Auto-generated TypeScript Client\n",
            schema.title
        ));
        code.push_str(&format!(" * Version: {}\n", schema.version));
        code.push_str(" * \n");
        code.push_str(" * This file is auto-generated. Do not edit manually.\n");
        code.push_str(" */\n\n");

        // Imports
        code.push_str("import { createClient, RedisClientType } from 'redis';\n\n");

        // Types
        code.push_str("export interface FerriteClientOptions {\n");
        code.push_str("  host?: string;\n");
        code.push_str("  port?: number;\n");
        code.push_str("  password?: string;\n");
        code.push_str("  database?: number;\n");
        code.push_str("}\n\n");

        // Client class
        code.push_str("export class FerriteClient {\n");
        code.push_str("  private client: RedisClientType;\n\n");

        // Constructor
        code.push_str("  constructor(options: FerriteClientOptions = {}) {\n");
        code.push_str("    this.client = createClient({\n");
        code.push_str("      socket: {\n");
        code.push_str("        host: options.host || 'localhost',\n");
        code.push_str("        port: options.port || 6379,\n");
        code.push_str("      },\n");
        code.push_str("      password: options.password,\n");
        code.push_str("      database: options.database,\n");
        code.push_str("    });\n");
        code.push_str("  }\n\n");

        // Connect/disconnect
        code.push_str("  async connect(): Promise<void> {\n");
        code.push_str("    await this.client.connect();\n");
        code.push_str("  }\n\n");

        code.push_str("  async disconnect(): Promise<void> {\n");
        code.push_str("    await this.client.disconnect();\n");
        code.push_str("  }\n\n");

        // Generate methods
        for commands in schema.commands.values() {
            for cmd in commands {
                code.push_str(&format!("  {}\n", mapper.generate_doc(&cmd.description)));
                code.push_str(&format!("  {} {{\n", mapper.generate_signature(cmd)));
                code.push_str(&self.generate_typescript_method_body(cmd));
                code.push_str("  }\n\n");
            }
        }

        code.push_str("}\n");

        code
    }

    /// Generate TypeScript method body
    fn generate_typescript_method_body(&self, cmd: &CommandSchema) -> String {
        let args: Vec<String> = cmd
            .parameters
            .iter()
            .map(|p| {
                if p.variadic {
                    format!("...{}", p.name)
                } else {
                    p.name.clone()
                }
            })
            .collect();

        format!(
            "    return this.client.sendCommand(['{}', {}]);\n",
            cmd.name,
            args.join(", ")
        )
    }

    /// Generate Python client
    fn generate_python_client(&self, schema: &ApiSchema) -> String {
        let mapper = self.language.type_mapper();
        let mut code = String::new();

        // Header
        code.push_str(&format!(
            "\"\"\"\n{} - Auto-generated Python Client\n\n",
            schema.title
        ));
        code.push_str(&format!("Version: {}\n", schema.version));
        code.push_str("\nThis file is auto-generated. Do not edit manually.\n\"\"\"\n\n");

        // Imports
        code.push_str("from typing import Any, Dict, List, Optional, Union\n");
        code.push_str("import redis.asyncio as redis\n\n\n");

        // Client class
        code.push_str("class FerriteClient:\n");
        code.push_str("    \"\"\"Ferrite client for Python.\"\"\"\n\n");

        // Constructor
        code.push_str("    def __init__(\n");
        code.push_str("        self,\n");
        code.push_str("        host: str = 'localhost',\n");
        code.push_str("        port: int = 6379,\n");
        code.push_str("        password: Optional[str] = None,\n");
        code.push_str("        db: int = 0,\n");
        code.push_str("    ):\n");
        code.push_str("        self._client = redis.Redis(\n");
        code.push_str("            host=host,\n");
        code.push_str("            port=port,\n");
        code.push_str("            password=password,\n");
        code.push_str("            db=db,\n");
        code.push_str("        )\n\n");

        // Connect/close
        code.push_str("    async def connect(self) -> None:\n");
        code.push_str("        \"\"\"Connect to the server.\"\"\"\n");
        code.push_str("        await self._client.ping()\n\n");

        code.push_str("    async def close(self) -> None:\n");
        code.push_str("        \"\"\"Close the connection.\"\"\"\n");
        code.push_str("        await self._client.close()\n\n");

        // Generate methods
        for commands in schema.commands.values() {
            for cmd in commands {
                code.push_str(&format!("    {}\n", mapper.generate_signature(cmd)));
                code.push_str(&format!(
                    "        {}\n",
                    mapper.generate_doc(&cmd.description)
                ));
                code.push_str(&self.generate_python_method_body(cmd));
                code.push('\n');
            }
        }

        code
    }

    /// Generate Python method body
    fn generate_python_method_body(&self, cmd: &CommandSchema) -> String {
        let args: Vec<String> = cmd
            .parameters
            .iter()
            .map(|p| {
                if p.variadic {
                    format!("*{}", p.name)
                } else {
                    p.name.clone()
                }
            })
            .collect();

        format!(
            "        return await self._client.execute_command('{}', {})\n",
            cmd.name,
            args.join(", ")
        )
    }

    /// Generate Go client
    fn generate_go_client(&self, schema: &ApiSchema) -> String {
        let mapper = self.language.type_mapper();
        let mut code = String::new();

        // Package and imports
        code.push_str(&format!("// {} - Auto-generated Go Client\n", schema.title));
        code.push_str(&format!("// Version: {}\n", schema.version));
        code.push_str("// This file is auto-generated. Do not edit manually.\n\n");
        code.push_str("package ferrite\n\n");
        code.push_str("import (\n");
        code.push_str("    \"context\"\n");
        code.push_str("    \"github.com/redis/go-redis/v9\"\n");
        code.push_str(")\n\n");

        // Client struct
        code.push_str("// Client is the Ferrite client.\n");
        code.push_str("type Client struct {\n");
        code.push_str("    rdb *redis.Client\n");
        code.push_str("}\n\n");

        // Options
        code.push_str("// Options for client configuration.\n");
        code.push_str("type Options struct {\n");
        code.push_str("    Addr     string\n");
        code.push_str("    Password string\n");
        code.push_str("    DB       int\n");
        code.push_str("}\n\n");

        // Constructor
        code.push_str("// NewClient creates a new Ferrite client.\n");
        code.push_str("func NewClient(opts Options) *Client {\n");
        code.push_str("    rdb := redis.NewClient(&redis.Options{\n");
        code.push_str("        Addr:     opts.Addr,\n");
        code.push_str("        Password: opts.Password,\n");
        code.push_str("        DB:       opts.DB,\n");
        code.push_str("    })\n");
        code.push_str("    return &Client{rdb: rdb}\n");
        code.push_str("}\n\n");

        // Close
        code.push_str("// Close closes the client connection.\n");
        code.push_str("func (c *Client) Close() error {\n");
        code.push_str("    return c.rdb.Close()\n");
        code.push_str("}\n\n");

        // Generate methods
        for commands in schema.commands.values() {
            for cmd in commands {
                code.push_str(&format!("{}\n", mapper.generate_doc(&cmd.description)));
                code.push_str(&format!("{} {{\n", mapper.generate_signature(cmd)));
                code.push_str(&self.generate_go_method_body(cmd));
                code.push_str("}\n\n");
            }
        }

        code
    }

    /// Generate Go method body
    fn generate_go_method_body(&self, cmd: &CommandSchema) -> String {
        let args: Vec<String> = cmd.parameters.iter().map(|p| p.name.clone()).collect();
        let mapper = self.language.type_mapper();
        let return_type = mapper.map_type(&cmd.return_type);

        format!(
            "    result := c.rdb.Do(ctx, \"{}\"{})\n    return result.{}(), result.Err()\n",
            cmd.name,
            if args.is_empty() {
                String::new()
            } else {
                format!(", {}", args.join(", "))
            },
            if return_type.contains("string") {
                "Text"
            } else {
                "Int64"
            }
        )
    }

    /// Generate Rust client
    fn generate_rust_client(&self, schema: &ApiSchema) -> String {
        let mapper = self.language.type_mapper();
        let mut code = String::new();

        // Header
        code.push_str(&format!(
            "//! {} - Auto-generated Rust Client\n",
            schema.title
        ));
        code.push_str(&format!("//! Version: {}\n", schema.version));
        code.push_str("//!\n");
        code.push_str("//! This file is auto-generated. Do not edit manually.\n\n");

        // Imports
        code.push_str("use redis::{AsyncCommands, Client as RedisClient, RedisResult, Value};\n");
        code.push_str("use std::collections::HashMap;\n\n");

        // Result type
        code.push_str("pub type Result<T> = RedisResult<T>;\n\n");

        // Client struct
        code.push_str("/// Ferrite client.\n");
        code.push_str("pub struct Client {\n");
        code.push_str("    conn: redis::aio::MultiplexedConnection,\n");
        code.push_str("}\n\n");

        code.push_str("impl Client {\n");

        // Constructor
        code.push_str("    /// Create a new client and connect.\n");
        code.push_str("    pub async fn connect(url: &str) -> Result<Self> {\n");
        code.push_str("        let client = RedisClient::open(url)?;\n");
        code.push_str("        let conn = client.get_multiplexed_async_connection().await?;\n");
        code.push_str("        Ok(Self { conn })\n");
        code.push_str("    }\n\n");

        // Generate methods
        for commands in schema.commands.values() {
            for cmd in commands {
                code.push_str(&format!("    {}\n", mapper.generate_doc(&cmd.description)));
                code.push_str(&format!("    {} {{\n", mapper.generate_signature(cmd)));
                code.push_str(&self.generate_rust_method_body(cmd));
                code.push_str("    }\n\n");
            }
        }

        code.push_str("}\n");

        code
    }

    /// Generate Rust method body
    fn generate_rust_method_body(&self, cmd: &CommandSchema) -> String {
        let args: Vec<String> = cmd
            .parameters
            .iter()
            .map(|p| {
                if p.variadic {
                    format!("&{}.into_iter().collect::<Vec<_>>()", p.name)
                } else {
                    format!("&{}", p.name)
                }
            })
            .collect();

        format!(
            "        redis::cmd(\"{}\"){}.query_async(&mut self.conn.clone()).await\n",
            cmd.name,
            if args.is_empty() {
                String::new()
            } else {
                let mut result = String::new();
                for a in &args {
                    result.push_str(&format!(".arg({})", a));
                }
                result
            }
        )
    }

    /// Generate Java client
    fn generate_java_client(&self, schema: &ApiSchema) -> String {
        let mapper = self.language.type_mapper();
        let mut code = String::new();

        code.push_str(&format!(
            "/**\n * {} - Auto-generated Java Client\n",
            schema.title
        ));
        code.push_str(&format!(" * Version: {}\n", schema.version));
        code.push_str(" * \n * This file is auto-generated. Do not edit manually.\n */\n\n");

        code.push_str("package io.ferrite.client;\n\n");
        code.push_str("import java.util.*;\n");
        code.push_str("import java.util.concurrent.CompletableFuture;\n");
        code.push_str("import io.lettuce.core.*;\n");
        code.push_str("import io.lettuce.core.api.async.*;\n\n");

        code.push_str("public class FerriteClient {\n");
        code.push_str("    private final RedisAsyncCommands<String, String> commands;\n\n");

        code.push_str("    public FerriteClient(String host, int port) {\n");
        code.push_str(
            "        RedisClient client = RedisClient.create(RedisURI.create(host, port));\n",
        );
        code.push_str("        this.commands = client.connect().async();\n");
        code.push_str("    }\n\n");

        for commands in schema.commands.values() {
            for cmd in commands {
                code.push_str(&format!("    {}\n", mapper.generate_doc(&cmd.description)));
                code.push_str(&format!("    {} {{\n", mapper.generate_signature(cmd)));
                code.push_str("        // Implementation\n");
                code.push_str("        return CompletableFuture.completedFuture(null);\n");
                code.push_str("    }\n\n");
            }
        }

        code.push_str("}\n");
        code
    }

    /// Generate C# client
    fn generate_csharp_client(&self, schema: &ApiSchema) -> String {
        let mapper = self.language.type_mapper();
        let mut code = String::new();

        code.push_str(&format!("// {} - Auto-generated C# Client\n", schema.title));
        code.push_str(&format!("// Version: {}\n", schema.version));
        code.push_str("// This file is auto-generated. Do not edit manually.\n\n");

        code.push_str("using System;\n");
        code.push_str("using System.Collections.Generic;\n");
        code.push_str("using System.Threading.Tasks;\n");
        code.push_str("using StackExchange.Redis;\n\n");

        code.push_str("namespace Ferrite.Client\n{\n");
        code.push_str("    public class FerriteClient : IDisposable\n    {\n");
        code.push_str("        private readonly ConnectionMultiplexer _redis;\n");
        code.push_str("        private readonly IDatabase _db;\n\n");

        code.push_str(
            "        public FerriteClient(string connectionString = \"localhost:6379\")\n",
        );
        code.push_str("        {\n");
        code.push_str("            _redis = ConnectionMultiplexer.Connect(connectionString);\n");
        code.push_str("            _db = _redis.GetDatabase();\n");
        code.push_str("        }\n\n");

        for commands in schema.commands.values() {
            for cmd in commands {
                code.push_str(&format!(
                    "        {}\n",
                    mapper.generate_doc(&cmd.description)
                ));
                code.push_str(&format!("        {}\n", mapper.generate_signature(cmd)));
                code.push_str("        {\n");
                code.push_str("            // Implementation\n");
                code.push_str("            return Task.FromResult<object>(null);\n");
                code.push_str("        }\n\n");
            }
        }

        code.push_str("        public void Dispose()\n");
        code.push_str("        {\n");
        code.push_str("            _redis?.Dispose();\n");
        code.push_str("        }\n");
        code.push_str("    }\n}\n");

        code
    }

    /// Generate PHP client
    fn generate_php_client(&self, schema: &ApiSchema) -> String {
        let mapper = self.language.type_mapper();
        let mut code = String::new();

        code.push_str("<?php\n");
        code.push_str(&format!(
            "/**\n * {} - Auto-generated PHP Client\n",
            schema.title
        ));
        code.push_str(&format!(" * Version: {}\n", schema.version));
        code.push_str(" * This file is auto-generated. Do not edit manually.\n */\n\n");

        code.push_str("namespace Ferrite;\n\n");
        code.push_str("use Predis\\Client;\n\n");

        code.push_str("class FerriteClient\n{\n");
        code.push_str("    private Client $client;\n\n");

        code.push_str(
            "    public function __construct(string $host = 'localhost', int $port = 6379)\n",
        );
        code.push_str("    {\n");
        code.push_str("        $this->client = new Client(['host' => $host, 'port' => $port]);\n");
        code.push_str("    }\n\n");

        for commands in schema.commands.values() {
            for cmd in commands {
                code.push_str(&format!("    {}\n", mapper.generate_doc(&cmd.description)));
                code.push_str(&format!("    {}\n", mapper.generate_signature(cmd)));
                code.push_str("    {\n");
                code.push_str("        // Implementation\n");
                code.push_str("        return null;\n");
                code.push_str("    }\n\n");
            }
        }

        code.push_str("}\n");
        code
    }

    /// Generate Ruby client
    fn generate_ruby_client(&self, schema: &ApiSchema) -> String {
        let mapper = self.language.type_mapper();
        let mut code = String::new();

        code.push_str(&format!(
            "# {} - Auto-generated Ruby Client\n",
            schema.title
        ));
        code.push_str(&format!("# Version: {}\n", schema.version));
        code.push_str("# This file is auto-generated. Do not edit manually.\n\n");

        code.push_str("require 'redis'\n\n");

        code.push_str("module Ferrite\n");
        code.push_str("  class Client\n");
        code.push_str("    def initialize(host: 'localhost', port: 6379, password: nil, db: 0)\n");
        code.push_str(
            "      @redis = Redis.new(host: host, port: port, password: password, db: db)\n",
        );
        code.push_str("    end\n\n");

        for commands in schema.commands.values() {
            for cmd in commands {
                code.push_str(&format!("    {}\n", mapper.generate_doc(&cmd.description)));
                code.push_str(&format!("    {}\n", mapper.generate_signature(cmd)));
                code.push_str("      # Implementation\n");
                code.push_str("    end\n\n");
            }
        }

        code.push_str("  end\n");
        code.push_str("end\n");

        code
    }

    /// Generate package manifest
    pub fn generate_manifest(&self, _schema: &ApiSchema, metadata: &super::SdkMetadata) -> String {
        match self.language {
            Language::TypeScript => self.generate_package_json(metadata),
            Language::Python => self.generate_pyproject(metadata),
            Language::Go => self.generate_go_mod(metadata),
            Language::Rust => self.generate_cargo_toml(metadata),
            Language::Java => self.generate_pom_xml(metadata),
            Language::CSharp => self.generate_csproj(metadata),
            Language::Php => self.generate_composer_json(metadata),
            Language::Ruby => self.generate_gemspec(metadata),
        }
    }

    fn generate_package_json(&self, metadata: &super::SdkMetadata) -> String {
        format!(
            r#"{{
  "name": "{}",
  "version": "{}",
  "description": "{}",
  "main": "dist/index.js",
  "types": "dist/index.d.ts",
  "license": "{}",
  "dependencies": {{
    "redis": "^4.6.0"
  }},
  "devDependencies": {{
    "typescript": "^5.0.0"
  }}
}}
"#,
            metadata.name, metadata.version, metadata.description, metadata.license
        )
    }

    fn generate_pyproject(&self, metadata: &super::SdkMetadata) -> String {
        format!(
            r#"[project]
name = "{}"
version = "{}"
description = "{}"
license = "{}"
dependencies = ["redis>=5.0.0"]

[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"
"#,
            metadata.name, metadata.version, metadata.description, metadata.license
        )
    }

    fn generate_go_mod(&self, metadata: &super::SdkMetadata) -> String {
        format!(
            r#"module github.com/ferrite/{}

go 1.21

require github.com/redis/go-redis/v9 v9.3.0
"#,
            metadata.name
        )
    }

    fn generate_cargo_toml(&self, metadata: &super::SdkMetadata) -> String {
        format!(
            r#"[package]
name = "{}"
version = "{}"
description = "{}"
license = "{}"
edition = "2021"

[dependencies]
redis = {{ version = "0.24", features = ["tokio-comp", "aio"] }}
tokio = {{ version = "1", features = ["full"] }}
"#,
            metadata.name, metadata.version, metadata.description, metadata.license
        )
    }

    fn generate_pom_xml(&self, metadata: &super::SdkMetadata) -> String {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<project>
    <modelVersion>4.0.0</modelVersion>
    <groupId>io.ferrite</groupId>
    <artifactId>{}</artifactId>
    <version>{}</version>
    <description>{}</description>
</project>
"#,
            metadata.name, metadata.version, metadata.description
        )
    }

    fn generate_csproj(&self, metadata: &super::SdkMetadata) -> String {
        format!(
            r#"<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net8.0</TargetFramework>
    <PackageId>{}</PackageId>
    <Version>{}</Version>
    <Description>{}</Description>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="StackExchange.Redis" Version="2.7.0" />
  </ItemGroup>
</Project>
"#,
            metadata.name, metadata.version, metadata.description
        )
    }

    fn generate_composer_json(&self, metadata: &super::SdkMetadata) -> String {
        format!(
            r#"{{
    "name": "ferrite/{}",
    "description": "{}",
    "version": "{}",
    "require": {{
        "predis/predis": "^2.0"
    }}
}}
"#,
            metadata.name, metadata.description, metadata.version
        )
    }

    fn generate_gemspec(&self, metadata: &super::SdkMetadata) -> String {
        format!(
            r#"Gem::Specification.new do |s|
  s.name        = '{}'
  s.version     = '{}'
  s.summary     = '{}'
  s.license     = '{}'
  s.add_dependency 'redis', '~> 5.0'
end
"#,
            metadata.name, metadata.version, metadata.description, metadata.license
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_typescript_generation() {
        let engine = TemplateEngine::new(Language::TypeScript);
        let schema = ApiSchema::default();
        let code = engine.generate_client(&schema);

        assert!(code.contains("class FerriteClient"));
        assert!(code.contains("async get"));
        assert!(code.contains("async set"));
    }

    #[test]
    fn test_python_generation() {
        let engine = TemplateEngine::new(Language::Python);
        let schema = ApiSchema::default();
        let code = engine.generate_client(&schema);

        assert!(code.contains("class FerriteClient"));
        assert!(code.contains("async def get"));
    }

    #[test]
    fn test_rust_generation() {
        let engine = TemplateEngine::new(Language::Rust);
        let schema = ApiSchema::default();
        let code = engine.generate_client(&schema);

        assert!(code.contains("pub struct Client"));
        assert!(code.contains("pub async fn get"));
    }
}
