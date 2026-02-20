//! Server Mode Example
//!
//! Demonstrates running Ferrite as a standalone Redis-compatible server:
//! - Creating server configuration
//! - Loading config from TOML files
//! - Server initialization
//! - Configuration options (networking, storage, persistence)
//!
//! Note: This example shows the setup but doesn't actually run the server
//! to avoid blocking. In a real application, you would call server.run().await
//!
//! Run with: cargo run --example server_mode

use ferrite::config::{
    Config, LogFormat, LoggingConfig, MetricsConfig, PersistenceConfig, ServerConfig,
    StorageBackendType, StorageConfig, SyncPolicy,
};
use std::path::PathBuf;
use std::time::Duration;

fn main() -> anyhow::Result<()> {
    println!("=== Ferrite Server Mode Example ===\n");

    // ==================== Default Configuration ====================
    println!("--- Default Configuration ---");

    let default_config = Config::default();
    println!("Server:");
    println!(
        "  Bind: {}:{}",
        default_config.server.bind, default_config.server.port
    );
    println!(
        "  Max connections: {}",
        default_config.server.max_connections
    );
    println!("Storage:");
    println!("  Backend: {:?}", default_config.storage.backend);
    println!(
        "  Max memory: {} MB",
        default_config.storage.max_memory / 1024 / 1024
    );
    println!("  Databases: {}", default_config.storage.databases);
    println!("Persistence:");
    println!("  AOF enabled: {}", default_config.persistence.aof_enabled);
    println!("  AOF sync: {:?}", default_config.persistence.aof_sync);
    println!("Metrics:");
    println!("  Enabled: {}", default_config.metrics.enabled);
    println!("  Port: {}", default_config.metrics.port);

    // ==================== Custom Configuration ====================
    println!("\n--- Custom Configuration ---");

    let custom_config = Config {
        server: ServerConfig {
            bind: "0.0.0.0".to_string(), // Listen on all interfaces
            port: 6380,                  // Custom port
            max_connections: 20000,      // Higher connection limit
            tcp_keepalive: 300,          // 5 minutes
            timeout: 0,                  // No timeout
            acl_file: Some(PathBuf::from("./data/users.acl")),
        },
        storage: StorageConfig {
            backend: StorageBackendType::Memory, // In-memory backend
            max_memory: 4 * 1024 * 1024 * 1024,  // 4GB
            databases: 16,
            data_dir: PathBuf::from("./data"),
            max_key_size: 512 * 1024 * 1024,   // 512MB
            max_value_size: 512 * 1024 * 1024, // 512MB
            ..Default::default()
        },
        persistence: PersistenceConfig {
            aof_enabled: true,
            aof_path: PathBuf::from("./data/appendonly.aof"),
            aof_sync: SyncPolicy::EverySecond, // Redis-compatible default
            checkpoint_enabled: true,
            checkpoint_interval: Duration::from_secs(300), // 5 minutes
            checkpoint_dir: PathBuf::from("./data/checkpoints"),
        },
        metrics: MetricsConfig {
            enabled: true,
            port: 9091,
            bind: "127.0.0.1".to_string(),
        },
        logging: LoggingConfig {
            level: "info".to_string(),
            format: LogFormat::Pretty,
            file: None, // Log to stdout
        },
        ..Default::default()
    };

    println!("Custom configuration created:");
    println!(
        "  Address: {}:{}",
        custom_config.server.bind, custom_config.server.port
    );
    println!(
        "  Max memory: {} GB",
        custom_config.storage.max_memory / 1024 / 1024 / 1024
    );
    println!("  AOF sync: {:?}", custom_config.persistence.aof_sync);

    // ==================== Configuration from TOML String ====================
    println!("\n--- Configuration from TOML ---");

    let toml_config = r#"
[server]
bind = "127.0.0.1"
port = 6379
max_connections = 10000
tcp_keepalive = 300
timeout = 0

[storage]
backend = "memory"
max_memory = 1073741824  # 1GB
databases = 16
data_dir = "./data"

[persistence]
aof_enabled = true
aof_path = "./data/appendonly.aof"
aof_sync = "everysecond"
checkpoint_enabled = true
checkpoint_interval = 300

[metrics]
enabled = true
port = 9090
bind = "127.0.0.1"

[logging]
level = "info"
format = "pretty"
"#;

    let parsed_config = Config::parse_str(toml_config)?;
    println!("Parsed TOML configuration:");
    println!(
        "  Server: {}:{}",
        parsed_config.server.bind, parsed_config.server.port
    );
    println!("  AOF sync: {:?}", parsed_config.persistence.aof_sync);

    // Validate the configuration
    parsed_config.validate()?;
    println!("  Configuration validated successfully!");

    // ==================== Configuration Validation ====================
    println!("\n--- Configuration Validation ---");

    // Example of invalid configuration
    let mut invalid_config = Config::default();
    invalid_config.server.port = 0; // Invalid port

    match invalid_config.validate() {
        Ok(_) => println!("Configuration is valid"),
        Err(e) => println!("Validation error (expected): {}", e),
    }

    // ==================== TLS Configuration ====================
    println!("\n--- TLS Configuration (Example) ---");

    let tls_toml = r#"
[server]
bind = "0.0.0.0"
port = 6379

[tls]
enabled = true
port = 6380
cert_file = "/etc/ferrite/tls/server.crt"
key_file = "/etc/ferrite/tls/server.key"
# Optional: for mutual TLS
# ca_file = "/etc/ferrite/tls/ca.crt"
# require_client_cert = true
"#;

    println!("TLS configuration example:");
    println!("{}", tls_toml);
    // Note: Would fail validation without actual cert files

    // ==================== Cluster Configuration ====================
    println!("\n--- Cluster Configuration (Example) ---");

    let cluster_toml = r#"
[cluster]
enabled = true
bus_port_offset = 10000
node_timeout = 15000
replica_count = 1
failover_enabled = true
require_full_coverage = true
known_nodes = [
    "192.168.1.10:6379",
    "192.168.1.11:6379",
    "192.168.1.12:6379"
]
"#;

    println!("Cluster configuration example:");
    println!("{}", cluster_toml);

    // ==================== Replication Configuration ====================
    println!("\n--- Replication Configuration (Example) ---");

    let replication_toml = r#"
[replication]
# For a replica, specify the primary
replicaof = "192.168.1.10:6379"
replica_read_only = true
backlog_size = 1048576  # 1MB
repl_timeout = 60
"#;

    println!("Replication configuration example:");
    println!("{}", replication_toml);

    // ==================== Server Startup Pattern ====================
    println!("\n--- Server Startup Pattern ---");

    println!("To start a Ferrite server, use this pattern in your main.rs:");
    println!();
    let startup_pattern = r#"
use ferrite::config::Config;
use ferrite::server::Server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Option 1: Default configuration
    let config = Config::default();

    // Option 2: Load from file
    // let config = Config::from_file(&PathBuf::from("ferrite.toml"))?;

    // Validate configuration
    config.validate()?;

    // Create and run server
    let server = Server::new(config).await?;
    server.run().await?;

    Ok(())
}
"#;
    println!("{}", startup_pattern);

    // ==================== Production Configuration Template ====================
    println!("\n--- Production Configuration Template ---");

    let production_config = r#"
# Ferrite Production Configuration
# Save as ferrite.toml

[server]
bind = "0.0.0.0"
port = 6379
max_connections = 10000
tcp_keepalive = 300
timeout = 0
acl_file = "/etc/ferrite/users.acl"

[storage]
backend = "memory"
max_memory = 4294967296  # 4GB
databases = 16
data_dir = "/var/lib/ferrite"
max_key_size = 536870912    # 512MB
max_value_size = 536870912  # 512MB

[persistence]
aof_enabled = true
aof_path = "/var/lib/ferrite/appendonly.aof"
aof_sync = "everysecond"
checkpoint_enabled = true
checkpoint_interval = 3600  # 1 hour
checkpoint_dir = "/var/lib/ferrite/checkpoints"

[metrics]
enabled = true
port = 9090
bind = "127.0.0.1"  # Only expose internally

[logging]
level = "info"
format = "json"  # Structured logging for production

[tls]
enabled = true
port = 6380
cert_file = "/etc/ferrite/tls/server.crt"
key_file = "/etc/ferrite/tls/server.key"

[audit]
enabled = true
log_file = "/var/log/ferrite/audit.log"
format = "json"
log_auth = true
log_admin = true
"#;

    println!("{}", production_config);

    println!("\n=== Example Complete ===");
    Ok(())
}
