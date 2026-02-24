//! Ferrite - A high-performance, tiered-storage key-value store
//!
//! This is the main entry point for the Ferrite server.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{ArgAction, Parser, Subcommand};
use ferrite::config::Config;
use ferrite::server::Server;
use ferrite::startup_errors::{format_startup_error, StartupError};
use ferrite::telemetry::{self, TelemetryConfig};
use tracing::{error, info, warn};

mod doctor;

/// Ferrite - A high-performance, tiered-storage key-value store
///
/// Designed as a drop-in Redis replacement with tiered storage support.
#[derive(Parser, Debug)]
#[command(name = "ferrite")]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Path to configuration file (TOML)
    #[arg(short = 'c', long = "config", value_name = "FILE", global = true)]
    config: Option<PathBuf>,

    /// Port to listen on (overrides config file)
    #[arg(
        short = 'p',
        long = "port",
        value_name = "PORT",
        env = "FERRITE_PORT",
        global = true
    )]
    port: Option<u16>,

    /// Address to bind to (overrides config file)
    #[arg(
        short = 'b',
        long = "bind",
        alias = "host",
        value_name = "ADDR",
        env = "FERRITE_BIND",
        global = true
    )]
    bind: Option<String>,

    /// Log level: trace, debug, info, warn, error (overrides config file)
    #[arg(
        short = 'l',
        long = "log-level",
        alias = "loglevel",
        value_name = "LEVEL",
        env = "FERRITE_LOG_LEVEL",
        global = true
    )]
    loglevel: Option<String>,

    /// Number of databases (1-16, overrides config file)
    #[arg(
        long = "databases",
        value_name = "NUM",
        env = "FERRITE_DATABASES",
        global = true
    )]
    databases: Option<u8>,

    /// Data directory path
    #[arg(
        long = "data-dir",
        value_name = "PATH",
        env = "FERRITE_DATA_DIR",
        global = true
    )]
    data_dir: Option<PathBuf>,

    /// Metrics bind address (overrides config file)
    #[arg(
        long = "metrics-bind",
        value_name = "ADDR",
        env = "FERRITE_METRICS_BIND",
        global = true
    )]
    metrics_bind: Option<String>,

    /// Metrics port (overrides config file)
    #[arg(
        long = "metrics-port",
        value_name = "PORT",
        env = "FERRITE_METRICS_PORT",
        global = true
    )]
    metrics_port: Option<u16>,

    /// Test configuration and exit without starting server
    #[arg(long = "test-config", action = ArgAction::SetTrue, global = true)]
    test_config: bool,

    /// Dump effective configuration to stdout and exit
    #[arg(long = "dump-config", action = ArgAction::SetTrue, global = true)]
    dump_config: bool,

    /// Runtime config overrides in key=value format (can be specified multiple times)
    #[arg(long = "set", value_name = "KEY=VALUE", action = ArgAction::Append, global = true)]
    config_overrides: Vec<String>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the Ferrite server (default if no command specified)
    Run,

    /// Initialize a new Ferrite configuration
    Init {
        /// Output path for the configuration file
        #[arg(short = 'o', long = "output", default_value = "ferrite.toml")]
        output: PathBuf,

        /// Data directory to use
        #[arg(short = 'd', long = "data-dir", default_value = "./data")]
        data_dir: PathBuf,

        /// Overwrite existing configuration file
        #[arg(short = 'f', long = "force", action = ArgAction::SetTrue)]
        force: bool,

        /// Skip creating directories
        #[arg(long = "no-dirs", action = ArgAction::SetTrue)]
        no_dirs: bool,

        /// Generate minimal configuration (fewer comments)
        #[arg(long = "minimal", action = ArgAction::SetTrue)]
        minimal: bool,
    },

    /// Generate shell completions
    #[cfg(feature = "cli")]
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },

    /// Run preflight checks for configuration and environment
    Doctor,
}

impl Cli {
    /// Apply CLI argument overrides to the configuration
    fn apply_to_config(&self, config: &mut Config) -> Result<(), String> {
        if let Some(port) = self.port {
            config.server.port = port;
        }
        if let Some(ref bind) = self.bind {
            config.server.bind = bind.clone();
        }
        if let Some(ref level) = self.loglevel {
            config.logging.level = level.clone();
        }
        if let Some(databases) = self.databases {
            if !(1..=16).contains(&databases) {
                return Err("databases must be between 1 and 16".to_string());
            }
            config.storage.databases = databases;
        }
        if let Some(ref data_dir) = self.data_dir {
            config.storage.data_dir = data_dir.clone();
        }
        if let Some(ref metrics_bind) = self.metrics_bind {
            config.metrics.bind = metrics_bind.clone();
        }
        if let Some(metrics_port) = self.metrics_port {
            config.metrics.port = metrics_port;
        }

        // Parse --set key=value overrides
        for override_str in &self.config_overrides {
            let parts: Vec<&str> = override_str.splitn(2, '=').collect();
            if parts.len() != 2 {
                return Err(format!(
                    "Invalid config override '{}': expected key=value format",
                    override_str
                ));
            }
            let key = parts[0];
            let value = parts[1];

            // Handle common config paths
            match key {
                "server.port" => {
                    config.server.port = value
                        .parse()
                        .map_err(|_| format!("Invalid port value: {}", value))?;
                }
                "server.bind" => {
                    config.server.bind = value.to_string();
                }
                "logging.level" => {
                    config.logging.level = value.to_string();
                }
                "storage.databases" => {
                    config.storage.databases = value
                        .parse()
                        .map_err(|_| format!("Invalid databases value: {}", value))?;
                }
                "persistence.aof_enabled" => {
                    config.persistence.aof_enabled = value
                        .parse()
                        .map_err(|_| format!("Invalid boolean value: {}", value))?;
                }
                "metrics.enabled" => {
                    config.metrics.enabled = value
                        .parse()
                        .map_err(|_| format!("Invalid boolean value: {}", value))?;
                }
                "metrics.bind" => {
                    config.metrics.bind = value.to_string();
                }
                "metrics.port" => {
                    config.metrics.port = value
                        .parse()
                        .map_err(|_| format!("Invalid metrics port value: {}", value))?;
                }
                _ => {
                    return Err(format!("Unknown config key: {}", key));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum ConfigSource {
    Explicit(PathBuf),
    DefaultFile(PathBuf),
    ExampleFile(PathBuf),
    Defaults,
}

impl ConfigSource {
    fn label(&self) -> String {
        match self {
            ConfigSource::Explicit(path)
            | ConfigSource::DefaultFile(path)
            | ConfigSource::ExampleFile(path) => path.display().to_string(),
            ConfigSource::Defaults => "built-in defaults".to_string(),
        }
    }
}

fn load_config(cli: &Cli) -> Result<(Config, ConfigSource), StartupError> {
    if let Some(path) = &cli.config {
        if !path.exists() {
            return Err(StartupError::config_not_found(path));
        }
        let config = Config::from_file(path)
            .map_err(|e| StartupError::config_parse_error(Some(path), &e.to_string()))?;
        return Ok((config, ConfigSource::Explicit(path.clone())));
    }

    let default_path = PathBuf::from("ferrite.toml");
    if default_path.exists() {
        let config = Config::from_file(&default_path)
            .map_err(|e| StartupError::config_parse_error(Some(&default_path), &e.to_string()))?;
        return Ok((config, ConfigSource::DefaultFile(default_path)));
    }

    let example_path = PathBuf::from("ferrite.example.toml");
    if example_path.exists() {
        let config = Config::from_file(&example_path)
            .map_err(|e| StartupError::config_parse_error(Some(&example_path), &e.to_string()))?;
        return Ok((config, ConfigSource::ExampleFile(example_path)));
    }

    Ok((Config::default(), ConfigSource::Defaults))
}

fn init_logging(config: &Config) {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.logging.level));

    match config.logging.format {
        ferrite::config::LogFormat::Json => {
            tracing_subscriber::registry()
                .with(filter)
                .with(fmt::layer().json())
                .init();
        }
        ferrite::config::LogFormat::Pretty => {
            tracing_subscriber::registry()
                .with(filter)
                .with(fmt::layer().pretty())
                .init();
        }
    }
}

fn generate_config_toml(data_dir: &std::path::Path, minimal: bool) -> String {
    let data_dir_str = data_dir.display();

    if minimal {
        format!(
            r#"# Ferrite Configuration
# Generated by `ferrite init`

[server]
bind = "127.0.0.1"
port = 6379

[storage]
data_dir = "{data_dir_str}"
databases = 16
max_memory = 1073741824  # 1GB

[persistence]
aof_enabled = true
aof_path = "{data_dir_str}/appendonly.aof"
checkpoint_enabled = true
checkpoint_interval = 300
checkpoint_dir = "{data_dir_str}/checkpoints"

[metrics]
enabled = true
port = 9090
bind = "127.0.0.1"

[logging]
level = "info"
format = "pretty"
"#
        )
    } else {
        format!(
            r##"# Ferrite Configuration
# Generated by `ferrite init`
#
# This file configures the Ferrite server. For full documentation, see:
# https://ferrite.dev/docs/reference/configuration

# ============================================================================
# Server Configuration
# ============================================================================
[server]
# Address to bind to. Use 0.0.0.0 to accept connections from any interface.
bind = "127.0.0.1"

# Port to listen on. Default Redis port is 6379.
port = 6379

# Maximum number of simultaneous client connections.
max_connections = 10000

# TCP keepalive interval in seconds (0 to disable).
tcp_keepalive = 300

# Client timeout in seconds (0 to disable).
timeout = 0

# ============================================================================
# Storage Configuration
# ============================================================================
[storage]
# Directory for persistent data files.
data_dir = "{data_dir_str}"

# Number of logical databases (1-16).
databases = 16

# Maximum memory usage in bytes. When exceeded, eviction policies apply.
# Default: 1GB (1073741824 bytes)
max_memory = 1073741824

# Storage backend: "memory" for pure in-memory, "hybridlog" for tiered storage.
backend = "memory"

# Maximum key size in bytes (512MB default).
max_key_size = 536870912

# Maximum value size in bytes (512MB default).
max_value_size = 536870912

# ============================================================================
# Persistence Configuration
# ============================================================================
[persistence]
# Enable append-only file for durability.
aof_enabled = true

# Path to the append-only file.
aof_path = "{data_dir_str}/appendonly.aof"

# AOF sync policy: "always", "everysecond" (default), or "no".
# Also accepts "everysec" as an alias for "everysecond".
# - "always": Sync after every write (safest, slowest)
# - "everysecond": Sync every second (good balance)
# - "no": Let OS handle syncing (fastest, less safe)
aof_sync = "everysecond"

# Enable periodic checkpoints (RDB-like snapshots).
checkpoint_enabled = true

# Checkpoint interval.
checkpoint_interval = 300

# Checkpoint directory.
checkpoint_dir = "{data_dir_str}/checkpoints"

# ============================================================================
# Metrics Configuration
# ============================================================================
[metrics]
# Enable Prometheus metrics endpoint.
enabled = true

# Metrics server port.
port = 9090

# Metrics bind address.
bind = "127.0.0.1"

# ============================================================================
# Logging Configuration
# ============================================================================
[logging]
# Log level: trace, debug, info, warn, error
level = "info"

# Log format: "pretty" for human-readable, "json" for structured logging.
format = "pretty"

# Optional log file path. If not set, logs go to stdout.
# file = "/var/log/ferrite/ferrite.log"

# ============================================================================
# TLS Configuration (optional)
# ============================================================================
# [tls]
# enabled = false
# port = 6380
# cert_file = "/path/to/cert.pem"
# key_file = "/path/to/key.pem"
# ca_file = "/path/to/ca.pem"  # For mTLS
# require_client_cert = false

# ============================================================================
# Cluster Configuration (optional)
# ============================================================================
# [cluster]
# enabled = false
# node_id = "node1"
# announce_ip = "192.168.1.10"
# announce_port = 6379
# cluster_port = 16379

# ============================================================================
# Replication Configuration (optional)
# ============================================================================
# [replication]
# role = "primary"  # or "replica"
# primary_host = "primary.example.com"
# primary_port = 6379
# replica_read_only = true
"##
        )
    }
}

fn cmd_init(
    output: PathBuf,
    data_dir: PathBuf,
    force: bool,
    no_dirs: bool,
    minimal: bool,
) -> ExitCode {
    // Check if config file already exists
    if output.exists() && !force {
        let err = StartupError::Other {
            message: format!("Configuration file '{}' already exists", output.display()),
            suggestion: Some("Use --force to overwrite the existing configuration.".to_string()),
        };
        eprintln!("{}", format_startup_error(&err));
        return ExitCode::FAILURE;
    }

    // Create data directory if requested
    if !no_dirs {
        if let Err(e) = fs::create_dir_all(&data_dir) {
            let err = if e.kind() == std::io::ErrorKind::PermissionDenied {
                StartupError::permission_denied(&data_dir, "creating")
            } else {
                StartupError::io_error("creating data directory", Some(&data_dir), e)
            };
            eprintln!("{}", format_startup_error(&err));
            return ExitCode::FAILURE;
        }
        println!("Created data directory: {}", data_dir.display());
    }

    // Generate configuration content
    let config_content = generate_config_toml(data_dir.as_path(), minimal);

    // Write configuration file
    match fs::File::create(&output) {
        Ok(mut file) => {
            if let Err(e) = file.write_all(config_content.as_bytes()) {
                let err = StartupError::io_error("writing configuration file", Some(&output), e);
                eprintln!("{}", format_startup_error(&err));
                return ExitCode::FAILURE;
            }
        }
        Err(e) => {
            let err = if e.kind() == std::io::ErrorKind::PermissionDenied {
                StartupError::permission_denied(&output, "creating")
            } else {
                StartupError::io_error("creating configuration file", Some(&output), e)
            };
            eprintln!("{}", format_startup_error(&err));
            return ExitCode::FAILURE;
        }
    }

    println!("Created configuration file: {}", output.display());
    println!();
    println!("Ferrite initialized successfully!");
    println!();
    println!("To start Ferrite:");
    println!("  ferrite --config {}", output.display());
    println!();
    println!("Or set environment variables:");
    println!("  export FERRITE_PORT=6379");
    println!("  export FERRITE_DATA_DIR={}", data_dir.display());
    println!("  ferrite");
    println!();
    println!("For more information, visit: https://ferrite.dev/docs");

    ExitCode::SUCCESS
}

#[cfg(feature = "cli")]
fn cmd_completions(shell: clap_complete::Shell) -> ExitCode {
    use clap::CommandFactory;
    use clap_complete::generate;

    let mut cmd = Cli::command();
    let name = cmd.get_name().to_string();
    generate(shell, &mut cmd, name, &mut std::io::stdout());

    ExitCode::SUCCESS
}

fn cmd_doctor(cli: &Cli) -> ExitCode {
    // Determine config path and port from CLI args
    let config_path = cli.config.as_ref().map(|p| p.to_string_lossy().to_string());
    let port = cli.port.unwrap_or(6379);

    // Run structured diagnostics
    let results = doctor::run_diagnostics(config_path.as_deref(), port);
    doctor::print_report(&results);

    if doctor::all_passed(&results) {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

async fn cmd_run(cli: &Cli) -> ExitCode {
    // Load configuration from file or use defaults
    let (mut config, source) = match load_config(cli) {
        Ok(result) => result,
        Err(err) => {
            eprintln!("{}", format_startup_error(&err));
            return ExitCode::FAILURE;
        }
    };

    match &source {
        ConfigSource::ExampleFile(path) => {
            eprintln!(
                "Using {} as configuration. Run `ferrite init` to generate ferrite.toml.",
                path.display()
            );
        }
        ConfigSource::Defaults => {
            eprintln!("No config file found; starting with built-in defaults (memory backend, port 6379).");
            eprintln!("Run 'ferrite init' to create a configuration file.");
        }
        _ => {}
    }

    // Apply CLI argument overrides (includes environment variables via clap)
    if let Err(e) = cli.apply_to_config(&mut config) {
        let err = StartupError::InvalidConfigValue {
            key: "CLI override".to_string(),
            value: None,
            expected: e,
        };
        eprintln!("{}", format_startup_error(&err));
        return ExitCode::FAILURE;
    }

    // Validate configuration
    if let Err(e) = config.validate() {
        let err = StartupError::InvalidConfigValue {
            key: "configuration".to_string(),
            value: None,
            expected: e.to_string(),
        };
        eprintln!("{}", format_startup_error(&err));
        return ExitCode::FAILURE;
    }

    // Dump effective configuration and exit
    if cli.dump_config {
        match toml::to_string_pretty(&config) {
            Ok(output) => {
                print!("{output}");
                return ExitCode::SUCCESS;
            }
            Err(e) => {
                let err = StartupError::Other {
                    message: format!("Failed to serialize configuration: {}", e),
                    suggestion: Some("Try removing invalid values from the config.".to_string()),
                };
                eprintln!("{}", format_startup_error(&err));
                return ExitCode::FAILURE;
            }
        }
    }

    // Handle --test-config: validate and exit without starting server
    if cli.test_config {
        println!("Configuration OK");
        println!("  Bind: {}:{}", config.server.bind, config.server.port);
        println!("  Databases: {}", config.storage.databases);
        println!("  Log level: {}", config.logging.level);
        println!("  AOF enabled: {}", config.persistence.aof_enabled);
        println!("  Metrics enabled: {}", config.metrics.enabled);
        println!("  Data directory: {}", config.storage.data_dir.display());
        return ExitCode::SUCCESS;
    }

    // Initialize OpenTelemetry (before logging, so OTel layer can be composed)
    let otel_config = TelemetryConfig {
        enabled: config.otel.enabled,
        endpoint: config.otel.endpoint.clone(),
        service_name: config.otel.service_name.clone(),
        traces_enabled: config.otel.traces_enabled,
        metrics_enabled: config.otel.metrics_enabled,
        ..TelemetryConfig::default()
    };
    let telemetry_handle = if otel_config.enabled {
        match telemetry::init_telemetry(&otel_config) {
            Ok(handle) => {
                // When OTel is initialized, it sets up the tracing subscriber,
                // so skip init_logging to avoid double-init.
                Some(handle)
            }
            Err(e) => {
                eprintln!("Warning: Failed to initialize OpenTelemetry: {}", e);
                init_logging(&config);
                None
            }
        }
    } else {
        init_logging(&config);
        None
    };

    // Print startup banner
    print_banner(&config, &source);

    // Create and run server
    let port = config.server.port;
    let bind_addr = config.server.bind.clone();
    match Server::new(config).await {
        Ok(server) => match server.run().await {
            Ok(()) => {
                if let Some(handle) = telemetry_handle {
                    telemetry::shutdown_telemetry(handle);
                }
                info!("Ferrite shut down gracefully");
                ExitCode::SUCCESS
            }
            Err(e) => {
                // Check for common error types and provide actionable messages
                let error_str = e.to_string().to_lowercase();
                if error_str.contains("address already in use") || error_str.contains("addr in use")
                {
                    let err = StartupError::port_in_use(port);
                    eprintln!("{}", format_startup_error(&err));
                } else if error_str.contains("permission denied") {
                    let err = StartupError::BindAddressError {
                        address: format!("{}:{}", bind_addr, port),
                        message:
                            "Permission denied - ports below 1024 require root/admin privileges"
                                .to_string(),
                    };
                    eprintln!("{}", format_startup_error(&err));
                } else {
                    error!("Server error: {}", e);
                }
                ExitCode::FAILURE
            }
        },
        Err(e) => {
            // Check for common startup errors and provide actionable messages
            let error_str = e.to_string().to_lowercase();
            if error_str.contains("address already in use") || error_str.contains("addr in use") {
                let err = StartupError::port_in_use(port);
                eprintln!("{}", format_startup_error(&err));
            } else if error_str.contains("permission denied") {
                let err = StartupError::BindAddressError {
                    address: format!("{}:{}", bind_addr, port),
                    message: "Permission denied - ports below 1024 require root/admin privileges"
                        .to_string(),
                };
                eprintln!("{}", format_startup_error(&err));
            } else if error_str.contains("invalid") && error_str.contains("address") {
                let err = StartupError::BindAddressError {
                    address: format!("{}:{}", bind_addr, port),
                    message: e.to_string(),
                };
                eprintln!("{}", format_startup_error(&err));
            } else {
                error!("Failed to create server: {}", e);
                eprintln!("\nRun 'ferrite doctor' for detailed diagnostics.");
            }
            ExitCode::FAILURE
        }
    }
}

fn print_banner(config: &Config, source: &ConfigSource) {
    let version = env!("CARGO_PKG_VERSION");
    let address = config.server.address();

    info!("Starting Ferrite v{}", version);
    info!("  Config: {}", source.label());
    info!("  Listening on: {}", address);
    info!("  Databases: {}", config.storage.databases);
    if config.metrics.enabled {
        info!(
            "  Metrics: {}://{}:{}{}",
            if config.tls.enabled { "https" } else { "http" },
            config.server.bind,
            config.metrics.port,
            "/metrics"
        );
    }
    if config.persistence.aof_enabled {
        info!("  AOF: enabled (sync: {:?})", config.persistence.aof_sync);
    }
    info!("  Data directory: {}", config.storage.data_dir.display());

    if !config.tls.enabled {
        warn!("TLS is disabled — connections are unencrypted. Enable TLS for production use.");
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    // Parse CLI arguments using clap (handles --help and --version automatically)
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Init {
            output,
            data_dir,
            force,
            no_dirs,
            minimal,
        }) => cmd_init(output.clone(), data_dir.clone(), *force, *no_dirs, *minimal),

        #[cfg(feature = "cli")]
        Some(Commands::Completions { shell }) => cmd_completions(*shell),

        Some(Commands::Doctor) => cmd_doctor(&cli),

        Some(Commands::Run) | None => cmd_run(&cli).await,
    }
}
