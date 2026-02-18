//! ferrite-cli - Interactive CLI client for Ferrite/Redis
//!
//! A feature-rich command-line interface with REPL, tab completion,
//! and multiple output formats.

mod client;
mod commands;
mod completer;
mod output;
mod repl;
mod stat;

use std::io::{self, BufRead};
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use colored::Colorize;

/// ferrite-cli - Interactive CLI for Ferrite/Redis
#[derive(Parser, Debug)]
#[command(name = "ferrite-cli")]
#[command(author, version, about = "Interactive CLI client for Ferrite/Redis")]
struct Args {
    /// Host to connect to
    #[arg(short = 'h', long, default_value = "127.0.0.1")]
    host: String,

    /// Port to connect to
    #[arg(short = 'p', long, default_value = "6379")]
    port: u16,

    /// Database number to select (0-15)
    #[arg(short = 'n', long, default_value = "0")]
    db: u8,

    /// Password for authentication
    #[arg(short = 'a', long)]
    password: Option<String>,

    /// Username for ACL authentication
    #[arg(long)]
    user: Option<String>,

    /// Output format
    #[arg(long, value_enum, default_value = "raw")]
    format: OutputFormat,

    /// Enable stat mode (continuous stats display like redis-cli --stat)
    #[arg(long)]
    stat: bool,

    /// Interval for stat mode in seconds
    #[arg(long, default_value = "1")]
    stat_interval: u64,

    /// Commands to execute (non-interactive mode)
    #[arg(trailing_var_arg = true)]
    command: Vec<String>,
}

/// Output format for command results
#[derive(Clone, Copy, Debug, ValueEnum)]
enum OutputFormat {
    /// Raw output (default, similar to redis-cli)
    Raw,
    /// Pretty-printed with colors
    Pretty,
    /// JSON format
    Json,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();

    // Create client and connect
    let mut client = match client::FerriteClient::connect(&args.host, args.port).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{}: {}", "Connection failed".red(), e);
            return ExitCode::FAILURE;
        }
    };

    // Authenticate if password provided
    if let Some(ref password) = args.password {
        if let Err(e) = client.authenticate(args.user.as_deref(), password).await {
            eprintln!("{}: {}", "Authentication failed".red(), e);
            return ExitCode::FAILURE;
        }
    }

    // Select database
    if args.db != 0 {
        if let Err(e) = client.select(args.db).await {
            eprintln!("{}: {}", "Failed to select database".red(), e);
            return ExitCode::FAILURE;
        }
    }

    // Stat mode
    if args.stat {
        let interval = Duration::from_secs(args.stat_interval);
        match stat::run_stat_mode(client, interval).await {
            Ok(()) => return ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("{}: {}", "Stat mode error".red(), e);
                return ExitCode::FAILURE;
            }
        }
    }

    // Non-interactive mode: execute command and exit
    if !args.command.is_empty() {
        let cmd_args: Vec<&str> = args.command.iter().map(|s| s.as_str()).collect();
        match client.send_command(&cmd_args).await {
            Ok(response) => {
                let formatter = output::get_formatter(args.format);
                println!("{}", formatter.format(&response));
                return ExitCode::SUCCESS;
            }
            Err(e) => {
                eprintln!("{}: {}", "Error".red(), e);
                return ExitCode::FAILURE;
            }
        }
    }

    // Check if stdin is a TTY for interactive mode
    if atty::is(atty::Stream::Stdin) {
        // Interactive REPL mode
        let mut repl_instance =
            match repl::Repl::new(client, args.format, args.host.clone(), args.port, args.db) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("{}: {}", "Failed to initialize REPL".red(), e);
                    return ExitCode::FAILURE;
                }
            };
        repl_instance.run().await
    } else {
        // Pipe/batch mode: read commands from stdin
        let stdin = io::stdin();
        let formatter = output::get_formatter(args.format);

        for line in stdin.lock().lines() {
            match line {
                Ok(cmd_line) => {
                    let cmd_line = cmd_line.trim();
                    if cmd_line.is_empty() || cmd_line.starts_with('#') {
                        continue;
                    }

                    let parts: Vec<&str> = cmd_line.split_whitespace().collect();
                    if parts.is_empty() {
                        continue;
                    }

                    match client.send_command(&parts).await {
                        Ok(response) => {
                            println!("{}", formatter.format(&response));
                        }
                        Err(e) => {
                            eprintln!("{}: {}", "Error".red(), e);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("{}: {}", "Read error".red(), e);
                    return ExitCode::FAILURE;
                }
            }
        }

        ExitCode::SUCCESS
    }
}
