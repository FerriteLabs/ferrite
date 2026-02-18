//! Ferrite TUI Dashboard
//!
//! A terminal-based dashboard for monitoring Ferrite/Redis servers.
//!
//! # Usage
//!
//! ```bash
//! cargo run --features tui --bin ferrite-tui
//! cargo run --features tui --bin ferrite-tui -- -h 127.0.0.1 -p 6379
//! cargo run --features tui --bin ferrite-tui -- -a mypassword -i 500
//! ```
//!
//! # Keyboard Shortcuts
//!
//! - `q` - Quit
//! - `Tab` / `1-6` - Switch tabs (Overview, Keys, Commands, Memory, Network, Logs)
//! - `p` - Pause/resume auto-refresh
//! - `/` - Search mode (keys tab: set SCAN pattern)
//! - `:` - Command mode
//! - `j`/`k` or arrows - Navigate lists
//! - `Enter` - Select / view details
//! - `d` - Delete key (with confirmation)

use clap::Parser;

/// Ferrite TUI Dashboard - CLI arguments
#[derive(Parser, Debug)]
#[command(name = "ferrite-tui")]
#[command(about = "Terminal dashboard for Ferrite/Redis servers")]
struct Args {
    /// Server hostname
    #[arg(short = 'h', long = "host", default_value = "127.0.0.1")]
    host: String,

    /// Server port
    #[arg(short = 'p', long = "port", default_value_t = 6379)]
    port: u16,

    /// Password for authentication
    #[arg(short = 'a', long = "password")]
    password: Option<String>,

    /// Refresh interval in milliseconds
    #[arg(short = 'i', long = "interval", default_value_t = 1000)]
    interval: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    ferrite::tui::app::run(
        &args.host,
        args.port,
        args.password.as_deref(),
        args.interval,
    )
    .await
}
