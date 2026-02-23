//! Interactive REPL for ferrite-cli

use std::process::ExitCode;

use colored::Colorize;
use rustyline::error::ReadlineError;
use rustyline::history::FileHistory;
use rustyline::{Config, EditMode, Editor};

use crate::client::FerriteClient;
use crate::completer::FerriteHelper;
use crate::output::{get_formatter, OutputFormatter};
use crate::OutputFormat;

/// Interactive REPL for Ferrite/Redis
pub struct Repl {
    client: FerriteClient,
    editor: Editor<FerriteHelper, FileHistory>,
    formatter: Box<dyn OutputFormatter>,
    host: String,
    port: u16,
    db: u8,
}

impl Repl {
    /// Create a new REPL instance
    pub fn new(
        client: FerriteClient,
        format: OutputFormat,
        host: String,
        port: u16,
        db: u8,
    ) -> Result<Self, String> {
        let config = Config::builder()
            .history_ignore_space(true)
            .edit_mode(EditMode::Emacs)
            .build();

        let helper = FerriteHelper::new();
        let mut editor =
            Editor::with_config(config).map_err(|e| format!("Failed to create editor: {}", e))?;
        editor.set_helper(Some(helper));

        // Load history
        let history_path = dirs::home_dir()
            .map(|p| p.join(".ferrite_cli_history"))
            .unwrap_or_else(|| ".ferrite_cli_history".into());
        let _ = editor.load_history(&history_path);

        Ok(Self {
            client,
            editor,
            formatter: get_formatter(format),
            host,
            port,
            db,
        })
    }

    /// Run the REPL loop
    pub async fn run(&mut self) -> ExitCode {
        println!(
            "Connected to {}:{} (database {})",
            self.host.cyan(),
            self.port.to_string().cyan(),
            self.db.to_string().cyan()
        );
        println!(
            "Type {} for help, {} to exit",
            "HELP".green(),
            "QUIT".green()
        );
        println!();

        loop {
            let prompt = format!("{}:{}[{}]> ", self.host, self.port, self.db);

            match self.editor.readline(&prompt) {
                Ok(line) => {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }

                    // Add to history
                    let _ = self.editor.add_history_entry(line);

                    // Parse command
                    let parts: Vec<String> =
                        line.split_whitespace().map(|s| s.to_string()).collect();
                    if parts.is_empty() {
                        continue;
                    }

                    // Handle local commands
                    let cmd_upper = parts[0].to_uppercase();
                    match cmd_upper.as_str() {
                        "QUIT" | "EXIT" => {
                            println!("Goodbye!");
                            self.save_history();
                            return ExitCode::SUCCESS;
                        }
                        "CLEAR" => {
                            print!("\x1B[2J\x1B[1;1H");
                            continue;
                        }
                        "HELP" => {
                            self.print_help();
                            continue;
                        }
                        _ => {}
                    }

                    // Send command to server
                    let parts_refs: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
                    match self.client.send_command(&parts_refs).await {
                        Ok(response) => {
                            // Handle SELECT to update prompt
                            if cmd_upper == "SELECT" && !response.is_error() {
                                if let Ok(new_db) = parts
                                    .get(1)
                                    .map(|s| s.as_str())
                                    .unwrap_or("0")
                                    .parse::<u8>()
                                {
                                    self.db = new_db;
                                }
                            }

                            println!("{}", self.formatter.format(&response));
                        }
                        Err(e) => {
                            eprintln!("{}: {}", "Error".red(), e);
                        }
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    println!("^C");
                    continue;
                }
                Err(ReadlineError::Eof) => {
                    println!("^D");
                    self.save_history();
                    return ExitCode::SUCCESS;
                }
                Err(e) => {
                    eprintln!("{}: {}", "Readline error".red(), e);
                    return ExitCode::FAILURE;
                }
            }
        }
    }

    /// Parse a line into arguments, handling quoted strings
    #[allow(dead_code)] // Planned for v0.2 — reserved for enhanced REPL input parsing
    fn parse_line<'a>(&self, line: &'a str) -> Vec<&'a str> {
        // Simple split for now - could be enhanced to handle quotes
        line.split_whitespace().collect()
    }

    /// Print help information
    fn print_help(&self) {
        println!("{}", "ferrite-cli - Interactive Redis/Ferrite CLI".bold());
        println!();
        println!("{}", "Local commands:".underline());
        println!("  {}        - Exit the CLI", "QUIT".green());
        println!("  {}       - Clear the screen", "CLEAR".green());
        println!("  {}        - Show this help", "HELP".green());
        println!();
        println!("{}", "Usage:".underline());
        println!("  Type any Redis command to execute it");
        println!("  Use {} for command completion", "TAB".cyan());
        println!(
            "  Use {}/{} for history navigation",
            "UP".cyan(),
            "DOWN".cyan()
        );
        println!();
        println!("{}", "Examples:".underline());
        println!("  SET mykey \"Hello World\"");
        println!("  GET mykey");
        println!("  KEYS *");
        println!("  INFO server");
    }

    /// Save command history
    fn save_history(&mut self) {
        let history_path = dirs::home_dir()
            .map(|p| p.join(".ferrite_cli_history"))
            .unwrap_or_else(|| ".ferrite_cli_history".into());
        let _ = self.editor.save_history(&history_path);
    }
}

// Add dirs crate dependency stub
mod dirs {
    use std::path::PathBuf;

    pub fn home_dir() -> Option<PathBuf> {
        std::env::var_os("HOME").map(PathBuf::from)
    }
}
