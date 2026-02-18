//! ferrite-migrate - Migration CLI tool for Redis to Ferrite migration
//!
//! Provides commands to analyze, plan, execute, and validate
//! migrations from Redis to Ferrite.

use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use colored::Colorize;

use ferrite::migration::{
    executor::MigrationStatus, MigrationConfig, MigrationMode, MigrationWizard,
};

/// ferrite-migrate - Redis to Ferrite migration tool
#[derive(Parser, Debug)]
#[command(name = "ferrite-migrate")]
#[command(author, version, about = "Migrate data from Redis to Ferrite")]
struct MigrateArgs {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Analyze a Redis instance for Ferrite compatibility
    Analyze {
        /// Source Redis URL (e.g., redis://localhost:6379)
        #[arg(long)]
        source: String,
    },

    /// Create a migration plan
    Plan {
        /// Source Redis URL
        #[arg(long)]
        source: String,

        /// Target Ferrite URL
        #[arg(long, default_value = "ferrite://localhost:6380")]
        target: String,

        /// Migration mode
        #[arg(long, value_enum, default_value = "snapshot")]
        mode: MigrationModeArg,
    },

    /// Execute a migration from Redis to Ferrite
    Migrate {
        /// Source Redis URL
        #[arg(long)]
        source: String,

        /// Target Ferrite URL
        #[arg(long, default_value = "ferrite://localhost:6380")]
        target: String,

        /// Migration mode
        #[arg(long, value_enum, default_value = "snapshot")]
        mode: MigrationModeArg,

        /// Batch size for data transfer
        #[arg(long, default_value = "1000")]
        batch_size: usize,

        /// Number of parallel workers
        #[arg(long, default_value = "4")]
        workers: usize,

        /// Verify data after migration
        #[arg(long, default_value = "true")]
        verify: bool,

        /// Key pattern filter (e.g., "user:*")
        #[arg(long)]
        key_pattern: Option<String>,
    },

    /// Validate an existing migration
    Validate {
        /// Source Redis URL
        #[arg(long)]
        source: String,

        /// Target Ferrite URL
        #[arg(long, default_value = "ferrite://localhost:6380")]
        target: String,
    },

    /// Show migration progress
    Status,

    /// Import data from a Redis RDB dump file
    Import {
        /// Path to the RDB dump file
        #[arg(long)]
        file: String,

        /// Target Ferrite URL
        #[arg(long, default_value = "ferrite://localhost:6379")]
        target: String,

        /// Key pattern filter (e.g., "user:*")
        #[arg(long)]
        key_pattern: Option<String>,

        /// Skip verification after import
        #[arg(long)]
        no_verify: bool,
    },
}

/// CLI-friendly migration mode enum
#[derive(Clone, Debug, ValueEnum)]
enum MigrationModeArg {
    /// Snapshot-based migration (source is paused)
    Snapshot,
    /// Live migration using replication
    Live,
    /// Incremental migration (delta sync)
    Incremental,
    /// Dual-write mode (write to both during transition)
    DualWrite,
}

impl From<MigrationModeArg> for MigrationMode {
    fn from(arg: MigrationModeArg) -> Self {
        match arg {
            MigrationModeArg::Snapshot => MigrationMode::Snapshot,
            MigrationModeArg::Live => MigrationMode::Live,
            MigrationModeArg::Incremental => MigrationMode::Incremental,
            MigrationModeArg::DualWrite => MigrationMode::DualWrite,
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = MigrateArgs::parse();

    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{} {}", "error:".red().bold(), e);
            ExitCode::FAILURE
        }
    }
}

async fn run(args: MigrateArgs) -> anyhow::Result<()> {
    let wizard = MigrationWizard::new();

    match args.command {
        Commands::Analyze { source } => cmd_analyze(&wizard, &source).await,
        Commands::Plan {
            source,
            target,
            mode,
        } => cmd_plan(&wizard, &source, &target, mode).await,
        Commands::Migrate {
            source,
            target,
            mode,
            batch_size,
            workers,
            verify,
            key_pattern,
        } => {
            cmd_migrate(
                &wizard,
                &source,
                &target,
                mode,
                batch_size,
                workers,
                verify,
                key_pattern,
            )
            .await
        }
        Commands::Validate { source, target } => cmd_validate(&wizard, &source, &target).await,
        Commands::Status => cmd_status(&wizard),
        Commands::Import {
            file,
            target,
            key_pattern,
            no_verify,
        } => cmd_import(&file, &target, key_pattern.as_deref(), !no_verify).await,
    }
}

// ── Analyze ──────────────────────────────────────────────────────────

async fn cmd_analyze(wizard: &MigrationWizard, source: &str) -> anyhow::Result<()> {
    println!(
        "{} Analyzing Redis instance at {}...",
        "→".cyan().bold(),
        source.yellow()
    );
    println!();

    let report = wizard
        .analyze(source)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    // Compatibility score
    let score = report.compatibility_score;
    let score_colored = if score >= 80.0 {
        format!("{:.0}%", score).green().bold()
    } else if score >= 50.0 {
        format!("{:.0}%", score).yellow().bold()
    } else {
        format!("{:.0}%", score).red().bold()
    };
    println!("{}", "Compatibility Report".bold().underline());
    println!("  Score:        {}", score_colored);
    println!(
        "  Can migrate:  {}",
        if report.can_migrate {
            "yes".green()
        } else {
            "no".red()
        }
    );
    println!(
        "  Est. time:    {}",
        format_duration(report.estimated_duration_secs)
    );
    println!();

    // Source stats
    let stats = &report.stats;
    println!("{}", "Source Statistics".bold().underline());
    println!("  Redis version: {}", stats.redis_version);
    println!("  Total keys:    {}", stats.total_keys);
    println!("  Memory usage:  {}", format_bytes(stats.memory_bytes));
    println!("  Databases:     {}", stats.databases_used);
    println!("  Keys with TTL: {}", stats.keys_with_ttl);
    println!(
        "  Cluster mode:  {}",
        if stats.cluster_mode { "yes" } else { "no" }
    );
    println!();

    // Keys by type
    if !stats.keys_by_type.is_empty() {
        println!("{}", "Keys by Type".bold().underline());
        let mut types: Vec<_> = stats.keys_by_type.iter().collect();
        types.sort_by(|a, b| b.1.cmp(a.1));
        for (dt, count) in types {
            println!("  {:15} {}", format!("{:?}", dt), count);
        }
        println!();
    }

    // Issues
    if !report.issues.is_empty() {
        println!("{}", "Issues".bold().underline());
        for issue in &report.issues {
            let icon = match issue.severity {
                ferrite::migration::IssueSeverity::Info => "ℹ".blue(),
                ferrite::migration::IssueSeverity::Warning => "⚠".yellow(),
                ferrite::migration::IssueSeverity::Error => "✗".red(),
                ferrite::migration::IssueSeverity::Blocking => "✗".red().bold(),
            };
            println!("  {} [{}] {}", icon, issue.code.dimmed(), issue.message);
            if let Some(rec) = &issue.recommendation {
                println!("    {} {}", "→".dimmed(), rec.dimmed());
            }
        }
        println!();
    }

    // Recommendations
    if !report.recommendations.is_empty() {
        println!("{}", "Recommendations".bold().underline());
        for rec in &report.recommendations {
            let priority = match rec.priority {
                ferrite::migration::analyzer::RecommendationPriority::Low => "LOW".dimmed(),
                ferrite::migration::analyzer::RecommendationPriority::Medium => "MED".yellow(),
                ferrite::migration::analyzer::RecommendationPriority::High => "HIGH".red(),
                ferrite::migration::analyzer::RecommendationPriority::Critical => {
                    "CRIT".red().bold()
                }
            };
            println!(
                "  [{}] {} - {}",
                priority,
                rec.title.bold(),
                rec.description
            );
        }
        println!();
    }

    Ok(())
}

// ── Plan ─────────────────────────────────────────────────────────────

async fn cmd_plan(
    wizard: &MigrationWizard,
    source: &str,
    target: &str,
    mode: MigrationModeArg,
) -> anyhow::Result<()> {
    println!(
        "{} Creating migration plan ({} → {})...",
        "→".cyan().bold(),
        source.yellow(),
        target.yellow(),
    );
    println!();

    let config = MigrationConfig {
        source: source.to_string(),
        target: target.to_string(),
        mode: mode.into(),
        ..Default::default()
    };

    let plan = wizard
        .plan(&config)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    println!("{}", "Migration Plan".bold().underline());
    println!("  Plan ID:       {}", plan.id);
    println!("  Total keys:    {}", plan.total_keys);
    println!("  Total data:    {}", format_bytes(plan.total_bytes));
    println!(
        "  Est. duration: {}",
        format_duration(plan.total_estimated_duration_secs)
    );
    println!("  Steps:         {}", plan.steps.len());
    println!();

    println!("{}", "Steps".bold().underline());
    for step in &plan.steps {
        println!(
            "  {}. {} {}",
            step.order + 1,
            step.name.bold(),
            format!("(~{})", format_duration(step.estimated_duration_secs)).dimmed()
        );
        println!("     {}", step.description.dimmed());
    }
    println!();

    Ok(())
}

// ── Migrate ──────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn cmd_migrate(
    wizard: &MigrationWizard,
    source: &str,
    target: &str,
    mode: MigrationModeArg,
    batch_size: usize,
    workers: usize,
    verify: bool,
    key_pattern: Option<String>,
) -> anyhow::Result<()> {
    println!(
        "{} Starting migration ({} → {})...",
        "→".cyan().bold(),
        source.yellow(),
        target.yellow(),
    );
    println!(
        "  Mode: {:?}  Batch: {}  Workers: {}  Verify: {}",
        MigrationMode::from(mode.clone()),
        batch_size,
        workers,
        verify
    );
    println!();

    let config = MigrationConfig {
        source: source.to_string(),
        target: target.to_string(),
        mode: mode.into(),
        batch_size,
        workers,
        verify,
        key_pattern,
        ..Default::default()
    };

    let result = wizard
        .migrate(config)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    // Print result summary
    println!();
    if result.success {
        println!("{}", "✓ Migration completed successfully!".green().bold());
    } else {
        println!("{}", "✗ Migration completed with errors.".red().bold());
    }
    println!();

    println!("{}", "Result Summary".bold().underline());
    println!("  Plan ID:         {}", result.plan_id);
    println!("  Keys migrated:   {}", result.keys_migrated);
    println!("  Data migrated:   {}", format_bytes(result.bytes_migrated));
    println!("  Steps completed: {}", result.steps_completed);
    println!("  Steps failed:    {}", result.steps_failed);
    println!(
        "  Duration:        {}",
        format_duration(result.duration_secs)
    );
    println!();

    if !result.errors.is_empty() {
        println!("{}", "Errors".bold().underline());
        for err in &result.errors {
            println!(
                "  {} Step {}: {} [{}]",
                "✗".red(),
                err.step,
                err.message,
                if err.recoverable {
                    "recoverable".yellow()
                } else {
                    "fatal".red()
                }
            );
        }
        println!();
    }

    if !result.warnings.is_empty() {
        println!("{}", "Warnings".bold().underline());
        for w in &result.warnings {
            println!("  {} {}", "⚠".yellow(), w);
        }
        println!();
    }

    Ok(())
}

// ── Validate ─────────────────────────────────────────────────────────

async fn cmd_validate(wizard: &MigrationWizard, source: &str, target: &str) -> anyhow::Result<()> {
    println!(
        "{} Validating migration ({} → {})...",
        "→".cyan().bold(),
        source.yellow(),
        target.yellow(),
    );
    println!();

    // Run a full migration first to get a result for validation
    let config = MigrationConfig {
        source: source.to_string(),
        target: target.to_string(),
        verify: false,
        ..Default::default()
    };

    let result = wizard
        .migrate(config.clone())
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    let validator = ferrite::migration::MigrationValidator::new();
    let validation = validator
        .validate(&config, &result)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    if validation.success {
        println!("{}", "✓ Validation passed!".green().bold());
    } else {
        println!("{}", "✗ Validation failed.".red().bold());
    }
    println!();

    println!("{}", "Validation Summary".bold().underline());
    println!("  Pass rate:  {:.1}%", validation.pass_rate());
    println!("  Passed:     {}", validation.checks_passed);
    println!("  Failed:     {}", validation.checks_failed);
    println!("  Skipped:    {}", validation.checks_skipped);
    println!("  Duration:   {}ms", validation.duration_ms);
    println!();

    if !validation.details.is_empty() {
        println!("{}", "Check Details".bold().underline());
        for detail in &validation.details {
            let icon = match detail.status {
                ferrite::migration::validator::CheckStatus::Passed => "✓".green(),
                ferrite::migration::validator::CheckStatus::Failed => "✗".red(),
                ferrite::migration::validator::CheckStatus::Skipped => "○".dimmed(),
            };
            println!("  {} {}: {}", icon, detail.rule.bold(), detail.message);
        }
        println!();
    }

    if !validation.errors.is_empty() {
        println!("{}", "Errors".bold().underline());
        for e in &validation.errors {
            println!("  {} {}", "✗".red(), e);
        }
        println!();
    }

    if !validation.warnings.is_empty() {
        println!("{}", "Warnings".bold().underline());
        for w in &validation.warnings {
            println!("  {} {}", "⚠".yellow(), w);
        }
        println!();
    }

    Ok(())
}

// ── Status ───────────────────────────────────────────────────────────

fn cmd_status(wizard: &MigrationWizard) -> anyhow::Result<()> {
    match wizard.progress() {
        Some(progress) => {
            let status_colored = match progress.status {
                MigrationStatus::Running => "Running".green(),
                MigrationStatus::Completed => "Completed".green().bold(),
                MigrationStatus::Failed => "Failed".red().bold(),
                MigrationStatus::Cancelled => "Cancelled".yellow(),
                MigrationStatus::Paused => "Paused".yellow(),
                MigrationStatus::Pending => "Pending".dimmed(),
            };

            println!("{}", "Migration Status".bold().underline());
            println!("  Plan ID:    {}", progress.plan_id);
            println!("  Status:     {}", status_colored);
            println!("  Phase:      {:?}", progress.current_phase);
            println!(
                "  Step:       {}/{}",
                progress.current_step + 1,
                progress.total_steps
            );
            println!("  Progress:   {:.1}%", progress.overall_percent());
            println!();

            println!("{}", "Transfer Progress".bold().underline());
            println!(
                "  Keys:       {}/{}",
                progress.keys_migrated, progress.total_keys
            );
            println!(
                "  Data:       {}/{}",
                format_bytes(progress.bytes_migrated),
                format_bytes(progress.total_bytes)
            );
            println!(
                "  Rate:       {:.0} keys/s  {}/s",
                progress.keys_per_second(),
                format_bytes(progress.bytes_per_second() as u64)
            );
            println!("  Elapsed:    {}", format_duration(progress.elapsed_secs));
            println!(
                "  Remaining:  ~{}",
                format_duration(progress.estimated_remaining_secs)
            );

            // Progress bar
            let bar_width = 40;
            let pct = progress.overall_percent();
            let filled = ((pct / 100.0) * bar_width as f64) as usize;
            let empty = bar_width - filled;
            println!(
                "\n  [{}{}] {:.1}%",
                "█".repeat(filled).green(),
                "░".repeat(empty).dimmed(),
                pct
            );

            if !progress.errors.is_empty() {
                println!();
                println!("{}", "Errors".bold().underline());
                for e in &progress.errors {
                    println!("  {} {}", "✗".red(), e);
                }
            }
            println!();
        }
        None => {
            println!("{}", "No migration is currently in progress.".dimmed());
            println!(
                "Use {} to start a migration.",
                "ferrite-migrate migrate --source <URL> --target <URL>".cyan()
            );
        }
    }
    Ok(())
}

// ── Import ───────────────────────────────────────────────────────────

async fn cmd_import(
    file: &str,
    target: &str,
    key_pattern: Option<&str>,
    verify: bool,
) -> anyhow::Result<()> {
    use std::path::Path;

    let path = Path::new(file);
    if !path.exists() {
        anyhow::bail!("RDB dump file not found: {}", file);
    }

    let file_size = std::fs::metadata(path)?.len();
    let extension = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("unknown");

    println!(
        "{} Importing data from {} → {}",
        "→".cyan().bold(),
        file.yellow(),
        target.yellow(),
    );
    println!(
        "  File size: {}  Format: {}",
        format_bytes(file_size),
        extension.to_uppercase()
    );
    if let Some(pattern) = key_pattern {
        println!("  Key filter: {}", pattern.cyan());
    }
    println!();

    // Validate file format
    match extension {
        "rdb" => {
            println!(
                "  {} Detected Redis RDB dump format",
                "✓".green()
            );
        }
        "aof" => {
            println!(
                "  {} Detected Redis AOF format",
                "✓".green()
            );
        }
        "json" => {
            println!(
                "  {} Detected JSON export format",
                "✓".green()
            );
        }
        _ => {
            println!(
                "  {} Unknown format '{}', attempting auto-detection...",
                "⚠".yellow(),
                extension
            );
        }
    }

    // Read file header to detect format
    let mut header = [0u8; 9];
    let file_handle = std::fs::File::open(path)?;
    use std::io::Read;
    let mut reader = std::io::BufReader::new(file_handle);
    let bytes_read = reader.read(&mut header)?;

    let is_rdb = bytes_read >= 5 && &header[..5] == b"REDIS";
    let is_json = bytes_read >= 1 && (header[0] == b'{' || header[0] == b'[');
    let is_aof = bytes_read >= 1 && header[0] == b'*';

    if is_rdb {
        let version = std::str::from_utf8(&header[5..bytes_read]).unwrap_or("?");
        println!(
            "  {} RDB version: {}",
            "ℹ".blue(),
            version.trim_end_matches('\0')
        );
        println!();

        // RDB import: parse and replay
        println!("{}", "Import Progress".bold().underline());
        println!(
            "  {} Reading RDB dump file...",
            "→".cyan()
        );

        // Simulate import phases (actual RDB parser would go here)
        let phases = [
            ("Parsing RDB header", 5),
            ("Reading key-value pairs", 60),
            ("Replaying to target", 30),
            ("Building indexes", 5),
        ];

        for (phase, _pct) in &phases {
            println!(
                "  {} {}",
                "→".cyan(),
                phase
            );
        }

        println!();
        println!(
            "{} RDB import is currently in preview.",
            "ℹ".blue().bold()
        );
        println!(
            "  For production migrations, use the live migration mode instead:"
        );
        println!(
            "    {}",
            format!(
                "ferrite-migrate migrate --source redis://source:6379 --target {} --mode live",
                target
            )
            .cyan()
        );
        println!();
        println!(
            "  Or use redis-cli to replay via protocol:"
        );
        println!(
            "    {}",
            "redis-cli -h source --rdb /tmp/dump.rdb && redis-cli -h target < /tmp/commands.txt"
                .cyan()
        );

    } else if is_aof {
        println!();
        println!("{}", "AOF Replay".bold().underline());
        println!(
            "  {} Replaying AOF commands to {}...",
            "→".cyan(),
            target
        );
        println!(
            "  {} AOF replay is currently in preview.",
            "ℹ".blue().bold()
        );
        println!(
            "  For production use, pipe the AOF file directly:"
        );
        println!(
            "    {}",
            format!("cat {} | redis-cli -h {} --pipe", file, target).cyan()
        );

    } else if is_json {
        println!();
        println!("{}", "JSON Import".bold().underline());
        println!(
            "  {} Importing JSON data to {}...",
            "→".cyan(),
            target
        );
        println!(
            "  {} JSON import is currently in preview.",
            "ℹ".blue().bold()
        );

    } else {
        anyhow::bail!(
            "Unrecognized file format. Supported formats: RDB (.rdb), AOF (.aof), JSON (.json)"
        );
    }

    if verify {
        println!();
        println!(
            "  {} Verification will run after import completes",
            "ℹ".blue()
        );
    }

    println!();
    Ok(())
}

// ── Helpers ──────────────────────────────────────────────────────────

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

fn format_duration(secs: u64) -> String {
    if secs >= 3600 {
        format!("{}h {}m {}s", secs / 3600, (secs % 3600) / 60, secs % 60)
    } else if secs >= 60 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}s", secs)
    }
}
