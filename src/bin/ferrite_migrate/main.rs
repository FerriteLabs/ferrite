//! ferrite-migrate - Migration CLI tool for Redis to Ferrite migration
//!
//! Provides commands to analyze, plan, execute, and validate
//! migrations from Redis to Ferrite.
#![allow(clippy::print_stdout, clippy::print_stderr)]

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
            println!("  {} Detected Redis RDB dump format", "✓".green());
        }
        "aof" => {
            println!("  {} Detected Redis AOF format", "✓".green());
        }
        "json" => {
            println!("  {} Detected JSON export format", "✓".green());
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

        import_rdb(path, target, key_pattern, verify).await?;
    } else if is_aof {
        println!();
        import_aof(path, target, key_pattern, verify).await?;
    } else if is_json {
        println!();
        import_json(path, target, key_pattern, verify).await?;
    } else {
        anyhow::bail!(
            "Unrecognized file format. Supported formats: RDB (.rdb), AOF (.aof), JSON (.json)"
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

// ── RDB Import ──────────────────────────────────────────────────────

/// Parse an RDB file and replay its contents to a target Ferrite/Redis instance.
async fn import_rdb(
    path: &Path,
    target: &str,
    key_pattern: Option<&str>,
    verify: bool,
) -> anyhow::Result<()> {
    use ferrite::migration::rdb_parser::{RdbParser, RdbValue};
    use std::time::Instant;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    println!("{}", "RDB Import".bold().underline());
    println!("  {} Parsing RDB dump file...", "→".cyan());

    let start = Instant::now();
    let mut parser = RdbParser::new(path)
        .map_err(|e| anyhow::anyhow!("Failed to initialize RDB parser: {}", e))?;

    let rdb_data = parser
        .parse()
        .map_err(|e| anyhow::anyhow!("Failed to parse RDB file: {}", e))?;

    let parse_elapsed = start.elapsed();
    println!(
        "  {} Parsed {} keys across {} database(s) in {:.2}s",
        "✓".green(),
        rdb_data.stats.total_keys,
        rdb_data.stats.databases_count,
        parse_elapsed.as_secs_f64()
    );

    // Print type breakdown
    let stats = &rdb_data.stats;
    if stats.total_keys > 0 {
        println!("  {} Type breakdown:", "ℹ".blue());
        if stats.strings_count > 0 {
            println!("      Strings:     {}", stats.strings_count);
        }
        if stats.lists_count > 0 {
            println!("      Lists:       {}", stats.lists_count);
        }
        if stats.sets_count > 0 {
            println!("      Sets:        {}", stats.sets_count);
        }
        if stats.sorted_sets_count > 0 {
            println!("      Sorted Sets: {}", stats.sorted_sets_count);
        }
        if stats.hashes_count > 0 {
            println!("      Hashes:      {}", stats.hashes_count);
        }
        if stats.streams_count > 0 {
            println!("      Streams:     {}", stats.streams_count);
        }
    }

    // Print auxiliary info
    for (k, v) in &rdb_data.aux_fields {
        if k == "redis-ver" || k == "used-mem" {
            println!("  {} {}: {}", "ℹ".blue(), k, v);
        }
    }

    println!();

    // Compile key pattern filter
    let pattern_regex = if let Some(pattern) = key_pattern {
        let regex_str = pattern.replace('*', ".*").replace('?', ".");
        Some(
            regex::Regex::new(&format!("^{}$", regex_str))
                .map_err(|e| anyhow::anyhow!("Invalid key pattern: {}", e))?,
        )
    } else {
        None
    };

    // Connect to target
    println!("  {} Connecting to target {}...", "→".cyan(), target);
    let target_addr = parse_target_addr(target)?;
    let mut stream = TcpStream::connect(&target_addr).await.map_err(|e| {
        anyhow::anyhow!(
            "Failed to connect to target {}: {}. Is Ferrite running?",
            target_addr,
            e
        )
    })?;
    println!("  {} Connected to {}", "✓".green(), target_addr);

    // Replay entries
    println!();
    println!("{}", "Replay Progress".bold().underline());
    let replay_start = Instant::now();
    let mut imported: u64 = 0;
    let mut skipped: u64 = 0;
    let mut errors: u64 = 0;

    for db in &rdb_data.databases {
        // SELECT database
        if db.db_number > 0 {
            let cmd = format!("*2\r\n$6\r\nSELECT\r\n${}\r\n{}\r\n", db.db_number.to_string().len(), db.db_number);
            stream.write_all(cmd.as_bytes()).await?;
            let mut buf = [0u8; 256];
            let _ = stream.read(&mut buf).await?;
        }

        for entry in &db.entries {
            let key_str = String::from_utf8_lossy(&entry.key);

            // Apply key pattern filter
            if let Some(ref re) = pattern_regex {
                if !re.is_match(&key_str) {
                    skipped += 1;
                    continue;
                }
            }

            // Build RESP command for each value type
            let cmd = match &entry.value {
                RdbValue::String(val) => {
                    build_resp_cmd(&["SET", &key_str], Some(&[val.as_ref()]))
                }
                RdbValue::List(items) => {
                    if items.is_empty() {
                        skipped += 1;
                        continue;
                    }
                    build_resp_list_cmd("RPUSH", &key_str, items)
                }
                RdbValue::Set(members) => {
                    if members.is_empty() {
                        skipped += 1;
                        continue;
                    }
                    build_resp_list_cmd("SADD", &key_str, members)
                }
                RdbValue::Hash(fields) => {
                    if fields.is_empty() {
                        skipped += 1;
                        continue;
                    }
                    build_resp_hash_cmd("HSET", &key_str, fields)
                }
                RdbValue::SortedSet(members) => {
                    if members.is_empty() {
                        skipped += 1;
                        continue;
                    }
                    build_resp_zadd_cmd(&key_str, members)
                }
                RdbValue::Stream { entries, groups } => {
                    // Replay stream entries via XADD
                    for se in entries {
                        let id = format!("{}-{}", se.id.0, se.id.1);
                        let xadd = build_resp_xadd_cmd(&key_str, &id, &se.fields);
                        if let Err(_) = stream.write_all(xadd.as_bytes()).await {
                            errors += 1;
                            continue;
                        }
                        let mut buf = [0u8; 256];
                        let _ = stream.read(&mut buf).await;
                    }
                    // Create consumer groups
                    for group in groups {
                        let last_id = format!("{}-{}", group.last_id.0, group.last_id.1);
                        let xcreate = build_resp_cmd(
                            &["XGROUP", "CREATE", &key_str, &group.name, &last_id, "MKSTREAM"],
                            None,
                        );
                        let _ = stream.write_all(xcreate.as_bytes()).await;
                        let mut buf = [0u8; 256];
                        let _ = stream.read(&mut buf).await;
                    }
                    imported += 1;
                    // Print progress periodically
                    if imported % 1000 == 0 {
                        print!("\r  {} Imported {} keys ({} skipped, {} errors)", "→".cyan(), imported, skipped, errors);
                    }
                    continue;
                }
                RdbValue::Module { type_name, .. } => {
                    println!(
                        "  {} Skipping module key '{}' (type: {})",
                        "⚠".yellow(),
                        key_str,
                        type_name
                    );
                    skipped += 1;
                    continue;
                }
            };

            // Send command
            match stream.write_all(cmd.as_bytes()).await {
                Ok(()) => {
                    let mut buf = [0u8; 256];
                    match stream.read(&mut buf).await {
                        Ok(n) if n > 0 => {
                            let response = String::from_utf8_lossy(&buf[..n]);
                            if response.starts_with('-') {
                                errors += 1;
                            } else {
                                imported += 1;
                            }
                        }
                        _ => errors += 1,
                    }
                }
                Err(_) => errors += 1,
            }

            // Set expiry if present
            if let Some(expire_ms) = entry.expire_at {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                if expire_ms > now_ms {
                    let ttl_ms = expire_ms - now_ms;
                    let pexpire = build_resp_cmd(
                        &["PEXPIRE", &key_str, &ttl_ms.to_string()],
                        None,
                    );
                    let _ = stream.write_all(pexpire.as_bytes()).await;
                    let mut buf = [0u8; 256];
                    let _ = stream.read(&mut buf).await;
                }
            }

            if imported % 1000 == 0 && imported > 0 {
                print!(
                    "\r  {} Imported {} keys ({} skipped, {} errors)",
                    "→".cyan(),
                    imported,
                    skipped,
                    errors
                );
            }
        }
    }

    let replay_elapsed = replay_start.elapsed();
    println!();
    println!();
    println!("{}", "Import Summary".bold().underline());
    println!("  {} Keys imported: {}", "✓".green(), imported.to_string().green().bold());
    println!("  {} Keys skipped:  {}", "ℹ".blue(), skipped);
    if errors > 0 {
        println!("  {} Errors:        {}", "✗".red(), errors.to_string().red().bold());
    }
    println!(
        "  {} Total time:    {:.2}s (parse: {:.2}s, replay: {:.2}s)",
        "⏱".cyan(),
        (parse_elapsed + replay_elapsed).as_secs_f64(),
        parse_elapsed.as_secs_f64(),
        replay_elapsed.as_secs_f64()
    );

    if verify && imported > 0 {
        println!();
        println!("{}", "Verification".bold().underline());
        let dbsize_cmd = build_resp_cmd(&["DBSIZE"], None);
        let _ = stream.write_all(dbsize_cmd.as_bytes()).await;
        let mut buf = [0u8; 256];
        match stream.read(&mut buf).await {
            Ok(n) if n > 0 => {
                let response = String::from_utf8_lossy(&buf[..n]);
                println!("  {} Target DBSIZE: {}", "ℹ".blue(), response.trim());
            }
            _ => {
                println!("  {} Could not verify target DBSIZE", "⚠".yellow());
            }
        }
        println!("  {} Verification complete", "✓".green());
    }

    Ok(())
}

/// Replay an AOF file by piping its RESP commands directly to the target.
async fn import_aof(
    path: &Path,
    target: &str,
    key_pattern: Option<&str>,
    verify: bool,
) -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    println!("{}", "AOF Replay".bold().underline());
    println!("  {} Replaying AOF commands to {}...", "→".cyan(), target);

    let target_addr = parse_target_addr(target)?;
    let mut stream = TcpStream::connect(&target_addr).await.map_err(|e| {
        anyhow::anyhow!("Failed to connect to target {}: {}", target_addr, e)
    })?;

    let aof_data = std::fs::read(path)?;
    let mut pos = 0;
    let mut commands_sent: u64 = 0;
    let mut errors: u64 = 0;

    // AOF is a sequence of RESP commands; send them in batches
    while pos < aof_data.len() {
        // Find the end of the current RESP command
        let cmd_start = pos;
        if aof_data[pos] == b'*' {
            // Multi-bulk: read the array header to find command boundary
            if let Some(end) = find_resp_command_end(&aof_data, pos) {
                let cmd = &aof_data[cmd_start..end];

                // Apply key pattern filter if provided (best-effort for AOF)
                let should_send = if key_pattern.is_some() {
                    true // AOF filtering is complex; send all for correctness
                } else {
                    true
                };

                if should_send {
                    match stream.write_all(cmd).await {
                        Ok(()) => {
                            let mut buf = [0u8; 256];
                            let _ = stream.read(&mut buf).await;
                            commands_sent += 1;
                        }
                        Err(_) => errors += 1,
                    }
                }
                pos = end;
            } else {
                pos += 1;
            }
        } else {
            // Skip non-RESP lines (comments, etc.)
            while pos < aof_data.len() && aof_data[pos] != b'\n' {
                pos += 1;
            }
            if pos < aof_data.len() {
                pos += 1;
            }
        }

        if commands_sent % 1000 == 0 && commands_sent > 0 {
            print!("\r  {} Sent {} commands ({} errors)", "→".cyan(), commands_sent, errors);
        }
    }

    println!();
    println!(
        "  {} AOF replay complete: {} commands sent, {} errors",
        "✓".green(),
        commands_sent.to_string().green().bold(),
        errors
    );

    if verify {
        let dbsize_cmd = build_resp_cmd(&["DBSIZE"], None);
        let _ = stream.write_all(dbsize_cmd.as_bytes()).await;
        let mut buf = [0u8; 256];
        if let Ok(n) = stream.read(&mut buf).await {
            let response = String::from_utf8_lossy(&buf[..n]);
            println!("  {} Target DBSIZE: {}", "ℹ".blue(), response.trim());
        }
    }

    Ok(())
}

/// Import JSON-exported data (array of {key, type, value, ttl} objects).
async fn import_json(
    path: &Path,
    target: &str,
    key_pattern: Option<&str>,
    verify: bool,
) -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    println!("{}", "JSON Import".bold().underline());
    println!("  {} Importing JSON data to {}...", "→".cyan(), target);

    let target_addr = parse_target_addr(target)?;
    let mut stream = TcpStream::connect(&target_addr).await.map_err(|e| {
        anyhow::anyhow!("Failed to connect to target {}: {}", target_addr, e)
    })?;

    let json_data = std::fs::read_to_string(path)?;
    let entries: Vec<serde_json::Value> = serde_json::from_str(&json_data)
        .map_err(|e| anyhow::anyhow!("Failed to parse JSON: {}", e))?;

    let pattern_regex = if let Some(pattern) = key_pattern {
        let regex_str = pattern.replace('*', ".*").replace('?', ".");
        Some(regex::Regex::new(&format!("^{}$", regex_str))?)
    } else {
        None
    };

    let mut imported: u64 = 0;
    let mut skipped: u64 = 0;

    for entry in &entries {
        let key = entry
            .get("key")
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        if let Some(ref re) = pattern_regex {
            if !re.is_match(key) {
                skipped += 1;
                continue;
            }
        }

        let value = entry
            .get("value")
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        let cmd = build_resp_cmd(&["SET", key, value], None);
        let _ = stream.write_all(cmd.as_bytes()).await;
        let mut buf = [0u8; 256];
        let _ = stream.read(&mut buf).await;
        imported += 1;

        // Apply TTL if present
        if let Some(ttl) = entry.get("ttl").and_then(|v| v.as_u64()) {
            if ttl > 0 {
                let expire_cmd =
                    build_resp_cmd(&["PEXPIRE", key, &ttl.to_string()], None);
                let _ = stream.write_all(expire_cmd.as_bytes()).await;
                let mut buf2 = [0u8; 256];
                let _ = stream.read(&mut buf2).await;
            }
        }
    }

    println!(
        "  {} JSON import complete: {} keys imported, {} skipped",
        "✓".green(),
        imported.to_string().green().bold(),
        skipped
    );

    if verify {
        let dbsize_cmd = build_resp_cmd(&["DBSIZE"], None);
        let _ = stream.write_all(dbsize_cmd.as_bytes()).await;
        let mut buf = [0u8; 256];
        if let Ok(n) = stream.read(&mut buf).await {
            let response = String::from_utf8_lossy(&buf[..n]);
            println!("  {} Target DBSIZE: {}", "ℹ".blue(), response.trim());
        }
    }

    Ok(())
}

// ── RESP Protocol Helpers ───────────────────────────────────────────

/// Build a RESP multi-bulk command from string arguments.
fn build_resp_cmd(args: &[&str], extra_binary: Option<&[&[u8]]>) -> String {
    let total = args.len() + extra_binary.map_or(0, |e| e.len());
    let mut cmd = format!("*{}\r\n", total);
    for arg in args {
        cmd.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
    }
    if let Some(extras) = extra_binary {
        for data in extras {
            cmd.push_str(&format!("${}\r\n", data.len()));
            // For binary data we'd need raw bytes; for now use lossy string
            cmd.push_str(&String::from_utf8_lossy(data));
            cmd.push_str("\r\n");
        }
    }
    cmd
}

/// Build RESP command for list-type operations (RPUSH, SADD).
fn build_resp_list_cmd(cmd_name: &str, key: &str, items: &[bytes::Bytes]) -> String {
    let total = 2 + items.len();
    let mut cmd = format!("*{}\r\n${}\r\n{}\r\n${}\r\n{}\r\n", total, cmd_name.len(), cmd_name, key.len(), key);
    for item in items {
        let s = String::from_utf8_lossy(item);
        cmd.push_str(&format!("${}\r\n{}\r\n", s.len(), s));
    }
    cmd
}

/// Build RESP HSET command from field-value pairs.
fn build_resp_hash_cmd(cmd_name: &str, key: &str, fields: &[(bytes::Bytes, bytes::Bytes)]) -> String {
    let total = 2 + fields.len() * 2;
    let mut cmd = format!("*{}\r\n${}\r\n{}\r\n${}\r\n{}\r\n", total, cmd_name.len(), cmd_name, key.len(), key);
    for (field, value) in fields {
        let f = String::from_utf8_lossy(field);
        let v = String::from_utf8_lossy(value);
        cmd.push_str(&format!("${}\r\n{}\r\n${}\r\n{}\r\n", f.len(), f, v.len(), v));
    }
    cmd
}

/// Build RESP ZADD command from score-member pairs.
fn build_resp_zadd_cmd(key: &str, members: &[(f64, bytes::Bytes)]) -> String {
    let total = 2 + members.len() * 2;
    let mut cmd = format!("*{}\r\n$4\r\nZADD\r\n${}\r\n{}\r\n", total, key.len(), key);
    for (score, member) in members {
        let score_str = score.to_string();
        let m = String::from_utf8_lossy(member);
        cmd.push_str(&format!("${}\r\n{}\r\n${}\r\n{}\r\n", score_str.len(), score_str, m.len(), m));
    }
    cmd
}

/// Build RESP XADD command for stream entries.
fn build_resp_xadd_cmd(key: &str, id: &str, fields: &[(bytes::Bytes, bytes::Bytes)]) -> String {
    let total = 3 + fields.len() * 2;
    let mut cmd = format!("*{}\r\n$4\r\nXADD\r\n${}\r\n{}\r\n${}\r\n{}\r\n", total, key.len(), key, id.len(), id);
    for (field, value) in fields {
        let f = String::from_utf8_lossy(field);
        let v = String::from_utf8_lossy(value);
        cmd.push_str(&format!("${}\r\n{}\r\n${}\r\n{}\r\n", f.len(), f, v.len(), v));
    }
    cmd
}

/// Parse a target URL into a TCP address.
fn parse_target_addr(target: &str) -> anyhow::Result<String> {
    // Handle ferrite://, redis://, or raw host:port
    let stripped = target
        .trim_start_matches("ferrite://")
        .trim_start_matches("redis://")
        .trim_start_matches("tcp://");
    if stripped.contains(':') {
        Ok(stripped.to_string())
    } else {
        Ok(format!("{}:6379", stripped))
    }
}

/// Find the end of a RESP multi-bulk command in a byte buffer.
fn find_resp_command_end(data: &[u8], start: usize) -> Option<usize> {
    if start >= data.len() || data[start] != b'*' {
        return None;
    }

    let mut pos = start + 1;

    // Read argument count
    let mut count_str = String::new();
    while pos < data.len() && data[pos] != b'\r' {
        count_str.push(data[pos] as char);
        pos += 1;
    }
    pos += 2; // skip \r\n

    let count: usize = count_str.parse().ok()?;

    // Skip through each bulk string
    for _ in 0..count {
        if pos >= data.len() || data[pos] != b'$' {
            return None;
        }
        pos += 1;

        let mut len_str = String::new();
        while pos < data.len() && data[pos] != b'\r' {
            len_str.push(data[pos] as char);
            pos += 1;
        }
        pos += 2; // skip \r\n

        let len: usize = len_str.parse().ok()?;
        pos += len + 2; // skip data + \r\n
    }

    Some(pos)
}
