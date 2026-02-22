//! Ferrite Doctor — system diagnostics and health checks
//!
//! Runs a series of checks to validate the environment, configuration,
//! and runtime health of a Ferrite installation.

use std::env;
use std::fs;
use std::net::TcpListener;
use std::path::Path;

/// Result of a single diagnostic check
pub struct CheckResult {
    pub name: String,
    pub status: CheckStatus,
    pub message: String,
}

pub enum CheckStatus {
    Ok,
    Warning,
    Error,
}

impl std::fmt::Display for CheckStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckStatus::Ok => write!(f, "✅ OK"),
            CheckStatus::Warning => write!(f, "⚠️  WARN"),
            CheckStatus::Error => write!(f, "❌ ERROR"),
        }
    }
}

/// Run all diagnostic checks and return results
pub fn run_diagnostics(config_path: Option<&str>, port: u16) -> Vec<CheckResult> {
    let mut results = vec![
        check_os(),
        check_rust_version(),
        check_memory(),
        check_disk_space(),
        check_port_available(port),
        check_metrics_port(9090),
    ];

    // Configuration checks
    if let Some(path) = config_path {
        results.push(check_config_file(path));
    } else {
        results.push(check_default_config());
    }

    // Data directory checks
    results.push(check_data_directory());

    // Linux-specific checks
    #[cfg(target_os = "linux")]
    {
        results.push(check_io_uring_support());
        results.push(check_ulimits());
        results.push(check_transparent_hugepages());
        results.push(check_overcommit_memory());
    }

    // macOS-specific checks
    #[cfg(target_os = "macos")]
    {
        results.push(check_macos_limits());
    }

    results
}

/// Print diagnostics report to stdout
#[allow(clippy::print_stdout)]
pub fn print_report(results: &[CheckResult]) {
    println!("\n🔬 Ferrite Doctor — System Diagnostics\n");
    println!("{}", "=".repeat(60));

    let mut ok_count = 0;
    let mut warn_count = 0;
    let mut err_count = 0;

    for result in results {
        match result.status {
            CheckStatus::Ok => ok_count += 1,
            CheckStatus::Warning => warn_count += 1,
            CheckStatus::Error => err_count += 1,
        }
        println!("[{}] {}", result.status, result.name);
        println!("   {}\n", result.message);
    }

    println!("{}", "=".repeat(60));
    println!(
        "Summary: {} passed, {} warnings, {} errors\n",
        ok_count, warn_count, err_count
    );

    if err_count > 0 {
        println!("❌ Some checks failed. Please address the errors above before running Ferrite in production.");
    } else if warn_count > 0 {
        println!("⚠️  Ferrite should work, but consider addressing the warnings for optimal performance.");
    } else {
        println!("✅ All checks passed! Ferrite is ready to run.");
    }
}

/// Returns true if all checks passed (no errors)
pub fn all_passed(results: &[CheckResult]) -> bool {
    results
        .iter()
        .all(|r| !matches!(r.status, CheckStatus::Error))
}

fn check_os() -> CheckResult {
    let os = env::consts::OS;
    let arch = env::consts::ARCH;
    CheckResult {
        name: "Operating System".to_string(),
        status: CheckStatus::Ok,
        message: format!("{} ({})", os, arch),
    }
}

fn check_rust_version() -> CheckResult {
    CheckResult {
        name: "Build Info".to_string(),
        status: CheckStatus::Ok,
        message: format!(
            "ferrite {} (built with rustc {})",
            env!("CARGO_PKG_VERSION"),
            rustc_version()
        ),
    }
}

fn rustc_version() -> &'static str {
    option_env!("RUSTC_VERSION").unwrap_or("unknown")
}

fn check_memory() -> CheckResult {
    #[cfg(target_os = "linux")]
    {
        if let Ok(meminfo) = fs::read_to_string("/proc/meminfo") {
            for line in meminfo.lines() {
                if line.starts_with("MemTotal:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if let Some(kb_str) = parts.get(1) {
                        if let Ok(kb) = kb_str.parse::<u64>() {
                            let gb = kb as f64 / 1024.0 / 1024.0;
                            let status = if gb < 1.0 {
                                CheckStatus::Warning
                            } else {
                                CheckStatus::Ok
                            };
                            return CheckResult {
                                name: "System Memory".to_string(),
                                status,
                                message: format!("{:.1} GB total", gb),
                            };
                        }
                    }
                }
            }
        }
    }

    #[cfg(target_os = "macos")]
    {
        if let Ok(output) = std::process::Command::new("sysctl")
            .args(["-n", "hw.memsize"])
            .output()
        {
            if let Ok(bytes_str) = String::from_utf8(output.stdout) {
                if let Ok(bytes) = bytes_str.trim().parse::<u64>() {
                    let gb = bytes as f64 / 1024.0 / 1024.0 / 1024.0;
                    let status = if gb < 1.0 {
                        CheckStatus::Warning
                    } else {
                        CheckStatus::Ok
                    };
                    return CheckResult {
                        name: "System Memory".to_string(),
                        status,
                        message: format!("{:.1} GB total", gb),
                    };
                }
            }
        }
    }

    CheckResult {
        name: "System Memory".to_string(),
        status: CheckStatus::Ok,
        message: "Unable to determine (not critical)".to_string(),
    }
}

fn check_disk_space() -> CheckResult {
    let data_dir = dirs_or_default();
    match fs::metadata(&data_dir) {
        Ok(_) => {
            #[cfg(unix)]
            {
                let test_file = format!("{}/.ferrite_doctor_check", data_dir);
                match fs::write(&test_file, "check") {
                    Ok(_) => {
                        let _ = fs::remove_file(&test_file);
                        CheckResult {
                            name: "Disk Space".to_string(),
                            status: CheckStatus::Ok,
                            message: format!("Data directory '{}' is writable", data_dir),
                        }
                    }
                    Err(e) => CheckResult {
                        name: "Disk Space".to_string(),
                        status: CheckStatus::Error,
                        message: format!("Data directory '{}' is not writable: {}", data_dir, e),
                    },
                }
            }
            #[cfg(not(unix))]
            {
                CheckResult {
                    name: "Disk Space".to_string(),
                    status: CheckStatus::Ok,
                    message: format!("Data directory '{}' exists", data_dir),
                }
            }
        }
        Err(_) => CheckResult {
            name: "Disk Space".to_string(),
            status: CheckStatus::Warning,
            message: format!(
                "Data directory '{}' does not exist (will be created on startup)",
                data_dir
            ),
        },
    }
}

fn check_port_available(port: u16) -> CheckResult {
    match TcpListener::bind(format!("127.0.0.1:{}", port)) {
        Ok(_) => CheckResult {
            name: format!("Port {} (server)", port),
            status: CheckStatus::Ok,
            message: "Available".to_string(),
        },
        Err(_) => CheckResult {
            name: format!("Port {} (server)", port),
            status: CheckStatus::Warning,
            message: "Port is in use — Ferrite may already be running or another service is using this port".to_string(),
        },
    }
}

fn check_metrics_port(port: u16) -> CheckResult {
    match TcpListener::bind(format!("127.0.0.1:{}", port)) {
        Ok(_) => CheckResult {
            name: format!("Port {} (metrics)", port),
            status: CheckStatus::Ok,
            message: "Available".to_string(),
        },
        Err(_) => CheckResult {
            name: format!("Port {} (metrics)", port),
            status: CheckStatus::Warning,
            message: "Metrics port is in use — Prometheus metrics endpoint may not start"
                .to_string(),
        },
    }
}

fn check_config_file(path: &str) -> CheckResult {
    if Path::new(path).exists() {
        match fs::read_to_string(path) {
            Ok(contents) => match contents.parse::<toml::Value>() {
                Ok(_) => CheckResult {
                    name: "Configuration File".to_string(),
                    status: CheckStatus::Ok,
                    message: format!("'{}' is valid TOML", path),
                },
                Err(e) => CheckResult {
                    name: "Configuration File".to_string(),
                    status: CheckStatus::Error,
                    message: format!("'{}' has invalid TOML: {}", path, e),
                },
            },
            Err(e) => CheckResult {
                name: "Configuration File".to_string(),
                status: CheckStatus::Error,
                message: format!("Cannot read '{}': {}", path, e),
            },
        }
    } else {
        CheckResult {
            name: "Configuration File".to_string(),
            status: CheckStatus::Error,
            message: format!("'{}' does not exist", path),
        }
    }
}

fn check_default_config() -> CheckResult {
    let paths = ["ferrite.toml", "/etc/ferrite/ferrite.toml"];

    for path in &paths {
        if Path::new(path).exists() {
            return CheckResult {
                name: "Configuration File".to_string(),
                status: CheckStatus::Ok,
                message: format!("Found config at '{}'", path),
            };
        }
    }

    CheckResult {
        name: "Configuration File".to_string(),
        status: CheckStatus::Ok,
        message: "No config file found — will use defaults".to_string(),
    }
}

fn check_data_directory() -> CheckResult {
    let dir = dirs_or_default();
    if Path::new(&dir).exists() {
        CheckResult {
            name: "Data Directory".to_string(),
            status: CheckStatus::Ok,
            message: format!("'{}' exists", dir),
        }
    } else {
        CheckResult {
            name: "Data Directory".to_string(),
            status: CheckStatus::Ok,
            message: format!("'{}' will be created on first run", dir),
        }
    }
}

#[cfg(target_os = "linux")]
fn check_io_uring_support() -> CheckResult {
    if let Ok(version) = fs::read_to_string("/proc/version") {
        let parts: Vec<&str> = version.split_whitespace().collect();
        if let Some(ver) = parts.get(2) {
            let ver_parts: Vec<&str> = ver.split('.').collect();
            if let (Some(major), Some(minor)) = (
                ver_parts.first().and_then(|v| v.parse::<u32>().ok()),
                ver_parts.get(1).and_then(|v| v.parse::<u32>().ok()),
            ) {
                if major > 5 || (major == 5 && minor >= 11) {
                    return CheckResult {
                        name: "io_uring Support".to_string(),
                        status: CheckStatus::Ok,
                        message: format!("Kernel {}.{} supports io_uring", major, minor),
                    };
                } else {
                    return CheckResult {
                        name: "io_uring Support".to_string(),
                        status: CheckStatus::Warning,
                        message: format!(
                            "Kernel {}.{} — io_uring requires 5.11+. Will use tokio::fs fallback.",
                            major, minor
                        ),
                    };
                }
            }
        }
    }

    CheckResult {
        name: "io_uring Support".to_string(),
        status: CheckStatus::Warning,
        message: "Could not determine kernel version".to_string(),
    }
}

#[cfg(target_os = "linux")]
fn check_ulimits() -> CheckResult {
    if let Ok(limits) = fs::read_to_string("/proc/self/limits") {
        for line in limits.lines() {
            if line.starts_with("Max open files") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if let Some(soft) = parts.get(3).and_then(|v| v.parse::<u64>().ok()) {
                    if soft < 10240 {
                        return CheckResult {
                            name: "File Descriptor Limit".to_string(),
                            status: CheckStatus::Warning,
                            message: format!(
                                "Soft limit is {} — recommend at least 10240 for production. Run: ulimit -n 65536",
                                soft
                            ),
                        };
                    } else {
                        return CheckResult {
                            name: "File Descriptor Limit".to_string(),
                            status: CheckStatus::Ok,
                            message: format!("Soft limit: {}", soft),
                        };
                    }
                }
            }
        }
    }
    CheckResult {
        name: "File Descriptor Limit".to_string(),
        status: CheckStatus::Ok,
        message: "Could not determine (not critical)".to_string(),
    }
}

#[cfg(target_os = "linux")]
fn check_transparent_hugepages() -> CheckResult {
    if let Ok(thp) = fs::read_to_string("/sys/kernel/mm/transparent_hugepage/enabled") {
        if thp.contains("[always]") {
            return CheckResult {
                name: "Transparent Huge Pages".to_string(),
                status: CheckStatus::Warning,
                message: "THP is set to 'always' — this can cause latency spikes. Recommend: echo madvise > /sys/kernel/mm/transparent_hugepage/enabled".to_string(),
            };
        }
    }
    CheckResult {
        name: "Transparent Huge Pages".to_string(),
        status: CheckStatus::Ok,
        message: "Not set to 'always' (good)".to_string(),
    }
}

#[cfg(target_os = "linux")]
fn check_overcommit_memory() -> CheckResult {
    if let Ok(val) = fs::read_to_string("/proc/sys/vm/overcommit_memory") {
        let val = val.trim();
        if val == "0" {
            return CheckResult {
                name: "Memory Overcommit".to_string(),
                status: CheckStatus::Warning,
                message: "vm.overcommit_memory=0 (heuristic). Recommend setting to 1 for persistence: sysctl vm.overcommit_memory=1".to_string(),
            };
        }
    }
    CheckResult {
        name: "Memory Overcommit".to_string(),
        status: CheckStatus::Ok,
        message: "vm.overcommit_memory is configured".to_string(),
    }
}

#[cfg(target_os = "macos")]
fn check_macos_limits() -> CheckResult {
    if let Ok(output) = std::process::Command::new("launchctl")
        .args(["limit", "maxfiles"])
        .output()
    {
        if let Ok(out) = String::from_utf8(output.stdout) {
            return CheckResult {
                name: "File Descriptor Limit".to_string(),
                status: CheckStatus::Ok,
                message: format!("maxfiles: {}", out.trim()),
            };
        }
    }
    CheckResult {
        name: "File Descriptor Limit".to_string(),
        status: CheckStatus::Ok,
        message: "Could not determine (not critical)".to_string(),
    }
}

fn dirs_or_default() -> String {
    env::var("FERRITE_DATA_DIR").unwrap_or_else(|_| "./data".to_string())
}
