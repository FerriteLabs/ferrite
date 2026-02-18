//! Stat mode for ferrite-cli (like redis-cli --stat)

use std::time::{Duration, Instant};

use colored::Colorize;

use crate::client::FerriteClient;

/// Run stat mode - continuously display server stats
pub async fn run_stat_mode(mut client: FerriteClient, interval: Duration) -> std::io::Result<()> {
    println!(
        "{}",
        "------- data ------ --------------------- load ---------------------- - child -".bold()
    );
    println!("keys       mem      clients blocked requests            connections          ");

    let mut last_requests: Option<u64> = None;
    let mut last_time = Instant::now();

    loop {
        match fetch_stats(&mut client).await {
            Ok(stats) => {
                let elapsed = last_time.elapsed().as_secs_f64();
                let requests_per_sec = if let Some(last) = last_requests {
                    if elapsed > 0.0 {
                        ((stats.total_commands - last) as f64 / elapsed) as u64
                    } else {
                        0
                    }
                } else {
                    0
                };

                println!(
                    "{:<10} {:<8} {:<7} {:<7} {:<8} ({:>8}/s) {:<12}",
                    format_number(stats.keys),
                    format_bytes(stats.used_memory),
                    stats.connected_clients,
                    stats.blocked_clients,
                    format_number(stats.total_commands),
                    format_number(requests_per_sec),
                    stats.total_connections,
                );

                last_requests = Some(stats.total_commands);
                last_time = Instant::now();
            }
            Err(e) => {
                eprintln!("{}: {}", "Error fetching stats".red(), e);
            }
        }

        tokio::time::sleep(interval).await;
    }
}

struct ServerStats {
    keys: u64,
    used_memory: u64,
    connected_clients: u64,
    blocked_clients: u64,
    total_commands: u64,
    total_connections: u64,
}

async fn fetch_stats(client: &mut FerriteClient) -> std::io::Result<ServerStats> {
    let response = client.send_command(&["INFO"]).await?;

    let info_text = match response {
        crate::client::Frame::Bulk(Some(data)) => String::from_utf8(data).unwrap_or_default(),
        crate::client::Frame::Simple(s) => s,
        _ => String::new(),
    };

    let mut stats = ServerStats {
        keys: 0,
        used_memory: 0,
        connected_clients: 0,
        blocked_clients: 0,
        total_commands: 0,
        total_connections: 0,
    };

    for line in info_text.lines() {
        if let Some((key, value)) = line.split_once(':') {
            match key {
                "used_memory" => {
                    stats.used_memory = value.parse().unwrap_or(0);
                }
                "connected_clients" => {
                    stats.connected_clients = value.parse().unwrap_or(0);
                }
                "blocked_clients" => {
                    stats.blocked_clients = value.parse().unwrap_or(0);
                }
                "total_commands_processed" => {
                    stats.total_commands = value.parse().unwrap_or(0);
                }
                "total_connections_received" => {
                    stats.total_connections = value.parse().unwrap_or(0);
                }
                _ => {
                    // Parse db keys: db0:keys=123,expires=45
                    if key.starts_with("db") {
                        if let Some(keys_part) = value.split(',').next() {
                            if let Some(keys_str) = keys_part.strip_prefix("keys=") {
                                stats.keys += keys_str.parse::<u64>().unwrap_or(0);
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(stats)
}

fn format_number(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.2}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.2}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.2}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.2}G", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.2}M", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.2}K", bytes as f64 / 1024.0)
    } else {
        format!("{}B", bytes)
    }
}
