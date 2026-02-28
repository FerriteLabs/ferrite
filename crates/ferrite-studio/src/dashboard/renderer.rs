//! Dashboard rendering — terminal and HTML output
#![allow(dead_code)]

use super::data_source::DashboardSnapshot;

/// Renders the dashboard as an ASCII terminal widget.
pub struct TerminalRenderer;

impl TerminalRenderer {
    /// Render a full-screen ASCII dashboard from the given snapshot.
    pub fn render_dashboard(snapshot: &DashboardSnapshot) -> String {
        let mut out = String::with_capacity(2048);

        let version = &snapshot.server.version;
        let title = format!("Ferrite Dashboard v{}", version);

        // Top border
        out.push_str("╔══════════════════════════════════════════════════════════════════════╗\n");
        out.push_str(&format!("║ {:^68} ║\n", title));
        out.push_str("╠══════════════════════════════════════════════════════════════════════╣\n");

        // Server | Tiering | Cluster
        let uptime = format_uptime(snapshot.server.uptime_secs);
        let mem_display = format_bytes(snapshot.memory.used_bytes);
        let ops = format_number(snapshot.server.total_commands_processed);

        out.push_str(&format!(
            "║ {:<23} │ {:<21} │ {:<17} ║\n",
            "Server", "Tiering", "Cluster"
        ));
        out.push_str(&format!(
            "║ Uptime: {:<14} │ Memory: {:>8} keys │ State: {:<10} ║\n",
            uptime,
            format_number(snapshot.tiering.memory_keys),
            snapshot.cluster.state,
        ));
        out.push_str(&format!(
            "║ Clients: {:<13} │ Mmap: {:>10} keys │ Nodes: {:<10} ║\n",
            snapshot.server.connected_clients,
            format_number(snapshot.tiering.mmap_keys),
            format!(
                "{}/{}",
                snapshot.cluster.nodes_healthy, snapshot.cluster.nodes_total
            ),
        ));
        out.push_str(&format!(
            "║ Memory: {:<14} │ Disk: {:>10} keys │ Slots: {:<10} ║\n",
            mem_display,
            format_number(snapshot.tiering.disk_keys),
            format!("{}/{}", snapshot.cluster.slots_assigned, 16384),
        ));
        out.push_str(&format!(
            "║ Ops: {:<17} │ Cloud: {:>9} keys │ Lag: {:<12} ║\n",
            ops,
            format_number(snapshot.tiering.cloud_keys),
            format!("{}ms", snapshot.cluster.replication_lag_ms),
        ));
        if snapshot.tiering.monthly_cost > 0.0 {
            out.push_str(&format!(
                "║                         │ Cost: ${:<4}/mo → ${:<4}/mo │                   ║\n",
                snapshot.tiering.monthly_cost as u64, snapshot.tiering.optimal_cost as u64,
            ));
        }

        out.push_str("╠══════════════════════════════════════════════════════════════════════╣\n");

        // Hot Keys | Latency
        out.push_str(&format!(
            "║ {:<23} │ {:<44} ║\n",
            "Hot Keys", "Latency (µs)"
        ));

        let max_hot = 3.min(snapshot.hot_keys.len());
        for i in 0..3 {
            let key_col = if i < max_hot {
                let hk = &snapshot.hot_keys[i];
                let short_key = if hk.key.len() > 16 {
                    format!("{}…", &hk.key[..15])
                } else {
                    hk.key.clone()
                };
                format!(
                    "{}. {} {:>5}/s",
                    i + 1,
                    short_key,
                    format_number_f64(hk.ops_per_sec)
                )
            } else {
                String::new()
            };

            let lat_col = match i {
                0 => format!(
                    "GET  P50:{:<5.1}  P99:{:<5.1}",
                    snapshot.latency.get_p50_us, snapshot.latency.get_p99_us,
                ),
                1 => format!(
                    "SET  P50:{:<5.1}  P99:{:<5.1}",
                    snapshot.latency.set_p50_us, snapshot.latency.set_p99_us,
                ),
                2 => format!("ALL  P99:{:<5.1}", snapshot.latency.overall_p99_us,),
                _ => String::new(),
            };

            out.push_str(&format!("║ {:<23} │ {:<44} ║\n", key_col, lat_col));
        }

        out.push_str("╚══════════════════════════════════════════════════════════════════════╝\n");
        out
    }
}

/// Renders the dashboard as an HTML page with embedded CSS and JS.
pub struct HtmlRenderer;

impl HtmlRenderer {
    /// Generate a complete HTML page with embedded styles and auto-refresh.
    pub fn render_page(snapshot: &DashboardSnapshot) -> String {
        let json = serde_json::to_string(snapshot).unwrap_or_default();

        format!(
            r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Ferrite Dashboard</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, monospace;
         background: #0d1117; color: #c9d1d9; padding: 20px; }}
  .header {{ text-align: center; padding: 16px; border-bottom: 1px solid #30363d; margin-bottom: 20px; }}
  .header h1 {{ color: #58a6ff; font-size: 1.5rem; }}
  .grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; margin-bottom: 20px; }}
  .card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }}
  .card h2 {{ color: #58a6ff; font-size: 0.9rem; margin-bottom: 12px; text-transform: uppercase; }}
  .metric {{ display: flex; justify-content: space-between; padding: 4px 0; font-size: 0.85rem; }}
  .metric .label {{ color: #8b949e; }}
  .metric .value {{ color: #f0f6fc; font-weight: 600; }}
  .gauge {{ width: 100%; height: 8px; background: #21262d; border-radius: 4px; margin: 4px 0; }}
  .gauge-fill {{ height: 100%; border-radius: 4px; background: linear-gradient(90deg, #238636, #f0883e); }}
  .bar-chart {{ display: flex; align-items: flex-end; gap: 8px; height: 80px; margin-top: 8px; }}
  .bar {{ flex: 1; background: #238636; border-radius: 4px 4px 0 0; min-height: 4px; position: relative; }}
  .bar .bar-label {{ position: absolute; bottom: -18px; width: 100%; text-align: center; font-size: 0.65rem; color: #8b949e; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem; }}
  th, td {{ padding: 6px 8px; text-align: left; border-bottom: 1px solid #21262d; }}
  th {{ color: #8b949e; font-weight: 500; }}
  .ts-placeholder {{ height: 120px; display: flex; align-items: center; justify-content: center;
                     color: #484f58; font-style: italic; border: 1px dashed #30363d; border-radius: 8px; }}
  .status-ok {{ color: #3fb950; }}
  .status-warn {{ color: #f0883e; }}
  .status-err {{ color: #f85149; }}
</style>
</head>
<body>
<div class="header">
  <h1>🔥 Ferrite Dashboard v{version}</h1>
  <p style="color:#8b949e;font-size:0.8rem;">Uptime: {uptime} · Clients: {clients} · Ops: {ops}</p>
</div>

<div class="grid">
  <div class="card">
    <h2>Memory</h2>
    <div class="metric"><span class="label">Used</span><span class="value">{mem_used}</span></div>
    <div class="metric"><span class="label">Peak</span><span class="value">{mem_peak}</span></div>
    <div class="gauge"><div class="gauge-fill" style="width:{mem_pct}%"></div></div>
    <div class="metric"><span class="label">Fragmentation</span><span class="value">{frag:.2}x</span></div>
    <div class="metric"><span class="label">Evictions</span><span class="value">{evictions}</span></div>
  </div>

  <div class="card">
    <h2>Tier Distribution</h2>
    <div class="bar-chart">
      <div class="bar" style="height:{bar_mem}%"><div class="bar-label">Mem</div></div>
      <div class="bar" style="height:{bar_mmap}%" ><div class="bar-label">Mmap</div></div>
      <div class="bar" style="height:{bar_disk}%" ><div class="bar-label">Disk</div></div>
      <div class="bar" style="height:{bar_cloud}%"><div class="bar-label">Cloud</div></div>
    </div>
    <div class="metric" style="margin-top:24px"><span class="label">Cost</span><span class="value">${cost:.0}/mo</span></div>
    <div class="metric"><span class="label">Optimal</span><span class="value">${optimal:.0}/mo</span></div>
  </div>

  <div class="card">
    <h2>Cluster</h2>
    <div class="metric"><span class="label">State</span><span class="value {state_class}">{state}</span></div>
    <div class="metric"><span class="label">Nodes</span><span class="value">{healthy}/{total}</span></div>
    <div class="metric"><span class="label">Slots</span><span class="value">{slots}/16384</span></div>
    <div class="metric"><span class="label">Repl Lag</span><span class="value">{lag}ms</span></div>
  </div>
</div>

<div class="grid" style="grid-template-columns: 1fr 1fr;">
  <div class="card">
    <h2>Hot Keys</h2>
    <table>
      <tr><th>Key</th><th>Ops/s</th><th>Last Access</th></tr>
      {hot_keys_rows}
    </table>
  </div>
  <div class="card">
    <h2>Latency (µs)</h2>
    <table>
      <tr><th>Command</th><th>P50</th><th>P99</th></tr>
      <tr><td>GET</td><td>{get_p50:.1}</td><td>{get_p99:.1}</td></tr>
      <tr><td>SET</td><td>{set_p50:.1}</td><td>{set_p99:.1}</td></tr>
      <tr><td>Overall</td><td>—</td><td>{all_p99:.1}</td></tr>
    </table>
  </div>
</div>

<div class="card" style="margin-top:16px">
  <h2>Time Series</h2>
  <div class="ts-placeholder">Connect via WebSocket for live time-series data</div>
</div>

<script>
const SNAPSHOT = {json};
// Auto-refresh every 5 seconds
setTimeout(() => location.reload(), 5000);
</script>
</body>
</html>"#,
            version = snapshot.server.version,
            uptime = format_uptime(snapshot.server.uptime_secs),
            clients = snapshot.server.connected_clients,
            ops = format_number(snapshot.server.total_commands_processed),
            mem_used = format_bytes(snapshot.memory.used_bytes),
            mem_peak = format_bytes(snapshot.memory.peak_bytes),
            mem_pct = if snapshot.memory.peak_bytes > 0 {
                (snapshot.memory.used_bytes as f64 / snapshot.memory.peak_bytes as f64 * 100.0)
                    as u64
            } else {
                0
            },
            frag = snapshot.memory.fragmentation_ratio,
            evictions = snapshot.memory.eviction_count,
            bar_mem = tier_bar_pct(snapshot.tiering.memory_keys, &snapshot.tiering),
            bar_mmap = tier_bar_pct(snapshot.tiering.mmap_keys, &snapshot.tiering),
            bar_disk = tier_bar_pct(snapshot.tiering.disk_keys, &snapshot.tiering),
            bar_cloud = tier_bar_pct(snapshot.tiering.cloud_keys, &snapshot.tiering),
            cost = snapshot.tiering.monthly_cost,
            optimal = snapshot.tiering.optimal_cost,
            state = snapshot.cluster.state,
            state_class = match snapshot.cluster.state.as_str() {
                "ok" => "status-ok",
                "degraded" => "status-warn",
                _ => "status-err",
            },
            healthy = snapshot.cluster.nodes_healthy,
            total = snapshot.cluster.nodes_total,
            slots = snapshot.cluster.slots_assigned,
            lag = snapshot.cluster.replication_lag_ms,
            hot_keys_rows = render_hot_keys_rows(&snapshot.hot_keys),
            get_p50 = snapshot.latency.get_p50_us,
            get_p99 = snapshot.latency.get_p99_us,
            set_p50 = snapshot.latency.set_p50_us,
            set_p99 = snapshot.latency.set_p99_us,
            all_p99 = snapshot.latency.overall_p99_us,
            json = json,
        )
    }
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

fn format_uptime(secs: u64) -> String {
    let days = secs / 86400;
    let hours = (secs % 86400) / 3600;
    let mins = (secs % 3600) / 60;
    if days > 0 {
        format!("{}d {}h {}m", days, hours, mins)
    } else if hours > 0 {
        format!("{}h {}m", hours, mins)
    } else {
        format!("{}m", mins)
    }
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    const TB: u64 = 1024 * GB;

    if bytes >= TB {
        format!("{:.1} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

fn format_number(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.1}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

fn format_number_f64(n: f64) -> String {
    if n >= 1_000_000.0 {
        format!("{:.1}M", n / 1_000_000.0)
    } else if n >= 1_000.0 {
        format!("{:.1}K", n / 1_000.0)
    } else {
        format!("{:.0}", n)
    }
}

use super::data_source::TieringInfo;

fn tier_bar_pct(count: u64, tiering: &TieringInfo) -> u64 {
    let total = tiering.memory_keys + tiering.mmap_keys + tiering.disk_keys + tiering.cloud_keys;
    if total == 0 {
        return 5; // minimum bar height
    }
    let pct = (count as f64 / total as f64 * 100.0) as u64;
    pct.max(5) // minimum 5% for visibility
}

fn render_hot_keys_rows(hot_keys: &[super::data_source::HotKeyInfo]) -> String {
    if hot_keys.is_empty() {
        return "<tr><td colspan=\"3\" style=\"color:#484f58\">No hot keys detected</td></tr>"
            .to_string();
    }
    hot_keys
        .iter()
        .take(10)
        .map(|hk| {
            format!(
                "<tr><td>{}</td><td>{:.0}</td><td>{}ms ago</td></tr>",
                hk.key, hk.ops_per_sec, hk.last_access_ago_ms,
            )
        })
        .collect::<Vec<_>>()
        .join("\n      ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dashboard::data_source::DashboardDataSource;

    #[test]
    fn test_terminal_render() {
        let ds = DashboardDataSource::new();
        let snap = ds.collect_snapshot();
        let output = TerminalRenderer::render_dashboard(&snap);
        assert!(output.contains("Ferrite Dashboard"));
        assert!(output.contains("Server"));
        assert!(output.contains("Cluster"));
    }

    #[test]
    fn test_html_render() {
        let ds = DashboardDataSource::new();
        let snap = ds.collect_snapshot();
        let html = HtmlRenderer::render_page(&snap);
        assert!(html.contains("<!DOCTYPE html>"));
        assert!(html.contains("Ferrite Dashboard"));
        assert!(html.contains("Auto-refresh"));
    }

    #[test]
    fn test_format_uptime() {
        assert_eq!(format_uptime(0), "0m");
        assert_eq!(format_uptime(3661), "1h 1m");
        assert_eq!(format_uptime(190320), "2d 4h 52m");
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024 * 1024 * 2), "2.0 MB");
    }
}
