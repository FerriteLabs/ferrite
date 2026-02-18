//! Enhanced Metrics Visualization Module
//!
//! Provides detailed metrics panels with comprehensive visualization:
//! - Memory breakdown by database
//! - Command frequency analysis
//! - Network I/O monitoring
//! - Latency distribution
//! - Cache performance metrics

use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, List, ListItem, Paragraph, Sparkline},
    Frame,
};

use super::{format_bytes, format_number};

/// Enhanced metrics state with detailed breakdowns
#[derive(Clone)]
pub struct EnhancedMetricsState {
    // Command statistics
    pub cmd_stats: Vec<(String, u64)>,
    pub cmd_per_sec: Vec<(String, u64)>,
    pub total_commands: u64,

    // Memory breakdown
    pub memory_breakdown: Vec<(String, u64)>,
    pub used_memory: u64,
    pub max_memory: u64,
    pub peak_memory: u64,
    pub fragmentation_ratio: f64,

    // Network I/O
    pub network_in_history: Vec<u64>,
    pub network_out_history: Vec<u64>,
    pub network_in_per_sec: u64,
    pub network_out_per_sec: u64,
    pub total_connections: u64,
    pub rejected_connections: u64,
    pub connected_clients: u64,

    // Latency distribution
    pub latency_p50: f64,
    pub latency_p95: f64,
    pub latency_p99: f64,
    pub latency_p999: f64,
    pub latency_history: Vec<u64>,
    pub slow_commands: u64,

    // Cache performance
    pub hit_rate: f64,
    pub miss_rate: f64,
    pub keyspace_hits: u64,
    pub keyspace_misses: u64,
    pub eviction_rate: u64,
    pub expired_keys: u64,
    pub evicted_keys: u64,
}

impl Default for EnhancedMetricsState {
    fn default() -> Self {
        Self {
            cmd_stats: Vec::new(),
            cmd_per_sec: Vec::new(),
            total_commands: 0,
            memory_breakdown: Vec::new(),
            used_memory: 0,
            max_memory: 0,
            peak_memory: 0,
            fragmentation_ratio: 1.0,
            network_in_history: vec![0; 60],
            network_out_history: vec![0; 60],
            network_in_per_sec: 0,
            network_out_per_sec: 0,
            total_connections: 0,
            rejected_connections: 0,
            connected_clients: 0,
            latency_p50: 0.0,
            latency_p95: 0.0,
            latency_p99: 0.0,
            latency_p999: 0.0,
            latency_history: vec![0; 60],
            slow_commands: 0,
            hit_rate: 0.0,
            miss_rate: 0.0,
            keyspace_hits: 0,
            keyspace_misses: 0,
            eviction_rate: 0,
            expired_keys: 0,
            evicted_keys: 0,
        }
    }
}

/// Render the complete enhanced metrics view
pub fn render_enhanced_metrics(f: &mut Frame, state: &EnhancedMetricsState, area: Rect) {
    // Create main layout with 3 rows
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(10), // Row 1: Memory + Command Frequency
            Constraint::Length(10), // Row 2: Network I/O + Latency
            Constraint::Min(8),     // Row 3: Cache Performance + Additional
        ])
        .split(area);

    // Row 1: Memory Breakdown + Command Frequency
    render_row1(f, state, chunks[0]);

    // Row 2: Network I/O + Latency Distribution
    render_row2(f, state, chunks[1]);

    // Row 3: Cache Performance + Additional Metrics
    render_row3(f, state, chunks[2]);
}

/// Row 1: Memory Breakdown + Command Frequency
fn render_row1(f: &mut Frame, state: &EnhancedMetricsState, area: Rect) {
    let row_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    render_memory_breakdown(f, state, row_chunks[0]);
    render_command_frequency(f, state, row_chunks[1]);
}

/// Row 2: Network I/O + Latency Distribution
fn render_row2(f: &mut Frame, state: &EnhancedMetricsState, area: Rect) {
    let row_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    render_network_io(f, state, row_chunks[0]);
    render_latency_distribution(f, state, row_chunks[1]);
}

/// Row 3: Cache Performance + Additional Stats
fn render_row3(f: &mut Frame, state: &EnhancedMetricsState, area: Rect) {
    let row_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    render_cache_performance(f, state, row_chunks[0]);
    render_additional_stats(f, state, row_chunks[1]);
}

/// Panel 1: Memory Breakdown
fn render_memory_breakdown(f: &mut Frame, state: &EnhancedMetricsState, area: Rect) {
    let inner_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(area);

    // Memory gauge
    let ratio = if state.max_memory > 0 {
        (state.used_memory as f64 / state.max_memory as f64).min(1.0)
    } else {
        0.0
    };

    let gauge_color = if ratio > 0.9 {
        Color::Red
    } else if ratio > 0.7 {
        Color::Yellow
    } else {
        Color::Green
    };

    let memory_gauge = Gauge::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Memory Usage "),
        )
        .gauge_style(Style::default().fg(gauge_color))
        .ratio(ratio)
        .label(format!(
            "{} / {} ({:.1}%)",
            format_bytes(state.used_memory),
            format_bytes(state.max_memory),
            ratio * 100.0
        ));
    f.render_widget(memory_gauge, inner_chunks[0]);

    // Memory details
    let details = vec![
        Line::from(vec![
            Span::styled(
                "Peak Memory:  ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(format_bytes(state.peak_memory)),
        ]),
        Line::from(vec![
            Span::styled(
                "Fragmentation: ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("{:.2}", state.fragmentation_ratio),
                if state.fragmentation_ratio > 1.5 {
                    Style::default().fg(Color::Yellow)
                } else {
                    Style::default().fg(Color::Green)
                },
            ),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            "By Database:",
            Style::default().add_modifier(Modifier::BOLD),
        )),
    ];

    let mut all_lines = details;

    // Add database breakdown if available
    for (db, bytes) in state.memory_breakdown.iter().take(3) {
        all_lines.push(Line::from(format!("  {}: {}", db, format_bytes(*bytes))));
    }

    let details_widget =
        Paragraph::new(all_lines).block(Block::default().borders(Borders::ALL).title(" Details "));
    f.render_widget(details_widget, inner_chunks[1]);
}

/// Panel 2: Command Frequency
fn render_command_frequency(f: &mut Frame, state: &EnhancedMetricsState, area: Rect) {
    let inner_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0)])
        .split(area);

    // Top commands list
    let mut items = vec![ListItem::new(Line::from(vec![
        Span::styled(
            "COMMAND      ",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            "COUNT        ",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled("PER SEC", Style::default().add_modifier(Modifier::BOLD)),
    ]))];

    // Get top 10 commands
    for (cmd, count) in state.cmd_stats.iter().take(7) {
        let per_sec = state
            .cmd_per_sec
            .iter()
            .find(|(c, _)| c == cmd)
            .map(|(_, s)| *s)
            .unwrap_or(0);

        let line = Line::from(format!(
            "{:<12} {:<12} {}",
            cmd,
            format_number(*count),
            format_number(per_sec)
        ));
        items.push(ListItem::new(line));
    }

    let list = List::new(items).block(Block::default().borders(Borders::ALL).title(format!(
        " Top Commands (Total: {}) ",
        format_number(state.total_commands)
    )));

    f.render_widget(list, inner_chunks[0]);
}

/// Panel 3: Network I/O
fn render_network_io(f: &mut Frame, state: &EnhancedMetricsState, area: Rect) {
    let inner_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Input sparkline
            Constraint::Length(3), // Output sparkline
            Constraint::Min(0),    // Connection stats
        ])
        .split(area);

    // Network In sparkline
    let net_in = Sparkline::default()
        .block(Block::default().borders(Borders::ALL).title(format!(
            " Network In: {}/s ",
            format_bytes(state.network_in_per_sec)
        )))
        .data(&state.network_in_history)
        .style(Style::default().fg(Color::Cyan));
    f.render_widget(net_in, inner_chunks[0]);

    // Network Out sparkline
    let net_out = Sparkline::default()
        .block(Block::default().borders(Borders::ALL).title(format!(
            " Network Out: {}/s ",
            format_bytes(state.network_out_per_sec)
        )))
        .data(&state.network_out_history)
        .style(Style::default().fg(Color::Magenta));
    f.render_widget(net_out, inner_chunks[1]);

    // Connection stats
    let conn_stats = vec![
        Line::from(vec![
            Span::styled("Active:   ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(
                format!("{}", state.connected_clients),
                Style::default().fg(Color::Green),
            ),
        ]),
        Line::from(vec![
            Span::styled("Total:    ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format_number(state.total_connections)),
        ]),
        Line::from(vec![
            Span::styled("Rejected: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(
                format_number(state.rejected_connections),
                if state.rejected_connections > 0 {
                    Style::default().fg(Color::Red)
                } else {
                    Style::default().fg(Color::Green)
                },
            ),
        ]),
    ];

    let conn_widget = Paragraph::new(conn_stats).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Connections "),
    );
    f.render_widget(conn_widget, inner_chunks[2]);
}

/// Panel 4: Latency Distribution
fn render_latency_distribution(f: &mut Frame, state: &EnhancedMetricsState, area: Rect) {
    let inner_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(area);

    // Latency sparkline (P99 over time)
    let latency_sparkline = Sparkline::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" P99 Latency History "),
        )
        .data(&state.latency_history)
        .style(Style::default().fg(Color::Yellow));
    f.render_widget(latency_sparkline, inner_chunks[0]);

    // Latency percentiles
    let latency_stats = vec![
        Line::from(vec![
            Span::styled("P50:   ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(
                format!("{:.2}ms", state.latency_p50),
                get_latency_color(state.latency_p50),
            ),
        ]),
        Line::from(vec![
            Span::styled("P95:   ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(
                format!("{:.2}ms", state.latency_p95),
                get_latency_color(state.latency_p95),
            ),
        ]),
        Line::from(vec![
            Span::styled("P99:   ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(
                format!("{:.2}ms", state.latency_p99),
                get_latency_color(state.latency_p99),
            ),
        ]),
        Line::from(vec![
            Span::styled("P99.9: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(
                format!("{:.2}ms", state.latency_p999),
                get_latency_color(state.latency_p999),
            ),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled(
                "Slow Commands: ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format_number(state.slow_commands),
                if state.slow_commands > 0 {
                    Style::default().fg(Color::Red)
                } else {
                    Style::default().fg(Color::Green)
                },
            ),
        ]),
    ];

    let latency_widget = Paragraph::new(latency_stats).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Percentiles "),
    );
    f.render_widget(latency_widget, inner_chunks[1]);
}

/// Panel 5: Cache Performance
fn render_cache_performance(f: &mut Frame, state: &EnhancedMetricsState, area: Rect) {
    let inner_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(area);

    // Hit rate gauge
    let hit_gauge = Gauge::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Cache Hit Rate "),
        )
        .gauge_style(Style::default().fg(if state.hit_rate > 90.0 {
            Color::Green
        } else if state.hit_rate > 70.0 {
            Color::Yellow
        } else {
            Color::Red
        }))
        .ratio(state.hit_rate / 100.0)
        .label(format!("{:.1}%", state.hit_rate));
    f.render_widget(hit_gauge, inner_chunks[0]);

    // Cache stats
    let _total_ops = state.keyspace_hits + state.keyspace_misses;
    let cache_stats = vec![
        Line::from(vec![
            Span::styled("Hits:      ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(
                format_number(state.keyspace_hits),
                Style::default().fg(Color::Green),
            ),
        ]),
        Line::from(vec![
            Span::styled("Misses:    ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(
                format_number(state.keyspace_misses),
                Style::default().fg(Color::Red),
            ),
        ]),
        Line::from(vec![
            Span::styled("Miss Rate: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format!("{:.1}%", state.miss_rate)),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("Expired:   ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format_number(state.expired_keys)),
        ]),
        Line::from(vec![
            Span::styled("Evicted:   ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format_number(state.evicted_keys)),
        ]),
        Line::from(vec![
            Span::styled("Evict Rate:", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format!("{}/s", format_number(state.eviction_rate))),
        ]),
    ];

    let cache_widget = Paragraph::new(cache_stats)
        .block(Block::default().borders(Borders::ALL).title(" Statistics "));
    f.render_widget(cache_widget, inner_chunks[1]);
}

/// Panel 6: Additional Statistics
fn render_additional_stats(f: &mut Frame, state: &EnhancedMetricsState, area: Rect) {
    let stats = vec![
        Line::from(Span::styled(
            "Server Metrics",
            Style::default().add_modifier(Modifier::BOLD | Modifier::UNDERLINED),
        )),
        Line::from(""),
        Line::from(vec![
            Span::styled(
                "Total Commands:   ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(format_number(state.total_commands)),
        ]),
        Line::from(vec![
            Span::styled(
                "Connected Clients:",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!("{}", state.connected_clients)),
        ]),
        Line::from(vec![
            Span::styled(
                "Peak Memory:      ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(format_bytes(state.peak_memory)),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            "Performance",
            Style::default().add_modifier(Modifier::BOLD | Modifier::UNDERLINED),
        )),
        Line::from(""),
        Line::from(vec![
            Span::styled(
                "Fragmentation:    ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("{:.2}", state.fragmentation_ratio),
                if state.fragmentation_ratio > 1.5 {
                    Style::default().fg(Color::Yellow)
                } else {
                    Style::default().fg(Color::Green)
                },
            ),
        ]),
    ];

    let widget =
        Paragraph::new(stats).block(Block::default().borders(Borders::ALL).title(" Overview "));
    f.render_widget(widget, area);
}

/// Get color for latency value
fn get_latency_color(latency: f64) -> Style {
    if latency > 10.0 {
        Style::default().fg(Color::Red)
    } else if latency > 2.0 {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().fg(Color::Green)
    }
}
