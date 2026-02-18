//! UI layout and rendering for the Ferrite TUI dashboard.
//!
//! Each tab has its own rendering function:
//! - Overview: Server info, memory gauge, throughput sparkline, latency, HybridLog tiers
//! - Keys: Scrollable key browser with pattern filtering and detail view
//! - Commands: Real-time command stream and command statistics
//! - Memory: Memory breakdown chart, peak tracking, fragmentation
//! - Network: Connected clients list, bandwidth sparklines, connection stats
//! - Logs: Real-time log viewer with level filtering

use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, List, ListItem, Paragraph, Sparkline, Tabs, Wrap},
    Frame,
};

use super::app::{App, InputMode, Tab};
use super::metrics::{format_bytes, format_number, format_uptime};
use super::widgets::{
    self, centered_rect, key_type_color, latency_color, LogViewer, MemoryBar, SparklineChart,
};

/// Top-level rendering function: draws the tab bar and dispatches to the active tab.
pub fn draw(f: &mut Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Tab bar
            Constraint::Min(0),    // Tab content
            Constraint::Length(1), // Status bar
        ])
        .split(f.area());

    draw_tabs(f, app, chunks[0]);

    match app.current_tab {
        Tab::Overview => draw_overview(f, app, chunks[1]),
        Tab::Keys => draw_keys(f, app, chunks[1]),
        Tab::Commands => draw_commands(f, app, chunks[1]),
        Tab::Memory => draw_memory(f, app, chunks[1]),
        Tab::Network => draw_network(f, app, chunks[1]),
        Tab::Logs => draw_logs(f, app, chunks[1]),
    }

    draw_status_bar(f, app, chunks[2]);
}

// ── Tab Bar ─────────────────────────────────────────────────────────────────

fn draw_tabs(f: &mut Frame, app: &App, area: Rect) {
    let tab_titles: Vec<Line> = Tab::all()
        .iter()
        .map(|t| {
            let style = if *t == app.current_tab {
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::Gray)
            };
            Line::from(Span::styled(t.name(), style))
        })
        .collect();

    let tabs = Tabs::new(tab_titles)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Ferrite Dashboard "),
        )
        .select(app.current_tab.index())
        .style(Style::default().fg(Color::White))
        .highlight_style(Style::default().fg(Color::Cyan));
    f.render_widget(tabs, area);
}

// ── Status Bar ──────────────────────────────────────────────────────────────

fn draw_status_bar(f: &mut Frame, app: &App, area: Rect) {
    let mode_indicator = match app.input_mode {
        InputMode::Normal => Span::styled(
            " NORMAL ",
            Style::default()
                .fg(Color::Black)
                .bg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ),
        InputMode::Command => Span::styled(
            " COMMAND ",
            Style::default()
                .fg(Color::Black)
                .bg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        InputMode::Search => Span::styled(
            " SEARCH ",
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
    };

    let live_indicator = if app.paused {
        Span::styled(
            " PAUSED ",
            Style::default()
                .fg(Color::Black)
                .bg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
    } else {
        Span::styled(
            " LIVE ",
            Style::default()
                .fg(Color::Black)
                .bg(Color::Green)
                .add_modifier(Modifier::BOLD),
        )
    };

    let help = match app.input_mode {
        InputMode::Normal => {
            " q:quit  Tab/1-6:tabs  p:pause  /:search  ::command "
        }
        InputMode::Command => " Enter:execute  Esc:cancel ",
        InputMode::Search => " Enter:search  Esc:cancel ",
    };

    let status = Line::from(vec![
        mode_indicator,
        Span::raw(" "),
        live_indicator,
        Span::raw(format!(
            "  {}:{}",
            app.collector.host, app.collector.port
        )),
        Span::styled(help, Style::default().fg(Color::DarkGray)),
    ]);

    let paragraph = Paragraph::new(status);
    f.render_widget(paragraph, area);
}

// ── Overview Tab ────────────────────────────────────────────────────────────

fn draw_overview(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(7),  // Server info + Memory row
            Constraint::Length(5),  // Throughput sparkline
            Constraint::Length(5),  // Latency
            Constraint::Length(4),  // HybridLog tiers
            Constraint::Min(5),    // Clients + Slowlog
        ])
        .split(area);

    // Row 1: Server Info + Memory gauge
    let top_row = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(chunks[0]);

    draw_server_info(f, app, top_row[0]);
    draw_memory_overview(f, app, top_row[1]);

    // Row 2: Throughput sparkline
    draw_throughput(f, app, chunks[1]);

    // Row 3: Latency
    draw_latency_overview(f, app, chunks[2]);

    // Row 4: HybridLog tiers
    draw_hybridlog_tiers(f, app, chunks[3]);

    // Row 5: Clients + Slowlog
    let bottom_row = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[4]);

    draw_clients_overview(f, app, bottom_row[0]);
    draw_slowlog_overview(f, app, bottom_row[1]);
}

fn draw_server_info(f: &mut Frame, app: &App, area: Rect) {
    let stats = &app.collector.stats;
    let version = if stats.version.is_empty() {
        "unknown"
    } else {
        &stats.version
    };

    let text = vec![
        Line::from(vec![
            Span::styled("Version: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(version),
        ]),
        Line::from(vec![
            Span::styled("Uptime:  ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format_uptime(stats.uptime_seconds)),
        ]),
        Line::from(vec![
            Span::styled("Clients: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(
                format!("{}", stats.connected_clients),
                Style::default().fg(Color::Green),
            ),
            if stats.blocked_clients > 0 {
                Span::styled(
                    format!(" ({} blocked)", stats.blocked_clients),
                    Style::default().fg(Color::Yellow),
                )
            } else {
                Span::raw("")
            },
        ]),
        Line::from(vec![
            Span::styled("DB Keys: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format_number(stats.db_keys)),
        ]),
        Line::from(vec![
            Span::styled("Ops/sec: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(
                format_number(stats.ops_per_sec),
                Style::default().fg(Color::Cyan),
            ),
        ]),
    ];

    let paragraph = Paragraph::new(text).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Server Info "),
    );
    f.render_widget(paragraph, area);
}

fn draw_memory_overview(f: &mut Frame, app: &App, area: Rect) {
    let stats = &app.collector.stats;
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(area);

    // Memory gauge
    let bar = MemoryBar::new(stats.used_memory, stats.max_memory);
    bar.render(f, chunks[0]);

    // Memory details
    let text = vec![
        Line::from(vec![
            Span::styled("Peak:          ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format_bytes(stats.peak_memory)),
        ]),
        Line::from(vec![
            Span::styled("RSS:           ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format_bytes(stats.used_memory_rss)),
        ]),
        Line::from(vec![
            Span::styled("Fragmentation: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(
                format!("{:.2}", stats.fragmentation_ratio),
                if stats.fragmentation_ratio > 1.5 {
                    Style::default().fg(Color::Yellow)
                } else {
                    Style::default().fg(Color::Green)
                },
            ),
        ]),
    ];

    let paragraph = Paragraph::new(text).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Memory Details "),
    );
    f.render_widget(paragraph, chunks[1]);
}

fn draw_throughput(f: &mut Frame, app: &App, area: Rect) {
    let chart = SparklineChart::new(
        format!("OPS/SEC: {}", format_number(app.collector.stats.ops_per_sec)),
        &app.ops_history,
        Color::Cyan,
    );
    chart.render(f, area);
}

fn draw_latency_overview(f: &mut Frame, app: &App, area: Rect) {
    let stats = &app.collector.stats;
    let text = vec![
        Line::from(vec![
            Span::styled("P50:   ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(
                format!("{:.2}ms", stats.latency_p50),
                Style::default().fg(latency_color(stats.latency_p50)),
            ),
            Span::raw("    "),
            Span::styled("P95:   ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(
                format!("{:.2}ms", stats.latency_p95),
                Style::default().fg(latency_color(stats.latency_p95)),
            ),
            Span::raw("    "),
            Span::styled("P99:   ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(
                format!("{:.2}ms", stats.latency_p99),
                Style::default().fg(latency_color(stats.latency_p99)),
            ),
            Span::raw("    "),
            Span::styled("P99.9: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(
                format!("{:.2}ms", stats.latency_p999),
                Style::default().fg(latency_color(stats.latency_p999)),
            ),
        ]),
    ];

    let sparkline_data = &app.latency_history;
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(area);

    let paragraph = Paragraph::new(text).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Latency "),
    );
    f.render_widget(paragraph, chunks[0]);

    let sparkline = Sparkline::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" P99 History "),
        )
        .data(sparkline_data)
        .style(Style::default().fg(Color::Yellow));
    f.render_widget(sparkline, chunks[1]);
}

fn draw_hybridlog_tiers(f: &mut Frame, app: &App, area: Rect) {
    let stats = &app.collector.stats;
    let text = vec![
        Line::from(vec![
            Span::styled(
                " MUTABLE ",
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!(
                " {} ({} entries)  ",
                format_bytes(stats.hybridlog_mutable_bytes),
                format_number(stats.hybridlog_mutable_entries)
            )),
            Span::styled(
                " READ-ONLY ",
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!(
                " {} ({} entries)  ",
                format_bytes(stats.hybridlog_readonly_bytes),
                format_number(stats.hybridlog_readonly_entries)
            )),
            Span::styled(
                " DISK ",
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Blue)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!(
                " {} ({} entries)",
                format_bytes(stats.hybridlog_disk_bytes),
                format_number(stats.hybridlog_disk_entries)
            )),
        ]),
    ];

    let paragraph = Paragraph::new(text).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" HybridLog Tiers "),
    );
    f.render_widget(paragraph, area);
}

fn draw_clients_overview(f: &mut Frame, app: &App, area: Rect) {
    let mut lines: Vec<Line> = vec![Line::from(vec![
        Span::styled(
            format!("{:<6}", "ID"),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("{:<22}", "ADDRESS"),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled("CMD", Style::default().add_modifier(Modifier::BOLD)),
    ])];

    for client in app.collector.clients.iter().take(10) {
        lines.push(Line::from(format!(
            "{:<6}{:<22}{}",
            client.id, client.addr, client.last_cmd
        )));
    }

    let paragraph = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!(" Clients ({}) ", app.collector.stats.connected_clients)),
    );
    f.render_widget(paragraph, area);
}

fn draw_slowlog_overview(f: &mut Frame, app: &App, area: Rect) {
    let mut lines: Vec<Line> = vec![Line::from(vec![
        Span::styled(
            format!("{:>8}", "TIME"),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled("  COMMAND", Style::default().add_modifier(Modifier::BOLD)),
    ])];

    for entry in app.collector.slowlog.iter().take(10) {
        lines.push(Line::from(format!(
            "{:>6.1}ms  {}",
            entry.duration_ms, entry.command
        )));
    }

    if app.collector.slowlog.is_empty() {
        lines.push(Line::from(Span::styled(
            "  No slow commands recorded",
            Style::default().fg(Color::DarkGray),
        )));
    }

    let paragraph = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!(
                " Slowlog ({}) ",
                app.collector.stats.slow_commands_count
            )),
    );
    f.render_widget(paragraph, area);
}

// ── Keys Tab ────────────────────────────────────────────────────────────────

fn draw_keys(f: &mut Frame, app: &mut App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Search bar
            Constraint::Min(0),    // Key list or detail
        ])
        .split(area);

    draw_key_search_bar(f, app, chunks[0]);

    if app.key_delete_confirm {
        draw_key_delete_confirm(f, app, chunks[1]);
    } else if app.key_detail.is_some() {
        draw_key_detail(f, app, chunks[1]);
    } else {
        draw_key_list(f, app, chunks[1]);
    }
}

fn draw_key_search_bar(f: &mut Frame, app: &App, area: Rect) {
    let input_style = if app.input_mode == InputMode::Search {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default()
    };

    let help_text = if app.input_mode == InputMode::Search {
        "[Enter]search [Esc]cancel"
    } else if app.key_detail.is_some() {
        "[Esc]back [d]delete"
    } else {
        "[/,s]search [j/k]navigate [Enter]details [n]next [r]refresh [d]delete"
    };

    let search_text = format!(
        "Pattern: {} | Cursor: {} | {}",
        app.key_pattern, app.key_cursor, help_text
    );

    let search_bar = Paragraph::new(search_text)
        .style(input_style)
        .block(Block::default().borders(Borders::ALL).title(" Search "));
    f.render_widget(search_bar, area);
}

fn draw_key_list(f: &mut Frame, app: &mut App, area: Rect) {
    let keys = &app.keys;
    let items: Vec<ListItem> = keys
        .iter()
        .map(|key| {
            let type_color = key_type_color(&key.key_type);

            let ttl_str = if key.ttl < 0 {
                String::from("no expiry")
            } else {
                format!("{}s", key.ttl)
            };

            let size_str = key
                .size
                .map(|s| format!(" | {}", format_bytes(s)))
                .unwrap_or_default();

            let line = Line::from(vec![
                Span::styled(
                    format!("[{:6}] ", key.key_type),
                    Style::default().fg(type_color),
                ),
                Span::raw(&key.name),
                Span::styled(
                    format!("  TTL: {}{}", ttl_str, size_str),
                    Style::default().fg(Color::DarkGray),
                ),
            ]);
            ListItem::new(line)
        })
        .collect();

    let count_text = if keys.is_empty() {
        "No keys found - press 'r' to scan".to_string()
    } else {
        format!("{} keys", keys.len())
    };

    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" Keys ({}) ", count_text)),
        )
        .highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("> ");

    f.render_stateful_widget(list, area, &mut app.key_list_state);
}

fn draw_key_detail(f: &mut Frame, app: &App, area: Rect) {
    if let Some(detail) = &app.key_detail {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(6), Constraint::Min(0)])
            .split(area);

        let ttl_str = if detail.ttl < 0 {
            "No expiration".to_string()
        } else {
            format!("{} seconds", detail.ttl)
        };

        let type_color = key_type_color(&detail.key_type);

        let metadata = vec![
            Line::from(vec![
                Span::styled("Key:      ", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(&detail.key),
            ]),
            Line::from(vec![
                Span::styled("Type:     ", Style::default().add_modifier(Modifier::BOLD)),
                Span::styled(&detail.key_type, Style::default().fg(type_color)),
            ]),
            Line::from(vec![
                Span::styled("TTL:      ", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(ttl_str),
            ]),
            Line::from(vec![
                Span::styled("Encoding: ", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(&detail.encoding),
            ]),
        ];

        let meta_widget = Paragraph::new(metadata).block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Key Details "),
        );
        f.render_widget(meta_widget, chunks[0]);

        let value_widget = Paragraph::new(detail.value_preview.clone())
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" Value Preview "),
            )
            .wrap(Wrap { trim: false });
        f.render_widget(value_widget, chunks[1]);
    }
}

fn draw_key_delete_confirm(f: &mut Frame, app: &App, area: Rect) {
    let dialog_area = centered_rect(60, 30, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Confirm Delete ")
        .style(Style::default().bg(Color::Black));
    f.render_widget(block, dialog_area);

    let inner_area = Rect {
        x: dialog_area.x + 2,
        y: dialog_area.y + 1,
        width: dialog_area.width.saturating_sub(4),
        height: dialog_area.height.saturating_sub(2),
    };

    let key_name = app
        .keys
        .get(app.key_selected)
        .map(|k| k.name.as_str())
        .unwrap_or("(unknown)");

    let text = vec![
        Line::from(""),
        Line::from(Span::styled(
            "Are you sure you want to delete this key?",
            Style::default().fg(Color::Yellow),
        )),
        Line::from(""),
        Line::from(Span::styled(
            key_name,
            Style::default()
                .fg(Color::Red)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(vec![
            Span::raw("["),
            Span::styled("y", Style::default().fg(Color::Green)),
            Span::raw("]es  ["),
            Span::styled("n", Style::default().fg(Color::Red)),
            Span::raw("]o  ["),
            Span::styled("Esc", Style::default().fg(Color::Gray)),
            Span::raw("] cancel"),
        ]),
    ];

    let paragraph = Paragraph::new(text);
    f.render_widget(paragraph, inner_area);
}

// ── Commands Tab ────────────────────────────────────────────────────────────

fn draw_commands(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // Command stats summary
            Constraint::Min(0),    // Command history / live feed
        ])
        .split(area);

    // Command summary
    let stats = &app.collector.stats;
    let summary = vec![Line::from(vec![
        Span::styled(
            "Total Commands: ",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format_number(stats.total_commands),
            Style::default().fg(Color::Cyan),
        ),
        Span::raw("    "),
        Span::styled(
            "Ops/sec: ",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format_number(stats.ops_per_sec),
            Style::default().fg(Color::Green),
        ),
        Span::raw("    "),
        Span::styled(
            "Slow: ",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format_number(stats.slow_commands_count),
            if stats.slow_commands_count > 0 {
                Style::default().fg(Color::Red)
            } else {
                Style::default().fg(Color::Green)
            },
        ),
    ])];

    let summary_widget = Paragraph::new(summary).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Command Stats "),
    );
    f.render_widget(summary_widget, chunks[0]);

    // Command history
    let chunks_bottom = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[1]);

    draw_command_stats_table(f, app, chunks_bottom[0]);
    draw_command_history(f, app, chunks_bottom[1]);
}

fn draw_command_stats_table(f: &mut Frame, app: &App, area: Rect) {
    let mut items = vec![ListItem::new(Line::from(vec![
        Span::styled(
            format!("{:<14}", "COMMAND"),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("{:<12}", "CALLS"),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            "USEC/CALL",
            Style::default().add_modifier(Modifier::BOLD),
        ),
    ]))];

    for stat in app.collector.command_stats.iter().take(15) {
        let line = Line::from(vec![
            Span::styled(
                format!("{:<14}", stat.name),
                Style::default().fg(Color::Cyan),
            ),
            Span::raw(format!("{:<12}", format_number(stat.calls))),
            Span::raw(format!("{:.1}", stat.usec_per_call)),
        ]);
        items.push(ListItem::new(line));
    }

    if app.collector.command_stats.is_empty() {
        items.push(ListItem::new(Line::from(Span::styled(
            "  No command stats available",
            Style::default().fg(Color::DarkGray),
        ))));
    }

    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Top Commands "),
    );
    f.render_widget(list, area);
}

fn draw_command_history(f: &mut Frame, app: &App, area: Rect) {
    let lines: Vec<Line> = app
        .command_history
        .iter()
        .rev()
        .take(area.height.saturating_sub(2) as usize)
        .map(|cmd| {
            Line::from(vec![
                Span::styled(
                    format!("{} ", cmd.0),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(&cmd.1, Style::default().fg(Color::Cyan)),
            ])
        })
        .collect();

    let paragraph = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Command History (recent) "),
    );
    f.render_widget(paragraph, area);
}

// ── Memory Tab ──────────────────────────────────────────────────────────────

fn draw_memory(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // Memory gauge
            Constraint::Length(10), // Memory breakdown
            Constraint::Length(5),  // Allocation timeline sparkline
            Constraint::Min(5),    // Cache performance
        ])
        .split(area);

    // Memory gauge
    let bar = MemoryBar::new(app.collector.stats.used_memory, app.collector.stats.max_memory);
    bar.render(f, chunks[0]);

    // Memory breakdown
    draw_memory_breakdown(f, app, chunks[1]);

    // Allocation timeline
    let chart = SparklineChart::new(
        format!("Memory Timeline: {}", format_bytes(app.collector.stats.used_memory)),
        &app.memory_history,
        Color::Magenta,
    );
    chart.render(f, chunks[2]);

    // Cache performance
    draw_cache_performance(f, app, chunks[3]);
}

fn draw_memory_breakdown(f: &mut Frame, app: &App, area: Rect) {
    let stats = &app.collector.stats;

    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    // Left: Memory details
    let details = vec![
        Line::from(vec![
            Span::styled(
                "Used Memory:       ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format_bytes(stats.used_memory),
                Style::default().fg(Color::Cyan),
            ),
        ]),
        Line::from(vec![
            Span::styled(
                "RSS Memory:        ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(format_bytes(stats.used_memory_rss)),
        ]),
        Line::from(vec![
            Span::styled(
                "Peak Memory:       ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(format_bytes(stats.peak_memory)),
        ]),
        Line::from(vec![
            Span::styled(
                "Max Memory:        ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(format_bytes(stats.max_memory)),
        ]),
        Line::from(vec![
            Span::styled(
                "Fragmentation:     ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("{:.2}", stats.fragmentation_ratio),
                if stats.fragmentation_ratio > 1.5 {
                    Style::default().fg(Color::Yellow)
                } else {
                    Style::default().fg(Color::Green)
                },
            ),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled(
                "Overhead:          ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(if stats.used_memory_rss > stats.used_memory {
                format_bytes(stats.used_memory_rss - stats.used_memory)
            } else {
                "0B".to_string()
            }),
        ]),
    ];

    let details_widget = Paragraph::new(details).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Memory Details "),
    );
    f.render_widget(details_widget, chunks[0]);

    // Right: HybridLog tier breakdown
    let total_hl = stats.hybridlog_mutable_bytes
        + stats.hybridlog_readonly_bytes
        + stats.hybridlog_disk_bytes;

    let tier_lines = vec![
        Line::from(Span::styled(
            "HybridLog Tier Breakdown",
            Style::default().add_modifier(Modifier::BOLD | Modifier::UNDERLINED),
        )),
        Line::from(""),
        Line::from(vec![
            Span::styled(
                "Mutable:    ",
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!(
                "{} ({} entries)",
                format_bytes(stats.hybridlog_mutable_bytes),
                format_number(stats.hybridlog_mutable_entries)
            )),
        ]),
        Line::from(vec![
            Span::styled(
                "Read-Only:  ",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!(
                "{} ({} entries)",
                format_bytes(stats.hybridlog_readonly_bytes),
                format_number(stats.hybridlog_readonly_entries)
            )),
        ]),
        Line::from(vec![
            Span::styled(
                "Disk:       ",
                Style::default()
                    .fg(Color::Blue)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!(
                "{} ({} entries)",
                format_bytes(stats.hybridlog_disk_bytes),
                format_number(stats.hybridlog_disk_entries)
            )),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled(
                "Total:      ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(format_bytes(total_hl)),
        ]),
    ];

    let tier_widget = Paragraph::new(tier_lines).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Storage Tiers "),
    );
    f.render_widget(tier_widget, chunks[1]);
}

fn draw_cache_performance(f: &mut Frame, app: &App, area: Rect) {
    let stats = &app.collector.stats;
    let total_ops = stats.keyspace_hits + stats.keyspace_misses;
    let hit_rate = if total_ops > 0 {
        stats.keyspace_hits as f64 / total_ops as f64 * 100.0
    } else {
        0.0
    };

    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(area);

    // Hit rate gauge
    let gauge = Gauge::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Hit Rate "),
        )
        .gauge_style(Style::default().fg(if hit_rate > 90.0 {
            Color::Green
        } else if hit_rate > 70.0 {
            Color::Yellow
        } else {
            Color::Red
        }))
        .ratio((hit_rate / 100.0).min(1.0))
        .label(format!("{:.1}%", hit_rate));
    f.render_widget(gauge, chunks[0]);

    // Cache stats
    let cache_lines = vec![
        Line::from(vec![
            Span::styled("Hits:      ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(
                format_number(stats.keyspace_hits),
                Style::default().fg(Color::Green),
            ),
        ]),
        Line::from(vec![
            Span::styled("Misses:    ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(
                format_number(stats.keyspace_misses),
                Style::default().fg(Color::Red),
            ),
        ]),
        Line::from(vec![
            Span::styled("Expired:   ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format_number(stats.expired_keys)),
        ]),
        Line::from(vec![
            Span::styled("Evicted:   ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format_number(stats.evicted_keys)),
        ]),
    ];

    let cache_widget = Paragraph::new(cache_lines).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Cache Statistics "),
    );
    f.render_widget(cache_widget, chunks[1]);
}

// ── Network Tab ─────────────────────────────────────────────────────────────

fn draw_network(f: &mut Frame, app: &mut App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(8),  // Bandwidth sparklines
            Constraint::Length(5),  // Connection stats
            Constraint::Min(5),    // Client list
        ])
        .split(area);

    // Bandwidth sparklines
    widgets::render_bandwidth_panel(
        f,
        &app.net_in_history,
        &app.net_out_history,
        app.collector.stats.total_net_input_bytes,
        app.collector.stats.total_net_output_bytes,
        chunks[0],
    );

    // Connection stats
    draw_connection_stats(f, app, chunks[1]);

    // Client list
    widgets::render_client_table(
        f,
        &app.collector.clients,
        chunks[2],
        &mut app.client_list_state,
    );
}

fn draw_connection_stats(f: &mut Frame, app: &App, area: Rect) {
    let stats = &app.collector.stats;
    let text = vec![
        Line::from(vec![
            Span::styled(
                "Active Clients:     ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("{}", stats.connected_clients),
                Style::default().fg(Color::Green),
            ),
            Span::raw("    "),
            Span::styled(
                "Blocked Clients:    ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("{}", stats.blocked_clients),
                if stats.blocked_clients > 0 {
                    Style::default().fg(Color::Yellow)
                } else {
                    Style::default().fg(Color::Green)
                },
            ),
        ]),
        Line::from(vec![
            Span::styled(
                "Total Connections:  ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(format_number(stats.total_connections)),
            Span::raw("    "),
            Span::styled(
                "Rejected:           ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format_number(stats.rejected_connections),
                if stats.rejected_connections > 0 {
                    Style::default().fg(Color::Red)
                } else {
                    Style::default().fg(Color::Green)
                },
            ),
        ]),
    ];

    let paragraph = Paragraph::new(text).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Connection Statistics "),
    );
    f.render_widget(paragraph, area);
}

// ── Logs Tab ────────────────────────────────────────────────────────────────

fn draw_logs(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Log filter bar
            Constraint::Min(0),    // Log entries
        ])
        .split(area);

    // Filter bar
    let level_names = ["TRACE", "DEBUG", "INFO", "WARN", "ERROR"];
    let level_colors = [
        Color::DarkGray,
        Color::Gray,
        Color::Green,
        Color::Yellow,
        Color::Red,
    ];

    let mut filter_spans = vec![Span::styled(
        "Level filter: ",
        Style::default().add_modifier(Modifier::BOLD),
    )];

    for (i, (name, color)) in level_names.iter().zip(level_colors.iter()).enumerate() {
        let is_active = app.log_min_level as usize <= i;
        let style = if is_active {
            Style::default().fg(*color).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::DarkGray)
        };
        filter_spans.push(Span::styled(format!(" {} ", name), style));
    }

    filter_spans.push(Span::styled(
        "  [1-5] set level  [c] clear",
        Style::default().fg(Color::DarkGray),
    ));

    let filter_bar = Paragraph::new(Line::from(filter_spans)).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Log Filter "),
    );
    f.render_widget(filter_bar, chunks[0]);

    // Log entries
    let viewer = LogViewer::new(&app.collector.logs, app.log_min_level)
        .with_scroll(app.log_scroll_offset);
    viewer.render(f, chunks[1]);
}
