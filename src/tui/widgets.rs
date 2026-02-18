//! Reusable TUI widget components for the Ferrite dashboard.
//!
//! Provides higher-level building blocks on top of ratatui primitives:
//! - [`SparklineChart`] - Mini throughput graph using ratatui Sparkline
//! - [`MemoryBar`] - Color-coded memory usage gauge
//! - [`KeyTable`] - Scrollable key browser with type/TTL columns
//! - [`LogViewer`] - Filterable log display with level coloring
//! - [`MetricCard`] - Stat card showing a labeled value

use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, List, ListItem, ListState, Paragraph, Sparkline, Wrap},
    Frame,
};

use super::metrics::{format_bytes, KeyInfo, LogEntry, LogLevel};

// ── SparklineChart ──────────────────────────────────────────────────────────

/// A mini throughput/sparkline chart with a title showing the current value.
pub struct SparklineChart<'a> {
    pub title: String,
    pub data: &'a [u64],
    pub color: Color,
}

impl<'a> SparklineChart<'a> {
    /// Create a new sparkline chart.
    pub fn new(title: impl Into<String>, data: &'a [u64], color: Color) -> Self {
        Self {
            title: title.into(),
            data,
            color,
        }
    }

    /// Render the sparkline into the given frame area.
    pub fn render(&self, f: &mut Frame, area: Rect) {
        let sparkline = Sparkline::default()
            .block(
                Block::default()
                    .title(format!(" {} ", self.title))
                    .borders(Borders::ALL),
            )
            .data(self.data)
            .style(Style::default().fg(self.color));
        f.render_widget(sparkline, area);
    }
}

// ── MemoryBar ───────────────────────────────────────────────────────────────

/// A color-coded memory usage gauge that changes color based on utilization.
pub struct MemoryBar {
    pub used: u64,
    pub total: u64,
    pub label: Option<String>,
}

impl MemoryBar {
    /// Create a new memory bar.
    pub fn new(used: u64, total: u64) -> Self {
        Self {
            used,
            total,
            label: None,
        }
    }

    /// Set a custom label (defaults to "USED / TOTAL (PERCENT%)").
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Compute the usage ratio clamped to [0.0, 1.0].
    fn ratio(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            (self.used as f64 / self.total as f64).min(1.0)
        }
    }

    /// Pick a color based on utilization: green < 70%, yellow < 90%, red >= 90%.
    fn color(&self) -> Color {
        let r = self.ratio();
        if r > 0.9 {
            Color::Red
        } else if r > 0.7 {
            Color::Yellow
        } else {
            Color::Green
        }
    }

    /// Render the memory gauge into the given frame area.
    pub fn render(&self, f: &mut Frame, area: Rect) {
        let ratio = self.ratio();
        let label = self.label.clone().unwrap_or_else(|| {
            format!(
                "{} / {} ({:.1}%)",
                format_bytes(self.used),
                format_bytes(self.total),
                ratio * 100.0
            )
        });

        let gauge = Gauge::default()
            .block(Block::default().title(" MEMORY ").borders(Borders::ALL))
            .gauge_style(Style::default().fg(self.color()))
            .ratio(ratio)
            .label(label);
        f.render_widget(gauge, area);
    }
}

// ── KeyTable ────────────────────────────────────────────────────────────────

/// A scrollable key browser that displays keys with their type, TTL, and size.
pub struct KeyTable<'a> {
    pub keys: &'a [KeyInfo],
    pub list_state: &'a mut ListState,
}

impl<'a> KeyTable<'a> {
    /// Create a new key table.
    pub fn new(keys: &'a [KeyInfo], list_state: &'a mut ListState) -> Self {
        Self { keys, list_state }
    }

    /// Render the key table into the given frame area.
    pub fn render(self, f: &mut Frame, area: Rect) {
        let items: Vec<ListItem> = self
            .keys
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

        let count_text = if self.keys.is_empty() {
            "No keys found".to_string()
        } else {
            format!("{} keys", self.keys.len())
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

        f.render_stateful_widget(list, area, self.list_state);
    }
}

// ── LogViewer ───────────────────────────────────────────────────────────────

/// A filterable log viewer that displays log entries with colored severity levels.
pub struct LogViewer<'a> {
    pub entries: &'a [LogEntry],
    pub min_level: LogLevel,
    pub scroll_offset: usize,
}

impl<'a> LogViewer<'a> {
    /// Create a new log viewer.
    pub fn new(entries: &'a [LogEntry], min_level: LogLevel) -> Self {
        Self {
            entries,
            min_level,
            scroll_offset: 0,
        }
    }

    /// Set the scroll offset for pagination.
    pub fn with_scroll(mut self, offset: usize) -> Self {
        self.scroll_offset = offset;
        self
    }

    /// Render the log viewer into the given frame area.
    pub fn render(&self, f: &mut Frame, area: Rect) {
        let filtered: Vec<&LogEntry> = self
            .entries
            .iter()
            .filter(|e| e.level >= self.min_level)
            .collect();

        let visible_height = area.height.saturating_sub(2) as usize; // minus borders
        let total = filtered.len();
        let start = if total > visible_height + self.scroll_offset {
            total - visible_height - self.scroll_offset
        } else {
            0
        };
        let end = (start + visible_height).min(total);

        let lines: Vec<Line> = filtered[start..end]
            .iter()
            .map(|entry| {
                let level_color = log_level_color(entry.level);
                Line::from(vec![
                    Span::styled(
                        format!("{} ", entry.timestamp),
                        Style::default().fg(Color::DarkGray),
                    ),
                    Span::styled(
                        format!("{:5} ", entry.level.as_str()),
                        Style::default()
                            .fg(level_color)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::raw(&entry.message),
                ])
            })
            .collect();

        let level_label = self.min_level.as_str();
        let title = format!(
            " Logs ({} entries, filter: {}+) ",
            total, level_label
        );

        let paragraph = Paragraph::new(lines)
            .block(Block::default().borders(Borders::ALL).title(title))
            .wrap(Wrap { trim: false });
        f.render_widget(paragraph, area);
    }
}

// ── MetricCard ──────────────────────────────────────────────────────────────

/// A stat card displaying a labeled value, optionally with a subtitle or color.
pub struct MetricCard {
    pub label: String,
    pub value: String,
    pub subtitle: Option<String>,
    pub color: Color,
}

impl MetricCard {
    /// Create a new metric card.
    pub fn new(label: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            value: value.into(),
            subtitle: None,
            color: Color::Cyan,
        }
    }

    /// Set a subtitle shown below the value.
    pub fn with_subtitle(mut self, subtitle: impl Into<String>) -> Self {
        self.subtitle = Some(subtitle.into());
        self
    }

    /// Set the accent color for the value.
    pub fn with_color(mut self, color: Color) -> Self {
        self.color = color;
        self
    }

    /// Render the metric card into the given frame area.
    pub fn render(&self, f: &mut Frame, area: Rect) {
        let mut lines = vec![
            Line::from(Span::styled(
                &self.value,
                Style::default()
                    .fg(self.color)
                    .add_modifier(Modifier::BOLD),
            )),
        ];

        if let Some(ref sub) = self.subtitle {
            lines.push(Line::from(Span::styled(
                sub.as_str(),
                Style::default().fg(Color::DarkGray),
            )));
        }

        let paragraph = Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" {} ", self.label)),
        );
        f.render_widget(paragraph, area);
    }
}

// ── ClientTable ─────────────────────────────────────────────────────────────

/// Renders a table of connected clients with ID, address, database, and last command.
pub fn render_client_table(
    f: &mut Frame,
    clients: &[super::metrics::ClientInfo],
    area: Rect,
    list_state: &mut ListState,
) {
    let mut items = vec![ListItem::new(Line::from(vec![
        Span::styled(
            format!("{:<6}", "ID"),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("{:<22}", "ADDRESS"),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("{:<4}", "DB"),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("{:<8}", "IDLE"),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled("CMD", Style::default().add_modifier(Modifier::BOLD)),
    ]))];

    for client in clients.iter() {
        let line = Line::from(vec![
            Span::raw(format!("{:<6}", client.id)),
            Span::raw(format!("{:<22}", client.addr)),
            Span::raw(format!("{:<4}", client.db)),
            Span::raw(format!("{:<8}", format_idle(client.idle))),
            Span::styled(&client.last_cmd, Style::default().fg(Color::Cyan)),
        ]);
        items.push(ListItem::new(line));
    }

    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" Connected Clients ({}) ", clients.len())),
        )
        .highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("> ");

    f.render_stateful_widget(list, area, list_state);
}

/// Renders a bandwidth stats panel showing network in/out sparklines.
pub fn render_bandwidth_panel(
    f: &mut Frame,
    net_in_history: &[u64],
    net_out_history: &[u64],
    net_in_per_sec: u64,
    net_out_per_sec: u64,
    area: Rect,
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    let in_sparkline = Sparkline::default()
        .block(Block::default().borders(Borders::ALL).title(format!(
            " Network In: {}/s ",
            format_bytes(net_in_per_sec)
        )))
        .data(net_in_history)
        .style(Style::default().fg(Color::Cyan));
    f.render_widget(in_sparkline, chunks[0]);

    let out_sparkline = Sparkline::default()
        .block(Block::default().borders(Borders::ALL).title(format!(
            " Network Out: {}/s ",
            format_bytes(net_out_per_sec)
        )))
        .data(net_out_history)
        .style(Style::default().fg(Color::Magenta));
    f.render_widget(out_sparkline, chunks[1]);
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Get color for a Redis data type string.
pub fn key_type_color(key_type: &str) -> Color {
    match key_type {
        "string" => Color::Green,
        "list" => Color::Blue,
        "hash" => Color::Magenta,
        "set" => Color::Yellow,
        "zset" => Color::Cyan,
        "stream" => Color::Red,
        _ => Color::Gray,
    }
}

/// Get color for a log level.
pub fn log_level_color(level: LogLevel) -> Color {
    match level {
        LogLevel::Trace => Color::DarkGray,
        LogLevel::Debug => Color::Gray,
        LogLevel::Info => Color::Green,
        LogLevel::Warning => Color::Yellow,
        LogLevel::Error => Color::Red,
    }
}

/// Get color for a latency value (ms).
pub fn latency_color(ms: f64) -> Color {
    if ms > 10.0 {
        Color::Red
    } else if ms > 2.0 {
        Color::Yellow
    } else {
        Color::Green
    }
}

/// Format idle seconds into a compact string.
fn format_idle(seconds: u64) -> String {
    if seconds >= 3600 {
        format!("{}h", seconds / 3600)
    } else if seconds >= 60 {
        format!("{}m", seconds / 60)
    } else {
        format!("{}s", seconds)
    }
}

/// Create a centered rectangle within the given area.
pub fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::{
        backend::TestBackend,
        Terminal,
    };

    #[test]
    fn test_memory_bar_ratio() {
        let bar = MemoryBar::new(50, 100);
        assert!((bar.ratio() - 0.5).abs() < f64::EPSILON);

        let bar = MemoryBar::new(0, 0);
        assert!((bar.ratio() - 0.0).abs() < f64::EPSILON);

        let bar = MemoryBar::new(200, 100);
        assert!((bar.ratio() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_memory_bar_color() {
        let bar = MemoryBar::new(50, 100);
        assert_eq!(bar.color(), Color::Green);

        let bar = MemoryBar::new(80, 100);
        assert_eq!(bar.color(), Color::Yellow);

        let bar = MemoryBar::new(95, 100);
        assert_eq!(bar.color(), Color::Red);
    }

    #[test]
    fn test_key_type_color() {
        assert_eq!(key_type_color("string"), Color::Green);
        assert_eq!(key_type_color("list"), Color::Blue);
        assert_eq!(key_type_color("hash"), Color::Magenta);
        assert_eq!(key_type_color("set"), Color::Yellow);
        assert_eq!(key_type_color("zset"), Color::Cyan);
        assert_eq!(key_type_color("stream"), Color::Red);
        assert_eq!(key_type_color("unknown"), Color::Gray);
    }

    #[test]
    fn test_log_level_color() {
        assert_eq!(log_level_color(LogLevel::Trace), Color::DarkGray);
        assert_eq!(log_level_color(LogLevel::Debug), Color::Gray);
        assert_eq!(log_level_color(LogLevel::Info), Color::Green);
        assert_eq!(log_level_color(LogLevel::Warning), Color::Yellow);
        assert_eq!(log_level_color(LogLevel::Error), Color::Red);
    }

    #[test]
    fn test_latency_color() {
        assert_eq!(latency_color(0.5), Color::Green);
        assert_eq!(latency_color(5.0), Color::Yellow);
        assert_eq!(latency_color(15.0), Color::Red);
    }

    #[test]
    fn test_metric_card_creation() {
        let card = MetricCard::new("Ops/sec", "125,432")
            .with_color(Color::Cyan)
            .with_subtitle("peak: 200K");
        assert_eq!(card.label, "Ops/sec");
        assert_eq!(card.value, "125,432");
        assert_eq!(card.subtitle.as_deref(), Some("peak: 200K"));
        assert_eq!(card.color, Color::Cyan);
    }

    #[test]
    fn test_sparkline_chart_creation() {
        let data = vec![1, 2, 3, 4, 5];
        let chart = SparklineChart::new("OPS/SEC", &data, Color::Green);
        assert_eq!(chart.title, "OPS/SEC");
        assert_eq!(chart.data.len(), 5);
        assert_eq!(chart.color, Color::Green);
    }

    #[test]
    fn test_memory_bar_render() {
        let backend = TestBackend::new(80, 10);
        let mut terminal = Terminal::new(backend).expect("failed to create terminal");
        let bar = MemoryBar::new(500, 1000);

        terminal
            .draw(|f| {
                bar.render(f, f.area());
            })
            .expect("failed to draw");
    }

    #[test]
    fn test_metric_card_render() {
        let backend = TestBackend::new(40, 5);
        let mut terminal = Terminal::new(backend).expect("failed to create terminal");
        let card = MetricCard::new("Test", "42").with_subtitle("subtitle");

        terminal
            .draw(|f| {
                card.render(f, f.area());
            })
            .expect("failed to draw");
    }

    #[test]
    fn test_sparkline_render() {
        let backend = TestBackend::new(60, 5);
        let mut terminal = Terminal::new(backend).expect("failed to create terminal");
        let data = vec![10, 20, 30, 20, 10, 5, 15, 25];
        let chart = SparklineChart::new("Throughput", &data, Color::Cyan);

        terminal
            .draw(|f| {
                chart.render(f, f.area());
            })
            .expect("failed to draw");
    }

    #[test]
    fn test_log_viewer_render() {
        let backend = TestBackend::new(80, 15);
        let mut terminal = Terminal::new(backend).expect("failed to create terminal");

        let entries = vec![
            LogEntry {
                timestamp: "12:00:01".to_string(),
                level: LogLevel::Info,
                message: "Server started".to_string(),
            },
            LogEntry {
                timestamp: "12:00:02".to_string(),
                level: LogLevel::Debug,
                message: "Connection accepted".to_string(),
            },
            LogEntry {
                timestamp: "12:00:03".to_string(),
                level: LogLevel::Warning,
                message: "Memory usage high".to_string(),
            },
        ];

        let viewer = LogViewer::new(&entries, LogLevel::Info);

        terminal
            .draw(|f| {
                viewer.render(f, f.area());
            })
            .expect("failed to draw");
    }

    #[test]
    fn test_format_idle() {
        assert_eq!(format_idle(5), "5s");
        assert_eq!(format_idle(120), "2m");
        assert_eq!(format_idle(7200), "2h");
    }

    #[test]
    fn test_centered_rect() {
        let area = Rect::new(0, 0, 100, 50);
        let centered = centered_rect(50, 50, area);
        // The centered rect should be roughly in the middle
        assert!(centered.x > 0);
        assert!(centered.y > 0);
        assert!(centered.width < area.width);
        assert!(centered.height < area.height);
    }
}
