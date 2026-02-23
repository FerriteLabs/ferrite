//! Key Browser module for Ferrite TUI
//!
//! Provides functionality to browse, search, and inspect keys in the Redis/Ferrite server.

use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph},
    Frame,
};

/// Key information for the browser
#[derive(Clone)]
pub struct KeyInfo {
    pub name: String,
    pub key_type: String,
    pub ttl: i64,
    pub size: Option<u64>,
}

/// Detailed key information
pub struct KeyDetail {
    pub key: String,
    pub key_type: String,
    pub ttl: i64,
    pub encoding: String,
    pub value_preview: String,
}

/// Key browser state
pub struct KeyBrowserState {
    /// Pattern for SCAN command (e.g., "user:*", "session:*")
    pub pattern: String,
    /// SCAN cursor for pagination
    pub cursor: u64,
    /// List of discovered keys with metadata
    pub keys: Vec<KeyInfo>,
    /// Currently selected key index
    pub selected: usize,
    /// List widget state for rendering
    pub list_state: ListState,
    /// Whether in input mode for pattern editing
    pub input_mode: bool,
    /// Detail view for selected key
    pub detail_view: Option<KeyDetail>,
    /// Confirmation dialog for deletion
    pub delete_confirm: bool,
}

impl Default for KeyBrowserState {
    fn default() -> Self {
        Self {
            pattern: "*".to_string(),
            cursor: 0,
            keys: Vec::new(),
            selected: 0,
            list_state: ListState::default(),
            input_mode: false,
            detail_view: None,
            delete_confirm: false,
        }
    }
}

impl KeyBrowserState {
    /// Create a new KeyBrowserState with default pattern
    #[allow(dead_code)] // Planned for v0.2 — reserved for TUI key browser initialization
    pub fn new() -> Self {
        Self::default()
    }

    /// Move selection up
    pub fn select_previous(&mut self) {
        if self.selected > 0 {
            self.selected -= 1;
            self.list_state.select(Some(self.selected));
        }
    }

    /// Move selection down
    pub fn select_next(&mut self) {
        if self.selected < self.keys.len().saturating_sub(1) {
            self.selected += 1;
            self.list_state.select(Some(self.selected));
        }
    }

    /// Get the currently selected key
    pub fn selected_key(&self) -> Option<&KeyInfo> {
        self.keys.get(self.selected)
    }

    /// Clear the key list and reset cursor
    #[allow(dead_code)] // Planned for v0.2 — reserved for TUI key browser refresh operations
    pub fn clear_keys(&mut self) {
        self.keys.clear();
        self.cursor = 0;
        self.selected = 0;
        self.list_state.select(None);
    }

    /// Update keys list and select first item if available
    pub fn set_keys(&mut self, keys: Vec<KeyInfo>) {
        self.keys = keys;
        if !self.keys.is_empty() {
            self.selected = 0;
            self.list_state.select(Some(0));
        }
    }

    /// Enter input mode for pattern editing
    pub fn enter_input_mode(&mut self) {
        self.input_mode = true;
    }

    /// Exit input mode
    pub fn exit_input_mode(&mut self) {
        self.input_mode = false;
    }

    /// Add character to pattern
    pub fn push_pattern_char(&mut self, c: char) {
        self.pattern.push(c);
    }

    /// Remove last character from pattern
    pub fn pop_pattern_char(&mut self) {
        self.pattern.pop();
    }

    /// Reset pattern to default
    #[allow(dead_code)] // Planned for v0.2 — reserved for TUI pattern reset functionality
    pub fn reset_pattern(&mut self) {
        self.pattern = "*".to_string();
    }

    /// Show detail view for current key
    pub fn show_detail(&mut self, detail: KeyDetail) {
        self.detail_view = Some(detail);
    }

    /// Hide detail view
    pub fn hide_detail(&mut self) {
        self.detail_view = None;
    }

    /// Toggle delete confirmation
    pub fn toggle_delete_confirm(&mut self) {
        self.delete_confirm = !self.delete_confirm;
    }

    /// Cancel delete confirmation
    pub fn cancel_delete(&mut self) {
        self.delete_confirm = false;
    }
}

/// Render the key browser tab
pub fn render(f: &mut Frame, state: &mut KeyBrowserState, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Search bar
            Constraint::Min(0),    // Key list or detail
        ])
        .split(area);

    render_search_bar(f, state, chunks[0]);

    // Show delete confirmation dialog if active
    if state.delete_confirm {
        render_delete_confirm(f, state, chunks[1]);
    } else if state.detail_view.is_some() {
        render_key_detail(f, state, chunks[1]);
    } else {
        render_key_list(f, state, chunks[1]);
    }
}

/// Render the search/pattern input bar
fn render_search_bar(f: &mut Frame, state: &KeyBrowserState, area: Rect) {
    let input_style = if state.input_mode {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default()
    };

    let help_text = if state.input_mode {
        "[Enter]search [Esc]cancel"
    } else if state.detail_view.is_some() {
        "[Esc]back [d]delete"
    } else {
        "[/,s]search [j/k,↑↓]navigate [Enter]details [n]next page [r]refresh [d]delete"
    };

    let search_text = format!(
        "Pattern: {} | Cursor: {} | {}",
        state.pattern, state.cursor, help_text
    );

    let search_bar = Paragraph::new(search_text)
        .style(input_style)
        .block(Block::default().borders(Borders::ALL).title(" Search "));
    f.render_widget(search_bar, area);
}

/// Render the key list
fn render_key_list(f: &mut Frame, state: &mut KeyBrowserState, area: Rect) {
    let items: Vec<ListItem> = state
        .keys
        .iter()
        .map(|key| {
            let type_color = get_type_color(&key.key_type);

            let ttl_str = if key.ttl < 0 {
                "∞".to_string()
            } else {
                format!("{}s", key.ttl)
            };

            let size_str = if let Some(size) = key.size {
                format!(" | Size: {}", format_bytes(size))
            } else {
                String::new()
            };

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

    let count_text = if state.keys.is_empty() {
        "No keys found".to_string()
    } else {
        format!("{} keys", state.keys.len())
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

    f.render_stateful_widget(list, area, &mut state.list_state);
}

/// Render key detail view
fn render_key_detail(f: &mut Frame, state: &KeyBrowserState, area: Rect) {
    if let Some(detail) = &state.detail_view {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(5), // Metadata
                Constraint::Min(0),    // Value preview
            ])
            .split(area);

        render_key_metadata(f, detail, chunks[0]);
        render_key_value(f, detail, chunks[1]);
    }
}

/// Render key metadata section
fn render_key_metadata(f: &mut Frame, detail: &KeyDetail, area: Rect) {
    let ttl_str = if detail.ttl < 0 {
        "No expiration".to_string()
    } else {
        format!("{} seconds", detail.ttl)
    };

    let type_color = get_type_color(&detail.key_type);

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
    f.render_widget(meta_widget, area);
}

/// Render key value preview section
fn render_key_value(f: &mut Frame, detail: &KeyDetail, area: Rect) {
    let value_widget = Paragraph::new(detail.value_preview.clone())
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Value Preview "),
        )
        .wrap(ratatui::widgets::Wrap { trim: false });
    f.render_widget(value_widget, area);
}

/// Render delete confirmation dialog
fn render_delete_confirm(f: &mut Frame, state: &KeyBrowserState, area: Rect) {
    // Create a centered dialog
    let dialog_area = centered_rect(60, 20, area);

    // Clear the background
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Confirm Delete ")
        .style(Style::default().bg(Color::Black));

    f.render_widget(block, dialog_area);

    // Split into content area
    let inner_area = Rect {
        x: dialog_area.x + 2,
        y: dialog_area.y + 1,
        width: dialog_area.width.saturating_sub(4),
        height: dialog_area.height.saturating_sub(2),
    };

    let key_name = state
        .selected_key()
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
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
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

/// Get color for a given key type
fn get_type_color(key_type: &str) -> Color {
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

/// Format bytes with appropriate unit
fn format_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.1}GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.1}MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else {
        format!("{}B", bytes)
    }
}

/// Helper function to create a centered rectangle
fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
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
