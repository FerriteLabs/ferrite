//! Application state and main event loop for the Ferrite TUI dashboard.
//!
//! The [`TuiApp`] struct holds all application state and provides `run()` as the
//! main entry point. The event loop handles keyboard input and periodic data refresh.

use std::io;
use std::time::Duration;

use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, widgets::ListState, Terminal};

use super::metrics::{DataCollector, KeyDetail, KeyInfo, LogLevel};
use super::ui;

/// History buffer length for sparkline data (number of data points retained).
const HISTORY_LEN: usize = 60;

/// Available tabs in the TUI dashboard.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Tab {
    Overview,
    Keys,
    Commands,
    Memory,
    Network,
    Logs,
}

impl Tab {
    /// Return all tabs in display order.
    pub fn all() -> &'static [Tab] {
        &[
            Tab::Overview,
            Tab::Keys,
            Tab::Commands,
            Tab::Memory,
            Tab::Network,
            Tab::Logs,
        ]
    }

    /// Display name for the tab.
    pub fn name(&self) -> &'static str {
        match self {
            Tab::Overview => "Overview",
            Tab::Keys => "Keys",
            Tab::Commands => "Commands",
            Tab::Memory => "Memory",
            Tab::Network => "Network",
            Tab::Logs => "Logs",
        }
    }

    /// Zero-based index of this tab.
    pub fn index(&self) -> usize {
        match self {
            Tab::Overview => 0,
            Tab::Keys => 1,
            Tab::Commands => 2,
            Tab::Memory => 3,
            Tab::Network => 4,
            Tab::Logs => 5,
        }
    }

    /// Advance to the next tab (wrapping).
    pub fn next(&self) -> Tab {
        match self {
            Tab::Overview => Tab::Keys,
            Tab::Keys => Tab::Commands,
            Tab::Commands => Tab::Memory,
            Tab::Memory => Tab::Network,
            Tab::Network => Tab::Logs,
            Tab::Logs => Tab::Overview,
        }
    }
}

/// Input mode for the TUI.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum InputMode {
    /// Normal navigation mode (hjkl, tab, q, etc.)
    Normal,
    /// Command input mode (triggered by ':')
    Command,
    /// Search/filter mode (triggered by '/')
    Search,
}

/// All application state for the TUI dashboard.
pub struct App {
    // Data collection
    pub collector: DataCollector,

    // Navigation
    pub current_tab: Tab,
    pub input_mode: InputMode,
    pub should_quit: bool,
    pub paused: bool,

    // History buffers for sparklines
    pub ops_history: Vec<u64>,
    pub latency_history: Vec<u64>,
    pub memory_history: Vec<u64>,
    pub net_in_history: Vec<u64>,
    pub net_out_history: Vec<u64>,

    // Keys tab state
    pub keys: Vec<KeyInfo>,
    pub key_pattern: String,
    pub key_cursor: u64,
    pub key_selected: usize,
    pub key_list_state: ListState,
    pub key_detail: Option<KeyDetail>,
    pub key_delete_confirm: bool,

    // Commands tab state
    pub command_history: Vec<(String, String)>, // (timestamp, command)

    // Network tab state
    pub client_list_state: ListState,

    // Logs tab state
    pub log_min_level: LogLevel,
    pub log_scroll_offset: usize,

    // Command input buffer
    pub command_input: String,
}

impl App {
    /// Create a new App with the given data collector.
    pub fn new(collector: DataCollector) -> Self {
        Self {
            collector,
            current_tab: Tab::Overview,
            input_mode: InputMode::Normal,
            should_quit: false,
            paused: false,

            ops_history: vec![0; HISTORY_LEN],
            latency_history: vec![0; HISTORY_LEN],
            memory_history: vec![0; HISTORY_LEN],
            net_in_history: vec![0; HISTORY_LEN],
            net_out_history: vec![0; HISTORY_LEN],

            keys: Vec::new(),
            key_pattern: "*".to_string(),
            key_cursor: 0,
            key_selected: 0,
            key_list_state: ListState::default(),
            key_detail: None,
            key_delete_confirm: false,

            command_history: Vec::new(),
            client_list_state: ListState::default(),

            log_min_level: LogLevel::Info,
            log_scroll_offset: 0,

            command_input: String::new(),
        }
    }

    /// Update metrics data from the server.
    async fn update(&mut self) {
        if self.paused {
            return;
        }

        if let Err(_e) = self.collector.refresh().await {
            // Connection errors are silently handled; the dashboard shows stale data.
        }

        // Update history buffers
        self.ops_history.remove(0);
        self.ops_history.push(self.collector.stats.ops_per_sec);

        self.latency_history.remove(0);
        self.latency_history
            .push(self.collector.stats.latency_p99 as u64);

        self.memory_history.remove(0);
        // Store memory in KB for sparkline resolution
        self.memory_history
            .push(self.collector.stats.used_memory / 1024);

        self.net_in_history.remove(0);
        self.net_in_history
            .push(self.collector.stats.total_net_input_bytes);

        self.net_out_history.remove(0);
        self.net_out_history
            .push(self.collector.stats.total_net_output_bytes);
    }

    /// Scan keys using the current pattern and cursor.
    async fn scan_keys(&mut self) {
        if let Ok((cursor, keys)) = self
            .collector
            .scan_keys(&self.key_pattern, self.key_cursor, 20)
            .await
        {
            self.key_cursor = cursor;
            self.keys = keys;
            if !self.keys.is_empty() {
                self.key_selected = 0;
                self.key_list_state.select(Some(0));
            }
        }
    }

    /// Fetch detailed info for the currently selected key.
    async fn get_key_detail(&mut self) {
        if let Some(key_info) = self.keys.get(self.key_selected) {
            let name = key_info.name.clone();
            if let Ok(detail) = self.collector.get_key_detail(&name).await {
                self.key_detail = Some(detail);
            }
        }
    }

    /// Delete the currently selected key.
    async fn delete_key(&mut self) {
        let key_name = if let Some(detail) = &self.key_detail {
            Some(detail.key.clone())
        } else {
            self.keys.get(self.key_selected).map(|k| k.name.clone())
        };

        if let Some(key) = key_name {
            if self.collector.delete_key(&key).await.is_ok() {
                self.key_detail = None;
                self.scan_keys().await;
            }
        }
    }

    /// Handle a key press event. Returns true if an async operation is needed.
    fn handle_key(&mut self, key: KeyCode) -> AsyncAction {
        // Handle input modes first
        match self.input_mode {
            InputMode::Command => return self.handle_command_input(key),
            InputMode::Search => return self.handle_search_input(key),
            InputMode::Normal => {}
        }

        // Global keys (normal mode only)
        match key {
            KeyCode::Char('q') => {
                self.should_quit = true;
                return AsyncAction::None;
            }
            KeyCode::Char('1') => {
                self.current_tab = Tab::Overview;
                return AsyncAction::None;
            }
            KeyCode::Char('2') => {
                self.current_tab = Tab::Keys;
                return AsyncAction::ScanKeys;
            }
            KeyCode::Char('3') => {
                self.current_tab = Tab::Commands;
                return AsyncAction::None;
            }
            KeyCode::Char('4') => {
                self.current_tab = Tab::Memory;
                return AsyncAction::None;
            }
            KeyCode::Char('5') => {
                self.current_tab = Tab::Network;
                return AsyncAction::None;
            }
            KeyCode::Char('6') => {
                self.current_tab = Tab::Logs;
                return AsyncAction::None;
            }
            KeyCode::Tab => {
                self.current_tab = self.current_tab.next();
                if self.current_tab == Tab::Keys {
                    return AsyncAction::ScanKeys;
                }
                return AsyncAction::None;
            }
            KeyCode::Char(':') => {
                self.input_mode = InputMode::Command;
                self.command_input.clear();
                return AsyncAction::None;
            }
            KeyCode::Char('/') => {
                self.input_mode = InputMode::Search;
                self.command_input.clear();
                return AsyncAction::None;
            }
            KeyCode::Char('p') => {
                self.paused = !self.paused;
                return AsyncAction::None;
            }
            _ => {}
        }

        // Tab-specific keys
        match self.current_tab {
            Tab::Overview => self.handle_overview_key(key),
            Tab::Keys => self.handle_keys_key(key),
            Tab::Commands => AsyncAction::None,
            Tab::Memory => AsyncAction::None,
            Tab::Network => self.handle_network_key(key),
            Tab::Logs => self.handle_logs_key(key),
        }
    }

    fn handle_overview_key(&mut self, key: KeyCode) -> AsyncAction {
        if key == KeyCode::Char('r') {
            self.paused = false
        }
        AsyncAction::None
    }

    fn handle_keys_key(&mut self, key: KeyCode) -> AsyncAction {
        // Handle delete confirmation
        if self.key_delete_confirm {
            match key {
                KeyCode::Char('y') | KeyCode::Char('Y') => {
                    self.key_delete_confirm = false;
                    return AsyncAction::DeleteKey;
                }
                KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
                    self.key_delete_confirm = false;
                }
                _ => {}
            }
            return AsyncAction::None;
        }

        match key {
            KeyCode::Up | KeyCode::Char('k') => {
                if self.key_selected > 0 {
                    self.key_selected -= 1;
                    self.key_list_state.select(Some(self.key_selected));
                }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if self.key_selected < self.keys.len().saturating_sub(1) {
                    self.key_selected += 1;
                    self.key_list_state.select(Some(self.key_selected));
                }
            }
            KeyCode::Enter => {
                return AsyncAction::GetKeyDetail;
            }
            KeyCode::Char('n') => {
                return AsyncAction::ScanKeys;
            }
            KeyCode::Char('r') => {
                self.key_cursor = 0;
                return AsyncAction::ScanKeys;
            }
            KeyCode::Char('d') => {
                if self.key_detail.is_some() || !self.keys.is_empty() {
                    self.key_delete_confirm = true;
                }
            }
            KeyCode::Char('s') => {
                self.input_mode = InputMode::Search;
                self.command_input.clear();
            }
            KeyCode::Esc => {
                self.key_detail = None;
            }
            _ => {}
        }
        AsyncAction::None
    }

    fn handle_network_key(&mut self, key: KeyCode) -> AsyncAction {
        match key {
            KeyCode::Up | KeyCode::Char('k') => {
                let current = self.client_list_state.selected().unwrap_or(0);
                if current > 0 {
                    self.client_list_state.select(Some(current - 1));
                }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                let current = self.client_list_state.selected().unwrap_or(0);
                let max = self.collector.clients.len().saturating_sub(1);
                if current < max {
                    self.client_list_state.select(Some(current + 1));
                }
            }
            _ => {}
        }
        AsyncAction::None
    }

    fn handle_logs_key(&mut self, key: KeyCode) -> AsyncAction {
        match key {
            KeyCode::Char('1') => self.log_min_level = LogLevel::Trace,
            KeyCode::Char('2') => self.log_min_level = LogLevel::Debug,
            KeyCode::Char('3') => self.log_min_level = LogLevel::Info,
            KeyCode::Char('4') => self.log_min_level = LogLevel::Warning,
            KeyCode::Char('5') => self.log_min_level = LogLevel::Error,
            KeyCode::Char('c') => {
                self.collector.logs.clear();
                self.log_scroll_offset = 0;
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.log_scroll_offset = self.log_scroll_offset.saturating_add(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.log_scroll_offset = self.log_scroll_offset.saturating_sub(1);
            }
            _ => {}
        }
        AsyncAction::None
    }

    fn handle_command_input(&mut self, key: KeyCode) -> AsyncAction {
        match key {
            KeyCode::Enter => {
                let _cmd = self.command_input.clone();
                self.input_mode = InputMode::Normal;
                self.command_input.clear();
                // Commands would be sent to the server here
                AsyncAction::None
            }
            KeyCode::Esc => {
                self.input_mode = InputMode::Normal;
                self.command_input.clear();
                AsyncAction::None
            }
            KeyCode::Backspace => {
                self.command_input.pop();
                AsyncAction::None
            }
            KeyCode::Char(c) => {
                self.command_input.push(c);
                AsyncAction::None
            }
            _ => AsyncAction::None,
        }
    }

    fn handle_search_input(&mut self, key: KeyCode) -> AsyncAction {
        match key {
            KeyCode::Enter => {
                if self.current_tab == Tab::Keys {
                    self.key_pattern = if self.command_input.is_empty() {
                        "*".to_string()
                    } else {
                        self.command_input.clone()
                    };
                    self.key_cursor = 0;
                }
                self.input_mode = InputMode::Normal;
                self.command_input.clear();
                if self.current_tab == Tab::Keys {
                    return AsyncAction::ScanKeys;
                }
                AsyncAction::None
            }
            KeyCode::Esc => {
                self.input_mode = InputMode::Normal;
                self.command_input.clear();
                AsyncAction::None
            }
            KeyCode::Backspace => {
                self.command_input.pop();
                AsyncAction::None
            }
            KeyCode::Char(c) => {
                self.command_input.push(c);
                AsyncAction::None
            }
            _ => AsyncAction::None,
        }
    }
}

/// Actions that require async execution after key handling.
#[derive(Debug, PartialEq, Eq)]
enum AsyncAction {
    None,
    ScanKeys,
    GetKeyDetail,
    DeleteKey,
}

/// Main entry point for the TUI dashboard.
///
/// Connects to the Ferrite server at the given address, sets up the terminal,
/// and runs the event loop until the user quits.
///
/// # Arguments
///
/// * `host` - Server hostname (e.g. "127.0.0.1")
/// * `port` - Server port (e.g. 6379)
/// * `password` - Optional authentication password
/// * `interval_ms` - Refresh interval in milliseconds (default 1000)
pub async fn run(
    host: &str,
    port: u16,
    password: Option<&str>,
    interval_ms: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    // Connect to server
    let collector = DataCollector::new(host, port, password).await?;

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new(collector);
    let tick_rate = Duration::from_millis(interval_ms);

    // Main event loop
    loop {
        terminal.draw(|f| ui::draw(f, &mut app))?;

        if event::poll(tick_rate)? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    let action = app.handle_key(key.code);
                    match action {
                        AsyncAction::ScanKeys => app.scan_keys().await,
                        AsyncAction::GetKeyDetail => app.get_key_detail().await,
                        AsyncAction::DeleteKey => app.delete_key().await,
                        AsyncAction::None => {}
                    }
                }
            }
        }

        if app.should_quit {
            break;
        }

        app.update().await;
    }

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    Ok(())
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tab_all() {
        let tabs = Tab::all();
        assert_eq!(tabs.len(), 6);
        assert_eq!(tabs[0], Tab::Overview);
        assert_eq!(tabs[1], Tab::Keys);
        assert_eq!(tabs[2], Tab::Commands);
        assert_eq!(tabs[3], Tab::Memory);
        assert_eq!(tabs[4], Tab::Network);
        assert_eq!(tabs[5], Tab::Logs);
    }

    #[test]
    fn test_tab_names() {
        assert_eq!(Tab::Overview.name(), "Overview");
        assert_eq!(Tab::Keys.name(), "Keys");
        assert_eq!(Tab::Commands.name(), "Commands");
        assert_eq!(Tab::Memory.name(), "Memory");
        assert_eq!(Tab::Network.name(), "Network");
        assert_eq!(Tab::Logs.name(), "Logs");
    }

    #[test]
    fn test_tab_indices() {
        assert_eq!(Tab::Overview.index(), 0);
        assert_eq!(Tab::Keys.index(), 1);
        assert_eq!(Tab::Commands.index(), 2);
        assert_eq!(Tab::Memory.index(), 3);
        assert_eq!(Tab::Network.index(), 4);
        assert_eq!(Tab::Logs.index(), 5);
    }

    #[test]
    fn test_tab_next_wraps() {
        assert_eq!(Tab::Overview.next(), Tab::Keys);
        assert_eq!(Tab::Keys.next(), Tab::Commands);
        assert_eq!(Tab::Commands.next(), Tab::Memory);
        assert_eq!(Tab::Memory.next(), Tab::Network);
        assert_eq!(Tab::Network.next(), Tab::Logs);
        assert_eq!(Tab::Logs.next(), Tab::Overview);
    }

    #[test]
    fn test_input_mode_equality() {
        assert_eq!(InputMode::Normal, InputMode::Normal);
        assert_ne!(InputMode::Normal, InputMode::Command);
        assert_ne!(InputMode::Command, InputMode::Search);
    }
}
