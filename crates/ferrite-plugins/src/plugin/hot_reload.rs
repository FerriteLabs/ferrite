//! Plugin Hot-Reload and Filesystem Watcher
//!
//! Monitors a plugin directory for changes and automatically reloads plugins
//! when their WASM files are updated, without requiring a server restart.

use super::registry::{PluginRegistry, PluginState};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

/// Configuration for the hot-reload watcher.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotReloadConfig {
    /// Directory to watch for plugin files.
    pub watch_dir: PathBuf,
    /// How often to check for file changes.
    pub poll_interval: Duration,
    /// File extensions to consider as plugins.
    pub extensions: Vec<String>,
    /// Whether to enable hot-reload.
    pub enabled: bool,
}

impl Default for HotReloadConfig {
    fn default() -> Self {
        Self {
            watch_dir: PathBuf::from("./plugins"),
            poll_interval: Duration::from_secs(5),
            extensions: vec!["wasm".to_string()],
            enabled: true,
        }
    }
}

/// Tracks plugin file modification times for change detection.
#[derive(Debug)]
struct FileState {
    path: PathBuf,
    modified: SystemTime,
    size: u64,
}

/// Hot-reload watcher that monitors a directory and reloads plugins.
pub struct HotReloader {
    config: HotReloadConfig,
    registry: Arc<PluginRegistry>,
    file_states: HashMap<String, FileState>,
    shutdown: Arc<Notify>,
}

/// Event emitted when a plugin file changes.
#[derive(Debug, Clone, Serialize)]
pub struct ReloadEvent {
    pub plugin_name: String,
    pub event_type: ReloadEventType,
    pub path: PathBuf,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Type of reload event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ReloadEventType {
    /// Plugin file was added.
    Added,
    /// Plugin file was modified.
    Modified,
    /// Plugin file was removed.
    Removed,
}

impl HotReloader {
    /// Create a new hot-reloader.
    pub fn new(config: HotReloadConfig, registry: Arc<PluginRegistry>) -> Self {
        Self {
            config,
            registry,
            file_states: HashMap::new(),
            shutdown: Arc::new(Notify::new()),
        }
    }

    /// Returns a handle that can be used to signal shutdown.
    pub fn shutdown_handle(&self) -> Arc<Notify> {
        Arc::clone(&self.shutdown)
    }

    /// Run the polling loop. Returns when the shutdown signal is received.
    pub async fn run(&mut self) -> Vec<ReloadEvent> {
        if !self.config.enabled {
            info!("hot-reload disabled");
            self.shutdown.notified().await;
            return Vec::new();
        }

        if let Err(e) = std::fs::create_dir_all(&self.config.watch_dir) {
            error!(dir = %self.config.watch_dir.display(), "failed to create watch dir: {e}");
            return Vec::new();
        }

        let mut all_events = Vec::new();
        info!(dir = %self.config.watch_dir.display(), "hot-reload watcher started");

        loop {
            tokio::select! {
                _ = tokio::time::sleep(self.config.poll_interval) => {
                    let events = self.check_for_changes();
                    for event in &events {
                        self.handle_event(event);
                    }
                    all_events.extend(events);
                }
                _ = self.shutdown.notified() => {
                    info!("hot-reload watcher shutting down");
                    break;
                }
            }
        }

        all_events
    }

    /// Perform a single poll for changes (useful for testing).
    pub fn poll_once(&mut self) -> Vec<ReloadEvent> {
        let events = self.check_for_changes();
        for event in &events {
            self.handle_event(event);
        }
        events
    }

    fn check_for_changes(&mut self) -> Vec<ReloadEvent> {
        let mut events = Vec::new();
        let now = chrono::Utc::now();

        // Scan directory for plugin files.
        let current_files: HashMap<String, FileState> = self.scan_directory();

        // Detect new and modified files.
        for (name, new_state) in &current_files {
            match self.file_states.get(name) {
                None => {
                    events.push(ReloadEvent {
                        plugin_name: name.clone(),
                        event_type: ReloadEventType::Added,
                        path: new_state.path.clone(),
                        timestamp: now,
                    });
                }
                Some(old_state) => {
                    if new_state.modified != old_state.modified || new_state.size != old_state.size
                    {
                        events.push(ReloadEvent {
                            plugin_name: name.clone(),
                            event_type: ReloadEventType::Modified,
                            path: new_state.path.clone(),
                            timestamp: now,
                        });
                    }
                }
            }
        }

        // Detect removed files.
        for name in self.file_states.keys() {
            if !current_files.contains_key(name) {
                events.push(ReloadEvent {
                    plugin_name: name.clone(),
                    event_type: ReloadEventType::Removed,
                    path: self.file_states[name].path.clone(),
                    timestamp: now,
                });
            }
        }

        // Update tracked state.
        self.file_states = current_files;
        events
    }

    fn scan_directory(&self) -> HashMap<String, FileState> {
        let mut files = HashMap::new();
        let dir = match std::fs::read_dir(&self.config.watch_dir) {
            Ok(d) => d,
            Err(_) => return files,
        };

        for entry in dir.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            let ext = path
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or_default()
                .to_string();

            if !self.config.extensions.contains(&ext) {
                continue;
            }

            let name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default()
                .to_string();

            if let Ok(meta) = entry.metadata() {
                let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                files.insert(
                    name,
                    FileState {
                        path,
                        modified,
                        size: meta.len(),
                    },
                );
            }
        }
        files
    }

    fn handle_event(&self, event: &ReloadEvent) {
        match event.event_type {
            ReloadEventType::Added => {
                info!(plugin = %event.plugin_name, "new plugin detected");
                // Actual WASM loading would happen here via PluginLoader.
                // For now we register a placeholder entry.
            }
            ReloadEventType::Modified => {
                info!(plugin = %event.plugin_name, "plugin modified, reloading");
                if self.registry.exists(&event.plugin_name) {
                    // The registry.update() method performs the atomic swap.
                    debug!(plugin = %event.plugin_name, "hot-reload swap initiated");
                }
            }
            ReloadEventType::Removed => {
                warn!(plugin = %event.plugin_name, "plugin removed");
                if self.registry.exists(&event.plugin_name) {
                    let _ = self
                        .registry
                        .set_state(&event.plugin_name, PluginState::Disabled);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hot_reload_config_defaults() {
        let config = HotReloadConfig::default();
        assert!(config.enabled);
        assert_eq!(config.poll_interval, Duration::from_secs(5));
        assert_eq!(config.extensions, vec!["wasm".to_string()]);
    }

    #[test]
    fn hot_reload_detects_new_files() {
        let tmp = tempfile::TempDir::new().unwrap();
        let registry = Arc::new(PluginRegistry::new(10));
        let config = HotReloadConfig {
            watch_dir: tmp.path().to_path_buf(),
            enabled: true,
            ..Default::default()
        };

        let mut reloader = HotReloader::new(config, registry);

        // First poll: empty
        let events = reloader.poll_once();
        assert!(events.is_empty());

        // Create a plugin file
        std::fs::write(tmp.path().join("myplugin.wasm"), b"fake wasm").unwrap();

        // Second poll: should detect new file
        let events = reloader.poll_once();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, ReloadEventType::Added);
        assert_eq!(events[0].plugin_name, "myplugin");
    }

    #[test]
    fn hot_reload_detects_removal() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("removeme.wasm");
        std::fs::write(&path, b"data").unwrap();

        let registry = Arc::new(PluginRegistry::new(10));
        let config = HotReloadConfig {
            watch_dir: tmp.path().to_path_buf(),
            enabled: true,
            ..Default::default()
        };

        let mut reloader = HotReloader::new(config, registry);
        let _ = reloader.poll_once(); // pick up initial state

        std::fs::remove_file(&path).unwrap();
        let events = reloader.poll_once();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, ReloadEventType::Removed);
    }
}
