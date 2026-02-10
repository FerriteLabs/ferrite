//! Client registry for tracking connected clients
//!
//! This module provides a registry for tracking all connected clients,
//! enabling CLIENT LIST and other client management commands.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// Pause mode for CLIENT PAUSE command
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PauseMode {
    /// Pause all commands
    All,
    /// Pause only write commands
    Write,
}

/// Pause state for CLIENT PAUSE/UNPAUSE
#[derive(Debug)]
struct PauseState {
    /// Whether clients are currently paused
    paused: bool,
    /// When the pause will expire
    pause_until: Instant,
    /// Mode of pause (all or write-only)
    mode: PauseMode,
}

impl Default for PauseState {
    fn default() -> Self {
        Self {
            paused: false,
            pause_until: Instant::now(),
            mode: PauseMode::All,
        }
    }
}

/// Information about a connected client
#[derive(Debug, Clone)]
pub struct ClientInfo {
    /// Unique client ID
    pub id: u64,
    /// Client remote address
    pub addr: Option<SocketAddr>,
    /// Local address the client connected to
    pub laddr: Option<SocketAddr>,
    /// Client name (set via CLIENT SETNAME)
    pub name: Option<String>,
    /// Time when the client connected
    pub connected_at: Instant,
    /// Time of last command
    pub last_interaction: Instant,
    /// Currently selected database
    pub db: u8,
    /// Client flags (N=normal, M=master, S=slave, etc.)
    pub flags: String,
    /// Last command executed
    pub last_cmd: String,
    /// Number of subscriptions
    pub subscriptions: u32,
    /// Number of pattern subscriptions
    pub psubscriptions: u32,
    /// Whether in multi/exec transaction
    pub multi: i32,
    /// Output buffer length
    pub obl: u64,
    /// Output list length
    pub oll: u64,
    /// Total memory used by output buffers
    pub omem: u64,
    /// Authenticated user
    pub user: String,
}

impl ClientInfo {
    /// Create a new client info entry
    pub fn new(id: u64, addr: Option<SocketAddr>) -> Self {
        let now = Instant::now();
        Self {
            id,
            addr,
            laddr: None,
            name: None,
            connected_at: now,
            last_interaction: now,
            db: 0,
            flags: "N".to_string(),
            last_cmd: String::new(),
            subscriptions: 0,
            psubscriptions: 0,
            multi: -1,
            obl: 0,
            oll: 0,
            omem: 0,
            user: "default".to_string(),
        }
    }

    /// Get client age in seconds
    pub fn age(&self) -> u64 {
        self.connected_at.elapsed().as_secs()
    }

    /// Get idle time in seconds
    pub fn idle(&self) -> u64 {
        self.last_interaction.elapsed().as_secs()
    }

    /// Format as CLIENT LIST output line
    pub fn to_info_string(&self) -> String {
        let addr = self.addr.map(|a| a.to_string()).unwrap_or_default();
        let laddr = self.laddr.map(|a| a.to_string()).unwrap_or_default();
        let name = self.name.as_deref().unwrap_or("");

        format!(
            "id={} addr={} laddr={} fd=0 name={} age={} idle={} flags={} db={} sub={} psub={} multi={} qbuf=0 qbuf-free=0 obl={} oll={} omem={} events=r cmd={} user={}",
            self.id,
            addr,
            laddr,
            name,
            self.age(),
            self.idle(),
            self.flags,
            self.db,
            self.subscriptions,
            self.psubscriptions,
            self.multi,
            self.obl,
            self.oll,
            self.omem,
            self.last_cmd,
            self.user,
        )
    }
}

/// Registry for tracking connected clients
pub struct ClientRegistry {
    /// Map of client ID to client info
    clients: RwLock<HashMap<u64, ClientInfo>>,
    /// Next client ID
    next_id: AtomicU64,
    /// Pause state for CLIENT PAUSE/UNPAUSE
    pause_state: RwLock<PauseState>,
}

impl ClientRegistry {
    /// Create a new client registry
    pub fn new() -> Self {
        Self {
            clients: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            pause_state: RwLock::new(PauseState::default()),
        }
    }

    /// Pause all clients for the given duration
    pub fn pause(&self, timeout_ms: u64, mode: PauseMode) {
        let mut state = self.pause_state.write().unwrap_or_else(|e| e.into_inner());
        state.paused = true;
        state.pause_until = Instant::now() + Duration::from_millis(timeout_ms);
        state.mode = mode;
    }

    /// Unpause all clients
    pub fn unpause(&self) {
        let mut state = self.pause_state.write().unwrap_or_else(|e| e.into_inner());
        state.paused = false;
    }

    /// Check if clients are currently paused
    pub fn is_paused(&self) -> bool {
        let state = self.pause_state.read().unwrap_or_else(|e| e.into_inner());
        if state.paused {
            // Check if pause has expired
            if Instant::now() >= state.pause_until {
                drop(state);
                self.unpause();
                false
            } else {
                true
            }
        } else {
            false
        }
    }

    /// Check if write commands are paused
    pub fn is_write_paused(&self) -> bool {
        let state = self.pause_state.read().unwrap_or_else(|e| e.into_inner());
        if state.paused {
            if Instant::now() >= state.pause_until {
                drop(state);
                self.unpause();
                false
            } else {
                // All pauses affect writes
                true
            }
        } else {
            false
        }
    }

    /// Get time remaining in pause (milliseconds), 0 if not paused
    pub fn pause_remaining_ms(&self) -> u64 {
        let state = self.pause_state.read().unwrap_or_else(|e| e.into_inner());
        if state.paused {
            let remaining = state.pause_until.saturating_duration_since(Instant::now());
            remaining.as_millis() as u64
        } else {
            0
        }
    }

    /// Get the current pause mode
    pub fn pause_mode(&self) -> Option<PauseMode> {
        let state = self.pause_state.read().unwrap_or_else(|e| e.into_inner());
        if state.paused && Instant::now() < state.pause_until {
            Some(state.mode)
        } else {
            None
        }
    }

    /// Register a new client and return its ID
    pub fn register(&self, addr: Option<SocketAddr>) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let info = ClientInfo::new(id, addr);

        self.clients
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, info);
        id
    }

    /// Unregister a client
    pub fn unregister(&self, id: u64) {
        self.clients
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id);
    }

    /// Update client info
    pub fn update<F>(&self, id: u64, f: F)
    where
        F: FnOnce(&mut ClientInfo),
    {
        if let Some(client) = self
            .clients
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(&id)
        {
            f(client);
        }
    }

    /// Get a snapshot of a client's info
    pub fn get(&self, id: u64) -> Option<ClientInfo> {
        self.clients
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
            .cloned()
    }

    /// Get all clients
    pub fn list(&self) -> Vec<ClientInfo> {
        self.clients
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect()
    }

    /// Get the number of connected clients
    pub fn count(&self) -> usize {
        self.clients.read().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Find clients matching criteria
    pub fn find_by_addr(&self, addr: &str) -> Vec<ClientInfo> {
        self.clients
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|c| c.addr.map(|a| a.to_string()).as_deref() == Some(addr))
            .cloned()
            .collect()
    }

    /// Find clients by user
    pub fn find_by_user(&self, user: &str) -> Vec<ClientInfo> {
        self.clients
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|c| c.user == user)
            .cloned()
            .collect()
    }

    /// Kill a client by ID
    pub fn kill(&self, id: u64) -> bool {
        self.clients
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id)
            .is_some()
    }

    /// Get CLIENT LIST formatted output
    pub fn client_list(&self) -> String {
        self.clients
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .map(|c| c.to_info_string())
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl Default for ClientRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared client registry type
pub type SharedClientRegistry = std::sync::Arc<ClientRegistry>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_registration() {
        let registry = ClientRegistry::new();

        let id1 = registry.register(Some("127.0.0.1:12345".parse().unwrap()));
        let id2 = registry.register(Some("127.0.0.1:12346".parse().unwrap()));

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(registry.count(), 2);
    }

    #[test]
    fn test_client_unregistration() {
        let registry = ClientRegistry::new();

        let id = registry.register(Some("127.0.0.1:12345".parse().unwrap()));
        assert_eq!(registry.count(), 1);

        registry.unregister(id);
        assert_eq!(registry.count(), 0);
    }

    #[test]
    fn test_client_update() {
        let registry = ClientRegistry::new();

        let id = registry.register(Some("127.0.0.1:12345".parse().unwrap()));

        registry.update(id, |c| {
            c.name = Some("test-client".to_string());
            c.db = 5;
            c.last_cmd = "SET".to_string();
        });

        let client = registry.get(id).unwrap();
        assert_eq!(client.name, Some("test-client".to_string()));
        assert_eq!(client.db, 5);
        assert_eq!(client.last_cmd, "SET");
    }

    #[test]
    fn test_client_list_output() {
        let registry = ClientRegistry::new();

        let id = registry.register(Some("127.0.0.1:12345".parse().unwrap()));
        registry.update(id, |c| {
            c.name = Some("myclient".to_string());
            c.last_cmd = "GET".to_string();
        });

        let output = registry.client_list();
        assert!(output.contains("id=1"));
        assert!(output.contains("127.0.0.1:12345"));
        assert!(output.contains("name=myclient"));
        assert!(output.contains("cmd=GET"));
    }

    #[test]
    fn test_client_pause_unpause() {
        let registry = ClientRegistry::new();

        // Initially not paused
        assert!(!registry.is_paused());

        // Pause for 1000ms
        registry.pause(1000, PauseMode::All);
        assert!(registry.is_paused());
        assert_eq!(registry.pause_mode(), Some(PauseMode::All));

        // Remaining time should be > 0
        assert!(registry.pause_remaining_ms() > 0);

        // Unpause
        registry.unpause();
        assert!(!registry.is_paused());
        assert_eq!(registry.pause_mode(), None);
    }

    #[test]
    fn test_client_pause_write_mode() {
        let registry = ClientRegistry::new();

        // Pause with WRITE mode
        registry.pause(1000, PauseMode::Write);
        assert!(registry.is_paused());
        assert!(registry.is_write_paused());
        assert_eq!(registry.pause_mode(), Some(PauseMode::Write));
    }

    #[test]
    fn test_client_pause_expiry() {
        let registry = ClientRegistry::new();

        // Pause for 10ms
        registry.pause(10, PauseMode::All);
        assert!(registry.is_paused());

        // Wait for pause to expire
        std::thread::sleep(Duration::from_millis(20));

        // Should no longer be paused
        assert!(!registry.is_paused());
    }
}
