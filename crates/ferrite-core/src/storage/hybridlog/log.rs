//! HybridLog - Unified three-tier storage engine
//!
//! This module combines the mutable, read-only, and disk regions into
//! a unified storage engine with automatic tiering.
//!
//! Data flows through tiers based on access patterns:
//! - Hot data lives in the mutable region (fast writes, lock-free reads)
//! - Warm data moves to read-only region (zero-copy reads via mmap)
//! - Cold data moves to disk region (async I/O)

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime};

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use tracing::{debug, info, warn};

use super::address::{LogAddress, Region};
use super::disk::DiskRegion;
use super::mutable::MutableRegion;
use super::readonly::ReadOnlyRegion;

/// Configuration for HybridLog
#[derive(Debug, Clone)]
pub struct HybridLogConfig {
    /// Size of the mutable region in bytes
    pub mutable_size: usize,
    /// Size of the read-only region in bytes
    pub readonly_size: usize,
    /// Size of the disk region (0 = unlimited)
    pub disk_size: usize,
    /// Directory for persistent storage
    pub data_dir: PathBuf,
    /// Whether to enable automatic tiering
    pub auto_tiering: bool,
    /// Migration threshold (0.0-1.0) - trigger migration when region is this full
    pub migration_threshold: f64,
}

impl Default for HybridLogConfig {
    fn default() -> Self {
        Self {
            mutable_size: 64 * 1024 * 1024,   // 64MB
            readonly_size: 256 * 1024 * 1024, // 256MB
            disk_size: 0,                     // Unlimited
            data_dir: PathBuf::from("./data"),
            auto_tiering: true,
            migration_threshold: 0.8,
        }
    }
}

/// Entry metadata stored in the index
#[derive(Debug, Clone)]
struct IndexEntry {
    /// Address where the value is stored
    address: LogAddress,
    /// Access count for tiering decisions
    access_count: u64,
    /// Last access timestamp
    last_access: Instant,
    /// Optional expiration time
    expires_at: Option<SystemTime>,
}

impl IndexEntry {
    fn new(address: LogAddress) -> Self {
        Self {
            address,
            access_count: 0,
            last_access: Instant::now(),
            expires_at: None,
        }
    }

    fn with_expiry(address: LogAddress, expires_at: SystemTime) -> Self {
        Self {
            address,
            access_count: 0,
            last_access: Instant::now(),
            expires_at: Some(expires_at),
        }
    }

    fn is_expired(&self) -> bool {
        self.expires_at
            .map(|exp| SystemTime::now() >= exp)
            .unwrap_or(false)
    }

    fn record_access(&mut self) {
        self.access_count += 1;
        self.last_access = Instant::now();
    }
}

/// Statistics for HybridLog
#[derive(Debug, Clone, Default)]
pub struct HybridLogStats {
    /// Total number of keys
    pub key_count: u64,
    /// Total bytes in mutable region
    pub mutable_bytes: usize,
    /// Total bytes in read-only region
    pub readonly_bytes: usize,
    /// Total bytes in disk region
    pub disk_bytes: usize,
    /// Number of entries in mutable region
    pub mutable_entries: u64,
    /// Number of entries in read-only region
    pub readonly_entries: u64,
    /// Number of entries in disk region
    pub disk_entries: u64,
    /// Total GET operations
    pub get_ops: u64,
    /// Total SET operations
    pub set_ops: u64,
    /// Total DEL operations
    pub del_ops: u64,
    /// Number of migrations from mutable to read-only
    pub mutable_to_readonly_migrations: u64,
    /// Number of migrations from read-only to disk
    pub readonly_to_disk_migrations: u64,
    /// Number of background migrations completed
    pub background_migrations: u64,
    /// Number of background compactions completed
    pub background_compactions: u64,
    /// Total bytes reclaimed by compaction
    pub compaction_bytes_reclaimed: u64,
    /// Whether background tasks are running
    pub background_tasks_active: bool,
}

/// The HybridLog - three-tier storage engine
pub struct HybridLog {
    /// Index mapping keys to their locations
    index: DashMap<Bytes, IndexEntry>,
    /// Hot tier - in-memory mutable region
    mutable: MutableRegion,
    /// Warm tier - memory-mapped read-only region
    readonly: RwLock<ReadOnlyRegion>,
    /// Cold tier - disk-backed region
    disk: RwLock<DiskRegion>,
    /// Configuration
    config: HybridLogConfig,
    /// Operation counters
    get_count: AtomicU64,
    set_count: AtomicU64,
    del_count: AtomicU64,
    /// Migration counters
    mutable_to_readonly_count: AtomicU64,
    readonly_to_disk_count: AtomicU64,
    /// Background task counters
    background_migration_count: AtomicU64,
    background_compaction_count: AtomicU64,
    compaction_bytes_reclaimed: AtomicU64,
    /// Flag indicating if background tasks are active
    background_tasks_active: std::sync::atomic::AtomicBool,
}

impl HybridLog {
    /// Create a new HybridLog with the given configuration
    pub fn new(config: HybridLogConfig) -> io::Result<Self> {
        // Ensure data directory exists
        std::fs::create_dir_all(&config.data_dir)?;

        let readonly_path = config.data_dir.join("readonly.dat");
        let disk_path = config.data_dir.join("disk.dat");

        let mutable = MutableRegion::new(config.mutable_size);
        let readonly = ReadOnlyRegion::new(&readonly_path, config.readonly_size)?;
        let disk = DiskRegion::new(&disk_path, config.disk_size)?;

        info!(
            "Created HybridLog: mutable={}MB, readonly={}MB, disk={}",
            config.mutable_size / (1024 * 1024),
            config.readonly_size / (1024 * 1024),
            if config.disk_size == 0 {
                "unlimited".to_string()
            } else {
                format!("{}MB", config.disk_size / (1024 * 1024))
            }
        );

        Ok(Self {
            index: DashMap::new(),
            mutable,
            readonly: RwLock::new(readonly),
            disk: RwLock::new(disk),
            config,
            get_count: AtomicU64::new(0),
            set_count: AtomicU64::new(0),
            del_count: AtomicU64::new(0),
            mutable_to_readonly_count: AtomicU64::new(0),
            readonly_to_disk_count: AtomicU64::new(0),
            background_migration_count: AtomicU64::new(0),
            background_compaction_count: AtomicU64::new(0),
            compaction_bytes_reclaimed: AtomicU64::new(0),
            background_tasks_active: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Create a new HybridLog with default configuration
    pub fn with_defaults(data_dir: impl AsRef<Path>) -> io::Result<Self> {
        let config = HybridLogConfig {
            data_dir: data_dir.as_ref().to_path_buf(),
            ..Default::default()
        };
        Self::new(config)
    }

    /// Open an existing HybridLog
    pub fn open(config: HybridLogConfig) -> io::Result<Self> {
        let readonly_path = config.data_dir.join("readonly.dat");
        let disk_path = config.data_dir.join("disk.dat");

        let mutable = MutableRegion::new(config.mutable_size);

        let readonly = if readonly_path.exists() {
            ReadOnlyRegion::open(&readonly_path, config.readonly_size)?
        } else {
            ReadOnlyRegion::new(&readonly_path, config.readonly_size)?
        };

        let disk = if disk_path.exists() {
            DiskRegion::open(&disk_path, config.disk_size)?
        } else {
            DiskRegion::new(&disk_path, config.disk_size)?
        };

        let log = Self {
            index: DashMap::new(),
            mutable,
            readonly: RwLock::new(readonly),
            disk: RwLock::new(disk),
            config,
            get_count: AtomicU64::new(0),
            set_count: AtomicU64::new(0),
            del_count: AtomicU64::new(0),
            mutable_to_readonly_count: AtomicU64::new(0),
            readonly_to_disk_count: AtomicU64::new(0),
            background_migration_count: AtomicU64::new(0),
            background_compaction_count: AtomicU64::new(0),
            compaction_bytes_reclaimed: AtomicU64::new(0),
            background_tasks_active: std::sync::atomic::AtomicBool::new(false),
        };

        // Rebuild index from persistent storage
        log.rebuild_index()?;

        Ok(log)
    }

    /// Rebuild the index from persistent storage
    fn rebuild_index(&self) -> io::Result<()> {
        // Rebuild from read-only region
        {
            let readonly = self.readonly.read();
            readonly.iter(|offset, key, _value| {
                let key = Bytes::copy_from_slice(key);
                let address = LogAddress::read_only(offset);
                self.index.insert(key, IndexEntry::new(address));
            })?;
        }

        // Rebuild from disk region (overwrites read-only entries if duplicate)
        {
            let disk = self.disk.read();
            disk.iter(|offset, key, _value| {
                let key = Bytes::copy_from_slice(key);
                let address = LogAddress::disk(offset);
                self.index.insert(key, IndexEntry::new(address));
            })?;
        }

        info!("Rebuilt index with {} keys", self.index.len());

        Ok(())
    }

    /// Get a value by key
    pub fn get(&self, key: &Bytes) -> Option<Bytes> {
        self.get_count.fetch_add(1, Ordering::Relaxed);

        let mut entry = self.index.get_mut(key)?;

        // Check expiration
        if entry.is_expired() {
            drop(entry);
            self.index.remove(key);
            return None;
        }

        entry.record_access();
        let address = entry.address;
        drop(entry);

        self.read_value(address)
    }

    /// Read a value from the appropriate region
    fn read_value(&self, address: LogAddress) -> Option<Bytes> {
        match address.region() {
            Region::Mutable => self.mutable.read_value(address.offset()),
            Region::ReadOnly => {
                let readonly = self.readonly.read();
                readonly.read_value(address.offset())
            }
            Region::Disk => {
                // For sync access, we need to block on async read
                // In production, you'd want to use read_async
                let disk = self.disk.read();
                disk.read_sync(address.offset())
                    .ok()
                    .flatten()
                    .map(|(_, v)| v)
            }
            Region::Invalid => None,
        }
    }

    /// Set a value
    pub fn set(&self, key: Bytes, value: Bytes) -> io::Result<()> {
        self.set_count.fetch_add(1, Ordering::Relaxed);

        // Try to append to mutable region
        match self.mutable.try_append(&key, &value) {
            Some(address) => {
                self.index.insert(key, IndexEntry::new(address));
                Ok(())
            }
            None => {
                // Mutable region is full, trigger migration
                if self.config.auto_tiering {
                    self.migrate_mutable_to_readonly()?;

                    // Retry append
                    if let Some(address) = self.mutable.try_append(&key, &value) {
                        self.index.insert(key, IndexEntry::new(address));
                        Ok(())
                    } else {
                        // Still no space - shouldn't happen after migration
                        Err(io::Error::other("Failed to append after migration"))
                    }
                } else {
                    Err(io::Error::other(
                        "Mutable region full and auto-tiering disabled",
                    ))
                }
            }
        }
    }

    /// Set a value with expiration
    pub fn set_with_expiry(&self, key: Bytes, value: Bytes, expires_at: SystemTime) -> io::Result<()> {
        self.set_count.fetch_add(1, Ordering::Relaxed);

        match self.mutable.try_append(&key, &value) {
            Some(address) => {
                self.index
                    .insert(key, IndexEntry::with_expiry(address, expires_at));
                Ok(())
            }
            None => {
                if self.config.auto_tiering {
                    self.migrate_mutable_to_readonly()?;

                    if let Some(address) = self.mutable.try_append(&key, &value) {
                        self.index
                            .insert(key, IndexEntry::with_expiry(address, expires_at));
                        Ok(())
                    } else {
                        Err(io::Error::other("Failed to append after migration"))
                    }
                } else {
                    Err(io::Error::other("Mutable region full"))
                }
            }
        }
    }

    /// Delete a key
    pub fn del(&self, key: &Bytes) -> bool {
        self.del_count.fetch_add(1, Ordering::Relaxed);
        self.index.remove(key).is_some()
    }

    /// Check if a key exists
    pub fn exists(&self, key: &Bytes) -> bool {
        if let Some(entry) = self.index.get(key) {
            if entry.is_expired() {
                drop(entry);
                self.index.remove(key);
                return false;
            }
            true
        } else {
            false
        }
    }

    /// Get the TTL of a key in seconds
    pub fn ttl(&self, key: &Bytes) -> Option<i64> {
        let entry = self.index.get(key)?;

        match entry.expires_at {
            None => Some(-1),
            Some(exp) => {
                let now = SystemTime::now();
                if now >= exp {
                    drop(entry);
                    self.index.remove(key);
                    Some(-2)
                } else {
                    Some(exp.duration_since(now).unwrap_or_default().as_secs() as i64)
                }
            }
        }
    }

    /// Set expiration on a key
    pub fn expire(&self, key: &Bytes, expires_at: SystemTime) -> bool {
        if let Some(mut entry) = self.index.get_mut(key) {
            if entry.is_expired() {
                drop(entry);
                self.index.remove(key);
                return false;
            }
            entry.expires_at = Some(expires_at);
            true
        } else {
            false
        }
    }

    /// Get the number of keys
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Get all non-expired keys
    pub fn keys(&self) -> Vec<Bytes> {
        let mut keys = Vec::new();
        let mut expired = Vec::new();
        for entry in self.index.iter() {
            if entry.is_expired() {
                expired.push(entry.key().clone());
            } else {
                keys.push(entry.key().clone());
            }
        }
        for key in expired {
            self.index.remove(&key);
        }
        keys
    }

    /// Get access count for a key
    pub fn access_count(&self, key: &Bytes) -> Option<u64> {
        let entry = self.index.get(key)?;
        if entry.is_expired() {
            drop(entry);
            self.index.remove(key);
            return None;
        }
        Some(entry.access_count)
    }

    /// Get last access time for a key
    pub fn last_access(&self, key: &Bytes) -> Option<Instant> {
        let entry = self.index.get(key)?;
        if entry.is_expired() {
            drop(entry);
            self.index.remove(key);
            return None;
        }
        Some(entry.last_access)
    }

    /// Migrate data from mutable to read-only region
    fn migrate_mutable_to_readonly(&self) -> io::Result<()> {
        debug!("Migrating mutable region to read-only");

        let mut readonly = self.readonly.write();

        // Check if read-only region has space
        let mutable_used = self.mutable.used();
        if readonly.remaining() < mutable_used {
            // Need to migrate read-only to disk first
            drop(readonly);
            self.migrate_readonly_to_disk()?;
            readonly = self.readonly.write();
        }

        // Collect keys that point to mutable region
        let mut mutable_keys: Vec<(Bytes, u64)> = Vec::new();
        for entry in self.index.iter() {
            if entry.address.region() == Region::Mutable {
                mutable_keys.push((entry.key().clone(), entry.address.offset()));
            }
        }

        // Migrate each entry
        for (key, old_offset) in mutable_keys {
            // Read from mutable
            if let Some((read_key, value)) = self.mutable.read(old_offset) {
                if read_key == key {
                    // Write to read-only
                    if let Some(new_address) = readonly.append(&key, &value)? {
                        // Update index
                        if let Some(mut entry) = self.index.get_mut(&key) {
                            entry.address = new_address;
                        }
                    }
                }
            }
        }

        self.mutable_to_readonly_count
            .fetch_add(1, Ordering::Relaxed);

        // Clear mutable region
        // SAFETY: We've copied all data and updated all references
        unsafe {
            self.mutable.clear();
        }

        info!("Migrated {} bytes from mutable to read-only", mutable_used);

        Ok(())
    }

    /// Migrate data from read-only to disk region
    fn migrate_readonly_to_disk(&self) -> io::Result<()> {
        debug!("Migrating read-only region to disk");

        let readonly = self.readonly.read();
        let disk = self.disk.write();

        // Collect keys that point to read-only region
        let mut readonly_keys: Vec<(Bytes, u64)> = Vec::new();
        for entry in self.index.iter() {
            if entry.address.region() == Region::ReadOnly {
                readonly_keys.push((entry.key().clone(), entry.address.offset()));
            }
        }

        // Migrate each entry
        for (key, old_offset) in readonly_keys {
            // Read from read-only
            if let Some((read_key, value)) = readonly.read(old_offset) {
                if read_key == key {
                    // Write to disk
                    if let Some(new_address) = disk.append(&key, &value)? {
                        // Update index
                        if let Some(mut entry) = self.index.get_mut(&key) {
                            entry.address = new_address;
                        }
                    } else {
                        warn!("Disk region full, cannot complete migration");
                        return Err(io::Error::other("Disk region full"));
                    }
                }
            }
        }

        drop(readonly);

        self.readonly_to_disk_count.fetch_add(1, Ordering::Relaxed);

        // Compact read-only region (remove all migrated entries)
        let readonly = self.readonly.write();
        let live_keys: std::collections::HashSet<Vec<u8>> = self
            .index
            .iter()
            .filter(|e| e.address.region() == Region::ReadOnly)
            .map(|e| e.key().to_vec())
            .collect();

        readonly.compact(|key| live_keys.contains(key))?;

        info!("Migrated read-only region to disk");

        Ok(())
    }

    /// Get statistics
    pub fn stats(&self) -> HybridLogStats {
        let readonly = self.readonly.read();
        let disk = self.disk.read();

        HybridLogStats {
            key_count: self.index.len() as u64,
            mutable_bytes: self.mutable.used(),
            readonly_bytes: readonly.size(),
            disk_bytes: disk.size(),
            mutable_entries: self.mutable.entry_count(),
            readonly_entries: readonly.entry_count(),
            disk_entries: disk.entry_count(),
            get_ops: self.get_count.load(Ordering::Relaxed),
            set_ops: self.set_count.load(Ordering::Relaxed),
            del_ops: self.del_count.load(Ordering::Relaxed),
            mutable_to_readonly_migrations: self.mutable_to_readonly_count.load(Ordering::Relaxed),
            readonly_to_disk_migrations: self.readonly_to_disk_count.load(Ordering::Relaxed),
            background_migrations: self.background_migration_count.load(Ordering::Relaxed),
            background_compactions: self.background_compaction_count.load(Ordering::Relaxed),
            compaction_bytes_reclaimed: self.compaction_bytes_reclaimed.load(Ordering::Relaxed),
            background_tasks_active: self.background_tasks_active.load(Ordering::Relaxed),
        }
    }

    /// Reset operation statistics (for CONFIG RESETSTAT)
    pub fn reset_stats(&self) {
        self.get_count.store(0, Ordering::Relaxed);
        self.set_count.store(0, Ordering::Relaxed);
        self.del_count.store(0, Ordering::Relaxed);
        self.mutable_to_readonly_count.store(0, Ordering::Relaxed);
        self.readonly_to_disk_count.store(0, Ordering::Relaxed);
        self.background_migration_count.store(0, Ordering::Relaxed);
        self.background_compaction_count.store(0, Ordering::Relaxed);
        self.compaction_bytes_reclaimed.store(0, Ordering::Relaxed);
    }

    /// Force a checkpoint/flush of all data
    pub fn checkpoint(&self) -> io::Result<()> {
        // Migrate all mutable data to read-only for persistence
        if self.mutable.used() > 0 {
            self.migrate_mutable_to_readonly()?;
        }
        Ok(())
    }

    /// Compact all regions
    pub fn compact(&self) -> io::Result<()> {
        // Get live keys
        let live_keys: std::collections::HashSet<Vec<u8>> =
            self.index.iter().map(|e| e.key().to_vec()).collect();

        // Compact read-only region
        {
            let readonly = self.readonly.write();
            readonly.compact(|key| live_keys.contains(key))?;
        }

        // Compact disk region
        {
            let disk = self.disk.read();
            disk.compact(|key| live_keys.contains(key))?;
        }

        info!("Compaction complete");
        Ok(())
    }

    /// Check if mutable region needs migration
    pub fn should_migrate_mutable(&self) -> bool {
        let used = self.mutable.used() as f64;
        let capacity = self.mutable.capacity() as f64;
        (used / capacity) >= self.config.migration_threshold
    }

    /// Get the configuration
    pub fn config(&self) -> &HybridLogConfig {
        &self.config
    }

    // ==================== Async Operations ====================

    /// Get a value by key asynchronously
    ///
    /// This method is optimized for disk region reads, using async I/O
    /// to avoid blocking the runtime. For mutable and read-only regions,
    /// it falls back to synchronous reads.
    pub async fn get_async(&self, key: &Bytes) -> Option<Bytes> {
        self.get_count.fetch_add(1, Ordering::Relaxed);

        let mut entry = self.index.get_mut(key)?;

        // Check expiration
        if entry.is_expired() {
            drop(entry);
            self.index.remove(key);
            return None;
        }

        entry.record_access();
        let address = entry.address;
        drop(entry);

        self.read_value_async(address).await
    }

    /// Read a value asynchronously from the appropriate region
    async fn read_value_async(&self, address: LogAddress) -> Option<Bytes> {
        match address.region() {
            Region::Mutable => self.mutable.read_value(address.offset()),
            Region::ReadOnly => {
                let readonly = self.readonly.read();
                readonly.read_value(address.offset())
            }
            Region::Disk => {
                // Extract path and validate offset, then drop the guard before async I/O
                let read_params = {
                    let disk = self.disk.read();
                    disk.prepare_async_read(address.offset())
                };
                match read_params {
                    Some((path, max_size)) => {
                        match DiskRegion::async_read_at(&path, address.offset(), max_size).await {
                            Ok(value) => value,
                            Err(e) => {
                                warn!("Async disk read failed: {}", e);
                                None
                            }
                        }
                    }
                    None => None,
                }
            }
            Region::Invalid => None,
        }
    }

    /// Migrate mutable region to read-only asynchronously
    ///
    /// This is the async version of migrate_mutable_to_readonly for use
    /// in background tasks.
    pub async fn migrate_mutable_to_readonly_async(&self) -> io::Result<()> {
        // The actual migration logic is synchronous
        // We call it directly since the operations are CPU-bound and fast
        self.migrate_mutable_to_readonly()
    }

    /// Compact all regions asynchronously
    ///
    /// This is the async version of compact for use in background tasks.
    pub async fn compact_async(&self) -> io::Result<usize> {
        let initial_size = {
            let readonly = self.readonly.read();
            let disk = self.disk.read();
            readonly.size() + disk.size()
        };

        // Run compaction - the operations are I/O bound but use synchronous APIs
        self.compact()?;

        let final_size = {
            let readonly = self.readonly.read();
            let disk = self.disk.read();
            readonly.size() + disk.size()
        };

        let reclaimed = initial_size.saturating_sub(final_size);
        self.compaction_bytes_reclaimed
            .fetch_add(reclaimed as u64, Ordering::Relaxed);
        self.background_compaction_count
            .fetch_add(1, Ordering::Relaxed);

        Ok(reclaimed)
    }

    /// Check if background tasks should run
    pub fn are_background_tasks_active(&self) -> bool {
        self.background_tasks_active.load(Ordering::Relaxed)
    }

    /// Set the background tasks active flag
    pub fn set_background_tasks_active(&self, active: bool) {
        self.background_tasks_active
            .store(active, Ordering::Relaxed);
    }

    /// Record a background migration
    pub fn record_background_migration(&self) {
        self.background_migration_count
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Handle for controlling background tasks
pub struct BackgroundTaskHandle {
    /// Shutdown signal sender
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    /// Migration task handle
    migration_handle: Option<tokio::task::JoinHandle<()>>,
    /// Compaction task handle
    compaction_handle: Option<tokio::task::JoinHandle<()>>,
}

impl BackgroundTaskHandle {
    /// Signal background tasks to stop
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Wait for all background tasks to complete
    pub async fn wait(self) {
        if let Some(handle) = self.migration_handle {
            let _ = handle.await;
        }
        if let Some(handle) = self.compaction_handle {
            let _ = handle.await;
        }
    }
}

/// Configuration for background tasks
#[derive(Debug, Clone)]
pub struct BackgroundTaskConfig {
    /// Interval between migration checks
    pub migration_check_interval: std::time::Duration,
    /// Interval between compaction runs
    pub compaction_interval: std::time::Duration,
    /// Enable background migration
    pub enable_migration: bool,
    /// Enable background compaction
    pub enable_compaction: bool,
}

impl Default for BackgroundTaskConfig {
    fn default() -> Self {
        Self {
            migration_check_interval: std::time::Duration::from_millis(100),
            compaction_interval: std::time::Duration::from_secs(300), // 5 minutes
            enable_migration: true,
            enable_compaction: true,
        }
    }
}

/// Start background tasks for the HybridLog
///
/// This spawns background tasks for:
/// - Automatic migration from mutable to read-only region when threshold is reached
/// - Periodic compaction to reclaim space from deleted entries
///
/// Returns a handle that can be used to control the background tasks.
pub fn start_background_tasks(
    log: std::sync::Arc<HybridLog>,
    config: BackgroundTaskConfig,
) -> BackgroundTaskHandle {
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    log.set_background_tasks_active(true);

    // Spawn migration task
    let migration_handle = if config.enable_migration {
        let log_clone = log.clone();
        let mut rx = shutdown_rx.clone();
        let interval = config.migration_check_interval;

        Some(tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        if log_clone.should_migrate_mutable() {
                            debug!("Background migration triggered");
                            if let Err(e) = log_clone.migrate_mutable_to_readonly_async().await {
                                warn!("Background migration failed: {}", e);
                            } else {
                                log_clone.record_background_migration();
                            }
                        }
                    }
                    _ = rx.changed() => {
                        if *rx.borrow() {
                            info!("Background migration task shutting down");
                            break;
                        }
                    }
                }
            }
        }))
    } else {
        None
    };

    // Spawn compaction task
    let compaction_handle = if config.enable_compaction {
        let log_clone = log.clone();
        let mut rx = shutdown_rx;
        let interval = config.compaction_interval;

        Some(tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            // Skip the first tick to avoid immediate compaction on startup
            interval_timer.tick().await;

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        debug!("Background compaction triggered");
                        match log_clone.compact_async().await {
                            Ok(reclaimed) => {
                                info!("Background compaction completed, reclaimed {} bytes", reclaimed);
                            }
                            Err(e) => {
                                warn!("Background compaction failed: {}", e);
                            }
                        }
                    }
                    _ = rx.changed() => {
                        if *rx.borrow() {
                            info!("Background compaction task shutting down");
                            break;
                        }
                    }
                }
            }

            // Mark background tasks as inactive
            log_clone.set_background_tasks_active(false);
        }))
    } else {
        // Don't mark as inactive if migration is still enabled
        if !config.enable_migration {
            log.set_background_tasks_active(false);
        }
        None
    };

    BackgroundTaskHandle {
        shutdown_tx,
        migration_handle,
        compaction_handle,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_log() -> (HybridLog, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let config = HybridLogConfig {
            mutable_size: 4096,
            readonly_size: 8192,
            disk_size: 0,
            data_dir: dir.path().to_path_buf(),
            auto_tiering: true,
            migration_threshold: 0.8,
        };
        let log = HybridLog::new(config).unwrap();
        (log, dir)
    }

    #[test]
    fn test_basic_set_get() {
        let (log, _dir) = create_test_log();

        let key = Bytes::from("test_key");
        let value = Bytes::from("test_value");

        log.set(key.clone(), value.clone()).unwrap();

        let result = log.get(&key);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), value);
    }

    #[test]
    fn test_del() {
        let (log, _dir) = create_test_log();

        let key = Bytes::from("key");
        let value = Bytes::from("value");

        log.set(key.clone(), value).unwrap();
        assert!(log.exists(&key));

        assert!(log.del(&key));
        assert!(!log.exists(&key));
        assert!(log.get(&key).is_none());
    }

    #[test]
    fn test_exists() {
        let (log, _dir) = create_test_log();

        let key = Bytes::from("key");
        assert!(!log.exists(&key));

        log.set(key.clone(), Bytes::from("value")).unwrap();
        assert!(log.exists(&key));
    }

    #[test]
    fn test_expiration() {
        use std::time::Duration;

        let (log, _dir) = create_test_log();

        let key = Bytes::from("key");
        let expires_at = SystemTime::now() - Duration::from_secs(1);

        log.set_with_expiry(key.clone(), Bytes::from("value"), expires_at)
            .unwrap();

        // Should be expired
        assert!(log.get(&key).is_none());
        assert!(!log.exists(&key));
    }

    #[test]
    fn test_ttl() {
        use std::time::Duration;

        let (log, _dir) = create_test_log();

        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");

        // No expiration
        log.set(key1.clone(), Bytes::from("value1")).unwrap();
        assert_eq!(log.ttl(&key1), Some(-1));

        // With expiration
        let expires_at = SystemTime::now() + Duration::from_secs(60);
        log.set_with_expiry(key2.clone(), Bytes::from("value2"), expires_at)
            .unwrap();
        let ttl = log.ttl(&key2).unwrap();
        assert!(ttl > 0 && ttl <= 60);
    }

    #[test]
    fn test_multiple_operations() {
        let (log, _dir) = create_test_log();

        for i in 0..10 {
            let key = Bytes::from(format!("key{}", i));
            let value = Bytes::from(format!("value{}", i));
            log.set(key, value).unwrap();
        }

        assert_eq!(log.len(), 10);

        for i in 0..10 {
            let key = Bytes::from(format!("key{}", i));
            let expected = Bytes::from(format!("value{}", i));
            assert_eq!(log.get(&key).unwrap(), expected);
        }
    }

    #[test]
    fn test_overwrite() {
        let (log, _dir) = create_test_log();

        let key = Bytes::from("key");

        log.set(key.clone(), Bytes::from("value1")).unwrap();
        assert_eq!(log.get(&key).unwrap(), Bytes::from("value1"));

        log.set(key.clone(), Bytes::from("value2")).unwrap();
        assert_eq!(log.get(&key).unwrap(), Bytes::from("value2"));
    }

    #[test]
    fn test_stats() {
        let (log, _dir) = create_test_log();

        let key = Bytes::from("key");
        let value = Bytes::from("value");

        log.set(key.clone(), value).unwrap();
        log.get(&key);
        log.del(&key);

        let stats = log.stats();
        assert_eq!(stats.set_ops, 1);
        assert_eq!(stats.get_ops, 1);
        assert_eq!(stats.del_ops, 1);
    }

    #[test]
    fn test_migration_to_readonly() {
        let dir = tempdir().unwrap();
        let config = HybridLogConfig {
            mutable_size: 4096, // MutableRegion enforces minimum of 4096
            readonly_size: 1024 * 1024,
            disk_size: 0,
            data_dir: dir.path().to_path_buf(),
            auto_tiering: true,
            migration_threshold: 0.8,
        };
        let log = HybridLog::new(config).unwrap();

        // Fill the mutable region with larger entries to trigger migration
        // Each entry: header(16) + key(~10) + value(100) = ~126, aligned = 128 bytes
        // 4096 / 128 = 32 entries to fill, so 40 entries should trigger migration
        let large_value = "x".repeat(100);
        for i in 0..50 {
            let key = Bytes::from(format!("key{:05}", i));
            let value = Bytes::from(format!("{}{}", large_value, i));
            log.set(key, value).unwrap();
        }

        // Verify data is still accessible
        for i in 0..50 {
            let key = Bytes::from(format!("key{:05}", i));
            let result = log.get(&key);
            assert!(result.is_some(), "Key {} should exist", i);
        }

        let stats = log.stats();
        assert!(
            stats.mutable_to_readonly_migrations >= 1,
            "Expected at least 1 migration, got {}",
            stats.mutable_to_readonly_migrations
        );
    }

    #[test]
    fn test_checkpoint() {
        let (log, _dir) = create_test_log();

        let key = Bytes::from("key");
        let value = Bytes::from("value");

        log.set(key.clone(), value.clone()).unwrap();

        // Checkpoint should migrate to read-only
        log.checkpoint().unwrap();

        // Data should still be accessible
        assert_eq!(log.get(&key).unwrap(), value);
    }

    #[tokio::test]
    async fn test_get_async() {
        let (log, _dir) = create_test_log();

        let key = Bytes::from("test_key");
        let value = Bytes::from("test_value");

        log.set(key.clone(), value.clone()).unwrap();

        // Test async get
        let result = log.get_async(&key).await;
        assert!(result.is_some());
        assert_eq!(result.unwrap(), value);

        // Non-existent key
        let missing = log.get_async(&Bytes::from("missing")).await;
        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn test_get_async_expiration() {
        use std::time::Duration;

        let (log, _dir) = create_test_log();

        let key = Bytes::from("key");
        let expires_at = SystemTime::now() - Duration::from_secs(1);

        log.set_with_expiry(key.clone(), Bytes::from("value"), expires_at)
            .unwrap();

        // Async get should return None for expired key
        let result = log.get_async(&key).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_compact_async() {
        let (log, _dir) = create_test_log();

        // Add some data
        log.set(Bytes::from("keep1"), Bytes::from("value1"))
            .unwrap();
        log.set(Bytes::from("keep2"), Bytes::from("value2"))
            .unwrap();
        log.set(Bytes::from("delete"), Bytes::from("value3"))
            .unwrap();

        // Delete one key
        log.del(&Bytes::from("delete"));

        // Migrate to readonly so there's something to compact
        log.checkpoint().unwrap();

        // Compact
        let reclaimed = log.compact_async().await.unwrap();

        // Verify data is still accessible
        assert_eq!(
            log.get(&Bytes::from("keep1")).unwrap(),
            Bytes::from("value1")
        );
        assert_eq!(
            log.get(&Bytes::from("keep2")).unwrap(),
            Bytes::from("value2")
        );

        // Stats should be updated
        let stats = log.stats();
        assert_eq!(stats.background_compactions, 1);
        assert_eq!(stats.compaction_bytes_reclaimed, reclaimed as u64);
    }

    #[tokio::test]
    async fn test_background_task_stats() {
        let (log, _dir) = create_test_log();

        // Initially no background migrations
        let stats = log.stats();
        assert_eq!(stats.background_migrations, 0);
        assert!(!stats.background_tasks_active);

        // Manually record a background migration
        log.record_background_migration();

        let stats = log.stats();
        assert_eq!(stats.background_migrations, 1);
    }

    #[tokio::test]
    async fn test_background_tasks_startup_shutdown() {
        use std::sync::Arc;
        use std::time::Duration;

        let dir = tempdir().unwrap();
        let config = HybridLogConfig {
            mutable_size: 4096,
            readonly_size: 8192,
            disk_size: 0,
            data_dir: dir.path().to_path_buf(),
            auto_tiering: true,
            migration_threshold: 0.8,
        };
        let log = Arc::new(HybridLog::new(config).unwrap());

        // Configure background tasks with short intervals for testing
        let task_config = BackgroundTaskConfig {
            migration_check_interval: Duration::from_millis(10),
            compaction_interval: Duration::from_millis(50),
            enable_migration: true,
            enable_compaction: true,
        };

        // Start background tasks
        let handle = start_background_tasks(log.clone(), task_config);

        // Verify background tasks are active
        assert!(log.are_background_tasks_active());

        // Let it run briefly
        tokio::time::sleep(Duration::from_millis(30)).await;

        // Shutdown
        handle.shutdown();
        handle.wait().await;

        // After shutdown, background tasks should be inactive
        assert!(!log.are_background_tasks_active());
    }

    #[tokio::test]
    async fn test_background_migration_triggered() {
        use std::sync::Arc;
        use std::time::Duration;

        let dir = tempdir().unwrap();
        let config = HybridLogConfig {
            mutable_size: 4096,
            readonly_size: 1024 * 1024,
            disk_size: 0,
            data_dir: dir.path().to_path_buf(),
            auto_tiering: true,
            migration_threshold: 0.5, // Lower threshold to trigger migration faster
        };
        let log = Arc::new(HybridLog::new(config).unwrap());

        // Configure background tasks
        let task_config = BackgroundTaskConfig {
            migration_check_interval: Duration::from_millis(10),
            compaction_interval: Duration::from_secs(3600),
            enable_migration: true,
            enable_compaction: false,
        };

        // Fill mutable region above threshold but below auto-migration
        // so that background task triggers the migration
        let large_value = "x".repeat(100);
        for i in 0..15 {
            let key = Bytes::from(format!("key{:05}", i));
            let value = Bytes::from(format!("{}{}", large_value, i));
            log.set(key, value).unwrap();
        }

        // Start background tasks
        let handle = start_background_tasks(log.clone(), task_config);

        // Wait for migration to happen
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Shutdown
        handle.shutdown();
        handle.wait().await;

        // Verify data is still accessible
        for i in 0..15 {
            let key = Bytes::from(format!("key{:05}", i));
            assert!(
                log.get(&key).is_some(),
                "Key {} should exist after migration",
                i
            );
        }
    }
}
