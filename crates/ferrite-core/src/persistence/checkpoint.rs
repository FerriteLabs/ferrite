//! Checkpointing for Ferrite
//!
//! This module implements fork-less checkpointing for the HybridLog storage engine.
//! Unlike traditional Redis RDB, we don't fork the process. Instead, we leverage
//! the HybridLog's tiered architecture:
//!
//! 1. Migrate all mutable region data to read-only region
//! 2. Sync read-only region to disk
//! 3. Write checkpoint metadata
//!
//! Recovery:
//! 1. Load checkpoint metadata
//! 2. Open HybridLog with existing data files
//! 3. Replay AOF from checkpoint LSN

use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

#[cfg(feature = "crypto")]
use crate::crypto::{Encryption, SharedEncryption};

/// Checkpoint metadata stored alongside the checkpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointMetadata {
    /// Unique checkpoint ID (timestamp-based)
    pub id: u64,
    /// Creation timestamp (Unix milliseconds)
    pub created_at: u64,
    /// Number of keys at checkpoint time
    pub key_count: u64,
    /// Total data size in bytes
    pub data_size: u64,
    /// AOF sequence number (for replay from this point)
    pub aof_sequence: u64,
    /// Checksum of the data files
    pub checksum: u64,
    /// Version of the checkpoint format
    pub version: u32,
}

impl CheckpointMetadata {
    /// Current checkpoint format version
    pub const CURRENT_VERSION: u32 = 1;

    /// Create new checkpoint metadata
    pub fn new(key_count: u64, data_size: u64, aof_sequence: u64) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            id: now,
            created_at: now,
            key_count,
            data_size,
            aof_sequence,
            checksum: 0,
            version: Self::CURRENT_VERSION,
        }
    }

    /// Compute and set checksum
    pub fn with_checksum(mut self, checksum: u64) -> Self {
        self.checksum = checksum;
        self
    }
}

/// Checkpoint configuration
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Directory to store checkpoints
    pub checkpoint_dir: PathBuf,
    /// Maximum number of checkpoints to retain
    pub max_checkpoints: usize,
    /// Interval between automatic checkpoints (None = disabled)
    pub auto_checkpoint_interval: Option<Duration>,
    /// Minimum changes before allowing checkpoint
    pub min_changes_for_checkpoint: u64,
    /// Enable fsync after checkpoint writes
    pub sync_on_checkpoint: bool,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            checkpoint_dir: PathBuf::from("./checkpoints"),
            max_checkpoints: 3,
            auto_checkpoint_interval: Some(Duration::from_secs(300)), // 5 minutes
            min_changes_for_checkpoint: 1000,
            sync_on_checkpoint: true,
        }
    }
}

/// Result of a checkpoint operation
#[derive(Debug, Clone)]
pub struct CheckpointResult {
    /// The checkpoint metadata
    pub metadata: CheckpointMetadata,
    /// Time taken to create the checkpoint
    pub duration: Duration,
    /// Path to the checkpoint
    pub path: PathBuf,
}

/// Manages checkpoint creation and recovery
pub struct CheckpointManager {
    /// Configuration
    config: CheckpointConfig,
    /// Current AOF sequence number
    aof_sequence: AtomicU64,
    /// Number of changes since last checkpoint
    changes_since_checkpoint: AtomicU64,
    /// Whether a checkpoint is in progress
    checkpoint_in_progress: AtomicBool,
    /// Last checkpoint metadata
    last_checkpoint: RwLock<Option<CheckpointMetadata>>,
    /// Last checkpoint time
    last_checkpoint_time: RwLock<Option<Instant>>,
    /// Optional encryption for metadata files
    #[cfg(feature = "crypto")]
    encryption: Option<SharedEncryption>,
}

impl CheckpointManager {
    /// Create a new checkpoint manager
    pub fn new(config: CheckpointConfig) -> io::Result<Self> {
        // Ensure checkpoint directory exists
        fs::create_dir_all(&config.checkpoint_dir)?;

        let manager = Self {
            config,
            aof_sequence: AtomicU64::new(0),
            changes_since_checkpoint: AtomicU64::new(0),
            checkpoint_in_progress: AtomicBool::new(false),
            last_checkpoint: RwLock::new(None),
            last_checkpoint_time: RwLock::new(None),
            #[cfg(feature = "crypto")]
            encryption: None,
        };

        // Try to load the latest checkpoint metadata
        if let Some(metadata) = manager.load_latest_checkpoint_metadata()? {
            *manager.last_checkpoint.write() = Some(metadata.clone());
            manager
                .aof_sequence
                .store(metadata.aof_sequence, Ordering::Release);
            info!(
                "Loaded checkpoint {} with {} keys",
                metadata.id, metadata.key_count
            );
        }

        Ok(manager)
    }

    /// Create a new checkpoint manager with optional encryption
    #[cfg(feature = "crypto")]
    pub fn with_encryption(
        config: CheckpointConfig,
        encryption: Option<SharedEncryption>,
    ) -> io::Result<Self> {
        // Ensure checkpoint directory exists
        fs::create_dir_all(&config.checkpoint_dir)?;

        let manager = Self {
            config,
            aof_sequence: AtomicU64::new(0),
            changes_since_checkpoint: AtomicU64::new(0),
            checkpoint_in_progress: AtomicBool::new(false),
            last_checkpoint: RwLock::new(None),
            last_checkpoint_time: RwLock::new(None),
            encryption,
        };

        // Try to load the latest checkpoint metadata
        if let Some(metadata) = manager.load_latest_checkpoint_metadata()? {
            *manager.last_checkpoint.write() = Some(metadata.clone());
            manager
                .aof_sequence
                .store(metadata.aof_sequence, Ordering::Release);
            info!(
                "Loaded checkpoint {} with {} keys",
                metadata.id, metadata.key_count
            );
        }

        Ok(manager)
    }

    /// Get the checkpoint directory
    pub fn checkpoint_dir(&self) -> &Path {
        &self.config.checkpoint_dir
    }

    /// Get current AOF sequence number
    pub fn aof_sequence(&self) -> u64 {
        self.aof_sequence.load(Ordering::Acquire)
    }

    /// Increment AOF sequence and record a change
    pub fn record_change(&self) -> u64 {
        self.changes_since_checkpoint
            .fetch_add(1, Ordering::Relaxed);
        self.aof_sequence.fetch_add(1, Ordering::AcqRel)
    }

    /// Get changes since last checkpoint
    pub fn changes_since_checkpoint(&self) -> u64 {
        self.changes_since_checkpoint.load(Ordering::Relaxed)
    }

    /// Check if automatic checkpoint is needed
    pub fn should_auto_checkpoint(&self) -> bool {
        // Check if enough changes
        if self.changes_since_checkpoint() < self.config.min_changes_for_checkpoint {
            return false;
        }

        // Check if enough time has passed
        if let Some(interval) = self.config.auto_checkpoint_interval {
            let last_time = self.last_checkpoint_time.read();
            if let Some(last) = *last_time {
                return last.elapsed() >= interval;
            }
            return true; // No checkpoint yet
        }

        false
    }

    /// Create a checkpoint
    ///
    /// This performs a fork-less checkpoint by:
    /// 1. Migrating all mutable data to persistent storage
    /// 2. Writing checkpoint metadata
    pub fn create_checkpoint(
        &self,
        key_count: u64,
        data_size: u64,
        migrate_fn: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<CheckpointResult> {
        // Prevent concurrent checkpoints
        if self.checkpoint_in_progress.swap(true, Ordering::AcqRel) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "Checkpoint already in progress",
            ));
        }

        let start = Instant::now();
        let result = self.do_create_checkpoint(key_count, data_size, migrate_fn);

        self.checkpoint_in_progress.store(false, Ordering::Release);
        result.map(|r| {
            // Update last checkpoint time
            *self.last_checkpoint_time.write() = Some(Instant::now());
            CheckpointResult {
                metadata: r.0,
                duration: start.elapsed(),
                path: r.1,
            }
        })
    }

    fn do_create_checkpoint(
        &self,
        key_count: u64,
        data_size: u64,
        migrate_fn: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<(CheckpointMetadata, PathBuf)> {
        debug!("Starting checkpoint creation");

        // Get current AOF sequence
        let aof_seq = self.aof_sequence.load(Ordering::Acquire);

        // Perform migration (this makes data persistent)
        migrate_fn()?;

        // Compute checksum (simple hash of metadata for now)
        let checksum = self.compute_checksum(key_count, data_size, aof_seq);

        // Create metadata
        let metadata =
            CheckpointMetadata::new(key_count, data_size, aof_seq).with_checksum(checksum);

        // Write metadata file
        let checkpoint_path = self
            .config
            .checkpoint_dir
            .join(format!("checkpoint_{}.meta", metadata.id));

        let file = File::create(&checkpoint_path)?;
        let mut writer = BufWriter::new(file);

        let json = serde_json::to_string_pretty(&metadata)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // Encrypt if encryption is enabled
        #[cfg(feature = "crypto")]
        let data_to_write = if let Some(ref enc) = self.encryption {
            enc.encrypt(json.as_bytes())
                .map_err(|e| io::Error::other(e.to_string()))?
        } else {
            json.as_bytes().to_vec()
        };
        #[cfg(not(feature = "crypto"))]
        let data_to_write = json.as_bytes().to_vec();

        writer.write_all(&data_to_write)?;

        if self.config.sync_on_checkpoint {
            writer.get_ref().sync_all()?;
        }

        // Update tracking
        *self.last_checkpoint.write() = Some(metadata.clone());
        self.changes_since_checkpoint.store(0, Ordering::Release);

        // Cleanup old checkpoints
        self.cleanup_old_checkpoints()?;

        info!(
            "Created checkpoint {} with {} keys, {} bytes",
            metadata.id, key_count, data_size
        );

        Ok((metadata, checkpoint_path))
    }

    /// Compute a simple checksum
    fn compute_checksum(&self, key_count: u64, data_size: u64, aof_seq: u64) -> u64 {
        // Simple FNV-1a hash
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in key_count.to_le_bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        for byte in data_size.to_le_bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        for byte in aof_seq.to_le_bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }

    /// Load the latest checkpoint metadata
    pub fn load_latest_checkpoint_metadata(&self) -> io::Result<Option<CheckpointMetadata>> {
        let mut latest: Option<CheckpointMetadata> = None;

        if !self.config.checkpoint_dir.exists() {
            return Ok(None);
        }

        for entry in fs::read_dir(&self.config.checkpoint_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("meta") {
                match self.load_checkpoint_metadata(&path) {
                    Ok(metadata) => {
                        if latest.as_ref().map_or(true, |l| metadata.id > l.id) {
                            latest = Some(metadata);
                        }
                    }
                    Err(e) => {
                        warn!("Failed to load checkpoint {:?}: {}", path, e);
                    }
                }
            }
        }

        Ok(latest)
    }

    /// Load checkpoint metadata from a file
    fn load_checkpoint_metadata(&self, path: &Path) -> io::Result<CheckpointMetadata> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut data = Vec::new();
        reader.read_to_end(&mut data)?;

        // Check if data is encrypted and decrypt if needed
        #[cfg(feature = "crypto")]
        let json_data = if Encryption::is_encrypted(&data) {
            if let Some(ref enc) = self.encryption {
                enc.decrypt(&data)
                    .map_err(|e| io::Error::other(e.to_string()))?
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Encrypted checkpoint metadata but no encryption key provided",
                ));
            }
        } else {
            data
        };
        #[cfg(not(feature = "crypto"))]
        let json_data = data;

        let contents = String::from_utf8(json_data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        serde_json::from_str(&contents).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// List all available checkpoints
    pub fn list_checkpoints(&self) -> io::Result<Vec<CheckpointMetadata>> {
        let mut checkpoints = Vec::new();

        if !self.config.checkpoint_dir.exists() {
            return Ok(checkpoints);
        }

        for entry in fs::read_dir(&self.config.checkpoint_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("meta") {
                match self.load_checkpoint_metadata(&path) {
                    Ok(metadata) => checkpoints.push(metadata),
                    Err(e) => {
                        warn!("Failed to load checkpoint {:?}: {}", path, e);
                    }
                }
            }
        }

        // Sort by ID (newest first)
        checkpoints.sort_by(|a, b| b.id.cmp(&a.id));

        Ok(checkpoints)
    }

    /// Cleanup old checkpoints, keeping only the configured number
    fn cleanup_old_checkpoints(&self) -> io::Result<()> {
        let mut checkpoints = self.list_checkpoints()?;

        if checkpoints.len() <= self.config.max_checkpoints {
            return Ok(());
        }

        // Remove oldest checkpoints
        while checkpoints.len() > self.config.max_checkpoints {
            if let Some(old) = checkpoints.pop() {
                let path = self
                    .config
                    .checkpoint_dir
                    .join(format!("checkpoint_{}.meta", old.id));

                if let Err(e) = fs::remove_file(&path) {
                    warn!("Failed to remove old checkpoint {:?}: {}", path, e);
                } else {
                    debug!("Removed old checkpoint {}", old.id);
                }
            }
        }

        Ok(())
    }

    /// Delete a specific checkpoint
    pub fn delete_checkpoint(&self, id: u64) -> io::Result<()> {
        let path = self
            .config
            .checkpoint_dir
            .join(format!("checkpoint_{}.meta", id));

        if path.exists() {
            fs::remove_file(&path)?;
            info!("Deleted checkpoint {}", id);
        }

        Ok(())
    }

    /// Get the last checkpoint metadata
    pub fn last_checkpoint(&self) -> Option<CheckpointMetadata> {
        self.last_checkpoint.read().clone()
    }

    /// Verify checkpoint integrity
    pub fn verify_checkpoint(&self, metadata: &CheckpointMetadata) -> io::Result<bool> {
        let computed = self.compute_checksum(
            metadata.key_count,
            metadata.data_size,
            metadata.aof_sequence,
        );

        Ok(computed == metadata.checksum)
    }
}

/// Checkpoint recovery helper
pub struct CheckpointRecovery {
    /// Checkpoint directory
    checkpoint_dir: PathBuf,
}

impl CheckpointRecovery {
    /// Create a new recovery helper
    pub fn new(checkpoint_dir: impl AsRef<Path>) -> Self {
        Self {
            checkpoint_dir: checkpoint_dir.as_ref().to_path_buf(),
        }
    }

    /// Find the latest valid checkpoint
    pub fn find_latest_checkpoint(&self) -> io::Result<Option<CheckpointMetadata>> {
        let config = CheckpointConfig {
            checkpoint_dir: self.checkpoint_dir.clone(),
            ..Default::default()
        };

        let manager = CheckpointManager::new(config)?;
        manager.load_latest_checkpoint_metadata()
    }

    /// Get the AOF sequence to replay from
    pub fn get_replay_sequence(&self) -> io::Result<u64> {
        match self.find_latest_checkpoint()? {
            Some(metadata) => Ok(metadata.aof_sequence),
            None => Ok(0), // No checkpoint, replay from beginning
        }
    }
}

/// Background checkpoint scheduler
pub struct CheckpointScheduler {
    /// Whether the scheduler is running
    running: Arc<AtomicBool>,
    /// Handle to the background task
    handle: Option<std::thread::JoinHandle<()>>,
}

impl CheckpointScheduler {
    /// Create a new scheduler (not started)
    pub fn new() -> Self {
        Self {
            running: Arc::new(AtomicBool::new(false)),
            handle: None,
        }
    }

    /// Start the background scheduler
    pub fn start<F>(&mut self, interval: Duration, checkpoint_fn: F)
    where
        F: Fn() -> io::Result<()> + Send + 'static,
    {
        if self.running.swap(true, Ordering::AcqRel) {
            return; // Already running
        }

        let running = Arc::clone(&self.running);

        self.handle = Some(std::thread::spawn(move || {
            info!("Checkpoint scheduler started with interval {:?}", interval);

            while running.load(Ordering::Acquire) {
                std::thread::sleep(interval);

                if !running.load(Ordering::Acquire) {
                    break;
                }

                match checkpoint_fn() {
                    Ok(()) => debug!("Scheduled checkpoint completed"),
                    Err(e) => error!("Scheduled checkpoint failed: {}", e),
                }
            }

            info!("Checkpoint scheduler stopped");
        }));
    }

    /// Stop the scheduler
    pub fn stop(&mut self) {
        self.running.store(false, Ordering::Release);

        if let Some(handle) = self.handle.take() {
            // The thread will exit on the next iteration
            let _ = handle.join();
        }
    }

    /// Check if the scheduler is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }
}

impl Default for CheckpointScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for CheckpointScheduler {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_manager() -> (CheckpointManager, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let config = CheckpointConfig {
            checkpoint_dir: dir.path().to_path_buf(),
            max_checkpoints: 3,
            auto_checkpoint_interval: None,
            min_changes_for_checkpoint: 10,
            sync_on_checkpoint: false,
        };
        let manager = CheckpointManager::new(config).unwrap();
        (manager, dir)
    }

    #[test]
    fn test_create_checkpoint() {
        let (manager, _dir) = create_test_manager();

        let result = manager
            .create_checkpoint(
                100,
                1024,
                || Ok(()), // No-op migration
            )
            .unwrap();

        assert_eq!(result.metadata.key_count, 100);
        assert_eq!(result.metadata.data_size, 1024);
        assert!(result.path.exists());
    }

    #[test]
    fn test_checkpoint_metadata() {
        let metadata = CheckpointMetadata::new(50, 512, 100);

        assert_eq!(metadata.key_count, 50);
        assert_eq!(metadata.data_size, 512);
        assert_eq!(metadata.aof_sequence, 100);
        assert_eq!(metadata.version, CheckpointMetadata::CURRENT_VERSION);
    }

    #[test]
    fn test_record_change() {
        let (manager, _dir) = create_test_manager();

        assert_eq!(manager.aof_sequence(), 0);

        let seq = manager.record_change();
        assert_eq!(seq, 0);
        assert_eq!(manager.aof_sequence(), 1);
        assert_eq!(manager.changes_since_checkpoint(), 1);

        manager.record_change();
        manager.record_change();
        assert_eq!(manager.aof_sequence(), 3);
        assert_eq!(manager.changes_since_checkpoint(), 3);
    }

    #[test]
    fn test_list_checkpoints() {
        let (manager, _dir) = create_test_manager();

        // Create a few checkpoints
        manager.create_checkpoint(10, 100, || Ok(())).unwrap();
        std::thread::sleep(Duration::from_millis(10));
        manager.create_checkpoint(20, 200, || Ok(())).unwrap();
        std::thread::sleep(Duration::from_millis(10));
        manager.create_checkpoint(30, 300, || Ok(())).unwrap();

        let checkpoints = manager.list_checkpoints().unwrap();
        assert_eq!(checkpoints.len(), 3);

        // Should be sorted newest first
        assert_eq!(checkpoints[0].key_count, 30);
        assert_eq!(checkpoints[1].key_count, 20);
        assert_eq!(checkpoints[2].key_count, 10);
    }

    #[test]
    fn test_cleanup_old_checkpoints() {
        let dir = tempdir().unwrap();
        let config = CheckpointConfig {
            checkpoint_dir: dir.path().to_path_buf(),
            max_checkpoints: 2, // Smaller max for easier testing
            auto_checkpoint_interval: None,
            min_changes_for_checkpoint: 10,
            sync_on_checkpoint: false,
        };
        let manager = CheckpointManager::new(config).unwrap();

        // Create checkpoints with unique timestamps
        let mut created_count = 0u64;
        for i in 1..=4 {
            manager
                .create_checkpoint(i * 10, i * 100, || Ok(()))
                .unwrap();
            created_count = i;
            // Ensure unique timestamps
            std::thread::sleep(Duration::from_millis(2));
        }

        // Get final list
        let checkpoints = manager.list_checkpoints().unwrap();

        // Cleanup should have run, so we should have fewer checkpoints than created
        assert!(
            checkpoints.len() < created_count as usize,
            "Cleanup should remove old checkpoints. Created {}, have {}",
            created_count,
            checkpoints.len()
        );

        // Most recent checkpoint should be preserved
        assert_eq!(checkpoints[0].key_count, 40);
    }

    #[test]
    fn test_load_latest_checkpoint() {
        let dir = tempdir().unwrap();
        let config = CheckpointConfig {
            checkpoint_dir: dir.path().to_path_buf(),
            max_checkpoints: 5,
            auto_checkpoint_interval: None,
            min_changes_for_checkpoint: 10,
            sync_on_checkpoint: false,
        };

        // Create checkpoints
        {
            let manager = CheckpointManager::new(config.clone()).unwrap();
            manager.create_checkpoint(10, 100, || Ok(())).unwrap();
            std::thread::sleep(Duration::from_millis(10));
            manager.create_checkpoint(20, 200, || Ok(())).unwrap();
        }

        // Load in new manager
        {
            let manager = CheckpointManager::new(config).unwrap();
            let latest = manager.last_checkpoint().unwrap();
            assert_eq!(latest.key_count, 20);
        }
    }

    #[test]
    fn test_verify_checkpoint() {
        let (manager, _dir) = create_test_manager();

        let result = manager.create_checkpoint(50, 500, || Ok(())).unwrap();

        assert!(manager.verify_checkpoint(&result.metadata).unwrap());

        // Tamper with metadata
        let mut tampered = result.metadata.clone();
        tampered.key_count = 999;
        assert!(!manager.verify_checkpoint(&tampered).unwrap());
    }

    #[test]
    fn test_should_auto_checkpoint() {
        let dir = tempdir().unwrap();
        let config = CheckpointConfig {
            checkpoint_dir: dir.path().to_path_buf(),
            max_checkpoints: 3,
            auto_checkpoint_interval: Some(Duration::from_millis(50)),
            min_changes_for_checkpoint: 5,
            sync_on_checkpoint: false,
        };
        let manager = CheckpointManager::new(config).unwrap();

        // Not enough changes
        assert!(!manager.should_auto_checkpoint());

        // Add changes
        for _ in 0..5 {
            manager.record_change();
        }

        // Should trigger (no previous checkpoint)
        assert!(manager.should_auto_checkpoint());

        // Create checkpoint
        manager.create_checkpoint(10, 100, || Ok(())).unwrap();

        // Add more changes
        for _ in 0..5 {
            manager.record_change();
        }

        // Not enough time passed
        assert!(!manager.should_auto_checkpoint());

        // Wait for interval
        std::thread::sleep(Duration::from_millis(60));

        // Should trigger now
        assert!(manager.should_auto_checkpoint());
    }

    #[test]
    fn test_checkpoint_recovery() {
        let dir = tempdir().unwrap();
        let config = CheckpointConfig {
            checkpoint_dir: dir.path().to_path_buf(),
            max_checkpoints: 3,
            auto_checkpoint_interval: None,
            min_changes_for_checkpoint: 10,
            sync_on_checkpoint: false,
        };

        let manager = CheckpointManager::new(config).unwrap();

        // Record some changes
        for _ in 0..100 {
            manager.record_change();
        }

        manager.create_checkpoint(100, 1000, || Ok(())).unwrap();

        // Use recovery helper
        let recovery = CheckpointRecovery::new(dir.path());
        let replay_seq = recovery.get_replay_sequence().unwrap();
        assert_eq!(replay_seq, 100);
    }

    #[test]
    fn test_concurrent_checkpoint_prevention() {
        let (manager, _dir) = create_test_manager();

        // Simulate a long-running checkpoint
        manager
            .checkpoint_in_progress
            .store(true, Ordering::Release);

        let result = manager.create_checkpoint(10, 100, || Ok(()));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);

        manager
            .checkpoint_in_progress
            .store(false, Ordering::Release);

        // Should work now
        let result = manager.create_checkpoint(10, 100, || Ok(()));
        assert!(result.is_ok());
    }

    #[test]
    fn test_delete_checkpoint() {
        let (manager, _dir) = create_test_manager();

        let result = manager.create_checkpoint(10, 100, || Ok(())).unwrap();
        let id = result.metadata.id;

        assert!(result.path.exists());

        manager.delete_checkpoint(id).unwrap();

        assert!(!result.path.exists());
    }

    #[cfg(feature = "crypto")]
    #[test]
    fn test_checkpoint_encrypted_roundtrip() {
        use crate::config::EncryptionAlgorithm;
        use crate::crypto::EncryptionKey;

        let dir = tempdir().unwrap();
        let config = CheckpointConfig {
            checkpoint_dir: dir.path().to_path_buf(),
            max_checkpoints: 3,
            auto_checkpoint_interval: None,
            min_changes_for_checkpoint: 10,
            sync_on_checkpoint: false,
        };

        // Create encryption
        let key = EncryptionKey::generate();
        let encryption = Arc::new(Encryption::new(key, EncryptionAlgorithm::ChaCha20Poly1305));

        // Create manager with encryption
        let manager =
            CheckpointManager::with_encryption(config.clone(), Some(encryption.clone())).unwrap();

        // Create checkpoint
        let result = manager.create_checkpoint(100, 1024, || Ok(())).unwrap();

        assert_eq!(result.metadata.key_count, 100);
        assert_eq!(result.metadata.data_size, 1024);
        assert!(result.path.exists());

        // Verify metadata file is encrypted
        let file_contents = std::fs::read(&result.path).unwrap();
        assert!(Encryption::is_encrypted(&file_contents));

        // Create new manager with same key - should load checkpoint
        let manager2 = CheckpointManager::with_encryption(config, Some(encryption)).unwrap();
        let last_checkpoint = manager2.last_checkpoint().unwrap();
        assert_eq!(last_checkpoint.key_count, 100);
        assert_eq!(last_checkpoint.data_size, 1024);
    }

    #[cfg(feature = "crypto")]
    #[test]
    fn test_checkpoint_encrypted_wrong_key_fails() {
        use crate::config::EncryptionAlgorithm;
        use crate::crypto::EncryptionKey;

        let dir = tempdir().unwrap();
        let config = CheckpointConfig {
            checkpoint_dir: dir.path().to_path_buf(),
            max_checkpoints: 3,
            auto_checkpoint_interval: None,
            min_changes_for_checkpoint: 10,
            sync_on_checkpoint: false,
        };

        // Create with first key
        let key1 = EncryptionKey::generate();
        let encryption1 = Arc::new(Encryption::new(key1, EncryptionAlgorithm::ChaCha20Poly1305));
        let manager =
            CheckpointManager::with_encryption(config.clone(), Some(encryption1)).unwrap();

        // Create checkpoint
        manager.create_checkpoint(100, 1024, || Ok(())).unwrap();

        // Try to load with different key
        let key2 = EncryptionKey::generate();
        let encryption2 = Arc::new(Encryption::new(key2, EncryptionAlgorithm::ChaCha20Poly1305));

        // Loading with wrong key should fail or return no checkpoint
        let manager2 = CheckpointManager::with_encryption(config, Some(encryption2)).unwrap();
        // The load should fail silently (with warning), so last_checkpoint should be None
        assert!(manager2.last_checkpoint().is_none());
    }

    #[cfg(feature = "crypto")]
    #[test]
    fn test_checkpoint_encrypted_requires_key() {
        use crate::config::EncryptionAlgorithm;
        use crate::crypto::EncryptionKey;

        let dir = tempdir().unwrap();
        let config = CheckpointConfig {
            checkpoint_dir: dir.path().to_path_buf(),
            max_checkpoints: 3,
            auto_checkpoint_interval: None,
            min_changes_for_checkpoint: 10,
            sync_on_checkpoint: false,
        };

        // Create with encryption
        let key = EncryptionKey::generate();
        let encryption = Arc::new(Encryption::new(key, EncryptionAlgorithm::ChaCha20Poly1305));
        let manager = CheckpointManager::with_encryption(config.clone(), Some(encryption)).unwrap();

        // Create checkpoint
        manager.create_checkpoint(100, 1024, || Ok(())).unwrap();

        // Try to load without encryption key
        let manager2 = CheckpointManager::new(config).unwrap();
        // The load should fail silently (with warning), so last_checkpoint should be None
        assert!(manager2.last_checkpoint().is_none());
    }

    #[cfg(feature = "crypto")]
    #[test]
    fn test_checkpoint_unencrypted_read_with_encryption_key() {
        use crate::config::EncryptionAlgorithm;
        use crate::crypto::EncryptionKey;

        let dir = tempdir().unwrap();
        let config = CheckpointConfig {
            checkpoint_dir: dir.path().to_path_buf(),
            max_checkpoints: 3,
            auto_checkpoint_interval: None,
            min_changes_for_checkpoint: 10,
            sync_on_checkpoint: false,
        };

        // Create without encryption
        let manager = CheckpointManager::new(config.clone()).unwrap();
        manager.create_checkpoint(100, 1024, || Ok(())).unwrap();

        // Should be able to load with encryption key (backward compatibility)
        let key = EncryptionKey::generate();
        let encryption = Arc::new(Encryption::new(key, EncryptionAlgorithm::ChaCha20Poly1305));
        let manager2 = CheckpointManager::with_encryption(config, Some(encryption)).unwrap();

        let last_checkpoint = manager2.last_checkpoint().unwrap();
        assert_eq!(last_checkpoint.key_count, 100);
        assert_eq!(last_checkpoint.data_size, 1024);
    }
}
