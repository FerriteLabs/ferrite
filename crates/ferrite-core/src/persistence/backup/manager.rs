//! Backup Manager
//!
//! This module provides the backup manager that coordinates backup creation,
//! restore operations, and point-in-time recovery.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use super::storage::{BackupStorage, BackupStorageError, LocalBackupStorage};
use crate::storage::{Store, Value};

/// Backup error
#[derive(Debug, Error)]
pub enum BackupError {
    /// Storage error
    #[error("Storage error: {0}")]
    Storage(#[from] BackupStorageError),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Invalid backup
    #[error("Invalid backup: {0}")]
    Invalid(String),

    /// Backup not found
    #[error("Backup not found: {0}")]
    NotFound(String),

    /// Restore error
    #[error("Restore error: {0}")]
    Restore(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Backup type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackupType {
    /// Full database backup
    Full,
    /// Incremental backup (AOF-based)
    Incremental,
    /// Point-in-time snapshot
    PointInTime,
}

impl Default for BackupType {
    fn default() -> Self {
        Self::Full
    }
}

/// Backup configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupConfig {
    /// Backup storage directory
    pub backup_dir: PathBuf,
    /// Enable compression
    pub compress: bool,
    /// Maximum number of backups to retain
    pub max_backups: usize,
    /// Enable automatic backups
    pub auto_backup_enabled: bool,
    /// Auto backup interval in seconds
    pub auto_backup_interval: u64,
    /// Minimum changes before auto backup triggers
    pub min_changes_for_backup: u64,
    /// Enable cloud backup
    pub cloud_backup_enabled: bool,
    /// Cloud backup prefix
    pub cloud_prefix: Option<String>,
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            backup_dir: PathBuf::from("./backups"),
            compress: true,
            max_backups: 10,
            auto_backup_enabled: false,
            auto_backup_interval: 3600, // 1 hour
            min_changes_for_backup: 1000,
            cloud_backup_enabled: false,
            cloud_prefix: None,
        }
    }
}

/// Backup metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupInfo {
    /// Backup name/identifier
    pub name: String,
    /// Backup type
    pub backup_type: BackupType,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Size in bytes (compressed if applicable)
    pub size: u64,
    /// Uncompressed size
    pub uncompressed_size: u64,
    /// Number of keys in backup
    pub key_count: u64,
    /// Database indexes included
    pub databases: Vec<u8>,
    /// Server version at backup time
    pub server_version: String,
    /// Is compressed
    pub compressed: bool,
    /// Optional description
    pub description: Option<String>,
    /// AOF sequence number at time of backup (for incremental backups)
    #[serde(default)]
    pub aof_sequence: Option<u64>,
    /// Base backup name (for incremental backups)
    #[serde(default)]
    pub base_backup: Option<String>,
    /// Chain position (0 = base, 1+ = incremental number)
    #[serde(default)]
    pub chain_position: u32,
}

/// Incremental backup chain information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupChain {
    /// Base full backup name
    pub base_backup: String,
    /// Ordered list of incremental backups in the chain
    pub incrementals: Vec<String>,
    /// Total size of the chain
    pub total_size: u64,
    /// Timestamp of the oldest backup in chain
    pub oldest_timestamp: DateTime<Utc>,
    /// Timestamp of the newest backup in chain
    pub newest_timestamp: DateTime<Utc>,
}

impl BackupChain {
    /// Create a new backup chain from a base backup
    pub fn new(base_backup: &BackupInfo) -> Self {
        Self {
            base_backup: base_backup.name.clone(),
            incrementals: Vec::new(),
            total_size: base_backup.size,
            oldest_timestamp: base_backup.created_at,
            newest_timestamp: base_backup.created_at,
        }
    }

    /// Add an incremental backup to the chain
    pub fn add_incremental(&mut self, backup: &BackupInfo) {
        self.incrementals.push(backup.name.clone());
        self.total_size += backup.size;
        self.newest_timestamp = backup.created_at;
    }

    /// Get the number of backups in the chain (including base)
    pub fn len(&self) -> usize {
        1 + self.incrementals.len()
    }

    /// Check if chain is empty (should never be, as we always have base)
    pub fn is_empty(&self) -> bool {
        false
    }
}

/// Backup result
pub type BackupResult<T> = Result<T, BackupError>;

/// Restore options
#[derive(Debug, Clone, Default)]
pub struct RestoreOptions {
    /// Only restore specific databases
    pub databases: Option<Vec<u8>>,
    /// Clear existing data before restore
    pub clear_existing: bool,
    /// Restore to a different database
    pub target_db: Option<u8>,
    /// Dry run (validate but don't apply)
    pub dry_run: bool,
}

/// Restore result
#[derive(Debug, Clone)]
pub struct RestoreResult {
    /// Number of keys restored
    pub keys_restored: u64,
    /// Databases restored
    pub databases_restored: Vec<u8>,
    /// Duration of restore operation
    pub duration: Duration,
    /// Errors encountered (non-fatal)
    pub errors: Vec<String>,
    /// Was this a dry run
    pub dry_run: bool,
}

/// Backup file header magic number
const BACKUP_MAGIC: &[u8] = b"FERRITE_BKP";
/// Backup format version
const BACKUP_VERSION: u8 = 1;

/// Backup manager
pub struct BackupManager {
    /// Configuration
    config: BackupConfig,
    /// Local storage backend
    local_storage: Arc<dyn BackupStorage>,
    /// Cloud storage backend (optional)
    cloud_storage: Option<Arc<dyn BackupStorage>>,
    /// Store reference for backups
    store: Arc<Store>,
    /// Backup history
    backup_history: RwLock<Vec<BackupInfo>>,
    /// Changes since last backup
    changes_since_backup: RwLock<u64>,
    /// Shutdown signal (kept alive for broadcast receivers)
    _shutdown_tx: broadcast::Sender<()>,
    /// AOF path for incremental backups
    aof_path: Option<PathBuf>,
    /// Last AOF sequence number at backup time
    last_aof_sequence: RwLock<u64>,
    /// Last full backup name (for incremental chain)
    last_full_backup: RwLock<Option<String>>,
    /// Backup chains indexed by base backup name
    backup_chains: RwLock<HashMap<String, BackupChain>>,
}

impl BackupManager {
    /// Create a new backup manager
    pub fn new(config: BackupConfig, store: Arc<Store>) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);

        let local_storage: Arc<dyn BackupStorage> = Arc::new(
            LocalBackupStorage::new(config.backup_dir.clone())
                .with_compression(config.compress)
                .with_max_backups(config.max_backups),
        );

        Self {
            config,
            local_storage,
            cloud_storage: None,
            store,
            backup_history: RwLock::new(Vec::new()),
            changes_since_backup: RwLock::new(0),
            _shutdown_tx: shutdown_tx,
            aof_path: None,
            last_aof_sequence: RwLock::new(0),
            last_full_backup: RwLock::new(None),
            backup_chains: RwLock::new(HashMap::new()),
        }
    }

    /// Set the AOF path for incremental backups
    pub fn with_aof_path(mut self, path: PathBuf) -> Self {
        self.aof_path = Some(path);
        self
    }

    /// Set cloud storage backend
    pub fn with_cloud_storage(mut self, storage: Arc<dyn BackupStorage>) -> Self {
        self.cloud_storage = Some(storage);
        self
    }

    /// Record a change (for tracking min_changes_for_backup)
    pub fn record_change(&self) {
        *self.changes_since_backup.write() += 1;
    }

    /// Get changes since last backup
    pub fn changes_since_backup(&self) -> u64 {
        *self.changes_since_backup.read()
    }

    /// Create a full backup
    pub async fn create_backup(&self, description: Option<String>) -> BackupResult<BackupInfo> {
        let backup_name = self.generate_backup_name();
        info!("Creating full backup: {}", backup_name);

        let start_time = std::time::Instant::now();

        // Get current AOF sequence number
        let aof_sequence = self.get_current_aof_sequence();

        // Serialize the store data
        let (data, key_count, databases) = self.serialize_store()?;
        let uncompressed_size = data.len() as u64;

        // Store the backup
        self.local_storage.store(&backup_name, &data).await?;

        // Get actual stored size (may be compressed)
        let size = self.local_storage.size(&backup_name).await?;

        // Also store to cloud if enabled
        if let Some(cloud) = &self.cloud_storage {
            debug!("Storing backup to cloud: {}", backup_name);
            if let Err(e) = cloud.store(&backup_name, &data).await {
                warn!("Failed to store backup to cloud: {}", e);
            }
        }

        let backup_info = BackupInfo {
            name: backup_name.clone(),
            backup_type: BackupType::Full,
            created_at: Utc::now(),
            size,
            uncompressed_size,
            key_count,
            databases,
            server_version: env!("CARGO_PKG_VERSION").to_string(),
            compressed: self.config.compress,
            description,
            aof_sequence: Some(aof_sequence),
            base_backup: None,
            chain_position: 0,
        };

        // Record in history
        self.backup_history.write().push(backup_info.clone());

        // Update tracking for incremental backups
        *self.last_aof_sequence.write() = aof_sequence;
        *self.last_full_backup.write() = Some(backup_name.clone());

        // Create a new backup chain for this full backup
        let chain = BackupChain::new(&backup_info);
        self.backup_chains
            .write()
            .insert(backup_name.clone(), chain);

        // Reset change counter
        *self.changes_since_backup.write() = 0;

        info!(
            "Backup created: {} ({} keys, {} bytes, {:?})",
            backup_info.name,
            key_count,
            size,
            start_time.elapsed()
        );

        Ok(backup_info)
    }

    /// Get the current AOF sequence number (file size as proxy for position)
    fn get_current_aof_sequence(&self) -> u64 {
        if let Some(ref aof_path) = self.aof_path {
            match std::fs::metadata(aof_path) {
                Ok(metadata) => metadata.len(),
                Err(_) => 0,
            }
        } else {
            // Use timestamp as fallback if no AOF path
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64
        }
    }

    /// Create an incremental backup based on the last full backup
    ///
    /// Incremental backups store only the AOF entries since the last backup,
    /// allowing for faster backups and point-in-time recovery.
    pub async fn create_incremental_backup(
        &self,
        description: Option<String>,
    ) -> BackupResult<BackupInfo> {
        // Get the base backup name
        let base_backup = self.last_full_backup.read().clone().ok_or_else(|| {
            BackupError::Invalid("No base backup exists. Create a full backup first.".to_string())
        })?;

        // Get the AOF path
        let aof_path = self.aof_path.as_ref().ok_or_else(|| {
            BackupError::Invalid("AOF path not configured for incremental backups".to_string())
        })?;

        let last_sequence = *self.last_aof_sequence.read();
        let current_sequence = self.get_current_aof_sequence();

        if current_sequence <= last_sequence {
            return Err(BackupError::Invalid(
                "No new AOF entries since last backup".to_string(),
            ));
        }

        let backup_name = self.generate_incremental_backup_name();
        info!(
            "Creating incremental backup: {} (base: {}, AOF range: {}-{})",
            backup_name, base_backup, last_sequence, current_sequence
        );

        let start_time = std::time::Instant::now();

        // Read AOF entries since last backup
        let aof_data = self.read_aof_range(aof_path, last_sequence, current_sequence)?;
        let entry_count = self.count_aof_entries(&aof_data);
        let uncompressed_size = aof_data.len() as u64;

        // Create incremental backup data structure
        let incremental_data =
            self.serialize_incremental(&aof_data, &base_backup, last_sequence)?;

        // Store the backup
        self.local_storage
            .store(&backup_name, &incremental_data)
            .await?;

        // Get actual stored size
        let size = self.local_storage.size(&backup_name).await?;

        // Store to cloud if enabled
        if let Some(cloud) = &self.cloud_storage {
            debug!("Storing incremental backup to cloud: {}", backup_name);
            if let Err(e) = cloud.store(&backup_name, &incremental_data).await {
                warn!("Failed to store incremental backup to cloud: {}", e);
            }
        }

        // Get chain position
        let chain_position = {
            let chains = self.backup_chains.read();
            chains
                .get(&base_backup)
                .map(|c| (c.incrementals.len() + 1) as u32)
                .unwrap_or(1)
        };

        let backup_info = BackupInfo {
            name: backup_name.clone(),
            backup_type: BackupType::Incremental,
            created_at: Utc::now(),
            size,
            uncompressed_size,
            key_count: entry_count,
            databases: vec![], // Incremental doesn't track databases
            server_version: env!("CARGO_PKG_VERSION").to_string(),
            compressed: self.config.compress,
            description,
            aof_sequence: Some(current_sequence),
            base_backup: Some(base_backup.clone()),
            chain_position,
        };

        // Record in history
        self.backup_history.write().push(backup_info.clone());

        // Update chain
        if let Some(chain) = self.backup_chains.write().get_mut(&base_backup) {
            chain.add_incremental(&backup_info);
        }

        // Update last sequence
        *self.last_aof_sequence.write() = current_sequence;

        // Reset change counter
        *self.changes_since_backup.write() = 0;

        info!(
            "Incremental backup created: {} ({} entries, {} bytes, {:?})",
            backup_info.name,
            entry_count,
            size,
            start_time.elapsed()
        );

        Ok(backup_info)
    }

    /// Read AOF data from a specific byte range
    fn read_aof_range(&self, path: &PathBuf, start: u64, end: u64) -> BackupResult<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};

        let mut file = std::fs::File::open(path)?;
        file.seek(SeekFrom::Start(start))?;

        let length = (end - start) as usize;
        let mut buffer = vec![0u8; length];
        file.read_exact(&mut buffer)?;

        Ok(buffer)
    }

    /// Count the number of AOF entries in raw data
    fn count_aof_entries(&self, data: &[u8]) -> u64 {
        let mut count = 0u64;
        let mut offset = 0;

        while offset + 4 <= data.len() {
            let len = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;

            if offset + 4 + len > data.len() {
                break;
            }

            count += 1;
            offset += 4 + len;
        }

        count
    }

    /// Serialize incremental backup data
    fn serialize_incremental(
        &self,
        aof_data: &[u8],
        base_backup: &str,
        start_sequence: u64,
    ) -> BackupResult<Vec<u8>> {
        let mut buffer = BytesMut::new();

        // Write incremental header
        buffer.put_slice(b"FERRITE_INC");
        buffer.put_u8(1); // Version

        // Write base backup reference
        let base_bytes = base_backup.as_bytes();
        buffer.put_u32(base_bytes.len() as u32);
        buffer.put_slice(base_bytes);

        // Write sequence info
        buffer.put_u64(start_sequence);

        // Write AOF data length and data
        buffer.put_u64(aof_data.len() as u64);
        buffer.put_slice(aof_data);

        // Write metadata
        let metadata = serde_json::json!({
            "created_at": Utc::now().to_rfc3339(),
            "entry_count": self.count_aof_entries(aof_data),
        });
        let metadata_json =
            serde_json::to_vec(&metadata).map_err(|e| BackupError::Serialization(e.to_string()))?;
        buffer.put_u32(metadata_json.len() as u32);
        buffer.put_slice(&metadata_json);

        Ok(buffer.to_vec())
    }

    /// Generate incremental backup name
    fn generate_incremental_backup_name(&self) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        format!("ferrite-inc-{}", now)
    }

    /// Restore from a backup with its incremental chain
    ///
    /// This method restores the base backup first, then applies all incremental
    /// backups in order to reach the desired point in time.
    pub async fn restore_with_incrementals(
        &self,
        target_backup: &str,
        options: RestoreOptions,
    ) -> BackupResult<RestoreResult> {
        let start_time = std::time::Instant::now();

        // Get backup info to determine if it's incremental
        let backup_info = self.get_backup_info(target_backup).await?;

        match backup_info.backup_type {
            BackupType::Full => {
                // Just restore the full backup
                self.restore(target_backup, options).await
            }
            BackupType::Incremental => {
                // Get the base backup name
                let base_name = backup_info.base_backup.as_ref().ok_or_else(|| {
                    BackupError::Invalid("Incremental backup missing base reference".to_string())
                })?;

                info!(
                    "Restoring incremental backup {} with base {}",
                    target_backup, base_name
                );

                // Get the backup chain
                let chain = self.get_backup_chain(base_name).await?;

                // Determine which incrementals to apply
                let incrementals_to_apply: Vec<_> = chain
                    .incrementals
                    .iter()
                    .take_while(|name| *name != target_backup)
                    .chain(std::iter::once(&target_backup.to_string()))
                    .cloned()
                    .collect();

                // First restore the base backup
                let mut total_keys_restored = 0u64;
                let mut all_databases_restored = Vec::new();
                let mut all_errors = Vec::new();

                if !options.dry_run {
                    info!("Restoring base backup: {}", base_name);
                    let base_result = self.restore(base_name, options.clone()).await?;
                    total_keys_restored += base_result.keys_restored;
                    all_databases_restored.extend(base_result.databases_restored);
                    all_errors.extend(base_result.errors);
                }

                // Apply incrementals in order
                for inc_name in incrementals_to_apply {
                    info!("Applying incremental backup: {}", inc_name);
                    let inc_result = self.apply_incremental(&inc_name, &options).await?;
                    total_keys_restored += inc_result.keys_restored;
                    all_errors.extend(inc_result.errors);
                }

                Ok(RestoreResult {
                    keys_restored: total_keys_restored,
                    databases_restored: all_databases_restored,
                    duration: start_time.elapsed(),
                    errors: all_errors,
                    dry_run: options.dry_run,
                })
            }
            BackupType::PointInTime => {
                // Handle as full backup for now
                self.restore(target_backup, options).await
            }
        }
    }

    /// Apply an incremental backup by replaying its AOF entries
    async fn apply_incremental(
        &self,
        backup_name: &str,
        _options: &RestoreOptions,
    ) -> BackupResult<RestoreResult> {
        let start_time = std::time::Instant::now();

        // Retrieve the incremental backup
        let data = self.local_storage.retrieve(backup_name).await?;

        // Parse incremental format
        let (aof_data, _base_backup, _start_seq) = self.deserialize_incremental(&data)?;

        // Replay AOF entries
        let entries = self.parse_aof_data(&aof_data);
        let entry_count = entries.len();

        crate::persistence::aof::replay_aof(entries, &self.store);

        Ok(RestoreResult {
            keys_restored: entry_count as u64,
            databases_restored: vec![],
            duration: start_time.elapsed(),
            errors: vec![],
            dry_run: false,
        })
    }

    /// Deserialize incremental backup data
    fn deserialize_incremental(&self, data: &[u8]) -> BackupResult<(Vec<u8>, String, u64)> {
        use std::io::Read;

        let mut cursor = std::io::Cursor::new(data);

        // Verify header
        let mut header = [0u8; 11];
        cursor.read_exact(&mut header)?;
        if &header != b"FERRITE_INC" {
            return Err(BackupError::Invalid(
                "Invalid incremental backup header".to_string(),
            ));
        }

        // Read version
        let mut version = [0u8; 1];
        cursor.read_exact(&mut version)?;
        if version[0] != 1 {
            return Err(BackupError::Invalid(format!(
                "Unsupported incremental backup version: {}",
                version[0]
            )));
        }

        // Read base backup name (big-endian to match BytesMut::put_u32)
        let mut len_buf = [0u8; 4];
        cursor.read_exact(&mut len_buf)?;
        let base_len = u32::from_be_bytes(len_buf) as usize;
        let mut base_bytes = vec![0u8; base_len];
        cursor.read_exact(&mut base_bytes)?;
        let base_backup = String::from_utf8_lossy(&base_bytes).to_string();

        // Read start sequence (big-endian to match BytesMut::put_u64)
        let mut seq_buf = [0u8; 8];
        cursor.read_exact(&mut seq_buf)?;
        let start_sequence = u64::from_be_bytes(seq_buf);

        // Read AOF data length and data (big-endian)
        cursor.read_exact(&mut seq_buf)?;
        let aof_len = u64::from_be_bytes(seq_buf) as usize;
        let mut aof_data = vec![0u8; aof_len];
        cursor.read_exact(&mut aof_data)?;

        Ok((aof_data, base_backup, start_sequence))
    }

    /// Parse raw AOF data into entries
    fn parse_aof_data(&self, data: &[u8]) -> Vec<crate::persistence::aof::AofEntry> {
        let mut entries = Vec::new();
        let mut offset = 0;

        while offset + 4 <= data.len() {
            let len = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;

            if offset + 4 + len > data.len() {
                break;
            }

            let entry_data = &data[offset + 4..offset + 4 + len];
            if let Ok(entry) = crate::persistence::aof::AofEntry::deserialize(entry_data) {
                entries.push(entry);
            }

            offset += 4 + len;
        }

        entries
    }

    /// Get backup chain for a base backup
    pub async fn get_backup_chain(&self, base_backup: &str) -> BackupResult<BackupChain> {
        // First check cached chains
        if let Some(chain) = self.backup_chains.read().get(base_backup) {
            return Ok(chain.clone());
        }

        // Build chain from backup history
        let base_info = self.get_backup_info(base_backup).await?;
        if base_info.backup_type != BackupType::Full {
            return Err(BackupError::Invalid(format!(
                "{} is not a full backup",
                base_backup
            )));
        }

        let mut chain = BackupChain::new(&base_info);

        // Find all incrementals for this base
        let backups = self.list_backups().await?;
        for backup in backups {
            if backup.backup_type == BackupType::Incremental {
                if let Some(ref base) = backup.base_backup {
                    if base == base_backup {
                        chain.add_incremental(&backup);
                    }
                }
            }
        }

        // Cache the chain
        self.backup_chains
            .write()
            .insert(base_backup.to_string(), chain.clone());

        Ok(chain)
    }

    /// List all backup chains
    pub async fn list_backup_chains(&self) -> BackupResult<Vec<BackupChain>> {
        let backups = self.list_backups().await?;
        let mut chains = Vec::new();

        for backup in backups {
            if backup.backup_type == BackupType::Full {
                let chain = self.get_backup_chain(&backup.name).await?;
                chains.push(chain);
            }
        }

        Ok(chains)
    }

    /// Restore from a backup
    pub async fn restore(
        &self,
        backup_name: &str,
        options: RestoreOptions,
    ) -> BackupResult<RestoreResult> {
        info!("Restoring from backup: {}", backup_name);
        let start_time = std::time::Instant::now();

        // Retrieve the backup
        let data = self.local_storage.retrieve(backup_name).await?;

        // Parse and validate
        let (entries, metadata) = self.deserialize_backup(&data)?;

        let mut keys_restored = 0u64;
        let mut databases_restored = Vec::new();
        let mut errors = Vec::new();

        if options.dry_run {
            info!("Dry run - would restore {} keys", entries.len());
            return Ok(RestoreResult {
                keys_restored: entries.len() as u64,
                databases_restored: metadata.databases.clone(),
                duration: start_time.elapsed(),
                errors,
                dry_run: true,
            });
        }

        // Clear existing data if requested
        if options.clear_existing {
            // Note: Would need a clear method on Store
            debug!("Clear existing data requested (not implemented)");
        }

        // Restore entries
        for entry in entries {
            let target_db = options.target_db.unwrap_or(entry.database);

            // Check if we should restore this database
            if let Some(ref dbs) = options.databases {
                if !dbs.contains(&entry.database) {
                    continue;
                }
            }

            // Restore the key
            match self.restore_entry(target_db, &entry) {
                Ok(_) => {
                    keys_restored += 1;
                    if !databases_restored.contains(&target_db) {
                        databases_restored.push(target_db);
                    }
                }
                Err(e) => {
                    errors.push(format!("Failed to restore key {}: {}", entry.key, e));
                }
            }
        }

        let duration = start_time.elapsed();
        info!(
            "Restore complete: {} keys restored in {:?}",
            keys_restored, duration
        );

        Ok(RestoreResult {
            keys_restored,
            databases_restored,
            duration,
            errors,
            dry_run: false,
        })
    }

    /// List available backups
    pub async fn list_backups(&self) -> BackupResult<Vec<BackupInfo>> {
        let backup_names = self.local_storage.list().await?;
        let mut infos = Vec::new();

        for name in backup_names {
            if let Ok(data) = self.local_storage.retrieve(&name).await {
                if let Ok((_, mut metadata)) = self.deserialize_backup(&data) {
                    // Set the name from storage (metadata name is empty by default)
                    metadata.name = name;
                    infos.push(metadata);
                }
            }
        }

        // Sort by creation time (newest first)
        infos.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(infos)
    }

    /// Delete a backup
    pub async fn delete_backup(&self, backup_name: &str) -> BackupResult<()> {
        info!("Deleting backup: {}", backup_name);

        self.local_storage.delete(backup_name).await?;

        // Also delete from cloud if present
        if let Some(cloud) = &self.cloud_storage {
            if let Err(e) = cloud.delete(backup_name).await {
                warn!("Failed to delete backup from cloud: {}", e);
            }
        }

        // Remove from history
        self.backup_history
            .write()
            .retain(|b| b.name != backup_name);

        Ok(())
    }

    /// Get backup info
    pub async fn get_backup_info(&self, backup_name: &str) -> BackupResult<BackupInfo> {
        let data = self.local_storage.retrieve(backup_name).await?;
        let (_, metadata) = self.deserialize_backup(&data)?;
        Ok(metadata)
    }

    /// Check if auto backup should trigger
    pub fn should_auto_backup(&self) -> bool {
        self.config.auto_backup_enabled
            && *self.changes_since_backup.read() >= self.config.min_changes_for_backup
    }

    /// Generate a unique backup name
    fn generate_backup_name(&self) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        format!("ferrite-{}", now)
    }

    /// Serialize the store to backup format
    fn serialize_store(&self) -> BackupResult<(Vec<u8>, u64, Vec<u8>)> {
        let mut buffer = BytesMut::new();
        let mut key_count = 0u64;
        let mut databases = Vec::new();

        // Write header
        buffer.put_slice(BACKUP_MAGIC);
        buffer.put_u8(BACKUP_VERSION);

        // Reserve space for metadata (will fill in later)
        let metadata_pos = buffer.len();
        buffer.put_u64(0); // placeholder for entry count

        // Serialize all databases
        for db_index in 0u8..16 {
            let db_keys = self.store.keys(db_index);
            if db_keys.is_empty() {
                continue;
            }

            databases.push(db_index);

            for key in db_keys {
                if let Some(value) = self.store.get(db_index, &key) {
                    // Serialize entry - get TTL in seconds, convert to ms for backup
                    let ttl = self.store.ttl(db_index, &key).and_then(|t| {
                        if t > 0 {
                            Some(t * 1000) // Convert seconds to milliseconds
                        } else {
                            None
                        }
                    });
                    let entry = BackupEntry {
                        database: db_index,
                        key: String::from_utf8_lossy(&key).to_string(),
                        value: value.clone(),
                        ttl,
                    };

                    self.serialize_entry(&mut buffer, &entry)?;
                    key_count += 1;
                }
            }
        }

        // Fill in entry count
        let count_bytes = key_count.to_be_bytes();
        buffer[metadata_pos..metadata_pos + 8].copy_from_slice(&count_bytes);

        // Write footer with metadata
        let metadata = BackupInfo {
            name: String::new(), // Will be set by caller
            backup_type: BackupType::Full,
            created_at: Utc::now(),
            size: 0,
            uncompressed_size: buffer.len() as u64,
            key_count,
            databases: databases.clone(),
            server_version: env!("CARGO_PKG_VERSION").to_string(),
            compressed: self.config.compress,
            description: None,
            aof_sequence: None,
            base_backup: None,
            chain_position: 0,
        };

        let metadata_json =
            serde_json::to_vec(&metadata).map_err(|e| BackupError::Serialization(e.to_string()))?;
        buffer.put_u32(metadata_json.len() as u32);
        buffer.put_slice(&metadata_json);

        Ok((buffer.to_vec(), key_count, databases))
    }

    /// Serialize a single backup entry
    fn serialize_entry(&self, buffer: &mut BytesMut, entry: &BackupEntry) -> BackupResult<()> {
        // Entry format:
        // - database: u8
        // - key_len: u32
        // - key: [u8]
        // - value_type: u8
        // - value_len: u32
        // - value: [u8]
        // - has_ttl: u8
        // - ttl_ms: i64 (if has_ttl)

        buffer.put_u8(entry.database);

        // Key
        let key_bytes = entry.key.as_bytes();
        buffer.put_u32(key_bytes.len() as u32);
        buffer.put_slice(key_bytes);

        // Value
        match &entry.value {
            Value::String(s) => {
                buffer.put_u8(0); // String type
                buffer.put_u32(s.len() as u32);
                buffer.put_slice(s);
            }
            Value::List(list) => {
                buffer.put_u8(1); // List type
                buffer.put_u32(list.len() as u32);
                for item in list {
                    buffer.put_u32(item.len() as u32);
                    buffer.put_slice(item);
                }
            }
            Value::Hash(hash) => {
                buffer.put_u8(2); // Hash type
                buffer.put_u32(hash.len() as u32);
                for (k, v) in hash {
                    buffer.put_u32(k.len() as u32);
                    buffer.put_slice(k);
                    buffer.put_u32(v.len() as u32);
                    buffer.put_slice(v);
                }
            }
            Value::Set(set) => {
                buffer.put_u8(3); // Set type
                buffer.put_u32(set.len() as u32);
                for item in set {
                    buffer.put_u32(item.len() as u32);
                    buffer.put_slice(item);
                }
            }
            Value::SortedSet { by_member, .. } => {
                buffer.put_u8(4); // SortedSet type
                buffer.put_u32(by_member.len() as u32);
                for (member, score) in by_member {
                    buffer.put_u32(member.len() as u32);
                    buffer.put_slice(member);
                    buffer.put_f64(*score);
                }
            }
            Value::Stream(stream) => {
                buffer.put_u8(5); // Stream type
                buffer.put_u32(stream.entries.len() as u32);
                for (id, fields) in &stream.entries {
                    // Write entry ID
                    buffer.put_u64(id.ms);
                    buffer.put_u64(id.seq);
                    // Write fields
                    buffer.put_u32(fields.len() as u32);
                    for (k, v) in fields {
                        buffer.put_u32(k.len() as u32);
                        buffer.put_slice(k);
                        buffer.put_u32(v.len() as u32);
                        buffer.put_slice(v);
                    }
                }
                // Write last_id
                buffer.put_u64(stream.last_id.ms);
                buffer.put_u64(stream.last_id.seq);
            }
            Value::HyperLogLog(registers) => {
                buffer.put_u8(6); // HyperLogLog type
                buffer.put_u32(registers.len() as u32);
                buffer.put_slice(registers);
            }
        }

        // TTL
        match entry.ttl {
            Some(ttl) => {
                buffer.put_u8(1);
                buffer.put_i64(ttl);
            }
            None => {
                buffer.put_u8(0);
            }
        }

        Ok(())
    }

    /// Deserialize a backup
    fn deserialize_backup(&self, data: &[u8]) -> BackupResult<(Vec<BackupEntry>, BackupInfo)> {
        let mut cursor = std::io::Cursor::new(data);
        let mut buf = [0u8; 11];

        // Read and verify header
        std::io::Read::read_exact(&mut cursor, &mut buf[..BACKUP_MAGIC.len()])
            .map_err(|e| BackupError::Invalid(format!("Failed to read header: {}", e)))?;

        if &buf[..BACKUP_MAGIC.len()] != BACKUP_MAGIC {
            return Err(BackupError::Invalid(
                "Invalid backup magic number".to_string(),
            ));
        }

        let version = {
            let mut v = [0u8; 1];
            std::io::Read::read_exact(&mut cursor, &mut v)
                .map_err(|e| BackupError::Invalid(format!("Failed to read version: {}", e)))?;
            v[0]
        };

        if version != BACKUP_VERSION {
            return Err(BackupError::Invalid(format!(
                "Unsupported backup version: {}",
                version
            )));
        }

        // Read entry count
        let entry_count = {
            let mut buf = [0u8; 8];
            std::io::Read::read_exact(&mut cursor, &mut buf)
                .map_err(|e| BackupError::Invalid(format!("Failed to read entry count: {}", e)))?;
            u64::from_be_bytes(buf)
        };

        // Read entries
        let mut entries = Vec::with_capacity(entry_count as usize);
        for _ in 0..entry_count {
            let entry = self.deserialize_entry(&mut cursor)?;
            entries.push(entry);
        }

        // Read metadata
        let metadata_len = {
            let mut buf = [0u8; 4];
            std::io::Read::read_exact(&mut cursor, &mut buf).map_err(|e| {
                BackupError::Invalid(format!("Failed to read metadata length: {}", e))
            })?;
            u32::from_be_bytes(buf) as usize
        };

        let mut metadata_buf = vec![0u8; metadata_len];
        std::io::Read::read_exact(&mut cursor, &mut metadata_buf)
            .map_err(|e| BackupError::Invalid(format!("Failed to read metadata: {}", e)))?;

        let metadata: BackupInfo = serde_json::from_slice(&metadata_buf)
            .map_err(|e| BackupError::Serialization(e.to_string()))?;

        Ok((entries, metadata))
    }

    /// Deserialize a single entry
    fn deserialize_entry(&self, cursor: &mut std::io::Cursor<&[u8]>) -> BackupResult<BackupEntry> {
        use std::io::Read;

        // Database
        let database = {
            let mut buf = [0u8; 1];
            cursor.read_exact(&mut buf)?;
            buf[0]
        };

        // Key
        let key_len = {
            let mut buf = [0u8; 4];
            cursor.read_exact(&mut buf)?;
            u32::from_be_bytes(buf) as usize
        };
        let mut key_buf = vec![0u8; key_len];
        cursor.read_exact(&mut key_buf)?;
        let key = String::from_utf8_lossy(&key_buf).to_string();

        // Value type
        let value_type = {
            let mut buf = [0u8; 1];
            cursor.read_exact(&mut buf)?;
            buf[0]
        };

        let value = match value_type {
            0 => {
                // String
                let len = {
                    let mut buf = [0u8; 4];
                    cursor.read_exact(&mut buf)?;
                    u32::from_be_bytes(buf) as usize
                };
                let mut data = vec![0u8; len];
                cursor.read_exact(&mut data)?;
                Value::String(Bytes::from(data))
            }
            1 => {
                // List
                let count = {
                    let mut buf = [0u8; 4];
                    cursor.read_exact(&mut buf)?;
                    u32::from_be_bytes(buf) as usize
                };
                let mut list = std::collections::VecDeque::with_capacity(count);
                for _ in 0..count {
                    let len = {
                        let mut buf = [0u8; 4];
                        cursor.read_exact(&mut buf)?;
                        u32::from_be_bytes(buf) as usize
                    };
                    let mut data = vec![0u8; len];
                    cursor.read_exact(&mut data)?;
                    list.push_back(Bytes::from(data));
                }
                Value::List(list)
            }
            2 => {
                // Hash
                let count = {
                    let mut buf = [0u8; 4];
                    cursor.read_exact(&mut buf)?;
                    u32::from_be_bytes(buf) as usize
                };
                let mut hash = HashMap::with_capacity(count);
                for _ in 0..count {
                    let key_len = {
                        let mut buf = [0u8; 4];
                        cursor.read_exact(&mut buf)?;
                        u32::from_be_bytes(buf) as usize
                    };
                    let mut key_data = vec![0u8; key_len];
                    cursor.read_exact(&mut key_data)?;

                    let val_len = {
                        let mut buf = [0u8; 4];
                        cursor.read_exact(&mut buf)?;
                        u32::from_be_bytes(buf) as usize
                    };
                    let mut val_data = vec![0u8; val_len];
                    cursor.read_exact(&mut val_data)?;

                    hash.insert(Bytes::from(key_data), Bytes::from(val_data));
                }
                Value::Hash(hash)
            }
            3 => {
                // Set
                let count = {
                    let mut buf = [0u8; 4];
                    cursor.read_exact(&mut buf)?;
                    u32::from_be_bytes(buf) as usize
                };
                let mut set = std::collections::HashSet::with_capacity(count);
                for _ in 0..count {
                    let len = {
                        let mut buf = [0u8; 4];
                        cursor.read_exact(&mut buf)?;
                        u32::from_be_bytes(buf) as usize
                    };
                    let mut data = vec![0u8; len];
                    cursor.read_exact(&mut data)?;
                    set.insert(Bytes::from(data));
                }
                Value::Set(set)
            }
            4 => {
                // SortedSet
                let count = {
                    let mut buf = [0u8; 4];
                    cursor.read_exact(&mut buf)?;
                    u32::from_be_bytes(buf) as usize
                };
                let mut by_member = HashMap::with_capacity(count);
                let mut by_score = std::collections::BTreeMap::new();
                for _ in 0..count {
                    let member_len = {
                        let mut buf = [0u8; 4];
                        cursor.read_exact(&mut buf)?;
                        u32::from_be_bytes(buf) as usize
                    };
                    let mut member_data = vec![0u8; member_len];
                    cursor.read_exact(&mut member_data)?;
                    let member = Bytes::from(member_data);

                    let score = {
                        let mut buf = [0u8; 8];
                        cursor.read_exact(&mut buf)?;
                        f64::from_be_bytes(buf)
                    };

                    by_member.insert(member.clone(), score);
                    by_score.insert((ordered_float::OrderedFloat(score), member), ());
                }
                Value::SortedSet {
                    by_score,
                    by_member,
                }
            }
            5 => {
                // Stream
                use crate::storage::{Stream, StreamEntryId};
                use std::collections::BTreeMap;

                let entry_count = {
                    let mut buf = [0u8; 4];
                    cursor.read_exact(&mut buf)?;
                    u32::from_be_bytes(buf) as usize
                };

                let mut entries: BTreeMap<StreamEntryId, Vec<(Bytes, Bytes)>> = BTreeMap::new();
                for _ in 0..entry_count {
                    // Read entry ID
                    let ms = {
                        let mut buf = [0u8; 8];
                        cursor.read_exact(&mut buf)?;
                        u64::from_be_bytes(buf)
                    };
                    let seq = {
                        let mut buf = [0u8; 8];
                        cursor.read_exact(&mut buf)?;
                        u64::from_be_bytes(buf)
                    };
                    let id = StreamEntryId::new(ms, seq);

                    // Read fields
                    let field_count = {
                        let mut buf = [0u8; 4];
                        cursor.read_exact(&mut buf)?;
                        u32::from_be_bytes(buf) as usize
                    };

                    let mut fields = Vec::with_capacity(field_count);
                    for _ in 0..field_count {
                        let key_len = {
                            let mut buf = [0u8; 4];
                            cursor.read_exact(&mut buf)?;
                            u32::from_be_bytes(buf) as usize
                        };
                        let mut key_data = vec![0u8; key_len];
                        cursor.read_exact(&mut key_data)?;

                        let val_len = {
                            let mut buf = [0u8; 4];
                            cursor.read_exact(&mut buf)?;
                            u32::from_be_bytes(buf) as usize
                        };
                        let mut val_data = vec![0u8; val_len];
                        cursor.read_exact(&mut val_data)?;

                        fields.push((Bytes::from(key_data), Bytes::from(val_data)));
                    }

                    entries.insert(id, fields);
                }

                // Read last_id
                let last_ms = {
                    let mut buf = [0u8; 8];
                    cursor.read_exact(&mut buf)?;
                    u64::from_be_bytes(buf)
                };
                let last_seq = {
                    let mut buf = [0u8; 8];
                    cursor.read_exact(&mut buf)?;
                    u64::from_be_bytes(buf)
                };

                let stream = Stream {
                    entries,
                    last_id: StreamEntryId::new(last_ms, last_seq),
                    length: entry_count,
                    consumer_groups: std::collections::HashMap::new(),
                };
                Value::Stream(stream)
            }
            6 => {
                // HyperLogLog
                let len = {
                    let mut buf = [0u8; 4];
                    cursor.read_exact(&mut buf)?;
                    u32::from_be_bytes(buf) as usize
                };
                let mut registers = vec![0u8; len];
                cursor.read_exact(&mut registers)?;
                Value::HyperLogLog(registers)
            }
            _ => {
                return Err(BackupError::Invalid(format!(
                    "Unknown value type: {}",
                    value_type
                )))
            }
        };

        // TTL
        let has_ttl = {
            let mut buf = [0u8; 1];
            cursor.read_exact(&mut buf)?;
            buf[0] == 1
        };

        let ttl = if has_ttl {
            let mut buf = [0u8; 8];
            cursor.read_exact(&mut buf)?;
            Some(i64::from_be_bytes(buf))
        } else {
            None
        };

        Ok(BackupEntry {
            database,
            key,
            value,
            ttl,
        })
    }

    /// Restore a single entry
    fn restore_entry(&self, db: u8, entry: &BackupEntry) -> BackupResult<()> {
        let key = Bytes::from(entry.key.clone());

        // Set the value
        self.store.set(db, key.clone(), entry.value.clone());

        // Set TTL if present
        if let Some(ttl_ms) = entry.ttl {
            if ttl_ms > 0 {
                let expires_at = std::time::SystemTime::now() + Duration::from_millis(ttl_ms as u64);
                self.store.expire(db, &key, expires_at);
            }
        }

        Ok(())
    }
}

/// Internal backup entry structure
#[derive(Debug, Clone)]
struct BackupEntry {
    database: u8,
    key: String,
    value: Value,
    ttl: Option<i64>,
}

#[cfg(test)]
mod tests {
    #![allow(unused_imports, unused_variables)]
    use super::*;
    use tempfile::TempDir;

    fn create_test_manager(temp_dir: &TempDir) -> BackupManager {
        let config = BackupConfig {
            backup_dir: temp_dir.path().to_path_buf(),
            compress: false, // Easier to debug
            max_backups: 10,
            ..Default::default()
        };
        let store = Arc::new(Store::new(16));
        BackupManager::new(config, store)
    }

    #[tokio::test]
    async fn test_backup_empty_store() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        let backup = manager.create_backup(None).await.unwrap();
        assert_eq!(backup.key_count, 0);
        assert!(backup.databases.is_empty());
    }

    #[tokio::test]
    async fn test_backup_and_restore_strings() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        // Add some data
        manager
            .store
            .set(0, Bytes::from("key1"), Value::String(Bytes::from("value1")));
        manager
            .store
            .set(0, Bytes::from("key2"), Value::String(Bytes::from("value2")));
        manager
            .store
            .set(1, Bytes::from("key3"), Value::String(Bytes::from("value3")));

        // Create backup
        let backup = manager
            .create_backup(Some("Test backup".to_string()))
            .await
            .unwrap();
        assert_eq!(backup.key_count, 3);
        assert!(backup.databases.contains(&0));
        assert!(backup.databases.contains(&1));

        // Clear store (simulate loss)
        manager
            .store
            .del(0, &[Bytes::from("key1"), Bytes::from("key2")]);
        manager.store.del(1, &[Bytes::from("key3")]);

        // Restore
        let result = manager
            .restore(&backup.name, RestoreOptions::default())
            .await
            .unwrap();
        assert_eq!(result.keys_restored, 3);

        // Verify data
        assert_eq!(
            manager.store.get(0, &Bytes::from("key1")),
            Some(Value::String(Bytes::from("value1")))
        );
        assert_eq!(
            manager.store.get(0, &Bytes::from("key2")),
            Some(Value::String(Bytes::from("value2")))
        );
        assert_eq!(
            manager.store.get(1, &Bytes::from("key3")),
            Some(Value::String(Bytes::from("value3")))
        );
    }

    #[tokio::test]
    async fn test_backup_and_restore_lists() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        // Add list
        let list = std::collections::VecDeque::from([
            Bytes::from("a"),
            Bytes::from("b"),
            Bytes::from("c"),
        ]);
        manager
            .store
            .set(0, Bytes::from("mylist"), Value::List(list.clone()));

        // Backup and restore
        let backup = manager.create_backup(None).await.unwrap();
        manager.store.del(0, &[Bytes::from("mylist")]);
        manager
            .restore(&backup.name, RestoreOptions::default())
            .await
            .unwrap();

        // Verify
        match manager.store.get(0, &Bytes::from("mylist")) {
            Some(Value::List(restored)) => {
                assert_eq!(restored.len(), 3);
                assert_eq!(restored[0], Bytes::from("a"));
            }
            _ => panic!("Expected list"),
        }
    }

    #[tokio::test]
    async fn test_backup_and_restore_hashes() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        // Add hash
        let mut hash = HashMap::new();
        hash.insert(Bytes::from("field1"), Bytes::from("value1"));
        hash.insert(Bytes::from("field2"), Bytes::from("value2"));
        manager
            .store
            .set(0, Bytes::from("myhash"), Value::Hash(hash));

        // Backup and restore
        let backup = manager.create_backup(None).await.unwrap();
        manager.store.del(0, &[Bytes::from("myhash")]);
        manager
            .restore(&backup.name, RestoreOptions::default())
            .await
            .unwrap();

        // Verify
        match manager.store.get(0, &Bytes::from("myhash")) {
            Some(Value::Hash(restored)) => {
                assert_eq!(restored.len(), 2);
                assert_eq!(
                    restored.get(&Bytes::from("field1")),
                    Some(&Bytes::from("value1"))
                );
            }
            _ => panic!("Expected hash"),
        }
    }

    #[tokio::test]
    async fn test_list_backups() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        // Create multiple backups with 1.1 second delay to ensure different timestamps
        manager.create_backup(None).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        manager.create_backup(None).await.unwrap();

        let backups = manager.list_backups().await.unwrap();
        assert_eq!(backups.len(), 2);
    }

    #[tokio::test]
    async fn test_delete_backup() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        let backup = manager.create_backup(None).await.unwrap();
        assert!(manager.local_storage.exists(&backup.name).await.unwrap());

        manager.delete_backup(&backup.name).await.unwrap();
        assert!(!manager.local_storage.exists(&backup.name).await.unwrap());
    }

    #[tokio::test]
    async fn test_restore_dry_run() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        manager
            .store
            .set(0, Bytes::from("key1"), Value::String(Bytes::from("value1")));
        let backup = manager.create_backup(None).await.unwrap();
        manager.store.del(0, &[Bytes::from("key1")]);

        // Dry run should not restore
        let result = manager
            .restore(
                &backup.name,
                RestoreOptions {
                    dry_run: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert!(result.dry_run);
        assert_eq!(result.keys_restored, 1);
        assert!(manager.store.get(0, &Bytes::from("key1")).is_none());
    }

    #[tokio::test]
    async fn test_restore_specific_database() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        manager
            .store
            .set(0, Bytes::from("key0"), Value::String(Bytes::from("value0")));
        manager
            .store
            .set(1, Bytes::from("key1"), Value::String(Bytes::from("value1")));

        let backup = manager.create_backup(None).await.unwrap();
        manager.store.del(0, &[Bytes::from("key0")]);
        manager.store.del(1, &[Bytes::from("key1")]);

        // Only restore database 0
        let result = manager
            .restore(
                &backup.name,
                RestoreOptions {
                    databases: Some(vec![0]),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(result.keys_restored, 1);
        assert!(manager.store.get(0, &Bytes::from("key0")).is_some());
        assert!(manager.store.get(1, &Bytes::from("key1")).is_none());
    }

    #[test]
    fn test_change_tracking() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        assert_eq!(manager.changes_since_backup(), 0);

        manager.record_change();
        manager.record_change();
        manager.record_change();

        assert_eq!(manager.changes_since_backup(), 3);
    }

    #[test]
    fn test_backup_chain() {
        let backup_info = BackupInfo {
            name: "full-backup-1".to_string(),
            backup_type: BackupType::Full,
            created_at: Utc::now(),
            size: 1000,
            uncompressed_size: 2000,
            key_count: 50,
            databases: vec![0, 1],
            server_version: "0.1.0".to_string(),
            compressed: true,
            description: None,
            aof_sequence: Some(100),
            base_backup: None,
            chain_position: 0,
        };

        let mut chain = BackupChain::new(&backup_info);
        assert_eq!(chain.len(), 1);
        assert_eq!(chain.base_backup, "full-backup-1");
        assert!(chain.incrementals.is_empty());

        // Add incrementals
        let inc1 = BackupInfo {
            name: "inc-1".to_string(),
            backup_type: BackupType::Incremental,
            created_at: Utc::now(),
            size: 100,
            uncompressed_size: 200,
            key_count: 10,
            databases: vec![],
            server_version: "0.1.0".to_string(),
            compressed: true,
            description: None,
            aof_sequence: Some(200),
            base_backup: Some("full-backup-1".to_string()),
            chain_position: 1,
        };

        chain.add_incremental(&inc1);
        assert_eq!(chain.len(), 2);
        assert_eq!(chain.incrementals.len(), 1);
        assert_eq!(chain.total_size, 1100);
    }

    #[test]
    fn test_incremental_serialize_deserialize() {
        use std::io::Cursor;

        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        // Create some fake AOF data (length-prefixed entries)
        let mut aof_data = Vec::new();
        // Entry 1
        let entry1 = b"test entry 1";
        aof_data.extend_from_slice(&(entry1.len() as u32).to_le_bytes());
        aof_data.extend_from_slice(entry1);
        // Entry 2
        let entry2 = b"test entry 2";
        aof_data.extend_from_slice(&(entry2.len() as u32).to_le_bytes());
        aof_data.extend_from_slice(entry2);

        let base_backup = "ferrite-backup-123456789";
        let start_sequence = 1000u64;

        // Serialize
        let serialized = manager
            .serialize_incremental(&aof_data, base_backup, start_sequence)
            .unwrap();

        // Deserialize
        let (parsed_aof, parsed_base, parsed_seq) =
            manager.deserialize_incremental(&serialized).unwrap();

        assert_eq!(parsed_base, base_backup);
        assert_eq!(parsed_seq, start_sequence);
        assert_eq!(parsed_aof, aof_data);
    }

    #[test]
    fn test_count_aof_entries() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        // Create fake AOF data with 3 entries
        let mut aof_data = Vec::new();
        for i in 0..3 {
            let entry = format!("entry {}", i);
            aof_data.extend_from_slice(&(entry.len() as u32).to_le_bytes());
            aof_data.extend_from_slice(entry.as_bytes());
        }

        let count = manager.count_aof_entries(&aof_data);
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_backup_with_aof_sequence() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        manager
            .store
            .set(0, Bytes::from("key1"), Value::String(Bytes::from("value1")));

        let backup = manager.create_backup(None).await.unwrap();

        // Full backup should have chain_position 0
        assert_eq!(backup.chain_position, 0);
        assert!(backup.base_backup.is_none());
        assert!(backup.aof_sequence.is_some());

        // Check that backup chain was created
        let chains = manager.backup_chains.read();
        assert!(chains.contains_key(&backup.name));
    }

    #[tokio::test]
    async fn test_incremental_backup_without_base_fails() {
        let temp_dir = TempDir::new().unwrap();

        // Create manager with AOF path
        let aof_path = temp_dir.path().join("test.aof");
        std::fs::write(&aof_path, b"").unwrap();

        let config = BackupConfig {
            backup_dir: temp_dir.path().to_path_buf(),
            compress: false,
            ..Default::default()
        };
        let store = Arc::new(Store::new(16));
        let manager = BackupManager::new(config, store).with_aof_path(aof_path);

        // Should fail because no full backup exists
        let result = manager.create_incremental_backup(None).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("No base backup exists"));
    }

    #[tokio::test]
    async fn test_list_backup_chains() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        // Create a full backup
        manager
            .store
            .set(0, Bytes::from("key1"), Value::String(Bytes::from("value1")));
        let backup1 = manager
            .create_backup(Some("First backup".to_string()))
            .await
            .unwrap();

        // Create another full backup after delay
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let backup2 = manager
            .create_backup(Some("Second backup".to_string()))
            .await
            .unwrap();

        // List chains
        let chains = manager.list_backup_chains().await.unwrap();
        assert_eq!(chains.len(), 2);

        // Each chain should have the correct base backup
        let chain_bases: std::collections::HashSet<_> =
            chains.iter().map(|c| c.base_backup.clone()).collect();
        assert!(chain_bases.contains(&backup1.name));
        assert!(chain_bases.contains(&backup2.name));
    }

    // ==================== Additional Tests ====================

    // Tests for Sets
    #[tokio::test]
    async fn test_backup_and_restore_sets() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        // Add set
        let mut set = std::collections::HashSet::new();
        set.insert(Bytes::from("member1"));
        set.insert(Bytes::from("member2"));
        set.insert(Bytes::from("member3"));
        manager.store.set(0, Bytes::from("myset"), Value::Set(set));

        // Backup
        let backup = manager.create_backup(None).await.unwrap();
        assert_eq!(backup.key_count, 1);

        // Clear and restore
        manager.store.del(0, &[Bytes::from("myset")]);
        let result = manager
            .restore(&backup.name, RestoreOptions::default())
            .await
            .unwrap();
        assert_eq!(result.keys_restored, 1);

        // Verify
        match manager.store.get(0, &Bytes::from("myset")) {
            Some(Value::Set(restored)) => {
                assert_eq!(restored.len(), 3);
                assert!(restored.contains(&Bytes::from("member1")));
                assert!(restored.contains(&Bytes::from("member2")));
                assert!(restored.contains(&Bytes::from("member3")));
            }
            _ => panic!("Expected set"),
        }
    }

    #[tokio::test]
    async fn test_backup_and_restore_sorted_sets() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        // Add sorted set
        let mut by_score = std::collections::BTreeMap::new();
        let mut by_member = std::collections::HashMap::new();

        by_score.insert((ordered_float::OrderedFloat(1.0), Bytes::from("one")), ());
        by_score.insert((ordered_float::OrderedFloat(2.0), Bytes::from("two")), ());
        by_score.insert((ordered_float::OrderedFloat(3.0), Bytes::from("three")), ());
        by_member.insert(Bytes::from("one"), 1.0);
        by_member.insert(Bytes::from("two"), 2.0);
        by_member.insert(Bytes::from("three"), 3.0);

        manager.store.set(
            0,
            Bytes::from("myzset"),
            Value::SortedSet {
                by_score,
                by_member,
            },
        );

        // Backup
        let backup = manager.create_backup(None).await.unwrap();
        assert_eq!(backup.key_count, 1);

        // Clear and restore
        manager.store.del(0, &[Bytes::from("myzset")]);
        let result = manager
            .restore(&backup.name, RestoreOptions::default())
            .await
            .unwrap();
        assert_eq!(result.keys_restored, 1);

        // Verify
        match manager.store.get(0, &Bytes::from("myzset")) {
            Some(Value::SortedSet {
                by_score,
                by_member,
            }) => {
                assert_eq!(by_score.len(), 3);
                assert_eq!(by_member.len(), 3);
                assert_eq!(by_member.get(&Bytes::from("one")), Some(&1.0));
            }
            _ => panic!("Expected sorted set"),
        }
    }

    #[tokio::test]
    async fn test_backup_and_restore_with_ttl() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        // Add key with expiration using set_with_expiry
        let expiry = std::time::SystemTime::now() + Duration::from_secs(3600);
        manager.store.set_with_expiry(
            0,
            Bytes::from("expiring"),
            Value::String(Bytes::from("value")),
            expiry,
        );

        // Create backup
        let backup = manager.create_backup(None).await.unwrap();
        assert_eq!(backup.key_count, 1);

        // Clear and restore
        manager.store.del(0, &[Bytes::from("expiring")]);
        manager
            .restore(&backup.name, RestoreOptions::default())
            .await
            .unwrap();

        // Verify key exists
        assert!(manager.store.get(0, &Bytes::from("expiring")).is_some());
    }

    #[tokio::test]
    async fn test_backup_multiple_databases() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        // Add data to multiple databases
        for db in 0..5 {
            manager.store.set(
                db,
                Bytes::from(format!("key{}", db)),
                Value::String(Bytes::from(format!("value{}", db))),
            );
        }

        // Backup
        let backup = manager.create_backup(None).await.unwrap();
        assert_eq!(backup.key_count, 5);
        assert_eq!(backup.databases.len(), 5);

        // Clear all
        for db in 0..5 {
            manager.store.del(db, &[Bytes::from(format!("key{}", db))]);
        }

        // Restore
        let result = manager
            .restore(&backup.name, RestoreOptions::default())
            .await
            .unwrap();
        assert_eq!(result.keys_restored, 5);

        // Verify all databases
        for db in 0..5 {
            assert_eq!(
                manager.store.get(db, &Bytes::from(format!("key{}", db))),
                Some(Value::String(Bytes::from(format!("value{}", db))))
            );
        }
    }

    #[tokio::test]
    async fn test_backup_with_description() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        manager
            .store
            .set(0, Bytes::from("key"), Value::String(Bytes::from("value")));

        let description = "Test backup with custom description";
        let backup = manager
            .create_backup(Some(description.to_string()))
            .await
            .unwrap();

        assert_eq!(backup.description, Some(description.to_string()));
    }

    #[tokio::test]
    async fn test_restore_nonexistent_backup() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        let result = manager
            .restore("nonexistent_backup", RestoreOptions::default())
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_nonexistent_backup() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        let result = manager.delete_backup("nonexistent_backup").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_backup_large_value() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        // Create a large value (100KB)
        let large_value: String = "x".repeat(100_000);
        manager.store.set(
            0,
            Bytes::from("large_key"),
            Value::String(Bytes::from(large_value.clone())),
        );

        // Backup and restore
        let backup = manager.create_backup(None).await.unwrap();
        manager.store.del(0, &[Bytes::from("large_key")]);
        manager
            .restore(&backup.name, RestoreOptions::default())
            .await
            .unwrap();

        // Verify
        match manager.store.get(0, &Bytes::from("large_key")) {
            Some(Value::String(data)) => {
                assert_eq!(data.len(), 100_000);
            }
            _ => panic!("Expected string"),
        }
    }

    #[tokio::test]
    async fn test_backup_empty_list() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        // Add empty list
        let list: std::collections::VecDeque<Bytes> = std::collections::VecDeque::new();
        manager
            .store
            .set(0, Bytes::from("empty_list"), Value::List(list));

        // Backup
        let backup = manager.create_backup(None).await.unwrap();
        manager.store.del(0, &[Bytes::from("empty_list")]);
        manager
            .restore(&backup.name, RestoreOptions::default())
            .await
            .unwrap();

        // Verify
        match manager.store.get(0, &Bytes::from("empty_list")) {
            Some(Value::List(restored)) => {
                assert!(restored.is_empty());
            }
            _ => panic!("Expected empty list"),
        }
    }

    #[tokio::test]
    async fn test_backup_empty_hash() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        // Add empty hash
        let hash: HashMap<Bytes, Bytes> = HashMap::new();
        manager
            .store
            .set(0, Bytes::from("empty_hash"), Value::Hash(hash));

        // Backup
        let backup = manager.create_backup(None).await.unwrap();
        manager.store.del(0, &[Bytes::from("empty_hash")]);
        manager
            .restore(&backup.name, RestoreOptions::default())
            .await
            .unwrap();

        // Verify
        match manager.store.get(0, &Bytes::from("empty_hash")) {
            Some(Value::Hash(restored)) => {
                assert!(restored.is_empty());
            }
            _ => panic!("Expected empty hash"),
        }
    }

    #[tokio::test]
    async fn test_backup_empty_set() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        // Add empty set
        let set: std::collections::HashSet<Bytes> = std::collections::HashSet::new();
        manager
            .store
            .set(0, Bytes::from("empty_set"), Value::Set(set));

        // Backup
        let backup = manager.create_backup(None).await.unwrap();
        manager.store.del(0, &[Bytes::from("empty_set")]);
        manager
            .restore(&backup.name, RestoreOptions::default())
            .await
            .unwrap();

        // Verify
        match manager.store.get(0, &Bytes::from("empty_set")) {
            Some(Value::Set(restored)) => {
                assert!(restored.is_empty());
            }
            _ => panic!("Expected empty set"),
        }
    }

    #[tokio::test]
    async fn test_backup_config_default() {
        let config = BackupConfig::default();
        assert!(!config.backup_dir.as_os_str().is_empty());
        assert!(config.max_backups > 0);
    }

    #[tokio::test]
    async fn test_restore_with_clear_existing() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        // Add data
        manager
            .store
            .set(0, Bytes::from("key1"), Value::String(Bytes::from("value1")));

        // Create backup
        let backup = manager.create_backup(None).await.unwrap();

        // Add more data (not in backup)
        manager
            .store
            .set(0, Bytes::from("key2"), Value::String(Bytes::from("value2")));

        // Restore with clear_existing
        manager
            .restore(
                &backup.name,
                RestoreOptions {
                    clear_existing: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // key1 should exist
        assert!(manager.store.get(0, &Bytes::from("key1")).is_some());
    }

    #[tokio::test]
    async fn test_backup_binary_data() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        // Create binary data with null bytes and high bytes
        let binary_data: Vec<u8> = vec![0x00, 0x01, 0xFF, 0xFE, 0x00, 0x10, 0x7F, 0x80];
        manager.store.set(
            0,
            Bytes::from("binary_key"),
            Value::String(Bytes::from(binary_data.clone())),
        );

        // Backup and restore
        let backup = manager.create_backup(None).await.unwrap();
        manager.store.del(0, &[Bytes::from("binary_key")]);
        manager
            .restore(&backup.name, RestoreOptions::default())
            .await
            .unwrap();

        // Verify
        match manager.store.get(0, &Bytes::from("binary_key")) {
            Some(Value::String(data)) => {
                assert_eq!(data.as_ref(), binary_data.as_slice());
            }
            _ => panic!("Expected string with binary data"),
        }
    }

    #[tokio::test]
    async fn test_backup_unicode_keys() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        // Unicode key and value
        let unicode_key = "キー日本語";
        let unicode_value = "値中文한국어";

        manager.store.set(
            0,
            Bytes::from(unicode_key),
            Value::String(Bytes::from(unicode_value)),
        );

        // Backup and restore
        let backup = manager.create_backup(None).await.unwrap();
        manager.store.del(0, &[Bytes::from(unicode_key)]);
        manager
            .restore(&backup.name, RestoreOptions::default())
            .await
            .unwrap();

        // Verify
        match manager.store.get(0, &Bytes::from(unicode_key)) {
            Some(Value::String(data)) => {
                assert_eq!(data.as_ref(), unicode_value.as_bytes());
            }
            _ => panic!("Expected string with unicode"),
        }
    }

    #[tokio::test]
    async fn test_backup_info_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let manager = create_test_manager(&temp_dir);

        manager
            .store
            .set(0, Bytes::from("key"), Value::String(Bytes::from("value")));

        let backup = manager
            .create_backup(Some("Metadata test".to_string()))
            .await
            .unwrap();

        // Verify backup metadata
        assert!(!backup.name.is_empty());
        assert!(backup.size > 0);
        assert_eq!(backup.key_count, 1);
        assert!(backup.databases.contains(&0));
        assert_eq!(backup.description, Some("Metadata test".to_string()));
        assert_eq!(backup.backup_type, BackupType::Full);
    }

    #[tokio::test]
    async fn test_concurrent_backup_and_access() {
        use std::sync::Arc as StdArc;
        use tokio::task::JoinSet;

        let temp_dir = TempDir::new().unwrap();
        let manager = StdArc::new(create_test_manager(&temp_dir));

        // Add initial data
        for i in 0..10 {
            manager.store.set(
                0,
                Bytes::from(format!("key{}", i)),
                Value::String(Bytes::from(format!("value{}", i))),
            );
        }

        let mut join_set = JoinSet::new();

        // Spawn backup task
        let manager_clone = manager.clone();
        join_set.spawn(async move {
            manager_clone.create_backup(None).await.unwrap();
        });

        // Spawn concurrent read tasks
        for i in 0..5 {
            let manager_clone = manager.clone();
            join_set.spawn(async move {
                let _ = manager_clone
                    .store
                    .get(0, &Bytes::from(format!("key{}", i)));
            });
        }

        // Wait for all tasks
        while (join_set.join_next().await).is_some() {}

        // Verify data is intact
        for i in 0..10 {
            assert!(manager
                .store
                .get(0, &Bytes::from(format!("key{}", i)))
                .is_some());
        }
    }

    #[test]
    fn test_restore_options_default() {
        let options = RestoreOptions::default();
        assert!(!options.dry_run);
        assert!(!options.clear_existing);
        assert!(options.databases.is_none());
    }

    #[tokio::test]
    async fn test_backup_manager_new() {
        let temp_dir = TempDir::new().unwrap();
        let config = BackupConfig {
            backup_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        let store = Arc::new(Store::new(16));
        let manager = BackupManager::new(config.clone(), store);

        // Verify manager was created with correct config
        assert_eq!(manager.config.backup_dir, config.backup_dir);
    }
}

/// Property tests for backup consistency (P34)
#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;
    use tempfile::TempDir;

    fn create_test_manager(temp_dir: &TempDir) -> BackupManager {
        let config = BackupConfig {
            backup_dir: temp_dir.path().to_path_buf(),
            compress: false,
            max_backups: 10,
            ..Default::default()
        };
        let store = Arc::new(Store::new(16));
        BackupManager::new(config, store)
    }

    // Strategy for generating valid keys
    fn key_strategy() -> impl Strategy<Value = String> {
        "[a-z][a-z0-9]{0,15}".prop_filter("non-empty", |s| !s.is_empty())
    }

    // Strategy for generating string values
    fn value_strategy() -> impl Strategy<Value = String> {
        prop::string::string_regex("[a-zA-Z0-9]{1,100}").unwrap()
    }

    proptest! {
        /// P34: Backup and restore should preserve all data
        #[test]
        fn prop_backup_restore_preserves_data(
            keys in prop::collection::vec(key_strategy(), 1..20),
            values in prop::collection::vec(value_strategy(), 1..20)
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            // Clone values to move into async block
            let keys_owned = keys.clone();
            let values_owned = values.clone();
            rt.block_on(async move {
                let temp_dir = TempDir::new().unwrap();
                let manager = create_test_manager(&temp_dir);

                // Store key-value pairs (use HashMap to deduplicate keys)
                use std::collections::HashMap as StdHashMap;
                let mut pairs_map: StdHashMap<Bytes, Bytes> = StdHashMap::new();
                for (k, v) in keys_owned.iter().zip(values_owned.iter()) {
                    pairs_map.insert(Bytes::from(k.clone()), Bytes::from(v.clone()));
                }
                let pairs: Vec<_> = pairs_map.into_iter().collect();

                for (key, value) in &pairs {
                    manager.store.set(
                        0,
                        key.clone(),
                        Value::String(value.clone()),
                    );
                }

                // Create backup
                let backup = manager.create_backup(None).await.unwrap();
                prop_assert_eq!(backup.key_count as usize, pairs.len());

                // Delete all keys
                for (key, _) in &pairs {
                    manager.store.del(0, &[key.clone()]);
                }

                // Verify deleted
                for (key, _) in &pairs {
                    prop_assert!(manager.store.get(0, key).is_none());
                }

                // Restore
                let result = manager.restore(&backup.name, RestoreOptions::default()).await.unwrap();
                prop_assert_eq!(result.keys_restored as usize, pairs.len());

                // Verify all data is restored correctly
                for (key, value) in &pairs {
                    let restored = manager.store.get(0, key);
                    prop_assert!(restored.is_some(), "Key {:?} should exist after restore", key);
                    if let Some(Value::String(s)) = restored {
                        prop_assert_eq!(
                            s.clone(),
                            value.clone(),
                            "Value mismatch for key {:?}", key
                        );
                    } else {
                        prop_assert!(false, "Expected string value for key {:?}", key);
                    }
                }

                Ok(())
            })?;
        }

        /// P34b: Backup metadata should be consistent
        #[test]
        fn prop_backup_metadata_consistent(
            num_keys in 0usize..50
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let temp_dir = TempDir::new().unwrap();
                let manager = create_test_manager(&temp_dir);

                // Store keys
                for i in 0..num_keys {
                    manager.store.set(
                        0,
                        Bytes::from(format!("key{}", i)),
                        Value::String(Bytes::from(format!("value{}", i))),
                    );
                }

                // Create backup
                let backup = manager.create_backup(None).await.unwrap();

                // Verify metadata
                prop_assert_eq!(backup.key_count as usize, num_keys);
                prop_assert!(backup.size > 0 || num_keys == 0);
                prop_assert!(backup.uncompressed_size > 0 || num_keys == 0);
                prop_assert_eq!(backup.backup_type, BackupType::Full);

                // Databases should contain 0 if we have keys
                if num_keys > 0 {
                    prop_assert!(backup.databases.contains(&0));
                }

                Ok(())
            })?;
        }

        /// P34c: Dry run should not modify data
        #[test]
        fn prop_dry_run_no_modification(
            key in key_strategy(),
            value in value_strategy()
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            // Clone values to move into async block
            let key_owned = key.clone();
            let value_owned = value.clone();
            rt.block_on(async move {
                let temp_dir = TempDir::new().unwrap();
                let manager = create_test_manager(&temp_dir);

                let key_bytes = Bytes::from(key_owned.clone());
                let value_bytes = Bytes::from(value_owned.clone());

                // Store a key
                manager.store.set(
                    0,
                    key_bytes.clone(),
                    Value::String(value_bytes),
                );

                // Create backup
                let backup = manager.create_backup(None).await.unwrap();

                // Delete the key
                manager.store.del(0, &[key_bytes.clone()]);
                prop_assert!(manager.store.get(0, &key_bytes).is_none());

                // Dry run restore
                let result = manager.restore(
                    &backup.name,
                    RestoreOptions {
                        dry_run: true,
                        ..Default::default()
                    },
                ).await.unwrap();

                prop_assert!(result.dry_run);
                prop_assert_eq!(result.keys_restored, 1);

                // Key should still not exist
                prop_assert!(
                    manager.store.get(0, &key_bytes).is_none(),
                    "Dry run should not modify data"
                );

                Ok(())
            })?;
        }
    }
}
