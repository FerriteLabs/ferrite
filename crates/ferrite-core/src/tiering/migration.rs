//! Migration execution for tiering operations
//!
//! This module handles the actual movement of data between storage tiers,
//! with rate limiting, progress tracking, and consistency guarantees.

use super::stats::StorageTier;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex, RwLock};

/// Direction of migration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationDirection {
    /// Moving to a faster/hotter tier
    Promote,
    /// Moving to a slower/colder tier
    Demote,
}

impl MigrationDirection {
    /// Get display name
    pub fn name(&self) -> &'static str {
        match self {
            MigrationDirection::Promote => "promote",
            MigrationDirection::Demote => "demote",
        }
    }
}

/// State of a migration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationState {
    /// Waiting to be executed
    Pending,
    /// Currently being migrated
    InProgress,
    /// Successfully completed
    Completed,
    /// Failed with error
    Failed,
    /// Cancelled before completion
    Cancelled,
}

impl MigrationState {
    /// Check if terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            MigrationState::Completed | MigrationState::Failed | MigrationState::Cancelled
        )
    }

    /// Get display name
    pub fn name(&self) -> &'static str {
        match self {
            MigrationState::Pending => "pending",
            MigrationState::InProgress => "in_progress",
            MigrationState::Completed => "completed",
            MigrationState::Failed => "failed",
            MigrationState::Cancelled => "cancelled",
        }
    }
}

/// A single migration operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Migration {
    /// Unique migration ID
    pub id: u64,
    /// Key to migrate
    pub key: Bytes,
    /// Source tier
    pub from_tier: StorageTier,
    /// Target tier
    pub to_tier: StorageTier,
    /// Direction
    pub direction: MigrationDirection,
    /// Current state
    pub state: MigrationState,
    /// Size in bytes
    pub size: u64,
    /// Created timestamp (millis since epoch)
    pub created_at: u64,
    /// Completed timestamp (millis since epoch)
    pub completed_at: Option<u64>,
    /// Error message if failed
    pub error: Option<String>,
}

impl Migration {
    /// Create a new migration
    pub fn new(
        id: u64,
        key: Bytes,
        from_tier: StorageTier,
        to_tier: StorageTier,
        size: u64,
    ) -> Self {
        let direction = if to_tier < from_tier {
            MigrationDirection::Promote
        } else {
            MigrationDirection::Demote
        };

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        Self {
            id,
            key,
            from_tier,
            to_tier,
            direction,
            state: MigrationState::Pending,
            size,
            created_at: now,
            completed_at: None,
            error: None,
        }
    }

    /// Mark as in progress
    pub fn start(&mut self) {
        self.state = MigrationState::InProgress;
    }

    /// Mark as completed
    pub fn complete(&mut self) {
        self.state = MigrationState::Completed;
        self.completed_at = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        );
    }

    /// Mark as failed
    pub fn fail(&mut self, error: &str) {
        self.state = MigrationState::Failed;
        self.error = Some(error.to_string());
        self.completed_at = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        );
    }

    /// Mark as cancelled
    pub fn cancel(&mut self) {
        self.state = MigrationState::Cancelled;
        self.completed_at = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        );
    }

    /// Get duration in milliseconds
    pub fn duration_ms(&self) -> Option<u64> {
        self.completed_at.map(|c| c.saturating_sub(self.created_at))
    }
}

/// Statistics for migration operations
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MigrationStats {
    /// Total migrations queued
    pub total_queued: u64,
    /// Total migrations completed
    pub total_completed: u64,
    /// Total migrations failed
    pub total_failed: u64,
    /// Total bytes migrated
    pub bytes_migrated: u64,
    /// Promotions completed
    pub promotions: u64,
    /// Demotions completed
    pub demotions: u64,
    /// Current queue size
    pub queue_size: u64,
    /// Average migration time (ms)
    pub avg_migration_time_ms: f64,
}

impl MigrationStats {
    /// Record a completed migration
    pub fn record_complete(&mut self, migration: &Migration) {
        self.total_completed += 1;
        self.bytes_migrated += migration.size;
        self.queue_size = self.queue_size.saturating_sub(1);

        if let Some(duration) = migration.duration_ms() {
            // Rolling average
            let n = self.total_completed as f64;
            self.avg_migration_time_ms =
                self.avg_migration_time_ms * ((n - 1.0) / n) + duration as f64 / n;
        }

        match migration.direction {
            MigrationDirection::Promote => self.promotions += 1,
            MigrationDirection::Demote => self.demotions += 1,
        }
    }

    /// Record a failed migration
    pub fn record_failed(&mut self) {
        self.total_failed += 1;
        self.queue_size = self.queue_size.saturating_sub(1);
    }

    /// Record a queued migration
    pub fn record_queued(&mut self) {
        self.total_queued += 1;
        self.queue_size += 1;
    }
}

/// Rate limiter for migrations
pub struct MigrationRateLimiter {
    /// Max migrations per second
    rate_limit: u32,
    /// Tokens available
    tokens: AtomicU64,
    /// Last refill time
    last_refill: Mutex<Instant>,
}

impl MigrationRateLimiter {
    /// Create a new rate limiter
    pub fn new(rate_limit: u32) -> Self {
        Self {
            rate_limit,
            tokens: AtomicU64::new(rate_limit as u64),
            last_refill: Mutex::new(Instant::now()),
        }
    }

    /// Try to acquire a token (non-blocking)
    pub async fn try_acquire(&self) -> bool {
        self.refill().await;

        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            if current == 0 {
                return false;
            }
            if self
                .tokens
                .compare_exchange(current, current - 1, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Wait until a token is available
    pub async fn acquire(&self) {
        loop {
            if self.try_acquire().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Refill tokens based on elapsed time
    async fn refill(&self) {
        let mut last_refill = self.last_refill.lock().await;
        let now = Instant::now();
        let elapsed = now.duration_since(*last_refill);

        if elapsed >= Duration::from_secs(1) {
            self.tokens.store(self.rate_limit as u64, Ordering::Relaxed);
            *last_refill = now;
        } else {
            let new_tokens = (elapsed.as_millis() as u64 * self.rate_limit as u64) / 1000;
            if new_tokens > 0 {
                let current = self.tokens.load(Ordering::Relaxed);
                let max = self.rate_limit as u64;
                self.tokens
                    .store((current + new_tokens).min(max), Ordering::Relaxed);
                *last_refill = now;
            }
        }
    }

    /// Get current rate limit
    pub fn rate_limit(&self) -> u32 {
        self.rate_limit
    }
}

/// Callback type for migration completion
#[allow(dead_code)] // Planned for v0.2 — type alias for migration completion callbacks
pub type MigrationCallback = Box<dyn Fn(&Migration) + Send + Sync>;

/// Migration executor handles background data movement
pub struct MigrationExecutor {
    /// Migration queue
    queue: Arc<RwLock<VecDeque<Migration>>>,
    /// Migration statistics
    stats: Arc<RwLock<MigrationStats>>,
    /// Rate limiter
    rate_limiter: Arc<MigrationRateLimiter>,
    /// Next migration ID
    next_id: AtomicU64,
    /// Shutdown signal sender
    #[allow(dead_code)] // Planned for v0.2 — stored for graceful executor shutdown
    shutdown_tx: Option<mpsc::Sender<()>>,
    /// Whether executor is running
    running: Arc<std::sync::atomic::AtomicBool>,
}

impl MigrationExecutor {
    /// Create a new migration executor
    pub fn new(rate_limit: u32) -> Self {
        Self {
            queue: Arc::new(RwLock::new(VecDeque::new())),
            stats: Arc::new(RwLock::new(MigrationStats::default())),
            rate_limiter: Arc::new(MigrationRateLimiter::new(rate_limit)),
            next_id: AtomicU64::new(1),
            shutdown_tx: None,
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Queue a migration
    pub async fn queue_migration(
        &self,
        key: Bytes,
        from_tier: StorageTier,
        to_tier: StorageTier,
        size: u64,
    ) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let migration = Migration::new(id, key, from_tier, to_tier, size);

        let mut queue = self.queue.write().await;
        queue.push_back(migration);

        let mut stats = self.stats.write().await;
        stats.record_queued();

        id
    }

    /// Get pending migrations count
    pub async fn pending_count(&self) -> usize {
        self.queue.read().await.len()
    }

    /// Get migration by ID
    pub async fn get_migration(&self, id: u64) -> Option<Migration> {
        self.queue.read().await.iter().find(|m| m.id == id).cloned()
    }

    /// Cancel a pending migration
    pub async fn cancel_migration(&self, id: u64) -> bool {
        let mut queue = self.queue.write().await;
        if let Some(idx) = queue
            .iter()
            .position(|m| m.id == id && m.state == MigrationState::Pending)
        {
            queue[idx].cancel();
            return true;
        }
        false
    }

    /// Get the next pending migration
    pub async fn next_pending(&self) -> Option<Migration> {
        let queue = self.queue.read().await;
        queue
            .iter()
            .find(|m| m.state == MigrationState::Pending)
            .cloned()
    }

    /// Mark a migration as started
    pub async fn start_migration(&self, id: u64) -> bool {
        let mut queue = self.queue.write().await;
        if let Some(migration) = queue.iter_mut().find(|m| m.id == id) {
            migration.start();
            return true;
        }
        false
    }

    /// Mark a migration as completed
    pub async fn complete_migration(&self, id: u64) -> bool {
        let mut queue = self.queue.write().await;
        if let Some(migration) = queue.iter_mut().find(|m| m.id == id) {
            migration.complete();
            let migration_clone = migration.clone();
            drop(queue);

            let mut stats = self.stats.write().await;
            stats.record_complete(&migration_clone);
            return true;
        }
        false
    }

    /// Mark a migration as failed
    pub async fn fail_migration(&self, id: u64, error: &str) -> bool {
        let mut queue = self.queue.write().await;
        if let Some(migration) = queue.iter_mut().find(|m| m.id == id) {
            migration.fail(error);
            drop(queue);

            let mut stats = self.stats.write().await;
            stats.record_failed();
            return true;
        }
        false
    }

    /// Get migration statistics
    pub async fn stats(&self) -> MigrationStats {
        self.stats.read().await.clone()
    }

    /// Check if rate limit allows another migration
    pub async fn rate_limit_allows(&self) -> bool {
        self.rate_limiter.try_acquire().await
    }

    /// Wait for rate limit
    pub async fn wait_for_rate_limit(&self) {
        self.rate_limiter.acquire().await;
    }

    /// Clean up completed migrations (keep last N)
    pub async fn cleanup_completed(&self, keep: usize) {
        let mut queue = self.queue.write().await;

        // Count completed
        let completed_count = queue.iter().filter(|m| m.state.is_terminal()).count();

        if completed_count > keep {
            let to_remove = completed_count - keep;
            let mut removed = 0;

            queue.retain(|m| {
                if removed < to_remove && m.state.is_terminal() {
                    removed += 1;
                    false
                } else {
                    true
                }
            });
        }
    }

    /// Get estimated time to complete all pending migrations
    pub async fn estimated_completion_time(&self) -> Duration {
        let stats = self.stats.read().await;
        let pending = self.pending_count().await;

        if pending == 0 {
            return Duration::ZERO;
        }

        if stats.avg_migration_time_ms > 0.0 && self.rate_limiter.rate_limit() > 0 {
            let rate = self.rate_limiter.rate_limit() as f64;
            let time_per_migration = (1000.0 / rate).max(stats.avg_migration_time_ms);
            Duration::from_millis((pending as f64 * time_per_migration) as u64)
        } else {
            // Assume 10ms per migration if no stats
            Duration::from_millis(pending as u64 * 10)
        }
    }

    /// Check if executor is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

impl Default for MigrationExecutor {
    fn default() -> Self {
        Self::new(100) // 100 migrations per second
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_new() {
        let migration = Migration::new(
            1,
            Bytes::from("key"),
            StorageTier::Memory,
            StorageTier::Ssd,
            1024,
        );

        assert_eq!(migration.id, 1);
        assert_eq!(migration.direction, MigrationDirection::Demote);
        assert_eq!(migration.state, MigrationState::Pending);
    }

    #[test]
    fn test_migration_promote_direction() {
        let migration = Migration::new(
            1,
            Bytes::from("key"),
            StorageTier::Cloud,
            StorageTier::Memory,
            1024,
        );

        assert_eq!(migration.direction, MigrationDirection::Promote);
    }

    #[test]
    fn test_migration_lifecycle() {
        let mut migration = Migration::new(
            1,
            Bytes::from("key"),
            StorageTier::Memory,
            StorageTier::Ssd,
            1024,
        );

        assert_eq!(migration.state, MigrationState::Pending);

        migration.start();
        assert_eq!(migration.state, MigrationState::InProgress);

        migration.complete();
        assert_eq!(migration.state, MigrationState::Completed);
        assert!(migration.completed_at.is_some());
    }

    #[test]
    fn test_migration_failure() {
        let mut migration = Migration::new(
            1,
            Bytes::from("key"),
            StorageTier::Memory,
            StorageTier::Cloud,
            1024,
        );

        migration.start();
        migration.fail("Connection error");

        assert_eq!(migration.state, MigrationState::Failed);
        assert_eq!(migration.error, Some("Connection error".to_string()));
    }

    #[tokio::test]
    async fn test_migration_executor() {
        let executor = MigrationExecutor::new(100);

        let id = executor
            .queue_migration(
                Bytes::from("key1"),
                StorageTier::Memory,
                StorageTier::Ssd,
                1024,
            )
            .await;

        assert_eq!(executor.pending_count().await, 1);

        let migration = executor.get_migration(id).await;
        assert!(migration.is_some());
        assert_eq!(migration.unwrap().state, MigrationState::Pending);
    }

    #[tokio::test]
    async fn test_migration_executor_complete() {
        let executor = MigrationExecutor::new(100);

        let id = executor
            .queue_migration(
                Bytes::from("key1"),
                StorageTier::Memory,
                StorageTier::Ssd,
                1024,
            )
            .await;

        executor.start_migration(id).await;
        executor.complete_migration(id).await;

        let stats = executor.stats().await;
        assert_eq!(stats.total_completed, 1);
        assert_eq!(stats.demotions, 1);
    }

    #[tokio::test]
    async fn test_migration_stats() {
        let mut stats = MigrationStats::default();

        stats.record_queued();
        assert_eq!(stats.queue_size, 1);

        let migration = Migration::new(
            1,
            Bytes::from("key"),
            StorageTier::Memory,
            StorageTier::Ssd,
            1024,
        );
        stats.record_complete(&migration);

        assert_eq!(stats.total_completed, 1);
        assert_eq!(stats.bytes_migrated, 1024);
        assert_eq!(stats.demotions, 1);
    }

    #[tokio::test]
    async fn test_rate_limiter() {
        let limiter = MigrationRateLimiter::new(10);

        // Should be able to acquire 10 tokens
        for _ in 0..10 {
            assert!(limiter.try_acquire().await);
        }

        // 11th should fail
        assert!(!limiter.try_acquire().await);
    }
}
