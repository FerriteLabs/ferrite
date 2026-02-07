//! io_uring Zero-Copy I/O Engine
//!
//! High-performance async I/O layer with io_uring support on Linux
//! and graceful fallback to tokio::fs on other platforms.
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────┐
//! │                    Application Layer                        │
//! │   (AOF, HybridLog, Checkpoints, RDB Snapshots)              │
//! └──────────────────────────┬─────────────────────────────────┘
//!                            │
//! ┌──────────────────────────▼─────────────────────────────────┐
//! │                   IoEngine Trait                            │
//! │   Unified interface for all I/O operations                  │
//! └──────────────────────────┬─────────────────────────────────┘
//!                            │
//!        ┌───────────────────┼───────────────────┐
//!        │                   │                   │
//!        ▼                   ▼                   ▼
//! ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
//! │  UringEngine │   │ FallbackEngine│   │ BatchEngine  │
//! │  (Linux 5.1+)│   │ (tokio::fs)  │   │ (batched ops)│
//! └──────────────┘   └──────────────┘   └──────────────┘
//! ```
//!
//! # Features
//!
//! - **Zero-copy reads/writes**: Direct kernel-userspace data transfer
//! - **Batched submissions**: Multiple I/O ops per syscall
//! - **Registered buffers**: Pre-registered memory for reduced overhead
//! - **Fixed file descriptors**: Reduced per-op fd lookup
//! - **Linked operations**: Dependent I/O chains
//!
//! # Platform Support
//!
//! | Platform | Engine | Notes |
//! |----------|--------|-------|
//! | Linux 5.1+ | io_uring | Full features |
//! | Linux < 5.1 | Fallback | tokio::fs |
//! | macOS | Fallback | tokio::fs |
//! | Windows | Fallback | tokio::fs |

mod batch;
mod buffer;
mod engine;
mod fallback;
pub mod simulation;

#[cfg(target_os = "linux")]
mod uring;

pub use batch::{BatchedReader, BatchedWriter, IoBatch};
pub use buffer::{IoBuffer, IoBufferPool, RegisteredBuffer, BUFFER_SIZE};
pub use engine::{
    BatchIoEngine, IoEngine, IoEngineConfig, IoOperation, IoResult, IoStats, ReadResult,
    WriteResult,
};
pub use fallback::{FallbackConfig, FallbackEngine};

#[cfg(target_os = "linux")]
pub use uring::{UringConfig, UringEngine, WriteCoalescer};

use std::path::Path;
use std::sync::Arc;

/// Shared I/O engine handle
pub type SharedIoEngine = Arc<dyn IoEngine>;

/// Create the optimal I/O engine for the current platform
pub fn create_engine(config: IoEngineConfig) -> std::io::Result<SharedIoEngine> {
    #[cfg(target_os = "linux")]
    {
        match UringEngine::new(config.clone()) {
            Ok(engine) => {
                tracing::info!("Using io_uring engine with {} entries", config.queue_depth);
                Ok(Arc::new(engine))
            }
            Err(e) => {
                tracing::warn!("io_uring unavailable, falling back: {}", e);
                Ok(Arc::new(FallbackEngine::new(config)?))
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        tracing::info!("Using fallback I/O engine (non-Linux platform)");
        Ok(Arc::new(FallbackEngine::new(config)?))
    }
}

/// Open a file with the best available engine
pub async fn open_file(
    _path: impl AsRef<Path>,
    config: IoEngineConfig,
) -> std::io::Result<SharedIoEngine> {
    create_engine(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_engine_config_defaults() {
        let config = IoEngineConfig::default();
        assert_eq!(config.queue_depth, 256);
        assert_eq!(config.buffer_count, 32);
    }

    #[tokio::test]
    async fn test_create_engine() {
        let config = IoEngineConfig::default();
        let engine = create_engine(config);
        assert!(engine.is_ok());
    }
}
