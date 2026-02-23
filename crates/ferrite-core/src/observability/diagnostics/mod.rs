//! Integrated Diagnostics Suite
//!
//! Enhanced observability diagnostics including slow query analysis,
//! adaptive sampling, hot key detection, and bottleneck analysis.

#![allow(dead_code)]

pub mod bottleneck;
pub mod engine;
pub mod hotkeys;
pub mod sampling;
pub mod slowlog;

// Re-export the original diagnostics engine for backward compatibility
pub use engine::*;

// Re-export new diagnostics types
pub use bottleneck::{BottleneckAnalyzer, BottleneckReport, BottleneckType};
pub use hotkeys::{HotKeyDetector, HotKeyEntry};
pub use sampling::{AdaptiveSampler, SamplerConfig, SamplerStatus, SamplingState};
pub use slowlog::{SlowQueryAnalyzer, SlowQueryReport};
