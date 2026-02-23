#![forbid(unsafe_code)]
//! Adaptive Query Optimizer
//!
//! This module provides workload-aware optimization for Ferrite. It continuously
//! profiles command patterns, applies heuristic rules to detect optimization
//! opportunities, and can automatically apply recommendations with cooldown and
//! A/B testing support.
//!
//! ## Architecture
//!
//! - **[`WorkloadProfiler`]** — Tracks command frequencies, key access patterns,
//!   and rolling statistics using lock-free concurrent data structures.
//! - **[`AdaptiveOptimizer`]** — Analyzes workload snapshots against 15+ heuristic
//!   rules and produces an [`OptimizationPlan`].
//! - **[`AutoTuner`]** — Periodically runs the optimizer and applies eligible
//!   recommendations with configurable confidence thresholds and cooldowns.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use ferrite_core::optimizer::{WorkloadProfiler, AutoTuner, CommandKind};
//!
//! let profiler = WorkloadProfiler::new();
//! profiler.record_command("GET", CommandKind::Read);
//! profiler.record_key_access("user:1", CommandKind::Read, Some(256));
//!
//! let tuner = AutoTuner::default();
//! let plan = tuner.run_cycle(&profiler);
//! println!("Recommendations: {}", plan.len());
//! ```

pub mod auto_tuner;
pub mod optimizer;
pub mod profiler;
pub mod recommendation;

pub use auto_tuner::{AppliedOptimization, AutoTuner, AutoTunerConfig, AutoTunerStatus};
pub use optimizer::AdaptiveOptimizer;
pub use profiler::{CommandKind, WorkloadProfiler, WorkloadSnapshot};
pub use recommendation::{
    Action, OptimizationPlan, Recommendation, RecommendationPriority,
};
