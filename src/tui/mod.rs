//! Ferrite TUI Dashboard
//!
//! A built-in terminal-based dashboard for real-time monitoring of Ferrite servers.
//! Provides six tabs: Overview, Keys, Commands, Memory, Network, and Logs.
//!
//! # Usage
//!
//! ```bash
//! cargo run --features tui --bin ferrite-tui -- -h 127.0.0.1 -p 6379
//! ```
//!
//! # Architecture
//!
//! - [`app`] - Application state, event loop, and keyboard handling
//! - [`ui`] - Layout rendering for each tab
//! - [`widgets`] - Reusable TUI widget components
//! - [`metrics`] - Redis protocol data collection and INFO parsing

#[cfg(feature = "tui")]
pub mod app;
#[cfg(feature = "tui")]
pub mod metrics;
#[cfg(feature = "tui")]
pub mod ui;
#[cfg(feature = "tui")]
pub mod widgets;
