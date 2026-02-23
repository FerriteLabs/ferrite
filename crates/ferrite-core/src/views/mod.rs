#![forbid(unsafe_code)]

//! # Real-Time Materialized Views
//!
//! Materialized views cache the results of FerriteQL queries and automatically
//! stay in sync with their source data. Three refresh strategies are supported:
//!
//! | Strategy   | Behaviour                                       |
//! |------------|-------------------------------------------------|
//! | **Eager**  | Re-compute immediately when source keys change. |
//! | **Lazy**   | Mark as stale on change; refresh on next read.   |
//! | **Periodic** | Refresh on a configurable timer interval.      |
//!
//! ## Commands
//!
//! - `VIEW.CREATE name query [STRATEGY eager|lazy|periodic] [INTERVAL secs]`
//! - `VIEW.DROP name`
//! - `VIEW.QUERY name`
//! - `VIEW.LIST`
//! - `VIEW.REFRESH name`
//! - `VIEW.INFO name`

pub mod definition;
pub mod dependency;
pub mod engine;
pub mod refresh;

pub use definition::{RefreshStrategy, ViewDefinition, ViewRefreshResult, ViewRow, ViewStatus};
pub use dependency::DependencyGraph;
pub use engine::ViewEngine;
pub use refresh::ViewRefresher;
