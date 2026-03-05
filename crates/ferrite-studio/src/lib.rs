// Extension crate under active development — using targeted dead_code annotations
// #![allow(dead_code)] -- removed, using targeted annotations
#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! # ferrite-studio
//!
//! Web-based management UI and interactive playground for Ferrite.
//!
//! Provides a browser-accessible dashboard for monitoring, key inspection,
//! query execution, and interactive data exploration.
//!
//! # Modules
//!
//! - [`studio`] — Core web server and routing
//! - [`dashboard`] — Real-time monitoring dashboard
//! - [`playground`] — Interactive query playground
//! - [`devtools`] — Developer tools and debugging
//! - [`insights`] — Analytics and usage insights

pub mod dashboard;
pub mod devtools;
pub mod insights;
pub mod playground;
pub mod studio;
