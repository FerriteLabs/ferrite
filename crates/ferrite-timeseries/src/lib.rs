#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! # ferrite-timeseries
//!
//! Time-series data ingestion and downsampling for Ferrite.
//!
//! Provides time-series storage with automatic downsampling, retention
//! policies, and efficient range queries over timestamped data.
//!
//! # Modules
//!
//! - [`timeseries`] — Time-series ingestion, storage, and query

pub mod timeseries;
