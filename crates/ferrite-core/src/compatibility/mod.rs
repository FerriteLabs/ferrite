//! Redis Compatibility Certification Module
//!
//! Provides a structured framework for tracking, testing, and reporting
//! Ferrite's compatibility with Redis commands and behaviors. Produces
//! a machine-readable compatibility matrix and certification score.

#![allow(dead_code)]
pub mod certification;
pub mod dashboard;
pub mod report;
pub mod suite;
/// Redis TCL test suite compatibility tracker and matrix generator.
pub mod tcl_compat;
pub mod tracker;
pub mod trend;

pub use certification::{
    badge_url, compute_certification, markdown_summary, CategoryScore, CertificationGrade,
    CertificationResult, FailingCommand,
};
pub use dashboard::DashboardGenerator;
pub use report::{CategoryReport, CommandReport, CompatibilityReport};
pub use suite::{CategoryResults, CompatibilitySuite, SuiteResults, TestCase, TestCaseResult};
pub use tracker::{CommandCategory, CommandStatus, CompatibilityTracker, TestResult};
pub use trend::{CiRunner, TrendEntry, TrendHistory};
