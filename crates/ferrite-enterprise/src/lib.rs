// Extension crate under active development — using targeted dead_code annotations
// #![allow(dead_code)] -- removed, using targeted annotations
#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! # ferrite-enterprise
//!
//! Enterprise features — multi-tenancy, governance, audit, and federation.
//!
//! Provides tenant isolation, resource quotas, cross-region federation,
//! policy enforcement, active-active replication, and compliance auditing.
//!
//! # Modules
//!
//! - [`tenancy`] — Multi-tenant isolation and resource quotas
//! - [`active_active`] — Active-active geo-replication
//! - [`federation`] — Cross-cluster federation
//! - [`governance`] — Data governance and compliance
//! - [`policy`] — Policy engine and enforcement
//! - [`proxy`] — Intelligent routing proxy
//! - [`mesh`] — Service mesh integration

pub mod active_active;
pub mod federation;
pub mod governance;
pub mod mesh;
pub mod policy;
pub mod proxy;
pub mod tenancy;
