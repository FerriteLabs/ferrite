#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! # ferrite-k8s
//!
//! Kubernetes operator and integration for Ferrite.
//!
//! Provides CRD-based lifecycle management, auto-scaling, rolling upgrades,
//! and health monitoring for Ferrite clusters on Kubernetes.
//!
//! # Modules
//!
//! - [`k8s`] — Kubernetes operator, CRD definitions, and reconciliation

pub mod k8s;
