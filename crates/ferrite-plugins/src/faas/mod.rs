#![forbid(unsafe_code)]

//! # Serverless Functions at the Edge (FaaS)
//!
//! Provides a FaaS runtime for deploying, invoking, and scheduling
//! WebAssembly functions within Ferrite.

pub mod registry;
pub mod runtime;
pub mod scheduler;
