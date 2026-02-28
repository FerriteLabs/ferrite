//! Authentication and Access Control Lists (ACLs)
//!
//! This module implements Redis-compatible ACL functionality for user management
//! and command authorization.

#![allow(dead_code)]

mod acl;
pub mod policy_engine;
mod user;

pub use acl::{Acl, AclError, SharedAcl};
pub use user::{CommandPermission, KeyPattern, Permission, User};
