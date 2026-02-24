//! Data Mesh Gateway
//!
//! Provides a unified gateway for accessing heterogeneous data sources
//! through virtual namespaces, data contracts, and cross-source query routing.

#![forbid(unsafe_code)]
#[allow(dead_code)]
pub mod contract;
pub mod datasource;
pub mod gateway;
pub mod query_router;

pub use contract::{ContractRegistry, ContractSla, ContractStatus, DataContract};
pub use datasource::{
    DataSchema, DataSourceConfig, DataSourceStatus, DataSourceType, FieldType, SchemaField,
};
pub use gateway::{DataMeshGateway, GatewayConfig, GatewayStats};
pub use query_router::{QueryPlan, QueryRouter, QueryStep, StepType};
