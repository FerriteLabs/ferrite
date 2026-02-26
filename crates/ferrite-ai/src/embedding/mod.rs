//! Embedding Module
//!
//! This module provides embedding model management including:
//! - BYOE (Bring Your Own Embedding Model) for custom models
//! - Model registry with hot-swapping
//! - A/B testing between models
//! - Model validation and benchmarking
//! - Auto-embedding pipeline for text-to-vector conversion

pub mod byoe;
pub mod catalog;
pub mod pipeline;

pub use byoe::{
    AbTestConfig, ByoeConfig, CustomEmbeddingModel, EmbedError, HttpEmbeddingModel,
    MockEmbeddingModel, ModelInfo, ModelLoader, ModelMetadata, ModelRegistry, ModelSource,
    ModelSourceType, ModelStats, RegistryMetrics, RegistryMetricsSnapshot, ValidationCheck,
    ValidationResult,
};
pub use catalog::{BuiltinModel, ManagedEmbeddingConfig, ModelCatalogEntry, ModelManager};
pub use pipeline::{
    CohereEmbeddingProvider, EmbeddingPipeline, EmbeddingProvider, EmbeddingResult,
    HttpEmbeddingProvider, MockEmbeddingProvider, OpenAIEmbeddingProvider, PipelineConfig,
    PipelineModelAdapter, PipelineStats, ProviderConfig, ProviderType, TextInput,
};
