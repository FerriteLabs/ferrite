//! Streaming ML Inference Pipeline
//!
//! Native model serving with automatic batching and CDC-triggered inference.
//! Supports ONNX and TensorFlow Lite models with efficient batched execution.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                  Inference Pipeline                          │
//! ├─────────────────────────────────────────────────────────────┤
//! │  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐ │
//! │  │  Model   │   │  Batch   │   │ Runtime  │   │  Result  │ │
//! │  │ Registry │──▶│ Manager  │──▶│  Engine  │──▶│  Cache   │ │
//! │  └──────────┘   └──────────┘   └──────────┘   └──────────┘ │
//! │       │              │              │              │        │
//! │       ▼              ▼              ▼              ▼        │
//! │  Model Mgmt    Auto-Batching   ONNX/TFLite    Dedup/TTL    │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Features
//!
//! - **Model Registry**: Version management, A/B deployment, canary rollouts
//! - **Auto-Batching**: Dynamic batching for improved throughput
//! - **CDC Integration**: Trigger-based inference on data mutations
//! - **Result Caching**: Deduplicate predictions with configurable TTL
//!
//! # Example
//!
//! ```ignore
//! use ferrite::inference::{InferenceEngine, ModelConfig, InferenceRequest};
//!
//! let engine = InferenceEngine::new(config);
//!
//! // Load a model
//! engine.load_model("sentiment", ModelConfig::onnx("model.onnx")).await?;
//!
//! // Run inference
//! let result = engine.predict("sentiment", &input_data).await?;
//! ```

mod batch;
mod cache;
mod engine;
mod model;
mod trigger;

pub use batch::{BatchConfig, BatchManager, BatchResult};
pub use cache::{CachedPrediction, InferenceCache, InferenceCacheConfig};
pub use engine::{InferenceConfig, InferenceEngine, InferenceResult, InferenceStats};
pub use model::{Model, ModelConfig, ModelFormat, ModelInfo, ModelRegistry, ModelVersion};
pub use trigger::{
    InferenceTrigger, TriggerConfig, TriggerEvent, TriggerOperation, TriggerResult, TriggerStats,
};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Input data for inference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InferenceInput {
    /// Single value
    Scalar(f32),
    /// 1D array
    Vector(Vec<f32>),
    /// 2D array
    Matrix(Vec<Vec<f32>>),
    /// Text input (for NLP models)
    Text(String),
    /// Image as bytes
    Image(Vec<u8>),
    /// Raw bytes
    Raw(Vec<u8>),
    /// Named tensors
    Named(HashMap<String, Vec<f32>>),
}

/// Output from inference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InferenceOutput {
    /// Single value
    Scalar(f32),
    /// 1D array
    Vector(Vec<f32>),
    /// 2D array
    Matrix(Vec<Vec<f32>>),
    /// Classification with probabilities
    Classification {
        label: String,
        confidence: f32,
        all_labels: HashMap<String, f32>,
    },
    /// Embeddings
    Embedding(Vec<f32>),
    /// Detection boxes
    Detections(Vec<Detection>),
    /// Raw output
    Raw(Vec<u8>),
}

/// Object detection result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Detection {
    /// Class label
    pub label: String,
    /// Confidence score
    pub confidence: f32,
    /// Bounding box [x, y, width, height]
    pub bbox: [f32; 4],
}

/// Inference error types
#[derive(Debug, Clone, thiserror::Error)]
pub enum InferenceError {
    /// Model not found
    #[error("model not found: {0}")]
    ModelNotFound(String),

    /// Model loading error
    #[error("failed to load model: {0}")]
    LoadError(String),

    /// Inference runtime error
    #[error("inference error: {0}")]
    RuntimeError(String),

    /// Invalid input
    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// Timeout
    #[error("inference timed out after {0}ms")]
    Timeout(u64),

    /// Resource exhausted
    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),
}

/// Result type for inference operations
pub type Result<T> = std::result::Result<T, InferenceError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inference_input() {
        let scalar = InferenceInput::Scalar(1.0);
        let vector = InferenceInput::Vector(vec![1.0, 2.0, 3.0]);
        let text = InferenceInput::Text("hello".to_string());

        assert!(matches!(scalar, InferenceInput::Scalar(_)));
        assert!(matches!(vector, InferenceInput::Vector(_)));
        assert!(matches!(text, InferenceInput::Text(_)));
    }

    #[test]
    fn test_inference_output() {
        let mut labels = HashMap::new();
        labels.insert("positive".to_string(), 0.8);
        labels.insert("negative".to_string(), 0.2);

        let classification = InferenceOutput::Classification {
            label: "positive".to_string(),
            confidence: 0.8,
            all_labels: labels,
        };

        assert!(matches!(
            classification,
            InferenceOutput::Classification { confidence, .. } if confidence > 0.5
        ));
    }
}
