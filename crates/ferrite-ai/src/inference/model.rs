//! Model Registry and Management
//!
//! Handles model loading, versioning, and lifecycle management.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::{InferenceError, InferenceInput, InferenceOutput, Result};

/// Model format/runtime
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub enum ModelFormat {
    /// ONNX Runtime
    #[default]
    ONNX,
    /// TensorFlow Lite
    TFLite,
    /// PyTorch TorchScript
    TorchScript,
    /// Custom format
    Custom(String),
}



/// Model configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelConfig {
    /// Model name/identifier
    pub name: String,
    /// Model format
    pub format: ModelFormat,
    /// Path to model file (local or remote)
    pub path: String,
    /// Model version
    pub version: String,
    /// Input shape (for validation)
    pub input_shape: Option<Vec<usize>>,
    /// Output shape (for validation)
    pub output_shape: Option<Vec<usize>>,
    /// Labels for classification models
    pub labels: Option<Vec<String>>,
    /// Maximum batch size
    pub max_batch_size: usize,
    /// Inference timeout in milliseconds
    pub timeout_ms: u64,
    /// Enable GPU acceleration
    pub use_gpu: bool,
    /// Number of inference threads
    pub num_threads: usize,
    /// Quantization level
    pub quantization: Option<QuantizationType>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl Default for ModelConfig {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            format: ModelFormat::default(),
            path: String::new(),
            version: "1.0.0".to_string(),
            input_shape: None,
            output_shape: None,
            labels: None,
            max_batch_size: 32,
            timeout_ms: 5000,
            use_gpu: false,
            num_threads: 4,
            quantization: None,
            metadata: HashMap::new(),
        }
    }
}

impl ModelConfig {
    /// Create ONNX model config
    pub fn onnx(path: impl Into<String>) -> Self {
        Self {
            format: ModelFormat::ONNX,
            path: path.into(),
            ..Default::default()
        }
    }

    /// Create TFLite model config
    pub fn tflite(path: impl Into<String>) -> Self {
        Self {
            format: ModelFormat::TFLite,
            path: path.into(),
            ..Default::default()
        }
    }

    /// Set model name
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Set version
    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
    }

    /// Set max batch size
    pub fn with_max_batch_size(mut self, size: usize) -> Self {
        self.max_batch_size = size;
        self
    }

    /// Set labels for classification
    pub fn with_labels(mut self, labels: Vec<String>) -> Self {
        self.labels = Some(labels);
        self
    }

    /// Enable GPU
    pub fn with_gpu(mut self) -> Self {
        self.use_gpu = true;
        self
    }
}

/// Quantization type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum QuantizationType {
    /// INT8 quantization
    Int8,
    /// FP16 quantization
    Float16,
    /// Dynamic quantization
    Dynamic,
}

/// Model version information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelVersion {
    /// Version string
    pub version: String,
    /// Creation timestamp
    pub created_at: u64,
    /// Is this the active version?
    pub is_active: bool,
    /// Is this a canary deployment?
    pub is_canary: bool,
    /// Traffic percentage (for A/B testing)
    pub traffic_percentage: u8,
    /// Version-specific metadata
    pub metadata: HashMap<String, String>,
}

/// Model runtime information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelInfo {
    /// Model name
    pub name: String,
    /// Current active version
    pub active_version: String,
    /// All available versions
    pub versions: Vec<ModelVersion>,
    /// Model format
    pub format: ModelFormat,
    /// Input shape
    pub input_shape: Option<Vec<usize>>,
    /// Output shape
    pub output_shape: Option<Vec<usize>>,
    /// Labels (for classification)
    pub labels: Option<Vec<String>>,
    /// Load time in milliseconds
    pub load_time_ms: u64,
    /// Total predictions made
    pub total_predictions: u64,
    /// Average latency
    pub avg_latency_ms: f64,
    /// Last used timestamp
    pub last_used: u64,
}

/// A loaded model
pub struct Model {
    config: ModelConfig,
    // In production, this would hold actual model runtime handles
    // For simulation, we track stats
    predictions: AtomicU64,
    total_latency_ms: AtomicU64,
    loaded_at: Instant,
}

impl Model {
    /// Create a new model from config
    pub fn new(config: ModelConfig) -> Self {
        Self {
            config,
            predictions: AtomicU64::new(0),
            total_latency_ms: AtomicU64::new(0),
            loaded_at: Instant::now(),
        }
    }

    /// Run inference on this model
    pub async fn predict(&self, input: &InferenceInput) -> Result<InferenceOutput> {
        let start = Instant::now();

        // Simulate inference based on input type
        let output = match input {
            InferenceInput::Vector(v) => {
                // Simulate embedding or feature extraction
                let output_vec: Vec<f32> = v.iter().map(|x| x.tanh()).collect();
                InferenceOutput::Embedding(output_vec)
            }
            InferenceInput::Text(text) => {
                // Simulate text classification
                let positive_prob: f32 = if text.contains("good") || text.contains("great") {
                    0.8
                } else if text.contains("bad") || text.contains("terrible") {
                    0.2
                } else {
                    0.5
                };

                let mut labels = HashMap::new();
                labels.insert("positive".to_string(), positive_prob);
                labels.insert("negative".to_string(), 1.0 - positive_prob);

                let label = if positive_prob > 0.5 {
                    "positive"
                } else {
                    "negative"
                };

                InferenceOutput::Classification {
                    label: label.to_string(),
                    confidence: positive_prob.max(1.0 - positive_prob),
                    all_labels: labels,
                }
            }
            InferenceInput::Scalar(v) => InferenceOutput::Scalar(*v * 2.0),
            InferenceInput::Matrix(m) => {
                let flat: Vec<f32> = m.iter().flatten().map(|x| x.tanh()).collect();
                InferenceOutput::Vector(flat)
            }
            InferenceInput::Image(_) | InferenceInput::Raw(_) => {
                // Simulate simple output
                InferenceOutput::Vector(vec![0.1, 0.2, 0.3])
            }
            InferenceInput::Named(tensors) => {
                let combined: Vec<f32> = tensors.values().flatten().copied().collect();
                InferenceOutput::Vector(combined)
            }
        };

        let latency = start.elapsed().as_millis() as u64;
        self.predictions.fetch_add(1, Ordering::Relaxed);
        self.total_latency_ms.fetch_add(latency, Ordering::Relaxed);

        Ok(output)
    }

    /// Run batched inference
    pub async fn predict_batch(&self, inputs: &[InferenceInput]) -> Result<Vec<InferenceOutput>> {
        let mut outputs = Vec::with_capacity(inputs.len());
        for input in inputs {
            outputs.push(self.predict(input).await?);
        }
        Ok(outputs)
    }

    /// Get model info
    pub fn info(&self) -> ModelInfo {
        let predictions = self.predictions.load(Ordering::Relaxed);
        let total_latency = self.total_latency_ms.load(Ordering::Relaxed);

        ModelInfo {
            name: self.config.name.clone(),
            active_version: self.config.version.clone(),
            versions: vec![ModelVersion {
                version: self.config.version.clone(),
                created_at: 0,
                is_active: true,
                is_canary: false,
                traffic_percentage: 100,
                metadata: HashMap::new(),
            }],
            format: self.config.format.clone(),
            input_shape: self.config.input_shape.clone(),
            output_shape: self.config.output_shape.clone(),
            labels: self.config.labels.clone(),
            load_time_ms: self.loaded_at.elapsed().as_millis() as u64,
            total_predictions: predictions,
            avg_latency_ms: if predictions > 0 {
                total_latency as f64 / predictions as f64
            } else {
                0.0
            },
            last_used: 0,
        }
    }

    /// Get config
    pub fn config(&self) -> &ModelConfig {
        &self.config
    }
}

/// Model registry for managing multiple models
pub struct ModelRegistry {
    models: RwLock<HashMap<String, Model>>,
    total_loads: AtomicU64,
    total_predictions: AtomicU64,
}

impl ModelRegistry {
    /// Create a new model registry
    pub fn new() -> Self {
        Self {
            models: RwLock::new(HashMap::new()),
            total_loads: AtomicU64::new(0),
            total_predictions: AtomicU64::new(0),
        }
    }

    /// Load a model
    pub async fn load(&self, config: ModelConfig) -> Result<()> {
        let name = config.name.clone();
        let model = Model::new(config);

        self.models.write().insert(name, model);
        self.total_loads.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Unload a model
    pub fn unload(&self, name: &str) -> bool {
        self.models.write().remove(name).is_some()
    }

    /// Get model info
    pub fn get_info(&self, name: &str) -> Option<ModelInfo> {
        self.models.read().get(name).map(|m| m.info())
    }

    /// Run prediction on a model
    #[allow(clippy::await_holding_lock)]
    pub async fn predict(&self, name: &str, input: &InferenceInput) -> Result<InferenceOutput> {
        let models = self.models.read();
        let model = models
            .get(name)
            .ok_or_else(|| InferenceError::ModelNotFound(name.to_string()))?;

        let result = model.predict(input).await;
        self.total_predictions.fetch_add(1, Ordering::Relaxed);
        result
    }

    /// Run batched prediction
    #[allow(clippy::await_holding_lock)]
    pub async fn predict_batch(
        &self,
        name: &str,
        inputs: &[InferenceInput],
    ) -> Result<Vec<InferenceOutput>> {
        let models = self.models.read();
        let model = models
            .get(name)
            .ok_or_else(|| InferenceError::ModelNotFound(name.to_string()))?;

        let results = model.predict_batch(inputs).await;
        self.total_predictions
            .fetch_add(inputs.len() as u64, Ordering::Relaxed);
        results
    }

    /// List all models
    pub fn list(&self) -> Vec<String> {
        self.models.read().keys().cloned().collect()
    }

    /// Check if model exists
    pub fn has_model(&self, name: &str) -> bool {
        self.models.read().contains_key(name)
    }

    /// Get total models loaded
    pub fn count(&self) -> usize {
        self.models.read().len()
    }

    /// Get registry stats
    pub fn stats(&self) -> RegistryStats {
        RegistryStats {
            total_models: self.count(),
            total_loads: self.total_loads.load(Ordering::Relaxed),
            total_predictions: self.total_predictions.load(Ordering::Relaxed),
        }
    }
}

impl Default for ModelRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Registry statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryStats {
    /// Total models currently loaded
    pub total_models: usize,
    /// Total model loads
    pub total_loads: u64,
    /// Total predictions across all models
    pub total_predictions: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_model_config_builders() {
        let onnx = ModelConfig::onnx("model.onnx")
            .with_name("test")
            .with_version("1.0.0")
            .with_max_batch_size(64);

        assert_eq!(onnx.format, ModelFormat::ONNX);
        assert_eq!(onnx.name, "test");
        assert_eq!(onnx.max_batch_size, 64);

        let tflite = ModelConfig::tflite("model.tflite").with_gpu();
        assert_eq!(tflite.format, ModelFormat::TFLite);
        assert!(tflite.use_gpu);
    }

    #[tokio::test]
    async fn test_model_prediction() {
        let config = ModelConfig::onnx("test.onnx").with_name("test");
        let model = Model::new(config);

        let input = InferenceInput::Vector(vec![1.0, 2.0, 3.0]);
        let output = model.predict(&input).await.unwrap();

        assert!(matches!(output, InferenceOutput::Embedding(_)));
    }

    #[tokio::test]
    async fn test_text_classification() {
        let config = ModelConfig::onnx("classifier.onnx")
            .with_name("sentiment")
            .with_labels(vec!["positive".to_string(), "negative".to_string()]);
        let model = Model::new(config);

        let positive = model
            .predict(&InferenceInput::Text("This is great!".to_string()))
            .await
            .unwrap();

        if let InferenceOutput::Classification {
            label, confidence, ..
        } = positive
        {
            assert_eq!(label, "positive");
            assert!(confidence > 0.7);
        } else {
            panic!("Expected classification output");
        }
    }

    #[tokio::test]
    async fn test_model_registry() {
        let registry = ModelRegistry::new();

        let config = ModelConfig::onnx("test.onnx").with_name("model1");
        registry.load(config).await.unwrap();

        assert!(registry.has_model("model1"));
        assert!(!registry.has_model("model2"));
        assert_eq!(registry.count(), 1);

        let input = InferenceInput::Vector(vec![1.0]);
        let output = registry.predict("model1", &input).await.unwrap();
        assert!(matches!(output, InferenceOutput::Embedding(_)));
    }

    #[tokio::test]
    async fn test_batch_prediction() {
        let registry = ModelRegistry::new();
        registry
            .load(ModelConfig::onnx("test.onnx").with_name("test"))
            .await
            .unwrap();

        let inputs = vec![
            InferenceInput::Vector(vec![1.0]),
            InferenceInput::Vector(vec![2.0]),
            InferenceInput::Vector(vec![3.0]),
        ];

        let outputs = registry.predict_batch("test", &inputs).await.unwrap();
        assert_eq!(outputs.len(), 3);
    }
}
