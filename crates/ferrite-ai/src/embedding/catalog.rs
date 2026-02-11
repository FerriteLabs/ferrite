//! Managed Embedding Model Catalog
//!
//! Pre-packaged embedding model definitions with auto-download from
//! Hugging Face Hub. Provides zero-config setup for semantic caching
//! and vector search.

use super::byoe::EmbedError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

// ---------------------------------------------------------------------------
// Model catalog
// ---------------------------------------------------------------------------

/// Well-known embedding model identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BuiltinModel {
    /// all-MiniLM-L6-v2 (384 dimensions, ~80MB)
    AllMiniLmL6V2,
    /// e5-small-v2 (384 dimensions, ~130MB)
    E5SmallV2,
    /// bge-small-en-v1.5 (384 dimensions, ~130MB)
    BgeSmallEnV15,
}

impl BuiltinModel {
    /// Human-readable name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::AllMiniLmL6V2 => "all-MiniLM-L6-v2",
            Self::E5SmallV2 => "e5-small-v2",
            Self::BgeSmallEnV15 => "bge-small-en-v1.5",
        }
    }

    /// Output embedding dimension.
    pub fn dimensions(&self) -> usize {
        match self {
            Self::AllMiniLmL6V2 => 384,
            Self::E5SmallV2 => 384,
            Self::BgeSmallEnV15 => 384,
        }
    }

    /// Approximate model file size in bytes.
    pub fn size_bytes(&self) -> u64 {
        match self {
            Self::AllMiniLmL6V2 => 80_000_000,
            Self::E5SmallV2 => 130_000_000,
            Self::BgeSmallEnV15 => 130_000_000,
        }
    }

    /// Hugging Face model repository identifier.
    pub fn hf_repo(&self) -> &'static str {
        match self {
            Self::AllMiniLmL6V2 => "sentence-transformers/all-MiniLM-L6-v2",
            Self::E5SmallV2 => "intfloat/e5-small-v2",
            Self::BgeSmallEnV15 => "BAAI/bge-small-en-v1.5",
        }
    }

    /// Expected ONNX filename within the repository.
    pub fn onnx_filename(&self) -> &'static str {
        "model.onnx"
    }

    /// SHA-256 checksum of the ONNX model (placeholder; real values would be
    /// filled once the CI pins specific model versions).
    pub fn sha256(&self) -> Option<&'static str> {
        None // TODO: pin checksums once models are verified
    }

    /// Return all builtin models.
    pub fn all() -> &'static [BuiltinModel] {
        &[
            BuiltinModel::AllMiniLmL6V2,
            BuiltinModel::E5SmallV2,
            BuiltinModel::BgeSmallEnV15,
        ]
    }

    /// Convert from a string name (case-insensitive).
    pub fn from_name(name: &str) -> Option<Self> {
        let lower = name.to_lowercase();
        match lower.as_str() {
            "all-minilm-l6-v2" | "allminilml6v2" => Some(Self::AllMiniLmL6V2),
            "e5-small-v2" | "e5smallv2" => Some(Self::E5SmallV2),
            "bge-small-en-v1.5" | "bgesmallenv15" => Some(Self::BgeSmallEnV15),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Model Manager
// ---------------------------------------------------------------------------

/// Configuration for the managed embedding service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedEmbeddingConfig {
    /// Directory where models are stored locally.
    pub models_dir: PathBuf,
    /// Default model used when none is specified.
    pub default_model: BuiltinModel,
    /// Whether to allow automatic downloads on first use.
    pub auto_download: bool,
    /// HTTP connect timeout for downloads.
    #[serde(with = "humantime_serde")]
    pub download_timeout: std::time::Duration,
}

mod humantime_serde {
    use serde::{self, Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(duration.as_secs())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(secs))
    }
}

impl Default for ManagedEmbeddingConfig {
    fn default() -> Self {
        Self {
            models_dir: PathBuf::from("./models"),
            default_model: BuiltinModel::AllMiniLmL6V2,
            auto_download: true,
            download_timeout: std::time::Duration::from_secs(300),
        }
    }
}

/// Manages local embedding model lifecycle: discovery, download, loading.
pub struct ModelManager {
    config: ManagedEmbeddingConfig,
    /// Tracks which models are locally available.
    local_models: HashMap<BuiltinModel, PathBuf>,
}

impl ModelManager {
    /// Create a new manager, scanning the local `models_dir` for existing models.
    pub fn new(config: ManagedEmbeddingConfig) -> Self {
        let mut mgr = Self {
            config,
            local_models: HashMap::new(),
        };
        mgr.scan_local();
        mgr
    }

    /// Scan the models directory for already-downloaded model files.
    pub fn scan_local(&mut self) {
        self.local_models.clear();
        for model in BuiltinModel::all() {
            let path = self.model_path(model);
            if path.exists() {
                self.local_models.insert(*model, path);
            }
        }
    }

    /// Resolved path for a given model's ONNX file.
    pub fn model_path(&self, model: &BuiltinModel) -> PathBuf {
        self.config
            .models_dir
            .join(model.name())
            .join(model.onnx_filename())
    }

    /// Check whether a model is available locally.
    pub fn is_available(&self, model: &BuiltinModel) -> bool {
        self.local_models.contains_key(model)
    }

    /// List all locally available models.
    pub fn available_models(&self) -> Vec<&BuiltinModel> {
        self.local_models.keys().collect()
    }

    /// Ensure the given model is available locally, downloading it if allowed.
    ///
    /// Returns the local filesystem path to the ONNX file.
    pub async fn ensure_model(&mut self, model: &BuiltinModel) -> Result<PathBuf, EmbedError> {
        if let Some(path) = self.local_models.get(model) {
            return Ok(path.clone());
        }

        if !self.config.auto_download {
            return Err(EmbedError::ModelNotFound(format!(
                "model {} not found locally and auto_download is disabled",
                model.name()
            )));
        }

        self.download_model(model).await?;
        self.scan_local();

        self.local_models.get(model).cloned().ok_or_else(|| {
            EmbedError::ModelNotFound("download succeeded but model file not found".to_string())
        })
    }

    /// Ensure the default model is available.
    pub async fn ensure_default(&mut self) -> Result<PathBuf, EmbedError> {
        let model = self.config.default_model;
        self.ensure_model(&model).await
    }

    /// Download a model from Hugging Face Hub.
    async fn download_model(&self, model: &BuiltinModel) -> Result<(), EmbedError> {
        let url = format!(
            "https://huggingface.co/{}/resolve/main/{}",
            model.hf_repo(),
            model.onnx_filename()
        );
        let dest = self.model_path(model);

        tracing::info!(model = model.name(), url = %url, "downloading embedding model");

        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                EmbedError::ConfigError(format!("failed to create model directory: {e}"))
            })?;
        }

        let client = reqwest::Client::builder()
            .timeout(self.config.download_timeout)
            .build()
            .map_err(|e| EmbedError::ConfigError(format!("http client error: {e}")))?;

        let response: reqwest::Response = client
            .get(&url)
            .send()
            .await
            .map_err(|e| EmbedError::EmbeddingFailed(format!("download failed: {e}")))?;

        if !response.status().is_success() {
            return Err(EmbedError::EmbeddingFailed(format!(
                "download failed with status {}",
                response.status()
            )));
        }

        let bytes: bytes::Bytes = response.bytes().await.map_err(|e| {
            EmbedError::EmbeddingFailed(format!("failed to read response body: {e}"))
        })?;

        std::fs::write(&dest, &bytes)
            .map_err(|e| EmbedError::ConfigError(format!("failed to write model file: {e}")))?;

        tracing::info!(
            model = model.name(),
            size = bytes.len(),
            path = %dest.display(),
            "model downloaded successfully"
        );

        Ok(())
    }

    /// Remove a locally cached model.
    pub fn remove_model(&mut self, model: &BuiltinModel) -> Result<(), EmbedError> {
        let dir = self.config.models_dir.join(model.name());
        if dir.exists() {
            std::fs::remove_dir_all(&dir)
                .map_err(|e| EmbedError::ConfigError(format!("failed to remove model: {e}")))?;
        }
        self.local_models.remove(model);
        Ok(())
    }

    /// Return metadata for a builtin model.
    pub fn model_info(model: &BuiltinModel) -> ModelCatalogEntry {
        ModelCatalogEntry {
            name: model.name().to_string(),
            dimensions: model.dimensions(),
            size_bytes: model.size_bytes(),
            hf_repo: model.hf_repo().to_string(),
            description: match model {
                BuiltinModel::AllMiniLmL6V2 => {
                    "General-purpose English sentence embeddings. Good balance of speed and quality.".to_string()
                }
                BuiltinModel::E5SmallV2 => {
                    "Microsoft E5 small model. Strong performance on retrieval benchmarks.".to_string()
                }
                BuiltinModel::BgeSmallEnV15 => {
                    "BAAI BGE model. Excellent for search and RAG applications.".to_string()
                }
            },
        }
    }
}

/// Catalog entry describing a builtin model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelCatalogEntry {
    pub name: String,
    pub dimensions: usize,
    pub size_bytes: u64,
    pub hf_repo: String,
    pub description: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn builtin_model_round_trip() {
        for model in BuiltinModel::all() {
            assert_eq!(BuiltinModel::from_name(model.name()), Some(*model));
        }
    }

    #[test]
    fn model_manager_scan_empty_dir() {
        let tmp = TempDir::new().unwrap();
        let mgr = ModelManager::new(ManagedEmbeddingConfig {
            models_dir: tmp.path().to_path_buf(),
            ..Default::default()
        });
        assert!(mgr.available_models().is_empty());
    }

    #[test]
    fn model_manager_detects_existing_model() {
        let tmp = TempDir::new().unwrap();
        let model = BuiltinModel::AllMiniLmL6V2;
        let model_dir = tmp.path().join(model.name());
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(model_dir.join(model.onnx_filename()), b"fake-onnx").unwrap();

        let mgr = ModelManager::new(ManagedEmbeddingConfig {
            models_dir: tmp.path().to_path_buf(),
            ..Default::default()
        });
        assert!(mgr.is_available(&model));
    }

    #[test]
    fn remove_model_cleans_up() {
        let tmp = TempDir::new().unwrap();
        let model = BuiltinModel::E5SmallV2;
        let model_dir = tmp.path().join(model.name());
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(model_dir.join(model.onnx_filename()), b"data").unwrap();

        let mut mgr = ModelManager::new(ManagedEmbeddingConfig {
            models_dir: tmp.path().to_path_buf(),
            ..Default::default()
        });
        assert!(mgr.is_available(&model));
        mgr.remove_model(&model).unwrap();
        assert!(!mgr.is_available(&model));
        assert!(!model_dir.exists());
    }

    #[test]
    fn catalog_entry_has_content() {
        for model in BuiltinModel::all() {
            let entry = ModelManager::model_info(model);
            assert!(!entry.description.is_empty());
            assert_eq!(entry.dimensions, model.dimensions());
        }
    }
}
